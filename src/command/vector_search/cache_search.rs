//! FT.CACHESEARCH — cache-or-search command for semantic caching.
//!
//! Checks for a semantically similar cached query before falling back to full
//! KNN search. Cache entries are normal hash keys with vector fields, indexed
//! by the same FT.CREATE index. TTL is managed by the application via EXPIRE.
//!
//! Syntax:
//! ```text
//! FT.CACHESEARCH idx cache_prefix "*=>[KNN k @vec $query]" PARAMS 2 query <blob>
//!   THRESHOLD 0.95 FALLBACK KNN 10 [FILTER ...] [LIMIT offset count]
//! ```

use bytes::Bytes;

use crate::protocol::Frame;
use crate::vector::store::VectorStore;
use crate::vector::types::DistanceMetric;

use super::{
    extract_bulk, extract_param_blob, matches_keyword, parse_filter_clause, parse_knn_query,
    parse_limit_clause, parse_usize, search_local_filtered,
};

/// Parsed arguments for FT.CACHESEARCH.
struct CacheSearchArgs {
    index_name: Bytes,
    cache_prefix: Bytes,
    query_blob: Bytes,
    threshold: f32,
    fallback_k: usize,
    filter: Option<crate::vector::filter::FilterExpr>,
    offset: usize,
    count: usize,
}

/// Parse FT.CACHESEARCH arguments.
///
/// Expected layout:
///   args[0] = index_name
///   args[1] = cache_prefix (e.g. "cache:query:")
///   args[2] = query string (KNN syntax)
///   args[3..] = PARAMS ... THRESHOLD f32 FALLBACK KNN k [FILTER ...] [LIMIT offset count]
fn parse_cachesearch_args(args: &[Frame]) -> Result<CacheSearchArgs, Frame> {
    if args.len() < 6 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.CACHESEARCH' command",
        )));
    }

    let index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid index name"))),
    };

    let cache_prefix = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR invalid cache prefix",
            )));
        }
    };

    let query_str = match extract_bulk(&args[2]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid query"))),
    };

    // Parse KNN from query string: "*=>[KNN <k> @<field> $<param_name>]"
    // We don't use k from the query string directly; FALLBACK KNN k overrides it.
    let (_knn_k, _field_name, param_name) = match parse_knn_query(&query_str) {
        Some(parsed) => parsed,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR invalid KNN query syntax",
            )));
        }
    };

    // Extract query vector blob from PARAMS section.
    // Shift args by 1 for cache_prefix: PARAMS starts at args[3+] but
    // extract_param_blob scans from index 2, so we pass &args[1..] to
    // align the scan correctly (args[1] becomes the "query" position for extract_param_blob).
    let query_blob = match extract_param_blob(&args[1..], &param_name) {
        Some(blob) => blob,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR query vector parameter not found in PARAMS",
            )));
        }
    };

    // Scan for THRESHOLD keyword
    let mut threshold: Option<f32> = None;
    let mut fallback_k: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        if matches_keyword(&args[i], b"THRESHOLD") {
            i += 1;
            if i >= args.len() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR THRESHOLD requires a float value",
                )));
            }
            threshold = match extract_bulk(&args[i]) {
                Some(b) => std::str::from_utf8(&b)
                    .ok()
                    .and_then(|s| s.parse::<f32>().ok()),
                None => None,
            };
            if threshold.is_none() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR invalid THRESHOLD value",
                )));
            }
        } else if matches_keyword(&args[i], b"FALLBACK") {
            i += 1;
            // Expect "KNN" keyword
            if i >= args.len() || !matches_keyword(&args[i], b"KNN") {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR FALLBACK requires KNN <k>",
                )));
            }
            i += 1;
            if i >= args.len() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR FALLBACK KNN requires a count",
                )));
            }
            fallback_k = parse_usize(&args[i]);
            if fallback_k.is_none() {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR invalid FALLBACK KNN count",
                )));
            }
        }
        i += 1;
    }

    let threshold = match threshold {
        Some(t) => t,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR THRESHOLD is required for FT.CACHESEARCH",
            )));
        }
    };

    let fallback_k = match fallback_k {
        Some(k) => k,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR FALLBACK KNN is required for FT.CACHESEARCH",
            )));
        }
    };

    // Parse FILTER and LIMIT from the shifted args (offset by 1 for cache_prefix).
    let filter = parse_filter_clause(&args[1..]);
    let (offset, count) = parse_limit_clause(&args[1..]);

    Ok(CacheSearchArgs {
        index_name,
        cache_prefix,
        query_blob,
        threshold,
        fallback_k,
        filter,
        offset,
        count,
    })
}

/// Check whether a distance value is "within threshold" for a given metric.
///
/// - L2: distance <= threshold (lower is closer)
/// - Cosine / InnerProduct: distance >= threshold (higher is more similar)
#[inline]
fn is_within_threshold(distance: f32, threshold: f32, metric: DistanceMetric) -> bool {
    match metric {
        DistanceMetric::L2 => distance <= threshold,
        DistanceMetric::Cosine | DistanceMetric::InnerProduct => distance >= threshold,
    }
}

/// Probe the vector index for cache hits: entries whose Redis key starts with
/// `cache_prefix` and whose vector is within `threshold` of the query.
///
/// Returns the best matching cache entry's (redis_key, distance, key_hash) if
/// found within threshold. This is O(cache_size) brute-force on prefix-matched
/// entries -- acceptable because the cache prefix set is small (tens to hundreds).
///
/// Implementation: searches the full index (mutable + immutable segments) via
/// the existing search infrastructure, then filters results by cache prefix.
fn cache_probe(
    store: &mut VectorStore,
    index_name: &[u8],
    cache_prefix: &[u8],
    query_blob: &[u8],
    threshold: f32,
) -> Option<CacheHit> {
    let idx = store.get_index(index_name)?;
    let metric = idx.meta.metric;
    let key_hash_to_key = &idx.key_hash_to_key;

    // Count cache entries matching prefix. If zero, no probe needed.
    let cache_count = key_hash_to_key
        .values()
        .filter(|key| key.starts_with(cache_prefix))
        .count();

    if cache_count == 0 {
        return None;
    }

    // Search with k large enough to guarantee all cache entries appear in results.
    // Cache entries are in the same index, so we need k >= total_vectors to be fully
    // correct. In practice, cache hits should be the closest vectors, so a modest k
    // that covers the cache population plus margin is sufficient.
    // Cap at 10000 to avoid pathological performance on huge indexes.
    let k = cache_count.saturating_mul(3).clamp(100, 10_000);
    let _ = idx; // release immutable borrow before mutable search call

    let response =
        search_local_filtered(store, index_name, query_blob, k, None, 0, usize::MAX, None, 0);

    // Parse the response to find the best cache hit.
    let items = match &response {
        Frame::Array(items) => items,
        _ => return None,
    };
    if items.len() < 3 {
        return None;
    }

    // `metric` was captured before the search call; still valid since index
    // metadata is immutable after creation.
    let mut best: Option<CacheHit> = None;
    let mut pos = 1;
    while pos + 1 < items.len() {
        let doc_key = match &items[pos] {
            Frame::BulkString(b) => b,
            _ => {
                pos += 2;
                continue;
            }
        };

        // Check if this result's key starts with cache_prefix
        if doc_key.starts_with(cache_prefix) {
            // Extract score from the fields array
            let score = extract_score_from_fields(&items[pos + 1]);
            if is_within_threshold(score, threshold, metric) {
                let dominated = match &best {
                    Some(prev) => match metric {
                        DistanceMetric::L2 => score < prev.distance,
                        DistanceMetric::Cosine | DistanceMetric::InnerProduct => {
                            score > prev.distance
                        }
                    },
                    None => true,
                };
                if dominated {
                    best = Some(CacheHit {
                        key: doc_key.clone(),
                        distance: score,
                    });
                }
            }
        }
        pos += 2;
    }

    best
}

/// Result of a successful cache probe.
struct CacheHit {
    key: Bytes,
    distance: f32,
}

/// Extract `__vec_score` from a fields array Frame.
fn extract_score_from_fields(fields_frame: &Frame) -> f32 {
    let fields = match fields_frame {
        Frame::Array(f) => f,
        _ => return f32::MAX,
    };
    let mut i = 0;
    while i + 1 < fields.len() {
        if let Frame::BulkString(name) = &fields[i] {
            if name.as_ref() == b"__vec_score" {
                if let Frame::BulkString(val) = &fields[i + 1] {
                    if let Ok(s) = std::str::from_utf8(val) {
                        return s.parse::<f32>().unwrap_or(f32::MAX);
                    }
                }
            }
        }
        i += 2;
    }
    f32::MAX
}

/// FT.CACHESEARCH — cache-or-search in a single RTT.
///
/// 1. Probes cache: searches the index for entries whose key starts with
///    `cache_prefix` and whose vector distance is within `threshold`.
/// 2. On cache HIT: returns the cached entry with `cache_hit: "true"` metadata.
/// 3. On cache MISS: falls back to full KNN search with `fallback_k` and returns
///    results with `cache_hit: "false"` metadata.
pub fn ft_cachesearch(store: &mut VectorStore, args: &[Frame]) -> Frame {
    let parsed = match parse_cachesearch_args(args) {
        Ok(p) => p,
        Err(e) => return e,
    };

    // Phase 1: cache probe
    let hit = cache_probe(
        store,
        &parsed.index_name,
        &parsed.cache_prefix,
        &parsed.query_blob,
        parsed.threshold,
    );

    if let Some(cache_hit) = hit {
        // Cache HIT: build response with single cached result + cache_hit metadata.
        let mut score_buf = String::with_capacity(16);
        use std::fmt::Write;
        let _ = write!(score_buf, "{}", cache_hit.distance);

        let fields: crate::protocol::FrameVec = vec![
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from(score_buf)),
            Frame::BulkString(Bytes::from_static(b"cache_hit")),
            Frame::BulkString(Bytes::from_static(b"true")),
        ]
        .into();

        let items = vec![
            Frame::Integer(1),
            Frame::BulkString(cache_hit.key),
            Frame::Array(fields),
        ];
        crate::vector::metrics::increment_search();
        return Frame::Array(items.into());
    }

    // Phase 2: cache MISS — fall back to full KNN search.
    let knn_result = search_local_filtered(
        store,
        &parsed.index_name,
        &parsed.query_blob,
        parsed.fallback_k,
        parsed.filter.as_ref(),
        parsed.offset,
        parsed.count,
        None, // cache search always uses default field
        0,    // non-temporal
    );

    // Augment each result with cache_hit: "false" metadata.
    let augmented = augment_with_cache_metadata(knn_result, false);
    crate::vector::metrics::increment_search();
    augmented
}

// -- Test-accessible wrappers --

/// Test wrapper for `parse_cachesearch_args`.
#[cfg(test)]
pub fn parse_cachesearch_args_for_test(
    args: &[Frame],
) -> Result<(Bytes, Bytes, Bytes, f32, usize), Frame> {
    let parsed = parse_cachesearch_args(args)?;
    Ok((
        parsed.index_name,
        parsed.cache_prefix,
        parsed.query_blob,
        parsed.threshold,
        parsed.fallback_k,
    ))
}

/// Test wrapper for `is_within_threshold`.
#[cfg(test)]
pub fn is_within_threshold_for_test(distance: f32, threshold: f32, metric: DistanceMetric) -> bool {
    is_within_threshold(distance, threshold, metric)
}

/// Test wrapper for `augment_with_cache_metadata`.
#[cfg(test)]
pub fn augment_with_cache_metadata_for_test(response: Frame, is_hit: bool) -> Frame {
    augment_with_cache_metadata(response, is_hit)
}

/// Add `cache_hit` field to each document's fields array in an FT.SEARCH response.
fn augment_with_cache_metadata(response: Frame, is_hit: bool) -> Frame {
    let items = match response {
        Frame::Array(items) => items,
        other => return other,
    };
    if items.len() < 2 {
        return Frame::Array(items);
    }

    let cache_val = if is_hit {
        Bytes::from_static(b"true")
    } else {
        Bytes::from_static(b"false")
    };

    let mut result = Vec::with_capacity(items.len());
    result.push(items[0].clone()); // total count

    let mut pos = 1;
    while pos + 1 < items.len() {
        result.push(items[pos].clone()); // doc key

        // Augment fields array
        let mut fields = match &items[pos + 1] {
            Frame::Array(f) => f.to_vec(),
            _ => Vec::new(),
        };
        fields.push(Frame::BulkString(Bytes::from_static(b"cache_hit")));
        fields.push(Frame::BulkString(cache_val.clone()));
        result.push(Frame::Array(fields.into()));
        pos += 2;
    }

    Frame::Array(result.into())
}
