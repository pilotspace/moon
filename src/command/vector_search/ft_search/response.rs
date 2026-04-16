//! FT.SEARCH response builders and cross-shard merge helpers.
//!
//! Split from the monolithic `ft_search.rs` in Phase 152 (Plan 02.5). This file
//! owns Frame construction: single-shard `build_search_response`, hybrid
//! `build_hybrid_response`, graph-expansion `build_combined_response`, and the
//! `merge_search_results` coordinator function. Also hosts `parse_ft_search_args`
//! which returns a tuple for cross-shard dispatch (closely coupled to
//! `merge_search_results`), plus the private `extract_score_from_fields` helper.
//!
//! No behavior change relative to the original `ft_search.rs` — pure relocation.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::vector::filter::FilterExpr;
use crate::vector::types::SearchResult;

use super::parse::{extract_param_blob, parse_filter_clause, parse_knn_query, parse_limit_clause};

/// Build FT.SEARCH response array with pagination.
/// Format: [total_matches, "doc:0", ["__vec_score", "0.5"], "doc:1", ["__vec_score", "0.8"], ...]
///
/// `total` (first element) is always the full number of matching results (before pagination).
/// Only the slice `results[offset..offset+count]` is included as document entries.
/// LIMIT 0 0 returns `[total]` with no documents (count-only mode).
///
/// Looks up the original Redis key via `key_hash_to_key` map (populated at insert time
/// in `auto_index_hset`). Falls back to `vec:<internal_id>` only if the mapping is missing
/// (e.g., legacy data restored from a snapshot without the key map).
pub(crate) fn build_search_response(
    results: &SmallVec<[SearchResult; 32]>,
    key_hash_to_key: &std::collections::HashMap<u64, Bytes>,
    offset: usize,
    count: usize,
) -> Frame {
    let total = results.len() as i64;
    // NOTE: Vec/format! usage here is acceptable -- this is response building at end
    // of command path, not hot-path dispatch.
    let page: SmallVec<[&SearchResult; 32]> = results.iter().skip(offset).take(count).collect();
    let mut items = Vec::with_capacity(1 + page.len() * 2);
    items.push(Frame::Integer(total));

    for r in page {
        // Try to resolve original Redis key from key_hash; fallback to vec:<id>
        let doc_id = if r.key_hash != 0 {
            if let Some(orig_key) = key_hash_to_key.get(&r.key_hash) {
                orig_key.clone()
            } else {
                let mut buf = itoa::Buffer::new();
                let id_str = buf.format(r.id.0);
                let mut v = Vec::with_capacity(4 + id_str.len());
                v.extend_from_slice(b"vec:");
                v.extend_from_slice(id_str.as_bytes());
                Bytes::from(v)
            }
        } else {
            let mut buf = itoa::Buffer::new();
            let id_str = buf.format(r.id.0);
            let mut v = Vec::with_capacity(4 + id_str.len());
            v.extend_from_slice(b"vec:");
            v.extend_from_slice(id_str.as_bytes());
            Bytes::from(v)
        };
        items.push(Frame::BulkString(doc_id));

        // Score as nested array — use write! to pre-allocated buffer
        let mut score_buf = String::with_capacity(16);
        use std::fmt::Write;
        let _ = write!(score_buf, "{}", r.distance);
        let score_str = score_buf;
        let fields = vec![
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from(score_str)),
        ];
        items.push(Frame::Array(fields.into()));
    }

    Frame::Array(items.into())
}

/// Merge multiple per-shard FT.SEARCH responses into a global result with pagination.
///
/// Each shard response is: [num_results, doc_id, [score_fields], doc_id, [score_fields], ...]
/// This function extracts all (doc_id, score) pairs, sorts by score ascending (lower
/// distance = better), takes top-K, then applies LIMIT pagination (offset, count).
///
/// Cross-shard strategy: each shard returns up to k results. The coordinator merges
/// all shard results, sorts globally, truncates to k, then applies offset+count slice.
/// `total` in the response = total merged results before pagination (capped at k).
pub fn merge_search_results(
    shard_responses: &[Frame],
    k: usize,
    offset: usize,
    count: usize,
) -> Frame {
    // Collect all (score, doc_id, fields_frame) triples
    let mut all_results: Vec<(f32, Bytes, Frame)> = Vec::new();

    for resp in shard_responses {
        let items = match resp {
            Frame::Array(items) => items,
            Frame::Error(_) => continue, // skip errored shards
            _ => continue,
        };
        if items.is_empty() {
            continue;
        }
        // items[0] = count, then pairs of (doc_id, fields_array)
        let mut i = 1;
        while i + 1 < items.len() {
            let doc_id = match &items[i] {
                Frame::BulkString(b) => b.clone(),
                _ => {
                    i += 2;
                    continue;
                }
            };
            let fields = items[i + 1].clone();
            let score = extract_score_from_fields(&fields);
            all_results.push((score, doc_id, fields));
            i += 2;
        }
    }

    // Sort by score ascending (lower distance = better match)
    all_results.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    all_results.truncate(k);

    // Total = all merged results (before pagination, after k-truncation)
    let total = all_results.len() as i64;

    // Apply LIMIT pagination: skip offset, take count
    let page: Vec<&(f32, Bytes, Frame)> = all_results.iter().skip(offset).take(count).collect();

    let mut items = Vec::with_capacity(1 + page.len() * 2);
    items.push(Frame::Integer(total));
    for (_, doc_id, fields) in page {
        items.push(Frame::BulkString(doc_id.clone()));
        items.push(fields.clone());
    }
    Frame::Array(items.into())
}

/// Extract the numeric score from a fields array like ["__vec_score", "0.5"].
pub(super) fn extract_score_from_fields(fields: &Frame) -> f32 {
    if let Frame::Array(items) = fields {
        for pair in items.chunks(2) {
            if pair.len() == 2 {
                if let Frame::BulkString(key) = &pair[0] {
                    if key.as_ref() == b"__vec_score" {
                        if let Frame::BulkString(val) = &pair[1] {
                            if let Ok(s) = std::str::from_utf8(val) {
                                return s.parse().unwrap_or(f32::MAX);
                            }
                        }
                    }
                }
            }
        }
    }
    f32::MAX
}

/// Parse FT.SEARCH arguments into (index_name, query_blob, k, filter, offset, count).
///
/// Used by connection handlers to extract search parameters before dispatching
/// to the coordinator's scatter_vector_search_remote. Returns Err(Frame::Error)
/// if args are malformed.
///
/// The last two tuple elements are LIMIT offset and count:
/// - Default (no LIMIT): offset=0, count=usize::MAX (return all results)
/// - LIMIT 0 0: count-only mode (returns total but no documents)
pub fn parse_ft_search_args(
    args: &[Frame],
) -> Result<(Bytes, Bytes, usize, Option<FilterExpr>, usize, usize), Frame> {
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.SEARCH' command",
        )));
    }

    let index_name = match crate::command::vector_search::extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid index name"))),
    };

    let query_str = match crate::command::vector_search::extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid query"))),
    };

    let (k, _field_name, param_name) = match parse_knn_query(&query_str) {
        Some(parsed) => parsed,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR invalid KNN query syntax",
            )));
        }
    };

    let query_blob = match extract_param_blob(args, &param_name) {
        Some(blob) => blob,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR query vector parameter not found in PARAMS",
            )));
        }
    };

    let filter = parse_filter_clause(args);
    let (limit_offset, limit_count) = parse_limit_clause(args);
    Ok((index_name, query_blob, k, filter, limit_offset, limit_count))
}

/// Build FT.SEARCH hybrid response with dense_hits and sparse_hits metadata.
///
/// Format: `[total, doc_id, [score_fields], ..., "dense_hits", N, "sparse_hits", M]`
///
/// The metadata fields appear after all document entries so existing parsers
/// that stop at `total` document pairs remain backward-compatible.
pub(crate) fn build_hybrid_response(
    results: &[SearchResult],
    key_hash_to_key: &std::collections::HashMap<u64, Bytes>,
    dense_count: usize,
    sparse_count: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let total = results.len() as i64;
    let page: Vec<&SearchResult> = results.iter().skip(offset).take(count).collect();
    // 1 (total) + page*2 (doc+fields) + 4 (metadata: dense_hits key, value, sparse_hits key, value)
    let mut items = Vec::with_capacity(1 + page.len() * 2 + 4);
    items.push(Frame::Integer(total));

    for r in &page {
        // Resolve doc key
        let doc_id = if r.key_hash != 0 {
            if let Some(orig_key) = key_hash_to_key.get(&r.key_hash) {
                orig_key.clone()
            } else {
                let mut buf = itoa::Buffer::new();
                let id_str = buf.format(r.id.0);
                let mut v = Vec::with_capacity(4 + id_str.len());
                v.extend_from_slice(b"vec:");
                v.extend_from_slice(id_str.as_bytes());
                Bytes::from(v)
            }
        } else {
            let mut buf = itoa::Buffer::new();
            let id_str = buf.format(r.id.0);
            let mut v = Vec::with_capacity(4 + id_str.len());
            v.extend_from_slice(b"vec:");
            v.extend_from_slice(id_str.as_bytes());
            Bytes::from(v)
        };
        items.push(Frame::BulkString(doc_id));

        // Score field — use absolute value of RRF score for display
        let mut score_buf = String::with_capacity(16);
        use std::fmt::Write;
        let _ = write!(score_buf, "{}", r.distance.abs());
        let fields = vec![
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from(score_buf)),
        ];
        items.push(Frame::Array(fields.into()));
    }

    // Append metadata: dense_hits and sparse_hits
    items.push(Frame::BulkString(Bytes::from_static(b"dense_hits")));
    items.push(Frame::Integer(dense_count as i64));
    items.push(Frame::BulkString(Bytes::from_static(b"sparse_hits")));
    items.push(Frame::Integer(sparse_count as i64));

    Frame::Array(items.into())
}

/// Extract (redis_key, vec_score) pairs from an FT.SEARCH response Frame.
///
/// Response format: [total, key1, [__vec_score, "0.5"], key2, [__vec_score, "0.8"], ...]
#[cfg(feature = "graph")]
pub(crate) fn extract_seeds_from_response(response: &Frame) -> Vec<(Bytes, f32)> {
    let items = match response {
        Frame::Array(items) => items,
        _ => return Vec::new(),
    };
    if items.len() < 3 {
        return Vec::new();
    }
    let mut seeds = Vec::new();
    let mut i = 1;
    while i + 1 < items.len() {
        let key = match &items[i] {
            Frame::BulkString(b) => b.clone(),
            _ => {
                i += 2;
                continue;
            }
        };
        let score = extract_score_from_fields(&items[i + 1]);
        seeds.push((key, score));
        i += 2;
    }
    seeds
}

/// Build combined FT.SEARCH response with both KNN and graph-expanded results.
///
/// Original KNN results get `__graph_hops` = "0".
/// Expanded graph results get `__vec_score` = "0" and `__graph_hops` = hop distance.
/// The total count includes both KNN and expanded results.
#[cfg(feature = "graph")]
pub(crate) fn build_combined_response(
    knn_response: &Frame,
    expanded: &[crate::command::vector_search::graph_expand::ExpandedResult],
) -> Frame {
    let knn_items = match knn_response {
        Frame::Array(items) => items,
        _ => return knn_response.clone(),
    };
    if knn_items.is_empty() {
        return knn_response.clone();
    }

    // Count original KNN results (pairs after the first Integer element).
    let knn_count = (knn_items.len().saturating_sub(1)) / 2;
    let total = (knn_count + expanded.len()) as i64;

    let mut items = Vec::with_capacity(1 + (knn_count + expanded.len()) * 2);
    items.push(Frame::Integer(total));

    // Re-emit KNN results with __graph_hops = "0" added.
    let mut i = 1;
    while i + 1 < knn_items.len() {
        items.push(knn_items[i].clone()); // doc key
        // Augment existing fields with __graph_hops
        let mut fields = match &knn_items[i + 1] {
            Frame::Array(f) => f.to_vec(),
            _ => Vec::new(),
        };
        fields.push(Frame::BulkString(Bytes::from_static(b"__graph_hops")));
        fields.push(Frame::BulkString(Bytes::from_static(b"0")));
        items.push(Frame::Array(fields.into()));
        i += 2;
    }

    // Append expanded graph results.
    for er in expanded {
        items.push(Frame::BulkString(er.key.clone()));
        let mut hop_buf = itoa::Buffer::new();
        let hop_str = hop_buf.format(er.graph_hops);
        let mut score_buf = String::with_capacity(8);
        use std::fmt::Write;
        let _ = write!(score_buf, "{}", er.vec_score);
        let fields = vec![
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from(score_buf)),
            Frame::BulkString(Bytes::from_static(b"__graph_hops")),
            Frame::BulkString(Bytes::from(hop_str.to_owned())),
        ];
        items.push(Frame::Array(fields.into()));
    }

    Frame::Array(items.into())
}
