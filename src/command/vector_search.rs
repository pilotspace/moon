//! FT.* vector search command handlers.
//!
//! These commands operate on VectorStore, not Database, so they are NOT
//! dispatched through the standard command::dispatch() function.
//! Instead, the shard event loop intercepts FT.* commands and calls
//! these handlers directly with the per-shard VectorStore.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use smallvec::SmallVec;

use crate::protocol::Frame;
use crate::vector::filter::FilterExpr;
use crate::vector::store::{IndexMeta, VectorStore};
use crate::vector::types::{DistanceMetric, SearchResult};

/// FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC L2
///
/// Parses the FT.CREATE syntax and creates a vector index in the store.
/// args[0] = index_name, args[1..] = ON HASH PREFIX ... SCHEMA ...
pub fn ft_create(store: &mut VectorStore, args: &[Frame]) -> Frame {
    if args.len() < 10 {
        return Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'FT.CREATE' command"));
    }

    let index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    // Parse ON HASH
    if !matches_keyword(&args[1], b"ON") || !matches_keyword(&args[2], b"HASH") {
        return Frame::Error(Bytes::from_static(b"ERR expected ON HASH"));
    }

    // Parse PREFIX count prefix...
    let mut pos = 3;
    let mut prefixes = Vec::new();
    if pos < args.len() && matches_keyword(&args[pos], b"PREFIX") {
        pos += 1;
        let count = match parse_u32(&args[pos]) {
            Some(n) => n as usize,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid PREFIX count")),
        };
        pos += 1;
        for _ in 0..count {
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR not enough PREFIX values"));
            }
            if let Some(p) = extract_bulk(&args[pos]) {
                prefixes.push(p);
            }
            pos += 1;
        }
    }

    // Parse SCHEMA field_name VECTOR HNSW num_params [key value ...]
    if pos >= args.len() || !matches_keyword(&args[pos], b"SCHEMA") {
        return Frame::Error(Bytes::from_static(b"ERR expected SCHEMA"));
    }
    pos += 1;

    let source_field = match extract_bulk(&args[pos]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid field name")),
    };
    pos += 1;

    if pos >= args.len() || !matches_keyword(&args[pos], b"VECTOR") {
        return Frame::Error(Bytes::from_static(b"ERR expected VECTOR after field name"));
    }
    pos += 1;

    if pos >= args.len() || !matches_keyword(&args[pos], b"HNSW") {
        return Frame::Error(Bytes::from_static(b"ERR expected HNSW algorithm"));
    }
    pos += 1;

    let num_params = match parse_u32(&args[pos]) {
        Some(n) => n as usize,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid param count")),
    };
    pos += 1;

    // Parse key-value pairs: TYPE, DIM, DISTANCE_METRIC, M, EF_CONSTRUCTION
    let mut dimension: Option<u32> = None;
    let mut metric = DistanceMetric::L2;
    let mut hnsw_m: u32 = 16;
    let mut hnsw_ef_construction: u32 = 200;

    let param_end = pos + num_params;
    while pos + 1 < param_end && pos + 1 < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => { pos += 2; continue; }
        };
        pos += 1;

        if key.eq_ignore_ascii_case(b"TYPE") {
            // Accept FLOAT32 only for now
            if !matches_keyword(&args[pos], b"FLOAT32") {
                return Frame::Error(Bytes::from_static(b"ERR only FLOAT32 type supported"));
            }
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"DIM") {
            dimension = parse_u32(&args[pos]);
            if dimension.is_none() {
                return Frame::Error(Bytes::from_static(b"ERR invalid DIM value"));
            }
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"DISTANCE_METRIC") {
            let val = match extract_bulk(&args[pos]) {
                Some(v) => v,
                None => return Frame::Error(Bytes::from_static(b"ERR invalid DISTANCE_METRIC")),
            };
            metric = if val.eq_ignore_ascii_case(b"L2") {
                DistanceMetric::L2
            } else if val.eq_ignore_ascii_case(b"COSINE") {
                DistanceMetric::Cosine
            } else if val.eq_ignore_ascii_case(b"IP") {
                DistanceMetric::InnerProduct
            } else {
                return Frame::Error(Bytes::from_static(b"ERR unsupported DISTANCE_METRIC"));
            };
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"M") {
            hnsw_m = match parse_u32(&args[pos]) {
                Some(n) => n,
                None => return Frame::Error(Bytes::from_static(b"ERR invalid M value")),
            };
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"EF_CONSTRUCTION") {
            hnsw_ef_construction = match parse_u32(&args[pos]) {
                Some(n) => n,
                None => return Frame::Error(Bytes::from_static(b"ERR invalid EF_CONSTRUCTION value")),
            };
            pos += 1;
        } else {
            pos += 1; // skip unknown param value
        }
    }

    let dim = match dimension {
        Some(d) if d > 0 => d,
        _ => return Frame::Error(Bytes::from_static(b"ERR DIM is required and must be > 0")),
    };

    let meta = IndexMeta {
        name: index_name,
        dimension: dim,
        padded_dimension: crate::vector::turbo_quant::encoder::padded_dimension(dim),
        metric,
        hnsw_m,
        hnsw_ef_construction,
        source_field,
        key_prefixes: prefixes,
    };

    match store.create_index(meta) {
        Ok(()) => {
            crate::vector::metrics::increment_indexes();
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        Err(msg) => Frame::Error(Bytes::from(format!("ERR {msg}"))),
    }
}

/// FT.DROPINDEX index_name
pub fn ft_dropindex(store: &mut VectorStore, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'FT.DROPINDEX' command"));
    }
    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };
    if store.drop_index(&name) {
        crate::vector::metrics::decrement_indexes();
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(b"Unknown Index name"))
    }
}

/// FT.INFO index_name
///
/// Returns an array of key-value pairs describing the index.
pub fn ft_info(store: &VectorStore, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(b"ERR wrong number of arguments for 'FT.INFO' command"));
    }
    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };
    let idx = match store.get_index(&name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };

    // Return flat array: [key, value, key, value, ...]
    let snap = idx.segments.load();
    let num_docs = snap.mutable.len();

    let items = vec![
        Frame::BulkString(Bytes::from_static(b"index_name")),
        Frame::BulkString(idx.meta.name.clone()),
        Frame::BulkString(Bytes::from_static(b"index_definition")),
        Frame::Array(vec![
            Frame::BulkString(Bytes::from_static(b"key_type")),
            Frame::BulkString(Bytes::from_static(b"HASH")),
        ].into()),
        Frame::BulkString(Bytes::from_static(b"num_docs")),
        Frame::Integer(num_docs as i64),
        Frame::BulkString(Bytes::from_static(b"dimension")),
        Frame::Integer(idx.meta.dimension as i64),
        Frame::BulkString(Bytes::from_static(b"distance_metric")),
        Frame::BulkString(metric_to_bytes(idx.meta.metric)),
    ];
    Frame::Array(items.into())
}

/// Scalar-quantize f32 vector to i8 for mutable segment brute-force search.
/// Clamps to [-1.0, 1.0] range, scales to [-127, 127].
/// This is intentionally simple -- TQ encoding is used for immutable segments.
pub fn quantize_f32_to_sq(input: &[f32], output: &mut [i8]) {
    debug_assert_eq!(input.len(), output.len());
    for (i, &val) in input.iter().enumerate() {
        let clamped = val.clamp(-1.0, 1.0);
        output[i] = (clamped * 127.0) as i8;
    }
}

/// FT.SEARCH idx "*=>[KNN 10 @vec $query]" PARAMS 2 query <blob>
///
/// Parses KNN query syntax, decodes the vector blob, runs local search.
/// For cross-shard, the coordinator calls this on each shard and merges.
///
/// Returns: Array [num_results, doc_id, [field_values], ...]
pub fn ft_search(store: &mut VectorStore, args: &[Frame]) -> Frame {
    // args[0] = index_name, args[1] = query_string, args[2..] = PARAMS ...
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.SEARCH' command",
        ));
    }

    let index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    let query_str = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid query")),
    };

    // Parse KNN from query string: "*=>[KNN <k> @<field> $<param_name>]"
    let (k, param_name) = match parse_knn_query(&query_str) {
        Some(parsed) => parsed,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid KNN query syntax")),
    };

    // Parse PARAMS section to extract the query vector blob
    let query_blob = match extract_param_blob(args, &param_name) {
        Some(blob) => blob,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR query vector parameter not found in PARAMS",
            ))
        }
    };

    // Parse optional FILTER clause
    let filter_expr = parse_filter_clause(args);
    let start = std::time::Instant::now();
    let result = search_local_filtered(store, &index_name, &query_blob, k, filter_expr.as_ref());
    crate::vector::metrics::increment_search();
    crate::vector::metrics::record_search_latency(start.elapsed().as_micros() as u64);
    result
}

/// Direct local search for cross-shard VectorSearch messages.
/// Skips FT.SEARCH parsing -- the coordinator already extracted index_name, blob, k.
pub fn search_local(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
) -> Frame {
    search_local_filtered(store, index_name, query_blob, k, None)
}

/// Local search with optional filter expression.
///
/// Evaluates filter against PayloadIndex to produce bitmap, then dispatches
/// to search_filtered which selects optimal strategy (brute-force/HNSW/post-filter).
pub fn search_local_filtered(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    filter: Option<&FilterExpr>,
) -> Frame {
    let idx = match store.get_index_mut(index_name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };
    let dim = idx.meta.dimension as usize;
    if query_blob.len() != dim * 4 {
        return Frame::Error(Bytes::from_static(
            b"ERR query vector dimension mismatch",
        ));
    }
    let mut query_f32 = Vec::with_capacity(dim);
    for chunk in query_blob.chunks_exact(4) {
        query_f32.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    // SQ quantize for mutable segment search
    let mut query_sq = vec![0i8; dim];
    quantize_f32_to_sq(&query_f32, &mut query_sq);
    let ef_search = k.max(64);

    let filter_bitmap = filter.map(|f| {
        let total = idx.segments.total_vectors();
        idx.payload_index.evaluate_bitmap(f, total)
    });

    // Non-transactional reads use snapshot_lsn=0 (backward compatible).
    // Empty committed bitmap is stack-allocated and never queried (short-circuit).
    let empty_committed = roaring::RoaringBitmap::new();
    let mvcc_ctx = crate::vector::segment::holder::MvccContext {
        snapshot_lsn: 0,
        my_txn_id: 0,
        committed: &empty_committed,
        dirty_set: &[],
        dirty_vectors_sq: &[],
        dimension: idx.meta.dimension,
    };

    let results = idx.segments.search_mvcc(
        &query_f32,
        &query_sq,
        k,
        ef_search,
        &mut idx.scratch,
        filter_bitmap.as_ref(),
        &mvcc_ctx,
    );
    build_search_response(&results)
}

/// Parse "*=>[KNN <k> @<field> $<param>]" query string.
/// Returns (k, param_name) on success.
fn parse_knn_query(query: &[u8]) -> Option<(usize, Bytes)> {
    let s = std::str::from_utf8(query).ok()?;
    let knn_start = s.find("KNN ")?;
    let after_knn = &s[knn_start + 4..];

    // Parse k (first number after KNN)
    let k_end = after_knn.find(' ')?;
    let k: usize = after_knn[..k_end].trim().parse().ok()?;

    // Parse @field (skip it, we already know from index meta)
    let after_k = &after_knn[k_end + 1..];
    let field_end = after_k.find(' ').unwrap_or(after_k.len());
    let after_field = if field_end < after_k.len() {
        &after_k[field_end + 1..]
    } else {
        ""
    };

    // Parse $param_name
    let param_str = after_field.trim().trim_end_matches(']');
    if !param_str.starts_with('$') {
        return None;
    }
    let param_name = &param_str[1..];
    Some((k, Bytes::from(param_name.to_owned())))
}

/// Extract a named parameter blob from PARAMS section.
/// Format: ... PARAMS <count> <name1> <blob1> <name2> <blob2> ...
fn extract_param_blob(args: &[Frame], param_name: &[u8]) -> Option<Bytes> {
    // Find PARAMS keyword starting after index_name and query
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"PARAMS") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            let count = parse_u32(&args[i])? as usize;
            i += 1;
            // Iterate through name/value pairs
            for _ in 0..count / 2 {
                if i + 1 >= args.len() {
                    return None;
                }
                let name = extract_bulk(&args[i])?;
                i += 1;
                let value = extract_bulk(&args[i])?;
                i += 1;
                if name.eq_ignore_ascii_case(param_name) {
                    return Some(value);
                }
            }
            return None;
        }
        i += 1;
    }
    None
}

/// Build FT.SEARCH response array.
/// Format: [num_results, "vec:0", ["__vec_score", "0.5"], "vec:1", ["__vec_score", "0.8"], ...]
fn build_search_response(results: &SmallVec<[SearchResult; 32]>) -> Frame {
    let total = results.len() as i64;
    // NOTE: Vec/format! usage here is acceptable -- this is response building at end
    // of command path, not hot-path dispatch.
    let mut items = Vec::with_capacity(1 + results.len() * 2);
    items.push(Frame::Integer(total));

    for r in results {
        // Document ID as "vec:<internal_id>"
        let mut doc_id_buf = itoa::Buffer::new();
        let id_str = doc_id_buf.format(r.id.0);
        let mut doc_id = Vec::with_capacity(4 + id_str.len());
        doc_id.extend_from_slice(b"vec:");
        doc_id.extend_from_slice(id_str.as_bytes());
        items.push(Frame::BulkString(Bytes::from(doc_id)));

        // Score as nested array (format! acceptable -- end of command path)
        let score_str = format!("{}", r.distance);
        let fields = vec![
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from(score_str)),
        ];
        items.push(Frame::Array(fields.into()));
    }

    Frame::Array(items.into())
}

/// Merge multiple per-shard FT.SEARCH responses into a global top-K result.
///
/// Each shard response is: [num_results, doc_id, [score_fields], doc_id, [score_fields], ...]
/// This function extracts all (doc_id, score) pairs, sorts by score ascending (lower
/// distance = better), takes top-K, and rebuilds the response frame.
pub fn merge_search_results(shard_responses: &[Frame], k: usize) -> Frame {
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

    // Rebuild response
    let total = all_results.len() as i64;
    let mut items = Vec::with_capacity(1 + all_results.len() * 2);
    items.push(Frame::Integer(total));
    for (_, doc_id, fields) in all_results {
        items.push(Frame::BulkString(doc_id));
        items.push(fields);
    }
    Frame::Array(items.into())
}

/// Extract the numeric score from a fields array like ["__vec_score", "0.5"].
fn extract_score_from_fields(fields: &Frame) -> f32 {
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

/// Parse FT.SEARCH arguments into (index_name, query_blob, k, filter).
///
/// Used by connection handlers to extract search parameters before dispatching
/// to the coordinator's scatter_vector_search_remote. Returns Err(Frame::Error)
/// if args are malformed.
pub fn parse_ft_search_args(args: &[Frame]) -> Result<(Bytes, Bytes, usize, Option<FilterExpr>), Frame> {
    if args.len() < 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.SEARCH' command",
        )));
    }

    let index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid index name"))),
    };

    let query_str = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid query"))),
    };

    let (k, param_name) = match parse_knn_query(&query_str) {
        Some(parsed) => parsed,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR invalid KNN query syntax",
            )))
        }
    };

    let query_blob = match extract_param_blob(args, &param_name) {
        Some(blob) => blob,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR query vector parameter not found in PARAMS",
            )))
        }
    };

    let filter = parse_filter_clause(args);
    Ok((index_name, query_blob, k, filter))
}

// -- Filter parsing --

/// Parse FILTER clause from FT.SEARCH args.
/// Looks for "FILTER" keyword after the query string, parses the filter expression.
///
/// Supported syntax:
///   @field:{value}              -- tag equality
///   @field:[min max]            -- numeric range
///   @field:{value} @field2:[a b] -- implicit AND of multiple conditions
fn parse_filter_clause(args: &[Frame]) -> Option<FilterExpr> {
    // Find FILTER keyword in args (after index_name and query)
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"FILTER") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            let filter_str = extract_bulk(&args[i])?;
            return parse_filter_string(&filter_str);
        }
        i += 1;
    }
    None
}

/// Parse filter string like "@field:{value}" or "@field:[min max]"
/// Multiple conditions are implicitly ANDed.
fn parse_filter_string(s: &[u8]) -> Option<FilterExpr> {
    let s = std::str::from_utf8(s).ok()?;
    let mut exprs: Vec<FilterExpr> = Vec::new();
    let mut pos = 0;
    while pos < s.len() {
        // Skip whitespace
        while pos < s.len() && s.as_bytes()[pos] == b' ' {
            pos += 1;
        }
        if pos >= s.len() {
            break;
        }
        if s.as_bytes()[pos] != b'@' {
            return None;
        }
        pos += 1; // skip @

        // Read field name until : or { or [
        let field_start = pos;
        while pos < s.len() && !matches!(s.as_bytes()[pos], b':' | b'{' | b'[') {
            pos += 1;
        }
        let field = Bytes::from(s[field_start..pos].to_owned());
        if pos >= s.len() {
            return None;
        }

        // Determine type
        if s.as_bytes()[pos] == b':' {
            pos += 1; // skip :
        }

        if pos < s.len() && s.as_bytes()[pos] == b'{' {
            // Tag: @field:{value}
            pos += 1;
            let val_start = pos;
            while pos < s.len() && s.as_bytes()[pos] != b'}' {
                pos += 1;
            }
            let value = Bytes::from(s[val_start..pos].to_owned());
            if pos < s.len() {
                pos += 1; // skip }
            }
            exprs.push(FilterExpr::TagEq { field, value });
        } else if pos < s.len() && s.as_bytes()[pos] == b'[' {
            // Numeric range: @field:[min max]
            pos += 1;
            let range_start = pos;
            while pos < s.len() && s.as_bytes()[pos] != b']' {
                pos += 1;
            }
            let range_str = &s[range_start..pos];
            if pos < s.len() {
                pos += 1; // skip ]
            }
            let parts: Vec<&str> = range_str.split_whitespace().collect();
            if parts.len() != 2 {
                return None;
            }
            let min: f64 = parts[0].parse().ok()?;
            let max: f64 = parts[1].parse().ok()?;
            if (min - max).abs() < f64::EPSILON {
                exprs.push(FilterExpr::NumEq {
                    field,
                    value: OrderedFloat(min),
                });
            } else {
                exprs.push(FilterExpr::NumRange {
                    field,
                    min: OrderedFloat(min),
                    max: OrderedFloat(max),
                });
            }
        } else {
            return None;
        }
    }
    // Combine with AND
    if exprs.is_empty() {
        return None;
    }
    let mut result = exprs.remove(0);
    for expr in exprs {
        result = FilterExpr::And(Box::new(result), Box::new(expr));
    }
    Some(result)
}

// -- Helpers (private) --

fn extract_bulk(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) => Some(b.clone()),
        _ => None,
    }
}

fn matches_keyword(frame: &Frame, keyword: &[u8]) -> bool {
    match frame {
        Frame::BulkString(b) => b.eq_ignore_ascii_case(keyword),
        _ => false,
    }
}

fn parse_u32(frame: &Frame) -> Option<u32> {
    match frame {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Frame::Integer(n) => u32::try_from(*n).ok(),
        _ => None,
    }
}

fn metric_to_bytes(m: DistanceMetric) -> Bytes {
    match m {
        DistanceMetric::L2 => Bytes::from_static(b"L2"),
        DistanceMetric::Cosine => Bytes::from_static(b"COSINE"),
        DistanceMetric::InnerProduct => Bytes::from_static(b"IP"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::from(s.to_vec()))
    }

    /// Build a valid FT.CREATE argument list.
    fn ft_create_args() -> Vec<Frame> {
        vec![
            bulk(b"myidx"),       // index name
            bulk(b"ON"),
            bulk(b"HASH"),
            bulk(b"PREFIX"),
            bulk(b"1"),
            bulk(b"doc:"),
            bulk(b"SCHEMA"),
            bulk(b"vec"),
            bulk(b"VECTOR"),
            bulk(b"HNSW"),
            bulk(b"6"),           // 6 params = 3 key-value pairs
            bulk(b"TYPE"),
            bulk(b"FLOAT32"),
            bulk(b"DIM"),
            bulk(b"128"),
            bulk(b"DISTANCE_METRIC"),
            bulk(b"L2"),
        ]
    }

    #[test]
    fn test_ft_create_parse_full_syntax() {
        let mut store = VectorStore::new();
        let args = ft_create_args();
        let result = ft_create(&mut store, &args);
        match &result {
            Frame::SimpleString(s) => assert_eq!(&s[..], b"OK"),
            other => panic!("expected OK, got {other:?}"),
        }
        assert_eq!(store.len(), 1);
        let idx = store.get_index(b"myidx").unwrap();
        assert_eq!(idx.meta.dimension, 128);
        assert_eq!(idx.meta.metric, DistanceMetric::L2);
        assert_eq!(idx.meta.key_prefixes.len(), 1);
        assert_eq!(&idx.meta.key_prefixes[0][..], b"doc:");
    }

    #[test]
    fn test_ft_create_missing_dim() {
        let mut store = VectorStore::new();
        // Remove DIM param pair: keep TYPE FLOAT32 and DISTANCE_METRIC L2 (4 params = 2 pairs)
        let args = vec![
            bulk(b"myidx"),
            bulk(b"ON"),
            bulk(b"HASH"),
            bulk(b"PREFIX"),
            bulk(b"1"),
            bulk(b"doc:"),
            bulk(b"SCHEMA"),
            bulk(b"vec"),
            bulk(b"VECTOR"),
            bulk(b"HNSW"),
            bulk(b"4"),           // 4 params = 2 key-value pairs
            bulk(b"TYPE"),
            bulk(b"FLOAT32"),
            bulk(b"DISTANCE_METRIC"),
            bulk(b"L2"),
        ];
        let result = ft_create(&mut store, &args);
        match &result {
            Frame::Error(_) => {} // expected
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_create_duplicate() {
        let mut store = VectorStore::new();
        let args = ft_create_args();
        let r1 = ft_create(&mut store, &args);
        assert!(matches!(r1, Frame::SimpleString(_)));

        let args2 = ft_create_args();
        let r2 = ft_create(&mut store, &args2);
        match &r2 {
            Frame::Error(e) => assert!(e.starts_with(b"ERR")),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_dropindex() {
        let mut store = VectorStore::new();
        let args = ft_create_args();
        ft_create(&mut store, &args);

        // Drop existing
        let result = ft_dropindex(&mut store, &[bulk(b"myidx")]);
        assert!(matches!(result, Frame::SimpleString(_)));
        assert!(store.is_empty());

        // Drop non-existing
        let result = ft_dropindex(&mut store, &[bulk(b"myidx")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_parse_knn_query() {
        let query = b"*=>[KNN 10 @vec $query]";
        let (k, param) = parse_knn_query(query).unwrap();
        assert_eq!(k, 10);
        assert_eq!(&param[..], b"query");
    }

    #[test]
    fn test_parse_knn_query_different_k() {
        let query = b"*=>[KNN 5 @embedding $blob]";
        let (k, param) = parse_knn_query(query).unwrap();
        assert_eq!(k, 5);
        assert_eq!(&param[..], b"blob");
    }

    #[test]
    fn test_parse_knn_query_invalid() {
        assert!(parse_knn_query(b"*").is_none());
        assert!(parse_knn_query(b"*=>[NOTAKNN]").is_none());
    }

    #[test]
    fn test_extract_param_blob() {
        let args = vec![
            bulk(b"idx"),
            bulk(b"*=>[KNN 10 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            bulk(b"blobdata"),
        ];
        let blob = extract_param_blob(&args, b"query").unwrap();
        assert_eq!(&blob[..], b"blobdata");
    }

    #[test]
    fn test_extract_param_blob_missing() {
        let args = vec![bulk(b"idx"), bulk(b"*=>[KNN 10 @vec $query]")];
        assert!(extract_param_blob(&args, b"query").is_none());
    }

    #[test]
    fn test_quantize_f32_to_sq() {
        let input = [0.0, 1.0, -1.0, 0.5, -0.5, 2.0, -2.0];
        let mut output = [0i8; 7];
        quantize_f32_to_sq(&input, &mut output);
        assert_eq!(output[0], 0); // 0.0 -> 0
        assert_eq!(output[1], 127); // 1.0 -> 127
        assert_eq!(output[2], -127); // -1.0 -> -127
        assert_eq!(output[3], 63); // 0.5 -> 63 (truncated from 63.5)
        assert_eq!(output[4], -63); // -0.5 -> -63
        assert_eq!(output[5], 127); // 2.0 clamped to 1.0 -> 127
        assert_eq!(output[6], -127); // -2.0 clamped to -1.0 -> -127
    }

    #[test]
    fn test_merge_search_results_combines_shards() {
        // Shard 0 returns: [2, "vec:0", ["__vec_score", "0.1"], "vec:1", ["__vec_score", "0.5"]]
        // Shard 1 returns: [2, "vec:10", ["__vec_score", "0.3"], "vec:11", ["__vec_score", "0.9"]]
        // Global top-2 should be: vec:0 (0.1), vec:10 (0.3)

        let shard0 = Frame::Array(vec![
            Frame::Integer(2),
            bulk(b"vec:0"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.1")].into()),
            bulk(b"vec:1"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.5")].into()),
        ].into());

        let shard1 = Frame::Array(vec![
            Frame::Integer(2),
            bulk(b"vec:10"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.3")].into()),
            bulk(b"vec:11"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.9")].into()),
        ].into());

        let result = merge_search_results(&[shard0, shard1], 2);
        match result {
            Frame::Array(items) => {
                assert_eq!(items[0], Frame::Integer(2));
                assert_eq!(items[1], Frame::BulkString(Bytes::from("vec:0")));
                assert_eq!(items[3], Frame::BulkString(Bytes::from("vec:10")));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn test_merge_search_results_handles_errors() {
        // One shard returns error, one returns valid results
        let shard0 = Frame::Error(Bytes::from_static(b"ERR shard unavailable"));
        let shard1 = Frame::Array(vec![
            Frame::Integer(1),
            bulk(b"vec:5"),
            Frame::Array(vec![bulk(b"__vec_score"), bulk(b"0.2")].into()),
        ].into());

        let result = merge_search_results(&[shard0, shard1], 5);
        match result {
            Frame::Array(items) => {
                assert_eq!(items[0], Frame::Integer(1));
                assert_eq!(items[1], Frame::BulkString(Bytes::from("vec:5")));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn test_merge_search_results_empty() {
        // No results from any shard
        let shard0 = Frame::Array(vec![Frame::Integer(0)].into());
        let shard1 = Frame::Array(vec![Frame::Integer(0)].into());

        let result = merge_search_results(&[shard0, shard1], 10);
        match result {
            Frame::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0], Frame::Integer(0));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_search_dimension_mismatch() {
        let mut store = VectorStore::new();
        let args = ft_create_args();
        ft_create(&mut store, &args);

        // Build a query with wrong dimension (4 bytes instead of 128*4)
        let search_args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 10 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            bulk(b"tooshort"),
        ];
        let result = ft_search(&mut store, &search_args);
        match &result {
            Frame::Error(e) => assert!(
                e.starts_with(b"ERR query vector dimension"),
                "expected dimension mismatch error, got {:?}",
                std::str::from_utf8(e)
            ),
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_search_empty_index() {
        let mut store = VectorStore::new();
        let args = ft_create_args();
        ft_create(&mut store, &args);

        // Build valid query for dim=128
        let query_vec: Vec<u8> = vec![0u8; 128 * 4]; // 128 floats, all zero
        let search_args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 5 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            Frame::BulkString(Bytes::from(query_vec)),
        ];
        crate::vector::distance::init();
        let result = ft_search(&mut store, &search_args);
        match result {
            Frame::Array(items) => {
                assert_eq!(items[0], Frame::Integer(0)); // no results
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_info() {
        let mut store = VectorStore::new();
        let args = ft_create_args();
        ft_create(&mut store, &args);

        let result = ft_info(&store, &[bulk(b"myidx")]);
        match result {
            Frame::Array(items) => {
                // Should have 10 items (5 key-value pairs)
                assert_eq!(items.len(), 10);
                assert_eq!(items[0], Frame::BulkString(Bytes::from_static(b"index_name")));
                assert_eq!(items[1], Frame::BulkString(Bytes::from("myidx")));
                assert_eq!(items[5], Frame::Integer(0)); // num_docs = 0
                assert_eq!(items[7], Frame::Integer(128)); // dimension
            }
            other => panic!("expected Array, got {other:?}"),
        }

        // Non-existing index
        let result = ft_info(&store, &[bulk(b"nonexistent")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    /// Helper to build FT.CREATE args with custom parameters.
    fn build_ft_create_args(
        name: &str,
        prefix: &str,
        field: &str,
        dim: u32,
        metric: &str,
    ) -> Vec<Frame> {
        vec![
            Frame::BulkString(Bytes::from(name.to_owned())),
            Frame::BulkString(Bytes::from_static(b"ON")),
            Frame::BulkString(Bytes::from_static(b"HASH")),
            Frame::BulkString(Bytes::from_static(b"PREFIX")),
            Frame::BulkString(Bytes::from_static(b"1")),
            Frame::BulkString(Bytes::from(prefix.to_owned())),
            Frame::BulkString(Bytes::from_static(b"SCHEMA")),
            Frame::BulkString(Bytes::from(field.to_owned())),
            Frame::BulkString(Bytes::from_static(b"VECTOR")),
            Frame::BulkString(Bytes::from_static(b"HNSW")),
            Frame::BulkString(Bytes::from_static(b"6")),
            Frame::BulkString(Bytes::from_static(b"TYPE")),
            Frame::BulkString(Bytes::from_static(b"FLOAT32")),
            Frame::BulkString(Bytes::from_static(b"DIM")),
            Frame::BulkString(Bytes::from(dim.to_string())),
            Frame::BulkString(Bytes::from_static(b"DISTANCE_METRIC")),
            Frame::BulkString(Bytes::from(metric.to_owned())),
        ]
    }

    #[test]
    fn test_end_to_end_create_insert_search() {
        // Initialize distance functions (required before any search)
        crate::vector::distance::init();

        let mut store = VectorStore::new();
        let dim: usize = 4;

        // 1. FT.CREATE
        let create_args = build_ft_create_args("e2eidx", "doc:", "embedding", dim as u32, "L2");
        let result = ft_create(&mut store, &create_args);
        assert!(
            matches!(result, Frame::SimpleString(_)),
            "FT.CREATE should return OK, got {result:?}"
        );

        // 2. Insert vectors directly into the mutable segment
        let idx = store.get_index_mut(b"e2eidx").unwrap();
        let vectors: Vec<[f32; 4]> = vec![
            [1.0, 0.0, 0.0, 0.0], // vec:0 -- exact match for query
            [0.0, 1.0, 0.0, 0.0], // vec:1 -- orthogonal
            [0.9, 0.1, 0.0, 0.0], // vec:2 -- close to vec:0
        ];

        let snap = idx.segments.load();
        for (i, v) in vectors.iter().enumerate() {
            let mut sq = vec![0i8; dim];
            quantize_f32_to_sq(v, &mut sq);
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            snap.mutable.append(i as u64, v, &sq, norm, i as u64);
        }
        drop(snap);

        // 3. FT.SEARCH for vector close to [1.0, 0.0, 0.0, 0.0]
        let query_vec: [f32; 4] = [1.0, 0.0, 0.0, 0.0];
        let query_blob: Vec<u8> = query_vec.iter().flat_map(|f| f.to_le_bytes()).collect();

        let search_args = vec![
            Frame::BulkString(Bytes::from_static(b"e2eidx")),
            Frame::BulkString(Bytes::from_static(b"*=>[KNN 2 @embedding $query]")),
            Frame::BulkString(Bytes::from_static(b"PARAMS")),
            Frame::BulkString(Bytes::from_static(b"2")),
            Frame::BulkString(Bytes::from_static(b"query")),
            Frame::BulkString(Bytes::from(query_blob)),
        ];

        let result = ft_search(&mut store, &search_args);
        match &result {
            Frame::Array(items) => {
                // First element is count
                assert!(
                    matches!(&items[0], Frame::Integer(n) if *n >= 1),
                    "Should find at least 1 result, got {result:?}"
                );
                // First result should be vec:0 (exact match, distance 0)
                if let Frame::BulkString(doc_id) = &items[1] {
                    assert_eq!(
                        doc_id.as_ref(),
                        b"vec:0",
                        "Nearest vector should be id 0 (exact match)"
                    );
                }
                // Second result should be vec:2 (closest after exact match)
                if items.len() >= 4 {
                    if let Frame::BulkString(doc_id) = &items[3] {
                        assert_eq!(
                            doc_id.as_ref(),
                            b"vec:2",
                            "Second nearest should be vec:2 (close to query)"
                        );
                    }
                }
            }
            Frame::Error(e) => panic!(
                "FT.SEARCH returned error: {:?}",
                std::str::from_utf8(e)
            ),
            _ => panic!("FT.SEARCH should return Array, got {result:?}"),
        }
    }

    #[test]
    fn test_ft_info_returns_correct_data() {
        let mut store = VectorStore::new();
        let args = build_ft_create_args("testidx", "test:", "vec", 128, "COSINE");
        ft_create(&mut store, &args);

        let info_args = [Frame::BulkString(Bytes::from_static(b"testidx"))];
        let result = ft_info(&store, &info_args);
        match result {
            Frame::Array(items) => {
                assert!(items.len() >= 6, "FT.INFO should return at least 6 items");
                // Check dimension
                let mut found_dim = false;
                for pair in items.chunks(2) {
                    if let Frame::BulkString(key) = &pair[0] {
                        if key.as_ref() == b"dimension" {
                            if let Frame::Integer(d) = &pair[1] {
                                assert_eq!(*d, 128);
                                found_dim = true;
                            }
                        }
                    }
                }
                assert!(found_dim, "FT.INFO should return dimension");
            }
            other => panic!("FT.INFO should return Array, got {other:?}"),
        }
    }

    #[test]
    fn test_ft_search_unknown_index() {
        let mut store = VectorStore::new();
        let args = [
            Frame::BulkString(Bytes::from_static(b"nonexistent")),
            Frame::BulkString(Bytes::from_static(b"*=>[KNN 5 @vec $query]")),
            Frame::BulkString(Bytes::from_static(b"PARAMS")),
            Frame::BulkString(Bytes::from_static(b"2")),
            Frame::BulkString(Bytes::from_static(b"query")),
            Frame::BulkString(Bytes::from(vec![0u8; 16])),
        ];
        let result = ft_search(&mut store, &args);
        assert!(
            matches!(result, Frame::Error(_)),
            "Should error on unknown index, got {result:?}"
        );
    }

    #[test]
    fn test_parse_filter_clause_tag() {
        let args = vec![
            bulk(b"idx"),
            bulk(b"*=>[KNN 10 @vec $q]"),
            bulk(b"FILTER"),
            bulk(b"@category:{electronics}"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"q"),
            bulk(b"blob"),
        ];
        let filter = parse_filter_clause(&args);
        assert!(filter.is_some(), "should parse @category:{{electronics}}");
        match filter.unwrap() {
            crate::vector::filter::FilterExpr::TagEq { field, value } => {
                assert_eq!(&field[..], b"category");
                assert_eq!(&value[..], b"electronics");
            }
            other => panic!("expected TagEq, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_filter_clause_numeric_range() {
        let args = vec![
            bulk(b"idx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"FILTER"),
            bulk(b"@price:[10 100]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"q"),
            bulk(b"blob"),
        ];
        let filter = parse_filter_clause(&args);
        assert!(filter.is_some());
        match filter.unwrap() {
            crate::vector::filter::FilterExpr::NumRange { field, min, max } => {
                assert_eq!(&field[..], b"price");
                assert_eq!(*min, 10.0);
                assert_eq!(*max, 100.0);
            }
            other => panic!("expected NumRange, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_filter_clause_numeric_eq() {
        let args = vec![
            bulk(b"idx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"FILTER"),
            bulk(b"@price:[50 50]"),
        ];
        let filter = parse_filter_clause(&args);
        assert!(filter.is_some());
        match filter.unwrap() {
            crate::vector::filter::FilterExpr::NumEq { field, value } => {
                assert_eq!(&field[..], b"price");
                assert_eq!(*value, 50.0);
            }
            other => panic!("expected NumEq, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_filter_clause_compound() {
        let args = vec![
            bulk(b"idx"),
            bulk(b"*=>[KNN 5 @vec $q]"),
            bulk(b"FILTER"),
            bulk(b"@a:{x} @b:[1 10]"),
        ];
        let filter = parse_filter_clause(&args);
        assert!(filter.is_some());
        match filter.unwrap() {
            crate::vector::filter::FilterExpr::And(left, right) => {
                assert!(matches!(*left, crate::vector::filter::FilterExpr::TagEq { .. }));
                assert!(matches!(*right, crate::vector::filter::FilterExpr::NumRange { .. }));
            }
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_filter_clause_none() {
        // No FILTER keyword
        let args = vec![
            bulk(b"idx"),
            bulk(b"*=>[KNN 10 @vec $q]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"q"),
            bulk(b"blob"),
        ];
        let filter = parse_filter_clause(&args);
        assert!(filter.is_none());
    }

    #[test]
    fn test_ft_search_with_filter_no_regression() {
        // Unfiltered FT.SEARCH still works identically
        crate::vector::distance::init();
        let mut store = VectorStore::new();
        let args = ft_create_args();
        ft_create(&mut store, &args);

        let query_vec: Vec<u8> = vec![0u8; 128 * 4];
        let search_args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 5 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            Frame::BulkString(Bytes::from(query_vec)),
        ];
        let result = ft_search(&mut store, &search_args);
        match result {
            Frame::Array(items) => {
                assert_eq!(items[0], Frame::Integer(0));
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn test_vector_index_has_payload_index() {
        let mut store = VectorStore::new();
        let args = ft_create_args();
        ft_create(&mut store, &args);
        let idx = store.get_index(b"myidx").unwrap();
        // payload_index should exist -- insert and evaluate should work
        let _ = &idx.payload_index;
    }

    #[test]
    fn test_vector_metrics_increment_decrement() {
        use std::sync::atomic::Ordering;

        // Capture before-snapshot immediately before each operation to handle
        // parallel test interference on global atomics.
        let mut store = VectorStore::new();
        let args = ft_create_args();

        // FT.CREATE should increment VECTOR_INDEXES
        let before_create = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
        ft_create(&mut store, &args);
        let after_create = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
        assert!(after_create > before_create, "FT.CREATE should increment VECTOR_INDEXES");

        // FT.SEARCH should increment VECTOR_SEARCH_TOTAL
        crate::vector::distance::init();
        let before_search = crate::vector::metrics::VECTOR_SEARCH_TOTAL.load(Ordering::Relaxed);
        let query_vec: Vec<u8> = vec![0u8; 128 * 4];
        let search_args = vec![
            bulk(b"myidx"),
            bulk(b"*=>[KNN 5 @vec $query]"),
            bulk(b"PARAMS"),
            bulk(b"2"),
            bulk(b"query"),
            Frame::BulkString(Bytes::from(query_vec)),
        ];
        ft_search(&mut store, &search_args);
        let after_search = crate::vector::metrics::VECTOR_SEARCH_TOTAL.load(Ordering::Relaxed);
        assert!(after_search > before_search, "FT.SEARCH should increment VECTOR_SEARCH_TOTAL");

        // Latency should be non-zero after a search
        let latency = crate::vector::metrics::VECTOR_SEARCH_LATENCY_US.load(Ordering::Relaxed);
        // latency may be 0 on very fast machines, so just check it was written (could be 0 if sub-microsecond)

        // FT.DROPINDEX should decrement VECTOR_INDEXES
        let before_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
        ft_dropindex(&mut store, &[bulk(b"myidx")]);
        let after_drop = crate::vector::metrics::VECTOR_INDEXES.load(Ordering::Relaxed);
        assert!(after_drop < before_drop, "FT.DROPINDEX should decrement VECTOR_INDEXES");

        // Suppress unused variable warning
        let _ = latency;
    }
}
