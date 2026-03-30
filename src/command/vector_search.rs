//! FT.* vector search command handlers.
//!
//! These commands operate on VectorStore, not Database, so they are NOT
//! dispatched through the standard command::dispatch() function.
//! Instead, the shard event loop intercepts FT.* commands and calls
//! these handlers directly with the per-shard VectorStore.

use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::Frame;
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
        Ok(()) => Frame::SimpleString(Bytes::from_static(b"OK")),
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

    search_local(store, &index_name, &query_blob, k)
}

/// Direct local search for cross-shard VectorSearch messages.
/// Skips FT.SEARCH parsing -- the coordinator already extracted index_name, blob, k.
pub fn search_local(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
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
    let results = idx
        .segments
        .search(&query_f32, &query_sq, k, ef_search, &mut idx.scratch);
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
}
