//! FT.* vector search command handlers.
//!
//! These commands operate on VectorStore, not Database, so they are NOT
//! dispatched through the standard command::dispatch() function.
//! Instead, the shard event loop intercepts FT.* commands and calls
//! these handlers directly with the per-shard VectorStore.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::vector::store::{IndexMeta, VectorStore};
use crate::vector::types::DistanceMetric;

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
