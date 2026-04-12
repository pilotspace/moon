//! FT.* vector search command handlers.
//!
//! These commands operate on VectorStore, not Database, so they are NOT
//! dispatched through the standard command::dispatch() function.
//! Instead, the shard event loop intercepts FT.* commands and calls
//! these handlers directly with the per-shard VectorStore.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use smallvec::SmallVec;

use crate::protocol::{Frame, FrameVec};
use crate::vector::filter::FilterExpr;
use crate::vector::store::{IndexMeta, VectorStore};
use crate::vector::turbo_quant::collection::QuantizationConfig;
use crate::vector::types::{DistanceMetric, SearchResult};

/// FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC L2
///
/// Parses the FT.CREATE syntax and creates a vector index in the store.
/// args[0] = index_name, args[1..] = ON HASH PREFIX ... SCHEMA ...
pub fn ft_create(store: &mut VectorStore, args: &[Frame]) -> Frame {
    if args.len() < 10 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.CREATE' command",
        ));
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

    // Parse key-value pairs: TYPE, DIM, DISTANCE_METRIC, M, EF_CONSTRUCTION, EF_RUNTIME,
    // COMPACT_THRESHOLD, QUANTIZATION
    let mut dimension: Option<u32> = None;
    let mut metric = DistanceMetric::L2;
    let mut hnsw_m: u32 = 16;
    let mut hnsw_ef_construction: u32 = 200;
    let mut hnsw_ef_runtime: u32 = 0; // 0 = auto
    let mut compact_threshold: u32 = 0; // 0 = default (1000)
    let mut quantization = QuantizationConfig::TurboQuant4;
    let mut build_mode = crate::vector::turbo_quant::collection::BuildMode::Light;

    let param_end = pos + num_params;
    while pos + 1 < param_end && pos + 1 < args.len() {
        let key = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => {
                pos += 2;
                continue;
            }
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
                None => {
                    return Frame::Error(Bytes::from_static(b"ERR invalid EF_CONSTRUCTION value"));
                }
            };
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"EF_RUNTIME") {
            hnsw_ef_runtime = match parse_u32(&args[pos]) {
                Some(n) if n >= 10 && n <= 4096 => n,
                Some(_) => {
                    return Frame::Error(Bytes::from_static(b"ERR EF_RUNTIME must be 10-4096"));
                }
                None => return Frame::Error(Bytes::from_static(b"ERR invalid EF_RUNTIME value")),
            };
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"COMPACT_THRESHOLD") {
            compact_threshold = match parse_u32(&args[pos]) {
                Some(n) if n >= 100 && n <= 100000 => n,
                Some(_) => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR COMPACT_THRESHOLD must be 100-100000",
                    ));
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR invalid COMPACT_THRESHOLD value",
                    ));
                }
            };
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"BUILD_MODE") {
            let val = match extract_bulk(&args[pos]) {
                Some(v) => v,
                None => return Frame::Error(Bytes::from_static(b"ERR invalid BUILD_MODE value")),
            };
            build_mode = if val.eq_ignore_ascii_case(b"LIGHT") {
                crate::vector::turbo_quant::collection::BuildMode::Light
            } else if val.eq_ignore_ascii_case(b"EXACT") {
                crate::vector::turbo_quant::collection::BuildMode::Exact
            } else {
                return Frame::Error(Bytes::from_static(b"ERR BUILD_MODE must be LIGHT or EXACT"));
            };
            pos += 1;
        } else if key.eq_ignore_ascii_case(b"QUANTIZATION") {
            let val = match extract_bulk(&args[pos]) {
                Some(v) => v,
                None => return Frame::Error(Bytes::from_static(b"ERR invalid QUANTIZATION value")),
            };
            quantization = if val.eq_ignore_ascii_case(b"TQ1") {
                QuantizationConfig::TurboQuant1
            } else if val.eq_ignore_ascii_case(b"TQ2") {
                QuantizationConfig::TurboQuant2
            } else if val.eq_ignore_ascii_case(b"TQ3") {
                QuantizationConfig::TurboQuant3
            } else if val.eq_ignore_ascii_case(b"TQ4") {
                QuantizationConfig::TurboQuant4
            } else if val.eq_ignore_ascii_case(b"TQ4A2") {
                QuantizationConfig::TurboQuant4A2
            } else if val.eq_ignore_ascii_case(b"SQ8") {
                QuantizationConfig::Sq8
            } else {
                return Frame::Error(Bytes::from_static(
                    b"ERR unsupported QUANTIZATION (use TQ1, TQ2, TQ3, TQ4, TQ4A2, or SQ8)",
                ));
            };
            pos += 1;
        } else {
            pos += 1; // skip unknown param value
        }
    }

    let dim = match dimension {
        Some(d) if d > 0 && d <= 65536 => d,
        Some(d) if d > 65536 => {
            return Frame::Error(Bytes::from_static(b"ERR DIM must be between 1 and 65536"));
        }
        _ => return Frame::Error(Bytes::from_static(b"ERR DIM is required and must be > 0")),
    };

    let meta = IndexMeta {
        name: index_name,
        dimension: dim,
        padded_dimension: crate::vector::turbo_quant::encoder::padded_dimension(dim),
        metric,
        hnsw_m,
        hnsw_ef_construction,
        hnsw_ef_runtime,
        compact_threshold,
        source_field,
        key_prefixes: prefixes,
        quantization,
        build_mode,
    };

    match store.create_index(meta) {
        Ok(()) => {
            crate::vector::metrics::increment_indexes();
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        Err(msg) => {
            let mut buf = Vec::with_capacity(4 + msg.len());
            buf.extend_from_slice(b"ERR ");
            buf.extend_from_slice(msg.as_bytes());
            Frame::Error(Bytes::from(buf))
        }
    }
}

/// FT.DROPINDEX index_name
pub fn ft_dropindex(store: &mut VectorStore, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.DROPINDEX' command",
        ));
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

/// FT.COMPACT index_name
///
/// Explicitly compacts the mutable segment into an immutable HNSW segment.
/// This converts brute-force O(n) search to HNSW O(log n) search.
/// Call after bulk insert, before search workload begins.
pub fn ft_compact(store: &mut VectorStore, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.COMPACT' command",
        ));
    }
    let name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };
    let idx = match store.get_index_mut(&name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };
    // FT.COMPACT is explicit user intent: compact unconditionally, ignoring threshold.
    // Without this, when compact_threshold >= mutable_len, FT.COMPACT silently no-ops,
    // leaving all vectors in brute-force mutable segment (O(n) search instead of HNSW O(log n)).
    idx.force_compact();
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// FT._LIST
///
/// Returns an array of all index names. Compatible with the Redis
/// `FT._LIST` internal command used by tools and the Moon Console.
pub fn ft_list(store: &VectorStore) -> Frame {
    let names = store.index_names();
    let elements: Vec<Frame> = names
        .into_iter()
        .map(|n| Frame::BulkString(n.clone()))
        .collect();
    Frame::Array(FrameVec::from_vec(elements))
}

/// FT.INFO index_name
///
/// Returns an array of key-value pairs describing the index.
pub fn ft_info(store: &VectorStore, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.INFO' command",
        ));
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
    // Sum live counts across mutable + immutable segments.
    // Previously this only counted the mutable segment, showing num_docs=0 after FT.COMPACT.
    let mut num_docs = snap.mutable.len();
    for imm in snap.immutable.iter() {
        num_docs += imm.live_count() as usize;
    }

    // Use itoa for numeric formatting — no format!() on hot path.
    let ef_rt_bytes: Bytes = if idx.meta.hnsw_ef_runtime > 0 {
        let mut buf = itoa::Buffer::new();
        Bytes::copy_from_slice(buf.format(idx.meta.hnsw_ef_runtime).as_bytes())
    } else {
        Bytes::from_static(b"auto")
    };
    let ct_bytes: Bytes = if idx.meta.compact_threshold > 0 {
        let mut buf = itoa::Buffer::new();
        Bytes::copy_from_slice(buf.format(idx.meta.compact_threshold).as_bytes())
    } else {
        Bytes::from_static(b"1000")
    };
    let quant_bytes: Bytes = match idx.meta.quantization {
        QuantizationConfig::Sq8 => Bytes::from_static(b"SQ8"),
        QuantizationConfig::TurboQuant4 => Bytes::from_static(b"TurboQuant4"),
        QuantizationConfig::TurboQuantProd4 => Bytes::from_static(b"TurboQuantProd4"),
        QuantizationConfig::TurboQuant1 => Bytes::from_static(b"TurboQuant1"),
        QuantizationConfig::TurboQuant2 => Bytes::from_static(b"TurboQuant2"),
        QuantizationConfig::TurboQuant3 => Bytes::from_static(b"TurboQuant3"),
        QuantizationConfig::TurboQuant4A2 => Bytes::from_static(b"TurboQuant4A2"),
    };

    let items = vec![
        Frame::BulkString(Bytes::from_static(b"index_name")),
        Frame::BulkString(idx.meta.name.clone()),
        Frame::BulkString(Bytes::from_static(b"index_definition")),
        Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"key_type")),
                Frame::BulkString(Bytes::from_static(b"HASH")),
            ]
            .into(),
        ),
        Frame::BulkString(Bytes::from_static(b"num_docs")),
        Frame::Integer(num_docs as i64),
        Frame::BulkString(Bytes::from_static(b"dimension")),
        Frame::Integer(idx.meta.dimension as i64),
        Frame::BulkString(Bytes::from_static(b"distance_metric")),
        Frame::BulkString(metric_to_bytes(idx.meta.metric)),
        Frame::BulkString(Bytes::from_static(b"M")),
        Frame::Integer(idx.meta.hnsw_m as i64),
        Frame::BulkString(Bytes::from_static(b"EF_CONSTRUCTION")),
        Frame::Integer(idx.meta.hnsw_ef_construction as i64),
        Frame::BulkString(Bytes::from_static(b"EF_RUNTIME")),
        Frame::BulkString(ef_rt_bytes),
        Frame::BulkString(Bytes::from_static(b"COMPACT_THRESHOLD")),
        Frame::BulkString(ct_bytes),
        Frame::BulkString(Bytes::from_static(b"QUANTIZATION")),
        Frame::BulkString(quant_bytes),
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
            ));
        }
    };

    // Parse optional FILTER clause
    let filter_expr = parse_filter_clause(args);
    let result = search_local_filtered(store, &index_name, &query_blob, k, filter_expr.as_ref());
    crate::vector::metrics::increment_search();
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
    // Primary path: binary little-endian f32 blob (RediSearch-compatible).
    // Fallback: comma-separated floats in a UTF-8 string. This supports the
    // Moon Console REST/WS bridge which transmits args as JSON strings and
    // cannot carry raw binary blobs.
    let query_f32 = if query_blob.len() == dim * 4 {
        let mut v = Vec::with_capacity(dim);
        for chunk in query_blob.chunks_exact(4) {
            v.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        v
    } else if let Ok(text) = std::str::from_utf8(query_blob) {
        let parsed: Vec<f32> = text
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| s.trim().parse::<f32>().ok())
            .collect();
        if parsed.len() != dim {
            return Frame::Error(Bytes::from_static(b"ERR query vector dimension mismatch"));
        }
        parsed
    } else {
        return Frame::Error(Bytes::from_static(b"ERR query vector dimension mismatch"));
    };

    // Auto-compact mutable → HNSW if threshold reached (lazy, first search only).
    idx.try_compact();

    // ef_search: user-configurable via EF_RUNTIME in FT.CREATE, or auto-computed.
    // Higher ef = better recall but lower QPS. Auto scales with k and dimension:
    // base = k*20, min 200, boosted for high-d where TQ-ADC needs wider beam.
    let ef_search = if idx.meta.hnsw_ef_runtime > 0 {
        idx.meta.hnsw_ef_runtime as usize
    } else {
        let base = (k * 20).max(200);
        // Dimension boost: +50% at 384d+, +100% at 768d+
        let dim_factor = if idx.meta.dimension >= 768 {
            2
        } else if idx.meta.dimension >= 384 {
            3
        } else {
            2
        };
        (base * dim_factor / 2).clamp(200, 1000)
    };

    let filter_bitmap = filter.map(|f| {
        let total = idx.segments.total_vectors();
        idx.payload_index.evaluate_bitmap(f, total)
    });

    let empty_committed = roaring::RoaringBitmap::new();
    let mvcc_ctx = crate::vector::segment::holder::MvccContext {
        snapshot_lsn: 0,
        my_txn_id: 0,
        committed: &empty_committed,
        dirty_set: &[],
        dimension: idx.meta.dimension,
    };

    let results = idx.segments.search_mvcc(
        &query_f32,
        k,
        ef_search,
        &mut idx.scratch,
        filter_bitmap.as_ref(),
        &mvcc_ctx,
    );
    build_search_response(&results, &idx.key_hash_to_key)
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
/// Format: [num_results, "doc:0", ["__vec_score", "0.5"], "doc:1", ["__vec_score", "0.8"], ...]
///
/// Looks up the original Redis key via `key_hash_to_key` map (populated at insert time
/// in `auto_index_hset`). Falls back to `vec:<internal_id>` only if the mapping is missing
/// (e.g., legacy data restored from a snapshot without the key map).
fn build_search_response(
    results: &SmallVec<[SearchResult; 32]>,
    key_hash_to_key: &std::collections::HashMap<u64, Bytes>,
) -> Frame {
    let total = results.len() as i64;
    // NOTE: Vec/format! usage here is acceptable -- this is response building at end
    // of command path, not hot-path dispatch.
    let mut items = Vec::with_capacity(1 + results.len() * 2);
    items.push(Frame::Integer(total));

    for r in results {
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
pub fn parse_ft_search_args(
    args: &[Frame],
) -> Result<(Bytes, Bytes, usize, Option<FilterExpr>), Frame> {
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
mod tests;
