//! FT.* vector search command handlers.
//!
//! These commands operate on VectorStore, not Database, so they are NOT
//! dispatched through the standard command::dispatch() function.
//! Instead, the shard event loop intercepts FT.* commands and calls
//! these handlers directly with the per-shard VectorStore.

pub mod cache_search;
#[cfg(feature = "graph")]
pub mod graph_expand;
pub mod session;

use bytes::Bytes;
use ordered_float::OrderedFloat;
use smallvec::SmallVec;

use crate::protocol::{Frame, FrameVec};
use crate::vector::filter::FilterExpr;
use crate::vector::store::{IndexMeta, VectorFieldMeta, VectorStore, MAX_VECTOR_FIELDS};
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

    // Parse SCHEMA — supports multiple VECTOR field definitions:
    //   field_name VECTOR HNSW num_params [key value ...]
    //   field_name2 VECTOR HNSW num_params [key value ...]
    if pos >= args.len() || !matches_keyword(&args[pos], b"SCHEMA") {
        return Frame::Error(Bytes::from_static(b"ERR expected SCHEMA"));
    }
    pos += 1;

    let mut vector_fields: Vec<VectorFieldMeta> = Vec::new();
    let mut sparse_field_defs: Vec<(Bytes, u32)> = Vec::new();
    // Index-level HNSW params from the first field (backward compat)
    let mut first_hnsw_m: u32 = 16;
    let mut first_hnsw_ef_construction: u32 = 200;
    let mut first_hnsw_ef_runtime: u32 = 0;
    let mut first_compact_threshold: u32 = 0;

    // Loop: parse one or more field definitions until args exhausted
    while pos < args.len() {
        let field_name = match extract_bulk(&args[pos]) {
            Some(b) => b,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid field name")),
        };
        pos += 1;

        // Check for SPARSE field type
        if pos < args.len() && matches_keyword(&args[pos], b"SPARSE") {
            pos += 1;
            // Parse optional DIM parameter for sparse field
            let mut sparse_dim: u32 = 30000; // default SPLADE vocab size
            if pos + 1 < args.len() && matches_keyword(&args[pos], b"DIM") {
                pos += 1;
                sparse_dim = match parse_u32(&args[pos]) {
                    Some(d) if d > 0 => d,
                    _ => {
                        return Frame::Error(Bytes::from_static(
                            b"ERR invalid SPARSE DIM value",
                        ));
                    }
                };
                pos += 1;
            }
            // Store sparse field info for post-create wiring
            // We'll wire it up after the index is created
            sparse_field_defs.push((field_name, sparse_dim));
            continue;
        }

        if pos >= args.len() || !matches_keyword(&args[pos], b"VECTOR") {
            return Frame::Error(Bytes::from_static(b"ERR expected VECTOR or SPARSE after field name"));
        }
        pos += 1;

        match parse_vector_field_params(args, &mut pos) {
            Ok(parsed) => {
                // Validate DIM
                let dim = match parsed.dimension {
                    Some(d) if d > 0 && d <= 65536 => d,
                    Some(_) => {
                        return Frame::Error(Bytes::from_static(
                            b"ERR DIM must be between 1 and 65536",
                        ));
                    }
                    None => {
                        return Frame::Error(Bytes::from_static(
                            b"ERR DIM is required and must be > 0",
                        ));
                    }
                };

                // Check for duplicate field names (case-insensitive)
                if vector_fields
                    .iter()
                    .any(|f| f.field_name.eq_ignore_ascii_case(&field_name))
                {
                    return Frame::Error(Bytes::from_static(
                        b"ERR duplicate VECTOR field name in SCHEMA",
                    ));
                }

                // Capture index-level HNSW params from the first field
                if vector_fields.is_empty() {
                    first_hnsw_m = parsed.hnsw_m;
                    first_hnsw_ef_construction = parsed.hnsw_ef_construction;
                    first_hnsw_ef_runtime = parsed.hnsw_ef_runtime;
                    first_compact_threshold = parsed.compact_threshold;
                }

                vector_fields.push(VectorFieldMeta {
                    field_name,
                    dimension: dim,
                    padded_dimension:
                        crate::vector::turbo_quant::encoder::padded_dimension(dim),
                    metric: parsed.metric,
                    quantization: parsed.quantization,
                    build_mode: parsed.build_mode,
                });

                if vector_fields.len() > MAX_VECTOR_FIELDS {
                    return Frame::Error(Bytes::from_static(
                        b"ERR too many VECTOR fields (max 8)",
                    ));
                }
            }
            Err(frame) => return frame,
        }
    }

    if vector_fields.is_empty() && sparse_field_defs.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR at least one VECTOR or SPARSE field is required in SCHEMA",
        ));
    }
    // If only SPARSE fields, we still need a dummy VECTOR field for the index structure
    if vector_fields.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR at least one VECTOR field is required in SCHEMA (SPARSE fields are supplementary)",
        ));
    }

    // Build IndexMeta from the first (default) field for backward compatibility
    let default_field = &vector_fields[0];
    let meta = IndexMeta {
        name: index_name,
        dimension: default_field.dimension,
        padded_dimension: default_field.padded_dimension,
        metric: default_field.metric,
        hnsw_m: first_hnsw_m,
        hnsw_ef_construction: first_hnsw_ef_construction,
        hnsw_ef_runtime: first_hnsw_ef_runtime,
        compact_threshold: first_compact_threshold,
        source_field: default_field.field_name.clone(),
        key_prefixes: prefixes,
        quantization: default_field.quantization,
        build_mode: default_field.build_mode,
        vector_fields,
    };

    let index_name_clone = meta.name.clone();
    match store.create_index(meta) {
        Ok(()) => {
            // Wire up sparse field stores after index creation
            if !sparse_field_defs.is_empty() {
                if let Some(idx) = store.get_index_mut(index_name_clone.as_ref()) {
                    for (field_name, max_dim) in sparse_field_defs {
                        idx.sparse_stores.insert(
                            field_name,
                            crate::vector::sparse::store::SparseStore::new(max_dim),
                        );
                    }
                }
            }
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

/// Parsed VECTOR field parameters from HNSW num_params [key value ...].
struct ParsedVectorField {
    dimension: Option<u32>,
    metric: DistanceMetric,
    hnsw_m: u32,
    hnsw_ef_construction: u32,
    hnsw_ef_runtime: u32,
    compact_threshold: u32,
    quantization: QuantizationConfig,
    build_mode: crate::vector::turbo_quant::collection::BuildMode,
}

/// Parse VECTOR field params: HNSW num_params [TYPE FLOAT32] [DIM n] [DISTANCE_METRIC ...]
/// Advances `pos` past all consumed args. Returns parsed params or error Frame.
fn parse_vector_field_params(args: &[Frame], pos: &mut usize) -> Result<ParsedVectorField, Frame> {
    if *pos >= args.len() || !matches_keyword(&args[*pos], b"HNSW") {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR expected HNSW algorithm",
        )));
    }
    *pos += 1;

    let num_params = match parse_u32(&args[*pos]) {
        Some(n) => n as usize,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR invalid param count",
            )));
        }
    };
    *pos += 1;

    let mut dimension: Option<u32> = None;
    let mut metric = DistanceMetric::L2;
    let mut hnsw_m: u32 = 16;
    let mut hnsw_ef_construction: u32 = 200;
    let mut hnsw_ef_runtime: u32 = 0;
    let mut compact_threshold: u32 = 0;
    let mut quantization = QuantizationConfig::TurboQuant4;
    let mut build_mode = crate::vector::turbo_quant::collection::BuildMode::Light;

    let param_end = *pos + num_params;
    while *pos + 1 < param_end && *pos + 1 < args.len() {
        let key = match extract_bulk(&args[*pos]) {
            Some(b) => b,
            None => {
                *pos += 2;
                continue;
            }
        };
        *pos += 1;

        if key.eq_ignore_ascii_case(b"TYPE") {
            if !matches_keyword(&args[*pos], b"FLOAT32") {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR only FLOAT32 type supported",
                )));
            }
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"DIM") {
            dimension = parse_u32(&args[*pos]);
            if dimension.is_none() {
                return Err(Frame::Error(Bytes::from_static(b"ERR invalid DIM value")));
            }
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"DISTANCE_METRIC") {
            let val = match extract_bulk(&args[*pos]) {
                Some(v) => v,
                None => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid DISTANCE_METRIC",
                    )));
                }
            };
            metric = if val.eq_ignore_ascii_case(b"L2") {
                DistanceMetric::L2
            } else if val.eq_ignore_ascii_case(b"COSINE") {
                DistanceMetric::Cosine
            } else if val.eq_ignore_ascii_case(b"IP") {
                DistanceMetric::InnerProduct
            } else {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR unsupported DISTANCE_METRIC",
                )));
            };
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"M") {
            hnsw_m = match parse_u32(&args[*pos]) {
                Some(n) => n,
                None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid M value"))),
            };
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"EF_CONSTRUCTION") {
            hnsw_ef_construction = match parse_u32(&args[*pos]) {
                Some(n) => n,
                None => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid EF_CONSTRUCTION value",
                    )));
                }
            };
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"EF_RUNTIME") {
            hnsw_ef_runtime = match parse_u32(&args[*pos]) {
                Some(n) if n >= 10 && n <= 4096 => n,
                Some(_) => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR EF_RUNTIME must be 10-4096",
                    )));
                }
                None => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid EF_RUNTIME value",
                    )));
                }
            };
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"COMPACT_THRESHOLD") {
            compact_threshold = match parse_u32(&args[*pos]) {
                Some(n) if n >= 100 && n <= 100000 => n,
                Some(_) => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR COMPACT_THRESHOLD must be 100-100000",
                    )));
                }
                None => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid COMPACT_THRESHOLD value",
                    )));
                }
            };
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"BUILD_MODE") {
            let val = match extract_bulk(&args[*pos]) {
                Some(v) => v,
                None => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid BUILD_MODE value",
                    )));
                }
            };
            build_mode = if val.eq_ignore_ascii_case(b"LIGHT") {
                crate::vector::turbo_quant::collection::BuildMode::Light
            } else if val.eq_ignore_ascii_case(b"EXACT") {
                crate::vector::turbo_quant::collection::BuildMode::Exact
            } else {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR BUILD_MODE must be LIGHT or EXACT",
                )));
            };
            *pos += 1;
        } else if key.eq_ignore_ascii_case(b"QUANTIZATION") {
            let val = match extract_bulk(&args[*pos]) {
                Some(v) => v,
                None => {
                    return Err(Frame::Error(Bytes::from_static(
                        b"ERR invalid QUANTIZATION value",
                    )));
                }
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
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR unsupported QUANTIZATION (use TQ1, TQ2, TQ3, TQ4, TQ4A2, or SQ8)",
                )));
            };
            *pos += 1;
        } else {
            *pos += 1; // skip unknown param value
        }
    }

    Ok(ParsedVectorField {
        dimension,
        metric,
        hnsw_m,
        hnsw_ef_construction,
        hnsw_ef_runtime,
        compact_threshold,
        quantization,
        build_mode,
    })
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
    // force_compact now compacts ALL fields (default + additional).
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
/// Includes backward-compatible top-level fields (from default field) plus
/// a `vector_fields` nested array with per-field stats.
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

    // Count default field docs across mutable + immutable segments.
    let snap = idx.segments.load();
    let mut num_docs = snap.mutable.len();
    for imm in snap.immutable.iter() {
        num_docs += imm.live_count() as usize;
    }

    // Use itoa for numeric formatting -- no format!() on hot path.
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
    let quant_bytes: Bytes = quantization_to_bytes(idx.meta.quantization);

    // Backward-compatible top-level fields (from default field)
    let mut items = vec![
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

    // Per-field stats: vector_fields array
    let mut field_entries: Vec<Frame> = Vec::with_capacity(idx.meta.vector_fields.len());
    for (i, field_meta) in idx.meta.vector_fields.iter().enumerate() {
        let (field_num_docs, field_mutable, field_immutable_count) = if i == 0 {
            // Default field: use top-level segments
            let s = idx.segments.load();
            let mut docs = s.mutable.len();
            let imm_count = s.immutable.len();
            for imm in s.immutable.iter() {
                docs += imm.live_count() as usize;
            }
            (docs, s.mutable.len(), imm_count)
        } else if let Some(fs) = idx.field_segments.get(&field_meta.field_name) {
            let s = fs.segments.load();
            let mut docs = s.mutable.len();
            let imm_count = s.immutable.len();
            for imm in s.immutable.iter() {
                docs += imm.live_count() as usize;
            }
            (docs, s.mutable.len(), imm_count)
        } else {
            (0, 0, 0)
        };

        let field_quant = quantization_to_bytes(field_meta.quantization);

        let entry = vec![
            Frame::BulkString(Bytes::from_static(b"field_name")),
            Frame::BulkString(field_meta.field_name.clone()),
            Frame::BulkString(Bytes::from_static(b"dimension")),
            Frame::Integer(field_meta.dimension as i64),
            Frame::BulkString(Bytes::from_static(b"distance_metric")),
            Frame::BulkString(metric_to_bytes(field_meta.metric)),
            Frame::BulkString(Bytes::from_static(b"num_docs")),
            Frame::Integer(field_num_docs as i64),
            Frame::BulkString(Bytes::from_static(b"QUANTIZATION")),
            Frame::BulkString(field_quant),
            Frame::BulkString(Bytes::from_static(b"mutable_vectors")),
            Frame::Integer(field_mutable as i64),
            Frame::BulkString(Bytes::from_static(b"immutable_segments")),
            Frame::Integer(field_immutable_count as i64),
        ];
        field_entries.push(Frame::Array(entry.into()));
    }

    items.push(Frame::BulkString(Bytes::from_static(b"vector_fields")));
    items.push(Frame::Array(field_entries.into()));

    Frame::Array(items.into())
}

/// Convert QuantizationConfig to display bytes.
fn quantization_to_bytes(q: QuantizationConfig) -> Bytes {
    match q {
        QuantizationConfig::Sq8 => Bytes::from_static(b"SQ8"),
        QuantizationConfig::TurboQuant4 => Bytes::from_static(b"TurboQuant4"),
        QuantizationConfig::TurboQuantProd4 => Bytes::from_static(b"TurboQuantProd4"),
        QuantizationConfig::TurboQuant1 => Bytes::from_static(b"TurboQuant1"),
        QuantizationConfig::TurboQuant2 => Bytes::from_static(b"TurboQuant2"),
        QuantizationConfig::TurboQuant3 => Bytes::from_static(b"TurboQuant3"),
        QuantizationConfig::TurboQuant4A2 => Bytes::from_static(b"TurboQuant4A2"),
    }
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
///
/// `db` is an optional mutable Database reference for SESSION clause support.
/// When `None`, SESSION clause is silently ignored (backward compatible).
pub fn ft_search(
    store: &mut VectorStore,
    args: &[Frame],
    db: Option<&mut crate::storage::db::Database>,
) -> Frame {
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

    // Parse KNN from query string (optional — may be absent for sparse-only search)
    let knn_parsed = parse_knn_query(&query_str);

    // Parse optional SPARSE clause: SPARSE @field_name $param_name
    let sparse_clause = parse_sparse_clause(args);

    // Must have at least one retriever
    if knn_parsed.is_none() && sparse_clause.is_none() {
        return Frame::Error(Bytes::from_static(b"ERR invalid KNN query syntax"));
    }

    // Parse optional FILTER clause
    let filter_expr = parse_filter_clause(args);

    // Parse optional LIMIT offset count
    let (limit_offset, limit_count) = parse_limit_clause(args);

    // Parse optional SESSION clause
    let session_key = parse_session_clause(args);

    // Determine k from KNN or default to 10 for sparse-only
    let (k, field_name, dense_param) = match &knn_parsed {
        Some((k, fname, pname)) => (*k, fname.clone(), Some(pname.clone())),
        None => (10, None, None),
    };

    // Extract dense query blob if KNN is present
    let dense_blob = if let Some(ref pname) = dense_param {
        match extract_param_blob(args, pname) {
            Some(blob) => Some(blob),
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR query vector parameter not found in PARAMS",
                ));
            }
        }
    } else {
        None
    };

    // Extract sparse query pairs if SPARSE clause is present
    let sparse_query = if let Some((ref sparse_field, ref sparse_param)) = sparse_clause {
        match extract_param_blob(args, sparse_param) {
            Some(blob) => {
                let pairs = parse_sparse_query_blob(&blob);
                if pairs.is_empty() {
                    return Frame::Error(Bytes::from_static(
                        b"ERR invalid sparse query blob (must be pairs of u32+f32 LE)",
                    ));
                }
                Some((sparse_field.clone(), pairs))
            }
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR sparse query parameter not found in PARAMS",
                ));
            }
        }
    } else {
        None
    };

    // --- Hybrid path: both dense + sparse ---
    if dense_blob.is_some() && sparse_query.is_some() {
        let blob = dense_blob.as_ref().unwrap();
        let (sparse_field, sparse_pairs) = sparse_query.as_ref().unwrap();

        // Dense search
        let dense_raw = search_local_raw(
            store,
            &index_name,
            blob,
            k,
            filter_expr.as_ref(),
            field_name.as_ref(),
        );
        let (dense_results, key_hash_to_key) = match dense_raw {
            SearchRawResult::Ok { results, key_hash_to_key } => (results, key_hash_to_key),
            SearchRawResult::Error(frame) => return frame,
        };

        // Sparse search
        let idx = match store.get_index_mut(index_name.as_ref()) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };
        let sparse_results = match idx.sparse_stores.get(sparse_field.as_ref()) {
            Some(ss) => ss.search(sparse_pairs, k),
            None => Vec::new(),
        };

        // RRF fusion
        let (fused, dense_count, sparse_count) =
            crate::vector::fusion::rrf_fuse(&dense_results, &sparse_results, k);

        crate::vector::metrics::increment_search();
        return build_hybrid_response(
            &fused,
            &key_hash_to_key,
            dense_count,
            sparse_count,
            limit_offset,
            limit_count,
        );
    }

    // --- Sparse-only path ---
    if let Some((sparse_field, sparse_pairs)) = sparse_query {
        let idx = match store.get_index_mut(index_name.as_ref()) {
            Some(i) => i,
            None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
        };
        let sparse_results = match idx.sparse_stores.get(sparse_field.as_ref()) {
            Some(ss) => ss.search(&sparse_pairs, k),
            None => Vec::new(),
        };
        let key_hash_to_key = idx.key_hash_to_key.clone();
        let sparse_count = sparse_results.len();

        // Convert to SmallVec for build_hybrid_response
        let fused: Vec<SearchResult> = sparse_results;
        crate::vector::metrics::increment_search();
        return build_hybrid_response(
            &fused,
            &key_hash_to_key,
            0,
            sparse_count,
            limit_offset,
            limit_count,
        );
    }

    // --- Dense-only path (original behavior, backward compat) ---
    let blob = dense_blob.unwrap();

    // If SESSION clause present and db available, use session-aware path
    if let (Some(ref sess_key), Some(db)) = (session_key, db) {
        let result = search_local_raw(
            store,
            &index_name,
            &blob,
            k,
            filter_expr.as_ref(),
            field_name.as_ref(),
        );
        crate::vector::metrics::increment_search();
        return match result {
            SearchRawResult::Error(frame) => frame,
            SearchRawResult::Ok { mut results, key_hash_to_key } => {
                // Read session sorted set for filtering
                let session_members_snapshot: std::collections::HashMap<Bytes, f64> =
                    match db.get_sorted_set(sess_key) {
                        Ok(Some((members, _tree))) => members.clone(),
                        Ok(None) => std::collections::HashMap::new(),
                        Err(_) => std::collections::HashMap::new(),
                    };

                // Filter out previously returned results
                results = session::filter_session_results(
                    &results,
                    &session_members_snapshot,
                    &key_hash_to_key,
                );

                // Build response from filtered results
                let response = build_search_response(&results, &key_hash_to_key, limit_offset, limit_count);

                // Record new results in the session sorted set
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0);
                session::record_session_results(&results, db, sess_key, &key_hash_to_key, timestamp);

                response
            }
        };
    }

    // Standard dense-only path (no session)
    let result = search_local_filtered(
        store,
        &index_name,
        &blob,
        k,
        filter_expr.as_ref(),
        limit_offset,
        limit_count,
        field_name.as_ref(),
    );
    crate::vector::metrics::increment_search();
    result
}

/// FT.SEARCH with optional EXPAND GRAPH support.
///
/// Takes both VectorStore and an optional GraphStore reference. If the query
/// contains `EXPAND GRAPH depth:N` (or `EXPAND GRAPH N`), runs the normal
/// KNN search first, then expands results through graph topology, merging
/// both sets into a combined response with `__vec_score` and `__graph_hops`.
///
/// If no EXPAND clause is present or no graph_store is provided, delegates
/// to the standard `ft_search` (backward compatible).
#[cfg(feature = "graph")]
pub fn ft_search_with_graph(
    store: &mut VectorStore,
    graph_store: Option<&crate::graph::store::GraphStore>,
    args: &[Frame],
    db: Option<&mut crate::storage::db::Database>,
) -> Frame {
    let expand_depth = parse_expand_clause(args);

    // No expansion requested or no graph store — fall back to standard search.
    if expand_depth.is_none() || graph_store.is_none() {
        return ft_search(store, args, db);
    }

    let depth = expand_depth.unwrap_or(1);
    let gs = graph_store.unwrap();

    // Run the standard KNN search first (session filtering happens inside ft_search).
    let knn_result = ft_search(store, args, db);

    // Extract (key, score) pairs from the KNN response for seeding graph expansion.
    let seed_keys = extract_seeds_from_response(&knn_result);
    if seed_keys.is_empty() {
        return knn_result; // no KNN hits to expand
    }

    // Find the first graph that contains any of the seed keys.
    let graph_names = gs.list_graphs();
    let mut target_graph: Option<&crate::graph::store::NamedGraph> = None;
    'outer: for gname in &graph_names {
        if let Some(g) = gs.get_graph(gname) {
            for (key, _) in &seed_keys {
                if g.lookup_node_by_key(key).is_some() {
                    target_graph = Some(g);
                    break 'outer;
                }
            }
        }
    }

    let Some(graph) = target_graph else {
        return knn_result; // no graph has these keys
    };

    // Expand via BFS through graph topology.
    let expanded = graph_expand::expand_results_via_graph(graph, &seed_keys, depth);

    // Build combined response: original KNN results + expanded graph results.
    build_combined_response(&knn_result, &expanded)
}

/// Extract (redis_key, vec_score) pairs from an FT.SEARCH response Frame.
///
/// Response format: [total, key1, [__vec_score, "0.5"], key2, [__vec_score, "0.8"], ...]
#[cfg(feature = "graph")]
fn extract_seeds_from_response(response: &Frame) -> Vec<(Bytes, f32)> {
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
fn build_combined_response(
    knn_response: &Frame,
    expanded: &[graph_expand::ExpandedResult],
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

/// Parse optional `EXPAND GRAPH depth:N` clause from FT.SEARCH args.
///
/// Returns `None` if no EXPAND clause, `Some(depth)` if present.
/// Accepts both `EXPAND GRAPH depth:N` and `EXPAND GRAPH N` syntax.
#[cfg(feature = "graph")]
fn parse_expand_clause(args: &[Frame]) -> Option<u32> {
    for i in 0..args.len() {
        if matches_keyword(&args[i], b"EXPAND") {
            if i + 1 < args.len() && matches_keyword(&args[i + 1], b"GRAPH") {
                if i + 2 < args.len() {
                    // Parse "depth:N" syntax
                    if let Some(depth_arg) = extract_bulk(&args[i + 2]) {
                        if let Some(stripped) = depth_arg.strip_prefix(b"depth:") {
                            if let Ok(s) = std::str::from_utf8(stripped) {
                                return s.parse::<u32>().ok();
                            }
                        }
                        // Also accept bare integer
                        return parse_u32(&args[i + 2]);
                    }
                }
            }
        }
    }
    None
}

/// Result of search_local_raw — either raw results or an error Frame.
enum SearchRawResult {
    Ok {
        results: SmallVec<[SearchResult; 32]>,
        key_hash_to_key: std::collections::HashMap<u64, Bytes>,
    },
    Error(Frame),
}

/// Search returning raw results (not yet built into a Frame response).
/// Used by session-aware ft_search to filter results before response building.
///
/// `field_name` selects which named vector field to search. `None` uses the default
/// (first) field. `Some(name)` dispatches to the named field's segments.
fn search_local_raw(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    filter: Option<&FilterExpr>,
    field_name: Option<&Bytes>,
) -> SearchRawResult {
    let idx = match store.get_index_mut(index_name) {
        Some(i) => i,
        None => return SearchRawResult::Error(Frame::Error(Bytes::from_static(b"Unknown Index name"))),
    };

    // Resolve target field: determine dimension, segments, scratch, collection
    let (dim, use_default_field) = if let Some(fname) = field_name {
        if let Some(field_meta) = idx.meta.find_field(fname) {
            let is_default = fname.eq_ignore_ascii_case(&idx.meta.default_field().field_name);
            (field_meta.dimension as usize, is_default)
        } else {
            return SearchRawResult::Error(Frame::Error(Bytes::from(
                format!("ERR unknown vector field '@{}'", String::from_utf8_lossy(fname))
            )));
        }
    } else {
        (idx.meta.dimension as usize, true)
    };

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
            return SearchRawResult::Error(Frame::Error(Bytes::from_static(b"ERR query vector dimension mismatch")));
        }
        parsed
    } else {
        return SearchRawResult::Error(Frame::Error(Bytes::from_static(b"ERR query vector dimension mismatch")));
    };

    idx.try_compact();

    let ef_search = if idx.meta.hnsw_ef_runtime > 0 {
        idx.meta.hnsw_ef_runtime as usize
    } else {
        let base = (k * 20).max(200);
        let dim_factor = if dim >= 768 {
            2
        } else if dim >= 384 {
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

    // Dispatch to correct field's segments
    if use_default_field {
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &empty_committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        let results = idx.segments.search_mvcc(
            &query_f32,
            k,
            ef_search,
            &mut idx.scratch,
            filter_bitmap.as_ref(),
            &mvcc_ctx,
        );
        let key_hash_to_key = idx.key_hash_to_key.clone();
        SearchRawResult::Ok { results, key_hash_to_key }
    } else {
        let fname = field_name.unwrap();
        if let Some(fs) = idx.field_segments.get_mut(fname.as_ref()) {
            let mvcc_ctx = crate::vector::segment::holder::MvccContext {
                snapshot_lsn: 0,
                my_txn_id: 0,
                committed: &empty_committed,
                dirty_set: &[],
                dimension: dim as u32,
            };
            let results = fs.segments.search_mvcc(
                &query_f32,
                k,
                ef_search,
                &mut fs.scratch,
                filter_bitmap.as_ref(),
                &mvcc_ctx,
            );
            let key_hash_to_key = idx.key_hash_to_key.clone();
            SearchRawResult::Ok { results, key_hash_to_key }
        } else {
            SearchRawResult::Error(Frame::Error(Bytes::from(
                format!("ERR unknown vector field '@{}'", String::from_utf8_lossy(fname))
            )))
        }
    }
}

/// Direct local search for cross-shard VectorSearch messages.
/// Skips FT.SEARCH parsing -- the coordinator already extracted index_name, blob, k.
///
/// Returns all results (no pagination) -- the coordinator applies LIMIT after merge.
/// Always searches the default field (cross-shard multi-field not in scope).
pub fn search_local(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
) -> Frame {
    search_local_filtered(store, index_name, query_blob, k, None, 0, usize::MAX, None)
}

/// Local search with optional filter expression and pagination.
///
/// Evaluates filter against PayloadIndex to produce bitmap, then dispatches
/// to search_filtered which selects optimal strategy (brute-force/HNSW/post-filter).
///
/// `offset` and `count` control pagination of the result set. The total match count
/// is always returned as the first element; only the paginated slice of documents
/// is included in the response.
///
/// `field_name` selects which named vector field to search. `None` uses the default field.
pub fn search_local_filtered(
    store: &mut VectorStore,
    index_name: &[u8],
    query_blob: &[u8],
    k: usize,
    filter: Option<&FilterExpr>,
    offset: usize,
    count: usize,
    field_name: Option<&Bytes>,
) -> Frame {
    let idx = match store.get_index_mut(index_name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };

    // Resolve target field dimension
    let (dim, use_default_field) = if let Some(fname) = field_name {
        if let Some(field_meta) = idx.meta.find_field(fname) {
            let is_default = fname.eq_ignore_ascii_case(&idx.meta.default_field().field_name);
            (field_meta.dimension as usize, is_default)
        } else {
            return Frame::Error(Bytes::from(
                format!("ERR unknown vector field '@{}'", String::from_utf8_lossy(fname))
            ));
        }
    } else {
        (idx.meta.dimension as usize, true)
    };

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
        let dim_factor = if dim >= 768 {
            2
        } else if dim >= 384 {
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

    // Dispatch to correct field's segments
    if use_default_field {
        let mvcc_ctx = crate::vector::segment::holder::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &empty_committed,
            dirty_set: &[],
            dimension: dim as u32,
        };
        let results = idx.segments.search_mvcc(
            &query_f32,
            k,
            ef_search,
            &mut idx.scratch,
            filter_bitmap.as_ref(),
            &mvcc_ctx,
        );
        build_search_response(&results, &idx.key_hash_to_key, offset, count)
    } else {
        let fname = field_name.unwrap();
        if let Some(fs) = idx.field_segments.get_mut(fname.as_ref()) {
            let mvcc_ctx = crate::vector::segment::holder::MvccContext {
                snapshot_lsn: 0,
                my_txn_id: 0,
                committed: &empty_committed,
                dirty_set: &[],
                dimension: dim as u32,
            };
            let results = fs.segments.search_mvcc(
                &query_f32,
                k,
                ef_search,
                &mut fs.scratch,
                filter_bitmap.as_ref(),
                &mvcc_ctx,
            );
            build_search_response(&results, &idx.key_hash_to_key, offset, count)
        } else {
            Frame::Error(Bytes::from(
                format!("ERR unknown vector field '@{}'", String::from_utf8_lossy(fname))
            ))
        }
    }
}

/// Parse "*=>[KNN <k> @<field> $<param>]" query string.
/// Returns (k, field_name, param_name) on success.
///
/// `field_name` is `Some(name)` if the query contains `@field_name` (without the `@` prefix),
/// or `None` if the token after k starts with `$` directly (backward compat for queries
/// where @field is omitted or unrecognized).
pub(crate) fn parse_knn_query(query: &[u8]) -> Option<(usize, Option<Bytes>, Bytes)> {
    let s = std::str::from_utf8(query).ok()?;
    let knn_start = s.find("KNN ")?;
    let after_knn = &s[knn_start + 4..];

    // Parse k (first number after KNN)
    let k_end = after_knn.find(' ')?;
    let k: usize = after_knn[..k_end].trim().parse().ok()?;

    // Parse @field_name (optional — may be $param directly)
    let after_k = after_knn[k_end + 1..].trim_start();
    let (field_name, param_start) = if after_k.starts_with('@') {
        let field_end = after_k.find(' ').unwrap_or(after_k.len());
        let name = &after_k[1..field_end]; // strip @
        let remaining = if field_end < after_k.len() {
            after_k[field_end + 1..].trim_start()
        } else {
            ""
        };
        (Some(Bytes::from(name.to_owned())), remaining)
    } else {
        (None, after_k)
    };

    // Parse $param_name
    let param_str = param_start.trim().trim_end_matches(']');
    if !param_str.starts_with('$') {
        return None;
    }
    let param_name = &param_str[1..];
    Some((k, field_name, Bytes::from(param_name.to_owned())))
}

/// Extract a named parameter blob from PARAMS section.
/// Format: ... PARAMS <count> <name1> <blob1> <name2> <blob2> ...
pub(crate) fn extract_param_blob(args: &[Frame], param_name: &[u8]) -> Option<Bytes> {
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

    let index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Err(Frame::Error(Bytes::from_static(b"ERR invalid index name"))),
    };

    let query_str = match extract_bulk(&args[1]) {
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

// -- LIMIT parsing --

/// Parse LIMIT clause from FT.SEARCH args.
/// Scans args (starting after index_name and query) for "LIMIT" keyword,
/// then reads the next two args as offset and count (both usize).
///
/// Default: (0, usize::MAX) — return all results (backward compatible).
/// LIMIT 0 0 is valid: returns total count only (no document entries).
pub(crate) fn parse_limit_clause(args: &[Frame]) -> (usize, usize) {
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"LIMIT") {
            i += 1;
            let offset = if i < args.len() {
                parse_usize(&args[i]).unwrap_or(0)
            } else {
                return (0, usize::MAX);
            };
            i += 1;
            let count = if i < args.len() {
                parse_usize(&args[i]).unwrap_or(usize::MAX)
            } else {
                return (offset, usize::MAX);
            };
            return (offset, count);
        }
        i += 1;
    }
    (0, usize::MAX)
}

/// Parse a frame as usize. Similar to parse_u32 but returns usize for pagination.
pub(crate) fn parse_usize(frame: &Frame) -> Option<usize> {
    match frame {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        Frame::Integer(n) => usize::try_from(*n).ok(),
        _ => None,
    }
}

// -- Filter parsing --

/// Parse FILTER clause from FT.SEARCH args.
/// Looks for "FILTER" keyword after the query string, parses the filter expression.
///
/// Supported syntax:
///   @field:{value}              -- tag equality
///   @field:[min max]            -- numeric range
///   @field:{value} @field2:[a b] -- implicit AND of multiple conditions
pub(crate) fn parse_filter_clause(args: &[Frame]) -> Option<FilterExpr> {
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

/// Parse SESSION clause from FT.SEARCH args.
/// Looks for "SESSION" keyword after the query string, returns the session key.
///
/// Syntax: FT.SEARCH idx query ... SESSION sess:conv1
pub fn parse_session_clause(args: &[Frame]) -> Option<Bytes> {
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"SESSION") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            return extract_bulk(&args[i]);
        }
        i += 1;
    }
    None
}

/// Parse filter string like "@field:{value}" or "@field:[min max]" or "@field:[lon lat radius_km]"
/// Multiple conditions are implicitly ANDed.
/// Datetime filtering: epoch seconds use NumRange — no special handling needed (FNDN-02).
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
            let val_str = &s[val_start..pos];
            if pos < s.len() {
                pos += 1; // skip }
            }
            // Boolean detection: "true"/"false" (case-insensitive) → BoolEq
            if val_str.eq_ignore_ascii_case("true") {
                exprs.push(FilterExpr::BoolEq { field, value: true });
            } else if val_str.eq_ignore_ascii_case("false") {
                exprs.push(FilterExpr::BoolEq {
                    field,
                    value: false,
                });
            } else if val_str.contains(' ') {
                // Multi-word content → full-text match (AND semantics)
                let terms: Vec<Bytes> = val_str
                    .split_whitespace()
                    .map(|t| Bytes::from(t.to_owned()))
                    .collect();
                exprs.push(FilterExpr::TextMatch { field, terms });
            } else {
                let value = Bytes::from(val_str.to_owned());
                exprs.push(FilterExpr::TagEq { field, value });
            }
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
            if parts.len() == 3 {
                // Geo radius: @field:[lon lat radius_km]
                let lon: f64 = parts[0].parse().ok()?;
                let lat: f64 = parts[1].parse().ok()?;
                let radius_km: f64 = parts[2].parse().ok()?;
                exprs.push(FilterExpr::GeoRadius {
                    field,
                    lon,
                    lat,
                    radius_km,
                });
            } else if parts.len() == 2 {
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

/// FT.CONFIG SET index_name AUTOCOMPACT ON|OFF
/// FT.CONFIG GET index_name AUTOCOMPACT
///
/// Per-index configuration. Currently supports AUTOCOMPACT only.
/// args[0] = SET|GET, args[1] = index_name, args[2] = param_name, args[3] = value (SET only)
pub fn ft_config(store: &mut VectorStore, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.CONFIG' command",
        ));
    }
    let subcommand = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid subcommand")),
    };
    let index_name = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };
    let param_name = match extract_bulk(&args[2]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid parameter name")),
    };

    if subcommand.eq_ignore_ascii_case(b"SET") {
        if args.len() < 4 {
            return Frame::Error(Bytes::from_static(b"ERR SET requires a value"));
        }
        let value = match extract_bulk(&args[3]) {
            Some(b) => b,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid value")),
        };
        ft_config_set(store, &index_name, &param_name, &value)
    } else if subcommand.eq_ignore_ascii_case(b"GET") {
        ft_config_get(store, &index_name, &param_name)
    } else {
        Frame::Error(Bytes::from_static(
            b"ERR FT.CONFIG subcommand must be SET or GET",
        ))
    }
}

fn ft_config_set(
    store: &mut VectorStore,
    index_name: &[u8],
    param: &[u8],
    value: &[u8],
) -> Frame {
    let idx = match store.get_index_mut(index_name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };
    if param.eq_ignore_ascii_case(b"AUTOCOMPACT") {
        if value.eq_ignore_ascii_case(b"ON")
            || value == b"1"
            || value.eq_ignore_ascii_case(b"TRUE")
        {
            idx.autocompact_enabled = true;
            Frame::SimpleString(Bytes::from_static(b"OK"))
        } else if value.eq_ignore_ascii_case(b"OFF")
            || value == b"0"
            || value.eq_ignore_ascii_case(b"FALSE")
        {
            idx.autocompact_enabled = false;
            Frame::SimpleString(Bytes::from_static(b"OK"))
        } else {
            Frame::Error(Bytes::from_static(
                b"ERR AUTOCOMPACT value must be ON or OFF",
            ))
        }
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown config parameter"))
    }
}

fn ft_config_get(store: &mut VectorStore, index_name: &[u8], param: &[u8]) -> Frame {
    let idx = match store.get_index_mut(index_name) {
        Some(i) => i,
        None => return Frame::Error(Bytes::from_static(b"Unknown Index name")),
    };
    if param.eq_ignore_ascii_case(b"AUTOCOMPACT") {
        let val = if idx.autocompact_enabled {
            "ON"
        } else {
            "OFF"
        };
        Frame::BulkString(Bytes::from(val))
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown config parameter"))
    }
}

// -- FT.EXPAND (GraphRAG standalone expansion) --

/// FT.EXPAND idx key1 key2 ... DEPTH N [GRAPH graph_name]
///
/// Standalone graph expansion from explicit Redis keys. Returns graph neighbors
/// up to N hops with `__graph_hops` metadata per discovered node.
///
/// If `GRAPH graph_name` is specified, uses that named graph. Otherwise,
/// auto-detects by checking which graph has the first seed key registered.
///
/// Shard-local: only expands within the graph data on this shard (GRAF-05).
#[cfg(feature = "graph")]
pub fn ft_expand(
    graph_store: &crate::graph::store::GraphStore,
    args: &[Frame],
) -> Frame {
    // args[0] = index name (reserved for consistency), then keys, then DEPTH N, optionally GRAPH name
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.EXPAND' command",
        ));
    }

    // args[0] = index name (reserved, validate present)
    let _index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    // Scan for DEPTH keyword to find boundary between keys and options.
    let mut depth_pos = None;
    for i in 1..args.len() {
        if matches_keyword(&args[i], b"DEPTH") {
            depth_pos = Some(i);
            break;
        }
    }

    let depth_pos = match depth_pos {
        Some(p) => p,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR syntax error: expected DEPTH N",
            ));
        }
    };

    // Keys are args[1..depth_pos]
    if depth_pos <= 1 {
        return Frame::Error(Bytes::from_static(b"ERR no keys specified"));
    }

    let key_args = &args[1..depth_pos];

    // Parse DEPTH value
    if depth_pos + 1 >= args.len() {
        return Frame::Error(Bytes::from_static(
            b"ERR syntax error: DEPTH requires a value",
        ));
    }
    let depth = match parse_u32(&args[depth_pos + 1]) {
        Some(d) => d,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid DEPTH value")),
    };

    // Parse optional GRAPH graph_name after DEPTH N
    let mut graph_name: Option<Bytes> = None;
    let mut pos = depth_pos + 2;
    while pos < args.len() {
        if matches_keyword(&args[pos], b"GRAPH") {
            pos += 1;
            if pos >= args.len() {
                return Frame::Error(Bytes::from_static(
                    b"ERR syntax error: GRAPH requires a name",
                ));
            }
            graph_name = extract_bulk(&args[pos]);
            pos += 1;
        } else {
            pos += 1;
        }
    }

    // Build seed keys as (Bytes, f32) with vec_score = 0.0
    let seed_keys: SmallVec<[(Bytes, f32); 16]> = key_args
        .iter()
        .filter_map(|f| extract_bulk(f).map(|b| (b, 0.0f32)))
        .collect();

    if seed_keys.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR no keys specified"));
    }

    // Find the graph to use.
    let graph = if let Some(ref name) = graph_name {
        graph_store.get_graph(name)
    } else {
        // Auto-detect: find a graph that has the first seed key registered.
        let first_key = &seed_keys[0].0;
        let mut found = None;
        for gname in graph_store.list_graphs() {
            if let Some(g) = graph_store.get_graph(gname) {
                if g.lookup_node_by_key(first_key).is_some() {
                    found = Some(g);
                    break;
                }
            }
        }
        found
    };

    let graph = match graph {
        Some(g) => g,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR no graph contains the specified keys",
            ));
        }
    };

    // Perform graph expansion.
    let results = graph_expand::expand_results_via_graph(graph, &seed_keys, depth);

    // Format as RESP array: count, then for each result: key + field array.
    // Field array contains: __graph_hops, hops_value, __vec_score, "0"
    let total = results.len();
    let mut frames: Vec<Frame> = Vec::with_capacity(1 + total * 2);
    frames.push(Frame::Integer(total as i64));

    for r in &results {
        frames.push(Frame::BulkString(r.key.clone()));
        // Field array: [__graph_hops, N, __vec_score, "0"]
        let mut hop_buf = itoa::Buffer::new();
        let hop_str = hop_buf.format(r.graph_hops);
        let fields: FrameVec = vec![
            Frame::BulkString(Bytes::from_static(b"__graph_hops")),
            Frame::BulkString(Bytes::copy_from_slice(hop_str.as_bytes())),
            Frame::BulkString(Bytes::from_static(b"__vec_score")),
            Frame::BulkString(Bytes::from_static(b"0")),
        ]
        .into();
        frames.push(Frame::Array(fields));
    }

    Frame::Array(frames.into())
}

// -- SPARSE clause parsing --

/// Parse SPARSE clause from FT.SEARCH args.
///
/// Syntax: `... SPARSE @field_name $param_name ...`
///
/// Returns `(field_name, param_name)` where field_name has the `@` stripped
/// and param_name has the `$` stripped.
pub(crate) fn parse_sparse_clause(args: &[Frame]) -> Option<(Bytes, Bytes)> {
    let mut i = 2;
    while i < args.len() {
        if matches_keyword(&args[i], b"SPARSE") {
            i += 1;
            if i >= args.len() {
                return None;
            }
            // Read @field_name
            let field_raw = extract_bulk(&args[i])?;
            let field_str = std::str::from_utf8(&field_raw).ok()?;
            let field_name = if let Some(stripped) = field_str.strip_prefix('@') {
                Bytes::from(stripped.to_owned())
            } else {
                field_raw
            };
            i += 1;
            if i >= args.len() {
                return None;
            }
            // Read $param_name
            let param_raw = extract_bulk(&args[i])?;
            let param_str = std::str::from_utf8(&param_raw).ok()?;
            let param_name = if let Some(stripped) = param_str.strip_prefix('$') {
                Bytes::from(stripped.to_owned())
            } else {
                param_raw
            };
            return Some((field_name, param_name));
        }
        i += 1;
    }
    None
}

/// Parse a sparse query blob: alternating u32 (LE dim_id) + f32 (LE weight) pairs.
/// Returns empty Vec on invalid input.
fn parse_sparse_query_blob(blob: &[u8]) -> Vec<(u32, f32)> {
    if blob.len() % 8 != 0 || blob.is_empty() {
        return Vec::new();
    }
    let num_pairs = blob.len() / 8;
    let mut pairs = Vec::with_capacity(num_pairs);
    for i in 0..num_pairs {
        let offset = i * 8;
        let dim = u32::from_le_bytes([blob[offset], blob[offset + 1], blob[offset + 2], blob[offset + 3]]);
        let weight = f32::from_le_bytes([blob[offset + 4], blob[offset + 5], blob[offset + 6], blob[offset + 7]]);
        pairs.push((dim, weight));
    }
    pairs
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

// -- Helpers (private) --

pub(crate) fn extract_bulk(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) => Some(b.clone()),
        _ => None,
    }
}

pub(crate) fn matches_keyword(frame: &Frame, keyword: &[u8]) -> bool {
    match frame {
        Frame::BulkString(b) => b.eq_ignore_ascii_case(keyword),
        _ => false,
    }
}

pub(crate) fn parse_u32(frame: &Frame) -> Option<u32> {
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
