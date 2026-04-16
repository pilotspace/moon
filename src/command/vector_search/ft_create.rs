//! FT.CREATE command handler — creates vector/sparse indexes.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::vector::store::{IndexMeta, VectorFieldMeta, VectorStore, MAX_VECTOR_FIELDS};
use crate::vector::turbo_quant::collection::QuantizationConfig;
use crate::vector::types::DistanceMetric;

use super::{extract_bulk, matches_keyword, parse_u32};

/// FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC L2
///
/// Parses the FT.CREATE syntax and creates a vector index in the store.
/// args[0] = index_name, args[1..] = ON HASH PREFIX ... SCHEMA ...
pub fn ft_create(
    store: &mut VectorStore,
    _text_store: &mut crate::text::store::TextStore,
    args: &[Frame],
) -> Frame {
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
                        return Frame::Error(Bytes::from_static(b"ERR invalid SPARSE DIM value"));
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
            return Frame::Error(Bytes::from_static(
                b"ERR expected VECTOR or SPARSE after field name",
            ));
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
                    padded_dimension: crate::vector::turbo_quant::encoder::padded_dimension(dim),
                    metric: parsed.metric,
                    quantization: parsed.quantization,
                    build_mode: parsed.build_mode,
                });

                if vector_fields.len() > MAX_VECTOR_FIELDS {
                    return Frame::Error(Bytes::from_static(b"ERR too many VECTOR fields (max 8)"));
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
        schema_fields: Vec::new(),
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
            return Err(Frame::Error(Bytes::from_static(b"ERR invalid param count")));
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
