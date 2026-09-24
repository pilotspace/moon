//! Persist vector index metadata to a sidecar file.
//!
//! On FT.CREATE / FT.DROPINDEX, all active index definitions are written to
//! `{shard_dir}/vector-indexes.meta`. On recovery, this file is read before
//! snapshot load so that HASH keys can be auto-indexed as they are restored.
//!
//! ## Format v1 (legacy, read-only)
//!
//! ```text
//! [magic: 4B "VMIX"] [version: 1] [count: u16] [reserved: 1B]
//! Per index: name, dim, metric, hnsw params, source_field, prefixes
//! ```
//!
//! ## Format v2 (read/write for compat)
//!
//! Same as v1 per-index fields, followed by multi-vector field array:
//!
//! ```text
//! [magic: 4B "VMIX"] [version: 2] [count: u16] [reserved: 1B]
//! Per index:
//!   ... (same as v1 fields for backward compat) ...
//!   [field_count: u16]
//!   Per field:
//!     [field_name_len: u16] [field_name: bytes]
//!     [dimension: u32] [metric: u8] [quantization: u8] [build_mode: u8] [reserved: 1B]
//! ```
//!
//! ## Format v3 (W3-deep)
//!
//! Extends v2 with a per-index `compaction_weight` (f32 LE) appended after the
//! v2 vector_fields block. v1/v2 files are read with `compaction_weight = 1.0`.
//!
//! ```text
//! [magic: 4B "VMIX"] [version: 3] [count: u16] [reserved: 1B]
//! Per index:
//!   ... (same as v2 fields) ...
//!   [field_count: u16]
//!   Per field: ... (same as v2) ...
//!   [compaction_weight: f32 LE]   ← NEW in v3
//! ```
//!
//! ## Format v4 (WS5a db-scoped indexes)
//!
//! Extends v3 with a single `db_index: u8` byte appended after
//! `compaction_weight`, tagging which logical db (`SELECT 0..databases-1`)
//! the index belongs to. v1/v2/v3 sidecars (written before this field
//! existed) are read with `db_index = 0` — pre-v0.6.0 indexes become
//! db-0-owned, matching their previous global (all-db-visible-from-db-0)
//! behavior for the common case where db 0 is what was used.
//!
//! ```text
//! [magic: 4B "VMIX"] [version: 4] [count: u16] [reserved: 1B]
//! Per index:
//!   ... (same as v3 fields) ...
//!   [db_index: u8]   ← NEW in v4
//! ```
//!
//! ## Format v5 (search-tuning knobs)
//!
//! Extends v4 with the FT.CONFIG search-tuning knobs appended after
//! `db_index`. v1-v4 sidecars are read with the defaults (mult 4, beam off).
//!
//! ```text
//! Per index:
//!   ... (same as v4 fields) ...
//!   [rerank_mult: u32 LE] [exact_beam: u8] [reserved: 3B]   ← NEW in v5
//! ```
//!
//! ## Format v6 (payload schema, moon#1194 — written only in declared mode)
//!
//! Extends v5 with the non-vector schema fields FT.CREATE declared (TEXT /
//! TAG / NUMERIC), which schema-aware payload indexing needs across a
//! restart. v1-v5 sidecars are read with an empty `schema_fields` — exactly
//! how every version before this one came back (legacy: every HASH field is
//! payload-indexed).
//!
//! The writer emits v6 ONLY when `MOON_VECTOR_PAYLOAD_SCHEMA=declared` is on
//! and some index actually declared a payload field — the one thing only v6
//! can carry (moon#1227 review F5). Otherwise it writes v5, which the
//! previous release reads: an unconditional v6 made a rollback start with no
//! vector indexes after any FT.CREATE / FT.DROPINDEX / FT.CONFIG. v5 already
//! carries the RERANK_MULT / EXACT_BEAM knobs.
//!
//! ```text
//! Per index:
//!   ... (same as v5 fields) ...
//!   [payload_field_count: u16]                                ← NEW in v6
//!   Per field:
//!     [kind: u8 (1 TEXT, 2 TAG, 3 NUMERIC)] [name_len: u16] [name]
//!     TEXT only: [weight: f64 LE] [flags: u8 (1 nostem, 2 sortable, 4 noindex)]
//! ```

use std::io::{self, Read};
use std::path::Path;

use bytes::Bytes;

use crate::vector::store::{FieldType, IndexMeta, VectorFieldMeta};
use crate::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
use crate::vector::types::DistanceMetric;

const MAGIC: &[u8; 4] = b"VMIX";
const VERSION_V1: u8 = 1;
const VERSION_V2: u8 = 2;
const VERSION_V3: u8 = 3;
const VERSION_V4: u8 = 4;
/// v5 appends per-index search-tuning knobs (rerank_mult u32 LE,
/// exact_beam u8, 3 reserved bytes) after the v4 db_index byte.
const VERSION_V5: u8 = 5;

/// v6 (moon#1194): v5 + the declared TEXT/TAG/NUMERIC payload schema.
const VERSION_V6: u8 = 6;

const PAYLOAD_KIND_TEXT: u8 = 1;
const PAYLOAD_KIND_TAG: u8 = 2;
const PAYLOAD_KIND_NUMERIC: u8 = 3;

/// Default compaction weight used when reading v1/v2 sidecars without a stored weight.
const DEFAULT_WEIGHT_ON_LOAD: f32 = 1.0;

/// Default db_index used when reading v1/v2/v3 sidecars written before
/// WS5a db-scoped indexes existed.
const DEFAULT_DB_INDEX_ON_LOAD: u8 = 0;

/// Serialize a list of IndexMeta to bytes using v1 format (for testing v1 migration).
#[cfg(test)]
fn serialize_index_metas_v1(metas: &[&IndexMeta]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION_V1);
    buf.extend_from_slice(&(metas.len() as u16).to_le_bytes());
    buf.push(0); // reserved

    for m in metas {
        write_v1_per_index(&mut buf, m);
    }

    buf
}

/// Serialize a list of IndexMeta to bytes using v2 format.
///
/// v2 writes the same per-index fields as v1 (top-level dimension/metric/etc.
/// from `vector_fields[0]` for backward compatibility), then appends the full
/// `vector_fields` array.
pub fn serialize_index_metas(metas: &[&IndexMeta]) -> Vec<u8> {
    // Wrap with default weight=1.0 and delegate to the current writer.
    let pairs: Vec<(&IndexMeta, f32)> =
        metas.iter().map(|&m| (m, DEFAULT_WEIGHT_ON_LOAD)).collect();
    serialize_index_metas_for_mode(
        &pairs,
        crate::vector::filter::payload_schema::payload_schema_declared_mode(),
    )
}

/// The version the sidecar writer emits (moon#1227 review F5): v6 only when
/// schema-aware payload indexing is on (`declared_mode`) AND some index
/// declared a TEXT / TAG / NUMERIC field — the one thing v5 cannot carry.
/// Everything else is v5, which the previous release reads, so a rollback
/// keeps its vector indexes.
fn sidecar_write_version(pairs: &[(&IndexMeta, f32)], declared_mode: bool) -> u8 {
    let declares_payload = |m: &IndexMeta| {
        m.schema_fields
            .iter()
            .any(|f| !matches!(f, FieldType::Vector(_)))
    };
    if declared_mode && pairs.iter().any(|(m, _)| declares_payload(m)) {
        VERSION_V6
    } else {
        VERSION_V5
    }
}

/// Serialize with the version [`sidecar_write_version`] picks for
/// `declared_mode` (the process's `MOON_VECTOR_PAYLOAD_SCHEMA` in
/// production; explicit so both modes are testable in one process).
pub fn serialize_index_metas_for_mode(pairs: &[(&IndexMeta, f32)], declared_mode: bool) -> Vec<u8> {
    serialize_index_metas_versioned(pairs, sidecar_write_version(pairs, declared_mode))
}

/// Serialize `(IndexMeta, compaction_weight)` pairs to bytes using v3 format (W3-deep).
///
/// v3 extends v2 with a 4-byte LE f32 `compaction_weight` per index. Kept
/// for `#[cfg(test)]` v3-migration coverage; production writers use
/// [`serialize_index_metas_v4`] (via [`serialize_index_metas`]).
#[cfg(test)]
fn serialize_index_metas_v3(pairs: &[(&IndexMeta, f32)]) -> Vec<u8> {
    serialize_index_metas_versioned(pairs, VERSION_V3)
}

/// Serialize `(IndexMeta, compaction_weight)` pairs to bytes using the v4
/// format (WS5a): v3 plus a trailing `db_index: u8` per index. Kept for
/// v4-migration test coverage; production writers use
/// [`serialize_index_metas_v5`] (via [`serialize_index_metas`]).
pub fn serialize_index_metas_v4(pairs: &[(&IndexMeta, f32)]) -> Vec<u8> {
    serialize_index_metas_versioned(pairs, VERSION_V4)
}

/// Serialize `(IndexMeta, compaction_weight)` pairs to bytes using the v5
/// format: v4 plus the per-index search-tuning knobs (`rerank_mult` u32 LE,
/// `exact_beam` u8, 3 reserved bytes). The default sidecar format, and what
/// replicas receive.
pub fn serialize_index_metas_v5(pairs: &[(&IndexMeta, f32)]) -> Vec<u8> {
    serialize_index_metas_versioned(pairs, VERSION_V5)
}

/// Serialize `(IndexMeta, compaction_weight)` pairs to bytes using the v6
/// format: v5 plus the declared payload schema (moon#1194). Written only in
/// declared mode — see [`sidecar_write_version`].
pub fn serialize_index_metas_v6(pairs: &[(&IndexMeta, f32)]) -> Vec<u8> {
    serialize_index_metas_versioned(pairs, VERSION_V6)
}

/// Shared v3/v4/v5 serializer — `version` selects which trailing extensions
/// (`db_index` byte at v4, search-tuning knobs at v5) are written.
fn serialize_index_metas_versioned(pairs: &[(&IndexMeta, f32)], version: u8) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(MAGIC);
    buf.push(version);
    buf.extend_from_slice(&(pairs.len() as u16).to_le_bytes());
    buf.push(0); // reserved

    for (m, weight) in pairs {
        // v1-compatible top-level fields
        write_v1_per_index(&mut buf, m);

        // v2 vector_fields extension
        buf.extend_from_slice(&(m.vector_fields.len() as u16).to_le_bytes());
        for f in &m.vector_fields {
            buf.extend_from_slice(&(f.field_name.len() as u16).to_le_bytes());
            buf.extend_from_slice(&f.field_name);
            buf.extend_from_slice(&f.dimension.to_le_bytes());
            buf.push(f.metric as u8);
            buf.push(f.quantization as u8);
            buf.push(f.build_mode as u8);
            buf.push(0); // reserved
        }

        // v3 extension: compaction_weight (4 bytes LE f32)
        buf.extend_from_slice(&weight.to_le_bytes());

        // v4 extension: db_index (1 byte)
        if version >= VERSION_V4 {
            buf.push(m.db_index);
        }

        // v5 extension: search-tuning knobs
        if version >= VERSION_V5 {
            buf.extend_from_slice(&m.rerank_mult.to_le_bytes());
            buf.push(m.exact_beam as u8);
            buf.extend_from_slice(&[0u8; 3]); // reserved
        }

        // v6 extension: declared payload schema (TEXT/TAG/NUMERIC).
        if version >= VERSION_V6 {
            write_payload_schema(&mut buf, &m.schema_fields);
        }
    }

    buf
}

/// v6: the non-vector `schema_fields`, in declaration order.
fn write_payload_schema(buf: &mut Vec<u8>, schema: &[FieldType]) {
    let payload: Vec<&FieldType> = schema
        .iter()
        .filter(|f| !matches!(f, FieldType::Vector(_)))
        .collect();
    buf.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    for f in payload {
        let (kind, name) = match f {
            FieldType::Text { field_name, .. } => (PAYLOAD_KIND_TEXT, field_name),
            FieldType::Tag { field_name } => (PAYLOAD_KIND_TAG, field_name),
            FieldType::Numeric { field_name } => (PAYLOAD_KIND_NUMERIC, field_name),
            FieldType::Vector(_) => continue,
        };
        buf.push(kind);
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buf.extend_from_slice(name);
        if let FieldType::Text {
            weight,
            nostem,
            sortable,
            noindex,
            ..
        } = f
        {
            buf.extend_from_slice(&weight.to_le_bytes());
            buf.push(u8::from(*nostem) | u8::from(*sortable) << 1 | u8::from(*noindex) << 2);
        }
    }
}

/// v6: read what [`write_payload_schema`] wrote.
fn read_payload_schema(data: &[u8], cursor: &mut usize) -> io::Result<Vec<FieldType>> {
    let count = read_u16(data, cursor)? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = read_u8(data, cursor)?;
        let len = read_u16(data, cursor)? as usize;
        let field_name = Bytes::copy_from_slice(read_bytes(data, cursor, len)?);
        fields.push(match kind {
            PAYLOAD_KIND_TEXT => {
                let w = read_bytes(data, cursor, 8)?;
                let weight = f64::from_le_bytes([w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7]]);
                let flags = read_u8(data, cursor)?;
                FieldType::Text {
                    field_name,
                    weight,
                    nostem: flags & 1 != 0,
                    sortable: flags & 2 != 0,
                    noindex: flags & 4 != 0,
                }
            }
            PAYLOAD_KIND_TAG => FieldType::Tag { field_name },
            PAYLOAD_KIND_NUMERIC => FieldType::Numeric { field_name },
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unknown payload field kind {other}"),
                ));
            }
        });
    }
    Ok(fields)
}

/// Write the v1 per-index fields (shared between v1, v2, and v3 serializers).
fn write_v1_per_index(buf: &mut Vec<u8>, m: &IndexMeta) {
    // name
    buf.extend_from_slice(&(m.name.len() as u16).to_le_bytes());
    buf.extend_from_slice(&m.name);

    // fixed fields
    buf.extend_from_slice(&m.dimension.to_le_bytes());
    buf.push(m.metric as u8);
    buf.extend_from_slice(&m.hnsw_m.to_le_bytes());
    buf.extend_from_slice(&m.hnsw_ef_construction.to_le_bytes());
    buf.extend_from_slice(&m.hnsw_ef_runtime.to_le_bytes());
    buf.extend_from_slice(&m.compact_threshold.to_le_bytes());
    buf.push(m.quantization as u8);
    buf.push(m.build_mode as u8);
    buf.extend_from_slice(&[0u8; 2]); // reserved

    // source_field
    buf.extend_from_slice(&(m.source_field.len() as u16).to_le_bytes());
    buf.extend_from_slice(&m.source_field);

    // key_prefixes
    buf.extend_from_slice(&(m.key_prefixes.len() as u16).to_le_bytes());
    for p in &m.key_prefixes {
        buf.extend_from_slice(&(p.len() as u16).to_le_bytes());
        buf.extend_from_slice(p);
    }
}

/// Deserialize IndexMeta list from bytes. Handles v1 through v6 formats.
///
/// Older data is auto-migrated:
/// - v1: single source_field wrapped into 1-element `vector_fields`.
/// - v2: full field array; `compaction_weight` defaults to 1.0.
/// - v3: full field array + explicit `compaction_weight` per index.
/// - v4: v3 + `db_index` per index (pre-v4 defaults to 0).
/// - v5: v4 + `rerank_mult`/`exact_beam` (pre-v5 defaults to 4 / OFF).
/// - v6: v5 + the declared payload schema (pre-v6: empty `schema_fields`).
///
/// Returns `(IndexMeta, compaction_weight)` pairs.
pub fn deserialize_index_metas_with_weights(data: &[u8]) -> io::Result<Vec<(IndexMeta, f32)>> {
    if data.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "too short"));
    }
    if &data[0..4] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }
    let version = data[4];
    if !(VERSION_V1..=VERSION_V6).contains(&version) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported version {version}"),
        ));
    }
    let count = u16::from_le_bytes([data[5], data[6]]) as usize;
    let mut cursor = 8;
    let mut results = Vec::with_capacity(count);

    for _ in 0..count {
        let (
            meta_base,
            source_field,
            metric,
            quantization,
            build_mode,
            dimension,
            padded_dimension,
        ) = read_v1_per_index(data, &mut cursor)?;

        let vector_fields = if version >= VERSION_V2 {
            // Read v2+ vector_fields extension
            let field_count = read_u16(data, &mut cursor)? as usize;
            let mut fields = Vec::with_capacity(field_count);
            for _ in 0..field_count {
                let fn_len = read_u16(data, &mut cursor)? as usize;
                let field_name = Bytes::copy_from_slice(read_bytes(data, &mut cursor, fn_len)?);
                let f_dim = read_u32(data, &mut cursor)?;
                let f_metric_u8 = read_u8(data, &mut cursor)?;
                let f_quant_u8 = read_u8(data, &mut cursor)?;
                let f_build_u8 = read_u8(data, &mut cursor)?;
                cursor += 1; // reserved

                let f_metric = decode_metric(f_metric_u8);
                let f_quant = QuantizationConfig::from_u8(f_quant_u8);
                let f_build = decode_build_mode(f_build_u8);
                let f_padded = crate::vector::turbo_quant::encoder::padded_dimension(f_dim);

                fields.push(VectorFieldMeta {
                    field_name,
                    dimension: f_dim,
                    padded_dimension: f_padded,
                    metric: f_metric,
                    quantization: f_quant,
                    build_mode: f_build,
                });
            }
            fields
        } else {
            // v1 migration: wrap single field
            vec![VectorFieldMeta {
                field_name: source_field.clone(),
                dimension,
                padded_dimension,
                metric,
                quantization,
                build_mode,
            }]
        };

        // v3: read compaction_weight; v1/v2: default to 1.0.
        let compaction_weight = if version >= VERSION_V3 {
            let w_bytes = read_bytes(data, &mut cursor, 4)?;
            f32::from_le_bytes([w_bytes[0], w_bytes[1], w_bytes[2], w_bytes[3]])
        } else {
            DEFAULT_WEIGHT_ON_LOAD
        };

        // v4 (WS5a): read db_index; v1/v2/v3 sidecars predate db scoping and
        // default to db 0 (see module docs).
        let db_index = if version >= VERSION_V4 {
            read_u8(data, &mut cursor)?
        } else {
            DEFAULT_DB_INDEX_ON_LOAD
        };

        // v5: read search-tuning knobs; older versions default (mult 4,
        // beam off). Out-of-range persisted mult (corrupt/hand-edited
        // sidecar) clamps back to the default rather than importing a
        // pathological knob.
        let (rerank_mult, exact_beam) = if version >= VERSION_V5 {
            let mult = read_u32(data, &mut cursor)?;
            let beam = read_u8(data, &mut cursor)? != 0;
            cursor += 3; // reserved
            let mult = if (1..=64).contains(&mult) { mult } else { 4 };
            (mult, beam)
        } else {
            (4, false)
        };

        // v6: declared payload schema. Rebuilt in FT.CREATE's shape (vector
        // fields first). Older sidecars come back with no schema, exactly as
        // before (every HASH field payload-indexed).
        let schema_fields = if version >= VERSION_V6 {
            let payload = read_payload_schema(data, &mut cursor)?;
            vector_fields
                .iter()
                .cloned()
                .map(FieldType::Vector)
                .chain(payload)
                .collect()
        } else {
            Vec::new()
        };

        let meta = IndexMeta {
            name: meta_base.0,
            dimension,
            padded_dimension,
            metric,
            hnsw_m: meta_base.1,
            hnsw_ef_construction: meta_base.2,
            hnsw_ef_runtime: meta_base.3,
            compact_threshold: meta_base.4,
            source_field,
            key_prefixes: meta_base.5,
            quantization,
            build_mode,
            vector_fields,
            schema_fields,
            merge_mode: crate::vector::segment::compaction::MergeMode::GraphUnion,
            keep_raw: false,
            db_index,
            rerank_mult,
            exact_beam,
        };
        results.push((meta, compaction_weight));
    }

    Ok(results)
}

/// Deserialize IndexMeta list from bytes (backward-compat: drops compaction weights).
///
/// Delegates to `deserialize_index_metas_with_weights`; callers that only need
/// `IndexMeta` (e.g. existing unit tests) use this.
pub fn deserialize_index_metas(data: &[u8]) -> io::Result<Vec<IndexMeta>> {
    Ok(deserialize_index_metas_with_weights(data)?
        .into_iter()
        .map(|(m, _)| m)
        .collect())
}

/// Read v1 per-index fields from the data stream.
/// Returns a tuple of base fields + decoded enums for reuse.
#[allow(clippy::type_complexity)]
fn read_v1_per_index(
    data: &[u8],
    cursor: &mut usize,
) -> io::Result<(
    (Bytes, u32, u32, u32, u32, Vec<Bytes>), // name, hnsw_m, ef_con, ef_run, compact, prefixes
    Bytes,                                   // source_field
    DistanceMetric,
    QuantizationConfig,
    BuildMode,
    u32, // dimension
    u32, // padded_dimension
)> {
    // name
    let name_len = read_u16(data, cursor)? as usize;
    let name = Bytes::copy_from_slice(read_bytes(data, cursor, name_len)?);

    // fixed fields
    let dimension = read_u32(data, cursor)?;
    let metric_u8 = read_u8(data, cursor)?;
    let hnsw_m = read_u32(data, cursor)?;
    let hnsw_ef_construction = read_u32(data, cursor)?;
    let hnsw_ef_runtime = read_u32(data, cursor)?;
    let compact_threshold = read_u32(data, cursor)?;
    let quant_u8 = read_u8(data, cursor)?;
    let build_u8 = read_u8(data, cursor)?;
    *cursor += 2; // reserved

    // source_field
    let sf_len = read_u16(data, cursor)? as usize;
    let source_field = Bytes::copy_from_slice(read_bytes(data, cursor, sf_len)?);

    // key_prefixes
    let prefix_count = read_u16(data, cursor)? as usize;
    let mut key_prefixes = Vec::with_capacity(prefix_count);
    for _ in 0..prefix_count {
        let plen = read_u16(data, cursor)? as usize;
        let prefix = Bytes::copy_from_slice(read_bytes(data, cursor, plen)?);
        key_prefixes.push(prefix);
    }

    let metric = decode_metric(metric_u8);
    let quantization = QuantizationConfig::from_u8(quant_u8);
    let build_mode = decode_build_mode(build_u8);
    let padded_dimension = crate::vector::turbo_quant::encoder::padded_dimension(dimension);

    Ok((
        (
            name,
            hnsw_m,
            hnsw_ef_construction,
            hnsw_ef_runtime,
            compact_threshold,
            key_prefixes,
        ),
        source_field,
        metric,
        quantization,
        build_mode,
        dimension,
        padded_dimension,
    ))
}

#[inline]
fn decode_metric(v: u8) -> DistanceMetric {
    match v {
        0 => DistanceMetric::L2,
        1 => DistanceMetric::Cosine,
        2 => DistanceMetric::InnerProduct,
        _ => DistanceMetric::L2,
    }
}

#[inline]
fn decode_build_mode(v: u8) -> BuildMode {
    if v == 1 {
        BuildMode::Exact
    } else {
        BuildMode::Light
    }
}

/// Write all active index metadata to the sidecar file (v2 compat — weight defaults to 1.0).
///
/// Kept for callers that don't have weight state (e.g. recovery paths that reconstruct
/// IndexMeta before VectorIndex is created). Prefer `save_index_metadata_v3` when
/// `VectorIndex` weights are available.
pub fn save_index_metadata(shard_dir: &Path, metas: &[&IndexMeta]) -> io::Result<()> {
    let pairs: Vec<(&IndexMeta, f32)> =
        metas.iter().map(|&m| (m, DEFAULT_WEIGHT_ON_LOAD)).collect();
    save_index_metadata_v3(shard_dir, &pairs)
}

/// Write all active index metadata **with compaction weights** to the sidecar file
/// (v5, or v6 in declared mode — see [`sidecar_write_version`]; name kept as `_v3`
/// for API stability across the many existing call sites).
///
/// Called after FT.CREATE / FT.DROPINDEX / FT.CONFIG SET COMPACTION_WEIGHT.
/// Atomically replaces the file via `atomic_write_durable` (K3: temp +
/// fsync + rename + dir-fsync). Before this fix the write skipped the
/// directory fsync -- same gap as `src/text/index_persist.rs`'s sidecars,
/// found by the same K3 grep sweep
/// (`.planning/reviews/kernel-m2-brief-2026-07-12.md`).
pub fn save_index_metadata_v3(shard_dir: &Path, pairs: &[(&IndexMeta, f32)]) -> io::Result<()> {
    save_index_metadata_for_mode(
        shard_dir,
        pairs,
        crate::vector::filter::payload_schema::payload_schema_declared_mode(),
    )
}

/// [`save_index_metadata_v3`] for an explicit payload-schema mode.
fn save_index_metadata_for_mode(
    shard_dir: &Path,
    pairs: &[(&IndexMeta, f32)],
    declared_mode: bool,
) -> io::Result<()> {
    let path = shard_dir.join("vector-indexes.meta");
    // At least v5: this wrote v4 until moon#1194, so the v5 FT.CONFIG knobs
    // (RERANK_MULT / EXACT_BEAM) never reached the sidecar and reset to their
    // defaults on every restart. v6 only in declared mode (moon#1227 review
    // F5): an unconditional v6 cost a rollback every vector index.
    let data = serialize_index_metas_for_mode(pairs, declared_mode);
    crate::persistence::atomic::atomic_write_durable(&path, &data)?;
    Ok(())
}

/// Load index metadata from the sidecar file (returns `IndexMeta` only, drops weights).
///
/// Returns empty vec if the file doesn't exist (fresh server).
pub fn load_index_metadata(shard_dir: &Path) -> io::Result<Vec<IndexMeta>> {
    Ok(load_index_metadata_with_weights(shard_dir)?
        .into_iter()
        .map(|(m, _)| m)
        .collect())
}

/// Load index metadata **and** compaction weights from the sidecar file.
///
/// Returns empty vec if the file doesn't exist (fresh server).
/// v1/v2 files return weight=1.0 for all indexes.
pub fn load_index_metadata_with_weights(shard_dir: &Path) -> io::Result<Vec<(IndexMeta, f32)>> {
    let path = shard_dir.join("vector-indexes.meta");
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut f = std::fs::File::open(&path)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;

    deserialize_index_metas_with_weights(&data)
}

// ── Binary read helpers ─────────────────────────────────────────────────

#[inline]
fn read_u8(data: &[u8], cursor: &mut usize) -> io::Result<u8> {
    if *cursor >= data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u8"));
    }
    let v = data[*cursor];
    *cursor += 1;
    Ok(v)
}

#[inline]
fn read_u16(data: &[u8], cursor: &mut usize) -> io::Result<u16> {
    if *cursor + 2 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u16"));
    }
    let v = u16::from_le_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2;
    Ok(v)
}

#[inline]
fn read_u32(data: &[u8], cursor: &mut usize) -> io::Result<u32> {
    if *cursor + 4 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "u32"));
    }
    let v = u32::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(v)
}

#[inline]
fn read_bytes<'a>(data: &'a [u8], cursor: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    if *cursor + len > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "bytes"));
    }
    let v = &data[*cursor..*cursor + len];
    *cursor += len;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(name: &str, dim: u32, prefix: &str, field: &str) -> IndexMeta {
        let padded = crate::vector::turbo_quant::encoder::padded_dimension(dim);
        IndexMeta {
            name: Bytes::from(name.to_owned()),
            dimension: dim,
            padded_dimension: padded,
            metric: DistanceMetric::L2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_runtime: 0,
            compact_threshold: 1000,
            source_field: Bytes::from(field.to_owned()),
            key_prefixes: vec![Bytes::from(prefix.to_owned())],
            quantization: QuantizationConfig::TurboQuant4,
            build_mode: BuildMode::Light,
            vector_fields: vec![VectorFieldMeta {
                field_name: Bytes::from(field.to_owned()),
                dimension: dim,
                padded_dimension: padded,
                metric: DistanceMetric::L2,
                quantization: QuantizationConfig::TurboQuant4,
                build_mode: BuildMode::Light,
            }],
            schema_fields: Vec::new(),
            merge_mode: crate::vector::segment::compaction::MergeMode::GraphUnion,
            keep_raw: false,
            db_index: 0,
            rerank_mult: 4,
            exact_beam: false,
        }
    }

    fn make_meta_for_db(
        name: &str,
        dim: u32,
        prefix: &str,
        field: &str,
        db_index: u8,
    ) -> IndexMeta {
        let mut meta = make_meta(name, dim, prefix, field);
        meta.db_index = db_index;
        meta
    }

    #[test]
    fn test_roundtrip_single() {
        let meta = make_meta("idx", 128, "doc:", "vec");
        let data = serialize_index_metas(&[&meta]);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "idx");
        assert_eq!(result[0].dimension, 128);
        assert_eq!(result[0].metric, DistanceMetric::L2);
        assert_eq!(result[0].hnsw_m, 16);
        assert_eq!(result[0].source_field, "vec");
        assert_eq!(result[0].key_prefixes.len(), 1);
        assert_eq!(result[0].key_prefixes[0], "doc:");
        assert_eq!(result[0].quantization, QuantizationConfig::TurboQuant4);
    }

    #[test]
    fn test_roundtrip_multiple() {
        let m1 = make_meta("idx1", 384, "v:", "emb");
        let m2 = make_meta("idx2", 768, "img:", "feat");
        let data = serialize_index_metas(&[&m1, &m2]);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "idx1");
        assert_eq!(result[0].dimension, 384);
        assert_eq!(result[1].name, "idx2");
        assert_eq!(result[1].dimension, 768);
        assert_eq!(result[1].key_prefixes[0], "img:");
    }

    #[test]
    fn test_roundtrip_empty() {
        let data = serialize_index_metas(&[]);
        let result = deserialize_index_metas(&data).unwrap();
        assert!(result.is_empty());
    }

    /// WS5a (db-scoped indexes): the sidecar round-trips `db_index` — via
    /// the current (v5) writer AND via an explicit v4 sidecar (migration).
    #[test]
    fn test_roundtrip_v4_db_index() {
        let m1 = make_meta_for_db("idx1", 128, "doc:", "vec", 0);
        let m2 = make_meta_for_db("idx2", 128, "doc:", "vec", 7);
        let data = serialize_index_metas(&[&m1, &m2]);
        // No payload schema declared: v5 in either mode (moon#1227 review F5).
        assert_eq!(data[4], VERSION_V5);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result[0].db_index, 0);
        assert_eq!(result[1].db_index, 7);

        // Explicit v4 data (written before the search-tuning knobs) still
        // carries db_index and loads knob defaults.
        let v4_data = serialize_index_metas_v4(&[(&m1, 1.0), (&m2, 1.0)]);
        assert_eq!(v4_data[4], VERSION_V4);
        let result = deserialize_index_metas(&v4_data).unwrap();
        assert_eq!(result[1].db_index, 7);
        assert_eq!(result[1].rerank_mult, 4);
        assert!(!result[1].exact_beam);
    }

    /// v5: search-tuning knobs round-trip; corrupt out-of-range mult clamps.
    #[test]
    fn test_v5_roundtrip_search_tuning_knobs() {
        let mut meta = make_meta("idx_v5", 128, "doc:", "vec");
        meta.rerank_mult = 16;
        meta.exact_beam = true;
        let data = serialize_index_metas_v5(&[(&meta, 1.0)]);
        assert_eq!(data[4], VERSION_V5);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result[0].rerank_mult, 16);
        assert!(result[0].exact_beam);

        // The v5 knob block is the last 8 bytes of a v5 buffer:
        // [rerank_mult: u32 LE][exact_beam: u8][reserved: 3B].
        let mut data = serialize_index_metas_v5(&[(&meta, 1.0)]);
        let n = data.len();
        data[n - 8..n - 4].copy_from_slice(&999u32.to_le_bytes());
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result[0].rerank_mult, 4, "out-of-range mult must clamp");
    }

    /// Pre-v5 sidecars carry no knob block — defaults apply.
    #[test]
    fn test_v1_reads_default_search_tuning_knobs() {
        let meta = make_meta("idx_v1", 128, "doc:", "vec");
        let data = serialize_index_metas_v1(&[&meta]);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result[0].rerank_mult, 4);
        assert!(!result[0].exact_beam);
    }

    /// WS5a: a v3 sidecar (written before db_index existed) loads every
    /// index as db 0 — pre-v0.6.0 sidecars migrate forward without operator
    /// action, matching their previous global (db-0-equivalent) visibility.
    #[test]
    fn test_v3_sidecar_defaults_to_db_zero() {
        let meta = make_meta("legacyidx", 128, "doc:", "vec");
        let v3_data = serialize_index_metas_v3(&[(&meta, 1.0)]);
        assert_eq!(v3_data[4], VERSION_V3);
        let result = deserialize_index_metas(&v3_data).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].db_index, 0);
    }

    fn payload_schema_meta() -> IndexMeta {
        let mut meta = make_meta("rag", 384, "doc:", "emb");
        meta.schema_fields = meta
            .vector_fields
            .iter()
            .cloned()
            .map(FieldType::Vector)
            .chain([
                FieldType::Text {
                    field_name: Bytes::from_static(b"content"),
                    weight: 2.5,
                    nostem: true,
                    sortable: false,
                    noindex: true,
                },
                FieldType::Tag {
                    field_name: Bytes::from_static(b"lang"),
                },
                FieldType::Numeric {
                    field_name: Bytes::from_static(b"year"),
                },
            ])
            .collect();
        meta
    }

    /// Declared TEXT/TAG/NUMERIC fields in FT.CREATE's shape.
    fn describe(schema: &[FieldType]) -> Vec<String> {
        schema
            .iter()
            .map(|f| match f {
                FieldType::Vector(v) => format!("vector {:?}", v.field_name),
                FieldType::Text {
                    field_name,
                    weight,
                    nostem,
                    sortable,
                    noindex,
                } => format!("text {field_name:?} {weight} {nostem} {sortable} {noindex}"),
                FieldType::Tag { field_name } => format!("tag {field_name:?}"),
                FieldType::Numeric { field_name } => format!("numeric {field_name:?}"),
            })
            .collect()
    }

    /// moon#1194: v6 persists the declared payload schema. Red before the
    /// bump: every sidecar reloaded with an empty `schema_fields`, so
    /// schema-aware payload indexing could not survive a restart.
    #[test]
    fn v6_roundtrips_the_declared_payload_schema() {
        let meta = payload_schema_meta();
        let data = serialize_index_metas_for_mode(&[(&meta, 1.0)], true);
        assert_eq!(data[4], VERSION_V6);
        let back = deserialize_index_metas(&data).unwrap();
        assert_eq!(
            describe(&back[0].schema_fields),
            describe(&meta.schema_fields)
        );
        // Truncating inside the schema block is an error, never a guess.
        for cut in [1usize, 3, 9] {
            assert!(
                deserialize_index_metas(&data[..data.len() - cut]).is_err(),
                "cut {cut}"
            );
        }
    }

    /// Backward compatibility: v1-v5 sidecars carry no payload schema and
    /// load with an empty `schema_fields`, exactly as before.
    #[test]
    fn pre_v6_sidecars_load_without_a_payload_schema() {
        let meta = payload_schema_meta();
        for data in [
            serialize_index_metas_v1(&[&meta]),
            serialize_index_metas_v3(&[(&meta, 1.0)]),
            serialize_index_metas_v4(&[(&meta, 1.0)]),
            serialize_index_metas_v5(&[(&meta, 1.0)]),
        ] {
            let back = deserialize_index_metas(&data).unwrap();
            assert!(back[0].schema_fields.is_empty(), "v{}", data[4]);
            assert_eq!(back[0].name, meta.name);
        }
    }

    /// The sidecar file keeps the FT.CONFIG search knobs. Red before the v6
    /// bump: `save_index_metadata_v3` wrote a v4 sidecar, so RERANK_MULT /
    /// EXACT_BEAM silently reset to 4 / OFF on every restart. In declared
    /// mode it keeps the payload schema too.
    #[test]
    fn sidecar_file_keeps_search_tuning_knobs_and_schema() {
        let tmp = tempfile::tempdir().unwrap();
        let mut meta = payload_schema_meta();
        meta.rerank_mult = 12;
        meta.exact_beam = true;
        save_index_metadata_for_mode(tmp.path(), &[(&meta, 1.0)], true).unwrap();
        let bytes = std::fs::read(tmp.path().join("vector-indexes.meta")).unwrap();
        assert_eq!(bytes[4], VERSION_V6);
        let loaded = load_index_metadata(tmp.path()).unwrap();
        assert_eq!(loaded[0].rerank_mult, 12);
        assert!(loaded[0].exact_beam);
        assert_eq!(
            describe(&loaded[0].schema_fields),
            describe(&meta.schema_fields)
        );
    }

    /// moon#1227 review F5: the sidecar the default mode writes must be one
    /// the PREVIOUS binary (which reads <= v5) can load. Writing v6 after any
    /// FT.CREATE / FT.DROPINDEX / FT.CONFIG made a rollback start with no
    /// vector indexes at all. v5 already carries RERANK_MULT / EXACT_BEAM, so
    /// the knobs still survive a restart; only the payload schema — which
    /// nothing reads unless `MOON_VECTOR_PAYLOAD_SCHEMA=declared` — is v6-only.
    #[test]
    fn default_mode_sidecar_is_v5_and_keeps_the_search_knobs() {
        let tmp = tempfile::tempdir().unwrap();
        let mut meta = payload_schema_meta();
        meta.rerank_mult = 12;
        meta.exact_beam = true;
        save_index_metadata_v3(tmp.path(), &[(&meta, 1.0)]).unwrap();
        let bytes = std::fs::read(tmp.path().join("vector-indexes.meta")).unwrap();
        let declared = crate::vector::filter::payload_schema::payload_schema_declared_mode();
        assert_eq!(
            bytes[4],
            if declared { VERSION_V6 } else { VERSION_V5 },
            "declared mode {declared}: the sidecar version a rollback must read"
        );
        let loaded = load_index_metadata(tmp.path()).unwrap();
        assert_eq!(loaded[0].rerank_mult, 12);
        assert!(loaded[0].exact_beam);
    }

    /// moon#1227 review F5: v6 is written only when it carries something —
    /// declared mode AND a declared payload field; otherwise the default
    /// writer's bytes are EXACTLY a v5 sidecar, which is what the previous
    /// release wrote and reads.
    #[test]
    fn v6_is_written_only_for_a_declared_payload_schema() {
        let plain = make_meta("plain", 128, "doc:", "vec");
        let rag = payload_schema_meta();
        for (declared, pairs, want) in [
            (false, vec![(&plain, 1.0f32)], VERSION_V5),
            (false, vec![(&plain, 1.0), (&rag, 1.0)], VERSION_V5),
            (true, vec![(&plain, 1.0)], VERSION_V5),
            (true, vec![(&plain, 1.0), (&rag, 1.0)], VERSION_V6),
        ] {
            let data = serialize_index_metas_for_mode(&pairs, declared);
            assert_eq!(
                data[4],
                want,
                "declared {declared}, {} indexes",
                pairs.len()
            );
            if want == VERSION_V5 {
                assert_eq!(
                    data,
                    serialize_index_metas_v5(&pairs),
                    "the default writer must emit a plain v5 sidecar"
                );
            }
            // The current reader loads either.
            let back = deserialize_index_metas_with_weights(&data).unwrap();
            assert_eq!(back.len(), pairs.len());
        }
    }

    #[test]
    fn test_save_load_file() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = make_meta("test_idx", 256, "key:", "vector");
        save_index_metadata(tmp.path(), &[&meta]).unwrap();

        let loaded = load_index_metadata(tmp.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "test_idx");
        assert_eq!(loaded[0].dimension, 256);
    }

    #[test]
    fn test_load_nonexistent() {
        let tmp = tempfile::tempdir().unwrap();
        let loaded = load_index_metadata(tmp.path()).unwrap();
        assert!(loaded.is_empty());
    }

    /// K3 regression guard: `save_index_metadata_v3` must go through the
    /// shared `atomic_write_durable` primitive (temp + fsync + rename +
    /// dir-fsync), not the pre-fix sequence that skipped the directory
    /// fsync. Directly observable in a unit test: only the final
    /// `vector-indexes.meta` file remains after a successful save -- no
    /// leftover `.vector-indexes.meta.tmp`.
    #[test]
    fn test_save_leaves_no_leftover_temp_file() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = make_meta("test_idx", 256, "key:", "vector");
        save_index_metadata(tmp.path(), &[&meta]).unwrap();

        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            entries,
            vec![std::ffi::OsString::from("vector-indexes.meta")]
        );
    }

    #[test]
    fn test_cosine_metric_roundtrip() {
        let mut meta = make_meta("cos_idx", 64, "e:", "emb");
        meta.metric = DistanceMetric::Cosine;
        meta.hnsw_ef_runtime = 500;
        meta.compact_threshold = 5000;
        meta.build_mode = BuildMode::Exact;
        let data = serialize_index_metas(&[&meta]);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result[0].metric, DistanceMetric::Cosine);
        assert_eq!(result[0].hnsw_ef_runtime, 500);
        assert_eq!(result[0].compact_threshold, 5000);
        assert_eq!(result[0].build_mode, BuildMode::Exact);
    }

    #[test]
    fn test_multiple_prefixes() {
        let mut meta = make_meta("multi", 128, "a:", "vec");
        meta.key_prefixes.push(Bytes::from_static(b"b:"));
        meta.key_prefixes.push(Bytes::from_static(b"c:"));
        let data = serialize_index_metas(&[&meta]);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result[0].key_prefixes.len(), 3);
        assert_eq!(result[0].key_prefixes[1], "b:");
        assert_eq!(result[0].key_prefixes[2], "c:");
    }

    #[test]
    fn test_serialize_deserialize_v2_single_field() {
        let meta = make_meta("idx", 128, "doc:", "vec");
        let data = serialize_index_metas(&[&meta]);
        // No payload schema: the current writer emits v5 (moon#1227 review F5).
        assert_eq!(data[4], VERSION_V5);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].vector_fields.len(), 1);
        assert_eq!(result[0].vector_fields[0].field_name, "vec");
        assert_eq!(result[0].vector_fields[0].dimension, 128);
        assert_eq!(result[0].vector_fields[0].metric, DistanceMetric::L2);
        assert_eq!(
            result[0].vector_fields[0].quantization,
            QuantizationConfig::TurboQuant4
        );
        assert_eq!(result[0].vector_fields[0].build_mode, BuildMode::Light);
    }

    #[test]
    fn test_serialize_deserialize_v2_multi_field() {
        let padded_128 = crate::vector::turbo_quant::encoder::padded_dimension(128);
        let padded_384 = crate::vector::turbo_quant::encoder::padded_dimension(384);
        let padded_768 = crate::vector::turbo_quant::encoder::padded_dimension(768);
        let mut meta = make_meta("multi_idx", 128, "doc:", "title_vec");
        meta.vector_fields = vec![
            VectorFieldMeta {
                field_name: Bytes::from_static(b"title_vec"),
                dimension: 128,
                padded_dimension: padded_128,
                metric: DistanceMetric::L2,
                quantization: QuantizationConfig::TurboQuant4,
                build_mode: BuildMode::Light,
            },
            VectorFieldMeta {
                field_name: Bytes::from_static(b"body_vec"),
                dimension: 384,
                padded_dimension: padded_384,
                metric: DistanceMetric::Cosine,
                quantization: QuantizationConfig::Sq8,
                build_mode: BuildMode::Exact,
            },
            VectorFieldMeta {
                field_name: Bytes::from_static(b"image_vec"),
                dimension: 768,
                padded_dimension: padded_768,
                metric: DistanceMetric::InnerProduct,
                quantization: QuantizationConfig::TurboQuant2,
                build_mode: BuildMode::Light,
            },
        ];

        let data = serialize_index_metas(&[&meta]);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].vector_fields.len(), 3);

        // Field 0: title_vec
        assert_eq!(result[0].vector_fields[0].field_name, "title_vec");
        assert_eq!(result[0].vector_fields[0].dimension, 128);
        assert_eq!(result[0].vector_fields[0].metric, DistanceMetric::L2);
        assert_eq!(
            result[0].vector_fields[0].quantization,
            QuantizationConfig::TurboQuant4
        );

        // Field 1: body_vec
        assert_eq!(result[0].vector_fields[1].field_name, "body_vec");
        assert_eq!(result[0].vector_fields[1].dimension, 384);
        assert_eq!(result[0].vector_fields[1].metric, DistanceMetric::Cosine);
        assert_eq!(
            result[0].vector_fields[1].quantization,
            QuantizationConfig::Sq8
        );
        assert_eq!(result[0].vector_fields[1].build_mode, BuildMode::Exact);

        // Field 2: image_vec
        assert_eq!(result[0].vector_fields[2].field_name, "image_vec");
        assert_eq!(result[0].vector_fields[2].dimension, 768);
        assert_eq!(
            result[0].vector_fields[2].metric,
            DistanceMetric::InnerProduct
        );
        assert_eq!(
            result[0].vector_fields[2].quantization,
            QuantizationConfig::TurboQuant2
        );
    }

    #[test]
    fn test_v1_migration() {
        // Serialize with v1 format
        let meta = make_meta("legacy", 256, "key:", "embedding");
        let v1_data = serialize_index_metas_v1(&[&meta]);
        assert_eq!(v1_data[4], VERSION_V1);

        // Deserialize with the unified deserializer -- should auto-migrate
        let result = deserialize_index_metas(&v1_data).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "legacy");
        assert_eq!(result[0].dimension, 256);
        assert_eq!(result[0].source_field, "embedding");

        // v1 migration should create a 1-element vector_fields
        assert_eq!(result[0].vector_fields.len(), 1);
        assert_eq!(result[0].vector_fields[0].field_name, "embedding");
        assert_eq!(result[0].vector_fields[0].dimension, 256);
        assert_eq!(result[0].vector_fields[0].metric, DistanceMetric::L2);
        assert_eq!(
            result[0].vector_fields[0].quantization,
            QuantizationConfig::TurboQuant4
        );
    }

    #[test]
    fn test_v2_preserves_v1_top_level() {
        let meta = make_meta("compat", 512, "p:", "vec_field");
        let data = serialize_index_metas(&[&meta]);
        let result = deserialize_index_metas(&data).unwrap();
        assert_eq!(result[0].dimension, 512);
        assert_eq!(result[0].source_field, "vec_field");
        assert_eq!(result[0].metric, DistanceMetric::L2);
        // Top-level fields match vector_fields[0]
        assert_eq!(
            result[0].vector_fields[0].field_name,
            result[0].source_field
        );
        assert_eq!(result[0].vector_fields[0].dimension, result[0].dimension);
        assert_eq!(result[0].vector_fields[0].metric, result[0].metric);
    }

    // ── W3-deep: v3 format tests ───────────────────────────────────────────

    #[test]
    fn test_v3_weight_roundtrip() {
        let meta = make_meta("hot_idx", 128, "doc:", "vec");
        let data = serialize_index_metas_v3(&[(&meta, 7.5)]);
        assert_eq!(data[4], VERSION_V3, "must write v3 version byte");

        let result = deserialize_index_metas_with_weights(&data).unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            (result[0].1 - 7.5f32).abs() < 1e-6,
            "weight must round-trip"
        );
    }

    #[test]
    fn test_v3_default_weight_from_v1() {
        let meta = make_meta("legacy", 64, "x:", "v");
        let v1_data = serialize_index_metas_v1(&[&meta]);
        let result = deserialize_index_metas_with_weights(&v1_data).unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            (result[0].1 - 1.0f32).abs() < 1e-6,
            "v1 files must load with default weight=1.0"
        );
    }

    #[test]
    fn test_v3_default_weight_from_v2_format() {
        // serialize_index_metas used to write v2; now writes v3 — but test
        // that weight=1.0 is the default in v3 output too.
        let meta = make_meta("v2compat", 128, "d:", "emb");
        let data = serialize_index_metas(&[&meta]); // delegates to v3 with weight=1.0
        let result = deserialize_index_metas_with_weights(&data).unwrap();
        assert!(
            (result[0].1 - 1.0f32).abs() < 1e-6,
            "default weight must be 1.0"
        );
    }

    #[test]
    fn test_v3_weight_zero_persists() {
        let meta = make_meta("disabled", 64, "d:", "v");
        let data = serialize_index_metas_v3(&[(&meta, 0.0)]);
        let result = deserialize_index_metas_with_weights(&data).unwrap();
        assert!(
            (result[0].1 - 0.0f32).abs() < 1e-9,
            "weight=0.0 must persist exactly"
        );
    }

    #[test]
    fn test_v3_weight_multiple_indexes() {
        let m1 = make_meta("idx1", 128, "a:", "v");
        let m2 = make_meta("idx2", 256, "b:", "v");
        let data = serialize_index_metas_v3(&[(&m1, 3.0), (&m2, 0.5)]);
        let result = deserialize_index_metas_with_weights(&data).unwrap();
        assert_eq!(result.len(), 2);
        assert!((result[0].1 - 3.0f32).abs() < 1e-6);
        assert!((result[1].1 - 0.5f32).abs() < 1e-6);
    }

    #[test]
    fn test_v3_save_load_file_with_weights() {
        let tmp = tempfile::tempdir().unwrap();
        let meta = make_meta("weighted_idx", 128, "doc:", "vec");
        save_index_metadata_v3(tmp.path(), &[(&meta, 42.0)]).unwrap();

        let loaded = load_index_metadata_with_weights(tmp.path()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0.name, "weighted_idx");
        assert!(
            (loaded[0].1 - 42.0f32).abs() < 1e-6,
            "weight must survive file round-trip"
        );
    }
}
