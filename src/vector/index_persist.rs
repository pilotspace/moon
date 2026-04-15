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
//! ## Format v2 (current)
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

use std::io::{self, Read, Write};
use std::path::Path;

use bytes::Bytes;

use crate::vector::store::{IndexMeta, VectorFieldMeta};
use crate::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
use crate::vector::types::DistanceMetric;

const MAGIC: &[u8; 4] = b"VMIX";
const VERSION_V1: u8 = 1;
const VERSION_V2: u8 = 2;

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
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION_V2);
    buf.extend_from_slice(&(metas.len() as u16).to_le_bytes());
    buf.push(0); // reserved

    for m in metas {
        // Write v1-compatible top-level fields
        write_v1_per_index(&mut buf, m);

        // Write v2 vector_fields extension
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
    }

    buf
}

/// Write the v1 per-index fields (shared between v1 and v2 serializers).
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

/// Deserialize IndexMeta list from bytes. Handles both v1 and v2 formats.
///
/// v1 data is auto-migrated: the single source_field is wrapped into a
/// 1-element `vector_fields` Vec. v2 data reads the full field array.
pub fn deserialize_index_metas(data: &[u8]) -> io::Result<Vec<IndexMeta>> {
    if data.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "too short"));
    }
    if &data[0..4] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }
    let version = data[4];
    if version != VERSION_V1 && version != VERSION_V2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported version {version}"),
        ));
    }
    let count = u16::from_le_bytes([data[5], data[6]]) as usize;
    let mut cursor = 8;
    let mut metas = Vec::with_capacity(count);

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

        let vector_fields = if version == VERSION_V2 {
            // Read v2 vector_fields extension
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

        metas.push(IndexMeta {
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
        });
    }

    Ok(metas)
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

/// Write all active index metadata to the sidecar file.
///
/// Called after FT.CREATE and FT.DROPINDEX. Atomically replaces the file
/// via write-to-temp + rename.
pub fn save_index_metadata(shard_dir: &Path, metas: &[&IndexMeta]) -> io::Result<()> {
    let path = shard_dir.join("vector-indexes.meta");
    let tmp_path = shard_dir.join(".vector-indexes.meta.tmp");

    let data = serialize_index_metas(metas);

    let mut f = std::fs::File::create(&tmp_path)?;
    f.write_all(&data)?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, &path)?;

    Ok(())
}

/// Load index metadata from the sidecar file.
///
/// Returns empty vec if the file doesn't exist (fresh server).
pub fn load_index_metadata(shard_dir: &Path) -> io::Result<Vec<IndexMeta>> {
    let path = shard_dir.join("vector-indexes.meta");
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut f = std::fs::File::open(&path)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;

    deserialize_index_metas(&data)
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
        }
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
        // Verify v2 version byte
        assert_eq!(data[4], VERSION_V2);
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
}
