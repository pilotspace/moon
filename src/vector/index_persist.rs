//! Persist vector index metadata to a sidecar file.
//!
//! On FT.CREATE / FT.DROPINDEX, all active index definitions are written to
//! `{shard_dir}/vector-indexes.meta`. On recovery, this file is read before
//! snapshot load so that HASH keys can be auto-indexed as they are restored.
//!
//! Format: simple length-prefixed binary (no external dependencies).
//!
//! ```text
//! [magic: 4B "VMIX"] [version: u8] [count: u16] [reserved: 1B]
//! For each index:
//!   [name_len: u16] [name: bytes]
//!   [dim: u32] [metric: u8] [hnsw_m: u32] [ef_construction: u32] [ef_runtime: u32]
//!   [compact_threshold: u32] [quantization: u8] [build_mode: u8] [reserved: 2B]
//!   [source_field_len: u16] [source_field: bytes]
//!   [prefix_count: u16]
//!     [prefix_len: u16] [prefix: bytes] ...
//! ```

use std::io::{self, Read, Write};
use std::path::Path;

use bytes::Bytes;

use crate::vector::store::IndexMeta;
use crate::vector::turbo_quant::collection::{BuildMode, QuantizationConfig};
use crate::vector::types::DistanceMetric;

const MAGIC: &[u8; 4] = b"VMIX";
const VERSION: u8 = 1;

/// Serialize a list of IndexMeta to bytes.
pub fn serialize_index_metas(metas: &[&IndexMeta]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&(metas.len() as u16).to_le_bytes());
    buf.push(0); // reserved

    for m in metas {
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

    buf
}

/// Deserialize IndexMeta list from bytes.
pub fn deserialize_index_metas(data: &[u8]) -> io::Result<Vec<IndexMeta>> {
    if data.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "too short"));
    }
    if &data[0..4] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }
    let version = data[4];
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported version {version}"),
        ));
    }
    let count = u16::from_le_bytes([data[5], data[6]]) as usize;
    let mut cursor = 8;
    let mut metas = Vec::with_capacity(count);

    for _ in 0..count {
        // name
        let name_len = read_u16(data, &mut cursor)? as usize;
        let name = Bytes::copy_from_slice(read_bytes(data, &mut cursor, name_len)?);

        // fixed fields
        let dimension = read_u32(data, &mut cursor)?;
        let metric_u8 = read_u8(data, &mut cursor)?;
        let hnsw_m = read_u32(data, &mut cursor)?;
        let hnsw_ef_construction = read_u32(data, &mut cursor)?;
        let hnsw_ef_runtime = read_u32(data, &mut cursor)?;
        let compact_threshold = read_u32(data, &mut cursor)?;
        let quant_u8 = read_u8(data, &mut cursor)?;
        let build_u8 = read_u8(data, &mut cursor)?;
        cursor += 2; // reserved

        // source_field
        let sf_len = read_u16(data, &mut cursor)? as usize;
        let source_field = Bytes::copy_from_slice(read_bytes(data, &mut cursor, sf_len)?);

        // key_prefixes
        let prefix_count = read_u16(data, &mut cursor)? as usize;
        let mut key_prefixes = Vec::with_capacity(prefix_count);
        for _ in 0..prefix_count {
            let plen = read_u16(data, &mut cursor)? as usize;
            let prefix = Bytes::copy_from_slice(read_bytes(data, &mut cursor, plen)?);
            key_prefixes.push(prefix);
        }

        let metric = match metric_u8 {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => DistanceMetric::L2,
        };
        let quantization = QuantizationConfig::from_u8(quant_u8);
        let build_mode = if build_u8 == 1 {
            BuildMode::Exact
        } else {
            BuildMode::Light
        };
        let padded_dimension = crate::vector::turbo_quant::encoder::padded_dimension(dimension);

        metas.push(IndexMeta {
            name,
            dimension,
            padded_dimension,
            metric,
            hnsw_m,
            hnsw_ef_construction,
            hnsw_ef_runtime,
            compact_threshold,
            source_field,
            key_prefixes,
            quantization,
            build_mode,
        });
    }

    Ok(metas)
}

/// Write all active index metadata to the sidecar file.
///
/// Called after FT.CREATE and FT.DROPINDEX. Atomically replaces the file
/// via write-to-temp + rename.
pub fn save_index_metadata(
    shard_dir: &Path,
    metas: &[&IndexMeta],
) -> io::Result<()> {
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
        IndexMeta {
            name: Bytes::from(name.to_owned()),
            dimension: dim,
            padded_dimension: crate::vector::turbo_quant::encoder::padded_dimension(dim),
            metric: DistanceMetric::L2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_runtime: 0,
            compact_threshold: 1000,
            source_field: Bytes::from(field.to_owned()),
            key_prefixes: vec![Bytes::from(prefix.to_owned())],
            quantization: QuantizationConfig::TurboQuant4,
            build_mode: BuildMode::Light,
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
}
