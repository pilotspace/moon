//! Persist text index metadata to a sidecar file.
//!
//! On FT.CREATE / FT.DROPINDEX with TEXT fields, all active text index
//! definitions are written to `{shard_dir}/text-indexes.meta`. On recovery,
//! this file is read so that HASH keys can be re-indexed into restored text
//! indexes.
//!
//! ## Format v1
//!
//! ```text
//! [magic: 4B "TMIX"] [version: 1] [count: u16] [reserved: 1B]
//! Per index:
//!   [name_len: u16] [name: bytes]
//!   [bm25_k1: f32] [bm25_b: f32]
//!   [prefix_count: u16] per prefix: [prefix_len: u16] [prefix: bytes]
//!   [field_count: u16] per field:
//!     [field_name_len: u16] [field_name: bytes]
//!     [weight: f64]
//!     [flags: u8] — bit 0 = nostem, bit 1 = sortable, bit 2 = noindex
//! ```

use std::io::{self, Read, Write};
use std::path::Path;

use bytes::Bytes;

use crate::text::types::{BM25Config, TextFieldDef};

const MAGIC: &[u8; 4] = b"TMIX";
const VERSION: u8 = 1;

/// Lightweight schema-only representation of a TextIndex for persistence.
///
/// Contains everything needed to reconstruct an empty TextIndex (without
/// runtime posting data). Document content is re-indexed from WAL replay.
#[derive(Debug, Clone)]
pub struct TextIndexMeta {
    pub name: Bytes,
    pub bm25_config: BM25Config,
    pub key_prefixes: Vec<Bytes>,
    pub text_fields: Vec<TextFieldDef>,
}

/// Serialize text index metadata to bytes.
pub fn serialize_text_index_metas(indexes: &[TextIndexMeta]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.extend_from_slice(&(indexes.len() as u16).to_le_bytes());
    buf.push(0); // reserved

    for idx in indexes {
        // name
        buf.extend_from_slice(&(idx.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(&idx.name);

        // BM25 config
        buf.extend_from_slice(&idx.bm25_config.k1.to_le_bytes());
        buf.extend_from_slice(&idx.bm25_config.b.to_le_bytes());

        // key_prefixes
        buf.extend_from_slice(&(idx.key_prefixes.len() as u16).to_le_bytes());
        for p in &idx.key_prefixes {
            buf.extend_from_slice(&(p.len() as u16).to_le_bytes());
            buf.extend_from_slice(p);
        }

        // text_fields
        buf.extend_from_slice(&(idx.text_fields.len() as u16).to_le_bytes());
        for f in &idx.text_fields {
            buf.extend_from_slice(&(f.field_name.len() as u16).to_le_bytes());
            buf.extend_from_slice(&f.field_name);
            buf.extend_from_slice(&f.weight.to_le_bytes());
            let flags: u8 = (f.nostem as u8) | ((f.sortable as u8) << 1) | ((f.noindex as u8) << 2);
            buf.push(flags);
        }
    }

    buf
}

/// Deserialize text index metadata from bytes.
pub fn deserialize_text_index_metas(data: &[u8]) -> io::Result<Vec<TextIndexMeta>> {
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
            format!("unsupported text index version {version}"),
        ));
    }
    let count = u16::from_le_bytes([data[5], data[6]]) as usize;
    let mut cursor = 8;
    let mut metas = Vec::with_capacity(count);

    for _ in 0..count {
        // name
        let name_len = read_u16(data, &mut cursor)? as usize;
        let name = Bytes::copy_from_slice(read_bytes(data, &mut cursor, name_len)?);

        // BM25 config
        let k1 = read_f32(data, &mut cursor)?;
        let b = read_f32(data, &mut cursor)?;
        let bm25_config = BM25Config { k1, b };

        // key_prefixes
        let prefix_count = read_u16(data, &mut cursor)? as usize;
        let mut key_prefixes = Vec::with_capacity(prefix_count);
        for _ in 0..prefix_count {
            let plen = read_u16(data, &mut cursor)? as usize;
            let prefix = Bytes::copy_from_slice(read_bytes(data, &mut cursor, plen)?);
            key_prefixes.push(prefix);
        }

        // text_fields
        let field_count = read_u16(data, &mut cursor)? as usize;
        let mut text_fields = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let fn_len = read_u16(data, &mut cursor)? as usize;
            let field_name = Bytes::copy_from_slice(read_bytes(data, &mut cursor, fn_len)?);
            let weight = read_f64(data, &mut cursor)?;
            let flags = read_u8(data, &mut cursor)?;
            text_fields.push(TextFieldDef {
                field_name,
                weight,
                nostem: flags & 0x01 != 0,
                sortable: flags & 0x02 != 0,
                noindex: flags & 0x04 != 0,
            });
        }

        metas.push(TextIndexMeta {
            name,
            bm25_config,
            key_prefixes,
            text_fields,
        });
    }

    Ok(metas)
}

/// Write all active text index metadata to the sidecar file.
///
/// Atomically replaces the file via write-to-temp + rename.
pub fn save_text_index_metadata(shard_dir: &Path, indexes: &[TextIndexMeta]) -> io::Result<()> {
    let path = shard_dir.join("text-indexes.meta");
    let tmp_path = shard_dir.join(".text-indexes.meta.tmp");

    let data = serialize_text_index_metas(indexes);

    let mut f = std::fs::File::create(&tmp_path)?;
    f.write_all(&data)?;
    f.sync_all()?;
    std::fs::rename(&tmp_path, &path)?;

    Ok(())
}

/// Load text index metadata from the sidecar file.
///
/// Returns empty vec if the file doesn't exist (fresh server).
pub fn load_text_index_metadata(shard_dir: &Path) -> io::Result<Vec<TextIndexMeta>> {
    let path = shard_dir.join("text-indexes.meta");
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut f = std::fs::File::open(&path)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;

    deserialize_text_index_metas(&data)
}

// -- Binary read helpers --------------------------------------------------

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
fn read_f32(data: &[u8], cursor: &mut usize) -> io::Result<f32> {
    if *cursor + 4 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "f32"));
    }
    let v = f32::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ]);
    *cursor += 4;
    Ok(v)
}

#[inline]
fn read_f64(data: &[u8], cursor: &mut usize) -> io::Result<f64> {
    if *cursor + 8 > data.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "f64"));
    }
    let v = f64::from_le_bytes([
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
        data[*cursor + 4],
        data[*cursor + 5],
        data[*cursor + 6],
        data[*cursor + 7],
    ]);
    *cursor += 8;
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

    fn make_meta(name: &str, prefix: &str, fields: &[(&str, f64, u8)]) -> TextIndexMeta {
        TextIndexMeta {
            name: Bytes::from(name.to_owned()),
            bm25_config: BM25Config::default(),
            key_prefixes: vec![Bytes::from(prefix.to_owned())],
            text_fields: fields
                .iter()
                .map(|(fname, weight, flags)| TextFieldDef {
                    field_name: Bytes::from(fname.to_string()),
                    weight: *weight,
                    nostem: flags & 0x01 != 0,
                    sortable: flags & 0x02 != 0,
                    noindex: flags & 0x04 != 0,
                })
                .collect(),
        }
    }

    #[test]
    fn test_roundtrip_single() {
        let meta = make_meta("idx", "doc:", &[("title", 2.0, 0), ("body", 1.0, 0)]);
        let data = serialize_text_index_metas(&[meta.clone()]);
        let result = deserialize_text_index_metas(&data).expect("deserialize");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "idx");
        assert_eq!(result[0].key_prefixes.len(), 1);
        assert_eq!(result[0].key_prefixes[0], "doc:");
        assert_eq!(result[0].text_fields.len(), 2);
        assert_eq!(result[0].text_fields[0].field_name, "title");
        assert!((result[0].text_fields[0].weight - 2.0).abs() < f64::EPSILON);
        assert_eq!(result[0].text_fields[1].field_name, "body");
        assert!((result[0].text_fields[1].weight - 1.0).abs() < f64::EPSILON);
        assert!((result[0].bm25_config.k1 - 1.2).abs() < f32::EPSILON);
        assert!((result[0].bm25_config.b - 0.75).abs() < f32::EPSILON);
    }

    #[test]
    fn test_roundtrip_multiple() {
        let m1 = make_meta("article_idx", "article:", &[("title", 2.0, 0)]);
        let m2 = make_meta(
            "blog_idx",
            "blog:",
            &[("content", 1.0, 0), ("tags", 0.5, 0)],
        );
        let data = serialize_text_index_metas(&[m1, m2]);
        let result = deserialize_text_index_metas(&data).expect("deserialize");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "article_idx");
        assert_eq!(result[1].name, "blog_idx");
        assert_eq!(result[1].text_fields.len(), 2);
    }

    #[test]
    fn test_roundtrip_empty() {
        let data = serialize_text_index_metas(&[]);
        let result = deserialize_text_index_metas(&data).expect("deserialize");
        assert!(result.is_empty());
    }

    #[test]
    fn test_save_load_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let meta = make_meta("test_idx", "key:", &[("title", 1.0, 0)]);
        save_text_index_metadata(tmp.path(), &[meta]).expect("save");

        let loaded = load_text_index_metadata(tmp.path()).expect("load");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].name, "test_idx");
        assert_eq!(loaded[0].key_prefixes[0], "key:");
    }

    #[test]
    fn test_load_nonexistent() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let loaded = load_text_index_metadata(tmp.path()).expect("load");
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_field_flags_roundtrip() {
        // nostem=true, sortable=false, noindex=false -> flags = 0x01
        // nostem=false, sortable=true, noindex=false -> flags = 0x02
        // nostem=true, sortable=true, noindex=true  -> flags = 0x07
        let meta = TextIndexMeta {
            name: Bytes::from_static(b"flags_idx"),
            bm25_config: BM25Config { k1: 1.5, b: 0.8 },
            key_prefixes: vec![Bytes::from_static(b"f:")],
            text_fields: vec![
                TextFieldDef {
                    field_name: Bytes::from_static(b"nostem_only"),
                    weight: 1.0,
                    nostem: true,
                    sortable: false,
                    noindex: false,
                },
                TextFieldDef {
                    field_name: Bytes::from_static(b"sortable_only"),
                    weight: 2.5,
                    nostem: false,
                    sortable: true,
                    noindex: false,
                },
                TextFieldDef {
                    field_name: Bytes::from_static(b"all_flags"),
                    weight: 0.5,
                    nostem: true,
                    sortable: true,
                    noindex: true,
                },
            ],
        };

        let data = serialize_text_index_metas(&[meta]);
        let result = deserialize_text_index_metas(&data).expect("deserialize");
        assert_eq!(result.len(), 1);

        let fields = &result[0].text_fields;
        assert_eq!(fields.len(), 3);

        // Field 0: nostem only
        assert!(fields[0].nostem);
        assert!(!fields[0].sortable);
        assert!(!fields[0].noindex);
        assert!((fields[0].weight - 1.0).abs() < f64::EPSILON);

        // Field 1: sortable only
        assert!(!fields[1].nostem);
        assert!(fields[1].sortable);
        assert!(!fields[1].noindex);
        assert!((fields[1].weight - 2.5).abs() < f64::EPSILON);

        // Field 2: all flags
        assert!(fields[2].nostem);
        assert!(fields[2].sortable);
        assert!(fields[2].noindex);
        assert!((fields[2].weight - 0.5).abs() < f64::EPSILON);

        // BM25 config roundtrip
        assert!((result[0].bm25_config.k1 - 1.5).abs() < f32::EPSILON);
        assert!((result[0].bm25_config.b - 0.8).abs() < f32::EPSILON);
    }

    #[test]
    fn test_magic_bytes() {
        let data = serialize_text_index_metas(&[]);
        assert_eq!(&data[0..4], b"TMIX");
        assert_eq!(data[4], 1); // version
    }

    #[test]
    fn test_bad_magic_rejected() {
        let mut data = serialize_text_index_metas(&[]);
        data[0] = b'X';
        assert!(deserialize_text_index_metas(&data).is_err());
    }

    #[test]
    fn test_too_short_rejected() {
        let data = vec![0u8; 4];
        assert!(deserialize_text_index_metas(&data).is_err());
    }

    #[test]
    fn test_multiple_prefixes() {
        let meta = TextIndexMeta {
            name: Bytes::from_static(b"multi"),
            bm25_config: BM25Config::default(),
            key_prefixes: vec![
                Bytes::from_static(b"a:"),
                Bytes::from_static(b"b:"),
                Bytes::from_static(b"c:"),
            ],
            text_fields: vec![TextFieldDef::new(Bytes::from_static(b"content"))],
        };

        let data = serialize_text_index_metas(&[meta]);
        let result = deserialize_text_index_metas(&data).expect("deserialize");
        assert_eq!(result[0].key_prefixes.len(), 3);
        assert_eq!(result[0].key_prefixes[0], "a:");
        assert_eq!(result[0].key_prefixes[1], "b:");
        assert_eq!(result[0].key_prefixes[2], "c:");
    }
}
