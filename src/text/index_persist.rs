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
//!
//! ## Format v2 (current — WS5a db-scoped indexes)
//!
//! Extends v1 with a single `db_index: u8` byte appended after the v1
//! per-index fields, mirroring `src/vector/index_persist.rs` format v4.
//! v1 sidecars (written before db scoping existed) are read with
//! `db_index = 0`.
//!
//! ```text
//! [magic: 4B "TMIX"] [version: 2] [count: u16] [reserved: 1B]
//! Per index:
//!   ... (same as v1 fields) ...
//!   [db_index: u8]   ← NEW in v2
//! ```

use std::io::{self, Read};
use std::path::Path;

use bytes::Bytes;

use crate::text::types::{BM25Config, TextFieldDef};

const MAGIC: &[u8; 4] = b"TMIX";
const VERSION_V1: u8 = 1;
const VERSION_V2: u8 = 2;
/// Default db_index used when reading v1 sidecars written before WS5a.
const DEFAULT_DB_INDEX_ON_LOAD: u8 = 0;

const FST_MAGIC: &[u8; 4] = b"TFST";
const FST_VERSION: u8 = 1;

/// Combined term-dict + FST sidecar (kernel M4, task #50).
///
/// Extends the FST-only sidecar with the term dictionary the FST's ids were
/// built against, so a loader can reconstruct BOTH pieces from the same
/// generation and never mix a stale-id-space FST with a freshly-rescanned
/// (differently-ordered) term dictionary -- see `TextStore::load_fst_sidecars`'s
/// doc comment in `src/text/store.rs` for the full corruption mechanism this
/// closes.
///
/// ```text
/// [magic: 4B "TFS2"] [version: 1B] [field_count: 2B]
/// Per field:
///   [next_id: 4B] [fst_high_water_mark: 4B] [term_count: 4B]
///   Per term: [term_len: 2B] [term: bytes] [term_id: 4B]
///   [fst_len: 4B] [fst_bytes: fst_len]   (fst_len=0 -> no FST for this field)
/// ```
const TERM_FST_MAGIC: &[u8; 4] = b"TFS2";
const TERM_FST_VERSION: u8 = 1;

/// One field's persisted term-dict + optional FST bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldTermFstSidecar {
    pub next_id: u32,
    pub fst_high_water_mark: u32,
    pub terms: Vec<(String, u32)>,
    pub fst_bytes: Option<Vec<u8>>,
}

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
    /// Logical db this index belongs to (WS5a). Defaults to `0` for v1
    /// sidecars and for callers not yet threading a real db_index.
    pub db_index: u8,
}

/// Serialize text index metadata to bytes (current v2 format — WS5a db_index).
pub fn serialize_text_index_metas(indexes: &[TextIndexMeta]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);

    buf.extend_from_slice(MAGIC);
    buf.push(VERSION_V2);
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

        // v2 extension: db_index (1 byte)
        buf.push(idx.db_index);
    }

    buf
}

/// Deserialize text index metadata from bytes. Handles v1 and v2 formats.
pub fn deserialize_text_index_metas(data: &[u8]) -> io::Result<Vec<TextIndexMeta>> {
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

        // v2 (WS5a): read db_index; v1 sidecars predate db scoping.
        let db_index = if version >= VERSION_V2 {
            read_u8(data, &mut cursor)?
        } else {
            DEFAULT_DB_INDEX_ON_LOAD
        };

        metas.push(TextIndexMeta {
            name,
            bm25_config,
            key_prefixes,
            text_fields,
            db_index,
        });
    }

    Ok(metas)
}

/// Write all active text index metadata to the sidecar file.
///
/// Atomically replaces the file via `atomic_write_durable` (K3: temp +
/// fsync + rename + dir-fsync). Before this fix the write skipped the
/// directory fsync -- the rename could complete but not survive a crash,
/// since `data=ordered`-journaled filesystems only make a rename durable
/// once its containing directory is itself fsynced.
pub fn save_text_index_metadata(shard_dir: &Path, indexes: &[TextIndexMeta]) -> io::Result<()> {
    let path = shard_dir.join("text-indexes.meta");
    let data = serialize_text_index_metas(indexes);
    crate::persistence::atomic::atomic_write_durable(&path, &data)?;
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

/// Persist per-field FST bytes to `{shard_dir}/{index_name}.fst`.
///
/// Format (TFST v1):
/// ```text
/// [magic: 4B "TFST"] [version: 1B] [field_count: 2B]
/// Per field:
///   [fst_len: 4B] [raw_fst_bytes: fst_len]
///   (fst_len=0 means no FST for this field)
/// ```
///
/// Atomic via `atomic_write_durable` (K3: temp + fsync + rename +
/// dir-fsync). Before this fix the write skipped the directory fsync, same
/// gap as `save_text_index_metadata` above.
pub fn save_fst_sidecar(
    shard_dir: &Path,
    index_name: &[u8],
    fst_bytes_per_field: &[Option<&[u8]>],
) -> io::Result<()> {
    let name_str = String::from_utf8_lossy(index_name);
    let path = shard_dir.join(format!("{name_str}.fst"));

    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(FST_MAGIC);
    buf.push(FST_VERSION);
    buf.extend_from_slice(&(fst_bytes_per_field.len() as u16).to_le_bytes());

    for field_fst in fst_bytes_per_field {
        match field_fst {
            Some(bytes) => {
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            None => {
                buf.extend_from_slice(&0u32.to_le_bytes()); // fst_len=0 = no FST for this field
            }
        }
    }

    crate::persistence::atomic::atomic_write_durable(&path, &buf)?;
    Ok(())
}

/// Load per-field FST bytes from `{shard_dir}/{index_name}.fst`.
///
/// Returns empty Vec if file not present (D-11: missing sidecar = fst_maps stay None).
/// Returns `Vec<Option<Vec<u8>>>` — one entry per field, None if that field had no FST.
pub fn load_fst_sidecar(shard_dir: &Path, index_name: &[u8]) -> io::Result<Vec<Option<Vec<u8>>>> {
    let name_str = String::from_utf8_lossy(index_name);
    let path = shard_dir.join(format!("{name_str}.fst"));
    if !path.exists() {
        return Ok(Vec::new()); // D-11: missing sidecar -> fst_map = None
    }

    let mut f = std::fs::File::open(&path)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;

    if data.len() < 7 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FST sidecar too short",
        ));
    }
    if &data[0..4] != FST_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad FST magic"));
    }
    let version = data[4];
    if version != FST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported FST version {version}"),
        ));
    }

    let field_count = u16::from_le_bytes([data[5], data[6]]) as usize;
    let mut cursor = 7;
    let mut result = Vec::with_capacity(field_count);

    for _ in 0..field_count {
        let fst_len = read_u32(&data, &mut cursor)? as usize;
        if fst_len == 0 {
            result.push(None);
        } else {
            let fst_bytes = read_bytes(&data, &mut cursor, fst_len)?.to_vec();
            result.push(Some(fst_bytes));
        }
    }

    Ok(result)
}

/// Serialize the combined term-dict + FST sidecar (pure function, fuzzable).
pub fn serialize_term_fst_sidecar(fields: &[FieldTermFstSidecar]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(TERM_FST_MAGIC);
    buf.push(TERM_FST_VERSION);
    buf.extend_from_slice(&(fields.len() as u16).to_le_bytes());

    for field in fields {
        buf.extend_from_slice(&field.next_id.to_le_bytes());
        buf.extend_from_slice(&field.fst_high_water_mark.to_le_bytes());
        buf.extend_from_slice(&(field.terms.len() as u32).to_le_bytes());
        for (term, id) in &field.terms {
            buf.extend_from_slice(&(term.len() as u16).to_le_bytes());
            buf.extend_from_slice(term.as_bytes());
            buf.extend_from_slice(&id.to_le_bytes());
        }
        match &field.fst_bytes {
            Some(bytes) => {
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            None => buf.extend_from_slice(&0u32.to_le_bytes()),
        }
    }
    buf
}

/// Deserialize the combined term-dict + FST sidecar (pure function, fuzzable).
///
/// Fails closed on ANY structural problem (bad magic/version, truncation,
/// non-UTF8 term bytes) by returning `Err` -- callers must treat an `Err`
/// exactly like a missing sidecar (full rescan), never partially apply the
/// result.
pub fn deserialize_term_fst_sidecar(data: &[u8]) -> io::Result<Vec<FieldTermFstSidecar>> {
    if data.len() < 7 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "term-fst sidecar too short",
        ));
    }
    if &data[0..4] != TERM_FST_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad term-fst magic",
        ));
    }
    let version = data[4];
    if version != TERM_FST_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported term-fst version {version}"),
        ));
    }
    let field_count = u16::from_le_bytes([data[5], data[6]]) as usize;
    let mut cursor = 7;
    let mut fields = Vec::with_capacity(field_count);

    for _ in 0..field_count {
        let next_id = read_u32(data, &mut cursor)?;
        let fst_high_water_mark = read_u32(data, &mut cursor)?;
        let term_count = read_u32(data, &mut cursor)? as usize;
        // Bound the pre-allocation against what is actually left to read. The
        // tightest legal term entry is 6 bytes (u16 length + a 1-byte term is
        // already 7, and a 0-length term is 2 + 4), so a count above
        // remaining/6 cannot be honest -- and honouring it meant asking the
        // allocator for ~101 GB from a small file, which is how the nightly
        // `term_fst_sidecar` fuzz target has failed every run since at least
        // 2026-08-26 (it still passed on 2026-08-11).
        // `read_bytes` already refuses to read past the end; what it cannot do
        // is stop the Vec from being reserved before the first term is read.
        const MIN_TERM_BYTES: usize = 6;
        if term_count > (data.len() - cursor) / MIN_TERM_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "term-fst sidecar term count exceeds remaining input",
            ));
        }
        let mut terms = Vec::with_capacity(term_count);
        for _ in 0..term_count {
            let term_len = read_u16(data, &mut cursor)? as usize;
            let term_bytes = read_bytes(data, &mut cursor, term_len)?;
            let term = std::str::from_utf8(term_bytes)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-utf8 term"))?
                .to_owned();
            let id = read_u32(data, &mut cursor)?;
            terms.push((term, id));
        }
        let fst_len = read_u32(data, &mut cursor)? as usize;
        let fst_bytes = if fst_len == 0 {
            None
        } else {
            Some(read_bytes(data, &mut cursor, fst_len)?.to_vec())
        };
        fields.push(FieldTermFstSidecar {
            next_id,
            fst_high_water_mark,
            terms,
            fst_bytes,
        });
    }

    Ok(fields)
}

/// Persist per-field term-dict + FST bytes to `{shard_dir}/{index_name}.tfst`.
///
/// Atomic via `atomic_write_durable` (K3: temp + fsync + rename + dir-fsync),
/// same primitive as every other text/vector sidecar writer.
pub fn save_term_fst_sidecar(
    shard_dir: &Path,
    index_name: &[u8],
    fields: &[FieldTermFstSidecar],
) -> io::Result<()> {
    let name_str = String::from_utf8_lossy(index_name);
    let path = shard_dir.join(format!("{name_str}.tfst"));
    let data = serialize_term_fst_sidecar(fields);
    crate::persistence::atomic::atomic_write_durable(&path, &data)?;
    Ok(())
}

/// Load per-field term-dict + FST bytes from `{shard_dir}/{index_name}.tfst`.
///
/// Returns `Ok(None)` if the file doesn't exist (D-11-style: no sidecar is
/// not an error, caller falls back to today's full-rescan behavior). Returns
/// `Err` on any structural corruption -- callers MUST treat that identically
/// to "missing" (fail closed), never load a partially-valid result.
pub fn load_term_fst_sidecar(
    shard_dir: &Path,
    index_name: &[u8],
) -> io::Result<Option<Vec<FieldTermFstSidecar>>> {
    let name_str = String::from_utf8_lossy(index_name);
    let path = shard_dir.join(format!("{name_str}.tfst"));
    if !path.exists() {
        return Ok(None);
    }
    let mut f = std::fs::File::open(&path)?;
    let mut data = Vec::new();
    f.read_to_end(&mut data)?;
    deserialize_term_fst_sidecar(&data).map(Some)
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
            db_index: 0,
        }
    }

    #[test]
    fn test_roundtrip_single() {
        let meta = make_meta("idx", "doc:", &[("title", 2.0, 0), ("body", 1.0, 0)]);
        let data = serialize_text_index_metas(std::slice::from_ref(&meta));
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

    /// WS5a (db-scoped indexes): v2 sidecar round-trips `db_index`.
    #[test]
    fn test_roundtrip_v2_db_index() {
        let mut m1 = make_meta("idx1", "doc:", &[("body", 1.0, 0)]);
        m1.db_index = 0;
        let mut m2 = make_meta("idx2", "doc:", &[("body", 1.0, 0)]);
        m2.db_index = 7;
        let data = serialize_text_index_metas(&[m1, m2]);
        assert_eq!(data[4], VERSION_V2);
        let result = deserialize_text_index_metas(&data).expect("deserialize");
        assert_eq!(result[0].db_index, 0);
        assert_eq!(result[1].db_index, 7);
    }

    /// WS5a: a v1 sidecar (written before db_index existed) loads every
    /// index as db 0.
    #[test]
    fn test_v1_sidecar_defaults_to_db_zero() {
        let meta = make_meta("legacyidx", "doc:", &[("body", 1.0, 0)]);
        // Hand-roll a v1 payload: same as v2 serializer output minus the
        // trailing db_index byte.
        let v2_data = serialize_text_index_metas(std::slice::from_ref(&meta));
        let mut v1_data = v2_data[..v2_data.len() - 1].to_vec();
        v1_data[4] = VERSION_V1;
        let result = deserialize_text_index_metas(&v1_data).expect("deserialize v1");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].db_index, 0);
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

    /// K3 regression guard: `save_text_index_metadata` must go through the
    /// shared `atomic_write_durable` primitive (temp + fsync + rename +
    /// dir-fsync), not the pre-fix write-temp+rename-with-no-dir-fsync
    /// sequence. Directly observable in a unit test: only the final
    /// `text-indexes.meta` file remains after a successful save -- no
    /// leftover `.text-indexes.meta.tmp`.
    #[test]
    fn test_save_leaves_no_leftover_temp_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let meta = make_meta("idx", "key:", &[("title", 1.0, 0)]);
        save_text_index_metadata(tmp.path(), &[meta]).expect("save");

        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("text-indexes.meta")]);
    }

    /// Same regression guard as above, for the FST sidecar writer.
    #[test]
    fn test_save_fst_sidecar_leaves_no_leftover_temp_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        save_fst_sidecar(tmp.path(), b"idx", &[Some(b"fake-fst-bytes")]).expect("save");

        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("idx.fst")]);
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
            db_index: 0,
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
        assert_eq!(data[4], VERSION_V2); // version (WS5a: now v2)
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
            db_index: 0,
        };

        let data = serialize_text_index_metas(&[meta]);
        let result = deserialize_text_index_metas(&data).expect("deserialize");
        assert_eq!(result[0].key_prefixes.len(), 3);
        assert_eq!(result[0].key_prefixes[0], "a:");
        assert_eq!(result[0].key_prefixes[1], "b:");
        assert_eq!(result[0].key_prefixes[2], "c:");
    }

    // ── Kernel M4 (task #50): combined term-dict + FST sidecar ───────────

    fn sample_fields() -> Vec<FieldTermFstSidecar> {
        vec![
            FieldTermFstSidecar {
                next_id: 3,
                fst_high_water_mark: 3,
                terms: vec![
                    ("alpha".to_owned(), 0),
                    ("beta".to_owned(), 1),
                    ("gamma".to_owned(), 2),
                ],
                fst_bytes: Some(b"fake_fst_bytes_field0".to_vec()),
            },
            FieldTermFstSidecar {
                next_id: 1,
                fst_high_water_mark: 0,
                terms: vec![("delta".to_owned(), 0)],
                fst_bytes: None,
            },
        ]
    }

    #[test]
    fn term_fst_sidecar_roundtrips() {
        let fields = sample_fields();
        let data = serialize_term_fst_sidecar(&fields);
        let decoded = deserialize_term_fst_sidecar(&data).expect("deserialize");
        assert_eq!(decoded, fields);
    }

    #[test]
    fn term_fst_sidecar_save_load_roundtrips_through_disk() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let fields = sample_fields();
        save_term_fst_sidecar(tmp.path(), b"idx", &fields).expect("save");
        let loaded = load_term_fst_sidecar(tmp.path(), b"idx")
            .expect("load")
            .expect("sidecar present");
        assert_eq!(loaded, fields);
    }

    #[test]
    fn term_fst_sidecar_missing_returns_none() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let loaded = load_term_fst_sidecar(tmp.path(), b"nonexistent").expect("load");
        assert!(loaded.is_none());
    }

    #[test]
    fn term_fst_sidecar_empty_fields_roundtrips() {
        let data = serialize_term_fst_sidecar(&[]);
        let decoded = deserialize_term_fst_sidecar(&data).expect("deserialize");
        assert!(decoded.is_empty());
    }

    #[test]
    fn term_fst_sidecar_bad_magic_rejected() {
        let mut data = serialize_term_fst_sidecar(&sample_fields());
        data[0] = b'X';
        assert!(deserialize_term_fst_sidecar(&data).is_err());
    }

    #[test]
    fn term_fst_sidecar_bad_version_rejected() {
        let mut data = serialize_term_fst_sidecar(&sample_fields());
        data[4] = 0xFF;
        assert!(deserialize_term_fst_sidecar(&data).is_err());
    }

    #[test]
    fn term_fst_sidecar_too_short_rejected() {
        let data = vec![0u8; 3];
        assert!(deserialize_term_fst_sidecar(&data).is_err());
    }

    #[test]
    fn term_fst_sidecar_truncated_term_bytes_rejected() {
        let data = serialize_term_fst_sidecar(&sample_fields());
        // Truncate mid-way through the term/FST payload -- any cut here
        // must fail closed (Err), never panic or return a partial Vec.
        let truncated = &data[..data.len() - 5];
        assert!(deserialize_term_fst_sidecar(truncated).is_err());
    }

    #[test]
    fn term_fst_sidecar_non_utf8_term_rejected() {
        let mut fields = sample_fields();
        // Overwrite the first field's term list with invalid UTF-8 bytes
        // encoded at the right length so the byte-level framing still
        // parses right up to the UTF-8 validation step.
        fields[0].terms.clear();
        let mut buf = Vec::new();
        buf.extend_from_slice(&TERM_FST_MAGIC[..]);
        buf.push(TERM_FST_VERSION);
        buf.extend_from_slice(&1u16.to_le_bytes()); // field_count = 1
        buf.extend_from_slice(&1u32.to_le_bytes()); // next_id
        buf.extend_from_slice(&0u32.to_le_bytes()); // fst_high_water_mark
        buf.extend_from_slice(&1u32.to_le_bytes()); // term_count
        buf.extend_from_slice(&2u16.to_le_bytes()); // term_len = 2
        buf.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        buf.extend_from_slice(&0u32.to_le_bytes()); // term_id
        buf.extend_from_slice(&0u32.to_le_bytes()); // fst_len = 0
        assert!(deserialize_term_fst_sidecar(&buf).is_err());
    }

    /// K3 regression guard (mirrors `test_save_leaves_no_leftover_temp_file`):
    /// the combined saver must go through `atomic_write_durable`, leaving no
    /// leftover `.tfst.tmp` file after a successful save.
    #[test]
    fn term_fst_sidecar_save_leaves_no_leftover_temp_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        save_term_fst_sidecar(tmp.path(), b"idx", &sample_fields()).expect("save");

        let entries: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries.len(), 1, "expected exactly one file: {entries:?}");
        assert_eq!(entries[0].to_string_lossy(), "idx.tfst");
    }

    /// A `term_count` read straight off disk is a full u32 and was handed to
    /// `Vec::with_capacity` unchecked. The nightly fuzzer found it: ASan
    /// reported `out of memory: allocator is trying to allocate 0x179f80e020
    /// bytes` (~101 GB), and `term_fst_sidecar` has failed every night since.
    /// A sidecar is attacker-reachable as a corrupt or truncated file, so the
    /// decoder must reject the count, not try to honour it.
    ///
    /// Asserts the error KIND, not a crash, because the crash is not portable:
    /// Linux under ASan aborts on the reservation, while macOS backs it with
    /// lazily-committed virtual pages, honours it, and only then hits EOF. So
    /// without the bound this returns `UnexpectedEof` (verified by removing
    /// it) and with the bound `InvalidData` -- exactly the difference between
    /// "refused" and "reserved ~101 GB, then noticed".
    #[test]
    fn a_huge_term_count_is_rejected_instead_of_preallocated() {
        let mut data = Vec::new();
        data.extend_from_slice(TERM_FST_MAGIC);
        data.push(TERM_FST_VERSION);
        data.extend_from_slice(&1u16.to_le_bytes()); // field_count = 1
        data.extend_from_slice(&0u32.to_le_bytes()); // next_id
        data.extend_from_slice(&0u32.to_le_bytes()); // fst_high_water_mark
        data.extend_from_slice(&u32::MAX.to_le_bytes()); // term_count — the bomb
        let err = deserialize_term_fst_sidecar(&data)
            .expect_err("a term_count of u32::MAX in a 19-byte sidecar must fail closed");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// The bound must not reject honest input: the tightest legal encoding is
    /// a 1-byte term, so a count that exactly fills the remaining bytes has to
    /// survive. A guard tuned one byte too strict would fail closed on real
    /// sidecars and silently force a full rescan on every load.
    #[test]
    fn a_densely_packed_but_honest_term_count_still_loads() {
        let mut data = Vec::new();
        data.extend_from_slice(TERM_FST_MAGIC);
        data.push(TERM_FST_VERSION);
        data.extend_from_slice(&1u16.to_le_bytes()); // field_count = 1
        data.extend_from_slice(&7u32.to_le_bytes()); // next_id
        data.extend_from_slice(&3u32.to_le_bytes()); // fst_high_water_mark
        data.extend_from_slice(&2u32.to_le_bytes()); // term_count = 2
        for (term, id) in [("a", 1u32), ("b", 2u32)] {
            data.extend_from_slice(&(term.len() as u16).to_le_bytes());
            data.extend_from_slice(term.as_bytes());
            data.extend_from_slice(&id.to_le_bytes());
        }
        data.extend_from_slice(&0u32.to_le_bytes()); // fst_len = 0
        let fields = deserialize_term_fst_sidecar(&data).expect("honest sidecar must load");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].next_id, 7);
        assert_eq!(
            fields[0].terms,
            vec![("a".to_string(), 1), ("b".to_string(), 2)]
        );
    }
}
