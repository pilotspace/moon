//! `.tpost` — the durable form of a text index (postings, term dicts, FSTs,
//! doc maps, TAG/NUMERIC entries, per-doc content checksums).
//!
//! Design, validity contract and fallback triggers:
//! `docs/internal/text-postings-persistence.md`. In one line: the file is a
//! *cache* of what a rebuild would produce, stamped so a boot can prove per
//! document whether the cached postings still describe the live hash.
//!
//! Layout (all little-endian):
//!
//! ```text
//! header  [magic "TPS1"] [version u8] [flags u8] [reserved u16]
//!         [schema_hash u64] [payload_len u64]
//! payload [name_len u32] [name] [db_index u8] [next_doc_id u32] [text_field_count u16]
//!         [doc_count u32] per doc:
//!             [doc_id u32] [key_len u32] [key] [content_checksum u64] [insert_lsn u64]
//!             [field_lengths u32 × text_field_count]
//!         per text field:
//!             [next_id u32] [fst_high_water_mark u32]
//!             [term_count u32] per term: [term_len u32] [term] [term_id u32]
//!             [fst_len u32] [fst bytes]              (fst_len = 0 -> no FST)
//!             [posting_count u32] per posting:
//!                 [term_id u32] [has_positions u8] [n u32]
//!                 [doc_ids u32 × n] [term_freqs u32 × n]
//!                 if has_positions: per doc [count u32] [positions u32 × count]
//!         [tag_doc_count u32] per doc: [doc_id u32] [n u16] per entry:
//!             [field_len u32] [field] [value_len u32] [value]
//!         [numeric_doc_count u32] per doc: [doc_id u32] [n u16] per entry:
//!             [field_len u32] [field] [value f64]
//! trailer [xxh64 over header + payload]
//! ```
//!
//! Every byte string carries a u32 length: a Redis key can be far longer
//! than 64 KiB and a truncated key would install a WRONG key on load.
//!
//! The decoder is the trust boundary: it validates framing, the trailer
//! checksum and every structural invariant the store relies on, never
//! pre-allocates on a count the remaining bytes cannot back, and returns a
//! plain struct — nothing is installed into a live index until the whole
//! file validated (`TextIndex::install_recovered`). Fuzzed by
//! `fuzz/fuzz_targets/text_postings_file.rs`.

use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use thiserror::Error;

use crate::text::posting::PostingList;
use crate::text::store::TextIndex;

pub const MAGIC: &[u8; 4] = b"TPS1";
pub const VERSION: u8 = 1;
const HEADER_LEN: usize = 4 + 1 + 1 + 2 + 8 + 8;
const TRAILER_LEN: usize = 8;
/// Refuse files whose header claims more than this; the decoder never
/// allocates on the claim, but the read itself should not be unbounded.
pub const MAX_FILE_LEN: u64 = 16 << 30;

/// Why a `.tpost` file was refused. Every variant means "rebuild this
/// index" — the caller logs it and takes the pre-existing rescan path.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PostingsDecodeError {
    #[error("file too short ({0} bytes)")]
    TooShort(usize),
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported postings file version {0}")]
    UnsupportedVersion(u8),
    #[error("payload length mismatch: header says {declared}, file has {actual}")]
    LengthMismatch { declared: u64, actual: u64 },
    #[error("trailer checksum mismatch")]
    ChecksumMismatch,
    #[error("truncated while reading {0}")]
    Truncated(&'static str),
    #[error("invalid content: {0}")]
    Invalid(&'static str),
}

/// One indexed document as persisted.
#[derive(Debug, Clone, PartialEq)]
pub struct PersistedDoc {
    pub doc_id: u32,
    pub key: Bytes,
    pub content_checksum: u64,
    pub insert_lsn: u64,
    pub field_lengths: Vec<u32>,
}

/// One text field's dictionary, FST and postings.
#[derive(Debug)]
pub struct PersistedField {
    pub next_id: u32,
    pub fst_high_water_mark: u32,
    pub terms: Vec<(String, u32)>,
    pub fst_bytes: Option<Vec<u8>>,
    /// `(term_id, list)` — `term_id` is unique per field and names a term
    /// of `terms`; the list is non-empty with strictly increasing doc ids
    /// that all name a document of `PersistedTextIndex::docs`.
    pub postings: Vec<(u32, PostingList)>,
}

/// A fully validated `.tpost` file, ready for `TextIndex::install_recovered`.
#[derive(Debug)]
pub struct PersistedTextIndex {
    pub schema_hash: u64,
    pub name: Bytes,
    pub db_index: u8,
    pub next_doc_id: u32,
    pub docs: Vec<PersistedDoc>,
    pub fields: Vec<PersistedField>,
    pub tag_docs: Vec<(u32, Vec<(Bytes, Bytes)>)>,
    pub numeric_docs: Vec<(u32, Vec<(Bytes, f64)>)>,
}

/// The trailer stamp over `body` (header + payload). Public so the fuzz
/// target can re-stamp a mutated body and reach the structural checks.
#[must_use]
pub fn trailer_checksum(body: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(body, 0)
}

/// `{dir}/{xxh64(name):016x}.tpost` — hex like the vector plane's
/// `idx-*` dirs, so an index name can never escape the directory.
pub fn postings_file_path(dir: &Path, index_name: &[u8]) -> PathBuf {
    dir.join(format!(
        "{:016x}.tpost",
        xxhash_rust::xxh64::xxh64(index_name, 0)
    ))
}

/// Read a `.tpost` file. `Ok(None)` = no file (the ordinary state for an
/// index that was never flushed).
pub fn read_postings_file(dir: &Path, index_name: &[u8]) -> io::Result<Option<Vec<u8>>> {
    let path = postings_file_path(dir, index_name);
    match std::fs::metadata(&path) {
        Ok(m) if m.len() > MAX_FILE_LEN => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} is {} bytes, over the {} limit",
                    path.display(),
                    m.len(),
                    MAX_FILE_LEN
                ),
            ));
        }
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    }
    match std::fs::read(&path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

// ───────────────────────────── encode ─────────────────────────────

struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    #[inline]
    fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }
    #[inline]
    fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }
    #[inline]
    fn bytes32(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.buf.extend_from_slice(b);
    }
}

/// Serialize a live index. Pure CPU, no syscalls — this is the part that
/// runs on the shard thread; the bytes go to `text::persist_writer`.
///
/// Keys, term ids and postings are written in sorted order so the same
/// index state always produces the same bytes (tests diff them).
#[must_use]
pub fn encode_index(idx: &TextIndex) -> Vec<u8> {
    let mut w = Writer {
        buf: Vec::with_capacity(4096 + idx.resident_bytes() / 2),
    };
    w.buf.extend_from_slice(MAGIC);
    w.u8(VERSION);
    w.u8(0); // flags
    w.u16(0); // reserved
    w.u64(idx.schema_hash());
    let payload_len_at = w.buf.len();
    w.u64(0);
    let payload_start = w.buf.len();

    w.bytes32(&idx.name);
    w.u8(idx.db_index);
    w.u32(idx.next_doc_id());
    let field_count = idx.text_fields.len();
    w.u16(field_count as u16);

    // Docs, ascending doc_id.
    let mut doc_ids: Vec<u32> = idx.doc_id_to_key.keys().copied().collect();
    doc_ids.sort_unstable();
    w.u32(doc_ids.len() as u32);
    for &doc_id in &doc_ids {
        w.u32(doc_id);
        let key = idx
            .doc_id_to_key
            .get(&doc_id)
            .map(|k| k.as_ref())
            .unwrap_or(&[]);
        w.bytes32(key);
        w.u64(
            idx.doc_id_to_content_checksum
                .get(&doc_id)
                .copied()
                .unwrap_or(0),
        );
        w.u64(idx.doc_id_to_insert_lsn.get(&doc_id).copied().unwrap_or(0));
        for f in 0..field_count {
            let len = idx
                .doc_field_lengths
                .get(&doc_id)
                .and_then(|l| l.get(f).copied())
                .unwrap_or(0);
            w.u32(len);
        }
    }

    // Fields.
    for f in 0..field_count {
        let dict = &idx.field_term_dicts[f];
        w.u32(dict.next_id());
        w.u32(dict.fst_high_water_mark);
        let mut terms: Vec<(&str, u32)> = dict.iter().map(|(t, &id)| (t, id)).collect();
        terms.sort_unstable_by_key(|&(_, id)| id);
        w.u32(terms.len() as u32);
        for (term, id) in terms {
            w.bytes32(term.as_bytes());
            w.u32(id);
        }
        #[cfg(feature = "text-index")]
        let fst_bytes: Option<&[u8]> = idx
            .fst_maps
            .get(f)
            .and_then(|m| m.as_ref())
            .map(|m| m.as_fst().as_bytes());
        #[cfg(not(feature = "text-index"))]
        let fst_bytes: Option<&[u8]> = None;
        match fst_bytes {
            Some(b) => {
                w.u32(b.len() as u32);
                w.buf.extend_from_slice(b);
            }
            None => w.u32(0),
        }
        // Empty lists (every doc of a term removed) are legal live state but
        // a rebuild never produces them: skip, so loaded == rebuilt.
        let mut postings: Vec<(u32, &PostingList)> = idx.field_postings[f]
            .iter()
            .filter(|(_, p)| !p.doc_ids.is_empty())
            .collect();
        postings.sort_unstable_by_key(|&(t, _)| t);
        w.u32(postings.len() as u32);
        for (term_id, list) in postings {
            w.u32(term_id);
            let has_positions = list.positions.is_some();
            w.u8(has_positions as u8);
            w.u32(list.doc_ids.len() as u32);
            for d in &list.doc_ids {
                w.u32(d);
            }
            for &tf in &list.term_freqs {
                w.u32(tf);
            }
            if let Some(pos) = &list.positions {
                for p in pos {
                    w.u32(p.len() as u32);
                    for &v in p {
                        w.u32(v);
                    }
                }
            }
        }
    }

    // TAG / NUMERIC per-doc entries, ascending doc_id.
    #[cfg(feature = "text-index")]
    {
        let mut tag_docs: Vec<(&u32, &smallvec::SmallVec<[(Bytes, Bytes); 8]>)> =
            idx.doc_tag_entries.iter().collect();
        tag_docs.sort_unstable_by_key(|(d, _)| **d);
        w.u32(tag_docs.len() as u32);
        for (doc_id, entries) in tag_docs {
            w.u32(*doc_id);
            w.u16(entries.len() as u16);
            for (field, value) in entries.iter() {
                w.bytes32(field);
                w.bytes32(value);
            }
        }
        let mut num_docs: Vec<(
            &u32,
            &smallvec::SmallVec<[(Bytes, ordered_float::OrderedFloat<f64>); 4]>,
        )> = idx.doc_numeric_entries.iter().collect();
        num_docs.sort_unstable_by_key(|(d, _)| **d);
        w.u32(num_docs.len() as u32);
        for (doc_id, entries) in num_docs {
            w.u32(*doc_id);
            w.u16(entries.len() as u16);
            for (field, value) in entries.iter() {
                w.bytes32(field);
                w.u64(value.0.to_bits());
            }
        }
    }
    #[cfg(not(feature = "text-index"))]
    {
        w.u32(0);
        w.u32(0);
    }

    let payload_len = (w.buf.len() - payload_start) as u64;
    w.buf[payload_len_at..payload_len_at + 8].copy_from_slice(&payload_len.to_le_bytes());
    let checksum = trailer_checksum(&w.buf);
    w.u64(checksum);
    w.buf
}

// ───────────────────────────── decode ─────────────────────────────

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    #[inline]
    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }
    #[inline]
    fn take(&mut self, n: usize, what: &'static str) -> Result<&'a [u8], PostingsDecodeError> {
        if self.remaining() < n {
            return Err(PostingsDecodeError::Truncated(what));
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    #[inline]
    fn u8(&mut self, what: &'static str) -> Result<u8, PostingsDecodeError> {
        Ok(self.take(1, what)?[0])
    }
    #[inline]
    fn u16(&mut self, what: &'static str) -> Result<u16, PostingsDecodeError> {
        let b = self.take(2, what)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    #[inline]
    fn u32(&mut self, what: &'static str) -> Result<u32, PostingsDecodeError> {
        let b = self.take(4, what)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    #[inline]
    fn u64(&mut self, what: &'static str) -> Result<u64, PostingsDecodeError> {
        let b = self.take(8, what)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }
    #[inline]
    fn bytes32(&mut self, what: &'static str) -> Result<&'a [u8], PostingsDecodeError> {
        let len = self.u32(what)? as usize;
        self.take(len, what)
    }
    /// A count is only trusted if the remaining bytes can hold `count`
    /// items of at least `min_item_len` bytes each — so `Vec::with_capacity`
    /// below is always bounded by the file size, never by the claim.
    #[inline]
    fn count(
        &mut self,
        min_item_len: usize,
        what: &'static str,
    ) -> Result<usize, PostingsDecodeError> {
        let n = self.u32(what)? as usize;
        if n.checked_mul(min_item_len)
            .is_none_or(|need| need > self.remaining())
        {
            return Err(PostingsDecodeError::Invalid(what));
        }
        Ok(n)
    }
    fn u32_array(&mut self, n: usize, what: &'static str) -> Result<Vec<u32>, PostingsDecodeError> {
        let raw = self.take(
            n.checked_mul(4).ok_or(PostingsDecodeError::Invalid(what))?,
            what,
        )?;
        Ok(raw
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect())
    }
}

/// Decode and validate a whole `.tpost` file. See the module docs for what
/// "validate" covers; `Ok` means every invariant `install_recovered` relies
/// on holds, and the only remaining checks are against the live schema.
pub fn decode(data: &[u8]) -> Result<PersistedTextIndex, PostingsDecodeError> {
    if data.len() < HEADER_LEN + TRAILER_LEN {
        return Err(PostingsDecodeError::TooShort(data.len()));
    }
    if &data[..4] != MAGIC {
        return Err(PostingsDecodeError::BadMagic);
    }
    if data[4] != VERSION {
        return Err(PostingsDecodeError::UnsupportedVersion(data[4]));
    }
    let mut r = Reader { data, pos: 5 };
    let _flags = r.u8("flags")?;
    let _reserved = r.u16("reserved")?;
    let schema_hash = r.u64("schema_hash")?;
    let declared = r.u64("payload_len")?;
    let actual = (data.len() - HEADER_LEN - TRAILER_LEN) as u64;
    if declared != actual {
        return Err(PostingsDecodeError::LengthMismatch { declared, actual });
    }
    // Trailer first: a torn or bit-rotted file must fail here, before any
    // structural check could be fooled into a partial read.
    let body = &data[..data.len() - TRAILER_LEN];
    let stored = u64::from_le_bytes(
        data[data.len() - TRAILER_LEN..]
            .try_into()
            .map_err(|_| PostingsDecodeError::TooShort(data.len()))?,
    );
    if trailer_checksum(body) != stored {
        return Err(PostingsDecodeError::ChecksumMismatch);
    }
    r.data = body;

    let name = Bytes::copy_from_slice(r.bytes32("name")?);
    let db_index = r.u8("db_index")?;
    let next_doc_id = r.u32("next_doc_id")?;
    let field_count = r.u16("text_field_count")? as usize;

    // Docs.
    let doc_count = r.count(4 + 4 + 8 + 8 + 4 * field_count, "doc_count")?;
    let mut docs = Vec::with_capacity(doc_count);
    let mut seen_docs: HashSet<u32> = HashSet::with_capacity(doc_count);
    let mut seen_keys: HashSet<&[u8]> = HashSet::with_capacity(doc_count);
    for _ in 0..doc_count {
        let doc_id = r.u32("doc_id")?;
        if doc_id >= next_doc_id || !seen_docs.insert(doc_id) {
            return Err(PostingsDecodeError::Invalid(
                "doc_id out of range or duplicate",
            ));
        }
        let key = r.bytes32("doc key")?;
        if key.is_empty() || !seen_keys.insert(key) {
            return Err(PostingsDecodeError::Invalid("empty or duplicate doc key"));
        }
        let content_checksum = r.u64("content_checksum")?;
        let insert_lsn = r.u64("insert_lsn")?;
        let field_lengths = r.u32_array(field_count, "field_lengths")?;
        docs.push(PersistedDoc {
            doc_id,
            key: Bytes::copy_from_slice(key),
            content_checksum,
            insert_lsn,
            field_lengths,
        });
    }

    // Fields.
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        let next_id = r.u32("next_id")?;
        let fst_high_water_mark = r.u32("fst_high_water_mark")?;
        if fst_high_water_mark > next_id {
            return Err(PostingsDecodeError::Invalid(
                "fst_high_water_mark > next_id",
            ));
        }
        let term_count = r.count(4 + 4, "term_count")?;
        let mut terms = Vec::with_capacity(term_count);
        let mut seen_ids: HashSet<u32> = HashSet::with_capacity(term_count);
        let mut seen_terms: HashSet<&[u8]> = HashSet::with_capacity(term_count);
        for _ in 0..term_count {
            let raw = r.bytes32("term")?;
            let term = std::str::from_utf8(raw)
                .map_err(|_| PostingsDecodeError::Invalid("term is not UTF-8"))?;
            let id = r.u32("term_id")?;
            if id >= next_id || !seen_ids.insert(id) || !seen_terms.insert(raw) {
                return Err(PostingsDecodeError::Invalid(
                    "term_id out of range, duplicate id or duplicate term",
                ));
            }
            terms.push((term.to_owned(), id));
        }
        let fst_len = r.u32("fst_len")? as usize;
        let fst_bytes = if fst_len == 0 {
            None
        } else {
            Some(r.take(fst_len, "fst bytes")?.to_vec())
        };
        let posting_count = r.count(4 + 1 + 4 + 8, "posting_count")?;
        let mut postings = Vec::with_capacity(posting_count);
        let mut seen_posting_terms: HashSet<u32> = HashSet::with_capacity(posting_count);
        for _ in 0..posting_count {
            let term_id = r.u32("posting term_id")?;
            if !seen_ids.contains(&term_id) || !seen_posting_terms.insert(term_id) {
                return Err(PostingsDecodeError::Invalid(
                    "posting names an unknown term or repeats one",
                ));
            }
            let has_positions = match r.u8("has_positions")? {
                0 => false,
                1 => true,
                _ => return Err(PostingsDecodeError::Invalid("has_positions flag")),
            };
            let n = r.count(8, "posting doc count")?;
            if n == 0 {
                return Err(PostingsDecodeError::Invalid("empty posting list"));
            }
            let doc_ids = r.u32_array(n, "posting doc_ids")?;
            if doc_ids.iter().any(|d| !seen_docs.contains(d)) {
                return Err(PostingsDecodeError::Invalid("posting names an unknown doc"));
            }
            let term_freqs = r.u32_array(n, "posting term_freqs")?;
            let positions = if has_positions {
                let mut per_doc = Vec::with_capacity(n);
                for _ in 0..n {
                    let count = r.count(4, "positions count")?;
                    per_doc.push(r.u32_array(count, "positions")?);
                }
                Some(per_doc)
            } else {
                None
            };
            let list = PostingList::from_parts(&doc_ids, term_freqs, positions)
                .ok_or(PostingsDecodeError::Invalid("posting list invariants"))?;
            postings.push((term_id, list));
        }
        fields.push(PersistedField {
            next_id,
            fst_high_water_mark,
            terms,
            fst_bytes,
            postings,
        });
    }

    // TAG entries.
    let tag_doc_count = r.count(4 + 2, "tag_doc_count")?;
    let mut tag_docs = Vec::with_capacity(tag_doc_count);
    let mut seen_tag_docs: HashSet<u32> = HashSet::with_capacity(tag_doc_count);
    for _ in 0..tag_doc_count {
        let doc_id = r.u32("tag doc_id")?;
        if !seen_docs.contains(&doc_id) || !seen_tag_docs.insert(doc_id) {
            return Err(PostingsDecodeError::Invalid(
                "tag entry for unknown or repeated doc",
            ));
        }
        let n = r.u16("tag entry count")? as usize;
        if n == 0 || n.checked_mul(8).is_none_or(|need| need > r.remaining()) {
            return Err(PostingsDecodeError::Invalid("tag entry count"));
        }
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let field = r.bytes32("tag field")?;
            let value = r.bytes32("tag value")?;
            if field.is_empty() || value.is_empty() {
                return Err(PostingsDecodeError::Invalid("empty tag field or value"));
            }
            entries.push((Bytes::copy_from_slice(field), Bytes::copy_from_slice(value)));
        }
        tag_docs.push((doc_id, entries));
    }

    // NUMERIC entries.
    let numeric_doc_count = r.count(4 + 2, "numeric_doc_count")?;
    let mut numeric_docs = Vec::with_capacity(numeric_doc_count);
    let mut seen_num_docs: HashSet<u32> = HashSet::with_capacity(numeric_doc_count);
    for _ in 0..numeric_doc_count {
        let doc_id = r.u32("numeric doc_id")?;
        if !seen_docs.contains(&doc_id) || !seen_num_docs.insert(doc_id) {
            return Err(PostingsDecodeError::Invalid(
                "numeric entry for unknown or repeated doc",
            ));
        }
        let n = r.u16("numeric entry count")? as usize;
        if n == 0 || n.checked_mul(4 + 8).is_none_or(|need| need > r.remaining()) {
            return Err(PostingsDecodeError::Invalid("numeric entry count"));
        }
        let mut entries = Vec::with_capacity(n);
        for _ in 0..n {
            let field = r.bytes32("numeric field")?;
            let value = f64::from_bits(r.u64("numeric value")?);
            if field.is_empty() || !value.is_finite() {
                return Err(PostingsDecodeError::Invalid(
                    "empty numeric field or non-finite value",
                ));
            }
            entries.push((Bytes::copy_from_slice(field), value));
        }
        numeric_docs.push((doc_id, entries));
    }

    if r.remaining() != 0 {
        return Err(PostingsDecodeError::Invalid("trailing bytes after payload"));
    }

    Ok(PersistedTextIndex {
        schema_hash,
        name,
        db_index,
        next_doc_id,
        docs,
        fields,
        tag_docs,
        numeric_docs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Frame;
    use crate::text::types::{BM25Config, TextFieldDef};

    fn frames(pairs: &[(&str, &str)]) -> Vec<Frame> {
        pairs
            .iter()
            .flat_map(|(f, v)| {
                [
                    Frame::BulkString(Bytes::copy_from_slice(f.as_bytes())),
                    Frame::BulkString(Bytes::copy_from_slice(v.as_bytes())),
                ]
            })
            .collect()
    }

    fn sample_index() -> TextIndex {
        #[cfg(feature = "text-index")]
        let mut idx = TextIndex::new_with_schema(
            Bytes::from_static(b"ix"),
            vec![Bytes::from_static(b"t:")],
            vec![
                TextFieldDef::new(Bytes::from_static(b"title")),
                TextFieldDef::new(Bytes::from_static(b"body")),
            ],
            vec![crate::text::types::TagFieldDef::new(Bytes::from_static(
                b"cat",
            ))],
            vec![crate::text::types::NumericFieldDef::new(
                Bytes::from_static(b"n"),
            )],
            BM25Config::default(),
        );
        #[cfg(not(feature = "text-index"))]
        let mut idx = TextIndex::new(
            Bytes::from_static(b"ix"),
            vec![Bytes::from_static(b"t:")],
            vec![
                TextFieldDef::new(Bytes::from_static(b"title")),
                TextFieldDef::new(Bytes::from_static(b"body")),
            ],
            BM25Config::default(),
        );
        let docs: [&[(&str, &str)]; 3] = [
            &[
                ("title", "hello world"),
                ("body", "the quick brown fox"),
                ("cat", "a,b"),
                ("n", "5"),
            ],
            &[
                ("title", "fox news"),
                ("body", "a lazy dog and a quick cat"),
                ("cat", "b"),
                ("n", "7"),
            ],
            &[
                ("title", "third"),
                ("body", "hello again hello"),
                ("cat", "c"),
                ("n", "-1.5"),
            ],
        ];
        for (i, d) in docs.iter().enumerate() {
            let key = format!("t:{i}");
            let kh = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
            let args = frames(d);
            idx.index_document_with_lsn(kh, key.as_bytes(), &args, 10 + i as u64);
            #[cfg(feature = "text-index")]
            {
                idx.tag_index_document(kh, key.as_bytes(), &args);
                idx.numeric_index_document(kh, key.as_bytes(), &args);
            }
            idx.record_content_checksum(kh, &args);
        }
        idx
    }

    #[test]
    fn encode_is_deterministic_and_decodes() {
        let idx = sample_index();
        let a = encode_index(&idx);
        let b = encode_index(&idx);
        assert_eq!(a, b, "same state must give the same bytes");
        let p = decode(&a).expect("decode");
        assert_eq!(p.name, "ix");
        assert_eq!(p.docs.len(), 3);
        assert_eq!(p.fields.len(), 2);
        assert_eq!(p.next_doc_id, 3);
        assert_eq!(p.schema_hash, idx.schema_hash());
        let body_terms: usize = p.fields[1].terms.len();
        assert!(
            body_terms >= 6,
            "body dict has the analysed terms, got {body_terms}"
        );
        #[cfg(feature = "text-index")]
        {
            assert_eq!(p.tag_docs.len(), 3);
            assert_eq!(p.numeric_docs.len(), 3);
        }
    }

    #[test]
    fn every_framing_error_is_rejected_not_panicked() {
        let idx = sample_index();
        let good = encode_index(&idx);
        assert!(matches!(
            decode(&good[..10]),
            Err(PostingsDecodeError::TooShort(_))
        ));
        let mut bad = good.clone();
        bad[0] = b'X';
        assert_eq!(decode(&bad).err(), Some(PostingsDecodeError::BadMagic));
        let mut bad = good.clone();
        bad[4] = 9;
        assert_eq!(
            decode(&bad).err(),
            Some(PostingsDecodeError::UnsupportedVersion(9))
        );
        // truncated anywhere: length mismatch
        for cut in [
            good.len() - 1,
            good.len() - 9,
            HEADER_LEN + 8,
            HEADER_LEN + 40,
        ] {
            assert!(
                matches!(
                    decode(&good[..cut]),
                    Err(PostingsDecodeError::LengthMismatch { .. })
                ),
                "cut at {cut}"
            );
        }
        // appended garbage: length mismatch too
        let mut bad = good.clone();
        bad.extend_from_slice(b"junk");
        assert!(matches!(
            decode(&bad),
            Err(PostingsDecodeError::LengthMismatch { .. })
        ));
        // a single flipped payload byte: checksum
        for at in [HEADER_LEN + 3, HEADER_LEN + 60, good.len() - 12] {
            let mut bad = good.clone();
            bad[at] ^= 0x40;
            assert_eq!(
                decode(&bad).err(),
                Some(PostingsDecodeError::ChecksumMismatch),
                "flip at {at}"
            );
        }
    }

    /// Re-stamp the trailer so a structural corruption reaches the
    /// structural checks instead of the checksum.
    fn restamp(mut bytes: Vec<u8>) -> Vec<u8> {
        let n = bytes.len();
        let sum = trailer_checksum(&bytes[..n - 8]);
        bytes[n - 8..].copy_from_slice(&sum.to_le_bytes());
        bytes
    }

    #[test]
    fn structural_violations_are_rejected_after_a_valid_checksum() {
        let idx = sample_index();
        let good = encode_index(&idx);
        // next_doc_id is at payload offset: name_len(4)+name(2)+db(1)
        let ndi_at = HEADER_LEN + 4 + 2 + 1;
        let mut bad = good.clone();
        bad[ndi_at..ndi_at + 4].copy_from_slice(&0u32.to_le_bytes());
        assert!(
            matches!(decode(&restamp(bad)), Err(PostingsDecodeError::Invalid(_))),
            "doc_id >= next_doc_id"
        );
        // a hostile doc_count that no remaining bytes can back
        let dc_at = ndi_at + 4 + 2;
        let mut bad = good.clone();
        bad[dc_at..dc_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(
            matches!(
                decode(&restamp(bad)),
                Err(PostingsDecodeError::Invalid("doc_count"))
            ),
            "count must be bounded by the file, never pre-allocated"
        );
        // duplicate doc: make doc #2's id equal doc #1's
        let d0_at = dc_at + 4;
        let d1_at = d0_at + 4 + 4 + 3 + 8 + 8 + 4 * 2;
        let mut bad = good.clone();
        bad[d1_at..d1_at + 4].copy_from_slice(&good[d0_at..d0_at + 4]);
        assert!(matches!(
            decode(&restamp(bad)),
            Err(PostingsDecodeError::Invalid(_))
        ));
    }

    #[test]
    fn file_path_is_hex_and_cannot_escape_the_dir() {
        let dir = Path::new("/persist");
        let p = postings_file_path(dir, b"../../etc/passwd");
        assert_eq!(p.parent(), Some(dir));
        assert!(p.file_name().unwrap().to_string_lossy().ends_with(".tpost"));
        assert_eq!(p.file_name().unwrap().len(), 16 + 6);
    }

    #[test]
    fn read_missing_file_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_postings_file(tmp.path(), b"nope").unwrap().is_none());
    }

    /// Same shape as `sample_index()` but empty — what the boot path has
    /// after `TextIndex::from_meta`.
    fn empty_like(idx: &TextIndex) -> TextIndex {
        let meta = crate::text::index_persist::TextIndexMeta {
            name: idx.name.clone(),
            bm25_config: idx.bm25_config,
            key_prefixes: idx.key_prefixes.clone(),
            text_fields: idx.text_fields.clone(),
            db_index: idx.db_index,
            #[cfg(feature = "text-index")]
            tag_fields: idx.tag_fields.clone(),
            #[cfg(feature = "text-index")]
            numeric_fields: idx.numeric_fields.clone(),
        };
        TextIndex::from_meta(&meta)
    }

    fn hits(idx: &TextIndex, field: usize, terms: &[&str]) -> Vec<(Bytes, f32)> {
        let q: Vec<String> = terms.iter().map(|t| (*t).to_owned()).collect();
        let mut v: Vec<(Bytes, f32)> = idx
            .search_field(field, &q, None, None, 100)
            .into_iter()
            .map(|r| (r.key, r.score))
            .collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// The correctness bar: an index installed from `.tpost` answers exactly
    /// like the live one it was encoded from — hit sets AND scores, for
    /// single-term, multi-term AND, TAG and NUMERIC — and its resident-bytes
    /// accounting equals the ground-truth walk.
    #[cfg(feature = "text-index")]
    #[test]
    fn installed_index_answers_identically_to_the_live_one() {
        let live = sample_index();
        let bytes = encode_index(&live);
        let mut loaded = empty_like(&live);
        loaded
            .install_recovered(decode(&bytes).expect("decode"))
            .expect("install");

        for (field, terms) in [
            (0usize, vec!["hello"]),
            (0, vec!["fox"]),
            (1, vec!["quick"]),
            (1, vec!["quick", "fox"]),
            (1, vec!["hello"]),
            (1, vec!["nope"]),
        ] {
            let a = hits(&live, field, &terms);
            let b = hits(&loaded, field, &terms);
            assert_eq!(a, b, "field {field} terms {terms:?}");
        }
        assert_eq!(live.num_docs(), loaded.num_docs());
        assert_eq!(live.num_terms(), loaded.num_terms());
        assert_eq!(live.next_doc_id(), loaded.next_doc_id());
        assert_eq!(live.doc_id_to_key, loaded.doc_id_to_key);
        assert_eq!(live.doc_id_to_insert_lsn, loaded.doc_id_to_insert_lsn);
        assert_eq!(
            live.doc_id_to_content_checksum,
            loaded.doc_id_to_content_checksum
        );
        for f in 0..live.text_fields.len() {
            assert_eq!(live.field_stats[f].num_docs, loaded.field_stats[f].num_docs);
            assert_eq!(
                live.field_stats[f].total_field_length,
                loaded.field_stats[f].total_field_length
            );
            assert_eq!(
                live.field_postings[f].estimated_bytes(),
                loaded.field_postings[f].estimated_bytes()
            );
        }
        let cat = Bytes::from_static(b"cat");
        for v in ["a", "b", "c", "zz"] {
            let mut x = live.search_tag(&cat, &Bytes::copy_from_slice(v.as_bytes()));
            let mut y = loaded.search_tag(&cat, &Bytes::copy_from_slice(v.as_bytes()));
            x.sort_unstable();
            y.sort_unstable();
            assert_eq!(x, y, "tag {v}");
        }
        let n = Bytes::from_static(b"n");
        let mut x = live.search_numeric_range(&n, -2.0, 6.0, false, false);
        let mut y = loaded.search_numeric_range(&n, -2.0, 6.0, false, false);
        x.sort_unstable();
        y.sort_unstable();
        assert_eq!(x, y);
        assert_eq!(
            loaded.resident_bytes(),
            loaded.resident_bytes_ground_truth()
        );
        assert_eq!(live.resident_bytes(), loaded.resident_bytes());
        assert!(loaded.recovered_from_sidecar);
        assert!(!loaded.is_dirty(), "freshly installed == freshly persisted");
        // and it re-encodes to the very same bytes
        assert_eq!(encode_index(&loaded), bytes);
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn install_is_all_or_nothing_on_schema_mismatch_and_non_empty_target() {
        let live = sample_index();
        let bytes = encode_index(&live);
        // schema mismatch: same name, one field fewer
        let mut other = TextIndex::new(
            live.name.clone(),
            live.key_prefixes.clone(),
            vec![TextFieldDef::new(Bytes::from_static(b"title"))],
            live.bm25_config,
        );
        let err = other
            .install_recovered(decode(&bytes).unwrap())
            .expect_err("schema mismatch");
        assert!(err.contains("schema"), "{err}");
        assert_eq!(other.num_docs(), 0, "nothing installed");
        // non-empty target
        let mut busy = empty_like(&live);
        busy.index_document(1, b"t:x", &frames(&[("title", "already here")]));
        assert!(busy.install_recovered(decode(&bytes).unwrap()).is_err());
        assert_eq!(busy.num_docs(), 1);
        // a tag entry naming a field outside the schema is refused at install
        let mut p = decode(&bytes).unwrap();
        p.tag_docs[0]
            .1
            .push((Bytes::from_static(b"ghost"), Bytes::from_static(b"x")));
        let mut fresh = empty_like(&live);
        assert!(fresh.install_recovered(p).is_err());
        assert_eq!(fresh.num_docs(), 0);
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn dirty_tracking_follows_every_mutation() {
        let mut idx = empty_like(&sample_index());
        assert!(!idx.is_dirty(), "a fresh index is clean");
        let kh = 7u64;
        idx.index_document(kh, b"t:1", &frames(&[("title", "one")]));
        assert!(idx.is_dirty());
        let _ = idx.encode_for_persist(std::time::Instant::now());
        assert!(!idx.is_dirty());
        idx.record_content_checksum(kh, &frames(&[("title", "one")]));
        assert!(idx.is_dirty());
        let _ = idx.encode_for_persist(std::time::Instant::now());
        idx.tag_index_document(kh, b"t:1", &frames(&[("cat", "a")]));
        assert!(idx.is_dirty());
        let _ = idx.encode_for_persist(std::time::Instant::now());
        idx.numeric_index_document(kh, b"t:1", &frames(&[("n", "3")]));
        assert!(idx.is_dirty());
        let _ = idx.encode_for_persist(std::time::Instant::now());
        idx.build_fst();
        assert!(idx.is_dirty());
        let _ = idx.encode_for_persist(std::time::Instant::now());
        let doc = idx.key_hash_to_doc_id[&kh];
        idx.remove_doc_by_doc_id(doc);
        assert!(idx.is_dirty());
    }

    /// The 1 % duty cycle: after an encode, the index is not due again until
    /// `max(1 s, 100 × cost)` has passed, even if dirty.
    #[cfg(feature = "text-index")]
    #[test]
    fn persist_due_honours_the_duty_cycle() {
        let mut idx = sample_index();
        let t0 = std::time::Instant::now();
        assert!(idx.persist_due(t0));
        let _ = idx.encode_for_persist(t0);
        assert!(!idx.persist_due(t0), "clean");
        idx.index_document(99, b"t:9", &frames(&[("title", "x")]));
        assert!(
            !idx.persist_due(t0 + std::time::Duration::from_millis(500)),
            "dirty but inside the 1 s floor"
        );
        assert!(idx.persist_due(t0 + std::time::Duration::from_secs(2)));
    }

    /// Whole-store round trip through the writer thread and the boot loader,
    /// then the failure paths: a flipped byte falls back, a foreign file is
    /// swept, and `.tfst` seeding leaves a loaded index alone.
    #[cfg(feature = "text-index")]
    #[test]
    fn store_persists_loads_and_falls_back_through_real_files() {
        use crate::text::store::TextStore;
        let tmp = tempfile::tempdir().unwrap();
        let live = sample_index();
        let expected = hits(&live, 1, &["quick", "fox"]);
        let mut store = TextStore::new();
        store.set_persist_dir(tmp.path().to_path_buf());
        store.create_index(live.name.clone(), live).unwrap();
        assert_eq!(
            store
                .persist_all_postings_and_wait(std::time::Duration::from_secs(10))
                .unwrap(),
            1
        );
        let path = postings_file_path(tmp.path(), b"ix");
        assert!(path.exists());
        let meta = store.collect_index_metas().remove(0);

        // boot: restore the definition, load the file
        let mut booted = TextStore::new();
        booted.set_persist_dir(tmp.path().to_path_buf());
        booted
            .restore_index(meta.name.clone(), TextIndex::from_meta(&meta))
            .unwrap();
        let names = vec![meta.name.clone()];
        let mut loaded = Vec::new();
        assert_eq!(booted.load_postings_files(&names, 0, 64, &mut loaded), None);
        assert_eq!(loaded, vec![(Bytes::from_static(b"ix"), 3)]);
        assert_eq!(
            hits(booted.get_index(b"ix").unwrap(), 1, &["quick", "fox"]),
            expected
        );
        // `.tfst` seeding must not disturb a loaded index
        booted.load_term_fst_sidecars();
        assert_eq!(
            hits(booted.get_index(b"ix").unwrap(), 1, &["quick", "fox"]),
            expected
        );

        // corruption: one flipped byte -> not loaded, index stays empty
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[40] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();
        let mut booted2 = TextStore::new();
        booted2.set_persist_dir(tmp.path().to_path_buf());
        booted2
            .restore_index(meta.name.clone(), TextIndex::from_meta(&meta))
            .unwrap();
        let mut loaded2 = Vec::new();
        booted2.load_postings_files(&names, 0, 64, &mut loaded2);
        assert!(loaded2.is_empty(), "corrupt file must fall back");
        assert_eq!(booted2.get_index(b"ix").unwrap().num_docs(), 0);

        // orphan sweep removes a file no index owns; the live one survives
        let orphan = postings_file_path(tmp.path(), b"dropped-long-ago");
        std::fs::write(&orphan, b"junk").unwrap();
        booted2.sweep_orphan_postings_files();
        crate::text::persist_writer::writer()
            .flush_blocking(std::time::Duration::from_secs(10))
            .unwrap();
        assert!(!orphan.exists());
        assert!(path.exists());

        // drop deletes the file
        assert!(booted2.drop_index(b"ix"));
        crate::text::persist_writer::writer()
            .flush_blocking(std::time::Duration::from_secs(10))
            .unwrap();
        assert!(!path.exists());
    }
}
