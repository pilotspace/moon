//! moon#1220 (item 2) through PUBLIC APIs only (compiles unchanged against the wave-1 base
//! `f32546c`): posting positions stored contiguously per run instead of one `Vec<u32>` per
//! (term, document).
//!
//! * `tpost_bytes_are_unchanged_by_the_position_layout` — the `.tpost` encoding of a fixed corpus
//!   (chunked postings, upserts of old documents, deletions) hashes to the value the base layout
//!   produced: the in-memory change is invisible on disk, so no version gate is needed.
//! * `positions_cost_bytes_per_entry_not_a_vec_each` — red on the base: every (term, document)
//!   entry held its own `Vec<u32>` of positions (a 24-byte header plus a heap chunk — 32 bytes
//!   minimum under glibc, 8 under jemalloc — for what is usually ONE `u32`). Measured as this
//!   process's `VmRSS` growth per posting entry while building a large index (Linux only).
//!
//! The tests share one lock (moon#1226): libtest runs them on concurrent threads of ONE process,
//! and the golden corpora's allocations landed inside the `VmRSS` window of the per-entry test.

#![cfg(feature = "text-index")]

use bytes::Bytes;
use moon::protocol::Frame;
use moon::text::store::TextIndex;
use moon::text::types::{BM25Config, TextFieldDef};

fn body_frames(field: &'static [u8], text: String) -> [Frame; 2] {
    [
        Frame::BulkString(Bytes::from_static(field)),
        Frame::BulkString(Bytes::from(text)),
    ]
}

fn new_index(fields: &[&'static [u8]]) -> TextIndex {
    TextIndex::new(
        Bytes::from_static(b"pos"),
        vec![Bytes::from_static(b"p:")],
        fields
            .iter()
            .map(|f| TextFieldDef::new(Bytes::from_static(f)))
            .collect(),
        BM25Config::default(),
    )
}

/// Serialises this binary's tests (see the module docs).
static SERIAL: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn index(idx: &mut TextIndex, d: u32, args: &[Frame]) {
    let key = format!("p:{d}");
    idx.index_document(
        xxhash_rust::xxh64::xxh64(key.as_bytes(), 0),
        key.as_bytes(),
        args,
    );
}

/// Deterministic multi-field corpus: repeated words (tf > 1, several positions per entry),
/// postings far past the 256-entry run size, upserts of OLD docs with new bodies (mid-posting
/// re-inserts and position extensions), and deletions that are never re-added.
fn golden_corpus() -> TextIndex {
    const WORDS: [&str; 9] = [
        "anchor", "beacon", "cinder", "dynamo", "ember", "flint", "garnet", "harbor", "ingot",
    ];
    let mut idx = new_index(&[b"title", b"body"]);
    let text = |d: u32, salt: u32, n: u32| {
        let mut s = String::new();
        for i in 0..n {
            let w = (d.wrapping_mul(2_654_435_761) ^ (i * 40_503) ^ salt) % 23;
            s.push_str(WORDS[(w as usize * w as usize) % WORDS.len()]);
            s.push(' ');
        }
        s
    };
    for d in 0..2_400u32 {
        let mut args = body_frames(b"body", text(d, 0, 3 + d % 11)).to_vec();
        if d % 4 != 0 {
            args.extend(body_frames(b"title", text(d, 7, 1 + d % 3)));
        }
        index(&mut idx, d, &args);
    }
    for d in (0..900u32).step_by(7) {
        let args = body_frames(b"body", text(d, 99, 2 + d % 13));
        index(&mut idx, d, &args);
    }
    for d in (3..2_400u32).step_by(17) {
        idx.remove_doc_by_doc_id(d);
    }
    idx
}

/// xxh64 of the `.tpost` encoding of `golden_corpus()`.
///
/// WS13 pinned `0xe4dc_7f45_6580_79e0`, the wave-1 base layout's encoding (`f32546c`, before the
/// contiguous-positions change), to prove that change invisible on disk. moon#1220 item 3 then
/// made `.tpost` rewrites number documents densely when the index has holes, and this corpus
/// deletes 141 documents it never re-adds: its file is now the same layout with every doc id
/// replaced by its rank among the live ids (same length, 293,326 bytes). Verified when the value
/// was taken: with the renumbering disabled the encoding still hashes to `0xe4dc_7f45_6580_79e0`.
const GOLDEN_TPOST_XXH64: u64 = 0x6424_a6f3_373f_a40d;

#[test]
fn tpost_bytes_are_unchanged_by_the_position_layout() {
    let _serial = SERIAL.lock();
    let idx = golden_corpus();
    let bytes = moon::text::postings_persist::encode_index(&idx);
    let got = xxhash_rust::xxh64::xxh64(&bytes, 0);
    eprintln!("golden .tpost: {} bytes, xxh64 {got:#018x}", bytes.len());
    assert_eq!(
        got,
        GOLDEN_TPOST_XXH64,
        "the .tpost encoding changed: {got:#018x} ({} bytes)",
        bytes.len()
    );
}

/// `golden_corpus()` followed by new documents that take the freed ids back (the reuse allocator,
/// moon#1221 review): smallest free id first, so the index ends hole-free and the encoding is the
/// in-memory numbering verbatim — no renumbering involved.
fn golden_corpus_with_reuse() -> TextIndex {
    let mut idx = golden_corpus();
    let freed = idx.free_doc_ids().len() as u32;
    assert!(freed > 100, "the golden corpus leaves holes: {freed}");
    for d in 0..freed + 40 {
        let body = format!(
            "reused{} anchor ember {}",
            d % 5,
            "flint ".repeat((d % 4) as usize)
        );
        index(&mut idx, 10_000 + d, &body_frames(b"body", body));
    }
    assert!(idx.free_doc_ids().is_empty(), "every freed id was reused");
    idx
}

/// xxh64 of `golden_corpus_with_reuse()`'s encoding (311,915 bytes) — the same with and without
/// the moon#1220 renumbering (the index has no holes; both were checked when it was taken).
const GOLDEN_REUSE_TPOST_XXH64: u64 = 0x0e9d_e59f_a98b_a6de;

/// moon#1226: a doc-id-reuse case in the golden set — reused ids interleave with old ones in
/// every posting, and the file must still round-trip and stay stable.
#[test]
fn tpost_bytes_of_a_reused_id_corpus_are_pinned() {
    let _serial = SERIAL.lock();
    let idx = golden_corpus_with_reuse();
    let bytes = moon::text::postings_persist::encode_index(&idx);
    let got = xxhash_rust::xxh64::xxh64(&bytes, 0);
    eprintln!(
        "golden reuse .tpost: {} bytes, xxh64 {got:#018x}",
        bytes.len()
    );
    let p = moon::text::postings_persist::decode(&bytes).expect("decode");
    assert_eq!(
        p.next_doc_id,
        idx.next_doc_id(),
        "hole-free: ids written as they are"
    );
    assert_eq!(
        got,
        GOLDEN_REUSE_TPOST_XXH64,
        "the reused-id .tpost encoding changed: {got:#018x} ({} bytes)",
        bytes.len()
    );
}

/// This process's resident set, in bytes.
#[cfg(target_os = "linux")]
fn vm_rss() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
        .expect("VmRSS line");
    kb * 1024
}

#[cfg(target_os = "linux")]
#[test]
fn positions_cost_bytes_per_entry_not_a_vec_each() {
    let _serial = SERIAL.lock();
    const DOCS: u32 = 120_000;
    const TOKENS: u32 = 24;
    // A 60-word vocabulary: every posting is tens of thousands of entries long, so per-posting
    // overheads vanish and the per-(term, doc) cost dominates the growth.
    let vocab: Vec<String> = (0..60u32).map(|i| format!("w{i:02}x")).collect();
    let mut idx = new_index(&[b"body"]);
    let rss_before = vm_rss();
    for d in 0..DOCS {
        let mut s = String::new();
        for i in 0..TOKENS {
            let r = (d.wrapping_mul(2_246_822_519) ^ i.wrapping_mul(3_266_489_917)) % 60;
            s.push_str(&vocab[r as usize]);
            s.push(' ');
        }
        index(&mut idx, d, &body_frames(b"body", s));
    }
    let grown = vm_rss().saturating_sub(rss_before);
    let entries: u64 = vocab
        .iter()
        .filter_map(|w| idx.field_term_dicts[0].get(w))
        .map(|t| u64::from(idx.field_postings[0].doc_freq(t)))
        .sum();
    let per_entry = grown as f64 / entries as f64;
    eprintln!("{entries} (term, doc) entries: VmRSS +{grown} B = {per_entry:.1} B per entry");
    assert!(entries > u64::from(DOCS) * 12, "fixture: {entries} entries");
    // Whole-index growth (keys, doc columns, the doc -> terms map, bitmaps, tf and positions)
    // divided by the entry count. Unoptimised build, glibc: base layout 107.6 B per entry,
    // contiguous positions 48.4 B; the bound sits between them.
    assert!(
        per_entry < 75.0,
        "{per_entry:.1} resident bytes per (term, doc) entry"
    );
    std::hint::black_box(&idx);
}
