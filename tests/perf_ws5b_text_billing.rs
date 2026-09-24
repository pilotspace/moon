//! moon#1194 (text part): a text index's per-document side tables must be billed at their REAL
//! size. HEAD kept `doc_tag_entries` as `HashMap<u32, SmallVec<[(Bytes, Bytes); 8]>>` — 528 B
//! inline per tagged document, ~600-1200 B with hashbrown slack — while
//! `TextIndex::resident_bytes()` charged ~100 B, so `used_memory` under-reported ~5x (the NUMERIC
//! table ~4x).
//!
//! Method: the test measures the process's resident set (`VmRSS`) across a batch of TAG / NUMERIC
//! indexing calls and compares it with the `resident_bytes()` delta the index bills for the same
//! calls. The docs already exist (keys and TEXT postings are built first), so the delta is the
//! TAG/NUMERIC side tables plus their small shared value maps. No custom global allocator (that
//! would need `unsafe`); the batch is large (tens of MB on HEAD) so page granularity and allocator
//! chunk headers stay inside the tolerance. Linux-only (`/proc/self/status`). One test per phase
//! runs in its own process-wide order via a single `#[test]`, so no concurrent test perturbs RSS.

#![cfg(all(feature = "text-index", target_os = "linux"))]

use bytes::Bytes;
use moon::protocol::Frame;
use moon::text::store::TextIndex;
use moon::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

/// Current resident set size in bytes.
fn rss() -> isize {
    let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    status
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<isize>().ok())
        .expect("VmRSS")
        * 1024
}

fn frames(pairs: &[(&str, String)]) -> Vec<Frame> {
    pairs
        .iter()
        .flat_map(|(k, v)| {
            [
                Frame::BulkString(Bytes::copy_from_slice(k.as_bytes())),
                Frame::BulkString(Bytes::copy_from_slice(v.as_bytes())),
            ]
        })
        .collect()
}

const DOCS: usize = 60_000;

fn index_with_docs() -> (TextIndex, Vec<(u64, String)>) {
    let mut idx = TextIndex::new_with_schema(
        Bytes::from_static(b"bill"),
        vec![Bytes::from_static(b"d:")],
        vec![TextFieldDef::new(Bytes::from_static(b"body"))],
        vec![
            TagFieldDef::new(Bytes::from_static(b"status")),
            TagFieldDef::new(Bytes::from_static(b"region")),
        ],
        vec![
            NumericFieldDef::new(Bytes::from_static(b"price")),
            NumericFieldDef::new(Bytes::from_static(b"stock")),
        ],
        BM25Config::default(),
    );
    let mut keys = Vec::with_capacity(DOCS);
    for d in 0..DOCS {
        let key = format!("d:{d}");
        let kh = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
        idx.index_document(
            kh,
            key.as_bytes(),
            &frames(&[("body", format!("w{} common", d % 97))]),
        );
        keys.push((kh, key));
    }
    (idx, keys)
}

/// `(real, billed)` bytes added by `f`.
fn measure(idx: &mut TextIndex, f: impl FnOnce(&mut TextIndex)) -> (isize, isize) {
    let (real0, billed0) = (rss(), idx.resident_bytes() as isize);
    f(idx);
    (rss() - real0, idx.resident_bytes() as isize - billed0)
}

fn check(what: &str, real: isize, billed: isize, max_real_per_doc: f64) {
    let ratio = billed as f64 / real as f64;
    let per_doc = real as f64 / DOCS as f64;
    eprintln!(
        "{what}: real {real} B ({per_doc:.1} B/doc), billed {billed} B ({:.1} B/doc), billed/real {ratio:.2}",
        billed as f64 / DOCS as f64
    );
    assert!(
        (0.6..=1.5).contains(&ratio),
        "{what}: billed {billed} B vs resident {real} B (ratio {ratio:.2}) — used_memory mis-reports"
    );
    assert!(
        per_doc < max_real_per_doc,
        "{what}: side table costs {per_doc:.0} B/doc"
    );
}

#[test]
fn tag_and_numeric_side_tables_are_billed_at_their_real_size() {
    let (mut idx, keys) = index_with_docs();

    // TAG: 2-3 values per doc over two fields (one CSV-split, mixed case).
    let statuses = ["open", "closed", "pending", "Review"];
    let regions = ["eu,us", "apac", "us", "latam,eu"];
    let (real, billed) = measure(&mut idx, |idx| {
        for (d, (kh, key)) in keys.iter().enumerate() {
            let args = frames(&[
                ("status", statuses[d % 4].to_owned()),
                ("region", regions[d % 4].to_owned()),
            ]);
            idx.tag_index_document(*kh, key.as_bytes(), &args);
        }
    });
    // HEAD's inline slot alone was 528 B/doc.
    check("TAG", real, billed, 200.0);

    // NUMERIC: two values per doc.
    let (real, billed) = measure(&mut idx, |idx| {
        for (d, (kh, key)) in keys.iter().enumerate() {
            let args = frames(&[
                ("price", format!("{}.25", d % 50)),
                ("stock", format!("{}", d % 7)),
            ]);
            idx.numeric_index_document(*kh, key.as_bytes(), &args);
        }
    });
    check("NUMERIC", real, billed, 120.0);

    // Upserting every doc's TAG field with the same value keeps the billing stable.
    let before = idx.resident_bytes();
    for (d, (kh, key)) in keys.iter().enumerate() {
        let args = frames(&[("status", statuses[d % 4].to_owned())]);
        idx.tag_index_document(*kh, key.as_bytes(), &args);
    }
    assert_eq!(
        idx.resident_bytes(),
        before,
        "same-value upsert bills the same"
    );
}
