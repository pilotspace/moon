//! Freed doc-id reuse (moon#1221 review, refs moon#1194).
//!
//! A new document takes the smallest id no live document holds, so the dense per-document columns
//! are bounded by the peak live-document count. These tests pin the two halves of that contract:
//! the free set is exactly `[0, next_doc_id) \ live` with `next_doc_id = max live + 1` under any
//! churn, and a reused id carries NOTHING of its previous owner — every query, BM25 statistic,
//! TAG/NUMERIC filter and AS_OF snapshot answers exactly like a fresh index holding the same docs.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::text::query::{QuerySchema, eval_query_counted, parse_query};
use crate::text::store::TextIndex;
use crate::text::types::{BM25Config, NumericFieldDef, TagFieldDef, TextFieldDef};

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

fn schema_index() -> TextIndex {
    TextIndex::new_with_schema(
        Bytes::from_static(b"reuse"),
        vec![Bytes::from_static(b"d:")],
        vec![
            TextFieldDef::new(Bytes::from_static(b"title")),
            TextFieldDef::new(Bytes::from_static(b"body")),
        ],
        vec![TagFieldDef::new(Bytes::from_static(b"cat"))],
        vec![NumericFieldDef::new(Bytes::from_static(b"n"))],
        BM25Config::default(),
    )
}

/// Index one document the way `auto_index_hset` does (text with LSN, TAG, NUMERIC, checksum).
fn put(idx: &mut TextIndex, key: &str, pairs: &[(&str, &str)], lsn: u64) {
    let kh = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
    let args = frames(pairs);
    idx.index_document_with_lsn(kh, key.as_bytes(), &args, lsn);
    idx.tag_index_document(kh, key.as_bytes(), &args);
    idx.numeric_index_document(kh, key.as_bytes(), &args);
    idx.record_content_checksum(kh, &args);
}

fn remove_key(idx: &mut TextIndex, key: &str) {
    let kh = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
    let doc = *idx.key_hash_to_doc_id.get(&kh).expect("tracked");
    idx.remove_doc_by_doc_id(doc);
}

/// The documents both indexes end up holding: distinct, asymmetric term frequencies and lengths,
/// shared words with the junk documents, TAG and NUMERIC values, increasing LSNs.
fn wanted_docs() -> Vec<(String, Vec<(&'static str, String)>, u64)> {
    let words = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
    ];
    (0..40usize)
        .map(|i| {
            let mut body = String::new();
            for (w, word) in words.iter().enumerate() {
                for _ in 0..((i + w) % 4) {
                    body.push_str(word);
                    body.push(' ');
                }
            }
            let title = format!("{} {}", words[i % 7], words[(i * 3 + 1) % 7]);
            let cat = ["red", "green", "blue"][i % 3].to_owned();
            let n = format!("{}", (i * 37) % 23);
            (
                format!("d:{i}"),
                vec![("title", title), ("body", body), ("cat", cat), ("n", n)],
                1_000 + i as u64,
            )
        })
        .collect()
}

fn put_wanted(idx: &mut TextIndex, doc: &(String, Vec<(&'static str, String)>, u64)) {
    let pairs: Vec<(&str, &str)> = doc.1.iter().map(|(f, v)| (*f, v.as_str())).collect();
    put(idx, &doc.0, &pairs, doc.2);
}

/// `(key, score bits)` of every match plus the total, key-sorted: doc ids differ between the two
/// indexes, so tie ORDER legitimately differs; the set, the scores and the count may not.
fn answer(idx: &TextIndex, q: &str) -> (Vec<(Bytes, u32)>, usize) {
    let node = parse_query(q.as_bytes(), &QuerySchema::from_index(idx)).expect("parse");
    let (hits, total) = eval_query_counted(idx, &node, None, None, 10_000);
    let mut v: Vec<(Bytes, u32)> = hits
        .into_iter()
        .map(|r| (r.key, r.score.to_bits()))
        .collect();
    v.sort();
    (v, total)
}

fn keys_of(idx: &TextIndex, ids: impl IntoIterator<Item = u32>) -> Vec<Bytes> {
    let mut v: Vec<Bytes> = ids
        .into_iter()
        .map(|d| idx.doc_id_to_key.get(&d).expect("live id").clone())
        .collect();
    v.sort();
    v
}

const QUERIES: &[&str] = &[
    "alpha",
    "golf",
    "alpha bravo",
    "charlie | echo",
    "@title:delta",
    "@body:foxtrot @title:alpha",
    "bra*",
    "%chrlie%",
    "@cat:{red}",
    "@cat:{green | blue} alpha",
    "@n:[3 11]",
    "@n:[(5 +inf] echo",
    "*",
    "junkword",
];

fn assert_same_answers(label: &str, got: &TextIndex, want: &TextIndex) {
    let mut nonempty = 0;
    for q in QUERIES {
        let (g, w) = (answer(got, q), answer(want, q));
        assert_eq!(g, w, "{label}: FT.SEARCH {q:?}");
        nonempty += usize::from(!w.0.is_empty());
    }
    assert!(
        nonempty >= QUERIES.len() - 2,
        "{label}: {nonempty} non-empty"
    );
    assert_eq!(got.num_docs(), want.num_docs(), "{label}: N");
    for f in 0..want.text_fields.len() {
        assert_eq!(
            got.field_stats[f].num_docs, want.field_stats[f].num_docs,
            "{label}: field {f} num_docs"
        );
        assert_eq!(
            got.field_stats[f].total_field_length, want.field_stats[f].total_field_length,
            "{label}: field {f} total length (avgdl)"
        );
        for (term, &id) in want.field_term_dicts[f].iter() {
            let gid = got.field_term_dicts[f].get(term).expect("term known");
            assert_eq!(
                got.field_postings[f].doc_freq(gid),
                want.field_postings[f].doc_freq(id),
                "{label}: df({term})"
            );
        }
    }
    let cat = Bytes::from_static(b"cat");
    for v in ["red", "green", "blue"] {
        let v = Bytes::copy_from_slice(v.as_bytes());
        assert_eq!(
            keys_of(got, got.search_tag(&cat, &v)),
            keys_of(want, want.search_tag(&cat, &v)),
            "{label}: tag {v:?}"
        );
    }
    let n = Bytes::from_static(b"n");
    assert_eq!(
        keys_of(got, got.search_numeric_range(&n, 2.0, 15.0, false, false)),
        keys_of(want, want.search_numeric_range(&n, 2.0, 15.0, false, false)),
        "{label}: numeric"
    );
    // AS_OF: a reused id must carry its NEW owner's insert LSN, never the freed doc's.
    for as_of in [1u64, 50, 1_000, 1_017, 1_039, u64::MAX] {
        for field in 0..2 {
            let terms = vec!["alpha".to_owned()];
            let g = keys_of(
                got,
                got.search_field_as_of(field, &terms, None, None, 1_000, as_of)
                    .into_iter()
                    .map(|r| r.doc_id),
            );
            let w = keys_of(
                want,
                want.search_field_as_of(field, &terms, None, None, 1_000, as_of)
                    .into_iter()
                    .map(|r| r.doc_id),
            );
            assert_eq!(g, w, "{label}: AS_OF {as_of} field {field}");
        }
    }
    // No stale hash can reach an id; every live key round-trips.
    assert_eq!(got.key_hash_to_doc_id.len(), got.doc_id_to_key.len());
    for (kh, &d) in &got.key_hash_to_doc_id {
        let key = got.doc_id_to_key.get(&d).expect("hash maps to a live id");
        assert_eq!(xxhash_rust::xxh64::xxh64(key, 0), *kh);
    }
    assert_eq!(got.resident_bytes(), got.resident_bytes_ground_truth());
}

/// Junk documents are indexed, partly removed (interior holes AND the highest id), the wanted
/// documents then reuse the freed ids, and the rest of the junk goes. The churned index — and the
/// same index after a `.tpost` round trip — must answer exactly like a fresh one.
#[test]
fn reused_doc_ids_answer_exactly_like_a_fresh_index() {
    let wanted = wanted_docs();
    let mut fresh = schema_index();
    for d in &wanted {
        put_wanted(&mut fresh, d);
    }

    let mut churned = schema_index();
    let junk = |i: usize| {
        (
            format!("j:{i}"),
            format!("junkword alpha alpha echo w{i}"),
            ["red", "zzz"][i % 2],
            format!("{}", i % 7),
        )
    };
    for i in 0..30 {
        let (k, body, cat, n) = junk(i);
        put(
            &mut churned,
            &k,
            &[
                ("title", "golf golf"),
                ("body", &body),
                ("cat", cat),
                ("n", &n),
            ],
            1 + i as u64,
        );
    }
    // Free id 0, interior ids and the highest id (29).
    for i in (0..30).step_by(3).chain([29]) {
        remove_key(&mut churned, &junk(i).0);
    }
    assert_eq!(
        churned.next_doc_id(),
        29,
        "freeing the top id lowers the counter"
    );
    assert!(churned.free_doc_ids().contains(0));
    for d in &wanted {
        put_wanted(&mut churned, d);
    }
    assert_eq!(
        churned.key_hash_to_doc_id[&xxhash_rust::xxh64::xxh64(b"d:0", 0)],
        0,
        "the first new document takes the smallest freed id"
    );
    for i in (0..30).filter(|i| i % 3 != 0 && *i != 29) {
        remove_key(&mut churned, &junk(i).0);
    }
    let reused = (0..30u32)
        .filter(|&d| churned.doc_id_to_key.get(&d).is_some())
        .count();
    assert!(reused >= 10, "fixture must reuse freed ids ({reused})");
    assert!(
        !churned.free_doc_ids().is_empty(),
        "fixture must leave holes for the round trip"
    );
    assert_same_answers("churned", &churned, &fresh);

    // Holes survive a `.tpost` round trip as free ids, and the loaded index still answers alike.
    let bytes = crate::text::postings_persist::encode_index(&churned);
    let mut loaded = empty_like(&churned);
    loaded
        .install_recovered(crate::text::postings_persist::decode(&bytes).expect("decode"))
        .expect("install");
    assert_eq!(loaded.next_doc_id(), churned.next_doc_id());
    assert_eq!(loaded.free_doc_ids(), churned.free_doc_ids());
    assert_same_answers("loaded", &loaded, &fresh);
}

fn empty_like(idx: &TextIndex) -> TextIndex {
    TextIndex::from_meta(&crate::text::index_persist::TextIndexMeta {
        name: idx.name.clone(),
        bm25_config: idx.bm25_config,
        key_prefixes: idx.key_prefixes.clone(),
        text_fields: idx.text_fields.clone(),
        db_index: idx.db_index,
        tag_fields: idx.tag_fields.clone(),
        numeric_fields: idx.numeric_fields.clone(),
    })
}

/// SplitMix64 — deterministic, dependency-free.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Under random inserts, upserts and removals (id 0, interior ids, the top id, bulk removals):
/// `next_doc_id == max live + 1`, the free set is exactly `[0, next_doc_id) \ live`, a new key
/// always takes the smallest free id, and the column footprints never exceed what the peak live
/// count needs.
#[test]
fn free_doc_ids_stay_exact_under_random_churn() {
    let mut idx = schema_index();
    let mut rng = Rng(0x1221);
    let mut peak = 0usize;
    for step in 0..6_000u64 {
        let key = format!("d:{}", rng.below(700));
        let kh = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
        match rng.below(10) {
            0..=5 => {
                let expect = idx
                    .key_hash_to_doc_id
                    .get(&kh)
                    .copied()
                    .or_else(|| idx.free_doc_ids().min())
                    .unwrap_or(idx.next_doc_id());
                let body = format!("t{} t{}", step % 13, step % 5);
                put(
                    &mut idx,
                    &key,
                    &[("body", &body), ("cat", "c"), ("n", "1")],
                    step + 1,
                );
                assert_eq!(idx.key_hash_to_doc_id[&kh], expect, "step {step}");
            }
            6..=8 => {
                if let Some(&d) = idx.key_hash_to_doc_id.get(&kh) {
                    idx.remove_doc_by_doc_id(d);
                }
            }
            _ => {
                // Bulk removal (FT.INVALIDATE_RANGE-style), ascending ids.
                let ids: Vec<u32> = idx.doc_id_to_key.keys().filter(|d| d % 3 == 0).collect();
                for d in ids {
                    idx.remove_doc_by_doc_id(d);
                }
            }
        }
        let live = idx.doc_id_to_key.live().clone();
        peak = peak.max(live.len() as usize);
        let next = live.max().map_or(0, |m| m + 1);
        assert_eq!(idx.next_doc_id(), next, "step {step}");
        let mut free = roaring::RoaringBitmap::new();
        free.insert_range(0..next);
        free -= &live;
        assert_eq!(idx.free_doc_ids(), &free, "step {step}");
        assert_eq!(idx.doc_numeric_entries.len(), live.len() as usize);
        assert_eq!(idx.doc_tag_entries.len(), live.len() as usize);
    }
    assert!(peak > 100, "fixture must build up live docs ({peak})");
    // Columns are bounded by the peak live count (×2 for Vec growth), not by 6K operations.
    let slot = std::mem::size_of::<Option<Bytes>>();
    assert!(idx.doc_id_to_key.footprint() <= 2 * peak.max(16) * slot);
    assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());
}

/// Removing every document gives the column memory back; the next document starts at id 0.
#[test]
fn emptying_the_index_releases_the_columns() {
    let mut idx = schema_index();
    for i in 0..3_000 {
        put(
            &mut idx,
            &format!("d:{i}"),
            &[("title", "a b"), ("body", "c"), ("cat", "x"), ("n", "2")],
            1 + i as u64,
        );
    }
    let full = idx.resident_bytes();
    let ids: Vec<u32> = idx.doc_id_to_key.keys().collect();
    for d in ids {
        idx.remove_doc_by_doc_id(d);
    }
    assert_eq!(idx.next_doc_id(), 0);
    assert!(idx.free_doc_ids().is_empty());
    assert_eq!(idx.doc_id_to_key.footprint(), 0);
    assert_eq!(idx.doc_field_lengths.footprint(), 0);
    assert_eq!(idx.doc_tag_entries.footprint(), 0);
    assert!(
        idx.resident_bytes() * 10 < full,
        "{} vs {full}",
        idx.resident_bytes()
    );
    assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());
    put(&mut idx, "d:new", &[("title", "a")], 9);
    assert_eq!(
        idx.key_hash_to_doc_id[&xxhash_rust::xxh64::xxh64(b"d:new", 0)],
        0
    );
}

/// A caller that indexed under a hash other than `xxh64(key)` (unit tests do) must not leave a
/// hash pointing at a freed — and then reused — id.
#[test]
fn removal_under_a_synthetic_key_hash_leaves_no_stale_hash() {
    let mut idx = schema_index();
    idx.index_document(777, b"d:a", &frames(&[("title", "one")]));
    idx.index_document(778, b"d:b", &frames(&[("title", "two")]));
    idx.remove_doc_by_doc_id(0);
    assert!(!idx.key_hash_to_doc_id.contains_key(&777));
    idx.index_document(779, b"d:c", &frames(&[("title", "three")]));
    assert_eq!(idx.key_hash_to_doc_id.get(&779), Some(&0));
    assert_eq!(idx.key_hash_to_doc_id.len(), 2);
    assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());
}
