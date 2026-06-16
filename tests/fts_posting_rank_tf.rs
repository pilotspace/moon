//! RED tests for `fts-posting-rank-tf` (milestone v3-1-fts-hardening).
//!
//! Contract FROZEN @ v1 (`.add/tasks/fts-posting-rank-tf/TASK.md` §3): `PostingList`'s
//! `term_freqs` / `positions` are RANK-aligned to the sorted `doc_ids` bitmap, and
//! `PostingList::tf(doc_id)` is a sub-linear, correct term-frequency lookup that survives
//! document updates.
//!
//! TDD — these MUST be RED before build:
//!  * unit tests are red because `tf()` / `positions_for()` don't exist yet, AND because a
//!    naive `position()`-over-insertion-order lookup returns wrong values (6,3,1 vs 5,2,3);
//!  * integration tests are red because the current push-to-end write (posting.rs:96-97)
//!    misaligns `term_freqs` after a document update, corrupting BM25 ranking (TASK.md §1 A1).
//!
//! The bug only manifests with DISTINCT term frequencies — equal tfs mask the misalignment,
//! so every scenario below uses distinct counts.
#![cfg(feature = "text-index")]

use bytes::Bytes;
use moon::protocol::Frame;
use moon::text::posting::PostingStore;
use moon::text::store::TextIndex;
use moon::text::types::{BM25Config, TextFieldDef};

// ─────────────────────────── unit: rank-aligned TF contract ───────────────────────────

/// Add `n` occurrences of `term_id` for `doc_id` (simulates first-index tf=n).
fn add_n(store: &mut PostingStore, term_id: u32, doc_id: u32, n: u32) {
    for _ in 0..n {
        store.add_term_occurrence(term_id, doc_id, None);
    }
}

/// M1 / A1 — the headline correctness fix.
#[test]
fn test_tf_correct_after_low_id_update() {
    // term 1: doc0 ×1, doc1 ×2, doc2 ×3 — ascending insert (aligned today).
    let mut store = PostingStore::new();
    add_n(&mut store, 1, 0, 1);
    add_n(&mut store, 1, 1, 2);
    add_n(&mut store, 1, 2, 3);
    // Update the LOWEST-id doc: remove then re-index with ×5 (the misaligning path).
    store.remove_doc(0);
    add_n(&mut store, 1, 0, 5);

    let p = store.get_posting(1).expect("posting exists");
    // CONTRACT: tf(doc_id) is the TRUE term frequency, regardless of insert/update order.
    // A naive position()-over-insertion-order lookup reads (6, 3, 1) here — the bug.
    assert_eq!(
        p.tf(0),
        5,
        "updated low-id doc must read its own tf, not a neighbour's"
    );
    assert_eq!(
        p.tf(1),
        2,
        "doc1 tf must be untouched by doc0's re-insertion"
    );
    assert_eq!(
        p.tf(2),
        3,
        "doc2 tf must be untouched by doc0's re-insertion"
    );
}

/// Reject — `tf_absent` defined default.
#[test]
fn test_tf_absent_is_zero() {
    let mut store = PostingStore::new();
    add_n(&mut store, 1, 0, 2);
    let p = store.get_posting(1).expect("posting exists");
    assert_eq!(
        p.tf(999),
        0,
        "absent doc_id -> tf 0 (no panic, no neighbour leak)"
    );
}

/// M5 — positions stay aligned with TF after an update.
#[test]
fn test_positions_aligned_after_update() {
    let mut store = PostingStore::new();
    store.add_term_occurrence(1, 0, Some(vec![0]));
    store.add_term_occurrence(1, 1, Some(vec![0]));
    store.add_term_occurrence(1, 2, Some(vec![0]));
    // Update low-id doc0 with 5 occurrences at distinct positions.
    store.remove_doc(0);
    store.add_term_occurrence(1, 0, Some(vec![0, 1, 2, 3, 4]));

    let p = store.get_posting(1).expect("posting exists");
    let pos0 = p.positions_for(0).expect("positions tracked for doc0");
    assert_eq!(
        pos0,
        &[0, 1, 2, 3, 4][..],
        "doc0 positions must belong to doc0 after update"
    );
}

/// M2 — TF lookup is sub-linear: many lookups on a large posting must not be O(N) each.
#[test]
fn test_high_df_tf_lookup_is_sublinear() {
    // One term in 50_000 docs (ascending). A linear .position() lookup is O(N) per call;
    // rank() is sub-linear. Best-of-K guard (runner-noise tolerant, like perf_v0112).
    const N: u32 = 50_000;
    let mut store = PostingStore::new();
    for d in 0..N {
        store.add_term_occurrence(1, d, None);
    }
    let p = store.get_posting(1).expect("posting exists");

    // Sample lookups spread across the posting; assert correctness AND a sub-linear budget.
    let mut best = std::time::Duration::MAX;
    for _ in 0..5 {
        let t = std::time::Instant::now();
        let mut acc: u64 = 0;
        let mut d = 0u32;
        while d < N {
            acc += p.tf(d) as u64; // each tf is 1 here
            d += 7; // ~7_143 lookups
        }
        std::hint::black_box(acc);
        best = best.min(t.elapsed());
        if best < std::time::Duration::from_millis(50) {
            break;
        }
    }
    // ~7k rank-lookups over 50k docs must finish well under a linear-scan budget.
    assert!(
        best < std::time::Duration::from_millis(50),
        "high-DF TF lookups took {best:?} — expected sub-linear (rank), not O(N) per call"
    );
}

// ─────────────────────── integration: BM25 ranking through the store ───────────────────────

fn make_index() -> TextIndex {
    let field = TextFieldDef::new(Bytes::from_static(b"body"));
    TextIndex::new(
        Bytes::from_static(b"idx"),
        Vec::new(),
        vec![field],
        BM25Config::default(),
    )
}

fn idx_doc(idx: &mut TextIndex, key_hash: u64, key: &str, text: &str) {
    let args = vec![
        Frame::BulkString(Bytes::from_static(b"body")),
        Frame::BulkString(Bytes::copy_from_slice(text.as_bytes())),
    ];
    idx.index_document(key_hash, key.as_bytes(), &args);
}

fn result_keys(results: &[moon::text::store::TextSearchResult]) -> Vec<String> {
    results
        .iter()
        .map(|r| String::from_utf8_lossy(r.key.as_ref()).into_owned())
        .collect()
}

/// M3 (search_field / AND path) + M1 end-to-end — ranking must reflect the TRUE tfs after an update.
#[test]
fn test_search_field_ranking_correct_after_update() {
    // alpha counts: a=1 (doc_id 0), b=2 (1), c=3 (2). Then update a -> 5.
    let mut idx = make_index();
    idx_doc(&mut idx, 0, "a", "alpha");
    idx_doc(&mut idx, 1, "b", "alpha alpha");
    idx_doc(&mut idx, 2, "c", "alpha alpha alpha");
    idx_doc(&mut idx, 0, "a", "alpha alpha alpha alpha alpha"); // update a -> tf 5

    let results = idx.search_field(0, &["alpha".to_string()], None, None, 10);
    // TRUE BM25 order (len-normalised): a(tf5,dl5) > c(tf3,dl3) > b(tf2,dl2)  -> [a, c, b].
    // The misalignment bug reads tf (6,3,1) -> order [b, a, c]. Either way, a must rank first.
    let keys = result_keys(&results);
    assert_eq!(
        keys,
        vec!["a", "c", "b"],
        "ranking must reflect TRUE tfs (a=5,c=3,b=2)"
    );
}

/// M3 (search_field_or / OR-expanded path) — the second read site uses the same correct lookup.
#[test]
fn test_search_field_or_ranking_correct_after_update() {
    let mut idx = make_index();
    idx_doc(&mut idx, 0, "a", "alpha");
    idx_doc(&mut idx, 1, "b", "alpha alpha");
    idx_doc(&mut idx, 2, "c", "alpha alpha alpha");
    idx_doc(&mut idx, 0, "a", "alpha alpha alpha alpha alpha");

    // Resolve the indexed term_id for "alpha" (whatever stem the analyzer produced).
    let term_id = idx.field_term_dicts[0]
        .get("alpha")
        .expect("term 'alpha' indexed");
    let results = idx.search_field_or(0, &[term_id], None, None, 10);
    let keys = result_keys(&results);
    assert_eq!(
        keys.first().map(String::as_str),
        Some("a"),
        "OR path: a (tf=5) must rank first"
    );
}

/// M4 — no regression on the already-correct ascending-insert path (must STAY green).
#[test]
fn test_ascending_insert_ranking_unchanged() {
    let mut idx = make_index();
    idx_doc(&mut idx, 0, "a", "alpha alpha alpha"); // tf 3
    idx_doc(&mut idx, 1, "b", "alpha alpha"); // tf 2
    idx_doc(&mut idx, 2, "c", "alpha"); // tf 1
    // No updates — the path the old code already handled correctly.
    let results = idx.search_field(0, &["alpha".to_string()], None, None, 10);
    let keys = result_keys(&results);
    assert_eq!(
        keys,
        vec!["a", "b", "c"],
        "ascending-insert ranking by tf must be stable"
    );
}
