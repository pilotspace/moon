//! Unit tests of `text::posting` (moved verbatim out of posting.rs to keep it under the
//! 1500-line cap, moon#1226).

use super::*;

/// RSS/CPU wave 5 (item A hygiene follow-up): a posting's `term_freqs`
/// (and `positions`, when tracked) grow to the peak document count ever
/// seen for that term. The `postings` HashMap entry is intentionally
/// kept forever once created (existing contract — see `remove_doc` doc
/// comment), but the per-entry `Vec` buffers must not hold onto peak
/// capacity once every document has been removed.
#[test]
fn remove_doc_shrinks_now_empty_posting_capacity() {
    let mut store = PostingStore::new();
    // Below FLAT_MAX: the flat-column capacity is what this test watches.
    for doc_id in 0..200u32 {
        store.add_term_occurrence(7, doc_id, None);
    }
    let peak_cap = store.get_posting(7).unwrap().term_freqs.capacity();
    assert!(peak_cap >= 200, "expected growth to >=200, got {peak_cap}");

    for doc_id in 0..200u32 {
        store.remove_doc(doc_id);
    }

    // Entry survives (existing contract) ...
    let posting = store.get_posting(7).expect("entry must survive removal");
    assert_eq!(posting.doc_ids.len(), 0);
    assert_eq!(posting.tf(0), 0);
    // ... but its buffer no longer holds peak capacity.
    assert!(
        posting.term_freqs.capacity() < peak_cap,
        "expected shrink after last doc removed: peak={peak_cap} still={}",
        posting.term_freqs.capacity()
    );
}

/// Same shrink must apply to the `positions` buffer when position
/// tracking is enabled for the term.
#[test]
fn remove_doc_shrinks_now_empty_posting_positions_capacity() {
    let mut store = PostingStore::new();
    for doc_id in 0..200u32 {
        store.add_term_occurrence(3, doc_id, Some(vec![doc_id]));
    }
    let cap = |p: &PostingList| {
        let col = p.positions.as_ref().unwrap();
        col.ends.capacity() + col.data.capacity()
    };
    let peak_cap = cap(store.get_posting(3).unwrap());
    assert!(peak_cap >= 400);

    for doc_id in 0..200u32 {
        store.remove_doc(doc_id);
    }

    let posting = store.get_posting(3).unwrap();
    let pos_cap = cap(posting);
    assert!(
        pos_cap < peak_cap,
        "expected positions shrink: peak={peak_cap} still={pos_cap}"
    );
}

/// A term that still has live documents after a removal must not be
/// touched by the shrink (only a fully-emptied posting shrinks).
#[test]
fn remove_doc_does_not_shrink_still_live_posting() {
    let mut store = PostingStore::new();
    for doc_id in 0..50u32 {
        store.add_term_occurrence(1, doc_id, None);
    }
    let cap_before = store.get_posting(1).unwrap().term_freqs.capacity();

    store.remove_doc(0); // one doc gone, 49 remain live

    let posting = store.get_posting(1).unwrap();
    assert_eq!(posting.doc_ids.len(), 49);
    assert_eq!(
        posting.term_freqs.capacity(),
        cap_before,
        "must not shrink while the posting still has live docs"
    );
}

/// K4 (P0 fix): RED-first — the O(1) incremental `resident_bytes`
/// accumulator maintained by `add_term_occurrence`/`remove_doc` must
/// never drift from a from-scratch ground-truth recompute, across a
/// mixed sequence of new terms, repeat occurrences (tf bump + position
/// append), a position-tracking upgrade, and both full and partial doc
/// removal (including the term_id-shared-across-docs case that leaves a
/// posting with live docs after another doc is removed).
#[test]
fn estimated_bytes_matches_ground_truth_after_mixed_mutations() {
    let mut store = PostingStore::new();
    assert_eq!(store.estimated_bytes(), 0);
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );

    // New terms, some with positions, some without.
    store.add_term_occurrence(1, 100, Some(vec![0, 3]));
    store.add_term_occurrence(2, 100, None);
    store.add_term_occurrence(3, 100, Some(vec![7]));
    store.add_term_occurrence(1, 101, Some(vec![1]));
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );

    // Repeat occurrence: tf bump + position append on an existing doc.
    store.add_term_occurrence(1, 100, Some(vec![5, 6]));
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );

    // Upgrade: term 2 had no position tracking, now gets one.
    store.add_term_occurrence(2, 101, Some(vec![2]));
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );

    // Shared term across many docs.
    for doc_id in 200..210u32 {
        store.add_term_occurrence(3, doc_id, Some(vec![doc_id]));
    }
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );

    // Partial removal: term 3 keeps live docs after doc 205 is removed.
    store.remove_doc(205);
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );

    // Full removal of a document touching multiple terms.
    store.remove_doc(100);
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );

    // Drain every remaining document -- resident_bytes must settle back
    // to the entry-overhead-only floor (never below it: entries survive
    // empty per the documented contract), matching ground truth exactly.
    for doc_id in [101, 200, 201, 202, 203, 204, 206, 207, 208, 209] {
        store.remove_doc(doc_id);
    }
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );
    assert_eq!(
        store.estimated_bytes(),
        3 * POSTING_ENTRY_OVERHEAD,
        "3 terms ever created, all doc occurrences drained -- only entry overhead remains"
    );
}

/// K4 (P0 fix): `estimated_bytes()` must be a pure O(1) load with no
/// iteration in the accessor -- enforced by construction here: the
/// accessor is called on a store sized large enough that an O(n) walk
/// would be trivially detectable by any reasonable wall-clock budget,
/// paired with the source-level guarantee that the method body is a
/// single field read (see the implementation above).
#[test]
fn estimated_bytes_is_o1_not_a_walk() {
    let mut store = PostingStore::new();
    for term_id in 0..5_000u32 {
        for doc_id in 0..20u32 {
            store.add_term_occurrence(term_id, doc_id, Some(vec![doc_id]));
        }
    }
    let start = std::time::Instant::now();
    for _ in 0..100_000 {
        std::hint::black_box(store.estimated_bytes());
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < std::time::Duration::from_millis(200),
        "100k reads of estimated_bytes() took {elapsed:?} -- looks like a walk, not O(1)"
    );
}

/// moon#1221 review: `PostingCursor::seek` agrees with `tf()` over roaring bitmap AND array
/// containers and across chunked columns, under forward seeks of every stride. Reused doc ids
/// (freed by FT.INVALIDATE_RANGE) make mid-posting inserts routine, so the cursor must hold on
/// any id layout, not only an append-only one.
#[test]
fn cursor_matches_tf_over_bitmap_and_array_containers() {
    let mut rng = Rng(0x5eed);
    let dense: Vec<u32> = (0..200_000u32)
        .filter(|d| d % 3 != 1 || d % 7 == 0)
        .collect();
    let sparse: Vec<u32> = (0..4_000u32).map(|i| i * 997).collect();
    let mut mixed: Vec<u32> = (0..70_000u32).filter(|d| d % 5 != 2).collect();
    mixed.extend((0..3_000u32).map(|i| 70_000 + i * 613));
    let mut checked = 0usize;
    for ids in [dense, sparse, mixed] {
        let tfs: Vec<u32> = ids.iter().map(|d| 1 + (d * 7 + d / 3) % 11).collect();
        let p = PostingList::from_parts(&ids, tfs, None).expect("parts");
        assert!(p.chunks.is_some(), "fixture must be chunked");
        let max = *ids.last().expect("non-empty") + 5_000;
        for trial in 0..60 {
            let mut c = p.cursor();
            let mut d = rng.below(64) as u32;
            while d < max {
                let tf = p.tf(d);
                assert_eq!(
                    c.seek(d),
                    (tf != 0).then_some(tf),
                    "trial {trial} seek({d})"
                );
                checked += 1;
                d += match rng.below(10) {
                    0..=4 => 1,
                    5 | 6 => 1 + rng.below(100) as u32,
                    7 | 8 => 1 + rng.below(3_000) as u32,
                    _ => 1 + rng.below(70_000) as u32,
                };
            }
        }
    }
    assert!(checked > 10_000, "only {checked} seeks");
}

// ── moon#1195: chunked rank-aligned columns ─────────────────────────────

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
        self.next() % n.max(1)
    }
}

type Model = std::collections::BTreeMap<u32, (u32, Vec<u32>)>;

/// Layout invariants of a chunked posting (flat postings trivially hold).
fn assert_layout(p: &PostingList) {
    if let Some(c) = &p.chunks {
        assert!(!c.runs.is_empty());
        assert_eq!(c.runs.len(), c.starts.len());
        assert_eq!(c.starts[0], 0);
        let mut expect = 0u32;
        for (run, &start) in c.runs.iter().zip(&c.starts) {
            assert_eq!(start, expect, "run starts are the running entry count");
            assert!(!run.tf.is_empty() && run.tf.len() <= RUN_MAX);
            // moon#1220: the memmove bound on position data (one entry may exceed it alone).
            assert!(
                run.pos.positions() <= RUN_POS_MAX || run.tf.len() == 1,
                "run of {} entries holds {} positions",
                run.tf.len(),
                run.pos.positions()
            );
            if p.has_positions() {
                assert_eq!(run.pos.len(), run.tf.len());
                assert_pos_column(&run.pos);
            } else {
                assert!(run.pos.len() == 0 && run.pos.data.is_empty());
            }
            expect += run.tf.len() as u32;
        }
        assert!(p.term_freqs.is_empty());
        assert!(
            p.positions
                .as_ref()
                .is_none_or(|col| col.len() == 0 && col.data.is_empty())
        );
        assert!(c.len() >= FLAT_MIN || c.positions() > FLAT_POS_MAX / 2);
    } else {
        assert!(p.term_freqs.len() <= FLAT_MAX);
        if let Some(col) = &p.positions {
            assert_eq!(col.len(), p.term_freqs.len());
            assert!(col.positions() <= FLAT_POS_MAX || col.len() <= 1);
            assert_pos_column(col);
        }
    }
}

/// `ends` is non-decreasing and ends exactly at the data length.
fn assert_pos_column(col: &PosColumn) {
    assert!(
        col.ends.windows(2).all(|w| w[0] <= w[1]),
        "ends non-decreasing"
    );
    assert_eq!(col.ends.last().map_or(0, |&e| e as usize), col.data.len());
}

/// Every read path of the posting agrees with the model.
fn assert_matches(store: &PostingStore, term: u32, model: &Model, universe: u32, rng: &mut Rng) {
    let p = store.get_posting(term).expect("posting");
    assert_layout(p);
    assert_eq!(p.doc_ids.len() as usize, model.len());
    assert_eq!(store.doc_freq(term) as usize, model.len());
    assert_eq!(
        p.tf_values().collect::<Vec<_>>(),
        model.values().map(|(tf, _)| *tf).collect::<Vec<_>>(),
        "tf column in rank order"
    );
    if p.has_positions() {
        assert_eq!(
            p.position_lists().map(<[u32]>::to_vec).collect::<Vec<_>>(),
            model
                .values()
                .map(|(_, pos)| pos.clone())
                .collect::<Vec<_>>(),
            "position column in rank order"
        );
    }
    for d in 0..universe {
        let want = model.get(&d);
        assert_eq!(p.tf(d), want.map_or(0, |(tf, _)| *tf), "tf({d})");
        if p.has_positions() {
            assert_eq!(p.positions_for(d), want.map(|(_, pos)| pos.as_slice()));
        }
    }
    // Cursor over a random ascending probe set (sparse and dense stretches).
    let mut cursor = p.cursor();
    let mut d = 0u32;
    while d < universe {
        let want = model.get(&d).map(|(tf, _)| *tf);
        assert_eq!(cursor.seek(d), want, "cursor seek({d})");
        d += 1 + if rng.below(4) == 0 {
            rng.below(600) as u32
        } else {
            0
        };
    }
    assert_eq!(
        store.estimated_bytes(),
        store.estimated_bytes_ground_truth()
    );
}

fn add(store: &mut PostingStore, model: &mut Model, term: u32, doc: u32, pos: u32) {
    store.add_term_occurrence(term, doc, Some(vec![pos]));
    let e = model.entry(doc).or_insert((0, Vec::new()));
    e.0 += 1;
    e.1.push(pos);
}

/// moon#1195: random churn across every layout transition — flat → chunked
/// (> FLAT_MAX), run splits (> RUN_MAX), run merges / removals (< RUN_MIN),
/// chunked → flat (< FLAT_MIN), a position-tracking upgrade while chunked —
/// with DISTINCT per-doc tfs (CONVENTIONS: equal tfs hide misalignment).
#[test]
fn chunked_columns_match_the_model_under_churn() {
    const T: u32 = 1; // dense, tracked
    const U: u32 = 2; // untracked until upgraded while chunked
    const N: u32 = 4_000;
    let mut rng = Rng(1195);
    let mut store = PostingStore::new();
    let mut model = Model::new();
    let mut order: Vec<u32> = (0..N).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    // Random-order inserts: crosses FLAT_MAX, then splits runs repeatedly.
    for (n, &d) in order.iter().enumerate() {
        for k in 0..=(d % 5) {
            add(&mut store, &mut model, T, d, d * 8 + k);
        }
        store.add_term_occurrence(U, d, None);
        if n % 997 == 0 {
            assert_matches(&store, T, &model, N, &mut rng);
        }
    }
    assert!(store.get_posting(T).is_some_and(|p| p.chunks.is_some()));
    assert_matches(&store, T, &model, N, &mut rng);

    // Upgrade the untracked, chunked posting U: every doc gets [] except one.
    assert!(!store.get_posting(U).is_some_and(PostingList::has_positions));
    store.add_term_occurrence(U, 17, Some(vec![99, 100]));
    let u = store.get_posting(U).expect("U");
    assert!(u.chunks.is_some() && u.has_positions());
    assert_layout(u);
    assert_eq!(u.positions_for(17), Some(&[99u32, 100][..]));
    assert_eq!(u.tf(17), 2);
    assert_eq!(u.positions_for(18), Some(&[][..]));

    // Remove a contiguous block (run merges/removals) and random docs.
    for d in 1_000..2_200 {
        store.remove_doc(d);
        model.remove(&d);
    }
    for _ in 0..800 {
        let d = rng.below(u64::from(N)) as u32;
        store.remove_doc(d);
        model.remove(&d);
    }
    assert_matches(&store, T, &model, N, &mut rng);
    // Re-add (upsert) old docs mid-posting with new, distinct tfs.
    for _ in 0..600 {
        let d = rng.below(u64::from(N)) as u32;
        store.remove_doc(d);
        model.remove(&d);
        for k in 0..(1 + rng.below(4) as u32) {
            add(&mut store, &mut model, T, d, 7 * k + d % 3);
        }
    }
    assert_matches(&store, T, &model, N, &mut rng);
    // Drain below FLAT_MIN: converts back to one flat run.
    let live: Vec<u32> = model.keys().copied().collect();
    for &d in live.iter().skip(FLAT_MIN - 14) {
        store.remove_doc(d);
        model.remove(&d);
    }
    assert!(store.get_posting(T).is_some_and(|p| p.chunks.is_none()));
    assert_matches(&store, T, &model, N, &mut rng);
}

/// moon#1220: postings whose documents repeat the term many times. Runs (and
/// the flat layout) are bounded by POSITION count too, so an insert, extend
/// or remove never memmoves more than about `RUN_POS_MAX` positions — a
/// single entry above the bound is a run of its own. Skewed, distinct tfs
/// (most 1–3, some hundreds, a few past `RUN_POS_MAX`), inserted batched
/// (`add_term_positions`, the indexing path) and extended per token
/// (`add_term_occurrence`), then removals, upserts and a drain; every read
/// path agrees with the model after each phase.
#[test]
fn high_tf_postings_respect_the_position_bound_under_churn() {
    const H: u32 = 9;
    const N: u32 = 3_000;
    let mut rng = Rng(1220);
    let mut store = PostingStore::new();
    let mut model = Model::new();
    let tf_of = |rng: &mut Rng, d: u32| -> u32 {
        match rng.below(100) {
            0 => RUN_POS_MAX as u32 + 1 + d % 700,
            1..=6 => 100 + (d * 7) % 300,
            _ => 1 + d % 3,
        }
    };
    let put = |store: &mut PostingStore, model: &mut Model, d: u32, tf: u32| {
        let positions: Vec<u32> = (0..tf).map(|k| d * 3 + k * 2).collect();
        store.add_term_positions(H, d, &positions);
        let e = model.entry(d).or_insert((0, Vec::new()));
        e.0 += tf;
        e.1.extend_from_slice(&positions);
    };
    let mut order: Vec<u32> = (0..N).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    for (n, &d) in order.iter().enumerate() {
        let tf = tf_of(&mut rng, d);
        put(&mut store, &mut model, d, tf);
        if n % 499 == 0 {
            assert_matches(&store, H, &model, N, &mut rng);
        }
    }
    let p = store.get_posting(H).expect("H");
    let c = p.chunks.as_ref().expect("chunked");
    assert!(
        c.runs
            .iter()
            .any(|r| r.tf.len() == 1 && r.pos.positions() > RUN_POS_MAX),
        "a single high-tf entry must form a run of its own"
    );
    assert!(
        c.runs.iter().any(|r| r.tf.len() < RUN_MAX / 2),
        "the position bound must have split runs below the entry bound"
    );
    assert_matches(&store, H, &model, N, &mut rng);
    // Per-token extends of existing entries (the pre-batching call shape).
    for _ in 0..400 {
        let d = order[rng.below(u64::from(N)) as usize];
        add(&mut store, &mut model, H, d, 1_000_000 + d);
    }
    assert_matches(&store, H, &model, N, &mut rng);
    // Removals, then upserts of old documents with new tfs.
    for _ in 0..1_200 {
        let d = rng.below(u64::from(N)) as u32;
        store.remove_doc(d);
        model.remove(&d);
    }
    assert_matches(&store, H, &model, N, &mut rng);
    for _ in 0..500 {
        let d = rng.below(u64::from(N)) as u32;
        store.remove_doc(d);
        model.remove(&d);
        let tf = tf_of(&mut rng, d);
        put(&mut store, &mut model, d, tf);
    }
    assert_matches(&store, H, &model, N, &mut rng);
    // Drain to a handful of LOW-tf docs: back to one flat run.
    let live: Vec<u32> = model.keys().copied().collect();
    for &d in &live {
        if model.len() <= 20 {
            break;
        }
        store.remove_doc(d);
        model.remove(&d);
    }
    let high: Vec<u32> = model
        .iter()
        .filter(|(_, (tf, _))| *tf > 3)
        .map(|(&d, _)| d)
        .collect();
    for d in high {
        store.remove_doc(d);
        model.remove(&d);
    }
    assert!(store.get_posting(H).is_some_and(|p| p.chunks.is_none()));
    assert_matches(&store, H, &model, N, &mut rng);
    // Batched and per-token indexing leave identical state.
    let mut batched = PostingStore::new();
    let mut per_token = PostingStore::new();
    for d in 0..600u32 {
        let tf = 1 + (d * 13) % 9 + if d % 97 == 0 { 3_000 } else { 0 };
        let positions: Vec<u32> = (0..tf).map(|k| k * 5 + d % 4).collect();
        batched.add_term_positions(4, d, &positions);
        for &p in &positions {
            per_token.add_term_occurrence(4, d, Some(vec![p]));
        }
    }
    let (a, b) = (
        batched.get_posting(4).expect("a"),
        per_token.get_posting(4).expect("b"),
    );
    assert_eq!(a.doc_ids, b.doc_ids);
    assert!(a.tf_values().eq(b.tf_values()));
    assert!(a.position_lists().eq(b.position_lists()));
    assert_eq!(batched.estimated_bytes(), per_token.estimated_bytes());
    assert_layout(a);
    assert_layout(b);
}

/// `from_parts` (the `.tpost` load path) builds the same columns as
/// incremental indexing, for a posting long enough to be chunked.
#[test]
fn from_parts_chunks_long_postings_identically() {
    let n = 5_000u32;
    let doc_ids: Vec<u32> = (0..n).map(|i| i * 3 + (i % 2)).collect();
    let tfs: Vec<u32> = (0..n).map(|i| 1 + i % 9).collect();
    let pos: Vec<Vec<u32>> = (0..n).map(|i| vec![i; (1 + i % 9) as usize]).collect();
    let p = PostingList::from_parts(&doc_ids, tfs.clone(), Some(pos.clone())).expect("parts");
    assert!(p.chunks.is_some());
    assert_layout(&p);
    assert_eq!(p.tf_values().collect::<Vec<_>>(), tfs);
    assert_eq!(
        p.position_lists().map(<[u32]>::to_vec).collect::<Vec<_>>(),
        pos
    );
    for (i, &d) in doc_ids.iter().enumerate() {
        assert_eq!(p.tf(d), tfs[i]);
        assert_eq!(p.positions_for(d), Some(pos[i].as_slice()));
    }
}

/// moon#1226 red test: runs left behind by a split hold what they store,
/// not the pre-split buffer. An ascending (append) load — the common
/// indexing order — writes only the last run, so HEAD's left halves kept
/// ~2x their tf / position capacity forever.
#[test]
fn split_runs_do_not_keep_the_pre_split_capacity() {
    let mut store = PostingStore::new();
    for d in 0..20_000u32 {
        let tf = 1 + d % 3;
        let positions: Vec<u32> = (0..tf).map(|k| k * 7 + d % 5).collect();
        store.add_term_positions(1, d, &positions);
    }
    let p = store.get_posting(1).expect("posting");
    assert_layout(p);
    let chunks = p.chunks.as_ref().expect("chunked");
    let settled = &chunks.runs[..chunks.runs.len() - 1]; // the last run is still growing
    assert!(settled.len() > 50, "fixture must split many times");
    let (mut len, mut cap) = (0usize, 0usize);
    for r in settled {
        len += r.tf.len() + r.pos.ends.len() + r.pos.data.len();
        cap += r.tf.capacity() + r.pos.ends.capacity() + r.pos.data.capacity();
    }
    assert!(
        cap * 10 <= len * 11,
        "settled runs hold {cap} u32 slots for {len} values ({:.2}x)",
        cap as f64 / len as f64
    );
}
