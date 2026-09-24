//! moon#1195 red → green through PUBLIC APIs only (compiles unchanged against HEAD `935c555`,
//! where it fails): re-indexing the oldest document of a large posting must cost the same as the
//! newest — HEAD memmoved O(posting length) per term on the remove AND the re-insert.

use std::time::{Duration, Instant};

use moon::text::posting::{PostingList, PostingStore};

/// moon#1195. Six postings of 200K docs, bulk-loaded through the `.tpost` constructor.
#[test]
fn reindexing_the_oldest_doc_costs_the_same_as_the_newest() {
    const N: u32 = 200_000;
    const TERMS: u32 = 6;
    let doc_ids: Vec<u32> = (0..N).collect();
    let lists = (0..TERMS)
        .map(|t| {
            let tfs = (0..N).map(|d| 1 + (d + t) % 3).collect();
            let pos = (0..N)
                .map(|d| vec![t; (1 + (d + t) % 3) as usize])
                .collect();
            (
                t,
                PostingList::from_parts(&doc_ids, tfs, Some(pos)).expect("parts"),
            )
        })
        .collect();
    let mut store = PostingStore::from_lists(lists).expect("store");
    let upsert = |store: &mut PostingStore, d: u32| {
        let t0 = Instant::now();
        store.remove_doc(d);
        for t in 0..TERMS {
            store.add_term_occurrence(t, d, Some(vec![t]));
        }
        t0.elapsed()
    };
    let (mut first, mut last) = (Duration::MAX, Duration::MAX);
    for _ in 0..9 {
        first = first.min(upsert(&mut store, 0));
        last = last.min(upsert(&mut store, N - 1));
    }
    assert!(
        first <= last.max(Duration::from_micros(50)) * 4,
        "doc 0 took {first:?} vs doc N-1 {last:?}"
    );
    assert_eq!(store.doc_freq(0), N);
    assert_eq!(store.get_posting(0).map(|p| p.tf(0)), Some(1));
}
