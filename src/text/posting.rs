/// Posting list storage for the BM25 inverted index.
///
/// Each term maps to a `PostingList` containing:
/// - `doc_ids`: RoaringBitmap of document IDs containing this term
/// - `term_freqs`: Per-document term frequency (parallel to doc_ids iteration order)
/// - `positions`: Optional per-document position lists (for phrase queries)
///
/// Positions are stored as `Option<Vec<Vec<u32>>>` per D-04: saves memory when
/// positions are not needed, but stores them from day one for future phrase
/// queries and HIGHLIGHT support.
use roaring::RoaringBitmap;
use smallvec::SmallVec;
use std::collections::HashMap;

/// A single term's posting data across all documents.
pub struct PostingList {
    /// Bitmap of document IDs containing this term.
    pub doc_ids: RoaringBitmap,
    /// Term frequency per document, indexed parallel to `doc_ids` iteration order.
    /// `term_freqs[i]` corresponds to the i-th document in `doc_ids`.
    pub term_freqs: Vec<u32>,
    /// Optional per-document position lists.
    /// When `Some`, `positions[i]` is the list of token positions for the i-th doc.
    /// When `None`, positions are not tracked (saves memory).
    pub positions: Option<Vec<Vec<u32>>>,
}

impl PostingList {
    /// Create a new empty posting list with position tracking.
    fn new_with_positions() -> Self {
        Self {
            doc_ids: RoaringBitmap::new(),
            term_freqs: Vec::new(),
            positions: Some(Vec::new()),
        }
    }

    /// Create a new empty posting list without position tracking.
    fn new_without_positions() -> Self {
        Self {
            doc_ids: RoaringBitmap::new(),
            term_freqs: Vec::new(),
            positions: None,
        }
    }

    /// 0-based index of `doc_id` within the rank-aligned parallel arrays.
    ///
    /// `RoaringBitmap::rank(d)` is the count of stored ids `<= d`, so for a present
    /// `doc_id` it is the 1-based sorted position; subtract one for the array index.
    /// Sub-linear (container-stride + popcount), unlike `iter().position()` (O(N)).
    #[inline]
    fn rank_index(&self, doc_id: u32) -> usize {
        (self.doc_ids.rank(doc_id) as usize).saturating_sub(1)
    }

    /// Term frequency of `doc_id` in this posting list.
    ///
    /// Returns the rank-aligned `term_freqs` entry when the doc is present, else `0`
    /// (the `tf_absent` default — BM25 treats the term as not occurring). Never panics:
    /// the rank-alignment invariant guarantees the index is valid, and a defensive
    /// `get` degrades to `0` rather than indexing out of bounds.
    #[inline]
    pub fn tf(&self, doc_id: u32) -> u32 {
        if !self.doc_ids.contains(doc_id) {
            return 0;
        }
        self.term_freqs
            .get(self.rank_index(doc_id))
            .copied()
            .unwrap_or(0)
    }

    /// Position list for `doc_id` (rank-aligned), or `None` when positions are not
    /// tracked or the doc is absent.
    #[inline]
    pub fn positions_for(&self, doc_id: u32) -> Option<&[u32]> {
        if !self.doc_ids.contains(doc_id) {
            return None;
        }
        let idx = self.rank_index(doc_id);
        self.positions
            .as_ref()
            .and_then(|p| p.get(idx))
            .map(Vec::as_slice)
    }
}

/// Fixed per-term overhead charged exactly once, when a term's `PostingList`
/// entry is first created in `postings` (K4 P0 fix: this entry is kept
/// forever even after its last document is removed -- see `remove_doc`'s doc
/// comment -- so the cost is charged once and never refunded, matching that
/// contract). Approximates the `HashMap<u32, PostingList>` bucket overhead
/// plus the `PostingList` struct shell (its growable contents are charged
/// separately via `POSTING_OCCURRENCE_COST`/`POSITION_COST`).
const POSTING_ENTRY_OVERHEAD: usize = 48 + std::mem::size_of::<PostingList>();

/// Fixed approximate cost of one (term, doc) occurrence: one `term_freqs`
/// `u32` slot plus an amortized per-id `RoaringBitmap` cost. A flat constant
/// -- not `RoaringBitmap::serialized_size()` -- because compressed bitmap
/// size is non-linear/non-additive across arbitrary insert/remove patterns
/// and cannot be delta-tracked in O(1); this is the same "monotonic signal,
/// not exact RSS" approximation style used by `ColdIndex`/`Database::
/// entry_overhead` elsewhere in the accounting spine.
const POSTING_OCCURRENCE_COST: usize = 4 + 4;

/// Fixed approximate cost of one tracked token position (`u32`).
const POSITION_COST: usize = std::mem::size_of::<u32>();

/// Per-field inverted index storing term_id -> PostingList.
pub struct PostingStore {
    postings: HashMap<u32, PostingList>,
    /// Reverse index: doc_id -> the term_ids that document contributed (a set, no duplicates).
    /// Lets `remove_doc` visit only a document's own terms instead of scanning every posting,
    /// making per-doc removal O(terms-in-doc) instead of O(total vocabulary) — the upsert/bulk
    /// re-index cliff (fts-upsert-incremental). Kept in sync with `postings`: `add_term_occurrence`
    /// records the edge on the new-doc branch; `remove_doc` erases the doc's entry.
    doc_terms: HashMap<u32, SmallVec<[u32; 8]>>,
    /// K4 (P0 fix): O(1) cached total mirroring `estimated_bytes()`.
    /// Maintained incrementally at every mutation site (`add_term_occurrence`,
    /// `remove_doc`) instead of being recomputed by a full walk on every read
    /// -- `estimated_bytes()` used to be an O(vocabulary) walk called
    /// unconditionally every 100ms from the shard eviction tick, which does
    /// not scale with corpus size. `estimated_bytes_ground_truth`
    /// (`#[cfg(test)]`) is the walk this field must always match.
    resident_bytes: usize,
}

impl PostingStore {
    /// Create an empty posting store.
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_terms: HashMap::new(),
            resident_bytes: 0,
        }
    }

    /// Add a term occurrence for a document.
    ///
    /// If the document already exists in the posting list, its term frequency
    /// is incremented and positions are appended (if provided).
    ///
    /// # Position handling
    /// - `positions: Some(pos)` -- store positions; upgrades a no-position list to have positions
    /// - `positions: None` -- don't track positions for this occurrence; keeps existing positions if any
    pub fn add_term_occurrence(&mut self, term_id: u32, doc_id: u32, positions: Option<Vec<u32>>) {
        let is_new_term = !self.postings.contains_key(&term_id);
        let posting = self.postings.entry(term_id).or_insert_with(|| {
            if positions.is_some() {
                PostingList::new_with_positions()
            } else {
                PostingList::new_without_positions()
            }
        });
        if is_new_term {
            self.resident_bytes += POSTING_ENTRY_OVERHEAD;
        }

        if posting.doc_ids.contains(doc_id) {
            // Existing doc: increment at the rank-aligned index.
            let idx = posting.rank_index(doc_id);
            posting.term_freqs[idx] += 1;
            // Append positions if provided.
            let mut added_positions = 0usize;
            if let Some(pos) = &positions {
                added_positions = pos.len();
                if let Some(pos_list) = &mut posting.positions {
                    pos_list[idx].extend_from_slice(pos);
                } else {
                    // Upgrade: create position tracking, aligned to current docs.
                    let mut pos_list = vec![Vec::new(); posting.term_freqs.len()];
                    pos_list[idx] = pos.clone();
                    posting.positions = Some(pos_list);
                }
            }
            self.resident_bytes += added_positions * POSITION_COST;
        } else {
            // New document: insert into the bitmap, then insert tf/positions AT THE RANK
            // INDEX (not push) so term_freqs/positions stay rank-aligned with doc_ids — correct
            // even when doc_id is not the current maximum (the document-update re-add path).
            posting.doc_ids.insert(doc_id);
            let idx = posting.rank_index(doc_id);
            posting.term_freqs.insert(idx, 1);
            let mut added_positions = 0usize;
            match (&mut posting.positions, &positions) {
                (Some(pos_list), Some(pos)) => {
                    added_positions = pos.len();
                    pos_list.insert(idx, pos.clone());
                }
                (Some(pos_list), None) => pos_list.insert(idx, Vec::new()),
                (None, Some(pos)) => {
                    // Upgrade: track positions for all docs; this doc's positions at idx.
                    added_positions = pos.len();
                    let mut pos_list = vec![Vec::new(); posting.term_freqs.len()];
                    pos_list[idx] = pos.clone();
                    posting.positions = Some(pos_list);
                }
                (None, None) => {}
            }
            // Record the (doc -> term) reverse edge exactly once: this branch fires only the first
            // time `doc_id` joins `term_id`'s posting, so no de-dup is needed. `posting`'s borrow of
            // `self.postings` has ended (last use above), so this disjoint-field access is sound.
            self.doc_terms.entry(doc_id).or_default().push(term_id);
            self.resident_bytes += POSTING_OCCURRENCE_COST + added_positions * POSITION_COST;
        }
    }

    /// Get a reference to a posting list for the given term.
    pub fn get_posting(&self, term_id: u32) -> Option<&PostingList> {
        self.postings.get(&term_id)
    }

    /// Number of documents containing the given term.
    pub fn doc_freq(&self, term_id: u32) -> u32 {
        self.postings
            .get(&term_id)
            .map(|p| p.doc_ids.len() as u32)
            .unwrap_or(0)
    }

    /// Number of unique terms in this store.
    pub fn term_count(&self) -> usize {
        self.postings.len()
    }

    /// Clear all postings for a specific document (used during upsert).
    ///
    /// Returns the old term frequencies `(term_id, old_tf)` for stats adjustment (order
    /// unspecified — callers only sum it). Visits ONLY the terms this document contributed via the
    /// `doc_terms` reverse map — O(terms-in-doc), not O(total vocabulary) — eliminating the upsert /
    /// bulk re-index cliff. The empty-posting entries are intentionally left in `postings` (a fully
    /// removed term keeps an empty `PostingList`), matching the prior O(V) implementation so
    /// `doc_freq`/`tf`/search output stay byte-identical.
    pub fn remove_doc(&mut self, doc_id: u32) -> Vec<(u32, u32)> {
        // absent_doc_noop: a doc never indexed has no reverse entry -> nothing to remove.
        let Some(term_ids) = self.doc_terms.remove(&doc_id) else {
            return Vec::new();
        };
        let mut removed = Vec::with_capacity(term_ids.len());
        for term_id in term_ids {
            // stale_reverse_entry_skip: defend against a reverse edge whose posting is gone or no
            // longer holds the doc — skip, never unwrap/expect/panic.
            let Some(posting) = self.postings.get_mut(&term_id) else {
                continue;
            };
            if !posting.doc_ids.contains(doc_id) {
                continue;
            }
            // Rank-aligned index — compute BEFORE removing from the bitmap.
            let idx = posting.rank_index(doc_id);
            if idx < posting.term_freqs.len() {
                let old_tf = posting.term_freqs.remove(idx);
                posting.doc_ids.remove(doc_id);
                let mut freed_positions = 0usize;
                if let Some(pos_list) = &mut posting.positions {
                    if idx < pos_list.len() {
                        freed_positions = pos_list[idx].len();
                        pos_list.remove(idx);
                    }
                }
                removed.push((term_id, old_tf));
                // K4 (P0 fix): symmetric uncharge for the occurrence + its positions
                // added by `add_term_occurrence`. The entry's `POSTING_ENTRY_OVERHEAD`
                // is deliberately NOT refunded here -- the `postings` map entry itself
                // survives (see below), matching the never-refunded charge on creation.
                self.resident_bytes = self
                    .resident_bytes
                    .saturating_sub(POSTING_OCCURRENCE_COST + freed_positions * POSITION_COST);
                // The `postings` HashMap entry itself is kept even when empty
                // (see doc comment on `remove_doc` — callers rely on
                // `tf`/`doc_freq` for a "term with zero live docs" staying
                // answerable without a fresh insert). But once the LAST doc
                // leaves, the entry's Vec buffers have no reason to keep
                // capacity sized for a document count of zero — release it.
                // Reallocation on the next occurrence of this term is a
                // one-time, bounded cost; the alternative is holding peak
                // capacity forever for a term that may never recur.
                if posting.doc_ids.is_empty() {
                    posting.term_freqs.shrink_to_fit();
                    if let Some(pos_list) = &mut posting.positions {
                        pos_list.shrink_to_fit();
                    }
                }
            }
        }
        removed
    }

    /// Reverse-map term_ids a document contributed (rank-unordered). `#[cfg(test)]` accessor.
    #[cfg(test)]
    pub(crate) fn doc_terms_for(&self, doc_id: u32) -> Option<&[u32]> {
        self.doc_terms.get(&doc_id).map(SmallVec::as_slice)
    }

    /// Number of distinct documents tracked in the reverse map. `#[cfg(test)]` accessor.
    #[cfg(test)]
    pub(crate) fn doc_terms_count(&self) -> usize {
        self.doc_terms.len()
    }

    /// Test-only: forcibly clear `doc_id` from `term_id`'s posting WITHOUT touching the reverse map,
    /// to synthesize the stale-reverse-entry state that `remove_doc` must tolerate.
    #[cfg(test)]
    pub(crate) fn test_force_clear_doc_from_posting(&mut self, term_id: u32, doc_id: u32) {
        if let Some(posting) = self.postings.get_mut(&term_id) {
            if posting.doc_ids.contains(doc_id) {
                let idx = posting.rank_index(doc_id);
                if idx < posting.term_freqs.len() {
                    posting.term_freqs.remove(idx);
                }
                posting.doc_ids.remove(doc_id);
                if let Some(p) = &mut posting.positions {
                    if idx < p.len() {
                        p.remove(idx);
                    }
                }
            }
        }
    }

    /// Estimated memory usage in bytes.
    ///
    /// K4 (P0 fix): O(1) cached read. This used to be an O(vocabulary) walk
    /// calling `RoaringBitmap::serialized_size()` per term -- fine as an
    /// occasional diagnostic, but this is invoked unconditionally every
    /// 100ms from the shard eviction tick (`persistence_tick.rs`), where an
    /// O(n) walk does not scale with corpus size. See
    /// `estimated_bytes_ground_truth` (`#[cfg(test)]`) for the equivalent
    /// full-walk formula this cached value must always match.
    #[must_use]
    pub fn estimated_bytes(&self) -> usize {
        self.resident_bytes
    }

    /// Ground-truth full recompute of `estimated_bytes()`, using the exact
    /// same fixed-cost formula as the incremental accumulator. Test-only:
    /// exists solely to assert the incremental accumulator never drifts from
    /// a from-scratch recount after a mixed mutation sequence.
    #[cfg(test)]
    pub(crate) fn estimated_bytes_ground_truth(&self) -> usize {
        let mut total = 0usize;
        for posting in self.postings.values() {
            total += POSTING_ENTRY_OVERHEAD;
            total += posting.doc_ids.len() as usize * POSTING_OCCURRENCE_COST;
            if let Some(ref pos_list) = posting.positions {
                for positions in pos_list {
                    total += positions.len() * POSITION_COST;
                }
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
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
        for doc_id in 0..500u32 {
            store.add_term_occurrence(7, doc_id, None);
        }
        let peak_cap = store.get_posting(7).unwrap().term_freqs.capacity();
        assert!(peak_cap >= 500, "expected growth to >=500, got {peak_cap}");

        for doc_id in 0..500u32 {
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
        for doc_id in 0..300u32 {
            store.add_term_occurrence(3, doc_id, Some(vec![doc_id]));
        }
        let peak_cap = store
            .get_posting(3)
            .unwrap()
            .positions
            .as_ref()
            .unwrap()
            .capacity();
        assert!(peak_cap >= 300);

        for doc_id in 0..300u32 {
            store.remove_doc(doc_id);
        }

        let posting = store.get_posting(3).unwrap();
        let pos_cap = posting.positions.as_ref().unwrap().capacity();
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
}
