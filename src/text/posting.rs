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

/// Per-field inverted index storing term_id -> PostingList.
pub struct PostingStore {
    postings: HashMap<u32, PostingList>,
    /// Reverse index: doc_id -> the term_ids that document contributed (a set, no duplicates).
    /// Lets `remove_doc` visit only a document's own terms instead of scanning every posting,
    /// making per-doc removal O(terms-in-doc) instead of O(total vocabulary) — the upsert/bulk
    /// re-index cliff (fts-upsert-incremental). Kept in sync with `postings`: `add_term_occurrence`
    /// records the edge on the new-doc branch; `remove_doc` erases the doc's entry.
    doc_terms: HashMap<u32, SmallVec<[u32; 8]>>,
}

impl PostingStore {
    /// Create an empty posting store.
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_terms: HashMap::new(),
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
        let posting = self.postings.entry(term_id).or_insert_with(|| {
            if positions.is_some() {
                PostingList::new_with_positions()
            } else {
                PostingList::new_without_positions()
            }
        });

        if posting.doc_ids.contains(doc_id) {
            // Existing doc: increment at the rank-aligned index.
            let idx = posting.rank_index(doc_id);
            posting.term_freqs[idx] += 1;
            // Append positions if provided.
            if let Some(pos) = &positions {
                if let Some(pos_list) = &mut posting.positions {
                    pos_list[idx].extend_from_slice(pos);
                } else {
                    // Upgrade: create position tracking, aligned to current docs.
                    let mut pos_list = vec![Vec::new(); posting.term_freqs.len()];
                    pos_list[idx] = pos.clone();
                    posting.positions = Some(pos_list);
                }
            }
        } else {
            // New document: insert into the bitmap, then insert tf/positions AT THE RANK
            // INDEX (not push) so term_freqs/positions stay rank-aligned with doc_ids — correct
            // even when doc_id is not the current maximum (the document-update re-add path).
            posting.doc_ids.insert(doc_id);
            let idx = posting.rank_index(doc_id);
            posting.term_freqs.insert(idx, 1);
            match (&mut posting.positions, &positions) {
                (Some(pos_list), Some(pos)) => pos_list.insert(idx, pos.clone()),
                (Some(pos_list), None) => pos_list.insert(idx, Vec::new()),
                (None, Some(pos)) => {
                    // Upgrade: track positions for all docs; this doc's positions at idx.
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
                if let Some(pos_list) = &mut posting.positions {
                    if idx < pos_list.len() {
                        pos_list.remove(idx);
                    }
                }
                removed.push((term_id, old_tf));
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
    pub fn estimated_bytes(&self) -> usize {
        let mut total = 0;
        for posting in self.postings.values() {
            total += posting.doc_ids.serialized_size();
            total += posting.term_freqs.len() * 4;
            if let Some(ref pos_list) = posting.positions {
                for positions in pos_list {
                    total += positions.len() * 4;
                }
                total += pos_list.len() * std::mem::size_of::<Vec<u32>>();
            }
        }
        total
    }
}
