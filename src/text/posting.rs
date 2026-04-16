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
}

/// Per-field inverted index storing term_id -> PostingList.
pub struct PostingStore {
    postings: HashMap<u32, PostingList>,
}

impl PostingStore {
    /// Create an empty posting store.
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
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
            // Upsert: find the index of this doc_id
            let idx = posting.doc_ids.iter().position(|id| id == doc_id);
            if let Some(idx) = idx {
                posting.term_freqs[idx] += 1;
                // Append positions if provided
                if let Some(pos) = &positions {
                    if let Some(pos_list) = &mut posting.positions {
                        pos_list[idx].extend_from_slice(pos);
                    } else {
                        // Upgrade: create position tracking
                        let mut pos_list = vec![Vec::new(); posting.term_freqs.len()];
                        pos_list[idx] = pos.clone();
                        posting.positions = Some(pos_list);
                    }
                }
            }
        } else {
            // New document
            posting.doc_ids.insert(doc_id);
            posting.term_freqs.push(1);
            match (&mut posting.positions, &positions) {
                (Some(pos_list), Some(pos)) => {
                    pos_list.push(pos.clone());
                }
                (Some(pos_list), None) => {
                    pos_list.push(Vec::new());
                }
                (None, Some(pos)) => {
                    // Upgrade: create position tracking for all existing docs
                    let mut pos_list = vec![Vec::new(); posting.term_freqs.len() - 1];
                    pos_list.push(pos.clone());
                    posting.positions = Some(pos_list);
                }
                (None, None) => {
                    // No positions, no change
                }
            }
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
    /// Returns the old term frequencies for stats adjustment.
    pub fn remove_doc(&mut self, doc_id: u32) -> Vec<(u32, u32)> {
        let mut removed = Vec::new();
        for (&term_id, posting) in &mut self.postings {
            if posting.doc_ids.contains(doc_id) {
                if let Some(idx) = posting.doc_ids.iter().position(|id| id == doc_id) {
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
        }
        removed
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
