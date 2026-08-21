/// Full-text index for payload filtering.
///
/// Provides Unicode-aware tokenization (NFKD normalization, word segmentation,
/// Snowball stemming) and per-term RoaringBitmap storage for AND-semantics search.
///
/// Feature-gated behind `text-index` to avoid pulling unicode/stemming deps
/// when not needed.
#[cfg(feature = "text-index")]
pub use text_index_impl::TextIndex;

#[cfg(feature = "text-index")]
mod text_index_impl {
    use bytes::Bytes;
    use roaring::RoaringBitmap;
    use rust_stemmers::{Algorithm, Stemmer};
    use std::collections::HashMap;
    use std::sync::Arc;
    use unicode_normalization::UnicodeNormalization;
    use unicode_segmentation::UnicodeSegmentation;

    /// One field's index: the inverted map plus the forward map that makes
    /// removal proportional to the DOCUMENT rather than to the vocabulary.
    ///
    /// Both sides hold the SAME `Arc<str>` allocation for a term, so the
    /// forward map costs one pointer per (document, term) pair and never a
    /// second copy of the term text.
    #[derive(Default)]
    struct FieldIndex {
        /// stemmed term -> bitmap of doc_ids carrying it
        terms: HashMap<Arc<str>, RoaringBitmap>,
        /// doc_id -> the terms that document contributed
        ///
        /// Without this, removing one document meant walking every term in the
        /// field to ask "were you carrying it?" — and the answer was no for
        /// almost all of them.
        docs: HashMap<u32, Vec<Arc<str>>>,
    }

    /// Full-text index storing per-field, per-stemmed-term RoaringBitmaps.
    pub struct TextIndex {
        /// field_name -> that field's inverted + forward index
        indexes: HashMap<Bytes, FieldIndex>,
    }

    impl TextIndex {
        /// Create an empty text index.
        pub fn new() -> Self {
            Self {
                indexes: HashMap::new(),
            }
        }

        /// Tokenize and index a text value for the given field and doc_id.
        pub fn insert(&mut self, field: &Bytes, text: &[u8], doc_id: u32) {
            let text_str = match std::str::from_utf8(text) {
                Ok(s) => s,
                Err(_) => return, // Skip non-UTF8 text
            };
            let terms = Self::tokenize(text_str);
            let field_idx = self.indexes.entry(field.clone()).or_default();
            let carried = field_idx.docs.entry(doc_id).or_default();
            for term in terms {
                // Reuse the term's existing allocation when the field has
                // already seen it, so the forward map adds a pointer and not a
                // second copy of the string.
                let key: Arc<str> = match field_idx.terms.get_key_value(term.as_str()) {
                    Some((existing, _)) => Arc::clone(existing),
                    None => Arc::from(term.as_str()),
                };
                field_idx
                    .terms
                    .entry(Arc::clone(&key))
                    .or_default()
                    .insert(doc_id);
                carried.push(key);
            }
            // A term repeated within one document, or re-indexed by a later
            // insert for the same doc, must be retired exactly once.
            carried.sort_unstable();
            carried.dedup();
        }

        /// Search: return bitmap of docs matching ALL given stemmed terms (AND semantics).
        pub fn search(&self, field: &Bytes, terms: &[String]) -> RoaringBitmap {
            if terms.is_empty() {
                return RoaringBitmap::new();
            }
            let Some(field_idx) = self.indexes.get(field) else {
                return RoaringBitmap::new();
            };
            let mut result: Option<RoaringBitmap> = None;
            for term in terms {
                let bm = field_idx
                    .terms
                    .get(term.as_str())
                    .cloned()
                    .unwrap_or_default();
                result = Some(match result {
                    Some(existing) => existing & bm,
                    None => bm,
                });
            }
            result.unwrap_or_default()
        }

        /// Tokenize raw text into stemmed terms.
        ///
        /// Pipeline:
        /// 1. NFKD normalize (decompose accented characters)
        /// 2. Strip combining marks (diacritics)
        /// 3. Lowercase
        /// 4. Unicode word segmentation
        /// 5. Filter tokens < 2 chars
        /// 6. Snowball stem (English)
        pub fn tokenize(text: &str) -> Vec<String> {
            let stemmer = Stemmer::create(Algorithm::English);
            // NFKD normalize and strip combining marks
            let normalized: String = text
                .nfkd()
                .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
                .collect();
            let lowered = normalized.to_lowercase();
            lowered
                .unicode_words()
                .filter(|w| w.len() >= 2)
                .map(|w| stemmer.stem(w).into_owned())
                .collect()
        }

        /// Number of distinct terms currently indexed for `field`.
        ///
        /// This is the cost every removal and every search miss pays, so it is
        /// the quantity that must not grow without bound as documents come and
        /// go.
        pub fn vocabulary_len(&self, field: &Bytes) -> usize {
            self.indexes.get(field).map_or(0, |f| f.terms.len())
        }

        /// Remove an internal ID from the terms it actually carried in `field`.
        ///
        /// Costs one lookup per term THE DOCUMENT held, not one per term the
        /// field has ever seen. A term whose last carrier leaves is retired
        /// rather than left behind as an empty bitmap, so the vocabulary a
        /// search miss and a later removal must traverse tracks the live
        /// corpus instead of its whole history.
        pub fn remove_field(&mut self, field: &Bytes, internal_id: u32) {
            if let Some(field_idx) = self.indexes.get_mut(field) {
                Self::retire_doc(field_idx, internal_id);
            }
        }

        /// Remove an internal ID from every field.
        pub fn remove(&mut self, internal_id: u32) {
            for field_idx in self.indexes.values_mut() {
                Self::retire_doc(field_idx, internal_id);
            }
        }

        /// Drop `doc` from one field, retiring any term it was the last to
        /// carry.
        fn retire_doc(field_idx: &mut FieldIndex, doc: u32) {
            let Some(carried) = field_idx.docs.remove(&doc) else {
                // Not in this field. Nothing to walk — this is the case the
                // old vocabulary sweep paid full price for.
                return;
            };
            for term in carried {
                // `retain`-style prune: a term with no carriers left is dead
                // weight for every future search and removal.
                if let Some(bitmap) = field_idx.terms.get_mut(&term) {
                    bitmap.remove(doc);
                    if bitmap.is_empty() {
                        field_idx.terms.remove(&term);
                    }
                }
            }
        }
    }
}

#[cfg(all(test, feature = "text-index"))]
mod tests {
    use super::TextIndex;
    use bytes::Bytes;

    fn field(s: &str) -> Bytes {
        Bytes::from(s.to_owned())
    }

    /// moon T2: removing a document must retire the vocabulary it alone carried.
    ///
    /// The pre-fix index walked EVERY term bitmap in the field on removal and
    /// left the emptied ones in place, so the term map only ever grew: a field
    /// that had seen a million distinct terms kept paying for all million on
    /// every later removal, and on every search miss, long after the documents
    /// were gone. Unbounded growth with no reader — the same shape as moon#546.
    /// Cost of retiring documents, as the field vocabulary grows.
    ///
    /// `#[ignore]`d: this is a measurement, not an assertion — it exists so the
    /// claim in the commit message is reproducible rather than asserted. Run:
    /// `cargo test --release --lib bench_removal_cost -- --ignored --nocapture`
    #[test]
    #[ignore = "measurement harness; run explicitly with --nocapture"]
    fn bench_removal_cost_vs_vocabulary() {
        use std::time::Instant;
        let f = field("desc");
        println!("docs  vocab   remove-all");
        for docs in [250_u32, 500, 1000, 2000] {
            let mut idx = TextIndex::new();
            for d in 0..docs {
                // 10 terms per doc, all distinct across the corpus, so the
                // vocabulary grows with the corpus exactly as a real text
                // field's does.
                let text: String = (0..10).map(|t| format!("term{d}x{t} ")).collect();
                idx.insert(&f, text.as_bytes(), d);
            }
            let vocab = idx.vocabulary_len(&f);
            let t0 = Instant::now();
            for d in 0..docs {
                idx.remove_field(&f, d);
            }
            let el = t0.elapsed();
            println!("{docs:<5} {vocab:<7} {el:?}");
        }
    }

    #[test]
    fn test_removal_retires_terms_no_document_carries() {
        let mut idx = TextIndex::new();
        let f = field("desc");
        idx.insert(&f, b"alpha bravo charlie", 0);
        idx.insert(&f, b"alpha delta echo", 1);
        assert_eq!(
            idx.vocabulary_len(&f),
            5,
            "alpha bravo charli delta echo (stemmed)"
        );

        // Doc 0 leaves. `alpha` is still carried by doc 1 and must survive;
        // `bravo`/`charlie` belonged to doc 0 alone and must be gone.
        idx.remove_field(&f, 0);
        assert_eq!(
            idx.vocabulary_len(&f),
            3,
            "bravo and charli were carried only by doc 0 and must be retired"
        );
        let alpha = TextIndex::tokenize("alpha");
        assert!(
            idx.search(&f, &alpha).contains(1),
            "doc 1 still carries alpha; retiring doc 0 must not disturb it"
        );

        // Last document leaves: the field's vocabulary must be empty, not a
        // graveyard of empty bitmaps.
        idx.remove_field(&f, 1);
        assert_eq!(
            idx.vocabulary_len(&f),
            0,
            "every document is gone; no term may remain"
        );
    }

    /// The same guarantee for the all-fields path used by vector deletion.
    #[test]
    fn test_remove_all_fields_retires_vocabulary() {
        let mut idx = TextIndex::new();
        idx.insert(&field("title"), b"quantum entanglement", 7);
        idx.insert(&field("body"), b"spooky action distance", 7);
        idx.insert(&field("body"), b"spooky season", 8);

        idx.remove(7);
        assert_eq!(idx.vocabulary_len(&field("title")), 0, "title emptied");
        assert_eq!(
            idx.vocabulary_len(&field("body")),
            2,
            "doc 8 still carries spooki and season"
        );
        let spooky = TextIndex::tokenize("spooky");
        let hits = idx.search(&field("body"), &spooky);
        assert!(hits.contains(8) && !hits.contains(7));
    }

    #[test]
    fn test_text_index_basic_insert_and_search() {
        let mut idx = TextIndex::new();
        idx.insert(&field("desc"), b"Machine Learning models for NLP", 0);
        idx.insert(&field("desc"), b"Deep learning neural networks", 1);
        idx.insert(&field("desc"), b"Database indexing strategies", 2);

        // "learning" stems to "learn"
        let terms = TextIndex::tokenize("learning");
        let bm = idx.search(&field("desc"), &terms);
        assert!(bm.contains(0), "doc 0 should match 'learning'");
        assert!(bm.contains(1), "doc 1 should match 'learning'");
        assert!(!bm.contains(2), "doc 2 should NOT match 'learning'");
    }

    #[test]
    fn test_text_index_and_semantics() {
        let mut idx = TextIndex::new();
        idx.insert(&field("desc"), b"Machine learning models", 0);
        idx.insert(&field("desc"), b"Machine vision systems", 1);
        idx.insert(&field("desc"), b"Learning algorithms", 2);

        // "machine learning" -> both "machin" and "learn" must match
        let terms = TextIndex::tokenize("machine learning");
        let bm = idx.search(&field("desc"), &terms);
        assert!(bm.contains(0), "doc 0 has both 'machine' and 'learning'");
        assert!(!bm.contains(1), "doc 1 has 'machine' but not 'learning'");
        assert!(!bm.contains(2), "doc 2 has 'learning' but not 'machine'");
    }

    #[test]
    fn test_text_index_stemming() {
        let mut idx = TextIndex::new();
        idx.insert(&field("desc"), b"The runners are running fast", 0);
        idx.insert(&field("desc"), b"She runs every morning", 1);
        idx.insert(&field("desc"), b"The cat sat on the mat", 2);

        // "run" should match docs with "runners", "running", "runs"
        let terms = TextIndex::tokenize("run");
        let bm = idx.search(&field("desc"), &terms);
        assert!(bm.contains(0), "doc 0 has 'runners'/'running'");
        assert!(bm.contains(1), "doc 1 has 'runs'");
        assert!(!bm.contains(2), "doc 2 has no run-related words");
    }

    #[test]
    fn test_text_index_unicode_normalization() {
        let mut idx = TextIndex::new();
        // "cafe\u{0301}" is "café" with combining accent
        idx.insert(&field("desc"), "caf\u{e9} latte".as_bytes(), 0);
        idx.insert(&field("desc"), "caf\u{0065}\u{0301} mocha".as_bytes(), 1);

        // Both should normalize to "cafe"
        let terms = TextIndex::tokenize("cafe");
        let bm = idx.search(&field("desc"), &terms);
        assert!(bm.contains(0), "precomposed cafe should match");
        assert!(bm.contains(1), "decomposed cafe should match");
    }

    #[test]
    fn test_text_index_empty_query() {
        let mut idx = TextIndex::new();
        idx.insert(&field("desc"), b"some text", 0);

        let bm = idx.search(&field("desc"), &[]);
        assert!(bm.is_empty(), "empty query should return empty bitmap");
    }

    #[test]
    fn test_text_index_unknown_field() {
        let idx = TextIndex::new();
        let terms = TextIndex::tokenize("hello");
        let bm = idx.search(&field("nonexistent"), &terms);
        assert!(bm.is_empty(), "unknown field should return empty bitmap");
    }

    #[test]
    fn test_text_index_remove() {
        let mut idx = TextIndex::new();
        idx.insert(&field("desc"), b"machine learning", 0);
        idx.insert(&field("desc"), b"machine vision", 1);

        idx.remove(0);

        let terms = TextIndex::tokenize("machine");
        let bm = idx.search(&field("desc"), &terms);
        assert!(!bm.contains(0), "doc 0 should be removed");
        assert!(bm.contains(1), "doc 1 should still exist");
    }

    #[test]
    fn test_text_index_remove_field() {
        let mut idx = TextIndex::new();
        idx.insert(&field("desc"), b"machine learning", 0);
        idx.insert(&field("title"), b"machine vision", 0);

        idx.remove_field(&field("desc"), 0);

        let terms = TextIndex::tokenize("machine");
        let bm_desc = idx.search(&field("desc"), &terms);
        assert!(bm_desc.is_empty(), "desc field should be empty for doc 0");

        let bm_title = idx.search(&field("title"), &terms);
        assert!(bm_title.contains(0), "title field should still have doc 0");
    }

    #[test]
    fn test_tokenize_short_words_filtered() {
        // Words < 2 chars should be filtered out
        let terms = TextIndex::tokenize("I am a big cat");
        // "I", "a" should be filtered; "am", "big", "cat" should remain (after stemming)
        assert!(!terms.iter().any(|t| t.len() < 2));
        assert!(terms.len() >= 2, "should have at least 'big' and 'cat'");
    }
}
