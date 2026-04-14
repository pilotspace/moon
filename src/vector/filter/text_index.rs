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
    use unicode_normalization::UnicodeNormalization;
    use unicode_segmentation::UnicodeSegmentation;

    /// Full-text index storing per-field, per-stemmed-term RoaringBitmaps.
    pub struct TextIndex {
        /// field_name -> { stemmed_term -> bitmap of doc_ids }
        indexes: HashMap<Bytes, HashMap<String, RoaringBitmap>>,
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
            let field_map = self.indexes.entry(field.clone()).or_default();
            for term in terms {
                field_map.entry(term).or_default().insert(doc_id);
            }
        }

        /// Search: return bitmap of docs matching ALL given stemmed terms (AND semantics).
        pub fn search(&self, field: &Bytes, terms: &[String]) -> RoaringBitmap {
            if terms.is_empty() {
                return RoaringBitmap::new();
            }
            let Some(field_map) = self.indexes.get(field) else {
                return RoaringBitmap::new();
            };
            let mut result: Option<RoaringBitmap> = None;
            for term in terms {
                let bm = field_map.get(term).cloned().unwrap_or_default();
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

        /// Remove an internal ID from all term bitmaps for a given field.
        pub fn remove_field(&mut self, field: &Bytes, internal_id: u32) {
            if let Some(field_map) = self.indexes.get_mut(field) {
                for bitmap in field_map.values_mut() {
                    bitmap.remove(internal_id);
                }
            }
        }

        /// Remove an internal ID from ALL fields' bitmaps.
        pub fn remove(&mut self, internal_id: u32) {
            for field_map in self.indexes.values_mut() {
                for bitmap in field_map.values_mut() {
                    bitmap.remove(internal_id);
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
