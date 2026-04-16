/// Text analyzer pipeline for BM25 full-text search.
///
/// Provides configurable tokenization through:
/// 1. NFKD Unicode normalization (decompose accented characters)
/// 2. Lowercase conversion
/// 3. Unicode word segmentation (UAX#29)
/// 4. Stop word removal
/// 5. Snowball stemming (optional, disabled via NOSTEM)
///
/// Feature-gated behind `text-index` for optional deps. When the feature
/// is disabled, a simple whitespace-based fallback is provided.
use std::collections::HashSet;

/// Configurable text analysis pipeline.
///
/// Created once per TEXT field (not per document) to amortize stemmer
/// construction cost. The stemmer is stored as an `Option` to support
/// the NOSTEM field modifier.
pub struct AnalyzerPipeline {
    /// Snowball stemmer instance (None when NOSTEM is set).
    #[cfg(feature = "text-index")]
    stemmer: Option<rust_stemmers::Stemmer>,
    /// Set of stop words to filter out during tokenization.
    stop_words: HashSet<String>,
    /// Minimum token length to keep (default 2).
    min_token_len: usize,
}

impl std::fmt::Debug for AnalyzerPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalyzerPipeline")
            .field("has_stemmer", &self.has_stemmer())
            .field("stop_words_count", &self.stop_words.len())
            .field("min_token_len", &self.min_token_len)
            .finish()
    }
}

impl AnalyzerPipeline {
    /// Create a new analyzer pipeline.
    ///
    /// # Arguments
    /// * `language` - Snowball stemmer language algorithm
    /// * `no_stem` - If true, skip stemming (NOSTEM modifier)
    #[cfg(feature = "text-index")]
    pub fn new(language: rust_stemmers::Algorithm, no_stem: bool) -> Self {
        let stemmer = if no_stem {
            None
        } else {
            Some(rust_stemmers::Stemmer::create(language))
        };

        let stop_words_slice = stop_words::get(stop_words::LANGUAGE::English);
        let stop_words: HashSet<String> =
            stop_words_slice.iter().map(|s| (*s).to_owned()).collect();

        Self {
            stemmer,
            stop_words,
            min_token_len: 2,
        }
    }

    /// Create a new analyzer pipeline (fallback when text-index feature disabled).
    #[cfg(not(feature = "text-index"))]
    pub fn new_fallback() -> Self {
        Self {
            stop_words: HashSet::new(),
            min_token_len: 2,
        }
    }

    /// Whether this pipeline has a stemmer configured.
    fn has_stemmer(&self) -> bool {
        #[cfg(feature = "text-index")]
        {
            self.stemmer.is_some()
        }
        #[cfg(not(feature = "text-index"))]
        {
            false
        }
    }

    /// Tokenize text into (stemmed_term, original_position) pairs.
    ///
    /// Pipeline:
    /// 1. NFKD normalize + strip combining marks
    /// 2. Lowercase
    /// 3. Unicode word segmentation with position tracking
    /// 4. Filter tokens shorter than min_token_len (position still advances)
    /// 5. Filter stop words (position still advances)
    /// 6. Apply stemmer if present
    ///
    /// Positions track the ORIGINAL word offsets, not the filtered output
    /// offsets. This is critical for phrase query proximity scoring.
    #[cfg(feature = "text-index")]
    pub fn tokenize_with_positions(&self, text: &str) -> Vec<(String, u32)> {
        use unicode_normalization::UnicodeNormalization;
        use unicode_segmentation::UnicodeSegmentation;

        // Step 1: NFKD normalize and strip combining marks
        let normalized: String = text
            .nfkd()
            .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
            .collect();

        // Step 2: Lowercase
        let lowered = normalized.to_lowercase();

        // Steps 3-6: Segment, filter, stem
        let mut result = Vec::new();
        for (pos, word) in lowered.unicode_words().enumerate() {
            // Step 4: Filter short tokens
            if word.len() < self.min_token_len {
                continue;
            }

            // Step 5: Filter stop words
            if self.stop_words.contains(word) {
                continue;
            }

            // Step 6: Stem
            let term = if let Some(ref stemmer) = self.stemmer {
                stemmer.stem(word).into_owned()
            } else {
                word.to_owned()
            };

            result.push((term, pos as u32));
        }

        result
    }

    /// Fallback tokenizer when text-index feature is disabled.
    /// Simple whitespace split with lowercase.
    #[cfg(not(feature = "text-index"))]
    pub fn tokenize_with_positions(&self, text: &str) -> Vec<(String, u32)> {
        text.split_whitespace()
            .enumerate()
            .filter(|(_, w)| w.len() >= self.min_token_len)
            .map(|(pos, w)| (w.to_lowercase(), pos as u32))
            .collect()
    }
}
