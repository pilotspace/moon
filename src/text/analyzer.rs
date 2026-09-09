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

/// RediSearch's default English stop-word list — the 33 words a RediSearch index drops
/// unless `FT.CREATE ... STOPWORDS` overrides it.
///
/// moon#690: this replaced `stop_words::get(stop_words::LANGUAGE::English)`, which resolves
/// to the **stopwords-iso** list — 1,298 entries including `hello`, `world`, `test`, `name`,
/// `order`, `open` and `index`. Those never reached the index, so a document whose only word
/// was `hello` indexed zero terms and was unreachable by its own content, with no error and
/// no warning to go on.
///
/// The list is spelled out here rather than pulled from a crate so that what moon silently
/// discards is greppable, reviewable, and cannot change under a dependency bump.
pub const DEFAULT_STOP_WORDS: [&str; 33] = [
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with",
];

/// Shortest word either consumer keeps. Both pipelines used `2` before they
/// were unified; it is the one length rule they share, and
/// `AnalyzerPipeline::min_token_len` must not go below it (see `terms_from`).
pub const MIN_TOKEN_LEN: usize = 2;

// Deterministic probe: how many normalize + segment passes this thread has run
// over field text (moon#885). One per `AnalyzedText` — the unit of work the
// text plane and the vector payload index used to each pay separately.
//
// Thread-local by design: a shard is one thread, so the count a test reads
// around one `auto_index_hset` call is exactly that call's work and no other
// test's. Read it with `segment_passes`.
#[cfg(feature = "text-index")]
thread_local! {
    static SEGMENT_PASSES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Number of normalize + segment passes run on the calling thread so far.
#[cfg(feature = "text-index")]
pub fn segment_passes() -> u64 {
    SEGMENT_PASSES.with(std::cell::Cell::get)
}

#[cfg(feature = "text-index")]
pub(crate) fn note_segment_pass() {
    SEGMENT_PASSES.with(|c| c.set(c.get() + 1));
}

/// Configurable text analysis pipeline.
///
/// Created once per TEXT field (not per document) to amortize stemmer
/// construction cost. The stemmer is stored as an `Option` to support
/// the NOSTEM field modifier.
pub struct AnalyzerPipeline {
    /// Snowball stemmer instance (None when NOSTEM is set).
    #[cfg(feature = "text-index")]
    stemmer: Option<rust_stemmers::Stemmer>,
    /// True when `stemmer` is the English Snowball stemmer — the one the
    /// vector payload index also uses — so a term this pipeline stems can be
    /// handed to that consumer instead of stemmed a second time (moon#885).
    #[cfg(feature = "text-index")]
    english_stem: bool,
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

        let stop_words: HashSet<String> =
            DEFAULT_STOP_WORDS.iter().map(|s| (*s).to_owned()).collect();

        Self {
            english_stem: stemmer.is_some() && language == rust_stemmers::Algorithm::English,
            stemmer,
            stop_words,
            // Coupled to `AnalyzedText::segment`, which drops words shorter
            // than `MIN_TOKEN_LEN` before any pipeline sees them: a value
            // below it here would be silently ineffective (`terms_from`
            // debug-asserts the bound).
            min_token_len: MIN_TOKEN_LEN,
        }
    }

    /// Create a new analyzer pipeline (fallback when text-index feature disabled).
    #[cfg(not(feature = "text-index"))]
    pub fn new_fallback() -> Self {
        Self {
            stop_words: HashSet::new(),
            // Same coupling as `new`: the shared segment pass keeps nothing
            // shorter than `MIN_TOKEN_LEN`.
            min_token_len: MIN_TOKEN_LEN,
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
        let mut analysis = AnalyzedText::segment(text);
        self.terms_from(&mut analysis)
            .into_iter()
            .map(|(term, pos)| (term.into_owned(), pos))
            .collect()
    }

    /// This field's terms — stop words dropped, stemmed per the field's
    /// NOSTEM setting, positions as ORIGINAL word ordinals — read off a
    /// shared [`AnalyzedText`] instead of a private normalize + segment pass.
    ///
    /// Same output as [`tokenize_with_positions`](Self::tokenize_with_positions)
    /// (that method is implemented on top of this one). The difference is
    /// what it leaves behind: when this pipeline stems with English Snowball,
    /// every non-stop word's stem is recorded in `analysis`, so a later
    /// [`AnalyzedText::english_terms`] call by the payload index stems only
    /// the stop words this pipeline skipped. Under NOSTEM nothing is
    /// recorded and nothing is stemmed — a text-only NOSTEM field costs
    /// exactly what it did before.
    ///
    /// Borrowed terms point into `analysis`; an owned term appears only for a
    /// non-English stemmer, which no current caller constructs.
    #[cfg(feature = "text-index")]
    pub fn terms_from<'a>(
        &self,
        analysis: &'a mut AnalyzedText,
    ) -> Vec<(std::borrow::Cow<'a, str>, u32)> {
        // `segment` already dropped everything shorter than `MIN_TOKEN_LEN`;
        // a pipeline minimum below that could never take effect.
        debug_assert!(
            self.min_token_len >= MIN_TOKEN_LEN,
            "min_token_len {} is below the shared segment floor {MIN_TOKEN_LEN}",
            self.min_token_len
        );
        // Phase 1 (mutable): decide which words survive and, for an English
        // stemmed field, fill their shared stem slots. Indices are kept so
        // phase 2 can hand out borrows without re-checking the stop list.
        let mut kept: Vec<u32> = Vec::with_capacity(analysis.word_count());
        for i in 0..analysis.word_count() {
            let word = analysis.word(i);
            if word.len() < self.min_token_len || self.stop_words.contains(word) {
                continue;
            }
            kept.push(i as u32);
        }
        if let (true, Some(stemmer)) = (self.english_stem, &self.stemmer) {
            for &i in &kept {
                analysis.ensure_english(i as usize, stemmer);
            }
        }

        // Phase 2 (shared): hand out the terms.
        let analysis: &'a AnalyzedText = analysis;
        kept.into_iter()
            .map(|i| {
                let i = i as usize;
                let pos = analysis.position(i);
                let term = match &self.stemmer {
                    None => std::borrow::Cow::Borrowed(analysis.word(i)),
                    Some(_) if self.english_stem => {
                        std::borrow::Cow::Borrowed(analysis.english_term(i))
                    }
                    Some(other) => {
                        std::borrow::Cow::Owned(other.stem(analysis.word(i)).into_owned())
                    }
                };
                (term, pos)
            })
            .collect()
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

/// One normalize + segment pass over a field value, shared by the two things
/// that need it: the BM25 text plane and the vector payload index (moon#885).
///
/// Holds the NFKD-normalized, mark-stripped, lowercased text once, the byte
/// span and ordinal of every word at least [`MIN_TOKEN_LEN`] bytes long, and
/// a lazily filled English Snowball stem per word. Nothing is filtered or
/// stemmed here: stop words and NOSTEM are the consumers' business, and each
/// applies its own rule to the same words. Stems are filled on demand and
/// remembered, so a word is stemmed at most once no matter how many
/// consumers ask.
#[cfg(feature = "text-index")]
pub struct AnalyzedText {
    /// Normalized, lowercased text; every word span points into it.
    lowered: String,
    spans: Vec<Span>,
    /// Index-aligned with `spans`.
    english: Vec<StemSlot>,
}

#[cfg(feature = "text-index")]
#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
    /// Ordinal among ALL segmented words, short ones included — the position
    /// contract phrase queries depend on.
    pos: u32,
}

#[cfg(feature = "text-index")]
enum StemSlot {
    /// Nobody has needed this word's English stem yet.
    Pending,
    /// Snowball returned the word unchanged; read `AnalyzedText::word`.
    Unchanged,
    Stemmed(String),
}

#[cfg(feature = "text-index")]
impl AnalyzedText {
    /// Normalize (NFKD, strip combining marks, lowercase) and segment
    /// (UAX#29 words, drop those shorter than [`MIN_TOKEN_LEN`]). This is the
    /// pass [`segment_passes`] counts.
    pub fn segment(text: &str) -> Self {
        use unicode_normalization::UnicodeNormalization;
        use unicode_segmentation::UnicodeSegmentation;

        note_segment_pass();
        let normalized: String = text
            .nfkd()
            .filter(|c| !unicode_normalization::char::is_combining_mark(*c))
            .collect();
        // `str::to_lowercase`, not per-char: it applies the final-sigma rule.
        let lowered = normalized.to_lowercase();
        let base = lowered.as_ptr() as usize;
        let mut spans = Vec::new();
        for (pos, word) in lowered.unicode_words().enumerate() {
            if word.len() < MIN_TOKEN_LEN {
                continue;
            }
            let start = word.as_ptr() as usize - base;
            spans.push(Span {
                start,
                end: start + word.len(),
                pos: pos as u32,
            });
        }
        let english = spans.iter().map(|_| StemSlot::Pending).collect();
        Self {
            lowered,
            spans,
            english,
        }
    }

    /// Words kept (at least [`MIN_TOKEN_LEN`] bytes), in text order.
    pub fn word_count(&self) -> usize {
        self.spans.len()
    }

    /// The normalized, lowercased surface form of word `i`.
    pub fn word(&self, i: usize) -> &str {
        let span = self.spans[i];
        &self.lowered[span.start..span.end]
    }

    /// Ordinal of word `i` among all segmented words.
    pub fn position(&self, i: usize) -> u32 {
        self.spans[i].pos
    }

    fn ensure_english(&mut self, i: usize, english: &rust_stemmers::Stemmer) {
        if matches!(self.english[i], StemSlot::Pending) {
            self.english[i] = match english.stem(self.word(i)) {
                std::borrow::Cow::Borrowed(_) => StemSlot::Unchanged,
                std::borrow::Cow::Owned(stem) => StemSlot::Stemmed(stem),
            };
        }
    }

    /// English stem of word `i`. Callers fill the slot first; a `Pending`
    /// slot reads as the surface form rather than panicking.
    fn english_term(&self, i: usize) -> &str {
        match &self.english[i] {
            StemSlot::Stemmed(stem) => stem.as_str(),
            StemSlot::Unchanged | StemSlot::Pending => self.word(i),
        }
    }

    /// Every word, English-stemmed — the payload index's contract: no
    /// stop-word list, no NOSTEM. Stems any slot still pending and reuses
    /// the ones a text-plane field already filled.
    pub fn english_terms(&mut self) -> impl Iterator<Item = &str> + '_ {
        let english = rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English);
        for i in 0..self.spans.len() {
            self.ensure_english(i, &english);
        }
        let this: &Self = self;
        (0..this.spans.len()).map(move |i| this.english_term(i))
    }
}

/// The analyses one HSET's field values have already been through, so the
/// second consumer of a value finds the first one's work (moon#885).
///
/// Lives for one `auto_index_hset` call. Keyed by the VALUE, never the field
/// name: the text plane indexes the first occurrence of a duplicated field
/// while the payload index sees every pair, and two fields with identical
/// bytes analyze identically anyway. Lookup tries pointer identity first —
/// both consumers borrow the same `Frame` — and falls back to a byte
/// compare, so a `Bytes` copied out of the frame still hits.
#[cfg(feature = "text-index")]
#[derive(Default)]
pub struct AnalysisCache {
    entries: Vec<(bytes::Bytes, AnalyzedText)>,
    /// Slot to overwrite once `MAX_ENTRIES` is reached — a plain rotation;
    /// past the cap a value may be analyzed twice, never wrongly.
    next_evict: usize,
}

#[cfg(feature = "text-index")]
impl AnalysisCache {
    /// Upper bound on remembered values: a HASH with more fields than this
    /// simply loses the oldest analyses. Bounds the memory one HSET can pin.
    pub const MAX_ENTRIES: usize = 64;

    pub fn new() -> Self {
        Self::default()
    }

    /// The analysis of `value`, run now if this is its first consumer.
    /// `None` when `value` is not UTF-8 — both consumers skip such fields.
    pub fn get_or_segment(&mut self, value: &bytes::Bytes) -> Option<&mut AnalyzedText> {
        let hit = self.entries.iter().position(|(cached, _)| {
            (cached.as_ptr() == value.as_ptr() && cached.len() == value.len()) || cached == value
        });
        let slot = match hit {
            Some(i) => i,
            None => {
                let analysis = AnalyzedText::segment(std::str::from_utf8(value).ok()?);
                if self.entries.len() < Self::MAX_ENTRIES {
                    self.entries.push((value.clone(), analysis));
                    self.entries.len() - 1
                } else {
                    let i = self.next_evict;
                    self.next_evict = (i + 1) % Self::MAX_ENTRIES;
                    self.entries[i] = (value.clone(), analysis);
                    i
                }
            }
        };
        Some(&mut self.entries[slot].1)
    }
}

/// Stand-in when `text-index` is off: there is no payload text index to
/// share with, so nothing is cached and the text plane's whitespace fallback
/// runs as before.
#[cfg(not(feature = "text-index"))]
#[derive(Default)]
pub struct AnalysisCache;

#[cfg(not(feature = "text-index"))]
impl AnalysisCache {
    pub fn new() -> Self {
        Self
    }
}
