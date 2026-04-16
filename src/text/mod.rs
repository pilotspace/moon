/// BM25 full-text search engine.
///
/// Provides per-shard text indexing with configurable analyzer pipelines,
/// Okapi BM25 scoring, RoaringBitmap-based posting lists, and a TextStore
/// registry mirroring the VectorStore pattern.
///
/// # Module structure
///
/// - `types` — Field definitions and BM25 configuration
/// - `analyzer` — Tokenization pipeline (NFKD, stemming, stop words)
/// - `bm25` — Scoring function and field statistics
/// - `posting` — Inverted index posting lists with RoaringBitmap
/// - `term_dict` — Mutable term-to-ID dictionary
/// - `store` — TextStore and TextIndex per-shard registry

pub mod analyzer;
pub mod bm25;
pub mod posting;
pub mod store;
pub mod term_dict;
pub mod types;

#[cfg(test)]
mod tests {
    use super::analyzer::AnalyzerPipeline;
    use super::bm25::{bm25_score, FieldStats};
    use super::posting::PostingStore;
    use super::term_dict::TermDictionary;
    use super::types::{BM25Config, TextFieldDef};

    // ===== Analyzer tests =====

    #[cfg(feature = "text-index")]
    #[test]
    fn test_analyzer_stop_word_removal() {
        let analyzer = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, false);
        let tokens = analyzer.tokenize_with_positions("The quick brown fox");
        // "The" -> "the" -> stop word, removed
        let terms: Vec<&str> = tokens.iter().map(|(t, _)| t.as_str()).collect();
        assert!(!terms.contains(&"the"), "stop word 'the' should be removed");
        // "quick", "brown", "fox" should remain (possibly stemmed)
        assert!(tokens.len() >= 3, "should have at least 3 non-stop tokens, got: {:?}", tokens);
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn test_analyzer_positions_preserved() {
        let analyzer = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, false);
        let tokens = analyzer.tokenize_with_positions("The quick brown fox");
        // "The" is at position 0 but removed (stop word)
        // "quick" is at original position 1
        // "brown" is at original position 2
        // "fox" is at original position 3
        // Positions reflect ORIGINAL word offsets
        let positions: Vec<u32> = tokens.iter().map(|(_, p)| *p).collect();
        assert!(positions.contains(&1), "quick should be at position 1");
        assert!(positions.contains(&2), "brown should be at position 2");
        assert!(positions.contains(&3), "fox should be at position 3");
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn test_analyzer_stemming() {
        let analyzer = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, false);
        let tokens = analyzer.tokenize_with_positions("Running runs ran");
        let terms: Vec<&str> = tokens.iter().map(|(t, _)| t.as_str()).collect();
        // "running" and "runs" should both stem to "run"
        assert_eq!(terms.iter().filter(|t| **t == "run").count(), 2,
            "running and runs should both stem to 'run', got: {:?}", terms);
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn test_analyzer_nostem() {
        let analyzer = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, true);
        let tokens = analyzer.tokenize_with_positions("Running runs");
        let terms: Vec<&str> = tokens.iter().map(|(t, _)| t.as_str()).collect();
        // Without stemming, terms should be lowercase but not stemmed
        assert!(terms.contains(&"running"), "without stemming, 'running' stays as is, got: {:?}", terms);
        assert!(terms.contains(&"runs"), "without stemming, 'runs' stays as is, got: {:?}", terms);
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn test_analyzer_nfkd_normalization() {
        let analyzer = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, true);
        // "cafe\u{0301}" = "cafe" + combining acute accent -> NFKD strips the accent
        let tokens = analyzer.tokenize_with_positions("caf\u{0065}\u{0301}");
        let terms: Vec<&str> = tokens.iter().map(|(t, _)| t.as_str()).collect();
        assert!(terms.contains(&"cafe"), "NFKD should normalize cafe\\u0301 to cafe, got: {:?}", terms);
    }

    // ===== BM25 tests =====

    #[test]
    fn test_bm25_score_typical() {
        // tf=1, df=10, N=1000, dl=100, avgdl=120, k1=1.2, b=0.75
        let score = bm25_score(1.0, 10, 1000, 100, 120.0, 1.2, 0.75);
        // Manual calculation:
        // IDF = ln((1000 - 10 + 0.5) / (10 + 0.5) + 1.0) = ln(990.5/10.5 + 1.0) = ln(95.333) = 4.557
        // tf_norm = (1.0 * 2.2) / (1.0 + 1.2 * (0.25 + 0.75 * 100/120))
        //         = 2.2 / (1.0 + 1.2 * (0.25 + 0.625)) = 2.2 / (1.0 + 1.05) = 2.2 / 2.05 = 1.073
        // score = 4.557 * 1.073 = 4.889
        assert!((score - 4.889).abs() < 0.1, "BM25 score should be ~4.89, got {}", score);
    }

    #[test]
    fn test_bm25_score_zero_tf() {
        let score = bm25_score(0.0, 10, 1000, 100, 120.0, 1.2, 0.75);
        assert_eq!(score, 0.0, "zero term frequency should return 0.0");
    }

    #[test]
    fn test_bm25_score_zero_docs() {
        let score = bm25_score(1.0, 0, 0, 100, 120.0, 1.2, 0.75);
        assert_eq!(score, 0.0, "zero documents should return 0.0");
    }

    #[test]
    fn test_bm25_score_zero_df() {
        let score = bm25_score(1.0, 0, 1000, 100, 120.0, 1.2, 0.75);
        assert_eq!(score, 0.0, "zero document frequency should return 0.0");
    }

    #[test]
    fn test_field_stats_default() {
        let stats = FieldStats::new();
        assert_eq!(stats.num_docs, 0);
        assert_eq!(stats.total_field_length, 0);
    }

    #[test]
    fn test_field_stats_avg_doc_len_zero_docs() {
        let stats = FieldStats::new();
        assert_eq!(stats.avg_doc_len(), 0.0, "avg_doc_len should be 0.0 when num_docs is 0");
    }

    #[test]
    fn test_field_stats_avg_doc_len() {
        let stats = FieldStats {
            num_docs: 10,
            total_field_length: 500,
        };
        assert!((stats.avg_doc_len() - 50.0).abs() < f32::EPSILON);
    }

    // ===== PostingStore tests =====

    #[test]
    fn test_posting_store_add_and_query() {
        let mut store = PostingStore::new();
        store.add_term_occurrence(0, 1, Some(vec![0, 5]));
        store.add_term_occurrence(0, 2, Some(vec![3]));

        assert_eq!(store.doc_freq(0), 2, "term 0 should be in 2 documents");
        assert_eq!(store.doc_freq(99), 0, "unknown term should have 0 doc freq");

        let posting = store.get_posting(0).expect("posting should exist");
        assert!(posting.doc_ids.contains(1));
        assert!(posting.doc_ids.contains(2));
        assert_eq!(posting.term_freqs.len(), 2);
    }

    #[test]
    fn test_posting_store_upsert() {
        let mut store = PostingStore::new();
        store.add_term_occurrence(0, 1, Some(vec![0]));
        store.add_term_occurrence(0, 1, Some(vec![5]));

        assert_eq!(store.doc_freq(0), 1, "upsert should not duplicate doc_id");
        let posting = store.get_posting(0).expect("posting should exist");
        assert_eq!(posting.term_freqs[0], 2, "term freq should increment on upsert");
        if let Some(ref positions) = posting.positions {
            assert_eq!(positions[0], vec![0, 5], "positions should be appended");
        } else {
            panic!("positions should be Some");
        }
    }

    #[test]
    fn test_posting_store_no_positions() {
        let mut store = PostingStore::new();
        store.add_term_occurrence(0, 1, None);
        store.add_term_occurrence(0, 2, None);

        let posting = store.get_posting(0).expect("posting should exist");
        assert!(posting.positions.is_none(), "positions should be None when not provided");
        assert_eq!(posting.term_freqs.len(), 2);
    }

    #[test]
    fn test_posting_store_position_upgrade() {
        let mut store = PostingStore::new();
        // First occurrence without positions
        store.add_term_occurrence(0, 1, None);
        // Second doc with positions -> upgrade
        store.add_term_occurrence(0, 2, Some(vec![3]));

        let posting = store.get_posting(0).expect("posting should exist");
        assert!(posting.positions.is_some(), "positions should be upgraded to Some");
    }

    #[test]
    fn test_posting_store_term_count() {
        let mut store = PostingStore::new();
        store.add_term_occurrence(0, 1, None);
        store.add_term_occurrence(1, 1, None);
        store.add_term_occurrence(2, 2, None);

        assert_eq!(store.term_count(), 3, "should have 3 unique terms");
    }

    // ===== TermDictionary tests =====

    #[test]
    fn test_term_dict_get_or_insert() {
        let mut dict = TermDictionary::new();
        let id1 = dict.get_or_insert("hello");
        let id2 = dict.get_or_insert("world");
        let id3 = dict.get_or_insert("hello");

        assert_eq!(id1, id3, "same term should return same id");
        assert_ne!(id1, id2, "different terms should have different ids");
        assert_eq!(dict.term_count(), 2, "should have 2 unique terms");
    }

    #[test]
    fn test_term_dict_get() {
        let mut dict = TermDictionary::new();
        dict.get_or_insert("hello");

        assert!(dict.get("hello").is_some());
        assert!(dict.get("world").is_none());
    }

    // ===== Types tests =====

    #[test]
    fn test_bm25_config_default() {
        let config = BM25Config::default();
        assert!((config.k1 - 1.2).abs() < f32::EPSILON);
        assert!((config.b - 0.75).abs() < f32::EPSILON);
    }

    #[test]
    fn test_text_field_def_defaults() {
        let field = TextFieldDef::new(bytes::Bytes::from_static(b"title"));
        assert_eq!(field.weight, 1.0);
        assert!(!field.nostem);
        assert!(!field.sortable);
        assert!(!field.noindex);
    }
}
