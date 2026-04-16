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
pub mod fst_dict;
pub mod index_persist;
pub mod posting;
pub mod store;
pub mod term_dict;
pub mod types;

#[cfg(test)]
mod tests {
    #[cfg(feature = "text-index")]
    use super::analyzer::AnalyzerPipeline;
    use super::bm25::{FieldStats, bm25_score};
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
        assert!(
            tokens.len() >= 3,
            "should have at least 3 non-stop tokens, got: {:?}",
            tokens
        );
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
        assert_eq!(
            terms.iter().filter(|t| **t == "run").count(),
            2,
            "running and runs should both stem to 'run', got: {:?}",
            terms
        );
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn test_analyzer_nostem() {
        let analyzer = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, true);
        let tokens = analyzer.tokenize_with_positions("Running runs");
        let terms: Vec<&str> = tokens.iter().map(|(t, _)| t.as_str()).collect();
        // Without stemming, terms should be lowercase but not stemmed
        assert!(
            terms.contains(&"running"),
            "without stemming, 'running' stays as is, got: {:?}",
            terms
        );
        assert!(
            terms.contains(&"runs"),
            "without stemming, 'runs' stays as is, got: {:?}",
            terms
        );
    }

    #[cfg(feature = "text-index")]
    #[test]
    fn test_analyzer_nfkd_normalization() {
        let analyzer = AnalyzerPipeline::new(rust_stemmers::Algorithm::English, true);
        // "cafe\u{0301}" = "cafe" + combining acute accent -> NFKD strips the accent
        let tokens = analyzer.tokenize_with_positions("caf\u{0065}\u{0301}");
        let terms: Vec<&str> = tokens.iter().map(|(t, _)| t.as_str()).collect();
        assert!(
            terms.contains(&"cafe"),
            "NFKD should normalize cafe\\u0301 to cafe, got: {:?}",
            terms
        );
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
        assert!(
            (score - 4.889).abs() < 0.1,
            "BM25 score should be ~4.89, got {}",
            score
        );
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
        assert_eq!(
            stats.avg_doc_len(),
            0.0,
            "avg_doc_len should be 0.0 when num_docs is 0"
        );
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
        assert_eq!(
            posting.term_freqs[0], 2,
            "term freq should increment on upsert"
        );
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
        assert!(
            posting.positions.is_none(),
            "positions should be None when not provided"
        );
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
        assert!(
            posting.positions.is_some(),
            "positions should be upgraded to Some"
        );
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

    // ===== TextStore / TextIndex integration tests =====

    #[cfg(feature = "text-index")]
    mod store_tests {
        use super::super::store::{TextIndex, TextStore};
        use super::super::types::{BM25Config, TextFieldDef};
        use crate::protocol::Frame;
        use bytes::Bytes;

        /// Helper: create HSET-style args as [field1, value1, field2, value2, ...]
        fn hset_args(pairs: &[(&str, &str)]) -> Vec<Frame> {
            let mut args = Vec::new();
            for (field, value) in pairs {
                args.push(Frame::BulkString(Bytes::copy_from_slice(field.as_bytes())));
                args.push(Frame::BulkString(Bytes::copy_from_slice(value.as_bytes())));
            }
            args
        }

        fn make_title_body_index() -> TextIndex {
            let title_field = TextFieldDef {
                field_name: Bytes::from_static(b"title"),
                weight: 2.0,
                nostem: false,
                sortable: false,
                noindex: false,
            };
            let body_field = TextFieldDef::new(Bytes::from_static(b"body"));
            TextIndex::new(
                Bytes::from_static(b"article_idx"),
                vec![Bytes::from_static(b"doc:")],
                vec![title_field, body_field],
                BM25Config::default(),
            )
        }

        #[test]
        fn test_text_index_basic_document() {
            let mut idx = make_title_body_index();
            let args = hset_args(&[
                ("title", "Machine Learning for NLP"),
                (
                    "body",
                    "This article covers modern NLP techniques using deep learning",
                ),
            ]);
            let key = b"doc:1";
            let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
            idx.index_document(key_hash, key, &args);

            assert_eq!(idx.num_docs(), 1, "should have 1 document");
            assert!(idx.num_terms() > 0, "should have indexed some terms");
            // Both fields should have stats
            assert_eq!(
                idx.field_stats[0].num_docs, 1,
                "title field should have 1 doc"
            );
            assert_eq!(
                idx.field_stats[1].num_docs, 1,
                "body field should have 1 doc"
            );
            assert!(
                idx.field_stats[0].total_field_length > 0,
                "title should have tokens"
            );
            assert!(
                idx.field_stats[1].total_field_length > 0,
                "body should have tokens"
            );
        }

        #[test]
        fn test_text_index_upsert_no_double_count() {
            let mut idx = make_title_body_index();
            let key = b"doc:1";
            let key_hash = xxhash_rust::xxh64::xxh64(key, 0);

            // First insert
            let args1 = hset_args(&[
                ("title", "Original Title"),
                ("body", "Original body content here"),
            ]);
            idx.index_document(key_hash, key, &args1);
            let avg_after_first = idx.field_stats[0].avg_doc_len();

            // Upsert same key_hash
            let args2 = hset_args(&[
                ("title", "Updated Title"),
                ("body", "Updated body content here with more words"),
            ]);
            idx.index_document(key_hash, key, &args2);

            assert_eq!(idx.num_docs(), 1, "upsert should not increase doc count");
            assert_eq!(
                idx.field_stats[0].num_docs, 1,
                "field stats num_docs should stay 1"
            );
            // avg_doc_len should reflect the new document, not accumulate old + new
            let avg_after_upsert = idx.field_stats[0].avg_doc_len();
            assert!(
                avg_after_upsert > 0.0,
                "avg_doc_len should be positive after upsert"
            );
            // The title lengths are similar so avg should be close (not doubled)
            assert!(
                (avg_after_upsert - avg_after_first).abs() < 5.0,
                "avg_doc_len should not drift significantly: first={}, upsert={}",
                avg_after_first,
                avg_after_upsert
            );
        }

        #[test]
        fn test_text_index_noindex_field() {
            let mut idx = TextIndex::new(
                Bytes::from_static(b"test_idx"),
                vec![],
                vec![
                    TextFieldDef::new(Bytes::from_static(b"title")),
                    TextFieldDef {
                        field_name: Bytes::from_static(b"internal"),
                        weight: 1.0,
                        nostem: false,
                        sortable: false,
                        noindex: true, // This field should NOT be indexed
                    },
                ],
                BM25Config::default(),
            );

            let args = hset_args(&[
                ("title", "Indexed Content"),
                ("internal", "This should not be indexed"),
            ]);
            let key_hash = xxhash_rust::xxh64::xxh64(b"key:1", 0);
            idx.index_document(key_hash, b"key:1", &args);

            // Title field should be indexed
            assert_eq!(idx.field_stats[0].num_docs, 1);
            assert!(idx.field_stats[0].total_field_length > 0);

            // NOINDEX field should have empty stats
            assert_eq!(idx.field_stats[1].num_docs, 0);
            assert_eq!(idx.field_stats[1].total_field_length, 0);
            assert_eq!(idx.field_postings[1].term_count(), 0);
        }

        #[test]
        fn test_text_index_multiple_documents() {
            let mut idx = make_title_body_index();

            for i in 0..5 {
                let key = format!("doc:{}", i);
                let title = format!("Article number {} about testing", i);
                let body = format!("Body content for article {} with some words", i);
                let args = hset_args(&[("title", &title), ("body", &body)]);
                let key_hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
                idx.index_document(key_hash, key.as_bytes(), &args);
            }

            assert_eq!(idx.num_docs(), 5, "should have 5 documents");
            assert_eq!(idx.field_stats[0].num_docs, 5);
            assert_eq!(idx.field_stats[1].num_docs, 5);
        }

        #[test]
        fn test_text_store_prefix_matching() {
            let mut store = TextStore::new();
            let idx1 = TextIndex::new(
                Bytes::from_static(b"article_idx"),
                vec![Bytes::from_static(b"article:")],
                vec![TextFieldDef::new(Bytes::from_static(b"title"))],
                BM25Config::default(),
            );
            let idx2 = TextIndex::new(
                Bytes::from_static(b"blog_idx"),
                vec![Bytes::from_static(b"blog:")],
                vec![TextFieldDef::new(Bytes::from_static(b"title"))],
                BM25Config::default(),
            );

            store
                .create_index(Bytes::from_static(b"article_idx"), idx1)
                .expect("create");
            store
                .create_index(Bytes::from_static(b"blog_idx"), idx2)
                .expect("create");

            let matches = store.find_matching_index_names(b"article:1");
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].as_ref(), b"article_idx");

            let matches = store.find_matching_index_names(b"blog:hello");
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].as_ref(), b"blog_idx");

            let matches = store.find_matching_index_names(b"other:key");
            assert!(matches.is_empty(), "no prefix match for unrelated key");
        }

        #[test]
        fn test_text_store_duplicate_index() {
            let mut store = TextStore::new();
            let idx = TextIndex::new(
                Bytes::from_static(b"idx"),
                vec![],
                vec![TextFieldDef::new(Bytes::from_static(b"f"))],
                BM25Config::default(),
            );
            store
                .create_index(Bytes::from_static(b"idx"), idx)
                .expect("first create");

            let idx2 = TextIndex::new(
                Bytes::from_static(b"idx"),
                vec![],
                vec![TextFieldDef::new(Bytes::from_static(b"f"))],
                BM25Config::default(),
            );
            assert!(
                store
                    .create_index(Bytes::from_static(b"idx"), idx2)
                    .is_err()
            );
        }

        #[test]
        fn test_text_store_drop_index() {
            let mut store = TextStore::new();
            let idx = TextIndex::new(
                Bytes::from_static(b"idx"),
                vec![],
                vec![TextFieldDef::new(Bytes::from_static(b"f"))],
                BM25Config::default(),
            );
            store
                .create_index(Bytes::from_static(b"idx"), idx)
                .expect("create");
            assert_eq!(store.index_count(), 1);

            assert!(store.drop_index(b"idx"));
            assert_eq!(store.index_count(), 0);
            assert!(!store.drop_index(b"idx"));
        }

        #[test]
        fn test_text_store_empty_prefix_matches_all() {
            let mut store = TextStore::new();
            let idx = TextIndex::new(
                Bytes::from_static(b"all_idx"),
                vec![], // Empty prefix = match all
                vec![TextFieldDef::new(Bytes::from_static(b"title"))],
                BM25Config::default(),
            );
            store
                .create_index(Bytes::from_static(b"all_idx"), idx)
                .expect("create");

            let matches = store.find_matching_index_names(b"any:key");
            assert_eq!(matches.len(), 1, "empty prefix should match any key");
        }

        #[test]
        fn test_text_index_nostem_preserves_forms() {
            // NOSTEM field should NOT stem terms — "running" stays "running"
            let idx_stemmed = TextIndex::new(
                Bytes::from_static(b"stemmed_idx"),
                vec![],
                vec![TextFieldDef::new(Bytes::from_static(b"content"))],
                BM25Config::default(),
            );
            let idx_nostem = TextIndex::new(
                Bytes::from_static(b"nostem_idx"),
                vec![],
                vec![TextFieldDef {
                    field_name: Bytes::from_static(b"content"),
                    weight: 1.0,
                    nostem: true,
                    sortable: false,
                    noindex: false,
                }],
                BM25Config::default(),
            );

            let mut store = TextStore::new();
            store
                .create_index(Bytes::from_static(b"stemmed_idx"), idx_stemmed)
                .expect("create");
            store
                .create_index(Bytes::from_static(b"nostem_idx"), idx_nostem)
                .expect("create");

            let args = hset_args(&[("content", "running runs")]);
            let key_hash = xxhash_rust::xxh64::xxh64(b"doc:1", 0);

            // Index same document in both indexes
            store
                .get_index_mut(b"stemmed_idx")
                .expect("idx")
                .index_document(key_hash, b"doc:1", &args);
            store
                .get_index_mut(b"nostem_idx")
                .expect("idx")
                .index_document(key_hash, b"doc:1", &args);

            // Stemmed index: "running" and "runs" both become "run" -> 1 unique term
            let stemmed = store.get_index(b"stemmed_idx").expect("idx");
            // NOSTEM index: "running" and "runs" stay distinct -> 2 unique terms
            let nostem = store.get_index(b"nostem_idx").expect("idx");
            assert!(
                nostem.num_terms() > stemmed.num_terms(),
                "NOSTEM should have more unique terms ({}) than stemmed ({}) because 'running' and 'runs' stay distinct",
                nostem.num_terms(),
                stemmed.num_terms()
            );
        }

        #[test]
        fn test_text_index_three_docs_stats() {
            // Index 3 documents, verify num_docs==3, num_terms>0, avg_doc_len>0
            let mut idx = make_title_body_index();
            let docs = [
                (
                    "doc:1",
                    "Machine Learning",
                    "An introduction to ML techniques",
                ),
                (
                    "doc:2",
                    "Deep Learning",
                    "Neural networks and deep architectures",
                ),
                (
                    "doc:3",
                    "Natural Language",
                    "NLP processing with transformers",
                ),
            ];
            for (key, title, body) in &docs {
                let args = hset_args(&[("title", title), ("body", body)]);
                let key_hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
                idx.index_document(key_hash, key.as_bytes(), &args);
            }

            assert_eq!(idx.num_docs(), 3, "should have 3 documents");
            assert!(idx.num_terms() > 0, "should have indexed terms");
            assert!(
                idx.field_stats[0].avg_doc_len() > 0.0,
                "title avg_doc_len should be > 0"
            );
            assert!(
                idx.field_stats[1].avg_doc_len() > 0.0,
                "body avg_doc_len should be > 0"
            );
            assert!(idx.total_posting_bytes() > 0, "posting bytes should be > 0");
        }

        /// Validates the fixed auto_index_hset guard: TEXT-only index with
        /// no matching vector indexes still triggers text indexing.
        #[test]
        fn test_auto_index_text_only_guard() {
            // Create a TextStore with a TEXT-only index
            let mut text_store = TextStore::new();
            let text_index = TextIndex::new(
                Bytes::from_static(b"testidx"),
                vec![Bytes::from_static(b"doc:")],
                vec![TextFieldDef::new(Bytes::from_static(b"title"))],
                BM25Config::default(),
            );
            text_store
                .create_index(Bytes::from_static(b"testidx"), text_index)
                .expect("create text index");

            // Create an empty VectorStore (no vector indexes at all)
            let mut vector_store = crate::vector::store::VectorStore::new();

            // Build HSET-style args: [key, field, value, ...] matching real RESP layout.
            // auto_index_hset receives args with key at args[0].
            let args = vec![
                Frame::BulkString(Bytes::from_static(b"doc:1")),
                Frame::BulkString(Bytes::from_static(b"title")),
                Frame::BulkString(Bytes::copy_from_slice(b"Machine Learning for NLP")),
            ];

            // Call auto_index_hset_public with empty VectorStore
            crate::shard::spsc_handler::auto_index_hset_public(
                &mut vector_store,
                &mut text_store,
                b"doc:1",
                &args,
            );

            // Verify TEXT indexing fired despite no vector index matches
            let idx = text_store.get_index(b"testidx").expect("index exists");
            assert_eq!(
                idx.num_docs(),
                1,
                "TEXT-only index should have 1 doc after HSET via auto_index_hset"
            );
            assert!(
                idx.num_terms() > 0,
                "TEXT-only index should have terms after HSET via auto_index_hset, got {}",
                idx.num_terms()
            );
        }
    }
}
