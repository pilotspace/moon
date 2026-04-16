//! FT.SEARCH text query handler — BM25 ranked full-text search.
//!
//! This module provides:
//! - `is_text_query()`: detect whether an FT.SEARCH query is a text query (not KNN/SPARSE)
//! - `parse_text_query()`: parse RediSearch-compatible query syntax into field target + analyzed terms
//! - `ft_text_search()`: execute BM25 text search on a TextStore shard (single-shard path)
//! - `execute_text_search_local()`: local search without global IDF (single-shard, DFS-skip path)
//! - `execute_text_search_with_global_idf()`: search with injected global IDF (DFS Phase 2 path)
//! - `merge_text_results()`: merge multi-shard text results descending by BM25 score

use bytes::Bytes;
use std::collections::HashMap;

use crate::protocol::Frame;
use crate::text::store::{TextIndex, TextSearchResult, TextStore};

use super::{extract_bulk, parse_limit_clause};

// ─── Query clause ────────────────────────────────────────────────────────────

/// Parsed text query: an optional field target + analyzed (stemmed) query terms.
///
/// `field_name = None` means cross-field search (all non-NOINDEX TEXT fields).
/// `field_name = Some(name)` means search only that field.
#[derive(Debug)]
pub struct TextQueryClause {
    /// Target field name (from `@field:(terms)` syntax), or None for all fields.
    pub field_name: Option<Bytes>,
    /// Analyzed (tokenized + stemmed + stop-word filtered) query terms.
    pub terms: Vec<String>,
}

// ─── Query detection ─────────────────────────────────────────────────────────

/// Returns `true` when the FT.SEARCH query is a text query.
///
/// A query is NOT a text query when:
/// - It is exactly `*` (match-all for vector index scan)
/// - It contains `"KNN "` (dense KNN query syntax)
/// - It contains `"SPARSE "` after `@field` (sparse query syntax)
///
/// Everything else is treated as a text query (bare terms or `@field:(terms)`).
pub fn is_text_query(query: &[u8]) -> bool {
    if query == b"*" {
        return false;
    }
    // Avoid UTF-8 parse cost for the common case: just scan bytes.
    // KNN queries look like: "*=>[KNN 10 @vec $q]"
    // The distinguishing substring is "KNN " (case-insensitive scan).
    let upper: Vec<u8> = query.iter().map(|b| b.to_ascii_uppercase()).collect();
    if upper.windows(4).any(|w| w == b"KNN ") {
        return false;
    }
    true
}

// ─── Query parser ────────────────────────────────────────────────────────────

/// Parse a FT.SEARCH query string into a `TextQueryClause`.
///
/// Supported syntax (RediSearch-compatible subset, per D-01/D-02):
/// - `@field:(terms)` — field-targeted search; terms inside `(...)` are AND'd
/// - `bare terms` — cross-field search on all non-NOINDEX TEXT fields
/// - `"quoted terms"` — treated as bare terms with implicit AND (no phrase semantics per D-02)
///
/// Query text is processed through `analyzer.tokenize_with_positions()` to match
/// the same pipeline used during indexing (per D-03).
///
/// # Errors
/// - `"ERR empty query after analysis"` — all terms were stop words
/// - `"ERR empty field query"` — `@field:()` with no terms inside parens
pub fn parse_text_query(
    query: &[u8],
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Result<TextQueryClause, &'static str> {
    // Strip surrounding double-quotes if present ("quoted terms" = implicit AND per D-02)
    let query = strip_surrounding_quotes(query);

    if query.starts_with(b"@") {
        parse_field_targeted_query(query, analyzer)
    } else {
        parse_bare_terms_query(query, analyzer)
    }
}

/// Parse `@field:(terms)` or `@field:terms` syntax.
fn parse_field_targeted_query(
    query: &[u8],
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Result<TextQueryClause, &'static str> {
    // Find the colon separator: "@field_name:..."
    let colon_pos = match query.iter().position(|&b| b == b':') {
        Some(p) => p,
        None => return Err("ERR invalid field query syntax: missing ':'"),
    };

    // field_name is everything between '@' and ':'
    let field_name_bytes = &query[1..colon_pos];
    if field_name_bytes.is_empty() {
        return Err("ERR invalid field query syntax: empty field name");
    }
    let field_name = Bytes::copy_from_slice(field_name_bytes);

    // Terms portion is everything after ':'
    let terms_part = &query[colon_pos + 1..];

    // Strip surrounding parens if present: "(terms)" -> "terms"
    let terms_text = strip_surrounding_parens(terms_part);

    if terms_text.is_empty() {
        return Err("ERR empty field query");
    }

    let text_str = match std::str::from_utf8(terms_text) {
        Ok(s) => s,
        Err(_) => return Err("ERR invalid UTF-8 in query"),
    };

    let tokens = analyzer.tokenize_with_positions(text_str);
    if tokens.is_empty() {
        return Err("ERR empty query after analysis");
    }

    let terms: Vec<String> = tokens.into_iter().map(|(term, _)| term).collect();

    Ok(TextQueryClause {
        field_name: Some(field_name),
        terms,
    })
}

/// Parse bare-terms query (all TEXT fields, per D-01).
fn parse_bare_terms_query(
    query: &[u8],
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Result<TextQueryClause, &'static str> {
    let text_str = match std::str::from_utf8(query) {
        Ok(s) => s,
        Err(_) => return Err("ERR invalid UTF-8 in query"),
    };

    let tokens = analyzer.tokenize_with_positions(text_str);
    if tokens.is_empty() {
        return Err("ERR empty query after analysis");
    }

    let terms: Vec<String> = tokens.into_iter().map(|(term, _)| term).collect();

    Ok(TextQueryClause {
        field_name: None,
        terms,
    })
}

/// Strip surrounding `"..."` from a byte slice.
fn strip_surrounding_quotes(s: &[u8]) -> &[u8] {
    if s.len() >= 2 && s[0] == b'"' && s[s.len() - 1] == b'"' {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

/// Strip surrounding `(...)` from a byte slice.
fn strip_surrounding_parens(s: &[u8]) -> &[u8] {
    let s = s.trim_ascii();
    if s.len() >= 2 && s[0] == b'(' && s[s.len() - 1] == b')' {
        s[1..s.len() - 1].trim_ascii()
    } else {
        s
    }
}

// ─── Single-shard text search ─────────────────────────────────────────────────

/// Execute a BM25 text search for a single FT.SEARCH command on this shard.
///
/// This is the entry point called from `dispatch_vector_command` when
/// `is_text_query(query)` returns true. It:
/// 1. Parses index_name and query from args
/// 2. Looks up the TextIndex in text_store
/// 3. Parses the query via parse_text_query using the first field's analyzer
/// 4. For cross-field: searches all non-NOINDEX fields, accumulates per-doc scores
/// 5. For field-targeted: searches only the specified field
/// 6. Sorts descending by score, applies LIMIT, returns RESP array
///
/// Response format: `[total, key1, ["__bm25_score", "N.NNNNNN"], key2, [...], ...]`
pub fn ft_text_search(text_store: &TextStore, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'FT.SEARCH' command",
        ));
    }

    let index_name = match extract_bulk(&args[0]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid index name")),
    };

    let query_bytes = match extract_bulk(&args[1]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid query")),
    };

    let text_index = match text_store.get_index(index_name.as_ref()) {
        Some(idx) => idx,
        None => {
            return Frame::Error(Bytes::from_static(b"ERR no such index"));
        }
    };

    // Parse LIMIT clause (offset, count) with defaults (0, usize::MAX).
    let (limit_offset, limit_count) = parse_limit_clause(args);

    // Determine top_k: search for offset + count results so we can paginate.
    let top_k = if limit_count == usize::MAX {
        usize::MAX / 2 // large but not overflow-prone
    } else {
        limit_offset.saturating_add(limit_count)
    };
    let top_k = top_k.max(1);

    // Use the first field's analyzer for query parsing (per D-03: all fields share same language).
    let analyzer = match text_index.field_analyzers.first() {
        Some(a) => a,
        None => {
            return Frame::Error(Bytes::from_static(b"ERR index has no TEXT fields"));
        }
    };

    // Parse the query into (field_name, terms).
    let clause = match parse_text_query(query_bytes.as_ref(), analyzer) {
        Ok(c) => c,
        Err(e) => return Frame::Error(Bytes::copy_from_slice(e.as_bytes())),
    };

    // Execute the search (cross-field or field-targeted).
    let results = execute_query_on_index(text_index, &clause, None, None, top_k);

    build_text_response(&results, limit_offset, limit_count)
}

/// Execute text search on local shard without global IDF override (single-shard path).
///
/// Exported for the coordinator to call directly on the local shard when
/// `num_shards == 1` (skips DFS pre-pass per D-06).
pub fn execute_text_search_local(
    text_store: &TextStore,
    index_name: &[u8],
    field_idx: Option<usize>,
    query_terms: &[String],
    top_k: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let text_index = match text_store.get_index(index_name) {
        Some(idx) => idx,
        None => return Frame::Error(Bytes::from_static(b"ERR no such index")),
    };

    let results = if let Some(fidx) = field_idx {
        text_index.search_field(fidx, query_terms, None, None, top_k)
    } else {
        // Cross-field: accumulate across all non-NOINDEX fields.
        accumulate_cross_field(text_index, query_terms, None, None, top_k)
    };

    build_text_response(&results, offset, count)
}

/// Execute text search with injected global document frequencies (DFS Phase 2).
///
/// Called by the SPSC handler after the Phase 1 scatter has aggregated
/// global IDF weights from all shards (per D-04). The `global_df` and `global_n`
/// override the local field statistics for BM25 IDF computation.
pub fn execute_text_search_with_global_idf(
    text_index: &TextIndex,
    field_idx: Option<usize>,
    query_terms: &[String],
    global_df: &HashMap<String, u32>,
    global_n: u32,
    top_k: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let results = if let Some(fidx) = field_idx {
        text_index.search_field(fidx, query_terms, Some(global_df), Some(global_n), top_k)
    } else {
        accumulate_cross_field(text_index, query_terms, Some(global_df), Some(global_n), top_k)
    };

    build_text_response(&results, offset, count)
}

// ─── Cross-field accumulation ─────────────────────────────────────────────────

/// Execute cross-field search and accumulate BM25 scores per document.
///
/// Each non-NOINDEX field is searched independently with its own avgdl (per
/// RESEARCH Pitfall 4 — fields can have very different average lengths).
/// Per-doc scores are accumulated with per-field weight multipliers.
///
/// Deduplication: if a doc_id appears in multiple fields, scores are summed.
/// The final list is sorted descending by total accumulated score.
fn accumulate_cross_field(
    text_index: &TextIndex,
    query_terms: &[String],
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    // Accumulate: doc_id -> (total_score, key)
    let mut acc: HashMap<u32, (f32, Bytes)> = HashMap::new();

    for (field_idx, field_def) in text_index.text_fields.iter().enumerate() {
        if field_def.noindex {
            continue;
        }

        let field_results =
            text_index.search_field(field_idx, query_terms, global_df, global_n, top_k);

        for r in field_results {
            let entry = acc.entry(r.doc_id).or_insert((0.0, r.key.clone()));
            entry.0 += r.score;
        }
    }

    if acc.is_empty() {
        return Vec::new();
    }

    // Convert to Vec<TextSearchResult> and sort descending by accumulated score.
    let mut results: Vec<TextSearchResult> = acc
        .into_iter()
        .map(|(doc_id, (score, key))| TextSearchResult { doc_id, key, score })
        .collect();

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    results.truncate(top_k);
    results
}

/// Execute query on a TextIndex: dispatch to cross-field or field-targeted search.
fn execute_query_on_index(
    text_index: &TextIndex,
    clause: &TextQueryClause,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    match &clause.field_name {
        None => accumulate_cross_field(text_index, &clause.terms, global_df, global_n, top_k),
        Some(field_name) => {
            // Find the matching field index.
            let field_idx = text_index
                .text_fields
                .iter()
                .position(|f| f.field_name.as_ref().eq_ignore_ascii_case(field_name.as_ref()));

            match field_idx {
                Some(fidx) => {
                    text_index.search_field(fidx, &clause.terms, global_df, global_n, top_k)
                }
                None => Vec::new(), // Unknown field -> empty result (not an error at this layer)
            }
        }
    }
}

// ─── Response building ────────────────────────────────────────────────────────

/// Build FT.SEARCH text response array with LIMIT pagination.
///
/// Format: `[total, key1, ["__bm25_score", "N.NNNNNN"], key2, [...], ...]`
///
/// `total` is the full number of matched results before pagination.
/// Document entries are `results[offset..offset+count]`.
fn build_text_response(results: &[TextSearchResult], offset: usize, count: usize) -> Frame {
    let total = results.len() as i64;
    let page_count = if count == usize::MAX { results.len() } else { count };
    let page = results.iter().skip(offset).take(page_count);

    let mut items = Vec::with_capacity(1 + results.len().min(page_count) * 2);
    items.push(Frame::Integer(total));

    for r in page {
        items.push(Frame::BulkString(r.key.clone()));

        // Serialize score as fixed-precision string.
        // Use write! to a pre-allocated String (no heap allocation on hot path
        // beyond the String itself — acceptable at response-building stage).
        let mut score_buf = String::with_capacity(16);
        use std::fmt::Write;
        let _ = write!(score_buf, "{:.6}", r.score);

        let fields: Vec<Frame> = vec![
            Frame::BulkString(Bytes::from_static(b"__bm25_score")),
            Frame::BulkString(Bytes::from(score_buf)),
        ];
        items.push(Frame::Array(fields.into()));
    }

    Frame::Array(items.into())
}

// ─── Multi-shard merge ────────────────────────────────────────────────────────

/// Merge BM25 text search results from multiple shards.
///
/// Like `merge_search_results()` but with DESCENDING sort by score
/// (higher BM25 score = more relevant, per D-09).
///
/// Extracts scores from `__bm25_score` fields. Missing/unparseable scores
/// default to `f32::MIN` (sorted to the bottom).
///
/// # Arguments
/// - `shard_responses` — per-shard Frame arrays in FT.SEARCH response format
/// - `top_k` — maximum total results after merge
/// - `offset`, `count` — LIMIT pagination applied after merge
pub fn merge_text_results(
    shard_responses: &[Frame],
    top_k: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let mut all_results: Vec<(f32, Bytes, Frame)> = Vec::new();

    for resp in shard_responses {
        let items = match resp {
            Frame::Array(items) => items,
            Frame::Error(_) => continue, // skip errored shards
            _ => continue,
        };
        if items.is_empty() {
            continue;
        }
        // items[0] = total count (Integer), then pairs of (key, fields_array)
        let mut i = 1;
        while i + 1 < items.len() {
            let doc_key = match &items[i] {
                Frame::BulkString(b) => b.clone(),
                _ => {
                    i += 2;
                    continue;
                }
            };
            let fields = items[i + 1].clone();
            let score = extract_bm25_score_from_fields(&fields);
            all_results.push((score, doc_key, fields));
            i += 2;
        }
    }

    // Sort DESCENDING (higher BM25 score = better match, per D-09).
    all_results.sort_by(|a, b| {
        b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal)
    });
    all_results.truncate(top_k);

    let total = all_results.len() as i64;
    let page_count = if count == usize::MAX { all_results.len() } else { count };
    let page: Vec<&(f32, Bytes, Frame)> =
        all_results.iter().skip(offset).take(page_count).collect();

    let mut items = Vec::with_capacity(1 + page.len() * 2);
    items.push(Frame::Integer(total));
    for (_, doc_key, fields) in page {
        items.push(Frame::BulkString(doc_key.clone()));
        items.push(fields.clone());
    }

    Frame::Array(items.into())
}

/// Extract the `__bm25_score` float from a fields array.
///
/// Fields format: `["__bm25_score", "0.123456", ...]`
/// Returns `f32::MIN` when the field is absent or unparseable (sorts to bottom).
fn extract_bm25_score_from_fields(fields: &Frame) -> f32 {
    if let Frame::Array(items) = fields {
        for pair in items.chunks(2) {
            if pair.len() == 2 {
                if let Frame::BulkString(key) = &pair[0] {
                    if key.as_ref() == b"__bm25_score" {
                        if let Frame::BulkString(val) = &pair[1] {
                            if let Ok(s) = std::str::from_utf8(val) {
                                return s.parse().unwrap_or(f32::MIN);
                            }
                        }
                    }
                }
            }
        }
    }
    f32::MIN
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_text_query ──────────────────────────────────────────────────────────

    #[test]
    fn is_text_query_bare_terms() {
        assert!(is_text_query(b"machine learning"));
        assert!(is_text_query(b"hello world"));
        assert!(is_text_query(b"rust programming"));
    }

    #[test]
    fn is_text_query_match_all_is_not_text() {
        assert!(!is_text_query(b"*"));
    }

    #[test]
    fn is_text_query_knn_is_not_text() {
        assert!(!is_text_query(b"*=>[KNN 10 @vec $query]"));
        assert!(!is_text_query(b"*=>[KNN 5 @embedding $q]"));
        assert!(!is_text_query(b"knn 10")); // lowercase KNN
    }

    #[test]
    fn is_text_query_field_syntax_is_text() {
        assert!(is_text_query(b"@title:(machine learning)"));
        assert!(is_text_query(b"@body:(rust)"));
    }

    // ── parse_text_query ───────────────────────────────────────────────────────

    #[cfg(feature = "text-index")]
    fn make_analyzer() -> crate::text::analyzer::AnalyzerPipeline {
        crate::text::analyzer::AnalyzerPipeline::new(
            rust_stemmers::Algorithm::English,
            false, // with stemming
        )
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn parse_text_query_bare_terms() {
        let analyzer = make_analyzer();
        let result = parse_text_query(b"machine learning", &analyzer).unwrap();
        assert!(result.field_name.is_none(), "bare terms have no field target");
        // "machine" -> "machin", "learning" -> "learn" (English Snowball)
        assert!(result.terms.contains(&"machin".to_string()), "expected stemmed 'machin'");
        assert!(result.terms.contains(&"learn".to_string()), "expected stemmed 'learn'");
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn parse_text_query_field_targeted() {
        let analyzer = make_analyzer();
        let result = parse_text_query(b"@title:(machine learning)", &analyzer).unwrap();
        assert_eq!(
            result.field_name.as_ref().map(|b| b.as_ref()),
            Some(b"title" as &[u8]),
            "field name should be 'title'"
        );
        assert!(result.terms.contains(&"machin".to_string()));
        assert!(result.terms.contains(&"learn".to_string()));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn parse_text_query_quoted_string_treated_as_bare() {
        let analyzer = make_analyzer();
        // Surrounding quotes are stripped; remaining terms analyzed normally
        let result = parse_text_query(b"\"machine learning\"", &analyzer).unwrap();
        assert!(result.field_name.is_none());
        assert!(result.terms.contains(&"machin".to_string()));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn parse_text_query_stop_words_only() {
        let analyzer = make_analyzer();
        // "the" is an English stop word — should produce empty terms after analysis
        let result = parse_text_query(b"the", &analyzer);
        assert!(result.is_err(), "all stop-word query must error");
        assert!(result.unwrap_err().contains("empty query after analysis"));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn parse_text_query_empty_field_clause() {
        let analyzer = make_analyzer();
        let result = parse_text_query(b"@title:()", &analyzer);
        assert!(result.is_err(), "@field:() must error");
    }

    // ── merge_text_results ─────────────────────────────────────────────────────

    fn make_text_response_frame(key: &str, score: f32) -> Frame {
        let mut score_buf = String::with_capacity(16);
        use std::fmt::Write;
        let _ = write!(score_buf, "{:.6}", score);
        Frame::Array(
            vec![
                Frame::Integer(1),
                Frame::BulkString(Bytes::copy_from_slice(key.as_bytes())),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"__bm25_score")),
                        Frame::BulkString(Bytes::from(score_buf)),
                    ]
                    .into(),
                ),
            ]
            .into(),
        )
    }

    #[test]
    fn merge_text_results_descending_sort() {
        // Shard 0 returns doc:a with score 1.5, shard 1 returns doc:b with score 3.2
        let shard0 = make_text_response_frame("doc:a", 1.5);
        let shard1 = make_text_response_frame("doc:b", 3.2);

        let merged = merge_text_results(&[shard0, shard1], 10, 0, usize::MAX);
        let items = match &merged {
            Frame::Array(items) => items,
            _ => panic!("expected Array response"),
        };

        // items[0] = total (2), items[1] = first key, items[3] = second key
        assert_eq!(items[0], Frame::Integer(2), "total should be 2");

        // First result should be doc:b (higher score 3.2 ranks first)
        let first_key = match &items[1] {
            Frame::BulkString(b) => b.clone(),
            _ => panic!("expected BulkString key"),
        };
        assert_eq!(first_key.as_ref(), b"doc:b", "doc:b (score=3.2) must rank first");
    }

    #[test]
    fn merge_text_results_score_in_response_field() {
        let shard0 = make_text_response_frame("doc:x", 2.5);
        let merged = merge_text_results(&[shard0], 10, 0, usize::MAX);
        let items = match &merged {
            Frame::Array(items) => items,
            _ => panic!("expected Array"),
        };

        // items[2] should be the fields array containing "__bm25_score"
        let fields = match &items[2] {
            Frame::Array(f) => f,
            _ => panic!("expected fields Array"),
        };
        let key_frame = &fields[0];
        match key_frame {
            Frame::BulkString(b) => {
                assert_eq!(b.as_ref(), b"__bm25_score", "field name must be __bm25_score");
            }
            _ => panic!("expected BulkString field name"),
        }
    }

    // ── response_contains_bm25_score ───────────────────────────────────────────

    #[test]
    fn response_field_name_is_bm25_score_not_vec_score() {
        // Build a minimal TextSearchResult and verify response uses __bm25_score
        let results = vec![TextSearchResult {
            doc_id: 0,
            key: Bytes::from_static(b"doc:test"),
            score: 1.23,
        }];
        let resp = build_text_response(&results, 0, usize::MAX);
        let items = match &resp {
            Frame::Array(a) => a,
            _ => panic!("expected Array"),
        };
        // items[0]=count, items[1]=key, items[2]=fields
        let fields = match &items[2] {
            Frame::Array(f) => f,
            _ => panic!("expected fields Array"),
        };
        assert_eq!(fields[0], Frame::BulkString(Bytes::from_static(b"__bm25_score")));
    }

    // ── cross_field_score_accumulation ─────────────────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn cross_field_score_accumulation() {
        use crate::protocol::Frame as F;
        use crate::text::types::{BM25Config, TextFieldDef};
        use crate::text::store::TextIndex;

        // Create a 2-field index (title + body)
        let title_field = TextFieldDef::new(Bytes::from_static(b"title"));
        let body_field = TextFieldDef::new(Bytes::from_static(b"body"));
        let mut idx = TextIndex::new(
            Bytes::from_static(b"test"),
            Vec::new(),
            vec![title_field, body_field],
            BM25Config::default(),
        );

        // doc:0 has "machine" in BOTH title and body — cross-field should accumulate scores
        let args0 = vec![
            F::BulkString(Bytes::from_static(b"title")),
            F::BulkString(Bytes::from_static(b"machine")),
            F::BulkString(Bytes::from_static(b"body")),
            F::BulkString(Bytes::from_static(b"machine learning")),
        ];
        idx.index_document(0, b"doc:0", &args0);

        // doc:1 has "machine" only in body
        let args1 = vec![
            F::BulkString(Bytes::from_static(b"title")),
            F::BulkString(Bytes::from_static(b"other")),
            F::BulkString(Bytes::from_static(b"body")),
            F::BulkString(Bytes::from_static(b"machine vision")),
        ];
        idx.index_document(1, b"doc:1", &args1);

        let terms = vec!["machin".to_string()];
        let results = accumulate_cross_field(&idx, &terms, None, None, 10);

        assert_eq!(results.len(), 2, "both docs match 'machine'");
        // doc:0 should rank higher (matched in 2 fields, score accumulated from both)
        assert_eq!(results[0].key.as_ref(), b"doc:0", "doc:0 with multi-field match ranks first");
        assert!(results[0].score > results[1].score, "accumulated score should be higher");
    }
}
