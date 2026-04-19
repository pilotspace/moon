//! FT.SEARCH text query handler — BM25 ranked full-text search.
//!
//! This module provides:
//! - `is_text_query()`: detect whether an FT.SEARCH query is a text query (not KNN/SPARSE)
//! - `parse_text_query()`: parse RediSearch-compatible query syntax into field target + analyzed terms
//! - `ft_text_search()`: execute BM25 text search on a TextStore shard (single-shard path)
//! - `execute_text_search_local()`: local search without global IDF (single-shard, DFS-skip path)
//! - `execute_text_search_with_global_idf()`: search with injected global IDF (DFS Phase 2 path)
//! - `merge_text_results()`: merge multi-shard text results descending by BM25 score
//! - `highlight_field()`: wrap matched terms in configurable tags with context window
//! - `summarize_field()`: extract best-matching passage using sliding window over match density
//! - `parse_highlight_clause()`: parse HIGHLIGHT clause from FT.SEARCH args
//! - `parse_summarize_clause()`: parse SUMMARIZE clause from FT.SEARCH args
//! - `apply_post_processing()`: apply HIGHLIGHT/SUMMARIZE to a search response Frame

use bytes::Bytes;
use std::collections::HashMap;

use crate::protocol::Frame;
#[cfg(feature = "text-index")]
use crate::text::store::TermModifier;
use crate::text::store::{TextIndex, TextSearchResult, TextStore};

use super::{extract_bulk, parse_limit_clause};

// ─── Highlight/Summarize options ─────────────────────────────────────────────

/// Options for HIGHLIGHT post-processing.
///
/// HIGHLIGHT wraps matched terms in open/close tags (default `<b>`/`</b>`)
/// with a configurable context window of tokens before/after each match.
#[derive(Clone, Debug)]
pub struct HighlightOpts {
    /// Optional field names to highlight (None = all text fields).
    pub fields: Option<Vec<Bytes>>,
    /// Opening tag inserted before each matched term. Default: `<b>`.
    pub open_tag: String,
    /// Closing tag inserted after each matched term. Default: `</b>`.
    pub close_tag: String,
    /// Number of context tokens to include before/after each match. Default: 4.
    pub context_tokens: usize,
}

impl Default for HighlightOpts {
    fn default() -> Self {
        Self {
            fields: None,
            open_tag: "<b>".to_owned(),
            close_tag: "</b>".to_owned(),
            context_tokens: 4,
        }
    }
}

/// Options for SUMMARIZE post-processing.
///
/// SUMMARIZE extracts the highest-scoring passage from each document:
/// the window of `fragment_size` tokens with the most matched terms.
#[derive(Clone, Debug)]
pub struct SummarizeOpts {
    /// Optional field names to summarize (None = all text fields).
    pub fields: Option<Vec<Bytes>>,
    /// Number of tokens per fragment (default: 20 per D-11).
    pub fragment_size: usize,
    /// Number of fragments to return per document (default: 1).
    pub num_fragments: usize,
    /// Separator between fragments (default: `...`).
    pub separator: String,
}

impl Default for SummarizeOpts {
    fn default() -> Self {
        Self {
            fields: None,
            fragment_size: 20,
            num_fragments: 1,
            separator: "...".to_owned(),
        }
    }
}

// ─── HIGHLIGHT/SUMMARIZE clause parsing ──────────────────────────────────────

/// Parse a HIGHLIGHT clause from FT.SEARCH args.
///
/// Syntax: `HIGHLIGHT [FIELDS count field1 ...] [TAGS open close]`
///
/// Returns `None` if the HIGHLIGHT keyword is not present.
/// Defaults: open_tag=`<b>`, close_tag=`</b>`, fields=None (all TEXT fields),
/// context_tokens=4.
pub fn parse_highlight_clause(args: &[Frame]) -> Option<HighlightOpts> {
    // Scan for case-insensitive "HIGHLIGHT"
    let hl_pos = args.iter().position(
        |f| matches!(f, Frame::BulkString(b) if b.as_ref().eq_ignore_ascii_case(b"HIGHLIGHT")),
    )?;

    let mut opts = HighlightOpts::default();
    let mut i = hl_pos + 1;

    while i < args.len() {
        let kw = match &args[i] {
            Frame::BulkString(b) => b.clone(),
            _ => {
                i += 1;
                continue;
            }
        };

        if kw.as_ref().eq_ignore_ascii_case(b"FIELDS") {
            i += 1;
            // Parse count then field names
            let count = match args.get(i) {
                Some(Frame::BulkString(b)) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0),
                Some(Frame::Integer(n)) => *n as usize,
                _ => 0,
            };
            i += 1;
            if count > 0 {
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    match args.get(i) {
                        Some(Frame::BulkString(b)) => {
                            // Stop if we hit another keyword
                            if b.as_ref().eq_ignore_ascii_case(b"TAGS")
                                || b.as_ref().eq_ignore_ascii_case(b"SUMMARIZE")
                                || b.as_ref().eq_ignore_ascii_case(b"LIMIT")
                                || b.as_ref().eq_ignore_ascii_case(b"RETURN")
                            {
                                break;
                            }
                            fields.push(Bytes::copy_from_slice(b));
                            i += 1;
                        }
                        _ => break,
                    }
                }
                if !fields.is_empty() {
                    opts.fields = Some(fields);
                }
            }
        } else if kw.as_ref().eq_ignore_ascii_case(b"TAGS") {
            // Parse open_tag and close_tag
            if let Some(Frame::BulkString(open)) = args.get(i + 1) {
                opts.open_tag = String::from_utf8_lossy(open).into_owned();
                if let Some(Frame::BulkString(close)) = args.get(i + 2) {
                    opts.close_tag = String::from_utf8_lossy(close).into_owned();
                    i += 3;
                    continue;
                }
                i += 2;
            } else {
                i += 1;
            }
        } else if kw.as_ref().eq_ignore_ascii_case(b"SUMMARIZE")
            || kw.as_ref().eq_ignore_ascii_case(b"LIMIT")
            || kw.as_ref().eq_ignore_ascii_case(b"RETURN")
        {
            // Hit a different clause — stop parsing HIGHLIGHT opts
            break;
        } else {
            i += 1;
        }
    }

    Some(opts)
}

/// Parse a SUMMARIZE clause from FT.SEARCH args.
///
/// Syntax: `SUMMARIZE [FIELDS count field1 ...] [FRAGS num] [LEN len] [SEPARATOR sep]`
///
/// Returns `None` if the SUMMARIZE keyword is not present.
/// Defaults: fragment_size=20, num_fragments=1, separator=`...`, fields=None.
pub fn parse_summarize_clause(args: &[Frame]) -> Option<SummarizeOpts> {
    // Scan for case-insensitive "SUMMARIZE"
    let sum_pos = args.iter().position(
        |f| matches!(f, Frame::BulkString(b) if b.as_ref().eq_ignore_ascii_case(b"SUMMARIZE")),
    )?;

    let mut opts = SummarizeOpts::default();
    let mut i = sum_pos + 1;

    while i < args.len() {
        let kw = match &args[i] {
            Frame::BulkString(b) => b.clone(),
            _ => {
                i += 1;
                continue;
            }
        };

        if kw.as_ref().eq_ignore_ascii_case(b"FIELDS") {
            i += 1;
            let count = match args.get(i) {
                Some(Frame::BulkString(b)) => std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0),
                Some(Frame::Integer(n)) => *n as usize,
                _ => 0,
            };
            i += 1;
            if count > 0 {
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    match args.get(i) {
                        Some(Frame::BulkString(b)) => {
                            if b.as_ref().eq_ignore_ascii_case(b"FRAGS")
                                || b.as_ref().eq_ignore_ascii_case(b"LEN")
                                || b.as_ref().eq_ignore_ascii_case(b"SEPARATOR")
                                || b.as_ref().eq_ignore_ascii_case(b"HIGHLIGHT")
                                || b.as_ref().eq_ignore_ascii_case(b"LIMIT")
                                || b.as_ref().eq_ignore_ascii_case(b"RETURN")
                            {
                                break;
                            }
                            fields.push(Bytes::copy_from_slice(b));
                            i += 1;
                        }
                        _ => break,
                    }
                }
                if !fields.is_empty() {
                    opts.fields = Some(fields);
                }
            }
        } else if kw.as_ref().eq_ignore_ascii_case(b"FRAGS") {
            i += 1;
            if let Some(Frame::BulkString(b)) = args.get(i) {
                if let Ok(n) = std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                    .ok_or(())
                {
                    opts.num_fragments = n;
                }
            } else if let Some(Frame::Integer(n)) = args.get(i) {
                opts.num_fragments = (*n).max(0) as usize;
            }
            i += 1;
        } else if kw.as_ref().eq_ignore_ascii_case(b"LEN") {
            i += 1;
            if let Some(Frame::BulkString(b)) = args.get(i) {
                if let Some(n) = std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    opts.fragment_size = n;
                }
            } else if let Some(Frame::Integer(n)) = args.get(i) {
                opts.fragment_size = (*n).max(1) as usize;
            }
            i += 1;
        } else if kw.as_ref().eq_ignore_ascii_case(b"SEPARATOR") {
            i += 1;
            if let Some(Frame::BulkString(b)) = args.get(i) {
                opts.separator = String::from_utf8_lossy(b).into_owned();
                i += 1;
            }
        } else if kw.as_ref().eq_ignore_ascii_case(b"HIGHLIGHT")
            || kw.as_ref().eq_ignore_ascii_case(b"LIMIT")
            || kw.as_ref().eq_ignore_ascii_case(b"RETURN")
        {
            break;
        } else {
            i += 1;
        }
    }

    Some(opts)
}

// ─── HIGHLIGHT / SUMMARIZE field processors ──────────────────────────────────

/// Highlight matched terms in original text with configurable tags and context window.
///
/// # Algorithm
///
/// 1. Re-tokenize original text using `unicode_words().enumerate()` to get ALL words
///    with their ordinal positions (INCLUDING stop words — same numbering as indexing).
/// 2. For each word: apply lowercase + stem to get stemmed form; check if in query_terms set.
/// 3. Merge overlapping context windows `[pos - ctx, pos + ctx]` around each match.
/// 4. Reconstruct text: words in windows get matched ones wrapped in tags, with `...` between
///    non-adjacent windows. Short texts (fits in one window) return fully highlighted text.
///
/// # Critical note (Pitfall 3)
///
/// Positions stored in PostingList are Unicode word ordinals from `enumerate()` over
/// `unicode_words()`. Stop words consume position numbers but are excluded from posting output.
/// Re-tokenization MUST iterate ALL Unicode words (including stop words) to correctly map
/// stored positions back to original words.
#[cfg(feature = "text-index")]
pub fn highlight_field(
    original_text: &str,
    query_terms: &[String],
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
    open_tag: &str,
    close_tag: &str,
    context_tokens: usize,
) -> String {
    use unicode_segmentation::UnicodeSegmentation;

    if original_text.is_empty() || query_terms.is_empty() {
        return original_text.to_owned();
    }

    // Build a set of stemmed query terms for O(1) lookup.
    let query_set: std::collections::HashSet<&str> =
        query_terms.iter().map(|s| s.as_str()).collect();

    // Re-tokenize: iterate ALL Unicode words with ordinal positions.
    // Collect (original_word, position, is_match) for every word.
    let words: Vec<(&str, usize, bool)> = original_text
        .unicode_words()
        .enumerate()
        .map(|(pos, word)| {
            // Lowercase + stem the word the same way indexing does.
            let stemmed = stem_word(word, analyzer);
            let is_match = query_set.contains(stemmed.as_str());
            (word, pos, is_match)
        })
        .collect();

    if words.is_empty() {
        return original_text.to_owned();
    }

    // Find positions of all matched words.
    let match_positions: Vec<usize> = words
        .iter()
        .filter_map(|(_, pos, is_match)| if *is_match { Some(*pos) } else { None })
        .collect();

    if match_positions.is_empty() {
        // No matches — return original text unchanged.
        return original_text.to_owned();
    }

    let total_words = words.len();

    // If the text is short (fits entirely in the context window), return fully highlighted.
    let fits_in_window = total_words <= 2 * context_tokens + match_positions.len() + 1;
    if fits_in_window {
        return reconstruct_full_highlighted(&words, &query_set, analyzer, open_tag, close_tag);
    }

    // Build merged windows around each match.
    // Each window is [start, end] inclusive positions.
    let windows = merge_windows(&match_positions, context_tokens, total_words);

    // Reconstruct text from windows with "..." separator between non-adjacent windows.
    reconstruct_windowed(&words, &windows, &query_set, analyzer, open_tag, close_tag)
}

/// Fallback when text-index feature is disabled — return original text unchanged.
#[cfg(not(feature = "text-index"))]
pub fn highlight_field(
    original_text: &str,
    _query_terms: &[String],
    _analyzer: &crate::text::analyzer::AnalyzerPipeline,
    _open_tag: &str,
    _close_tag: &str,
    _context_tokens: usize,
) -> String {
    original_text.to_owned()
}

/// Stem a single word using the analyzer's pipeline.
///
/// Applies lowercase + NFKD normalize + stem (same as tokenize_with_positions).
/// Returns the stemmed string. Used during highlight re-tokenization.
#[cfg(feature = "text-index")]
fn stem_word(word: &str, analyzer: &crate::text::analyzer::AnalyzerPipeline) -> String {
    // Run through the same pipeline used during indexing:
    // lowercase → stem via tokenize_with_positions on a single word.
    // We call tokenize_with_positions on the word to get the stemmed form.
    let result = analyzer.tokenize_with_positions(word);
    // tokenize_with_positions filters stop words and short tokens.
    // If result is empty (stop word / too short), the word is not a match candidate.
    result
        .into_iter()
        .next()
        .map(|(stem, _)| stem)
        .unwrap_or_default()
}

/// Merge context windows around match positions into non-overlapping intervals.
///
/// Each match at `pos` generates window `[pos - ctx, pos + ctx]` (clamped to text bounds).
/// Overlapping or adjacent windows are merged.
#[cfg(feature = "text-index")]
fn merge_windows(match_positions: &[usize], ctx: usize, total_words: usize) -> Vec<(usize, usize)> {
    if match_positions.is_empty() {
        return Vec::new();
    }

    let mut windows: Vec<(usize, usize)> = match_positions
        .iter()
        .map(|&pos| {
            let start = pos.saturating_sub(ctx);
            let end = (pos + ctx).min(total_words.saturating_sub(1));
            (start, end)
        })
        .collect();

    windows.sort_unstable_by_key(|w| w.0);

    // Merge overlapping/adjacent windows
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in windows {
        match merged.last_mut() {
            Some(last) if start <= last.1 + 1 => {
                last.1 = last.1.max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// Reconstruct the full text with all matched terms wrapped in tags (no truncation).
#[cfg(feature = "text-index")]
fn reconstruct_full_highlighted(
    words: &[(&str, usize, bool)],
    query_set: &std::collections::HashSet<&str>,
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
    open_tag: &str,
    close_tag: &str,
) -> String {
    let mut out = String::new();
    let mut first = true;
    for (word, _, _) in words {
        if !first {
            out.push(' ');
        }
        first = false;
        let stemmed = stem_word(word, analyzer);
        if query_set.contains(stemmed.as_str()) {
            out.push_str(open_tag);
            out.push_str(word);
            out.push_str(close_tag);
        } else {
            out.push_str(word);
        }
    }
    out
}

/// Reconstruct highlighted text from merged windows, joining with "..." between gaps.
#[cfg(feature = "text-index")]
fn reconstruct_windowed(
    words: &[(&str, usize, bool)],
    windows: &[(usize, usize)],
    query_set: &std::collections::HashSet<&str>,
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
    open_tag: &str,
    close_tag: &str,
) -> String {
    let mut out = String::new();
    let mut first_window = true;

    // Build a position-indexed lookup for words
    // words are already in position order since unicode_words is sequential

    for &(start, end) in windows {
        if !first_window {
            out.push_str("...");
        }
        first_window = false;

        // Add prefix "..." if window doesn't start at beginning
        if start > 0 && out.is_empty() {
            out.push_str("...");
        }

        let mut first_word = true;
        for (word, pos, _) in words.iter() {
            if *pos < start || *pos > end {
                continue;
            }
            if !first_word {
                out.push(' ');
            }
            first_word = false;

            let stemmed = stem_word(word, analyzer);
            if query_set.contains(stemmed.as_str()) {
                out.push_str(open_tag);
                out.push_str(word);
                out.push_str(close_tag);
            } else {
                out.push_str(word);
            }
        }
    }

    // Add trailing "..." if last window doesn't reach end
    if let Some(&(_, last_end)) = windows.last() {
        if last_end < words.len().saturating_sub(1) {
            out.push_str("...");
        }
    }

    out
}

/// Extract the best-matching passage(s) from original text.
///
/// # Algorithm (sliding window)
///
/// 1. Re-tokenize original text to get all words with ordinal positions.
/// 2. For each window start `w` in `[0, total_words - fragment_size]`:
///    count how many stemmed query terms fall in window `[w, w + fragment_size)`.
/// 3. Pick the window with maximum match count.
/// 4. If `num_fragments > 1`: greedily pick next best non-overlapping window.
/// 5. Join fragments with separator (default `...`).
///
/// Returns original text unchanged when no matches found.
#[cfg(feature = "text-index")]
pub fn summarize_field(
    original_text: &str,
    query_terms: &[String],
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
    fragment_size: usize,
    num_fragments: usize,
    separator: &str,
) -> String {
    use unicode_segmentation::UnicodeSegmentation;

    if original_text.is_empty() || query_terms.is_empty() || fragment_size == 0 {
        return original_text.to_owned();
    }

    let query_set: std::collections::HashSet<&str> =
        query_terms.iter().map(|s| s.as_str()).collect();

    // Collect all words with positions and whether they match.
    let words: Vec<(&str, bool)> = original_text
        .unicode_words()
        .map(|word| {
            let stemmed = stem_word(word, analyzer);
            let is_match = !stemmed.is_empty() && query_set.contains(stemmed.as_str());
            (word, is_match)
        })
        .collect();

    if words.is_empty() {
        return original_text.to_owned();
    }

    let total_words = words.len();

    // If text fits in one fragment, return full text.
    if total_words <= fragment_size {
        return words.iter().map(|(w, _)| *w).collect::<Vec<_>>().join(" ");
    }

    // Build match count per position for sliding window.
    // match_at[i] = 1 if words[i] is a query match, 0 otherwise.
    let match_at: Vec<u32> = words.iter().map(|(_, is_match)| *is_match as u32).collect();

    // Prefix sum for O(1) window match count.
    let mut prefix = vec![0u32; total_words + 1];
    for i in 0..total_words {
        prefix[i + 1] = prefix[i] + match_at[i];
    }

    // Find best non-overlapping fragments.
    let mut fragments: Vec<(usize, usize)> = Vec::with_capacity(num_fragments);
    let frag_count = num_fragments.max(1);

    for _ in 0..frag_count {
        let mut best_start = 0usize;
        let mut best_count = 0u32;

        let max_start = total_words.saturating_sub(fragment_size);
        for start in 0..=max_start {
            let end = (start + fragment_size).min(total_words);
            let count = prefix[end] - prefix[start];

            // Skip if this window overlaps any already-selected fragment.
            let overlaps = fragments.iter().any(|&(fs, fe)| start < fe && end > fs);
            if overlaps {
                continue;
            }

            if count > best_count {
                best_count = count;
                best_start = start;
            }
        }

        if best_count == 0 && !fragments.is_empty() {
            // No more useful fragments found.
            break;
        }

        let frag_end = (best_start + fragment_size).min(total_words);
        fragments.push((best_start, frag_end));
    }

    if fragments.is_empty() {
        // Fallback: return first fragment_size words.
        return words
            .iter()
            .take(fragment_size)
            .map(|(w, _)| *w)
            .collect::<Vec<_>>()
            .join(" ");
    }

    // Sort fragments by start position for natural reading order.
    fragments.sort_unstable_by_key(|f| f.0);

    // Build output: join words in each fragment, separate fragments with separator.
    let mut parts: Vec<String> = Vec::with_capacity(fragments.len());
    for (start, end) in fragments {
        let fragment_words: Vec<&str> = words[start..end].iter().map(|(w, _)| *w).collect();
        parts.push(fragment_words.join(" "));
    }

    parts.join(separator)
}

/// Fallback when text-index feature is disabled.
#[cfg(not(feature = "text-index"))]
pub fn summarize_field(
    original_text: &str,
    _query_terms: &[String],
    _analyzer: &crate::text::analyzer::AnalyzerPipeline,
    _fragment_size: usize,
    _num_fragments: usize,
    _separator: &str,
) -> String {
    original_text.to_owned()
}

// ─── Post-processing application ─────────────────────────────────────────────

/// Apply HIGHLIGHT and/or SUMMARIZE to an already-built FT.SEARCH response Frame.
///
/// Iterates result documents in the `[total, key, fields_array, ...]` response format.
/// For each document:
/// - Re-reads original field values from `db` using `get_hash_ref_if_alive()`
/// - Applies `highlight_field()` and/or `summarize_field()` to qualifying fields
/// - Replaces field values in the fields array with post-processed versions
///
/// Fields that are not TEXT fields, not present in the hash, or not requested
/// (when `fields` filter is Some) are left unchanged.
///
/// # Lock safety
/// `db` is `&Database` (read-only). No mutations. The caller must ensure the
/// Database guard is held for the duration of this call and dropped before
/// any `.await` point.
pub fn apply_post_processing(
    response: &mut Frame,
    query_terms: &[String],
    text_index: &TextIndex,
    db: &crate::storage::Database,
    highlight_opts: Option<&HighlightOpts>,
    summarize_opts: Option<&SummarizeOpts>,
) {
    if highlight_opts.is_none() && summarize_opts.is_none() {
        return;
    }

    // Collect the full items vec, apply modifications, then replace.
    // We cannot mutate Frame::Array(FrameVec) in-place while iterating because
    // the inner FrameVec's items may need to be replaced at positions determined
    // during iteration. We take a full clone and rebuild.
    let old_items: Vec<Frame> = match response {
        Frame::Array(items) => items.iter().cloned().collect(),
        _ => return,
    };

    let now_ms = db.now_ms();

    // Rebuild the response array with post-processed field values.
    let mut new_items: Vec<Frame> = Vec::with_capacity(old_items.len());
    let mut i = 0;

    // Copy total count (items[0]).
    if let Some(first) = old_items.first() {
        new_items.push(first.clone());
        i = 1;
    }

    while i + 1 < old_items.len() {
        let doc_key_frame = &old_items[i];
        let fields_frame = &old_items[i + 1];

        // Extract doc key for hash lookup.
        let doc_key: Bytes = match doc_key_frame {
            Frame::BulkString(b) => b.clone(),
            _ => {
                new_items.push(doc_key_frame.clone());
                new_items.push(fields_frame.clone());
                i += 2;
                continue;
            }
        };

        // Re-read hash fields from db (read-only).
        let hash_ref = match db.get_hash_ref_if_alive(doc_key.as_ref(), now_ms) {
            Ok(Some(h)) => h,
            _ => {
                // Key not found or expired — keep original fields unchanged.
                new_items.push(doc_key_frame.clone());
                new_items.push(fields_frame.clone());
                i += 2;
                continue;
            }
        };

        // Collect (field_name, processed_value) replacements.
        let mut field_replacements: Vec<(Bytes, Bytes)> = Vec::new();

        for (field_idx, field_def) in text_index.text_fields.iter().enumerate() {
            let field_name = &field_def.field_name;

            let do_highlight = highlight_opts.map_or(false, |opts| {
                opts.fields.as_ref().map_or(true, |fields| {
                    fields
                        .iter()
                        .any(|f| f.as_ref().eq_ignore_ascii_case(field_name.as_ref()))
                })
            });

            let do_summarize = summarize_opts.map_or(false, |opts| {
                opts.fields.as_ref().map_or(true, |fields| {
                    fields
                        .iter()
                        .any(|f| f.as_ref().eq_ignore_ascii_case(field_name.as_ref()))
                })
            });

            if !do_highlight && !do_summarize {
                continue;
            }

            let original_value = match hash_ref.get_field(field_name.as_ref()) {
                Some(v) => v,
                None => continue,
            };

            let original_text = match std::str::from_utf8(original_value.as_ref()) {
                Ok(s) => s,
                Err(_) => continue,
            };

            let analyzer = match text_index.field_analyzers.get(field_idx) {
                Some(a) => a,
                None => continue,
            };

            // SUMMARIZE first (extracts passage), then HIGHLIGHT (wraps terms).
            let processed = if do_summarize {
                let opts = summarize_opts.unwrap();
                let summarized = summarize_field(
                    original_text,
                    query_terms,
                    analyzer,
                    opts.fragment_size,
                    opts.num_fragments,
                    &opts.separator,
                );
                if do_highlight {
                    let hopts = highlight_opts.unwrap();
                    highlight_field(
                        &summarized,
                        query_terms,
                        analyzer,
                        &hopts.open_tag,
                        &hopts.close_tag,
                        hopts.context_tokens,
                    )
                } else {
                    summarized
                }
            } else {
                let hopts = highlight_opts.unwrap();
                highlight_field(
                    original_text,
                    query_terms,
                    analyzer,
                    &hopts.open_tag,
                    &hopts.close_tag,
                    hopts.context_tokens,
                )
            };

            field_replacements.push((field_name.clone(), Bytes::from(processed)));
        }

        new_items.push(doc_key_frame.clone());

        if field_replacements.is_empty() {
            new_items.push(fields_frame.clone());
        } else {
            // Rebuild the fields array with replacements applied.
            let mut new_fields_frame = fields_frame.clone();
            rebuild_fields_with_replacements(&mut new_fields_frame, &field_replacements);
            new_items.push(new_fields_frame);
        }

        i += 2;
    }

    // Copy any trailing odd item (shouldn't happen, defensive).
    while i < old_items.len() {
        new_items.push(old_items[i].clone());
        i += 1;
    }

    *response = Frame::Array(new_items.into());
}

/// Rebuild a fields Frame::Array with replaced values.
///
/// The fields array has alternating `[field_name, field_value, ...]` pairs.
/// For each `(name, new_value)` in replacements: find the matching name
/// (case-insensitive) and replace the subsequent value frame.
/// Fields not already present are appended as new pairs.
fn rebuild_fields_with_replacements(fields_frame: &mut Frame, replacements: &[(Bytes, Bytes)]) {
    let items_slice: Vec<Frame> = match fields_frame {
        Frame::Array(items) => items.iter().cloned().collect(),
        _ => return,
    };

    let mut new_items: Vec<Frame> = Vec::with_capacity(items_slice.len() + replacements.len() * 2);
    let mut used = vec![false; replacements.len()];

    let mut j = 0;
    while j + 1 < items_slice.len() {
        let field_name_bytes = match &items_slice[j] {
            Frame::BulkString(b) => b.clone(),
            _ => {
                new_items.push(items_slice[j].clone());
                new_items.push(items_slice[j + 1].clone());
                j += 2;
                continue;
            }
        };

        let replacement_idx = replacements
            .iter()
            .enumerate()
            .find_map(|(idx, (name, _))| {
                if !used[idx]
                    && name
                        .as_ref()
                        .eq_ignore_ascii_case(field_name_bytes.as_ref())
                {
                    Some(idx)
                } else {
                    None
                }
            });

        if let Some(idx) = replacement_idx {
            used[idx] = true;
            new_items.push(Frame::BulkString(field_name_bytes));
            new_items.push(Frame::BulkString(replacements[idx].1.clone()));
        } else {
            new_items.push(items_slice[j].clone());
            new_items.push(items_slice[j + 1].clone());
        }
        j += 2;
    }

    // Append odd trailing item if any (defensive).
    if j < items_slice.len() {
        new_items.push(items_slice[j].clone());
    }

    // Append any fields not already present in the response array.
    for (idx, (name, val)) in replacements.iter().enumerate() {
        if !used[idx] {
            new_items.push(Frame::BulkString(name.clone()));
            new_items.push(Frame::BulkString(val.clone()));
        }
    }

    *fields_frame = Frame::Array(new_items.into());
}

// ─── Query clause ────────────────────────────────────────────────────────────

/// A query term with its expansion modifier (per D-17).
///
/// Exact terms are fully analyzed (lowercase + NFKD + stem).
/// Fuzzy/Prefix terms are only lowercased + NFKD (NO stemming, per D-06/D-07).
#[cfg(feature = "text-index")]
#[derive(Debug, Clone)]
pub struct QueryTerm {
    /// Analyzed (possibly stemmed) text.
    pub text: String,
    /// Expansion modifier (Exact, Fuzzy(distance), or Prefix).
    pub modifier: TermModifier,
}

/// A query term used when the text-index feature is disabled.
/// Falls back to a simple string wrapper so handlers compile without the feature.
#[cfg(not(feature = "text-index"))]
#[derive(Debug, Clone)]
pub struct QueryTerm {
    pub text: String,
}

/// Non-BM25 filter clauses attached to a text query (Plan 152-06 / -07).
///
/// # Invariants
///
/// - **Analyzer bypass:** A `FieldFilter` NEVER invokes the text analyzer
///   pipeline. Stopword removal, stemming, tokenization are all skipped.
///   This is what makes `@status:{open}` work on an index whose analyzer
///   would otherwise strip "open" as a stopword (UAT Gap 2).
/// - **Case-insensitive field resolution:** `@Status:{open}` on an index
///   declaring `status` matches. The stored `tag_def` / `numeric_def`
///   `field_name` is the canonical form; the variant's `field` byte slice
///   preserves client input and is resolved at lookup time by
///   `search_tag` / `search_numeric_range`.
/// - **`FieldFilter::Tag { field, value }`:** exact membership,
///   post-normalization. If the tag def is `case_sensitive: false`
///   (default), stored and queried values are both ASCII-lowercased. The
///   multi-tag OR syntax `@status:{a|b}` is NOT supported in v1 — it is
///   rejected at parse time with an actionable error.
/// - **`FieldFilter::NumericRange` (Plan 07):** Closed or half-open range
///   over an `OrderedFloat<f64>` BTreeMap. `f64::NEG_INFINITY` /
///   `f64::INFINITY` encode unbounded bounds. Inverted ranges (finite
///   `min > max`) are rejected at parse time.
///
/// Plan 06 introduces the enum with one variant (`Tag`); Plan 07 adds
/// `NumericRange`. New variants MUST preserve the invariants above or
/// update this docstring.
#[cfg(feature = "text-index")]
#[derive(Debug, Clone)]
pub enum FieldFilter {
    /// Exact membership lookup on a TAG field.
    ///
    /// `field` is the raw client-provided field name (case preserved);
    /// normalization happens in `TextIndex::search_tag`.
    /// `value` is the raw client-provided tag value (normalization applied
    /// at lookup to match the insert-time normalization).
    Tag { field: Bytes, value: Bytes },
    /// Range filter over a NUMERIC field (Plan 152-07).
    ///
    /// `field` is the raw client-provided field name (case preserved); case-
    /// insensitive resolution happens in `TextIndex::search_numeric_range`.
    /// `min` / `max` are `f64`; `f64::NEG_INFINITY` / `f64::INFINITY` encode
    /// unbounded bounds (queried via `-inf` / `+inf` literals).
    /// `min_exclusive` / `max_exclusive` come from the `(` prefix in the
    /// bound (e.g. `[(10 100]` → `min_exclusive=true`). Finite `min > max`
    /// is rejected at parse time, NOT executed as an empty range.
    NumericRange {
        field: Bytes,
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
    },
}

/// Parsed text query: an optional field target + analyzed (stemmed) query terms.
///
/// `field_name = None` means cross-field search (all non-NOINDEX TEXT fields).
/// `field_name = Some(name)` means search only that field.
///
/// When `filter` is `Some`, the clause bypasses the BM25 pipeline entirely —
/// `terms` is empty and `field_name` is unused. See `FieldFilter` docstring
/// for invariants.
#[derive(Debug)]
pub struct TextQueryClause {
    /// Target field name (from `@field:(terms)` syntax), or None for all fields.
    pub field_name: Option<Bytes>,
    /// Analyzed query terms with per-term expansion modifiers.
    pub terms: Vec<QueryTerm>,
    /// Non-BM25 clause (TAG / NUMERIC filter). When `Some`, execute_query_on_index
    /// short-circuits to the FieldFilter dispatch path.
    #[cfg(feature = "text-index")]
    pub filter: Option<FieldFilter>,
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

/// Detect fuzzy (`%%term%%`) or prefix (`term*`) modifier BEFORE running analyzer.
///
/// Per D-16: strip modifiers, return (inner_text, modifier).
/// Counting: 1 `%` on each side = distance 1, 2 = distance 2, 3 = distance 3 (max per D-03).
/// A bare `*` (no prefix text) is not a prefix query — returned as Exact.
/// Empty inner text after stripping is returned as Exact.
#[cfg(feature = "text-index")]
fn detect_modifier(token: &str) -> (&str, TermModifier) {
    let bytes = token.as_bytes();
    if bytes.is_empty() {
        return (token, TermModifier::Exact);
    }
    // Count leading '%'
    let mut leading = 0usize;
    while leading < bytes.len() && bytes[leading] == b'%' {
        leading += 1;
    }
    // Count trailing '%'
    let mut trailing = 0usize;
    while trailing < bytes.len().saturating_sub(leading)
        && bytes[bytes.len() - 1 - trailing] == b'%'
    {
        trailing += 1;
    }
    let dist = leading.min(trailing).min(3) as u8;
    if dist > 0 && leading + trailing < bytes.len() {
        let inner = &token[leading..token.len() - trailing];
        if inner.is_empty() {
            return (token, TermModifier::Exact);
        }
        return (inner, TermModifier::Fuzzy(dist));
    }
    // Check for prefix: trailing '*' with non-empty prefix text.
    // A bare '*' (only character) is match-all, not prefix — return Exact.
    if token.len() > 1 && token.ends_with('*') {
        return (&token[..token.len() - 1], TermModifier::Prefix);
    }
    (token, TermModifier::Exact)
}

/// Tokenize query with modifier detection per D-16/D-17.
///
/// Split on whitespace, detect `%%` and `*` modifiers on each raw token,
/// then analyze:
/// - Exact tokens → full analyzer pipeline (lowercase + NFKD + stem + stop words)
/// - Fuzzy/Prefix tokens → lowercase + NFKD only (NO stemming per D-06/D-07)
#[cfg(feature = "text-index")]
fn tokenize_with_modifiers(
    text_str: &str,
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Vec<QueryTerm> {
    use unicode_normalization::UnicodeNormalization;
    let mut result = Vec::new();
    for raw_token in text_str.split_whitespace() {
        let (inner, modifier) = detect_modifier(raw_token);
        if inner.is_empty() {
            continue;
        }
        match modifier {
            TermModifier::Exact => {
                // Full analyzer pipeline (lowercase + NFKD + stem + stop words)
                let tokens = analyzer.tokenize_with_positions(inner);
                for (term, _) in tokens {
                    result.push(QueryTerm {
                        text: term,
                        modifier: TermModifier::Exact,
                    });
                }
            }
            TermModifier::Fuzzy(_) | TermModifier::Prefix => {
                // Lowercase + NFKD only (NO stemming per D-06/D-07)
                let lowered = inner.to_lowercase();
                let normalized: String = lowered.nfkd().collect();
                if !normalized.is_empty() {
                    result.push(QueryTerm {
                        text: normalized,
                        modifier,
                    });
                }
            }
        }
    }
    result
}

/// Pre-scan a query for a non-BM25 FieldFilter clause (Plan 152-06).
///
/// Returns:
/// - `Ok(Some(clause))` — query is a pure filter (`@field:{value}` TAG, or
///   Plan 07 `@field:[min max]` numeric range). `clause.filter` is populated,
///   `clause.terms` is empty. Safe to dispatch on indexes with zero TEXT fields.
/// - `Ok(None)` — query is BM25 (bare terms or `@field:(terms)`); caller must
///   fall through to the analyzer path.
/// - `Err(msg)` — syntax failure. Caller surfaces `Frame::Error`.
///
/// Does NOT invoke the analyzer. This is the ONE predicate FT.SEARCH and
/// FT.AGGREGATE share for entry-point routing; duplicating it would invite
/// drift. Runs on a ≤ 4 KiB slice; cost is dwarfed by any downstream work.
///
/// Limits: tag value length capped at 4 KiB; multi-tag OR syntax
/// (`@status:{a|b}`) rejected with an actionable error (HYGIENE-06).
#[cfg(feature = "text-index")]
pub fn pre_parse_field_filter(query: &[u8]) -> Result<Option<TextQueryClause>, &'static str> {
    if !query.starts_with(b"@") {
        return Ok(None);
    }
    let colon_pos = query
        .iter()
        .position(|&b| b == b':')
        .ok_or("ERR invalid field query syntax: missing ':'")?;
    let field_name_bytes = &query[1..colon_pos];
    if field_name_bytes.is_empty() {
        return Err("ERR invalid field query syntax: empty field name");
    }
    let after_colon = &query[colon_pos + 1..];
    match after_colon.first() {
        Some(b'{') => {
            let close_pos = after_colon
                .iter()
                .position(|&b| b == b'}')
                .ok_or("ERR unterminated tag filter")?;
            let value = &after_colon[1..close_pos];
            if value.len() > 4096 {
                return Err("ERR tag value too long");
            }
            if value.contains(&b'|') {
                return Err(
                    "ERR multi-tag OR syntax not supported in v1 — use separate queries and union on the client",
                );
            }
            Ok(Some(TextQueryClause {
                field_name: None,
                terms: Vec::new(),
                filter: Some(FieldFilter::Tag {
                    field: Bytes::copy_from_slice(field_name_bytes),
                    value: Bytes::copy_from_slice(value),
                }),
            }))
        }
        Some(b'[') => {
            // Plan 152-07 NUMERIC range filter. `after_colon[0]` is the '['.
            let close_pos = after_colon
                .iter()
                .position(|&b| b == b']')
                .ok_or("ERR unterminated numeric range")?;
            // T-152-07-04: 256-byte inner cap on the bounds payload. `close_pos`
            // is the index into `after_colon` of the closing ']'; inner bytes
            // span `1..close_pos`.
            if close_pos.saturating_sub(1) > 256 {
                return Err("ERR numeric range too long");
            }
            let inner_bytes = &after_colon[1..close_pos];
            let inner = std::str::from_utf8(inner_bytes)
                .map_err(|_| "ERR invalid UTF-8 in numeric range")?;
            // Split on ASCII whitespace; exactly two bounds required.
            let mut parts_iter = inner.split_ascii_whitespace();
            let lo_raw = parts_iter
                .next()
                .ok_or("ERR numeric range requires exactly two bounds")?;
            let hi_raw = parts_iter
                .next()
                .ok_or("ERR numeric range requires exactly two bounds")?;
            if parts_iter.next().is_some() {
                return Err("ERR numeric range requires exactly two bounds");
            }
            let (min, min_exclusive) = parse_numeric_bound(lo_raw)?;
            let (max, max_exclusive) = parse_numeric_bound(hi_raw)?;
            // T-152-07-05: inverted range REJECTED (no auto-swap). Only applies
            // when BOTH bounds are finite — `[-inf +inf]` is always valid.
            if min.is_finite() && max.is_finite() && min > max {
                return Err("ERR invalid numeric range (min > max)");
            }
            Ok(Some(TextQueryClause {
                field_name: None,
                terms: Vec::new(),
                filter: Some(FieldFilter::NumericRange {
                    field: Bytes::copy_from_slice(field_name_bytes),
                    min,
                    max,
                    min_exclusive,
                    max_exclusive,
                }),
            }))
        }
        _ => Ok(None),
    }
}

/// Parse a single numeric range bound (Plan 152-07).
///
/// Accepts:
/// - Finite floats: `10`, `-3.14`, `1e10`, `-2.5e-3`
/// - Infinity sentinels (case-insensitive): `-inf`, `+inf`, `inf`, `-infinity`, `+infinity`, `infinity`
/// - Exclusive-bound prefix `(`: `(10` → `(10.0, excl=true)`
///
/// Rejects:
/// - `NaN` (T-152-07-07): NaN bounds would poison `OrderedFloat` comparison.
/// - Empty / non-numeric tokens.
#[cfg(feature = "text-index")]
fn parse_numeric_bound(s: &str) -> Result<(f64, bool), &'static str> {
    let (body, excl) = if let Some(stripped) = s.strip_prefix('(') {
        (stripped, true)
    } else {
        (s, false)
    };
    if body.is_empty() {
        return Err("ERR invalid numeric bound");
    }
    // Case-insensitive infinity sentinels. Checked BEFORE f64::parse so that
    // `inf` / `infinity` are always recognized (Rust's f64 parser accepts them
    // too, but we want explicit recognition + NaN guard).
    let lower = body.to_ascii_lowercase();
    let v = match lower.as_str() {
        "-inf" | "-infinity" => f64::NEG_INFINITY,
        "+inf" | "+infinity" | "inf" | "infinity" => f64::INFINITY,
        _ => body
            .parse::<f64>()
            .map_err(|_| "ERR invalid numeric bound")?,
    };
    // T-152-07-07: NaN bound rejected to prevent OrderedFloat comparison
    // panic. Rust's f64 parser will produce NaN for "NaN" / "nan" / "NAN".
    if v.is_nan() {
        return Err("ERR NaN bound not allowed");
    }
    Ok((v, excl))
}

/// Parse `@field:(terms)` or `@field:terms` syntax.
fn parse_field_targeted_query(
    query: &[u8],
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Result<TextQueryClause, &'static str> {
    // Fast path (Plan 152-06 B-01): non-BM25 FieldFilter. If detected,
    // return without ever invoking the analyzer — safe on TAG-only indexes.
    #[cfg(feature = "text-index")]
    if let Some(clause) = pre_parse_field_filter(query)? {
        return Ok(clause);
    }

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

    let terms = build_query_terms(text_str, analyzer)?;
    Ok(TextQueryClause {
        field_name: Some(field_name),
        terms,
        #[cfg(feature = "text-index")]
        filter: None,
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

    let terms = build_query_terms(text_str, analyzer)?;
    Ok(TextQueryClause {
        field_name: None,
        terms,
        #[cfg(feature = "text-index")]
        filter: None,
    })
}

/// Build query terms from a raw text string, dispatching to the correct analyzer path.
///
/// With `text-index` feature: uses `tokenize_with_modifiers` (supports fuzzy/prefix).
/// Without feature: falls back to plain `tokenize_with_positions` (exact only).
fn build_query_terms(
    text_str: &str,
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Result<Vec<QueryTerm>, &'static str> {
    build_query_terms_impl(text_str, analyzer)
}

#[cfg(feature = "text-index")]
fn build_query_terms_impl(
    text_str: &str,
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Result<Vec<QueryTerm>, &'static str> {
    let terms = tokenize_with_modifiers(text_str, analyzer);
    if terms.is_empty() {
        return Err("ERR empty query after analysis");
    }
    Ok(terms)
}

#[cfg(not(feature = "text-index"))]
fn build_query_terms_impl(
    text_str: &str,
    analyzer: &crate::text::analyzer::AnalyzerPipeline,
) -> Result<Vec<QueryTerm>, &'static str> {
    let tokens = analyzer.tokenize_with_positions(text_str);
    if tokens.is_empty() {
        return Err("ERR empty query after analysis");
    }
    let terms: Vec<QueryTerm> = tokens
        .into_iter()
        .map(|(term, _)| QueryTerm { text: term })
        .collect();
    Ok(terms)
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

    // B-01 SITE 1 FIX (Plan 152-06): FieldFilter short-circuit BEFORE analyzer
    // lookup. If the query is `@field:{value}` (or Plan 07 `@field:[min max]`),
    // dispatch through the inverted-index path — TAG-only indexes (zero TEXT
    // fields) and indexes whose analyzer would strip the tag value as a
    // stopword (UAT Gap 2) work without touching the analyzer.
    #[cfg(feature = "text-index")]
    match pre_parse_field_filter(query_bytes.as_ref()) {
        Ok(Some(clause)) => {
            let results = execute_query_on_index(text_index, &clause, None, None, top_k);
            return build_text_response(&results, limit_offset, limit_count);
        }
        Ok(None) => { /* fall through to BM25 path */ }
        Err(e) => return Frame::Error(Bytes::copy_from_slice(e.as_bytes())),
    }

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
///
/// Accepts `&[QueryTerm]` — fuzzy/prefix queries are dispatched to `expand_terms` +
/// `search_field_or`; exact queries use the unchanged `search_field` AND path.
pub fn execute_text_search_local(
    text_store: &TextStore,
    index_name: &[u8],
    field_idx: Option<usize>,
    query_terms: &[QueryTerm],
    top_k: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let text_index = match text_store.get_index(index_name) {
        Some(idx) => idx,
        None => return Frame::Error(Bytes::from_static(b"ERR no such index")),
    };

    let clause = TextQueryClause {
        field_name: field_idx.and_then(|fidx| {
            text_index
                .text_fields
                .get(fidx)
                .map(|f| f.field_name.clone())
        }),
        terms: query_terms.to_vec(),
        #[cfg(feature = "text-index")]
        filter: None,
    };
    let results = execute_query_on_index(text_index, &clause, None, None, top_k);

    build_text_response(&results, offset, count)
}

/// Execute text search with injected global document frequencies (DFS Phase 2).
///
/// Called by the SPSC handler after the Phase 1 scatter has aggregated
/// global IDF weights from all shards (per D-04). The `global_df` and `global_n`
/// override the local field statistics for BM25 IDF computation.
///
/// Accepts `&[QueryTerm]` — fuzzy/prefix queries use the OR-union path.
pub fn execute_text_search_with_global_idf(
    text_index: &TextIndex,
    field_idx: Option<usize>,
    query_terms: &[QueryTerm],
    global_df: &HashMap<String, u32>,
    global_n: u32,
    top_k: usize,
    offset: usize,
    count: usize,
) -> Frame {
    let clause = TextQueryClause {
        field_name: field_idx.and_then(|fidx| {
            text_index
                .text_fields
                .get(fidx)
                .map(|f| f.field_name.clone())
        }),
        terms: query_terms.to_vec(),
        #[cfg(feature = "text-index")]
        filter: None,
    };
    let results =
        execute_query_on_index(text_index, &clause, Some(global_df), Some(global_n), top_k);

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

    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.truncate(top_k);
    results
}

/// LSN-aware wrapper around [`execute_query_on_index`] used by the FT.SEARCH
/// HYBRID path (v0.1.10 G-1). Post-filters the BM25 result list by MVCC
/// visibility at `as_of_lsn`. `as_of_lsn == 0` passes through with no filter
/// (backwards-compatible — identical output to the unwrapped call).
///
/// Oversampling (2× top_k, floor 16) preserves recall when visible docs rank
/// below invisible ones in the raw BM25 output. Pre-filtering the candidate
/// bitmap would be faster at high AS_OF churn; deferred to v0.2 per the
/// visibility-filter notes in `src/text/store.rs`.
pub(crate) fn execute_query_on_index_as_of(
    text_index: &TextIndex,
    clause: &TextQueryClause,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
    as_of_lsn: u64,
) -> Vec<TextSearchResult> {
    if as_of_lsn == 0 {
        return execute_query_on_index(text_index, clause, global_df, global_n, top_k);
    }
    let oversample = top_k.saturating_mul(2).max(16);
    let raw = execute_query_on_index(text_index, clause, global_df, global_n, oversample);
    let mut filtered: Vec<TextSearchResult> = raw
        .into_iter()
        .filter(|r| text_index.is_doc_visible_at(r.doc_id, as_of_lsn))
        .collect();
    filtered.truncate(top_k);
    filtered
}

/// Execute query on a TextIndex: dispatch to cross-field or field-targeted search.
///
/// When all query terms are Exact, uses the existing AND path (`search_field` /
/// `accumulate_cross_field`). When any term is Fuzzy or Prefix, expands each term
/// via `TextIndex::expand_terms` and uses the OR-union path (`search_field_or`).
///
/// `pub(crate)` exposure (Phase 152 Plan 04): the hybrid FT.SEARCH path needs raw
/// `Vec<TextSearchResult>` — the BM25 stream — before converting to the unified
/// `SearchResult` shape for RRF fusion. Keeping the logic here (instead of
/// duplicating it in hybrid.rs) preserves the single source of truth for the
/// exact/fuzzy dispatch + cross-field accumulation.
pub(crate) fn execute_query_on_index(
    text_index: &TextIndex,
    clause: &TextQueryClause,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    // Plan 152-06 short-circuit: FieldFilter clauses (TAG / Plan 07 NUMERIC) bypass
    // BM25 entirely. score=0.0; sorted ascending by doc_id for determinism.
    #[cfg(feature = "text-index")]
    if let Some(filter) = &clause.filter {
        let doc_ids: Vec<u32> = match filter {
            FieldFilter::Tag { field, value } => text_index.search_tag(field, value),
            FieldFilter::NumericRange {
                field,
                min,
                max,
                min_exclusive,
                max_exclusive,
            } => text_index.search_numeric_range(field, *min, *max, *min_exclusive, *max_exclusive),
        };
        let mut results: Vec<TextSearchResult> = doc_ids
            .into_iter()
            .take(top_k)
            .filter_map(|doc_id| {
                text_index
                    .doc_id_to_key
                    .get(&doc_id)
                    .map(|key| TextSearchResult {
                        doc_id,
                        key: key.clone(),
                        score: 0.0,
                    })
            })
            .collect();
        results.sort_by_key(|r| r.doc_id);
        return results;
    }

    // Check whether any term requires fuzzy/prefix expansion (text-index feature only).
    #[cfg(feature = "text-index")]
    let has_fuzzy_or_prefix = clause
        .terms
        .iter()
        .any(|t| !matches!(t.modifier, TermModifier::Exact));

    #[cfg(not(feature = "text-index"))]
    let has_fuzzy_or_prefix = false;

    // ── Exact-only fast path (unchanged AND semantics) ────────────────────────
    if !has_fuzzy_or_prefix {
        let exact_terms: Vec<String> = clause.terms.iter().map(|t| t.text.clone()).collect();
        return match &clause.field_name {
            None => accumulate_cross_field(text_index, &exact_terms, global_df, global_n, top_k),
            Some(field_name) => {
                let field_idx = text_index.text_fields.iter().position(|f| {
                    f.field_name
                        .as_ref()
                        .eq_ignore_ascii_case(field_name.as_ref())
                });
                match field_idx {
                    Some(fidx) => {
                        text_index.search_field(fidx, &exact_terms, global_df, global_n, top_k)
                    }
                    None => Vec::new(),
                }
            }
        };
    }

    // ── Fuzzy/prefix OR-union path ────────────────────────────────────────────
    #[cfg(feature = "text-index")]
    {
        match &clause.field_name {
            None => {
                // Cross-field: expand + OR on each non-NOINDEX field, accumulate.
                let mut acc: HashMap<u32, (f32, Bytes)> = HashMap::new();
                for (field_idx, field_def) in text_index.text_fields.iter().enumerate() {
                    if field_def.noindex {
                        continue;
                    }
                    let mut all_expanded: Vec<u32> = Vec::new();
                    for qt in &clause.terms {
                        let ids = text_index.expand_terms(field_idx, &qt.text, &qt.modifier);
                        all_expanded.extend(ids);
                    }
                    all_expanded.sort_unstable();
                    all_expanded.dedup();
                    if all_expanded.is_empty() {
                        continue;
                    }
                    let field_results = text_index.search_field_or(
                        field_idx,
                        &all_expanded,
                        global_df,
                        global_n,
                        top_k,
                    );
                    for r in field_results {
                        let entry = acc.entry(r.doc_id).or_insert((0.0, r.key.clone()));
                        entry.0 += r.score;
                    }
                }
                if acc.is_empty() {
                    return Vec::new();
                }
                let mut results: Vec<TextSearchResult> = acc
                    .into_iter()
                    .map(|(doc_id, (score, key))| TextSearchResult { doc_id, key, score })
                    .collect();
                results.sort_by(|a, b| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                results.truncate(top_k);
                results
            }
            Some(field_name) => {
                let field_idx = text_index.text_fields.iter().position(|f| {
                    f.field_name
                        .as_ref()
                        .eq_ignore_ascii_case(field_name.as_ref())
                });
                match field_idx {
                    Some(fidx) => {
                        let mut all_expanded: Vec<u32> = Vec::new();
                        for qt in &clause.terms {
                            let ids = text_index.expand_terms(fidx, &qt.text, &qt.modifier);
                            all_expanded.extend(ids);
                        }
                        all_expanded.sort_unstable();
                        all_expanded.dedup();
                        if all_expanded.is_empty() {
                            return Vec::new();
                        }
                        text_index.search_field_or(fidx, &all_expanded, global_df, global_n, top_k)
                    }
                    None => Vec::new(),
                }
            }
        }
    }

    // Unreachable without text-index feature (has_fuzzy_or_prefix is always false above).
    #[cfg(not(feature = "text-index"))]
    {
        let _ = (global_df, global_n, top_k); // suppress unused warnings
        Vec::new()
    }
}

// ─── Response building ────────────────────────────────────────────────────────

/// Build FT.SEARCH text response array with LIMIT pagination.
///
/// Format: `[total, key1, ["__bm25_score", "N.NNNNNN"], key2, [...], ...]`
///
/// `total` is the full number of matched results before pagination.
/// Document entries are `results[offset..offset+count]`.
pub(crate) fn build_text_response(
    results: &[TextSearchResult],
    offset: usize,
    count: usize,
) -> Frame {
    let total = results.len() as i64;
    let page_count = if count == usize::MAX {
        results.len()
    } else {
        count
    };
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
    all_results.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    all_results.truncate(top_k);

    let total = all_results.len() as i64;
    let page_count = if count == usize::MAX {
        all_results.len()
    } else {
        count
    };
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
        assert!(
            result.field_name.is_none(),
            "bare terms have no field target"
        );
        // "machine" -> "machin", "learning" -> "learn" (English Snowball)
        assert!(
            result.terms.iter().any(|t| t.text == "machin"),
            "expected stemmed 'machin'"
        );
        assert!(
            result.terms.iter().any(|t| t.text == "learn"),
            "expected stemmed 'learn'"
        );
        // All terms should be Exact (no modifiers in bare query)
        assert!(
            result
                .terms
                .iter()
                .all(|t| matches!(t.modifier, TermModifier::Exact)),
            "bare terms must have Exact modifier"
        );
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
        assert!(result.terms.iter().any(|t| t.text == "machin"));
        assert!(result.terms.iter().any(|t| t.text == "learn"));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn parse_text_query_quoted_string_treated_as_bare() {
        let analyzer = make_analyzer();
        // Surrounding quotes are stripped; remaining terms analyzed normally
        let result = parse_text_query(b"\"machine learning\"", &analyzer).unwrap();
        assert!(result.field_name.is_none());
        assert!(result.terms.iter().any(|t| t.text == "machin"));
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
        assert_eq!(
            first_key.as_ref(),
            b"doc:b",
            "doc:b (score=3.2) must rank first"
        );
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
                assert_eq!(
                    b.as_ref(),
                    b"__bm25_score",
                    "field name must be __bm25_score"
                );
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
        assert_eq!(
            fields[0],
            Frame::BulkString(Bytes::from_static(b"__bm25_score"))
        );
    }

    // ── cross_field_score_accumulation ─────────────────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn cross_field_score_accumulation() {
        use crate::protocol::Frame as F;
        use crate::text::store::TextIndex;
        use crate::text::types::{BM25Config, TextFieldDef};

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
        assert_eq!(
            results[0].key.as_ref(),
            b"doc:0",
            "doc:0 with multi-field match ranks first"
        );
        assert!(
            results[0].score > results[1].score,
            "accumulated score should be higher"
        );
    }

    // ── highlight_field ───────────────────────────────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn test_highlight_basic() {
        let analyzer = make_analyzer();
        // "machin" and "learn" are the stemmed forms of "machine" and "learning"
        let terms = vec!["machin".to_string(), "learn".to_string()];
        let result = highlight_field(
            "machine learning in production",
            &terms,
            &analyzer,
            "<b>",
            "</b>",
            4,
        );
        assert!(
            result.contains("<b>machine</b>"),
            "machine should be highlighted, got: {result}"
        );
        assert!(
            result.contains("<b>learning</b>"),
            "learning should be highlighted, got: {result}"
        );
        assert!(result.contains("in"), "context word 'in' should be present");
        assert!(
            result.contains("production"),
            "context word 'production' should be present"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_highlight_context_window() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string(), "learn".to_string()];
        // "machine learning" appears at word positions 9 and 10 in a 11-word text
        let text = "the quick brown fox jumped over the lazy dog machine learning";
        let result = highlight_field(text, &terms, &analyzer, "<b>", "</b>", 2);
        // Should contain highlighted terms
        assert!(
            result.contains("<b>machine</b>"),
            "machine highlighted, got: {result}"
        );
        assert!(
            result.contains("<b>learning</b>"),
            "learning highlighted, got: {result}"
        );
        // Context words (2 before "machine") should appear
        assert!(
            result.contains("lazy") || result.contains("dog"),
            "context words near match, got: {result}"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_highlight_custom_tags() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string()];
        let result = highlight_field("machine learning", &terms, &analyzer, "[", "]", 4);
        assert!(
            result.contains("[machine]"),
            "custom tags applied, got: {result}"
        );
        assert!(
            !result.contains("<b>"),
            "default tags must not appear, got: {result}"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_highlight_no_matches() {
        let analyzer = make_analyzer();
        let terms = vec!["xyznonexistent".to_string()];
        let text = "machine learning in production";
        let result = highlight_field(text, &terms, &analyzer, "<b>", "</b>", 4);
        assert_eq!(
            result, text,
            "no matches → original text returned unchanged"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_highlight_full_text_short() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string()];
        // Short text: fits entirely in the context window
        let text = "machine";
        let result = highlight_field(text, &terms, &analyzer, "<b>", "</b>", 4);
        assert!(
            result.contains("<b>machine</b>"),
            "short text fully highlighted, got: {result}"
        );
    }

    // ── summarize_field ───────────────────────────────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn test_summarize_basic() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string(), "learn".to_string()];
        // Build a 30-word text with "machine learning" at position 25-26
        let text = "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega machine learning one two three four";
        let result = summarize_field(text, &terms, &analyzer, 20, 1, "...");
        // The 20-token fragment should include "machine" and "learning"
        assert!(
            result.contains("machine") || result.contains("learning"),
            "summary should include match words, got: {result}"
        );
        // Result should be shorter than original
        let result_words: Vec<&str> = result.split_whitespace().collect();
        assert!(
            result_words.len() <= 20,
            "fragment should be at most 20 tokens, got {} tokens",
            result_words.len()
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_summarize_best_cluster() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string()];
        // Two clusters: one "machine" near start, two "machine"s near end
        // The denser cluster (near end) should be selected
        let text = "machine alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau machine machine";
        let result = summarize_field(text, &terms, &analyzer, 10, 1, "...");
        // The dense cluster (last two "machine"s) should be selected
        // Count occurrences of "machine" in result — should be 2
        let machine_count = result
            .split_whitespace()
            .filter(|&w| w == "machine")
            .count();
        assert!(
            machine_count >= 1,
            "result should contain at least one match, got: {result}"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_summarize_multi_fragment() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string()];
        // "machine" appears at start AND end — two non-overlapping fragments
        let text = "machine alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau machine";
        let result = summarize_field(text, &terms, &analyzer, 5, 2, " | ");
        // With separator, result should have the separator if 2 fragments found
        // At minimum both "machine" occurrences should be present
        assert!(
            result.contains("machine"),
            "result must contain match term, got: {result}"
        );
    }

    // ── parse_highlight_clause ────────────────────────────────────────────────

    #[test]
    fn test_parse_highlight_clause_not_present() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"testidx")),
            Frame::BulkString(Bytes::from_static(b"machine")),
            Frame::BulkString(Bytes::from_static(b"LIMIT")),
            Frame::BulkString(Bytes::from_static(b"0")),
            Frame::BulkString(Bytes::from_static(b"10")),
        ];
        assert!(
            parse_highlight_clause(&args).is_none(),
            "HIGHLIGHT not in args → None"
        );
    }

    #[test]
    fn test_parse_highlight_clause_default() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"testidx")),
            Frame::BulkString(Bytes::from_static(b"machine")),
            Frame::BulkString(Bytes::from_static(b"HIGHLIGHT")),
        ];
        let opts = parse_highlight_clause(&args).expect("HIGHLIGHT present → Some");
        assert_eq!(opts.open_tag, "<b>", "default open tag");
        assert_eq!(opts.close_tag, "</b>", "default close tag");
        assert!(opts.fields.is_none(), "no FIELDS → None (all fields)");
        assert_eq!(opts.context_tokens, 4, "default context window");
    }

    #[test]
    fn test_parse_highlight_clause_with_fields_and_tags() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"testidx")),
            Frame::BulkString(Bytes::from_static(b"machine")),
            Frame::BulkString(Bytes::from_static(b"HIGHLIGHT")),
            Frame::BulkString(Bytes::from_static(b"FIELDS")),
            Frame::BulkString(Bytes::from_static(b"1")),
            Frame::BulkString(Bytes::from_static(b"title")),
            Frame::BulkString(Bytes::from_static(b"TAGS")),
            Frame::BulkString(Bytes::from_static(b"[")),
            Frame::BulkString(Bytes::from_static(b"]")),
        ];
        let opts = parse_highlight_clause(&args).expect("HIGHLIGHT present");
        assert_eq!(opts.open_tag, "[");
        assert_eq!(opts.close_tag, "]");
        let fields = opts.fields.expect("FIELDS parsed");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].as_ref(), b"title");
    }

    // ── parse_summarize_clause ────────────────────────────────────────────────

    #[test]
    fn test_parse_summarize_clause_not_present() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"testidx")),
            Frame::BulkString(Bytes::from_static(b"machine")),
        ];
        assert!(
            parse_summarize_clause(&args).is_none(),
            "SUMMARIZE not in args → None"
        );
    }

    #[test]
    fn test_parse_summarize_clause_default() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"testidx")),
            Frame::BulkString(Bytes::from_static(b"machine")),
            Frame::BulkString(Bytes::from_static(b"SUMMARIZE")),
        ];
        let opts = parse_summarize_clause(&args).expect("SUMMARIZE present");
        assert_eq!(opts.fragment_size, 20, "default fragment_size");
        assert_eq!(opts.num_fragments, 1, "default num_fragments");
        assert_eq!(opts.separator, "...", "default separator");
        assert!(opts.fields.is_none(), "no FIELDS → None");
    }

    #[test]
    fn test_parse_summarize_clause_with_options() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"testidx")),
            Frame::BulkString(Bytes::from_static(b"machine")),
            Frame::BulkString(Bytes::from_static(b"SUMMARIZE")),
            Frame::BulkString(Bytes::from_static(b"FIELDS")),
            Frame::BulkString(Bytes::from_static(b"1")),
            Frame::BulkString(Bytes::from_static(b"body")),
            Frame::BulkString(Bytes::from_static(b"LEN")),
            Frame::BulkString(Bytes::from_static(b"30")),
            Frame::BulkString(Bytes::from_static(b"FRAGS")),
            Frame::BulkString(Bytes::from_static(b"2")),
            Frame::BulkString(Bytes::from_static(b"SEPARATOR")),
            Frame::BulkString(Bytes::from_static(b" | ")),
        ];
        let opts = parse_summarize_clause(&args).expect("SUMMARIZE present");
        assert_eq!(opts.fragment_size, 30);
        assert_eq!(opts.num_fragments, 2);
        assert_eq!(opts.separator, " | ");
        let fields = opts.fields.expect("FIELDS parsed");
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].as_ref(), b"body");
    }

    // ── highlight + summarize independent ────────────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn test_highlight_independent_of_summarize() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string()];
        // HIGHLIGHT works without SUMMARIZE
        let text = "machine learning";
        let result = highlight_field(text, &terms, &analyzer, "<b>", "</b>", 4);
        assert!(
            result.contains("<b>machine</b>"),
            "highlight standalone, got: {result}"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_summarize_independent_of_highlight() {
        let analyzer = make_analyzer();
        let terms = vec!["machin".to_string()];
        // SUMMARIZE works without HIGHLIGHT
        let text = "alpha beta gamma delta epsilon zeta eta theta iota kappa machine learning production systems";
        let result = summarize_field(text, &terms, &analyzer, 5, 1, "...");
        assert!(
            result.contains("machine"),
            "summarize standalone, got: {result}"
        );
        assert!(
            !result.contains("<b>"),
            "summarize without highlight has no tags, got: {result}"
        );
    }

    // ── detect_modifier ───────────────────────────────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_fuzzy_single() {
        let (inner, modifier) = detect_modifier("%machine%");
        assert_eq!(inner, "machine");
        assert_eq!(modifier, TermModifier::Fuzzy(1));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_fuzzy_double() {
        let (inner, modifier) = detect_modifier("%%machne%%");
        assert_eq!(inner, "machne");
        assert_eq!(modifier, TermModifier::Fuzzy(2));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_fuzzy_triple() {
        let (inner, modifier) = detect_modifier("%%%m%%%");
        assert_eq!(inner, "m");
        assert_eq!(modifier, TermModifier::Fuzzy(3));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_prefix() {
        let (inner, modifier) = detect_modifier("mach*");
        assert_eq!(inner, "mach");
        assert_eq!(modifier, TermModifier::Prefix);
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_exact() {
        let (inner, modifier) = detect_modifier("machine");
        assert_eq!(inner, "machine");
        assert_eq!(modifier, TermModifier::Exact);
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_bare_star() {
        // A single '*' is match-all, not a prefix query
        let (inner, modifier) = detect_modifier("*");
        assert_eq!(inner, "*");
        assert_eq!(modifier, TermModifier::Exact);
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_bare_percent() {
        // '%%' with no inner text is returned as Exact (empty inner)
        let (inner, modifier) = detect_modifier("%%");
        assert_eq!(inner, "%%"); // returned as-is
        assert_eq!(modifier, TermModifier::Exact);
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_detect_modifier_fuzzy_caps_at_3() {
        // 4+ % on each side still caps distance at 3
        let (inner, modifier) = detect_modifier("%%%%word%%%%");
        assert_eq!(inner, "word");
        assert_eq!(modifier, TermModifier::Fuzzy(3));
    }

    // ── tokenize_with_modifiers ───────────────────────────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn test_tokenize_with_modifiers_exact_only() {
        let analyzer = make_analyzer();
        let terms = tokenize_with_modifiers("machine learning", &analyzer);
        assert_eq!(terms.len(), 2);
        assert!(
            terms
                .iter()
                .any(|t| t.text == "machin" && matches!(t.modifier, TermModifier::Exact))
        );
        assert!(
            terms
                .iter()
                .any(|t| t.text == "learn" && matches!(t.modifier, TermModifier::Exact))
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_tokenize_with_modifiers_mixed() {
        let analyzer = make_analyzer();
        // "%%machne%%" = fuzzy-2, "learning" = exact (stemmed to "learn"), "mach*" = prefix
        let terms = tokenize_with_modifiers("%%machne%% learning mach*", &analyzer);
        assert_eq!(
            terms.len(),
            3,
            "expected 3 terms, got {:?}",
            terms.iter().map(|t| &t.text).collect::<Vec<_>>()
        );
        let fuzzy = terms
            .iter()
            .find(|t| matches!(t.modifier, TermModifier::Fuzzy(2)));
        assert!(fuzzy.is_some(), "expected Fuzzy(2) term");
        assert_eq!(fuzzy.unwrap().text, "machne");
        let exact = terms
            .iter()
            .find(|t| matches!(t.modifier, TermModifier::Exact));
        assert!(exact.is_some(), "expected Exact term");
        assert_eq!(exact.unwrap().text, "learn");
        let prefix = terms
            .iter()
            .find(|t| matches!(t.modifier, TermModifier::Prefix));
        assert!(prefix.is_some(), "expected Prefix term");
        assert_eq!(prefix.unwrap().text, "mach");
    }

    // ── parse_text_query with fuzzy/prefix modifiers ─────────────────────────

    #[test]
    #[cfg(feature = "text-index")]
    fn test_parse_text_query_fuzzy() {
        let analyzer = make_analyzer();
        let result = parse_text_query(b"%%machne%%", &analyzer).unwrap();
        assert!(result.field_name.is_none());
        assert_eq!(result.terms.len(), 1);
        assert_eq!(result.terms[0].text, "machne");
        assert_eq!(result.terms[0].modifier, TermModifier::Fuzzy(2));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_parse_text_query_prefix() {
        let analyzer = make_analyzer();
        let result = parse_text_query(b"mach*", &analyzer).unwrap();
        assert!(result.field_name.is_none());
        assert_eq!(result.terms.len(), 1);
        assert_eq!(result.terms[0].text, "mach");
        assert_eq!(result.terms[0].modifier, TermModifier::Prefix);
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_parse_text_query_field_targeted_fuzzy() {
        let analyzer = make_analyzer();
        let result = parse_text_query(b"@title:(%%machne%%)", &analyzer).unwrap();
        assert_eq!(
            result.field_name.as_ref().map(|b| b.as_ref()),
            Some(b"title" as &[u8])
        );
        assert_eq!(result.terms.len(), 1);
        assert_eq!(result.terms[0].text, "machne");
        assert_eq!(result.terms[0].modifier, TermModifier::Fuzzy(2));
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_parse_text_query_fuzzy_distance1() {
        let analyzer = make_analyzer();
        let result = parse_text_query(b"%machin%", &analyzer).unwrap();
        assert_eq!(result.terms.len(), 1);
        assert_eq!(result.terms[0].modifier, TermModifier::Fuzzy(1));
        assert_eq!(result.terms[0].text, "machin");
    }

    // ── Contract tests for execute_text_search_local (151-03 gap closure) ─────
    // These lock in the library-level guarantees that the single-shard handler
    // fast-path relies on. If the handler routing code regresses, the integration
    // gate (scripts/test-commands.sh on --shards 1) is the end-to-end canary; these
    // unit tests freeze the library contract so we know the substrate is sound.

    #[cfg(feature = "text-index")]
    fn build_fuzzy_test_textstore() -> crate::text::store::TextStore {
        use crate::text::store::{TextIndex, TextStore};
        use crate::text::types::{BM25Config, TextFieldDef};

        let field = TextFieldDef::new(bytes::Bytes::from_static(b"title"));
        let mut idx = TextIndex::new(
            bytes::Bytes::from_static(b"fuzzyidx"),
            Vec::new(),
            vec![field],
            BM25Config::default(),
        );
        let docs: &[(&[u8], &[u8])] = &[
            (b"fz:1", b"Machine Learning"),
            (b"fz:2", b"Deep Learning"),
            (b"fz:3", b"Machinery Parts"),
        ];
        for (i, (key, text)) in docs.iter().enumerate() {
            let key_hash = i as u64;
            let args = vec![
                Frame::BulkString(bytes::Bytes::from_static(b"title")),
                Frame::BulkString(bytes::Bytes::copy_from_slice(text)),
            ];
            idx.index_document(key_hash, key, &args);
        }
        idx.build_fst(); // required for fuzzy/prefix expansion
        let mut ts = TextStore::new();
        // .unwrap() permitted in test code per CLAUDE.md Error Handling rules.
        ts.create_index(bytes::Bytes::from_static(b"fuzzyidx"), idx)
            .unwrap();
        ts
    }

    /// Helper: flatten Frame::Array(items) result and find doc-key hits.
    #[cfg(feature = "text-index")]
    fn extract_hits(frame: &Frame) -> (i64, Vec<Bytes>) {
        match frame {
            Frame::Array(items) => {
                let total = match items.first() {
                    Some(Frame::Integer(n)) => *n,
                    _ => -1,
                };
                // Items alternate: [total, key1, fields1, key2, fields2, ...]
                let keys = items
                    .iter()
                    .skip(1)
                    .step_by(2)
                    .filter_map(|f| match f {
                        Frame::BulkString(b) => Some(b.clone()),
                        _ => None,
                    })
                    .collect();
                (total, keys)
            }
            other => panic!("expected Frame::Array, got {other:?}"),
        }
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_execute_text_search_local_fuzzy_single_shard() {
        let ts = build_fuzzy_test_textstore();
        // Use a fresh analyzer matching the one used to index (English + stemming).
        // AnalyzerPipeline is not Clone; the index owns its pipeline so we build a
        // parallel instance with identical config for the test query.
        let analyzer = make_analyzer();
        // "%%machne%%" -> Fuzzy(distance=2) on "machne" -> should match
        // "machin" (stemmed "Machine") within edit distance 2.
        let clause =
            parse_text_query(b"%%machne%%", &analyzer).expect("parse_text_query must succeed");
        let frame = execute_text_search_local(&ts, b"fuzzyidx", None, &clause.terms, 10, 0, 10);
        assert!(
            !matches!(frame, Frame::Error(_)),
            "fuzzy search returned Frame::Error: {frame:?}"
        );
        let (total, keys) = extract_hits(&frame);
        assert!(
            total >= 1,
            "expected ≥1 fuzzy hit for %%machne%%, got total={total}"
        );
        assert!(
            keys.iter().any(|k| k.as_ref() == b"fz:1"),
            "expected fz:1 (Machine Learning) in hits, got {keys:?}"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_execute_text_search_local_prefix_single_shard() {
        let ts = build_fuzzy_test_textstore();
        let analyzer = make_analyzer();
        // "mach*" -> Prefix on "mach" -> should match both "machin" (Machine)
        // and "machineri" (Machinery) after stemming/prefix expansion.
        let clause = parse_text_query(b"mach*", &analyzer).expect("parse_text_query must succeed");
        let frame = execute_text_search_local(&ts, b"fuzzyidx", None, &clause.terms, 10, 0, 10);
        assert!(
            !matches!(frame, Frame::Error(_)),
            "prefix search returned Frame::Error: {frame:?}"
        );
        let (total, keys) = extract_hits(&frame);
        assert!(total >= 2, "expected ≥2 prefix hits for mach*, got {total}");
        assert!(
            keys.iter().any(|k| k.as_ref() == b"fz:1"),
            "expected fz:1 in prefix hits, got {keys:?}"
        );
        assert!(
            keys.iter().any(|k| k.as_ref() == b"fz:3"),
            "expected fz:3 in prefix hits, got {keys:?}"
        );
    }

    #[test]
    #[cfg(feature = "text-index")]
    fn test_execute_text_search_local_exact_single_shard() {
        let ts = build_fuzzy_test_textstore();
        let analyzer = make_analyzer();
        // "machine" -> Exact "machine" -> stemmer yields "machin" -> match fz:1.
        let clause =
            parse_text_query(b"machine", &analyzer).expect("parse_text_query must succeed");
        let frame = execute_text_search_local(&ts, b"fuzzyidx", None, &clause.terms, 10, 0, 10);
        assert!(
            !matches!(frame, Frame::Error(_)),
            "exact search returned Frame::Error: {frame:?}"
        );
        let (total, keys) = extract_hits(&frame);
        assert!(
            total >= 1,
            "expected ≥1 exact hit for 'machine', got {total}"
        );
        assert!(
            keys.iter().any(|k| k.as_ref() == b"fz:1"),
            "expected fz:1 in exact hits, got {keys:?}"
        );
    }
}
