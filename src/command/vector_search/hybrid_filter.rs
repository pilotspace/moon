//! HybridFilter — recursive filter type, evaluator, and wire-format parser
//! for the `FILTER` modifier in HYBRID FT.SEARCH.
//!
//! # Wire grammar (CHANGE E)
//!
//! ```text
//! FILTER <expr>
//! <expr> := TAG @<field> <value>
//!         | NUMERIC @<field> <min> <max>
//!         | AND <n> <expr>{n}
//!         | OR  <n> <expr>{n}
//! ```
//!
//! Examples:
//! - `FILTER TAG @source scratchpad`
//! - `FILTER AND 2 TAG @source a TAG @chunk_type fact`
//! - `FILTER OR 2 TAG @source a AND 2 TAG @source b NUMERIC @valid_from 0 99`
//!
//! Limits enforced at parse time: depth <= 4, total leaves <= 16.
//! Exceeding either returns `Frame::Error("ERR FILTER too complex")`.
//! Unknown `<expr>` heads return `Frame::Error("ERR unsupported FILTER type")`.
//! The parser is panic-free on all inputs (truncated, over-deep, garbage).
//!
//! # Filter evaluation (CHANGE B core)
//!
//! [`eval_filter`] builds a `HashSet<u32>` of matching `doc_id`s from the
//! shard-local [`TextIndex`]. It does NOT touch HNSW or vector stores — it
//! is purely a TextIndex bitmap operation. Evaluation is bottom-up (leaves
//! first, then And = intersection, Or = union).
//!
//! # Dense / sparse result joining
//!
//! A dense/sparse `SearchResult` is kept iff its `key_hash` maps to a
//! `doc_id` (via `text_index.key_hash_to_doc_id`) AND that `doc_id` is in
//! the allowlist. Results whose `key_hash` has NO text `doc_id` are
//! **dropped** — correctness-first: we cannot confirm the doc matches the
//! filter; a filtered BM25-only search would not see it either, so this
//! matches the k-starvation baseline (CHANGE B design note).

use std::collections::HashSet;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::text::store::TextIndex;
use crate::vector::types::SearchResult;

use super::{extract_bulk, matches_keyword};

// ─── Types ───────────────────────────────────────────────────────────────────

/// Recursive filter tree for HYBRID FT.SEARCH.
///
/// A `value` in [`HybridFilter::Tag`] that ends in `'*'` is treated as a
/// **prefix match** (StartsWith); all other values are exact matches.
#[derive(Debug, Clone)]
pub enum HybridFilter {
    /// `TAG @<field> <value>` — exact if value has no trailing `*`,
    /// prefix (StartsWith) if `value` ends with `'*'`.
    Tag { field: String, value: String },
    /// `NUMERIC @<field> <min> <max>` — inclusive range `[min, max]`.
    Numeric { field: String, min: f64, max: f64 },
    /// `AND <n> <expr>{n}` — intersection (all children must match).
    And(Vec<HybridFilter>),
    /// `OR <n> <expr>{n}` — union (at least one child must match).
    Or(Vec<HybridFilter>),
}

// ─── Wire parser ─────────────────────────────────────────────────────────────

/// Parser state threaded through the recursive descent.
struct ParseState<'a> {
    args: &'a [Frame],
    pos: usize,
    depth: usize,
    leaf_count: usize,
}

impl<'a> ParseState<'a> {
    fn new(args: &'a [Frame], start: usize) -> Self {
        Self {
            args,
            pos: start,
            depth: 0,
            leaf_count: 0,
        }
    }

    fn peek(&self) -> Option<&Frame> {
        self.args.get(self.pos)
    }

    fn consume(&mut self) -> Option<&Frame> {
        let f = self.args.get(self.pos)?;
        self.pos += 1;
        Some(f)
    }

    fn consume_bulk(&mut self) -> Option<Bytes> {
        let f = self.consume()?;
        extract_bulk(f)
    }
}

/// Parse a single `<expr>` from `state`. Advances `state.pos` past consumed tokens.
/// Returns `Err(Frame::Error)` on any malformed input, truncation, or limit violation.
fn parse_expr(state: &mut ParseState<'_>) -> Result<HybridFilter, Frame> {
    if state.depth > 4 {
        return Err(Frame::Error(Bytes::from_static(b"ERR FILTER too complex")));
    }
    let head = match state.peek() {
        Some(f) => f.clone(),
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR FILTER expression truncated",
            )));
        }
    };

    if matches_keyword(&head, b"TAG") {
        state.pos += 1; // consume TAG
        // @<field>
        let raw_field = state
            .consume_bulk()
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER TAG missing field")))?;
        // Strip leading `@` if present.
        let field_bytes = if let Some(stripped) = raw_field.strip_prefix(b"@") {
            Bytes::copy_from_slice(stripped)
        } else {
            raw_field
        };
        let field = std::str::from_utf8(&field_bytes)
            .map_err(|_| Frame::Error(Bytes::from_static(b"ERR FILTER TAG non-UTF8 field")))?
            .to_owned();
        // <value>
        let val_bytes = state
            .consume_bulk()
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER TAG missing value")))?;
        let value = std::str::from_utf8(&val_bytes)
            .map_err(|_| Frame::Error(Bytes::from_static(b"ERR FILTER TAG non-UTF8 value")))?
            .to_owned();
        state.leaf_count += 1;
        if state.leaf_count > 16 {
            return Err(Frame::Error(Bytes::from_static(b"ERR FILTER too complex")));
        }
        Ok(HybridFilter::Tag { field, value })
    } else if matches_keyword(&head, b"NUMERIC") {
        state.pos += 1; // consume NUMERIC
        // @<field>
        let raw_field = state
            .consume_bulk()
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER NUMERIC missing field")))?;
        let field_bytes = if let Some(stripped) = raw_field.strip_prefix(b"@") {
            Bytes::copy_from_slice(stripped)
        } else {
            raw_field
        };
        let field = std::str::from_utf8(&field_bytes)
            .map_err(|_| Frame::Error(Bytes::from_static(b"ERR FILTER NUMERIC non-UTF8 field")))?
            .to_owned();
        // <min>
        let min_bytes = state
            .consume_bulk()
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER NUMERIC missing min")))?;
        let min: f64 = std::str::from_utf8(&min_bytes)
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|v: &f64| v.is_finite())
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER NUMERIC invalid min")))?;
        // <max>
        let max_bytes = state
            .consume_bulk()
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER NUMERIC missing max")))?;
        let max: f64 = std::str::from_utf8(&max_bytes)
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|v: &f64| v.is_finite())
            .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER NUMERIC invalid max")))?;
        state.leaf_count += 1;
        if state.leaf_count > 16 {
            return Err(Frame::Error(Bytes::from_static(b"ERR FILTER too complex")));
        }
        Ok(HybridFilter::Numeric { field, min, max })
    } else if matches_keyword(&head, b"AND") {
        state.pos += 1; // consume AND
        state.depth += 1;
        if state.depth > 4 {
            return Err(Frame::Error(Bytes::from_static(b"ERR FILTER too complex")));
        }
        let n = parse_arity(state)?;
        let mut children = Vec::with_capacity(n);
        for _ in 0..n {
            children.push(parse_expr(state)?);
        }
        state.depth -= 1;
        Ok(HybridFilter::And(children))
    } else if matches_keyword(&head, b"OR") {
        state.pos += 1; // consume OR
        state.depth += 1;
        if state.depth > 4 {
            return Err(Frame::Error(Bytes::from_static(b"ERR FILTER too complex")));
        }
        let n = parse_arity(state)?;
        let mut children = Vec::with_capacity(n);
        for _ in 0..n {
            children.push(parse_expr(state)?);
        }
        state.depth -= 1;
        Ok(HybridFilter::Or(children))
    } else {
        // Unknown head — consume it so callers don't re-see it.
        state.pos += 1;
        Err(Frame::Error(Bytes::from_static(
            b"ERR unsupported FILTER type",
        )))
    }
}

/// Parse a non-negative arity count from the next argument.
fn parse_arity(state: &mut ParseState<'_>) -> Result<usize, Frame> {
    let f = state
        .consume()
        .ok_or_else(|| Frame::Error(Bytes::from_static(b"ERR FILTER arity truncated")))?;
    match f {
        Frame::Integer(n) => {
            if *n < 0 || *n > 16 {
                Err(Frame::Error(Bytes::from_static(b"ERR FILTER too complex")))
            } else {
                Ok(*n as usize)
            }
        }
        Frame::BulkString(b) => {
            let s = std::str::from_utf8(b)
                .map_err(|_| Frame::Error(Bytes::from_static(b"ERR FILTER invalid arity")))?;
            let n: usize = s
                .parse()
                .map_err(|_| Frame::Error(Bytes::from_static(b"ERR FILTER invalid arity")))?;
            if n > 16 {
                Err(Frame::Error(Bytes::from_static(b"ERR FILTER too complex")))
            } else {
                Ok(n)
            }
        }
        _ => Err(Frame::Error(Bytes::from_static(
            b"ERR FILTER invalid arity",
        ))),
    }
}

/// Try to parse a `FILTER <expr>` block starting at `start_index` in `args`.
///
/// Returns:
/// - `Ok(None)` — `FILTER` keyword not present at `start_index`.
/// - `Ok(Some((filter, end_index)))` — parsed successfully; `end_index` is
///   one past the last token consumed.
/// - `Err(Frame::Error)` — `FILTER` keyword present but expression is malformed.
///
/// This function MUST NOT panic on any input.
pub fn parse_filter_modifier(
    args: &[Frame],
    start_index: usize,
) -> Result<Option<(HybridFilter, usize)>, Frame> {
    if start_index >= args.len() {
        return Ok(None);
    }
    if !matches_keyword(&args[start_index], b"FILTER") {
        return Ok(None);
    }
    // FILTER keyword found — the <expr> starts at start_index + 1.
    let expr_start = start_index + 1;
    if expr_start >= args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR FILTER expression truncated",
        )));
    }
    let mut state = ParseState::new(args, expr_start);
    let filter = parse_expr(&mut state)?;
    Ok(Some((filter, state.pos)))
}

// ─── Tag-prefix search ───────────────────────────────────────────────────────

/// Search a TAG field for all values that begin with `prefix` and return the
/// union of their doc_id bitmaps.
///
/// This is the StartsWith counterpart to [`TextIndex::search_tag`] (exact).
/// It scans the `tag_indexes[field]` BTreeMap for keys beginning with `prefix`
/// and unions their doc_id bitmaps. This is an allowlist-build operation, not
/// on the dispatch hot-path.
///
/// Returns an empty `HashSet` if the field is unknown or no values match.
#[cfg(feature = "text-index")]
pub fn search_tag_prefix(text_index: &TextIndex, field: &Bytes, prefix: &[u8]) -> HashSet<u32> {
    // Case-insensitive field resolution (mirrors search_tag).
    let canonical_field = match text_index
        .tag_fields
        .iter()
        .find(|f| f.field_name.eq_ignore_ascii_case(field.as_ref()))
    {
        Some(f) => f.field_name.clone(),
        None => return HashSet::new(),
    };

    let tag_map = match text_index.tag_indexes.get(&canonical_field) {
        Some(m) => m,
        None => return HashSet::new(),
    };

    // Normalize prefix the same way values are normalized on insert.
    // tag_fields have case_sensitive flag — lowercase unless case-sensitive.
    let case_sensitive = text_index
        .tag_fields
        .iter()
        .find(|f| f.field_name == canonical_field)
        .map_or(false, |f| f.case_sensitive);

    let norm_prefix: Vec<u8> = if case_sensitive {
        prefix.to_vec()
    } else {
        prefix.iter().map(|b| b.to_ascii_lowercase()).collect()
    };

    // Scan for matching keys and union their bitmaps.
    let mut result = HashSet::new();
    for (key, bitmap) in tag_map {
        if key.starts_with(&norm_prefix) {
            for doc_id in bitmap.iter() {
                result.insert(doc_id);
            }
        }
    }
    result
}

// ─── Filter evaluator ────────────────────────────────────────────────────────

/// Evaluate a `HybridFilter` against `text_index` and return the set of
/// matching `doc_id`s.
///
/// Evaluation is bottom-up: leaves produce bitmaps via the `TextIndex` tag /
/// numeric APIs; `And` folds by intersection; `Or` folds by union.
///
/// An empty `And` or `Or` children list returns the empty set (conservative).
#[cfg(feature = "text-index")]
pub fn eval_filter(filter: &HybridFilter, text_index: &TextIndex) -> HashSet<u32> {
    match filter {
        HybridFilter::Tag { field, value } => {
            let field_b = Bytes::from(field.as_bytes().to_vec());
            // Prefix match: value ends with '*'.
            if value.ends_with('*') {
                let prefix = value.trim_end_matches('*').as_bytes();
                search_tag_prefix(text_index, &field_b, prefix)
            } else {
                let value_b = Bytes::from(value.as_bytes().to_vec());
                text_index
                    .search_tag(&field_b, &value_b)
                    .into_iter()
                    .collect()
            }
        }
        HybridFilter::Numeric { field, min, max } => {
            let field_b = Bytes::from(field.as_bytes().to_vec());
            text_index
                .search_numeric_range(&field_b, *min, *max, false, false)
                .into_iter()
                .collect()
        }
        HybridFilter::And(children) => {
            if children.is_empty() {
                return HashSet::new();
            }
            let mut iter = children.iter();
            // Start from the first child's result.
            let mut result = eval_filter(iter.next().expect("checked non-empty"), text_index);
            for child in iter {
                let child_set = eval_filter(child, text_index);
                result.retain(|id| child_set.contains(id));
                if result.is_empty() {
                    return result;
                }
            }
            result
        }
        HybridFilter::Or(children) => {
            let mut result = HashSet::new();
            for child in children {
                let child_set = eval_filter(child, text_index);
                result.extend(child_set);
            }
            result
        }
    }
}

// ─── Stream filtering helpers ────────────────────────────────────────────────

/// Filter a BM25 result stream: retain only entries whose `doc_id` is in
/// `allowlist`. The `TextSearchResult.doc_id` field is the text-index doc_id.
#[cfg(feature = "text-index")]
pub fn filter_bm25_results(
    results: &mut Vec<crate::text::store::TextSearchResult>,
    allowlist: &HashSet<u32>,
) {
    results.retain(|r| allowlist.contains(&r.doc_id));
}

/// Filter a dense/sparse result stream: retain only entries whose `key_hash`
/// maps to a `doc_id` via `key_hash_to_doc_id` AND that `doc_id` is in
/// `allowlist`.
///
/// Results with no `key_hash_to_doc_id` entry are **dropped** (correctness-
/// first: we cannot confirm they pass the filter — see module-level doc note).
#[cfg(feature = "text-index")]
pub fn filter_vector_results(
    results: &mut Vec<SearchResult>,
    key_hash_to_doc_id: &std::collections::HashMap<u64, u32>,
    allowlist: &HashSet<u32>,
) {
    results.retain(|r| {
        key_hash_to_doc_id
            .get(&r.key_hash)
            .map_or(false, |doc_id| allowlist.contains(doc_id))
    });
}

// ─── Unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    // Helper: build args for parse_filter_modifier tests.
    // The "FILTER" keyword is placed at index 0.
    fn filter_args(tokens: &[&[u8]]) -> Vec<Frame> {
        let mut v = vec![bs(b"FILTER")];
        v.extend(tokens.iter().map(|t| bs(t)));
        v
    }

    // ── Basic parse cases ─────────────────────────────────────────────────────

    #[test]
    fn test_parse_filter_absent() {
        let args = vec![bs(b"FUSION"), bs(b"RRF")];
        let r = parse_filter_modifier(&args, 0).expect("no error");
        assert!(r.is_none(), "no FILTER keyword → None");
    }

    #[test]
    fn test_parse_filter_tag_exact() {
        let args = filter_args(&[b"TAG", b"@source", b"scratchpad"]);
        let (f, end) = parse_filter_modifier(&args, 0)
            .expect("no error")
            .expect("some");
        assert_eq!(end, 4); // FILTER TAG @source scratchpad = 4 tokens consumed
        match f {
            HybridFilter::Tag { field, value } => {
                assert_eq!(field, "source");
                assert_eq!(value, "scratchpad");
            }
            _ => panic!("expected Tag"),
        }
    }

    #[test]
    fn test_parse_filter_tag_prefix() {
        let args = filter_args(&[b"TAG", b"@source", b"scratch*"]);
        let (f, _) = parse_filter_modifier(&args, 0)
            .expect("no error")
            .expect("some");
        match f {
            HybridFilter::Tag { field, value } => {
                assert_eq!(field, "source");
                assert_eq!(value, "scratch*");
            }
            _ => panic!("expected Tag"),
        }
    }

    #[test]
    fn test_parse_filter_numeric() {
        let args = filter_args(&[b"NUMERIC", b"@valid_from", b"0", b"100"]);
        let (f, end) = parse_filter_modifier(&args, 0)
            .expect("no error")
            .expect("some");
        assert_eq!(end, 5);
        match f {
            HybridFilter::Numeric { field, min, max } => {
                assert_eq!(field, "valid_from");
                assert!((min - 0.0).abs() < 1e-9);
                assert!((max - 100.0).abs() < 1e-9);
            }
            _ => panic!("expected Numeric"),
        }
    }

    #[test]
    fn test_parse_filter_and_two_tags() {
        // FILTER AND 2 TAG @source a TAG @chunk_type fact
        let args = filter_args(&[
            b"AND", b"2", b"TAG", b"@source", b"a", b"TAG", b"@type", b"fact",
        ]);
        let (f, _) = parse_filter_modifier(&args, 0)
            .expect("no error")
            .expect("some");
        match f {
            HybridFilter::And(children) => {
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn test_parse_filter_or() {
        // FILTER OR 2 TAG @source a TAG @source b
        let args = filter_args(&[
            b"OR", b"2", b"TAG", b"@source", b"a", b"TAG", b"@source", b"b",
        ]);
        let (f, _) = parse_filter_modifier(&args, 0)
            .expect("no error")
            .expect("some");
        match f {
            HybridFilter::Or(children) => {
                assert_eq!(children.len(), 2);
            }
            _ => panic!("expected Or"),
        }
    }

    // ── Panic-free error cases ────────────────────────────────────────────────

    #[test]
    fn test_parse_filter_truncated_after_filter_keyword() {
        // FILTER keyword with nothing after it.
        let args = vec![bs(b"FILTER")];
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "truncated after FILTER → error");
        match r.unwrap_err() {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(s.contains("truncated") || s.contains("FILTER"), "got: {s}");
            }
            _ => panic!("expected Error frame"),
        }
    }

    #[test]
    fn test_parse_filter_truncated_mid_tag() {
        // FILTER TAG @source  — value missing.
        let args = filter_args(&[b"TAG", b"@source"]);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "truncated mid-TAG → error");
    }

    #[test]
    fn test_parse_filter_truncated_mid_numeric() {
        // FILTER NUMERIC @valid_from 0  — max missing.
        let args = filter_args(&[b"NUMERIC", b"@valid_from", b"0"]);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "truncated mid-NUMERIC → error");
    }

    #[test]
    fn test_parse_filter_garbage_head() {
        // FILTER BOGUS_TYPE @field value.
        let args = filter_args(&[b"BOGUS_TYPE", b"@field", b"value"]);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "unknown FILTER type → error");
        if let Err(Frame::Error(msg)) = r {
            assert_eq!(&msg[..], b"ERR unsupported FILTER type");
        }
    }

    #[test]
    fn test_parse_filter_over_depth() {
        // Build a 5-deep AND nesting: AND 1 AND 1 AND 1 AND 1 AND 1 TAG @f v
        // depth limit is 4; the 5th AND violates it.
        let args = filter_args(&[
            b"AND", b"1", b"AND", b"1", b"AND", b"1", b"AND", b"1", b"AND",
            b"1", // <-- depth 5
            b"TAG", b"@f", b"v",
        ]);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "depth > 4 → error");
        if let Err(Frame::Error(msg)) = r {
            assert_eq!(&msg[..], b"ERR FILTER too complex");
        }
    }

    #[test]
    fn test_parse_filter_over_width() {
        // AND 17 TAG @f v{17} — leaf count exceeds 16.
        let mut tokens: Vec<&[u8]> = vec![b"AND", b"17"];
        for _ in 0..17 {
            tokens.push(b"TAG");
            tokens.push(b"@f");
            tokens.push(b"v");
        }
        let args = filter_args(&tokens);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "leaf count > 16 → error");
        if let Err(Frame::Error(msg)) = r {
            assert_eq!(&msg[..], b"ERR FILTER too complex");
        }
    }

    #[test]
    fn test_parse_filter_non_numeric_min() {
        let args = filter_args(&[b"NUMERIC", b"@field", b"notanumber", b"100"]);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "non-numeric min → error");
    }

    #[test]
    fn test_parse_filter_non_numeric_max() {
        let args = filter_args(&[b"NUMERIC", b"@field", b"0", b"NaN"]);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "NaN max → error (not finite)");
    }

    #[test]
    fn test_parse_filter_start_index_beyond_range() {
        // start_index past end of args → Ok(None).
        let args = vec![bs(b"RRF")];
        let r = parse_filter_modifier(&args, 5).expect("no error");
        assert!(r.is_none());
    }

    #[test]
    fn test_parse_filter_arity_over_16() {
        // OR 17 … → over-width at arity level.
        let args = filter_args(&[b"OR", b"17"]);
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "arity > 16 → error");
    }

    #[test]
    fn test_parse_filter_negative_arity() {
        // AND -1 … → invalid.
        let args = vec![bs(b"FILTER"), bs(b"AND"), Frame::Integer(-1)];
        let r = parse_filter_modifier(&args, 0);
        assert!(r.is_err(), "negative arity → error");
    }
}
