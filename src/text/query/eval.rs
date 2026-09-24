//! FT.SEARCH query evaluator — folds a parsed [`QueryNode`] AST to a matched doc-id set and a
//! best-effort BM25-scored result list (task `fts-query-eval-dispatch` 2b; contract inherited
//! from `fts-query-combinators` §3, FROZEN @ v1).
//!
//! Two layers:
//!   * [`eval_set`] — the authoritative MEMBERSHIP. `And` = ∩, `Or` = ∪, `Empty` = ∅; leaves are
//!     posting / TAG / NUMERIC bitmaps combined directly (moon#1191 — HEAD ran the full BM25
//!     scorer per TEXT leaf just to collect doc ids). Pure — set membership is
//!     document-frequency-independent, so DFS weights do not enter here.
//!     `eval_set(root)` restricted to resolvable documents is the "total matched" cardinality (the
//!     FROZEN boundary that `fts-search-count-semantics` consumes) — kept `pub` for that task.
//!   * [`eval_query`] — `eval_set` + best-effort scoring. TEXT leaves contribute BM25, summed
//!     across OR branches and across leaves; docs matched only by TAG/NUMERIC score `0.0`. The
//!     final order is score DESC, doc_id ASC (deterministic).
//!
//! **Why per-leaf score summation is regression-safe.** BM25 is additive across query terms, and
//! `search_field` computes exactly Σ-over-terms. So for a single-field query the sum of per-leaf
//! BM25 contributions equals the old combined `search_field(&[t1, t2, …])` score — byte-identical
//! to the pre-2b path. Across multiple fields the sum is the RediSearch-correct cross-field score.
//!
//! **One scoring pass (moon#1191).** The matched set is walked once in ascending doc-id order;
//! every TEXT leaf is a [`LeafScorer`] whose doc-ordered posting cursors supply term frequencies,
//! with IDF hoisted per term. Scores are bit-identical to the former two-pass evaluation (same f32
//! operation order — see `crate::text::score`), a bounded top-k keeps the page, and keys are
//! cloned for the page only. `total` is the matched set's cardinality after restricting it to
//! documents that resolve to a key.
//!
//! Tokens in the AST are RAW (un-analyzed); analysis happens here so the parser stays pure. Analysis
//! reuses the index's per-field [`AnalyzerPipeline`] (the same one indexing used), so query and
//! document terms agree regardless of stemming.

#![cfg(feature = "text-index")]

use std::borrow::Cow;
use std::collections::HashMap;

use bytes::Bytes;
use roaring::RoaringBitmap;

use super::ast::QueryNode;
use crate::text::score::{FieldScorer, LeafScorer, QueryScorer, TopK};
use crate::text::store::{TermModifier, TextIndex, TextSearchResult};

/// Fold the AST to the matched doc-id set (frozen §3 set-semantics).
///
/// `And(xs)` intersects children, `Or(xs)` unions them, `Empty` is `∅`. Leaves are the index's own
/// posting / TAG / NUMERIC bitmaps in the shared id space (`ensure_doc_id`), so the set operations
/// compose directly. Ids that do not resolve to a key are dropped by `eval_query_counted`
/// (`TextIndex::restrict_to_live`); in a consistent index there are none.
pub fn eval_set(node: &QueryNode, idx: &TextIndex) -> RoaringBitmap {
    let leaves = LeafTable::build(node, idx, None, None);
    eval_set_cow(node, idx, &leaves).into_owned()
}

/// Every TEXT term leaf of one query, resolved ONCE (analysis, dictionary lookups, fuzzy/prefix
/// expansion, cursors, IDF) and shared by the membership fold and the scoring pass — HEAD analysed
/// and expanded each leaf twice. Keyed by node address (compared, never dereferenced); in AST DFS
/// order, which is the order leaf scores are summed in.
struct LeafTable<'a> {
    leaves: Vec<(*const QueryNode, LeafScorer<'a>)>,
}

impl<'a> LeafTable<'a> {
    fn build(
        node: &QueryNode,
        idx: &'a TextIndex,
        global_df: Option<&HashMap<String, u32>>,
        global_n: Option<u32>,
    ) -> Self {
        let mut leaves = Vec::new();
        collect_leaf_scorers(node, idx, global_df, global_n, &mut leaves);
        Self { leaves }
    }

    fn get(&self, node: &QueryNode) -> Option<&LeafScorer<'a>> {
        let key: *const QueryNode = node;
        self.leaves
            .iter()
            .find(|(n, _)| std::ptr::eq(*n, key))
            .map(|(_, leaf)| leaf)
    }

    fn into_scorer(self) -> QueryScorer<'a> {
        QueryScorer::new(self.leaves.into_iter().map(|(_, leaf)| leaf).collect())
    }
}

/// [`eval_set`] that borrows a single leaf's bitmap instead of cloning it, so `alpha beta` never
/// copies either (possibly very broad) posting — only the intersection is allocated.
fn eval_set_cow<'a>(
    node: &QueryNode,
    idx: &'a TextIndex,
    leaves: &LeafTable<'_>,
) -> Cow<'a, RoaringBitmap> {
    match node {
        QueryNode::Empty => Cow::Owned(RoaringBitmap::new()),

        // moon#693: membership comes from the document registry, not a posting list — so
        // `*` also returns documents no term query can reach (one whose text analyzed to
        // nothing is still in the index). `live_docs` is exactly the key set of `doc_id_to_key`,
        // the registry `eval_query_counted` resolves results through.
        QueryNode::MatchAll => Cow::Borrowed(idx.live_docs()),

        QueryNode::Term { .. } => Cow::Owned(
            leaves
                .get(node)
                .map_or_else(RoaringBitmap::new, LeafScorer::candidates),
        ),

        QueryNode::Tag { field, values } => {
            let mut hits = values.iter().filter_map(|v| idx.tag_value_bitmap(field, v));
            let Some(first) = hits.next() else {
                return Cow::Owned(RoaringBitmap::new());
            };
            let mut acc = Cow::Borrowed(first);
            for bm in hits {
                *acc.to_mut() |= bm;
            }
            acc
        }

        QueryNode::Numeric {
            field,
            min,
            max,
            min_excl,
            max_excl,
        } => Cow::Owned(idx.numeric_range_bitmap(field, *min, *max, *min_excl, *max_excl)),

        QueryNode::And(children) => {
            // moon#690: a stop-word leaf is REMOVED from the conjunction, not intersected as
            // ∅ — `alpha the` means `alpha`, which is what RediSearch does. Intersecting it
            // zeroed every conjunction that happened to contain a stop word.
            let mut sets: Vec<Cow<'a, RoaringBitmap>> = Vec::with_capacity(children.len());
            for child in children.iter().filter(|c| !is_stop_word_only(c, idx)) {
                let set = eval_set_cow(child, idx, leaves);
                if set.is_empty() {
                    // ∩ with ∅ stays empty — short-circuit the remaining children.
                    return Cow::Owned(RoaringBitmap::new());
                }
                sets.push(set);
            }
            // Every child was a stop word: nothing is being asked for, so nothing matches.
            // Otherwise intersect smallest-first (moon#1191: HEAD folded in query order).
            sets.sort_by_key(|s| s.len());
            let mut iter = sets.into_iter();
            let Some(first) = iter.next() else {
                return Cow::Owned(RoaringBitmap::new());
            };
            let Some(second) = iter.next() else {
                return first;
            };
            let mut acc = &*first & &*second;
            for set in iter {
                if acc.is_empty() {
                    break;
                }
                acc &= &*set;
            }
            Cow::Owned(acc)
        }

        QueryNode::Or(children) => {
            let mut acc = RoaringBitmap::new();
            for child in children {
                acc |= &*eval_set_cow(child, idx, leaves);
            }
            Cow::Owned(acc)
        }
    }
}

/// Evaluate the AST to a best-effort BM25-scored, ordered result list.
///
/// Membership is `eval_set` (authoritative); scoring sums TEXT-leaf BM25 per doc (filters score
/// `0.0`). Order: score DESC, doc_id ASC. `global_df`/`global_n` forward the DFS global IDF weights
/// to the text leaves (multi-shard path, E5). Truncated to `top_k`.
///
/// Thin wrapper over [`eval_query_counted`] — the `.0` projection, kept for the frozen
/// fts-query-eval-dispatch §3 contract (`eval_query(..) -> Vec<TextSearchResult>`). Callers that also
/// need the FT.SEARCH total-matched count use [`eval_query_counted`] directly.
pub fn eval_query(
    idx: &TextIndex,
    node: &QueryNode,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    eval_query_counted(idx, node, global_df, global_n, top_k).0
}

/// Like [`eval_query`] but ALSO returns the true total number of matched, key-resolvable documents
/// — the FT.SEARCH integer reply (`reply[0]`, RediSearch semantics), counted BEFORE the `top_k`
/// truncation (fts-search-count-semantics C1). `eval_set` is evaluated EXACTLY ONCE.
///
/// The total is the cardinality of the match set restricted to documents present in
/// `doc_id_to_key`: a `doc_id` without a key is unreturnable, so it is excluded from both the page
/// and the total (`unresolvable_doc_uncounted`). In a consistent index the two coincide.
pub fn eval_query_counted(
    idx: &TextIndex,
    node: &QueryNode,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> (Vec<TextSearchResult>, usize) {
    // 1. Authoritative membership (complete — no truncation), resolvable docs only.
    let leaves = LeafTable::build(node, idx, global_df, global_n);
    let set = idx.restrict_to_live(eval_set_cow(node, idx, &leaves).into_owned());
    let total_matched = set.len() as usize;
    if total_matched == 0 {
        return (Vec::new(), 0);
    }

    // 2. One scoring pass: every TEXT leaf (AST order) scores the docs it matches.
    let mut scorer = leaves.into_scorer();

    let mut top = TopK::new(top_k, total_matched);
    if scorer.is_constant_zero() {
        // Pure TAG / NUMERIC / `*`: every doc scores 0.0, so the order is doc_id ASC and the page
        // is the first `top_k` ids — no pass over the whole set.
        for doc in set.iter().take(top_k) {
            top.push(doc, 0.0);
        }
    } else {
        scorer.prepare(idx, &set);
        for doc in &set {
            top.push(doc, scorer.score(idx, doc));
        }
    }

    // 3. Keys resolved (and cloned) for the returned page only.
    (top.into_results(idx), total_matched)
}

/// Walk the AST, collecting one [`LeafScorer`] per TEXT leaf in DFS order (the order HEAD's
/// `accumulate_text_scores` summed them in). TAG / NUMERIC / `*` / Empty leaves contribute nothing
/// (they score `0.0` by absence); OR and AND children alike are visited, so a doc matched through
/// any branch collects every TEXT leaf it satisfies.
fn collect_leaf_scorers<'a>(
    node: &QueryNode,
    idx: &'a TextIndex,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    out: &mut Vec<(*const QueryNode, LeafScorer<'a>)>,
) {
    match node {
        // MatchAll scores 0.0 by absence, like the other non-TEXT leaves (moon#693).
        QueryNode::Empty
        | QueryNode::MatchAll
        | QueryNode::Tag { .. }
        | QueryNode::Numeric { .. } => {}

        QueryNode::Term {
            field,
            token,
            modifier,
        } => {
            let fields = leaf_field_scorers(idx, *field, token, modifier, global_df, global_n);
            let key: *const QueryNode = node;
            out.push((key, LeafScorer::new(fields, field.is_none())));
        }

        QueryNode::And(children) | QueryNode::Or(children) => {
            for child in children {
                collect_leaf_scorers(child, idx, global_df, global_n, out);
            }
        }
    }
}

/// The per-field scorers of a single TEXT term leaf.
///
/// `field = Some(idx)` restricts to one text field; `None` covers all non-NOINDEX text fields
/// (cross-field union, per-doc sum — matches the pre-2b `accumulate_cross_field` behaviour).
/// Exact terms run the field's full analyzer (lowercase + NFKD + stem + stop-words) and AND-match;
/// fuzzy/prefix terms are lowercased + NFKD only (no stemming, per D-06/D-07), expanded via
/// `expand_terms`, then OR-matched. A field that can match nothing yields no scorer. Invalid UTF-8
/// in the raw token yields no matches (the analyzer operates on `&str`); this never panics.
fn leaf_field_scorers<'a>(
    idx: &'a TextIndex,
    field: Option<usize>,
    raw: &Bytes,
    modifier: &TermModifier,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
) -> Vec<FieldScorer<'a>> {
    let Ok(raw_str) = std::str::from_utf8(raw) else {
        return Vec::new();
    };
    leaf_fields(idx, field)
        .into_iter()
        .filter_map(|fidx| field_scorer(idx, fidx, raw_str, modifier, global_df, global_n))
        .collect()
}

fn field_scorer<'a>(
    idx: &'a TextIndex,
    fidx: usize,
    raw_str: &str,
    modifier: &TermModifier,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
) -> Option<FieldScorer<'a>> {
    match modifier {
        TermModifier::Exact => {
            // A single raw token may analyze to 0 terms (stop word) or >1 (rare); the field-leaf
            // AND-matches them, consistent with the pre-2b exact path.
            let terms = analyze_raw(idx, fidx, raw_str, modifier);
            FieldScorer::exact(idx, fidx, &terms, global_df, global_n)
        }
        TermModifier::Fuzzy(_) | TermModifier::Prefix => {
            if fidx >= idx.field_postings.len() {
                return None;
            }
            let normalized = analyze_raw(idx, fidx, raw_str, modifier);
            let normalized = normalized.first()?;
            let ids = idx.expand_terms(fidx, normalized, modifier);
            FieldScorer::any_of(idx, fidx, &ids, global_n)
        }
    }
}

/// Analyze a raw query token against one field, returning the analyzed term string(s).
///
/// `Exact` runs the field's full analyzer (lowercase + NFKD + stem + stop-words) and may yield 0
/// terms (stop word) or >1; fuzzy/prefix terms are lowercased + NFKD only (no stemming, D-06/D-07)
/// and yield a single normalized string. Centralizes the query-time analysis reused by the leaf
/// evaluator and the DFS / HIGHLIGHT term collectors so they cannot drift.
fn analyze_raw(
    idx: &TextIndex,
    fidx: usize,
    raw_str: &str,
    modifier: &TermModifier,
) -> Vec<String> {
    match modifier {
        TermModifier::Exact => match idx.field_analyzers.get(fidx) {
            Some(analyzer) => analyzer
                .tokenize_with_positions(raw_str)
                .into_iter()
                .map(|(term, _pos)| term)
                .collect(),
            None => Vec::new(),
        },
        TermModifier::Fuzzy(_) | TermModifier::Prefix => {
            use unicode_normalization::UnicodeNormalization;
            let normalized: String = raw_str.to_lowercase().nfkd().collect();
            if normalized.is_empty() {
                Vec::new()
            } else {
                vec![normalized]
            }
        }
    }
}

/// Whether this node is a query token that ANALYZES TO NOTHING — i.e. a stop word.
///
/// The discriminator is "analyzed to nothing", NOT "matched nothing": a term that survives
/// analysis but is absent from the index is a real, unsatisfied constraint and must still
/// zero its conjunction. Only that distinction separates `alpha the` (means `alpha`) from
/// `alpha zzz` (means nothing).
///
/// Narrow by construction — everything that is not provably a stop word answers `false`:
///   * `Exact` only. Fuzzy/prefix terms bypass the stop-word filter entirely, so they can
///     never be neutral.
///   * TAG / NUMERIC / `Empty` carry real constraints and must never be dropped.
///   * A term leaf on an index with no searchable TEXT field is a constraint that cannot be
///     satisfied, not an absent one — dropping it would turn the query into match-all.
///   * A field whose analyzer is missing (a schema/analyzer length mismatch) answers `false`
///     rather than silently widening the query.
fn is_stop_word_only(node: &QueryNode, idx: &TextIndex) -> bool {
    let QueryNode::Term {
        field,
        token,
        modifier,
    } = node
    else {
        return false;
    };
    if !matches!(modifier, TermModifier::Exact) {
        return false;
    }
    let Ok(raw_str) = std::str::from_utf8(token) else {
        return false;
    };
    let fields = leaf_fields(idx, *field);
    if fields.is_empty() {
        return false;
    }
    fields.into_iter().all(|fidx| {
        idx.field_analyzers
            .get(fidx)
            .is_some_and(|a| a.tokenize_with_positions(raw_str).is_empty())
    })
}

/// The fields a term leaf scores against: its scoped field, or every non-NOINDEX text field.
fn leaf_fields(idx: &TextIndex, field: Option<usize>) -> Vec<usize> {
    match field {
        Some(fidx) => vec![fidx],
        None => (0..idx.text_fields.len())
            .filter(|&i| !idx.text_fields[i].noindex)
            .collect(),
    }
}

/// Collect the analyzed text-term strings a query matches on, for HIGHLIGHT / SUMMARIZE
/// post-processing (which highlights the matched terms in document bodies). Walks every TEXT leaf
/// (TAG / NUMERIC contribute no highlightable text), analyzes each against its field(s), and returns
/// the de-duplicated term strings. Invalid-UTF8 tokens yield nothing (never panics).
pub fn collect_highlight_terms(node: &QueryNode, idx: &TextIndex) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    collect_highlight_terms_inner(node, idx, &mut out);
    out.sort_unstable();
    out.dedup();
    out
}

fn collect_highlight_terms_inner(node: &QueryNode, idx: &TextIndex, out: &mut Vec<String>) {
    match node {
        // MatchAll contributes no text term to highlight or to weight (moon#693).
        QueryNode::Empty
        | QueryNode::MatchAll
        | QueryNode::Tag { .. }
        | QueryNode::Numeric { .. } => {}
        QueryNode::Term {
            field,
            token,
            modifier,
        } => {
            let Ok(raw_str) = std::str::from_utf8(token) else {
                return;
            };
            for fidx in leaf_fields(idx, *field) {
                out.extend(analyze_raw(idx, fidx, raw_str, modifier));
            }
        }
        QueryNode::And(children) | QueryNode::Or(children) => {
            for child in children {
                collect_highlight_terms_inner(child, idx, out);
            }
        }
    }
}

/// Collect the analyzed EXACT terms a query needs document frequencies for, for the DFS Phase-1
/// scatter (`doc_freq_for_terms`).
///
/// Returns **at most one** `(field_hint, terms)` entry. This is a hard invariant: each shard emits
/// one `"N"` (total-doc-count) sentinel **per entry**, and `aggregate_doc_freq` SUMS the `"N"`s
/// across shards — so more than one entry inflates the global N (wrong IDF), and zero entries when
/// the query has text leaves zeroes it (broken IDF). Therefore:
///   * No text leaf at all (pure TAG/NUMERIC/Empty) → `[]` — no text scoring, so no N is needed.
///   * Any text leaf (exact OR fuzzy/prefix) → exactly one entry, so N is gathered once. `terms`
///     holds only `Exact` analyzed terms (fuzzy/prefix score via `search_field_or`, which uses LOCAL
///     df — excluded, matching pre-2b). `field_hint` is the common field when every text leaf is the
///     same `@field`, else `None` (df counted against field 0 — the pre-2b cross-field behaviour).
pub fn collect_df_field_terms(
    node: &QueryNode,
    idx: &TextIndex,
) -> Vec<(Option<usize>, Vec<String>)> {
    let mut acc = DfAcc {
        has_text: false,
        single_field: None,
        consistent: true,
        terms: Vec::new(),
    };
    collect_df_terms_inner(node, idx, &mut acc);
    if !acc.has_text {
        return Vec::new();
    }
    // field_hint = the single shared field, only if every text leaf agreed on it.
    let field_hint = if acc.consistent {
        acc.single_field
    } else {
        None
    };
    acc.terms.sort_unstable();
    acc.terms.dedup();
    vec![(field_hint, acc.terms)]
}

/// Accumulator enforcing the single-entry invariant of [`collect_df_field_terms`].
struct DfAcc {
    has_text: bool,
    single_field: Option<usize>,
    consistent: bool,
    terms: Vec<String>,
}

fn collect_df_terms_inner(node: &QueryNode, idx: &TextIndex, acc: &mut DfAcc) {
    match node {
        // MatchAll contributes no text term to highlight or to weight (moon#693).
        QueryNode::Empty
        | QueryNode::MatchAll
        | QueryNode::Tag { .. }
        | QueryNode::Numeric { .. } => {}
        QueryNode::Term {
            field,
            token,
            modifier,
        } => {
            acc.has_text = true;
            // Track field consistency for the single N-gathering hint.
            match field {
                None => acc.consistent = false, // cross-field leaf → hint must be None (field 0)
                Some(f) => match acc.single_field {
                    None => acc.single_field = Some(*f),
                    Some(prev) if prev != *f => acc.consistent = false, // mixed fields → None
                    _ => {}
                },
            }
            // Only Exact terms use global IDF (fuzzy/prefix → local df via search_field_or). Their
            // presence still counts for `has_text` (so N is gathered) but they add no df terms.
            if matches!(modifier, TermModifier::Exact) {
                if let Ok(raw_str) = std::str::from_utf8(token) {
                    acc.terms
                        .extend(analyze_raw(idx, field.unwrap_or(0), raw_str, modifier));
                }
            }
        }
        QueryNode::And(children) | QueryNode::Or(children) => {
            for child in children {
                collect_df_terms_inner(child, idx, acc);
            }
        }
    }
}

#[cfg(test)]
#[path = "eval_oracle_tests.rs"]
mod oracle_tests;
