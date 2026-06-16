//! FT.SEARCH query evaluator — folds a parsed [`QueryNode`] AST to a matched doc-id set and a
//! best-effort BM25-scored result list (task `fts-query-eval-dispatch` 2b; contract inherited
//! from `fts-query-combinators` §3, FROZEN @ v1).
//!
//! Two layers:
//!   * [`eval_set`] — the authoritative MEMBERSHIP. `And` = ∩, `Or` = ∪, `Empty` = ∅; leaves reuse
//!     the index's own search machinery (`search_field` / `search_field_or` / `search_tag` /
//!     `search_numeric_range`) and collect their doc-ids into a [`RoaringBitmap`]. Pure — set
//!     membership is document-frequency-independent, so DFS weights do not enter here.
//!     `eval_set(root).len()` is the "total matched" cardinality (the FROZEN boundary that
//!     `fts-search-count-semantics` consumes) — kept `pub` for that task.
//!   * [`eval_query`] — `eval_set` + best-effort scoring. TEXT leaves contribute BM25, summed
//!     across OR branches and across leaves; docs matched only by TAG/NUMERIC score `0.0`. The
//!     final order is score DESC, doc_id ASC (stable, deterministic).
//!
//! **Why per-leaf score summation is regression-safe.** BM25 is additive across query terms, and
//! `search_field` computes exactly Σ-over-terms. So for a single-field query the sum of per-leaf
//! BM25 contributions equals the old combined `search_field(&[t1, t2, …])` score — byte-identical
//! to the pre-2b path. Across multiple fields the sum is the RediSearch-correct cross-field score.
//!
//! Tokens in the AST are RAW (un-analyzed); analysis happens here so the parser stays pure. Analysis
//! reuses the index's per-field [`AnalyzerPipeline`] (the same one indexing used), so query and
//! document terms agree regardless of stemming.

#![cfg(feature = "text-index")]

use std::collections::HashMap;

use bytes::Bytes;
use roaring::RoaringBitmap;

use super::ast::QueryNode;
use crate::text::store::{TermModifier, TextIndex, TextSearchResult};

/// Fold the AST to the matched doc-id set (frozen §3 set-semantics).
///
/// `And(xs)` intersects children (fold `&=`), `Or(xs)` unions them (fold `|=`), `Empty` is `∅`.
/// Leaves reuse the index's search machinery and collect doc-ids into the shared id space
/// (`ensure_doc_id`), so the set operations compose directly.
pub fn eval_set(node: &QueryNode, idx: &TextIndex) -> RoaringBitmap {
    match node {
        QueryNode::Empty => RoaringBitmap::new(),

        QueryNode::Term {
            field,
            token,
            modifier,
        } => term_results(idx, *field, token, modifier, None, None, usize::MAX)
            .into_iter()
            .map(|r| r.doc_id)
            .collect(),

        QueryNode::Tag { field, values } => {
            let mut bm = RoaringBitmap::new();
            for value in values {
                for doc_id in idx.search_tag(field, value) {
                    bm.insert(doc_id);
                }
            }
            bm
        }

        QueryNode::Numeric {
            field,
            min,
            max,
            min_excl,
            max_excl,
        } => {
            let mut bm = RoaringBitmap::new();
            for doc_id in idx.search_numeric_range(field, *min, *max, *min_excl, *max_excl) {
                bm.insert(doc_id);
            }
            bm
        }

        QueryNode::And(children) => {
            let mut iter = children.iter();
            let Some(first) = iter.next() else {
                return RoaringBitmap::new();
            };
            let mut acc = eval_set(first, idx);
            for child in iter {
                if acc.is_empty() {
                    break; // ∩ with anything stays empty — short-circuit.
                }
                acc &= &eval_set(child, idx);
            }
            acc
        }

        QueryNode::Or(children) => {
            let mut acc = RoaringBitmap::new();
            for child in children {
                acc |= &eval_set(child, idx);
            }
            acc
        }
    }
}

/// Evaluate the AST to a best-effort BM25-scored, ordered result list.
///
/// Membership is `eval_set` (authoritative); scoring sums TEXT-leaf BM25 per doc (filters score
/// `0.0`). Order: score DESC, doc_id ASC (stable). `global_df`/`global_n` forward the DFS global
/// IDF weights to the text leaves (multi-shard path, E5). Truncated to `top_k`.
pub fn eval_query(
    idx: &TextIndex,
    node: &QueryNode,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    // 1. Authoritative membership (complete — no truncation).
    let set = eval_set(node, idx);
    if set.is_empty() {
        return Vec::new();
    }

    // 2. Best-effort scores from TEXT leaves only. usize::MAX so every matched doc gets its true
    //    BM25 (search_field caps capacity by candidate count, not top_k — see store.rs), avoiding
    //    truncation-induced 0.0 scores for docs that are in the set but below a small top_k.
    let mut scores: HashMap<u32, f32> = HashMap::new();
    accumulate_text_scores(node, idx, global_df, global_n, &mut scores);

    // 3. Assemble results for docs in the set; pure-filter docs score 0.0.
    let mut results: Vec<TextSearchResult> = set
        .iter()
        .filter_map(|doc_id| {
            idx.doc_id_to_key.get(&doc_id).map(|key| TextSearchResult {
                doc_id,
                key: key.clone(),
                score: scores.get(&doc_id).copied().unwrap_or(0.0),
            })
        })
        .collect();

    // 4. Order: score DESC, doc_id ASC (stable, deterministic tie-break).
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.doc_id.cmp(&b.doc_id))
    });
    results.truncate(top_k);
    results
}

/// Walk the AST, accumulating each TEXT leaf's BM25 contribution per doc into `scores`. TAG /
/// NUMERIC / Empty leaves contribute nothing (they score `0.0` by absence). Scores sum across OR
/// branches and across leaves — the additive-BM25 property keeps single-field queries identical to
/// the pre-2b combined score.
fn accumulate_text_scores(
    node: &QueryNode,
    idx: &TextIndex,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    scores: &mut HashMap<u32, f32>,
) {
    match node {
        QueryNode::Empty | QueryNode::Tag { .. } | QueryNode::Numeric { .. } => {}

        QueryNode::Term {
            field,
            token,
            modifier,
        } => {
            for r in term_results(
                idx,
                *field,
                token,
                modifier,
                global_df,
                global_n,
                usize::MAX,
            ) {
                *scores.entry(r.doc_id).or_insert(0.0) += r.score;
            }
        }

        QueryNode::And(children) | QueryNode::Or(children) => {
            for child in children {
                accumulate_text_scores(child, idx, global_df, global_n, scores);
            }
        }
    }
}

/// Evaluate a single TEXT term leaf to scored results, reusing the index's search machinery.
///
/// `field = Some(idx)` restricts to one text field; `None` searches all non-NOINDEX text fields
/// and sums per-doc scores across them (cross-field union — matches the pre-2b
/// `accumulate_cross_field` behaviour). Invalid UTF-8 in the raw token yields no matches (the
/// analyzer operates on `&str`); this never panics.
fn term_results(
    idx: &TextIndex,
    field: Option<usize>,
    raw: &Bytes,
    modifier: &TermModifier,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    let Ok(raw_str) = std::str::from_utf8(raw) else {
        return Vec::new();
    };

    match field {
        Some(fidx) => {
            term_results_in_field(idx, fidx, raw_str, modifier, global_df, global_n, top_k)
        }
        None => {
            // Cross-field: union over non-NOINDEX text fields, summing per-doc scores.
            let mut acc: HashMap<u32, (f32, Bytes)> = HashMap::new();
            for fidx in 0..idx.text_fields.len() {
                if idx.text_fields[fidx].noindex {
                    continue;
                }
                for r in
                    term_results_in_field(idx, fidx, raw_str, modifier, global_df, global_n, top_k)
                {
                    let entry = acc.entry(r.doc_id).or_insert((0.0, r.key.clone()));
                    entry.0 += r.score;
                }
            }
            acc.into_iter()
                .map(|(doc_id, (score, key))| TextSearchResult { doc_id, key, score })
                .collect()
        }
    }
}

/// Evaluate a term leaf within a single text field. Exact terms run the field's full analyzer
/// (lowercase + NFKD + stem + stop-words) and AND-match via `search_field`; fuzzy/prefix terms are
/// lowercased + NFKD only (no stemming, per D-06/D-07), expanded via `expand_terms`, then OR-matched
/// via `search_field_or`.
fn term_results_in_field(
    idx: &TextIndex,
    fidx: usize,
    raw_str: &str,
    modifier: &TermModifier,
    global_df: Option<&HashMap<String, u32>>,
    global_n: Option<u32>,
    top_k: usize,
) -> Vec<TextSearchResult> {
    match modifier {
        TermModifier::Exact => {
            // A single raw token may analyze to 0 terms (stop word) or >1 (rare); search_field
            // AND-matches them, consistent with the pre-2b exact path.
            let terms = analyze_raw(idx, fidx, raw_str, modifier);
            if terms.is_empty() {
                return Vec::new();
            }
            idx.search_field(fidx, &terms, global_df, global_n, top_k)
        }

        TermModifier::Fuzzy(_) | TermModifier::Prefix => {
            let normalized = analyze_raw(idx, fidx, raw_str, modifier);
            let Some(normalized) = normalized.first() else {
                return Vec::new();
            };
            let ids = idx.expand_terms(fidx, normalized, modifier);
            if ids.is_empty() {
                return Vec::new();
            }
            idx.search_field_or(fidx, &ids, global_df, global_n, top_k)
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
        QueryNode::Empty | QueryNode::Tag { .. } | QueryNode::Numeric { .. } => {}
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
        QueryNode::Empty | QueryNode::Tag { .. } | QueryNode::Numeric { .. } => {}
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
