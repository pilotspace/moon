//! Single-pass BM25 scoring kernel (moon#1191).
//!
//! HEAD scored every matched document twice (once to build the match set,
//! once to score it), looked up each term's posting, `df` and IDF per
//! (document, term), cloned a key per match and fully sorted the match set
//! before truncating to `top_k`. This kernel:
//!
//!   * hoists the posting reference, `df` and IDF per term, and the length
//!     normalisation per (document, field);
//!   * reads term frequencies through doc-ordered [`PostingCursor`]s (the
//!     candidate set is walked in ascending doc-id order, so a per-document
//!     `rank()` is never needed);
//!   * keeps a bounded top-k selection and resolves / clones keys only for
//!     the returned page.
//!
//! **Term-at-a-time for wide expansions (moon#1220).** A fuzzy/prefix leaf
//! expands to up to 50 terms; seeking 50 cursors per candidate made a
//! 50-term prefix over 200K matches cost 10M seeks. When one pass over every
//! expanded posting is cheaper ([`FieldScorer::prepare`]), the leaf's scores
//! are folded term by term into a dense window over the candidates' id range
//! and each candidate then reads its score back.
//!
//! **Bit-identical to HEAD.** Every f32 operation happens in HEAD's order:
//! a field scores `0.0 + t1 + t2 …` (query-term order), a fuzzy/prefix field
//! keeps the `>`-max over its expanded terms, a cross-field leaf sums
//! `0.0 + f0 + f1 …` (field order), and a query sums `0.0 + leaf1 + leaf2 …`
//! (AST order). `bm25_from_parts` is a literal split of `bm25_score`. The
//! ordering is HEAD's `score DESC, doc_id ASC`.

use std::collections::{BinaryHeap, HashMap};

use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::text::bm25::{bm25_from_parts, bm25_idf, bm25_len_norm};
use crate::text::posting::{PostingCursor, PostingList};
use crate::text::store::{TextIndex, TextSearchResult};

/// Per-field BM25 constants, fixed for one query.
#[derive(Clone, Copy)]
struct FieldCtx {
    field_idx: usize,
    avgdl: f32,
    k1: f32,
    b: f32,
    weight: f32,
}

impl FieldCtx {
    fn new(idx: &TextIndex, field_idx: usize) -> Self {
        Self {
            field_idx,
            avgdl: idx.field_stats[field_idx].avg_doc_len(),
            k1: idx.bm25_config.k1,
            b: idx.bm25_config.b,
            weight: idx.text_fields[field_idx].weight as f32,
        }
    }
}

/// One query term within a field: its cursor and hoisted IDF. `idf == None`
/// is `bm25_score`'s degenerate guard (`N == 0 || df == 0` -> `0.0`).
struct TermCursor<'a> {
    cursor: PostingCursor<'a>,
    idf: Option<f32>,
}

impl TermCursor<'_> {
    #[inline]
    fn contribution(&self, tf: u32, len_norm: f32, ctx: &FieldCtx) -> f32 {
        // `bm25_score(tf, ..) * weight`, with `bm25_score`'s `tf == 0` guard.
        let raw = match self.idf {
            Some(idf) if tf != 0 => bm25_from_parts(tf as f32, idf, len_norm, ctx.k1),
            _ => 0.0,
        };
        raw * ctx.weight
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Combine {
    /// Exact terms: the doc must contain every term; scores sum (`search_field`).
    AllSum,
    /// Fuzzy/prefix expansion: any term matches; the best score wins (`search_field_or`).
    /// Only built by the `text-index`-gated OR path (the tokio CI leg compiles without it).
    #[cfg_attr(not(feature = "text-index"), allow(dead_code))]
    AnyMax,
}

/// A fuzzy/prefix leaf needs at least this many expanded terms before
/// term-at-a-time scoring is considered (fewer cursors seek cheaply enough).
const TAAT_MIN_TERMS: usize = 4;

/// Term-at-a-time scans every entry of every expanded posting once (plus the
/// candidate id window); document-at-a-time pays one cursor seek per
/// (candidate, term), and a seek costs several posting-entry steps (a jump
/// is an `advance_to` plus a bitmap `rank`). Scan when the entries are at
/// most this many per seek saved.
const TAAT_ENTRIES_PER_SEEK: u64 = 4;

/// A fuzzy/prefix field-leaf's scores, folded term-at-a-time over the id
/// window `[lo, lo + best.len())` (moon#1220): `best[d - lo]` is the leaf's
/// score for doc `d`, NaN when no expanded term contains `d`. A real score is
/// never NaN — the `>`-max starts at `0.0` and NaN never compares greater.
struct TaatScores {
    lo: u32,
    best: Vec<f32>,
}

impl TaatScores {
    /// `Some(Some(score))` / `Some(None)` (not matched) inside the window,
    /// `None` outside it.
    #[inline]
    fn get(&self, doc: u32) -> Option<Option<f32>> {
        let s = *self.best.get(doc.checked_sub(self.lo)? as usize)?;
        Some((!s.is_nan()).then_some(s))
    }
}

#[cfg(test)]
thread_local! {
    /// Test override of the term-at-a-time choice: `Some(true)` scores every
    /// fuzzy/prefix leaf term-at-a-time, `Some(false)` never does.
    static FORCE_TAAT: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
thread_local! {
    /// How many term-at-a-time folds ran on this thread (tests assert the
    /// path they compare was actually taken).
    static TAAT_FOLDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Term-at-a-time folds run on this thread so far.
#[cfg(all(test, feature = "text-index"))]
pub(crate) fn taat_folds() -> usize {
    TAAT_FOLDS.with(std::cell::Cell::get)
}

/// Run `f` with the term-at-a-time choice forced (`None` = the cost model).
#[cfg(all(test, feature = "text-index"))]
pub(crate) fn with_forced_taat<R>(force: Option<bool>, f: impl FnOnce() -> R) -> R {
    let prev = FORCE_TAAT.with(|c| c.replace(force));
    let out = f();
    FORCE_TAAT.with(|c| c.set(prev));
    out
}

/// Scores one (leaf, field) pair — exactly what `search_field` /
/// `search_field_or` computed for a single document.
pub(crate) struct FieldScorer<'a> {
    ctx: FieldCtx,
    combine: Combine,
    terms: Vec<TermCursor<'a>>,
    /// The postings, for building the candidate set.
    postings: Vec<&'a PostingList>,
    /// Term-at-a-time scores of an `AnyMax` leaf (see [`Self::prepare`]).
    taat: Option<TaatScores>,
}

impl<'a> FieldScorer<'a> {
    /// Exact AND leaf over analysed `terms` (`search_field` semantics). `None`
    /// when the field-leaf matches nothing: an out-of-range field, no terms,
    /// or any term missing from the dictionary / postings.
    pub(crate) fn exact(
        idx: &'a TextIndex,
        field_idx: usize,
        terms: &[String],
        global_df: Option<&HashMap<String, u32>>,
        global_n: Option<u32>,
    ) -> Option<Self> {
        if field_idx >= idx.field_postings.len() || terms.is_empty() {
            return None;
        }
        let n = global_n.unwrap_or(idx.field_stats[field_idx].num_docs);
        let mut cursors = Vec::with_capacity(terms.len());
        let mut postings = Vec::with_capacity(terms.len());
        for term in terms {
            let term_id = idx.field_term_dicts[field_idx].get(term)?;
            let posting = idx.field_postings[field_idx].get_posting(term_id)?;
            let df = global_df
                .and_then(|m| m.get(term.as_str()).copied())
                .unwrap_or_else(|| posting.doc_ids.len() as u32);
            cursors.push(TermCursor {
                cursor: posting.cursor(),
                idf: (n != 0 && df != 0).then(|| bm25_idf(df, n)),
            });
            postings.push(posting);
        }
        Some(Self {
            ctx: FieldCtx::new(idx, field_idx),
            combine: Combine::AllSum,
            terms: cursors,
            postings,
            taat: None,
        })
    }

    /// Fuzzy/prefix OR leaf over expanded term ids (`search_field_or`
    /// semantics: LOCAL df, `global_n` honoured, best term wins). `None` when
    /// no expanded term has a posting.
    #[cfg_attr(not(feature = "text-index"), allow(dead_code))]
    pub(crate) fn any_of(
        idx: &'a TextIndex,
        field_idx: usize,
        expanded_term_ids: &[u32],
        global_n: Option<u32>,
    ) -> Option<Self> {
        if field_idx >= idx.field_postings.len() || expanded_term_ids.is_empty() {
            return None;
        }
        let n = global_n.unwrap_or(idx.field_stats[field_idx].num_docs);
        let mut cursors = Vec::with_capacity(expanded_term_ids.len());
        let mut postings = Vec::with_capacity(expanded_term_ids.len());
        for &term_id in expanded_term_ids {
            let Some(posting) = idx.field_postings[field_idx].get_posting(term_id) else {
                continue;
            };
            let df = posting.doc_ids.len() as u32;
            cursors.push(TermCursor {
                cursor: posting.cursor(),
                idf: (n != 0 && df != 0).then(|| bm25_idf(df, n)),
            });
            postings.push(posting);
        }
        if cursors.is_empty() {
            return None;
        }
        Some(Self {
            ctx: FieldCtx::new(idx, field_idx),
            combine: Combine::AnyMax,
            terms: cursors,
            postings,
            taat: None,
        })
    }

    /// The field-leaf's match set: the intersection (rarest posting first) or
    /// the union of its postings. Not yet restricted to resolvable documents.
    pub(crate) fn candidates(&self) -> RoaringBitmap {
        match self.combine {
            Combine::AllSum => intersect_rarest_first(&self.postings),
            Combine::AnyMax => {
                let mut acc = RoaringBitmap::new();
                for p in &self.postings {
                    acc |= &p.doc_ids;
                }
                acc
            }
        }
    }

    /// Choose how a fuzzy/prefix (`AnyMax`) leaf scores `candidates` — the
    /// docs [`Self::score`] will be asked about, ascending (moon#1220).
    ///
    /// Document-at-a-time seeks every expanded term's cursor per candidate:
    /// `|candidates| × terms` seeks. Term-at-a-time walks each expanded
    /// posting once and folds its contributions into a dense window over the
    /// candidates' id range: `Σ |posting| + window` steps. The latter is
    /// taken when it is clearly cheaper — typically a bare prefix/fuzzy query
    /// (the candidates ARE the postings' union); a prefix ANDed with a narrow
    /// filter keeps seeking. Either way the scores are bit-identical: each doc
    /// folds its matching terms in expansion order with the same `>`-max from
    /// `0.0`. Exact (`AllSum`) leaves are unaffected.
    pub(crate) fn prepare(&mut self, idx: &TextIndex, candidates: &RoaringBitmap) {
        self.taat = None;
        if self.combine != Combine::AnyMax {
            return;
        }
        let (Some(lo), Some(hi)) = (candidates.min(), candidates.max()) else {
            return;
        };
        let seeks = candidates.len().saturating_mul(self.terms.len() as u64);
        let entries: u64 = self.postings.iter().map(|p| p.doc_ids.len()).sum();
        let window = u64::from(hi - lo) + 1;
        let cheaper = self.terms.len() >= TAAT_MIN_TERMS
            && entries <= TAAT_ENTRIES_PER_SEEK.saturating_mul(seeks)
            && window <= seeks;
        #[cfg(test)]
        let cheaper = FORCE_TAAT.with(std::cell::Cell::get).unwrap_or(cheaper);
        if cheaper {
            self.taat = Some(self.fold_term_at_a_time(idx, lo, hi));
        }
    }

    /// The `AnyMax` score of every doc in `[lo, hi]`, term by term. Per doc
    /// this is exactly [`Self::score`]'s fold: `best = 0.0`, then for each
    /// matching term IN EXPANSION ORDER `if s > best { best = s }`, with `s`
    /// from the same `contribution(tf, len_norm)` — so the bits are the same.
    fn fold_term_at_a_time(&self, idx: &TextIndex, lo: u32, hi: u32) -> TaatScores {
        #[cfg(test)]
        TAAT_FOLDS.with(|c| c.set(c.get() + 1));
        let ctx = self.ctx;
        let len_norm: Vec<f32> = (lo..=hi).map(|d| self.len_norm(idx, d)).collect();
        let mut best = vec![f32::NAN; len_norm.len()];
        for (term, posting) in self.terms.iter().zip(&self.postings) {
            posting.for_each_tf(|doc, tf| {
                let Some(at) = doc.checked_sub(lo) else {
                    return true; // below the window
                };
                let at = at as usize;
                let (Some(slot), Some(&norm)) = (best.get_mut(at), len_norm.get(at)) else {
                    return false; // past the window: the rest of the posting is too
                };
                let score = term.contribution(tf, norm, &ctx);
                let base = if slot.is_nan() { 0.0 } else { *slot };
                *slot = if score > base { score } else { base };
                true
            });
        }
        TaatScores { lo, best }
    }

    /// Score `doc` (ascending across calls). `None` when `doc` is not in this
    /// field-leaf's match set; otherwise HEAD's exact per-field score.
    #[inline]
    pub(crate) fn score(&mut self, idx: &TextIndex, doc: u32) -> Option<f32> {
        if let Some(scored) = self.taat.as_ref().and_then(|t| t.get(doc)) {
            return scored;
        }
        let ctx = self.ctx;
        match self.combine {
            Combine::AllSum => {
                let mut tfs: SmallVec<[u32; 8]> = SmallVec::new();
                for t in &mut self.terms {
                    tfs.push(t.cursor.seek(doc)?);
                }
                let len_norm = self.len_norm(idx, doc);
                let mut doc_score = 0.0f32;
                for (t, &tf) in self.terms.iter().zip(tfs.iter()) {
                    doc_score += t.contribution(tf, len_norm, &ctx);
                }
                Some(doc_score)
            }
            Combine::AnyMax => {
                // Seek every cursor (forward-only), remembering who matched.
                let mut hits: SmallVec<[(u16, u32); 8]> = SmallVec::new();
                for (i, t) in self.terms.iter_mut().enumerate() {
                    if let Some(tf) = t.cursor.seek(doc) {
                        hits.push((i as u16, tf));
                    }
                }
                if hits.is_empty() {
                    return None;
                }
                let len_norm = self.len_norm(idx, doc);
                let mut best_score = 0.0f32;
                for &(i, tf) in &hits {
                    let score = self.terms[i as usize].contribution(tf, len_norm, &ctx);
                    if score > best_score {
                        best_score = score;
                    }
                }
                Some(best_score)
            }
        }
    }

    #[inline]
    fn len_norm(&self, idx: &TextIndex, doc: u32) -> f32 {
        let dl = idx.doc_field_len(doc, self.ctx.field_idx);
        bm25_len_norm(dl, self.ctx.avgdl, self.ctx.k1, self.ctx.b)
    }
}

/// Intersect postings starting from the smallest (moon#1191: HEAD cloned query
/// term 0's posting, however broad, and intersected from there).
pub(crate) fn intersect_rarest_first(postings: &[&PostingList]) -> RoaringBitmap {
    let mut order: SmallVec<[&PostingList; 8]> = postings.iter().copied().collect();
    order.sort_by_key(|p| p.doc_ids.len());
    let mut iter = order.into_iter();
    let Some(first) = iter.next() else {
        return RoaringBitmap::new();
    };
    let Some(second) = iter.next() else {
        return first.doc_ids.clone();
    };
    // `&a & &b` allocates only the (small) result — no clone of a broad posting.
    let mut acc = &first.doc_ids & &second.doc_ids;
    for p in iter {
        if acc.is_empty() {
            break;
        }
        acc &= &p.doc_ids;
    }
    acc
}

/// A TEXT leaf of the query AST: one field scorer (field-scoped leaf) or one
/// per searchable field (cross-field leaf, per-doc sum in field order).
#[cfg(feature = "text-index")]
pub(crate) struct LeafScorer<'a> {
    fields: Vec<FieldScorer<'a>>,
    cross_field: bool,
}

#[cfg(feature = "text-index")]
impl<'a> LeafScorer<'a> {
    pub(crate) fn new(fields: Vec<FieldScorer<'a>>, cross_field: bool) -> Self {
        Self {
            fields,
            cross_field,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// See [`FieldScorer::prepare`].
    fn prepare(&mut self, idx: &TextIndex, candidates: &RoaringBitmap) {
        for f in &mut self.fields {
            f.prepare(idx, candidates);
        }
    }

    /// Union of the per-field match sets.
    pub(crate) fn candidates(&self) -> RoaringBitmap {
        let mut acc = RoaringBitmap::new();
        for f in &self.fields {
            acc |= f.candidates();
        }
        acc
    }

    #[inline]
    fn score(&mut self, idx: &TextIndex, doc: u32) -> Option<f32> {
        if !self.cross_field {
            return self.fields.first_mut()?.score(idx, doc);
        }
        // HEAD: `acc.entry(doc).or_insert(0.0) += field_score`, fields in order.
        let mut acc: Option<f32> = None;
        for f in &mut self.fields {
            if let Some(s) = f.score(idx, doc) {
                acc = Some(acc.unwrap_or(0.0) + s);
            }
        }
        acc
    }
}

/// Sum of every TEXT leaf's contribution (AST DFS order), `0.0` for a doc no
/// TEXT leaf matched — HEAD's `accumulate_text_scores` + `unwrap_or(0.0)`.
#[cfg(feature = "text-index")]
pub(crate) struct QueryScorer<'a> {
    leaves: Vec<LeafScorer<'a>>,
}

#[cfg(feature = "text-index")]
impl<'a> QueryScorer<'a> {
    pub(crate) fn new(leaves: Vec<LeafScorer<'a>>) -> Self {
        Self { leaves }
    }

    /// Tell every leaf which documents it will score (ascending), so wide
    /// fuzzy/prefix expansions can switch to term-at-a-time (moon#1220).
    pub(crate) fn prepare(&mut self, idx: &TextIndex, candidates: &RoaringBitmap) {
        for leaf in &mut self.leaves {
            leaf.prepare(idx, candidates);
        }
    }

    /// No TEXT leaf can contribute: every document scores `0.0`.
    pub(crate) fn is_constant_zero(&self) -> bool {
        self.leaves.iter().all(LeafScorer::is_empty)
    }

    #[inline]
    pub(crate) fn score(&mut self, idx: &TextIndex, doc: u32) -> f32 {
        let mut acc = 0.0f32;
        for leaf in &mut self.leaves {
            if let Some(s) = leaf.score(idx, doc) {
                acc += s;
            }
        }
        acc
    }
}

/// Total order over scores matching HEAD's `partial_cmp` for every non-NaN
/// value (`-0.0 == 0.0`). NaN — unreachable with finite field weights — sorts
/// below every number instead of HEAD's sort-algorithm-dependent placement.
#[inline]
fn score_key(score: f32) -> u32 {
    if score.is_nan() {
        return 0;
    }
    let bits = if score == 0.0 { 0 } else { score.to_bits() };
    if bits & 0x8000_0000 != 0 {
        !bits
    } else {
        bits | 0x8000_0000
    }
}

/// Heap entry ordered so the WORST result (lowest score, then highest doc id)
/// is the max — `BinaryHeap::peek` is the eviction candidate.
#[derive(Clone, Copy)]
struct Ranked {
    key: u32,
    doc: u32,
    score: f32,
}

impl Ranked {
    #[inline]
    fn new(doc: u32, score: f32) -> Self {
        Self {
            key: score_key(score),
            doc,
            score,
        }
    }
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.doc == other.doc
    }
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    /// `Less` = better: higher score first, then lower doc id.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.key.cmp(&self.key).then(self.doc.cmp(&other.doc))
    }
}

/// Bounded best-`k` selection in `score DESC, doc_id ASC` order.
pub(crate) struct TopK {
    k: usize,
    heap: BinaryHeap<Ranked>,
}

impl TopK {
    /// `expected` bounds the number of pushes (sizes the heap).
    pub(crate) fn new(k: usize, expected: usize) -> Self {
        Self {
            k,
            heap: BinaryHeap::with_capacity(k.min(expected)),
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, doc: u32, score: f32) {
        if self.k == 0 {
            return;
        }
        let r = Ranked::new(doc, score);
        if self.heap.len() < self.k {
            self.heap.push(r);
        } else if let Some(mut worst) = self.heap.peek_mut() {
            if r < *worst {
                *worst = r;
            }
        }
    }

    /// Best-first results; keys resolved (and cloned) only here, for the page.
    /// A doc without a key is skipped (callers already restricted to live docs).
    pub(crate) fn into_results(self, idx: &TextIndex) -> Vec<TextSearchResult> {
        let ranked = self.heap.into_sorted_vec();
        let mut out = Vec::with_capacity(ranked.len());
        for r in ranked {
            if let Some(key) = idx.doc_id_to_key.get(&r.doc) {
                out.push(TextSearchResult {
                    doc_id: r.doc,
                    key: key.clone(),
                    score: r.score,
                });
            }
        }
        out
    }
}

/// Score every doc of `candidates` (ascending) with `field` and keep the best
/// `top_k` — the shared body of `search_field` / `search_field_or` (and their
/// AS_OF variants, via `keep`).
pub(crate) fn top_k_for_field(
    idx: &TextIndex,
    mut field: FieldScorer<'_>,
    top_k: usize,
    keep: impl Fn(u32) -> bool,
) -> Vec<TextSearchResult> {
    let candidates = idx.restrict_to_live(field.candidates());
    field.prepare(idx, &candidates);
    let mut top = TopK::new(top_k, candidates.len() as usize);
    for doc in &candidates {
        if !keep(doc) {
            continue;
        }
        if let Some(score) = field.score(idx, doc) {
            top.push(doc, score);
        }
    }
    top.into_results(idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_key_orders_like_partial_cmp() {
        let vals = [
            f32::NEG_INFINITY,
            -3.5,
            -1e-30,
            -0.0,
            0.0,
            1e-30,
            0.25,
            7.0,
            f32::INFINITY,
        ];
        for &a in &vals {
            for &b in &vals {
                let want = a.partial_cmp(&b).unwrap();
                assert_eq!(score_key(a).cmp(&score_key(b)), want, "{a} vs {b}");
            }
        }
        assert!(score_key(f32::NAN) < score_key(f32::NEG_INFINITY));
    }

    /// moon#1220: an `AnyMax` leaf prepared term-at-a-time over a PARTIAL id window scores every
    /// doc — below, inside and above the window (the latter two through the cursor fallback) —
    /// bit-identically to a pure cursor scorer, for positive, zero and negative field weights,
    /// with distinct per-doc tfs (CONVENTIONS) and a chunked (> 256-entry) posting.
    #[cfg(feature = "text-index")]
    #[test]
    fn term_at_a_time_window_matches_cursor_scoring_bit_for_bit() {
        use crate::protocol::Frame;
        use crate::text::types::{BM25Config, TextFieldDef};
        use bytes::Bytes;

        for weight in [1.0f64, 2.5, 0.0, -1.5] {
            let mut field = TextFieldDef::new(Bytes::from_static(b"body"));
            field.weight = weight;
            let mut idx = TextIndex::new(
                Bytes::from_static(b"t"),
                vec![Bytes::from_static(b"d:")],
                vec![field],
                BM25Config::default(),
            );
            let words = ["pa", "pat", "path", "patch", "pater", "patio", "pawn"];
            for d in 0..900u32 {
                let mut body = String::new();
                for (i, w) in words.iter().enumerate() {
                    // Distinct tf per (doc, word); some docs miss some words; `path` is in
                    // nearly every doc (a chunked posting).
                    let tf = (d as usize * (i + 3) + i) % 5;
                    for _ in 0..tf {
                        body.push_str(w);
                        body.push(' ');
                    }
                }
                body.push_str(&"filler ".repeat((d % 7) as usize));
                if d % 11 != 0 {
                    body.push_str("path");
                }
                let key = format!("d:{d}");
                let args = [
                    Frame::BulkString(Bytes::from_static(b"body")),
                    Frame::BulkString(Bytes::from(body)),
                ];
                idx.index_document(u64::from(d) + 1, key.as_bytes(), &args);
            }
            let ids: Vec<u32> = words
                .iter()
                .filter_map(|w| idx.field_term_dicts[0].get(w))
                .collect();
            assert!(ids.len() >= TAAT_MIN_TERMS, "expansion too narrow: {ids:?}");
            let path = idx.field_term_dicts[0].get("path").expect("path");
            assert!(idx.field_postings[0].doc_freq(path) > 256);

            let window: RoaringBitmap = (300u32..620).filter(|d| d % 3 != 1).collect();
            let mut taat = FieldScorer::any_of(&idx, 0, &ids, None).expect("scorer");
            with_forced_taat(Some(true), || taat.prepare(&idx, &window));
            assert!(taat.taat.is_some(), "forced term-at-a-time did not fold");
            let mut cursor = FieldScorer::any_of(&idx, 0, &ids, None).expect("scorer");
            with_forced_taat(Some(false), || cursor.prepare(&idx, &window));
            assert!(cursor.taat.is_none());
            let mut matched = 0;
            for d in 0..900u32 {
                let (a, b) = (taat.score(&idx, d), cursor.score(&idx, d));
                assert_eq!(
                    a.map(f32::to_bits),
                    b.map(f32::to_bits),
                    "doc {d} w={weight}"
                );
                matched += usize::from(b.is_some_and(|s| s != 0.0));
            }
            assert_eq!(
                matched > 0,
                weight > 0.0,
                "w={weight}: positive scores exist"
            );
        }
    }

    #[test]
    fn top_k_matches_full_sort_with_ties() {
        // Distinct, asymmetric scores with deliberate ties (CONVENTIONS).
        let scores = [3.0f32, 1.0, 3.0, 2.5, 1.0, 9.0, 0.0, 2.5, 3.0, -1.0];
        let mut all: Vec<(u32, f32)> = scores
            .iter()
            .enumerate()
            .map(|(i, &s)| (i as u32, s))
            .collect();
        all.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap().then(a.0.cmp(&b.0)));
        for k in 0..=scores.len() + 2 {
            let mut top = TopK::new(k, scores.len());
            for (i, &s) in scores.iter().enumerate() {
                top.push(i as u32, s);
            }
            let got: Vec<(u32, f32)> = top
                .heap
                .into_sorted_vec()
                .into_iter()
                .map(|r| (r.doc, r.score))
                .collect();
            let want: Vec<(u32, f32)> = all.iter().copied().take(k).collect();
            assert_eq!(got, want, "k={k}");
        }
    }
}
