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

/// Ids folded per term-at-a-time block (moon#1226). The fold's scratch is two
/// `f32` per id of ONE block — 32 KB — however wide the candidates' id window;
/// it used to be two `f32` per id of the whole window, which the cost model
/// bounds only by `|candidates| × terms` (a 50-term prefix over sparse
/// candidates could ask for tens of MB).
const TAAT_BLOCK: u32 = 4096;

/// A fuzzy/prefix field-leaf's scores, folded term-at-a-time over the id
/// window `[lo, hi]` (moon#1220) one block at a time (moon#1226): `best[d -
/// block_lo]` is the leaf's score for doc `d` of the current block, NaN when
/// no expanded term contains `d`. A real score is never NaN — the `>`-max
/// starts at `0.0` and NaN never compares greater. Each expanded term keeps
/// its own cursor as the resume point between blocks, so the whole window
/// still costs one pass over every posting.
struct TaatScores<'a> {
    lo: u32,
    hi: u32,
    /// First id of the folded block; `best.len()` ids from here (0 = none yet).
    block_lo: u32,
    best: Vec<f32>,
    len_norm: Vec<f32>,
    /// One resume point per expanded term (same order as `FieldScorer::terms`).
    cursors: Vec<PostingCursor<'a>>,
    block: u32,
}

impl TaatScores<'_> {
    /// `Some(Some(score))` / `Some(None)` (not matched) inside the window,
    /// `None` outside it — or for a doc behind the folded block, which the
    /// ascending contract never asks for (the caller's cursors answer it).
    #[inline]
    fn get(
        &mut self,
        idx: &TextIndex,
        doc: u32,
        terms: &[TermCursor<'_>],
        ctx: &FieldCtx,
    ) -> Option<Option<f32>> {
        if doc < self.lo || doc > self.hi || doc < self.block_lo {
            return None;
        }
        let at = (doc - self.block_lo) as usize;
        if at >= self.best.len() {
            self.fold_block(idx, doc, terms, ctx);
        }
        let s = *self.best.get((doc - self.block_lo) as usize)?;
        Some((!s.is_nan()).then_some(s))
    }

    /// Fold the block `[start, min(start + block - 1, hi)]`, term by term in
    /// expansion order: per doc exactly [`FieldScorer::score`]'s fold
    /// (`best = 0.0`, then `if s > best { best = s }` for each matching term,
    /// `s` from the same `contribution(tf, len_norm)`) — so the same bits.
    fn fold_block(
        &mut self,
        idx: &TextIndex,
        start: u32,
        terms: &[TermCursor<'_>],
        ctx: &FieldCtx,
    ) {
        let end = start.saturating_add(self.block - 1).min(self.hi);
        self.block_lo = start;
        self.len_norm.clear();
        self.len_norm.extend((start..=end).map(|d| {
            bm25_len_norm(
                idx.doc_field_len(d, ctx.field_idx),
                ctx.avgdl,
                ctx.k1,
                ctx.b,
            )
        }));
        self.best.clear();
        self.best.resize(self.len_norm.len(), f32::NAN);
        let (best, len_norm) = (&mut self.best, &self.len_norm);
        for (cursor, term) in self.cursors.iter_mut().zip(terms) {
            // Resume: skip what lies before this block, fold what lies in it.
            let _ = cursor.seek(start);
            cursor.drain_through(end, |doc, tf| {
                let at = (doc - start) as usize;
                if let (Some(slot), Some(&norm)) = (best.get_mut(at), len_norm.get(at)) {
                    let score = term.contribution(tf, norm, ctx);
                    let base = if slot.is_nan() { 0.0 } else { *slot };
                    *slot = if score > base { score } else { base };
                }
            });
        }
        #[cfg(test)]
        TAAT_SCRATCH_PEAK.with(|c| {
            let bytes = (self.best.capacity() + self.len_norm.capacity()) * 4;
            c.set(c.get().max(bytes));
        });
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
    /// Test override of [`TAAT_BLOCK`] (multi-block folds on small corpora).
    static TAAT_BLOCK_OVERRIDE: std::cell::Cell<Option<u32>> = const { std::cell::Cell::new(None) };
    /// Largest scratch (bytes) one term-at-a-time fold held on this thread.
    static TAAT_SCRATCH_PEAK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[inline]
fn taat_block() -> u32 {
    #[cfg(test)]
    if let Some(b) = TAAT_BLOCK_OVERRIDE.with(std::cell::Cell::get) {
        return b.max(1);
    }
    TAAT_BLOCK
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
    taat: Option<TaatScores<'a>>,
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
    /// posting once and folds its contributions into dense blocks of the
    /// candidates' id range (`TAAT_BLOCK` ids of scratch at a time, moon#1226):
    /// `Σ |posting| + window` steps. The latter is
    /// taken when it is clearly cheaper — typically a bare prefix/fuzzy query
    /// (the candidates ARE the postings' union); a prefix ANDed with a narrow
    /// filter keeps seeking. Either way the scores are bit-identical: each doc
    /// folds its matching terms in expansion order with the same `>`-max from
    /// `0.0`. Exact (`AllSum`) leaves are unaffected.
    pub(crate) fn prepare(&mut self, _idx: &TextIndex, candidates: &RoaringBitmap) {
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
            self.taat = Some(self.fold_term_at_a_time(lo, hi));
        }
    }

    /// Term-at-a-time state for the window `[lo, hi]`: one resume cursor per
    /// expanded term and block-sized scratch; blocks are folded lazily as
    /// [`Self::score`] walks the candidates (see [`TaatScores`]).
    fn fold_term_at_a_time(&self, lo: u32, hi: u32) -> TaatScores<'a> {
        #[cfg(test)]
        TAAT_FOLDS.with(|c| c.set(c.get() + 1));
        let block = taat_block();
        let cap = (hi - lo).saturating_add(1).min(block) as usize;
        TaatScores {
            lo,
            hi,
            block_lo: lo,
            best: Vec::with_capacity(cap),
            len_norm: Vec::with_capacity(cap),
            cursors: self.postings.iter().map(|p| p.cursor()).collect(),
            block,
        }
    }

    /// Score `doc` (ascending across calls). `None` when `doc` is not in this
    /// field-leaf's match set; otherwise HEAD's exact per-field score.
    #[inline]
    pub(crate) fn score(&mut self, idx: &TextIndex, doc: u32) -> Option<f32> {
        if let Some(taat) = self.taat.as_mut() {
            if let Some(scored) = taat.get(idx, doc, &self.terms, &self.ctx) {
                return scored;
            }
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
///
/// moon#1226: when `k` covers every expected push (`k >= expected`, e.g.
/// `LIMIT 0 10000` over a few hundred matches, or an unbounded caller) nothing
/// can ever be evicted, so the pushes go into a plain `Vec` sorted once at the
/// end — HEAD sifted each push into a `BinaryHeap` and then heap-sorted it.
/// Same order either way: `Ranked`'s order is total over distinct docs.
pub(crate) struct TopK {
    k: usize,
    heap: BinaryHeap<Ranked>,
    /// The `k >= expected` mode: every push, unsorted (`None` = heap mode).
    all: Option<Vec<Ranked>>,
}

impl TopK {
    /// `expected` bounds the number of pushes (sizes the heap).
    pub(crate) fn new(k: usize, expected: usize) -> Self {
        let keep_all = k >= expected;
        Self {
            k,
            heap: BinaryHeap::with_capacity(if keep_all { 0 } else { k.min(expected) }),
            all: keep_all.then(|| Vec::with_capacity(expected)),
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, doc: u32, score: f32) {
        if self.k == 0 {
            return;
        }
        let r = Ranked::new(doc, score);
        if let Some(all) = &mut self.all {
            if all.len() < self.k {
                all.push(r);
                return;
            }
            // More pushes than `expected` promised: continue as a heap.
            self.heap = BinaryHeap::from(std::mem::take(all));
            self.all = None;
        }
        if self.heap.len() < self.k {
            self.heap.push(r);
        } else if let Some(mut worst) = self.heap.peek_mut() {
            if r < *worst {
                *worst = r;
            }
        }
    }

    /// Best-first ranked entries.
    fn into_sorted(self) -> Vec<Ranked> {
        match self.all {
            Some(mut all) => {
                all.sort_unstable();
                all
            }
            None => self.heap.into_sorted_vec(),
        }
    }

    /// Best-first results; keys resolved (and cloned) only here, for the page.
    /// A doc without a key is skipped (callers already restricted to live docs).
    pub(crate) fn into_results(self, idx: &TextIndex) -> Vec<TextSearchResult> {
        let ranked = self.into_sorted();
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
            // moon#1226: one block (default), many blocks, odd-sized blocks and
            // single-id blocks — the per-term resume points must carry every
            // posting across block boundaries without skipping or repeating.
            for block in [None, Some(64u32), Some(7), Some(1)] {
                let mut taat = FieldScorer::any_of(&idx, 0, &ids, None).expect("scorer");
                with_taat_block(block, || {
                    with_forced_taat(Some(true), || taat.prepare(&idx, &window))
                });
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
                        "doc {d} w={weight} block={block:?}"
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
    }

    fn with_taat_block<R>(block: Option<u32>, f: impl FnOnce() -> R) -> R {
        let prev = TAAT_BLOCK_OVERRIDE.with(|c| c.replace(block));
        let out = f();
        TAAT_BLOCK_OVERRIDE.with(|c| c.set(prev));
        out
    }

    /// moon#1226 red test: the term-at-a-time scratch is bounded by one
    /// block, however wide the candidates' id window. HEAD allocated two f32
    /// per id of the whole window (`len_norm` + `best`), bounded only by
    /// `|candidates| × terms`: 8 B × 60,000 ids = 480 KB here, and tens of MB
    /// for a wide expansion over a large corpus.
    #[cfg(feature = "text-index")]
    #[test]
    fn term_at_a_time_scratch_is_one_block_not_the_whole_window() {
        use crate::text::types::{BM25Config, TextFieldDef};
        use bytes::Bytes;

        let mut idx = TextIndex::new(
            Bytes::from_static(b"t"),
            vec![Bytes::from_static(b"d:")],
            vec![TextFieldDef::new(Bytes::from_static(b"body"))],
            BM25Config::default(),
        );
        // Bulk-load 60,000 docs through the store API: 6 expanded terms with
        // distinct per-doc tfs, one of them in every doc.
        let words = ["pa", "pat", "path", "patch", "pater", "patio"];
        for d in 0..60_000u32 {
            let mut body = String::from("path ");
            for (i, w) in words.iter().enumerate() {
                for _ in 0..((d as usize + i) % 3) {
                    body.push_str(w);
                    body.push(' ');
                }
            }
            let key = format!("d:{d}");
            let args = [
                crate::protocol::Frame::BulkString(Bytes::from_static(b"body")),
                crate::protocol::Frame::BulkString(Bytes::from(body)),
            ];
            idx.index_document(u64::from(d) + 1, key.as_bytes(), &args);
        }
        let ids: Vec<u32> = words
            .iter()
            .filter_map(|w| idx.field_term_dicts[0].get(w))
            .collect();
        let window = idx.live_docs().clone();
        assert_eq!(window.len(), 60_000);
        TAAT_SCRATCH_PEAK.with(|c| c.set(0));
        let mut taat = FieldScorer::any_of(&idx, 0, &ids, None).expect("scorer");
        with_forced_taat(Some(true), || taat.prepare(&idx, &window));
        let mut cursor = FieldScorer::any_of(&idx, 0, &ids, None).expect("scorer");
        with_forced_taat(Some(false), || cursor.prepare(&idx, &window));
        for d in &window {
            assert_eq!(
                taat.score(&idx, d).map(f32::to_bits),
                cursor.score(&idx, d).map(f32::to_bits),
                "doc {d}"
            );
        }
        let peak = TAAT_SCRATCH_PEAK.with(std::cell::Cell::get);
        assert!(peak > 0, "the fold ran");
        // The design bound: two f32 per id of one 4096-id block (32 KiB).
        const ONE_BLOCK: usize = 8 * 4096;
        assert!(
            peak <= ONE_BLOCK,
            "term-at-a-time scratch {peak} B for a 60,000-id window (bound: {ONE_BLOCK} B)"
        );
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
        // `expected` below, at and above the push count: heap mode, the
        // keep-all mode (moon#1226) and its fall-back to the heap.
        for expected in [scores.len(), scores.len() / 2, scores.len() + 5] {
            for k in 0..=scores.len() + 2 {
                let mut top = TopK::new(k, expected);
                assert_eq!(top.all.is_some(), k >= expected, "mode for k={k}");
                for (i, &s) in scores.iter().enumerate() {
                    top.push(i as u32, s);
                }
                let got: Vec<(u32, f32)> = top
                    .into_sorted()
                    .into_iter()
                    .map(|r| (r.doc, r.score))
                    .collect();
                let want: Vec<(u32, f32)> = all.iter().copied().take(k).collect();
                assert_eq!(got, want, "k={k} expected={expected}");
            }
        }
    }
}
