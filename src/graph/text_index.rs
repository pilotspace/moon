//! `SegmentTextIndex` — per-frozen-CSR-segment text index for Cypher text
//! predicates (`CONTAINS` / `STARTS WITH` / `ENDS WITH` / `=~`), P3 design
//! part B (`tmp/DESIGN-RESULT-CACHE-FTS.md` section B).
//!
//! Reuses Moon's existing FTS primitives — `crate::text::posting::PostingStore`,
//! `crate::text::bm25::{FieldStats, bm25_score}`, `crate::text::term_dict::TermDictionary`
//! — verbatim, as a library. Those three modules have zero dependency on
//! `TextIndex`/`TextStore`'s doc-id space or MVCC/visibility model (confirmed
//! by reading `src/text/posting.rs`, `src/text/bm25.rs`,
//! `src/text/term_dict.rs`: none of them import `crate::text::store`), so no
//! physical file move was needed to decouple them — `use` alone reuses them
//! without forking a parallel implementation. The CSR row index (`u32`) IS
//! the posting doc-id directly: no id-mapping layer, mirroring
//! `LabelIndex`/`SegmentPropertyIndexes`, which already use `RoaringBitmap<row>`.
//!
//! # The SUPERSET-candidate correctness crux
//!
//! `CONTAINS`/`STARTS WITH`/`ENDS WITH` are **substring/prefix/suffix**
//! predicates over the RAW property value. Moon's analyzer pipeline
//! (`crate::text::analyzer::AnalyzerPipeline`) case-folds, Unicode-normalizes,
//! and (optionally) stems every token before it reaches a posting list. That
//! makes token-identity lookups UNSOUND as a pruning source for these three
//! predicates:
//!
//! - A substring can span or start inside a token that tokenization treats
//!   as a single, different unit (`CONTAINS 'rust'` must also match the
//!   value `"trusted"` — the substring "rust" is not a token boundary
//!   match, so a term-postings lookup for `"rust"` would MISS it: a false
//!   NEGATIVE, which the SUPERSET contract forbids).
//! - Stemming can change the string entirely (`"running"` stems to
//!   `"run"`), so `STARTS WITH 'runn'` would fail against the stemmed
//!   token even though the raw value legitimately starts with "runn".
//!
//! Given that, this index does **not** attempt token-level pruning for
//! `CONTAINS`/`STARTS WITH`/`ENDS WITH`/`=~`. Instead, `candidate_rows`
//! returns the **presence** bitmap: every row whose value at `prop_id` is a
//! `String`/`Bytes` at all. This is always a safe superset for ANY string
//! predicate on that property (a row without a string value there can never
//! satisfy `(Value::String(_), Value::String(_))` in
//! `crate::graph::cypher::executor::eval::eval_binary_op`, which is the
//! residual `Filter`'s authority — see `planner.rs`'s
//! `extract_text_conjuncts`), and it is still a real prune: segment size
//! down to "rows that carry this string property" for sparse optional text
//! fields (the common case for a bio/description-style field).
//!
//! The tokenized `postings`/`field_stats` ARE still built (reusing
//! `PostingStore`/`FieldStats`/`bm25_score` for real) and exposed via
//! [`SegmentTextIndex::bm25_score_for`] — an internal scoring hook, tested
//! directly, but NOT wired to a Cypher `ORDER BY` surface in this phase (no
//! concrete grammar for it was specified in the design doc; see CHANGELOG
//! for the deferred-follow-up note).
//!
//! # Mutable tier (scope decision, pre-approved)
//!
//! Same limitation as `SegmentPropertyIndexes`: the MUTABLE tier has no text
//! index. `IndexScan` execution (`executor/read.rs::index_scan_keys`) falls
//! back to an exact full scan of the mutable tail for text-only conjuncts —
//! correct, just unaccelerated, matching the mutable tier's existing story
//! for numeric properties before freeze.
//!
//! # GraphUnion merge (scope decision, pre-approved)
//!
//! `compaction.rs::compact_segments` does NOT remap or merge posting row
//! ids across input segments (unlike the vector engine's GraphUnion, which
//! stitches graphs over original codes). The merged segment's
//! `CsrSegment::text_index` field starts as a fresh, empty `OnceLock` —
//! rebuilt lazily, from scratch, on the merged segment's first text-predicate
//! query. This is a correctness-neutral simplification (the SUPERSET
//! contract holds regardless of whether the index exists yet) with a
//! bounded performance cost: the first text query against a freshly merged
//! segment pays a synchronous rebuild, same as any other lazy `OnceLock`
//! index in this file. Full posting-list-merge-with-row-remapping is
//! deferred (flagged as the single largest open question in the design doc).

use std::collections::HashMap;

use roaring::RoaringBitmap;

use crate::graph::types::{NodeMeta, PropertyValue};
use crate::text::bm25::{FieldStats, bm25_score};
use crate::text::posting::PostingStore;
use crate::text::term_dict::TermDictionary;

/// Per-segment text index. See module docs for the SUPERSET-candidate
/// correctness crux and the mutable-tier / GraphUnion scope decisions.
#[derive(Default)]
pub struct SegmentTextIndex {
    /// prop_id -> rows whose value at that property is String/Bytes. The
    /// ONLY structure consulted by `candidate_rows` (the
    /// `CONTAINS`/`STARTS WITH`/`ENDS WITH`/`=~` pruning source).
    present: HashMap<u16, RoaringBitmap>,
    /// prop_id -> tokenized inverted index (verbatim `PostingStore` reuse).
    /// Doc-id = CSR row. NOT consulted for pattern-predicate pruning (see
    /// module docs) -- backs `bm25_score_for` only.
    postings: HashMap<u16, PostingStore>,
    /// prop_id -> term dictionary backing `postings`' term_ids.
    term_dicts: HashMap<u16, TermDictionary>,
    /// prop_id -> aggregate field statistics (verbatim `FieldStats` reuse).
    field_stats: HashMap<u16, FieldStats>,
    /// prop_id -> row -> token count for that row's field value. Needed by
    /// `bm25_score_for` for the `field_length` term in the BM25 formula;
    /// not tracked by `PostingStore` itself (which is per-term, not per-doc).
    field_len: HashMap<u16, HashMap<u32, u32>>,
}

// Manual (not derived) `Debug`: `PostingStore`/`TermDictionary` don't
// implement it. `CsrSegment` derives `Debug` and holds this behind a
// `OnceLock`, which needs its inner type to be `Debug` -- a summary view
// (counts, not full contents) is sufficient for that purpose.
impl std::fmt::Debug for SegmentTextIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentTextIndex")
            .field("indexed_props", &self.present.len())
            .field("tokenized_props", &self.postings.len())
            .finish()
    }
}

impl SegmentTextIndex {
    /// Build from CSR node metadata + the v5 node-property blob. Mirrors
    /// `SegmentPropertyIndexes::build`'s exhaustive-scan shape: every row's
    /// properties are decoded once; an absent (property, row) pair means no
    /// row here can carry a string value there (empty is correct, not a
    /// fall-back-to-scan signal).
    pub fn build(node_meta: &[NodeMeta], node_props_blob: &[u8]) -> Self {
        let mut idx = Self::default();
        let analyzer = new_analyzer();
        for (row, nm) in node_meta.iter().enumerate() {
            if nm.property_offset == 0 {
                continue; // no property record for this row
            }
            let row = row as u32;
            let props =
                crate::graph::csr::props::decode_node_props(node_props_blob, nm.property_offset);
            for (pid, val) in &props {
                let (PropertyValue::String(s) | PropertyValue::Bytes(s)) = val else {
                    continue;
                };
                idx.present.entry(*pid).or_default().insert(row);

                // Tokenize for the (currently unwired) BM25 scoring hook.
                // Non-UTF8 byte strings simply contribute no tokens --
                // `present` still prunes correctly for them via CONTAINS/etc.
                let Ok(text) = std::str::from_utf8(s) else {
                    continue;
                };
                let tokens = analyzer.tokenize_with_positions(text);
                if tokens.is_empty() {
                    continue;
                }
                let dict = idx
                    .term_dicts
                    .entry(*pid)
                    .or_insert_with(TermDictionary::new);
                let store = idx.postings.entry(*pid).or_insert_with(PostingStore::new);
                for (term, _pos) in &tokens {
                    let term_id = dict.get_or_insert(term);
                    store.add_term_occurrence(term_id, row, None);
                }
                idx.field_len
                    .entry(*pid)
                    .or_default()
                    .insert(row, tokens.len() as u32);
                let stats = idx.field_stats.entry(*pid).or_default();
                stats.num_docs += 1;
                stats.total_field_length += tokens.len() as u64;
            }
        }
        idx.present.shrink_to_fit();
        idx.postings.shrink_to_fit();
        idx.term_dicts.shrink_to_fit();
        idx.field_stats.shrink_to_fit();
        idx.field_len.shrink_to_fit();
        idx
    }

    /// SUPERSET candidate rows for ANY string pattern predicate
    /// (`CONTAINS`/`STARTS WITH`/`ENDS WITH`/`=~`) against `prop_id`. `None`
    /// means no row in this segment ever carried a String/Bytes value under
    /// `prop_id` (exhaustive build -- see type docs), i.e. the empty set,
    /// same semantics as `SegmentPropertyIndexes::rows_eq` returning empty.
    pub fn candidate_rows(&self, prop_id: u16) -> Option<&RoaringBitmap> {
        self.present.get(&prop_id)
    }

    /// True when no row in this segment carries any string/bytes property at
    /// all (mirrors the "too few vectors" / "no v5 blob" cached-negative
    /// precedent -- `hnsw_bridge`, `SegmentPropertyIndexes::is_empty`).
    pub fn is_empty(&self) -> bool {
        self.present.is_empty()
    }

    /// Approximate resident bytes across every structure. Always heap-owned
    /// once built, regardless of whether the source segment is `Heap` or
    /// `Mmap` (same rule as `SegmentPropertyIndexes`/`hnsw_bridge` --
    /// see `CsrStorage::resident_bytes`).
    pub fn resident_bytes(&self) -> usize {
        let present: usize = self
            .present
            .values()
            .map(|bm| std::mem::size_of::<u16>() + bm.serialized_size())
            .sum();
        let postings: usize = self
            .postings
            .values()
            .map(PostingStore::estimated_bytes)
            .sum();
        let term_dicts: usize = self
            .term_dicts
            .values()
            .map(|d| d.term_count() * (std::mem::size_of::<u32>() + 24)) // rough avg term len
            .sum();
        let field_stats = self.field_stats.len() * std::mem::size_of::<FieldStats>();
        let field_len: usize = self
            .field_len
            .values()
            .map(|m| m.len() * (std::mem::size_of::<u32>() * 2))
            .sum();
        present + postings + term_dicts + field_stats + field_len
    }

    /// Internal BM25 relevance score for `row` against `query_terms` on
    /// `prop_id`'s tokenized index (P3 follow-up: not yet wired to a
    /// Cypher `ORDER BY` surface -- see module docs and CHANGELOG). Reuses
    /// `crate::text::bm25::bm25_score` verbatim, over this segment's own
    /// `PostingStore`/`TermDictionary`/`FieldStats` -- proves the
    /// extracted-library reuse end-to-end. `None` when `prop_id` was never
    /// tokenized in this segment or `row` has no recorded field length.
    pub fn bm25_score_for(&self, prop_id: u16, row: u32, query_terms: &[&str]) -> Option<f64> {
        let store = self.postings.get(&prop_id)?;
        let dict = self.term_dicts.get(&prop_id)?;
        let stats = self.field_stats.get(&prop_id)?;
        let field_length = *self.field_len.get(&prop_id)?.get(&row)?;
        let avg = stats.avg_doc_len();
        let mut total = 0.0f64;
        for term in query_terms {
            let Some(term_id) = dict.get(term) else {
                continue;
            };
            let tf = store.get_posting(term_id).map_or(0, |p| p.tf(row));
            if tf == 0 {
                continue;
            }
            let df = store.doc_freq(term_id);
            total += f64::from(bm25_score(
                tf as f32,
                df,
                stats.num_docs,
                field_length,
                avg,
                1.2,
                0.75,
            ));
        }
        Some(total)
    }
}

/// Construct the shared `AnalyzerPipeline`, branching on the `text-index`
/// feature (its real constructor needs `rust_stemmers::Algorithm`, gated
/// behind that feature; the fallback constructor is always available). Both
/// branches always compile -- `graph` must build with or without
/// `text-index` (see `CLAUDE.md` Feature Gates), and this is the only place
/// in `SegmentTextIndex` that needs to know the difference.
fn new_analyzer() -> crate::text::analyzer::AnalyzerPipeline {
    #[cfg(feature = "text-index")]
    {
        crate::text::analyzer::AnalyzerPipeline::new(rust_stemmers::Algorithm::English, false)
    }
    #[cfg(not(feature = "text-index"))]
    {
        crate::text::analyzer::AnalyzerPipeline::new_fallback()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::memgraph::MemGraph;
    use crate::graph::types::PropertyValue;
    use bytes::Bytes;
    use smallvec::smallvec;

    fn build_segment(rows: &[(&str, Option<&str>)]) -> crate::graph::csr::CsrSegment {
        let mut mg = MemGraph::new(100);
        for (name, bio) in rows {
            let mut props: smallvec::SmallVec<[(u16, PropertyValue); 4]> = smallvec![(
                crate::command::graph::graph_write::label_to_id(b"name"),
                PropertyValue::String(Bytes::copy_from_slice(name.as_bytes())),
            )];
            if let Some(bio) = bio {
                props.push((
                    crate::command::graph::graph_write::label_to_id(b"bio"),
                    PropertyValue::String(Bytes::copy_from_slice(bio.as_bytes())),
                ));
            }
            mg.add_node(smallvec![0], props, None, 1);
        }
        let frozen = mg.freeze().expect("freeze ok");
        crate::graph::csr::CsrSegment::from_frozen(frozen, 1).expect("from_frozen ok")
    }

    #[test]
    fn test_segment_text_index_lazy_build_on_first_query() {
        // Mirrors the SegmentPropertyIndexes test pattern: nothing is built
        // until the accessor is first called (see also
        // `csr::storage::tests::test_resident_bytes_grows_after_text_index_build`
        // for the `CsrStorage::text_index()` OnceLock-level assertion).
        let seg = build_segment(&[("alice", Some("loves rust"))]);
        assert!(seg.text_index.get().is_none());
        let _ = SegmentTextIndex::build(&seg.node_meta, &seg.node_props);
    }

    #[test]
    fn test_segment_text_index_none_cached_for_no_text_props() {
        let seg = build_segment(&[("alice", None), ("bob", None)]);
        let idx = SegmentTextIndex::build(&seg.node_meta, &seg.node_props);
        let name_id = crate::command::graph::graph_write::label_to_id(b"name");
        // `name` IS a string property -- present. `bio` never appears.
        assert!(idx.candidate_rows(name_id).is_some());
        let bio_id = crate::command::graph::graph_write::label_to_id(b"bio");
        assert!(idx.candidate_rows(bio_id).is_none());
    }

    #[test]
    fn test_present_bitmap_is_safe_superset_for_substring_across_token_boundary() {
        // The correctness crux: "trusted" contains "rust" as a raw
        // substring, but tokenizes to a DIFFERENT token than "rust". A
        // naive token-postings lookup for "rust" would MISS this row. The
        // presence bitmap must still include it (superset, not exact).
        let seg = build_segment(&[
            ("alice", Some("trusted colleague")),
            ("bob", Some("no relation")),
        ]);
        let idx = SegmentTextIndex::build(&seg.node_meta, &seg.node_props);
        let bio_id = crate::command::graph::graph_write::label_to_id(b"bio");
        let candidates = idx.candidate_rows(bio_id).cloned().unwrap_or_default();
        // Both rows have a string bio -- both must be superset candidates
        // for CONTAINS 'rust', even though only row 0's raw value actually
        // contains the substring (row 1 would be excluded by the residual
        // Filter downstream, never by the index).
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn test_resident_bytes_grows_after_build() {
        let seg = build_segment(&[("alice", Some("loves rust and graphs"))]);
        let idx = SegmentTextIndex::build(&seg.node_meta, &seg.node_props);
        assert!(idx.resident_bytes() > 0);
        assert!(SegmentTextIndex::default().resident_bytes() == 0);
    }

    #[test]
    fn test_bm25_score_for_reuses_shared_bm25_score() {
        let seg = build_segment(&[
            ("alice", Some("rust programming language")),
            ("bob", Some("rust rust rust")),
            ("carol", Some("completely unrelated text")),
        ]);
        let idx = SegmentTextIndex::build(&seg.node_meta, &seg.node_props);
        let bio_id = crate::command::graph::graph_write::label_to_id(b"bio");
        let s_alice = idx
            .bm25_score_for(bio_id, 0, &["rust"])
            .expect("alice row scored");
        let s_bob = idx
            .bm25_score_for(bio_id, 1, &["rust"])
            .expect("bob row scored");
        let s_carol = idx
            .bm25_score_for(bio_id, 2, &["rust"])
            .expect("carol row scored");
        // Higher term frequency for "rust" -> higher BM25 score.
        assert!(s_bob > s_alice, "bob={s_bob} alice={s_alice}");
        // No occurrence of the query term -> zero score.
        assert_eq!(s_carol, 0.0);
    }
}
