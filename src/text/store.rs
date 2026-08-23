// TextStore and TextIndex -- per-shard text index registry.
//
// TextStore mirrors the VectorStore pattern: a HashMap of named TextIndex
// instances, each holding per-field analyzers, posting stores, term
// dictionaries, and BM25 field statistics. TextIndex.index_document()
// is the core indexing entry point called from auto_index_hset.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::text::analyzer::AnalyzerPipeline;
use crate::text::bm25::{FieldStats, bm25_score};
use crate::text::index_persist::TextIndexMeta;
use crate::text::posting::PostingStore;
use crate::text::term_dict::TermDictionary;
use crate::text::types::{BM25Config, TextFieldDef};
#[cfg(feature = "text-index")]
use crate::text::types::{NumericFieldDef, TagFieldDef};

// ── K4 (P0 fix) accounting constants ───────────────────────────────────────
//
// `TextIndex::resident_bytes()` used to be an O(n) full-recompute walk over
// every posting/term/TAG/NUMERIC entry, called unconditionally every 100ms
// from the shard eviction tick (`persistence_tick.rs`) regardless of whether
// `maxmemory` is even set -- measured 6.4ms/call at 50K docs, 21.3ms at
// 200K, recurring P99 spikes for every command on that shard. These
// constants back an O(1) incremental accumulator instead (mirroring
// `ColdIndex`'s `COLD_ENTRY_OVERHEAD` pattern): fixed per-entry/per-
// occurrence approximations updated at every mutation site rather than
// walked on every read. Not exact -- `hashbrown`'s SwissTable layout and
// `RoaringBitmap`'s compressed container format are implementation details
// -- but monotonic, matching `Database::entry_overhead` (WS6) and
// `TermDictionary::resident_bytes`'s established convention.

/// Fixed per-entry `HashMap`/`BTreeMap` bucket-overhead constant.
const MAP_ENTRY_OVERHEAD: usize = 48;
/// Fixed approximate cost of one bit set in a `RoaringBitmap`. Not
/// `RoaringBitmap::serialized_size()` -- compressed bitmap size is
/// non-linear/non-additive across arbitrary insert/remove patterns and
/// cannot be delta-tracked in O(1) (same reasoning as `PostingStore`'s
/// `POSTING_OCCURRENCE_COST`). Only used by the TAG/NUMERIC accounting
/// helpers, which are `text-index`-only.
#[cfg(feature = "text-index")]
const ROARING_BIT_APPROX_COST: usize = 3;
/// Fixed approximate cost of a brand-new (empty) `RoaringBitmap` container.
#[cfg(feature = "text-index")]
const EMPTY_BITMAP_BASE_COST: usize = 8;

/// Modifier for a query term — controls expansion strategy (D-16).
///
/// Exact terms use direct HashMap TermDictionary lookup (unchanged path).
/// Fuzzy/Prefix terms expand via FST + HashMap dual-path (D-12).
///
/// Canonical definition lives here; Plan 02 will re-import from this location
/// into ft_text_search.rs rather than redefining it.
#[cfg(feature = "text-index")]
#[derive(Debug, Clone, PartialEq)]
pub enum TermModifier {
    /// Direct lookup — no expansion. Term is fully analyzed (stemmed).
    Exact,
    /// Levenshtein fuzzy match with edit distance 1–3 (D-03).
    /// Term is lowercased + NFKD but NOT stemmed (D-06).
    Fuzzy(u8),
    /// Prefix match (trailing asterisk syntax, D-07).
    /// Term is lowercased + NFKD but NOT stemmed (D-07).
    Prefix,
}

/// A BM25-scored search result from TextIndex::search_field().
pub struct TextSearchResult {
    /// Internal document ID.
    pub doc_id: u32,
    /// Original Redis key bytes.
    pub key: Bytes,
    /// Accumulated BM25 score (higher = more relevant).
    pub score: f32,
}

/// A single full-text search index with per-field BM25 data.
///
/// Created by FT.CREATE, populated by auto_index_hset on HSET commands.
/// Each TEXT field in the schema has its own analyzer, posting store,
/// term dictionary, and field statistics.
pub struct TextIndex {
    /// Index name (e.g., "article_idx").
    pub name: Bytes,
    /// Key prefixes for matching HSET keys to this index.
    pub key_prefixes: Vec<Bytes>,
    /// TEXT field definitions from the FT.CREATE schema.
    pub text_fields: Vec<TextFieldDef>,
    /// Per-index BM25 scoring parameters.
    pub bm25_config: BM25Config,
    /// Per-field analyzer pipelines (one per TEXT field).
    pub field_analyzers: Vec<AnalyzerPipeline>,
    /// Per-field inverted index posting stores.
    pub field_postings: Vec<PostingStore>,
    /// Per-field document statistics (num_docs, total_field_length).
    pub field_stats: Vec<FieldStats>,
    /// Per-field term dictionaries.
    pub field_term_dicts: Vec<TermDictionary>,
    /// Per-field FST maps for fuzzy/prefix expansion (one per TEXT field, parallel to field_term_dicts).
    /// None = no FST built yet (built at FT.COMPACT time). Exact queries unaffected when None (D-13).
    #[cfg(feature = "text-index")]
    pub fst_maps: Vec<Option<fst::Map<Vec<u8>>>>,
    /// Per-document field lengths: doc_id -> lengths per field index.
    pub doc_field_lengths: HashMap<u32, Vec<u32>>,
    /// Key hash -> doc_id mapping (same pattern as VectorIndex).
    pub key_hash_to_doc_id: HashMap<u64, u32>,
    /// doc_id -> original Redis key bytes.
    pub doc_id_to_key: HashMap<u32, Bytes>,
    /// Next doc_id to assign.
    next_doc_id: u32,

    // ── Bi-temporal MVCC (v0.1.10 G-1, closing HYB-03 deferral) ──────────
    //
    // Mirrors the vector-store MVCC pattern: every doc inserted inside an
    // auto-index path records its commit LSN so `FT.SEARCH HYBRID AS_OF`
    // can exclude post-snapshot docs from the BM25 stream. Empty / lsn=0
    // entries mean "pre-MVCC document" and are always visible (backwards-
    // compatible with tests and non-AS_OF callers).
    /// `doc_id -> insert LSN` (monotonic LSN from `VectorStore::txn_manager_mut().allocate_lsn()`).
    /// Used by `search_field_as_of` + `search_field_or_as_of` to filter
    /// candidates to those committed at or before the requested AS_OF.
    pub doc_id_to_insert_lsn: HashMap<u32, u64>,
    /// `doc_id -> delete LSN`. Reserved for v0.2 logical-delete wiring so
    /// historical AS_OF queries can still see deleted docs. Present today
    /// so the visibility helper is future-proof.
    pub doc_id_to_delete_lsn: HashMap<u32, u64>,

    // ── TAG index (Plan 152-06, Phase 152) ────────────────────────────────
    //
    // TAG semantics bypass the BM25 analyzer entirely. Storage is a two-level
    // map: field_name -> (normalized tag_value -> doc_id bitmap). `doc_tag_entries`
    // tracks the per-doc tag list so per-field upserts can evict stale entries
    // without wiping untouched fields (partial HSET case, Blocker 4).
    /// TAG field definitions from the FT.CREATE schema (empty on TEXT-only indexes).
    #[cfg(feature = "text-index")]
    pub tag_fields: Vec<TagFieldDef>,
    /// `field_name -> (tag_value -> RoaringBitmap<doc_id>)`.
    /// Outer key is the canonical declared field name (from `TagFieldDef::field_name`).
    /// Inner key is the normalized tag value (ASCII-lowercased unless CASESENSITIVE).
    #[cfg(feature = "text-index")]
    pub tag_indexes: HashMap<Bytes, HashMap<Bytes, roaring::RoaringBitmap>>,
    /// `doc_id -> list of (canonical_field, normalized_value)` entries currently
    /// indexed for that document. Used to revoke stale entries on per-field upsert.
    #[cfg(feature = "text-index")]
    pub doc_tag_entries: HashMap<u32, smallvec::SmallVec<[(Bytes, Bytes); 8]>>,

    // ── NUMERIC index (Plan 152-07, Phase 152) ────────────────────────────
    //
    // NUMERIC semantics bypass the BM25 analyzer entirely. Storage is a two-level
    // map: field_name -> BTreeMap<OrderedFloat<f64>, RoaringBitmap<doc_id>>.
    // `BTreeMap::range` resolves `[min max]` filters in O(log N) bucket seek.
    // `doc_numeric_entries` tracks the per-doc numeric values so per-field
    // upserts can evict stale entries without wiping untouched fields.
    /// NUMERIC field definitions from the FT.CREATE schema (empty on non-NUMERIC indexes).
    #[cfg(feature = "text-index")]
    pub numeric_fields: Vec<NumericFieldDef>,
    /// `field_name -> BTreeMap<OrderedFloat<f64>, RoaringBitmap<doc_id>>`.
    /// Outer key is the canonical declared field name. Inner BTreeMap yields
    /// O(log N) range scans via `BTreeMap::range`.
    #[cfg(feature = "text-index")]
    pub numeric_indexes: HashMap<
        Bytes,
        std::collections::BTreeMap<ordered_float::OrderedFloat<f64>, roaring::RoaringBitmap>,
    >,
    /// `doc_id -> list of (canonical_field, parsed_value)` entries currently
    /// indexed for that document. Used to revoke stale entries on per-field upsert.
    #[cfg(feature = "text-index")]
    pub doc_numeric_entries:
        HashMap<u32, smallvec::SmallVec<[(Bytes, ordered_float::OrderedFloat<f64>); 4]>>,

    /// Logical database this index was created in (WS5a db-scoped indexes —
    /// mirrors `IndexMeta::db_index` in `src/vector/store.rs`). Defaults to
    /// `0`: `TextStore::create_index` does not yet receive the connection's
    /// selected db (see `.planning/v0.6.0-release/WS5A-NOTES.md` gap
    /// report), so every text index is currently tagged db 0 —
    /// behavior-preserving with pre-WS5a global semantics.
    pub db_index: u8,

    /// K4 (P0 fix): O(1) cached total for every `resident_bytes()`
    /// contributor EXCEPT `field_postings`/`field_term_dicts` (those already
    /// carry their own O(field_count) cached totals). Covers per-document
    /// bookkeeping (`doc_field_lengths`, `key_hash_to_doc_id`,
    /// `doc_id_to_key`, MVCC LSN maps) plus TAG/NUMERIC/FST sidecar
    /// contributions. Maintained incrementally by the `charge_*`/`revoke_*`
    /// helpers below at every mutation site -- never recomputed by a walk.
    /// See `resident_bytes_ground_truth` (`#[cfg(test)]`) for the equivalent
    /// full-walk formula this field must always match.
    resident_bytes_extra: usize,

    /// Kernel M4 (task #50): set to `true` when this index's term
    /// dictionaries (and, where present, FST maps) were reconstructed from
    /// the `.tfst` sidecar on boot instead of rebuilt from scratch by the
    /// keyspace rescan. Surfaced additively via `FT.INFO` so operators can
    /// see when the fast-boot path was actually taken vs silently falling
    /// back to a full rescan (missing/stale/corrupt sidecar).
    pub recovered_from_sidecar: bool,
}

impl TextIndex {
    /// Create a new TextIndex for the given schema.
    ///
    /// Creates one AnalyzerPipeline, PostingStore, FieldStats, and
    /// TermDictionary per TEXT field. Analyzers use English stemming
    /// unless the field has `nostem: true`.
    #[cfg(feature = "text-index")]
    pub fn new(
        name: Bytes,
        key_prefixes: Vec<Bytes>,
        text_fields: Vec<TextFieldDef>,
        bm25_config: BM25Config,
    ) -> Self {
        let field_count = text_fields.len();
        let mut field_analyzers = Vec::with_capacity(field_count);
        let mut field_postings = Vec::with_capacity(field_count);
        let mut field_stats = Vec::with_capacity(field_count);
        let mut field_term_dicts = Vec::with_capacity(field_count);

        for field in &text_fields {
            field_analyzers.push(AnalyzerPipeline::new(
                rust_stemmers::Algorithm::English,
                field.nostem,
            ));
            field_postings.push(PostingStore::new());
            field_stats.push(FieldStats::new());
            field_term_dicts.push(TermDictionary::new());
        }

        Self {
            name,
            key_prefixes,
            text_fields,
            bm25_config,
            field_analyzers,
            field_postings,
            field_stats,
            field_term_dicts,
            #[cfg(feature = "text-index")]
            fst_maps: (0..field_count).map(|_| None).collect(),
            doc_field_lengths: HashMap::new(),
            key_hash_to_doc_id: HashMap::new(),
            doc_id_to_key: HashMap::new(),
            next_doc_id: 0,
            doc_id_to_insert_lsn: HashMap::new(),
            doc_id_to_delete_lsn: HashMap::new(),
            #[cfg(feature = "text-index")]
            tag_fields: Vec::new(),
            #[cfg(feature = "text-index")]
            tag_indexes: HashMap::new(),
            #[cfg(feature = "text-index")]
            doc_tag_entries: HashMap::new(),
            #[cfg(feature = "text-index")]
            numeric_fields: Vec::new(),
            #[cfg(feature = "text-index")]
            numeric_indexes: HashMap::new(),
            #[cfg(feature = "text-index")]
            doc_numeric_entries: HashMap::new(),
            db_index: 0,
            resident_bytes_extra: 0,
            recovered_from_sidecar: false,
        }
    }

    /// Create a TextIndex with an explicit TAG + NUMERIC schema (Plan 152-06 / 07).
    ///
    /// This is the constructor used by `FT.CREATE` when the parsed schema
    /// includes TAG or NUMERIC fields. Text-only callers continue to use
    /// `new()` — the signature for `new()` is unchanged, so the 33 existing
    /// call sites compile untouched.
    ///
    /// The outer `tag_indexes` / `numeric_indexes` maps are seeded with one
    /// empty inner map / btree per declared TAG / NUMERIC field so
    /// `search_tag` / `search_numeric_range` on never-inserted fields returns
    /// empty-but-present rather than missing-key (determinism).
    #[cfg(feature = "text-index")]
    pub fn new_with_schema(
        name: Bytes,
        key_prefixes: Vec<Bytes>,
        text_fields: Vec<TextFieldDef>,
        tag_fields: Vec<TagFieldDef>,
        numeric_fields: Vec<NumericFieldDef>,
        bm25_config: BM25Config,
    ) -> Self {
        let mut idx = Self::new(name, key_prefixes, text_fields, bm25_config);
        // K4 (P0 fix): the outer per-field entry is seeded here (empty inner
        // map/btree) so `search_tag`/`search_numeric_range` on a
        // never-inserted field returns empty-but-present rather than
        // missing-key. `resident_bytes_ground_truth` walks `tag_indexes`/
        // `numeric_indexes` regardless of whether the inner container is
        // empty, so this seeding must be charged too -- otherwise every
        // schema-declared TAG/NUMERIC field undercounts by one field entry.
        for tag_def in &tag_fields {
            let is_new = !idx.tag_indexes.contains_key(&tag_def.field_name);
            idx.tag_indexes
                .entry(tag_def.field_name.clone())
                .or_default();
            if is_new {
                idx.resident_bytes_extra += tag_def.field_name.len() + MAP_ENTRY_OVERHEAD;
            }
        }
        idx.tag_fields = tag_fields;
        for num_def in &numeric_fields {
            let is_new = !idx.numeric_indexes.contains_key(&num_def.field_name);
            idx.numeric_indexes
                .entry(num_def.field_name.clone())
                .or_default();
            if is_new {
                idx.resident_bytes_extra += num_def.field_name.len() + MAP_ENTRY_OVERHEAD;
            }
        }
        idx.numeric_fields = numeric_fields;
        idx
    }

    /// Allocate (or fetch) the internal doc_id for this key_hash.
    ///
    /// Shared by `index_document`, `tag_index_document`, and (Plan 07)
    /// `numeric_index_document` so doc_ids are stable regardless of which
    /// method sees a key first. Removes the implicit ordering dependency
    /// that existed when each method managed `next_doc_id` independently
    /// (Blocker 7).
    #[cfg(feature = "text-index")]
    pub(crate) fn ensure_doc_id(&mut self, key_hash: u64, key: &[u8]) -> u32 {
        if let Some(&id) = self.key_hash_to_doc_id.get(&key_hash) {
            return id;
        }
        let id = self.next_doc_id;
        self.next_doc_id += 1;
        self.key_hash_to_doc_id.insert(key_hash, id);
        self.doc_id_to_key.insert(id, Bytes::copy_from_slice(key));
        self.charge_new_doc_key(key.len());
        id
    }

    /// K4 (P0 fix): charge the bookkeeping cost of a genuinely NEW document
    /// entering `key_hash_to_doc_id` + `doc_id_to_key`. Callers gate this on
    /// "doc_id was just newly assigned" (never on an upsert of an existing
    /// key, since `HashMap::insert` on the same key is a same-size
    /// overwrite) so the charge fires exactly once per doc_id -- matching
    /// `remove_doc_by_doc_id`'s single unconditional uncharge, which reads
    /// the exact removed key length back from `doc_id_to_key.remove()`.
    fn charge_new_doc_key(&mut self, key_len: usize) {
        self.resident_bytes_extra += Self::doc_key_entry_cost(key_len);
    }

    /// Symmetric uncharge for [`Self::charge_new_doc_key`], called from
    /// `remove_doc_by_doc_id` with the exact key length of the removed
    /// entry. Sharing `doc_key_entry_cost` between charge and uncharge
    /// removes the risk of the two formulas drifting apart.
    fn uncharge_doc_key(&mut self, key_len: usize) {
        self.resident_bytes_extra = self
            .resident_bytes_extra
            .saturating_sub(Self::doc_key_entry_cost(key_len));
    }

    fn doc_key_entry_cost(key_len: usize) -> usize {
        std::mem::size_of::<u64>()
            + std::mem::size_of::<u32>()
            + MAP_ENTRY_OVERHEAD // key_hash_to_doc_id entry
            + std::mem::size_of::<u32>()
            + key_len
            + MAP_ENTRY_OVERHEAD // doc_id_to_key entry
    }

    /// Return `true` if a document is visible at the requested `as_of_lsn`
    /// snapshot. `as_of_lsn == 0` always returns `true` (no temporal filter,
    /// backwards-compatible with pre-v0.1.10 callers).
    ///
    /// Visibility rule (mirrors the vector-store MVCC filter):
    ///
    /// ```text
    /// insert_lsn <= as_of_lsn  AND  (delete_lsn == 0 OR delete_lsn > as_of_lsn)
    /// ```
    ///
    /// Documents with no recorded `insert_lsn` (empty map entry) are treated
    /// as pre-MVCC and always visible — prevents false-negatives on the 22+
    /// call sites in unit tests that construct docs via `index_document`
    /// without an LSN.
    #[inline]
    pub fn is_doc_visible_at(&self, doc_id: u32, as_of_lsn: u64) -> bool {
        if as_of_lsn == 0 {
            return true;
        }
        let insert_lsn = self.doc_id_to_insert_lsn.get(&doc_id).copied().unwrap_or(0);
        // Pre-MVCC docs (insert_lsn == 0) are always visible in historical snapshots.
        if insert_lsn != 0 && insert_lsn > as_of_lsn {
            return false;
        }
        let delete_lsn = self.doc_id_to_delete_lsn.get(&doc_id).copied().unwrap_or(0);
        delete_lsn == 0 || delete_lsn > as_of_lsn
    }

    /// Record the insertion LSN for a doc_id after `index_document` allocates it.
    /// Callers on the auto-index path (src/shard/spsc_handler.rs) pass the same
    /// monotonic LSN they allocate for the paired vector-field MVCC row so
    /// AS_OF queries stay consistent across vector and text streams.
    #[inline]
    pub fn set_doc_insert_lsn(&mut self, doc_id: u32, lsn: u64) {
        if lsn != 0 {
            let is_new = !self.doc_id_to_insert_lsn.contains_key(&doc_id);
            self.doc_id_to_insert_lsn.insert(doc_id, lsn);
            if is_new {
                self.resident_bytes_extra +=
                    std::mem::size_of::<u32>() + std::mem::size_of::<u64>() + MAP_ENTRY_OVERHEAD;
            }
        }
    }

    /// LSN-aware variant of [`Self::index_document`] — returns the assigned or
    /// reused doc_id AND records `insert_lsn` so AS_OF queries exclude the doc
    /// from pre-insert snapshots.
    ///
    /// `insert_lsn == 0` is a pre-MVCC fallback (e.g., non-HSET indexing paths
    /// or unit tests) and leaves the doc always-visible.
    pub fn index_document_with_lsn(
        &mut self,
        key_hash: u64,
        key: &[u8],
        args: &[crate::protocol::Frame],
        insert_lsn: u64,
    ) -> u32 {
        self.index_document(key_hash, key, args);
        let doc_id = *self
            .key_hash_to_doc_id
            .get(&key_hash)
            .expect("index_document populated key_hash_to_doc_id");
        self.set_doc_insert_lsn(doc_id, insert_lsn);
        doc_id
    }

    /// Index a document from HSET args.
    ///
    /// Handles upsert correctly: if the key_hash already exists, the old
    /// document's field lengths are subtracted from field_stats before
    /// re-indexing (prevents avgdl drift per Pitfall 2).
    ///
    /// # Arguments
    /// * `key_hash` - xxh64 hash of the Redis key
    /// * `key` - Raw Redis key bytes
    /// * `args` - HSET arguments: [field1, value1, field2, value2, ...]
    pub fn index_document(&mut self, key_hash: u64, key: &[u8], args: &[crate::protocol::Frame]) {
        let is_upsert = self.key_hash_to_doc_id.contains_key(&key_hash);
        let doc_id = if let Some(&existing_id) = self.key_hash_to_doc_id.get(&key_hash) {
            // Upsert: reuse existing doc_id
            // Remove old postings and adjust stats
            for field_idx in 0..self.text_fields.len() {
                if self.text_fields[field_idx].noindex {
                    continue;
                }
                // Remove old postings for this doc
                self.field_postings[field_idx].remove_doc(existing_id);
                // Subtract old field length from stats
                if let Some(old_lengths) = self.doc_field_lengths.get(&existing_id) {
                    if field_idx < old_lengths.len() {
                        let old_len = old_lengths[field_idx] as u64;
                        self.field_stats[field_idx].total_field_length = self.field_stats
                            [field_idx]
                            .total_field_length
                            .saturating_sub(old_len);
                    }
                }
            }
            existing_id
        } else {
            let id = self.next_doc_id;
            self.next_doc_id += 1;
            id
        };

        // Store key mapping. K4 (P0 fix): the bookkeeping charge fires only
        // on a genuinely new doc_id (`!is_upsert`) -- on upsert, both inserts
        // below overwrite the SAME key_hash/doc_id with byte-size-identical
        // content (same key, same doc_id), so charging again would double-count.
        self.key_hash_to_doc_id.insert(key_hash, doc_id);
        self.doc_id_to_key
            .insert(doc_id, Bytes::copy_from_slice(key));
        if !is_upsert {
            self.charge_new_doc_key(key.len());
        }

        // Initialize field lengths for this document
        let field_count = self.text_fields.len();
        let mut field_lengths = vec![0u32; field_count];

        // Index each TEXT field
        for field_idx in 0..field_count {
            if self.text_fields[field_idx].noindex {
                continue;
            }

            // Find field value in HSET args (pairwise: field_name, value, field_name, value, ...)
            let field_name = &self.text_fields[field_idx].field_name;
            let field_value = find_field_value(args, field_name);

            let Some(value_bytes) = field_value else {
                continue;
            };

            // Decode as UTF-8
            let Ok(text) = std::str::from_utf8(value_bytes) else {
                continue;
            };

            // Tokenize
            let tokens = self.field_analyzers[field_idx].tokenize_with_positions(text);
            let token_count = tokens.len() as u32;
            field_lengths[field_idx] = token_count;

            // Index each token
            for (term, position) in &tokens {
                let term_id = self.field_term_dicts[field_idx].get_or_insert(term);
                self.field_postings[field_idx].add_term_occurrence(
                    term_id,
                    doc_id,
                    Some(vec![*position]),
                );
            }

            // Update field stats
            if !is_upsert {
                self.field_stats[field_idx].num_docs += 1;
            }
            self.field_stats[field_idx].total_field_length += token_count as u64;
        }

        // K4 (P0 fix): charge only on a genuinely new doc -- on upsert this
        // `insert` replaces an existing entry with a Vec of the SAME length
        // (`field_count`, constant for this index), a byte-size-identical
        // overwrite that must not be re-charged.
        if !is_upsert {
            self.resident_bytes_extra += std::mem::size_of::<u32>()
                + field_count * std::mem::size_of::<u32>()
                + MAP_ENTRY_OVERHEAD;
        }
        self.doc_field_lengths.insert(doc_id, field_lengths);
    }

    /// Search a specific field for query terms with BM25 scoring.
    ///
    /// Uses RoaringBitmap AND intersection for implicit AND semantics (per D-02):
    /// all query terms must appear in a document for it to be a candidate.
    ///
    /// `global_df` and `global_n` override local posting list stats when provided.
    /// These are injected by the DFS pre-pass coordinator for multi-shard global IDF
    /// accuracy (per D-04). When `None`, local field statistics are used (single-shard path).
    ///
    /// Returns results sorted descending by BM25 score, truncated to `top_k`.
    pub fn search_field(
        &self,
        field_idx: usize,
        query_terms: &[String],
        global_df: Option<&HashMap<String, u32>>,
        global_n: Option<u32>,
        top_k: usize,
    ) -> Vec<TextSearchResult> {
        if field_idx >= self.field_postings.len() || query_terms.is_empty() {
            return Vec::new();
        }

        // Step 1: build candidate bitmap via RoaringBitmap AND intersection.
        // Per RESEARCH Pitfall 1: any absent term means no results (AND semantics).
        use roaring::RoaringBitmap;

        // Collect postings for each query term; early-exit if any term is missing.
        let mut term_postings: Vec<(String, u32)> = Vec::with_capacity(query_terms.len());
        for term in query_terms {
            let term_id = match self.field_term_dicts[field_idx].get(term) {
                Some(id) => id,
                None => return Vec::new(), // AND: missing term = no results
            };
            // Verify posting list exists
            if self.field_postings[field_idx]
                .get_posting(term_id)
                .is_none()
            {
                return Vec::new();
            }
            term_postings.push((term.clone(), term_id));
        }

        // Build candidate bitmap: start from first term's doc_ids, AND with rest.
        let mut candidate_bitmap: RoaringBitmap = {
            // Defensive (no expect/panic): term_postings is non-empty and each posting was just
            // verified present, but a missing posting here would mean the AND term has no docs ⇒
            // no results. Never panic on the BM25 hot path.
            let Some(first_posting) =
                self.field_postings[field_idx].get_posting(term_postings[0].1)
            else {
                return Vec::new();
            };
            first_posting.doc_ids.clone()
        };

        for (_, term_id) in &term_postings[1..] {
            // Defensive: an absent AND-term posting ⇒ empty intersection ⇒ no results.
            let Some(posting) = self.field_postings[field_idx].get_posting(*term_id) else {
                return Vec::new();
            };
            candidate_bitmap &= &posting.doc_ids;
        }

        if candidate_bitmap.is_empty() {
            return Vec::new();
        }

        // Step 2: score each surviving candidate document with BM25.
        let stats = &self.field_stats[field_idx];
        let n = global_n.unwrap_or(stats.num_docs);
        let avgdl = stats.avg_doc_len();
        let k1 = self.bm25_config.k1;
        let b = self.bm25_config.b;
        let weight = self.text_fields[field_idx].weight as f32;

        let mut results: Vec<TextSearchResult> =
            Vec::with_capacity(candidate_bitmap.len() as usize);

        for doc_id in &candidate_bitmap {
            let dl = self
                .doc_field_lengths
                .get(&doc_id)
                .and_then(|lens| lens.get(field_idx).copied())
                .unwrap_or(0);

            let mut doc_score = 0.0f32;
            for (term, term_id) in &term_postings {
                // Defensive: skip a term whose posting vanished rather than panic; its BM25
                // contribution is simply omitted (the doc already matched the AND candidate set).
                let Some(posting) = self.field_postings[field_idx].get_posting(*term_id) else {
                    continue;
                };

                // Rank-aligned TF lookup (fts-posting-rank-tf): sub-linear and correct after
                // document updates. term_freqs is now kept in sorted-doc_id (rank) order, so the
                // old linear scan is gone — see PostingList::tf. (Supersedes the former
                // "RESEARCH Pitfall 1" note, which assumed insertion-order term_freqs.)
                let tf = posting.tf(doc_id) as f32;

                // Use global_df if provided (DFS path), else local doc frequency.
                let df = global_df
                    .and_then(|m| m.get(term.as_str()).copied())
                    .unwrap_or_else(|| posting.doc_ids.len() as u32);

                doc_score += bm25_score(tf, df, n, dl, avgdl, k1, b) * weight;
            }

            // Resolve original Redis key for this document.
            let key = match self.doc_id_to_key.get(&doc_id) {
                Some(k) => k.clone(),
                None => continue, // orphaned doc_id — skip
            };

            results.push(TextSearchResult {
                doc_id,
                key,
                score: doc_score,
            });
        }

        // Step 3: sort descending by BM25 score (higher = more relevant per D-07).
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }

    /// Collect document frequency for each term + total N for the DFS pre-pass.
    ///
    /// Returns `(Vec<(term, df)>, N)` where:
    /// - `df` is the number of documents in this shard containing the term
    /// - `N` is `field_stats[field_idx].num_docs` (total docs with this field on this shard)
    ///
    /// Used by the DFS Phase 1 scatter to aggregate global IDF weights before
    /// executing the actual search in Phase 2 (per D-04).
    pub fn doc_freq_for_terms(
        &self,
        field_idx: usize,
        terms: &[String],
    ) -> (Vec<(String, u32)>, u32) {
        if field_idx >= self.field_postings.len() {
            return (Vec::new(), 0);
        }

        let n = self.field_stats[field_idx].num_docs;
        let mut result = Vec::with_capacity(terms.len());

        for term in terms {
            let df = match self.field_term_dicts[field_idx].get(term) {
                Some(term_id) => self.field_postings[field_idx].doc_freq(term_id),
                None => 0,
            };
            result.push((term.clone(), df));
        }

        (result, n)
    }

    /// Build FST maps for all fields from current TermDictionary contents.
    ///
    /// Called at FT.COMPACT time. Replaces any existing FST maps atomically (D-14).
    /// After build, updates `fst_high_water_mark` so post-compaction terms can be
    /// identified for dual-path expansion (D-12).
    ///
    /// Build failures are logged as warnings but do not abort — FST is an
    /// acceleration structure; its absence only affects fuzzy/prefix queries.
    #[cfg(feature = "text-index")]
    pub fn build_fst(&mut self) {
        for field_idx in 0..self.field_term_dicts.len() {
            match crate::text::fst_dict::build_fst_from_term_dict(&self.field_term_dicts[field_idx])
            {
                Ok(bytes) => match fst::Map::new(bytes) {
                    Ok(map) => {
                        self.set_fst_map(field_idx, Some(map));
                        // Update high water mark: terms with id >= this were added post-compaction.
                        self.field_term_dicts[field_idx].fst_high_water_mark =
                            self.field_term_dicts[field_idx].next_id();
                    }
                    Err(e) => tracing::warn!("FST load failed for field {field_idx}: {e}"),
                },
                Err(e) => tracing::warn!("FST build failed for field {field_idx}: {e}"),
            }
        }
    }

    /// K4 (P0 fix): replace `fst_maps[field_idx]` and re-sync its
    /// `resident_bytes_extra` contribution in one step. This is a "re-sync
    /// at structural event" (not a periodic walk): `fst::Map::as_fst().size()`
    /// is an O(1) read of the underlying byte-slice length, and this only
    /// runs at FST (re)build time (`build_fst`, `load_fst_sidecars`) --
    /// events that are already O(vocabulary) themselves, so folding in an
    /// O(1)-per-field recount adds no asymptotic cost.
    #[cfg(feature = "text-index")]
    fn set_fst_map(&mut self, field_idx: usize, map: Option<fst::Map<Vec<u8>>>) {
        if let Some(old) = &self.fst_maps[field_idx] {
            self.resident_bytes_extra = self
                .resident_bytes_extra
                .saturating_sub(old.as_fst().size());
        }
        if let Some(new_map) = &map {
            self.resident_bytes_extra += new_map.as_fst().size();
        }
        self.fst_maps[field_idx] = map;
    }

    /// Expand a single query term into matching term IDs via FST + HashMap fallback.
    ///
    /// Exact terms: direct TermDictionary lookup (unchanged path).
    /// Fuzzy/Prefix: FST expansion + post-compaction HashMap scan (D-12).
    /// Returns empty Vec if no FST and term is Fuzzy/Prefix (D-13: not an error).
    #[cfg(feature = "text-index")]
    pub fn expand_terms(&self, field_idx: usize, text: &str, modifier: &TermModifier) -> Vec<u32> {
        const MAX_EXPANDED: usize = 50; // D-09

        match modifier {
            TermModifier::Exact => self.field_term_dicts[field_idx]
                .get(text)
                .map(|id| vec![id])
                .unwrap_or_default(),
            TermModifier::Fuzzy(dist) => {
                let hwm = self.field_term_dicts[field_idx].fst_high_water_mark;
                match &self.fst_maps[field_idx] {
                    Some(fst_map) => {
                        let mut ids = crate::text::fst_dict::expand_fuzzy(
                            fst_map,
                            text,
                            *dist,
                            &self.field_postings[field_idx],
                            MAX_EXPANDED,
                        );
                        // D-12 dual-path: also scan post-compaction HashMap terms.
                        let mut extra = crate::text::fst_dict::expand_fuzzy_hashmap(
                            &self.field_term_dicts[field_idx],
                            text,
                            *dist,
                            &self.field_postings[field_idx],
                            hwm,
                            MAX_EXPANDED,
                        );
                        ids.append(&mut extra);
                        // Deduplicate and re-cap.
                        ids.sort_unstable();
                        ids.dedup();
                        if ids.len() > MAX_EXPANDED {
                            let postings = &self.field_postings[field_idx];
                            ids.sort_unstable_by(|a, b| {
                                postings.doc_freq(*b).cmp(&postings.doc_freq(*a))
                            });
                            ids.truncate(MAX_EXPANDED);
                        }
                        ids
                    }
                    None => {
                        // No FST: brute-force scan entire HashMap (no compaction happened yet).
                        crate::text::fst_dict::expand_fuzzy_hashmap(
                            &self.field_term_dicts[field_idx],
                            text,
                            *dist,
                            &self.field_postings[field_idx],
                            0,
                            MAX_EXPANDED,
                        )
                    }
                }
            }
            TermModifier::Prefix => {
                let hwm = self.field_term_dicts[field_idx].fst_high_water_mark;
                match &self.fst_maps[field_idx] {
                    Some(fst_map) => {
                        let mut ids = crate::text::fst_dict::expand_prefix(
                            fst_map,
                            text,
                            &self.field_postings[field_idx],
                            MAX_EXPANDED,
                        );
                        let mut extra = crate::text::fst_dict::expand_prefix_hashmap(
                            &self.field_term_dicts[field_idx],
                            text,
                            &self.field_postings[field_idx],
                            hwm,
                            MAX_EXPANDED,
                        );
                        ids.append(&mut extra);
                        ids.sort_unstable();
                        ids.dedup();
                        if ids.len() > MAX_EXPANDED {
                            let postings = &self.field_postings[field_idx];
                            ids.sort_unstable_by(|a, b| {
                                postings.doc_freq(*b).cmp(&postings.doc_freq(*a))
                            });
                            ids.truncate(MAX_EXPANDED);
                        }
                        ids
                    }
                    None => {
                        // No FST: brute-force scan entire HashMap.
                        crate::text::fst_dict::expand_prefix_hashmap(
                            &self.field_term_dicts[field_idx],
                            text,
                            &self.field_postings[field_idx],
                            0,
                            MAX_EXPANDED,
                        )
                    }
                }
            }
        }
    }

    /// Search a field with OR union of expanded term IDs (fuzzy/prefix queries).
    ///
    /// Returns the union of docs matching ANY of the expanded_term_ids.
    /// Each doc is scored by the BEST-matching expanded term's BM25 (not sum, per D-05).
    ///
    /// This is the OR counterpart to `search_field()` which uses AND intersection.
    /// Called for fuzzy/prefix queries after `expand_terms()` produces expanded_term_ids.
    #[cfg(feature = "text-index")]
    pub fn search_field_or(
        &self,
        field_idx: usize,
        expanded_term_ids: &[u32],
        global_df: Option<&HashMap<String, u32>>,
        global_n: Option<u32>,
        top_k: usize,
    ) -> Vec<TextSearchResult> {
        use roaring::RoaringBitmap;

        if field_idx >= self.field_postings.len() || expanded_term_ids.is_empty() {
            return Vec::new();
        }

        // OR: union all posting list bitmaps (any expanded term match counts, D-05).
        let mut candidate_bitmap = RoaringBitmap::new();
        for &term_id in expanded_term_ids {
            if let Some(posting) = self.field_postings[field_idx].get_posting(term_id) {
                candidate_bitmap |= &posting.doc_ids;
            }
        }
        if candidate_bitmap.is_empty() {
            return Vec::new();
        }

        // Score each candidate: MAX BM25 across all matching expanded terms (D-05: best, not sum).
        let stats = &self.field_stats[field_idx];
        let n = global_n.unwrap_or(stats.num_docs);
        let avgdl = stats.avg_doc_len();
        let k1 = self.bm25_config.k1;
        let b = self.bm25_config.b;
        let weight = self.text_fields[field_idx].weight as f32;

        let mut results: Vec<TextSearchResult> =
            Vec::with_capacity(candidate_bitmap.len() as usize);

        // global_df maps term strings -> df, but we have term_ids here (OR-union path).
        // For fuzzy/prefix expansion, always use local posting list doc_freq.
        // The global_df parameter is accepted for API symmetry with search_field() but unused.
        let _ = global_df;

        for doc_id in &candidate_bitmap {
            let dl = self
                .doc_field_lengths
                .get(&doc_id)
                .and_then(|lens| lens.get(field_idx).copied())
                .unwrap_or(0);

            let mut best_score = 0.0f32;
            for &term_id in expanded_term_ids {
                let Some(posting) = self.field_postings[field_idx].get_posting(term_id) else {
                    continue;
                };
                if !posting.doc_ids.contains(doc_id) {
                    continue;
                }

                // Rank-aligned TF lookup (fts-posting-rank-tf) — same as search_field.
                let tf = posting.tf(doc_id) as f32;

                // Use local posting list df for expanded term IDs.
                let df = posting.doc_ids.len() as u32;

                let score = bm25_score(tf, df, n, dl, avgdl, k1, b) * weight;
                if score > best_score {
                    best_score = score;
                }
            }

            let key = match self.doc_id_to_key.get(&doc_id) {
                Some(k) => k.clone(),
                None => continue, // orphaned doc_id — skip
            };

            results.push(TextSearchResult {
                doc_id,
                key,
                score: best_score,
            });
        }

        // Sort descending by BM25 score, truncate to top_k.
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(top_k);
        results
    }

    // ── TAG indexing (Plan 152-06) ─────────────────────────────────────────

    /// Index TAG fields from an HSET payload.
    ///
    /// Per-field upsert semantics (Blocker 4): only fields present in `args`
    /// have their prior entries revoked before re-inserting. Fields absent
    /// from the HSET payload preserve their previous tag assignments — this
    /// is what makes `HSET doc:1 priority low` not clobber a prior
    /// `HSET doc:1 status open priority high`.
    ///
    /// Safety caps:
    /// - `TAG_VALUE_MAX_LEN = 4096` bytes per HSET value (rejected with warn).
    /// - `TAG_VALUES_PER_FIELD_PER_DOC = 1024` distinct values per field per doc.
    ///
    /// Allocation profile: write-path, not dispatch hot-path. One
    /// `Bytes::copy_from_slice` per touched TAG field (bounded by
    /// TAG_VALUE_MAX_LEN); ASCII-lowercase fast-path avoids a second copy
    /// when the value is already lowercase.
    #[cfg(feature = "text-index")]
    pub fn tag_index_document(
        &mut self,
        key_hash: u64,
        key: &[u8],
        args: &[crate::protocol::Frame],
    ) {
        if self.tag_fields.is_empty() {
            return;
        }

        const TAG_VALUE_MAX_LEN: usize = 4096;
        const TAG_VALUES_PER_FIELD_PER_DOC: usize = 1_024;

        let doc_id = self.ensure_doc_id(key_hash, key);

        // Determine which declared TAG fields the HSET payload touches.
        let mut touched: smallvec::SmallVec<[Bytes; 8]> = smallvec::SmallVec::new();
        for tag_def in &self.tag_fields {
            if tag_def.noindex {
                continue;
            }
            if find_field_value(args, &tag_def.field_name).is_some() {
                touched.push(tag_def.field_name.clone()); // Arc bump, not deep copy
            }
        }

        // Rebuild `doc_tag_entries[doc_id]`: keep untouched-field entries, drop touched-field entries.
        // K4 (P0 fix): uncharge the prior entry's cost ONLY when an entry
        // actually existed (`doc_tag_entries` never stores an empty Vec --
        // see the `!next.is_empty()` guard below -- so `Some(_)` always
        // means real, previously-charged content).
        let prior_opt = self.doc_tag_entries.remove(&doc_id);
        if let Some(prior_entries) = &prior_opt {
            self.resident_bytes_extra = self
                .resident_bytes_extra
                .saturating_sub(Self::tag_entries_cost(prior_entries));
        }
        let prior = prior_opt.unwrap_or_default();
        let mut next: smallvec::SmallVec<[(Bytes, Bytes); 8]> = smallvec::SmallVec::new();
        for (field, value) in prior.into_iter() {
            let is_touched = touched.iter().any(|f| f == &field);
            if is_touched {
                self.tag_bitmap_revoke(&field, &value, doc_id);
            } else {
                next.push((field, value));
            }
        }

        // Insert fresh entries for each touched field.
        // K4 (P0 fix): clone the per-field def out of `self.tag_fields` up
        // front so the loop body is free to call `&mut self` accounting
        // helpers (`tag_bitmap_insert`) -- `for tag_def in &self.tag_fields`
        // would hold an immutable borrow of `self` alive for the whole loop.
        for i in 0..self.tag_fields.len() {
            let tag_def = self.tag_fields[i].clone();
            if tag_def.noindex {
                continue;
            }
            let Some(value_bytes_slice) = find_field_value(args, &tag_def.field_name) else {
                continue;
            };
            if value_bytes_slice.len() > TAG_VALUE_MAX_LEN {
                tracing::warn!(
                    field = ?tag_def.field_name,
                    len = value_bytes_slice.len(),
                    "TAG value exceeds 4 KiB — skipped"
                );
                continue;
            }

            // Bounded write-path allocation: one Bytes::copy_from_slice per touched
            // tag field. `Frame::BulkString` stores Bytes but `find_field_value`
            // yields `&[u8]` (cross-cutting refactor out of scope for gap closure).
            let value_bytes: Bytes = Bytes::copy_from_slice(value_bytes_slice);

            let mut seen: smallvec::SmallVec<[Bytes; 16]> = smallvec::SmallVec::new();
            let mut truncated = false;
            let mut cursor = 0usize;
            while cursor <= value_bytes.len() {
                let end = value_bytes[cursor..]
                    .iter()
                    .position(|b| *b == tag_def.separator)
                    .map(|p| cursor + p)
                    .unwrap_or(value_bytes.len());
                let chunk_len = end.saturating_sub(cursor);
                if chunk_len > 0 {
                    let normalized = normalize_tag_value(
                        &value_bytes,
                        cursor,
                        chunk_len,
                        tag_def.case_sensitive,
                    );
                    if !seen.iter().any(|s| s == &normalized) {
                        if seen.len() < TAG_VALUES_PER_FIELD_PER_DOC {
                            seen.push(normalized);
                        } else {
                            truncated = true;
                            break;
                        }
                    }
                }
                if end == value_bytes.len() {
                    break;
                }
                cursor = end + 1;
            }
            if truncated {
                tracing::warn!(
                    field = ?tag_def.field_name,
                    limit = TAG_VALUES_PER_FIELD_PER_DOC,
                    "TAG values truncated"
                );
            }

            let canonical_field = tag_def.field_name.clone(); // Arc bump
            for value in seen.into_iter() {
                self.tag_bitmap_insert(&canonical_field, &value, doc_id);
                next.push((canonical_field.clone(), value));
            }
        }

        if !next.is_empty() {
            self.resident_bytes_extra += Self::tag_entries_cost(&next);
            self.doc_tag_entries.insert(doc_id, next);
        }
    }

    /// K4 (P0 fix): insert `doc_id` into `tag_indexes[field][value]`,
    /// charging the O(1) fixed-cost delta for any newly-created field/value/
    /// doc-bit. Shared by `tag_index_document`'s insert loop.
    #[cfg(feature = "text-index")]
    fn tag_bitmap_insert(&mut self, field: &Bytes, value: &Bytes, doc_id: u32) {
        let field_is_new = !self.tag_indexes.contains_key(field);
        let field_map = self.tag_indexes.entry(field.clone()).or_default();
        let value_is_new = !field_map.contains_key(value);
        let bm = field_map.entry(value.clone()).or_default();
        let doc_is_new = !bm.contains(doc_id);
        bm.insert(doc_id);
        if doc_is_new {
            self.resident_bytes_extra += ROARING_BIT_APPROX_COST;
        }
        if value_is_new {
            self.resident_bytes_extra += value.len() + MAP_ENTRY_OVERHEAD + EMPTY_BITMAP_BASE_COST;
        }
        if field_is_new {
            self.resident_bytes_extra += field.len() + MAP_ENTRY_OVERHEAD;
        }
    }

    /// K4 (P0 fix): revoke `doc_id` from `tag_indexes[field][value]`,
    /// uncharging the O(1) fixed-cost delta symmetrically with
    /// `tag_bitmap_insert`. The outer per-field entry is never uncharged --
    /// it is never removed from `tag_indexes` either (matches
    /// `PostingStore`'s "entry survives empty" contract). Shared by both
    /// `tag_index_document`'s revoke loop and `remove_doc_by_doc_id`.
    #[cfg(feature = "text-index")]
    fn tag_bitmap_revoke(&mut self, field: &Bytes, value: &Bytes, doc_id: u32) {
        if let Some(field_map) = self.tag_indexes.get_mut(field) {
            if let Some(bm) = field_map.get_mut(value) {
                let was_present = bm.contains(doc_id);
                bm.remove(doc_id);
                if was_present {
                    self.resident_bytes_extra = self
                        .resident_bytes_extra
                        .saturating_sub(ROARING_BIT_APPROX_COST);
                }
                if bm.is_empty() {
                    field_map.remove(value);
                    self.resident_bytes_extra = self
                        .resident_bytes_extra
                        .saturating_sub(value.len() + MAP_ENTRY_OVERHEAD + EMPTY_BITMAP_BASE_COST);
                }
            }
        }
    }

    /// K4 (P0 fix): fixed-cost approximation of one `doc_tag_entries[doc_id]`
    /// entry (mirrors the removed inline formula from the old
    /// `resident_bytes()` walk). Pure function of the entries slice so it
    /// can be called both before insert (to charge) and after remove (to
    /// uncharge) without borrowing `self`.
    #[cfg(feature = "text-index")]
    fn tag_entries_cost(entries: &[(Bytes, Bytes)]) -> usize {
        std::mem::size_of::<u32>()
            + MAP_ENTRY_OVERHEAD
            + entries
                .iter()
                .map(|(a, b)| a.len() + b.len() + 32)
                .sum::<usize>()
    }

    /// LSN-aware wrapper around [`Self::search_field`] — post-filters the
    /// scored result list by MVCC visibility at `as_of_lsn`.
    ///
    /// Backwards-compatible: `as_of_lsn == 0` is a no-op passthrough and
    /// produces identical output to `search_field`.
    ///
    /// Implementation note: post-filter (rather than pre-filter the candidate
    /// bitmap) is a deliberate trade-off — it wastes BM25 scoring work on
    /// invisible docs in exchange for zero risk of breaking the existing
    /// scoring path. To avoid recall loss when visible docs rank behind many
    /// invisible docs, we oversample up to `next_doc_id` (full index) when
    /// AS_OF is active. For the no-filter path we only oversample by 2×
    /// because there is no filter-driven recall loss.
    pub fn search_field_as_of(
        &self,
        field_idx: usize,
        query_terms: &[String],
        global_df: Option<&HashMap<String, u32>>,
        global_n: Option<u32>,
        top_k: usize,
        as_of_lsn: u64,
    ) -> Vec<TextSearchResult> {
        if as_of_lsn == 0 {
            return self.search_field(field_idx, query_terms, global_df, global_n, top_k);
        }
        // Unbounded oversample (bounded by index size) — the adversarial test
        // `g1_as_of_top_k_oversample_rescues_low_ranked_visible_doc` proved
        // 2× oversample is insufficient when a visible doc ranks last. We
        // ask search_field for up to `next_doc_id` results (every doc in the
        // index) so no visible candidate is truncated before filtering. BM25
        // still short-circuits on empty postings, so the cost is bounded by
        // candidate_bitmap size, not index size.
        let oversample = (self.next_doc_id as usize).max(top_k).max(16);
        let raw = self.search_field(field_idx, query_terms, global_df, global_n, oversample);
        let mut filtered: Vec<TextSearchResult> = raw
            .into_iter()
            .filter(|r| self.is_doc_visible_at(r.doc_id, as_of_lsn))
            .collect();
        filtered.truncate(top_k);
        filtered
    }

    /// LSN-aware counterpart to [`Self::search_field_or`] (fuzzy/prefix OR path).
    /// See [`Self::search_field_as_of`] for the filter contract.
    #[cfg(feature = "text-index")]
    pub fn search_field_or_as_of(
        &self,
        field_idx: usize,
        expanded_term_ids: &[u32],
        global_df: Option<&HashMap<String, u32>>,
        global_n: Option<u32>,
        top_k: usize,
        as_of_lsn: u64,
    ) -> Vec<TextSearchResult> {
        if as_of_lsn == 0 {
            return self.search_field_or(field_idx, expanded_term_ids, global_df, global_n, top_k);
        }
        let oversample = (self.next_doc_id as usize).max(top_k).max(16);
        let raw = self.search_field_or(
            field_idx,
            expanded_term_ids,
            global_df,
            global_n,
            oversample,
        );
        let mut filtered: Vec<TextSearchResult> = raw
            .into_iter()
            .filter(|r| self.is_doc_visible_at(r.doc_id, as_of_lsn))
            .collect();
        filtered.truncate(top_k);
        filtered
    }

    /// Look up documents tagged with a specific value on a specific field.
    ///
    /// Returns doc_ids in ascending order. Field resolution is
    /// case-insensitive: `@Status:{open}` on an index declaring `status`
    /// resolves correctly (Blocker 2). The value is normalized using the
    /// same rules used on insert (ASCII-lowercase unless CASESENSITIVE).
    #[cfg(feature = "text-index")]
    pub fn search_tag(&self, field: &Bytes, value: &Bytes) -> Vec<u32> {
        let (canonical_field, case_sensitive) = match self
            .tag_fields
            .iter()
            .find(|f| f.field_name.eq_ignore_ascii_case(field.as_ref()))
        {
            Some(f) => (f.field_name.clone(), f.case_sensitive),
            None => return Vec::new(),
        };

        let normalized_value: Bytes = if case_sensitive {
            value.clone()
        } else if value.iter().all(|b| !b.is_ascii_uppercase()) {
            value.clone()
        } else {
            let mut v = Vec::with_capacity(value.len());
            for b in value.iter() {
                v.push(b.to_ascii_lowercase());
            }
            Bytes::from(v)
        };

        match self
            .tag_indexes
            .get(&canonical_field)
            .and_then(|m| m.get(&normalized_value))
        {
            Some(bm) => bm.iter().collect(),
            None => Vec::new(),
        }
    }

    // ── NUMERIC indexing (Plan 152-07) ─────────────────────────────────────

    /// Index NUMERIC fields from an HSET payload.
    ///
    /// Per-field upsert semantics (mirrors `tag_index_document`): only fields
    /// present in `args` have their prior entries revoked before re-inserting.
    /// Fields absent from the HSET payload preserve their previous numeric
    /// assignments.
    ///
    /// Write-path guards (T-152-07-02):
    /// - Non-UTF8 value → skipped silently + `tracing::debug!`.
    /// - Non-numeric value → skipped silently + `tracing::debug!` (RediSearch-compatible).
    /// - NaN / ±Infinity → skipped silently + `tracing::debug!`. These would
    ///   corrupt BTreeMap ordering (NaN != NaN) or bloat range queries. Rust's
    ///   f64 parser accepts "NaN" / "Infinity" literals — the post-parse
    ///   `is_nan() || is_infinite()` guard is load-bearing.
    ///
    /// Safety cap (T-152-07-01):
    /// - `NUMERIC_CARDINALITY_LIMIT = 10_000_000` distinct values per field.
    ///   Reached = `tracing::warn!` rate-limited, new distinct values dropped.
    ///
    /// Allocation profile: write-path, not dispatch hot-path. One
    /// `Bytes::clone` (Arc bump) per touched NUMERIC field for canonical field
    /// name tracking; one `OrderedFloat<f64>` copy (16 B) per value. No heap
    /// allocation from `find_field_value` — it returns a borrowed `&[u8]`.
    #[cfg(feature = "text-index")]
    pub fn numeric_index_document(
        &mut self,
        key_hash: u64,
        key: &[u8],
        args: &[crate::protocol::Frame],
    ) {
        if self.numeric_fields.is_empty() {
            return;
        }

        const NUMERIC_CARDINALITY_LIMIT: usize = 10_000_000;

        let doc_id = self.ensure_doc_id(key_hash, key);

        // Determine which declared NUMERIC fields the HSET payload touches.
        let mut touched: smallvec::SmallVec<[Bytes; 4]> = smallvec::SmallVec::new();
        for num_def in &self.numeric_fields {
            if num_def.noindex {
                continue;
            }
            if find_field_value(args, &num_def.field_name).is_some() {
                touched.push(num_def.field_name.clone());
            }
        }

        // Rebuild `doc_numeric_entries[doc_id]`: keep untouched-field entries, drop touched-field entries.
        // K4 (P0 fix): uncharge the prior entry's cost ONLY when an entry
        // actually existed (mirrors the TAG-side reasoning in
        // `tag_index_document` -- `doc_numeric_entries` never stores an
        // empty Vec, see the `!next.is_empty()` guard below).
        let prior_opt = self.doc_numeric_entries.remove(&doc_id);
        if let Some(prior_entries) = &prior_opt {
            self.resident_bytes_extra = self
                .resident_bytes_extra
                .saturating_sub(Self::numeric_entries_cost(prior_entries));
        }
        let prior = prior_opt.unwrap_or_default();
        let mut next: smallvec::SmallVec<[(Bytes, ordered_float::OrderedFloat<f64>); 4]> =
            smallvec::SmallVec::new();
        for (field, value) in prior.into_iter() {
            let is_touched = touched.iter().any(|f| f == &field);
            if is_touched {
                self.numeric_bitmap_revoke(&field, &value, doc_id);
            } else {
                next.push((field, value));
            }
        }

        // Insert fresh entries for each touched field.
        for num_def in &self.numeric_fields {
            if num_def.noindex {
                continue;
            }
            let Some(value_bytes) = find_field_value(args, &num_def.field_name) else {
                continue;
            };
            let value_str = match std::str::from_utf8(value_bytes) {
                Ok(s) => s,
                Err(_) => {
                    tracing::debug!(
                        field = ?num_def.field_name,
                        "non-UTF8 numeric value skipped"
                    );
                    continue;
                }
            };
            let parsed: f64 = match value_str.parse() {
                Ok(v) => v,
                Err(_) => {
                    tracing::debug!(
                        field = ?num_def.field_name,
                        raw = ?value_str,
                        "non-numeric value skipped"
                    );
                    continue;
                }
            };
            // T-152-07-02: NaN / ±Infinity guard.
            if parsed.is_nan() || parsed.is_infinite() {
                tracing::debug!(
                    field = ?num_def.field_name,
                    raw = ?value_str,
                    "NaN/Inf numeric value skipped"
                );
                continue;
            }
            let of = ordered_float::OrderedFloat(parsed);
            let canonical_field = num_def.field_name.clone();
            let field_is_new = !self.numeric_indexes.contains_key(&canonical_field);
            let btree = self
                .numeric_indexes
                .entry(canonical_field.clone())
                .or_default();
            // T-152-07-01: cardinality cap.
            if !btree.contains_key(&of) && btree.len() >= NUMERIC_CARDINALITY_LIMIT {
                tracing::warn!(
                    field = ?canonical_field,
                    "numeric cardinality cap reached; dropping new value"
                );
                continue;
            }
            // K4 (P0 fix): charge the O(1) fixed-cost delta for any
            // newly-created field/value/doc-bit -- checked AFTER the
            // cardinality cap so a dropped value is never charged.
            let value_is_new = !btree.contains_key(&of);
            let bm = btree.entry(of).or_default();
            let doc_is_new = !bm.contains(doc_id);
            bm.insert(doc_id);
            if doc_is_new {
                self.resident_bytes_extra += ROARING_BIT_APPROX_COST;
            }
            if value_is_new {
                self.resident_bytes_extra +=
                    std::mem::size_of::<f64>() + MAP_ENTRY_OVERHEAD + EMPTY_BITMAP_BASE_COST;
            }
            if field_is_new {
                self.resident_bytes_extra += canonical_field.len() + MAP_ENTRY_OVERHEAD;
            }
            next.push((canonical_field, of));
        }

        if !next.is_empty() {
            self.resident_bytes_extra += Self::numeric_entries_cost(&next);
            self.doc_numeric_entries.insert(doc_id, next);
        }
    }

    /// K4 (P0 fix): revoke `doc_id` from `numeric_indexes[field][value]`,
    /// uncharging the O(1) fixed-cost delta. Mirrors `tag_bitmap_revoke`;
    /// shared by `numeric_index_document`'s revoke loop and
    /// `remove_doc_by_doc_id`.
    #[cfg(feature = "text-index")]
    fn numeric_bitmap_revoke(
        &mut self,
        field: &Bytes,
        value: &ordered_float::OrderedFloat<f64>,
        doc_id: u32,
    ) {
        if let Some(btree) = self.numeric_indexes.get_mut(field) {
            if let Some(bm) = btree.get_mut(value) {
                let was_present = bm.contains(doc_id);
                bm.remove(doc_id);
                if was_present {
                    self.resident_bytes_extra = self
                        .resident_bytes_extra
                        .saturating_sub(ROARING_BIT_APPROX_COST);
                }
                if bm.is_empty() {
                    btree.remove(value);
                    self.resident_bytes_extra = self.resident_bytes_extra.saturating_sub(
                        std::mem::size_of::<f64>() + MAP_ENTRY_OVERHEAD + EMPTY_BITMAP_BASE_COST,
                    );
                }
            }
        }
    }

    /// K4 (P0 fix): fixed-cost approximation of one
    /// `doc_numeric_entries[doc_id]` entry. Mirrors `tag_entries_cost`.
    #[cfg(feature = "text-index")]
    fn numeric_entries_cost(entries: &[(Bytes, ordered_float::OrderedFloat<f64>)]) -> usize {
        std::mem::size_of::<u32>()
            + MAP_ENTRY_OVERHEAD
            + entries.iter().map(|(a, _)| a.len() + 16).sum::<usize>()
    }

    /// Resolve a NUMERIC range filter to sorted doc_ids.
    ///
    /// Uses `BTreeMap::range` — O(log N) seek + sequential bucket scan. The
    /// bound encoding matches RediSearch grammar:
    /// - `min_exclusive=false` (default) → `Included(min)`
    /// - `min_exclusive=true` (from `(min`) → `Excluded(min)`
    /// - `min = f64::NEG_INFINITY` → `Unbounded` (sentinel; `-inf` in query)
    /// - Symmetric for `max` / `max_exclusive` / `f64::INFINITY`.
    ///
    /// Field resolution is case-insensitive (`@Score:[1 10]` on an index
    /// declaring `score` resolves correctly).
    ///
    /// NaN bounds MUST be rejected at parse time (`parse_numeric_bound`); they
    /// never reach this function. Empty / unknown fields return empty Vec.
    #[cfg(feature = "text-index")]
    pub fn search_numeric_range(
        &self,
        field: &Bytes,
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
    ) -> Vec<u32> {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        // Case-insensitive field resolution (same discipline as search_tag).
        let canonical_field = match self
            .numeric_fields
            .iter()
            .find(|f| f.field_name.eq_ignore_ascii_case(field.as_ref()))
        {
            Some(f) => &f.field_name,
            None => return Vec::new(),
        };

        let Some(btree) = self.numeric_indexes.get(canonical_field) else {
            return Vec::new();
        };

        // `BTreeMap::range` PANICS by contract when start > end, or when start
        // == end with either bound excluded — and a panic on a shard thread
        // aborts the whole moon process (moon#664). Three of this function's
        // four callers validate their bounds first; `FT.HYBRID`'s
        // `FILTER NUMERIC <field> <min> <max>` checks only that each bound is
        // finite, never that they are ordered. Being total here catches that
        // caller and the next one: an impossible range has no members, which
        // is an empty result, not an abort.
        //
        // The comparison is in `OrderedFloat` space — the same total order the
        // BTreeMap itself uses — so it stays correct for the infinities and
        // cannot be fooled by a NaN that slipped past a parser.
        let lo_v = ordered_float::OrderedFloat(min);
        let hi_v = ordered_float::OrderedFloat(max);
        if lo_v > hi_v || (lo_v == hi_v && (min_exclusive || max_exclusive)) {
            return Vec::new();
        }

        let lo = if min == f64::NEG_INFINITY {
            Unbounded
        } else if min_exclusive {
            Excluded(ordered_float::OrderedFloat(min))
        } else {
            Included(ordered_float::OrderedFloat(min))
        };
        let hi = if max == f64::INFINITY {
            Unbounded
        } else if max_exclusive {
            Excluded(ordered_float::OrderedFloat(max))
        } else {
            Included(ordered_float::OrderedFloat(max))
        };

        let mut result = roaring::RoaringBitmap::new();
        for (_k, bm) in btree.range((lo, hi)) {
            result |= bm;
        }
        result.iter().collect()
    }

    /// Number of indexed documents.
    pub fn num_docs(&self) -> u32 {
        self.key_hash_to_doc_id.len() as u32
    }

    /// Total unique terms across all fields.
    pub fn num_terms(&self) -> usize {
        self.field_term_dicts.iter().map(|d| d.term_count()).sum()
    }

    /// Estimated total posting list memory in bytes.
    pub fn total_posting_bytes(&self) -> usize {
        self.field_postings
            .iter()
            .map(|p| p.estimated_bytes())
            .sum()
    }

    /// Approximate total resident bytes owned by this index: posting lists,
    /// term dictionaries (BM25 path), FST sidecars (fuzzy/prefix expansion),
    /// per-document bookkeeping maps (field lengths, key<->doc_id, MVCC
    /// LSNs), and TAG/NUMERIC secondary indexes.
    ///
    /// K4 accounting spine (kernel-m2-brief-2026-07-12 stage 2): the sole
    /// contributor to `TextStore::resident_bytes()`, which is folded into
    /// the shard's `store_memory.text` atomic (elastic memory budget
    /// used-term + MEMORY DOCTOR / Prometheus surfacing) -- previously
    /// hard-coded 0.
    ///
    /// K4 (P0 fix): O(1) cached read -- `field_postings`/`field_term_dicts`
    /// each carry their own O(field_count) cached total (already O(1) per
    /// field), and everything else is folded into `resident_bytes_extra`,
    /// maintained incrementally at every mutation site. This used to be an
    /// O(doc-count + vocabulary) walk called unconditionally every 100ms
    /// from the shard eviction tick, which does not scale with corpus size.
    /// See `resident_bytes_ground_truth` (`#[cfg(test)]`) for the equivalent
    /// full-walk formula this cached value must always match.
    ///
    /// Excluded as negligible/bounded, not doc-scaling: `AnalyzerPipeline`
    /// (one stemmer + stop-word set per field, built once at FT.CREATE),
    /// `FieldStats` (two scalars per field), `BM25Config` / `text_fields` /
    /// `key_prefixes` / `name` (schema metadata, O(field count)). Every
    /// `HashMap`/`BTreeMap` entry cost below uses the same fixed-overhead
    /// approximation as `TermDictionary::resident_bytes` and
    /// `graph::index::PropertyIndex::resident_bytes`'s `serialized_size()`
    /// convention -- a monotonic signal for the budget, not exact RSS.
    #[must_use]
    pub fn resident_bytes(&self) -> usize {
        let postings: usize = self
            .field_postings
            .iter()
            .map(PostingStore::estimated_bytes)
            .sum();
        let term_dicts: usize = self
            .field_term_dicts
            .iter()
            .map(TermDictionary::resident_bytes)
            .sum();
        postings + term_dicts + self.resident_bytes_extra
    }

    /// Ground-truth full recompute of `resident_bytes()`, using the exact
    /// same fixed-cost formulas as the incremental accumulators
    /// (`PostingStore::estimated_bytes_ground_truth`,
    /// `TermDictionary::resident_bytes_ground_truth`, and the TAG/NUMERIC/
    /// FST/bookkeeping formulas inlined below). Test-only: exists solely to
    /// assert the incremental accumulators never drift from a from-scratch
    /// recount after a mixed mutation sequence. Its only caller (`mod
    /// tests` below) is itself gated on `feature = "text-index"`, so this
    /// must match or it's dead code under a `text-index`-off test build.
    #[cfg(all(test, feature = "text-index"))]
    pub(crate) fn resident_bytes_ground_truth(&self) -> usize {
        let postings: usize = self
            .field_postings
            .iter()
            .map(PostingStore::estimated_bytes_ground_truth)
            .sum();
        let term_dicts: usize = self
            .field_term_dicts
            .iter()
            .map(TermDictionary::resident_bytes_ground_truth)
            .sum();

        #[cfg(feature = "text-index")]
        let fst: usize = self
            .fst_maps
            .iter()
            .filter_map(|m| m.as_ref())
            .map(|m| m.as_fst().size())
            .sum();
        #[cfg(not(feature = "text-index"))]
        let fst: usize = 0;

        let doc_field_lengths: usize = self
            .doc_field_lengths
            .values()
            .map(|v| {
                std::mem::size_of::<u32>()
                    + v.len() * std::mem::size_of::<u32>()
                    + MAP_ENTRY_OVERHEAD
            })
            .sum();
        let key_hash_to_doc_id = self.key_hash_to_doc_id.len()
            * (std::mem::size_of::<u64>() + std::mem::size_of::<u32>() + MAP_ENTRY_OVERHEAD);
        let doc_id_to_key: usize = self
            .doc_id_to_key
            .values()
            .map(|k| std::mem::size_of::<u32>() + k.len() + MAP_ENTRY_OVERHEAD)
            .sum();
        let lsn_maps = (self.doc_id_to_insert_lsn.len() + self.doc_id_to_delete_lsn.len())
            * (std::mem::size_of::<u32>() + std::mem::size_of::<u64>() + MAP_ENTRY_OVERHEAD);

        #[cfg(feature = "text-index")]
        let tag: usize = self
            .tag_indexes
            .iter()
            .map(|(field, inner)| {
                field.len()
                    + MAP_ENTRY_OVERHEAD
                    + inner
                        .iter()
                        .map(|(v, bm)| {
                            v.len()
                                + MAP_ENTRY_OVERHEAD
                                + EMPTY_BITMAP_BASE_COST
                                + bm.len() as usize * ROARING_BIT_APPROX_COST
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
            + self
                .doc_tag_entries
                .values()
                .map(|entries| Self::tag_entries_cost(entries))
                .sum::<usize>();
        #[cfg(not(feature = "text-index"))]
        let tag: usize = 0;

        #[cfg(feature = "text-index")]
        let numeric: usize = self
            .numeric_indexes
            .iter()
            .map(|(field, tree)| {
                field.len()
                    + MAP_ENTRY_OVERHEAD
                    + tree
                        .values()
                        .map(|bm| {
                            std::mem::size_of::<f64>()
                                + MAP_ENTRY_OVERHEAD
                                + EMPTY_BITMAP_BASE_COST
                                + bm.len() as usize * ROARING_BIT_APPROX_COST
                        })
                        .sum::<usize>()
            })
            .sum::<usize>()
            + self
                .doc_numeric_entries
                .values()
                .map(|entries| Self::numeric_entries_cost(entries))
                .sum::<usize>();
        #[cfg(not(feature = "text-index"))]
        let numeric: usize = 0;

        postings
            + term_dicts
            + fst
            + doc_field_lengths
            + key_hash_to_doc_id
            + doc_id_to_key
            + lsn_maps
            + tag
            + numeric
    }

    /// Hard-delete a document identified by `doc_id` from all inverted indexes.
    ///
    /// Removes:
    /// - BM25 posting entries for all TEXT fields.
    /// - Field-length accounting (num_docs, total_field_length) for all TEXT fields.
    /// - TAG bitmap entries (via `doc_tag_entries`).
    /// - NUMERIC BTreeMap entries (via `doc_numeric_entries`).
    /// - `doc_field_lengths`, `doc_id_to_key`, `key_hash_to_doc_id`.
    /// - MVCC LSN records (`doc_id_to_insert_lsn`, `doc_id_to_delete_lsn`).
    ///
    /// Does nothing if `doc_id` is not present in `doc_id_to_key` (idempotent).
    ///
    /// This is the building block used by `FT.INVALIDATE_RANGE` for force-push
    /// bulk-invalidation of stale recall (see INTEGRATION-PLAN.md §3.3).
    pub fn remove_doc_by_doc_id(&mut self, doc_id: u32) {
        // Guard: nothing to do if doc isn't tracked.
        if !self.doc_id_to_key.contains_key(&doc_id) {
            return;
        }

        // ── TEXT field removal ────────────────────────────────────────────────
        for field_idx in 0..self.text_fields.len() {
            if self.text_fields[field_idx].noindex {
                continue;
            }
            // Subtract field length from stats before clearing postings.
            if let Some(lengths) = self.doc_field_lengths.get(&doc_id) {
                if let Some(&len) = lengths.get(field_idx) {
                    let len64 = len as u64;
                    self.field_stats[field_idx].total_field_length = self.field_stats[field_idx]
                        .total_field_length
                        .saturating_sub(len64);
                    if len64 > 0 {
                        self.field_stats[field_idx].num_docs =
                            self.field_stats[field_idx].num_docs.saturating_sub(1);
                    }
                }
            }
            self.field_postings[field_idx].remove_doc(doc_id);
        }

        // ── TAG field removal ─────────────────────────────────────────────────
        // K4 (P0 fix): shared `tag_bitmap_revoke`/`tag_entries_cost` helpers
        // -- same logic `tag_index_document`'s revoke loop uses -- so the two
        // call sites cannot drift apart.
        #[cfg(feature = "text-index")]
        if let Some(entries) = self.doc_tag_entries.remove(&doc_id) {
            self.resident_bytes_extra = self
                .resident_bytes_extra
                .saturating_sub(Self::tag_entries_cost(&entries));
            for (field, value) in entries {
                self.tag_bitmap_revoke(&field, &value, doc_id);
            }
        }

        // ── NUMERIC field removal ─────────────────────────────────────────────
        // K4 (P0 fix): shared `numeric_bitmap_revoke`/`numeric_entries_cost`.
        #[cfg(feature = "text-index")]
        if let Some(entries) = self.doc_numeric_entries.remove(&doc_id) {
            self.resident_bytes_extra = self
                .resident_bytes_extra
                .saturating_sub(Self::numeric_entries_cost(&entries));
            for (field, value) in entries {
                self.numeric_bitmap_revoke(&field, &value, doc_id);
            }
        }

        // ── Metadata cleanup ──────────────────────────────────────────────────
        // K4 (P0 fix): uncharge using the ACTUAL removed Vec's length --
        // self-correcting even if field_count ever varied per doc (it
        // currently doesn't).
        if let Some(lengths) = self.doc_field_lengths.remove(&doc_id) {
            self.resident_bytes_extra = self.resident_bytes_extra.saturating_sub(
                std::mem::size_of::<u32>()
                    + lengths.len() * std::mem::size_of::<u32>()
                    + MAP_ENTRY_OVERHEAD,
            );
        }
        // Remove from key_hash -> doc_id map (need to find the key_hash).
        if let Some(key) = self.doc_id_to_key.remove(&doc_id) {
            let key_hash = xxhash_rust::xxh64::xxh64(&key, 0);
            self.key_hash_to_doc_id.remove(&key_hash);
            self.uncharge_doc_key(key.len());
        }
        if self.doc_id_to_insert_lsn.remove(&doc_id).is_some() {
            self.resident_bytes_extra = self.resident_bytes_extra.saturating_sub(
                std::mem::size_of::<u32>() + std::mem::size_of::<u64>() + MAP_ENTRY_OVERHEAD,
            );
        }
        // `doc_id_to_delete_lsn` currently has no insertion call site anywhere
        // in the codebase (reserved for future v0.2 logical-delete wiring --
        // see the field's doc comment on `TextIndex`), so this `.remove()` is
        // always a no-op today. Written defensively symmetric so a future
        // insert-side wiring doesn't silently leak accounting.
        if self.doc_id_to_delete_lsn.remove(&doc_id).is_some() {
            self.resident_bytes_extra = self.resident_bytes_extra.saturating_sub(
                std::mem::size_of::<u32>() + std::mem::size_of::<u64>() + MAP_ENTRY_OVERHEAD,
            );
        }
    }
}

#[cfg(feature = "text-index")]
thread_local! {
    /// Scratch buffer for ASCII-lowercase normalization of TAG values on the
    /// HSET write path. Reused across calls to avoid per-tag Vec allocation
    /// on the slow path. Retained capacity is bounded by the tag-value cap
    /// (4 KiB) so it does not leak a large buffer across shards.
    static TAG_SCRATCH: std::cell::RefCell<Vec<u8>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Normalize a TAG value slice for storage / lookup.
///
/// Fast path: if the slice is already ASCII-lowercase (or `case_sensitive`
/// is set), return a zero-copy `Bytes::slice` — no allocation. Slow path:
/// fill the per-thread TAG_SCRATCH buffer and return one `Bytes::copy_from_slice`.
#[cfg(feature = "text-index")]
fn normalize_tag_value(
    value_bytes: &Bytes,
    offset: usize,
    len: usize,
    case_sensitive: bool,
) -> Bytes {
    let slice = value_bytes.slice(offset..offset + len);
    if case_sensitive {
        return slice;
    }
    if slice.iter().all(|b| !b.is_ascii_uppercase()) {
        return slice;
    }
    TAG_SCRATCH.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        buf.reserve(slice.len());
        for b in slice.iter() {
            buf.push(b.to_ascii_lowercase());
        }
        Bytes::copy_from_slice(&buf)
    })
}

/// Find a field value in HSET-style pairwise args.
///
/// Args layout: [field1, value1, field2, value2, ...]
/// Returns the raw bytes of the value for the matching field name.
fn find_field_value<'a>(args: &'a [crate::protocol::Frame], field_name: &[u8]) -> Option<&'a [u8]> {
    let mut i = 0;
    while i + 1 < args.len() {
        if let crate::protocol::Frame::BulkString(name) = &args[i] {
            if name.as_ref() == field_name {
                if let crate::protocol::Frame::BulkString(value) = &args[i + 1] {
                    return Some(value.as_ref());
                }
            }
        }
        i += 2;
    }
    None
}

/// Per-shard registry of TextIndex instances.
///
/// Mirrors VectorStore: HashMap<Bytes, TextIndex> with prefix-based
/// key matching for auto-indexing.
pub struct TextStore {
    indexes: HashMap<Bytes, TextIndex>,
    /// Shard directory for persisting text index metadata sidecar.
    /// Set once during event loop init when persistence is enabled.
    persist_dir: Option<std::path::PathBuf>,
    /// Monotonic freshness counter for the FT text engine on this shard.
    ///
    /// Bumped (Release) after every successful mutating operation: `create_index`,
    /// `drop_index`, and document indexing via `index_document_with_lsn`,
    /// `tag_index_document`, `numeric_index_document`. Exposed by `FT.INFO`
    /// under `text_version_token`.
    ///
    /// Semantics:
    /// - Starts at 0 on shard boot; NOT restored from WAL (freshness hint only).
    /// - Monotonic within a single shard; no cross-shard atomicity.
    /// - Counter never wraps in practice (u64::MAX ≈ 1.8 × 10¹⁹ writes).
    /// - Failed writes do NOT bump the counter.
    version_token: AtomicU64,
}

impl TextStore {
    /// Create an empty TextStore.
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            persist_dir: None,
            version_token: AtomicU64::new(0),
        }
    }

    /// Return the current FT text engine version token for this shard.
    ///
    /// Uses `Acquire` ordering so the caller observes all writes that preceded
    /// the most recent `bump_version` call on this shard.
    #[inline]
    pub fn version_token(&self) -> u64 {
        self.version_token.load(Ordering::Acquire)
    }

    /// Bump the FT text version token by 1 after a successful write.
    ///
    /// Uses `Release` ordering so that any subsequent `Acquire` load on any
    /// thread observes the completed write. Returns the new value.
    #[inline]
    pub fn bump_version(&self) -> u64 {
        self.version_token.fetch_add(1, Ordering::Release) + 1
    }

    /// Set the shard directory for index metadata persistence.
    /// Called once during event loop init when persistence is enabled.
    pub fn set_persist_dir(&mut self, dir: std::path::PathBuf) {
        self.persist_dir = Some(dir);
    }

    /// Persist current text index metadata to the sidecar file.
    /// No-op if persist_dir is not set (persistence disabled).
    fn save_index_meta_sidecar(&self) {
        if let Some(ref dir) = self.persist_dir {
            let metas = self.collect_index_metas();
            if let Err(e) = crate::text::index_persist::save_text_index_metadata(dir, &metas) {
                tracing::warn!("Failed to save text index metadata: {}", e);
            }
        }
    }

    /// Approximate total resident bytes across every text index on this
    /// shard (K4 accounting spine). Sum of `TextIndex::resident_bytes()`;
    /// see that method's doc comment for what is counted/excluded. `0` for
    /// an empty store -- called from the shard's 100ms tick and published
    /// into `store_memory.text`, which previously never left its
    /// hard-coded-0 initial value.
    pub fn resident_bytes(&self) -> usize {
        self.indexes.values().map(TextIndex::resident_bytes).sum()
    }

    /// Collect schema-only metadata from all text indexes for persistence.
    pub fn collect_index_metas(&self) -> Vec<TextIndexMeta> {
        self.indexes
            .values()
            .map(|idx| TextIndexMeta {
                name: idx.name.clone(),
                bm25_config: idx.bm25_config,
                key_prefixes: idx.key_prefixes.clone(),
                text_fields: idx.text_fields.clone(),
                db_index: idx.db_index,
            })
            .collect()
    }

    /// Create a new text index. Returns Err if the name already exists.
    pub fn create_index(&mut self, name: Bytes, index: TextIndex) -> Result<(), &'static str> {
        if self.indexes.contains_key(&name) {
            return Err("Index already exists");
        }
        self.indexes.insert(name, index);
        self.save_index_meta_sidecar();
        // Bump version AFTER successful create (monotonicity-on-success contract).
        self.bump_version();
        Ok(())
    }

    /// Drop a text index by name. Returns true if it existed.
    ///
    /// NOTE (WS5a): NOT db-scoped — see [`Self::drop_index_for_db`].
    pub fn drop_index(&mut self, name: &[u8]) -> bool {
        let removed = self.indexes.remove(name).is_some();
        if removed {
            self.save_index_meta_sidecar();
            // Bump version AFTER successful drop (monotonicity-on-success contract).
            self.bump_version();
        }
        removed
    }

    /// Db-scoped variant of [`Self::drop_index`] (WS5a): refuses to drop an
    /// index owned by a different db.
    pub fn drop_index_for_db(&mut self, name: &[u8], db_index: u8) -> bool {
        match self.indexes.get(name) {
            Some(idx) if idx.db_index == db_index => self.drop_index(name),
            _ => false,
        }
    }

    /// FLUSHALL/FLUSHDB parity (persistence-review R3): reset every text
    /// index to an empty state (postings, term dicts, doc maps, TAG/NUMERIC
    /// indexes) while KEEPING the FT.CREATE schema — mirroring restart
    /// semantics. Without this, flushed hashes stayed matchable as ghost
    /// documents until the next restart.
    ///
    /// Clears indexes in EVERY logical db — the correct primitive for
    /// FLUSHALL. FLUSHDB must use [`Self::clear_all_contents_for_db`]
    /// instead (WS5a) — the two commands are not yet differentiated at the
    /// call sites, see the WS5a gap report.
    pub fn clear_all_contents(&mut self) {
        self.clear_contents_matching(|_| true);
    }

    /// Db-scoped variant of [`Self::clear_all_contents`] for FLUSHDB
    /// (WS5a): only resets indexes owned by `db_index`.
    pub fn clear_all_contents_for_db(&mut self, db_index: u8) {
        self.clear_contents_matching(|idx| idx.db_index == db_index);
    }

    #[cfg_attr(not(feature = "text-index"), allow(unused_variables))]
    fn clear_contents_matching(&mut self, predicate: impl Fn(&TextIndex) -> bool) {
        #[cfg(feature = "text-index")]
        {
            let mut any = false;
            for idx in self.indexes.values_mut() {
                if !predicate(idx) {
                    continue;
                }
                // Preserve db_index across the schema-only recreate -- a
                // fresh TextIndex::new_with_schema() defaults to db 0, which
                // would otherwise silently re-home the index to db 0 on
                // every FLUSH.
                let db_index = idx.db_index;
                *idx = TextIndex::new_with_schema(
                    idx.name.clone(),
                    idx.key_prefixes.clone(),
                    idx.text_fields.clone(),
                    idx.tag_fields.clone(),
                    idx.numeric_fields.clone(),
                    idx.bm25_config,
                );
                idx.db_index = db_index;
                any = true;
            }
            if any {
                self.bump_version();
            }
        }
        // Without the text-index feature no TextIndex constructor exists and
        // no documents can have been indexed — nothing to clear.
    }

    /// Get a read-only reference to a text index.
    ///
    /// NOTE (WS5a): NOT db-scoped — see [`Self::get_index_for_db`] and the
    /// WS5a gap report for the call-site migration.
    pub fn get_index(&self, name: &[u8]) -> Option<&TextIndex> {
        self.indexes.get(name)
    }

    /// Db-scoped variant of [`Self::get_index`] (WS5a).
    pub fn get_index_for_db(&self, name: &[u8], db_index: u8) -> Option<&TextIndex> {
        self.indexes
            .get(name)
            .filter(|idx| idx.db_index == db_index)
    }

    /// Get a mutable reference to a text index.
    ///
    /// NOTE (WS5a): NOT db-scoped — see [`Self::get_index_mut_for_db`].
    pub fn get_index_mut(&mut self, name: &[u8]) -> Option<&mut TextIndex> {
        self.indexes.get_mut(name)
    }

    /// Db-scoped variant of [`Self::get_index_mut`] (WS5a).
    pub fn get_index_mut_for_db(&mut self, name: &[u8], db_index: u8) -> Option<&mut TextIndex> {
        self.indexes
            .get_mut(name)
            .filter(|idx| idx.db_index == db_index)
    }

    /// Find all index names whose key_prefixes match the given key.
    ///
    /// NOTE (WS5a): NOT db-scoped — the HSET auto-index hook still calls
    /// this unscoped variant (see [`Self::find_matching_index_names_for_db`]
    /// and the WS5a gap report).
    pub fn find_matching_index_names(&self, key: &[u8]) -> Vec<Bytes> {
        let mut matches = Vec::new();
        for (name, index) in &self.indexes {
            // Empty prefix list means match all keys
            if index.key_prefixes.is_empty() {
                matches.push(name.clone());
                continue;
            }
            for prefix in &index.key_prefixes {
                if key.starts_with(prefix.as_ref()) {
                    matches.push(name.clone());
                    break;
                }
            }
        }
        matches
    }

    /// Db-scoped variant of [`Self::find_matching_index_names`] (WS5a).
    pub fn find_matching_index_names_for_db(&self, key: &[u8], db_index: u8) -> Vec<Bytes> {
        let mut matches = Vec::new();
        for (name, index) in &self.indexes {
            if index.db_index != db_index {
                continue;
            }
            if index.key_prefixes.is_empty() {
                matches.push(name.clone());
                continue;
            }
            for prefix in &index.key_prefixes {
                if key.starts_with(prefix.as_ref()) {
                    matches.push(name.clone());
                    break;
                }
            }
        }
        matches
    }

    /// List all index names (for FT._LIST).
    ///
    /// NOTE (WS5a): NOT db-scoped — see [`Self::index_names_for_db`].
    pub fn index_names(&self) -> Vec<Bytes> {
        self.indexes.keys().cloned().collect()
    }

    /// Db-scoped variant of [`Self::index_names`] (WS5a).
    pub fn index_names_for_db(&self, db_index: u8) -> Vec<Bytes> {
        self.indexes
            .iter()
            .filter(|(_, idx)| idx.db_index == db_index)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Number of text indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }

    /// Save FST sidecar for a specific index. No-op if persist_dir not set.
    ///
    /// Called after `TextIndex::build_fst()` at FT.COMPACT time (D-11).
    ///
    /// Deprecated by [`Self::save_term_fst_sidecar_for_index`] (kernel M4,
    /// task #50), which persists the term dictionary the FST's ids were
    /// built against in the same atomic write -- kept only for the existing
    /// FST-only roundtrip unit tests below; production code should call the
    /// combined saver instead.
    #[cfg(feature = "text-index")]
    #[cfg(test)]
    pub fn save_fst_sidecar_for_index(&self, index_name: &[u8]) {
        if let Some(ref dir) = self.persist_dir {
            if let Some(idx) = self.indexes.get(index_name) {
                let fst_data: Vec<Option<&[u8]>> = idx
                    .fst_maps
                    .iter()
                    .map(|opt| opt.as_ref().map(|m| m.as_fst().as_bytes()))
                    .collect();
                if let Err(e) =
                    crate::text::index_persist::save_fst_sidecar(dir, index_name, &fst_data)
                {
                    tracing::warn!(
                        "Failed to save FST sidecar for {}: {}",
                        String::from_utf8_lossy(index_name),
                        e
                    );
                }
            }
        }
    }

    /// Save the combined term-dict + FST sidecar for a specific index.
    /// No-op if persist_dir not set. Called after `TextIndex::build_fst()`
    /// at FT.COMPACT time (kernel M4, task #50 -- supersedes the FST-only
    /// sidecar so the loaded FST's ids are always backed by a matching term
    /// dictionary).
    #[cfg(feature = "text-index")]
    pub fn save_term_fst_sidecar_for_index(&self, index_name: &[u8]) {
        let Some(ref dir) = self.persist_dir else {
            return;
        };
        let Some(idx) = self.indexes.get(index_name) else {
            return;
        };
        let fields: Vec<crate::text::index_persist::FieldTermFstSidecar> = idx
            .field_term_dicts
            .iter()
            .enumerate()
            .map(|(field_idx, dict)| {
                let terms: Vec<(String, u32)> =
                    dict.iter().map(|(t, &id)| (t.to_owned(), id)).collect();
                let fst_bytes = idx
                    .fst_maps
                    .get(field_idx)
                    .and_then(|m| m.as_ref())
                    .map(|m| m.as_fst().as_bytes().to_vec());
                crate::text::index_persist::FieldTermFstSidecar {
                    next_id: dict.next_id(),
                    fst_high_water_mark: dict.fst_high_water_mark,
                    terms,
                    fst_bytes,
                }
            })
            .collect();
        if let Err(e) = crate::text::index_persist::save_term_fst_sidecar(dir, index_name, &fields)
        {
            tracing::warn!(
                "Failed to save term-dict+FST sidecar for {}: {}",
                String::from_utf8_lossy(index_name),
                e
            );
        }
    }

    /// Load FST-only sidecars for all indexes (legacy, id-space UNSAFE).
    ///
    /// # Deliberately NOT called during startup/recovery — do not wire this in
    ///
    /// It was wired into shard recovery once (kernel-m2-brief-2026-07-12.md,
    /// K3 site 3) and reverted after adversarial review found it corrupts
    /// FUZZY/PREFIX search results. The failure chain:
    ///
    /// 1. `TermDictionary::get_or_insert` assigns `term_id`s sequentially by
    ///    first-encounter order.
    /// 2. After a restart, the auto-reindex rescan rebuilds every text
    ///    index's term dictionary from `DashTable` hash-iteration order,
    ///    which is **not reproducible** across restarts — the same corpus
    ///    can assign completely different `term_id`s to the same terms.
    /// 3. The `.fst` sidecar on disk bakes in the OLD generation's
    ///    `term_id`s, with no corpus fingerprint tying it to the specific
    ///    term-dict generation it was built from.
    /// 4. `fst_high_water_mark` stays `0`, so nothing stops `expand_terms`
    ///    from merging stale-FST ids that now collide with unrelated,
    ///    freshly-assigned ids from the new generation — silently wrong
    ///    search results, not even a detectable error.
    ///
    /// Fixed by [`Self::load_term_fst_sidecars`] (kernel M4, task #50),
    /// which persists AND restores the term dictionary itself before the
    /// keyspace rescan runs, so ids never drift out from under a loaded
    /// FST. This FST-only function is kept solely for its own pre-existing
    /// roundtrip unit tests and must stay uncalled in production.
    #[cfg(feature = "text-index")]
    #[cfg(test)]
    pub fn load_fst_sidecars(&mut self) {
        if let Some(ref dir) = self.persist_dir {
            let dir = dir.clone();
            let names: Vec<Bytes> = self.indexes.keys().cloned().collect();
            for name in names {
                match crate::text::index_persist::load_fst_sidecar(&dir, name.as_ref()) {
                    Ok(field_fsts) if !field_fsts.is_empty() => {
                        if let Some(idx) = self.indexes.get_mut(name.as_ref()) {
                            for (field_idx, fst_bytes_opt) in field_fsts.into_iter().enumerate() {
                                if field_idx < idx.fst_maps.len() {
                                    if let Some(bytes) = fst_bytes_opt {
                                        match fst::Map::new(bytes) {
                                            // K4 (P0 fix): route through set_fst_map so the
                                            // resident_bytes_extra accounting stays in sync
                                            // (same helper build_fst uses).
                                            Ok(map) => idx.set_fst_map(field_idx, Some(map)),
                                            Err(e) => tracing::warn!(
                                                "FST load failed for {}[{}]: {}",
                                                String::from_utf8_lossy(name.as_ref()),
                                                field_idx,
                                                e
                                            ),
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(_) => {} // No sidecar — ok, fst_maps stay None
                    Err(e) => tracing::warn!(
                        "Failed to load FST sidecar for {}: {}",
                        String::from_utf8_lossy(name.as_ref()),
                        e
                    ),
                }
            }
        }
    }

    /// Load term-dict + FST sidecars for all indexes -- the SAFE loader
    /// (kernel M4, task #50).
    ///
    /// MUST be called after `create_index` has restored the index schemas
    /// from `text-indexes.meta` and BEFORE the keyspace auto-reindex rescan
    /// runs any `index_document` calls -- see `src/shard/event_loop.rs`'s
    /// recovery sequencing. Seeding `field_term_dicts` first makes the
    /// rescan's `TermDictionary::get_or_insert` calls resolve already-known
    /// terms to their PERSISTED ids (not fresh ones), and only ever assign
    /// brand-new ids (continuing from the persisted `next_id`) to terms that
    /// are genuinely new since the sidecar was written. That is what makes
    /// loading the FST alongside it safe: the FST's baked-in ids and the
    /// live term dictionary's ids are now the same id-space by construction.
    ///
    /// Fail-closed per index: any missing, truncated, corrupt,
    /// version-mismatched, or field-count-mismatched (schema changed since
    /// the sidecar was written) sidecar causes that index to be skipped
    /// entirely -- both the term dict AND the FST stay at their fresh-start
    /// state, identical to today's always-rescan behavior. Never partially
    /// apply a sidecar (e.g. seed the dict but skip a corrupt FST, or vice
    /// versa) -- that would silently reintroduce the id-space mismatch this
    /// function exists to prevent.
    #[cfg(feature = "text-index")]
    pub fn load_term_fst_sidecars(&mut self) {
        let Some(ref dir) = self.persist_dir else {
            return;
        };
        let dir = dir.clone();
        let names: Vec<Bytes> = self.indexes.keys().cloned().collect();
        for name in names {
            let loaded = match crate::text::index_persist::load_term_fst_sidecar(
                &dir,
                name.as_ref(),
            ) {
                Ok(Some(fields)) => fields,
                Ok(None) => continue, // no sidecar -- fresh start, today's behavior
                Err(e) => {
                    tracing::warn!(
                        "Term-dict+FST sidecar for {} failed to load, falling back to full rescan: {}",
                        String::from_utf8_lossy(name.as_ref()),
                        e
                    );
                    continue;
                }
            };

            let Some(idx) = self.indexes.get_mut(name.as_ref()) else {
                continue;
            };
            if loaded.len() != idx.field_term_dicts.len() {
                // Schema changed (field count differs) since the sidecar
                // was written -- stale, fail closed.
                tracing::warn!(
                    "Term-dict+FST sidecar for {} has {} field(s), index has {} -- stale sidecar, falling back to full rescan",
                    String::from_utf8_lossy(name.as_ref()),
                    loaded.len(),
                    idx.field_term_dicts.len()
                );
                continue;
            }

            // Build every field's TermDictionary BEFORE mutating anything on
            // `idx`, so a single bad field aborts the whole index cleanly
            // (all-or-nothing per index).
            let mut rebuilt_dicts = Vec::with_capacity(loaded.len());
            let mut ok = true;
            for field in &loaded {
                match TermDictionary::from_pairs(
                    field.terms.clone(),
                    field.next_id,
                    field.fst_high_water_mark,
                ) {
                    Some(dict) => rebuilt_dicts.push(dict),
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok || rebuilt_dicts.len() != loaded.len() {
                tracing::warn!(
                    "Term-dict+FST sidecar for {} is internally inconsistent -- falling back to full rescan",
                    String::from_utf8_lossy(name.as_ref())
                );
                continue;
            }

            for (field_idx, (dict, field)) in rebuilt_dicts.into_iter().zip(loaded).enumerate() {
                idx.field_term_dicts[field_idx] = dict;
                if let Some(fst_bytes) = field.fst_bytes {
                    match fst::Map::new(fst_bytes) {
                        Ok(map) => idx.set_fst_map(field_idx, Some(map)),
                        Err(e) => tracing::warn!(
                            "FST bytes for {}[{}] failed to parse despite a valid term-dict sidecar: {}",
                            String::from_utf8_lossy(name.as_ref()),
                            field_idx,
                            e
                        ),
                    }
                }
            }
            idx.recovered_from_sidecar = true;
        }
    }
}

#[cfg(test)]
#[cfg(feature = "text-index")]
mod tests {
    use super::*;

    /// Build a minimal TextIndex with a single TEXT field named "body"
    /// and index N documents. Used by multiple tests.
    fn make_index_with_docs(docs: &[(&str, &str)]) -> TextIndex {
        use crate::protocol::Frame;
        use crate::text::types::BM25Config;
        let field = TextFieldDef::new(Bytes::from_static(b"body"));
        let mut idx = TextIndex::new(
            Bytes::from_static(b"test_idx"),
            Vec::new(),
            vec![field],
            BM25Config::default(),
        );
        for (i, (key, text)) in docs.iter().enumerate() {
            let key_hash = i as u64;
            let args = vec![
                Frame::BulkString(Bytes::from_static(b"body")),
                Frame::BulkString(Bytes::copy_from_slice(text.as_bytes())),
            ];
            idx.index_document(key_hash, key.as_bytes(), &args);
        }
        idx
    }

    /// WS5a (db-scoped indexes): TextStore db-tagged variants mirror the
    /// VectorStore ones (`src/vector/store.rs` `mod ws5a_db_scoping`).
    mod ws5a_db_scoping {
        use super::*;

        fn make_text_index(name: &str, prefix: &str, db_index: u8) -> TextIndex {
            let field = TextFieldDef::new(Bytes::from_static(b"body"));
            let mut idx = TextIndex::new(
                Bytes::from(name.to_owned()),
                vec![Bytes::from(prefix.to_owned())],
                vec![field],
                crate::text::types::BM25Config::default(),
            );
            idx.db_index = db_index;
            idx
        }

        #[test]
        fn get_index_for_db_is_invisible_cross_db() {
            let mut store = TextStore::new();
            store
                .create_index(
                    Bytes::from_static(b"idx"),
                    make_text_index("idx", "doc:", 3),
                )
                .unwrap();

            assert!(store.get_index_for_db(b"idx", 3).is_some());
            assert!(store.get_index_for_db(b"idx", 0).is_none());
            // Legacy unscoped accessor keeps its historical (global) behavior.
            assert!(store.get_index(b"idx").is_some());
        }

        #[test]
        fn index_names_for_db_filters_by_owner() {
            let mut store = TextStore::new();
            store
                .create_index(Bytes::from_static(b"a"), make_text_index("a", "doc:", 0))
                .unwrap();
            store
                .create_index(Bytes::from_static(b"b"), make_text_index("b", "doc:", 1))
                .unwrap();

            assert_eq!(store.index_names_for_db(0), vec![Bytes::from_static(b"a")]);
            assert_eq!(store.index_names_for_db(1), vec![Bytes::from_static(b"b")]);
            assert_eq!(store.index_names().len(), 2);
        }

        #[test]
        fn find_matching_index_names_for_db_scopes_auto_index() {
            let mut store = TextStore::new();
            store
                .create_index(
                    Bytes::from_static(b"db0idx"),
                    make_text_index("db0idx", "doc:", 0),
                )
                .unwrap();
            store
                .create_index(
                    Bytes::from_static(b"db1idx"),
                    make_text_index("db1idx", "doc:", 1),
                )
                .unwrap();

            assert_eq!(
                store.find_matching_index_names_for_db(b"doc:1", 0),
                vec![Bytes::from_static(b"db0idx")]
            );
            assert_eq!(
                store.find_matching_index_names_for_db(b"doc:1", 1),
                vec![Bytes::from_static(b"db1idx")]
            );
            assert_eq!(store.find_matching_index_names(b"doc:1").len(), 2);
        }

        #[test]
        fn clear_all_contents_for_db_leaves_other_dbs_untouched_and_preserves_db_index() {
            let mut store = TextStore::new();
            store
                .create_index(
                    Bytes::from_static(b"db0idx"),
                    make_text_index("db0idx", "doc:", 0),
                )
                .unwrap();
            store
                .create_index(
                    Bytes::from_static(b"db1idx"),
                    make_text_index("db1idx", "doc:", 1),
                )
                .unwrap();

            store.clear_all_contents_for_db(0);

            // Definitions survive in both dbs, and db_index is preserved
            // across the schema-only recreate (not silently reset to 0).
            assert_eq!(store.index_count(), 2);
            assert_eq!(store.get_index(b"db0idx").unwrap().db_index, 0);
            assert_eq!(store.get_index(b"db1idx").unwrap().db_index, 1);
        }

        #[test]
        fn drop_index_for_db_refuses_cross_db_drop() {
            let mut store = TextStore::new();
            store
                .create_index(
                    Bytes::from_static(b"idx"),
                    make_text_index("idx", "doc:", 3),
                )
                .unwrap();

            assert!(!store.drop_index_for_db(b"idx", 0));
            assert_eq!(store.index_count(), 1);
            assert!(store.drop_index_for_db(b"idx", 3));
            assert_eq!(store.index_count(), 0);
        }
    }

    #[test]
    fn test_search_field_basic() {
        // doc 0: "machine vision", doc 1: "deep learning", doc 2: "machine learning deep"
        let idx = make_index_with_docs(&[
            ("doc:0", "machine vision"),
            ("doc:1", "deep learning"),
            ("doc:2", "machine learning deep"),
        ]);

        // "machine" AND "learning": only doc 2 should match
        // Terms go through the same stemmer as indexing (English Snowball)
        // "machine" -> "machin", "learning" -> "learn"
        let terms = vec!["machin".to_string(), "learn".to_string()];
        let results = idx.search_field(0, &terms, None, None, 10);

        assert_eq!(results.len(), 1, "Only doc:2 matches both terms");
        assert_eq!(results[0].key.as_ref(), b"doc:2");
        assert!(results[0].score > 0.0, "BM25 score must be positive");
    }

    #[test]
    fn test_search_field_score_ordering() {
        // doc 0: "machine" appears once, doc 1: "machine machine machine" (higher TF)
        let idx = make_index_with_docs(&[
            ("doc:0", "machine vision"),
            ("doc:1", "machine machine machine"),
        ]);

        let terms = vec!["machin".to_string()];
        let results = idx.search_field(0, &terms, None, None, 10);

        assert_eq!(results.len(), 2, "Both docs contain 'machine'");
        // doc:1 has higher TF -> higher BM25 -> should be first (descending sort)
        assert!(
            results[0].score >= results[1].score,
            "Results must be sorted descending by score"
        );
        assert_eq!(
            results[0].key.as_ref(),
            b"doc:1",
            "doc:1 with higher TF should rank first"
        );
    }

    #[test]
    fn test_search_field_global_idf() {
        // doc 0: "machine", doc 1: "machine learning"
        let idx = make_index_with_docs(&[("doc:0", "machine"), ("doc:1", "machine learning")]);

        let terms = vec!["machin".to_string()];

        // Without global override: use local stats (2 docs, df=2 for "machine")
        let local_results = idx.search_field(0, &terms, None, None, 10);
        assert_eq!(local_results.len(), 2);

        // With global_df override: simulate cross-shard context where "machin" has df=10 globally
        // and N=100 total docs across all shards
        let mut global_df = HashMap::new();
        global_df.insert("machin".to_string(), 10u32);
        let global_results = idx.search_field(0, &terms, Some(&global_df), Some(100), 10);
        assert_eq!(global_results.len(), 2);

        // Scores differ when global IDF is used (df=10/N=100 vs df=2/N=2)
        // Both sets return 2 results; just verify global path produces valid positive scores
        for r in &global_results {
            assert!(r.score >= 0.0, "Score must be non-negative");
        }
    }

    #[test]
    fn test_search_field_missing_term() {
        let idx = make_index_with_docs(&[("doc:0", "machine vision"), ("doc:1", "deep learning")]);

        // "xyznonexistent" is not in the index: AND semantics -> empty result
        let terms = vec!["xyznonexist".to_string()];
        let results = idx.search_field(0, &terms, None, None, 10);
        assert!(results.is_empty(), "Missing term must return empty Vec");

        // Two terms where one is missing: still empty
        let terms2 = vec!["machin".to_string(), "xyznonexist".to_string()];
        let results2 = idx.search_field(0, &terms2, None, None, 10);
        assert!(
            results2.is_empty(),
            "AND with missing term must return empty Vec"
        );
    }

    #[test]
    fn test_doc_freq_for_terms() {
        // doc 0: "machine vision", doc 1: "machine learning", doc 2: "deep learning"
        let idx = make_index_with_docs(&[
            ("doc:0", "machine vision"),
            ("doc:1", "machine learning"),
            ("doc:2", "deep learning"),
        ]);

        let terms = vec![
            "machin".to_string(),
            "learn".to_string(),
            "vision".to_string(),
        ];
        let (df_pairs, n) = idx.doc_freq_for_terms(0, &terms);

        assert_eq!(n, 3, "3 docs total");
        // Find each term's df
        let machin_df = df_pairs
            .iter()
            .find(|(t, _)| t == "machin")
            .map(|(_, df)| *df)
            .unwrap_or(0);
        let learn_df = df_pairs
            .iter()
            .find(|(t, _)| t == "learn")
            .map(|(_, df)| *df)
            .unwrap_or(0);
        let vision_df = df_pairs
            .iter()
            .find(|(t, _)| t == "vision")
            .map(|(_, df)| *df)
            .unwrap_or(0);

        assert_eq!(machin_df, 2, "'machine' appears in doc:0 and doc:1");
        assert_eq!(learn_df, 2, "'learning' appears in doc:1 and doc:2");
        assert_eq!(vision_df, 1, "'vision' appears only in doc:0");
    }

    #[test]
    fn test_build_fst_and_search_field_or() {
        // doc:0 "machine vision", doc:1 "deep learning", doc:2 "machine learning deep"
        let mut idx = make_index_with_docs(&[
            ("doc:0", "machine vision"),
            ("doc:1", "deep learning"),
            ("doc:2", "machine learning deep"),
        ]);

        // Build FST from current TermDictionary
        idx.build_fst();

        // fst_maps[0] should now be Some
        assert!(
            idx.fst_maps[0].is_some(),
            "FST map should be built after build_fst()"
        );

        // Expand "machin" (stemmed "machine") via expand_terms with Exact modifier
        let term_ids = idx.expand_terms(0, "machin", &TermModifier::Exact);
        assert_eq!(term_ids.len(), 1, "Exact 'machin' should find 1 term_id");

        // search_field_or with the expanded ids (OR: docs 0 and 2 both have "machin")
        let results = idx.search_field_or(0, &term_ids, None, None, 10);
        assert_eq!(
            results.len(),
            2,
            "OR search for 'machin' should match doc:0 and doc:2"
        );
        // All results should have positive scores
        for r in &results {
            assert!(r.score > 0.0, "BM25 score must be positive");
        }
    }

    #[test]
    fn test_expand_terms_exact() {
        let idx = make_index_with_docs(&[("doc:0", "machine vision")]);
        // "machin" is the stemmed form of "machine" stored in TermDictionary
        let ids = idx.expand_terms(0, "machin", &TermModifier::Exact);
        assert_eq!(ids.len(), 1, "Exact term lookup should return 1 id");

        // Non-existent term returns empty
        let missing = idx.expand_terms(0, "xyz_nonexistent", &TermModifier::Exact);
        assert!(missing.is_empty(), "Missing term should return empty Vec");
    }

    #[test]
    fn test_expand_terms_fuzzy_no_fst() {
        // When fst_maps is None, fuzzy should fall back to HashMap brute-force
        let idx = make_index_with_docs(&[("doc:0", "machine vision")]);
        // fst_maps[0] is None (no build_fst called)
        assert!(
            idx.fst_maps[0].is_none(),
            "fst_maps should be None initially"
        );

        // "machn" is edit-distance 1 from "machin" — brute-force should find it
        let ids = idx.expand_terms(0, "machn", &TermModifier::Fuzzy(1));
        assert!(
            !ids.is_empty(),
            "Fuzzy without FST should still find 'machin' via HashMap brute-force"
        );
    }

    #[test]
    fn test_fst_sidecar_roundtrip() {
        use crate::text::index_persist::{load_fst_sidecar, save_fst_sidecar};

        let tmp = tempfile::tempdir().expect("tempdir");

        // Build two mock field FSTs (just simple byte vecs for testing format)
        let field0_bytes = b"some_fst_bytes_field0";
        let field1_bytes: Option<&[u8]> = None; // field 1 has no FST

        let fst_data: Vec<Option<&[u8]>> = vec![Some(field0_bytes), field1_bytes];
        save_fst_sidecar(tmp.path(), b"test_idx", &fst_data).expect("save FST sidecar");

        let loaded = load_fst_sidecar(tmp.path(), b"test_idx").expect("load FST sidecar");
        assert_eq!(loaded.len(), 2, "Should have 2 field entries");
        assert!(loaded[0].is_some(), "Field 0 should have FST bytes");
        assert_eq!(
            loaded[0].as_deref().unwrap(),
            field0_bytes,
            "Field 0 FST bytes should match"
        );
        assert!(loaded[1].is_none(), "Field 1 should be None");
    }

    #[test]
    fn test_fst_sidecar_missing_returns_empty() {
        use crate::text::index_persist::load_fst_sidecar;

        let tmp = tempfile::tempdir().expect("tempdir");
        // No file written — load should return empty Vec (not error)
        let loaded = load_fst_sidecar(tmp.path(), b"nonexistent_idx").expect("load");
        assert!(loaded.is_empty(), "Missing sidecar should return empty Vec");
    }

    // ── Kernel M4 (task #50): term-dict + FST sidecar durability ─────────

    /// Build a fresh TextStore of the same shape `make_index_with_docs`
    /// builds directly on TextIndex, so both the "before crash" and
    /// "after restart" sides of the equivalence test share one schema.
    fn make_store_with_docs(persist_dir: &std::path::Path, docs: &[(&str, &str)]) -> TextStore {
        use crate::protocol::Frame;
        use crate::text::types::BM25Config;
        let mut store = TextStore::new();
        store.set_persist_dir(persist_dir.to_path_buf());
        let field = TextFieldDef::new(Bytes::from_static(b"body"));
        let idx = TextIndex::new(
            Bytes::from_static(b"test_idx"),
            Vec::new(),
            vec![field],
            BM25Config::default(),
        );
        store
            .create_index(Bytes::from_static(b"test_idx"), idx)
            .expect("create_index");
        let idx = store.get_index_mut(b"test_idx").expect("index exists");
        for (i, (key, text)) in docs.iter().enumerate() {
            let args = vec![
                Frame::BulkString(Bytes::from_static(b"body")),
                Frame::BulkString(Bytes::copy_from_slice(text.as_bytes())),
            ];
            idx.index_document(i as u64, key.as_bytes(), &args);
        }
        store
    }

    /// The core equivalence gate for task #50: an index recovered from the
    /// `.tfst` sidecar (term-dict seed + rescan) must answer FUZZY and
    /// PREFIX queries -- which exercise the FST, not just the HashMap
    /// brute-force path -- IDENTICALLY to a from-scratch rebuilt index over
    /// the same corpus.
    #[test]
    fn load_term_fst_sidecars_survives_restart_and_matches_rebuilt() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let docs: &[(&str, &str)] = &[
            ("doc:0", "machine vision"),
            ("doc:1", "deep learning"),
            ("doc:2", "machine learning deep"),
        ];

        // "Before crash": build the index, compact (builds FST), persist
        // the combined term-dict+FST sidecar -- mirrors FT.COMPACT.
        let mut before = make_store_with_docs(tmp.path(), docs);
        {
            let idx = before.get_index_mut(b"test_idx").expect("index exists");
            idx.build_fst();
        }
        before.save_term_fst_sidecar_for_index(b"test_idx");

        let expected_fuzzy = {
            let idx = before.get_index(b"test_idx").expect("index exists");
            idx.expand_terms(0, "machn", &TermModifier::Fuzzy(1))
        };
        let expected_prefix = {
            let idx = before.get_index(b"test_idx").expect("index exists");
            idx.expand_terms(0, "lear", &TermModifier::Prefix)
        };
        assert!(!expected_fuzzy.is_empty(), "fixture sanity: fuzzy matches");
        assert!(
            !expected_prefix.is_empty(),
            "fixture sanity: prefix matches"
        );

        // "After restart": schema-only empty index (as `create_index`
        // restores from `text-indexes.meta`), THEN load_term_fst_sidecars
        // (seeds term dicts + FST BEFORE any doc is re-indexed), THEN the
        // keyspace rescan re-indexes the same docs -- exactly the sequence
        // wired in `src/shard/event_loop.rs`.
        let mut after = TextStore::new();
        after.set_persist_dir(tmp.path().to_path_buf());
        let field = TextFieldDef::new(Bytes::from_static(b"body"));
        let empty_idx = TextIndex::new(
            Bytes::from_static(b"test_idx"),
            Vec::new(),
            vec![field],
            crate::text::types::BM25Config::default(),
        );
        after
            .create_index(Bytes::from_static(b"test_idx"), empty_idx)
            .expect("create_index");
        after.load_term_fst_sidecars();
        assert!(
            after.get_index(b"test_idx").unwrap().recovered_from_sidecar,
            "sidecar was valid -- recovered_from_sidecar must be true"
        );
        {
            use crate::protocol::Frame;
            let idx = after.get_index_mut(b"test_idx").expect("index exists");
            for (i, (key, text)) in docs.iter().enumerate() {
                let args = vec![
                    Frame::BulkString(Bytes::from_static(b"body")),
                    Frame::BulkString(Bytes::copy_from_slice(text.as_bytes())),
                ];
                idx.index_document(i as u64, key.as_bytes(), &args);
            }
        }

        let actual_fuzzy = {
            let idx = after.get_index(b"test_idx").expect("index exists");
            idx.expand_terms(0, "machn", &TermModifier::Fuzzy(1))
        };
        let actual_prefix = {
            let idx = after.get_index(b"test_idx").expect("index exists");
            idx.expand_terms(0, "lear", &TermModifier::Prefix)
        };

        let sorted = |mut v: Vec<u32>| {
            v.sort_unstable();
            v
        };
        assert_eq!(
            sorted(actual_fuzzy),
            sorted(expected_fuzzy),
            "FUZZY term-id expansion after sidecar-recovered restart must match a from-scratch rebuild"
        );
        assert_eq!(
            sorted(actual_prefix),
            sorted(expected_prefix),
            "PREFIX term-id expansion after sidecar-recovered restart must match a from-scratch rebuild"
        );

        // And the search results built on top of those ids must match too.
        let expected_results = {
            let idx = before.get_index(b"test_idx").expect("index exists");
            let ids = idx.expand_terms(0, "machn", &TermModifier::Fuzzy(1));
            let mut r = idx.search_field_or(0, &ids, None, None, 10);
            r.sort_by(|a, b| a.key.cmp(&b.key));
            r.into_iter().map(|r| r.key).collect::<Vec<_>>()
        };
        let actual_results = {
            let idx = after.get_index(b"test_idx").expect("index exists");
            let ids = idx.expand_terms(0, "machn", &TermModifier::Fuzzy(1));
            let mut r = idx.search_field_or(0, &ids, None, None, 10);
            r.sort_by(|a, b| a.key.cmp(&b.key));
            r.into_iter().map(|r| r.key).collect::<Vec<_>>()
        };
        assert_eq!(actual_results, expected_results);
    }

    /// No `.tfst` sidecar on disk (fresh index, or FT.COMPACT was never
    /// called) -- `load_term_fst_sidecars` must be a silent no-op, leaving
    /// `recovered_from_sidecar` false and the term dict empty (today's
    /// full-rescan behavior).
    #[test]
    fn load_term_fst_sidecars_missing_is_noop() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let mut store = TextStore::new();
        store.set_persist_dir(tmp.path().to_path_buf());
        let field = TextFieldDef::new(Bytes::from_static(b"body"));
        let idx = TextIndex::new(
            Bytes::from_static(b"test_idx"),
            Vec::new(),
            vec![field],
            crate::text::types::BM25Config::default(),
        );
        store
            .create_index(Bytes::from_static(b"test_idx"), idx)
            .expect("create_index");

        store.load_term_fst_sidecars();

        let idx = store.get_index(b"test_idx").expect("index exists");
        assert!(!idx.recovered_from_sidecar);
        assert_eq!(idx.field_term_dicts[0].term_count(), 0);
    }

    /// A sidecar written for a 1-field schema must NOT be applied to an
    /// index that now has 2 fields (schema changed since the sidecar was
    /// written) -- fail closed rather than silently misapplying ids to the
    /// wrong field.
    #[test]
    fn load_term_fst_sidecars_field_count_mismatch_falls_back() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let docs: &[(&str, &str)] = &[("doc:0", "machine vision")];
        let mut before = make_store_with_docs(tmp.path(), docs);
        {
            let idx = before.get_index_mut(b"test_idx").expect("index exists");
            idx.build_fst();
        }
        before.save_term_fst_sidecar_for_index(b"test_idx");

        let mut after = TextStore::new();
        after.set_persist_dir(tmp.path().to_path_buf());
        let idx = TextIndex::new(
            Bytes::from_static(b"test_idx"),
            Vec::new(),
            vec![
                TextFieldDef::new(Bytes::from_static(b"body")),
                TextFieldDef::new(Bytes::from_static(b"body2")),
            ],
            crate::text::types::BM25Config::default(),
        );
        after
            .create_index(Bytes::from_static(b"test_idx"), idx)
            .expect("create_index");

        after.load_term_fst_sidecars();

        let idx = after.get_index(b"test_idx").expect("index exists");
        assert!(
            !idx.recovered_from_sidecar,
            "field-count mismatch must fail closed, not partially apply"
        );
        assert_eq!(idx.field_term_dicts[0].term_count(), 0);
        assert_eq!(idx.field_term_dicts[1].term_count(), 0);
    }

    // ── v0.1.10 G-1: BM25 AS_OF MVCC filter ──────────────────────────────

    /// Doc with no recorded insert_lsn (pre-MVCC) is always visible.
    #[test]
    fn test_is_doc_visible_at_pre_mvcc_doc_always_visible() {
        let idx = make_index_with_docs(&[("doc:0", "alpha")]);
        // No insert_lsn recorded (via index_document, not index_document_with_lsn)
        assert!(
            idx.is_doc_visible_at(0, 0),
            "as_of_lsn=0 always visible (no filter)"
        );
        assert!(
            idx.is_doc_visible_at(0, 100),
            "pre-MVCC doc visible at any AS_OF"
        );
    }

    /// Doc inserted at lsn=50 is visible at AS_OF>=50 and invisible before.
    #[test]
    fn test_is_doc_visible_at_honours_insert_lsn() {
        let mut idx = make_index_with_docs(&[("doc:post", "alpha")]);
        // Retrieve the doc_id that index_document assigned, then record insert_lsn
        let key_hash = 0u64;
        let doc_id = *idx.key_hash_to_doc_id.get(&key_hash).expect("doc indexed");
        idx.set_doc_insert_lsn(doc_id, 50);

        assert!(!idx.is_doc_visible_at(doc_id, 49), "AS_OF before insert");
        assert!(idx.is_doc_visible_at(doc_id, 50), "AS_OF == insert");
        assert!(idx.is_doc_visible_at(doc_id, 99), "AS_OF after insert");
    }

    /// search_field_as_of with as_of_lsn=0 passes through unchanged (regression guard).
    #[test]
    fn test_search_field_as_of_zero_lsn_passthrough() {
        let idx =
            make_index_with_docs(&[("doc:0", "alpha"), ("doc:1", "alpha"), ("doc:2", "alpha")]);
        let terms = vec!["alpha".to_string()];
        let baseline = idx.search_field(0, &terms, None, None, 10);
        let as_of = idx.search_field_as_of(0, &terms, None, None, 10, 0);
        assert_eq!(
            baseline.len(),
            as_of.len(),
            "as_of_lsn=0 must not drop hits"
        );
        // Ensure the same set of keys is returned
        let mut baseline_keys: Vec<&[u8]> = baseline.iter().map(|r| r.key.as_ref()).collect();
        let mut as_of_keys: Vec<&[u8]> = as_of.iter().map(|r| r.key.as_ref()).collect();
        baseline_keys.sort();
        as_of_keys.sort();
        assert_eq!(baseline_keys, as_of_keys);
    }

    /// search_field_as_of excludes post-snapshot docs.
    #[test]
    fn test_search_field_as_of_excludes_post_snapshot() {
        let mut idx = make_index_with_docs(&[
            ("doc:pre:0", "alpha"),
            ("doc:pre:1", "alpha"),
            ("doc:post:0", "alpha"),
            ("doc:post:1", "alpha"),
        ]);
        // Pre-snapshot: inserted at lsn 10, 20
        idx.set_doc_insert_lsn(*idx.key_hash_to_doc_id.get(&0).unwrap(), 10);
        idx.set_doc_insert_lsn(*idx.key_hash_to_doc_id.get(&1).unwrap(), 20);
        // Snapshot LSN = 25
        // Post-snapshot: inserted at lsn 30, 40
        idx.set_doc_insert_lsn(*idx.key_hash_to_doc_id.get(&2).unwrap(), 30);
        idx.set_doc_insert_lsn(*idx.key_hash_to_doc_id.get(&3).unwrap(), 40);

        let terms = vec!["alpha".to_string()];
        let results = idx.search_field_as_of(0, &terms, None, None, 10, 25);

        let keys: std::collections::HashSet<&[u8]> =
            results.iter().map(|r| r.key.as_ref()).collect();
        assert!(
            keys.contains(b"doc:pre:0".as_ref()),
            "pre-snapshot doc visible"
        );
        assert!(
            keys.contains(b"doc:pre:1".as_ref()),
            "pre-snapshot doc visible"
        );
        assert!(
            !keys.contains(b"doc:post:0".as_ref()),
            "post-snapshot doc excluded"
        );
        assert!(
            !keys.contains(b"doc:post:1".as_ref()),
            "post-snapshot doc excluded"
        );
    }

    /// `index_document_with_lsn` returns the assigned doc_id AND records insert_lsn.
    #[test]
    fn test_index_document_with_lsn_records_lsn_and_returns_doc_id() {
        use crate::protocol::Frame;
        use crate::text::types::BM25Config;
        let field = TextFieldDef::new(Bytes::from_static(b"body"));
        let mut idx = TextIndex::new(
            Bytes::from_static(b"t"),
            Vec::new(),
            vec![field],
            BM25Config::default(),
        );
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"body")),
            Frame::BulkString(Bytes::from_static(b"alpha")),
        ];
        let doc_id = idx.index_document_with_lsn(42, b"doc:x", &args, 77);
        assert_eq!(doc_id, 0, "first doc gets id 0");
        assert_eq!(
            idx.doc_id_to_insert_lsn.get(&0).copied(),
            Some(77),
            "insert_lsn recorded"
        );
        assert!(idx.is_doc_visible_at(0, 77));
        assert!(!idx.is_doc_visible_at(0, 76));
    }

    // ── K4 accounting spine: TextIndex/TextStore resident_bytes ──────────

    #[test]
    fn resident_bytes_zero_for_empty_index() {
        let idx = make_index_with_docs(&[]);
        assert_eq!(idx.resident_bytes(), 0);
    }

    #[test]
    fn resident_bytes_grows_with_indexed_docs() {
        let empty = make_index_with_docs(&[]);
        let indexed = make_index_with_docs(&[
            ("doc:1", "the quick brown fox jumps over the lazy dog"),
            ("doc:2", "a completely different sentence about cats"),
        ]);
        assert!(
            indexed.resident_bytes() > empty.resident_bytes(),
            "indexed docs must grow resident_bytes: empty={} indexed={}",
            empty.resident_bytes(),
            indexed.resident_bytes()
        );
    }

    /// RED-first (K4 stage 2 contract): an empty `TextStore` must report 0,
    /// and creating an index + indexing documents must strictly grow the
    /// store-level total -- the aggregate the elastic memory budget's
    /// used-term (`ShardStoreMemory::text`) now sees.
    #[test]
    fn text_store_resident_bytes_grows_after_indexing() {
        let mut store = TextStore::new();
        assert_eq!(store.resident_bytes(), 0, "empty store reports 0");

        let idx = make_index_with_docs(&[
            ("doc:1", "the quick brown fox jumps over the lazy dog"),
            ("doc:2", "a completely different sentence about cats"),
            ("doc:3", "yet another document with distinct vocabulary"),
        ]);
        store
            .create_index(Bytes::from_static(b"test_idx"), idx)
            .expect("create_index");

        assert!(
            store.resident_bytes() > 0,
            "store with an indexed document must report > 0"
        );
    }

    /// K4 (P0 fix): RED-first -- the O(1) incremental accumulator
    /// (`resident_bytes_extra` plus the per-field `PostingStore`/
    /// `TermDictionary` caches) must never drift from a from-scratch
    /// ground-truth recompute, across a mixed sequence of: indexing N docs
    /// across TEXT + TAG + NUMERIC fields, an upsert that changes all three,
    /// an FST build (structural resync event), a partial hard-delete, and
    /// draining the index back to empty.
    #[test]
    fn resident_bytes_matches_ground_truth_after_mixed_mutations() {
        use crate::protocol::Frame;
        use crate::text::types::{NumericFieldDef, TagFieldDef};

        let text_field = TextFieldDef::new(Bytes::from_static(b"body"));
        let tag_field = TagFieldDef {
            field_name: Bytes::from_static(b"status"),
            separator: b',',
            case_sensitive: false,
            sortable: false,
            noindex: false,
        };
        let numeric_field = NumericFieldDef {
            field_name: Bytes::from_static(b"score"),
            sortable: false,
            noindex: false,
        };
        let mut idx = TextIndex::new_with_schema(
            Bytes::from_static(b"mixed_idx"),
            Vec::new(),
            vec![text_field],
            vec![tag_field],
            vec![numeric_field],
            BM25Config::default(),
        );
        assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());

        let make_args = |body: &str, status: &str, score: &str| {
            vec![
                Frame::BulkString(Bytes::from_static(b"body")),
                Frame::BulkString(Bytes::copy_from_slice(body.as_bytes())),
                Frame::BulkString(Bytes::from_static(b"status")),
                Frame::BulkString(Bytes::copy_from_slice(status.as_bytes())),
                Frame::BulkString(Bytes::from_static(b"score")),
                Frame::BulkString(Bytes::copy_from_slice(score.as_bytes())),
            ]
        };

        // K4 (P0 fix): use the REAL xxh64 hash of the key, not a synthetic
        // index -- `remove_doc_by_doc_id` recomputes `key_hash` from the
        // stored key bytes via `xxhash_rust::xxh64::xxh64` to evict
        // `key_hash_to_doc_id` (production callers, e.g. `spsc_handler.rs`,
        // always pass the real hash). A synthetic key_hash would desync that
        // recomputation from the map's actual key, silently leaking the
        // `key_hash_to_doc_id` entry while still uncharging it -- a test-only
        // trap, not a production accounting bug.
        let key_hash_of = |key: &str| xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);

        let docs = [
            ("doc:1", "the quick brown fox", "open,urgent", "1.5"),
            ("doc:2", "a lazy dog sleeps", "closed", "2.0"),
            ("doc:3", "quick fox jumps again", "open", "3.5"),
        ];
        for (key, body, status, score) in docs {
            let key_hash = key_hash_of(key);
            let args = make_args(body, status, score);
            idx.index_document(key_hash, key.as_bytes(), &args);
            idx.tag_index_document(key_hash, key.as_bytes(), &args);
            idx.numeric_index_document(key_hash, key.as_bytes(), &args);
            assert_eq!(
                idx.resident_bytes(),
                idx.resident_bytes_ground_truth(),
                "drift after indexing {key}"
            );
        }

        // Upsert doc:1 -- new TEXT + TAG + NUMERIC content on the same key.
        let key_hash = key_hash_of("doc:1");
        let args = make_args("totally different body text", "closed", "9.9");
        idx.index_document(key_hash, b"doc:1", &args);
        idx.tag_index_document(key_hash, b"doc:1", &args);
        idx.numeric_index_document(key_hash, b"doc:1", &args);
        assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());

        // FST build -- structural resync event, not a periodic walk.
        idx.build_fst();
        assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());

        // Partial hard-delete: remove doc:2 by its assigned doc_id (1 --
        // insertion order, 0-based, matches `docs` above).
        idx.remove_doc_by_doc_id(1);
        assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());

        // Drain the rest -- resident_bytes must settle back near zero,
        // matching ground truth exactly (entry-overhead floors survive,
        // matching PostingStore/TermDictionary's "entries never removed"
        // contract).
        idx.remove_doc_by_doc_id(0);
        idx.remove_doc_by_doc_id(2);
        assert_eq!(idx.resident_bytes(), idx.resident_bytes_ground_truth());
    }

    /// K4 (P0 fix): `resident_bytes()` must be a pure O(1) load with no
    /// iteration in the accessor -- enforced by construction here: called
    /// 100k times against a 5,000-doc index, well within a wall-clock budget
    /// that an O(n) walk (the reviewer measured 6.4ms/call at 50K docs)
    /// would blow through by orders of magnitude.
    #[test]
    fn resident_bytes_is_o1_not_a_walk() {
        let keys: Vec<String> = (0..5_000).map(|i| format!("doc:{i}")).collect();
        let docs: Vec<(&str, &str)> = keys
            .iter()
            .map(|k| {
                (
                    k.as_str(),
                    "the quick brown fox jumps over the lazy dog and keeps going",
                )
            })
            .collect();
        let idx = make_index_with_docs(&docs);

        let start = std::time::Instant::now();
        for _ in 0..100_000 {
            std::hint::black_box(idx.resident_bytes());
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(300),
            "100k reads of resident_bytes() took {elapsed:?} -- looks like a walk, not O(1)"
        );
    }
}

// Plan 152-06 TAG storage tests live in a sibling file so runtime code in
// store.rs + pre-existing BM25 tests stay under the 1500-LOC cap.
#[cfg(test)]
#[cfg(feature = "text-index")]
#[path = "store_tag_tests.rs"]
mod tag_tests;

// Plan 152-07 NUMERIC storage tests — same sibling-file pattern as TAG tests.
#[cfg(test)]
#[cfg(feature = "text-index")]
#[path = "store_numeric_tests.rs"]
mod numeric_tests;
