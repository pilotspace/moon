// TextStore and TextIndex -- per-shard text index registry.
//
// TextStore mirrors the VectorStore pattern: a HashMap of named TextIndex
// instances, each holding per-field analyzers, posting stores, term
// dictionaries, and BM25 field statistics. TextIndex.index_document()
// is the core indexing entry point called from auto_index_hset.

use bytes::Bytes;
use std::collections::HashMap;

use crate::text::analyzer::AnalyzerPipeline;
use crate::text::bm25::{FieldStats, bm25_score};
use crate::text::index_persist::TextIndexMeta;
use crate::text::posting::PostingStore;
use crate::text::term_dict::TermDictionary;
#[cfg(feature = "text-index")]
use crate::text::types::TagFieldDef;
use crate::text::types::{BM25Config, TextFieldDef};

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
            #[cfg(feature = "text-index")]
            tag_fields: Vec::new(),
            #[cfg(feature = "text-index")]
            tag_indexes: HashMap::new(),
            #[cfg(feature = "text-index")]
            doc_tag_entries: HashMap::new(),
        }
    }

    /// Create a TextIndex with an explicit TAG schema (Plan 152-06).
    ///
    /// This is the constructor used by `FT.CREATE` when the parsed schema
    /// includes TAG fields. Text-only callers continue to use `new()` — the
    /// signature for `new()` is unchanged, so the 33 existing call sites
    /// compile untouched.
    ///
    /// The outer `tag_indexes` map is seeded with one empty inner map per
    /// declared TAG field so `search_tag` on never-inserted fields returns
    /// empty-but-present rather than missing-key (determinism).
    #[cfg(feature = "text-index")]
    pub fn new_with_schema(
        name: Bytes,
        key_prefixes: Vec<Bytes>,
        text_fields: Vec<TextFieldDef>,
        tag_fields: Vec<TagFieldDef>,
        bm25_config: BM25Config,
    ) -> Self {
        let mut idx = Self::new(name, key_prefixes, text_fields, bm25_config);
        for tag_def in &tag_fields {
            idx.tag_indexes
                .entry(tag_def.field_name.clone())
                .or_default();
        }
        idx.tag_fields = tag_fields;
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
        self.doc_id_to_key
            .insert(id, Bytes::copy_from_slice(key));
        id
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

        // Store key mapping
        self.key_hash_to_doc_id.insert(key_hash, doc_id);
        self.doc_id_to_key
            .insert(doc_id, Bytes::copy_from_slice(key));

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
            // Safety: term_postings is non-empty (query_terms non-empty guard above)
            let first_posting = self.field_postings[field_idx]
                .get_posting(term_postings[0].1)
                .expect("posting exists: checked above");
            first_posting.doc_ids.clone()
        };

        for (_, term_id) in &term_postings[1..] {
            let posting = self.field_postings[field_idx]
                .get_posting(*term_id)
                .expect("posting exists: checked above");
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
                let posting = self.field_postings[field_idx]
                    .get_posting(*term_id)
                    .expect("posting exists: checked above");

                // Per RESEARCH Pitfall 1: use linear scan (not rank()) for correct TF lookup.
                // term_freqs is in insertion order, NOT sorted doc_id order.
                let tf = posting
                    .doc_ids
                    .iter()
                    .position(|id| id == doc_id)
                    .map(|idx| posting.term_freqs[idx] as f32)
                    .unwrap_or(0.0);

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
                        self.fst_maps[field_idx] = Some(map);
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

                // Linear scan TF lookup (same as search_field — insertion order, not rank).
                let tf = posting
                    .doc_ids
                    .iter()
                    .position(|id| id == doc_id)
                    .map(|idx| posting.term_freqs[idx] as f32)
                    .unwrap_or(0.0);

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
        let prior = self.doc_tag_entries.remove(&doc_id).unwrap_or_default();
        let mut next: smallvec::SmallVec<[(Bytes, Bytes); 8]> = smallvec::SmallVec::new();
        for (field, value) in prior.into_iter() {
            let is_touched = touched.iter().any(|f| f == &field);
            if is_touched {
                if let Some(field_map) = self.tag_indexes.get_mut(&field) {
                    if let Some(bm) = field_map.get_mut(&value) {
                        bm.remove(doc_id);
                        if bm.is_empty() {
                            field_map.remove(&value);
                        }
                    }
                }
            } else {
                next.push((field, value));
            }
        }

        // Insert fresh entries for each touched field.
        for tag_def in &self.tag_fields {
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
            let field_map = self
                .tag_indexes
                .entry(canonical_field.clone())
                .or_default();
            for value in seen.into_iter() {
                field_map
                    .entry(value.clone())
                    .or_default()
                    .insert(doc_id);
                next.push((canonical_field.clone(), value));
            }
        }

        if !next.is_empty() {
            self.doc_tag_entries.insert(doc_id, next);
        }
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
}

impl TextStore {
    /// Create an empty TextStore.
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            persist_dir: None,
        }
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

    /// Collect schema-only metadata from all text indexes for persistence.
    pub fn collect_index_metas(&self) -> Vec<TextIndexMeta> {
        self.indexes
            .values()
            .map(|idx| TextIndexMeta {
                name: idx.name.clone(),
                bm25_config: idx.bm25_config,
                key_prefixes: idx.key_prefixes.clone(),
                text_fields: idx.text_fields.clone(),
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
        Ok(())
    }

    /// Drop a text index by name. Returns true if it existed.
    pub fn drop_index(&mut self, name: &[u8]) -> bool {
        let removed = self.indexes.remove(name).is_some();
        if removed {
            self.save_index_meta_sidecar();
        }
        removed
    }

    /// Get a read-only reference to a text index.
    pub fn get_index(&self, name: &[u8]) -> Option<&TextIndex> {
        self.indexes.get(name)
    }

    /// Get a mutable reference to a text index.
    pub fn get_index_mut(&mut self, name: &[u8]) -> Option<&mut TextIndex> {
        self.indexes.get_mut(name)
    }

    /// Find all index names whose key_prefixes match the given key.
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

    /// List all index names (for FT._LIST).
    pub fn index_names(&self) -> Vec<Bytes> {
        self.indexes.keys().cloned().collect()
    }

    /// Number of text indexes.
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }

    /// Save FST sidecar for a specific index. No-op if persist_dir not set.
    ///
    /// Called after `TextIndex::build_fst()` at FT.COMPACT time (D-11).
    #[cfg(feature = "text-index")]
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

    /// Load FST sidecars for all indexes. Called during startup/recovery (D-11).
    ///
    /// If a sidecar is missing for an index, that index's fst_maps remain None
    /// (fuzzy/prefix queries will fall back to HashMap brute-force, D-13).
    #[cfg(feature = "text-index")]
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
                                            Ok(map) => idx.fst_maps[field_idx] = Some(map),
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
            assert!(
                r.score > 0.0 || r.score == 0.0,
                "Score must be non-negative"
            );
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

    // ── TAG storage tests (Plan 152-06) ──────────────────────────────────────

    fn tag_args(pairs: &[(&[u8], &[u8])]) -> Vec<crate::protocol::Frame> {
        let mut v = Vec::with_capacity(pairs.len() * 2);
        for (k, val) in pairs {
            v.push(crate::protocol::Frame::BulkString(Bytes::copy_from_slice(k)));
            v.push(crate::protocol::Frame::BulkString(Bytes::copy_from_slice(val)));
        }
        v
    }

    fn tag_only_index(tag_names: &[&[u8]]) -> TextIndex {
        use crate::text::types::BM25Config;
        let tag_fields: Vec<TagFieldDef> = tag_names
            .iter()
            .map(|n| TagFieldDef::new(Bytes::copy_from_slice(n)))
            .collect();
        TextIndex::new_with_schema(
            Bytes::from_static(b"tag_idx"),
            Vec::new(),
            Vec::new(),
            tag_fields,
            BM25Config::default(),
        )
    }

    #[test]
    fn tag_field_def_defaults() {
        let def = TagFieldDef::new(Bytes::from_static(b"status"));
        assert_eq!(def.field_name.as_ref(), b"status");
        assert_eq!(def.separator, b',');
        assert!(!def.case_sensitive);
        assert!(!def.sortable);
        assert!(!def.noindex);
    }

    #[test]
    fn new_with_schema_seeds_tag_indexes() {
        let idx = tag_only_index(&[b"status", b"priority"]);
        assert_eq!(idx.tag_fields.len(), 2);
        // Outer map seeded with empty inner map per declared field.
        assert!(idx.tag_indexes.contains_key(&Bytes::from_static(b"status")));
        assert!(idx.tag_indexes.contains_key(&Bytes::from_static(b"priority")));
        assert!(idx.text_fields.is_empty());
        assert!(idx.field_analyzers.is_empty());
    }

    #[test]
    fn new_preserves_legacy_signature_and_tag_fields_empty() {
        use crate::text::types::BM25Config;
        let idx = TextIndex::new(
            Bytes::from_static(b"legacy"),
            Vec::new(),
            vec![TextFieldDef::new(Bytes::from_static(b"body"))],
            BM25Config::default(),
        );
        assert!(idx.tag_fields.is_empty());
        assert!(idx.tag_indexes.is_empty());
        assert_eq!(idx.text_fields.len(), 1);
    }

    #[test]
    fn tag_index_document_multi_field() {
        let mut idx = tag_only_index(&[b"status", b"priority"]);
        let args = tag_args(&[(b"status", b"open"), (b"priority", b"high")]);
        idx.tag_index_document(0xdead_beef, b"doc:1", &args);
        assert_eq!(idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open")).len(), 1);
        assert_eq!(idx.search_tag(&Bytes::from_static(b"priority"), &Bytes::from_static(b"high")).len(), 1);
    }

    #[test]
    fn tag_index_document_multi_value_split_default_comma() {
        let mut idx = tag_only_index(&[b"tags"]);
        let args = tag_args(&[(b"tags", b"a,b,c")]);
        idx.tag_index_document(1, b"doc:x", &args);
        for v in [b"a", b"b", b"c"] {
            let r = idx.search_tag(&Bytes::from_static(b"tags"), &Bytes::copy_from_slice(v));
            assert_eq!(r.len(), 1, "tag {:?} missing", v);
        }
    }

    #[test]
    fn tag_index_document_custom_separator() {
        use crate::text::types::BM25Config;
        let mut tag_def = TagFieldDef::new(Bytes::from_static(b"tags"));
        tag_def.separator = b';';
        let mut idx = TextIndex::new_with_schema(
            Bytes::from_static(b"sep"),
            Vec::new(),
            Vec::new(),
            vec![tag_def],
            BM25Config::default(),
        );
        let args = tag_args(&[(b"tags", b"a;b;c")]);
        idx.tag_index_document(2, b"doc:y", &args);
        for v in [b"a", b"b", b"c"] {
            assert_eq!(
                idx.search_tag(&Bytes::from_static(b"tags"), &Bytes::copy_from_slice(v)).len(),
                1
            );
        }
    }

    #[test]
    fn tag_default_case_insensitive_and_fast_path() {
        let mut idx = tag_only_index(&[b"status"]);
        let args = tag_args(&[(b"status", b"Open")]);
        idx.tag_index_document(10, b"d:1", &args);
        // Query can be mixed-case; search_tag normalizes.
        let r = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"));
        assert_eq!(r.len(), 1);
        let r_up = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"OPEN"));
        assert_eq!(r_up.len(), 1);
    }

    #[test]
    fn tag_case_sensitive_preserves_case() {
        use crate::text::types::BM25Config;
        let mut tag_def = TagFieldDef::new(Bytes::from_static(b"code"));
        tag_def.case_sensitive = true;
        let mut idx = TextIndex::new_with_schema(
            Bytes::from_static(b"cs"),
            Vec::new(),
            Vec::new(),
            vec![tag_def],
            BM25Config::default(),
        );
        let args = tag_args(&[(b"code", b"Open")]);
        idx.tag_index_document(20, b"d:2", &args);
        assert_eq!(idx.search_tag(&Bytes::from_static(b"code"), &Bytes::from_static(b"Open")).len(), 1);
        // Lowercased lookup should NOT match on case-sensitive field.
        assert_eq!(idx.search_tag(&Bytes::from_static(b"code"), &Bytes::from_static(b"open")).len(), 0);
    }

    #[test]
    fn tag_per_field_upsert_preserves_untouched() {
        // Blocker 4: partial HSET must not clobber prior entries for absent fields.
        let mut idx = tag_only_index(&[b"status", b"priority"]);
        let args1 = tag_args(&[(b"status", b"open"), (b"priority", b"high")]);
        idx.tag_index_document(100, b"doc:X", &args1);
        // Second HSET touches only priority.
        let args2 = tag_args(&[(b"priority", b"low")]);
        idx.tag_index_document(100, b"doc:X", &args2);
        // status=open must still match doc_id=0
        let status_open = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"));
        assert_eq!(status_open.len(), 1, "status=open must be preserved by partial HSET");
        let prio_high = idx.search_tag(&Bytes::from_static(b"priority"), &Bytes::from_static(b"high"));
        assert_eq!(prio_high.len(), 0, "prior priority=high must be revoked");
        let prio_low = idx.search_tag(&Bytes::from_static(b"priority"), &Bytes::from_static(b"low"));
        assert_eq!(prio_low.len(), 1, "new priority=low must be indexed");
    }

    #[test]
    fn tag_full_upsert_revokes_old() {
        let mut idx = tag_only_index(&[b"status", b"priority"]);
        let args1 = tag_args(&[(b"status", b"open"), (b"priority", b"high")]);
        idx.tag_index_document(200, b"doc:Y", &args1);
        let args2 = tag_args(&[(b"status", b"closed"), (b"priority", b"low")]);
        idx.tag_index_document(200, b"doc:Y", &args2);
        assert_eq!(idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open")).len(), 0);
        assert_eq!(idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"closed")).len(), 1);
    }

    #[test]
    fn search_tag_case_insensitive_field_resolution() {
        // Blocker 2: @Status:{open} on index `status` must match.
        let mut idx = tag_only_index(&[b"status"]);
        let args = tag_args(&[(b"status", b"open")]);
        idx.tag_index_document(300, b"doc:Z", &args);
        let r = idx.search_tag(&Bytes::from_static(b"Status"), &Bytes::from_static(b"open"));
        assert_eq!(r.len(), 1, "mixed-case field lookup must resolve canonical `status`");
    }

    #[test]
    fn search_tag_unknown_field_returns_empty() {
        let idx = tag_only_index(&[b"status"]);
        let r = idx.search_tag(&Bytes::from_static(b"missing"), &Bytes::from_static(b"open"));
        assert!(r.is_empty());
    }

    #[test]
    fn tag_ensure_doc_id_shared_allocator() {
        // Blocker 7: tag_index_document and index_document share ensure_doc_id.
        // Whichever method touches a key_hash first owns the doc_id allocation;
        // the second method reuses that doc_id instead of allocating anew.
        use crate::text::types::BM25Config;
        let mut idx = TextIndex::new_with_schema(
            Bytes::from_static(b"shared"),
            Vec::new(),
            vec![TextFieldDef::new(Bytes::from_static(b"body"))],
            vec![TagFieldDef::new(Bytes::from_static(b"status"))],
            BM25Config::default(),
        );
        // Feed TAG first: allocates doc_id=0 via ensure_doc_id.
        let tag_args_v = tag_args(&[(b"status", b"open")]);
        idx.tag_index_document(777, b"doc:shared", &tag_args_v);
        let tag_ids = idx.search_tag(&Bytes::from_static(b"status"), &Bytes::from_static(b"open"));
        assert_eq!(tag_ids, vec![0]);
        // Key mapping must be present at doc_id=0.
        assert_eq!(idx.key_hash_to_doc_id.get(&777), Some(&0));
        assert_eq!(idx.doc_id_to_key.get(&0).map(|b| b.as_ref()), Some(&b"doc:shared"[..]));
        // Feed TEXT second for the SAME key_hash: index_document detects the
        // existing mapping and reuses doc_id=0 (its own upsert branch).
        let txt_args = tag_args(&[(b"body", b"hello world")]);
        idx.index_document(777, b"doc:shared", &txt_args);
        // No second doc_id allocation.
        assert_eq!(idx.key_hash_to_doc_id.len(), 1);
        assert_eq!(idx.key_hash_to_doc_id.get(&777), Some(&0));
        // A third touch on the same key must also reuse doc_id=0.
        idx.tag_index_document(777, b"doc:shared", &tag_args_v);
        assert_eq!(idx.key_hash_to_doc_id.len(), 1);
    }

    #[test]
    fn tag_empty_schema_noop() {
        // Tests the early return: tag_fields.is_empty() ⇒ tag_index_document is a no-op.
        use crate::text::types::BM25Config;
        let mut idx = TextIndex::new(
            Bytes::from_static(b"noop"),
            Vec::new(),
            vec![TextFieldDef::new(Bytes::from_static(b"body"))],
            BM25Config::default(),
        );
        let args = tag_args(&[(b"status", b"open")]);
        idx.tag_index_document(888, b"doc:empty", &args);
        assert!(idx.tag_indexes.is_empty());
        assert!(idx.doc_tag_entries.is_empty());
        // No doc_id should be allocated (ensure_doc_id is only called if tag_fields non-empty).
        assert!(idx.doc_id_to_key.is_empty());
    }

    #[test]
    fn pre_parse_field_filter_tag_basic() {
        use crate::command::vector_search::ft_text_search::{pre_parse_field_filter, FieldFilter};
        let r = pre_parse_field_filter(b"@status:{open}").expect("ok");
        let clause = r.expect("some");
        assert!(clause.terms.is_empty());
        assert_eq!(clause.field_name, None);
        match clause.filter {
            Some(FieldFilter::Tag { field, value }) => {
                assert_eq!(field.as_ref(), b"status");
                assert_eq!(value.as_ref(), b"open");
            }
            _ => panic!("expected Tag filter"),
        }
    }

    #[test]
    fn pre_parse_field_filter_falls_through_for_bm25() {
        use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
        // Paren form — fall through to BM25.
        assert!(pre_parse_field_filter(b"@title:(machine learning)").unwrap().is_none());
        // Bare-terms query — not a field filter.
        assert!(pre_parse_field_filter(b"machine learning").unwrap().is_none());
    }

    #[test]
    fn pre_parse_field_filter_errors() {
        use crate::command::vector_search::ft_text_search::pre_parse_field_filter;
        // Unterminated brace.
        assert!(pre_parse_field_filter(b"@status:{open").is_err());
        // Multi-tag OR rejected.
        let err = pre_parse_field_filter(b"@status:{open|closed}").unwrap_err();
        assert!(err.contains("multi-tag OR"));
        // Empty field name.
        assert!(pre_parse_field_filter(b"@:{x}").is_err());
        // Value too long (> 4 KiB).
        let long = {
            let mut q = b"@x:{".to_vec();
            q.extend(std::iter::repeat_n(b'A', 4097));
            q.push(b'}');
            q
        };
        assert!(pre_parse_field_filter(&long).is_err());
    }
}
