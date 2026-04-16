// TextStore and TextIndex -- per-shard text index registry.
//
// TextStore mirrors the VectorStore pattern: a HashMap of named TextIndex
// instances, each holding per-field analyzers, posting stores, term
// dictionaries, and BM25 field statistics. TextIndex.index_document()
// is the core indexing entry point called from auto_index_hset.

use bytes::Bytes;
use std::collections::HashMap;

use crate::text::analyzer::AnalyzerPipeline;
use crate::text::bm25::FieldStats;
use crate::text::index_persist::TextIndexMeta;
use crate::text::posting::PostingStore;
use crate::text::term_dict::TermDictionary;
use crate::text::types::{BM25Config, TextFieldDef};

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
    /// Per-document field lengths: doc_id -> lengths per field index.
    pub doc_field_lengths: HashMap<u32, Vec<u32>>,
    /// Key hash -> doc_id mapping (same pattern as VectorIndex).
    pub key_hash_to_doc_id: HashMap<u64, u32>,
    /// doc_id -> original Redis key bytes.
    pub doc_id_to_key: HashMap<u32, Bytes>,
    /// Next doc_id to assign.
    next_doc_id: u32,
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
            doc_field_lengths: HashMap::new(),
            key_hash_to_doc_id: HashMap::new(),
            doc_id_to_key: HashMap::new(),
            next_doc_id: 0,
        }
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
    pub fn index_document(
        &mut self,
        key_hash: u64,
        key: &[u8],
        args: &[crate::protocol::Frame],
    ) {
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
                        self.field_stats[field_idx].total_field_length =
                            self.field_stats[field_idx].total_field_length.saturating_sub(old_len);
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
        self.field_postings.iter().map(|p| p.estimated_bytes()).sum()
    }
}

/// Find a field value in HSET-style pairwise args.
///
/// Args layout: [field1, value1, field2, value2, ...]
/// Returns the raw bytes of the value for the matching field name.
fn find_field_value<'a>(
    args: &'a [crate::protocol::Frame],
    field_name: &[u8],
) -> Option<&'a [u8]> {
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
}
