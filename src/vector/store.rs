//! Per-shard VectorStore -- owns all vector indexes for one shard.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as PubSubRegistry).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::storage::tiered::SegmentHandle;
use crate::vector::filter::PayloadIndex;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::mvcc::manager::TransactionManager;
use crate::vector::segment::compaction;
use crate::vector::segment::{SegmentHolder, SegmentList};
use crate::vector::turbo_quant::collection::{BuildMode, CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::types::DistanceMetric;

pub use crate::vector::segment::compaction::{MergeMode, MergeStats};
// Aliases kept for external callers that reference the old names.
pub use crate::vector::segment::compaction::{
    MergeMode as IndexMergeMode, MergeStats as IndexMergeStats,
};

/// Maximum number of named vector fields per index.
pub const MAX_VECTOR_FIELDS: usize = 8;

/// Per-field vector configuration for multi-vector indexes.
/// Each named vector field has independent dimension, metric, quantization.
#[derive(Clone, Debug)]
pub struct VectorFieldMeta {
    /// Field name in the HASH (e.g., "title_vec", "body_vec").
    pub field_name: Bytes,
    /// Original (unpadded) dimension.
    pub dimension: u32,
    /// Padded dimension (next power of 2).
    pub padded_dimension: u32,
    /// Distance metric for this field.
    pub metric: DistanceMetric,
    /// Quantization config for this field.
    pub quantization: QuantizationConfig,
    /// Build mode for this field.
    pub build_mode: BuildMode,
}

/// Field type variants for mixed-schema indexes (TEXT + VECTOR).
///
/// Used by FT.INFO to report all field types in a unified schema view,
/// and by mixed TEXT+VECTOR indexes to track the complete schema.
#[derive(Debug, Clone)]
pub enum FieldType {
    /// Dense vector field with HNSW index.
    Vector(VectorFieldMeta),
    /// Full-text search field with BM25 scoring.
    Text {
        field_name: Bytes,
        weight: f64,
        nostem: bool,
        sortable: bool,
        noindex: bool,
    },
}

/// Metadata describing a vector index (from FT.CREATE).
#[derive(Clone)]
pub struct IndexMeta {
    /// Index name (e.g., "idx").
    pub name: Bytes,
    /// Original (unpadded) dimension.
    pub dimension: u32,
    /// Padded dimension (next power of 2).
    pub padded_dimension: u32,
    /// Distance metric.
    pub metric: DistanceMetric,
    /// HNSW M parameter (max neighbors per layer).
    pub hnsw_m: u32,
    /// HNSW ef_construction parameter.
    pub hnsw_ef_construction: u32,
    /// HNSW ef_runtime (search beam width). 0 = auto: max(k*15, 200).
    /// Higher = better recall, lower QPS. Range: 10-4096.
    pub hnsw_ef_runtime: u32,
    /// Minimum vectors in mutable segment before auto-compaction triggers.
    /// Lower = more frequent compaction (smaller HNSW graphs, more segments).
    /// Higher = fewer compactions (larger graphs, better recall). Range: 100-100000.
    pub compact_threshold: u32,
    /// The HASH field name that contains the vector blob (e.g., "vec").
    pub source_field: Bytes,
    /// Key prefixes to auto-index (from PREFIX clause).
    pub key_prefixes: Vec<Bytes>,
    /// Quantization algorithm. Default: TurboQuant4.
    pub quantization: QuantizationConfig,
    /// Build mode: Light (fast, less memory) or Exact (higher recall).
    pub build_mode: crate::vector::turbo_quant::collection::BuildMode,
    /// Per-field vector configurations. For single-field indexes (backward compat),
    /// this contains exactly one entry matching the top-level fields.
    /// For multi-vector indexes, each entry describes one named vector field.
    pub vector_fields: Vec<VectorFieldMeta>,
    /// Complete schema field list for mixed-type indexes (TEXT + VECTOR).
    /// Empty for legacy vector-only indexes (backward compatible).
    /// Used by FT.INFO to report all field types in a unified schema view.
    pub schema_fields: Vec<FieldType>,
    /// Merge mode for immutable segment consolidation. Default: GraphUnion.
    /// Set via FT.CREATE … MERGE_MODE GRAPH_UNION|KEEP_RAW|NONE.
    pub merge_mode: MergeMode,
    /// When true, retain raw f32 vectors in memory on ImmutableSegments for
    /// lossless re-quantization during merge. Default: false.
    /// Set via FT.CREATE … KEEP_RAW ON.
    pub keep_raw: bool,
}

impl IndexMeta {
    /// Returns the default (first) vector field.
    /// All indexes have at least one field; single-field indexes use this exclusively.
    pub fn default_field(&self) -> &VectorFieldMeta {
        &self.vector_fields[0]
    }

    /// Case-insensitive lookup of a vector field by name.
    pub fn find_field(&self, name: &[u8]) -> Option<&VectorFieldMeta> {
        self.vector_fields
            .iter()
            .find(|f| f.field_name.eq_ignore_ascii_case(name))
    }

    /// Returns true if this index has more than one vector field.
    pub fn is_multi_field(&self) -> bool {
        self.vector_fields.len() > 1
    }
}

/// Per-field segment storage. Each named vector field has independent
/// segments, scratch space, and collection metadata.
pub struct FieldSegments {
    pub segments: SegmentHolder,
    pub scratch: SearchScratch,
    pub collection: Arc<CollectionMetadata>,
}

/// A single vector index: meta + segments + scratch + collection config.
///
/// The top-level `segments`, `scratch`, `collection` are the DEFAULT field
/// (always `vector_fields[0]`). `field_segments` stores ADDITIONAL named
/// fields (empty for single-field indexes). This pragmatic approach avoids
/// a massive caller migration while supporting multi-field indexes.
pub struct VectorIndex {
    pub meta: IndexMeta,
    /// Default field segments (vector_fields[0]).
    pub segments: SegmentHolder,
    /// Default field scratch space.
    pub scratch: SearchScratch,
    /// Default field collection metadata.
    pub collection: Arc<CollectionMetadata>,
    pub payload_index: PayloadIndex,
    /// Maps `key_hash` (xxh64 of original Redis hash key) → original key bytes.
    ///
    /// Populated at insert time via `auto_index_hset`. Used by `FT.SEARCH` to
    /// return the original Redis key (e.g., `doc:1755`) instead of the internal
    /// `vec:<internal_id>` form. Survives compaction and segment merging because
    /// it's keyed by the stable `key_hash`, not the volatile internal ID.
    pub key_hash_to_key: std::collections::HashMap<u64, Bytes>,
    /// Maps `key_hash` → `global_id` for metadata-only updates.
    ///
    /// When `HSET doc:1 category "science"` is called without a vector blob,
    /// the auto-indexer looks up the existing `global_id` here to update the
    /// PayloadIndex for that vector without re-inserting it.
    pub key_hash_to_global_id: std::collections::HashMap<u64, u32>,
    /// Whether auto-compaction is enabled. Default: true.
    /// Set to false via FT.CONFIG SET idx AUTOCOMPACT OFF for bulk ingestion.
    /// Manual FT.COMPACT always works regardless of this flag.
    pub autocompact_enabled: bool,
    /// Additional named vector fields (beyond the default field).
    /// Empty for single-field indexes. Keyed by field_name from VectorFieldMeta.
    pub field_segments: HashMap<Bytes, FieldSegments>,
    /// Sparse vector stores, keyed by field name.
    /// Populated when FT.CREATE includes SPARSE field declarations.
    pub sparse_stores: HashMap<Bytes, crate::vector::sparse::store::SparseStore>,
}

/// Default minimum vector count to trigger compaction before search.
/// Overridden by IndexMeta.compact_threshold when set via FT.CREATE.
const DEFAULT_COMPACT_THRESHOLD: usize = 1000;

impl VectorIndex {
    /// Returns all vector field names (default + additional).
    pub fn all_field_names(&self) -> Vec<&Bytes> {
        let mut names = vec![&self.meta.vector_fields[0].field_name];
        for name in self.field_segments.keys() {
            names.push(name);
        }
        names
    }

    /// Look up segment holder, scratch, and collection for a named field.
    /// Returns the default field's data if `name` matches `vector_fields[0]`,
    /// otherwise looks up `field_segments`.
    pub fn field_segment_holder(
        &self,
        name: &[u8],
    ) -> Option<(&SegmentHolder, &SearchScratch, &Arc<CollectionMetadata>)> {
        let default_name = &self.meta.vector_fields[0].field_name;
        if default_name.eq_ignore_ascii_case(name) {
            return Some((&self.segments, &self.scratch, &self.collection));
        }
        self.field_segments
            .get(name)
            .map(|fs| (&fs.segments, &fs.scratch, &fs.collection))
    }

    /// Mutable version of `field_segment_holder`.
    pub fn field_segment_holder_mut(
        &mut self,
        name: &[u8],
    ) -> Option<(
        &mut SegmentHolder,
        &mut SearchScratch,
        &Arc<CollectionMetadata>,
    )> {
        let default_name = &self.meta.vector_fields[0].field_name;
        if default_name.eq_ignore_ascii_case(name) {
            return Some((&mut self.segments, &mut self.scratch, &self.collection));
        }
        self.field_segments
            .get_mut(name)
            .map(|fs| (&mut fs.segments, &mut fs.scratch, &fs.collection))
    }

    /// Compact the mutable segment into an immutable HNSW segment if beneficial.
    ///
    /// Triggered lazily on first search when the mutable segment exceeds the
    /// threshold and no immutable segments exist yet. After compaction, searches
    /// use HNSW (O(log n)) instead of brute force (O(n)).
    ///
    /// This is a blocking operation (builds HNSW graph). For production, this
    /// should be moved to a background task with async notification.
    pub fn try_compact(&mut self) {
        if !self.autocompact_enabled {
            return;
        }
        let mutable_len;
        {
            let snapshot = self.segments.load();
            mutable_len = snapshot.mutable.len();
        } // drop snapshot guard before freeze/compact

        let threshold = if self.meta.compact_threshold > 0 {
            self.meta.compact_threshold as usize
        } else {
            DEFAULT_COMPACT_THRESHOLD
        };
        if mutable_len >= threshold {
            Self::compact_segments(
                &mut self.segments,
                &mut self.scratch,
                &self.collection,
                self.meta.dimension,
            );
        }
        // Also try compact additional fields with the same threshold
        for (_, fs) in &mut self.field_segments {
            let fs_len = fs.segments.load().mutable.len();
            if fs_len >= threshold {
                let dim = fs.collection.dimension;
                Self::compact_segments(&mut fs.segments, &mut fs.scratch, &fs.collection, dim);
            }
        }
    }

    /// Unconditionally compact the mutable segment into an immutable HNSW segment.
    ///
    /// Unlike `try_compact()`, this bypasses the `compact_threshold` check and always
    /// compacts if the mutable segment contains at least 1 vector. Called directly by
    /// the `FT.COMPACT` command (explicit user intent).
    ///
    /// **Note**: Existing immutable segments are NOT merged. Tested experimentally —
    /// decoding TQ4 codes back to f32 then re-encoding accumulates lossy quantization
    /// error and destroys recall (drops from 0.73 → 0.0005 with 14 segments). True
    /// merge requires retaining f32 vectors in immutable segments (memory cost) or
    /// implementing a quantization-aware HNSW union (complex).
    ///
    /// To get a single segment, use a higher `COMPACT_THRESHOLD` so the mutable
    /// segment compacts only once at the end of bulk loading.
    ///
    /// Without `force_compact`, when `compact_threshold >= mutable_len`, FT.COMPACT
    /// silently no-ops, leaving all vectors in brute-force mutable segment
    /// (O(n) search instead of HNSW O(log n)).
    pub fn force_compact(&mut self) {
        // Compact default field
        Self::compact_segments(
            &mut self.segments,
            &mut self.scratch,
            &self.collection,
            self.meta.dimension,
        );
        // Compact additional fields
        for (_, fs) in &mut self.field_segments {
            let dim = fs.collection.dimension;
            Self::compact_segments(&mut fs.segments, &mut fs.scratch, &fs.collection, dim);
        }
    }

    /// Compact a single field's mutable segment into an immutable HNSW segment.
    fn compact_segments(
        segments: &mut SegmentHolder,
        scratch: &mut SearchScratch,
        collection: &Arc<CollectionMetadata>,
        dimension: u32,
    ) {
        let mutable_len = segments.load().mutable.len();
        if mutable_len == 0 {
            return;
        }

        let frozen = segments.load().mutable.freeze();
        let seed = collection.collection_id.wrapping_mul(6364136223846793005);

        match compaction::compact(&frozen, collection, seed, None) {
            Ok(immutable) => {
                let num_nodes = immutable.graph().num_nodes();
                let padded = collection.padded_dimension;
                *scratch = SearchScratch::new(num_nodes, padded);

                let old = segments.load();
                let next_global = old.mutable.next_global_id();
                let mut imm_list = old.immutable.clone();
                imm_list.push(Arc::new(immutable));
                let new_mutable = Arc::new(crate::vector::segment::mutable::MutableSegment::new(
                    dimension,
                    collection.clone(),
                ));
                new_mutable.set_global_id_base(next_global);
                let new_list = SegmentList {
                    mutable: new_mutable,
                    immutable: imm_list,
                    ivf: old.ivf.clone(),
                    warm: old.warm.clone(),
                    cold: old.cold.clone(),
                };
                segments.swap(new_list);
            }
            Err(_e) => {
                // Compaction failed (recall too low, etc.) — fall back to brute force
            }
        }
    }
}

impl VectorIndex {
    /// Check each immutable segment's age. If older than `warm_after_secs`,
    /// transition it to warm tier (mmap-backed on disk).
    ///
    /// After transition, the segment is replaced by a WarmSearchSegment that
    /// reads TQ codes and HNSW graph from mmap'd .mpf files. The segment
    /// remains searchable -- no data loss from the user's perspective.
    ///
    /// Returns the number of segments transitioned.
    pub fn try_warm_transitions(
        &self,
        shard_dir: &std::path::Path,
        manifest: &mut crate::persistence::manifest::ShardManifest,
        warm_after_secs: u64,
        next_file_id: &mut u64,
        wal: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    ) -> usize {
        let snapshot = self.segments.load();
        let mut to_warm: Vec<usize> = Vec::new();
        for (i, imm) in snapshot.immutable.iter().enumerate() {
            if imm.age_secs() >= warm_after_secs {
                to_warm.push(i);
            }
        }
        if to_warm.is_empty() {
            return 0;
        }

        let mut new_immutable = snapshot.immutable.clone();
        let mut new_warm = snapshot.warm.clone();
        let mut transitioned = 0usize;

        // Process in reverse order to maintain valid indices during removal.
        for &idx in to_warm.iter().rev() {
            let imm = &snapshot.immutable[idx];
            let file_id = *next_file_id;
            *next_file_id += 1;

            let graph_bytes = imm.graph().to_bytes_compressed();
            let codes_data = imm.vectors_tq().as_slice();
            let mvcc_data = imm.mvcc_raw_bytes();

            match crate::storage::tiered::warm_tier::transition_to_warm(
                shard_dir,
                file_id, // segment_id == file_id
                file_id,
                codes_data,
                &graph_bytes,
                None, // vectors_data (f16 reranking -- not used yet)
                &mvcc_data,
                manifest,
                wal.as_mut(),
            ) {
                Ok(handle) => {
                    // Remove the old ImmutableSegment from the in-memory list.
                    // The ImmutableSegment is purely in-memory (no on-disk files),
                    // so it needs no SegmentHandle tombstoning -- it's simply dropped.
                    //
                    // Tombstone lifecycle for the NEW warm segment:
                    //   1. `handle` (SegmentHandle) is passed to WarmSearchSegment below
                    //   2. WarmSearchSegment stores it as `_handle` (Arc refcount)
                    //   3. When later transitioned to cold: mark_tombstoned() is called
                    //   4. On index drop: mark_tombstoned() is called
                    //   5. Directory is deleted only when last Arc ref drops AND tombstoned
                    new_immutable.remove(idx);

                    // Open mmap-backed warm search segment to keep data searchable.
                    // transition_to_warm places files at shard_dir/vectors/segment-{id}/
                    let seg_dir = shard_dir.join("vectors").join(format!("segment-{file_id}"));
                    match crate::vector::persistence::warm_search::WarmSearchSegment::from_files(
                        &seg_dir,
                        file_id,
                        self.collection.clone(),
                        handle,
                        false, // mlock_codes: off by default for warm tier
                    ) {
                        Ok(warm_seg) => {
                            new_warm.push(Arc::new(warm_seg));
                            tracing::info!(
                                "Warm transition: segment {} ({} vectors, age {}s) -> searchable warm",
                                file_id,
                                imm.total_count(),
                                imm.age_secs()
                            );
                        }
                        Err(e) => {
                            // Transition wrote files but failed to open for search.
                            // Log error; data is on disk but not searchable until restart.
                            tracing::error!(
                                "Warm search open failed for segment {}: {} (data on disk, not searchable)",
                                file_id,
                                e
                            );
                        }
                    }

                    transitioned += 1;
                }
                Err(e) => {
                    tracing::error!("Warm transition failed for segment {}: {}", file_id, e);
                }
            }
        }

        if transitioned > 0 {
            let new_list = SegmentList {
                mutable: Arc::clone(&snapshot.mutable),
                immutable: new_immutable,
                ivf: snapshot.ivf.clone(),
                warm: new_warm,
                cold: snapshot.cold.clone(),
            };
            self.segments.swap(new_list);
        }
        transitioned
    }
}

impl VectorIndex {
    /// Check each warm segment's age. If older than `cold_after_secs`,
    /// transition it to cold tier (PQ codes in RAM + Vamana graph on NVMe).
    ///
    /// After transition, the warm segment is replaced by a DiskAnnSegment
    /// that performs approximate search via PQ asymmetric distance and
    /// Vamana beam traversal from disk. The warm segment is tombstoned.
    ///
    /// Returns the number of segments transitioned.
    pub fn try_cold_transitions(
        &self,
        shard_dir: &std::path::Path,
        manifest: &mut crate::persistence::manifest::ShardManifest,
        cold_after_secs: u64,
        next_file_id: &mut u64,
    ) -> usize {
        let snapshot = self.segments.load();
        let mut to_cold: Vec<usize> = Vec::new();
        for (i, warm) in snapshot.warm.iter().enumerate() {
            if warm.age_secs() >= cold_after_secs {
                to_cold.push(i);
            }
        }
        if to_cold.is_empty() {
            return 0;
        }

        let mut new_warm = snapshot.warm.clone();
        let mut new_cold = snapshot.cold.clone();
        let mut transitioned = 0usize;
        let dim = self.meta.dimension as usize;

        // Process in reverse order to maintain valid indices during removal.
        for &idx in to_cold.iter().rev() {
            let warm_seg = &snapshot.warm[idx];
            let warm_file_id = warm_seg.segment_id();
            let cold_file_id = *next_file_id;
            *next_file_id += 1;

            match crate::storage::tiered::cold_tier::transition_to_cold(
                shard_dir,
                warm_seg,
                warm_file_id,
                cold_file_id,
                dim,
                manifest,
            ) {
                Ok(diskann_seg) => {
                    new_warm.remove(idx);
                    new_cold.push(Arc::new(diskann_seg));
                    tracing::info!(
                        "Cold transition: segment {} ({} vectors, age {}s) -> DiskANN cold",
                        cold_file_id,
                        warm_seg.total_count(),
                        warm_seg.age_secs(),
                    );
                    // Mark the old warm segment for cleanup when refs drop.
                    warm_seg.mark_tombstoned();
                    transitioned += 1;
                }
                Err(e) => {
                    tracing::error!(
                        "Cold transition failed for warm segment {}: {}",
                        warm_file_id,
                        e
                    );
                }
            }
        }

        if transitioned > 0 {
            let new_list = SegmentList {
                mutable: Arc::clone(&snapshot.mutable),
                immutable: snapshot.immutable.clone(),
                ivf: snapshot.ivf.clone(),
                warm: new_warm,
                cold: new_cold,
            };
            self.segments.swap(new_list);
        }
        transitioned
    }
}

/// Per-shard store of all vector indexes. Directly owned by shard thread.
pub struct VectorStore {
    indexes: HashMap<Bytes, VectorIndex>,
    /// Monotonically increasing collection ID counter.
    next_collection_id: u64,
    /// Per-shard MVCC transaction manager.
    txn_manager: TransactionManager,
    /// Segments recovered from persistence, awaiting FT.CREATE to claim them.
    /// Key: collection_id. Populated during crash recovery.
    pending_segments: HashMap<u64, crate::vector::persistence::recovery::RecoveredCollection>,
    /// Shard directory for persisting index metadata sidecar.
    /// Set once during event loop init when disk-offload is enabled.
    persist_dir: Option<std::path::PathBuf>,
}

impl VectorStore {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            next_collection_id: 1,
            txn_manager: TransactionManager::new(),
            pending_segments: HashMap::new(),
            persist_dir: None,
        }
    }

    /// Set the shard directory for index metadata persistence.
    /// Called once during event loop init when disk-offload is enabled.
    pub fn set_persist_dir(&mut self, dir: std::path::PathBuf) {
        self.persist_dir = Some(dir);
    }

    /// Persist current index metadata to the sidecar file.
    /// No-op if persist_dir is not set (disk-offload disabled).
    fn save_index_meta_sidecar(&self) {
        if let Some(ref dir) = self.persist_dir {
            let metas = self.collect_index_metas();
            if let Err(e) = crate::vector::index_persist::save_index_metadata(dir, &metas) {
                tracing::warn!("Failed to save vector index metadata: {}", e);
            }
        }
    }

    /// Read-only access to the transaction manager.
    #[inline]
    pub fn txn_manager(&self) -> &TransactionManager {
        &self.txn_manager
    }

    /// Mutable access to the transaction manager.
    #[inline]
    pub fn txn_manager_mut(&mut self) -> &mut TransactionManager {
        &mut self.txn_manager
    }

    /// Resident bytes split into `(mutable_bytes, immutable_bytes)` across
    /// all vector indexes on this shard.
    ///
    /// Mutable = brute-force buffers (TQ codes + raw f32 + entries).
    /// Immutable = HNSW graphs + TQ codes + QJL + norms + MVCC headers.
    /// O(index_count * segment_count) -- acceptable for metrics scrape cadence.
    pub fn resident_bytes(&self) -> (usize, usize) {
        let mut total_mutable: usize = 0;
        let mut total_immutable: usize = 0;
        for idx in self.indexes.values() {
            let (m, i) = idx.segments.resident_bytes();
            total_mutable += m;
            total_immutable += i;
            // Additional named fields beyond the default field.
            for fs in idx.field_segments.values() {
                let (m2, i2) = fs.segments.resident_bytes();
                total_mutable += m2;
                total_immutable += i2;
            }
        }
        (total_mutable, total_immutable)
    }

    /// Total count of immutable (sealed HNSW) segments across all indexes and fields.
    ///
    /// Used by the MA1 write-stall guard to detect segment backlog.
    /// O(index_count * field_count) — cheap enough for the 1s sweep tick.
    pub fn total_immutable_segment_count(&self) -> usize {
        let mut count = 0usize;
        for idx in self.indexes.values() {
            let snap = idx.segments.load();
            count += snap.immutable.len();
            for fs in idx.field_segments.values() {
                let fs_snap = fs.segments.load();
                count += fs_snap.immutable.len();
            }
        }
        count
    }

    /// Attach recovered segments from persistence. Called by shard restore.
    ///
    /// Stores recovered collections in pending_segments, keyed by collection_id.
    /// They will be attached to indexes when FT.CREATE runs (or immediately if
    /// the index already exists).
    pub fn attach_recovered(
        &mut self,
        recovered: crate::vector::persistence::recovery::RecoveredState,
    ) {
        for (collection_id, collection) in recovered.collections {
            self.pending_segments.insert(collection_id, collection);
        }
    }

    /// Number of pending (unattached) recovered collections.
    #[allow(dead_code)]
    pub fn pending_count(&self) -> usize {
        self.pending_segments.len()
    }

    /// Create a new index. Returns Err(&str) if index already exists.
    pub fn create_index(&mut self, mut meta: IndexMeta) -> Result<(), &'static str> {
        if self.indexes.contains_key(&meta.name) {
            return Err("Index already exists");
        }

        // Backward compatibility: if vector_fields is empty, populate from top-level fields.
        if meta.vector_fields.is_empty() {
            meta.vector_fields = vec![VectorFieldMeta {
                field_name: meta.source_field.clone(),
                dimension: meta.dimension,
                padded_dimension: padded_dimension(meta.dimension),
                metric: meta.metric,
                quantization: meta.quantization,
                build_mode: meta.build_mode,
            }];
        }
        let collection_id = self.next_collection_id;
        self.next_collection_id += 1;

        let padded = padded_dimension(meta.dimension);
        let collection = Arc::new(CollectionMetadata::with_build_mode(
            collection_id,
            meta.dimension,
            meta.metric,
            meta.quantization,
            collection_id, // use collection_id as seed for determinism
            meta.build_mode,
        ));
        let segments = SegmentHolder::new(meta.dimension, collection.clone());
        let scratch = SearchScratch::new(0, padded);

        // Create additional field segments for multi-field indexes.
        let mut extra_fields = HashMap::new();
        for field_meta in meta.vector_fields.iter().skip(1) {
            let field_cid = self.next_collection_id;
            self.next_collection_id += 1;
            let field_padded = padded_dimension(field_meta.dimension);
            let field_collection = Arc::new(CollectionMetadata::with_build_mode(
                field_cid,
                field_meta.dimension,
                field_meta.metric,
                field_meta.quantization,
                field_cid,
                field_meta.build_mode,
            ));
            let field_segments = SegmentHolder::new(field_meta.dimension, field_collection.clone());
            let field_scratch = SearchScratch::new(0, field_padded);
            extra_fields.insert(
                field_meta.field_name.clone(),
                FieldSegments {
                    segments: field_segments,
                    scratch: field_scratch,
                    collection: field_collection,
                },
            );
        }

        let name = meta.name.clone();
        self.indexes.insert(
            name.clone(),
            VectorIndex {
                meta,
                segments,
                scratch,
                collection,
                payload_index: PayloadIndex::new(),
                key_hash_to_key: std::collections::HashMap::new(),
                key_hash_to_global_id: std::collections::HashMap::new(),
                autocompact_enabled: true,
                field_segments: extra_fields,
                sparse_stores: HashMap::new(),
            },
        );

        // Persist index metadata sidecar
        self.save_index_meta_sidecar();

        // Check if recovered segments exist for this collection_id
        if let Some(recovered) = self.pending_segments.remove(&collection_id) {
            if let Some(index) = self.indexes.get(&name) {
                let mut immutable_arcs: Vec<
                    Arc<crate::vector::segment::immutable::ImmutableSegment>,
                > = Vec::with_capacity(recovered.immutable.len());
                for (imm, _meta) in recovered.immutable {
                    immutable_arcs.push(Arc::new(imm));
                }
                let new_list = crate::vector::segment::SegmentList {
                    mutable: Arc::new(recovered.mutable),
                    immutable: immutable_arcs,
                    ivf: Vec::new(),
                    warm: Vec::new(),
                    cold: Vec::new(),
                };
                index.segments.swap(new_list);
            }
        }

        Ok(())
    }

    /// Drop an index by name. Returns true if it existed.
    ///
    /// Tombstones any warm segments so their on-disk directories are cleaned up
    /// once all in-flight search references (Arc snapshots) are dropped.
    pub fn drop_index(&mut self, name: &[u8]) -> bool {
        if let Some(index) = self.indexes.remove(name) {
            // Tombstone warm segments: mark for deletion on last Arc drop.
            let snapshot = index.segments.load();
            for warm_seg in &snapshot.warm {
                warm_seg.mark_tombstoned();
            }
            // Persist index metadata sidecar
            self.save_index_meta_sidecar();
            true
        } else {
            false
        }
    }

    /// Get index reference by name.
    pub fn get_index(&self, name: &[u8]) -> Option<&VectorIndex> {
        self.indexes.get(name)
    }

    /// Get mutable index reference by name.
    pub fn get_index_mut(&mut self, name: &[u8]) -> Option<&mut VectorIndex> {
        self.indexes.get_mut(name)
    }

    /// List all index names.
    pub fn index_names(&self) -> Vec<&Bytes> {
        self.indexes.keys().collect()
    }

    /// Find indexes whose key_prefixes match the given key.
    /// Returns refs to matching VectorIndex entries.
    pub fn find_matching_indexes(&self, key: &[u8]) -> Vec<&VectorIndex> {
        self.indexes
            .values()
            .filter(|idx| idx.meta.key_prefixes.iter().any(|p| key.starts_with(p)))
            .collect()
    }

    /// Find matching index names for auto-indexing.
    /// Caller must collect names first to avoid borrow issues.
    pub fn find_matching_index_names(&self, key: &[u8]) -> Vec<Bytes> {
        self.indexes
            .iter()
            .filter_map(|(name, idx)| {
                if idx.meta.key_prefixes.iter().any(|p| key.starts_with(p)) {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Mark vectors as deleted for a key that was removed (DEL/HDEL/UNLINK).
    ///
    /// Finds all indexes whose key_prefixes match the key, computes the key_hash,
    /// and marks matching entries as deleted in the mutable segment. This prevents
    /// stale vectors from appearing in search results.
    ///
    /// NOTE: Vec allocation for matching_names is acceptable -- this only fires
    /// when a deleted key matches an index prefix (rare per-operation).
    pub fn mark_deleted_for_key(&mut self, key: &[u8]) {
        let matching_names = self.find_matching_index_names(key);
        if matching_names.is_empty() {
            return;
        }
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        for idx_name in matching_names {
            if let Some(idx) = self.indexes.get(&idx_name) {
                let snap = idx.segments.load();
                snap.mutable.mark_deleted_by_key_hash(key_hash, 1);
            }
        }
    }

    /// Number of indexes.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Collect references to all active IndexMeta for persistence.
    pub fn collect_index_metas(&self) -> Vec<&IndexMeta> {
        self.indexes.values().map(|idx| &idx.meta).collect()
    }

    /// Attempt warm transitions for ALL indexes. Called from persistence tick.
    ///
    /// Returns the total number of segments transitioned across all indexes.
    pub fn try_warm_transitions_all(
        &self,
        shard_dir: &std::path::Path,
        manifest: &mut crate::persistence::manifest::ShardManifest,
        warm_after_secs: u64,
        next_file_id: &mut u64,
        wal: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    ) -> usize {
        let names: Vec<bytes::Bytes> = self.indexes.keys().cloned().collect();
        let mut total = 0;
        for name in names {
            if let Some(idx) = self.indexes.get(&name) {
                total += idx.try_warm_transitions(
                    shard_dir,
                    manifest,
                    warm_after_secs,
                    next_file_id,
                    wal,
                );
            }
        }
        total
    }

    /// Attempt cold transitions for ALL indexes. Called from persistence tick.
    ///
    /// Scans warm segments in each index, transitions those older than
    /// `cold_after_secs` to DiskANN cold tier. Returns total count.
    pub fn try_cold_transitions_all(
        &self,
        shard_dir: &std::path::Path,
        manifest: &mut crate::persistence::manifest::ShardManifest,
        cold_after_secs: u64,
        next_file_id: &mut u64,
    ) -> usize {
        let names: Vec<bytes::Bytes> = self.indexes.keys().cloned().collect();
        let mut total = 0;
        for name in names {
            if let Some(idx) = self.indexes.get(&name) {
                total +=
                    idx.try_cold_transitions(shard_dir, manifest, cold_after_secs, next_file_id);
            }
        }
        total
    }

    /// Register warm segments recovered from disk into the appropriate indexes.
    ///
    /// Called during shard restore after v3 recovery identifies warm-tier segments
    /// in the manifest. For each (segment_id, segment_dir), tries to open a
    /// WarmSearchSegment and add it to whatever index matches the collection metadata.
    pub fn register_warm_segments(&mut self, warm_segments: Vec<(u64, std::path::PathBuf)>) {
        use crate::vector::persistence::warm_search::WarmSearchSegment;

        let mut loaded = 0usize;
        for (segment_id, segment_dir) in &warm_segments {
            // Try each index — the segment belongs to whichever collection's metadata
            // matches the codes data. In practice there's usually one index per shard.
            for idx in self.indexes.values() {
                let handle = SegmentHandle::new(*segment_id, segment_dir.clone());
                match WarmSearchSegment::from_files(
                    segment_dir,
                    *segment_id,
                    idx.collection.clone(),
                    handle,
                    false, // mlock_codes off during recovery (can be changed later)
                ) {
                    Ok(warm_seg) => {
                        let old = idx.segments.load();
                        let mut new_warm = old.warm.clone();
                        new_warm.push(std::sync::Arc::new(warm_seg));
                        let new_list = crate::vector::segment::SegmentList {
                            mutable: std::sync::Arc::clone(&old.mutable),
                            immutable: old.immutable.clone(),
                            ivf: old.ivf.clone(),
                            warm: new_warm,
                            cold: old.cold.clone(),
                        };
                        idx.segments.swap(new_list);
                        loaded += 1;
                        tracing::info!(
                            "Registered warm segment {} from {:?}",
                            segment_id,
                            segment_dir
                        );
                        break; // Segment belongs to one index only
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Warm segment {} not compatible with index: {}",
                            segment_id,
                            e
                        );
                    }
                }
            }
        }
        if loaded > 0 {
            tracing::info!(
                "Registered {}/{} warm segments on startup",
                loaded,
                warm_segments.len()
            );
        }
    }

    /// Enforce the warm-segment mmap budget across all indexes.
    ///
    /// For each `VectorIndex` (default field + named fields), loads the current
    /// `SegmentList`, calls `budget.enforce_budget`, and atomically swaps the
    /// (possibly trimmed) list back. Newly added warm segments (from recent warm
    /// transitions) are registered into the budget before enforcement.
    ///
    /// Returns the total number of segments evicted across all indexes.
    pub fn enforce_mmap_budget_all(
        &self,
        budget: &mut crate::vector::persistence::mmap_budget::MmapBudget,
    ) -> u64 {
        let mut total_evicted: u64 = 0;

        for idx in self.indexes.values() {
            // Default field
            total_evicted += enforce_segment_holder_budget(&idx.segments, budget);
            // Named fields (multi-vector indexes)
            for fs in idx.field_segments.values() {
                total_evicted += enforce_segment_holder_budget(&fs.segments, budget);
            }
        }

        total_evicted
    }

    /// Register cold DiskANN segments recovered from disk into the appropriate indexes.
    ///
    /// Called during shard restore after v3 recovery identifies cold-tier segments
    /// in the manifest. For each (segment_id, segment_dir), logs the discovery.
    ///
    /// Full DiskAnnSegment reconstruction from disk requires serialized PQ codebooks
    /// (future work). For now, this discovers and logs cold segments so they are
    /// tracked by the system. Full loading will be added when PQ codebook
    /// serialization is implemented.
    pub fn register_cold_segments(&mut self, cold_segments: Vec<(u64, std::path::PathBuf)>) {
        let mut loaded = 0usize;
        for (segment_id, segment_dir) in &cold_segments {
            // Try each index -- the segment belongs to whichever collection matches.
            for idx in self.indexes.values() {
                let seg_vamana = segment_dir.join("vamana.mpf");
                if seg_vamana.exists() {
                    tracing::info!(
                        "Cold segment {} at {:?} discovered for index {:?} (full loading requires stored PQ codebook)",
                        segment_id,
                        segment_dir,
                        std::str::from_utf8(&idx.meta.name).unwrap_or("<non-utf8>"),
                    );
                    loaded += 1;
                    break; // Segment belongs to one index only
                }
            }
        }
        if loaded > 0 {
            tracing::info!(
                "Discovered {}/{} cold segments on startup",
                loaded,
                cold_segments.len()
            );
        }
    }

    // ── P2: Segment merge public API ──────────────────────────────────────────

    /// Total immutable segment count for a named index.
    /// Returns None if the index does not exist.
    pub fn immutable_segment_count(&self, name: &[u8]) -> Option<usize> {
        self.indexes
            .get(name)
            .map(|idx| idx.segments.load().immutable.len())
    }

    /// True if the named index satisfies any auto-merge trigger condition:
    /// - immutable segment count > MERGE_SEGMENT_THRESHOLD (16), OR
    /// - dead_fraction > 0.20 across any segment AND live vectors fit in 512 MB
    ///
    /// Returns None if the index does not exist.
    pub fn needs_merge(&self, name: &[u8]) -> Option<bool> {
        let idx = self.indexes.get(name)?;
        if idx.meta.merge_mode == MergeMode::None {
            return Some(false);
        }
        let snap = idx.segments.load();
        let imm_count = snap.immutable.len();
        if imm_count > compaction::MERGE_SEGMENT_THRESHOLD {
            return Some(true);
        }
        // Dead-fraction trigger: any segment with >20% dead AND fits in 512 MB.
        let has_high_dead = snap.immutable.iter().any(|s| compaction::needs_vacuum(s));
        if has_high_dead {
            let live_bytes: usize = snap
                .immutable
                .iter()
                .map(|s| s.live_count() as usize * idx.collection.bytes_per_code_per_vector())
                .sum();
            let fits = live_bytes < compaction::MERGE_MEMORY_CEILING;
            return Some(fits);
        }
        Some(false)
    }

    /// Force-compact the mutable segment of a named index into a new immutable segment.
    /// Wrapper over `VectorIndex::force_compact()` for test/command convenience.
    pub fn force_compact_index(&mut self, name: &[u8]) -> Result<(), &'static str> {
        match self.indexes.get_mut(name) {
            Some(idx) => {
                idx.force_compact();
                Ok(())
            }
            None => Err("index not found"),
        }
    }

    /// Insert a raw f32 vector into a named index.
    ///
    /// Convenience wrapper for tests and the VACUUM command. Production ingestion
    /// goes through `auto_index_hset` (HSET hook). This method reuses the same
    /// mutable-segment append path.
    pub fn insert_vector(
        &mut self,
        index_name: &[u8],
        vector: &[f32],
        key_hash: u64,
        key: bytes::Bytes,
    ) -> Result<(), &'static str> {
        let idx = self.indexes.get_mut(index_name).ok_or("index not found")?;
        let snap = idx.segments.load();
        let insert_lsn = snap.mutable.len() as u64 + 1;
        drop(snap);
        let sq_vec: Vec<i8> = vector
            .iter()
            .map(|&x| (x * 127.0).clamp(-128.0, 127.0) as i8)
            .collect();
        let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        idx.segments
            .load()
            .mutable
            .append(key_hash, vector, &sq_vec, norm, insert_lsn);
        idx.key_hash_to_key.insert(key_hash, key);
        Ok(())
    }

    /// Search a named index and return the top-k global IDs.
    ///
    /// Convenience wrapper for tests.
    pub fn search_index(
        &mut self,
        name: &[u8],
        query: &[f32],
        k: usize,
        ef_search: usize,
    ) -> Result<Vec<u32>, &'static str> {
        let idx = self.indexes.get_mut(name).ok_or("index not found")?;
        let results = idx.segments.search(query, k, ef_search, &mut idx.scratch);
        Ok(results.iter().map(|r| r.id.0).collect())
    }

    /// Force-merge all immutable segments in a named index using its configured
    /// merge mode and a default recall tolerance of 0.90.
    ///
    /// Returns `MergeStats` describing what was done.
    /// Returns `Err` if the index does not exist.
    pub fn force_merge_index(&mut self, name: &[u8]) -> Result<MergeStats, &'static str> {
        self.force_merge_index_with_tolerance(name, 0.90)
            .map_err(|_| "merge failed or index not found")
    }

    /// Force-merge all immutable segments in a named index with an explicit
    /// recall tolerance.
    ///
    /// Returns `Ok(MergeStats)` if merge was successful or not needed.
    /// Returns `Err` if the index was not found or the recall gate fired.
    pub fn force_merge_index_with_tolerance(
        &mut self,
        name: &[u8],
        recall_tolerance: f32,
    ) -> Result<MergeStats, String> {
        let idx = self
            .indexes
            .get_mut(name)
            .ok_or_else(|| "index not found".to_string())?;

        let mode = idx.meta.merge_mode;
        if mode == MergeMode::None {
            return Ok(MergeStats {
                segments_merged: 0,
                live_vectors: 0,
                recall: 1.0,
            });
        }

        let snap = idx.segments.load();
        let imm_count = snap.immutable.len();
        if imm_count < 2 {
            return Ok(MergeStats {
                segments_merged: 0,
                live_vectors: snap
                    .immutable
                    .first()
                    .map_or(0, |s| s.live_count() as usize),
                recall: 1.0,
            });
        }

        let segs: Vec<Arc<crate::vector::segment::ImmutableSegment>> = snap.immutable.to_vec();
        let collection = idx.collection.clone();
        let seed = collection.collection_id.wrapping_mul(6364136223846793005);
        drop(snap);

        match compaction::merge_immutable(&segs, &collection, seed, mode, recall_tolerance) {
            Ok(merged) => {
                let live = merged.live_count() as usize;
                // Atomically swap: replace all immutable segments with the single merged one.
                let old = idx.segments.load();
                let new_list = SegmentList {
                    mutable: Arc::clone(&old.mutable),
                    immutable: vec![Arc::new(merged)],
                    ivf: old.ivf.clone(),
                    warm: old.warm.clone(),
                    cold: old.cold.clone(),
                };
                idx.segments.swap(new_list);

                // Rebuild scratch for the merged segment.
                let new_snap = idx.segments.load();
                if let Some(s) = new_snap.immutable.first() {
                    idx.scratch = crate::vector::hnsw::search::SearchScratch::new(
                        s.graph().num_nodes(),
                        idx.meta.padded_dimension,
                    );
                }

                tracing::info!(
                    index = ?std::str::from_utf8(name).unwrap_or("<non-utf8>"),
                    segments_merged = imm_count,
                    live_vectors = live,
                    "P2 merge complete"
                );

                Ok(MergeStats {
                    segments_merged: imm_count,
                    live_vectors: live,
                    recall: 1.0, // gate passed
                })
            }
            Err(compaction::CompactionError::RecallTooLow { recall, required }) => {
                tracing::warn!(recall, required, "P2 merge aborted: recall gate fired");
                Err(format!(
                    "merge recall {recall:.4} < tolerance {required:.4}"
                ))
            }
            Err(e) => Err(format!("merge failed: {e}")),
        }
    }

    /// Run a vacuum pass over all indexes: merge any index that satisfies the
    /// auto-merge trigger conditions (`needs_merge`).
    ///
    /// Called by the `VACUUM VECTOR <idx>` command and (future) autovacuum daemon (P4).
    /// Returns aggregated merge statistics across all merged indexes.
    pub fn run_vacuum_pass(&mut self) -> VacuumPassStats {
        let names: Vec<bytes::Bytes> = self.indexes.keys().cloned().collect();
        let mut stats = VacuumPassStats::default();
        for name in names {
            if self.needs_merge(&name) == Some(true) {
                // Use 0.70 tolerance for vacuum: catch catastrophic recall collapse
                // without false-positives on small/medium indexes.
                match self.force_merge_index_with_tolerance(&name, 0.70) {
                    Ok(ms) => {
                        stats.indexes_merged += 1;
                        stats.total_merged += ms.segments_merged;
                        stats.total_live_vectors += ms.live_vectors;
                        tracing::info!(
                            segments_merged = ms.segments_merged,
                            live_vectors = ms.live_vectors,
                            "P2 vacuum_pass: merged index"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            index = ?std::str::from_utf8(&name).unwrap_or("<non-utf8>"),
                            error = %e,
                            "vacuum_pass: merge failed"
                        );
                    }
                }
            }
        }
        stats
    }
}

/// Statistics from a `run_vacuum_pass()` call.
#[derive(Debug, Default, Clone, Copy)]
pub struct VacuumPassStats {
    /// Number of indexes where merge ran.
    pub indexes_merged: usize,
    /// Total segments consumed across all merged indexes.
    pub total_merged: usize,
    /// Total live vectors in the output segments.
    pub total_live_vectors: usize,
}

// ── Budget enforcement helper (module-level, not a method) ───────────────────

/// Enforce the mmap budget for a single `SegmentHolder`.
///
/// 1. Loads the current `SegmentList` snapshot (lock-free).
/// 2. Registers any warm segments not yet known to `budget`.
/// 3. Calls `budget.enforce_budget` on a mutable clone of the list.
/// 4. If any segments were evicted, atomically swaps the trimmed list back.
///
/// Returns the count of segments evicted.
fn enforce_segment_holder_budget(
    holder: &SegmentHolder,
    budget: &mut crate::vector::persistence::mmap_budget::MmapBudget,
) -> u64 {
    let snapshot = holder.load();

    // Collect the set of IDs currently in the warm list.
    let live_ids: std::collections::HashSet<u64> =
        snapshot.warm.iter().map(|w| w.segment_id()).collect();

    // Self-healing reconciliation: remove tracker entries for segments that
    // are no longer in the warm list (cold-tier transition, index drop, etc.).
    // This prevents permanently-inflated pressure from orphaned entries.
    let stale_ids: Vec<u64> = budget
        .tracked_ids()
        .filter(|id| !live_ids.contains(id))
        .collect();
    for id in stale_ids {
        budget.remove_segment(id);
    }

    // Register / update all currently-live warm segments.
    // `register_segment` uses delta accounting — no global-atomic drift on
    // repeated calls (common: every warm-check tick re-registers all segments).
    for warm in &snapshot.warm {
        budget.register_segment(warm.segment_id(), warm.resident_bytes() as u64);
    }

    // Build a mutable owned SegmentList for the enforcer to trim.
    let mut list = SegmentList {
        mutable: Arc::clone(&snapshot.mutable),
        immutable: snapshot.immutable.clone(),
        ivf: snapshot.ivf.clone(),
        warm: snapshot.warm.clone(),
        cold: snapshot.cold.clone(),
    };

    let stats = budget.enforce_budget(&mut list);

    if stats.segments_evicted > 0 {
        // Atomically swap in the trimmed list. In-flight queries that already
        // loaded the old snapshot will finish normally (Arc keeps them alive).
        holder.swap(list);
        tracing::info!(
            evicted = stats.segments_evicted,
            freed = stats.bytes_freed,
            remaining = stats.bytes_after,
            "warm mmap budget: evicted {} segment(s), freed {} B, {} B remaining",
            stats.segments_evicted,
            stats.bytes_freed,
            stats.bytes_after,
        );
    }

    stats.segments_evicted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(name: &str, dim: u32, prefixes: &[&str]) -> IndexMeta {
        IndexMeta {
            name: Bytes::from(name.to_owned()),
            dimension: dim,
            padded_dimension: padded_dimension(dim),
            metric: DistanceMetric::L2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_runtime: 0,
            compact_threshold: 0,
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: prefixes
                .iter()
                .map(|p| Bytes::from(p.to_string()))
                .collect(),
            quantization: QuantizationConfig::TurboQuant4,
            build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
            vector_fields: Vec::new(), // populated by create_index
            schema_fields: Vec::new(),
            merge_mode: MergeMode::GraphUnion,
            keep_raw: false,
        }
    }

    fn make_meta_quant(name: &str, dim: u32, quant: QuantizationConfig) -> IndexMeta {
        IndexMeta {
            name: Bytes::from(name.to_owned()),
            dimension: dim,
            padded_dimension: padded_dimension(dim),
            metric: DistanceMetric::L2,
            hnsw_m: 16,
            hnsw_ef_construction: 200,
            hnsw_ef_runtime: 0,
            compact_threshold: 0,
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: vec![Bytes::from_static(b"doc:")],
            quantization: quant,
            build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
            vector_fields: Vec::new(), // populated by create_index
            schema_fields: Vec::new(),
            merge_mode: MergeMode::GraphUnion,
            keep_raw: false,
        }
    }

    #[test]
    fn test_new_is_empty() {
        let store = VectorStore::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_create_index() {
        let mut store = VectorStore::new();
        let meta = make_meta("idx", 128, &["doc:"]);
        assert!(store.create_index(meta).is_ok());
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());

        // Duplicate should fail
        let meta2 = make_meta("idx", 128, &["doc:"]);
        assert!(store.create_index(meta2).is_err());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_drop_index() {
        let mut store = VectorStore::new();
        let meta = make_meta("idx", 128, &["doc:"]);
        store.create_index(meta).unwrap();

        assert!(store.drop_index(b"idx"));
        assert!(store.is_empty());

        // Drop non-existent
        assert!(!store.drop_index(b"idx"));
        assert!(!store.drop_index(b"nonexistent"));
    }

    #[test]
    fn test_find_matching_indexes() {
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("idx1", 64, &["user:"]))
            .unwrap();
        store
            .create_index(make_meta("idx2", 64, &["product:"]))
            .unwrap();
        store
            .create_index(make_meta("idx3", 64, &["user:", "item:"]))
            .unwrap();

        let matches = store.find_matching_indexes(b"user:123");
        assert_eq!(matches.len(), 2);

        let matches = store.find_matching_indexes(b"product:456");
        assert_eq!(matches.len(), 1);

        let matches = store.find_matching_indexes(b"item:789");
        assert_eq!(matches.len(), 1);

        let matches = store.find_matching_indexes(b"order:000");
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_get_index() {
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("myidx", 256, &["doc:"]))
            .unwrap();

        let idx = store.get_index(b"myidx").unwrap();
        assert_eq!(idx.meta.dimension, 256);
        assert_eq!(idx.meta.hnsw_m, 16);

        assert!(store.get_index(b"nonexistent").is_none());
    }

    // -- MVCC tests (Phase 65-02) --

    #[test]
    fn test_vector_store_has_txn_manager() {
        let store = VectorStore::new();
        // txn_manager accessible, starts with 0 active
        assert_eq!(store.txn_manager().active_count(), 0);
        assert_eq!(store.txn_manager().committed_count(), 0);
    }

    #[test]
    fn test_vector_store_txn_manager_mut() {
        let mut store = VectorStore::new();
        let txn = store.txn_manager_mut().begin();
        assert_eq!(txn.txn_id, 1);
        assert_eq!(store.txn_manager().active_count(), 1);
    }

    // -- Warm transition tests (Phase 75-11) --

    #[test]
    fn test_try_warm_transitions_all_immediate() {
        // With warm_after_secs=0, all immutable segments should transition.
        use crate::vector::aligned_buffer::AlignedBuffer;
        use crate::vector::distance;
        use crate::vector::hnsw::graph::HnswGraph;
        use crate::vector::segment::immutable::ImmutableSegment;

        distance::init();
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("idx", 128, &["doc:"]))
            .unwrap();

        // Create a minimal immutable segment and swap it in.
        let idx = store.get_index(b"idx").unwrap();
        let collection = idx.collection.clone();
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));
        let imm = Arc::new(ImmutableSegment::new(
            graph,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            16,
            Vec::new(),
            16,
            Vec::new(),
            collection,
            0,
            0,
        ));

        let old_snap = idx.segments.load();
        let new_list = SegmentList {
            mutable: Arc::clone(&old_snap.mutable),
            immutable: vec![imm],
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
        };
        idx.segments.swap(new_list);
        drop(old_snap);

        // Verify we have 1 immutable segment.
        assert_eq!(idx.segments.load().immutable.len(), 1);

        // Try warm transition with age threshold 0 (everything qualifies).
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest =
            crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let count = store.try_warm_transitions_all(
            &shard_dir,
            &mut manifest,
            0,
            &mut next_file_id,
            &mut None,
        );
        assert_eq!(count, 1);

        // Immutable list should now be empty (segment moved to warm).
        let idx = store.get_index(b"idx").unwrap();
        let snap = idx.segments.load();
        assert_eq!(snap.immutable.len(), 0);
        // Warm list should now have 1 segment (searchable warm).
        assert_eq!(snap.warm.len(), 1);
    }

    #[test]
    fn test_try_warm_transitions_high_threshold_skips() {
        // With warm_after_secs=999999, nothing should transition.
        use crate::vector::aligned_buffer::AlignedBuffer;
        use crate::vector::distance;
        use crate::vector::hnsw::graph::HnswGraph;
        use crate::vector::segment::immutable::ImmutableSegment;

        distance::init();
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("idx", 128, &["doc:"]))
            .unwrap();

        let idx = store.get_index(b"idx").unwrap();
        let collection = idx.collection.clone();
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));
        let imm = Arc::new(ImmutableSegment::new(
            graph,
            AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            16,
            Vec::new(),
            16,
            Vec::new(),
            collection,
            0,
            0,
        ));

        let old_snap = idx.segments.load();
        idx.segments.swap(SegmentList {
            mutable: Arc::clone(&old_snap.mutable),
            immutable: vec![imm],
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
        });
        drop(old_snap);

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();
        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest =
            crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let count = store.try_warm_transitions_all(
            &shard_dir,
            &mut manifest,
            999_999,
            &mut next_file_id,
            &mut None,
        );
        assert_eq!(count, 0);

        // Immutable list should still have 1 segment.
        let idx = store.get_index(b"idx").unwrap();
        assert_eq!(idx.segments.load().immutable.len(), 1);
    }

    // -- Multi-bit quantization tests (Phase 72-02) --

    #[test]
    fn test_create_index_with_tq2_has_4_centroids() {
        let mut store = VectorStore::new();
        let meta = make_meta_quant("idx_tq2", 128, QuantizationConfig::TurboQuant2);
        store.create_index(meta).unwrap();

        let idx = store.get_index(b"idx_tq2").unwrap();
        assert_eq!(idx.collection.codebook.len(), 4);
        assert_eq!(idx.collection.codebook_boundaries.len(), 3);
        assert_eq!(idx.collection.quantization, QuantizationConfig::TurboQuant2);
    }

    #[test]
    fn test_create_index_with_tq1_has_2_centroids() {
        let mut store = VectorStore::new();
        let meta = make_meta_quant("idx_tq1", 128, QuantizationConfig::TurboQuant1);
        store.create_index(meta).unwrap();

        let idx = store.get_index(b"idx_tq1").unwrap();
        assert_eq!(idx.collection.codebook.len(), 2);
        assert_eq!(idx.collection.quantization, QuantizationConfig::TurboQuant1);
    }

    #[test]
    fn test_create_index_default_tq4() {
        let mut store = VectorStore::new();
        let meta = make_meta("idx_default", 128, &["doc:"]);
        store.create_index(meta).unwrap();

        let idx = store.get_index(b"idx_default").unwrap();
        assert_eq!(idx.collection.codebook.len(), 16);
        assert_eq!(idx.collection.quantization, QuantizationConfig::TurboQuant4);
    }

    // -- Cold segment registration tests (Phase 79-04) --

    #[test]
    fn test_register_cold_segments_empty() {
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("idx", 128, &["doc:"]))
            .unwrap();
        // Should not panic with empty input
        store.register_cold_segments(Vec::new());
    }

    #[test]
    fn test_register_cold_segments_discovers() {
        let mut store = VectorStore::new();
        store
            .create_index(make_meta("idx", 128, &["doc:"]))
            .unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-10-diskann");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::write(seg_dir.join("vamana.mpf"), [0u8; 64]).unwrap();

        // Should discover the segment without panicking
        store.register_cold_segments(vec![(10, seg_dir)]);
    }
}
