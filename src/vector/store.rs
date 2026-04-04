//! Per-shard VectorStore -- owns all vector indexes for one shard.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as PubSubRegistry).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::vector::filter::PayloadIndex;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::mvcc::manager::TransactionManager;
use crate::vector::segment::compaction;
use crate::vector::segment::{SegmentHolder, SegmentList};
use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::types::DistanceMetric;

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
}

/// A single vector index: meta + segments + scratch + collection config.
pub struct VectorIndex {
    pub meta: IndexMeta,
    pub segments: SegmentHolder,
    pub scratch: SearchScratch,
    pub collection: Arc<CollectionMetadata>,
    pub payload_index: PayloadIndex,
}

/// Default minimum vector count to trigger compaction before search.
/// Overridden by IndexMeta.compact_threshold when set via FT.CREATE.
const DEFAULT_COMPACT_THRESHOLD: usize = 1000;

impl VectorIndex {
    /// Compact the mutable segment into an immutable HNSW segment if beneficial.
    ///
    /// Triggered lazily on first search when the mutable segment exceeds the
    /// threshold and no immutable segments exist yet. After compaction, searches
    /// use HNSW (O(log n)) instead of brute force (O(n)).
    ///
    /// This is a blocking operation (builds HNSW graph). For production, this
    /// should be moved to a background task with async notification.
    pub fn try_compact(&mut self) {
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
        if mutable_len < threshold {
            return;
        }

        let frozen = self.segments.load().mutable.freeze();
        // Use a deterministic seed based on collection ID for reproducibility
        let seed = self
            .collection
            .collection_id
            .wrapping_mul(6364136223846793005);

        match compaction::compact(&frozen, &self.collection, seed, None) {
            Ok(immutable) => {
                // Resize scratch to match new graph size
                let num_nodes = immutable.graph().num_nodes();
                let padded = self.collection.padded_dimension;
                self.scratch = SearchScratch::new(num_nodes, padded);

                // Swap: empty mutable + append new immutable to existing list.
                // The new mutable segment's global_id_base continues from where
                // the compacted segment left off, ensuring unique IDs across segments.
                let old = self.segments.load();
                let next_global = old.mutable.next_global_id();
                let mut imm_list = old.immutable.clone();
                imm_list.push(Arc::new(immutable));
                let new_mutable = Arc::new(crate::vector::segment::mutable::MutableSegment::new(
                    self.meta.dimension,
                    self.collection.clone(),
                ));
                new_mutable.set_global_id_base(next_global);
                let new_list = SegmentList {
                    mutable: new_mutable,
                    immutable: imm_list,
                    ivf: old.ivf.clone(),
                    warm: old.warm.clone(),
                    cold: old.cold.clone(),
                };
                self.segments.swap(new_list);
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
                    // Remove from in-memory immutable list.
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
                                file_id, e
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
                        warm_file_id, e
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
    pub fn create_index(&mut self, meta: IndexMeta) -> Result<(), &'static str> {
        if self.indexes.contains_key(&meta.name) {
            return Err("Index already exists");
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

        let name = meta.name.clone();
        self.indexes.insert(
            name.clone(),
            VectorIndex {
                meta,
                segments,
                scratch,
                collection,
                payload_index: PayloadIndex::new(),
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
                    shard_dir, manifest, warm_after_secs, next_file_id, wal,
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
                total += idx.try_cold_transitions(
                    shard_dir, manifest, cold_after_secs, next_file_id,
                );
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
        use crate::storage::tiered::SegmentHandle;
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
                            segment_id, segment_dir
                        );
                        break; // Segment belongs to one index only
                    }
                    Err(e) => {
                        tracing::debug!(
                            "Warm segment {} not compatible with index: {}",
                            segment_id, e
                        );
                    }
                }
            }
        }
        if loaded > 0 {
            tracing::info!("Registered {}/{} warm segments on startup", loaded, warm_segments.len());
        }
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
        store.create_index(make_meta("idx", 128, &["doc:"])).unwrap();

        // Create a minimal immutable segment and swap it in.
        let idx = store.get_index(b"idx").unwrap();
        let collection = idx.collection.clone();
        let empty_graph = HnswGraph::new(
            0, 16, 32, 0, 0,
            AlignedBuffer::new(0), Vec::new(), Vec::new(), Vec::new(), Vec::new(), 68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));
        let imm = Arc::new(ImmutableSegment::new(
            graph, AlignedBuffer::new(0), Vec::new(), Vec::new(), 16,
            Vec::new(), 16, Vec::new(), collection, 0, 0,
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
        let mut manifest = crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let count = store.try_warm_transitions_all(
            &shard_dir, &mut manifest, 0, &mut next_file_id, &mut None,
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
        store.create_index(make_meta("idx", 128, &["doc:"])).unwrap();

        let idx = store.get_index(b"idx").unwrap();
        let collection = idx.collection.clone();
        let empty_graph = HnswGraph::new(
            0, 16, 32, 0, 0,
            AlignedBuffer::new(0), Vec::new(), Vec::new(), Vec::new(), Vec::new(), 68,
        );
        let graph = HnswGraph::from_bytes(&empty_graph.to_bytes())
            .unwrap_or_else(|_| panic!("empty graph"));
        let imm = Arc::new(ImmutableSegment::new(
            graph, AlignedBuffer::new(0), Vec::new(), Vec::new(), 16,
            Vec::new(), 16, Vec::new(), collection, 0, 0,
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
        let mut manifest = crate::persistence::manifest::ShardManifest::create(&manifest_path).unwrap();
        let mut next_file_id = 1u64;

        let count = store.try_warm_transitions_all(
            &shard_dir, &mut manifest, 999_999, &mut next_file_id, &mut None,
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
        store.create_index(make_meta("idx", 128, &["doc:"])).unwrap();
        // Should not panic with empty input
        store.register_cold_segments(Vec::new());
    }

    #[test]
    fn test_register_cold_segments_discovers() {
        let mut store = VectorStore::new();
        store.create_index(make_meta("idx", 128, &["doc:"])).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-10-diskann");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::write(seg_dir.join("vamana.mpf"), &[0u8; 64]).unwrap();

        // Should discover the segment without panicking
        store.register_cold_segments(vec![(10, seg_dir)]);
    }
}
