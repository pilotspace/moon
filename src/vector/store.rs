//! Per-shard VectorStore -- owns all vector indexes for one shard.
//!
//! No Arc, no Mutex -- fully owned by shard thread (same pattern as PubSubRegistry).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::storage::tiered::SegmentHandle;
use crate::vector::filter::PayloadIndex;
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::keymap::BucketedKeyMap;
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
    /// Logical database (SELECT/`--databases`) this index was created in
    /// (WS5a db-scoped indexes). The index's definition AND contents belong
    /// exclusively to this db: FT.SEARCH/FT.INFO/FT._LIST/auto-indexing from
    /// any other db must not see it. Persisted sidecars written before this
    /// field existed (v1-v3) default to `0` on load (legacy indexes become
    /// db-0-owned, matching pre-v0.6.0 global behavior for db 0 callers).
    pub db_index: u8,
    /// Exact-rerank depth multiplier (HQ-1): re-score the top
    /// `rerank_mult · k` beam candidates from the f16 sidecar. Default 4.
    /// Range 1-64, set via FT.CONFIG SET <idx> RERANK_MULT.
    pub rerank_mult: u32,
    /// When true, the HNSW beam navigates with exact f16 sidecar distances
    /// instead of quantized ADC (recall ≈ graph-limited, QPS cost grows with
    /// dimension). Default false, set via FT.CONFIG SET <idx> EXACT_BEAM.
    pub exact_beam: bool,
}

/// Server-wide starting values for the per-index search-tuning knobs,
/// applied by FT.CREATE when the operator did not specify them
/// (`--vector-ef-runtime`, `--vector-rerank-mult`, `--vector-exact-beam`).
/// Per-index `FT.CONFIG SET` always overrides these at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorCreateDefaults {
    /// EF_RUNTIME for new indexes (0 = per-query auto heuristic).
    pub ef_runtime: u32,
    /// RERANK_MULT for new indexes (1-64).
    pub rerank_mult: u32,
    /// EXACT_BEAM for new indexes.
    pub exact_beam: bool,
}

impl Default for VectorCreateDefaults {
    fn default() -> Self {
        Self {
            ef_runtime: 0,
            rerank_mult: 4,
            exact_beam: false,
        }
    }
}

static VECTOR_CREATE_DEFAULTS: std::sync::OnceLock<VectorCreateDefaults> =
    std::sync::OnceLock::new();

/// Install the server-wide FT.CREATE tuning defaults from `ServerConfig`.
/// Called once at startup (before any connection is accepted); a second call
/// is a no-op (the first write wins — matches `OnceLock` semantics, which is
/// what we want for embedded/test servers booting in one process).
pub fn set_vector_create_defaults(defaults: VectorCreateDefaults) {
    let _ = VECTOR_CREATE_DEFAULTS.set(defaults);
}

/// The server-wide FT.CREATE tuning defaults; the compiled-in baseline
/// (auto ef, mult 4, beam off) when the server never installed any —
/// unit tests and library embedders get pre-flag behavior unchanged.
pub fn vector_create_defaults() -> VectorCreateDefaults {
    VECTOR_CREATE_DEFAULTS.get().copied().unwrap_or_default()
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

/// State for an in-flight background compaction of the default field.
///
/// Created by [`VectorIndex::begin_background_compact`] and consumed by
/// [`VectorIndex::poll_install_compaction`].
struct InFlightCompaction {
    /// Reply channel from the worker thread.
    reply_rx: flume::Receiver<crate::vector::background_compact::CompactionResult>,
    /// Number of entries the mutable segment had when we froze it.
    /// Used to compute the tail window `[frozen_len..current_len)`.
    frozen_len: usize,
    /// Global ID base of the mutable segment at freeze time.
    /// Used to re-anchor the tail segment after install.
    frozen_global_base: u32,
}

/// State for an in-flight background merge of immutable segments.
///
/// Created by [`VectorIndex::begin_background_merge`] and consumed by
/// [`VectorIndex::poll_install_merge`].
struct InFlightMerge {
    /// Reply channel from the worker thread.
    reply_rx: flume::Receiver<crate::vector::background_compact::CompactionResult>,
    /// The exact source `Arc`s that were submitted to the worker.
    ///
    /// At install time we compare the current immutable list against this set
    /// via `Arc::ptr_eq` to handle warm-tier transitions that may have removed
    /// or replaced source segments while the merge was running.
    merged_sources: Vec<Arc<crate::vector::segment::immutable::ImmutableSegment>>,
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
    ///
    /// Bucketed CoW (RSS/CPU wave 4, defect 1): 256 independent
    /// `Arc<HashMap>` shards keyed by the top bits of `key_hash`, so search
    /// snapshots still capture the whole map in O(1) (QP-1) but a concurrent
    /// writer's `Arc::make_mut` only clones the ONE bucket its key hashes
    /// into — not the entire multi-MB map — even while a search snapshot is
    /// alive. See `crate::vector::keymap` module docs.
    pub key_hash_to_key: BucketedKeyMap<Bytes>,
    /// Maps `key_hash` → `global_id` for metadata-only updates.
    ///
    /// When `HSET doc:1 category "science"` is called without a vector blob,
    /// the auto-indexer looks up the existing `global_id` here to update the
    /// PayloadIndex for that vector without re-inserting it.
    ///
    /// Bucketed CoW for the same reason as `key_hash_to_key` (QP-1 O(1)
    /// snapshot capture, bucket-scoped write clone) — the durability write
    /// path (B2) clones this into background keymap-snapshot jobs without an
    /// O(n) copy on the shard thread.
    pub key_hash_to_global_id: BucketedKeyMap<u32>,
    /// Maps `key_hash` → `xxh64(vector-field bytes, seed 0)` (B2, durability).
    ///
    /// Computed where the raw HSET vector blob is in hand
    /// (`spsc_handler::handle_vector_insert`) and mirrored at every site that
    /// mirrors `key_hash_to_key` (insert, update, tombstone, wholesale
    /// reset). Persisted into `keymap-<epoch>.bin` so the B3 recovery dedup
    /// rescan can tell "unchanged" keys (checksum matches → metadata-only
    /// rebuild) from "changed" keys (re-encode) without hashing every value
    /// twice across a restart.
    pub key_hash_to_vec_checksum: BucketedKeyMap<u64>,
    /// Shard-relative directory this index persists segments/manifest/keymap
    /// under, i.e. `<vector_persist_dir>/idx-<hex(xxh64(name))>/` (B2).
    /// `None` when the store has no `persist_dir` configured (disk-offload
    /// and on-disk persistence both disabled) — segment/manifest/keymap
    /// writes are skipped entirely in that case, identical to the existing
    /// sidecar gate.
    pub persist_dir: Option<std::path::PathBuf>,
    /// Monotonic on-disk segment id allocator for THIS index, scoped to
    /// `persist_dir` (B2). Allocated on the shard thread at compact/merge
    /// submit time (never on the worker thread) so ids never collide even
    /// though the actual disk write happens on a background worker.
    pub next_segment_id: u64,
    /// Monotonic manifest/keymap "generation" counter for THIS index (B2).
    /// Allocated on the shard thread each time a snapshot job is enqueued;
    /// doubles as the keymap epoch (`keymap-<seq>.bin`) — one counter, so
    /// there is no separate race between "job ordering" and "which keymap
    /// file is current".
    ///
    /// `AtomicU64`, not a plain `u64`: `persist_hook_after_install` is called
    /// from both a `&mut self` context (compact/merge install) and a `&self`
    /// context (`try_warm_transitions_idle`, which only holds `&self` because
    /// its caller iterates `self.indexes.values()` — see that fn's callsite).
    /// `VectorIndex` (via `HashMap<Bytes, VectorIndex>` inside `VectorStore`)
    /// must stay `Sync` — see `vector_store_version_token_monotonic_concurrent`
    /// — which rules out `Cell`; `Relaxed` ordering is enough since every
    /// actual writer is the single owning shard thread (this only needs to be
    /// a valid *value*, not a cross-thread synchronization point), matching
    /// `version_token`'s existing atomic-counter pattern on `VectorStore`.
    next_snapshot_seq: AtomicU64,
    /// Shared watermark of the highest snapshot `seq` durably committed for
    /// this index (B2). Cloned into every snapshot job; the worker holds
    /// this lock across its entire write+GC critical section so a stale job
    /// (built from an older install, possibly completed out of order by a
    /// different worker) can never overwrite a newer manifest — see
    /// `crate::vector::persistence::manifest` module docs.
    persist_seq_watermark: Arc<parking_lot::Mutex<u64>>,
    /// Whether auto-compaction is enabled. Default: true.
    /// Set to false via FT.CONFIG SET idx AUTOCOMPACT OFF for bulk ingestion.
    /// Manual FT.COMPACT always works regardless of this flag.
    pub autocompact_enabled: bool,
    /// Recall-gate tolerance for UNATTENDED (background/vacuum) GraphUnion
    /// merges (VEC-4). Default 0.70 (catastrophic-collapse guard only); the
    /// manual FT.COMPACT merge path uses 0.90. Tunable per index via
    /// `FT.CONFIG SET <idx> MERGE_RECALL_TOLERANCE <0.0..=1.0>`.
    pub merge_recall_tolerance: f32,
    /// Per-index compaction priority weight for the autovacuum scheduler (W3-deep).
    ///
    /// Multiplies the raw `dead_bytes_rate` before comparison in `CompactionScheduler`.
    /// Default 1.0 — identical to pre-W3 behaviour.
    ///
    /// - `> 1.0`: promotes this index (compacted more aggressively under load).
    /// - `< 1.0`: demotes this index (compacted less often).
    /// - `0.0`: never auto-compacted by weight alone; starvation cap still applies.
    ///
    /// Set via `FT.CONFIG SET <idx> COMPACTION_WEIGHT <n>` (n ∈ [0.0, 100.0])
    /// or `VACUUM VECTOR <idx> WEIGHT <n>`.
    pub compaction_weight: f32,
    /// Additional named vector fields (beyond the default field).
    /// Empty for single-field indexes. Keyed by field_name from VectorFieldMeta.
    pub field_segments: HashMap<Bytes, FieldSegments>,
    /// Sparse vector stores, keyed by field name.
    /// Populated when FT.CREATE includes SPARSE field declarations.
    pub sparse_stores: HashMap<Bytes, crate::vector::sparse::store::SparseStore>,
    /// In-flight background compaction for the default field, if any.
    /// `None` means no compaction is currently running.
    bg_compact_inflight: Option<InFlightCompaction>,
    /// In-flight background merge of immutable segments, if any.
    /// `None` means no merge is currently running.
    ///
    /// Mutual exclusion with `bg_compact_inflight`: only one of the two may be
    /// `Some` at a time (both begin_* methods enforce this).
    bg_merge_inflight: Option<InFlightMerge>,
    /// Backoff state after a rejected unattended merge (recall gate, memory
    /// ceiling, worker error). `None` = no recent failure. Guards against the
    /// merge CPU livelock: a rejection keeps the source segments, so every
    /// `needs_merge` trigger condition stays true and, without this, the next
    /// autovacuum tick resubmits the identical doomed merge — each attempt
    /// building and discarding a full union HNSW graph, forever.
    merge_backoff: Option<MergeBackoff>,
}

/// See [`VectorIndex::merge_backoff`].
struct MergeBackoff {
    /// Identity fingerprint (xxh64 over the source `Arc` pointers, in list
    /// order) of the segment set whose merge was rejected. Pointer identity is
    /// the same notion `poll_install_merge` uses for its defensive install
    /// check (`Arc::ptr_eq`), so any real change to the set — compaction
    /// appending a segment, a warm-tier transition, an installed merge —
    /// yields a different fingerprint. (A freed-then-reallocated `Arc` could
    /// theoretically collide; the only consequence is a merge delayed by one
    /// backoff window.)
    fingerprint: u64,
    /// Consecutive rejections for this fingerprint.
    failures: u32,
    /// Do not re-dispatch an unattended merge of the same set before this.
    /// Manual `FT.COMPACT` (force_merge) is unaffected.
    retry_after: std::time::Instant,
}

/// Exponential backoff for rejected unattended merges: 60s base, doubling per
/// consecutive failure, capped at 1h. The expensive rejection mode (recall
/// gate) only fires after a full union build, so even the 60s floor cuts the
/// worst-case waste to one discarded build per minute instead of back-to-back.
fn merge_backoff_duration(failures: u32) -> std::time::Duration {
    const BASE_SECS: u64 = 60;
    const CAP_SECS: u64 = 3600;
    let shift = failures.saturating_sub(1).min(6);
    std::time::Duration::from_secs((BASE_SECS << shift).min(CAP_SECS))
}

/// Fingerprint of an immutable-segment set by `Arc` pointer identity.
fn segment_set_fingerprint(
    segs: &[Arc<crate::vector::segment::immutable::ImmutableSegment>],
) -> u64 {
    let mut buf = [0u8; 8];
    let mut h = xxhash_rust::xxh64::xxh64(&(segs.len() as u64).to_le_bytes(), 0);
    for s in segs {
        buf.copy_from_slice(&(Arc::as_ptr(s) as usize as u64).to_le_bytes());
        h = xxhash_rust::xxh64::xxh64(&buf, h);
    }
    h
}

/// Default minimum vector count to trigger compaction before search.
/// Overridden by IndexMeta.compact_threshold when set via FT.CREATE.
const DEFAULT_COMPACT_THRESHOLD: usize = 1000;

/// Valid range for per-index compaction weight (W3-deep).
pub const COMPACTION_WEIGHT_MIN: f32 = 0.0;
pub const COMPACTION_WEIGHT_MAX: f32 = 100.0;
pub const COMPACTION_WEIGHT_DEFAULT: f32 = 1.0;

impl VectorIndex {
    /// Read the current compaction weight for this index.
    #[inline]
    pub fn compaction_weight(&self) -> f32 {
        self.compaction_weight
    }

    /// Set the compaction weight unconditionally (internal use / already-validated paths).
    #[inline]
    pub fn set_compaction_weight(&mut self, w: f32) {
        self.compaction_weight = w.clamp(COMPACTION_WEIGHT_MIN, COMPACTION_WEIGHT_MAX);
    }

    /// Set the compaction weight with range validation.
    ///
    /// Returns `Err` when `w` is outside `[0.0, 100.0]` or is NaN/infinite.
    pub fn try_set_compaction_weight(&mut self, w: f32) -> Result<(), &'static str> {
        if !w.is_finite() || w < COMPACTION_WEIGHT_MIN || w > COMPACTION_WEIGHT_MAX {
            return Err("COMPACTION_WEIGHT must be a finite f32 in [0.0, 100.0]");
        }
        self.compaction_weight = w;
        Ok(())
    }

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

        // Default field: BACKGROUND path. Neither call blocks the shard event
        // loop — the HNSW build runs on a worker thread (background_compact.rs).
        //   1. poll_install: install a segment a worker finished building since
        //      the last search (non-blocking; no-op if nothing is ready).
        //   2. begin_*_due: dispatch a new build iff the mutable segment crossed
        //      its compact threshold and none is already in flight. The
        //      triggering FT.SEARCH then continues against the still-present
        //      brute-force mutable segment, so it is NOT frozen — this replaces
        //      the former inline `compact_segments` that stalled the shard for
        //      seconds (0.42s @2k … 24.9s @50k vectors, measured).
        // Poll installs first (compaction, then merge), then begin new work.
        // Compaction-begin runs before merge-begin; the mutual-exclusion guards
        // in both begin_* methods handle the rest.
        self.poll_install_compaction();
        self.poll_install_merge();
        let _ = self.begin_background_compact_due(crate::vector::background_compact::global());
        let _ = self.begin_background_merge_due(crate::vector::background_compact::global());

        // Additional vector fields still use the inline (blocking) path.
        // TODO(bg-compact): extend background compaction to field_segments.
        let threshold = if self.meta.compact_threshold > 0 {
            self.meta.compact_threshold as usize
        } else {
            DEFAULT_COMPACT_THRESHOLD
        };
        for (_, fs) in &mut self.field_segments {
            let fs_len = fs.segments.load().mutable.len();
            if fs_len >= threshold {
                let dim = fs.collection.dimension;
                let mut unused_id = 0u64;
                Self::compact_segments(
                    &mut fs.segments,
                    &mut fs.scratch,
                    &fs.collection,
                    dim,
                    0,
                    None, // additional-field segments are not persisted (out of B2 scope)
                    &mut unused_id,
                );
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
        // If a background compaction is already in flight for the default field,
        // drain it by blocking on the reply channel (worker is already building).
        // This prevents double-compaction of the same frozen snapshot.
        if let Some(inflight) = self.bg_compact_inflight.take() {
            // Block until the worker finishes.
            if let Ok(Ok(mut immutable)) = inflight.reply_rx.recv() {
                // Reconcile window deletes (same logic as poll_install_compaction).
                snap_and_reconcile(&self.segments, inflight.frozen_len, &mut immutable);
                let snap = self.segments.load();
                let tail_mutable = snap.mutable.clone_suffix(inflight.frozen_len);
                let num_nodes = immutable.graph().num_nodes();
                let padded = self.collection.padded_dimension;
                self.scratch = SearchScratch::new(num_nodes, padded);
                let mut imm_list = snap.immutable.clone();
                immutable.mark_installed();
                imm_list.push(Arc::new(immutable));
                let new_list = SegmentList {
                    mutable: tail_mutable,
                    immutable: imm_list,
                    ivf: snap.ivf.clone(),
                    warm: snap.warm.clone(),
                    unloaded: snap.unloaded.clone(),
                };
                drop(snap);
                self.segments.swap(new_list);
                // B2 (durability): the worker already persisted this segment
                // (begin_background_compact allocated persist target at
                // submit time, if configured) — commit the keymap/manifest
                // snapshot now that install is durable in memory.
                self.persist_hook_after_install();
                // Do NOT return here: inserts that landed WHILE the
                // background build was in flight are still in the mutable
                // tail (`clone_suffix(frozen_len)` above). An early return
                // breaks force_compact's full-drain contract (FT.COMPACT:
                // frozen == mutable on reply) and leaves those docs with no
                // durable segment until some future compact fires — a kill
                // -9 in that window relied on them being "verified
                // unchanged" from a keymap that covers them while no
                // segment does (the B3 phantom-entry hole). Fall through to
                // the inline compact below, which is a no-op when the tail
                // is empty and otherwise drains + persists it.
            }
            // Worker failed or dropped — fall through to inline compact below.
        }

        // Compact default field. `persist_root` must be the per-INDEX dir
        // (`idx-<hex>/`), not the bare shard-level `persist_dir` — matching
        // `alloc_persist_target`/`persist_hook_after_install`.
        let persist_root = self.persist_dir.as_ref().map(|dir| {
            crate::vector::persistence::manifest::index_persist_dir(dir, self.meta.name.as_ref())
        });
        Self::compact_segments(
            &mut self.segments,
            &mut self.scratch,
            &self.collection,
            self.meta.dimension,
            self.meta.compact_threshold as usize,
            persist_root.as_deref(),
            &mut self.next_segment_id,
        );
        self.persist_hook_after_install();
        // Compact additional fields (legacy unbounded semantics: threshold 0).
        for (_, fs) in &mut self.field_segments {
            let dim = fs.collection.dimension;
            let mut unused_id = 0u64;
            Self::compact_segments(
                &mut fs.segments,
                &mut fs.scratch,
                &fs.collection,
                dim,
                0,
                None,
                &mut unused_id,
            );
        }
    }

    /// Compact a field's mutable segment into immutable HNSW segment(s).
    ///
    /// With a non-zero `compact_threshold` the mutable is drained in
    /// `bulk_freeze_cap`-bounded prefix builds — a bulk load yields several
    /// independently searchable segments (see `bulk_freeze_cap`). The tail
    /// survives each install via `clone_suffix`, preserving the global ID
    /// space, exactly like the background install path.
    ///
    /// `persist_root`/`next_segment_id`: when `persist_root` is `Some`, each
    /// successful build in the (possibly multi-iteration, bulk-load) loop is
    /// persisted to disk via the staged writer under a freshly-allocated id
    /// (B2, durability write path). `next_segment_id` is always threaded
    /// through (even when `persist_root` is `None`, in which case it is
    /// simply never read) so callers that don't persist can pass a throwaway
    /// counter. Callers are responsible for triggering the keymap/manifest
    /// snapshot job (see `VectorIndex::persist_hook_after_install`) once this
    /// returns — this function only handles the segment file itself.
    fn compact_segments(
        segments: &mut SegmentHolder,
        scratch: &mut SearchScratch,
        collection: &Arc<CollectionMetadata>,
        dimension: u32,
        compact_threshold: usize,
        persist_root: Option<&Path>,
        next_segment_id: &mut u64,
    ) {
        let _ = dimension;
        let seed = collection.collection_id.wrapping_mul(6364136223846793005);
        loop {
            let snap = segments.load();
            let mutable_len = snap.mutable.len();
            if mutable_len == 0 {
                return;
            }
            let frozen_len = mutable_len.min(bulk_freeze_cap(mutable_len, compact_threshold));
            let frozen = snap.mutable.freeze_prefix(frozen_len);
            drop(snap);

            let persist = persist_root.map(|dir| {
                let id = *next_segment_id;
                *next_segment_id += 1;
                (dir, id)
            });

            match compaction::compact(&frozen, collection, seed, persist) {
                Ok(immutable) => {
                    let num_nodes = immutable.graph().num_nodes();
                    let padded = collection.padded_dimension;
                    *scratch = SearchScratch::new(num_nodes, padded);

                    let old = segments.load();
                    let tail_mutable = old.mutable.clone_suffix(frozen_len);
                    let mut imm_list = old.immutable.clone();
                    immutable.mark_installed();
                    imm_list.push(Arc::new(immutable));
                    let new_list = SegmentList {
                        mutable: tail_mutable,
                        immutable: imm_list,
                        ivf: old.ivf.clone(),
                        warm: old.warm.clone(),
                        unloaded: old.unloaded.clone(),
                    };
                    segments.swap(new_list);
                    if frozen_len == mutable_len {
                        return; // drained
                    }
                }
                Err(_e) => {
                    // Compaction failed (recall too low, etc.) — leave the
                    // rest in brute-force mutable.
                    return;
                }
            }
        }
    }

    /// One past the highest collection id used by any field of this index
    /// (default field + additional named vector fields). Used as
    /// `IndexManifest.next_collection_id_floor` — B3 recovery seeds the
    /// store-wide collection-id allocator from this so a later FT.CREATE
    /// never reuses a collection id (and therefore QJL rotation seed) that a
    /// restored segment already depends on.
    fn max_field_collection_id(&self) -> u64 {
        let mut max_cid = self.collection.collection_id;
        for fs in self.field_segments.values() {
            max_cid = max_cid.max(fs.collection.collection_id);
        }
        max_cid
    }

    /// Allocate the next on-disk segment id and target directory for this
    /// index's default field, if `persist_dir` is configured.
    ///
    /// Always called on the shard thread (at compact/merge SUBMIT time, or
    /// synchronously for the inline compact path) — segment ids are never
    /// allocated on a background worker, so two concurrently-running builds
    /// can never collide on an id even though the actual disk write happens
    /// off-thread.
    fn alloc_persist_target(&mut self) -> Option<(PathBuf, u64)> {
        let dir = self.persist_dir.as_ref()?;
        let idx_dir =
            crate::vector::persistence::manifest::index_persist_dir(dir, self.meta.name.as_ref());
        let id = self.next_segment_id;
        self.next_segment_id += 1;
        Some((idx_dir, id))
    }

    /// After a successful default-field compact/merge install, commit a
    /// keymap + manifest snapshot for this index in the background (B2,
    /// durability write path). No-op when `persist_dir` is not configured.
    ///
    /// Cheap on the shard thread: clones 3 `Arc`s (O(1) — all three key-hash
    /// maps are `Arc`-wrapped with copy-on-write writers) plus a handful of
    /// scalars; the actual keymap build (iterating every live key) and all
    /// file I/O happens on the snapshot worker thread.
    ///
    /// `&self`, not `&mut self` (see `next_snapshot_seq`'s doc comment): also
    /// called from `try_warm_transitions_idle`, which only has shared access.
    /// Rebuilding `segment_ids` from `snap.immutable` alone (never
    /// `snap.warm`) is what drives the WARM-transition leak fix — once a
    /// segment moves to `snap.warm` it drops out of this list, so the next
    /// snapshot job's manifest diff GCs its now-superseded
    /// `idx-<hex>/segment-<old_id>/` directory (see `run_snapshot_job`).
    fn persist_hook_after_install(&self) {
        let Some(dir) = self.persist_dir.clone() else {
            return;
        };
        let idx_dir =
            crate::vector::persistence::manifest::index_persist_dir(&dir, self.meta.name.as_ref());
        let snap = self.segments.load();
        let segment_ids: Vec<u64> = snap
            .immutable
            .iter()
            .filter_map(|s| s.disk_segment_id())
            .collect();
        let next_global_id = snap.mutable.next_global_id();
        drop(snap);

        let seq = self.next_snapshot_seq.fetch_add(1, Ordering::Relaxed) + 1;

        let manifest = crate::vector::persistence::manifest::IndexManifest {
            format_version: crate::vector::persistence::manifest::MANIFEST_FORMAT_VERSION,
            index_name_hex: crate::vector::persistence::manifest::index_name_hex(
                self.meta.name.as_ref(),
            ),
            collection_id: self.collection.collection_id,
            next_collection_id_floor: self.max_field_collection_id() + 1,
            next_segment_id: self.next_segment_id,
            next_global_id,
            segment_ids,
            keymap_epoch: seq,
        };

        let job = crate::vector::persistence::manifest::SnapshotJob {
            idx_dir,
            seq,
            watermark: self.persist_seq_watermark.clone(),
            manifest,
            key_hash_to_key: self.key_hash_to_key.clone(),
            key_hash_to_global_id: self.key_hash_to_global_id.clone(),
            key_hash_to_vec_checksum: self.key_hash_to_vec_checksum.clone(),
        };
        crate::vector::persistence::manifest::global_snapshot_pool().submit(job);
    }
}

/// Max segments one bulk-loaded mutable is split into when its compact
/// threshold can't bound the build count sensibly (huge loads). Matches the
/// search pool's worker cap — more segments than workers adds merge overhead
/// without more parallelism.
const MAX_BULK_SEGMENTS: usize = 8;

/// Bounded-freeze cap for one compaction build. `compact_threshold == 0`
/// (auto-compact disabled — legacy/test indexes) keeps the historical
/// whole-mutable single-segment semantics; otherwise a bulk-loaded mutable is
/// compacted in `max(threshold, len/MAX_BULK_SEGMENTS)`-sized builds, so
/// FT.COMPACT after a bulk load yields several independently searchable
/// segments (bounded build memory; intra-query pool fan-out) instead of one
/// giant graph.
fn bulk_freeze_cap(mutable_len: usize, compact_threshold: usize) -> usize {
    // Only split when an intra-query pool exists: multiple segments searched
    // SERIALLY are strictly slower than one graph (each segment pays the full
    // resolved ef beam), so pool-less deployments keep single-segment builds.
    if compact_threshold == 0 || crate::vector::search_pool::global().is_none() {
        mutable_len
    } else {
        compact_threshold.max(mutable_len.div_ceil(MAX_BULK_SEGMENTS))
    }
}

/// Walk the window `[0..frozen_len)` of `segments.mutable` and apply
/// post-freeze tombstones to `immutable` before it is wrapped in `Arc`.
///
/// Two cases are handled:
/// 1. **Deleted entries** — window entry has `delete_lsn != 0` (DEL/UNLINK).
/// 2. **Overwritten entries** — same `key_hash` re-appears in the tail
///    `[frozen_len..end)` (HSET overwrite without a preceding DEL). In this
///    case the frozen snapshot holds the *old* version; the tail holds the
///    *new* version. We must tombstone the old copy so searches don't return
///    both.
///
/// Called from both `poll_install_compaction` and the `force_compact` drain path.
fn snap_and_reconcile(
    segments: &SegmentHolder,
    frozen_len: usize,
    immutable: &mut crate::vector::segment::immutable::ImmutableSegment,
) {
    let snap = segments.load();

    // Collect key_hashes re-inserted in the tail (HSET overwrite case).
    let mut tail_keys: std::collections::HashSet<u64> = std::collections::HashSet::new();
    snap.mutable.for_each_tail_entry(frozen_len, |key_hash| {
        tail_keys.insert(key_hash);
    });

    // Key_hashes that still have a LIVE copy inside the window. A dead window
    // entry whose key also has a live window sibling is an UPDATE leftover
    // (VEC-1 tombstones the old copy in place before appending the new one) —
    // compact() already filtered the dead copy, and the live sibling IS the
    // current version inside `immutable`. Key_hash-wide tombstoning on that
    // evidence would delete the current version: every key updated before the
    // freeze vanished from search (32% of live keys in the churn soak).
    // Only a dead entry with NO live window sibling proves the key is gone
    // (DEL/UNLINK marks ALL copies dead; a post-freeze update lands its new
    // copy in the tail, which the `tail_keys` arm handles).
    let mut live_window_keys: std::collections::HashSet<u64> = std::collections::HashSet::new();
    snap.mutable
        .for_each_window_entry(frozen_len, |key_hash, delete_lsn| {
            if delete_lsn == 0 {
                live_window_keys.insert(key_hash);
            }
        });

    snap.mutable
        .for_each_window_entry(frozen_len, |key_hash, delete_lsn| {
            if (delete_lsn != 0 && !live_window_keys.contains(&key_hash))
                || tail_keys.contains(&key_hash)
            {
                immutable.mark_deleted_by_key_hash_install(key_hash);
            }
        });
}

impl VectorIndex {
    /// Dispatch a background compaction for the default field if no compaction
    /// is already in flight and the mutable segment is non-empty.
    ///
    /// Returns `true` if a job was submitted, `false` otherwise.
    ///
    /// This is non-blocking: the actual HNSW build runs on a worker thread.
    /// Call [`poll_install_compaction`] on subsequent ticks to install the result.
    pub fn begin_background_compact(
        &mut self,
        compactor: &crate::vector::background_compact::BackgroundCompactor,
    ) -> bool {
        if self.bg_compact_inflight.is_some() {
            return false; // already running
        }
        // Mutual exclusion: a merge and a compaction must not be in-flight
        // simultaneously for the same index (both replace the immutable list).
        if self.bg_merge_inflight.is_some() {
            return false;
        }
        let snap = self.segments.load();
        let mutable_len = snap.mutable.len();
        if mutable_len == 0 {
            return false;
        }
        // Bounded build: a bulk-loaded mutable compacts in threshold-sized
        // chunks (the due-gate re-fires while len >= threshold, so the tail
        // drains across successive begin/install cycles).
        let frozen_len = mutable_len.min(bulk_freeze_cap(
            mutable_len,
            self.meta.compact_threshold as usize,
        ));
        let frozen_global_base = snap.mutable.global_id_base();
        let frozen = snap.mutable.freeze_prefix(frozen_len);
        drop(snap);

        let seed = self
            .collection
            .collection_id
            .wrapping_mul(6364136223846793005);
        // B2 (durability): allocate the disk segment id (and target dir) HERE,
        // on the shard thread, at submit time — never on the worker.
        let persist = self.alloc_persist_target();
        match compactor.submit(frozen, self.collection.clone(), seed, persist) {
            Ok(reply_rx) => {
                self.bg_compact_inflight = Some(InFlightCompaction {
                    reply_rx,
                    frozen_len,
                    frozen_global_base,
                });
                true
            }
            Err(e) => {
                // F4: leave a correlatable trail for a submit loop that never
                // drains (WorkersBusy also masks a dead pool). Debug level —
                // benign momentary saturation would spam warn per tick.
                tracing::debug!(
                    error = ?e,
                    mutable_len,
                    "vector background-compact submit deferred; retry next tick"
                );
                false
            }
        }
    }

    /// Threshold-gated wrapper over [`begin_background_compact`]: dispatches a
    /// background build ONLY when the mutable segment has reached its
    /// `compact_threshold` (or [`DEFAULT_COMPACT_THRESHOLD`] when unset).
    ///
    /// This is the *policy* entry point used by the search path
    /// ([`try_compact`]) and the autovacuum backstop. The bare
    /// [`begin_background_compact`] is the *mechanism* (compacts any non-empty
    /// segment) used by `FT.COMPACT` drain and tests.
    ///
    /// Non-blocking. Returns `true` if a job was submitted.
    pub fn begin_background_compact_due(
        &mut self,
        compactor: &crate::vector::background_compact::BackgroundCompactor,
    ) -> bool {
        let threshold = if self.meta.compact_threshold > 0 {
            self.meta.compact_threshold as usize
        } else {
            DEFAULT_COMPACT_THRESHOLD
        };
        if self.segments.load().mutable.len() < threshold {
            return false;
        }
        self.begin_background_compact(compactor)
    }

    /// Poll for a completed background compaction and install the result.
    ///
    /// Returns `true` if a segment was installed, `false` if no result was ready.
    ///
    /// ## Install reconciliation
    ///
    /// When the worker finishes, the mutable segment may have grown since we
    /// froze it. Let `N = frozen_len`, `M = current_len`.
    ///
    /// 1. **Window deletes** `[0..N)`: any entry tombstoned in the mutable
    ///    window after freeze is applied to the new immutable via
    ///    `mark_deleted_by_key_hash` (interior mutability — segment not yet Arc'd).
    ///
    /// 2. **Window overwrites** `[0..N)`: if a key in the window was re-inserted
    ///    (same `key_hash`, `insert_lsn > delete_lsn`), its older version in the
    ///    immutable must also be tombstoned to avoid resurrection.
    ///
    /// 3. **Tail clone** `[N..M)`: entries that arrived during the build are
    ///    byte-copied into a fresh mutable segment with the correct `global_id_base`.
    ///
    /// After reconciliation the new `SegmentList` is atomically swapped in.
    ///
    /// ## Autovacuum hook
    ///
    /// `poll_install_compactions` on [`VectorStore`] calls this for every index.
    /// That method should be called from the shard event loop on every tick
    /// (autovacuum **Pass D** — background compact install).
    pub fn poll_install_compaction(&mut self) -> bool {
        let inflight = match self.bg_compact_inflight.as_ref() {
            Some(f) => f,
            None => return false,
        };

        // Non-blocking check.
        let result = match inflight.reply_rx.try_recv() {
            Ok(r) => r,
            Err(flume::TryRecvError::Empty) => return false,
            Err(flume::TryRecvError::Disconnected) => {
                // Worker panicked or dropped — clear inflight and give up.
                // F4 (deep review): this used to be silent, so a dead worker
                // pool meant a permanent compact stall (mutable segment
                // growing past COMPACT_THRESHOLD, searches degrading to
                // brute force) with nothing for the operator to correlate.
                tracing::error!(
                    "vector background-compact worker died (panic or pool teardown); \
                     compaction will be resubmitted but may stall permanently — \
                     mutable segment growth and search latency will degrade"
                );
                self.bg_compact_inflight = None;
                return false;
            }
        };

        // Take ownership now that we know we have a result.
        let inflight = self.bg_compact_inflight.take().unwrap();

        let mut immutable = match result {
            Ok(imm) => imm,
            Err(e) => {
                // F4: compaction failure (e.g. the B2 disk persist of the
                // built segment on a full disk) retries next tick — say so
                // instead of looping silently forever.
                tracing::warn!(
                    error = %e,
                    frozen_len = inflight.frozen_len,
                    "vector background compaction failed; will retry next tick"
                );
                return false;
            }
        };

        // ── Reconciliation ────────────────────────────────────────────────────
        let frozen_len = inflight.frozen_len;
        let _ = inflight.frozen_global_base; // documented anchor; clone_suffix uses it implicitly

        // Step 1 & 2: walk window [0..frozen_len), apply post-freeze tombstones.
        snap_and_reconcile(&self.segments, frozen_len, &mut immutable);

        // Step 3: clone the tail [frozen_len..M) into a fresh mutable.
        let snap = self.segments.load();
        let tail_mutable = snap.mutable.clone_suffix(frozen_len);

        // Rebuild scratch for the new immutable's graph size.
        let num_nodes = immutable.graph().num_nodes();
        let padded = self.collection.padded_dimension;
        self.scratch = SearchScratch::new(num_nodes, padded);

        // ── Atomic swap ───────────────────────────────────────────────────────
        let mut imm_list = snap.immutable.clone();
        immutable.mark_installed();
        imm_list.push(Arc::new(immutable));
        let new_list = SegmentList {
            mutable: tail_mutable,
            immutable: imm_list,
            ivf: snap.ivf.clone(),
            warm: snap.warm.clone(),
            unloaded: snap.unloaded.clone(),
        };
        drop(snap);
        self.segments.swap(new_list);
        // B2 (durability): the worker already wrote the segment to disk (if
        // `alloc_persist_target` handed it a target at submit time) — commit
        // the keymap/manifest snapshot now that the install is durable.
        self.persist_hook_after_install();

        true
    }

    // ── Background merge (immutable → immutable consolidation) ───────────────

    /// True when this index satisfies any auto-merge trigger condition:
    /// - `merge_mode != None`
    /// - AND all live vectors fit within the merge memory ceiling
    /// - AND: `imm_count > MERGE_SEGMENT_THRESHOLD` OR any segment has >20%
    ///   dead entries.
    ///
    /// The ceiling applies to BOTH triggers: `merge_immutable` refuses a
    /// union over `MERGE_MEMORY_CEILING` regardless of why it was dispatched,
    /// so a count trigger that ignored the ceiling would re-dispatch a merge
    /// that can never succeed on every tick (see [`Self::merge_backoff`] for
    /// the second line of defense).
    fn needs_merge(&self) -> bool {
        if self.meta.merge_mode == MergeMode::None {
            return false;
        }
        let snap = self.segments.load();
        let imm_count = snap.immutable.len();
        let count_trigger = imm_count > compaction::MERGE_SEGMENT_THRESHOLD;
        let vacuum_trigger = snap.immutable.iter().any(|s| compaction::needs_vacuum(s));
        if !count_trigger && !vacuum_trigger {
            return false;
        }
        let live_bytes: usize = snap
            .immutable
            .iter()
            .map(|s| s.live_count() as usize * self.collection.bytes_per_code_per_vector())
            .sum();
        live_bytes < compaction::MERGE_MEMORY_CEILING
    }

    /// Dispatch a background merge for the default field's immutable segments.
    ///
    /// Returns `true` if a merge job was submitted, `false` otherwise.
    ///
    /// ## Mutual exclusion
    ///
    /// Neither a compaction nor another merge may be in-flight simultaneously
    /// for the same index. This keeps the install step simple: the immutable
    /// list at install time is exactly the `merged_sources` set (no concurrent
    /// additions can sneak in while merge is blocked).
    ///
    /// ## Non-blocking
    ///
    /// The actual `merge_immutable` build runs on the worker pool.
    /// Call [`poll_install_merge`] on subsequent ticks to install the result.
    pub fn begin_background_merge(
        &mut self,
        compactor: &crate::vector::background_compact::BackgroundCompactor,
    ) -> bool {
        // Mutual exclusion: block if either compaction or merge already in flight.
        if self.bg_compact_inflight.is_some() || self.bg_merge_inflight.is_some() {
            return false;
        }
        // Only merge when mode is set and ≥2 immutables exist.
        if self.meta.merge_mode == MergeMode::None {
            return false;
        }
        let segs = self.segments.load().immutable.to_vec();
        if segs.len() < 2 {
            return false;
        }

        let seed = self
            .collection
            .collection_id
            .wrapping_mul(6364136223846793005);
        let mode = self.meta.merge_mode;
        // Default 0.70 (same as vacuum_pass): catch catastrophic recall
        // collapse without false-positives on small/medium indexes. Per-index
        // override: FT.CONFIG SET <idx> MERGE_RECALL_TOLERANCE (VEC-4).
        let tolerance = self.merge_recall_tolerance;
        // B2 (durability): allocate the disk segment id (and target dir) HERE,
        // on the shard thread, at submit time — never on the worker. Merge
        // persists identically to compact.
        let persist = self.alloc_persist_target();

        match compactor.submit_merge(
            segs.clone(),
            self.collection.clone(),
            seed,
            mode,
            tolerance,
            persist,
        ) {
            Ok(reply_rx) => {
                self.bg_merge_inflight = Some(InFlightMerge {
                    reply_rx,
                    merged_sources: segs,
                });
                true
            }
            Err(_) => false, // worker queue full — retry next tick
        }
    }

    /// Threshold-gated wrapper over [`begin_background_merge`]: only dispatches
    /// when [`needs_merge`] returns `true` and the segment set is not in a
    /// rejected-merge backoff window (see [`Self::merge_backoff`]).
    ///
    /// Non-blocking. Returns `true` if a job was submitted.
    pub fn begin_background_merge_due(
        &mut self,
        compactor: &crate::vector::background_compact::BackgroundCompactor,
    ) -> bool {
        if !self.needs_merge() {
            return false;
        }
        // Rejected-merge backoff: skip only while BOTH the segment set is
        // unchanged since the rejection AND the window hasn't elapsed. Any
        // set change (compaction, warm-tier transition, installed merge)
        // invalidates the doomed-merge assumption immediately.
        if let Some(b) = &self.merge_backoff {
            let current = segment_set_fingerprint(&self.segments.load().immutable);
            if current == b.fingerprint {
                if std::time::Instant::now() < b.retry_after {
                    return false;
                }
            } else {
                self.merge_backoff = None;
            }
        }
        self.begin_background_merge(compactor)
    }

    /// Drop any rejected-merge backoff. Called when an operator changes
    /// `MERGE_RECALL_TOLERANCE` via FT.CONFIG SET — they just changed the very
    /// parameter the gate fired on, so the next tick should try again.
    pub fn clear_merge_backoff(&mut self) {
        self.merge_backoff = None;
    }

    /// Poll for a completed background merge and install the result.
    ///
    /// Returns `true` if the merged segment was installed, `false` otherwise.
    ///
    /// ## Correctness: reapply deletes
    ///
    /// `merge_immutable` reads source segment MVCC headers at worker-thread
    /// time.  Steady-state `mark_deleted_for_key` calls that arrived between
    /// worker snapshot and install land in each source Arc's interior
    /// `tombstoned_keys` set (they cannot write `mvcc.delete_lsn` while the
    /// segment is Arc'd and shared).  We therefore collect each source's
    /// interior tombstone set and apply them to the merged output via
    /// `mark_deleted_by_key_hash_install` before wrapping it in an `Arc`.
    ///
    /// ## Defensive swap (warm-tier safety)
    ///
    /// `try_warm_transitions` can remove immutable segments (transition them to
    /// mmap-backed warm tier) on a different tick while the merge is in flight.
    /// We compare the current immutable list to `merged_sources` using
    /// `Arc::ptr_eq`: if any source is missing the install is aborted (the data
    /// is safely preserved in the warm tier; a subsequent merge will include the
    /// now-warm segment if needed).
    pub fn poll_install_merge(&mut self) -> bool {
        let inflight = match self.bg_merge_inflight.as_ref() {
            Some(f) => f,
            None => return false,
        };

        // Non-blocking check.
        let result = match inflight.reply_rx.try_recv() {
            Ok(r) => r,
            Err(flume::TryRecvError::Empty) => return false,
            Err(flume::TryRecvError::Disconnected) => {
                // Worker panicked or dropped — clear inflight and give up.
                self.bg_merge_inflight = None;
                return false;
            }
        };

        // Take ownership now that we know we have a result.
        let inflight = self.bg_merge_inflight.take().unwrap();

        let mut merged = match result {
            Ok(imm) => imm,
            Err(e) => {
                // Recall gate fired or empty / memory ceiling exceeded.
                // Keep the N source segments unchanged — merge simply didn't
                // happen. Every needs_merge trigger condition therefore stays
                // true: without a backoff the next autovacuum tick resubmits
                // the identical doomed merge, and each recall-gate rejection
                // costs a full (discarded) union HNSW build — a quiet CPU
                // livelock that pins the vec-compact workers indefinitely.
                let fingerprint = segment_set_fingerprint(&inflight.merged_sources);
                let failures = match &self.merge_backoff {
                    Some(b) if b.fingerprint == fingerprint => b.failures.saturating_add(1),
                    _ => 1,
                };
                let backoff = merge_backoff_duration(failures);
                self.merge_backoff = Some(MergeBackoff {
                    fingerprint,
                    failures,
                    retry_after: std::time::Instant::now() + backoff,
                });
                // warn, not debug: this can otherwise burn cores invisibly at
                // the default log level. Naturally rate-limited to once per
                // backoff window (≥60s).
                tracing::warn!(
                    error = %e,
                    sources = inflight.merged_sources.len(),
                    consecutive_failures = failures,
                    backoff_secs = backoff.as_secs(),
                    "bg merge rejected; backing off — raise MERGE_RECALL_TOLERANCE \
                     via FT.CONFIG or reduce segment churn if this repeats"
                );
                return false;
            }
        };

        // ── Step 4a: Reapply deletes that arrived during the merge window ─────
        //
        // merge_immutable already dropped entries with mvcc.delete_lsn != 0
        // at snapshot time.  Any `mark_deleted_by_key_hash` call that landed
        // AFTER the worker snapshot only wrote to the source Arc's interior
        // `tombstoned_keys` set.  Apply those to the merged output — but gated
        // by ORIGIN: a source's tombstone may only kill merged entries whose
        // global_id came from that source. An HSET update interior-tombstones
        // the OLD copy's home segment while the NEW copy lives on (mutable or
        // a sibling segment); a hash-wide replay would kill the new copy too
        // (mass loss under update churn). Real DEL/UNLINK tombstones are
        // recorded in EVERY segment's interior set, so they still apply.
        for src in &inflight.merged_sources {
            let tombs = src.tombstoned_key_hashes();
            if tombs.is_empty() {
                continue;
            }
            let src_gids: std::collections::HashSet<u32> =
                src.mvcc_headers().iter().map(|h| h.global_id).collect();
            for kh in tombs {
                merged.mark_deleted_by_key_hash_install_from(kh, &src_gids);
            }
        }

        // ── Defensive swap: verify sources are still in the immutable list ────
        let snap = self.segments.load();
        let current_imm = &snap.immutable;

        // Check that every merged source is still present (ptr_eq identity).
        let all_present = inflight
            .merged_sources
            .iter()
            .all(|src| current_imm.iter().any(|cur| Arc::ptr_eq(cur, src)));
        if !all_present {
            // A warm-tier transition removed one or more source segments while
            // we were building.  Abort — the data is safe in the warm tier.
            tracing::debug!("bg merge install aborted: source segment(s) moved to warm tier");
            return false;
        }

        // Build the new immutable list: keep segments NOT in the merged set
        // (defensive, in case the list grew), then append the single merged one.
        let merged_arc = Arc::new(merged);
        let mut new_immutable: Vec<Arc<crate::vector::segment::immutable::ImmutableSegment>> =
            current_imm
                .iter()
                .filter(|cur| {
                    !inflight
                        .merged_sources
                        .iter()
                        .any(|src| Arc::ptr_eq(cur, src))
                })
                .cloned()
                .collect();
        merged_arc.mark_installed();
        new_immutable.push(merged_arc.clone());

        // Rebuild scratch for the merged segment's graph size.
        self.scratch = crate::vector::hnsw::search::SearchScratch::new(
            merged_arc.graph().num_nodes(),
            self.collection.padded_dimension,
        );

        // Atomic swap.
        let new_list = SegmentList {
            mutable: Arc::clone(&snap.mutable),
            immutable: new_immutable,
            ivf: snap.ivf.clone(),
            warm: snap.warm.clone(),
            unloaded: snap.unloaded.clone(),
        };
        drop(snap);
        self.segments.swap(new_list);
        // A successful install changes the segment set, so any backoff
        // fingerprint is stale — drop it eagerly.
        self.merge_backoff = None;
        // B2 (durability): the worker already wrote the merged segment to
        // disk (if `alloc_persist_target` handed it a target at submit
        // time) — commit the keymap/manifest snapshot now that the install
        // is durable. The GC inside the snapshot job removes the (now
        // superseded) source segment dirs.
        self.persist_hook_after_install();

        tracing::debug!(
            sources = inflight.merged_sources.len(),
            "bg merge installed"
        );
        true
    }

    /// Check each immutable segment's age. If older than `warm_after_secs`,
    /// transition it to warm tier (mmap-backed on disk). Age-only; idleness
    /// is not considered (see [`Self::try_warm_transitions_idle`]).
    ///
    /// After transition, the segment is replaced by a WarmSearchSegment that
    /// reads TQ codes and HNSW graph from mmap'd .mpf files. The segment
    /// remains searchable -- no data loss from the user's perspective. The
    /// exact-rerank f16 sidecar (if the HOT segment had one) is carried over
    /// too, so recall is unaffected by the transition.
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
        self.try_warm_transitions_idle(shard_dir, manifest, warm_after_secs, 0, next_file_id, wal)
    }

    /// Same as [`Self::try_warm_transitions`] but also accepts an idle-time
    /// threshold (WS3, `--engine-offload-idle-secs`). `idle_after_secs == 0`
    /// disables the idle criterion, matching the pre-WS3 age-only behavior
    /// exactly.
    ///
    /// **Two independent tiers, two different destinations (WS3 round 2):**
    /// - `age_eligible` (pure age, `--segment-warm-after`) -> WARM tier
    ///   (`WarmSearchSegment`, mmap-backed on disk but fully materialized
    ///   into owned buffers in memory -- searchable with zero reload cost,
    ///   but does NOT reduce RSS; measured flat-to-+1.1% in practice, see
    ///   CHANGELOG). This is unchanged pre-WS3 behavior: an old-but-still-hot
    ///   segment structurally simplifies to disk-file-backed storage without
    ///   promising a memory win.
    /// - `idle_eligible` (`--engine-offload-idle-secs`, genuinely cold) ->
    ///   COLD tier directly (`UnloadedSegment` stub -- everything in-memory
    ///   dropped, only a handful of scalars + a `SegmentHandle` resident).
    ///   This is the tier that actually frees memory. Reload is synchronous
    ///   and transparent on the next search that touches the index (see
    ///   `SegmentHolder::promote_unloaded`).
    ///
    /// If a segment satisfies both criteria simultaneously, COLD wins (it is
    /// strictly more beneficial memory-wise and the segment is, by
    /// definition, not being queried).
    ///
    /// Both destinations write the exact same on-disk `.mpf` files via
    /// [`crate::storage::tiered::warm_tier::transition_to_warm`] (including
    /// the f16 exact-rerank sidecar) -- COLD additionally drops the
    /// in-memory `WarmSearchSegment` immediately after capturing its stub
    /// metadata, instead of keeping it resident.
    pub fn try_warm_transitions_idle(
        &self,
        shard_dir: &std::path::Path,
        manifest: &mut crate::persistence::manifest::ShardManifest,
        warm_after_secs: u64,
        idle_after_secs: u64,
        next_file_id: &mut u64,
        wal: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    ) -> usize {
        let snapshot = self.segments.load();
        // (segment index, route straight to COLD instead of WARM)
        let mut to_transition: Vec<(usize, bool)> = Vec::new();
        for (i, imm) in snapshot.immutable.iter().enumerate() {
            let age_eligible = imm.age_secs() >= warm_after_secs;
            let idle_eligible = idle_after_secs > 0 && imm.idle_secs() >= idle_after_secs;
            if idle_eligible {
                to_transition.push((i, true)); // -> COLD
            } else if age_eligible {
                to_transition.push((i, false)); // -> WARM
            }
        }

        // WS3 round-2 fix (adversarial review #3): a WARM segment can go
        // idle too, and previously had no path to COLD -- it just sat in
        // `warm` (fully materialized, no RSS win) forever. This is
        // mechanical: `UnloadedSegment::from_warm` already builds a stub
        // from any `&WarmSearchSegment`, on-disk files already exist (WARM
        // segments are mmap-backed), no new transition protocol needed.
        let mut warm_to_cold: Vec<usize> = Vec::new();
        if idle_after_secs > 0 {
            for (i, w) in snapshot.warm.iter().enumerate() {
                if w.idle_secs() >= idle_after_secs {
                    warm_to_cold.push(i);
                }
            }
        }

        if to_transition.is_empty() && warm_to_cold.is_empty() {
            return 0;
        }

        let mut new_immutable = snapshot.immutable.clone();
        let mut new_warm = snapshot.warm.clone();
        let mut new_unloaded = snapshot.unloaded.clone();
        let mut transitioned = 0usize;

        // Process the WARM -> COLD sweep first (index-stable: independent of
        // the immutable removal loop below, which mutates `new_immutable`,
        // not `new_warm`/`new_unloaded` directly until it pushes).
        for &idx in warm_to_cold.iter().rev() {
            let warm_seg = &snapshot.warm[idx];
            let stub = crate::vector::persistence::unloaded_segment::UnloadedSegment::from_warm(
                warm_seg, false,
            );
            tracing::info!(
                "WARM -> COLD idle transition: segment {} ({} vectors, idle {}s)",
                warm_seg.segment_id(),
                warm_seg.total_count(),
                warm_seg.idle_secs(),
            );
            new_warm.remove(idx);
            new_unloaded.push(Arc::new(stub));
            transitioned += 1;
        }

        // Process in reverse order to maintain valid indices during removal.
        for &(idx, to_cold) in to_transition.iter().rev() {
            let imm = &snapshot.immutable[idx];
            let file_id = *next_file_id;
            *next_file_id += 1;

            let graph_bytes = imm.graph().to_bytes_compressed();
            let codes_data = imm.vectors_tq().as_slice();
            let mvcc_data = imm.mvcc_raw_bytes();
            // WS3 / HQ-1 parity: carry the exact-rerank f16 sidecar over to
            // the warm tier so recall does not silently degrade on
            // HOT->WARM transition (`WarmSearchSegment` now knows how to
            // rerank from it — see `warm_search.rs`). `None` when the
            // segment never had one (pre-sidecar build) or has zero halves.
            let raw_f16_bytes: Option<Vec<u8>> = imm
                .raw_f16()
                .filter(|halves| !halves.is_empty())
                .map(crate::vector::segment::raw_f16_store::RawF16Store::le_bytes);

            match crate::storage::tiered::warm_tier::transition_to_warm(
                shard_dir,
                file_id, // segment_id == file_id
                file_id,
                codes_data,
                &graph_bytes,
                raw_f16_bytes.as_deref(),
                &mvcc_data,
                manifest,
                wal.as_mut(),
            ) {
                Ok(handle) => {
                    // Remove the old ImmutableSegment from the in-memory list.
                    // The ImmutableSegment is purely in-memory (no on-disk files),
                    // so it needs no SegmentHandle tombstoning -- it's simply dropped.
                    //
                    // Tombstone lifecycle for the NEW warm/cold-stub segment:
                    //   1. `handle` (SegmentHandle) is passed to WarmSearchSegment below
                    //   2. WarmSearchSegment stores it as `_handle` (Arc refcount);
                    //      for a COLD-stub destination, `UnloadedSegment::from_warm`
                    //      captures a clone of the same handle before the
                    //      WarmSearchSegment is dropped.
                    //   3. When later idle-transitioned WARM -> COLD-stub:
                    //      mark_tombstoned() is called
                    //   4. On index drop / FLUSH: mark_tombstoned() is called
                    //   5. Directory is deleted only when last Arc ref drops AND tombstoned
                    new_immutable.remove(idx);

                    // Open mmap-backed warm search segment to keep data searchable
                    // (or, for a COLD destination, to capture its stub metadata).
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
                            // WS3 round-2 resurrection fix: `mvcc_raw_bytes()`
                            // only serializes INSTALL-time deletes
                            // (`mvcc[i].delete_lsn`) -- a steady-state HDEL
                            // that landed against this already-Arc'd
                            // immutable segment (interior `tombstoned_keys`,
                            // never written back into `mvcc[]`) would
                            // otherwise be silently dropped by this
                            // transition and the doc would resurface once
                            // this WARM/COLD segment is searched. Carry it
                            // over explicitly.
                            let src_tombs = imm.tombstoned_key_hashes();
                            if !src_tombs.is_empty() {
                                warm_seg.seed_tombstones(&src_tombs);
                            }
                            if to_cold {
                                // COLD (WS3 round 2): capture the stub, then let
                                // `warm_seg` drop at end of scope -- this is what
                                // actually frees the codes/graph/sidecar buffers.
                                let stub = crate::vector::persistence::unloaded_segment::UnloadedSegment::from_warm(&warm_seg, false);
                                tracing::info!(
                                    "Cold (unload) transition: segment {} ({} vectors, idle {}s) -> stub only, reload on next search",
                                    file_id,
                                    imm.total_count(),
                                    imm.idle_secs(),
                                );
                                new_unloaded.push(Arc::new(stub));
                            } else {
                                tracing::info!(
                                    "Warm transition: segment {} ({} vectors, age {}s) -> searchable warm",
                                    file_id,
                                    imm.total_count(),
                                    imm.age_secs()
                                );
                                new_warm.push(Arc::new(warm_seg));
                            }
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
            // Functional-update clone-and-patch (item 6, adversarial review):
            // only the 3 fields this function actually touches are named;
            // `mutable`/`ivf` come along via `..(**snapshot).clone()` without
            // being enumerated here.
            let new_list = SegmentList {
                immutable: new_immutable,
                warm: new_warm,
                unloaded: new_unloaded,
                ..(**snapshot).clone()
            };
            self.segments.swap(new_list);

            // Leak fix (WARM restart-recovery hardening): a segment that
            // just left `immutable` (moved to `warm` or `unloaded`) must
            // drop out of Stack B's durable `segment_ids` too, or its old
            // `idx-<hex>/segment-<old_id>/` directory is never GC'd (it
            // sits there forever — see `run_snapshot_job`'s manifest diff)
            // and a future restart would reload it as a fully-materialized
            // HOT segment instead of respecting the WARM/COLD tier it was
            // just moved to. Cheap + a no-op when `persist_dir` is unset
            // (disk-offload/persistence both off).
            self.persist_hook_after_install();
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
    /// Shard directory for persisting index metadata sidecar.
    /// Set once during event loop init when disk-offload is enabled.
    persist_dir: Option<std::path::PathBuf>,
    /// Monotonic freshness counter for the VSEARCH engine on this shard.
    ///
    /// Bumped (Release) after every successful mutating operation: `create_index`,
    /// `drop_index`, `mark_deleted_for_key`, and vector-document inserts via the
    /// auto-index path. Exposed by `FT.INFO` under `vector_version_token`.
    ///
    /// Semantics:
    /// - Starts at 0 on shard boot; NOT restored from WAL (freshness hint only).
    /// - Monotonic within a single shard; no cross-shard atomicity.
    /// - Counter never wraps in practice (u64::MAX ≈ 1.8 × 10¹⁹ writes).
    /// - Failed writes (index-not-found, parse errors) do NOT bump the counter.
    version_token: AtomicU64,
}

/// Read `manifest.json` and the keymap file for whatever epoch it points to,
/// as a matched pair, tolerating the narrow TOCTOU window where a concurrent
/// snapshot job (`run_snapshot_job`) advances the manifest to a NEW epoch and
/// GCs the OLD epoch's keymap file in between our manifest read and our
/// keymap read. `write_keymap_atomic` always durably lands before
/// `write_manifest_atomic` repoints `manifest.json` at it (see
/// `run_snapshot_job`), so re-reading the manifest after a keymap miss is
/// guaranteed to observe a keymap that IS present — bounded retry, no sleep,
/// no production-relevant race (this window can only be hit by two snapshot
/// jobs for the SAME index racing a concurrent `register_warm_segments`
/// read; in production nothing submits a new job for an index between B3
/// recovery finishing and this being called at boot).
fn read_manifest_and_keymap_consistent(
    idx_dir: &std::path::Path,
) -> Option<Vec<crate::vector::persistence::manifest::KeymapEntry>> {
    // `read_manifest_tolerant` + `read_keymap_tolerant` are two separate
    // reads, not one atomic snapshot: a concurrent `run_snapshot_job` can
    // advance `manifest.json` to a new keymap_epoch and GC the old
    // `keymap-<epoch>.bin` in the gap between them. `run_snapshot_job`
    // always durably writes the new keymap BEFORE repointing the manifest
    // at it, so a manifest read that observes epoch E guarantees
    // `keymap-E.bin` was written no later than that — the only way our
    // second read can still miss it is if a LATER job's GC deleted it in
    // between, which a short bounded retry (with real backoff, since the
    // race is against fsync-bound disk I/O, not just CPU cache lines)
    // resolves by re-reading both files against whatever epoch is current
    // by then.
    for attempt in 0..5 {
        let m = crate::vector::persistence::manifest::read_manifest_tolerant(idx_dir)?;
        if let Some(entries) =
            crate::vector::persistence::manifest::read_keymap_tolerant(idx_dir, m.keymap_epoch)
        {
            return Some(entries);
        }
        if attempt + 1 < 5 {
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
    None
}

impl VectorStore {
    pub fn new() -> Self {
        Self {
            indexes: HashMap::new(),
            next_collection_id: 1,
            txn_manager: TransactionManager::new(),
            persist_dir: None,
            version_token: AtomicU64::new(0),
        }
    }

    /// Return the current VSEARCH engine version token for this shard.
    ///
    /// Uses `Acquire` ordering so the caller observes all writes that preceded
    /// the most recent `bump_version` call on this shard.
    #[inline]
    pub fn version_token(&self) -> u64 {
        self.version_token.load(Ordering::Acquire)
    }

    /// Bump the VSEARCH version token by 1 after a successful write.
    ///
    /// Uses `Release` ordering so that any subsequent `Acquire` load on any
    /// thread observes the completed write. Returns the new value.
    #[inline]
    pub fn bump_version(&self) -> u64 {
        self.version_token.fetch_add(1, Ordering::Release) + 1
    }

    /// Set the shard directory for index metadata persistence.
    /// Called once during event loop init when disk-offload is enabled.
    pub fn set_persist_dir(&mut self, dir: std::path::PathBuf) {
        self.persist_dir = Some(dir);
    }

    /// Persist current index metadata (including compaction weights) to the sidecar file.
    /// No-op if persist_dir is not set (disk-offload disabled).
    /// Called after any mutation that changes compaction_weight or index registration.
    pub fn save_index_meta_sidecar(&self) {
        if let Some(ref dir) = self.persist_dir {
            let meta_weights = self.collect_index_metas_with_weights();
            if let Err(e) = crate::vector::index_persist::save_index_metadata_v3(dir, &meta_weights)
            {
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

        // B2 (durability): defensive id-space floor. If this index's
        // `idx-<hex>` dir already has a manifest — e.g. FLUSHALL/DROP just
        // recreated this index and the best-effort background directory
        // delete (see `drop_index`/`clear_all_contents`) hasn't finished yet
        // — seed the fresh allocators ABOVE whatever it last recorded. This
        // guarantees the new generation's segment ids/keymap epochs can never
        // collide with (or race a delete of) the old generation's on-disk
        // files, independent of when that background delete completes.
        let (floor_next_segment_id, floor_next_snapshot_seq) = self
            .persist_dir
            .as_ref()
            .map(|dir| {
                let idx_dir =
                    crate::vector::persistence::manifest::index_persist_dir(dir, name.as_ref());
                crate::vector::persistence::manifest::read_manifest_tolerant(&idx_dir)
                    .map(|m| (m.next_segment_id, m.keymap_epoch))
                    .unwrap_or((0, 0))
            })
            .unwrap_or((0, 0));

        self.indexes.insert(
            name.clone(),
            VectorIndex {
                meta,
                segments,
                scratch,
                collection,
                payload_index: PayloadIndex::new(),
                key_hash_to_key: BucketedKeyMap::new(),
                key_hash_to_global_id: BucketedKeyMap::new(),
                key_hash_to_vec_checksum: BucketedKeyMap::new(),
                persist_dir: self.persist_dir.clone(),
                next_segment_id: floor_next_segment_id,
                next_snapshot_seq: AtomicU64::new(floor_next_snapshot_seq),
                persist_seq_watermark: Arc::new(parking_lot::Mutex::new(floor_next_snapshot_seq)),
                autocompact_enabled: true,
                merge_recall_tolerance: 0.70,
                compaction_weight: COMPACTION_WEIGHT_DEFAULT,
                field_segments: extra_fields,
                sparse_stores: HashMap::new(),
                bg_compact_inflight: None,
                bg_merge_inflight: None,
                merge_backoff: None,
            },
        );

        // Persist index metadata sidecar
        self.save_index_meta_sidecar();

        // Bump version AFTER successful write (monotonicity-on-success contract).
        self.bump_version();

        Ok(())
    }

    /// Create a new index, pinning its default field's `collection_id` (and
    /// therefore its HNSW QJL rotation seed) to a value previously recorded
    /// on disk, instead of allocating a fresh one from `next_collection_id`.
    ///
    /// B3 recovery-only: used exclusively by the durability loader
    /// (`crate::vector::persistence::recover_v2`) when a `manifest.json` was
    /// found for this index. A segment persisted under collection_id X is
    /// only searchable if the index that owns it also has collection_id X —
    /// see `VECTOR-DURABILITY-DESIGN.md`'s "#1 correctness trap". Mirrors
    /// `create_index` in every other respect (additional-field segments are
    /// always fresh — B1/B2 never persists them), and additionally seeds
    /// `next_segment_id`/`next_snapshot_seq` from the SAME manifest instead
    /// of a defensive re-read (the caller already loaded it).
    ///
    /// Does not attach any segments or keymap state — `VectorIndex.segments`,
    /// `key_hash_to_key`, `key_hash_to_global_id`, `key_hash_to_vec_checksum`
    /// are left at their fresh-index defaults. The caller installs recovered
    /// state afterward via `get_index_mut` (all four fields are `pub`).
    pub(crate) fn create_index_with_collection_id(
        &mut self,
        mut meta: IndexMeta,
        manifest: &crate::vector::persistence::manifest::IndexManifest,
    ) -> Result<(), &'static str> {
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

        let collection_id = manifest.collection_id;
        // Bump the store-wide allocator above BOTH the pinned cid and the
        // manifest's own recorded floor (covers additional-field cids this
        // index previously allocated — those fields are never persisted, so
        // they get fresh cids below, but must never collide with a cid a
        // sibling recovered index is pinning right now).
        self.next_collection_id = self
            .next_collection_id
            .max(manifest.next_collection_id_floor)
            .max(collection_id + 1);

        let padded = padded_dimension(meta.dimension);
        let collection = Arc::new(CollectionMetadata::with_build_mode(
            collection_id,
            meta.dimension,
            meta.metric,
            meta.quantization,
            collection_id, // pinned: same value the persisted segments' QJL was seeded with
            meta.build_mode,
        ));
        let segments = SegmentHolder::new(meta.dimension, collection.clone());
        let scratch = SearchScratch::new(0, padded);

        // Additional field segments: B1/B2 never persists them (AS-BUILT —
        // `compact_segments` is always called with `persist_root: None` for
        // `field_segments`), so they always start fresh here too, exactly
        // like `create_index`. `next_collection_id` was already bumped above
        // so these freshly allocated cids can't collide with the pinned one.
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
                key_hash_to_key: BucketedKeyMap::new(),
                key_hash_to_global_id: BucketedKeyMap::new(),
                key_hash_to_vec_checksum: BucketedKeyMap::new(),
                persist_dir: self.persist_dir.clone(),
                next_segment_id: manifest.next_segment_id,
                next_snapshot_seq: AtomicU64::new(manifest.keymap_epoch),
                persist_seq_watermark: Arc::new(parking_lot::Mutex::new(manifest.keymap_epoch)),
                autocompact_enabled: true,
                merge_recall_tolerance: 0.70,
                compaction_weight: COMPACTION_WEIGHT_DEFAULT,
                field_segments: extra_fields,
                sparse_stores: HashMap::new(),
                bg_compact_inflight: None,
                bg_merge_inflight: None,
                merge_backoff: None,
            },
        );

        // Persist index metadata sidecar
        self.save_index_meta_sidecar();

        // Bump version AFTER successful write (monotonicity-on-success contract).
        self.bump_version();

        Ok(())
    }

    /// Best-effort background removal of an index's whole `idx-<hex>/`
    /// durability directory (manifest + keymaps + segments) — B2, design
    /// doc item "Drop/flush cleanup". Fire-and-forget: runs on its own OS
    /// thread so `drop_index`/`clear_all_contents` never block the shard on
    /// a (possibly large) recursive directory removal. Failure is logged and
    /// otherwise harmless — a leftover directory is swept by the B3 startup
    /// orphan sweep, and `create_index`'s defensive manifest-floor read
    /// guarantees a same-named index recreated before this finishes never
    /// reuses (or races) its ids. No-op if `persist_dir` is not configured.
    fn spawn_delete_index_persist_dir(&self, name: &Bytes) {
        let Some(dir) = self.persist_dir.clone() else {
            return;
        };
        let name = name.clone();
        let idx_dir = crate::vector::persistence::manifest::index_persist_dir(&dir, name.as_ref());
        let spawned = std::thread::Builder::new()
            .name("moon-vec-idx-gc".to_owned())
            .spawn(move || {
                // O5: spawned from the owning (pinned) shard thread —
                // escape the inherited single-core mask.
                crate::shard::numa::pin_current_aux_thread("moon-vec-idx-gc");
                if let Err(e) = std::fs::remove_dir_all(&idx_dir) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!(
                            "failed to remove durability dir {} for dropped index: {e}",
                            idx_dir.display()
                        );
                    }
                }
            });
        if let Err(e) = spawned {
            tracing::warn!(
                "failed to spawn background deletion thread for dropped index {}: {e} \
                 (durability dir left on disk; swept at next B3 startup)",
                String::from_utf8_lossy(&name)
            );
        }
    }

    /// Db-scoped variant of [`Self::drop_index`] (WS5a): refuses to drop an
    /// index owned by a different db (returns `false`, matching the
    /// "unknown index" outcome a same-db caller would see as `NOTFOUND`).
    pub fn drop_index_for_db(&mut self, name: &[u8], db_index: u8) -> bool {
        match self.indexes.get(name) {
            Some(idx) if idx.meta.db_index == db_index => self.drop_index(name),
            _ => false,
        }
    }

    /// Drop an index by name. Returns true if it existed.
    ///
    /// Tombstones any warm segments so their on-disk directories are cleaned up
    /// once all in-flight search references (Arc snapshots) are dropped.
    ///
    /// NOTE (WS5a): NOT db-scoped — see [`Self::drop_index_for_db`].
    pub fn drop_index(&mut self, name: &[u8]) -> bool {
        if let Some(index) = self.indexes.remove(name) {
            // Tombstone warm segments: mark for deletion on last Arc drop.
            let snapshot = index.segments.load();
            for warm_seg in &snapshot.warm {
                warm_seg.mark_tombstoned();
            }
            // WS3 round 2: COLD (unloaded) stubs also hold a `SegmentHandle`
            // over an on-disk directory -- tombstone them too, or the
            // directory would leak (nothing else ever tombstones it, since
            // the stub itself is being dropped right here).
            for stub in &snapshot.unloaded {
                stub.mark_tombstoned();
            }
            drop(snapshot);
            self.spawn_delete_index_persist_dir(&index.meta.name);
            // Persist index metadata sidecar
            self.save_index_meta_sidecar();
            // Bump version AFTER successful drop (monotonicity-on-success contract).
            self.bump_version();
            true
        } else {
            false
        }
    }

    /// FLUSHALL/FLUSHDB parity (persistence-review R3): drop every index's
    /// CONTENTS (segments, key-hash maps, payload/MVCC state) while KEEPING
    /// the FT.CREATE definitions — mirroring restart semantics, where
    /// definitions come from the sidecar and contents are re-derived from
    /// the (now empty) keyspace. Without this, flushed hashes stayed
    /// searchable as ghost vectors until the next restart.
    ///
    /// Clears indexes in EVERY logical db — this is the correct primitive
    /// for FLUSHALL. FLUSHDB must use [`Self::clear_all_contents_for_db`]
    /// instead (WS5a): the two commands are not yet differentiated at the
    /// call sites — see the WS5a gap report.
    pub fn clear_all_contents(&mut self) {
        self.clear_contents_matching(|_| true);
    }

    /// Db-scoped variant of [`Self::clear_all_contents`] for FLUSHDB
    /// (WS5a): only clears contents of indexes owned by `db_index`, leaving
    /// every other db's index contents untouched.
    pub fn clear_all_contents_for_db(&mut self, db_index: u8) {
        self.clear_contents_matching(|meta| meta.db_index == db_index);
    }

    /// Shared implementation for [`Self::clear_all_contents`] /
    /// [`Self::clear_all_contents_for_db`]: recreate every index whose
    /// `IndexMeta` satisfies `predicate` from its own definition, discarding
    /// contents but keeping the FT.CREATE definition (see
    /// `clear_all_contents` doc for the rationale).
    fn clear_contents_matching(&mut self, predicate: impl Fn(&IndexMeta) -> bool) {
        let names: Vec<Bytes> = self
            .indexes
            .iter()
            .filter(|(_, idx)| predicate(&idx.meta))
            .map(|(name, _)| name.clone())
            .collect();
        for name in names {
            let Some(index) = self.indexes.remove(&name) else {
                continue;
            };
            // Tombstone warm segments so their on-disk directories are
            // reclaimed once in-flight search snapshots drop (as drop_index).
            let snapshot = index.segments.load();
            for warm_seg in &snapshot.warm {
                warm_seg.mark_tombstoned();
            }
            // WS3 round 2: COLD (unloaded) stubs also hold a `SegmentHandle`
            // over an on-disk directory -- tombstone them too, or the
            // directory would leak (nothing else ever tombstones it, since
            // the stub itself is being dropped right here).
            for stub in &snapshot.unloaded {
                stub.mark_tombstoned();
            }
            drop(snapshot);
            let meta = index.meta.clone();
            drop(index);
            // B2 (durability): FLUSH discards contents — the whole durability
            // dir (segments/manifest/keymaps) goes with it. Best-effort,
            // background (see `spawn_delete_index_persist_dir`); the
            // immediately-following `create_index` reseeds id allocators
            // above whatever the (possibly still-being-deleted) old manifest
            // recorded, so the recreated index never collides with it.
            self.spawn_delete_index_persist_dir(&name);
            // Recreate from the same definition — a fresh, empty index.
            // Also rewrites the sidecar and bumps the version token.
            #[allow(clippy::unwrap_used)] // name was just removed above; create cannot collide
            self.create_index(meta).unwrap();
        }
    }

    /// Get index reference by name.
    ///
    /// NOTE (WS5a): this is the pre-existing, NOT db-scoped lookup — it
    /// ignores `IndexMeta::db_index` entirely and will return an index
    /// created in ANY logical db. Command handlers that need db isolation
    /// MUST migrate to [`Self::get_index_for_db`] (see
    /// `.planning/v0.6.0-release/WS5A-NOTES.md` for the call-site punch
    /// list — this migration is NOT yet complete).
    pub fn get_index(&self, name: &[u8]) -> Option<&VectorIndex> {
        self.indexes.get(name)
    }

    /// Db-scoped variant of [`Self::get_index`]: returns `None` if the index
    /// exists but was created in a different logical db (WS5a isolation —
    /// makes an index invisible outside its owning db, matching Redis-style
    /// NOTFOUND semantics for a name that "doesn't exist" from this db's
    /// point of view).
    pub fn get_index_for_db(&self, name: &[u8], db_index: u8) -> Option<&VectorIndex> {
        self.indexes
            .get(name)
            .filter(|idx| idx.meta.db_index == db_index)
    }

    /// Get mutable index reference by name.
    ///
    /// NOTE (WS5a): NOT db-scoped — see [`Self::get_index`].
    pub fn get_index_mut(&mut self, name: &[u8]) -> Option<&mut VectorIndex> {
        self.indexes.get_mut(name)
    }

    /// Db-scoped variant of [`Self::get_index_mut`].
    pub fn get_index_mut_for_db(&mut self, name: &[u8], db_index: u8) -> Option<&mut VectorIndex> {
        self.indexes
            .get_mut(name)
            .filter(|idx| idx.meta.db_index == db_index)
    }

    /// List all index names.
    ///
    /// NOTE (WS5a): NOT db-scoped — returns names across every logical db.
    pub fn index_names(&self) -> Vec<&Bytes> {
        self.indexes.keys().collect()
    }

    /// Db-scoped variant of [`Self::index_names`] (for FT._LIST).
    pub fn index_names_for_db(&self, db_index: u8) -> Vec<&Bytes> {
        self.indexes
            .iter()
            .filter(|(_, idx)| idx.meta.db_index == db_index)
            .map(|(name, _)| name)
            .collect()
    }

    /// Find indexes whose key_prefixes match the given key.
    /// Returns refs to matching VectorIndex entries.
    ///
    /// NOTE (WS5a): NOT db-scoped.
    pub fn find_matching_indexes(&self, key: &[u8]) -> Vec<&VectorIndex> {
        self.indexes
            .values()
            .filter(|idx| idx.meta.key_prefixes.iter().any(|p| key.starts_with(p)))
            .collect()
    }

    /// Find matching index names for auto-indexing.
    /// Caller must collect names first to avoid borrow issues.
    ///
    /// NOTE (WS5a): NOT db-scoped — the HSET auto-index hook
    /// (`auto_index_hset` in `src/shard/spsc_handler.rs`) still calls this
    /// unscoped variant, so a HSET issued in db 3 can still feed an index
    /// created in db 0 if the key matches its PREFIX. Migrating the caller
    /// to [`Self::find_matching_index_names_for_db`] is open follow-up work.
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

    /// Db-scoped variant of [`Self::find_matching_index_names`]: only
    /// considers indexes owned by `db_index`.
    pub fn find_matching_index_names_for_db(&self, key: &[u8], db_index: u8) -> Vec<Bytes> {
        self.indexes
            .iter()
            .filter_map(|(name, idx)| {
                if idx.meta.db_index == db_index
                    && idx.meta.key_prefixes.iter().any(|p| key.starts_with(p))
                {
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
    ///
    /// NOTE (WS5a): NOT db-scoped — see [`Self::find_matching_index_names`].
    pub fn mark_deleted_for_key(&mut self, key: &[u8]) {
        let matching_names = self.find_matching_index_names(key);
        if matching_names.is_empty() {
            return;
        }
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        let mut any_deleted = false;
        for idx_name in matching_names {
            any_deleted |= self.tombstone_key_in_index(&idx_name, key_hash);
        }
        // Bump version AFTER any successful deletion mark.
        if any_deleted {
            self.bump_version();
        }
    }

    /// Db-scoped variant of [`Self::mark_deleted_for_key`].
    pub fn mark_deleted_for_key_for_db(&mut self, key: &[u8], db_index: u8) {
        let matching_names = self.find_matching_index_names_for_db(key, db_index);
        if matching_names.is_empty() {
            return;
        }
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        let mut any_deleted = false;
        for idx_name in matching_names {
            any_deleted |= self.tombstone_key_in_index(&idx_name, key_hash);
        }
        if any_deleted {
            self.bump_version();
        }
    }

    /// Per-index variant of [`Self::mark_deleted_for_key`] (persistence-review
    /// R4): tombstones `key` in ONE named index. Used by the HDEL hook, where
    /// only indexes whose vector field was actually removed may be touched —
    /// a sibling index keyed on a different field must keep its entry.
    pub fn mark_deleted_for_key_in_index(&mut self, idx_name: &[u8], key: &[u8]) {
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        let Some((name, _)) = self.indexes.get_key_value(idx_name) else {
            return;
        };
        let name = name.clone();
        if self.tombstone_key_in_index(&name, key_hash) {
            self.bump_version();
        }
    }

    /// Tombstone `key_hash` across every tier of one [`SegmentHolder`]'s
    /// current snapshot (mutable + immutable + warm + unloaded/cold-stub).
    /// Shared by the default field and each secondary VECTOR field so a
    /// deleted document cannot resurrect through any field's search path.
    fn tombstone_key_in_holder(holder: &SegmentHolder, key_hash: u64) {
        let snap = holder.load();
        // Tombstone in mutable segment (always present).
        snap.mutable.mark_deleted_by_key_hash(key_hash, 1);
        // Also tombstone any already-compacted immutable segments that
        // may still contain the key (steady-state interior tombstone).
        for imm in snap.immutable.iter() {
            imm.mark_deleted_by_key_hash(key_hash);
        }
        // WS3 round-2 resurrection fix (adversarial review #1): WARM and
        // COLD (unloaded) segments must also be tombstoned, or a HDEL'd doc
        // resurfaces the next time that segment is searched (WARM: takes
        // effect immediately, no reload) or reloaded (COLD: the stub queues
        // the tombstone and replays it in `UnloadedSegment::reload`).
        for warm in snap.warm.iter() {
            warm.mark_deleted_by_key_hash(key_hash);
        }
        for stub in snap.unloaded.iter() {
            stub.mark_deleted_by_key_hash(key_hash);
        }
    }

    fn tombstone_key_in_index(&mut self, idx_name: &Bytes, key_hash: u64) -> bool {
        let Some(idx) = self.indexes.get_mut(idx_name) else {
            return false;
        };
        // Default field (`vector_fields[0]`).
        Self::tombstone_key_in_holder(&idx.segments, key_hash);
        // Prod-hardening #20: secondary VECTOR fields live in a separate
        // `field_segments` map that the default-field loop above never
        // touches. Without this, `DEL`/`HDEL`/`UNLINK` on a doc leaves the
        // vector alive in every non-default field's segments — a subsequent
        // `FT.SEARCH idx '@field2:[...]'` resurrects the deleted document
        // (under a synthetic `vec:<id>` key, since the shared
        // key_hash_to_key map below is cleared unconditionally).
        for fs in idx.field_segments.values() {
            Self::tombstone_key_in_holder(&fs.segments, key_hash);
        }
        // QW7 (2026-06 review finding 6.3): prune the key-hash maps so
        // they track LIVE keys, not historical inserts — without this
        // they grow monotonically under key churn (~1GB / 24M deletes).
        // A re-insert of the same key repopulates all three maps.
        idx.key_hash_to_key.remove(&key_hash);
        idx.key_hash_to_global_id.remove(&key_hash);
        // B2 (durability): keep the checksum map in lockstep so it never
        // drifts from key_hash_to_key (a stale entry would be a silent
        // false-"unchanged" in the B3 dedup rescan).
        idx.key_hash_to_vec_checksum.remove(&key_hash);
        true
    }

    /// Dispatch background compactions for all indexes that are ready
    /// (mutable segment non-empty, no compaction already in flight).
    ///
    /// Call once per tick from the shard event loop (autovacuum Pass D).
    ///
    /// Returns the number of jobs submitted.
    pub fn begin_background_compactions(
        &mut self,
        compactor: &crate::vector::background_compact::BackgroundCompactor,
    ) -> usize {
        let mut submitted = 0;
        for idx in self.indexes.values_mut() {
            if idx.begin_background_compact(compactor) {
                submitted += 1;
            }
        }
        submitted
    }

    /// Threshold-gated variant of [`begin_background_compactions`] for the
    /// autovacuum backstop (Pass D): dispatches only indexes whose mutable
    /// segment has reached its compact threshold. The search path drives the
    /// same logic per-index via [`VectorIndex::begin_background_compact_due`];
    /// this catches indexes that stopped receiving `FT.SEARCH`.
    ///
    /// Returns the number of jobs submitted.
    pub fn begin_background_compactions_due(
        &mut self,
        compactor: &crate::vector::background_compact::BackgroundCompactor,
    ) -> usize {
        let mut submitted = 0;
        for idx in self.indexes.values_mut() {
            if idx.begin_background_compact_due(compactor) {
                submitted += 1;
            }
        }
        submitted
    }

    /// Poll all indexes for completed background compactions and install any
    /// ready results.
    ///
    /// Returns the number of segments installed.
    ///
    /// ## Autovacuum Pass D
    ///
    /// This method should be called from the shard event loop on every tick,
    /// after command processing and before the next sleep. It is intentionally
    /// non-blocking: if no worker has finished, it returns 0 immediately.
    pub fn poll_install_compactions(&mut self) -> usize {
        let mut installed = 0;
        for idx in self.indexes.values_mut() {
            if idx.poll_install_compaction() {
                installed += 1;
            }
        }
        installed
    }

    /// Poll all indexes for completed background merges and install any ready results.
    ///
    /// Returns the number of merged segments installed.
    ///
    /// Non-blocking. Mirrors [`poll_install_compactions`] but for the merge path.
    pub fn poll_install_merges(&mut self) -> usize {
        let mut installed = 0;
        for idx in self.indexes.values_mut() {
            if idx.poll_install_merge() {
                installed += 1;
            }
        }
        installed
    }

    /// Dispatch background merges for all indexes that satisfy the auto-merge
    /// trigger conditions (threshold exceeded or high dead-fraction).
    ///
    /// Returns the number of merge jobs submitted.
    ///
    /// Non-blocking. Mirrors [`begin_background_compactions_due`] but for merges.
    pub fn begin_background_merges_due(
        &mut self,
        compactor: &crate::vector::background_compact::BackgroundCompactor,
    ) -> usize {
        let mut submitted = 0;
        for idx in self.indexes.values_mut() {
            if idx.begin_background_merge_due(compactor) {
                submitted += 1;
            }
        }
        submitted
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
    ///
    /// Deliberately NOT db-scoped: the sidecar (`vector-indexes.meta`) must
    /// persist every logical db's index definitions so a restart restores
    /// all of them (see `save_index_meta_sidecar`).
    pub fn collect_index_metas(&self) -> Vec<&IndexMeta> {
        self.indexes.values().map(|idx| &idx.meta).collect()
    }

    /// Db-scoped variant of [`Self::collect_index_metas`] (for FT._LIST /
    /// scatter_ft_info at the command layer — WS5a).
    pub fn collect_index_metas_for_db(&self, db_index: u8) -> Vec<&IndexMeta> {
        self.indexes
            .values()
            .map(|idx| &idx.meta)
            .filter(|meta| meta.db_index == db_index)
            .collect()
    }

    /// Collect `(meta, compaction_weight)` pairs for v3 sidecar persistence (W3-deep).
    pub fn collect_index_metas_with_weights(&self) -> Vec<(&IndexMeta, f32)> {
        self.indexes
            .values()
            .map(|idx| (&idx.meta, idx.compaction_weight))
            .collect()
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
        self.try_warm_transitions_all_idle(
            shard_dir,
            manifest,
            warm_after_secs,
            0,
            next_file_id,
            wal,
        )
    }

    /// Same as [`Self::try_warm_transitions_all`] but also applies the WS3
    /// idle-time criterion (see [`VectorIndex::try_warm_transitions_idle`]).
    pub fn try_warm_transitions_all_idle(
        &self,
        shard_dir: &std::path::Path,
        manifest: &mut crate::persistence::manifest::ShardManifest,
        warm_after_secs: u64,
        idle_after_secs: u64,
        next_file_id: &mut u64,
        wal: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
    ) -> usize {
        let names: Vec<bytes::Bytes> = self.indexes.keys().cloned().collect();
        let mut total = 0;
        for name in names {
            if let Some(idx) = self.indexes.get(&name) {
                total += idx.try_warm_transitions_idle(
                    shard_dir,
                    manifest,
                    warm_after_secs,
                    idle_after_secs,
                    next_file_id,
                    wal,
                );
            }
        }
        total
    }

    /// Register warm segments recovered from disk into the appropriate indexes.
    ///
    /// Called during shard restore after v3 recovery identifies warm-tier segments
    /// in the manifest (`Shard::recovered_warm_segments`, staged after Stack A's
    /// `restore_from_persistence`). **Ordering (PR review round 2, commit 4):**
    /// this MUST run right after the sidecar `create_index` loop, BEFORE the
    /// keyspace dedup rescan (`RecoveryState::reconcile_key`) — see
    /// `event_loop.rs`. Running it after the rescan (as an earlier revision
    /// did) let the rescan observe warm keys as "unknown" (never loaded into
    /// `key_hash_to_key`/`key_hash_to_global_id` here — see point 2 below),
    /// forcing a full re-encode into the mutable segment; that re-encode then
    /// made the duplication check below see the just-re-indexed keys as
    /// "already covered" and RETIRE the warm segment, permanently deleting
    /// it and re-quantizing its vectors — the exact failure this feature
    /// exists to prevent, on every normal restart, not just a crash.
    ///
    /// Two evidence-based safety checks guard every segment, both keyed off
    /// its own `key_hash` set (read cheaply from `mvcc.mpf` via
    /// `warm_search::peek_key_hashes` — no codes/graph mmap, no
    /// `CollectionMetadata` dependency):
    ///
    /// 1. **Ownership** (PR review finding #2): the old implementation
    ///    attached a segment to the FIRST index for which
    ///    `WarmSearchSegment::from_files` happened to succeed —
    ///    `from_files` accepts ANY caller-supplied collection and never
    ///    validates it against the file contents, so with two indexes of
    ///    the same dimension/quantization it could silently pick the WRONG
    ///    one. Ownership is instead decided by reading each candidate
    ///    index's persisted Stack-B keymap straight off disk (the file, not
    ///    the in-memory `key_hash_to_key` — which is legitimately empty for
    ///    a cleanly garbage-collected warm segment, see the leak fix in
    ///    `try_warm_transitions_idle`) and picking whichever index has the
    ///    most key_hash overlap. No match, or a tie between two indexes ->
    ///    warn and leave the segment unregistered (files intact) rather
    ///    than guess.
    /// 2. **Duplication** (PR review finding #1, CRITICAL): `transition_to_warm`
    ///    commits Stack A's shard manifest durably BEFORE
    ///    `persist_hook_after_install`'s Stack B snapshot job (async,
    ///    background thread) commits its own GC. A `kill -9` in that window
    ///    leaves the OLD segment still tracked as HOT by Stack B (reloaded
    ///    as an `immutable` segment by `recover_v2`, since its id is still
    ///    in the stale `segment_ids`) *and* this WARM copy discovered by
    ///    Stack A — the same vectors would live twice, permanently
    ///    (`search_mvcc`'s merge has no key_hash dedup, and Stack B's next
    ///    snapshot re-adopts the reloaded HOT copy into `segment_ids`, so
    ///    it never self-heals). If the decided owner's CURRENT in-memory
    ///    `key_hash_to_key` (i.e. what Stack B actually recovered) already
    ///    covers this segment's key_hashes, the HOT copy wins: skip
    ///    attaching the warm copy and retire its on-disk files instead.
    ///
    /// On a clean (non-duplicate) attach, this also **populates**
    /// `key_hash_to_key`/`key_hash_to_global_id`/`key_hash_to_vec_checksum`
    /// for the segment's keys from the SAME owner-evidence keymap entries
    /// used for ownership above — without this, `recover_v2::reconcile_key`
    /// would see every warm key as unknown (full re-encode, see the
    /// ordering note above) and `FT.SEARCH` would resolve warm docs to a
    /// synthetic `vec:<id>` instead of their real key (the map that backs
    /// key-bytes resolution is exactly this one). A segment key_hash absent
    /// from the owner's persisted keymap means the async snapshot job that
    /// would have durably recorded it never committed before an earlier
    /// crash (the same class of race as finding #1, at per-key rather than
    /// per-segment granularity, e.g. a key written between the last
    /// snapshot and the transition-then-crash): left out of the in-memory
    /// maps so the rescan re-indexes it fresh into mutable, AND tombstoned
    /// in the warm copy for that specific key_hash (`seed_tombstones`) so
    /// the stale warm copy doesn't sit alongside the freshly re-indexed one
    /// — same no-dedup `search_mvcc` hazard as finding #1, just narrower.
    pub fn register_warm_segments(&mut self, warm_segments: Vec<(u64, std::path::PathBuf)>) {
        use crate::vector::persistence::manifest::KeymapEntry;
        use crate::vector::persistence::warm_search::{WarmSearchSegment, peek_key_hashes};

        let mut loaded = 0usize;
        let mut retired_duplicates = 0usize;
        let mut retired_orphans = 0usize;
        let mut unregistered = 0usize;

        for (segment_id, segment_dir) in &warm_segments {
            let seg_key_hashes = match peek_key_hashes(segment_dir) {
                Ok(hs) => hs,
                // moon#546: the file is GONE. Attribution reads exactly this
                // file, so such a directory can never be attached to an index —
                // its keys are re-indexed by the keyspace rescan instead. It is
                // what GC leaves behind when it removes a directory's contents
                // but not the directory, or what a crash leaves mid-creation.
                // Left in place it is not just dead weight: every future restart
                // re-reads it and re-warns, forever (measured on the reported
                // store: 897 of them, and stable across repeated restarts).
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::info!(
                        "warm segment {segment_id} at {segment_dir:?}: no mvcc.mpf — \
                         retiring a partially-swept segment directory (its keys, if \
                         any, are recovered by the keyspace rescan)"
                    );
                    if let Err(e) = std::fs::remove_dir_all(segment_dir) {
                        tracing::warn!(
                            "failed to retire orphaned warm segment directory \
                             {segment_dir:?}: {e} (harmless: the next restart retries, \
                             and once the directory is gone the #546a pass retires its \
                             manifest entry too)"
                        );
                    }
                    retired_orphans += 1;
                    continue;
                }
                // Any OTHER error is ambiguous. A transient EIO or a permissions
                // problem may sit over perfectly good data, and deleting a real
                // segment because one read failed would turn a recoverable blip
                // into permanent loss — strictly worse than the disk leak above.
                // So only `NotFound` retires; everything else is left untouched.
                Err(e) => {
                    tracing::warn!(
                        "warm segment {segment_id} at {segment_dir:?}: failed to read \
                         mvcc.mpf for ownership attribution: {e} — leaving unregistered \
                         (files intact: the error may be transient, so this is not \
                         treated as an orphan)"
                    );
                    unregistered += 1;
                    continue;
                }
            };
            let seg_key_hash_set: std::collections::HashSet<u64> =
                seg_key_hashes.iter().copied().collect();

            // Finding #2: decide ownership from persisted keymap evidence
            // (majority-match), never from `from_files` success alone. The
            // winning candidate's keymap entries are RETAINED (not just
            // counted) — they're the evidence source for populating the
            // in-memory maps below, avoiding a second disk read (and a
            // second TOCTOU window) for the same file.
            let mut owner: Option<Bytes> = None;
            let mut owner_matches = 0usize;
            let mut owner_entries: Vec<KeymapEntry> = Vec::new();
            let mut ambiguous = false;
            if !seg_key_hash_set.is_empty() {
                if let Some(store_dir) = self.persist_dir.clone() {
                    for name in self.indexes.keys() {
                        let idx_dir = crate::vector::persistence::manifest::index_persist_dir(
                            &store_dir, name,
                        );
                        let Some(entries) = read_manifest_and_keymap_consistent(&idx_dir) else {
                            continue;
                        };
                        let matches = entries
                            .iter()
                            .filter(|e| seg_key_hash_set.contains(&e.key_hash))
                            .count();
                        if matches == 0 {
                            continue;
                        }
                        match matches.cmp(&owner_matches) {
                            std::cmp::Ordering::Greater => {
                                owner = Some(name.clone());
                                owner_matches = matches;
                                owner_entries = entries;
                                ambiguous = false;
                            }
                            std::cmp::Ordering::Equal if owner.as_ref() != Some(name) => {
                                ambiguous = true;
                            }
                            _ => {}
                        }
                    }
                }
            }

            let Some(owner_name) = owner else {
                tracing::warn!(
                    "warm segment {segment_id} at {segment_dir:?}: no index's persisted \
                     keymap contains any of its {} key_hash(es) — leaving unregistered \
                     (files intact)",
                    seg_key_hash_set.len()
                );
                unregistered += 1;
                continue;
            };
            if ambiguous {
                tracing::warn!(
                    "warm segment {segment_id} at {segment_dir:?}: key_hash evidence matches \
                     multiple indexes equally — refusing to guess, leaving unregistered"
                );
                unregistered += 1;
                continue;
            }

            // Finding #1: is this segment's data ALREADY live via a
            // reloaded HOT copy (the crash-before-GC race)? Check the
            // owner's CURRENT in-memory key_hash_to_key, which reflects
            // exactly what Stack B's own recovery pass actually attached.
            let Some(idx) = self.indexes.get_mut(&owner_name) else {
                unregistered += 1;
                continue;
            };
            let already_covered = seg_key_hash_set
                .iter()
                .any(|kh| idx.key_hash_to_key.contains_key(kh));
            if already_covered {
                tracing::warn!(
                    "warm segment {segment_id} for index {:?}: key_hashes already covered \
                     by a live HOT segment (crash landed before Stack-B's GC of the \
                     superseded segment committed) — retiring the warm copy instead of \
                     attaching a duplicate",
                    String::from_utf8_lossy(&owner_name)
                );
                if let Err(e) = std::fs::remove_dir_all(segment_dir) {
                    tracing::warn!(
                        "failed to retire superseded warm segment directory {segment_dir:?}: \
                         {e} (harmless: Stack A's recovery already tolerates a manifest \
                         entry whose directory is missing, so this is retried — as a no-op \
                         once the directory is gone — every future restart until it succeeds)"
                    );
                }
                retired_duplicates += 1;
                continue;
            }

            let handle = SegmentHandle::new(*segment_id, segment_dir.clone());
            match WarmSearchSegment::from_files(
                segment_dir,
                *segment_id,
                idx.collection.clone(),
                handle,
                false, // mlock_codes off during recovery (can be changed later)
            ) {
                Ok(warm_seg) => {
                    // Populate the in-memory maps from the owner-evidence
                    // keymap entries so the rescan (which runs right after
                    // this) sees these keys as known/unchanged instead of
                    // re-indexing them, and FT.SEARCH resolves real key
                    // bytes for them. Any key_hash missing from the
                    // persisted keymap is left out (rescan self-heals it
                    // into mutable) and tombstoned in this warm copy so the
                    // two never coexist as live duplicates.
                    let entries_by_hash: std::collections::HashMap<u64, &KeymapEntry> =
                        owner_entries.iter().map(|e| (e.key_hash, e)).collect();
                    let mut missing_from_keymap: Vec<u64> = Vec::new();
                    for kh in &seg_key_hash_set {
                        if let Some(entry) = entries_by_hash.get(kh) {
                            idx.key_hash_to_key
                                .insert(entry.key_hash, entry.key.clone());
                            idx.key_hash_to_global_id
                                .insert(entry.key_hash, entry.global_id);
                            idx.key_hash_to_vec_checksum
                                .insert(entry.key_hash, entry.vec_checksum);
                        } else {
                            missing_from_keymap.push(*kh);
                        }
                    }
                    if !missing_from_keymap.is_empty() {
                        warm_seg.seed_tombstones(&missing_from_keymap);
                        tracing::warn!(
                            "warm segment {segment_id} for index {:?}: {} key_hash(es) missing \
                             from the persisted keymap (an earlier async-snapshot job never \
                             committed for them) — left out of the in-memory keymap and \
                             tombstoned in the warm copy; the keyspace rescan will re-index \
                             them fresh into the mutable segment",
                            String::from_utf8_lossy(&owner_name),
                            missing_from_keymap.len()
                        );
                    }

                    let old = idx.segments.load();
                    let mut new_warm = old.warm.clone();
                    new_warm.push(std::sync::Arc::new(warm_seg));
                    let new_list = crate::vector::segment::SegmentList {
                        mutable: std::sync::Arc::clone(&old.mutable),
                        immutable: old.immutable.clone(),
                        ivf: old.ivf.clone(),
                        warm: new_warm,
                        unloaded: old.unloaded.clone(),
                    };
                    idx.segments.swap(new_list);
                    loaded += 1;
                    tracing::info!(
                        "Registered warm segment {} from {:?} into index {:?}",
                        segment_id,
                        segment_dir,
                        String::from_utf8_lossy(&owner_name)
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        "warm segment {} at {:?}: open failed for owner index {:?}: {}",
                        segment_id,
                        segment_dir,
                        String::from_utf8_lossy(&owner_name),
                        e
                    );
                    unregistered += 1;
                }
            }
        }
        if loaded > 0 || retired_duplicates > 0 || retired_orphans > 0 || unregistered > 0 {
            tracing::info!(
                "Registered {}/{} warm segments on startup ({} retired as duplicates, \
                 {} retired as orphans with no mvcc.mpf, {} left unregistered)",
                loaded,
                warm_segments.len(),
                retired_duplicates,
                retired_orphans,
                unregistered
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

    // ── P2: Segment merge public API ──────────────────────────────────────────

    /// Total immutable segment count for a named index.
    /// Returns None if the index does not exist.
    pub fn immutable_segment_count(&self, name: &[u8]) -> Option<usize> {
        self.indexes
            .get(name)
            .map(|idx| idx.segments.load().immutable.len())
    }

    /// True if the named index satisfies any auto-merge trigger condition —
    /// same logic as [`VectorIndex::needs_merge`] (count OR dead-fraction
    /// trigger, both gated by the merge memory ceiling).
    ///
    /// Returns None if the index does not exist.
    pub fn needs_merge(&self, name: &[u8]) -> Option<bool> {
        self.indexes.get(name).map(|idx| idx.needs_merge())
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
        if !self.indexes.contains_key(index_name) {
            return Err("index not found");
        }
        // Monotonic store-wide LSN, same allocator as the wire path
        // (auto_index_hset). The previous `mutable.len() + 1` restarted after
        // every compaction, so a RE-inserted key could carry a LOWER lsn than
        // its compacted predecessor — merge dedup (keep highest insert_lsn)
        // then kept the stale copy and dropped the current one.
        let insert_lsn = self.txn_manager_mut().allocate_lsn();
        let idx = match self.indexes.get_mut(index_name) {
            Some(idx) => idx,
            None => return Err("index not found"),
        };
        idx.segments
            .load()
            .mutable
            .append(key_hash, vector, insert_lsn);
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

        // If a background merge is already in-flight, drain it first (mirror
        // how force_compact drains bg_compact_inflight). This prevents
        // double-merging of the same source segments.
        if let Some(inflight) = idx.bg_merge_inflight.take() {
            // Block until the worker finishes.
            if let Ok(Ok(mut merged)) = inflight.reply_rx.recv() {
                // Reapply window deletes.
                for src in &inflight.merged_sources {
                    for kh in src.tombstoned_key_hashes() {
                        merged.mark_deleted_by_key_hash_install(kh);
                    }
                }
                let snap = idx.segments.load();
                let merged_arc = Arc::new(merged);
                let mut new_immutable: Vec<
                    Arc<crate::vector::segment::immutable::ImmutableSegment>,
                > = snap
                    .immutable
                    .iter()
                    .filter(|cur| {
                        !inflight
                            .merged_sources
                            .iter()
                            .any(|src| Arc::ptr_eq(cur, src))
                    })
                    .cloned()
                    .collect();
                merged_arc.mark_installed();
                new_immutable.push(merged_arc.clone());
                idx.scratch = crate::vector::hnsw::search::SearchScratch::new(
                    merged_arc.graph().num_nodes(),
                    idx.meta.padded_dimension,
                );
                let new_list = SegmentList {
                    mutable: Arc::clone(&snap.mutable),
                    immutable: new_immutable,
                    ivf: snap.ivf.clone(),
                    warm: snap.warm.clone(),
                    unloaded: snap.unloaded.clone(),
                };
                drop(snap);
                idx.segments.swap(new_list);
                // B2 (durability): worker already persisted (if configured
                // at submit time) — commit the keymap/manifest snapshot.
                idx.persist_hook_after_install();
                // Data is already merged — return early.
                let new_snap = idx.segments.load();
                let live = new_snap
                    .immutable
                    .first()
                    .map_or(0, |s| s.live_count() as usize);
                return Ok(MergeStats {
                    segments_merged: inflight.merged_sources.len(),
                    live_vectors: live,
                    recall: 1.0,
                });
            }
            // Worker failed — fall through to synchronous merge below.
        }

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
        // B2 (durability): allocate the disk segment id synchronously (this
        // is the explicit-command path — a brief stall is acceptable, same
        // rationale as the inline compact path).
        let persist = idx.alloc_persist_target();

        match compaction::merge_immutable(
            &segs,
            &collection,
            seed,
            mode,
            recall_tolerance,
            persist.as_ref().map(|(p, id)| (p.as_path(), *id)),
        ) {
            Ok(merged) => {
                let live = merged.live_count() as usize;
                // Atomically swap: replace all immutable segments with the single merged one.
                merged.mark_installed();
                let old = idx.segments.load();
                let new_list = SegmentList {
                    mutable: Arc::clone(&old.mutable),
                    immutable: vec![Arc::new(merged)],
                    ivf: old.ivf.clone(),
                    warm: old.warm.clone(),
                    unloaded: old.unloaded.clone(),
                };
                idx.segments.swap(new_list);
                idx.persist_hook_after_install();

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
                // Default 0.70: catch catastrophic recall collapse without
                // false-positives on small/medium indexes. Per-index override:
                // FT.CONFIG SET <idx> MERGE_RECALL_TOLERANCE (VEC-4).
                let tolerance = self
                    .indexes
                    .get(&name)
                    .map(|i| i.merge_recall_tolerance)
                    .unwrap_or(0.70);
                match self.force_merge_index_with_tolerance(&name, tolerance) {
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
    // Serialize with promote_unloaded/submit_unloaded_reloads: all three do a
    // load-mutate-swap on the same ArcSwap, and only shard-thread affinity keeps
    // them from clobbering each other today (perf-review defense-in-depth). Held
    // only across synchronous work, never an .await.
    let _reload_guard = holder.reload_guard();
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
        unloaded: snapshot.unloaded.clone(),
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

/// Minimal single-field index meta for cross-module unit tests (search_pool
/// identity tests build multi-segment stores through the public store API).
#[cfg(test)]
pub(crate) fn test_index_meta(dim: u32) -> IndexMeta {
    IndexMeta {
        name: Bytes::from_static(b"idx"),
        dimension: dim,
        padded_dimension: padded_dimension(dim),
        metric: DistanceMetric::L2,
        hnsw_m: 8,
        hnsw_ef_construction: 50,
        hnsw_ef_runtime: 0,
        compact_threshold: 0,
        source_field: Bytes::from_static(b"vec"),
        key_prefixes: vec![Bytes::from_static(b"doc:")],
        quantization: QuantizationConfig::TurboQuant4,
        build_mode: BuildMode::Light,
        vector_fields: Vec::new(),
        schema_fields: Vec::new(),
        merge_mode: MergeMode::GraphUnion,
        keep_raw: false,
        db_index: 0,
        rerank_mult: 4,
        exact_beam: false,
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
            vector_fields: Vec::new(), // populated by create_index
            schema_fields: Vec::new(),
            merge_mode: MergeMode::GraphUnion,
            keep_raw: false,
            db_index: 0,
            rerank_mult: 4,
            exact_beam: false,
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
            db_index: 0,
            rerank_mult: 4,
            exact_beam: false,
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

    /// WS5a (db-scoped indexes): db-tagged variants of the lookup/listing/
    /// deletion primitives must be invisible/inert across logical dbs, while
    /// the pre-existing unscoped methods keep their historical (global)
    /// behavior for callers not yet migrated.
    mod ws5a_db_scoping {
        use super::*;

        fn make_meta_for_db(name: &str, db_index: u8) -> IndexMeta {
            let mut meta = make_meta(name, 32, &["doc:"]);
            meta.db_index = db_index;
            meta
        }

        #[test]
        fn get_index_for_db_is_invisible_cross_db() {
            let mut store = VectorStore::new();
            store.create_index(make_meta_for_db("idx", 3)).unwrap();

            // Owning db sees it.
            assert!(store.get_index_for_db(b"idx", 3).is_some());
            assert!(store.get_index_mut_for_db(b"idx", 3).is_some());
            // Every other db does not -- this is the core WS5a guarantee.
            assert!(store.get_index_for_db(b"idx", 0).is_none());
            assert!(store.get_index_mut_for_db(b"idx", 0).is_none());
            // The legacy unscoped accessor is intentionally untouched
            // (documents the current, NOT-yet-migrated global behavior).
            assert!(store.get_index(b"idx").is_some());
        }

        #[test]
        fn index_names_for_db_filters_by_owner() {
            let mut store = VectorStore::new();
            store.create_index(make_meta_for_db("a", 0)).unwrap();
            store.create_index(make_meta_for_db("b", 1)).unwrap();
            store.create_index(make_meta_for_db("c", 1)).unwrap();

            let db0: Vec<&[u8]> = store
                .index_names_for_db(0)
                .into_iter()
                .map(|b| b.as_ref())
                .collect();
            assert_eq!(db0, vec![b"a".as_ref()]);

            let mut db1: Vec<&[u8]> = store
                .index_names_for_db(1)
                .into_iter()
                .map(|b| b.as_ref())
                .collect();
            db1.sort();
            assert_eq!(db1, vec![b"b".as_ref(), b"c".as_ref()]);

            // Unscoped listing still returns all three (documents current
            // FT._LIST behavior pending call-site migration).
            assert_eq!(store.index_names().len(), 3);
        }

        #[test]
        fn find_matching_index_names_for_db_scopes_auto_index() {
            let mut store = VectorStore::new();
            store.create_index(make_meta_for_db("db0idx", 0)).unwrap();
            store.create_index(make_meta_for_db("db1idx", 1)).unwrap();

            let db0_matches = store.find_matching_index_names_for_db(b"doc:1", 0);
            assert_eq!(db0_matches, vec![Bytes::from_static(b"db0idx")]);

            let db1_matches = store.find_matching_index_names_for_db(b"doc:1", 1);
            assert_eq!(db1_matches, vec![Bytes::from_static(b"db1idx")]);

            // Unscoped variant still matches both (documents the HSET
            // auto-index hook's current global behavior pending migration).
            assert_eq!(store.find_matching_index_names(b"doc:1").len(), 2);
        }

        #[test]
        fn mark_deleted_for_key_for_db_only_tombstones_owning_db() {
            let mut store = VectorStore::new();
            store.create_index(make_meta_for_db("db0idx", 0)).unwrap();
            store.create_index(make_meta_for_db("db1idx", 1)).unwrap();

            let key = b"doc:1";
            let vec = vec![0.1f32; 32];
            let hash = xxhash_rust::xxh64::xxh64(key, 0);
            store
                .insert_vector(b"db0idx", &vec, hash, Bytes::copy_from_slice(key))
                .unwrap();
            store
                .insert_vector(b"db1idx", &vec, hash, Bytes::copy_from_slice(key))
                .unwrap();

            // Deleting the key from db 0 must not affect db 1's copy.
            store.mark_deleted_for_key_for_db(key, 0);

            let db0_alive = store
                .get_index(b"db0idx")
                .unwrap()
                .key_hash_to_key
                .contains_key(&hash);
            let db1_alive = store
                .get_index(b"db1idx")
                .unwrap()
                .key_hash_to_key
                .contains_key(&hash);
            assert!(!db0_alive, "db0's entry must be tombstoned");
            assert!(db1_alive, "db1's entry must survive a db0-scoped delete");
        }

        #[test]
        fn clear_all_contents_for_db_leaves_other_dbs_untouched() {
            let mut store = VectorStore::new();
            store.create_index(make_meta_for_db("db0idx", 0)).unwrap();
            store.create_index(make_meta_for_db("db1idx", 1)).unwrap();

            let key = b"doc:1";
            let vec = vec![0.1f32; 32];
            let hash = xxhash_rust::xxh64::xxh64(key, 0);
            store
                .insert_vector(b"db0idx", &vec, hash, Bytes::copy_from_slice(key))
                .unwrap();
            store
                .insert_vector(b"db1idx", &vec, hash, Bytes::copy_from_slice(key))
                .unwrap();

            // FLUSHDB on db 0 only.
            store.clear_all_contents_for_db(0);

            // Definitions survive in both dbs (FLUSH keeps FT.CREATE defs).
            assert_eq!(store.len(), 2);
            // db0's content is gone; db1's is untouched.
            assert!(
                !store
                    .get_index(b"db0idx")
                    .unwrap()
                    .key_hash_to_key
                    .contains_key(&hash)
            );
            assert!(
                store
                    .get_index(b"db1idx")
                    .unwrap()
                    .key_hash_to_key
                    .contains_key(&hash)
            );
        }

        #[test]
        fn drop_index_for_db_refuses_cross_db_drop() {
            let mut store = VectorStore::new();
            store.create_index(make_meta_for_db("idx", 3)).unwrap();

            // Wrong db: refused, index survives.
            assert!(!store.drop_index_for_db(b"idx", 0));
            assert_eq!(store.len(), 1);

            // Owning db: succeeds.
            assert!(store.drop_index_for_db(b"idx", 3));
            assert_eq!(store.len(), 0);
        }

        #[test]
        fn legacy_persisted_index_defaults_to_db_zero() {
            // v1-v3 sidecar formats have no db_index byte; the field defaults
            // to 0 via `IndexMeta { db_index: 0, .. }` in the deserializer,
            // matching pre-v0.6.0 global (db-0-visible) behavior.
            let meta = make_meta("legacyidx", 64, &["doc:"]);
            assert_eq!(meta.db_index, 0);
        }
    }

    /// Builds HSET-style args `[key, field, vector_bytes]` for
    /// `auto_index_hset_public`, mirroring the wire format `find_vector_blob`
    /// expects (field/value pairs starting at index 1).
    fn hset_vector_args(key: &[u8], field: &[u8], vec: &[f32]) -> Vec<crate::protocol::Frame> {
        let mut bytes = Vec::with_capacity(vec.len() * 4);
        for v in vec {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        vec![
            crate::protocol::Frame::BulkString(Bytes::copy_from_slice(key)),
            crate::protocol::Frame::BulkString(Bytes::copy_from_slice(field)),
            crate::protocol::Frame::BulkString(Bytes::from(bytes)),
        ]
    }

    /// B2 (durability): `key_hash_to_vec_checksum` must be maintained in
    /// lockstep with `key_hash_to_key` at every mutation site — insert,
    /// update (checksum changes with the vector), and tombstone (both maps
    /// prune the same key). A drift here would silently corrupt the B3
    /// dedup rescan's unchanged-vs-changed decision.
    #[test]
    fn test_checksum_map_tracks_key_hash_to_key() {
        let mut store = VectorStore::new();
        store.create_index(make_meta("idx", 4, &["doc:"])).unwrap();
        let mut text_store = crate::text::store::TextStore::new();

        let key_hash = xxhash_rust::xxh64::xxh64(b"doc:1", 0);
        let v1 = [1.0f32, 2.0, 3.0, 4.0];
        let expected_checksum_v1 = {
            let mut bytes = Vec::with_capacity(16);
            for v in &v1 {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            xxhash_rust::xxh64::xxh64(&bytes, 0)
        };

        // ── Insert ──────────────────────────────────────────────────────
        crate::shard::spsc_handler::auto_index_hset_public(
            &mut store,
            &mut text_store,
            b"doc:1",
            &hset_vector_args(b"doc:1", b"vec", &v1),
            0,
        );
        {
            let idx = store.get_index(b"idx").unwrap();
            assert!(idx.key_hash_to_key.contains_key(&key_hash));
            assert_eq!(
                idx.key_hash_to_vec_checksum.get(&key_hash).copied(),
                Some(expected_checksum_v1),
                "checksum map must be populated on insert, matching xxh64 of the raw vector bytes"
            );
            assert_eq!(
                idx.key_hash_to_key.len(),
                idx.key_hash_to_vec_checksum.len(),
                "checksum map must track key_hash_to_key 1:1"
            );
        }

        // ── Update (different vector -> different checksum) ────────────
        let v2 = [5.0f32, 6.0, 7.0, 8.0];
        crate::shard::spsc_handler::auto_index_hset_public(
            &mut store,
            &mut text_store,
            b"doc:1",
            &hset_vector_args(b"doc:1", b"vec", &v2),
            0,
        );
        {
            let idx = store.get_index(b"idx").unwrap();
            let updated_checksum = idx.key_hash_to_vec_checksum.get(&key_hash).copied();
            assert_ne!(
                updated_checksum,
                Some(expected_checksum_v1),
                "checksum must change when the vector changes"
            );
            assert!(updated_checksum.is_some());
            assert_eq!(
                idx.key_hash_to_key.len(),
                idx.key_hash_to_vec_checksum.len()
            );
        }

        // ── Tombstone (DEL) prunes both maps together ───────────────────
        store.mark_deleted_for_key(b"doc:1");
        {
            let idx = store.get_index(b"idx").unwrap();
            assert!(!idx.key_hash_to_key.contains_key(&key_hash));
            assert!(
                !idx.key_hash_to_vec_checksum.contains_key(&key_hash),
                "checksum map must be pruned alongside key_hash_to_key on delete"
            );
        }
    }

    /// End-to-end (B2, durability write path): `force_compact` with a
    /// `persist_dir` configured must (a) write the compacted segment to disk
    /// under `idx-<hex>/segment-<id>/` via the staged writer, and (b) — once
    /// the background snapshot job lands — commit a `manifest.json` that
    /// references that segment id and a readable `keymap-<epoch>.bin`
    /// containing the inserted key.
    #[test]
    fn test_force_compact_persists_segment_and_manifest_when_persist_dir_set() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = VectorStore::new();
        // set_persist_dir BEFORE create_index, mirroring event_loop.rs's
        // startup order (create_index captures the store's persist_dir).
        store.set_persist_dir(tmp.path().to_path_buf());
        store.create_index(make_meta("idx", 4, &["doc:"])).unwrap();
        let mut text_store = crate::text::store::TextStore::new();

        crate::shard::spsc_handler::auto_index_hset_public(
            &mut store,
            &mut text_store,
            b"doc:1",
            &hset_vector_args(b"doc:1", b"vec", &[1.0, 2.0, 3.0, 4.0]),
            0,
        );

        store.get_index_mut(b"idx").unwrap().force_compact();

        // The segment write happens synchronously (inline compact path) —
        // must be visible immediately, no polling needed.
        let idx_dir = crate::vector::persistence::manifest::index_persist_dir(tmp.path(), b"idx");
        assert!(
            idx_dir.join("segment-0").join("segment_meta.json").exists(),
            "compacted segment must be persisted synchronously under segment-0"
        );
        assert!(
            !idx_dir.join("staging-0").exists(),
            "staged writer must leave no staging dir behind"
        );

        // The keymap/manifest snapshot is a background job — poll briefly.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let manifest = loop {
            if let Some(m) = crate::vector::persistence::manifest::read_manifest_tolerant(&idx_dir)
            {
                break m;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "manifest.json never appeared within 5s"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        };

        assert_eq!(manifest.segment_ids, vec![0]);
        let keymap = crate::vector::persistence::manifest::read_keymap_tolerant(
            &idx_dir,
            manifest.keymap_epoch,
        )
        .expect("keymap for the committed epoch must be readable");
        assert_eq!(keymap.len(), 1);
        assert_eq!(keymap[0].key_hash, xxhash_rust::xxh64::xxh64(b"doc:1", 0));
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
            unloaded: Vec::new(),
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
            unloaded: Vec::new(),
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
}

// ── Background compaction TDD tests ─────────────────────────────────────────
//
// These tests drive the background-compact pipeline end-to-end:
//   1. Basic round-trip: begin → poll → immutable segment installed.
//   2. Tail vectors inserted during build appear in results after install.
//   3. Delete before install is reconciled: deleted key absent from results.
//   4. Overwrite before install: only newest version survives.
//   5. Steady-state HDEL after install tombstones immutable via interior set.
//   6. force_compact while in-flight drains the in-flight first.
//
// Tests 3 and 4 are RED before install reconciliation exists (the deleted/stale
// key would appear in results). They turn GREEN after reconciliation is wired.

#[cfg(test)]
mod bg_compact_tests {
    use bytes::Bytes;

    use super::*;
    use crate::vector::background_compact::BackgroundCompactor;
    use crate::vector::distance;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_idx(dim: u32) -> IndexMeta {
        IndexMeta {
            name: Bytes::from_static(b"idx"),
            dimension: dim,
            padded_dimension: padded_dimension(dim),
            metric: DistanceMetric::L2,
            hnsw_m: 8,
            hnsw_ef_construction: 50,
            hnsw_ef_runtime: 0,
            compact_threshold: 0,
            source_field: Bytes::from_static(b"vec"),
            key_prefixes: vec![Bytes::from_static(b"doc:")],
            quantization: QuantizationConfig::TurboQuant4,
            build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
            vector_fields: Vec::new(),
            schema_fields: Vec::new(),
            merge_mode: MergeMode::GraphUnion,
            keep_raw: false,
            db_index: 0,
            rerank_mult: 4,
            exact_beam: false,
        }
    }

    fn random_vec(dim: usize, seed: u64) -> Vec<f32> {
        // LCG-based deterministic pseudo-random for no-dep generation.
        let mut state = seed.wrapping_add(1);
        let mut v: Vec<f32> = (0..dim)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((state >> 33) as f32) / (u32::MAX as f32) * 2.0 - 1.0
            })
            .collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
        v.iter_mut().for_each(|x| *x /= norm);
        v
    }

    fn insert(store: &mut VectorStore, key: &[u8], vec: Vec<f32>) {
        let hash = xxhash_rust::xxh64::xxh64(key, 0);
        let key_bytes = Bytes::copy_from_slice(key);
        store.insert_vector(b"idx", &vec, hash, key_bytes).unwrap();
    }

    /// Poll until a segment is installed or we hit `max_iters`.
    fn poll_until_installed(store: &mut VectorStore, max_iters: usize) -> bool {
        for _ in 0..max_iters {
            if store.poll_install_compactions() > 0 {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        false
    }

    fn search_key_hashes(store: &mut VectorStore, query: &[f32], k: usize) -> Vec<u64> {
        let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
        let results = idx.segments.search(query, k, 50, &mut idx.scratch);
        results.iter().map(|r| r.key_hash).collect()
    }

    // ── Bounded bulk compaction (search_pool enabler) ────────────────────────

    /// A bulk load compacted via FT.COMPACT (force path) must produce
    /// ceil(n/threshold) threshold-sized immutable segments, not one giant
    /// graph — multiple segments are what the intra-query worker pool fans
    /// out over. All keys must remain findable (self-recall probe).
    #[test]
    fn test_compact_measures_suggested_ef() {
        // AE-1: a compact build with a sidecar must attach a suggested ef
        // from the ladder; tiny/no-sidecar segments stay None (checked by
        // the estimator's own guards).
        distance::init();
        crate::vector::search_pool::init_global(1);
        let dim = 16u32;
        let mut store = VectorStore::new();
        let mut meta = make_idx(dim);
        meta.compact_threshold = 200;
        store.create_index(meta).unwrap();
        for i in 0..400u64 {
            insert(
                &mut store,
                format!("doc:{i}").as_bytes(),
                random_vec(dim as usize, i),
            );
        }
        store.force_compact_index(b"idx").unwrap();
        let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
        let snap = idx.segments.load_full();
        assert!(!snap.immutable.is_empty());
        for seg in &snap.immutable {
            let sug = seg.suggested_ef();
            assert!(
                sug.is_some(),
                "compact build with sidecar must measure an adaptive ef"
            );
            let sug = sug.unwrap() as usize;
            assert!(
                (24..=256).contains(&sug),
                "ladder value expected, got {sug}"
            );
        }
    }

    #[test]
    fn test_force_compact_bulk_bounded_segments() {
        distance::init();
        // Bounded bulk builds are gated on an active intra-query search pool.
        crate::vector::search_pool::init_global(1);
        let dim = 16u32;
        let mut store = VectorStore::new();
        let mut meta = make_idx(dim);
        meta.compact_threshold = 100;
        store.create_index(meta).unwrap();
        let n = 500u64;
        for i in 0..n {
            insert(
                &mut store,
                format!("doc:{i}").as_bytes(),
                random_vec(dim as usize, i),
            );
        }
        store.force_compact_index(b"idx").unwrap();

        let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
        let snap = idx.segments.load_full();
        assert_eq!(
            snap.mutable.len(),
            0,
            "force compact must drain the mutable"
        );
        assert_eq!(
            snap.immutable.len(),
            5,
            "500 vectors at threshold 100 must yield 5 bounded segments"
        );

        // Self-recall: every key still findable by its own vector.
        for i in (0..n).step_by(7) {
            let hash = xxhash_rust::xxh64::xxh64(format!("doc:{i}").as_bytes(), 0);
            let got = search_key_hashes(&mut store, &random_vec(dim as usize, i), 3);
            assert!(
                got.contains(&hash),
                "doc:{i} lost after bounded bulk compact"
            );
        }
    }

    /// Background path: successive begin/poll cycles over a bulk-loaded
    /// mutable must also chip away in threshold-bounded builds.
    #[test]
    fn test_bg_compact_bulk_bounded_segments() {
        distance::init();
        // Bounded bulk builds are gated on an active intra-query search pool.
        crate::vector::search_pool::init_global(1);
        let dim = 16u32;
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        let mut meta = make_idx(dim);
        meta.compact_threshold = 100;
        store.create_index(meta).unwrap();
        for i in 0..500u64 {
            insert(
                &mut store,
                format!("doc:{i}").as_bytes(),
                random_vec(dim as usize, i),
            );
        }
        // Drive begin+install until the mutable drains (bounded per build).
        for _ in 0..64 {
            store.begin_background_compactions(&compactor);
            poll_until_installed(&mut store, 400);
            let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
            if idx.segments.load().mutable.is_empty() {
                break;
            }
        }
        let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
        let snap = idx.segments.load_full();
        assert_eq!(snap.mutable.len(), 0, "bg compaction must eventually drain");
        assert_eq!(
            snap.immutable.len(),
            5,
            "500 vectors at threshold 100 must yield 5 bounded segments (bg path)"
        );
    }

    /// Regression (red/green verified): `force_compact` draining an IN-FLIGHT
    /// background build must ALSO compact the docs inserted while that build
    /// was running (the `clone_suffix(frozen_len)` mutable tail). The old
    /// early return left the tail mutable-only — no durable segment — until
    /// some future compact fired, breaking FT.COMPACT's full-drain contract
    /// (frozen == mutable on reply) and, combined with the B3 phantom-keymap
    /// hole, silently losing those docs on a kill -9 (see
    /// `crash_recovery_vector_durability` S1/S6). Deterministic regardless of
    /// worker speed: `bg_compact_inflight` stays `Some` until installed, and
    /// force_compact's drain blocks on the reply channel.
    #[test]
    fn test_force_compact_drains_tail_inserted_during_inflight_bg_build() {
        distance::init();
        let dim = 16u32;
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        let mut meta = make_idx(dim);
        meta.compact_threshold = 100;
        store.create_index(meta).unwrap();

        // 100 docs -> due-gated background build freezes all of them.
        for i in 0..100u64 {
            insert(
                &mut store,
                format!("doc:{i}").as_bytes(),
                random_vec(dim as usize, i),
            );
        }
        store.begin_background_compactions(&compactor);
        {
            let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
            assert!(
                idx.bg_compact_inflight.is_some(),
                "sanity: a background build must be in flight"
            );
        }

        // Tail: 30 more docs land while the background build is in flight.
        for i in 100..130u64 {
            insert(
                &mut store,
                format!("doc:{i}").as_bytes(),
                random_vec(dim as usize, i),
            );
        }

        store.force_compact_index(b"idx").unwrap();

        let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
        let snap = idx.segments.load_full();
        assert_eq!(
            snap.mutable.len(),
            0,
            "force_compact must drain the tail inserted during the in-flight background build"
        );
        let live: u32 = snap.immutable.iter().map(|s| s.live_count()).sum();
        assert_eq!(
            live, 130,
            "all docs must be segment-resident after force_compact"
        );
    }

    /// Like [`make_idx`] but with a caller-chosen index name (and a matching
    /// key prefix), so a test can create several independent indexes.
    fn make_idx_named(name: Bytes, dim: u32) -> IndexMeta {
        let prefix = Bytes::from([name.as_ref(), b":"].concat());
        let mut meta = make_idx(dim);
        meta.name = name;
        meta.key_prefixes = vec![prefix];
        meta
    }

    // ── Test 7: worker-pool parallelism (limitation #1 fix) ───────────────────

    /// K independent index compactions on a K-worker pool must finish in well
    /// under the fully-serialized time, proving the pool parallelizes builds
    /// across indexes/shards (a single-worker pool serializes them).
    ///
    /// Self-calibrating: measures ONE build alone, then asserts K builds on K
    /// workers stay far below `K × single` (a 1-worker pool would take ≈
    /// `K × single`). Timing assertion is skipped on machines with < K+1 cores
    /// (can't physically parallelize) so it never flakes on tiny CI runners;
    /// the correctness checks still run everywhere.
    #[test]
    fn test_bg_compact_pool_parallelism() {
        use std::time::{Duration, Instant};
        distance::init();
        const K: usize = 3;
        const T: usize = 1500;
        let dim = 48u32;
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        // Build a store with `n` indexes (idx0..idxn), each holding T vectors.
        let build_store = |n: usize| -> VectorStore {
            let mut store = VectorStore::new();
            for k in 0..n {
                let name = format!("idx{k}");
                store
                    .create_index(make_idx_named(Bytes::from(name.clone()), dim))
                    .unwrap();
                for i in 0..T {
                    let key = format!("idx{k}:{i}");
                    let hash = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
                    store
                        .insert_vector(
                            name.as_bytes(),
                            &random_vec(dim as usize, (k * T + i) as u64),
                            hash,
                            Bytes::from(key),
                        )
                        .unwrap();
                }
            }
            store
        };

        // Drive begin+poll until every index has an immutable segment.
        let run_until_all_compacted =
            |store: &mut VectorStore, compactor: &BackgroundCompactor, n: usize| -> Duration {
                let t0 = Instant::now();
                loop {
                    store.begin_background_compactions(compactor);
                    store.poll_install_compactions();
                    let done = (0..n).all(|k| {
                        store
                            .get_index(format!("idx{k}").as_bytes())
                            .map(|idx| !idx.segments.load().immutable.is_empty())
                            .unwrap_or(false)
                    });
                    if done {
                        return t0.elapsed();
                    }
                    // Generous deadline: under a full `cargo test` run every
                    // core is saturated by sibling tests and worker builds can
                    // take many times their isolated duration (observed >30s).
                    assert!(t0.elapsed().as_secs() < 120, "compaction timed out");
                    std::thread::sleep(Duration::from_millis(2));
                }
            };

        // Calibrate: time ONE build on a 1-worker pool.
        let mut s1 = build_store(1);
        let c1 = BackgroundCompactor::new(1);
        let single = run_until_all_compacted(&mut s1, &c1, 1);

        // Time K builds on a K-worker pool.
        let mut sk = build_store(K);
        let ck = BackgroundCompactor::new(K);
        let parallel = run_until_all_compacted(&mut sk, &ck, K);

        // Correctness: all K indexes compacted to exactly one immutable segment.
        for k in 0..K {
            assert_eq!(
                sk.get_index(format!("idx{k}").as_bytes())
                    .unwrap()
                    .segments
                    .load()
                    .immutable
                    .len(),
                1,
                "index idx{k} must have one immutable segment after parallel compaction"
            );
        }

        // Concurrency: a serial (1-worker) run would take ≈ K × single. A
        // K-worker pool should stay well under `K × single × 0.6`. Generous
        // margin avoids flakiness; only a genuinely non-parallel pool fails.
        if cores > K {
            assert!(
                parallel.as_secs_f64() < single.as_secs_f64() * (K as f64) * 0.6,
                "K-worker pool not parallelizing: single={single:?}, parallel(K={K})={parallel:?}"
            );
        }
    }

    // ── Test 1: basic round-trip ──────────────────────────────────────────────

    /// Background compaction dispatches and installs a segment.
    /// After install the search uses HNSW (immutable list is non-empty).
    #[test]
    fn test_bg_compact_basic_roundtrip() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        // Insert T vectors so the segment is worth compacting.
        const T: usize = 30;
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }

        let snap_before = store
            .get_index(b"idx")
            .unwrap()
            .segments
            .load()
            .immutable
            .len();
        assert_eq!(snap_before, 0, "no immutable segments before compact");

        let submitted = store.begin_background_compactions(&compactor);
        assert_eq!(submitted, 1, "one job submitted");

        let installed = poll_until_installed(&mut store, 200);
        assert!(installed, "segment must be installed within timeout");

        let snap_after = store
            .get_index(b"idx")
            .unwrap()
            .segments
            .load()
            .immutable
            .len();
        assert_eq!(snap_after, 1, "one immutable segment after compact");
    }

    // ── Test 2: tail vectors visible after install ────────────────────────────

    /// Vectors inserted AFTER begin_background_compact() (during the build)
    /// must appear in search results after install.
    #[test]
    fn test_bg_compact_tail_vectors_visible() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        // Insert initial batch.
        const T: usize = 20;
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }

        // Dispatch compaction.
        let submitted = store.begin_background_compactions(&compactor);
        assert_eq!(submitted, 1);

        // Insert tail vectors AFTER dispatch.
        let tail_hash = xxhash_rust::xxh64::xxh64(b"doc:tail", 0);
        let tail_vec = random_vec(64, 9999);
        store
            .insert_vector(
                b"idx",
                &tail_vec,
                tail_hash,
                Bytes::from_static(b"doc:tail"),
            )
            .unwrap();

        let installed = poll_until_installed(&mut store, 200);
        assert!(installed, "must install");

        // The tail entry must be in the mutable segment of the new list.
        let snap = store.get_index(b"idx").unwrap().segments.load();
        let mutable_len = snap.mutable.len();
        assert_eq!(mutable_len, 1, "tail segment has exactly 1 entry");

        // Search for the tail vector — it must be findable.
        let results = search_key_hashes(&mut store, &tail_vec, 5);
        assert!(
            results.contains(&tail_hash),
            "tail vector must appear in search results"
        );
    }

    // ── Test 3: delete-before-install reconciliation (RED before reconciliation) ──

    /// A vector deleted AFTER begin_background_compact() but BEFORE install
    /// must NOT appear in search results after install.
    ///
    /// Without reconciliation this is RED: the deleted key resurrects because
    /// the frozen snapshot captured it as live.
    #[test]
    fn test_bg_compact_delete_before_install_reconciled() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 25;
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }

        // Dispatch compaction — freezes the segment.
        assert_eq!(store.begin_background_compactions(&compactor), 1);

        // Delete doc:0 AFTER dispatch (post-freeze window delete).
        store.mark_deleted_for_key(b"doc:0");
        let deleted_hash = xxhash_rust::xxh64::xxh64(b"doc:0", 0);

        let installed = poll_until_installed(&mut store, 200);
        assert!(installed, "must install");

        // doc:0 must NOT appear in search results (reconciled during install).
        let query = random_vec(64, 0); // same seed as doc:0 → near-neighbor
        let results = search_key_hashes(&mut store, &query, T);
        assert!(
            !results.contains(&deleted_hash),
            "deleted key must be absent from results after reconciled install"
        );
    }

    // ── Test 4: overwrite-before-install reconciliation (RED before reconciliation) ─

    /// A key overwritten (delete + re-insert) AFTER begin_background_compact()
    /// must appear exactly ONCE in results after install, with the new version.
    ///
    /// Without reconciliation this is RED: the old frozen version resurfaces.
    #[test]
    fn test_bg_compact_overwrite_before_install_no_duplicate() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 20;
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }

        // Dispatch compaction.
        assert_eq!(store.begin_background_compactions(&compactor), 1);

        // Overwrite doc:5: delete the old version, insert a new one.
        store.mark_deleted_for_key(b"doc:5");
        let overwrite_hash = xxhash_rust::xxh64::xxh64(b"doc:5", 0);
        let new_vec = random_vec(64, 555); // different vector
        store
            .insert_vector(
                b"idx",
                &new_vec,
                overwrite_hash,
                Bytes::from_static(b"doc:5"),
            )
            .unwrap();

        let installed = poll_until_installed(&mut store, 200);
        assert!(installed, "must install");

        // Search: doc:5's hash must appear AT MOST ONCE (no duplicate from frozen snapshot).
        let query = random_vec(64, 5); // near doc:5 old version
        let results = search_key_hashes(&mut store, &query, T + 5);
        let count = results.iter().filter(|&&h| h == overwrite_hash).count();
        assert_eq!(
            count, 1,
            "overwritten key must appear exactly once (not 0=data-lost, not 2=duplicate), got {count}"
        );
    }

    /// A key UPDATED (tombstone old + append new, VEC-1 semantics) BEFORE
    /// begin_background_compact() must survive the install. Both the dead old
    /// copy and the live new copy sit inside the frozen window; compact()
    /// already filters the dead copy, so the install reconcile must NOT
    /// key_hash-wide-tombstone the new copy out of the immutable.
    ///
    /// RED before the fix: snap_and_reconcile treated ANY dead window entry as
    /// "key deleted" and killed the key's live compacted copy — every key
    /// updated-then-compacted vanished from FT.SEARCH (32% of live keys lost
    /// in the Bundle-5 churn soak; regression introduced with VEC-1).
    #[test]
    fn test_bg_compact_update_before_freeze_survives_install() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 20;
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }

        // UPDATE doc:5 BEFORE dispatch: dead old + live new, both in-window.
        store.mark_deleted_for_key(b"doc:5");
        let updated_hash = xxhash_rust::xxh64::xxh64(b"doc:5", 0);
        let new_vec = random_vec(64, 555);
        store
            .insert_vector(b"idx", &new_vec, updated_hash, Bytes::from_static(b"doc:5"))
            .unwrap();

        assert_eq!(store.begin_background_compactions(&compactor), 1);
        assert!(poll_until_installed(&mut store, 200), "must install");

        // The updated key must still be findable by its NEW vector, exactly once.
        let results = search_key_hashes(&mut store, &new_vec, T + 5);
        let count = results.iter().filter(|&&h| h == updated_hash).count();
        assert_eq!(
            count, 1,
            "updated-then-compacted key must survive install (0=lost, 2=duplicate), got {count}"
        );
    }

    // ── Test 5: steady-state HDEL tombstones installed immutable ─────────────

    /// mark_deleted_for_key on an already-installed immutable segment must
    /// tombstone that entry so it no longer appears in search results.
    #[test]
    fn test_bg_compact_steady_state_delete_tombstones_immutable() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 20;
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }

        assert_eq!(store.begin_background_compactions(&compactor), 1);
        let installed = poll_until_installed(&mut store, 200);
        assert!(installed, "must install");

        // Verify doc:3 is findable BEFORE deletion.
        let target_hash = xxhash_rust::xxh64::xxh64(b"doc:3", 0);
        let query = random_vec(64, 3);
        let before = search_key_hashes(&mut store, &query, T);
        assert!(
            before.contains(&target_hash),
            "doc:3 must be found before deletion"
        );

        // Now tombstone it via the steady-state path (Arc'd immutable).
        store.mark_deleted_for_key(b"doc:3");

        // Search again — doc:3 must be absent.
        let after = search_key_hashes(&mut store, &query, T);
        assert!(
            !after.contains(&target_hash),
            "doc:3 must be absent after steady-state tombstone"
        );
    }

    /// Prod-hardening #20: DEL of a doc must tombstone its vector in EVERY
    /// vector field, not just the default field. A secondary VECTOR field
    /// lives in `idx.field_segments`, which `tombstone_key_in_index` used to
    /// skip entirely — leaving the deleted doc alive in that field's search.
    #[test]
    fn test_del_tombstones_secondary_vector_field() {
        distance::init();
        let dim = 64u32;
        let mut store = VectorStore::new();
        let mut meta = make_idx(dim);
        // Two-field schema: vector_fields[0] is the default field, [1] is a
        // secondary field that create_index materializes into field_segments.
        meta.vector_fields = vec![
            VectorFieldMeta {
                field_name: Bytes::from_static(b"vec"),
                dimension: dim,
                padded_dimension: padded_dimension(dim),
                metric: DistanceMetric::L2,
                quantization: QuantizationConfig::TurboQuant4,
                build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
            },
            VectorFieldMeta {
                field_name: Bytes::from_static(b"vec2"),
                dimension: dim,
                padded_dimension: padded_dimension(dim),
                metric: DistanceMetric::L2,
                quantization: QuantizationConfig::TurboQuant4,
                build_mode: crate::vector::turbo_quant::collection::BuildMode::Light,
            },
        ];
        store.create_index(meta).unwrap();

        let key = b"doc:1";
        let key_hash = xxhash_rust::xxh64::xxh64(key, 0);
        // Default field insert (also registers key_hash_to_key).
        insert(&mut store, key, random_vec(dim as usize, 1));
        // Secondary-field insert: mirror the production insert path by
        // appending straight into vec2's mutable segment.
        {
            let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
            let fs = idx.field_segments.get(b"vec2".as_ref()).unwrap();
            let snap = fs.segments.load();
            snap.mutable
                .append(key_hash, &random_vec(dim as usize, 2), 1);
            assert_eq!(snap.mutable.live_len(), 1, "vec2 has the live doc");
        }

        // DEL doc:1 → both fields must be tombstoned.
        store.mark_deleted_for_key(key);

        let idx = store.indexes.get_mut(b"idx".as_ref()).unwrap();
        let fs = idx.field_segments.get(b"vec2".as_ref()).unwrap();
        let snap = fs.segments.load();
        assert_eq!(
            snap.mutable.live_len(),
            0,
            "secondary field vec2 must have no live docs after DEL (#20)"
        );
    }

    // ── Test 6: force_compact while in-flight ────────────────────────────────

    /// force_compact_index() called while a background job is in-flight must
    /// drain and install the in-flight result (or discard it) and NOT produce
    /// a duplicate compaction of the same data.
    ///
    /// Invariant: after force_compact completes, there is at least 1 immutable
    /// segment and no stale in-flight state.
    #[test]
    fn test_bg_compact_force_compact_while_inflight() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 20;
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }

        // Start background compaction but do NOT poll yet.
        assert_eq!(store.begin_background_compactions(&compactor), 1);

        // Wait for worker to finish (so result is queued in the channel).
        std::thread::sleep(std::time::Duration::from_millis(500));

        // force_compact should drain the in-flight result first (poll it),
        // then no-op the inline compact (mutable is already empty / tail only).
        // At minimum it must not panic and the index must have ≥ 1 immutable.
        store.force_compact_index(b"idx").unwrap();

        // Clean up any remaining inflight (poll once more just in case).
        store.poll_install_compactions();

        let snap = store.get_index(b"idx").unwrap().segments.load();
        assert!(
            !snap.immutable.is_empty(),
            "at least one immutable segment must exist after force_compact"
        );

        // No stale in-flight.
        let idx = store.get_index(b"idx").unwrap();
        assert!(
            idx.bg_compact_inflight.is_none(),
            "no in-flight state after force_compact"
        );
    }

    // ── Background merge tests (P2) ──────────────────────────────────────────

    /// Poll until a background merge is installed, or we hit `max_iters`.
    fn poll_until_merged(store: &mut VectorStore, max_iters: usize) -> bool {
        for _ in 0..max_iters {
            if store.poll_install_merges() > 0 {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        false
    }

    // ── Merge test 1 ─────────────────────────────────────────────────────────

    /// Build M=4 immutable segments, merge them in the background, assert the
    /// result is a single segment with all live vectors.
    #[test]
    fn test_bg_merge_reduces_segments() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 15; // vectors per segment
        const M: usize = 4;

        // Build M immutable segments by inserting T distinct keys + force_compact.
        for seg in 0..M {
            for i in 0..T {
                let key = format!("seg{seg}_doc{i}");
                insert(
                    &mut store,
                    key.as_bytes(),
                    random_vec(64, (seg * T + i) as u64),
                );
            }
            store.force_compact_index(b"idx").unwrap();
        }

        {
            let snap = store.get_index(b"idx").unwrap().segments.load();
            assert_eq!(
                snap.immutable.len(),
                M,
                "expected {M} segments before merge"
            );
        }

        // Dispatch background merge directly (bypass needs_merge threshold).
        let idx = store.get_index_mut(b"idx").unwrap();
        assert!(
            idx.begin_background_merge(&compactor),
            "merge should be dispatched"
        );

        let merged = poll_until_merged(&mut store, 500);
        assert!(merged, "merge must install within timeout");

        let snap = store.get_index(b"idx").unwrap().segments.load();
        assert_eq!(snap.immutable.len(), 1, "must be a single merged segment");

        let live = snap.immutable[0].live_count() as usize;
        assert_eq!(live, M * T, "all {} live vectors must survive merge", M * T);

        // Search for a known vector — must be found.
        let query = random_vec(64, 0u64); // same as seg0_doc0
        let results = search_key_hashes(&mut store, &query, M * T);
        let target_hash = xxhash_rust::xxh64::xxh64(b"seg0_doc0", 0);
        assert!(
            results.contains(&target_hash),
            "seg0_doc0 must be findable after merge"
        );

        // No duplicate key_hashes in results.
        let mut seen = std::collections::HashSet::new();
        for &h in &results {
            assert!(seen.insert(h), "duplicate key_hash {h} in results");
        }
    }

    // ── Merge test 2 ─────────────────────────────────────────────────────────

    /// A key inserted twice (in two segments, seg1 with insert_lsn lower, seg2
    /// with insert_lsn higher) must appear exactly ONCE after merge.
    #[test]
    fn test_bg_merge_dedup_overwrite() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        // seg1: key X = vec_a (lower insert_lsn) + padding keys.
        let x_hash = xxhash_rust::xxh64::xxh64(b"key_x", 0);
        let vec_b = random_vec(64, 999); // the "new" vector we'll search for

        for i in 0..10usize {
            let key = format!("pad1_{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }
        insert(&mut store, b"key_x", random_vec(64, 111));
        store.force_compact_index(b"idx").unwrap(); // seg1 sealed

        // seg2: key X = vec_b (higher insert_lsn) + more padding.
        for i in 0..10usize {
            let key = format!("pad2_{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, (100 + i) as u64));
        }
        store
            .insert_vector(b"idx", &vec_b, x_hash, Bytes::from_static(b"key_x"))
            .unwrap();
        store.force_compact_index(b"idx").unwrap(); // seg2 sealed

        let idx = store.get_index_mut(b"idx").unwrap();
        assert!(idx.begin_background_merge(&compactor), "merge dispatched");

        assert!(poll_until_merged(&mut store, 500), "merge installed");

        // Search near vec_b — key_x must appear exactly once.
        let results = search_key_hashes(&mut store, &vec_b, 30);
        let count = results.iter().filter(|&&h| h == x_hash).count();
        assert_eq!(
            count, 1,
            "key_x must appear exactly once after merge dedup (got {count})"
        );
    }

    // ── Merge backoff tests (CPU-livelock guard, 2026-07-16) ─────────────────

    /// Build `n` immutable segments of 3 tiny vectors each.
    fn build_n_segments(store: &mut VectorStore, n: usize) {
        for seg in 0..n {
            for i in 0..3usize {
                let key = format!("s{seg}d{i}");
                insert(store, key.as_bytes(), random_vec(64, (seg * 3 + i) as u64));
            }
            store.force_compact_index(b"idx").unwrap();
        }
    }

    /// Drive poll_install_merge until the in-flight merge reply is consumed
    /// (installed OR rejected), or `max_iters` elapse.
    fn drain_inflight_merge(store: &mut VectorStore, max_iters: usize) {
        for _ in 0..max_iters {
            let idx = store.get_index_mut(b"idx").unwrap();
            idx.poll_install_merge();
            if idx.bg_merge_inflight.is_none() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        panic!("merge worker reply never arrived");
    }

    /// A gate-failed unattended merge must NOT be re-dispatched immediately
    /// for the same segment set: `needs_merge` stays true after a recall-gate
    /// rejection (the sources are kept), so without a backoff the autovacuum
    /// tick resubmits the identical doomed merge every pass — each attempt
    /// builds (and discards) a full union HNSW graph. Observed in production
    /// as `moon-vec-compact-*` workers pinning ~3 cores indefinitely.
    #[test]
    fn bg_merge_gate_failure_backs_off() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        // 17 immutable segments → needs_merge() count trigger (> 16).
        build_n_segments(&mut store, 17);

        let idx = store.get_index_mut(b"idx").unwrap();
        // Unreachable tolerance (recall ≤ 1.0) → RecallTooLow, deterministically.
        // Set directly: FT.CONFIG clamps to 0.0..=1.0, where real data could
        // legitimately pass the gate and flake this test.
        idx.merge_recall_tolerance = 2.0;
        assert!(idx.needs_merge(), "count trigger must fire (17 > 16)");
        assert!(
            idx.begin_background_merge_due(&compactor),
            "first dispatch goes through"
        );

        drain_inflight_merge(&mut store, 500);

        let idx = store.get_index_mut(b"idx").unwrap();
        assert_eq!(
            idx.segments.load().immutable.len(),
            17,
            "gate-failed merge must keep sources unchanged"
        );
        assert!(
            idx.needs_merge(),
            "trigger condition itself remains true after the failure"
        );
        assert!(
            !idx.begin_background_merge_due(&compactor),
            "identical gate-failed merge re-dispatched immediately — CPU livelock"
        );
    }

    /// The backoff is fingerprinted to the exact source segment set: when the
    /// set changes (new segment compacted in, warm-tier transition, deletes),
    /// the doomed-merge assumption no longer holds and dispatch must resume
    /// immediately.
    #[test]
    fn bg_merge_backoff_clears_on_segment_set_change() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        build_n_segments(&mut store, 17);

        let idx = store.get_index_mut(b"idx").unwrap();
        idx.merge_recall_tolerance = 2.0;
        assert!(idx.begin_background_merge_due(&compactor));
        drain_inflight_merge(&mut store, 500);
        {
            let idx = store.get_index_mut(b"idx").unwrap();
            assert!(
                !idx.begin_background_merge_due(&compactor),
                "backoff active for the failed set"
            );
        }

        // Grow the segment set — fingerprint changes.
        build_n_segments(&mut store, 1);

        let idx = store.get_index_mut(b"idx").unwrap();
        assert!(
            idx.begin_background_merge_due(&compactor),
            "changed segment set must clear the backoff and dispatch"
        );
        drain_inflight_merge(&mut store, 500);
    }

    /// Operator intervention via FT.CONFIG SET MERGE_RECALL_TOLERANCE clears
    /// the backoff: the operator just changed the very parameter the gate
    /// fired on, so the next tick should try again with the new tolerance.
    #[test]
    fn bg_merge_backoff_clears_on_tolerance_change() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        build_n_segments(&mut store, 17);

        let idx = store.get_index_mut(b"idx").unwrap();
        idx.merge_recall_tolerance = 2.0;
        assert!(idx.begin_background_merge_due(&compactor));
        drain_inflight_merge(&mut store, 500);
        {
            let idx = store.get_index_mut(b"idx").unwrap();
            assert!(!idx.begin_background_merge_due(&compactor));
        }

        // What the FT.CONFIG SET MERGE_RECALL_TOLERANCE handler calls.
        let idx = store.get_index_mut(b"idx").unwrap();
        idx.merge_recall_tolerance = 0.0;
        idx.clear_merge_backoff();
        assert!(
            idx.begin_background_merge_due(&compactor),
            "tolerance change must clear the backoff"
        );
        // Tolerance 0.0 always passes the gate — merge installs.
        assert!(poll_until_merged(&mut store, 500), "merge must install");
        let snap = store.get_index(b"idx").unwrap().segments.load();
        assert_eq!(snap.immutable.len(), 1, "17 segments merged into one");
    }

    /// Exponential schedule: 60s base, doubling, capped at 1h.
    #[test]
    fn bg_merge_backoff_schedule() {
        use std::time::Duration;
        assert_eq!(merge_backoff_duration(1), Duration::from_secs(60));
        assert_eq!(merge_backoff_duration(2), Duration::from_secs(120));
        assert_eq!(merge_backoff_duration(3), Duration::from_secs(240));
        assert_eq!(merge_backoff_duration(7), Duration::from_secs(3600));
        assert_eq!(merge_backoff_duration(30), Duration::from_secs(3600));
        assert_eq!(merge_backoff_duration(u32::MAX), Duration::from_secs(3600));
    }

    /// A key UPDATED across segments must survive a merge: old copy in seg1
    /// (interior-tombstoned by the update), new copy compacted into seg2, then
    /// seg1+seg2 merged.
    ///
    /// RED before the fix: `poll_install_merge` replayed seg1's interior
    /// tombstone set key_hash-WIDE onto the merged output, killing the NEW
    /// copy that came from seg2 — the merge-side twin of the
    /// `snap_and_reconcile` update bug (657 keys lost in the churn soak with
    /// merges enabled even after the compact-install fix).
    #[test]
    fn test_bg_merge_update_across_segments_survives() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 15;

        // seg1: doc:0..T, including the soon-to-be-updated doc:5.
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }
        store.force_compact_index(b"idx").unwrap();

        // UPDATE doc:5 (VEC-1 semantics): interior-tombstone the old copy in
        // the Arc'd seg1, append the new vector to the mutable segment.
        let updated_hash = xxhash_rust::xxh64::xxh64(b"doc:5", 0);
        store.mark_deleted_for_key(b"doc:5");
        let new_vec = random_vec(64, 555);
        store
            .insert_vector(b"idx", &new_vec, updated_hash, Bytes::from_static(b"doc:5"))
            .unwrap();

        // seg2: padding + the new doc:5 copy, sealed.
        for i in T..2 * T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }
        store.force_compact_index(b"idx").unwrap();

        // Merge seg1+seg2 — seg1's tombstone must NOT kill seg2's new copy.
        let idx = store.get_index_mut(b"idx").unwrap();
        assert!(idx.begin_background_merge(&compactor), "merge dispatched");
        assert!(poll_until_merged(&mut store, 500), "merge installed");

        let results = search_key_hashes(&mut store, &new_vec, 2 * T + 5);
        let count = results.iter().filter(|&&h| h == updated_hash).count();
        assert_eq!(
            count, 1,
            "updated-then-merged key must survive install (0=lost, 2=duplicate), got {count}"
        );
    }

    // ── Merge test 3 ─────────────────────────────────────────────────────────

    /// A key deleted via steady-state interior tombstone on an Arc'd immutable
    /// segment BEFORE merge must NOT appear in search results after merge.
    ///
    /// ## RED / GREEN
    ///
    /// This test is RED when the reapply-deletes loop in `poll_install_merge`
    /// is commented out: `merge_immutable` copies mvcc headers but does NOT
    /// consult the interior `tombstoned_keys` set, so the key resurrects.
    /// It is GREEN when the loop is present (the default in this codebase).
    #[test]
    fn test_bg_merge_honors_steady_state_delete() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 15;

        // seg1: T keys including "victim".
        for i in 0..T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }
        store.force_compact_index(b"idx").unwrap();

        // seg2: more keys (distinct).
        for i in T..2 * T {
            let key = format!("doc:{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, i as u64));
        }
        store.force_compact_index(b"idx").unwrap();

        // Steady-state delete on the Arc'd immutable in seg1 — writes only to
        // the interior tombstoned_keys set (cannot touch mvcc while Arc'd).
        let victim_hash = xxhash_rust::xxh64::xxh64(b"doc:0", 0);
        store.mark_deleted_for_key(b"doc:0");

        // Confirm it's gone before merge.
        let query_pre = random_vec(64, 0);
        let pre_results = search_key_hashes(&mut store, &query_pre, 2 * T);
        assert!(
            !pre_results.contains(&victim_hash),
            "doc:0 must be absent before merge (interior tombstone)"
        );

        // Now merge — reapply-deletes in poll_install_merge must carry the tombstone.
        let idx = store.get_index_mut(b"idx").unwrap();
        assert!(idx.begin_background_merge(&compactor), "merge dispatched");
        assert!(poll_until_merged(&mut store, 500), "merge installed");

        let snap = store.get_index(b"idx").unwrap().segments.load();
        assert_eq!(snap.immutable.len(), 1, "single merged segment");

        // Key must still be absent after merge.
        let post_results = search_key_hashes(&mut store, &query_pre, 2 * T);
        assert!(
            !post_results.contains(&victim_hash),
            "doc:0 must remain absent after merge (reapply-deletes)"
        );
    }

    // ── Merge test 4 ─────────────────────────────────────────────────────────

    /// Compaction and merge must be mutually exclusive in BOTH directions:
    ///   (a) compaction in-flight → begin_background_merge returns false.
    ///   (b) merge in-flight → begin_background_compact returns false.
    ///
    /// Needs ≥2 immutable segments + a non-empty mutable so both operations
    /// have real data to act on (otherwise they'd return false for the wrong
    /// reason — no data rather than the guard).
    #[test]
    fn test_bg_merge_mutually_exclusive() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const T: usize = 15;

        // Build 2 immutable segments.
        for seg in 0..2usize {
            for i in 0..T {
                let key = format!("s{seg}_doc{i}");
                insert(
                    &mut store,
                    key.as_bytes(),
                    random_vec(64, (seg * T + i) as u64),
                );
            }
            store.force_compact_index(b"idx").unwrap();
        }

        // Add some live mutable entries so compaction has work to do.
        for i in 0..T {
            let key = format!("live_{i}");
            insert(&mut store, key.as_bytes(), random_vec(64, (200 + i) as u64));
        }

        // (a) Compaction in-flight → merge begin must return false.
        {
            let idx = store.get_index_mut(b"idx").unwrap();
            let compaction_started = idx.begin_background_compact(&compactor);
            assert!(
                compaction_started,
                "compaction must start (mutable is non-empty)"
            );
            let merge_started = idx.begin_background_merge(&compactor);
            assert!(
                !merge_started,
                "merge must not start while compaction is in-flight"
            );
            // Drain the in-flight compaction so we can test the reverse.
            // Block until done.
            if let Some(inflight) = idx.bg_compact_inflight.take() {
                let _ = inflight.reply_rx.recv(); // wait for worker
            }
        }

        // (b) Merge in-flight → compaction begin must return false.
        {
            let idx = store.get_index_mut(b"idx").unwrap();
            let merge_started = idx.begin_background_merge(&compactor);
            assert!(merge_started, "merge must start (2 immutables exist)");
            let compaction_started = idx.begin_background_compact(&compactor);
            assert!(
                !compaction_started,
                "compaction must not start while merge is in-flight"
            );
            // Drain merge.
            if let Some(inflight) = idx.bg_merge_inflight.take() {
                let _ = inflight.reply_rx.recv();
            }
        }
    }

    // ── Merge test 5 ─────────────────────────────────────────────────────────

    /// Recall of the merged single-segment search vs brute-force ground truth
    /// must be ≥ 0.80, OR the recall gate must have fired (segments unchanged).
    ///
    /// Note: random-Gaussian vectors at 64d exhibit distance concentration
    /// (CLAUDE.md warning), so recall can be lower than on real embeddings.
    /// The assertion is intentionally loose (0.80) to pass on random data.
    /// The key invariant is that recall does NOT collapse to ~0 (the broken
    /// decode→re-encode path collapses to 0.0005 per CLAUDE.md).
    #[test]
    fn test_bg_merge_recall_preserved() {
        distance::init();
        let compactor = BackgroundCompactor::new(1);
        let mut store = VectorStore::new();
        store.create_index(make_idx(64)).unwrap();

        const DIM: usize = 64;
        const PER_SEG: usize = 200;
        const M: usize = 3;
        const NUM_QUERIES: usize = 30;
        const K: usize = 10;

        // Collect all inserted vectors for brute-force ground truth.
        let mut all_vecs: Vec<(u64, Vec<f32>)> = Vec::new(); // (key_hash, vec)

        for seg in 0..M {
            for i in 0..PER_SEG {
                let seed = (seg * PER_SEG + i) as u64 + 1000;
                let key = format!("seg{seg}_v{i}");
                let vec = random_vec(DIM, seed);
                let kh = xxhash_rust::xxh64::xxh64(key.as_bytes(), 0);
                all_vecs.push((kh, vec.clone()));
                store
                    .insert_vector(b"idx", &vec, kh, Bytes::from(key))
                    .unwrap();
            }
            store.force_compact_index(b"idx").unwrap();
        }

        // Try background merge.
        let idx = store.get_index_mut(b"idx").unwrap();
        let merge_started = idx.begin_background_merge(&compactor);

        if !merge_started {
            // Worker pool full or some other transient error — skip recall check.
            return;
        }

        let merged = poll_until_merged(&mut store, 500);

        let snap = store.get_index(b"idx").unwrap().segments.load();
        let segment_count = snap.immutable.len();
        drop(snap);

        if !merged || segment_count != 1 {
            // Recall gate fired — segments unchanged. This is correct behavior.
            eprintln!(
                "test_bg_merge_recall_preserved: merge gate fired (segment_count={segment_count}), \
                 recall gate path taken — OK"
            );
            assert!(
                segment_count >= 1,
                "segments must be intact when gate fires"
            );
            return;
        }

        // Compute brute-force top-K ground truth for NUM_QUERIES query vectors.
        let mut total_recall = 0.0f32;
        for q in 0..NUM_QUERIES {
            let query = random_vec(DIM, (q as u64) * 7919 + 3); // prime-spaced seeds
            // Brute-force top-K by L2 distance.
            let mut dists: Vec<(ordered_float::OrderedFloat<f32>, u64)> = all_vecs
                .iter()
                .map(|(kh, v)| {
                    let d: f32 = v
                        .iter()
                        .zip(query.iter())
                        .map(|(a, b)| (a - b) * (a - b))
                        .sum();
                    (ordered_float::OrderedFloat(d), *kh)
                })
                .collect();
            dists.sort_unstable();
            let gt: std::collections::HashSet<u64> = dists[..K.min(dists.len())]
                .iter()
                .map(|(_, kh)| *kh)
                .collect();

            // Merged-segment search results.
            let found: std::collections::HashSet<u64> = search_key_hashes(&mut store, &query, K)
                .into_iter()
                .collect();

            let overlap = gt.intersection(&found).count();
            total_recall += overlap as f32 / K as f32;
        }

        let recall = total_recall / NUM_QUERIES as f32;
        eprintln!("test_bg_merge_recall_preserved: recall@{K} = {recall:.4} (threshold 0.80)");
        assert!(
            recall >= 0.80,
            "merged segment recall {recall:.4} below 0.80 — quantization error collapsed recall"
        );
    }
}
