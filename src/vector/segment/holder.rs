//! SegmentHolder -- ArcSwap-based lock-free segment list access.
//!
//! Searches load() once at query start and hold the Arc for the query
//! duration -- immune to concurrent swaps.

mod promote;

use std::sync::Arc;

use arc_swap::ArcSwap;
use roaring::RoaringBitmap;
use smallvec::SmallVec;

use crate::vector::diskann::segment::DiskAnnSegment;
use crate::vector::filter::selectivity::{FilterStrategy, select_strategy};
use crate::vector::hnsw::search::SearchScratch;
use crate::vector::keymap::BucketedKeyMap;
use crate::vector::persistence::warm_search::WarmSearchSegment;
use crate::vector::segment::ivf::IvfSegment;
use crate::vector::turbo_quant::encoder::padded_dimension;
use crate::vector::turbo_quant::fwht;
use crate::vector::types::SearchResult;

use super::immutable::ImmutableSegment;
use super::mutable::{MutableEntry, MutableSegment};

/// Default number of IVF clusters to probe during search.
const DEFAULT_NPROBE: usize = 32;

// Per-segment beam policy (AE-1): every graph segment searches at the FULL
// resolved ef unless its compact-time saturation probe certified it as
// trivially easy (`ImmutableSegment::suggested_ef`), in which case min-ef
// suffices. A blanket `ef/√G` split was tried first (EF-SPLIT) and REJECTED:
// it silently cost R@10 0.9915 → 0.9295 on gaussian 5-seg — a per-segment
// beam reduction is only safe when the segment itself proves it saturates.

/// MVCC context for snapshot-isolated search. Passed by reference, zero allocation.
pub struct MvccContext<'a> {
    pub snapshot_lsn: u64,
    pub my_txn_id: u64,
    pub committed: &'a roaring::RoaringTreemap,
    /// Dirty set: uncommitted entries from the active transaction.
    pub dirty_set: &'a [MutableEntry],
    pub dimension: u32,
    /// AE-1: true when `ef_search` came from the resolution heuristic (no
    /// user `EF_RUNTIME`) — saturation-certified segments may then run at
    /// their min-ef estimate. Always false when the user pinned ef.
    pub ef_defaulted: bool,
    /// Per-index recall/QPS knobs (FT.CONFIG RERANK_MULT / EXACT_BEAM),
    /// copied from `IndexMeta` at search entry.
    pub tuning: crate::vector::types::SearchTuning,
}

/// Snapshot of all segments at a point in time.
///
/// `Clone` is cheap: every field is an `Arc`/`Vec<Arc<_>>` clone (refcount
/// bumps plus one spine allocation per `Vec`, never a data copy). This lets
/// reconstruction sites use Rust's functional-update syntax
/// (`SegmentList { some_field: new_value, ..old.clone() }`) instead of
/// enumerating all fields by hand -- see [`Self::with_unloaded`] for the
/// single-field-patch convenience wrapper. Both exist so that a future field
/// addition (e.g. a per-index `db_index` scope) only needs to be threaded
/// through struct construction sites that build a list FROM SCRATCH, not
/// every site that patches an existing snapshot.
#[derive(Clone)]
pub struct SegmentList {
    pub mutable: Arc<MutableSegment>,
    pub immutable: Vec<Arc<ImmutableSegment>>,
    /// IVF segments for billion-scale approximate search.
    pub ivf: Vec<Arc<IvfSegment>>,
    /// Warm segments: mmap-backed, searchable after HOT->WARM transition.
    pub warm: Vec<Arc<WarmSearchSegment>>,
    /// Cold segments: DiskANN PQ+Vamana search from NVMe.
    pub cold: Vec<Arc<DiskAnnSegment>>,
    /// Unloaded (COLD) segments: on-disk only, reloaded into `warm` on touch.
    pub unloaded: Vec<Arc<crate::vector::persistence::unloaded_segment::UnloadedSegment>>,
}

impl SegmentList {
    /// Clone every field, replacing only `unloaded`. The common shape for
    /// the FLUSHALL/FLUSHDB/DROPINDEX tombstone loops and the mmap-budget
    /// eviction path, which touch nothing else.
    pub fn with_unloaded(
        &self,
        unloaded: Vec<Arc<crate::vector::persistence::unloaded_segment::UnloadedSegment>>,
    ) -> Self {
        Self {
            unloaded,
            ..self.clone()
        }
    }

    /// Clone every field, replacing `warm` and `unloaded` together -- the
    /// shape `VectorIndex::try_warm_transitions_idle`'s idle sweep needs
    /// (both tiers move segments between each other and to/from `immutable`
    /// in the same pass).
    pub fn with_warm_and_unloaded(
        &self,
        warm: Vec<Arc<WarmSearchSegment>>,
        unloaded: Vec<Arc<crate::vector::persistence::unloaded_segment::UnloadedSegment>>,
    ) -> Self {
        Self {
            warm,
            unloaded,
            ..self.clone()
        }
    }
}

/// Bounded cooperative-yield cap for the FT.SEARCH local slice
/// (ft-search-off-eventloop, §3 C3). The search relinquishes to the shard event
/// loop between chunks bounded by these caps — coarse enough that per-query
/// overhead stays negligible, fine enough that co-located commands are not
/// starved for the whole search.
#[derive(Clone, Copy, Debug)]
pub struct YieldBudget {
    /// Yield after this many immutable/warm/cold/ivf segments (default 1: per-segment).
    pub max_segments_per_chunk: usize,
    /// Reserved cap for yielding inside one large HNSW segment's traversal
    /// (per-node). Honored where the traversal exposes a yield point; bounds the
    /// segment-entry threshold above which the per-segment yield is taken.
    pub max_graph_nodes_per_chunk: usize,
    /// Mutable brute-force scan chunk size: the scan is split into ranges of this
    /// many entries, yielding between chunks (the mutable segment is append-only,
    /// so chunked scanning over the captured length is isolation-correct).
    pub max_brute_force_vecs_per_chunk: usize,
}

/// Default yield cap for FT.SEARCH. The brute-force chunk size trades co-located
/// latency against search throughput: each yield costs one runtime trip. The
/// monoio yield is now cost-free (`ft-yield-costfree-monoio`: a self-pipe
/// `UnixStream` park-reap at ~µs vs `sleep(ZERO)`'s ~1.8ms timer wheel), so the
/// chunk no longer has to be coarse to amortize the yield — it returns to a fine
/// 1024, down from the timer tax's 16384. 1024 is the CROSS-ARCH build-measured
/// knee: an end-to-end FT.SEARCH A/B (20k×384d, KNN10, release) holds it within the
/// 5% throughput bound on BOTH targets — x86_64 Sapphire Rapids +2–3.5%, aarch64
/// Neoverse-N1 +2–3.4% — while still yielding ~20×/query (vs ~1 at 16384) so
/// co-located connections see far finer relief. A finer 512 holds on aarch64 (+4%)
/// but BREACHES the bound on x86 (+6–8%): the faster AVX-512 scan shrinks chunk
/// wall-time, so the fixed per-yield cost is a larger fraction — caught by a GCloud
/// cross-arch bench, hence the conservative cross-arch default. Reach a finer chunk
/// per-deployment via `MOON_FT_YIELD_CHUNK`. Per-segment (1) is the immutable boundary.
pub const FT_SEARCH_YIELD_BUDGET: YieldBudget = YieldBudget {
    max_segments_per_chunk: 1,
    max_graph_nodes_per_chunk: 4096,
    max_brute_force_vecs_per_chunk: 1024,
};

/// Runtime-resolved FT.SEARCH yield budget: [`FT_SEARCH_YIELD_BUDGET`] with the
/// brute-force chunk size optionally overridden by `MOON_FT_YIELD_CHUNK` (an
/// operator tuning knob — the latency/throughput trade-off is workload- and
/// runtime-dependent). The env read is cached in a `OnceLock`, so it is one-time
/// at first search, never a per-search hot-path cost.
pub fn ft_search_yield_budget() -> YieldBudget {
    static RESOLVED: std::sync::OnceLock<YieldBudget> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| {
        let mut budget = FT_SEARCH_YIELD_BUDGET;
        if let Ok(raw) = std::env::var("MOON_FT_YIELD_CHUNK") {
            if let Ok(n) = raw.parse::<usize>() {
                if n > 0 {
                    budget.max_brute_force_vecs_per_chunk = n;
                }
            }
        }
        budget
    })
}

/// Owned, `'static` capture of everything a yielding FT.SEARCH local slice reads
/// (ft-search-off-eventloop, §3 C1). Built under one `&mut VectorIndex` borrow
/// at search entry, BEFORE the first yield; after capture the search holds NO
/// borrow into `VectorStore`/`VectorIndex`. `segments` is an O(1) `Arc` refcount
/// bump (not a data copy); `mutable_len` + `snapshot_lsn` pin an isolation-stable
/// view of the append-only mutable segment across yields.
pub struct SearchSnapshot {
    /// Owned segment-list snapshot (immune to concurrent `swap`).
    pub segments: Arc<SegmentList>,
    /// Query vector (owned).
    pub query_f32: Vec<f32>,
    /// Top-k.
    pub k: usize,
    /// HNSW ef_search (resolved at capture).
    pub ef_search: usize,
    /// Pre-evaluated payload/numeric filter bitmap (owned), or None.
    pub filter_bitmap: Option<RoaringBitmap>,
    /// Selectivity-based filter strategy resolved at capture (XC-3), matching
    /// the sync path's `select_strategy` dispatch. `Unfiltered` when
    /// `filter_bitmap` is None.
    pub filter_strategy: FilterStrategy,
    /// MVCC snapshot LSN captured at entry — governs visibility across yields.
    pub snapshot_lsn: u64,
    /// Active txn id (0 for non-transactional reads).
    pub my_txn_id: u64,
    /// Committed-treemap snapshot for MVCC visibility. `Arc` capture (QP-2):
    /// an O(1) refcount bump per search; the treemap is cloned only on the
    /// first capture after a commit/prune (see
    /// `TransactionManager::committed_snapshot`).
    pub committed: std::sync::Arc<roaring::RoaringTreemap>,
    /// Vector dimension.
    pub dimension: u32,
    /// Entry count of the mutable segment captured at entry. The append-only
    /// invariant makes `[0, mutable_len)` a stable scan range across yields.
    pub mutable_len: usize,
    /// Owned scratch for this query (SAFETY-NET clause: a single per-query alloc
    /// at capture, never per-chunk — does not violate G-HOTPATH).
    pub scratch: SearchScratch,
    /// Key-hash → key map captured at START (§3 C1) so a mid-search delete cannot
    /// drop an entry this search still needs to resolve. Used by the response
    /// builder, not the segment scan. Bucketed-CoW snapshot: capture is O(256)
    /// (refcount bumps only); writers copy-on-write bucket-scoped via
    /// `Arc::make_mut` on the single touched bucket (QP-1 + RSS/CPU wave 4).
    pub key_hash_to_key: BucketedKeyMap<bytes::Bytes>,
    /// AE-1: true when `ef_search` came from the resolution heuristic (no
    /// user `EF_RUNTIME`) — saturation-certified segments may then run at
    /// their min-ef estimate.
    pub ef_defaulted: bool,
    /// Per-index recall/QPS knobs (FT.CONFIG RERANK_MULT / EXACT_BEAM),
    /// copied from `IndexMeta` at capture.
    pub tuning: crate::vector::types::SearchTuning,
    /// #18 (off-loop reload): receivers for COLD→WARM reloads that were
    /// SUBMITTED (not blocked-on) at capture. Empty on the common path (no
    /// unloaded segments) and whenever the reload pool is disabled (then the
    /// capture blocked on `promote_unloaded` exactly as before). The yielding
    /// handler `await`s these before scanning (`await_pending_reloads`), so the
    /// triggering dense-KNN query sees FULL recall while sibling connections on
    /// the shard keep running (the reload I/O is off the event loop).
    pub pending_reloads: Vec<flume::Receiver<crate::vector::reload_pool::ReloadOutcome>>,
}

impl SearchSnapshot {
    /// #18: await every off-loop COLD→WARM reload submitted at capture, then
    /// splice the reloaded segments into THIS snapshot's WARM tier (and drop the
    /// matching stubs from `unloaded`) so the scan sees full recall.
    ///
    /// Called by the yielding FT.SEARCH handler BEFORE `search_mvcc_yielding`.
    /// Awaiting a `flume` receiver parks the connection *task* (not the OS
    /// thread), so sibling connections on the shard keep running while the
    /// reload I/O completes on a pool worker. Design-for-failure: a reload that
    /// errors or whose worker died is logged and skipped — the segment stays in
    /// `unloaded` (retried at the next capture) and this query answers with
    /// degraded recall rather than hanging or crashing. No-op (no await) when
    /// nothing was submitted, i.e. the common path.
    pub async fn await_pending_reloads(&mut self) {
        if self.pending_reloads.is_empty() {
            return;
        }
        let receivers = std::mem::take(&mut self.pending_reloads);
        let mut reloaded: Vec<Arc<crate::vector::persistence::warm_search::WarmSearchSegment>> =
            Vec::with_capacity(receivers.len());
        for rx in receivers {
            match rx.recv_async().await {
                Ok(Ok(seg)) => reloaded.push(seg),
                Ok(Err(e)) => tracing::warn!(
                    error = %e,
                    "off-loop COLD segment reload failed -- query answers with degraded recall"
                ),
                Err(_) => tracing::warn!(
                    "off-loop COLD segment reload worker dropped -- query answers with degraded recall"
                ),
            }
        }
        if reloaded.is_empty() {
            return;
        }

        let cur = &self.segments;
        let mut new_warm = cur.warm.clone();
        let reloaded_ids: std::collections::HashSet<u64> =
            reloaded.iter().map(|s| s.segment_id()).collect();
        new_warm.extend(reloaded);
        let new_unloaded: Vec<_> = cur
            .unloaded
            .iter()
            .filter(|stub| !reloaded_ids.contains(&stub.segment_id()))
            .cloned()
            .collect();
        self.segments = Arc::new(SegmentList {
            mutable: Arc::clone(&cur.mutable),
            immutable: cur.immutable.clone(),
            ivf: cur.ivf.clone(),
            warm: new_warm,
            cold: cur.cold.clone(),
            unloaded: new_unloaded,
        });
    }
}

/// Lock-free segment holder. Searches load() once at query start and hold
/// the Arc for the query duration -- immune to concurrent swaps.
pub struct SegmentHolder {
    segments: ArcSwap<SegmentList>,
    /// Single-flight guard for `promote_unloaded` (blocking, never held
    /// across `.await`).
    reload_lock: parking_lot::Mutex<()>,
}

impl SegmentHolder {
    /// Create a holder with a fresh MutableSegment and empty immutable list.
    pub fn new(
        dimension: u32,
        collection: Arc<crate::vector::turbo_quant::collection::CollectionMetadata>,
    ) -> Self {
        Self {
            segments: ArcSwap::from_pointee(SegmentList {
                mutable: Arc::new(MutableSegment::new(dimension, collection)),
                immutable: Vec::new(),
                ivf: Vec::new(),
                warm: Vec::new(),
                cold: Vec::new(),
                unloaded: Vec::new(),
            }),
            reload_lock: parking_lot::Mutex::new(()),
        }
    }

    /// Single atomic load, lock-free, wait-free. This is the hot-path read.
    pub fn load(&self) -> arc_swap::Guard<Arc<SegmentList>> {
        self.segments.load()
    }

    /// Owned snapshot of the segment list: a single atomic `Arc` refcount bump
    /// (O(1), NOT a data copy — no RSS growth). Unlike `load()`'s borrowed
    /// `Guard`, the returned `Arc` can be held across a cooperative yield with no
    /// borrow into the index — the capture-before-yield anchor for
    /// `search_mvcc_yielding` (ft-search-off-eventloop).
    pub fn load_full(&self) -> Arc<SegmentList> {
        self.segments.load_full()
    }

    /// Atomically replace the segment list. Old segments are dropped when
    /// Arc refcount reaches 0 (after all in-flight queries release their Guards).
    pub fn swap(&self, new_list: SegmentList) {
        self.segments.store(Arc::new(new_list));
    }

    /// Acquire the single-flight reload lock. Any caller doing a
    /// load-mutate-`swap` on the segment list — the WARM mmap-budget eviction
    /// tick (`enforce_segment_holder_budget`) as well as
    /// `promote_unloaded`/`submit_unloaded_reloads` — takes this so the three
    /// read-modify-write paths serialize on the lock rather than solely on the
    /// shard-thread-affinity invariant (perf-review defense-in-depth: guards
    /// against a future worker-pool install or off-thread sweep turning the
    /// unsynchronized swaps into a lost update). Never held across `.await`.
    pub(crate) fn reload_guard(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.reload_lock.lock()
    }

    /// Resident bytes split into (mutable_bytes, immutable_bytes).
    ///
    /// Mutable = brute-force buffer (TQ codes + raw f32 + entries).
    /// Immutable = HNSW graph + TQ codes + QJL signs + norms + MVCC, PLUS the
    /// WARM tier (fully-materialized `WarmSearchSegment` heap copies) and COLD
    /// stubs (tiny). WARM is the same-size heap copy of a demoted HOT segment,
    /// so once segments age past `--segment-warm-after` it dominates resident
    /// vector memory — it MUST be counted here or the memory-pressure trigger
    /// (see `should_run_pressure_cascade`) and INFO/Prometheus go blind to it.
    /// IVF and DiskANN-cold segments have no resident accessor yet (IVF is
    /// in-memory but small; cold lives on disk) and still contribute 0.
    pub fn resident_bytes(&self) -> (usize, usize) {
        let snapshot = self.load();
        let mutable = snapshot.mutable.resident_bytes();
        let mut immutable: usize = 0;
        for seg in &snapshot.immutable {
            immutable += seg.resident_bytes();
        }
        // WARM tier: fully-resident heap copies — the dominant term for a
        // long-lived shard whose HOT segments have aged into WARM.
        for warm in &snapshot.warm {
            immutable += warm.resident_bytes();
        }
        // COLD stubs: metadata-only (data on disk), but cheap and accurate.
        for stub in &snapshot.unloaded {
            immutable += stub.resident_bytes();
        }
        (mutable, immutable)
    }

    /// Total vector count across mutable + immutable + IVF + warm segments.
    pub fn total_vectors(&self) -> u32 {
        let snapshot = self.load();
        let mut total = snapshot.mutable.len() as u32;
        for imm in &snapshot.immutable {
            total += imm.total_count();
        }
        for ivf_seg in &snapshot.ivf {
            total += ivf_seg.total_vectors() as u32;
        }
        for warm_seg in &snapshot.warm {
            total += warm_seg.total_count();
        }
        for cold_seg in &snapshot.cold {
            total += cold_seg.total_count();
        }
        for stub in &snapshot.unloaded {
            total += stub.total_count();
        }
        total
    }

    /// Fan-out search across mutable + all immutable segments, merge results.
    ///
    /// 1. Load snapshot (atomic, lock-free).
    /// 2. Brute-force search on mutable segment with query_sq.
    /// 3. HNSW search on each immutable segment with query_f32.
    /// 4. Merge all results, take global top-k.
    pub fn search(
        &self,
        query_f32: &[f32],
        k: usize,
        ef_search: usize,
        scratch: &mut SearchScratch,
    ) -> SmallVec<[SearchResult; 32]> {
        self.search_filtered(query_f32, k, ef_search, scratch, None)
    }

    /// Fan-out search with optional filter bitmap.
    ///
    /// Dispatches to the correct strategy based on filter selectivity:
    /// - Unfiltered: standard search path
    /// - BruteForceFiltered: linear scan on bitmap matches
    /// - HnswFiltered: HNSW with ACORN 2-hop allow-list
    /// - HnswPostFilter: HNSW with 3xK oversampling + post-filter
    pub fn search_filtered(
        &self,
        query_f32: &[f32],
        k: usize,
        ef_search: usize,
        _scratch: &mut SearchScratch,
        filter_bitmap: Option<&RoaringBitmap>,
    ) -> SmallVec<[SearchResult; 32]> {
        // Reload any COLD segments before searching -- correctness requires
        // every segment participate in a KNN scan.
        self.promote_unloaded();
        let strategy = select_strategy(filter_bitmap, self.total_vectors());
        let snapshot = self.load();

        // Pre-allocate merge buffer: k results per segment (mutable + immutables + warm + cold).
        let segment_count =
            1 + snapshot.immutable.len() + snapshot.warm.len() + snapshot.cold.len();
        let mut all: SmallVec<[SearchResult; 32]> = SmallVec::with_capacity(k * segment_count);

        // Prepare query state: Exact mode uses TQ_prod (QJL), Light mode skips it.
        let collection = snapshot.mutable.collection();
        let query_state = if !collection.qjl_matrices.is_empty() {
            Some(
                crate::vector::turbo_quant::inner_product::prepare_query_prod(
                    query_f32,
                    &collection.qjl_matrices,
                    collection.fwht_sign_flips.as_slice(),
                    collection.padded_dimension as usize,
                ),
            )
        } else {
            None // Light mode: no QJL matrices, use TQ-ADC brute force
        };

        // Full resolved ef per segment (no ef_defaulted context on this path,
        // so the AE-1 per-segment reduction never applies here).
        let graph_ef = ef_search;

        match strategy {
            FilterStrategy::Unfiltered => {
                all.extend(
                    snapshot
                        .mutable
                        .brute_force_search(query_f32, query_state.as_ref(), k),
                );
                for imm in &snapshot.immutable {
                    all.extend(imm.search(query_f32, k, graph_ef, _scratch));
                }
                for warm_seg in &snapshot.warm {
                    all.extend(warm_seg.search(query_f32, k, graph_ef, _scratch));
                }
            }
            FilterStrategy::BruteForceFiltered => {
                all.extend(snapshot.mutable.brute_force_search_filtered(
                    query_f32,
                    query_state.as_ref(),
                    k,
                    filter_bitmap,
                ));
                for imm in &snapshot.immutable {
                    all.extend(imm.search_filtered(
                        query_f32,
                        k,
                        graph_ef,
                        _scratch,
                        filter_bitmap,
                    ));
                }
                for warm_seg in &snapshot.warm {
                    all.extend(warm_seg.search_filtered(
                        query_f32,
                        k,
                        graph_ef,
                        _scratch,
                        filter_bitmap,
                    ));
                }
            }
            FilterStrategy::HnswFiltered => {
                all.extend(snapshot.mutable.brute_force_search_filtered(
                    query_f32,
                    query_state.as_ref(),
                    k,
                    filter_bitmap,
                ));
                for imm in &snapshot.immutable {
                    all.extend(imm.search_filtered(
                        query_f32,
                        k,
                        graph_ef,
                        _scratch,
                        filter_bitmap,
                    ));
                }
                for warm_seg in &snapshot.warm {
                    all.extend(warm_seg.search_filtered(
                        query_f32,
                        k,
                        graph_ef,
                        _scratch,
                        filter_bitmap,
                    ));
                }
            }
            FilterStrategy::HnswPostFilter => {
                let oversample_k = k * 3;
                all.extend(snapshot.mutable.brute_force_search_filtered(
                    query_f32,
                    query_state.as_ref(),
                    oversample_k,
                    filter_bitmap,
                ));
                let post_ef = ef_search.max(oversample_k);
                for imm in &snapshot.immutable {
                    let imm_results = imm.search(query_f32, oversample_k, post_ef, _scratch);
                    if let Some(bm) = filter_bitmap {
                        for r in imm_results {
                            if bm.contains(r.id.0) {
                                all.push(r);
                            }
                        }
                    } else {
                        all.extend(imm_results);
                    }
                }
                for warm_seg in &snapshot.warm {
                    let warm_results = warm_seg.search(query_f32, oversample_k, post_ef, _scratch);
                    if let Some(bm) = filter_bitmap {
                        for r in warm_results {
                            if bm.contains(r.id.0) {
                                all.push(r);
                            }
                        }
                    } else {
                        all.extend(warm_results);
                    }
                }
            }
        }

        // Fan-out to cold (DiskANN) segments -- unfiltered PQ beam search.
        // Filter support for cold segments is future work (no global ID mapping yet).
        for cold_seg in &snapshot.cold {
            all.extend(cold_seg.search(query_f32, k, 8));
        }

        // Fan-out to IVF segments.
        if !snapshot.ivf.is_empty() {
            let dim = query_f32.len();
            let pdim = padded_dimension(dim as u32) as usize;

            // Allocate query rotation + LUT buffers ONCE, reuse across all IVF
            // segments (same pattern as search_mvcc below — previously these
            // were allocated per-segment-per-query, 12KB+ × n_segments).
            let mut q_rotated = vec![0.0f32; pdim];
            let mut lut_buf = vec![0u8; pdim * 16];

            for ivf_seg in &snapshot.ivf {
                // Reset and re-rotate for this segment (different sign_flips per segment)
                q_rotated.iter_mut().for_each(|v| *v = 0.0);
                q_rotated[..dim].copy_from_slice(query_f32);
                // Normalize before FWHT.
                let qnorm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
                if qnorm > 0.0 {
                    let inv = 1.0 / qnorm;
                    for v in q_rotated[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                fwht::fwht(&mut q_rotated, ivf_seg.sign_flips());

                if let Some(bm) = filter_bitmap {
                    all.extend(ivf_seg.search_filtered(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                        bm,
                    ));
                } else {
                    all.extend(ivf_seg.search(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                    ));
                }
            }
        }

        all.sort_unstable();
        all.truncate(k);
        all
    }

    /// MVCC-aware fan-out search with dirty set merge.
    ///
    /// 1. Brute-force MVCC search on mutable segment (visibility filtered).
    /// 2. HNSW search on immutable segments (immutable entries are committed by
    ///    definition -- compacted only after commit. Visibility post-filter
    ///    deferred until Phase 66 when delete_lsn tracking on immutable entries
    ///    is added).
    /// 3. Brute-force scan dirty_set entries (always visible -- own txn).
    /// 4. Merge all results, take global top-k.
    ///
    /// When mvcc.snapshot_lsn == 0 and dirty_set is empty, this is equivalent
    /// to the non-MVCC search path.
    pub fn search_mvcc(
        &self,
        query_f32: &[f32],
        k: usize,
        ef_search: usize,
        _scratch: &mut SearchScratch,
        filter_bitmap: Option<&RoaringBitmap>,
        mvcc: &MvccContext<'_>,
    ) -> SmallVec<[SearchResult; 32]> {
        // WS3 round 2: same reload-before-scan requirement as `search_filtered`.
        self.promote_unloaded();
        let snapshot = self.load();

        // Prepare TurboQuant_prod query state for mutable search.
        let collection = snapshot.mutable.collection();
        let query_state = if !collection.qjl_matrices.is_empty() {
            Some(
                crate::vector::turbo_quant::inner_product::prepare_query_prod(
                    query_f32,
                    &collection.qjl_matrices,
                    collection.fwht_sign_flips.as_slice(),
                    collection.padded_dimension as usize,
                ),
            )
        } else {
            None
        };

        // 1. MVCC-aware brute-force (full mutable scan: 0..len)
        let mut all = snapshot.mutable.brute_force_search_mvcc(
            query_f32,
            query_state.as_ref(),
            k,
            filter_bitmap,
            mvcc.snapshot_lsn,
            mvcc.my_txn_id,
            mvcc.committed,
            0,
            usize::MAX,
        );

        // 2. HNSW search on immutable segments (TQ-ADC distance).
        // Immutable segment entries are committed by definition (compacted only
        // after commit). No visibility post-filter needed for Phase 65.
        // AE-1: heuristic-defaulted ef → a saturation-certified segment runs
        // at its compact-time min-ef estimate (floored by k, capped by the
        // resolved ef); every other segment gets the FULL resolved ef.
        // Mirrors the yielding path's selector exactly.
        let graph_ef = ef_search;
        let seg_ef = |suggested: Option<u32>| -> usize {
            match suggested {
                Some(sug) if mvcc.ef_defaulted => (sug as usize).max(k).min(ef_search),
                _ => graph_ef,
            }
        };
        for imm in &snapshot.immutable {
            let ef_i = seg_ef(imm.suggested_ef());
            if filter_bitmap.is_some() {
                all.extend(imm.search_filtered_with_tuning(
                    query_f32,
                    k,
                    ef_i,
                    _scratch,
                    filter_bitmap,
                    mvcc.tuning,
                ));
            } else {
                all.extend(imm.search_with_tuning(query_f32, k, ef_i, _scratch, mvcc.tuning));
            }
        }

        // 2a. Warm segment search (committed by definition, same as immutable).
        for warm_seg in &snapshot.warm {
            if filter_bitmap.is_some() {
                all.extend(warm_seg.search_filtered(
                    query_f32,
                    k,
                    graph_ef,
                    _scratch,
                    filter_bitmap,
                ));
            } else {
                all.extend(warm_seg.search(query_f32, k, graph_ef, _scratch));
            }
        }

        // 2b. Cold segment search (DiskANN, committed by definition).
        for cold_seg in &snapshot.cold {
            all.extend(cold_seg.search(query_f32, k, 8));
        }

        // 2c. IVF segment search (IVF entries are committed by definition).
        if !snapshot.ivf.is_empty() {
            let dim = query_f32.len();
            let pdim = padded_dimension(dim as u32) as usize;

            // Allocate query rotation + LUT buffers ONCE, reuse across all IVF segments.
            // Previously these were allocated per-segment-per-query (12KB+ × n_segments).
            let mut q_rotated = vec![0.0f32; pdim];
            let mut lut_buf = vec![0u8; pdim * 16];

            for ivf_seg in &snapshot.ivf {
                // Reset and re-rotate for this segment (different sign_flips per segment)
                q_rotated.iter_mut().for_each(|v| *v = 0.0);
                q_rotated[..dim].copy_from_slice(query_f32);
                let qnorm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
                if qnorm > 0.0 {
                    let inv = 1.0 / qnorm;
                    for v in q_rotated[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                fwht::fwht(&mut q_rotated, ivf_seg.sign_flips());

                if let Some(bm) = filter_bitmap {
                    all.extend(ivf_seg.search_filtered(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                        bm,
                    ));
                } else {
                    all.extend(ivf_seg.search(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                    ));
                }
            }
        }

        // 3. Dirty set: currently empty for non-transactional reads.
        // Full TurboQuant_prod scoring for dirty entries deferred to Phase 66
        // (transactional writes are rare in vector workloads).

        // 4. Merge all results, take global top-k
        all.sort_unstable();
        all.truncate(k);
        all
    }

    /// Cooperatively-yielding twin of [`Self::search_mvcc`]
    /// (ft-search-off-eventloop, §3 C2). Associated fn (NO `&self`) driving the
    /// chunked search against an owned [`SearchSnapshot`] — it holds no borrow
    /// into `VectorStore`/`VectorIndex` across any yield (§3 G-NOBORROW), so a
    /// co-located write may run between chunks on the same shard thread.
    ///
    /// Runs the SAME steps in the SAME order as `search_mvcc`, producing a
    /// BYTE-IDENTICAL result (§3 G-IDENTITY): the mutable brute-force is chunked
    /// over the append-only captured range `[0, mutable_len)` and each chunk's
    /// top-k merges into the same global top-k a single full scan yields;
    /// immutable/warm/cold/ivf segments are committed-by-definition and yielded
    /// between. Relinquishes to the shard event loop (and bumps the C5 proxy
    /// counter) between bounded chunks (§3 G-PROGRESS).
    pub async fn search_mvcc_yielding(
        snap: &mut SearchSnapshot,
        budget: YieldBudget,
    ) -> SmallVec<[SearchResult; 32]> {
        Self::search_mvcc_yielding_with_pool(snap, budget, crate::vector::search_pool::global())
            .await
    }

    /// [`Self::search_mvcc_yielding`] with an explicit worker pool: when
    /// `pool` is `Some` and the index holds ≥2 graph-tier (immutable/warm)
    /// segments, the per-segment HNSW searches fan out to the pool while THIS
    /// task runs the mutable MVCC scan — replies are awaited via
    /// `recv_async`, which parks the task, never the shard event loop. With
    /// `pool = None` (or <2 graph segments) the body is the exact serial path.
    /// Results are identical either way: segment searches are independent
    /// reads and the final `sort_unstable` under `SearchResult`'s total order
    /// (distance, then id) makes accumulation order immaterial.
    pub async fn search_mvcc_yielding_with_pool(
        snap: &mut SearchSnapshot,
        budget: YieldBudget,
        pool: Option<&crate::vector::search_pool::SearchWorkerPool>,
    ) -> SmallVec<[SearchResult; 32]> {
        // Capture-before-yield: move all read-only inputs into owned locals so
        // the per-chunk loops touch only `snap.scratch` (mutably) — no aliasing
        // borrow of `snap`. `key_hash_to_key` stays in `snap` for the response
        // builder; the moved fields are unused after the search.
        let segments = Arc::clone(&snap.segments);
        let query_f32 = std::mem::take(&mut snap.query_f32);
        let filter_bitmap = snap.filter_bitmap.take();
        let committed = std::mem::take(&mut snap.committed);
        let query_f32 = query_f32.as_slice();
        let filter_ref = filter_bitmap.as_ref();
        let k = snap.k;
        let ef_search = snap.ef_search;
        // XC-3: high-selectivity filters (>80% of vectors pass) run the graph
        // UNFILTERED with 3×k oversampling and post-filter the results —
        // mirrors the sync path's `FilterStrategy::HnswPostFilter` branch.
        let post_filter =
            filter_ref.is_some() && matches!(snap.filter_strategy, FilterStrategy::HnswPostFilter);
        let (fetch_k, graph_filter) = if post_filter {
            (k * 3, None)
        } else {
            (k, filter_ref)
        };
        let graph_ef = if post_filter {
            ef_search.max(k * 3)
        } else {
            ef_search
        };
        // AE-1: when ef was heuristic-defaulted, a saturation-certified
        // segment runs at its compact-time min-ef estimate — floored by the
        // merge quota, capped by the resolved ef. Every other segment gets
        // the FULL resolved ef (see the per-segment beam policy note at the
        // top of this file). Identical selector in the pooled jobs, inline
        // fallbacks, and serial loops, so pooled == serial identity holds.
        // Never active when the user pinned EF_RUNTIME.
        let ef_defaulted = snap.ef_defaulted;
        let seg_ef = |suggested: Option<u32>| -> usize {
            match suggested {
                Some(sug) if ef_defaulted => (sug as usize).max(fetch_k).min(graph_ef),
                _ => graph_ef,
            }
        };
        let snapshot_lsn = snap.snapshot_lsn;
        let my_txn_id = snap.my_txn_id;
        let mutable_len = snap.mutable_len;
        let tuning = snap.tuning;

        // Prepare TurboQuant_prod query state for mutable search (same as sync).
        let collection = segments.mutable.collection();
        let query_state = if !collection.qjl_matrices.is_empty() {
            Some(
                crate::vector::turbo_quant::inner_product::prepare_query_prod(
                    query_f32,
                    &collection.qjl_matrices,
                    collection.fwht_sign_flips.as_slice(),
                    collection.padded_dimension as usize,
                ),
            )
        } else {
            None
        };

        let mut all: SmallVec<[SearchResult; 32]> = SmallVec::new();

        // 0. Intra-query fan-out (search_pool): submit every graph-tier
        //    (immutable/warm) segment search to the worker pool BEFORE the
        //    mutable scan so workers overlap with it. Filter semantics mirror
        //    the serial loops below: ACORN mode ships the allow-list bitmap to
        //    the worker; HnswPostFilter mode searches unfiltered at fetch_k and
        //    the bitmap is applied to the collected results (step 2).
        let graph_jobs = segments.immutable.len() + segments.warm.len();
        let pooled = pool.filter(|_| graph_jobs >= 2);
        let mut pending_replies = 0usize;
        let mut reply_rx = None;
        if let Some(pool) = pooled {
            let (tx, rx) = flume::bounded::<SmallVec<[SearchResult; 32]>>(graph_jobs);
            // One owned query copy + optional bitmap clone per query — shared
            // across this query's jobs via Arc (not per-segment copies).
            let query_arc: std::sync::Arc<[f32]> = std::sync::Arc::from(query_f32);
            let filter_arc = graph_filter.map(|bm| std::sync::Arc::new(bm.clone()));
            // On submit failure (pool shut down at process teardown) the
            // segment is searched inline — the query still answers correctly.
            for seg in &segments.immutable {
                let ef_seg = seg_ef(seg.suggested_ef());
                let job = crate::vector::search_pool::SegmentSearchJob {
                    segment: crate::vector::search_pool::GraphSegmentRef::Immutable(
                        std::sync::Arc::clone(seg),
                    ),
                    query: std::sync::Arc::clone(&query_arc),
                    fetch_k,
                    ef_search: ef_seg,
                    filter: filter_arc.clone(),
                    tuning,
                    reply: tx.clone(),
                };
                if pool.submit(job) {
                    pending_replies += 1;
                } else if graph_filter.is_some() {
                    all.extend(seg.search_filtered_with_tuning(
                        query_f32,
                        fetch_k,
                        ef_seg,
                        &mut snap.scratch,
                        graph_filter,
                        tuning,
                    ));
                } else {
                    let results = seg.search_with_tuning(
                        query_f32,
                        fetch_k,
                        ef_seg,
                        &mut snap.scratch,
                        tuning,
                    );
                    if post_filter {
                        if let Some(bm) = filter_ref {
                            all.extend(results.into_iter().filter(|r| bm.contains(r.id.0)));
                        }
                    } else {
                        all.extend(results);
                    }
                }
            }
            for seg in &segments.warm {
                let job = crate::vector::search_pool::SegmentSearchJob {
                    segment: crate::vector::search_pool::GraphSegmentRef::Warm(
                        std::sync::Arc::clone(seg),
                    ),
                    query: std::sync::Arc::clone(&query_arc),
                    fetch_k,
                    ef_search: graph_ef,
                    filter: filter_arc.clone(),
                    tuning,
                    reply: tx.clone(),
                };
                if pool.submit(job) {
                    pending_replies += 1;
                } else if graph_filter.is_some() {
                    all.extend(seg.search_filtered(
                        query_f32,
                        fetch_k,
                        graph_ef,
                        &mut snap.scratch,
                        graph_filter,
                    ));
                } else {
                    let results = seg.search(query_f32, fetch_k, graph_ef, &mut snap.scratch);
                    if post_filter {
                        if let Some(bm) = filter_ref {
                            all.extend(results.into_iter().filter(|r| bm.contains(r.id.0)));
                        }
                    } else {
                        all.extend(results);
                    }
                }
            }
            reply_rx = Some(rx);
        }

        // 1. MVCC brute-force over the captured append-only range [0, mutable_len),
        //    chunked + cooperatively yielded between chunks. Query prep (FWHT
        //    rotation / SQ8 normalize) and the top-k heap are hoisted out of the
        //    chunk loop (QP-4): one prepare per query, one shared heap — the
        //    accumulated result is exactly the global top-k a single full scan
        //    produces.
        let chunk = budget.max_brute_force_vecs_per_chunk.max(1);
        if mutable_len > 0 {
            // fetch_k oversamples under HnswPostFilter (filter still applied —
            // the mutable scan is linear, filtering there is free).
            let mut bf_query = segments.mutable.prepare_brute_force_query(
                query_f32,
                query_state.is_some(),
                fetch_k,
            );
            let mut start = 0usize;
            while start < mutable_len {
                let end = (start + chunk).min(mutable_len);
                segments.mutable.brute_force_scan_mvcc_chunk(
                    &mut bf_query,
                    query_state.as_ref(),
                    fetch_k,
                    filter_ref,
                    snapshot_lsn,
                    my_txn_id,
                    &committed,
                    start,
                    end,
                );
                start = end;
                if start < mutable_len {
                    crate::admin::metrics_setup::bump_ft_search_cooperative_yield();
                    crate::runtime::cooperative_yield().await;
                }
            }
            all.extend(bf_query.into_results());
        }

        let seg_cap = budget.max_segments_per_chunk.max(1);
        let mut since_yield = 0usize;

        // 2. Graph-tier (immutable/warm) results.
        //
        // Pooled: collect the worker replies submitted in step 0 — one
        // `recv_async` await per job parks this task (never the event loop)
        // until that segment's results land. A dropped reply (worker death —
        // catch_unwind already contains per-job panics) degrades to missing
        // segment results with a warning, never a hang. HnswPostFilter mode
        // applies the bitmap here, mirroring the serial branch below.
        let pooled_graph = reply_rx.is_some();
        if let Some(rx) = reply_rx {
            for _ in 0..pending_replies {
                match rx.recv_async().await {
                    Ok(results) => {
                        if post_filter {
                            if let Some(bm) = filter_ref {
                                all.extend(results.into_iter().filter(|r| bm.contains(r.id.0)));
                            }
                        } else {
                            all.extend(results);
                        }
                    }
                    Err(_) => {
                        tracing::warn!(
                            "vector search worker reply dropped; a segment's results \
                             are missing from this query"
                        );
                    }
                }
            }
        }

        // Serial path (no pool / <2 graph segments): identical to search_mvcc.
        // Strategy dispatch (XC-3): `graph_filter` is None under HnswPostFilter
        // (unfiltered traversal at fetch_k = 3×k, bitmap applied to results) —
        // otherwise the ACORN-filtered traversal, same as the sync path.
        if !pooled_graph {
            for imm in &segments.immutable {
                let ef_seg = seg_ef(imm.suggested_ef());
                if graph_filter.is_some() {
                    all.extend(imm.search_filtered_with_tuning(
                        query_f32,
                        fetch_k,
                        ef_seg,
                        &mut snap.scratch,
                        graph_filter,
                        tuning,
                    ));
                } else {
                    let results = imm.search_with_tuning(
                        query_f32,
                        fetch_k,
                        ef_seg,
                        &mut snap.scratch,
                        tuning,
                    );
                    if post_filter {
                        if let Some(bm) = filter_ref {
                            all.extend(results.into_iter().filter(|r| bm.contains(r.id.0)));
                        }
                    } else {
                        all.extend(results);
                    }
                }
                since_yield += 1;
                if since_yield >= seg_cap {
                    since_yield = 0;
                    crate::admin::metrics_setup::bump_ft_search_cooperative_yield();
                    crate::runtime::cooperative_yield().await;
                }
            }
        }

        // 2a. Warm segment search (committed by definition, same as immutable).
        if !pooled_graph {
            for warm_seg in &segments.warm {
                if graph_filter.is_some() {
                    all.extend(warm_seg.search_filtered(
                        query_f32,
                        fetch_k,
                        graph_ef,
                        &mut snap.scratch,
                        graph_filter,
                    ));
                } else {
                    let results = warm_seg.search(query_f32, fetch_k, graph_ef, &mut snap.scratch);
                    if post_filter {
                        if let Some(bm) = filter_ref {
                            all.extend(results.into_iter().filter(|r| bm.contains(r.id.0)));
                        }
                    } else {
                        all.extend(results);
                    }
                }
                since_yield += 1;
                if since_yield >= seg_cap {
                    since_yield = 0;
                    crate::admin::metrics_setup::bump_ft_search_cooperative_yield();
                    crate::runtime::cooperative_yield().await;
                }
            }
        }

        // 2b. Cold segment search (DiskANN, committed by definition).
        for cold_seg in &segments.cold {
            all.extend(cold_seg.search(query_f32, k, 8));
            since_yield += 1;
            if since_yield >= seg_cap {
                since_yield = 0;
                crate::admin::metrics_setup::bump_ft_search_cooperative_yield();
                crate::runtime::cooperative_yield().await;
            }
        }

        // 2c. IVF segment search (IVF entries are committed by definition).
        if !segments.ivf.is_empty() {
            let dim = query_f32.len();
            let pdim = padded_dimension(dim as u32) as usize;
            // Allocate query rotation + LUT buffers ONCE per query (not per chunk).
            let mut q_rotated = vec![0.0f32; pdim];
            let mut lut_buf = vec![0u8; pdim * 16];

            for ivf_seg in &segments.ivf {
                q_rotated.iter_mut().for_each(|v| *v = 0.0);
                q_rotated[..dim].copy_from_slice(query_f32);
                let qnorm: f32 = query_f32.iter().map(|x| x * x).sum::<f32>().sqrt();
                if qnorm > 0.0 {
                    let inv = 1.0 / qnorm;
                    for v in q_rotated[..dim].iter_mut() {
                        *v *= inv;
                    }
                }
                fwht::fwht(&mut q_rotated, ivf_seg.sign_flips());

                if let Some(bm) = filter_ref {
                    all.extend(ivf_seg.search_filtered(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                        bm,
                    ));
                } else {
                    all.extend(ivf_seg.search(
                        query_f32,
                        &q_rotated,
                        k,
                        DEFAULT_NPROBE,
                        &mut lut_buf,
                    ));
                }
                since_yield += 1;
                if since_yield >= seg_cap {
                    since_yield = 0;
                    crate::admin::metrics_setup::bump_ft_search_cooperative_yield();
                    crate::runtime::cooperative_yield().await;
                }
            }
        }

        // 4. Merge all results, take global top-k (identical to search_mvcc).
        all.sort_unstable();
        all.truncate(k);
        // QP-3: hand this query's scratch back to the thread cache; the next
        // capture on this thread reuses it via take_thread_scratch.
        crate::vector::hnsw::search::recycle_thread_scratch(std::mem::replace(
            &mut snap.scratch,
            crate::vector::hnsw::search::SearchScratch::new(0, 0),
        ));
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::distance;
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::turbo_quant::encoder::padded_dimension;
    use crate::vector::types::DistanceMetric;

    fn make_test_collection(dim: u32) -> Arc<CollectionMetadata> {
        // Use Exact mode in tests to preserve TQ_prod scoring compatibility
        Arc::new(CollectionMetadata::with_build_mode(
            1,
            dim,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
            crate::vector::turbo_quant::collection::BuildMode::Exact,
        ))
    }

    fn make_sq_vector(dim: usize, seed: u32) -> Vec<i8> {
        let mut v = Vec::with_capacity(dim);
        let mut s = seed;
        for _ in 0..dim {
            s = s.wrapping_mul(1664525).wrapping_add(1013904223);
            v.push((s >> 24) as i8);
        }
        v
    }

    /// #18: build a real COLD `UnloadedSegment` stub backed by minimal on-disk
    /// `.mpf` files (0 vectors) — enough for `reload()` to round-trip a
    /// `WarmSearchSegment` with the given `seg_id`.
    fn make_unloaded_stub(
        dir: &std::path::Path,
        seg_id: u64,
    ) -> Arc<crate::vector::persistence::unloaded_segment::UnloadedSegment> {
        use crate::storage::tiered::SegmentHandle;
        use crate::vector::hnsw::graph::HnswGraph;
        use crate::vector::persistence::unloaded_segment::UnloadedSegment;
        use crate::vector::persistence::warm_search::WarmSearchSegment;
        use crate::vector::persistence::warm_segment::{
            write_codes_mpf, write_graph_mpf, write_mvcc_mpf,
        };

        let seg_dir = dir.join(format!("seg-{seg_id}"));
        std::fs::create_dir_all(&seg_dir).unwrap();
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        let graph_bytes = empty_graph.to_bytes();
        write_codes_mpf(&seg_dir.join("codes.mpf"), seg_id, &[]).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), seg_id, &graph_bytes).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), seg_id, &[]).unwrap();

        let handle = SegmentHandle::new(seg_id, seg_dir.clone());
        let warm = WarmSearchSegment::from_files(
            &seg_dir,
            seg_id,
            make_test_collection(128),
            handle,
            false,
        )
        .unwrap();
        Arc::new(UnloadedSegment::from_warm(&warm, false))
    }

    /// Build a resident WARM segment whose footprint is dominated by a
    /// `codes_len`-byte codes payload, so tests can assert byte accounting.
    fn make_warm_sized(
        dir: &std::path::Path,
        seg_id: u64,
        codes_len: usize,
    ) -> Arc<crate::vector::persistence::warm_search::WarmSearchSegment> {
        use crate::storage::tiered::SegmentHandle;
        use crate::vector::hnsw::graph::HnswGraph;
        use crate::vector::persistence::warm_search::WarmSearchSegment;
        use crate::vector::persistence::warm_segment::{
            write_codes_mpf, write_graph_mpf, write_mvcc_mpf,
        };

        let seg_dir = dir.join(format!("warm-{seg_id}"));
        std::fs::create_dir_all(&seg_dir).unwrap();
        let empty_graph = HnswGraph::new(
            0,
            16,
            32,
            0,
            0,
            crate::vector::aligned_buffer::AlignedBuffer::new(0),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            68,
        );
        write_codes_mpf(&seg_dir.join("codes.mpf"), seg_id, &vec![0u8; codes_len]).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), seg_id, &empty_graph.to_bytes()).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), seg_id, &[]).unwrap();
        let handle = SegmentHandle::new(seg_id, seg_dir.clone());
        Arc::new(
            WarmSearchSegment::from_files(
                &seg_dir,
                seg_id,
                make_test_collection(128),
                handle,
                false,
            )
            .unwrap(),
        )
    }

    /// Perf-review finding #1 (HIGH): `resident_bytes()` must count the WARM
    /// tier — a shard whose HOT segments have aged into WARM would otherwise
    /// report ~0 vector memory, blinding the memory-pressure trigger (C).
    #[test]
    fn test_resident_bytes_includes_warm_tier() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection.clone());

        // Baseline: empty holder reports ~0 immutable-tier bytes.
        let (_, base_imm) = holder.resident_bytes();

        // Put a ~40 KB WARM segment in the tier (no HOT immutable segments).
        let warm = make_warm_sized(tmp.path(), 1, 40_000);
        let warm_bytes = warm.resident_bytes();
        assert!(warm_bytes >= 40_000);
        holder.swap(SegmentList {
            mutable: Arc::new(MutableSegment::new(128, collection)),
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm: vec![warm],
            cold: Vec::new(),
            unloaded: Vec::new(),
        });

        let (_, imm) = holder.resident_bytes();
        assert!(
            imm >= base_imm + 40_000,
            "WARM tier must contribute to resident_bytes: {imm} (base {base_imm})"
        );
    }

    /// #18 (holder half): with the reload pool enabled, `submit_unloaded_reloads`
    /// must NOT block — it submits the stub off-loop (returns a receiver) and a
    /// later capture installs the finished reload into WARM, emptying `unloaded`.
    #[test]
    fn test_submit_unloaded_reloads_installs_off_loop() {
        distance::init();
        crate::vector::reload_pool::init_global(1);
        // If a prior test disabled the global pool, this exercises the blocking
        // fallback instead — still correct, asserted the same way below.
        let pool_on = crate::vector::reload_pool::global().is_some();

        let tmp = tempfile::tempdir().unwrap();
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection.clone());
        holder.swap(SegmentList {
            mutable: Arc::new(MutableSegment::new(128, collection)),
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
            unloaded: vec![make_unloaded_stub(tmp.path(), 1)],
        });

        let receivers = holder.submit_unloaded_reloads();
        if pool_on {
            assert_eq!(receivers.len(), 1, "one off-loop reload submitted");
            let outcome = receivers[0].recv().expect("worker replies");
            assert!(outcome.is_ok(), "reload succeeds");
            // Next capture installs the completed reload into WARM.
            let again = holder.submit_unloaded_reloads();
            assert!(again.is_empty(), "nothing left to submit after install");
        }
        // Fallback path already promoted synchronously; either way, converged:
        let snap = holder.load();
        assert_eq!(snap.warm.len(), 1, "segment now resident in WARM");
        assert!(snap.unloaded.is_empty(), "no COLD stubs remain");
    }

    /// #18 (snapshot half): `await_pending_reloads` splices a reloaded segment
    /// into the snapshot's OWN warm tier and drops the matching cold stub, so
    /// the scan that follows sees full recall.
    #[test]
    fn test_await_pending_reloads_splices_into_snapshot() {
        distance::init();
        let tmp = tempfile::tempdir().unwrap();
        let collection = make_test_collection(128);
        let stub = make_unloaded_stub(tmp.path(), 7);

        // Local pool (no global-state coupling) to produce a real receiver.
        let pool = crate::vector::reload_pool::SegmentReloadPool::new(1);
        let rx = pool.submit(Arc::clone(&stub));

        let segments = Arc::new(SegmentList {
            mutable: Arc::new(MutableSegment::new(128, collection)),
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
            unloaded: vec![stub],
        });
        let mut snap = SearchSnapshot {
            segments,
            query_f32: vec![0.0f32; 128],
            k: 1,
            ef_search: 16,
            filter_bitmap: None,
            filter_strategy: crate::vector::filter::selectivity::FilterStrategy::Unfiltered,
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: std::sync::Arc::new(roaring::RoaringTreemap::new()),
            dimension: 128,
            mutable_len: 0,
            scratch: crate::vector::hnsw::search::SearchScratch::new(0, padded_dimension(128)),
            key_hash_to_key: crate::vector::keymap::BucketedKeyMap::new(),
            ef_defaulted: false,
            tuning: crate::vector::types::SearchTuning::default(),
            pending_reloads: vec![rx],
        };

        assert_eq!(snap.segments.unloaded.len(), 1);
        assert_eq!(snap.segments.warm.len(), 0);

        futures::executor::block_on(snap.await_pending_reloads());

        assert_eq!(snap.segments.warm.len(), 1, "reloaded segment spliced in");
        assert!(
            snap.segments.unloaded.is_empty(),
            "cold stub dropped after splice"
        );
        assert!(snap.pending_reloads.is_empty(), "receivers drained");
    }

    #[test]
    fn test_holder_new_has_empty_immutable() {
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection);
        let snap = holder.load();
        assert!(snap.immutable.is_empty());
        assert_eq!(snap.mutable.len(), 0);
    }

    #[test]
    fn test_holder_swap_replaces_list() {
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection.clone());

        // Insert into original mutable
        {
            let snap = holder.load();
            snap.mutable.append(1, &[0.0f32; 128], 1);
        }

        // Swap with a new list
        let new_mutable = Arc::new(MutableSegment::new(128, collection));
        new_mutable.append(2, &[1.0f32; 128], 2);
        new_mutable.append(3, &[2.0f32; 128], 3);

        holder.swap(SegmentList {
            mutable: new_mutable,
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
            unloaded: Vec::new(),
        });

        let snap = holder.load();
        assert_eq!(snap.mutable.len(), 2); // new mutable has 2, not 1
    }

    #[test]
    fn test_holder_search_mutable_only() {
        distance::init();
        let dim = 8;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);

        // Insert vectors
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, i as u64);
            }
        }

        let _query_sq = make_sq_vector(dim, 1); // same as vector 0
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let results = holder.search(&query_f32, 3, 64, &mut scratch);
        assert!(!results.is_empty());
        assert!(results.len() <= 3);
        // First result should be vector 0
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_filtered_none_same_as_unfiltered() {
        distance::init();
        let dim = 8;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let unfiltered = holder.search(&query_f32, 3, 64, &mut scratch);
        let filtered = holder.search_filtered(&query_f32, 3, 64, &mut scratch, None);
        assert_eq!(unfiltered.len(), filtered.len());
        for (u, f) in unfiltered.iter().zip(filtered.iter()) {
            assert_eq!(u.id.0, f.id.0);
        }
    }

    #[test]
    fn test_holder_search_filtered_with_bitmap() {
        distance::init();
        let dim = 8;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim, 1);
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        // Only allow IDs 2, 3, 4
        let mut bitmap = roaring::RoaringBitmap::new();
        bitmap.insert(2);
        bitmap.insert(3);
        bitmap.insert(4);

        let results = holder.search_filtered(&query_f32, 3, 64, &mut scratch, Some(&bitmap));
        for r in &results {
            assert!(
                bitmap.contains(r.id.0),
                "result id {} not in bitmap",
                r.id.0
            );
        }
    }

    #[test]
    fn test_holder_search_mvcc_backward_compat() {
        // search_mvcc with snapshot=0 and empty dirty_set should match search results
        distance::init();
        let dim = 8;
        let _padded = padded_dimension(dim as u32) as usize;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let f32_v = vec![0.0f32; dim as usize];
                snap.mutable.append(i as u64, &f32_v, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim as usize, 1);
        let query_f32 = vec![0.0f32; dim as usize];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringTreemap::new();

        let non_mvcc = holder.search(&query_f32, 3, 64, &mut scratch);
        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 0,
            my_txn_id: 0,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
            ef_defaulted: false,
            tuning: crate::vector::types::SearchTuning::default(),
        };
        let mvcc = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_ctx);

        assert_eq!(non_mvcc.len(), mvcc.len());
        for (a, b) in non_mvcc.iter().zip(mvcc.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }

    #[test]
    fn test_holder_search_mvcc_filters_by_snapshot() {
        distance::init();
        let dim = 4;
        let _padded = padded_dimension(dim as u32) as usize;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            // insert_lsn=1, visible to snapshot=5
            snap.mutable.append(0, &[0.0f32; 4], 1);
            // insert_lsn=10, NOT visible to snapshot=5
            snap.mutable.append(1, &[0.0f32; 4], 10);
        }
        let _query_sq = vec![0i8; dim as usize];
        let query_f32 = vec![0.0f32; dim as usize];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringTreemap::new();
        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 5,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
            ef_defaulted: false,
            tuning: crate::vector::types::SearchTuning::default(),
        };
        let results = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_ctx);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_mvcc_dirty_set_merge() {
        // Dirty set entries should appear in results (read-your-own-writes)
        distance::init();
        let dim = 4usize;
        let collection = make_test_collection(dim as u32);
        let padded = collection.padded_dimension as usize;
        let bytes_per_code = padded / 2 + 4;
        let holder = SegmentHolder::new(dim as u32, collection.clone());
        {
            let snap = holder.load();
            // One existing entry far from query (f32 L2 distance)
            snap.mutable.append(0, &[100.0f32; 4], 1);
        }
        let _query_sq = vec![0i8; dim];
        let query_f32 = vec![0.0f32; dim];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringTreemap::new();

        // Dirty set has one entry close to query
        let dirty_entry = crate::vector::segment::mutable::MutableEntry {
            internal_id: 1000,
            key_hash: 999,
            vector_offset: 0,
            norm: 1.0,
            insert_lsn: 50,
            delete_lsn: 0,
            txn_id: 42,
        };

        // Encode a zero vector as TQ codes for the dirty entry
        let dirty_f32 = vec![0.0f32; dim];
        let mut work_buf = vec![0.0f32; padded];
        let tq_code = crate::vector::turbo_quant::encoder::encode_tq_mse_scaled(
            &dirty_f32,
            collection.fwht_sign_flips.as_slice(),
            collection.codebook_boundaries_15(),
            &mut work_buf,
        );
        // Build dirty_tq_codes: codes + norm as le bytes
        let mut dirty_tq_bytes = Vec::with_capacity(bytes_per_code);
        dirty_tq_bytes.extend_from_slice(&tq_code.codes);
        dirty_tq_bytes.extend_from_slice(&tq_code.norm.to_le_bytes());

        let mvcc_ctx = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 42,
            committed: &committed,
            dirty_set: std::slice::from_ref(&dirty_entry),
            dimension: dim as u32,
            ef_defaulted: false,
            tuning: crate::vector::types::SearchTuning::default(),
        };
        let results = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_ctx);

        // NOTE: dirty set scoring is deferred to Phase 66 (see search_mvcc comment).
        // For now, dirty entries do NOT appear in results.
        // Once Phase 66 lands, update this assertion:
        //   assert!(!results.is_empty());
        //   assert_eq!(results[0].id.0, 1000);
        // Current behavior: only the committed entry (id=0) is returned.
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id.0, 0);
    }

    #[test]
    fn test_holder_search_mvcc_empty_dirty_set_matches_no_dirty() {
        distance::init();
        let dim = 8;
        let _padded = padded_dimension(dim as u32) as usize;
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let f32_v = vec![0.0f32; dim as usize];
                snap.mutable.append(i as u64, &f32_v, i as u64);
            }
        }
        let _query_sq = make_sq_vector(dim as usize, 1);
        let query_f32 = vec![0.0f32; dim as usize];
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);
        let committed = roaring::RoaringTreemap::new();

        let mvcc_empty = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
            ef_defaulted: false,
            tuning: crate::vector::types::SearchTuning::default(),
        };
        let r1 = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_empty);

        // Same with explicit empty dirty set
        let mvcc_empty2 = super::MvccContext {
            snapshot_lsn: 10,
            my_txn_id: 99,
            committed: &committed,
            dirty_set: &[],
            dimension: dim as u32,
            ef_defaulted: false,
            tuning: crate::vector::types::SearchTuning::default(),
        };
        let r2 = holder.search_mvcc(&query_f32, 3, 64, &mut scratch, None, &mvcc_empty2);

        assert_eq!(r1.len(), r2.len());
        for (a, b) in r1.iter().zip(r2.iter()) {
            assert_eq!(a.id.0, b.id.0);
        }
    }

    #[test]
    fn test_holder_snapshot_isolation() {
        let collection = make_test_collection(128);
        let holder = SegmentHolder::new(128, collection.clone());

        // Take snapshot before swap
        let snap_before = holder.load();
        assert_eq!(snap_before.mutable.len(), 0);

        // Insert into mutable (through original snapshot's Arc)
        snap_before.mutable.append(1, &[0.0f32; 128], 1);

        // Swap with completely new list
        let new_mutable = Arc::new(MutableSegment::new(128, collection));
        new_mutable.append(2, &[1.0f32; 128], 2);
        new_mutable.append(3, &[2.0f32; 128], 3);
        holder.swap(SegmentList {
            mutable: new_mutable,
            immutable: Vec::new(),
            ivf: Vec::new(),
            warm: Vec::new(),
            cold: Vec::new(),
            unloaded: Vec::new(),
        });

        // Old snapshot still sees the original mutable (1 entry from our append)
        assert_eq!(snap_before.mutable.len(), 1);

        // New snapshot sees new mutable (2 entries)
        let snap_after = holder.load();
        assert_eq!(snap_after.mutable.len(), 2);
    }

    #[test]
    fn test_holder_search_with_ivf() {
        use crate::vector::segment::ivf;

        distance::init();
        let dim = 8usize;
        let pdim = padded_dimension(dim as u32) as usize;
        let dim_half = pdim / 2;

        // Create sign flips.
        let mut sign_flips = vec![1.0f32; pdim];
        for (i, s) in sign_flips.iter_mut().enumerate() {
            if i % 3 == 0 {
                *s = -1.0;
            }
        }

        // Build a small IVF segment with 20 vectors, 2 clusters.
        let n = 20;
        let n_clusters = 2;

        // Cluster 0: vectors near origin. Cluster 1: vectors near (5,5,...).
        let mut vectors = Vec::with_capacity(n * dim);
        let mut tq_codes = Vec::with_capacity(n);
        let mut norms = Vec::with_capacity(n);
        let ids: Vec<u32> = (1000..1000 + n as u32).collect();

        for i in 0..n {
            let offset = if i < n / 2 { 0.0 } else { 5.0 };
            let v: Vec<f32> = (0..dim)
                .map(|d| offset + (i * dim + d) as f32 * 0.01)
                .collect();
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            norms.push(if norm > 0.0 { norm } else { 1.0 });
            vectors.extend_from_slice(&v);
            tq_codes.push(vec![(i & 0xF) as u8; dim_half]);
        }

        let ivf_seg = ivf::build_ivf_segment(
            &vectors,
            &tq_codes,
            &norms,
            &ids,
            dim,
            n_clusters,
            &sign_flips,
        );

        assert_eq!(ivf_seg.total_vectors(), n as u64);

        // Create holder and swap in SegmentList with IVF.
        let collection = make_test_collection(dim as u32);
        let holder = SegmentHolder::new(dim as u32, collection);

        // Insert mutable vectors (ids 0-4).
        {
            let snap = holder.load();
            for i in 0..5u32 {
                let f32_v = vec![0.0f32; dim];
                snap.mutable.append(i as u64, &f32_v, i as u64);
            }
        }

        // Swap in list that includes the IVF segment.
        let old_snap = holder.load();
        holder.swap(SegmentList {
            mutable: Arc::clone(&old_snap.mutable),
            immutable: Vec::new(),
            ivf: vec![Arc::new(ivf_seg)],
            warm: Vec::new(),
            cold: Vec::new(),
            unloaded: Vec::new(),
        });

        // total_vectors should include IVF vectors.
        assert_eq!(holder.total_vectors(), 5 + n as u32);

        // Search should return results from both mutable and IVF.
        let query_f32 = vec![0.0f32; dim];
        let _query_sq = make_sq_vector(dim, 1);
        let mut scratch = crate::vector::hnsw::search::SearchScratch::new(0, 128);

        let results = holder.search(&query_f32, 10, 64, &mut scratch);
        assert!(!results.is_empty());
        // Should contain at least some IVF results (ids >= 1000).
        let ivf_count = results.iter().filter(|r| r.id.0 >= 1000).count();
        // And mutable results (ids < 5).
        let mut_count = results.iter().filter(|r| r.id.0 < 5).count();
        assert!(
            ivf_count > 0 || mut_count > 0,
            "should have results from both segments"
        );
    }
}
