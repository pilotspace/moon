//! WS3 round 2: `SegmentHolder::promote_unloaded` -- reload COLD (unloaded)
//! segments back into the WARM tier on the next search that touches them.
//!
//! Split out of `holder.rs` (directory-module convention, see
//! `CLAUDE.md`'s "Module Structure" rule) to keep that file under the
//! 1500-line cap. As a child module of `holder`, this file has the same
//! access to `SegmentHolder`'s private fields as code living directly in
//! `holder.rs` would.
//!
//! Design (see `docs/guides/tuning.md` "Vector/FTS/graph idle-unload" for
//! the full tier writeup): a KNN query must scan every segment for
//! correctness, so a COLD segment cannot stay unloaded and still
//! participate -- `promote_unloaded` is called at the top of every search
//! path (`SegmentHolder::search_filtered`, `SegmentHolder::search_mvcc`, and
//! the yielding worker-pool capture in
//! `command/vector_search/ft_search/dispatch.rs`) and reloads any COLD
//! segments into `warm` before the actual scan runs. The fast path (no
//! unloaded segments) is a single `is_empty()` check with no lock and no
//! allocation.
//!
//! Single-flight: `reload_lock` is a blocking (not try_lock) mutex, so N
//! concurrent callers touching the same COLD segment all wait for the one
//! reload rather than racing ahead with a stale, missing-segment snapshot --
//! correctness over latency for the (rare) first-touch-after-idle query. The
//! lock is only ever held across synchronous file I/O
//! (`UnloadedSegment::reload`), never across an `.await` point. A segment
//! that fails to reload (corrupt/missing files) is left in `unloaded` and
//! retried on the next touch; it does not poison the rest of the batch.

use std::collections::HashSet;
use std::sync::Arc;

use super::{SegmentHolder, SegmentList};

use crate::vector::persistence::unloaded_segment::UnloadedSegment;
use crate::vector::persistence::warm_search::WarmSearchSegment;

/// #18 reload-admission gate: evict least-recently-accessed WARM segments —
/// converting each to a reloadable COLD stub (mirrors `MmapBudget` + the
/// idle-unload path) — until the WARM tier's resident bytes fit `cap_bytes`.
///
/// Called right after a reload/promotion installs freshly-hot segments into
/// `new_warm`, so a query burst that first-touches many COLD segments can't
/// balloon the WARM tier past the per-shard cap: older, idle segments are
/// pushed back to COLD (reloadable) to make room for the ones this query needs.
/// `protect` holds the ids just installed for THIS query — never evict them
/// (they are the reason we reloaded). `cap_bytes == 0` disables the gate.
///
/// The per-shard `MmapBudget` tick remains the authoritative enforcer; this is
/// the immediate bound between ticks. With multiple vector indexes on one
/// shard each holder gates to the full per-shard cap (a loose upper bound); the
/// tick reconciles the true per-shard total 10s later, now reloadably (fix A).
fn evict_warm_lru_to_budget(
    new_warm: &mut Vec<Arc<WarmSearchSegment>>,
    still_unloaded: &mut Vec<Arc<UnloadedSegment>>,
    cap_bytes: u64,
    protect: &HashSet<u64>,
) {
    if cap_bytes == 0 {
        return;
    }
    let mut total: u64 = new_warm.iter().map(|w| w.resident_bytes() as u64).sum();
    if total <= cap_bytes {
        return;
    }

    // Evictable = everything not needed by the current query, oldest-access
    // first (true LRU via the atomic bumped on every search).
    let mut order: Vec<usize> = (0..new_warm.len())
        .filter(|&i| !protect.contains(&new_warm[i].segment_id()))
        .collect();
    order.sort_by_key(|&i| new_warm[i].last_access_micros());

    let mut evict: HashSet<usize> = HashSet::new();
    for &i in &order {
        if total <= cap_bytes {
            break;
        }
        total = total.saturating_sub(new_warm[i].resident_bytes() as u64);
        evict.insert(i);
    }
    if evict.is_empty() {
        return;
    }

    let mut kept: Vec<Arc<WarmSearchSegment>> = Vec::with_capacity(new_warm.len() - evict.len());
    for (i, w) in std::mem::take(new_warm).into_iter().enumerate() {
        if evict.contains(&i) {
            // Durable disk backing means from_warm captures a reloadable stub.
            still_unloaded.push(Arc::new(UnloadedSegment::from_warm(w.as_ref(), false)));
        } else {
            kept.push(w);
        }
    }
    *new_warm = kept;
    tracing::debug!(
        evicted = evict.len(),
        cap_bytes,
        "reload-admission gate: demoted idle WARM segment(s) to COLD to fit budget"
    );
}

impl SegmentHolder {
    /// Reload every COLD (`unloaded`) segment into `warm`, synchronously.
    /// Returns the number of segments promoted.
    pub fn promote_unloaded(&self) -> usize {
        {
            let snap = self.segments.load();
            if snap.unloaded.is_empty() {
                return 0;
            }
        }
        let _guard = self.reload_lock.lock();
        let snap = self.segments.load();
        if snap.unloaded.is_empty() {
            // Another thread already promoted everything while we waited.
            return 0;
        }

        let mut new_warm = snap.warm.clone();
        let mut still_unloaded: Vec<Arc<UnloadedSegment>> = Vec::new();
        let mut promoted = 0usize;
        let mut protect: HashSet<u64> = HashSet::new();
        for stub in snap.unloaded.iter() {
            match stub.reload() {
                Ok(warm_seg) => {
                    tracing::info!(
                        segment_id = stub.segment_id(),
                        docs = stub.total_count(),
                        "COLD segment reloaded on search touch"
                    );
                    protect.insert(warm_seg.segment_id());
                    new_warm.push(Arc::new(warm_seg));
                    promoted += 1;
                }
                Err(e) => {
                    tracing::error!(
                        segment_id = stub.segment_id(),
                        error = %e,
                        "COLD segment reload failed -- left unloaded, will retry on next touch"
                    );
                    still_unloaded.push(Arc::clone(stub));
                }
            }
        }

        if promoted > 0 {
            // Bound WARM growth: demote idle segments back to COLD to fit the cap.
            evict_warm_lru_to_budget(
                &mut new_warm,
                &mut still_unloaded,
                crate::vector::reload_pool::warm_budget_per_shard(),
                &protect,
            );
            let new_list = SegmentList {
                mutable: Arc::clone(&snap.mutable),
                immutable: snap.immutable.clone(),
                ivf: snap.ivf.clone(),
                warm: new_warm,
                cold: snap.cold.clone(),
                unloaded: still_unloaded,
            };
            self.segments.store(Arc::new(new_list));
        }
        promoted
    }

    /// #18 (off-loop reload): the non-blocking counterpart to
    /// [`Self::promote_unloaded`], for the yielding FT.SEARCH capture path.
    ///
    /// Instead of blocking the shard event loop on `UnloadedSegment::reload`
    /// (mmap + page-in of a whole segment) it:
    /// 1. installs any reload that ALREADY completed (submitted by an earlier
    ///    query) into `warm` — a cheap in-memory swap, the memory-reclaim step;
    /// 2. SUBMITS every still-cold stub to the process reload pool and returns
    ///    the receivers so the caller can stash them in the `SearchSnapshot`
    ///    and `await` them off-loop (see `SearchSnapshot::await_pending_reloads`).
    ///
    /// Falls back to the blocking `promote_unloaded` (returning no receivers)
    /// when the pool is disabled/uninitialized — identical to pre-#18 behavior,
    /// which is also exactly what non-yielding sync search paths still do.
    ///
    /// Fast path: a single `is_empty()` check with no lock and no allocation
    /// when there are no COLD segments (the overwhelmingly common case).
    pub fn submit_unloaded_reloads(
        &self,
    ) -> Vec<flume::Receiver<crate::vector::reload_pool::ReloadOutcome>> {
        {
            let snap = self.segments.load();
            if snap.unloaded.is_empty() {
                return Vec::new();
            }
        }
        let Some(pool) = crate::vector::reload_pool::global() else {
            // Pool disabled: preserve the blocking pre-#18 contract exactly.
            self.promote_unloaded();
            return Vec::new();
        };

        let _guard = self.reload_lock.lock();
        let snap = self.segments.load();
        if snap.unloaded.is_empty() {
            return Vec::new();
        }

        let mut new_warm = snap.warm.clone();
        let mut still_unloaded: Vec<Arc<UnloadedSegment>> = Vec::new();
        let mut receivers = Vec::new();
        let mut installed = 0usize;
        let mut protect: HashSet<u64> = HashSet::new();
        for stub in snap.unloaded.iter() {
            if let Some(seg) = pool.take_completed(stub.segment_id()) {
                // A prior query already reloaded this off-loop — install it now
                // so future queries hit WARM and the stub's memory is freed.
                tracing::info!(
                    segment_id = stub.segment_id(),
                    "COLD segment installed from completed off-loop reload"
                );
                protect.insert(seg.segment_id());
                new_warm.push(seg);
                installed += 1;
            } else {
                // Not ready yet: submit (single-flight in the pool) and keep the
                // stub cold; this query awaits the receiver for full recall.
                receivers.push(pool.submit(Arc::clone(stub)));
                still_unloaded.push(Arc::clone(stub));
            }
        }

        if installed > 0 {
            // Bound WARM growth: demote idle segments back to COLD to fit the cap.
            evict_warm_lru_to_budget(
                &mut new_warm,
                &mut still_unloaded,
                crate::vector::reload_pool::warm_budget_per_shard(),
                &protect,
            );
            let new_list = SegmentList {
                mutable: Arc::clone(&snap.mutable),
                immutable: snap.immutable.clone(),
                ivf: snap.ivf.clone(),
                warm: new_warm,
                cold: snap.cold.clone(),
                unloaded: still_unloaded,
            };
            self.segments.store(Arc::new(new_list));
        }
        receivers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tiered::SegmentHandle;
    use crate::vector::distance;
    use crate::vector::hnsw::graph::HnswGraph;
    use crate::vector::persistence::warm_segment::{
        write_codes_mpf, write_graph_mpf, write_mvcc_mpf,
    };
    use crate::vector::turbo_quant::collection::{CollectionMetadata, QuantizationConfig};
    use crate::vector::types::DistanceMetric;
    use std::path::Path;

    fn make_collection() -> Arc<CollectionMetadata> {
        Arc::new(CollectionMetadata::new(
            1,
            128,
            DistanceMetric::L2,
            QuantizationConfig::TurboQuant4,
            42,
        ))
    }

    /// Build a WARM segment whose resident footprint is dominated by a
    /// `codes_len`-byte codes payload, so tests can drive the byte budget.
    fn make_warm_sized(dir: &Path, seg_id: u64, codes_len: usize) -> Arc<WarmSearchSegment> {
        distance::init();
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
        write_codes_mpf(&seg_dir.join("codes.mpf"), seg_id, &vec![0u8; codes_len]).unwrap();
        write_graph_mpf(&seg_dir.join("graph.mpf"), seg_id, &graph_bytes).unwrap();
        write_mvcc_mpf(&seg_dir.join("mvcc.mpf"), seg_id, &[]).unwrap();
        let handle = SegmentHandle::new(seg_id, seg_dir.clone());
        Arc::new(
            WarmSearchSegment::from_files(&seg_dir, seg_id, make_collection(), handle, false)
                .unwrap(),
        )
    }

    /// B: the gate demotes least-recently-accessed, non-protected WARM segments
    /// to reloadable stubs until the tier fits the cap.
    #[test]
    fn test_gate_demotes_idle_warm_to_fit_budget() {
        let tmp = tempfile::tempdir().unwrap();
        // 3 segments ~30 KB each; distinct access times (a oldest, c newest).
        let a = make_warm_sized(tmp.path(), 1, 30_000);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = make_warm_sized(tmp.path(), 2, 30_000);
        std::thread::sleep(std::time::Duration::from_millis(2));
        let c = make_warm_sized(tmp.path(), 3, 30_000);
        let each = a.resident_bytes() as u64;
        assert!(each >= 30_000, "codes payload should dominate resident bytes");

        // Cap fits one segment but not two. Protect `c` (the "just-reloaded" one).
        let cap = each + each / 2; // between 1x and 2x
        let mut warm = vec![a, b, c];
        let mut unloaded: Vec<Arc<UnloadedSegment>> = Vec::new();
        let protect: HashSet<u64> = HashSet::from([3]);

        evict_warm_lru_to_budget(&mut warm, &mut unloaded, cap, &protect);

        let after: u64 = warm.iter().map(|w| w.resident_bytes() as u64).sum();
        assert!(after <= cap, "WARM must fit the cap: {after} > {cap}");
        assert!(
            warm.iter().any(|w| w.segment_id() == 3),
            "protected (just-reloaded) segment must survive"
        );
        // Evicted segments became reloadable stubs, oldest-first (id 1 then 2).
        assert!(!unloaded.is_empty(), "idle segments demoted to stubs");
        assert!(unloaded.iter().any(|s| s.segment_id() == 1));
        let reloaded = unloaded[0].reload().expect("evicted stub reloads");
        assert_eq!(reloaded.segment_id(), unloaded[0].segment_id());
    }

    /// A protected segment is never evicted even when it alone exceeds the cap
    /// (the query needs it — full-recall floor).
    #[test]
    fn test_gate_never_evicts_protected() {
        let tmp = tempfile::tempdir().unwrap();
        let a = make_warm_sized(tmp.path(), 1, 40_000);
        let mut warm = vec![a];
        let mut unloaded: Vec<Arc<UnloadedSegment>> = Vec::new();
        let protect: HashSet<u64> = HashSet::from([1]);
        // Cap far below the single protected segment.
        evict_warm_lru_to_budget(&mut warm, &mut unloaded, 1_000, &protect);
        assert_eq!(warm.len(), 1, "protected segment stays even over cap");
        assert!(unloaded.is_empty());
    }

    /// cap == 0 disables the gate entirely (no eviction).
    #[test]
    fn test_gate_disabled_when_cap_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let warm_seg = make_warm_sized(tmp.path(), 1, 30_000);
        let mut warm = vec![warm_seg];
        let mut unloaded: Vec<Arc<UnloadedSegment>> = Vec::new();
        evict_warm_lru_to_budget(&mut warm, &mut unloaded, 0, &HashSet::new());
        assert_eq!(warm.len(), 1);
        assert!(unloaded.is_empty());
    }
}
