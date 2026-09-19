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

use std::sync::Arc;

use super::{SegmentHolder, SegmentList};

use crate::vector::persistence::unloaded_segment::UnloadedSegment;

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
        for stub in snap.unloaded.iter() {
            match stub.reload() {
                Ok(warm_seg) => {
                    tracing::info!(
                        segment_id = stub.segment_id(),
                        docs = stub.total_count(),
                        "COLD segment reloaded on search touch"
                    );
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
            let new_list = SegmentList {
                mutable: Arc::clone(&snap.mutable),
                immutable: snap.immutable.clone(),
                ivf: snap.ivf.clone(),
                warm: new_warm,
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
        for stub in snap.unloaded.iter() {
            if let Some(seg) = take_completed_reload(pool, stub) {
                // A prior query already reloaded this off-loop — install it now
                // so future queries hit WARM and the stub's memory is freed.
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
            let new_list = SegmentList {
                mutable: Arc::clone(&snap.mutable),
                immutable: snap.immutable.clone(),
                ivf: snap.ivf.clone(),
                warm: new_warm,
                unloaded: still_unloaded,
            };
            self.segments.store(Arc::new(new_list));
        }
        receivers
    }

    /// Install every off-loop reload of this holder's COLD stubs that has
    /// already finished, WITHOUT submitting new ones. Non-blocking: a cheap
    /// in-memory swap under the reload lock. Returns the number installed.
    ///
    /// Called right after a yielding FT.SEARCH has awaited its reloads
    /// (moon#1070), so a COLD segment leaves `unloaded` with the query that
    /// touched it rather than with whichever query happens to come next: until
    /// then the reloaded segment sat in the pool, resident but invisible to
    /// `FT.INFO` and to the mmap budget, and the stub stayed the index's
    /// tombstone sink.
    pub fn install_completed_reloads(&self) -> usize {
        {
            let snap = self.segments.load();
            if snap.unloaded.is_empty() {
                return 0;
            }
        }
        let Some(pool) = crate::vector::reload_pool::global() else {
            return 0;
        };
        let _guard = self.reload_lock.lock();
        let snap = self.segments.load();
        let mut new_warm = snap.warm.clone();
        let mut still_unloaded: Vec<Arc<UnloadedSegment>> = Vec::new();
        let mut installed = 0usize;
        for stub in snap.unloaded.iter() {
            match take_completed_reload(pool, stub) {
                Some(seg) => {
                    new_warm.push(seg);
                    installed += 1;
                }
                None => still_unloaded.push(Arc::clone(stub)),
            }
        }
        if installed > 0 {
            self.segments.store(Arc::new(
                snap.with_warm_and_unloaded(new_warm, still_unloaded),
            ));
        }
        installed
    }
}

/// Take `stub`'s finished off-loop reload out of the pool, ready to install.
///
/// The worker applied the stub's tombstones when the reload RAN; every DEL
/// since then was recorded by the stub alone, which the install is about to
/// drop. Replay them onto the reloaded segment first (moon#1070), or the
/// deleted docs come back as soon as it is installed.
fn take_completed_reload(
    pool: &crate::vector::reload_pool::SegmentReloadPool,
    stub: &UnloadedSegment,
) -> Option<Arc<crate::vector::persistence::warm_search::WarmSearchSegment>> {
    let seg = pool.take_completed(stub.segment_id())?;
    stub.replay_tombstones_onto(&seg);
    tracing::info!(
        segment_id = stub.segment_id(),
        "COLD segment installed from completed off-loop reload"
    );
    Some(seg)
}
