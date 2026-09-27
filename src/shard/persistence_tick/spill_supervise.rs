//! The shard tick's half of spill-thread supervision (moon#1265): reconcile
//! what a dead spill thread held, and respawn it once its backoff is over.
//! The thread-side half (death detection, the restart policy, the channels)
//! is `storage::tiered::spill_thread::supervision`.
//!
//! # The reconcile
//!
//! It runs on the first tick that saw the thread dead, right after that
//! tick drained and applied every completion the thread sent — the death was
//! sampled before the drain, so after it nothing more will ever arrive from
//! that incarnation. At that instant every spill request of the shard is in
//! exactly one of three places:
//!
//! 1. **applied** — its completion was drained: its in-flight record is
//!    retired, its superseded entry settled;
//! 2. **queued** — still in the request channel, never seen by any thread:
//!    no file carries its id;
//! 3. **lost** — the dead thread had taken it (its buffer, a flush it was
//!    writing): no completion will come, and at most an unlisted file
//!    carries its id.
//!
//! Queued requests are taken off the channel and queued again in the same
//! order with the SAME ids for the next incarnation (on a restart; on a
//! degrade they are lost too). Their in-flight records and superseded
//! entries stay exactly as they are — the next incarnation's completion
//! settles them through the unchanged moon#1253 path, and the watermark it
//! publishes only covers ids it flushed.
//!
//! Every other request is lost, and resolved here:
//! - a lost request that still owns its key's in-flight record is
//!   **rehydrated**: the payload goes back into the hot table (it never left
//!   RAM, and the AOF write that authorized the eviction still describes
//!   it). No new id is minted: the next eviction pass spills the key again
//!   through the ordinary path, under a fresh id from the shard's counter.
//! - a lost request that was superseded (DEL, overwrite, FLUSH, re-eviction
//!   while in flight) has its superseded entry **forgotten**: its slot can
//!   only be in an unlisted file, which no restart indexes, so no fold needs
//!   a head `DEL` for it (the same rule `spill_superseded_prune_below`
//!   applies to a completion dropped at shutdown).
//!
//! Re-sending a lost payload instead of rehydrating it was rejected: its
//! record would have to be re-keyed to a fresh id without the moon#1253
//! "retired while in flight" bookkeeping firing, a second path through the
//! most delicate invariant here, to save one re-eviction of keys that are
//! at most one flush (`FLUSH_ENTRY_CAP`) per death.
//!
//! Idempotent: a second reconcile finds no lost record (the first retired
//! them) and no superseded entry outside the queued set.
//!
//! Files the dead thread wrote but never announced stay on disk, unlisted;
//! the startup orphan sweep deletes them and reserves their ids (moon#1114),
//! and at runtime the shard's counter is above every id it ever minted, so
//! none is re-issued (moon#1067).

use std::collections::HashSet;

use crate::shard::slice::with_shard_db;
use crate::storage::tiered::spill_thread::supervisor::Verdict;
use crate::storage::tiered::spill_thread::{Respawn, SpillThread};

/// What one reconcile did (logged, and returned for tests).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reconciled {
    /// Requests queued again for the next incarnation.
    pub(crate) requeued: usize,
    /// Lost in-flight payloads put back into the hot table.
    pub(crate) rehydrated: usize,
    /// Lost in-flight payloads that could not go back (the key was
    /// re-created hot meanwhile — not a loss — or did not decode).
    pub(crate) not_rehydrated: usize,
    /// Superseded entries of lost requests forgotten.
    pub(crate) superseded_forgotten: usize,
    /// Reclaim jobs dropped from the queue.
    pub(crate) reclaim_jobs_dropped: usize,
}

/// After a completion drain: `was_dead` is [`SpillThread::is_dead`] as
/// sampled BEFORE that drain. The first tick to see an incarnation dead
/// reconciles it (see the module doc); a degrading death also stops the
/// shard's spilling for good.
pub(super) fn after_drain(
    st: &SpillThread,
    was_dead: bool,
    db_count: usize,
    shard_id: usize,
) -> Option<Reconciled> {
    // The monotonic supervision clock, not the cached wall clock: a wall
    // step would reset the restart budget (moon#1265 review).
    let now_ms = st.clock_ms();
    let death = st.take_death(was_dead, now_ms)?;
    let restart = matches!(death.verdict, Verdict::RespawnAt(_));
    let done = reconcile(st, db_count, shard_id, restart);
    tracing::debug!(
        shard_id,
        panic = death.panic.as_deref().unwrap_or("none"),
        in_reclaim = death.in_reclaim,
        "spill thread death handled"
    );
    Some(done)
}

/// Respawn the spill thread if its backoff is over. A respawn that fails
/// and spends the budget degrades the shard, which reconciles like a
/// degrading death.
pub(super) fn respawn_if_due(st: &SpillThread, db_count: usize, shard_id: usize) {
    let now_ms = st.clock_ms();
    if st.respawn_if_due(now_ms) == Respawn::Failed(Verdict::Degrade) {
        reconcile(st, db_count, shard_id, false);
    }
}

/// See the module doc. `restart`: the queued requests wait for the next
/// incarnation; otherwise (degraded) they are lost too and the channels are
/// closed.
pub(crate) fn reconcile(
    st: &SpillThread,
    db_count: usize,
    shard_id: usize,
    restart: bool,
) -> Reconciled {
    let mut done = Reconciled {
        reclaim_jobs_dropped: st.discard_queued_reclaim(),
        ..Reconciled::default()
    };
    let queued = st.take_queued_requests();
    let mut live: HashSet<u64> = HashSet::new();
    if restart {
        live.extend(queued.iter().map(|r| r.file_id));
        for refused in st.requeue(queued) {
            live.remove(&refused.file_id);
        }
        done.requeued = live.len();
    } else {
        drop(queued);
        st.stop_spilling();
    }
    for db_index in 0..db_count {
        with_shard_db(db_index, |db| {
            if db.spill_inflight_is_empty() && db.spill_superseded_is_empty() {
                return;
            }
            let lost: Vec<(bytes::Bytes, u64)> = db
                .spill_inflight_records()
                .filter(|(_, id)| !live.contains(id))
                .map(|(k, id)| (k.clone(), id))
                .collect();
            for (key, req_id) in lost {
                if rehydrate_lost(db, &key, req_id, shard_id) {
                    done.rehydrated += 1;
                } else {
                    done.not_rehydrated += 1;
                }
            }
            done.superseded_forgotten += db.spill_superseded_retain(|id| live.contains(&id));
        });
    }
    tracing::info!(
        shard_id,
        restart,
        requeued = done.requeued,
        rehydrated = done.rehydrated,
        not_rehydrated = done.not_rehydrated,
        superseded_forgotten = done.superseded_forgotten,
        reclaim_jobs_dropped = done.reclaim_jobs_dropped,
        "spill thread death reconciled (moon#1265)"
    );
    done
}

/// Put a lost request's payload back into the hot table. Same guards as the
/// failed-write branch of `apply_completion_vec`: the record must still be
/// this request's, and a key re-created hot meanwhile is left alone.
/// Returns whether the key went back.
fn rehydrate_lost(
    db: &mut crate::storage::Database,
    key: &bytes::Bytes,
    req_id: u64,
    shard_id: usize,
) -> bool {
    let payload = db.spill_inflight_payload(key, req_id);
    db.spill_inflight_clear(key, req_id);
    if db.get_version(key) != 0 {
        return false;
    }
    match payload.and_then(|(vt, bytes, ttl)| {
        crate::storage::eviction::rehydrate_spill_payload(vt, &bytes, ttl)
    }) {
        Some(hot) => {
            // Putting an evicted value back is no keyspace change
            // (moon#1232): the eviction was not counted either.
            let _quiet = crate::admin::metrics_setup::mute_keyspace_changes();
            db.set(key, hot);
            crate::storage::tiered::spill_thread::record_spill_thread_rehydrated();
            true
        }
        None => {
            tracing::error!(
                shard_id,
                req_id,
                key_len = key.len(),
                "spill thread died holding a payload that does not rehydrate — key lost \
                 until AOF-replay restart"
            );
            false
        }
    }
}
