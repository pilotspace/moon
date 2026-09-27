//! Supervision of a shard's spill thread (moon#1265): death detection, the
//! respawn, and the channel-side half of the reconcile. The shard-side half
//! (rehydrating what died with a thread, rebuilding the superseded sets)
//! lives in `shard::persistence_tick::spill_supervise`, which drives these
//! primitives from the shard tick.
//!
//! # What crosses the thread boundary, and what a respawn rebuilds
//!
//! | state | owner | on death |
//! |---|---|---|
//! | request / completion / reclaim channels | [`SpillThread`] keeps the thread-side ends | untouched: the next incarnation reattaches |
//! | requests still queued | the channel | kept, re-queued in order with their ids |
//! | requests the dead thread had taken (its buffer, a flush mid-write) | died with it | lost: their in-flight payloads are rehydrated by the shard |
//! | completions it sent | the channel | applied by the drain before the reconcile |
//! | `done_below` | shared `Arc` | kept; the next incarnation only moves it up |
//! | reclaim jobs queued / taken | the channel / died with it | dropped; the shard abandons their compactions |
//! | files it wrote but never announced | disk, unlisted | left for the startup orphan sweep |
//! | the file-id counter | the shard | untouched: no id is minted twice (moon#1067) |

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::supervisor::{Phase, RestartPolicy, RestartSupervisor, Verdict};
use super::{SpillRequest, SpillThread, ThreadEnds};

/// Shards whose spill thread is down right now: dead and not yet respawned,
/// in backoff, or degraded (INFO `spill_thread_alive` is 0 while any is).
static SPILL_THREADS_DOWN: AtomicU64 = AtomicU64::new(0);
/// Successful respawns, all shards (INFO `spill_thread_restarts`).
static SPILL_THREAD_RESTARTS: AtomicU64 = AtomicU64::new(0);
/// Shards whose restart budget is spent (INFO `spill_thread_degraded`).
static SPILL_THREADS_DEGRADED: AtomicU64 = AtomicU64::new(0);
/// In-flight payloads put back in RAM because the thread carrying their
/// request died (INFO `spill_thread_rehydrated`).
static SPILL_THREAD_REHYDRATED: AtomicU64 = AtomicU64::new(0);

/// `false` while any shard's spill thread is down (INFO `spill_thread_alive`).
#[inline]
pub fn spill_threads_alive() -> bool {
    SPILL_THREADS_DOWN.load(Ordering::Relaxed) == 0
}

/// Cumulative spill-thread respawns (INFO `spill_thread_restarts`).
#[inline]
pub fn spill_thread_restarts_total() -> u64 {
    SPILL_THREAD_RESTARTS.load(Ordering::Relaxed)
}

/// Shards that stopped spilling for good (INFO `spill_thread_degraded`).
#[inline]
pub fn spill_threads_degraded() -> u64 {
    SPILL_THREADS_DEGRADED.load(Ordering::Relaxed)
}

/// Cumulative in-flight payloads rehydrated after a spill-thread death.
#[inline]
pub fn spill_thread_rehydrated_total() -> u64 {
    SPILL_THREAD_REHYDRATED.load(Ordering::Relaxed)
}

/// Record one in-flight payload rehydrated after a spill-thread death.
#[inline]
pub(crate) fn record_spill_thread_rehydrated() {
    SPILL_THREAD_REHYDRATED.fetch_add(1, Ordering::Relaxed);
}

fn gauge_down(delta: i64) {
    let _ = SPILL_THREADS_DOWN.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_add_signed(delta))
    });
}

/// Where an incarnation leaves its panic message for the shard to log.
#[derive(Clone, Default)]
pub(super) struct ExitSlot(Arc<parking_lot::Mutex<Option<String>>>);

impl ExitSlot {
    /// Called on the dying thread, inside the `catch_unwind` handler.
    pub(super) fn record_panic(&self, payload: &(dyn std::any::Any + Send)) {
        let msg = payload
            .downcast_ref::<&'static str>()
            .map(|s| (*s).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "a non-string panic payload".to_string());
        *self.0.lock() = Some(msg);
    }

    pub(super) fn take(&self) -> Option<String> {
        self.0.lock().take()
    }
}

/// The running incarnation and its restart policy (behind
/// `SpillThread::worker`; locked briefly by the shard tick, never across an
/// `.await`, never by the spill thread itself).
pub(super) struct Worker {
    handle: Option<std::thread::JoinHandle<()>>,
    exit: ExitSlot,
    /// `None` once degraded: dropping them disconnects the channels.
    ends: Option<ThreadEnds>,
    supervisor: RestartSupervisor,
}

impl Worker {
    pub(super) fn new(
        handle: std::thread::JoinHandle<()>,
        exit: ExitSlot,
        ends: ThreadEnds,
    ) -> Self {
        Self {
            handle: Some(handle),
            exit,
            ends: Some(ends),
            supervisor: RestartSupervisor::new(
                RestartPolicy::DEFAULT,
                crate::storage::entry::current_time_ms(),
            ),
        }
    }

    /// Shutdown: the handle to join, and the gauges this shard held.
    pub(super) fn retire_for_shutdown(&mut self) -> Option<std::thread::JoinHandle<()>> {
        match self.supervisor.phase() {
            Phase::Running { .. } if self.handle.as_ref().is_some_and(|h| !h.is_finished()) => {}
            Phase::Running { .. } => {
                // Dead but never observed: nothing was counted.
            }
            Phase::Backoff { .. } => gauge_down(-1),
            Phase::Degraded => {
                gauge_down(-1);
                let _ = SPILL_THREADS_DEGRADED.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |n| Some(n.saturating_sub(1)),
                );
            }
        }
        self.handle.take()
    }
}

/// A death the shard just observed.
#[derive(Debug)]
pub(crate) struct Death {
    pub(crate) verdict: Verdict,
    /// The panic message, when the thread panicked.
    pub(crate) panic: Option<String>,
}

/// What [`SpillThread::respawn_if_due`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Respawn {
    NotDue,
    Respawned,
    /// The thread could not be started; the verdict for the next attempt.
    Failed(Verdict),
}

impl SpillThread {
    /// No thread is running for this shard right now: the current one exited
    /// without being asked to (a panic), or it is in backoff, or degraded.
    /// `false` once shutdown was requested.
    pub fn is_dead(&self) -> bool {
        if self.stop_flag.load(Ordering::Acquire) {
            return false;
        }
        let dead = {
            let w = self.worker.lock();
            match w.supervisor.phase() {
                Phase::Running { .. } => w
                    .handle
                    .as_ref()
                    .is_none_or(std::thread::JoinHandle::is_finished),
                Phase::Backoff { .. } | Phase::Degraded => true,
            }
        };
        if dead {
            // `is_finished` is a relaxed read of the value the thread's exit
            // released; this fence orders everything the thread did before it
            // exited (its last completion sends included) before the caller's
            // next read of the completion channel.
            std::sync::atomic::fence(Ordering::Acquire);
        }
        dead
    }

    /// Whether this shard's restart budget is spent (it spills no more).
    #[cfg(test)]
    pub(crate) fn is_degraded(&self) -> bool {
        self.worker.lock().supervisor.phase() == Phase::Degraded
    }

    /// Given [`Self::is_dead`] as sampled BEFORE the caller drained and
    /// applied the completions: the first time a running incarnation is
    /// seen dead, reap it, decide (respawn at a due time, or degrade),
    /// count and log it. `None` when there is nothing new to reconcile.
    ///
    /// The sample must predate the drain (moon#1253, review 5): a thread
    /// seen dead then had sent everything it ever will, so what the caller
    /// reconciles next is exactly what died with it.
    pub(crate) fn take_death(&self, was_dead: bool, now_ms: u64) -> Option<Death> {
        if !was_dead || self.stop_flag.load(Ordering::Acquire) {
            return None;
        }
        let mut w = self.worker.lock();
        if !matches!(w.supervisor.phase(), Phase::Running { .. }) {
            return None;
        }
        // A panic that escaped `catch_unwind` (none can: the whole body is
        // inside it) would still surface here as the join's error.
        let escaped = w.handle.take().is_some_and(|h| h.join().is_err());
        let panic = w
            .exit
            .take()
            .or_else(|| escaped.then(|| "a panic outside catch_unwind".to_string()));
        let verdict = w.supervisor.on_death(now_ms);
        let attempts = w.supervisor.attempts_in_window(now_ms);
        drop(w);
        gauge_down(1);
        crate::admin::metrics_setup::record_spill_thread_death();
        let panic_msg = panic.as_deref().unwrap_or("exited without a panic");
        match verdict {
            Verdict::RespawnAt(due_ms) => tracing::error!(
                shard_id = self.shard_id,
                panic = %panic_msg,
                respawn_in_ms = due_ms.saturating_sub(now_ms),
                restarts_in_window = attempts,
                max_restarts = RestartPolicy::DEFAULT.max_restarts,
                "spill thread died; the shard respawns it after a backoff (moon#1265). Until \
                 then its queued spills wait, and what the dead thread held goes back to RAM"
            ),
            Verdict::Degrade => self.note_degraded(panic_msg, attempts),
        }
        Some(Death { verdict, panic })
    }

    fn note_degraded(&self, why: &str, attempts: usize) {
        SPILL_THREADS_DEGRADED.fetch_add(1, Ordering::Relaxed);
        crate::admin::metrics_setup::set_spill_threads_degraded(spill_threads_degraded());
        tracing::warn!(
            shard_id = self.shard_id,
            last_death = %why,
            restarts_in_window = attempts,
            "spill thread restart budget spent: this shard is DEGRADED until the server restarts \
             (moon#1265). It spills nothing more — evictions drop keys under an evicting policy, \
             noeviction answers -OOM — and its cold reclaim stops; existing cold keys stay \
             readable. INFO spill_thread_degraded counts such shards"
        );
    }

    /// Respawn the thread if its backoff is over (shard clock `now_ms`).
    /// A failed spawn counts against the budget like a death; when it spends
    /// it, the shard is degraded and the caller must reconcile as for a
    /// degrading death.
    pub(crate) fn respawn_if_due(&self, now_ms: u64) -> Respawn {
        if self.stop_flag.load(Ordering::Acquire) {
            return Respawn::NotDue;
        }
        let mut w = self.worker.lock();
        if !w.supervisor.respawn_due(now_ms) {
            return Respawn::NotDue;
        }
        let Some(ends) = w.ends.clone() else {
            return Respawn::NotDue;
        };
        match Self::spawn_incarnation(
            self.shard_id,
            ends,
            self.stop_flag.clone(),
            self.done_below.clone(),
            self.fault.clone(),
        ) {
            Ok((handle, exit)) => {
                w.handle = Some(handle);
                w.exit = exit;
                w.supervisor.on_respawned(now_ms);
                drop(w);
                gauge_down(-1);
                SPILL_THREAD_RESTARTS.fetch_add(1, Ordering::Relaxed);
                crate::admin::metrics_setup::record_spill_thread_restart();
                tracing::info!(
                    shard_id = self.shard_id,
                    restarts_total = spill_thread_restarts_total(),
                    "spill thread respawned; spill-based eviction resumes (moon#1265)"
                );
                Respawn::Respawned
            }
            Err(e) => {
                let verdict = w.supervisor.on_spawn_failed(now_ms);
                let attempts = w.supervisor.attempts_in_window(now_ms);
                drop(w);
                tracing::error!(
                    shard_id = self.shard_id,
                    error = %e,
                    ?verdict,
                    "spill thread respawn failed (moon#1265)"
                );
                if verdict == Verdict::Degrade {
                    self.note_degraded("the respawn itself failed", attempts);
                }
                Respawn::Failed(verdict)
            }
        }
    }

    /// Take every spill request still queued for the thread, in order.
    /// Only while no thread is running (the reconcile): nothing else reads
    /// the channel then, and the shard thread — the only sender — is busy
    /// here, so the result is exactly what is queued.
    pub(crate) fn take_queued_requests(&self) -> Vec<SpillRequest> {
        let w = self.worker.lock();
        w.ends
            .as_ref()
            .map(|e| e.request_rx.try_iter().collect())
            .unwrap_or_default()
    }

    /// Queue `requests` again, in order, for the next incarnation. Returns
    /// those that did not fit (none can: they were just taken off this
    /// channel and nothing was sent in between).
    pub(crate) fn requeue(&self, requests: Vec<SpillRequest>) -> Vec<SpillRequest> {
        let mut refused = Vec::new();
        for req in requests {
            if let Err(e) = self.request_tx.try_send(req) {
                refused.push(e.into_inner());
            }
        }
        refused
    }

    /// Drop every reclaim job still queued (none has done any I/O). Their
    /// compactions are abandoned by the reclaim tick, which sees the thread
    /// dead. Returns how many.
    pub(crate) fn discard_queued_reclaim(&self) -> usize {
        let w = self.worker.lock();
        w.ends
            .as_ref()
            .map_or(0, |e| e.reclaim_rx.try_iter().count())
    }

    /// Degraded (moon#1265): drop the thread-side channel ends. The request
    /// channel disconnects, so every eviction takes the no-spill path, and
    /// nothing can be queued — and pinned in RAM — for a thread that will
    /// never run. Call after the queue was taken.
    pub(crate) fn stop_spilling(&self) {
        let ends = self.worker.lock().ends.take();
        drop(ends);
    }
}
