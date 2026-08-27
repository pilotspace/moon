//! Off-loop manifest commit agent (task #59).
//!
//! Under spill load, `ShardManifest::commit()` fsyncs used to run on the
//! shard event-loop thread — measured at 163–167 fsync calls per 8s window,
//! 1.0–2.1s cumulative block time, single calls up to 1.0s on a contended
//! device (2026-07-16 strace decomposition, `tmp/t59-decompose.sh`). Every
//! connection on the shard stalls for each one.
//!
//! Under `--appendonly yes` (the only config where the async-spill completion
//! path runs), the AOF is the durability backstop for spill placement: a
//! crash that loses a manifest commit costs an orphan-file sweep plus AOF
//! replay of the affected keys — never data loss. The commit therefore does
//! not need to block the event loop. This agent owns the manifest's file
//! handle ([`ManifestIo`]) on a dedicated per-shard thread and performs the
//! exact same ordered overflow→root→fsync sequence off-loop.
//!
//! Two commit flavors, both handed over via a single latest-snapshot slot
//! (never a queue — a bounded queue behind a stalled fsync would block the
//! shard thread, re-creating the very stall this agent removes):
//! - **Deferred** (`commit_deferred`): fire-and-forget slot replace + wake;
//!   the spill-completion path uses this. Consecutive snapshots coalesce by
//!   construction — each is a complete root, so only the newest needs to
//!   reach disk.
//! - **Durable** (`commit_durable`): slot replace + wake, then block on ack.
//!   Paths where the manifest IS the durability record (checkpoint protocol,
//!   no-AOF durable batch spill) keep exactly their old blocking semantics
//!   through this.
//!
//! Coalescing + ack correctness: every ack is fired after the NEWEST snapshot
//! persists. A newer root strictly supersedes an older one (each snapshot is
//! the full entry list at a higher epoch), so "your commit is durable"
//! remains true for the older requester.

use std::io;
use std::sync::Arc;

use parking_lot::Mutex;

use super::manifest::{ManifestIo, ManifestRoot};

/// State shared between callers and the sync thread. Commits REPLACE the
/// `latest` slot instead of queueing — each snapshot is a complete root at a
/// monotonically higher epoch, so only the newest must ever reach disk. The
/// slot never fills up, so a deferred commit can never block the shard
/// thread behind a slow persist (a saturated-disk fsync stall would have
/// filled any bounded queue and re-created exactly the stall this agent
/// exists to remove).
struct SyncShared {
    latest: Option<ManifestRoot>,
    /// Durable waiters. Always pushed together with a `latest` write, and
    /// taken together with it; firing after a NEWER snapshot persists is
    /// correct because a newer root strictly supersedes the waiter's own.
    acks: Vec<flume::Sender<io::Result<()>>>,
    /// Set once by `shutdown()`; the thread gives back its [`ManifestIo`]
    /// on this and exits after a final flush of the slot.
    shutdown: Option<flume::Sender<ManifestIo>>,
    /// True while the NEWEST snapshot handed to this agent is known to NOT
    /// be durable: the last `persist` failed and no newer snapshot has
    /// succeeded since. An empty-slot round must ack `Err` while this is
    /// set — the [`flush_all_agents`] barrier's "empty slot ⇒ something at
    /// least as new already persisted" premise is false after a failed
    /// persist (the failed root was consumed from the slot), and acking Ok
    /// would let the AOF rewrite prune the very records that back-stop the
    /// missing placements. Cleared only by a subsequent successful persist.
    last_persist_failed: bool,
}

/// Handle to the per-shard manifest-sync thread. Owned by `ShardManifest`
/// while deferred sync is enabled; `shutdown()` reclaims the [`ManifestIo`]
/// so post-shutdown commits can run inline again.
pub(crate) struct ManifestSyncAgent {
    shared: Arc<Mutex<SyncShared>>,
    /// Wake token, capacity 1. `Full` on send is fine: a token is already
    /// pending and state was written before the send, so the thread's next
    /// round observes it. `Disconnected` means the thread is gone — fail
    /// loudly, never a silent no-op.
    wake: flume::Sender<()>,
    join: Option<std::thread::JoinHandle<()>>,
}

/// Process-global registry of live sync agents, for [`flush_all_agents`].
/// Entries hold `Weak` shared-state refs plus a wake-sender clone; `spawn`
/// registers, `shutdown` (and dead-`Weak` pruning) deregisters.
static AGENT_REGISTRY: Mutex<Vec<(std::sync::Weak<Mutex<SyncShared>>, flume::Sender<()>)>> =
    Mutex::new(Vec::new());

/// Barrier: block until every live agent's pending deferred snapshot (if any)
/// is durable (deep-review D1).
///
/// The AOF rewrite's commit path deletes the old incr generation — the very
/// records that back-stop spill placements whose ShardManifest commit is
/// still sitting in an agent's deferred slot ("AOF replay + orphan sweep
/// reconstruct anything a lost manifest commit would have recorded"). Pruning
/// while a deferred commit is pending re-opens the crash window that
/// justification closed. Callers MUST invoke this before pruning and skip
/// the prune on error.
///
/// Implementation: push an ack-only waiter into each agent (the run loop
/// fires acks after persisting whatever is in the slot; an empty slot means a
/// previous round already persisted something at least as new — it acks
/// immediately with Ok). Bounded wait so a wedged agent fails the barrier
/// instead of hanging the rewrite.
pub(crate) fn flush_all_agents() -> io::Result<()> {
    let mut waiters = Vec::new();
    {
        let mut reg = AGENT_REGISTRY.lock();
        reg.retain(|(shared, _)| shared.strong_count() > 0);
        for (weak_shared, wake) in reg.iter() {
            let Some(shared) = weak_shared.upgrade() else {
                continue;
            };
            let (ack_tx, ack_rx) = flume::bounded::<io::Result<()>>(1);
            shared.lock().acks.push(ack_tx);
            match wake.try_send(()) {
                Ok(()) | Err(flume::TrySendError::Full(())) => {}
                Err(flume::TrySendError::Disconnected(())) => {
                    return Err(io::Error::other(
                        "manifest-sync barrier: an agent thread is gone with commits possibly pending",
                    ));
                }
            }
            waiters.push(ack_rx);
        }
    }
    for rx in waiters {
        rx.recv_timeout(std::time::Duration::from_secs(30))
            .map_err(|_| {
                io::Error::other("manifest-sync barrier: timed out waiting for a persist ack")
            })??;
    }
    Ok(())
}

impl std::fmt::Debug for ManifestSyncAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestSyncAgent")
            .field("pending", &self.shared.lock().latest.is_some())
            .finish()
    }
}

impl ManifestSyncAgent {
    /// Spawn the sync thread. On failure (thread limit, EAGAIN) the caller
    /// gets its [`ManifestIo`] BACK so it can keep committing inline — a
    /// failed spawn must degrade to on-loop fsyncs, never to a manifest that
    /// can no longer commit at all. The io is handed to the thread over a
    /// channel (not moved into the closure) precisely so it survives the
    /// failure path.
    pub(crate) fn spawn(io: ManifestIo, shard_id: usize) -> Result<Self, (ManifestIo, io::Error)> {
        let shared = Arc::new(Mutex::new(SyncShared {
            latest: None,
            acks: Vec::new(),
            shutdown: None,
            last_persist_failed: false,
        }));
        let (wake, rx) = flume::bounded(1);
        let (io_tx, io_rx) = flume::bounded::<ManifestIo>(1);
        let thread_shared = Arc::clone(&shared);
        match std::thread::Builder::new()
            .name(format!("manifest-sync-{shard_id}"))
            .spawn(move || {
                // O5: this thread is spawned from the shard's own (pinned)
                // event-loop thread and would otherwise inherit its exact
                // single-core mask — re-pin to the non-shard core set as the
                // first act, before anything else runs.
                crate::shard::numa::pin_current_aux_thread(&format!("manifest-sync-{shard_id}"));
                // The spawner sends the io immediately after a successful
                // spawn; Err here means the agent was dropped first — exit.
                let Ok(io) = io_rx.recv() else { return };
                run(io, thread_shared, rx)
            }) {
            Ok(join) => match io_tx.send(io) {
                Ok(()) => {
                    AGENT_REGISTRY
                        .lock()
                        .push((Arc::downgrade(&shared), wake.clone()));
                    Ok(Self {
                        shared,
                        wake,
                        join: Some(join),
                    })
                }
                // Thread died before its recv (can only be a panic in thread
                // start-up glue) — flume returns the unsent io in the error.
                Err(flume::SendError(io)) => {
                    let _ = join.join();
                    Err((io, io::Error::other("manifest-sync thread died at startup")))
                }
            },
            Err(e) => Err((io, e)),
        }
    }

    fn notify(&self) -> io::Result<()> {
        match self.wake.try_send(()) {
            Ok(()) | Err(flume::TrySendError::Full(())) => Ok(()),
            Err(flume::TrySendError::Disconnected(())) => {
                Err(io::Error::other("manifest-sync thread unavailable"))
            }
        }
    }

    /// Fire-and-forget commit of a complete root snapshot. Never blocks:
    /// replaces the shared slot and posts a wake token.
    pub(crate) fn commit_deferred(&self, root: ManifestRoot) -> io::Result<()> {
        self.shared.lock().latest = Some(root);
        self.notify()
    }

    /// Commit and block until the snapshot (or a newer one) is durable.
    pub(crate) fn commit_durable(&self, root: ManifestRoot) -> io::Result<()> {
        let (ack_tx, ack_rx) = flume::bounded::<io::Result<()>>(1);
        {
            let mut s = self.shared.lock();
            s.latest = Some(root);
            s.acks.push(ack_tx);
        }
        self.notify()?;
        ack_rx
            .recv()
            .map_err(|_| io::Error::other("manifest-sync thread died before ack"))?
    }

    /// Remove this agent's entry from [`AGENT_REGISTRY`]. Idempotent.
    fn deregister(&self) {
        AGENT_REGISTRY
            .lock()
            .retain(|(shared, _)| !std::sync::Weak::ptr_eq(shared, &Arc::downgrade(&self.shared)));
    }

    /// Flush any pending commit and reclaim the file-I/O state. Returns
    /// `None` only if the thread died abnormally (its panic already logged).
    pub(crate) fn shutdown(mut self) -> Option<ManifestIo> {
        // Deregister BEFORE the final flush: a flush_all_agents barrier
        // racing this shutdown must not push an ack the exiting thread will
        // never fire.
        self.deregister();
        let (gb_tx, gb_rx) = flume::bounded::<ManifestIo>(1);
        self.shared.lock().shutdown = Some(gb_tx);
        let _ = self.notify();
        let io = gb_rx.recv().ok();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
        io
    }
}

impl Drop for ManifestSyncAgent {
    /// A handle dropped WITHOUT `shutdown()` (shard-loop panic unwind, early
    /// teardown) must not leave a zombie: the registry's wake-sender clone
    /// would otherwise keep the wake channel connected forever, parking the
    /// sync thread (and its manifest fd) permanently — and the thread's own
    /// strong `Arc` keeps the `strong_count` pruning from ever collecting
    /// the entry. Deregistering here drops that last outside sender, so the
    /// thread's `rx.recv()` disconnects, it flushes the slot, and exits.
    /// Runs after `shutdown()` too (idempotent retain).
    fn drop(&mut self) {
        self.deregister();
    }
}

fn run(mut io: ManifestIo, shared: Arc<Mutex<SyncShared>>, rx: flume::Receiver<()>) {
    while rx.recv().is_ok() {
        // Take the whole state atomically. Anything written after this take
        // comes with its own wake token (writers post the token AFTER the
        // state write; `Full` implies an unconsumed token), so it is handled
        // in a later round — no lost updates.
        let (latest, acks, shutdown) = {
            let mut s = shared.lock();
            (
                s.latest.take(),
                std::mem::take(&mut s.acks),
                s.shutdown.take(),
            )
        };

        let res = match latest {
            Some(root) => {
                let r = io.persist(root);
                // Latch the outcome BEFORE firing acks so a barrier waiter
                // pushed mid-persist (its ack lands in the NEXT round) can
                // never observe a stale "ok" empty-slot answer.
                shared.lock().last_persist_failed = r.is_err();
                if let Err(ref e) = r {
                    tracing::warn!(error = %e, "manifest-sync: commit failed (placement metadata only; AOF replay + orphan sweep recover on restart)");
                }
                r
            }
            // Empty slot: a previous round already persisted a snapshot at
            // least as new as anything these (necessarily-raced) wakes saw —
            // UNLESS that previous round failed, in which case the newest
            // snapshot is known non-durable and the ack must say so (the
            // rewrite-prune barrier skips the prune on this error).
            None => {
                if shared.lock().last_persist_failed {
                    Err(io::Error::other(
                        "manifest-sync: newest snapshot is not durable (last persist failed \
                         and nothing newer has superseded it)",
                    ))
                } else {
                    Ok(())
                }
            }
        };
        for ack in acks {
            let mirrored = match &res {
                Ok(()) => Ok(()),
                // io::Error is not Clone; mirror kind + message per waiter.
                Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
            };
            let _ = ack.send(mirrored);
        }

        if let Some(give_back) = shutdown {
            let _ = give_back.send(io);
            return;
        }
    }
    // Wake channel disconnected without an explicit Shutdown: `ShardManifest`
    // was dropped. Every accepted commit's token is delivered before the
    // disconnect is reported (flume drains queued messages first), so the
    // slot was flushed; `io` (and its fd) drop here.
}

#[cfg(test)]
mod tests {
    use super::super::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
    use std::time::{Duration, Instant};

    fn make_entry(file_id: u64) -> FileEntry {
        FileEntry {
            file_id,
            file_type: crate::persistence::page::PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1,
            byte_size: 4096,
            created_lsn: file_id,
            db_index: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: file_id,
        }
    }

    #[test]
    fn deferred_commit_does_not_block_caller_and_reaches_disk() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-0.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(0);

        m.set_inject_sync_delay_ms(200);
        let t0 = Instant::now();
        m.add_file(make_entry(1));
        m.commit_deferred().expect("deferred commit send");
        let elapsed = t0.elapsed();
        m.set_inject_sync_delay_ms(0);
        assert!(
            elapsed < Duration::from_millis(100),
            "deferred commit must not block the caller for the injected sync \
             delay (took {elapsed:?})"
        );

        // Shutdown flushes pending commits and reclaims the io handle.
        m.shutdown_deferred();
        drop(m);
        let reopened = ShardManifest::open(&path).expect("reopen");
        assert_eq!(reopened.files().len(), 1, "deferred commit must reach disk");
    }

    /// D1 (deep review): the AOF rewrite prunes the old incr — the durability
    /// backstop for spill placements whose manifest commit is still sitting
    /// in this agent's deferred slot. The prune path needs a barrier that
    /// forces every registered agent's pending snapshot durable first.
    #[test]
    fn flush_all_agents_forces_pending_deferred_commit_durable() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-9.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(9);

        m.set_inject_sync_delay_ms(100);
        m.add_file(make_entry(41));
        m.commit_deferred().expect("deferred send");
        // Barrier must block until the deferred snapshot (or newer) is durable.
        super::flush_all_agents().expect("flush barrier");
        m.set_inject_sync_delay_ms(0);

        // No shutdown flush — the barrier alone must have persisted it.
        let reopened = ShardManifest::open(&path).expect("reopen");
        assert_eq!(
            reopened.files().len(),
            1,
            "flush_all_agents must persist the pending deferred snapshot"
        );
        m.shutdown_deferred();
    }

    /// The barrier must not hang (and must succeed) when agents are idle.
    ///
    /// Takes the registry lock: `flush_all_agents` walks the process-global
    /// registry, so a concurrently-registered agent from another test — worse,
    /// one with a latched persist failure — would fail this assertion. It is
    /// the same unguarded-reader shape as moon#750, in the one piece of state
    /// that genuinely has to stay global.
    #[test]
    fn flush_all_agents_is_noop_when_idle() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        super::flush_all_agents().expect("idle barrier must succeed");
    }

    #[test]
    fn durable_commit_blocks_until_persisted() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-1.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(1);

        m.set_inject_sync_delay_ms(150);
        let t0 = Instant::now();
        m.add_file(make_entry(7));
        m.commit().expect("durable commit");
        let elapsed = t0.elapsed();
        m.set_inject_sync_delay_ms(0);
        assert!(
            elapsed >= Duration::from_millis(150),
            "durable commit must wait for the persist (took {elapsed:?})"
        );

        // Durable means durable NOW: reopen from a second handle without any
        // flush and the entry must already be there.
        let reopened = ShardManifest::open(&path).expect("reopen");
        assert_eq!(reopened.files().len(), 1);
        m.shutdown_deferred();
    }

    #[test]
    fn queued_deferred_commits_coalesce_to_fewer_persists() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-2.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(2);

        // Get the worker INSIDE a stalled persist before the burst, and keep
        // every persist stalled until after the shutdown flush — otherwise
        // scheduling can let the worker race the burst commit-by-commit and
        // weaken the coalescing bound into flakiness.
        m.set_inject_sync_delay_ms(100);
        m.add_file(make_entry(99));
        m.commit_deferred().expect("priming deferred send");
        std::thread::sleep(Duration::from_millis(50));
        for i in 0..20u64 {
            m.add_file(make_entry(100 + i));
            m.commit_deferred().expect("deferred send");
        }
        m.shutdown_deferred();
        m.set_inject_sync_delay_ms(0);
        // Per-manifest counter (moon#750): it starts at 0 for this manifest and
        // no other test's commits can inflate it, so no before/after delta.
        let persists = m.persist_count();
        // Lower bound too: a counter that always read 0 would satisfy
        // `< 20` while proving nothing. The priming commit plus the shutdown
        // flush guarantee at least one persist actually ran.
        assert!(
            (1..20).contains(&persists),
            "20 queued deferred commits must coalesce into fewer persists, and \
             at least one persist must actually have run (saw {persists})"
        );

        drop(m);
        let reopened = ShardManifest::open(&path).expect("reopen");
        assert_eq!(
            reopened.files().len(),
            21,
            "coalescing must still land the final (superset) snapshot"
        );
    }

    /// The old bounded(64) request queue blocked the CALLER once a stalled
    /// persist let 64 snapshots pile up — a saturated-disk fsync stall would
    /// re-create on the shard thread exactly the stall the agent exists to
    /// remove. The latest-slot handoff must absorb an arbitrarily large burst
    /// without the caller ever waiting on the persist.
    #[test]
    fn burst_of_deferred_commits_never_blocks_the_caller() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-4.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(4);

        // Stall every persist and get the worker INSIDE one before bursting:
        // a burst fired while the worker is still draining its queue gets
        // consumed as fast as it is sent, which is why a plain burst never
        // reproduced the block. Blocking needs commits to arrive while the
        // worker is stuck in persist with no one draining.
        m.set_inject_sync_delay_ms(400);
        m.add_file(make_entry(999));
        m.commit_deferred().expect("priming deferred send");
        std::thread::sleep(Duration::from_millis(50));

        let t0 = Instant::now();
        for i in 0..100u64 {
            m.add_file(make_entry(1000 + i));
            m.commit_deferred().expect("deferred send");
        }
        let elapsed = t0.elapsed();
        m.set_inject_sync_delay_ms(0);
        assert!(
            elapsed < Duration::from_millis(200),
            "a 100-commit burst against a stalled persist must not block the \
             caller (took {elapsed:?})"
        );

        m.shutdown_deferred();
        drop(m);
        let reopened = ShardManifest::open(&path).expect("reopen");
        assert_eq!(
            reopened.files().len(),
            101,
            "the final (superset) snapshot must land despite coalescing"
        );
    }

    /// Deep-review P1: a FAILED deferred persist must fail the
    /// `flush_all_agents` barrier (empty-slot rounds included) until a newer
    /// snapshot persists successfully — otherwise the AOF rewrite prunes the
    /// old incr while the on-disk manifest is missing spill placements.
    #[test]
    fn failed_persist_latches_flush_barrier_until_superseded() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-5.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(5);

        m.set_inject_persist_error(true);
        m.add_file(make_entry(1));
        m.commit_deferred().expect("deferred send");
        // Whether the failing round has already consumed the slot or the
        // barrier's own round persists (and fails) it, the barrier must
        // report the newest snapshot as non-durable.
        assert!(
            super::flush_all_agents().is_err(),
            "barrier must fail while the newest snapshot is not durable"
        );
        // Still latched on a second, empty-slot barrier round.
        assert!(
            super::flush_all_agents().is_err(),
            "empty-slot rounds must stay latched after a failed persist"
        );

        // A newer snapshot that persists successfully heals the latch.
        m.set_inject_persist_error(false);
        m.add_file(make_entry(2));
        m.commit_deferred().expect("deferred send");
        assert!(
            super::flush_all_agents().is_ok(),
            "a successful newer persist must clear the latch"
        );
        m.shutdown_deferred();
    }

    /// Deep-review P2: dropping a deferred-mode manifest WITHOUT
    /// `shutdown_deferred` (panic unwind, early teardown) must deregister
    /// the agent and let its thread exit — the registry's wake-sender clone
    /// must not park the thread (and its manifest fd) forever.
    #[test]
    fn dropped_agent_without_shutdown_deregisters() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let before = super::AGENT_REGISTRY.lock().len();
        {
            let path = tmp.path().join("shard-6.manifest");
            let mut m = ShardManifest::create(&path).expect("create");
            m.enable_deferred_sync(6);
            m.add_file(make_entry(1));
            m.commit_deferred().expect("deferred send");
            // No shutdown_deferred: the agent handle drops with the manifest.
        }
        assert_eq!(
            super::AGENT_REGISTRY.lock().len(),
            before,
            "agent drop must deregister its registry entry"
        );
        // The barrier must not hang or error on the departed agent.
        assert!(super::flush_all_agents().is_ok());
    }

    #[test]
    fn shutdown_reclaims_io_and_inline_commits_still_work() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _registry = super::super::manifest::TEST_AGENT_REGISTRY_LOCK
            .lock()
            .unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-3.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(3);
        m.add_file(make_entry(1));
        m.commit_deferred().expect("deferred send");
        m.shutdown_deferred();

        // Post-shutdown the manifest is inline again: durable commit works.
        m.add_file(make_entry(2));
        m.commit().expect("inline commit after shutdown");
        drop(m);
        let reopened = ShardManifest::open(&path).expect("reopen");
        assert_eq!(reopened.files().len(), 2);
    }
}
