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
        }));
        let (wake, rx) = flume::bounded(1);
        let (io_tx, io_rx) = flume::bounded::<ManifestIo>(1);
        let thread_shared = Arc::clone(&shared);
        match std::thread::Builder::new()
            .name(format!("manifest-sync-{shard_id}"))
            .spawn(move || {
                // The spawner sends the io immediately after a successful
                // spawn; Err here means the agent was dropped first — exit.
                let Ok(io) = io_rx.recv() else { return };
                run(io, thread_shared, rx)
            }) {
            Ok(join) => match io_tx.send(io) {
                Ok(()) => Ok(Self {
                    shared,
                    wake,
                    join: Some(join),
                }),
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

    /// Flush any pending commit and reclaim the file-I/O state. Returns
    /// `None` only if the thread died abnormally (its panic already logged).
    pub(crate) fn shutdown(mut self) -> Option<ManifestIo> {
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
                if let Err(ref e) = r {
                    tracing::warn!(error = %e, "manifest-sync: commit failed (placement metadata only; AOF replay + orphan sweep recover on restart)");
                }
                r
            }
            // Empty slot: a previous round already persisted a snapshot at
            // least as new as anything these (necessarily-raced) wakes saw.
            None => Ok(()),
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
        let _knob = super::super::manifest::TEST_SYNC_KNOB_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-0.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(0);

        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(200, std::sync::atomic::Ordering::SeqCst);
        let t0 = Instant::now();
        m.add_file(make_entry(1));
        m.commit_deferred().expect("deferred commit send");
        let elapsed = t0.elapsed();
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(0, std::sync::atomic::Ordering::SeqCst);
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

    #[test]
    fn durable_commit_blocks_until_persisted() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _knob = super::super::manifest::TEST_SYNC_KNOB_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-1.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(1);

        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(150, std::sync::atomic::Ordering::SeqCst);
        let t0 = Instant::now();
        m.add_file(make_entry(7));
        m.commit().expect("durable commit");
        let elapsed = t0.elapsed();
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(0, std::sync::atomic::Ordering::SeqCst);
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
        let _knob = super::super::manifest::TEST_SYNC_KNOB_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-2.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(2);

        let before =
            super::super::manifest::TEST_PERSIST_COUNT.load(std::sync::atomic::Ordering::SeqCst);
        // Get the worker INSIDE a stalled persist before the burst, and keep
        // every persist stalled until after the shutdown flush — otherwise
        // scheduling can let the worker race the burst commit-by-commit and
        // weaken the coalescing bound into flakiness.
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(100, std::sync::atomic::Ordering::SeqCst);
        m.add_file(make_entry(99));
        m.commit_deferred().expect("priming deferred send");
        std::thread::sleep(Duration::from_millis(50));
        for i in 0..20u64 {
            m.add_file(make_entry(100 + i));
            m.commit_deferred().expect("deferred send");
        }
        m.shutdown_deferred();
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(0, std::sync::atomic::Ordering::SeqCst);
        let persists = super::super::manifest::TEST_PERSIST_COUNT
            .load(std::sync::atomic::Ordering::SeqCst)
            - before;
        assert!(
            persists < 20,
            "20 queued deferred commits must coalesce (saw {persists} persists)"
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
        let _knob = super::super::manifest::TEST_SYNC_KNOB_LOCK.lock().unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("shard-4.manifest");
        let mut m = ShardManifest::create(&path).expect("create");
        m.enable_deferred_sync(4);

        // Stall every persist and get the worker INSIDE one before bursting:
        // a burst fired while the worker is still draining its queue gets
        // consumed as fast as it is sent, which is why a plain burst never
        // reproduced the block. Blocking needs commits to arrive while the
        // worker is stuck in persist with no one draining.
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(400, std::sync::atomic::Ordering::SeqCst);
        m.add_file(make_entry(999));
        m.commit_deferred().expect("priming deferred send");
        std::thread::sleep(Duration::from_millis(50));

        let t0 = Instant::now();
        for i in 0..100u64 {
            m.add_file(make_entry(1000 + i));
            m.commit_deferred().expect("deferred send");
        }
        let elapsed = t0.elapsed();
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(0, std::sync::atomic::Ordering::SeqCst);
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

    #[test]
    fn shutdown_reclaims_io_and_inline_commits_still_work() {
        #[allow(clippy::unwrap_used)] // test-only; poisoning would already be a failed test
        let _knob = super::super::manifest::TEST_SYNC_KNOB_LOCK.lock().unwrap();
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
