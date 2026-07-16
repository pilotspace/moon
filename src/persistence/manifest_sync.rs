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
//! Two commit flavors:
//! - **Deferred** (`commit_deferred`): fire-and-forget snapshot send; the
//!   spill-completion path uses this. Consecutive queued snapshots coalesce —
//!   each is a complete root, so only the newest needs to reach disk.
//! - **Durable** (`commit_durable`): send + block on ack. Paths where the
//!   manifest IS the durability record (checkpoint protocol, no-AOF durable
//!   batch spill) keep exactly their old blocking semantics through this.
//!
//! Coalescing + ack correctness: when several commits coalesce, every ack is
//! fired after the NEWEST snapshot persists. A newer root strictly supersedes
//! an older one (each snapshot is the full entry list at a higher epoch), so
//! "your commit is durable" remains true for the older requester.

use std::io;

use super::manifest::{ManifestIo, ManifestRoot};

/// Queue depth for pending commit requests. Deferred sends block only if the
/// agent falls this many complete snapshots behind (each bounded by one
/// `persist`, i.e. ~2 fsyncs) — bounded, and still off the per-file cadence
/// the loop used to pay.
const QUEUE_CAP: usize = 64;

pub(crate) enum SyncRequest {
    Commit {
        root: ManifestRoot,
        ack: Option<flume::Sender<io::Result<()>>>,
    },
    Shutdown {
        give_back: flume::Sender<ManifestIo>,
    },
}

/// Handle to the per-shard manifest-sync thread. Owned by `ShardManifest`
/// while deferred sync is enabled; `shutdown()` reclaims the [`ManifestIo`]
/// so post-shutdown commits can run inline again.
pub(crate) struct ManifestSyncAgent {
    tx: flume::Sender<SyncRequest>,
    join: Option<std::thread::JoinHandle<()>>,
}

impl std::fmt::Debug for ManifestSyncAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestSyncAgent")
            .field("queued", &self.tx.len())
            .finish()
    }
}

impl ManifestSyncAgent {
    pub(crate) fn spawn(io: ManifestIo, shard_id: usize) -> Self {
        let (tx, rx) = flume::bounded(QUEUE_CAP);
        let join = match std::thread::Builder::new()
            .name(format!("manifest-sync-{shard_id}"))
            .spawn(move || run(io, rx))
        {
            Ok(j) => Some(j),
            Err(e) => {
                // Receiver is dropped with the failed spawn, so every send
                // fails loudly; durable callers surface the error, deferred
                // callers warn. No silent no-op.
                tracing::error!(shard_id, error = %e, "manifest-sync: thread spawn failed");
                None
            }
        };
        Self { tx, join }
    }

    /// Fire-and-forget commit of a complete root snapshot.
    pub(crate) fn commit_deferred(&self, root: ManifestRoot) -> io::Result<()> {
        self.tx
            .send(SyncRequest::Commit { root, ack: None })
            .map_err(|_| io::Error::other("manifest-sync thread unavailable"))
    }

    /// Commit and block until the snapshot (or a newer one) is durable.
    pub(crate) fn commit_durable(&self, root: ManifestRoot) -> io::Result<()> {
        let (ack_tx, ack_rx) = flume::bounded::<io::Result<()>>(1);
        self.tx
            .send(SyncRequest::Commit {
                root,
                ack: Some(ack_tx),
            })
            .map_err(|_| io::Error::other("manifest-sync thread unavailable"))?;
        ack_rx
            .recv()
            .map_err(|_| io::Error::other("manifest-sync thread died before ack"))?
    }

    /// Flush all pending commits and reclaim the file-I/O state. Returns
    /// `None` only if the thread died abnormally (its panic already logged).
    pub(crate) fn shutdown(mut self) -> Option<ManifestIo> {
        let (gb_tx, gb_rx) = flume::bounded::<ManifestIo>(1);
        let _ = self.tx.send(SyncRequest::Shutdown { give_back: gb_tx });
        let io = gb_rx.recv().ok();
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
        io
    }
}

fn run(mut io: ManifestIo, rx: flume::Receiver<SyncRequest>) {
    while let Ok(req) = rx.recv() {
        let (mut latest, mut acks) = match req {
            SyncRequest::Shutdown { give_back } => {
                let _ = give_back.send(io);
                return;
            }
            SyncRequest::Commit { root, ack } => (root, ack.into_iter().collect::<Vec<_>>()),
        };

        // Coalesce everything already queued: each snapshot is a complete
        // root at a monotonically higher epoch, so only the newest must hit
        // disk. Acks collected along the way fire after that newest persist.
        let mut pending_shutdown: Option<flume::Sender<ManifestIo>> = None;
        while let Ok(more) = rx.try_recv() {
            match more {
                SyncRequest::Commit { root, ack } => {
                    latest = root;
                    acks.extend(ack);
                }
                SyncRequest::Shutdown { give_back } => {
                    pending_shutdown = Some(give_back);
                    break;
                }
            }
        }

        let res = io.persist(latest);
        if let Err(ref e) = res {
            tracing::warn!(error = %e, "manifest-sync: commit failed (placement metadata only; AOF replay + orphan sweep recover on restart)");
        }
        for ack in acks {
            let mirrored = match &res {
                Ok(()) => Ok(()),
                // io::Error is not Clone; mirror kind + message per waiter.
                Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
            };
            let _ = ack.send(mirrored);
        }

        if let Some(give_back) = pending_shutdown {
            let _ = give_back.send(io);
            return;
        }
    }
    // Channel disconnected without an explicit Shutdown: `ShardManifest` was
    // dropped. flume delivers everything already queued before reporting the
    // disconnect, so no accepted commit is lost; `io` (and its fd) drop here.
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
            min_key_hash: 0,
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
        // Stall the agent on its first persist so the rest of the burst
        // queues up behind it and coalesces.
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(100, std::sync::atomic::Ordering::SeqCst);
        for i in 0..20u64 {
            m.add_file(make_entry(100 + i));
            m.commit_deferred().expect("deferred send");
        }
        super::super::manifest::TEST_INJECT_SYNC_DELAY_MS
            .store(0, std::sync::atomic::Ordering::SeqCst);
        m.shutdown_deferred();
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
            20,
            "coalescing must still land the final (superset) snapshot"
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
