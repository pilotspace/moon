//! Persistence command handlers (BGSAVE, BGREWRITEAOF).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use tracing::{error, info};

use crate::persistence::aof::{AofMessage, AofPoolSendError, AofWriterPool};
use crate::persistence::rdb;
use crate::protocol::Frame;
use crate::storage::Database;

/// Type alias for the per-database RwLock container.
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Global epoch counter for snapshot coordination across shards.
pub static SNAPSHOT_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Global flag indicating whether a background save is in progress.
pub static SAVE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Unix timestamp of last successful save (SAVE or BGSAVE).
pub static LAST_SAVE_TIME: AtomicU64 = AtomicU64::new(0);

/// Counter for shards that have completed the current snapshot.
/// When this reaches `num_shards`, SAVE_IN_PROGRESS is cleared.
pub static BGSAVE_SHARDS_REMAINING: AtomicU64 = AtomicU64::new(0);

/// Whether the last BGSAVE completed successfully.
pub static BGSAVE_LAST_STATUS: AtomicBool = AtomicBool::new(true);

/// Process-wide gate set at startup when the configuration combination
/// `--shards >= 2 + --appendonly yes` is selected (see `Config::per_shard_aof_active`).
///
/// `BGREWRITEAOF` under this combination silently truncates the WAL of every
/// shard except the rewriter's own shard while the consolidated multi-part AOF
/// base RDB written by the rewrite is **not** consumed on restart (verified
/// 2026-05-26 against HEAD `6e49050`: 38 % data loss reproducible). Until the
/// v2.0 multi-part AOF replay walks every shard's segment manifest, the only
/// safe behavior is to refuse the command in this config and point operators
/// at the runbook.
///
/// Note: `--disk-offload` is NOT part of the gate condition. The unsafe flag
/// fires for any `--shards >= 2 + --appendonly yes` combination regardless of
/// disk-offload state.
///
/// Set once in `main.rs` after CLI parsing; never cleared. Checked by
/// `bgrewriteaof_start_sharded` before dispatching the rewrite message.
pub static MULTI_SHARD_AOF_REWRITE_UNSAFE: AtomicBool = AtomicBool::new(false);

/// Global flag indicating whether a BGREWRITEAOF rewrite is currently in progress.
///
/// Set to `true` when `bgrewriteaof_start` or `bgrewriteaof_start_sharded` dispatches
/// a rewrite request to the AOF writer task.  Cleared by the AOF writer task itself
/// (in `src/persistence/aof.rs`) after `do_rewrite_single` / `do_rewrite_sharded` /
/// `rewrite_aof` returns (success or failure).
///
/// Used by SWAPDB to reject concurrent rewrite: the AOF writer snapshots the
/// databases at rewrite-start; a SWAPDB mid-rewrite would corrupt the snapshot.
pub static AOF_REWRITE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Start a background RDB save (BGSAVE command).
///
/// Clones all database entries under the lock, then spawns a blocking task
/// to serialize and write the RDB file. Returns immediately with a status message.
///
/// Returns an error frame if a save is already in progress.
pub fn bgsave_start(db: SharedDatabases, dir: String, dbfilename: String) -> Frame {
    // Check if a save is already running
    if SAVE_IN_PROGRESS.swap(true, Ordering::SeqCst) {
        return Frame::Error(Bytes::from_static(
            b"ERR Background save already in progress",
        ));
    }

    // Clone snapshot: lock each db individually with read lock
    // Include base_timestamp for TTL delta resolution during serialization
    let snapshot: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let base_ts = guard.base_timestamp();
            let entries = guard
                .data()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (entries, base_ts)
        })
        .collect();

    let path = PathBuf::from(dir).join(dbfilename);

    #[cfg(feature = "runtime-tokio")]
    tokio::task::spawn_blocking(move || {
        match rdb::save_from_snapshot(&snapshot, &path) {
            Ok(()) => {
                info!("Background RDB save completed: {}", path.display());
                BGSAVE_LAST_STATUS.store(true, Ordering::Relaxed);
                LAST_SAVE_TIME.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    Ordering::Relaxed,
                );
            }
            Err(e) => {
                error!("Background RDB save failed: {}", e);
                BGSAVE_LAST_STATUS.store(false, Ordering::Relaxed);
            }
        }
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    });

    #[cfg(feature = "runtime-monoio")]
    {
        // Monoio: synchronous save (no spawn_blocking available).
        // This blocks the current thread but is acceptable for the monoio
        // thread-per-core model where persistence is a rare operation.
        match rdb::save_from_snapshot(&snapshot, &path) {
            Ok(()) => {
                info!("Background RDB save completed: {}", path.display());
                BGSAVE_LAST_STATUS.store(true, Ordering::Relaxed);
                LAST_SAVE_TIME.store(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    Ordering::Relaxed,
                );
            }
            Err(e) => {
                error!("Background RDB save failed: {}", e);
                BGSAVE_LAST_STATUS.store(false, Ordering::Relaxed);
            }
        }
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    }

    Frame::SimpleString(Bytes::from_static(b"Background saving started"))
}

/// Start a cooperative per-shard snapshot (BGSAVE command, sharded mode).
///
/// Bumps the global epoch and broadcasts via the watch channel. Every shard's
/// event loop checks the watch channel on its periodic 1ms tick and starts its
/// own snapshot independently. Completion is tracked via BGSAVE_SHARDS_REMAINING
/// atomic counter — the last shard to finish clears SAVE_IN_PROGRESS.
///
/// This approach works reliably across both Tokio and Monoio because it uses
/// the same watch-channel path as auto-save, avoiding SPSC (which has no
/// self-producer for the local shard).
pub fn bgsave_start_sharded(
    snapshot_trigger: &crate::runtime::channel::WatchSender<u64>,
    num_shards: usize,
) -> Frame {
    if SAVE_IN_PROGRESS.swap(true, Ordering::SeqCst) {
        return Frame::Error(Bytes::from_static(
            b"ERR Background save already in progress",
        ));
    }

    let epoch = SNAPSHOT_EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
    BGSAVE_SHARDS_REMAINING.store(num_shards as u64, Ordering::SeqCst);

    // Broadcast epoch to all shards via watch channel.
    // Each shard picks this up on its next timer tick and starts its snapshot.
    let _ = snapshot_trigger.send(epoch);

    info!(
        "BGSAVE triggered: epoch {} across {} shards",
        epoch, num_shards
    );
    Frame::SimpleString(Bytes::from_static(b"Background saving started"))
}

/// Called by each shard after its snapshot completes (monoio runtime).
///
/// Decrements the shared counter. When the last shard finishes,
/// clears SAVE_IN_PROGRESS and updates LAST_SAVE_TIME.
pub fn bgsave_shard_done(success: bool) {
    if !success {
        BGSAVE_LAST_STATUS.store(false, Ordering::Relaxed);
    }
    // Use compare-exchange loop to prevent underflow below zero.
    // If the counter is already 0 (spurious call), do nothing.
    loop {
        let current = BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst);
        if current == 0 {
            tracing::warn!("BGSAVE shard done called with counter already at 0 -- ignoring");
            return;
        }
        match BGSAVE_SHARDS_REMAINING.compare_exchange(
            current,
            current - 1,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(prev) => {
                if prev == 1 {
                    // Last shard to finish
                    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
                    LAST_SAVE_TIME.store(
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        Ordering::Relaxed,
                    );
                    info!("BGSAVE complete: all shards finished");
                }
                return;
            }
            Err(_) => continue, // CAS retry
        }
    }
}

/// Translate an `AofWriterPool` send failure into a user-facing RESP error.
/// Under PerShard layout, `pool.try_send_rewrite` returns
/// `RewriteUnsupportedInPerShard` — the per-shard rewrite path lands in
/// step 6 of the per-shard AOF RFC. Until then BGREWRITEAOF refuses with
/// a stable error rather than silently no-op'ing.
fn rewrite_pool_error_frame(err: AofPoolSendError) -> Frame {
    match err {
        AofPoolSendError::RewriteUnsupportedInPerShard => Frame::Error(Bytes::from_static(
            b"ERR BGREWRITEAOF is not yet supported under per-shard AOF layout; per-shard rewrite ships in step 6 of the per-shard AOF migration",
        )),
        AofPoolSendError::SendFailed => Frame::Error(Bytes::from_static(
            b"ERR Background AOF rewrite failed to start",
        )),
    }
}

/// Start a background AOF rewrite (BGREWRITEAOF command).
///
/// Submits a Rewrite message through the writer pool, which generates
/// synthetic commands from current database state and replaces the AOF
/// file.
///
/// Uses CAS to set `AOF_REWRITE_IN_PROGRESS`: if a rewrite is already running,
/// returns an error immediately without corrupting the in-flight rewrite state.
pub fn bgrewriteaof_start(pool: &AofWriterPool, db: SharedDatabases) -> Frame {
    // CAS: only proceed if currently false; prevents a second caller from
    // clearing the flag while the first rewrite is still in progress.
    if AOF_REWRITE_IN_PROGRESS
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Frame::Error(Bytes::from_static(
            b"ERR Background AOF rewrite already in progress",
        ));
    }
    match pool.try_send_rewrite(AofMessage::Rewrite(db)) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(
            b"Background append only file rewriting started",
        )),
        Err(e) => {
            // Send failed (channel full) or PerShard rejection — rewrite never
            // started, so clear the in-progress flag we just set.
            AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
            rewrite_pool_error_frame(e)
        }
    }
}

/// Start BGREWRITEAOF in sharded mode using ShardDatabases.
///
/// Uses CAS to set `AOF_REWRITE_IN_PROGRESS`: if a rewrite is already running,
/// returns an error immediately without corrupting the in-flight rewrite state.
pub fn bgrewriteaof_start_sharded(
    pool: &AofWriterPool,
    shard_databases: std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
) -> Frame {
    // Refuse the rewrite under the known-unsafe config combo (see the
    // MULTI_SHARD_AOF_REWRITE_UNSAFE doc comment).  This is the
    // single-node v1.0-rc1 gate; the v2.0 multi-part AOF replay fix lifts
    // it.
    if MULTI_SHARD_AOF_REWRITE_UNSAFE.load(Ordering::Relaxed) {
        return Frame::Error(Bytes::from_static(
            b"ERR BGREWRITEAOF is not yet supported for --shards >= 2 + --appendonly yes. Options: (1) use --shards 1, (2) set --appendonly no, or (3) wait for per-shard BGREWRITEAOF in v0.2. See docs/runbooks/multi-shard-aof-rewrite.md.",
        ));
    }
    // CAS: only proceed if currently false; prevents a second caller from
    // clearing the flag while the first rewrite is still in progress.
    if AOF_REWRITE_IN_PROGRESS
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return Frame::Error(Bytes::from_static(
            b"ERR Background AOF rewrite already in progress",
        ));
    }

    // [F6] PerShard pools use the per-shard fan-out (synchronized seq bump +
    // single manifest commit across all writers). TopLevel multi-DB pools keep
    // the legacy single-writer RewriteSharded path.
    if pool.layout() == crate::persistence::aof_manifest::AofLayout::PerShard {
        // Per-shard fan-out for BOTH runtimes. The fold is synchronous std::fs
        // IO; on monoio the writer runs it inline, on tokio the writer converts
        // its `tokio::fs` handle to `std::fs` for the fold's duration (both run
        // on a dedicated block_on_local thread, so blocking is safe). The fold
        // body — and therefore the exactly-once invariant — is identical.
        //
        // try_send_rewrite_per_shard loads the manifest, builds the shared
        // coordinator, and reliably fans out to every writer. The in-progress
        // flag is cleared by the coordinator's final commit
        // (PerShardRewriteCoord::shard_done), not here.
        match pool.try_send_rewrite_per_shard(shard_databases) {
            Ok(()) => {
                return Frame::SimpleString(Bytes::from_static(
                    b"Background append only file rewriting started",
                ));
            }
            Err(e) => {
                AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
                return rewrite_pool_error_frame(e);
            }
        }
    }

    match pool.try_send_rewrite(AofMessage::RewriteSharded(shard_databases)) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(
            b"Background append only file rewriting started",
        )),
        Err(e) => {
            // Send failed (channel full) or PerShard rejection — rewrite never
            // started, so clear the in-progress flag we just set.
            AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
            rewrite_pool_error_frame(e)
        }
    }
}

/// SAVE command: synchronous save to disk. Blocks until complete.
///
/// Clones all entries under read locks (same as BGSAVE), then serializes
/// synchronously. Not supported in sharded mode -- use BGSAVE instead.
pub fn handle_save(db: &SharedDatabases, dir: &str, dbfilename: &str) -> Frame {
    if SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
        return Frame::Error(Bytes::from_static(
            b"ERR Background save already in progress",
        ));
    }

    // Clone snapshot: lock each db individually with read lock (same pattern as bgsave_start)
    let snapshot: Vec<(
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
        u32,
    )> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let base_ts = guard.base_timestamp();
            let entries = guard
                .data()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (entries, base_ts)
        })
        .collect();

    let path = PathBuf::from(dir).join(dbfilename);
    match rdb::save_from_snapshot(&snapshot, &path) {
        Ok(()) => {
            LAST_SAVE_TIME.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                Ordering::Relaxed,
            );
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        Err(e) => Frame::Error(Bytes::from(format!("ERR {}", e))),
    }
}

/// LASTSAVE command: returns Unix timestamp of last successful save.
pub fn handle_lastsave() -> Frame {
    let ts = LAST_SAVE_TIME.load(Ordering::Relaxed);
    Frame::Integer(ts as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_in_progress_flag() {
        // Reset flag
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        assert!(!SAVE_IN_PROGRESS.load(Ordering::SeqCst));

        // Simulate setting it
        SAVE_IN_PROGRESS.store(true, Ordering::SeqCst);
        assert!(SAVE_IN_PROGRESS.load(Ordering::SeqCst));

        // Reset
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    }

    #[test]
    fn test_last_save_time_initial() {
        // LAST_SAVE_TIME starts at 0 (no save has occurred yet)
        // Note: other tests may have modified this, so just verify the static exists
        // and is an AtomicU64 that can be loaded.
        let _ = LAST_SAVE_TIME.load(Ordering::Relaxed);
    }

    #[test]
    fn test_bgsave_last_status_initial() {
        // BGSAVE_LAST_STATUS starts as true (no failure has occurred)
        // Note: verify the static exists and is accessible
        let _ = BGSAVE_LAST_STATUS.load(Ordering::Relaxed);
    }

    // Serialize the multi-shard gate test against any other test mutating
    // the gate or AOF_REWRITE_IN_PROGRESS (parallel test runner otherwise
    // races on the process-wide AtomicBools).
    static GATE_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    #[test]
    fn test_bgrewriteaof_sharded_refuses_under_unsafe_config() {
        let _guard = GATE_TEST_LOCK.lock();
        // Use a small bounded channel so the test does not need an AOF
        // writer task; the gate must fire BEFORE try_send is reached.
        // Wrap as a TopLevel pool to match the post-2e-β helper signature.
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(1);
        let pool = AofWriterPool::top_level(tx);
        let shard_dbs = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
            crate::storage::Database::new(),
        ]]);

        // Snapshot prior state so the test is order-independent.
        let prior = MULTI_SHARD_AOF_REWRITE_UNSAFE.load(Ordering::Relaxed);
        let prior_in_progress = AOF_REWRITE_IN_PROGRESS.load(Ordering::SeqCst);
        AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);

        // Gate ON → must refuse with the documented ERR (and must NOT flip
        // AOF_REWRITE_IN_PROGRESS, otherwise a normal rewrite gets blocked).
        MULTI_SHARD_AOF_REWRITE_UNSAFE.store(true, Ordering::Relaxed);
        let frame = bgrewriteaof_start_sharded(&pool, shard_dbs.clone());
        match frame {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(
                    s.contains("BGREWRITEAOF is not yet supported")
                        && s.contains("multi-shard-aof-rewrite.md"),
                    "unexpected error: {s}"
                );
            }
            other => panic!("expected Frame::Error, got {other:?}"),
        }
        assert!(
            !AOF_REWRITE_IN_PROGRESS.load(Ordering::SeqCst),
            "gate must not set AOF_REWRITE_IN_PROGRESS"
        );

        // Gate OFF → the gate error must NOT fire. (Without an AOF writer
        // task draining the channel, the second call may succeed or return
        // "failed to start" depending on buffer state; the contract under
        // test here is only that the gate error is gone.)
        MULTI_SHARD_AOF_REWRITE_UNSAFE.store(false, Ordering::Relaxed);
        AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
        let frame2 = bgrewriteaof_start_sharded(&pool, shard_dbs);
        if let Frame::Error(msg) = &frame2 {
            let s = std::str::from_utf8(msg).unwrap();
            assert!(
                !s.contains("BGREWRITEAOF is not yet supported"),
                "gate error fired with gate off: {s}"
            );
        }

        // Restore prior state.
        AOF_REWRITE_IN_PROGRESS.store(prior_in_progress, Ordering::SeqCst);
        MULTI_SHARD_AOF_REWRITE_UNSAFE.store(prior, Ordering::Relaxed);
    }

    /// FIX-W1-4 r2: gate error message must NOT mention disk-offload (the gate
    /// fires for ANY `--shards >= 2 + --appendonly yes` config, regardless of
    /// disk-offload setting) and MUST end with the runbook reference.
    ///
    /// Red state (pre-fix, 881f8b8^): error contained "disk-offload enable"
    /// and recommended "set --disk-offload disable" — stale from the narrower
    /// original gate condition.
    ///
    /// Green (post-fix): message updated to accurate condition, no disk-offload
    /// mention, ends with "multi-shard-aof-rewrite.md."
    #[test]
    fn test_bgrewriteaof_gate_error_no_disk_offload_mention() {
        let _guard = GATE_TEST_LOCK.lock();
        let (tx, _rx) = crate::runtime::channel::mpsc_bounded::<AofMessage>(1);
        let pool = AofWriterPool::top_level(tx);
        let shard_dbs = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
            crate::storage::Database::new(),
        ]]);

        let prior = MULTI_SHARD_AOF_REWRITE_UNSAFE.load(Ordering::Relaxed);
        let prior_in_progress = AOF_REWRITE_IN_PROGRESS.load(Ordering::SeqCst);
        AOF_REWRITE_IN_PROGRESS.store(false, Ordering::SeqCst);
        MULTI_SHARD_AOF_REWRITE_UNSAFE.store(true, Ordering::Relaxed);

        let frame = bgrewriteaof_start_sharded(&pool, shard_dbs);

        MULTI_SHARD_AOF_REWRITE_UNSAFE.store(prior, Ordering::Relaxed);
        AOF_REWRITE_IN_PROGRESS.store(prior_in_progress, Ordering::SeqCst);

        match frame {
            Frame::Error(msg) => {
                let s = std::str::from_utf8(&msg).unwrap();
                assert!(
                    !s.contains("disk-offload"),
                    "gate error must NOT mention disk-offload \
                     (gate fires for --shards>=2 + --appendonly yes regardless \
                     of disk-offload state): {s}"
                );
                assert!(
                    s.ends_with("multi-shard-aof-rewrite.md."),
                    "gate error MUST end with the runbook reference \
                     'multi-shard-aof-rewrite.md.' for operator guidance: {s}"
                );
                assert!(
                    s.contains("--shards 1") && s.contains("--appendonly no"),
                    "gate error must offer actionable alternatives \
                     (--shards 1 and --appendonly no): {s}"
                );
            }
            other => panic!("expected Frame::Error when gate is ON, got {other:?}"),
        }
    }
}
