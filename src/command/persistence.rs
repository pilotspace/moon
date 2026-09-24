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

/// Whether any shard of the sharded save in progress has failed (moon#1230).
/// Cleared when a sharded save starts; the last shard to finish publishes it
/// into [`BGSAVE_LAST_STATUS`]. A per-save latch, because the status must
/// describe THAT save: `ok` when every shard succeeded, `err` when one did
/// not — and back to `ok` on the next save that succeeds, as redis does.
static BGSAVE_CURRENT_FAILED: AtomicBool = AtomicBool::new(false);

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
    let snapshot: Vec<
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
    > = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            guard
                .data()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .collect();

    let path = PathBuf::from(dir).join(dbfilename);

    #[cfg(feature = "runtime-tokio")]
    tokio::task::spawn_blocking(move || {
        match rdb::save_from_snapshot(&snapshot, &path) {
            Ok(()) => {
                info!("Background RDB save completed: {}", path.display());
                BGSAVE_LAST_STATUS.store(true, Ordering::Relaxed);
                // A completed save is the reset point for `rdb_changes_since_last_save`.
                crate::admin::metrics_setup::mark_save_completed();
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
                // A completed save is the reset point for `rdb_changes_since_last_save`.
                crate::admin::metrics_setup::mark_save_completed();
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

    BGSAVE_CURRENT_FAILED.store(false, Ordering::SeqCst);
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

/// Called by each shard after its part of a sharded save ends (both
/// runtimes' event loops, and a shard that had to skip the save).
///
/// Decrements the shared counter. The last shard to finish publishes the
/// save's outcome (moon#1230): `rdb_last_bgsave_status` is `ok` iff no shard
/// of THIS save failed, and only a successful save advances
/// `rdb_last_save_time` / `LASTSAVE` and resets `rdb_changes_since_last_save`
/// (redis: `lastsave` and the dirty counter move on success only). It then
/// clears `SAVE_IN_PROGRESS` — last, so a poller that sees the save finished
/// (SHUTDOWN SAVE) reads this save's status, not the previous one's.
pub fn bgsave_shard_done(success: bool) {
    if !success {
        BGSAVE_CURRENT_FAILED.store(true, Ordering::SeqCst);
    }
    // Use compare-exchange loop to prevent underflow below zero.
    // If the counter is already 0 (spurious call), do nothing.
    loop {
        let current = BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst);
        if current == 0 {
            tracing::warn!("BGSAVE shard done called with counter already at 0 -- ignoring");
            // No counted save to attach it to; a failure still shows (the
            // next save that succeeds clears it).
            if !success {
                BGSAVE_LAST_STATUS.store(false, Ordering::SeqCst);
            }
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
                    // Last shard to finish: publish THIS save's outcome.
                    let ok = !BGSAVE_CURRENT_FAILED.swap(false, Ordering::SeqCst);
                    if ok {
                        // A completed save is the reset point for
                        // `rdb_changes_since_last_save`; a failed one is not
                        // (the dataset is still unpersisted).
                        crate::admin::metrics_setup::mark_save_completed();
                        LAST_SAVE_TIME.store(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                            Ordering::SeqCst,
                        );
                        info!("BGSAVE complete: all shards finished");
                    } else {
                        error!("BGSAVE failed: at least one shard's snapshot failed (see above)");
                    }
                    BGSAVE_LAST_STATUS.store(ok, Ordering::SeqCst);
                    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
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
    match pool.try_send_rewrite(AofMessage::Rewrite(db, pool.overflow_for(0).clone())) {
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
    bgrewriteaof_start_sharded_with_flag(pool, shard_databases, &AOF_REWRITE_IN_PROGRESS)
}

/// [`bgrewriteaof_start_sharded`] serialized on `in_progress` instead of the
/// global [`AOF_REWRITE_IN_PROGRESS`] (test seam, PerShard pools only: a
/// TopLevel writer always releases the global flag).
pub(crate) fn bgrewriteaof_start_sharded_with_flag(
    pool: &AofWriterPool,
    shard_databases: std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
    in_progress: &'static AtomicBool,
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
    if in_progress
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
        // flag is released when the last writer drops the coordinator, after
        // its post-fold overflow drain (moon#1158), not here.
        match pool.try_send_rewrite_per_shard_with_flag(shard_databases, in_progress) {
            Ok(()) => {
                return Frame::SimpleString(Bytes::from_static(
                    b"Background append only file rewriting started",
                ));
            }
            // The flag is NOT cleared here (moon#1158): a fan-out that fails
            // part-way has already handed the rewrite to the writers before
            // the failure, and they still fold/roll back and drain. The pool
            // released it if no writer got the rewrite; otherwise the last
            // writer does.
            Err(e) => return rewrite_pool_error_frame(e),
        }
    }

    match pool.try_send_rewrite(AofMessage::RewriteSharded(
        shard_databases,
        pool.overflow_for(0).clone(),
    )) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(
            b"Background append only file rewriting started",
        )),
        Err(e) => {
            // Send failed (channel full) or PerShard rejection — rewrite never
            // started, so clear the in-progress flag we just set.
            in_progress.store(false, Ordering::SeqCst);
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
    let snapshot: Vec<
        Vec<(
            crate::storage::compact_key::CompactKey,
            crate::storage::entry::Entry,
        )>,
    > = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            guard
                .data()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .collect();

    let path = PathBuf::from(dir).join(dbfilename);
    match rdb::save_from_snapshot(&snapshot, &path) {
        Ok(()) => {
            // A completed save is the reset point for `rdb_changes_since_last_save`.
            crate::admin::metrics_setup::mark_save_completed();
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

/// Modifier parsed from SHUTDOWN's optional argument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownSaveMode {
    /// `SHUTDOWN SAVE` — force a synchronous save regardless of configured
    /// save points.
    Save,
    /// `SHUTDOWN NOSAVE` — skip any extra RDB save (Redis parity: whatever
    /// AOF/WAL already made durable is all that survives).
    NoSave,
    /// Bare `SHUTDOWN` — save iff RDB save points are configured (Redis
    /// parity).
    Default,
}

/// Parse SHUTDOWN's argument list.
///
/// Redis SHUTDOWN accepts an optional single modifier: `NOSAVE` or `SAVE`.
/// `ABORT` is rejected here — Moon's SHUTDOWN runs synchronously to
/// completion inside the command itself, so there is never an in-progress
/// shutdown to abort. The `FORCE`/`NOW` timeout-override modifiers Redis
/// added later are accepted as no-ops for client compatibility: Moon's
/// shutdown path already flushes durably via the same bounded sequence used
/// for SIGTERM and has no separate "hung shutdown" timeout to override.
pub fn parse_shutdown_args(args: &[Frame]) -> Result<ShutdownSaveMode, Frame> {
    let mut mode = None;
    for a in args {
        let bytes: &[u8] = match a {
            Frame::BulkString(b) => b.as_ref(),
            Frame::SimpleString(b) => b.as_ref(),
            _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
        };
        if bytes.eq_ignore_ascii_case(b"NOSAVE") {
            if mode.is_some() {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
            mode = Some(ShutdownSaveMode::NoSave);
        } else if bytes.eq_ignore_ascii_case(b"SAVE") {
            if mode.is_some() {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
            mode = Some(ShutdownSaveMode::Save);
        } else if bytes.eq_ignore_ascii_case(b"FORCE") || bytes.eq_ignore_ascii_case(b"NOW") {
            continue;
        } else if bytes.eq_ignore_ascii_case(b"ABORT") {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR No shutdown in progress",
            )));
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        }
    }
    Ok(mode.unwrap_or(ShutdownSaveMode::Default))
}

/// Poll interval used by SHUTDOWN SAVE in sharded/monoio mode while waiting
/// for the cooperative per-shard BGSAVE snapshot it triggers to complete.
pub const SHUTDOWN_SAVE_POLL_MS: u64 = 5;

/// Bound on how long SHUTDOWN SAVE waits for that snapshot. A wedged shard
/// must not hang SHUTDOWN forever; exceeding this fails the command (server
/// stays up, Redis parity: a save that doesn't complete blocks shutdown)
/// rather than exiting against a torn snapshot. 10s: long enough for a real
/// (large) snapshot under normal disk I/O, short enough that an operator
/// (or a client with a bounded read timeout) isn't left hanging.
pub const SHUTDOWN_SAVE_TIMEOUT_MS: u64 = 10_000;

/// Decide whether SHUTDOWN's `Default` mode should perform a synchronous
/// save, mirroring Redis: save iff at least one RDB save point is
/// configured.
pub fn shutdown_default_should_save(save_points: Option<&str>) -> bool {
    save_points.is_some_and(|s| !s.trim().is_empty())
}

/// LASTSAVE command: returns Unix timestamp of last successful save.
pub fn handle_lastsave() -> Frame {
    let ts = LAST_SAVE_TIME.load(Ordering::Relaxed);
    Frame::Integer(ts as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes every test that drives the process-wide BGSAVE statics.
    static BGSAVE_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    #[test]
    fn test_save_in_progress_flag() {
        let _guard = BGSAVE_TEST_LOCK.lock();
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

    /// Run one sharded save over `shards` shards whose outcomes are
    /// `results`, as the event loops report them.
    fn sharded_save(results: &[bool]) {
        let (tx, _rx) = crate::runtime::channel::watch(0u64);
        let reply = bgsave_start_sharded(&tx, results.len());
        assert!(
            matches!(reply, Frame::SimpleString(_)),
            "the save must start: {reply:?}"
        );
        for &ok in results {
            assert!(SAVE_IN_PROGRESS.load(Ordering::SeqCst), "still running");
            bgsave_shard_done(ok);
        }
        assert!(
            !SAVE_IN_PROGRESS.load(Ordering::SeqCst),
            "the last shard ends the save"
        );
    }

    /// moon#1230: the status describes the LAST save — `err` after a save a
    /// shard failed, `ok` again after one where every shard succeeded — and
    /// only a successful save advances `LASTSAVE`.
    #[test]
    fn bgsave_status_is_per_save_and_lastsave_moves_only_on_success() {
        let _guard = BGSAVE_TEST_LOCK.lock();
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        BGSAVE_SHARDS_REMAINING.store(0, Ordering::SeqCst);
        LAST_SAVE_TIME.store(7, Ordering::SeqCst);

        // One shard of four fails — first, in the middle, and last.
        for failing in [0usize, 2, 3] {
            let mut results = [true; 4];
            results[failing] = false;
            sharded_save(&results);
            assert!(
                !BGSAVE_LAST_STATUS.load(Ordering::SeqCst),
                "shard {failing} failed: the save failed"
            );
            assert_eq!(
                LAST_SAVE_TIME.load(Ordering::SeqCst),
                7,
                "a failed save must not advance LASTSAVE"
            );
        }

        sharded_save(&[true; 4]);
        assert!(
            BGSAVE_LAST_STATUS.load(Ordering::SeqCst),
            "every shard succeeded: the status must return to ok"
        );
        assert!(
            LAST_SAVE_TIME.load(Ordering::SeqCst) > 7,
            "a successful save advances LASTSAVE"
        );

        // The single-shard path is the same function with one shard.
        sharded_save(&[false]);
        assert!(!BGSAVE_LAST_STATUS.load(Ordering::SeqCst));
        sharded_save(&[true]);
        assert!(BGSAVE_LAST_STATUS.load(Ordering::SeqCst));
    }

    /// A second BGSAVE while one runs is refused and does not reset the
    /// running save's failure latch.
    #[test]
    fn a_refused_second_bgsave_does_not_clear_the_running_saves_failure() {
        let _guard = BGSAVE_TEST_LOCK.lock();
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        BGSAVE_SHARDS_REMAINING.store(0, Ordering::SeqCst);
        let (tx, _rx) = crate::runtime::channel::watch(0u64);
        assert!(matches!(
            bgsave_start_sharded(&tx, 2),
            Frame::SimpleString(_)
        ));
        bgsave_shard_done(false);
        assert!(matches!(bgsave_start_sharded(&tx, 2), Frame::Error(_)));
        bgsave_shard_done(true);
        assert!(!BGSAVE_LAST_STATUS.load(Ordering::SeqCst));
        assert!(!SAVE_IN_PROGRESS.load(Ordering::SeqCst));
    }

    /// A sharded auto-save starts as a COUNTED save (moon#1230): it arms the
    /// per-shard fan-in, so its completions publish its status and advance
    /// LASTSAVE, and it holds SAVE_IN_PROGRESS against a concurrent BGSAVE.
    #[test]
    fn a_sharded_auto_save_is_a_counted_save() {
        let _guard = BGSAVE_TEST_LOCK.lock();
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        BGSAVE_SHARDS_REMAINING.store(0, Ordering::SeqCst);
        LAST_SAVE_TIME.store(3, Ordering::SeqCst);
        let (tx, rx) = crate::runtime::channel::watch(0u64);
        let before = rx.borrow();
        assert!(crate::persistence::auto_save::start_counted_auto_save(&tx));
        assert!(rx.borrow() > before, "the shards are told to snapshot");
        assert!(SAVE_IN_PROGRESS.load(Ordering::SeqCst));
        let shards = crate::command::connection::shard_count() as u64;
        assert_eq!(BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst), shards);
        assert!(
            !crate::persistence::auto_save::start_counted_auto_save(&tx),
            "a second save while one runs is refused"
        );
        for _ in 0..shards {
            bgsave_shard_done(true);
        }
        assert!(!SAVE_IN_PROGRESS.load(Ordering::SeqCst));
        assert!(BGSAVE_LAST_STATUS.load(Ordering::SeqCst));
        assert!(
            LAST_SAVE_TIME.load(Ordering::SeqCst) > 3,
            "LASTSAVE advanced"
        );
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
        let (shard_dbs, _inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
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
        let (shard_dbs, _inits) = crate::shard::shared_databases::ShardDatabases::new(vec![vec![
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

    // ── SHUTDOWN argument parsing (task #27) ────────────────────────────

    #[test]
    fn test_parse_shutdown_args_bare() {
        assert_eq!(parse_shutdown_args(&[]).unwrap(), ShutdownSaveMode::Default);
    }

    #[test]
    fn test_parse_shutdown_args_nosave() {
        let args = [Frame::BulkString(Bytes::from_static(b"NOSAVE"))];
        assert_eq!(
            parse_shutdown_args(&args).unwrap(),
            ShutdownSaveMode::NoSave
        );
        // Case-insensitive, matching Redis.
        let args = [Frame::BulkString(Bytes::from_static(b"nosave"))];
        assert_eq!(
            parse_shutdown_args(&args).unwrap(),
            ShutdownSaveMode::NoSave
        );
    }

    #[test]
    fn test_parse_shutdown_args_save() {
        let args = [Frame::BulkString(Bytes::from_static(b"SAVE"))];
        assert_eq!(parse_shutdown_args(&args).unwrap(), ShutdownSaveMode::Save);
    }

    #[test]
    fn test_parse_shutdown_args_save_force_noop() {
        // SAVE FORCE / NOW are accepted no-ops for client compatibility.
        let args = [
            Frame::BulkString(Bytes::from_static(b"SAVE")),
            Frame::BulkString(Bytes::from_static(b"FORCE")),
        ];
        assert_eq!(parse_shutdown_args(&args).unwrap(), ShutdownSaveMode::Save);
    }

    #[test]
    fn test_parse_shutdown_args_conflicting_modifiers_reject() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"SAVE")),
            Frame::BulkString(Bytes::from_static(b"NOSAVE")),
        ];
        assert!(matches!(parse_shutdown_args(&args), Err(Frame::Error(_))));
    }

    #[test]
    fn test_parse_shutdown_args_abort_rejected() {
        let args = [Frame::BulkString(Bytes::from_static(b"ABORT"))];
        match parse_shutdown_args(&args) {
            Err(Frame::Error(msg)) => {
                assert!(std::str::from_utf8(&msg).unwrap().contains("No shutdown"));
            }
            other => panic!("expected ABORT rejection, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_shutdown_args_garbage_rejected() {
        let args = [Frame::BulkString(Bytes::from_static(b"BOGUS"))];
        assert!(matches!(parse_shutdown_args(&args), Err(Frame::Error(_))));
    }

    #[test]
    fn test_shutdown_default_should_save() {
        assert!(!shutdown_default_should_save(None));
        assert!(!shutdown_default_should_save(Some("")));
        assert!(!shutdown_default_should_save(Some("   ")));
        assert!(shutdown_default_should_save(Some("3600 1 300 100")));
    }
}
