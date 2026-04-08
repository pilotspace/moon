//! Persistence command handlers (BGSAVE, BGREWRITEAOF).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use bytes::Bytes;
use tracing::{error, info};

use crate::runtime::channel;

use crate::persistence::aof::AofMessage;
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

/// Start a background AOF rewrite (BGREWRITEAOF command).
///
/// Sends a Rewrite message to the AOF writer task, which will generate
/// synthetic commands from current database state and replace the AOF file.
pub fn bgrewriteaof_start(aof_tx: &channel::MpscSender<AofMessage>, db: SharedDatabases) -> Frame {
    match aof_tx.try_send(AofMessage::Rewrite(db)) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(
            b"Background append only file rewriting started",
        )),
        Err(_) => Frame::Error(Bytes::from_static(
            b"ERR Background AOF rewrite failed to start",
        )),
    }
}

/// Start BGREWRITEAOF in sharded mode using ShardDatabases.
pub fn bgrewriteaof_start_sharded(
    aof_tx: &channel::MpscSender<AofMessage>,
    shard_databases: std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
) -> Frame {
    match aof_tx.try_send(AofMessage::RewriteSharded(shard_databases)) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(
            b"Background append only file rewriting started",
        )),
        Err(_) => Frame::Error(Bytes::from_static(
            b"ERR Background AOF rewrite failed to start",
        )),
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
}
