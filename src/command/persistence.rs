//! Persistence command handlers (BGSAVE, BGREWRITEAOF).

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use ringbuf::traits::Producer;
use ringbuf::HeapProd;
use tracing::{error, info};

use tokio::sync::mpsc;

use crate::persistence::aof::AofMessage;
use crate::persistence::rdb;
use crate::protocol::Frame;
use crate::shard::dispatch::ShardMessage;
use crate::storage::Database;

/// Type alias for the per-database RwLock container.
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Global epoch counter for snapshot coordination across shards.
pub static SNAPSHOT_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Global flag indicating whether a background save is in progress.
pub static SAVE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Start a background RDB save (BGSAVE command).
///
/// Clones all database entries under the lock, then spawns a blocking task
/// to serialize and write the RDB file. Returns immediately with a status message.
///
/// Returns an error frame if a save is already in progress.
pub fn bgsave_start(
    db: SharedDatabases,
    dir: String,
    dbfilename: String,
) -> Frame {
    // Check if a save is already running
    if SAVE_IN_PROGRESS.swap(true, Ordering::SeqCst) {
        return Frame::Error(Bytes::from_static(
            b"ERR Background save already in progress",
        ));
    }

    // Clone snapshot: lock each db individually with read lock
    // Include base_timestamp for TTL delta resolution during serialization
    let snapshot: Vec<(Vec<(crate::storage::compact_key::CompactKey, crate::storage::entry::Entry)>, u32)> = db
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

    tokio::task::spawn_blocking(move || {
        match rdb::save_from_snapshot(&snapshot, &path) {
            Ok(()) => {
                info!("Background RDB save completed: {}", path.display());
            }
            Err(e) => {
                error!("Background RDB save failed: {}", e);
            }
        }
        SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    });

    Frame::SimpleString(Bytes::from_static(b"Background saving started"))
}

/// Start a cooperative per-shard snapshot (BGSAVE command, sharded mode).
///
/// Increments the global epoch and sends SnapshotBegin to all shards via SPSC.
/// Each shard independently snapshots its databases at this epoch.
/// Returns immediately -- snapshot happens asynchronously in shard event loops.
pub fn bgsave_start_sharded(
    producers: &Rc<RefCell<Vec<HeapProd<ShardMessage>>>>,
    snapshot_dir: &str,
    num_shards: usize,
) -> Frame {
    if SAVE_IN_PROGRESS.swap(true, Ordering::SeqCst) {
        return Frame::Error(Bytes::from_static(
            b"ERR Background save already in progress",
        ));
    }

    let epoch = SNAPSHOT_EPOCH.fetch_add(1, Ordering::SeqCst) + 1;
    let snap_dir = PathBuf::from(snapshot_dir);

    let mut prods = producers.borrow_mut();
    for prod in prods.iter_mut() {
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let _ = prod.try_push(ShardMessage::SnapshotBegin {
            epoch,
            snapshot_dir: snap_dir.clone(),
            reply_tx: tx,
        });
    }
    drop(prods);

    // Clear flag after sending -- shards handle completion independently.
    // A proper implementation would track completion via the oneshot channels.
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);

    info!("BGSAVE triggered: epoch {} across {} shards", epoch, num_shards);
    Frame::SimpleString(Bytes::from_static(b"Background saving started"))
}

/// Start a background AOF rewrite (BGREWRITEAOF command).
///
/// Sends a Rewrite message to the AOF writer task, which will generate
/// synthetic commands from current database state and replace the AOF file.
pub fn bgrewriteaof_start(
    aof_tx: &mpsc::Sender<AofMessage>,
    db: SharedDatabases,
) -> Frame {
    match aof_tx.try_send(AofMessage::Rewrite(db)) {
        Ok(()) => Frame::SimpleString(Bytes::from_static(
            b"Background append only file rewriting started",
        )),
        Err(_) => Frame::Error(Bytes::from_static(
            b"ERR Background AOF rewrite failed to start",
        )),
    }
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
}
