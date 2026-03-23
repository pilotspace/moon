//! Persistence command handlers (BGSAVE, BGREWRITEAOF).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tracing::{error, info};

use crate::persistence::rdb;
use crate::protocol::Frame;
use crate::storage::Database;

/// Global flag indicating whether a background save is in progress.
pub static SAVE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

/// Start a background RDB save (BGSAVE command).
///
/// Clones all database entries under the lock, then spawns a blocking task
/// to serialize and write the RDB file. Returns immediately with a status message.
///
/// Returns an error frame if a save is already in progress.
pub fn bgsave_start(
    db: Arc<Mutex<Vec<Database>>>,
    dir: String,
    dbfilename: String,
) -> Frame {
    // Check if a save is already running
    if SAVE_IN_PROGRESS.swap(true, Ordering::SeqCst) {
        return Frame::Error(Bytes::from_static(
            b"ERR Background save already in progress",
        ));
    }

    // Clone snapshot under the lock
    let snapshot: Vec<Vec<(Bytes, crate::storage::entry::Entry)>> = {
        let dbs = db.lock().unwrap();
        dbs.iter()
            .map(|db| {
                db.data()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .collect()
    };

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
