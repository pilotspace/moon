//! Timer arm body handlers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains expiry, eviction, block timeout,
//! and WAL sync tick handlers.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use crate::blocking::BlockingRegistry;
use crate::config::RuntimeConfig;
use crate::persistence::wal::WalWriter;
use crate::storage::Database;

/// Run cooperative active expiry across all databases.
pub(crate) fn run_active_expiry(databases: &Rc<RefCell<Vec<Database>>>) {
    let mut dbs = databases.borrow_mut();
    for db in dbs.iter_mut() {
        crate::server::expiration::expire_cycle_direct(db);
    }
}

/// Run background eviction if maxmemory is configured.
pub(crate) fn run_eviction(
    databases: &Rc<RefCell<Vec<Database>>>,
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
) {
    let rt = runtime_config.read().unwrap();
    if rt.maxmemory > 0 {
        let mut dbs = databases.borrow_mut();
        for db in dbs.iter_mut() {
            let _ = crate::storage::eviction::try_evict_if_needed(db, &rt);
        }
    }
}

/// Expire timed-out blocked clients.
pub(crate) fn expire_blocked_clients(blocking_rc: &Rc<RefCell<BlockingRegistry>>) {
    let now = std::time::Instant::now();
    blocking_rc.borrow_mut().expire_timed_out(now);
}

/// WAL fsync on 1-second interval (everysec durability).
pub(crate) fn sync_wal(wal_writer: &mut Option<WalWriter>) {
    if let Some(wal) = wal_writer {
        let _ = wal.sync_to_disk();
    }
}
