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

use super::shared_databases::ShardDatabases;

/// Run cooperative active expiry across all databases.
pub(crate) fn run_active_expiry(shard_databases: &Arc<ShardDatabases>, shard_id: usize) {
    let db_count = shard_databases.db_count();
    for i in 0..db_count {
        let mut guard = shard_databases.write_db(shard_id, i);
        crate::server::expiration::expire_cycle_direct(&mut guard);
    }
}

/// Run background eviction if maxmemory is configured.
pub(crate) fn run_eviction(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
) {
    let rt = runtime_config.read().unwrap();
    if rt.maxmemory > 0 {
        let db_count = shard_databases.db_count();
        for i in 0..db_count {
            let mut guard = shard_databases.write_db(shard_id, i);
            let _ = crate::storage::eviction::try_evict_if_needed(&mut guard, &rt);
        }
    }
}

/// Expire timed-out blocked clients.
pub(crate) fn expire_blocked_clients(blocking_rc: &Rc<RefCell<BlockingRegistry>>) {
    let now = std::time::Instant::now();
    blocking_rc.borrow_mut().expire_timed_out(now);
}

/// Checkpoint tick interval in milliseconds.
/// Same 1ms tick as WAL flush — checkpoint manager advances one tick per call.
#[allow(dead_code)]
pub const CHECKPOINT_TICK_MS: u64 = 1;

/// Warm tier transition check interval in milliseconds (10 seconds).
/// Infrequent enough to avoid overhead, responsive enough to catch aged segments.
pub const WARM_CHECK_INTERVAL_MS: u64 = 10_000;

/// WAL fsync on 1-second interval (everysec durability).
pub(crate) fn sync_wal(wal_writer: &mut Option<WalWriter>) {
    if let Some(wal) = wal_writer {
        if let Err(e) = wal.sync_to_disk() {
            tracing::error!("WAL sync failed: {}", e);
        }
    }
}

/// WAL v3 fsync on 1-second interval (mirrors v2 everysec pattern).
///
/// Calls `flush_sync()` which writes buffered data and fsyncs the segment file.
/// Only active when disk-offload is enabled and WalWriterV3 was successfully initialized.
pub(crate) fn sync_wal_v3(
    wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>,
) {
    if let Some(wal) = wal_v3 {
        if let Err(e) = wal.flush_sync() {
            tracing::error!("WAL v3 sync failed: {}", e);
        }
    }
}
