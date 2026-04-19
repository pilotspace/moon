//! Timer arm body handlers for the shard event loop.
//!
//! Extracted from shard/mod.rs. Contains expiry, eviction, block timeout,
//! and WAL sync tick handlers.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::blocking::BlockingRegistry;
use crate::config::RuntimeConfig;
use crate::persistence::wal::WalWriter;

use super::shared_databases::ShardDatabases;

/// Run cooperative active expiry across all databases.
/// Shard 0 also updates the RSS gauge (once per expiry cycle, ~100ms).
pub(crate) fn run_active_expiry(shard_databases: &Arc<ShardDatabases>, shard_id: usize) {
    let db_count = shard_databases.db_count();
    for i in 0..db_count {
        let mut guard = shard_databases.write_db(shard_id, i);
        crate::server::expiration::expire_cycle_direct(&mut guard);
    }
    // Update RSS gauge on shard 0 only, once per second (not every 100ms tick).
    // Gated by a simple counter to reduce /proc/self/statm open/read/close churn.
    if shard_id == 0 {
        use std::sync::atomic::{AtomicU8, Ordering};
        static RSS_TICK: AtomicU8 = AtomicU8::new(0);
        let tick = RSS_TICK.fetch_add(1, Ordering::Relaxed);
        if tick.is_multiple_of(10) {
            let rss = crate::admin::metrics_setup::get_rss_bytes();
            if rss > 0 {
                crate::admin::metrics_setup::update_rss_bytes(rss);
            }
        }
    }
}

/// Run background eviction if maxmemory is configured.
pub(crate) fn run_eviction(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
) {
    let rt = runtime_config.read();
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

/// Fire MQ triggers whose debounce window has elapsed.
///
/// Called from the event loop periodic tick at 100ms cadence (same interval
/// as expiry/eviction). For each trigger entry where `pending_fire_ms > 0`
/// and `pending_fire_ms <= now_ms`, fires the callback by publishing a
/// notification to the `mq:trigger:{queue_key}` pub/sub channel.
///
/// Triggers fire on the workspace home shard only (hash tag ensures
/// all workspace keys route to one shard, so the TriggerRegistry on
/// that shard is authoritative).
pub(crate) fn fire_pending_mq_triggers(
    shard_databases: &Arc<ShardDatabases>,
    shard_id: usize,
    now_ms: u64,
    pubsub_registry: &Arc<parking_lot::RwLock<crate::pubsub::PubSubRegistry>>,
) {
    let mut guard = shard_databases.trigger_registry(shard_id);
    let Some(reg) = guard.as_mut() else { return };

    // Collect keys of triggers ready to fire
    let ready_keys = reg.fire_ready(now_ms);
    if ready_keys.is_empty() {
        return;
    }

    // Collect (channel, message) pairs while holding trigger lock,
    // then publish after releasing it to avoid holding two locks.
    let mut notifications: Vec<(bytes::Bytes, bytes::Bytes)> = Vec::with_capacity(ready_keys.len());

    for key in &ready_keys {
        if let Some(entry) = reg.get(key) {
            // Build pub/sub channel name: "mq:trigger:{queue_key}"
            let channel = {
                let mut ch = Vec::with_capacity(11 + entry.queue_key.len());
                ch.extend_from_slice(b"mq:trigger:");
                ch.extend_from_slice(&entry.queue_key);
                bytes::Bytes::from(ch)
            };
            notifications.push((channel, entry.callback_cmd.clone()));
        }
        // Mark as fired (updates last_fire_ms, clears pending_fire_ms)
        reg.mark_fired(key, now_ms);
    }

    // Release trigger registry lock before acquiring pubsub lock
    drop(guard);

    // Publish each trigger notification via pub/sub
    if !notifications.is_empty() {
        let mut pubsub = pubsub_registry.write();
        for (channel, message) in &notifications {
            let _subscriber_count = pubsub.publish(channel, message);
            tracing::debug!(
                "Shard {}: MQ trigger fired on channel {:?} -> {} subscriber(s)",
                shard_id,
                String::from_utf8_lossy(channel),
                _subscriber_count,
            );
        }
    }
}

/// WAL v3 fsync on 1-second interval (mirrors v2 everysec pattern).
///
/// Calls `flush_sync()` which writes buffered data and fsyncs the segment file.
/// Only active when disk-offload is enabled and WalWriterV3 was successfully initialized.
pub(crate) fn sync_wal_v3(wal_v3: &mut Option<crate::persistence::wal_v3::segment::WalWriterV3>) {
    if let Some(wal) = wal_v3 {
        if let Err(e) = wal.flush_sync() {
            tracing::error!("WAL v3 sync failed: {}", e);
        }
    }
}
