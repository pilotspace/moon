use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::seq::IndexedRandom;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::storage::Database;

/// Type alias for the per-database RwLock container.
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Run the active expiration background task.
///
/// Every 100ms, iterates all databases and runs a probabilistic expiration
/// cycle on each. Shuts down gracefully when the cancellation token fires.
pub async fn run_active_expiration(
    db: SharedDatabases,
    shutdown: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                for lock in db.iter() {
                    let mut guard = lock.write();
                    expire_cycle(&mut *guard);
                }
            }
            _ = shutdown.cancelled() => {
                info!("Active expiration task shutting down");
                break;
            }
        }
    }
}

/// Public entry point for per-shard active expiry.
///
/// Shards call this directly on their owned databases without going through
/// the `SharedDatabases` wrapper (no Arc/RwLock needed in shared-nothing mode).
pub fn expire_cycle_direct(db: &mut Database) {
    expire_cycle(db);
}

/// Run one probabilistic expiration cycle on a single database.
///
/// Samples up to 20 random keys that have an expiry set. If more than 25%
/// of the sampled keys are expired, repeats the cycle. Enforces a 1ms time
/// budget to avoid holding the lock too long.
fn expire_cycle(db: &mut Database) {
    let start = Instant::now();
    let budget = Duration::from_millis(1);
    let mut rng = rand::rng();

    loop {
        let keys = db.keys_with_expiry();
        if keys.is_empty() {
            break;
        }

        let sample_size = keys.len().min(20);
        let sampled: Vec<_> = keys.choose_multiple(&mut rng, sample_size).cloned().collect();

        let mut expired_count = 0;
        for key in &sampled {
            if db.is_key_expired(key) {
                db.remove(key);
                expired_count += 1;
            }
        }

        // Stop if fewer than 25% expired or budget exhausted
        if expired_count * 4 < sample_size || start.elapsed() >= budget {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::storage::entry::{current_time_ms, Entry};

    #[test]
    fn test_expire_cycle_removes_expired_keys() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;
        let base_ts = db.base_timestamp();

        // Add 10 expired keys
        for i in 0..10 {
            let key = Bytes::from(format!("expired_{}", i));
            db.set(key, Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms, base_ts));
        }

        // Add 5 non-expired keys
        let future_ms = current_time_ms() + 3_600_000;
        for i in 0..5 {
            let key = Bytes::from(format!("alive_{}", i));
            db.set(key, Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms, base_ts));
        }

        // Add 3 keys without expiry
        for i in 0..3 {
            let key = Bytes::from(format!("noexpiry_{}", i));
            db.set(key, Entry::new_string(Bytes::from_static(b"v")));
        }

        expire_cycle(&mut db);

        // All expired keys should be removed
        for i in 0..10 {
            let key = format!("expired_{}", i);
            assert!(!db.is_key_expired(key.as_bytes()), "Key {} should have been removed", key);
        }

        // Non-expired keys should remain
        for i in 0..5 {
            let key = Bytes::from(format!("alive_{}", i));
            assert!(db.keys_with_expiry().contains(&key), "Key alive_{} should still exist", i);
        }

        // Keys without expiry should remain
        assert_eq!(db.len(), 5 + 3); // alive + noexpiry
    }

    #[test]
    fn test_expire_cycle_no_keys_with_expiry() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));

        expire_cycle(&mut db);

        // Nothing should change
        assert_eq!(db.len(), 2);
    }

    #[test]
    fn test_expire_cycle_empty_db() {
        let mut db = Database::new();
        expire_cycle(&mut db);
        assert_eq!(db.len(), 0);
    }
}
