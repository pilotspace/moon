#![allow(unused_imports)]
#[cfg(feature = "runtime-tokio")]
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime::cancel::CancellationToken;
use rand::seq::IndexedRandom;
#[cfg(feature = "runtime-tokio")]
use tracing::info;

use crate::storage::Database;
use crate::storage::db_hash_ttl::ReapOutcome;

/// Type alias for the per-database RwLock container.
#[cfg(feature = "runtime-tokio")]
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Run the active expiration background task.
///
/// Every 100ms, iterates all databases and runs a probabilistic expiration
/// cycle on each. Shuts down gracefully when the cancellation token fires.
#[cfg(feature = "runtime-tokio")]
pub async fn run_active_expiration(
    db: SharedDatabases,
    shutdown: CancellationToken,
    // #71b: replica-role mirror. A tokio process CAN be a replica (tokio has a
    // `stream_commands` apply path), and a replica must NOT run its own expiry
    // deletion sweep — it waits for the master's authoritative expire/DEL
    // record so both sides remove a key at the same point in the stream. When
    // this is `Some(true)` the tick becomes a no-op; logical expiry on reads
    // still applies. `None` (no replication configured) always sweeps.
    is_replica_mirror: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let is_replica = is_replica_mirror
                    .as_ref()
                    .is_some_and(|m| m.load(std::sync::atomic::Ordering::Acquire));
                if is_replica {
                    continue;
                }
                for lock in db.iter() {
                    let mut guard = lock.write();
                    // task #34 (Wave A): tokio's active-expiry sweep does not
                    // emit `record_reason_del` — master-side PSYNC is
                    // monoio-only (CLAUDE.md), so a tokio-runtime process is
                    // never a replication master; a no-op sink preserves
                    // today's behavior exactly.
                    // moon#542: route through `_direct` so the lazy-expiry
                    // pending queue physically drains under tokio too.
                    expire_cycle_direct(&mut guard, &mut |_| {});
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
///
/// `on_removed` fires once per whole-key removal from the probabilistic
/// sweep (sweep 1) — task #34 (Wave A): the shard event loop's
/// `run_active_expiry` uses this to emit a dual-plane `DEL` record for every
/// key the sweep actually deletes, so an attached replica and the AOF replay
/// both observe the master's own expiry decision instead of racing their own
/// independent TTL sweeps. Hash-field TTL reaps (sweep 2) deliberately do
/// NOT fire `on_removed` in Wave A — see the RFC's accepted-gap note
/// (replicas run the identical reaper against the identical TTLs, a bounded
/// divergence documented in CHANGELOG rather than wired up here).
pub fn expire_cycle_direct(db: &mut Database, on_removed: &mut dyn FnMut(&[u8])) {
    // moon#542: delete-and-emit the keys the LAZY paths discovered expired
    // since the last tick. Runs before the latch fast-path — the queue check
    // is one branch on an empty Vec, and a lazily-hidden key implies the
    // flag is latched true anyway (it had an expiry when it was hidden).
    drain_lazy_expired(db, on_removed);
    // Fast path: if the DB-level flag latches "no expiring keys", skip the
    // O(N) `keys_with_expiry()` scan entirely. Discovered by flamegraph:
    // with 100K TTL-less keys, the per-tick scan was consuming ~26% of
    // event-loop CPU on a SET p=64 workload. The flag is flipped true by
    // `Database::set` / `set_expiry` / `insert_for_load` and flipped false
    // only by `expire_cycle` itself when its scan comes back empty.
    if !db.maybe_has_expiring_keys() {
        return;
    }
    expire_cycle(db, on_removed);
}

/// Delete-and-emit the lazily-discovered expired keys (moon#542).
///
/// Each key is RE-VERIFIED before deletion: a write between the lazy read
/// and this tick replaced the entry with a new incarnation (fresh value, or
/// a fresh TTL that has not yet passed), and deleting that — or emitting a
/// DEL for it — would destroy live data on this node and on every replica.
/// A key that re-verifies as expired is deleted through the same
/// `on_removed` sink the probabilistic sweep uses, so it gets the identical
/// keyspace-notification + dual-plane DEL treatment.
fn drain_lazy_expired(db: &mut Database, on_removed: &mut dyn FnMut(&[u8])) {
    if !db.has_pending_lazy_expired() {
        return;
    }
    for key in db.take_pending_lazy_expired() {
        if db.is_key_expired(key.as_bytes()) {
            db.remove(key.as_bytes());
            on_removed(key.as_bytes());
        }
    }
}

/// Run one probabilistic expiration cycle on a single database.
///
/// Two sweeps per tick:
///
/// 1. **Whole-key sweep** — probabilistic 20-key sample from `keys_with_expiry()`.
///    Repeats while >25% of samples are expired and the 1ms budget allows.
///
/// 2. **Hash-field sweep** — iterates all `HashWithTtl` keys returned by
///    `hashes_with_field_expiry()` and calls `reap_expired_fields_one_hash`.
///    Keys where all fields expired are removed entirely.  Keys where the last
///    TTL sidecar entry is drained are downgraded back to plain `Hash`.
///
/// `maybe_has_expiring_keys` is cleared only when **both** sweeps return
/// empty, so a database with hash-field TTLs but no whole-key TTLs is not
/// incorrectly short-circuited on the next tick.
fn expire_cycle(db: &mut Database, on_removed: &mut dyn FnMut(&[u8])) {
    let start = Instant::now();
    let budget = Duration::from_millis(1);
    let mut rng = rand::rng();

    // ── Sweep 1: whole-key probabilistic expiry ──────────────────────────────
    loop {
        let keys = db.keys_with_expiry();
        if keys.is_empty() {
            break;
        }

        let sample_size = keys.len().min(20);
        let sampled: Vec<_> = keys.sample(&mut rng, sample_size).cloned().collect();

        let mut expired_count = 0;
        for key in &sampled {
            if db.is_key_expired(key.as_bytes()) {
                db.remove(key.as_bytes());
                on_removed(key.as_bytes());
                expired_count += 1;
            }
        }

        // Stop if fewer than 25% expired or budget exhausted
        if expired_count * 4 < sample_size || start.elapsed() >= budget {
            break;
        }
    }

    // ── Sweep 2: hash-field expiry ────────────────────────────────────────────
    // Collect keys up front to avoid borrow conflicts during mutation.
    let hash_keys = db.hashes_with_field_expiry();
    for key in &hash_keys {
        let outcome = db.reap_expired_fields_one_hash(key.as_bytes());
        if outcome == ReapOutcome::KeyDeleted {
            db.remove(key.as_bytes());
        }
    }

    // ── Flag maintenance ─────────────────────────────────────────────────────
    // Clear the fast-path flag only when both sweeps have nothing left.
    // If hash-field TTLs remain, the flag must stay set so future ticks
    // continue to run sweep 2.
    let no_whole_key_expiry = db.keys_with_expiry().is_empty();
    let no_hash_field_expiry = db.hashes_with_field_expiry().is_empty();
    if no_whole_key_expiry && no_hash_field_expiry {
        db.clear_maybe_has_expiring_keys();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::{Entry, current_time_ms};
    use bytes::Bytes;

    #[test]
    fn test_expire_cycle_removes_expired_keys() {
        let mut db = Database::new();
        let past_ms = current_time_ms() - 1000;

        // Add 10 expired keys
        for i in 0..10 {
            let key = Bytes::from(format!("expired_{}", i));
            db.set(
                key,
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms),
            );
        }

        // Add 5 non-expired keys
        let future_ms = current_time_ms() + 3_600_000;
        for i in 0..5 {
            let key = Bytes::from(format!("alive_{}", i));
            db.set(
                key,
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms),
            );
        }

        // Add 3 keys without expiry
        for i in 0..3 {
            let key = Bytes::from(format!("noexpiry_{}", i));
            db.set(key, Entry::new_string(Bytes::from_static(b"v")));
        }

        expire_cycle(&mut db, &mut |_| {});

        // All expired keys should be removed
        for i in 0..10 {
            let key = format!("expired_{}", i);
            assert!(
                !db.is_key_expired(key.as_bytes()),
                "Key {} should have been removed",
                key
            );
        }

        // Non-expired keys should remain
        for i in 0..5 {
            let key = crate::storage::compact_key::CompactKey::from(format!("alive_{}", i));
            assert!(
                db.keys_with_expiry().contains(&key),
                "Key alive_{} should still exist",
                i
            );
        }

        // Keys without expiry should remain
        assert_eq!(db.len(), 5 + 3); // alive + noexpiry
    }

    #[test]
    fn test_expire_cycle_no_keys_with_expiry() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k2"), Bytes::from_static(b"v2"));

        expire_cycle(&mut db, &mut |_| {});

        // Nothing should change
        assert_eq!(db.len(), 2);
    }

    #[test]
    fn test_expire_cycle_empty_db() {
        let mut db = Database::new();
        expire_cycle(&mut db, &mut |_| {});
        assert_eq!(db.len(), 0);
    }

    // ── Phase 197: hash-field active-expiry tests ────────────────────────────

    /// Seed a hash with `pairs` and arrange for `expire_field` to appear expired.
    ///
    /// Strategy: set the field's TTL to `now + 1_000` ms (future), then advance
    /// the cached clock past that value.  This avoids the `hash_set_field_ttl`
    /// past-expiry short-circuit that immediately deletes the field instead of
    /// storing it in the `ttls` sidecar.
    fn seed_hash_with_expired_field(
        db: &mut Database,
        key: &[u8],
        pairs: &[(&[u8], &[u8])],
        expire_field: &[u8],
    ) {
        use crate::storage::db::HashTtlCond;
        {
            let map = db
                .get_or_create_hash(key)
                .expect("hash creation must succeed");
            for (f, v) in pairs {
                map.insert(Bytes::copy_from_slice(f), Bytes::copy_from_slice(v));
            }
        }
        // Set a future expiry so it is stored in the ttls sidecar (not immediately
        // deleted by the past-expiry short-circuit in hash_set_field_ttl).
        let future_ms = db.now_ms() + 1_000;
        let r = db.hash_set_field_ttl(key, expire_field, future_ms, HashTtlCond::Always);
        assert_eq!(r, Ok(1), "TTL must be stored");
        // Advance the cached clock past the expiry — field is now expired-but-not-reaped.
        db.set_cached_now_ms_for_test(future_ms + 1);
    }

    #[test]
    fn test_expire_cycle_reaps_hash_fields() {
        let mut db = Database::new();
        seed_hash_with_expired_field(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")], b"f");

        // The hash key still exists; "f" is expired, "g" is live.
        assert_eq!(db.len(), 1);
        expire_cycle(&mut db, &mut |_| {});

        // Key must still exist (g is alive).
        assert_eq!(db.len(), 1);
        // Field "f" TTL entry must be physically gone after reaping.
        assert_eq!(db.hash_get_field_ttl_ms(b"h", b"f"), None);
        // Hash must still be accessible.
        assert!(db.get_hash(b"h").is_ok());
    }

    #[test]
    fn test_expire_cycle_deletes_key_when_all_hash_fields_expired() {
        let mut db = Database::new();
        seed_hash_with_expired_field(&mut db, b"h", &[(b"f", b"v")], b"f");

        expire_cycle(&mut db, &mut |_| {});

        // Key must be entirely removed.
        assert_eq!(db.len(), 0);
    }

    // ── moon#542: lazy expiry must DEFER deletion to the sweep so the
    // deletion is EMITTED (keyspace notification + dual-plane DEL). The old
    // behavior removed the key inside `get`/`get_mut`/`exists` where no
    // emission plane exists — the key vanished silently and an attached
    // replica kept it forever. ─────────────────────────────────────────────
    mod lazy_expiry_deferral {
        use super::*;

        fn db_with_expired_key(key: &[u8]) -> Database {
            let mut db = Database::new();
            let past_ms = current_time_ms() - 1_000;
            db.set(
                Bytes::copy_from_slice(key),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms),
            );
            // `get`/`exists` judge expiry against the db-cached clock.
            db.set_cached_now_ms_for_test(current_time_ms());
            db
        }

        fn drained_keys(db: &mut Database) -> Vec<Vec<u8>> {
            let mut got = Vec::new();
            expire_cycle_direct(db, &mut |k| got.push(k.to_vec()));
            got
        }

        /// `get` answers None but must NOT physically remove: the sweep tick
        /// deletes and emits.
        #[test]
        fn lazy_get_hides_and_defers_removal_until_sweep_emits() {
            let mut db = db_with_expired_key(b"k");
            assert!(db.get(b"k").is_none(), "expired key must read as absent");
            assert!(
                db.data().get(b"k").is_some(),
                "lazy read must HIDE, not remove — deletion belongs to the \
                 sweep so it can be emitted"
            );
            let got = drained_keys(&mut db);
            assert!(
                got.iter().any(|k| k == b"k"),
                "sweep must emit the lazily-read key (got {got:?})"
            );
            assert!(db.data().get(b"k").is_none(), "sweep must delete it");
            // Pins the DRAIN specifically: the probabilistic sweep could
            // mask a deleted drain in this 1-key db, but only the drain
            // consumes the queue.
            assert_eq!(
                db.pending_lazy_expired_len(),
                0,
                "drain must consume the queue"
            );
        }

        /// Same contract through `exists`.
        #[test]
        fn lazy_exists_hides_and_defers() {
            let mut db = db_with_expired_key(b"k");
            assert!(!db.exists(b"k"));
            assert!(db.data().get(b"k").is_some());
            let got = drained_keys(&mut db);
            assert!(got.iter().any(|k| k == b"k"));
        }

        /// Same contract through `get_mut`.
        #[test]
        fn lazy_get_mut_hides_and_defers() {
            let mut db = db_with_expired_key(b"k");
            assert!(db.get_mut(b"k").is_none());
            assert!(db.data().get(b"k").is_some());
            let got = drained_keys(&mut db);
            assert!(got.iter().any(|k| k == b"k"));
        }

        /// A key overwritten between the lazy read and the sweep is a NEW
        /// incarnation — the drain must not delete it or emit a DEL for it.
        #[test]
        fn overwritten_key_is_not_deleted_by_the_drain() {
            let mut db = db_with_expired_key(b"k");
            assert!(db.get(b"k").is_none());
            db.set(
                Bytes::from_static(b"k"),
                Entry::new_string(Bytes::from_static(b"fresh")),
            );
            let got = drained_keys(&mut db);
            assert!(
                !got.iter().any(|k| k == b"k"),
                "drain must skip the overwritten key"
            );
            assert!(db.get(b"k").is_some(), "fresh incarnation must survive");
        }

        /// The pending queue is bounded; past the cap the lazy path still
        /// hides (the probabilistic sweep is the backstop), it just stops
        /// recording.
        #[test]
        fn pending_queue_is_capped_and_still_hides() {
            let mut db = Database::new();
            let past_ms = current_time_ms() - 1_000;
            let n = crate::storage::db::PENDING_EXPIRED_CAP + 10;
            for i in 0..n {
                db.set(
                    Bytes::from(format!("k{i}")),
                    Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms),
                );
            }
            db.set_cached_now_ms_for_test(current_time_ms());
            for i in 0..n {
                assert!(db.get(format!("k{i}").as_bytes()).is_none());
            }
            assert!(
                db.pending_lazy_expired_len() <= crate::storage::db::PENDING_EXPIRED_CAP,
                "queue must not grow past the cap"
            );
        }
    }
}
