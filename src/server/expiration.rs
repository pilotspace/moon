#![allow(unused_imports)]
#[cfg(feature = "runtime-tokio")]
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime::cancel::CancellationToken;
#[cfg(feature = "runtime-tokio")]
use tracing::info;

use crate::storage::Database;
use crate::storage::db_hash_ttl::ReapOutcome;
use crate::storage::entry::current_time_ms;

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

/// Run one expiration cycle on a single database.
///
/// Two sweeps per tick:
///
/// 1. **Whole-key sweep** (moon#541) — pops DUE pairs off the front of the
///    deadline-ordered expiry index: exactly the expired keys, in expiry
///    order, no sampling and no O(N) scan (the pre-#541 probabilistic
///    20-key sample went blind when due keys were a small fraction of the
///    volatile population, and its two full-map scans per tick were the
///    ~26%-of-event-loop-CPU flamegraph hit this file's fast-path latch
///    was built to dodge). Work is O(due · log n), capped by the 1ms
///    budget; the backlog carries to the next tick.
///
/// 2. **Hash-field sweep** — iterates all `HashWithTtl` keys returned by
///    `hashes_with_field_expiry()` and calls `reap_expired_fields_one_hash`.
///    Keys where all fields expired are removed entirely.  Keys where the last
///    TTL sidecar entry is drained are downgraded back to plain `Hash`.
///    Gated by the `hash_field_ttl_possible` latch (moon#541): databases
///    that never stored a field TTL skip the O(N) scan entirely. The scan
///    itself is still O(N) when the latch is up — tracked as moon#543.
///
/// `maybe_has_expiring_keys` is cleared only when **both** sweeps have
/// nothing left, so a database with hash-field TTLs but no whole-key TTLs
/// is not incorrectly short-circuited on the next tick.
fn expire_cycle(db: &mut Database, on_removed: &mut dyn FnMut(&[u8])) {
    let start = Instant::now();
    let budget = Duration::from_millis(1);

    // ── Sweep 1: deadline-ordered whole-key expiry (moon#541) ───────────────
    let now_ms = current_time_ms();
    let mut popped = 0u32;
    while let Some((ts, key)) = db.peek_due_expiry(now_ms) {
        if db.is_key_expired(key.as_bytes()) {
            // `remove` unindexes the entry's CURRENT pair via `remove_hot`.
            db.remove(key.as_bytes());
            on_removed(key.as_bytes());
        } else if db
            .data()
            .get(key.as_bytes())
            .is_none_or(|e| e.expires_at_ms() != ts)
        {
            // The pair is PROVABLY stale: the entry is gone or its TTL was
            // retargeted since this pair was written — a pair a writer
            // failed to retire (writer-coverage bug; the
            // debug_expiry_index_consistent oracle exists to catch those in
            // tests). Drop it or this loop would peek it forever.
            db.drop_expiry_index_pair(ts, &key);
        } else {
            // The pair matches the entry exactly, yet the fresh clock says
            // "not expired" — the wall clock stepped backwards between the
            // cycle-start peek and this re-verification. The pair is VALID,
            // just not due; keep it for a later tick. The index is ordered,
            // so nothing after the head is due either.
            break;
        }
        // Budget check every 64 pops, not per key: `Instant::elapsed` is a
        // clock read, and the common tick pops far fewer than 64. At least
        // one key is always processed before the first check can stop us.
        popped += 1;
        if popped % 64 == 0 && start.elapsed() >= budget {
            break;
        }
    }

    // ── Sweep 2: hash-field expiry (latch-gated, moon#541) ───────────────────
    // The reap outcomes double as the latch's self-reset evidence: a key
    // stays eligible only while it remains `HashWithTtl` (FieldsRemoved /
    // NoOp); Downgraded and KeyDeleted leave the kind. When zero eligible
    // keys remain after the sweep, the latch lowers — no second O(N) scan.
    if db.hash_field_ttl_possible() {
        // Collect keys up front to avoid borrow conflicts during mutation.
        let hash_keys = db.hashes_with_field_expiry();
        let mut remaining = 0usize;
        for key in &hash_keys {
            match db.reap_expired_fields_one_hash(key.as_bytes()) {
                ReapOutcome::KeyDeleted => {
                    db.remove(key.as_bytes());
                }
                ReapOutcome::Downgraded => {}
                ReapOutcome::FieldsRemoved | ReapOutcome::NoOp => remaining += 1,
            }
        }
        if remaining == 0 {
            db.clear_hash_field_ttl_latch();
        }
    }

    // ── Flag maintenance ─────────────────────────────────────────────────────
    // Clear the fast-path flag only when both sweeps have nothing left.
    // If hash-field TTLs remain, the flag must stay set so future ticks
    // continue to run sweep 2. Both checks are O(1) now: the whole-key
    // side reads the index's emptiness, and the hash side reads the latch
    // sweep 2 just maintained from its own reap outcomes.
    if db.expiry_index_is_empty() && !db.hash_field_ttl_possible() {
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

    // ── moon#541: deadline-ordered expiry index ──────────────────────────

    /// The probabilistic sweep goes blind when due keys are a small fraction
    /// of the TTL'd population: a 20-key sample from 10_050 keys finds ~0.1
    /// of the 50 due ones, and the 25% continuation gate then stops the
    /// cycle after a single round — the due keys linger for many ticks
    /// (unboundedly, in expectation ~500 rounds). The deadline-ordered index
    /// pops exactly the due keys, so ONE cycle must remove all of them.
    #[test]
    fn expire_cycle_removes_all_due_keys_among_many_live_ones() {
        let mut db = Database::new();
        let future_ms = current_time_ms() + 3_600_000;
        for i in 0..10_000u32 {
            db.set(
                Bytes::from(format!("live_{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms),
            );
        }
        let past_ms = current_time_ms() - 1_000;
        for i in 0..50u32 {
            db.set(
                Bytes::from(format!("due_{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), past_ms),
            );
        }

        // Deterministic under CI preemption: a stalled runner can trip the
        // 1ms budget mid-sweep, so allow a bounded number of cycles and
        // assert the CUMULATIVE count. Still red on the sampling sweep: 20
        // cycles × a 20-key sample of 10_050 finds ~2 due keys, not 50.
        let mut removed = 0usize;
        let mut cycles = 0;
        while removed < 50 && cycles < 20 {
            expire_cycle(&mut db, &mut |_| removed += 1);
            cycles += 1;
        }

        assert_eq!(removed, 50, "the due keys must all be removed promptly");
        assert_eq!(db.len(), 10_000, "live keys must all survive");
    }

    /// A pair that fails the expiry re-check but still matches its entry's
    /// TTL exactly must be KEPT (the only honest explanation is a backwards
    /// wall-clock step); only a provably-stale pair — entry gone or TTL
    /// retargeted — is dropped, and the entry itself is never deleted.
    #[test]
    fn sweep_drops_only_provably_stale_pairs() {
        let mut db = Database::new();
        let future_ms = current_time_ms() + 3_600_000;
        db.set(
            Bytes::from_static(b"k"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms),
        );
        assert_eq!(db.expiry_index_len(), 1);

        // Inject a bogus DUE pair for the same key (simulating a pair a
        // buggy writer failed to retire): due by the clock, but the entry's
        // real TTL differs -> provably stale -> dropped, entry untouched.
        db.expiry_index_insert(current_time_ms() - 1_000, b"k");
        assert_eq!(db.expiry_index_len(), 2);

        let mut removed = 0usize;
        expire_cycle(&mut db, &mut |_| removed += 1);

        assert_eq!(removed, 0, "a stale pair must never delete a live entry");
        assert_eq!(db.len(), 1, "the entry survives");
        assert_eq!(
            db.expiry_index_len(),
            1,
            "the stale pair is dropped, the real pair is kept"
        );
        assert!(db.debug_expiry_index_consistent());
    }

    /// GETEX EX/EXAT with a seconds value near i64::MAX must answer the
    /// range error, not overflow the *1000 conversion (debug builds
    /// panicked; release builds silently wrapped to a bogus TTL).
    #[test]
    fn getex_rejects_overflowing_seconds() {
        use crate::protocol::Frame;
        let mut db = Database::new();
        for opt in [&b"EX"[..], &b"EXAT"[..]] {
            db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
            let args = [
                Frame::BulkString(Bytes::from_static(b"k")),
                Frame::BulkString(Bytes::copy_from_slice(opt)),
                Frame::BulkString(Bytes::from(i64::MAX.to_string())),
            ];
            let reply = crate::command::string::getex(&mut db, &args);
            assert!(
                matches!(reply, Frame::Error(_)),
                "overflowing {} must answer the range error",
                String::from_utf8_lossy(opt)
            );
        }
    }

    /// GETEX wrote its TTL through a raw `get_mut` + `set_expires_at_ms`,
    /// bypassing `Database::set_expiry` — so the DB-level latch never
    /// flipped and a key whose ONLY TTL came from GETEX was invisible to
    /// the active sweep forever (it could only die on a later read).
    #[test]
    fn getex_ttl_participates_in_active_expiry() {
        use crate::protocol::Frame;
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"k"), Bytes::from_static(b"v"));
        assert!(!db.maybe_has_expiring_keys());

        let args = [
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::BulkString(Bytes::from_static(b"PX")),
            Frame::BulkString(Bytes::from_static(b"30")),
        ];
        let reply = crate::command::string::getex(&mut db, &args);
        assert!(
            matches!(reply, Frame::BulkString(_)),
            "GETEX must answer the value"
        );
        assert!(
            db.maybe_has_expiring_keys(),
            "a GETEX-set TTL must arm the active sweep"
        );

        std::thread::sleep(Duration::from_millis(40));
        let mut removed: Vec<Vec<u8>> = Vec::new();
        expire_cycle_direct(&mut db, &mut |k| removed.push(k.to_vec()));
        assert_eq!(removed, vec![b"k".to_vec()], "sweep must emit the expiry");
        assert_eq!(db.len(), 0);
    }

    /// The hash-field-TTL latch: a database that never stored a field TTL
    /// must finish the cycle with the latch still down, and a database whose
    /// last HashWithTtl key is gone must have it lowered by the cycle's
    /// flag maintenance (self-reset gate, mirroring the whole-key flag).
    #[test]
    fn hash_ttl_latch_lowers_when_last_hash_ttl_key_gone() {
        let mut db = Database::new();
        seed_hash_with_expired_field(&mut db, b"h", &[(b"f", b"v")], b"f");
        assert!(db.hash_field_ttl_possible());

        // Reap: the only field expires -> key deleted -> next cycle's flag
        // maintenance sees zero HashWithTtl keys and lowers the latch.
        expire_cycle(&mut db, &mut |_| {});
        assert_eq!(db.len(), 0, "all-fields-expired hash must be deleted");
        expire_cycle(&mut db, &mut |_| {});
        assert!(
            !db.hash_field_ttl_possible(),
            "latch must lower once no HashWithTtl keys remain"
        );
    }

    /// Manual timing probe for the #541 claim (run with `-- --ignored`):
    /// per-tick sweep cost on a database where 100K keys ALL carry a
    /// (far-future) TTL — the population the fast-path latch cannot help,
    /// since it only short-circuits databases with zero TTLs. The old
    /// sweep paid three full O(N) scans per tick here; the index pays one
    /// O(log n) peek.
    #[test]
    #[ignore]
    fn timing_expire_cycle_100k_volatile_keys() {
        let mut db = Database::new();
        let future_ms = current_time_ms() + 3_600_000;
        for i in 0..100_000u32 {
            db.set(
                Bytes::from(format!("k{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), future_ms),
            );
        }
        let start = Instant::now();
        for _ in 0..1_000 {
            expire_cycle(&mut db, &mut |_| {});
        }
        eprintln!(
            "1000 expire_cycle calls on 100k volatile keys: {:?} ({:?}/tick)",
            start.elapsed(),
            start.elapsed() / 1_000
        );
        assert_eq!(db.len(), 100_000);
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
