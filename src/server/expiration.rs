#![allow(unused_imports)]
#[cfg(feature = "runtime-tokio")]
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime::cancel::CancellationToken;
#[cfg(feature = "runtime-tokio")]
use tracing::info;

use crate::storage::Database;
use crate::storage::db::{HASH_SWEEP_MAX_FIELDS_PER_KEY, HASH_SWEEP_MAX_KEYS_PER_TICK};
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
    // cycle entirely. Discovered by flamegraph: with 100K TTL-less keys, the
    // per-tick scan was consuming ~26% of event-loop CPU on a SET p=64
    // workload. The flag is flipped true by `Database::set` / `set_expiry` /
    // `insert_for_load` and flipped false only by the maintenance below.
    if !db.maybe_has_expiring_keys() {
        return;
    }
    // moon#552: the latch above only saves a database with ZERO TTL'd keys.
    // A TTL-heavy database entered the cycle every 100ms just to re-derive
    // "nothing due" — allocating a start `Instant`, reading the clock, and
    // walking both sweeps' entry conditions. Both indexes are deadline-
    // ordered, so ONE O(log n) head-peek each answers that question
    // definitively; when neither head is due the tick costs two BTree
    // `first()` calls and returns.
    if nothing_due(db) {
        // The flag maintenance `expire_cycle` would have done: with both
        // indexes empty there is nothing left to sweep, so lower the latch
        // and let the branch above claim every subsequent tick.
        if db.expiry_index_is_empty() && !db.hash_field_ttl_possible() {
            db.clear_maybe_has_expiring_keys();
        }
        return;
    }
    expire_cycle(db, on_removed);
}

/// True when [`expire_cycle`] would provably remove nothing (moon#552) — the
/// head-peek gate, extracted so it can be tested against each thing that can
/// make a cycle worth running.
///
/// O(log n): two deadline-index head reads. Deliberately does NOT consider
/// the lazy-expired queue — [`drain_lazy_expired`] runs before this gate.
///
/// Each sweep is asked against the clock IT reaps with (see `expire_cycle`),
/// so the gate can never skip work a sweep would have done.
#[inline]
fn nothing_due(db: &Database) -> bool {
    db.peek_due_expiry(current_time_ms()).is_none()
        && db.peek_due_hash_expiry(db.now_ms()).is_none()
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
/// 2. **Hash-field sweep** (moon#543) — the mirror image of sweep 1 over the
///    hash-field deadline index: pops DUE `(min_expiry_ms, key)` pairs and
///    reaps that hash's expired fields. Keys where all fields expired are
///    removed entirely; keys where the last TTL sidecar entry is drained are
///    downgraded back to plain `Hash`.
///
///    Before #543 this sweep collected EVERY `HashWithTtl` key in the
///    database with a full table scan and reaped all of them, with no time
///    budget, no sampling and no batch cap — on the shard event loop, every
///    100ms. It is now bounded three ways: the same 1ms wall-clock budget as
///    sweep 1, a hard [`HASH_SWEEP_MAX_KEYS_PER_TICK`] cap on hashes visited,
///    and a [`HASH_SWEEP_MAX_FIELDS_PER_KEY`] cap on fields drained per
///    visit. A hash left partly reaped is re-armed at its still-due minimum
///    and resumed on the next visit. Deferring a reap is invisible to
///    clients — the read path already filters `ttls` against the shard clock,
///    so an unreaped expired field cannot be returned; only the memory
///    reclaim is deferred.
///
/// The budget is SHARED and spent in order, so a large whole-key backlog can
/// leave sweep 2 with none. Sweep 2's loop therefore checks the budget AFTER
/// its first pop: every tick makes at least one hash's worth of progress.
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

    // ── Sweep 2: deadline-ordered hash-field expiry (moon#543) ──────────────
    //
    // Termination: each iteration retires the popped pair and re-arms `key`
    // at the hash's FRESH minimum. Either that minimum is past `now_ms` (the
    // head moves on) or the reap just removed at least one field (the total
    // field count strictly decreases) — because `min <= now_ms` means an
    // expired field existed, so the reap cannot have been a no-op. The two
    // caps and the budget bound the loop from above regardless.
    //
    // Clock: sweep 2 reaps against `db.now_ms()`, NOT sweep 1's
    // `current_time_ms()`. The hash read path filters `ttls` against
    // `Database::cached_now_ms`, so reaping against anything ahead of it
    // could physically drop a field that reads still consider live. In
    // production both are the same shard clock; the divergence only exists
    // for `set_cached_now_ms_for_test`.
    let hash_now_ms = db.now_ms();
    let mut visited = 0u32;
    while let Some((ts, key)) = db.peek_due_hash_expiry(hash_now_ms) {
        if db.reap_expired_fields_one_hash_at(
            key.as_bytes(),
            hash_now_ms,
            HASH_SWEEP_MAX_FIELDS_PER_KEY,
        ) == ReapOutcome::KeyDeleted
        {
            db.remove(key.as_bytes());
        }
        db.rearm_hash_expiry(ts, &key);
        visited += 1;
        if visited >= HASH_SWEEP_MAX_KEYS_PER_TICK || start.elapsed() >= budget {
            break;
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

    // ── moon#543 / moon#552: bounded, deadline-indexed sweeps ────────────

    use crate::storage::db::HashTtlCond;

    /// Build `count` hashes, each holding one field whose TTL has elapsed.
    /// The TTL is set in the future and the DB clock stepped past it
    /// afterwards, because `hash_set_field_ttl` deletes an already-past field
    /// inline — the fields must land in the sidecar for the SWEEP to find.
    fn db_with_due_field_hashes(count: u32) -> Database {
        let mut db = Database::new();
        let ttl = db.now_ms() + 1_000;
        for i in 0..count {
            let key = format!("h{i}");
            {
                let map = db.get_or_create_hash(key.as_bytes()).expect("hash");
                map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
            }
            assert_eq!(
                db.hash_set_field_ttl(key.as_bytes(), b"f", ttl, HashTtlCond::Always),
                Ok(1)
            );
        }
        // Step the DB clock past the deadline: the fields are now
        // expired-but-not-reaped, which is exactly the sweep's input.
        db.set_cached_now_ms_for_test(ttl + 1);
        db
    }

    /// moon#543 — THE bound. Sweep 2 used to collect every `HashWithTtl` key
    /// in the database with a full table scan and reap all of them in one
    /// tick, on the shard event loop. One cycle must now visit at most
    /// `HASH_SWEEP_MAX_KEYS_PER_TICK` hashes no matter how many are due.
    ///
    /// Deterministic on purpose: the 1ms wall-clock budget alone would make
    /// this assertion machine-speed dependent (a fast host reaps all 5000
    /// inside the budget), which is exactly why the hard key cap exists.
    #[test]
    fn hash_field_sweep_is_bounded_per_tick() {
        const DUE: u32 = 5_000;
        let mut db = db_with_due_field_hashes(DUE);
        assert_eq!(db.len(), DUE as usize);

        expire_cycle(&mut db, &mut |_| {});

        let reaped = DUE as usize - db.len();
        assert!(
            reaped >= 1,
            "the sweep must always make progress, reaped {reaped}"
        );
        assert!(
            reaped <= HASH_SWEEP_MAX_KEYS_PER_TICK as usize,
            "sweep 2 is unbudgeted: {reaped} hashes reaped in ONE tick \
             (cap is {HASH_SWEEP_MAX_KEYS_PER_TICK})"
        );
    }

    /// The bound must not become a leak: repeated ticks drain the whole
    /// backlog, and the index empties with it.
    #[test]
    fn hash_field_sweep_drains_backlog_across_ticks() {
        const DUE: u32 = 1_200;
        let mut db = db_with_due_field_hashes(DUE);

        let mut ticks = 0;
        while db.len() > 0 && ticks < 64 {
            expire_cycle(&mut db, &mut |_| {});
            ticks += 1;
        }

        assert_eq!(db.len(), 0, "every all-fields-expired hash must be removed");
        assert_eq!(
            db.hash_expiry_index_len(),
            0,
            "the hash-field index must empty with the backlog"
        );
        assert!(
            !db.hash_field_ttl_possible(),
            "with nothing indexed the sweep must be skippable again"
        );
        assert!(
            ticks > 1,
            "with {DUE} due hashes and a {HASH_SWEEP_MAX_KEYS_PER_TICK} cap \
             the drain must take more than one tick"
        );
    }

    /// A single enormous hash must not stall the loop either: one reap call
    /// removes at most `max_fields`, and the hash is picked up again.
    #[test]
    fn one_reap_call_removes_at_most_max_fields() {
        let mut db = Database::new();
        let ttl = db.now_ms() + 1_000;
        {
            let map = db.get_or_create_hash(b"big").expect("hash");
            for i in 0..100u32 {
                map.insert(Bytes::from(format!("f{i}")), Bytes::from_static(b"v"));
            }
        }
        for i in 0..100u32 {
            let f = format!("f{i}");
            assert_eq!(
                db.hash_set_field_ttl(b"big", f.as_bytes(), ttl, HashTtlCond::Always),
                Ok(1)
            );
        }
        db.set_cached_now_ms_for_test(ttl + 1);

        let outcome = db.reap_expired_fields_one_hash_at(b"big", db.now_ms(), 4);
        assert_eq!(outcome, ReapOutcome::FieldsRemoved);
        let live = db.get_hash(b"big").expect("hash").expect("exists").len();
        assert_eq!(
            live, 96,
            "exactly 4 of the 100 due fields may leave in one capped call"
        );

        // The whole hash still drains once the sweep resumes it.
        let mut ticks = 0;
        while db.len() > 0 && ticks < 64 {
            expire_cycle(&mut db, &mut |_| {});
            ticks += 1;
        }
        assert_eq!(db.len(), 0);
    }

    /// The sweep must touch only DUE hashes — the pre-#543 implementation
    /// called `reap_expired_fields_one_hash` on every `HashWithTtl` key in
    /// the database, due or not, at O(N) table-scan cost per tick.
    #[test]
    fn hash_field_sweep_visits_only_due_hashes() {
        let mut db = Database::new();
        let future = db.now_ms() + 3_600_000;
        for i in 0..500u32 {
            let key = format!("live{i}");
            {
                let map = db.get_or_create_hash(key.as_bytes()).expect("hash");
                map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
            }
            assert_eq!(
                db.hash_set_field_ttl(key.as_bytes(), b"f", future, HashTtlCond::Always),
                Ok(1)
            );
        }
        {
            let map = db.get_or_create_hash(b"due").expect("hash");
            map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        }
        let soon = db.now_ms() + 1_000;
        assert_eq!(
            db.hash_set_field_ttl(b"due", b"f", soon, HashTtlCond::Always),
            Ok(1)
        );
        db.set_cached_now_ms_for_test(soon + 1);

        expire_cycle(&mut db, &mut |_| {});

        assert_eq!(db.len(), 500, "only the due hash may be reaped");
        assert!(db.data().get(b"due").is_none());
        assert_eq!(
            db.hash_expiry_index_len(),
            500,
            "the live hashes stay indexed at their own deadlines"
        );
    }

    // ── moon#552: skip the cycle when nothing is due ─────────────────────

    /// The head-peek gate must answer "nothing due" for a TTL-HEAVY database
    /// whose keys simply are not due yet — the case the
    /// `maybe_has_expiring_keys` latch never covered.
    #[test]
    fn nothing_due_gate_covers_a_ttl_heavy_but_idle_database() {
        let mut db = Database::new();
        let now = current_time_ms();
        for i in 0..1_000u32 {
            db.set(
                Bytes::from(format!("k{i}")),
                Entry::new_string_with_expiry(Bytes::from_static(b"v"), now + 3_600_000),
            );
        }
        assert!(db.maybe_has_expiring_keys(), "the old latch stays up");
        assert!(
            nothing_due(&db),
            "1000 not-yet-due TTLs must not make the tick do work"
        );

        // One due key flips it.
        db.set(
            Bytes::from_static(b"due"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), now - 1),
        );
        assert!(!nothing_due(&db));
    }

    /// A due HASH FIELD must also defeat the gate — otherwise #552's skip
    /// would silently disable sweep 2 for every database with no whole-key
    /// TTLs at all.
    #[test]
    fn nothing_due_gate_respects_due_hash_fields() {
        let mut db = db_with_due_field_hashes(1);
        assert!(
            db.expiry_index_is_empty(),
            "no whole-key TTL exists in this database"
        );
        assert!(!nothing_due(&db), "a due hash field is work");

        let mut removed = 0usize;
        expire_cycle_direct(&mut db, &mut |_| removed += 1);
        assert_eq!(db.len(), 0, "the skip must not have swallowed the reap");
        assert!(nothing_due(&db), "and now there is nothing left to do");
    }

    /// End-to-end: the skip must not lose a key that becomes due later, and
    /// must not clear the latch while TTL'd keys remain.
    #[test]
    fn skipped_tick_still_expires_the_key_once_it_is_due() {
        let mut db = Database::new();
        let now = current_time_ms();
        db.set(
            Bytes::from_static(b"k"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), now + 3_600_000),
        );

        let mut removed = 0usize;
        expire_cycle_direct(&mut db, &mut |_| removed += 1);
        assert_eq!(removed, 0, "not due yet");
        assert_eq!(db.len(), 1);
        assert!(
            db.maybe_has_expiring_keys(),
            "a live TTL must keep the sweep armed"
        );

        // Retarget into the past (SET with a new TTL re-indexes the pair).
        db.set(
            Bytes::from_static(b"k"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), now - 1),
        );
        expire_cycle_direct(&mut db, &mut |_| removed += 1);
        assert_eq!(removed, 1, "the newly-due key must be expired and emitted");
        assert_eq!(db.len(), 0);
    }

    /// An empty database drops the latch through the skip path, so every
    /// later tick takes the one-branch fast path instead of two head-peeks.
    #[test]
    fn skip_path_lowers_the_latch_when_both_indexes_are_empty() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"k"),
            Entry::new_string_with_expiry(Bytes::from_static(b"v"), current_time_ms() + 60_000),
        );
        db.remove(b"k");
        assert!(db.maybe_has_expiring_keys(), "latch is still up after DEL");

        expire_cycle_direct(&mut db, &mut |_| {});
        assert!(
            !db.maybe_has_expiring_keys(),
            "the skip path must do the flag maintenance the cycle would have"
        );
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
