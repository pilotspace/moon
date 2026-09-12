//! In-place integer mutation for `INCR`/`INCRBY`/`DECR`/`DECRBY` (moon#942).
//!
//! # Why this exists
//!
//! Before this module, adding one to a counter cost **three independent
//! `DashTable` probes** plus a full `Entry` teardown and rebuild:
//!
//! ```text
//! incrby_internal
//!   db.get(key)                           kv_ops.rs:29   PROBE 1  (classify)
//!                                         kv_ops.rs:36   PROBE 2  (NLL re-probe)
//!   Entry::new_string_from_slice(..)                     new Entry built
//!   db.set(key, entry)
//!       entry_overhead(key, &entry)                      cost of the NEW value
//!       data.insert_or_update(CompactKey::from(key), ..) PROBE 3 + key bytes copied
//!           update closure: entry_overhead(key, existing) cost of the OLD value
//! ```
//!
//! Redis's `incrDecrCommand` does **one** `lookupKeyWrite` and then updates
//! `o->ptr` in place. [`Database::incr_hot_string_in_place`] is moon's
//! equivalent: one `get_mut` probe, one `CompactValue` assignment, and the
//! bookkeeping `Database::set` would have done — no re-hash of the key, no
//! `CompactKey` copy, no `Entry` rebuild.
//!
//! # The failure mode this module is built around
//!
//! The write the old path performed had **eleven** observable effects — nine
//! inside `Database::set`, two more applied by the caller to the `Entry` it
//! rebuilt. A fast path that does one of them and skips the other ten is not
//! faster, it is broken, and broken in ways no throughput benchmark can see.
//! Every one is enumerated here with the decision made for it, and every
//! *reproduced* one has a named test — in `#[cfg(test)] mod incr_in_place_942`
//! below for the ones a `Database` can observe, and in
//! `tests/incr_in_place_942.rs` for the ones that need a live server.
//!
//! | # | the old path did | in place |
//! |---|---|---|
//! | 1 | `record_keyspace_change()` | **reproduced**, on the success branch only |
//! | 2 | `spill_inflight_forget(key)` when the plane is non-empty (#459) | **reproduced** |
//! | 3 | `entry_overhead` old/new -> `used_memory` | **reproduced** as a value-size delta (`key.len() + 128` cancels) |
//! | 4 | `hash_expiry_index_note_value(key, &entry)` (moon#543) | **N/A** — a string replaces a string; neither can be `HashWithTtl` |
//! | 5 | `next_birth_version()` | **N/A** — the ticket is consumed only by `set`'s `Inserted` arm, and this path is always an update |
//! | 6 | `Entry::bump_version` on the `Updated` arm (moon#926) | **reproduced** via `stamp_mutation`, on the success branch only |
//! | 7 | `cold_index.remove(key)` on the `Updated` arm (task #56) | **reproduced** |
//! | 8 | `maybe_has_expiring_keys = true` when the entry has a TTL | **reproduced** |
//! | 9 | `expiry_index` maintenance when `old_ttl != new_ttl` | **N/A** — this path never changes a TTL, so `set`'s own guard would be false |
//! | 10 | `entry.set_last_access(db.now())` (caller-side) | **reproduced** from the shard-cached clock |
//! | 11 | LFU counter reset to `LFU_INIT_VAL` (caller-side, a consequence of rebuilding the `Entry`) | **reproduced** verbatim — changing which keys the evictor picks is not this patch's business |
//!
//! Two effects sit outside this table because they are not `Database`'s: the
//! `incrby` keyspace notification and the AOF/replication record. Both are
//! driven by the *command layer* from the reply — the handler still emits the
//! notification on success only, and `handler_monoio` still gates the log on
//! `metadata::is_write(cmd) && !matches!(response, Frame::Error(_))`. This
//! path changes neither, which is why every error outcome must keep returning
//! `Frame::Error`.
//!
//! The two cases the fast path refuses ([`IncrOutcome::NotHot`]) are the ones
//! where those "N/A"s stop holding: an absent key (the value may be in the
//! cold tier or the in-flight spill plane — fabricating a `0` here would
//! destroy it), and a TTL-expired key (still physically in the `DashTable`;
//! taking it in place would resurrect a dead value and keep its stale
//! deadline). Both fall back to the original `get` + `set` pair, which is
//! already correct for them.
//!
//! # What the fallback costs
//!
//! Stated so it is not discovered later: a refused call has already spent its
//! `get_mut` probe, so `INCR` on an **absent** key now costs 5 `DashTable`
//! probes where it used to cost 4 (and 3 instead of 2 on a TTL-expired one).
//! That is the deliberate trade — a counter is created once and incremented
//! many times, and the alternative (fabricating the key here) would have to
//! re-implement cold-tier promotion and in-flight-spill rehydration, which is
//! exactly how a spilled counter gets silently reset to zero.

use crate::storage::compact_value::CompactValue;
use crate::storage::db::{Database, stamp_mutation};
use crate::storage::entry::LFU_INIT_VAL;

/// What [`Database::incr_hot_string_in_place`] did.
///
/// Deliberately NOT a `Frame`: the storage layer does not build replies, and
/// the two error strings differ between "not an integer" and "would
/// overflow". The caller maps these onto the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IncrOutcome {
    /// Mutated in place. The stored value is now this.
    Applied(i64),
    /// The stored value is a string, but not one `i64::from_str` accepts.
    /// **Nothing was written.**
    NotInteger,
    /// The stored value is not a string at all. **Nothing was written.**
    WrongType,
    /// `current + delta` does not fit `i64`. **Nothing was written** — Redis
    /// leaves the counter at its old value on overflow, and so does this.
    Overflow,
    /// The key is not hot-and-live (absent, or present but TTL-expired). The
    /// caller must fall back to the general `get` + `set` path, which owns
    /// cold-tier promotion, in-flight-spill rehydration, lazy-expiry
    /// bookkeeping and key creation.
    NotHot,
}

impl Database {
    /// Add `delta` to the integer string stored at `key`, mutating the stored
    /// value **in place** — one `DashTable` probe, no `Entry` rebuild, no
    /// re-hash of the key.
    ///
    /// Returns [`IncrOutcome::NotHot`] without touching anything when the key
    /// is absent or expired; see the module docs for why those two cases are
    /// deliberately refused rather than handled here.
    ///
    /// Every error outcome leaves the keyspace **bit-for-bit unchanged** —
    /// no version bump (moon#926/#940: a failed CAS must not abort a watching
    /// transaction), no `record_keyspace_change`, no ledger movement.
    pub(crate) fn incr_hot_string_in_place(&mut self, key: &[u8], delta: i64) -> IncrOutcome {
        let now_ms = self.cached_now_ms;
        let now_secs = self.cached_now;

        // THE probe. Everything below is decided from this one borrow; unlike
        // `Database::get` (whose NLL re-probe at `kv_ops.rs:36` this path
        // avoids entirely) nothing here re-hashes the key.
        let Some(entry) = self.data.get_mut(key) else {
            return IncrOutcome::NotHot;
        };
        if entry.is_expired_at(now_ms) {
            // Physically present, logically gone. `Database::get` hides it and
            // leaves the emitting drain to delete it; replicating that here
            // would duplicate `note_lazy_expired`'s contract, so refuse.
            return IncrOutcome::NotHot;
        }
        let Some(bytes) = entry.value.as_bytes() else {
            return IncrOutcome::WrongType;
        };
        // Parsed exactly as the pre-#942 handler did — `from_utf8` then
        // `i64::from_str`, NOT `canonical_i64`. They disagree on leading
        // zeros and a leading `+`, and changing which one INCR uses is a
        // client-visible behaviour change that does not belong in a
        // performance patch.
        let Ok(text) = std::str::from_utf8(bytes) else {
            return IncrOutcome::NotInteger;
        };
        let Ok(current) = text.parse::<i64>() else {
            return IncrOutcome::NotInteger;
        };
        let Some(new_val) = current.checked_add(delta) else {
            return IncrOutcome::Overflow;
        };

        // ── Past this point the mutation is committed. ────────────────────
        // `entry_overhead` is `key.len() + value.estimate_memory() + 128`;
        // the key and the constant are identical before and after, so the
        // ledger delta is exactly the value's. Snapshotting it here rather
        // than recomputing the whole overhead twice is what makes this O(1)
        // with no key length walk.
        let old_cost = entry.value.estimate_memory();
        let mut itoa_buf = itoa::Buffer::new();
        // Assignment drops the old `CompactValue`, which is what frees the
        // `Box<[u8]>` on a heap->inline or heap->heap transition. `itoa`
        // writes into a stack buffer; nothing here allocates for a counter
        // that inlines (<= 12 digits).
        entry.value = CompactValue::from_slice(itoa_buf.format(new_val).as_bytes());
        let new_cost = entry.value.estimate_memory();
        entry.set_last_access(now_secs);
        // The pre-#942 path rebuilt the entry from scratch every time, which
        // reset the LFU counter to `LFU_INIT_VAL`. Preserved verbatim: making
        // INCR *accumulate* LFU credit would change which keys the evictor
        // picks, which is a behaviour change and not this patch's business.
        entry.set_access_counter(LFU_INIT_VAL);
        // moon#926: this IS the WATCH bump, and it is placed after every
        // early return above so an error reply cannot move it.
        stamp_mutation(entry);
        let has_expiry = entry.has_expiry();
        // `entry` borrow ends here; the rest needs `&mut self`.

        // (1) The rdb dirty counter. `set` charges one per write; so do we.
        crate::admin::metrics_setup::record_keyspace_change();

        // (2) #459: an overwrite makes a queued spill payload stale, and the
        // record is that completion's authorisation to publish into
        // `cold_index`. A key that is hot should not also be in flight (the
        // evictor `db.remove`s before it marks), but `set` does not rely on
        // that invariant and neither does this: one `is_empty()` load on the
        // normal path is not worth a resurrection bug.
        if !self.spill_inflight_is_empty() {
            self.spill_inflight_forget(key);
        }

        // (7) Task #56: a SECOND write to a cold-shadowed key proves the
        // shadow stale. This is a second write by construction — the key was
        // already hot. Leaving the shadow lets `demote_replayed_cold_shadows`
        // discard the newer hot value on the next restart.
        if let Some(ci) = self.cold_index.as_mut() {
            ci.remove(key);
        }

        // (8) The TTL is carried over untouched, so this is redundant
        // whenever the deadline index is non-empty — which it provably is
        // when `has_expiry` holds. Kept anyway, for exact parity with `set`:
        // one predictable branch, and it removes a cross-module invariant
        // from the correctness argument.
        if has_expiry {
            self.maybe_has_expiring_keys = true;
        }

        // (3) The ledger.
        self.adjust_memory(old_cost, new_cost);

        IncrOutcome::Applied(new_val)
    }
}

// ---------------------------------------------------------------------------
// moon#942 — INCR in place.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod incr_in_place_942 {
    use super::IncrOutcome;
    use crate::storage::db::Database;
    use crate::storage::entry::{Entry, current_time_ms};
    use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};
    use bytes::Bytes;

    /// The pre-#942 implementation of `incrby_internal`, verbatim minus the
    /// `Frame` plumbing and the keyspace notification. Every differential test
    /// below asserts the new in-place path is observationally identical to
    /// THIS — which is the only way to catch a `Database::set` side effect
    /// that the fast path silently dropped.
    fn reference_incr(db: &mut Database, key: &[u8], delta: i64) -> Result<i64, &'static str> {
        let (current, existing_expiry_ms) = match db.get(key) {
            Some(entry) => {
                let expiry = entry.expires_at_ms();
                match entry.value.as_bytes() {
                    Some(v) => {
                        let s = match std::str::from_utf8(v) {
                            Ok(s) => s,
                            Err(_) => return Err("not-integer"),
                        };
                        match s.parse::<i64>() {
                            Ok(n) => (n, expiry),
                            Err(_) => return Err("not-integer"),
                        }
                    }
                    None => return Err("wrongtype"),
                }
            }
            None => (0, 0),
        };
        let new_val = match current.checked_add(delta) {
            Some(v) => v,
            None => return Err("overflow"),
        };
        let mut itoa_buf = itoa::Buffer::new();
        let printed = itoa_buf.format(new_val).as_bytes();
        let mut entry = if existing_expiry_ms > 0 {
            Entry::new_string_from_slice_with_expiry(printed, existing_expiry_ms)
        } else {
            Entry::new_string_from_slice(printed)
        };
        entry.set_last_access(db.now());
        entry.set_access_counter(5);
        db.set(key, entry);
        Ok(new_val)
    }

    /// Everything a client (or the eviction ledger, or a WATCHing
    /// transaction, or a restart) can observe about a key after a write.
    #[derive(Debug, PartialEq, Eq)]
    struct Observed {
        value: Option<Vec<u8>>,
        ttl_ms: u64,
        version: u32,
        access_counter: u8,
        used_memory: usize,
        cold_shadow: bool,
        inflight: bool,
        maybe_expiring: bool,
        expiry_index_len: usize,
    }

    fn observe(db: &Database, key: &[u8]) -> Observed {
        let e = db.data.get(key);
        Observed {
            value: e.and_then(|e| e.value.as_bytes().map(|b| b.to_vec())),
            ttl_ms: e.map(|e| e.expires_at_ms()).unwrap_or(0),
            version: e.map(|e| e.version()).unwrap_or(0),
            access_counter: e.map(|e| e.access_counter()).unwrap_or(0),
            used_memory: db.used_memory,
            cold_shadow: db
                .cold_index
                .as_ref()
                .is_some_and(|ci| ci.lookup(key).is_some()),
            inflight: !db.spill_inflight_is_empty(),
            maybe_expiring: db.maybe_has_expiring_keys,
            expiry_index_len: db.expiry_index.len(),
        }
    }

    /// Run `seed` on two fresh databases, then apply the reference path to one
    /// and the in-place path to the other, and require every observable to
    /// agree.
    fn differential(label: &str, seed: impl Fn(&mut Database), delta: i64) {
        let key: &[u8] = b"ctr";

        let mut want_db = Database::new();
        seed(&mut want_db);
        let want_res = reference_incr(&mut want_db, key, delta);
        let want = observe(&want_db, key);

        let mut got_db = Database::new();
        seed(&mut got_db);
        let got_res = match got_db.incr_hot_string_in_place(key, delta) {
            IncrOutcome::Applied(n) => Ok(n),
            IncrOutcome::NotInteger => Err("not-integer"),
            IncrOutcome::WrongType => Err("wrongtype"),
            IncrOutcome::Overflow => Err("overflow"),
            IncrOutcome::NotHot => reference_incr(&mut got_db, key, delta),
        };
        let got = observe(&got_db, key);

        assert_eq!(want_res, got_res, "{label}: reply diverged");
        assert_eq!(want, got, "{label}: observable state diverged");
    }

    fn seed_string(v: &'static [u8]) -> impl Fn(&mut Database) {
        move |db: &mut Database| db.set(b"ctr", Entry::new_string(Bytes::from_static(v)))
    }

    // ── Side effect 1 · the value itself, across every encoding width ──────

    #[test]
    fn width_transitions_match_the_set_path() {
        for (label, v, delta) in [
            ("single digit", &b"1"[..], 1),
            ("9 -> 10", &b"9"[..], 1),
            ("10 -> 9", &b"10"[..], -1),
            ("99 -> 100", &b"99"[..], 1),
            ("100 -> 99", &b"100"[..], -1),
            ("0 -> -1", &b"0"[..], -1),
            ("-1 -> 0", &b"-1"[..], 1),
            ("-9 -> -10", &b"-9"[..], -1),
            ("i32 max", &b"2147483647"[..], 1),
            ("i32 min", &b"-2147483648"[..], -1),
            // The SSO seam: `CompactValue` inlines <= 12 bytes. 12 digits are
            // inline, 13 are a `Box<[u8]>`; both directions must land on the
            // right kind AND bill the right number of bytes.
            ("inline -> heap", &b"999999999999"[..], 1),
            ("heap -> inline", &b"1000000000000"[..], -1),
            ("heap -> heap", &b"1000000000000"[..], 1),
            ("negative inline -> heap", &b"-99999999999"[..], -1),
            ("i64 max - 1", &b"9223372036854775806"[..], 1),
            ("i64 min + 1", &b"-9223372036854775807"[..], -1),
        ] {
            differential(label, seed_string(v), delta);
        }
    }

    // ── Side effect 2 · WATCH version ──────────────────────────────────────

    #[test]
    fn version_bumps_exactly_once_per_successful_incr() {
        let mut db = Database::new();
        db.set(b"ctr", Entry::new_string(Bytes::from_static(b"0")));
        let mut prev = db.get_version(b"ctr");
        for i in 1..=64 {
            assert!(matches!(
                db.incr_hot_string_in_place(b"ctr", 1),
                IncrOutcome::Applied(_)
            ));
            let now = db.get_version(b"ctr");
            assert_eq!(
                now,
                Entry::bump_version(prev),
                "INCR #{i} must advance the WATCH version by exactly one step"
            );
            prev = now;
        }
    }

    #[test]
    fn a_failing_incr_never_bumps_the_version() {
        // moon#940's failure mode pointed at INCR: an error reply must leave a
        // watching transaction able to commit.
        for (label, seed, delta) in [
            ("non-integer value", &b"abc"[..], 1i64),
            ("overflow", &b"9223372036854775807"[..], 1),
            ("underflow", &b"-9223372036854775808"[..], -1),
        ] {
            let mut db = Database::new();
            db.set(b"ctr", Entry::new_string(Bytes::copy_from_slice(seed)));
            let before = observe(&db, b"ctr");
            let r = db.incr_hot_string_in_place(b"ctr", delta);
            assert!(
                matches!(r, IncrOutcome::NotInteger | IncrOutcome::Overflow),
                "{label}: expected an error outcome, got {r:?}"
            );
            assert_eq!(
                before,
                observe(&db, b"ctr"),
                "{label}: a failing INCR must leave the key, the version and \
                 the ledger completely untouched"
            );
        }
    }

    #[test]
    fn wrongtype_leaves_everything_untouched() {
        let mut db = Database::new();
        db.set(b"ctr", Entry::new_list());
        let before = observe(&db, b"ctr");
        assert!(matches!(
            db.incr_hot_string_in_place(b"ctr", 1),
            IncrOutcome::WrongType
        ));
        assert_eq!(before, observe(&db, b"ctr"));
    }

    // ── Side effect 3 · the memory ledger ──────────────────────────────────

    #[test]
    fn the_ledger_stays_exact_across_every_width() {
        let mut db = Database::new();
        db.set(
            b"ctr",
            Entry::new_string(Bytes::from_static(b"999999999990")),
        );
        for step in 0..40 {
            assert!(matches!(
                db.incr_hot_string_in_place(b"ctr", 1),
                IncrOutcome::Applied(_)
            ));
            let running = db.used_memory;
            db.recalculate_memory();
            assert_eq!(
                running, db.used_memory,
                "step {step}: the running ledger drifted from a full recompute"
            );
        }
        for step in 0..40 {
            assert!(matches!(
                db.incr_hot_string_in_place(b"ctr", -1),
                IncrOutcome::Applied(_)
            ));
            let running = db.used_memory;
            db.recalculate_memory();
            assert_eq!(running, db.used_memory, "shrink step {step}: ledger drift");
        }
    }

    // ── Side effect 4 · TTL and the deadline index ─────────────────────────

    #[test]
    fn an_existing_ttl_survives_and_the_deadline_index_does_not_move() {
        let deadline = current_time_ms() + 3_600_000;
        differential(
            "ttl preserved",
            move |db: &mut Database| {
                db.set(
                    b"ctr",
                    Entry::new_string_from_slice_with_expiry(b"41", deadline),
                );
            },
            1,
        );
    }

    #[test]
    fn the_expiring_keys_latch_is_raised_exactly_as_set_would_raise_it() {
        // `differential` cannot see this one: the seeding `db.set` has already
        // raised the latch, so dropping the re-raise is invisible there.
        // Lower it by hand first and the assertion becomes real.
        let deadline = current_time_ms() + 3_600_000;
        let mut db = Database::new();
        db.set(
            b"ctr",
            Entry::new_string_from_slice_with_expiry(b"41", deadline),
        );
        db.maybe_has_expiring_keys = false;
        assert!(matches!(
            db.incr_hot_string_in_place(b"ctr", 1),
            IncrOutcome::Applied(42)
        ));
        assert!(
            db.maybe_has_expiring_keys,
            "a write to an entry that carries a TTL raises the fast-path latch \
             in `Database::set`; the in-place path must raise it too"
        );
    }

    #[test]
    fn an_expired_key_is_not_taken_by_the_fast_path() {
        let mut db = Database::new();
        let deadline = current_time_ms() + 1_000;
        db.set(
            b"ctr",
            Entry::new_string_from_slice_with_expiry(b"41", deadline),
        );
        db.set_cached_now_ms_for_test(deadline + 1);
        assert!(
            matches!(db.incr_hot_string_in_place(b"ctr", 1), IncrOutcome::NotHot),
            "an expired entry is still IN the DashTable — taking it in place \
             would resurrect a dead value and keep its stale TTL"
        );
    }

    // ── Side effect 5 · the stale cold shadow (`set`'s Updated arm) ────────

    #[test]
    fn an_in_place_incr_drops_the_stale_cold_shadow() {
        let mut db = Database::new();
        let mut ci = ColdIndex::new();
        ci.insert(
            Bytes::from_static(b"ctr"),
            ColdLocation {
                file_id: 1,
                page_idx: 0,
                slot_idx: 0,
                ttl_ms: None,
                value_type: crate::persistence::kv_page::ValueType::String,
            },
        );
        db.cold_index = Some(ci);
        // First touch: ambiguous, leaves the shadow (this is `set`'s rule).
        db.set(b"ctr", Entry::new_string(Bytes::from_static(b"1")));
        assert!(db.cold_index.as_ref().unwrap().lookup(b"ctr").is_some());
        // Second touch, in place. Same proof as `set`'s `Updated` arm: a
        // second write to a cold-shadowed key proves the shadow stale, and
        // leaving it behind resurrects the old value across a restart.
        assert!(matches!(
            db.incr_hot_string_in_place(b"ctr", 1),
            IncrOutcome::Applied(2)
        ));
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"ctr").is_none(),
            "the in-place path must invalidate the now-stale cold shadow"
        );
    }

    // ── Side effect 6 · the in-flight spill plane (#459) ───────────────────

    #[test]
    fn an_in_place_incr_retires_an_in_flight_spill_payload() {
        use crate::storage::db::PendingSpill;
        let mut db = Database::new();
        db.set(b"ctr", Entry::new_string(Bytes::from_static(b"1")));
        // A queued spill still carrying the OLD payload. Its completion is
        // authorised to publish into `cold_index`; an overwrite has to
        // withdraw that authorisation or the stale value becomes
        // authoritative again after a restart (#459).
        db.spill_inflight_mark(
            Bytes::from_static(b"ctr"),
            PendingSpill {
                req_id: 1,
                value_type: crate::persistence::kv_page::ValueType::String,
                value_bytes: Bytes::from_static(b"1"),
                ttl_ms: None,
            },
        );
        assert!(!db.spill_inflight_is_empty());
        assert!(matches!(
            db.incr_hot_string_in_place(b"ctr", 1),
            IncrOutcome::Applied(2)
        ));
        assert!(
            db.spill_inflight_is_empty(),
            "the in-place path must retire the in-flight spill record, or its \
             completion republishes the pre-INCR value as a cold shadow (#459)"
        );
    }

    // ── Side effect 7 · the dirty counter ──────────────────────────────────
    //
    // `record_keyspace_change` lands in a PROCESS-GLOBAL sharded counter, so
    // a unit test cannot assert a delta of exactly one: every other test in
    // this binary is writing to the same counter concurrently. The guard for
    // this side effect is therefore an integration test against a real
    // server, where the process is ours — `rdb1_a_successful_incr_is_one_dirty_change`
    // in `tests/incr_in_place_942.rs`.

    // ── Side effect 8 · LRU / LFU metadata ─────────────────────────────────

    #[test]
    fn the_access_clock_is_the_shard_cached_one() {
        let mut db = Database::new();
        db.set(b"ctr", Entry::new_string(Bytes::from_static(b"1")));
        db.data.get_mut(b"ctr").unwrap().set_last_access(0);
        assert!(matches!(
            db.incr_hot_string_in_place(b"ctr", 1),
            IncrOutcome::Applied(2)
        ));
        assert_eq!(
            db.data.get(b"ctr").unwrap().last_access(),
            db.now(),
            "last_access must come from the shard-cached clock, never a syscall"
        );
    }

    #[test]
    fn the_lfu_counter_is_reset_the_way_the_set_path_resets_it() {
        // `differential` cannot see this one either: a freshly `set` entry is
        // already at `LFU_INIT_VAL`, so dropping the reset is invisible there.
        // Drive the counter somewhere else first.
        let mut db = Database::new();
        db.set(b"ctr", Entry::new_string(Bytes::from_static(b"1")));
        db.data.get_mut(b"ctr").unwrap().set_access_counter(200);
        assert!(matches!(
            db.incr_hot_string_in_place(b"ctr", 1),
            IncrOutcome::Applied(2)
        ));
        assert_eq!(
            db.data.get(b"ctr").unwrap().access_counter(),
            5,
            "the pre-#942 path rebuilt the entry, which reset the LFU counter \
             to LFU_INIT_VAL on every INCR. Preserving that verbatim is what \
             keeps the evictor picking the same victims."
        );
    }

    // ── The fallback seam: a key that is only in the spill plane ──────────

    /// The full command, not the storage method: this is the one case where
    /// declining in the fast path and delegating has to actually reach the
    /// promotion logic. A key mid-spill is in NO plane `data` can see, and
    /// answering `1` for it would be the #459 failure mode with an INCR on
    /// the front.
    #[test]
    fn incr_on_a_key_that_is_only_in_the_spill_plane_promotes_it() {
        use crate::protocol::Frame;
        use crate::storage::db::PendingSpill;

        let mut db = Database::new();
        // Exactly what `evict_one_async_spill` leaves behind: hot copy gone,
        // payload parked on the in-flight plane.
        db.spill_inflight_mark(
            Bytes::from_static(b"ctr"),
            PendingSpill {
                req_id: 1,
                value_type: crate::persistence::kv_page::ValueType::String,
                value_bytes: Bytes::from_static(b"41"),
                ttl_ms: None,
            },
        );
        assert!(
            !db.is_hot(b"ctr"),
            "precondition: the key is not in hot RAM"
        );

        let r =
            crate::command::string::incr(&mut db, &[Frame::BulkString(Bytes::from_static(b"ctr"))]);
        assert_eq!(
            r,
            Frame::Integer(42),
            "INCR must promote the in-flight payload and add to 41 — a `1` \
             here means the counter was silently reset to zero (#459)"
        );
        assert!(
            db.spill_inflight_is_empty(),
            "promotion retires the in-flight record"
        );
    }

    // ── The absent key: fabricating it here would skip the cold tier ───────

    #[test]
    fn an_absent_key_is_left_to_the_slow_path() {
        let mut db = Database::new();
        assert!(
            matches!(db.incr_hot_string_in_place(b"nope", 1), IncrOutcome::NotHot),
            "a miss must fall back: the key may be sitting in the cold tier \
             or the in-flight spill plane, and fabricating a 0 here would \
             destroy it"
        );
        assert!(
            db.data.get(b"nope").is_none(),
            "the fast path must not create"
        );
    }
}
