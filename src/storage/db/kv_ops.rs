//! Core keyspace operations: get/set/remove, lazy expiry, cold-tier promotion, scan (split from db/mod.rs).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::{DashTable, InsertOrUpdate};
use crate::storage::entry::{Entry, RedisValue, current_time_ms};

use crate::storage::db::{Database, entry_overhead};

impl Database {
    /// Get an entry by key, performing lazy expiration.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: a single immutable probe classifies the key as live,
    /// expired, or absent. NLL forces one re-probe on the live path
    /// (the cold fallback below mutates `self`, so the first borrow cannot
    /// be returned directly): 2 probes on a live hit, 1 on a miss — down
    /// from 3/2 with the previous expiry-check + `is_some()` + get chain.
    /// No LRU touch on reads (callers requiring LRU updates use `get_mut()`).
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        let now_ms = self.cached_now_ms;
        enum KeyState {
            Live,
            Expired,
            Absent,
        }
        let state = match self.data.get(key) {
            Some(e) if e.is_expired_at(now_ms) => KeyState::Expired,
            Some(_) => KeyState::Live,
            None => KeyState::Absent,
        };
        match state {
            // Hot path: single re-probe, borrow returned to caller.
            KeyState::Live => self.data.get(key),
            KeyState::Expired => {
                // moon#542: HIDE, don't remove. Deletion belongs to the
                // active-expiry drain, which emits the keyspace `expired`
                // notification and the dual-plane DEL this layer cannot
                // (no plane handles here) — and a replica must never
                // delete on read at all (it waits for the master's DEL).
                self.note_lazy_expired(key);
                None
            }
            KeyState::Absent => {
                // Cold fallback: promote from disk into hot RAM if spilled
                // there by eviction, then re-probe. `promote_cold_if_present`
                // also owns reclaiming a stale (TTL-expired) cold-index entry.
                self.promote_cold_if_present(key, now_ms);
                self.data.get(key)
            }
        }
    }

    /// If `key` is missing from hot RAM but present in the cold tier
    /// (spilled there by eviction), read it from disk and promote it into
    /// hot RAM, removing the cold-index entry — this is the cold-fallback
    /// branch [`Self::get`] has always used, factored out so every mutable
    /// accessor can share it.
    ///
    /// Returns `true` if `key` is present in hot RAM after this call returns
    /// (either because it already was, or because promotion just happened).
    /// Returns `false` for a genuine miss (absent from both tiers), or when
    /// the cold entry's TTL had already passed — the stale index entry is
    /// reclaimed as a byproduct in that case, same as the expired-hot branch
    /// above.
    ///
    /// Cheap on the common case callers actually care about (key already
    /// hot): a single `contains_key` probe short-circuits before ever
    /// touching the cold tier. Only a genuine miss pays for the `ColdIndex`
    /// lookup + a blocking disk `pread`.
    ///
    /// P0 fix (`.planning/reviews/storage-audit-2026-07-12-kv.md`):
    /// `get_or_create_hash`/`_list`/`_set`/`_sorted_set`/`_stream` (and their
    /// compact-encoding siblings `get_or_create_hash_listpack`,
    /// `get_or_create_list_listpack`, `get_or_create_intset`) used to skip
    /// straight to fabricating a brand-new EMPTY container whenever
    /// `self.data` missed the key — even when the real value was sitting
    /// right there in the cold tier. The fabricated container then got
    /// written back over the cold copy on the next `set`/mutation,
    /// permanently destroying it. Calling this method first closes that gap.
    ///
    /// Correctness (task #59 review): this is a plain, ORIGINAL synchronous
    /// blocking disk read — no timeout, no possibility of returning "not
    /// found" for a key that actually exists on disk. A prior revision of
    /// this method routed through a bounded/timeout off-thread pool that
    /// could time out and silently answer `Miss` for an existing spilled
    /// key; that was a correctness regression (a GET on an existing key
    /// could return nil under disk backlog) and has been removed. See
    /// `tmp/task59-design.md` for the corrected design: the shard-thread
    /// stall is instead addressed by [`Self::promote_cold_outcome`] +
    /// `storage::tiered::cold_read_pool::read_cold_entry_async`, used from
    /// the async connection-handler layer for the read-only single-key
    /// commands that can afford to `.await` (currently GET); MULTI/EXEC
    /// bodies and Lua `redis.call` keep calling this original synchronous
    /// method unchanged.
    pub fn promote_cold_if_present(&mut self, key: &[u8], now_ms: u64) -> bool {
        if self.data.contains_key(key) {
            return true;
        }
        if self.promote_inflight_if_present(key, now_ms) {
            return true;
        }
        // Look up the location first (cheap, in-memory) so the outcome below
        // can be paired with the location it was read from -- required by
        // `promote_cold_outcome`'s revalidation (see its doc comment). This
        // path never awaits between lookup and use, so the location can't
        // actually change out from under it, but sharing one code path with
        // the async caller keeps the invariant enforced in exactly one
        // place instead of two.
        let Some((location, shard_dir)) = self.cold_lookup_location(key) else {
            return false;
        };
        let outcome =
            crate::storage::tiered::cold_read::read_cold_entry(&shard_dir, location, now_ms, None);
        self.promote_cold_outcome(key, now_ms, location, outcome)
    }

    /// Pull `key` back into hot RAM from the IN-FLIGHT spill plane, if it is
    /// there and not TTL-expired. Returns `true` if the key is hot after this
    /// call because of it.
    ///
    /// Cheaper than the cold path it precedes: the payload is already in RAM
    /// (it is the queued `SpillRequest`'s own refcounted buffer), so this
    /// costs a rehydrate and no disk read at all.
    ///
    /// Retiring the in-flight record is not merely tidy — it withdraws the
    /// completion's authorization to publish into `cold_index`, which is
    /// correct here: the key is hot again, and publishing would leave a
    /// stale cold shadow behind it (#459).
    ///
    /// Every read path that can reach a cold key must call this first, or it
    /// will answer nil for a key that was only ever mid-spill.
    pub fn promote_inflight_if_present(&mut self, key: &[u8], now_ms: u64) -> bool {
        if self.spill_inflight_is_empty() {
            return false;
        }
        let Some(entry) = self.spill_inflight_entry(key, now_ms) else {
            return false;
        };
        self.spill_inflight_forget(key);
        self.set(key, entry);
        true
    }

    /// Apply an already-computed [`cold_read::ColdReadOutcome`] to hot RAM +
    /// the cold index, without performing any disk I/O itself.
    ///
    /// Factored out of [`Self::promote_cold_if_present`] (task #59) so a
    /// caller that can `.await` an off-shard-thread disk read (see
    /// `storage::tiered::cold_read_pool::read_cold_entry_async`) can do the
    /// slow part outside any lock/borrow of `self`, then come back and apply
    /// the *real* outcome here — never a timed-out placeholder. Same
    /// return-value contract as `promote_cold_if_present`: `true` iff `key`
    /// is present in hot RAM after this call.
    ///
    /// TOCTOU fix (task #59 review round 2): `outcome` was read from disk at
    /// `expected_location`, possibly across an `.await` on the caller's
    /// side. Anything can happen to `key` while that await was suspended —
    /// most importantly `DEL`/`FLUSHDB`/`UNLINK` on the SAME shard thread
    /// (single-threaded event loop; no lock protects this window). If we
    /// blindly promoted `outcome` here, a deleted key would come back to
    /// life: `self.data.contains_key(key)` is false (DEL removed the hot
    /// copy, and there never was one for a cold-only key), so the guard
    /// above doesn't catch it, and `self.set(...)` would resurrect the
    /// key permanently. So: before applying a `Hit`/`Expired` outcome,
    /// re-read the CURRENT cold-index location for `key` and only proceed
    /// if it's still exactly `expected_location`. If the entry is gone
    /// (DEL/FLUSHDB) or now points somewhere else (SET-then-re-evict during
    /// the await), discard `outcome` entirely and let the caller's normal
    /// dispatch answer from current state instead. This re-check and the
    /// mutation that follows it both run synchronously with no `.await`
    /// between them, so they're atomic from every other task's perspective
    /// on this single-threaded shard — the race window closes here.
    pub fn promote_cold_outcome(
        &mut self,
        key: &[u8],
        _now_ms: u64,
        expected_location: crate::storage::tiered::cold_index::ColdLocation,
        outcome: crate::storage::tiered::cold_read::ColdReadOutcome,
    ) -> bool {
        use crate::storage::tiered::cold_read::ColdReadOutcome;
        if self.data.contains_key(key) {
            return true;
        }
        if matches!(outcome, ColdReadOutcome::Hit(..) | ColdReadOutcome::Expired) {
            let still_valid = self
                .cold_index
                .as_ref()
                .and_then(|ci| ci.lookup(key))
                .is_some_and(|current| current == expected_location);
            if !still_valid {
                // The cold entry this outcome was read from is gone (DEL/
                // FLUSHDB/UNLINK/expiry-sweep) or has been replaced (a fresh
                // SET-then-re-evict landed a NEW cold entry for this key)
                // since we started reading it. Applying `outcome` now would
                // either resurrect a deleted key or clobber a newer cold
                // entry with stale bytes. Discard it and report "not
                // promoted" — the caller's normal dispatch path re-reads
                // current state and answers correctly (nil for a deleted
                // key, the new value for a re-spilled one).
                return false;
            }
        }
        match outcome {
            ColdReadOutcome::Hit(redis_value, ttl_ms) => {
                // Build an entry from the RedisValue (works for strings and collections).
                let mut entry = Entry::new_string(Bytes::new()); // placeholder
                entry.value =
                    crate::storage::compact_value::CompactValue::from_redis_value(redis_value);
                if let Some(ttl) = ttl_ms {
                    entry.set_expires_at_ms(ttl);
                }
                self.set(key, entry);
                if let Some(ref mut ci) = self.cold_index {
                    ci.remove(key);
                }
                true
            }
            ColdReadOutcome::Expired => {
                // Expired on disk: reclaim the index entry now, or it leaks
                // (the orphan sweep only checks hot-shadowing, never TTL).
                // Safe to remove unconditionally here -- the revalidation
                // above already confirmed the index still points at
                // `expected_location`, so this can't be clobbering a newer
                // entry.
                if let Some(ref mut ci) = self.cold_index {
                    ci.remove(key);
                }
                false
            }
            ColdReadOutcome::Miss => false,
        }
    }

    /// Cheap (no disk I/O, no promotion) check for whether `key` is present
    /// in the cold tier and not yet TTL-expired.
    ///
    /// Used by `EXISTS`: a spilled key logically exists even though it has
    /// no in-RAM `Entry`, but `EXISTS` doesn't need the *value* — paying for
    /// a disk read (or, worse, promoting the key into RAM) just to answer a
    /// boolean would be wasteful. A single `HashMap` probe against the
    /// in-RAM `ColdIndex` (which caches `ttl_ms` at insert time, see
    /// [`crate::storage::tiered::cold_index::ColdLocation::ttl_ms`]) is
    /// enough. Does not reclaim an expired entry (that needs `&mut self`
    /// and disk access to do safely) — the proactive sweep and the
    /// promoting paths handle reclamation.
    #[inline]
    pub(super) fn cold_contains_alive(&self, key: &[u8], now_ms: u64) -> bool {
        // A key whose spill is still in flight is in neither hot nor cold,
        // but it exists — the client's write was acked and nothing deleted
        // it. Answering `false` here is what made EXISTS deny live keys and
        // DEL answer :0 inside the window (#459).
        if self.spill_inflight_alive(key, now_ms) {
            return true;
        }
        let Some(ci) = self.cold_index.as_ref() else {
            return false;
        };
        match ci.lookup(key) {
            Some(loc) => loc.ttl_ms.is_none_or(|ttl| now_ms <= ttl),
            None => false,
        }
    }

    /// Non-promoting cold read-through for the `&self` "*_ref_if_alive"
    /// accessors: decodes a spilled value from disk WITHOUT touching hot RAM
    /// or the cold index. Thin wrapper over [`Self::get_cold_value`] using
    /// this `Database`'s own cached clock semantics is left to the caller
    /// (they already have `now_ms` in hand); kept as a private alias so the
    /// call sites below read as "cold read" rather than repeating the
    /// `cold_shard_dir`/`cold_index` plumbing.
    ///
    /// These accessors back BOTH the exclusive-dispatch path (`&mut
    /// Database`, which downgrades to `&self` for the call) AND the
    /// RwLock-shared-read dispatch path (`&Database` only — see
    /// `dispatch_read` / `*_readonly` command handlers). The latter cannot
    /// mutate `self` to promote a cold hit into hot RAM, so the safe fix is
    /// "decode it from disk every time it's cold" rather than "silently
    /// report the key absent" (same P0 as [`Self::promote_cold_if_present`]).
    ///
    /// [`Self::get_cold_value`] consults the in-flight plane before the cold
    /// one (#459), so these accessors inherit it and see a key mid-spill.
    #[inline]
    pub(super) fn cold_read_only(&self, key: &[u8], now_ms: u64) -> Option<RedisValue> {
        self.get_cold_value(key, now_ms)
    }

    /// Get a mutable reference to an entry by key, performing lazy expiration and access tracking.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: immutable check for expiry (rare path), then single get_mut
    /// for LRU touch + return. Reduces from 3 lookups to 2 for non-expired keys.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        // Mutable access is a write intent; see `record_keyspace_change`
        // for why counting the intent (rather than the mutation) is the
        // safe direction for `rdb_changes_since_last_save`.
        crate::admin::metrics_setup::record_keyspace_change();
        let now = self.cached_now;
        let now_ms = self.cached_now_ms;
        // Immutable check for expiry (avoids get_mut + remove + get_mut triple lookup)
        let expired = self.data.get(key).is_some_and(|e| e.is_expired_at(now_ms));
        if expired {
            // moon#542: hide + defer to the emitting drain (see `get`).
            self.note_lazy_expired(key);
            return None;
        }
        // Single get_mut: touch LRU + return
        let entry = self.data.get_mut(key)?;
        entry.set_last_access(now);
        Some(entry)
    }

    /// Insert or replace an entry, tracking memory and version.
    ///
    /// Uses `DashTable::insert_or_update` for a single SIMD probe on both hit
    /// and miss paths. The old `get_mut` + `insert` pattern ran two probes on
    /// miss (PERF-08).
    ///
    /// `key` is BORROWED on purpose. Every use below is by reference —
    /// `spill_inflight_forget`, `entry_overhead`, `hash_expiry_index_note_value`,
    /// `CompactKey::from` (which copies the bytes either way), `ColdIndex::remove`
    /// and both expiry-index writers all take `&[u8]`, and the `Bytes` is never
    /// moved anywhere. Taking `Bytes` forced a `key.clone()` at every write
    /// command's call site: one `shared_v_clone` on the way in and one
    /// `shared_v_drop` on the way out, per command, producing nothing.
    pub fn set(&mut self, key: &[u8], entry: Entry) {
        crate::admin::metrics_setup::record_keyspace_change();
        // An overwrite makes any in-flight spill payload for this key stale.
        // Retiring the record here stops its completion publishing the OLD
        // value into `cold_index`, where it would sit as a shadow behind the
        // new hot value and become authoritative again after a restart
        // demoted the hot copy (#459). One `is_empty()` load on the write
        // hot path when nothing is spilling, which is the normal case.
        if !self.spill_inflight_is_empty() {
            self.spill_inflight_forget(key);
        }
        let new_cost = entry_overhead(key, &entry);
        let has_expiry = entry.has_expiry();
        let new_ttl = entry.expires_at_ms();
        // moon#543: a HashWithTtl value arriving whole (RESTORE, cold
        // promotion, replication apply) must be indexed for the hash-field
        // sweep — one discriminant match on the write path, no allocation
        // for every other value kind.
        self.hash_expiry_index_note_value(key, &entry);
        let mut old_cost: usize = 0;
        let mut old_ttl: u64 = 0;

        // Cell enables interior mutability through shared references, allowing
        // both closures to capture &entry_cell without conflicting &mut borrows.
        // Exactly one closure runs (FnOnce), so the take() is safe.
        let entry_cell = std::cell::Cell::new(Some(entry));

        // Drawn before the closures because both borrow `self.data`. Wasted on
        // the hit path, which is deliberate: the counter is a ticket dispenser,
        // not a count, and a gap costs nothing.
        let birth = self.next_birth_version();

        // `insert_or_update` invariant: exactly one of the two closures fires
        // exactly once per call, so the `Cell::take()` below cannot observe
        // a None value. Annotated for the hot-path unwrap ratchet.
        #[allow(clippy::expect_used)]
        let result = self.data.insert_or_update(
            CompactKey::from(key), // CompactKey copies the bytes either way
            |existing: &mut Entry| {
                // Hit path: replace existing entry, bump version.
                let new_entry = entry_cell.take().expect("update closure called once");
                old_cost = entry_overhead(key, existing);
                old_ttl = existing.expires_at_ms();
                let new_version = Entry::bump_version(existing.version());
                *existing = new_entry;
                existing.set_version(new_version);
            },
            || {
                // Miss path: stamp the creation ticket. Constructors all start
                // at INITIAL_VERSION, which made every incarnation of a key
                // indistinguishable from its first — the WATCH ABA hole.
                let mut new_entry = entry_cell.take().expect("make closure called once on miss");
                new_entry.set_version(birth);
                new_entry
            },
        );

        match result {
            InsertOrUpdate::Updated(_) => {
                self.used_memory = self.used_memory.saturating_sub(old_cost) + new_cost;
                // Task #56 finding 1 (adversarial review, stale-data
                // resurrection): a SECOND-OR-LATER write to a key that ALSO
                // carries a cold-tier shadow proves that shadow is stale.
                //
                // Crash-consistency proof: AOF replay reconstructs a key's
                // entire write history against an EMPTY DashTable (`db.clear()`
                // runs in `main.rs` right before every replay branch), so the
                // first replayed write to a given key is always `Inserted`
                // (ambiguous -- it may be exactly the write whose later
                // eviction produced this cold entry, so it's left alone) and
                // every SUBSEQUENT write to the SAME key during that same
                // replay is `Updated` -- which can only happen if the AOF
                // recorded a write to this key AFTER the one that got
                // spilled, i.e. the cold copy is provably stale relative to
                // the value now hot. Clearing the shadow here, synchronously,
                // the moment a second touch is observed (live traffic or
                // replay -- same code path, same proof) closes the gap:
                // without this, a crash between a live overwrite of a
                // cold-shadowed key and the next eviction/orphan-sweep left
                // the on-disk manifest's cold entry pointing at the OLD
                // value; `demote_replayed_cold_shadows` (`src/main.rs`,
                // running after replay finishes) would then treat that
                // untouched-looking cold entry as "provably redundant" and
                // use it to discard the newer hot value on restart --
                // silently resurrecting stale data. See
                // `tests/cold_shadow_overwrite_resurrection.rs` for the
                // end-to-end regression guard and
                // `storage::db::tests::test_second_write_invalidates_cold_shadow`
                // for the unit-level proof.
                if let Some(ci) = self.cold_index.as_mut() {
                    ci.remove(key);
                }
            }
            InsertOrUpdate::Inserted(_) => {
                self.used_memory += new_cost;
            }
        }
        if has_expiry {
            self.maybe_has_expiring_keys = true;
        }
        // moon#541: keep the deadline index in lock-step across all four
        // TTL transitions (none→ttl, ttl→none, ttl→ttl', unchanged). The
        // `Inserted` arm leaves `old_ttl` at 0, so this covers both paths.
        if old_ttl != new_ttl {
            if old_ttl != 0 {
                self.expiry_index_remove(old_ttl, key);
            }
            if new_ttl != 0 {
                self.expiry_index_insert(new_ttl, key);
            }
        }
    }

    /// Clear all entries and reset memory accounting.
    ///
    /// Used during multi-part AOF recovery to wipe any state populated by
    /// earlier recovery phases (per-shard WAL replay, legacy appendonly.aof)
    /// before loading the authoritative base RDB + incr log. Without this,
    /// non-idempotent commands from pre-existing state would be double-applied.
    pub fn clear(&mut self) {
        crate::admin::metrics_setup::record_keyspace_change();
        self.data = DashTable::new();
        self.used_memory = 0;
        self.maybe_has_expiring_keys = false;
        self.expiry_index.clear();
        self.hash_expiry_index.clear();
        self.hot_keys.clear();
        // D1: FLUSH must clear the cold tier too, or flushed keys stay
        // readable via cold read-through. Files are queued for unlink and
        // reclaimed by the orphan sweep (which holds the manifest handle).
        if let Some(ci) = self.cold_index.as_mut() {
            ci.clear_all();
        }
        // FLUSH is a bulk DEL, and retiring the in-flight record is what
        // makes a DEL FINAL (#459): the record is the completion's
        // authorization to insert into `cold_index`, so leaving it here let a
        // spill that landed after the flush resurrect a flushed key — and
        // kept it in `DBSIZE`, `KEYS` and `GET` in the meantime, since all
        // three consult the in-flight plane. The orphan sweep reclaims the
        // now-unreferenced spill file, exactly as it does for a DEL.
        //
        // Also drops the moon#466 pending-byte charge, which `used_memory = 0`
        // above would otherwise contradict.
        self.spill_inflight.clear();
        self.spill_inflight_bytes = 0;
    }

    /// The hot-key sketch (sampling hooks, HOTKEYS command, coordinator
    /// merge). Interior mutability — `&self` suffices for observe/top/clear.
    #[inline]
    pub fn hot_keys(&self) -> &crate::storage::hotkey::HotKeySketch {
        &self.hot_keys
    }

    /// Bulk-load insert: skip duplicate check and per-key memory accounting.
    ///
    /// Used exclusively during RDB/AOF restore where keys are guaranteed unique and
    /// we recalculate `used_memory` once after the entire load completes.
    ///
    /// Still draws a creation ticket, unlike the rest of the fast path it skips.
    /// Versions are not persisted, so every restored entry would otherwise carry
    /// `INITIAL_VERSION`, and the FIRST key created after a restore would draw
    /// ticket 1 and collide with them — reopening the ABA hole for exactly the
    /// window right after a restart. One u32 increment per restored key against
    /// a decode is not measurable.
    #[inline]
    pub fn insert_for_load(&mut self, key: Bytes, mut entry: Entry) {
        if entry.has_expiry() {
            self.maybe_has_expiring_keys = true;
            self.expiry_index_insert(entry.expires_at_ms(), &key);
        }
        self.hash_expiry_index_note_value(&key, &entry);
        entry.set_version(self.next_birth_version());
        self.data.insert(CompactKey::from(key), entry);
    }

    /// Recalculate `used_memory` by scanning all entries. Call once after bulk load.
    pub fn recalculate_memory(&mut self) {
        let mut total = 0usize;
        let mut any_expiring = false;
        // moon#541: this post-bulk-load pass is also the index's healer —
        // rebuild it from scratch so any load path that bypassed
        // `insert_for_load` still ends consistent.
        let mut index = std::collections::BTreeSet::new();
        let mut hash_index = std::collections::BTreeSet::new();
        for (key, entry) in self.data.iter() {
            total += entry_overhead(key.as_bytes(), entry);
            if entry.has_expiry() {
                any_expiring = true;
                index.insert((entry.expires_at_ms(), key.clone()));
            }
            // moon#543: the same healing property for the hash-field index —
            // a load path that bypassed `insert_for_load` still ends indexed.
            if let crate::storage::compact_value::RedisValueRef::HashWithTtl { ttls, .. } =
                entry.value.as_redis_value()
                && let Some(min) = ttls.values().copied().min()
            {
                hash_index.insert((min, key.clone()));
            }
        }
        self.used_memory = total;
        self.maybe_has_expiring_keys = any_expiring;
        self.expiry_index = index;
        // Authoritative, like the whole-key index rebuild above.
        self.hash_expiry_index = hash_index;
    }

    /// Pre-size the internal hash table for an expected key count.
    ///
    /// WARNING: This REPLACES the internal hash table with a fresh one sized for
    /// `additional` entries. It MUST be called on an empty `Database` (typically
    /// immediately after `Database::new()` during RDB/AOF bulk load). Calling it
    /// on a populated database silently discards all entries.
    ///
    /// Named `reserve` rather than `reset_with_capacity` to match the plan
    /// nomenclature, but the debug assertion guards the misuse case.
    pub fn reserve(&mut self, additional: usize) {
        debug_assert!(
            self.data.is_empty(),
            "Database::reserve() must only be called on an empty database (bulk-load pre-sizing); called with {} existing entries",
            self.data.len()
        );
        if additional > self.data.len() {
            let new_table = DashTable::with_capacity(additional);
            self.data = new_table;
            self.maybe_has_expiring_keys = false;
            // moon#541: the replaced table's index/latch state goes with it —
            // matching `clear` (no-ops on the documented empty-db call, but
            // the misuse case must not leave stale pairs behind).
            self.expiry_index.clear();
            self.hash_expiry_index.clear();
        }
    }

    /// Remove a key and return its entry. No expiry check needed (DEL removes regardless).
    ///
    /// Also removes any COLD (spilled) copy of the key — without this, a DEL
    /// of an evicted key is a no-op and the next GET resurrects the value via
    /// cold read-through (D1, tmp/OFFLOAD-COMPRESSION-REVIEW.md). A cold-only
    /// key returns `None` (no in-RAM entry exists); callers that must COUNT
    /// cold-only removals (DEL/UNLINK) use [`Self::remove_counting_cold`].
    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        crate::admin::metrics_setup::record_keyspace_change();
        let _ = self.remove_cold_only(key);
        self.remove_hot(key)
    }

    /// Remove hot + cold copies; returns `true` when EITHER existed, so
    /// DEL/UNLINK count spilled keys as removed (Redis semantics: the key
    /// logically exists). A cold entry that is already TTL-expired (judged
    /// from the cached `ColdLocation::ttl_ms`, no disk read) is reclaimed
    /// but NOT counted — DEL of a logically-expired key answers 0. The
    /// removed hot entry, when present, is also returned so UNLINK can
    /// size its async-drop decision.
    pub fn remove_counting_cold(&mut self, key: &[u8]) -> (bool, Option<Entry>) {
        crate::admin::metrics_setup::record_keyspace_change();
        let now_ms = self.cached_now_ms;
        let cold_alive = self
            .cold_index
            .as_ref()
            .and_then(|ci| ci.lookup(key))
            .is_some_and(|loc| loc.ttl_ms.is_none_or(|ttl| now_ms <= ttl));
        // An in-flight key counts as removed for the same reason it counts as
        // existing: the write was acked and nothing has deleted it yet.
        let inflight_alive = self.spill_inflight_alive(key, now_ms);
        let had_cold = self.remove_cold_only(key);
        let hot = self.remove_hot(key);
        (
            hot.is_some() || (had_cold && cold_alive) || inflight_alive,
            hot,
        )
    }

    /// Drops the cold copy AND any in-flight spill record.
    ///
    /// Retiring the in-flight record is the load-bearing half (#459): it is
    /// the completion's authorization to insert into `cold_index`, so
    /// without this a DEL issued during the spill window was undone when the
    /// spill landed — and committed to the manifest, so it survived restart.
    #[inline]
    fn remove_cold_only(&mut self, key: &[u8]) -> bool {
        self.spill_inflight_forget(key);
        self.cold_index.as_mut().is_some_and(|ci| ci.remove(key))
    }

    #[inline]
    pub(super) fn remove_hot(&mut self, key: &[u8]) -> Option<Entry> {
        if let Some(entry) = self.data.remove(key) {
            self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            // moon#541: unindex — the removed entry knows its own pair.
            if entry.has_expiry() {
                self.expiry_index_remove(entry.expires_at_ms(), key);
            }
            // moon#543: same for the hash-field index (best-effort; one
            // `is_empty` load when no field TTL exists anywhere).
            self.hash_expiry_index_forget(key, &entry);
            Some(entry)
        } else {
            None
        }
    }

    /// Task #56 (used_memory truthful under disk-offload + AOF restart):
    /// drop hot DashTable copies that are redundant with an already-cold key.
    ///
    /// AOF replay has no knowledge of the disk-offload cold tier -- a key
    /// evicted-and-spilled to cold storage BEFORE a crash gets no
    /// corresponding DEL record in the AOF (a spilled entry stays
    /// cold-readable, not deleted, so `record_reason_del` never fires for
    /// it -- see its doc comment). Replaying the AOF's full command history
    /// therefore unconditionally re-applies that key's original SET and
    /// lands it back in hot RAM, inflating `used_memory` well past
    /// `--maxmemory` immediately after every restart until later eviction
    /// ticks claw it back down (task #56 diagnosis; observed ~6x the
    /// steady-state ledger in a 40k-key repro, `tests/used_memory_offload_truthful.rs`).
    ///
    /// Call this ONCE per shard, after AOF replay finishes and before the
    /// server starts accepting connections. Any key present in
    /// `cold_index` at that point was cold-and-untouched as of the crash:
    /// the index was rebuilt from the crash-consistent manifest *before*
    /// AOF replay ran, and replay only ever writes hot -- it cannot
    /// re-cold a key. If AOF replay *also* landed that key in the hot
    /// DashTable, both copies hold the identical value. Proof sketch: the
    /// AOF's last command touching a crash-time-cold key must be the same
    /// SET the eviction path later spilled -- any *later* live write would
    /// have made the key hot again and required a fresh eviction to become
    /// cold once more, which the manifest (and thus `cold_index`) would
    /// already reflect as its current entry. It is therefore always safe
    /// to drop the redundant hot copy and let the existing cold-index
    /// entry keep serving reads -- no cold-tier file is touched, only the
    /// in-memory accounting is corrected.
    ///
    /// This is the mirror image of the live-traffic orphan sweeper
    /// ([`crate::storage::tiered::cold_index::ColdIndex::orphan_sweep`]):
    /// that one deletes a *stale cold* copy shadowed by a *newer hot*
    /// write seen during live traffic (hot wins). This one deletes a
    /// *redundant hot* copy manufactured by replaying an *old* write whose
    /// crash-time fate was already cold (cold wins) -- the two precedences
    /// only coexist because this method runs strictly before the server is
    /// reachable; never call it once live traffic may have run.
    pub fn demote_replayed_cold_shadows(&mut self) -> usize {
        let Some(ci) = self.cold_index.as_ref() else {
            return 0;
        };
        let shadow_keys: Vec<Bytes> = ci
            .iter()
            .filter(|(key, _)| self.is_hot(key))
            .map(|(key, _)| key.clone())
            .collect();
        let mut demoted = 0usize;
        for key in &shadow_keys {
            if self.remove_hot(key).is_some() {
                demoted += 1;
            }
        }
        demoted
    }

    /// Check if a key exists, performing lazy expiration.
    /// Optimized: single lookup via get instead of check_expired + contains_key.
    ///
    /// A cold-only key (spilled by eviction, no in-RAM `Entry`) still counts
    /// as existing — checked via the cheap in-RAM [`Self::cold_contains_alive`]
    /// (no disk I/O, no promotion; P0 cold-collection-visibility fix).
    pub fn exists(&mut self, key: &[u8]) -> bool {
        let now_ms = self.cached_now_ms;
        match self.data.get(key) {
            None => self.cold_contains_alive(key, now_ms),
            Some(entry) => {
                if entry.is_expired_at(now_ms) {
                    // moon#542: hide + defer to the emitting drain (see
                    // `get`). Cold fallback preserved: an expired hot entry
                    // never shadows a still-alive cold copy.
                    self.note_lazy_expired(key);
                    self.cold_contains_alive(key, now_ms)
                } else {
                    true
                }
            }
        }
    }

    /// Number of entries (including potentially expired ones).
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Number of LOGICAL keys: hot entries plus disk-offloaded (cold) keys,
    /// counting a key that exists in both planes exactly once (issue #355 —
    /// a spilled-but-readable key is still a key; DBSIZE / INFO `# Keyspace`
    /// previously reported the resident set only, an ~86% under-report in
    /// the 2026-07-16 G2 re-run).
    ///
    /// Hot∩cold overlap is transient but real: a fresh SET over a cold-only
    /// key lands on `set()`'s `Inserted` arm, which deliberately leaves the
    /// cold shadow (removing it there would defeat restart-as-cold — every
    /// first replayed write is `Inserted` — see the ambiguity proof in
    /// [`Self::set`]). The overlap is therefore subtracted with an O(cold)
    /// probe pass instead of a maintained counter: `cold_index` is a `pub`
    /// field mutated directly by the spill-completion path, so an
    /// incremental counter would have unfenceable drift risk, while INFO
    /// already tolerates an O(hot) scan per call in
    /// [`Self::expires_count`]. Like `len()`, potentially-expired entries
    /// (hot or cold) are counted.
    pub fn logical_len(&self) -> usize {
        let hot = self.data.len();
        let hot_and_cold = match &self.cold_index {
            None => hot,
            Some(ci) => {
                let overlap = ci
                    .iter()
                    .filter(|(key, _)| self.data.contains_key(key))
                    .count();
                hot + ci.len() - overlap
            }
        };
        if self.spill_inflight_is_empty() {
            return hot_and_cold;
        }
        // Keys mid-spill are in neither plane above but are logically
        // present (#459): counting only hot+cold made DBSIZE answer 124 for
        // 400 acked keys and then climb to 400 on its own. Same
        // count-each-key-once discipline as the cold overlap — a key can be
        // in flight while a fresh SET has already re-created it hot, or
        // while a superseded cold entry still exists.
        let inflight_only = self
            .spill_inflight_keys()
            .filter(|k| {
                !self.data.contains_key(k)
                    && !self
                        .cold_index
                        .as_ref()
                        .is_some_and(|ci| ci.lookup(k).is_some())
            })
            .count();
        hot_and_cold + inflight_only
    }

    /// Iterator over all keys (caller does glob filtering).
    ///
    /// Hot plane only: under disk-offload, spilled keys have no in-RAM
    /// `Entry` and are NOT yielded here — keyspace enumerators (SCAN /
    /// KEYS / RANDOMKEY) must union this with [`Self::cold_only_keys`]
    /// or spilled keys silently vanish from enumeration (#364).
    pub fn keys(&self) -> impl Iterator<Item = &CompactKey> {
        self.data.keys()
    }

    /// Live (non-expired at `now_ms`) hot-plane keys, judged from the entry
    /// during iteration — no per-key hash lookup, no reclamation side
    /// effect. SCAN's per-page walk (#368) uses this instead of
    /// `keys()` + `get_if_alive()` per key, which paid a second full-table
    /// lookup pass.
    pub fn iter_live_keys(&self, now_ms: u64) -> impl Iterator<Item = &CompactKey> + '_ {
        self.data.iter().filter_map(move |(k, e)| {
            if e.is_expired_at(now_ms) {
                None
            } else {
                Some(k)
            }
        })
    }

    /// O(COUNT) hot-plane SCAN page (#368): entries with
    /// `scan hash48 >= from_h48`, live at `now_ms`, ascending by
    /// `(hash48, key)`, walking only the DashTable segments that cover the
    /// requested hash range (see [`crate::storage::dashtable::DashTable::hash_page`]).
    ///
    /// Hash mapping: the cursor is the table's own key hash truncated to
    /// its top 48 bits (`hash_key(key) >> 16`), so
    /// `h64 >= from_h48 << 16  ⟺  (h64 >> 16) >= from_h48` — the segment
    /// range walk and the 48-bit cursor agree exactly.
    ///
    /// Returns `(page, more)`; `more = true` means unvisited segments
    /// (strictly larger hashes) remain.
    pub fn scan_hot_page(
        &self,
        from_h48: u64,
        want: usize,
        now_ms: u64,
    ) -> (Vec<(u64, CompactKey)>, bool) {
        // The h64→h48 bridge (and the "equal-h48 group never straddles a
        // page" guarantee) requires segment routing to use at most the top
        // 48 hash bits. Depth > 48 needs a 2^48-entry directory —
        // unreachable in practice, but the invariant is load-bearing.
        debug_assert!(
            self.data.directory_depth() <= 48,
            "SCAN 48-bit cursor mapping requires directory depth <= 48"
        );
        let (page, more) = self
            .data
            .hash_page(from_h48 << 16, want, move |_, e| !e.is_expired_at(now_ms));
        let mut page: Vec<(u64, CompactKey)> =
            page.into_iter().map(|(h, k)| (h >> 16, k)).collect();
        // hash_page sorts by full (h64, key); truncation to 48 bits can
        // reorder keys WITHIN an equal-h48 group, so re-establish
        // (h48, key) order (groups never span pages: equal h48 ⇒ same
        // segment, and pages are whole-segment granular).
        page.sort_unstable_by(|a, b| (a.0, a.1.as_bytes()).cmp(&(b.0, b.1.as_bytes())));
        (page, more)
    }

    /// Keys visible ONLY via the cold plane at `now_ms`: present in the
    /// in-RAM cold index, not TTL-expired (judged from the cached
    /// [`crate::storage::tiered::cold_index::ColdLocation::ttl_ms`] — no
    /// disk I/O), and NOT shadowed by a live hot-plane entry.
    ///
    /// Together with the hot-alive subset of [`Self::keys`] this
    /// partitions the logical keyspace with no overlap, so keyspace
    /// enumerators (SCAN / KEYS / RANDOMKEY, #364) can take the union of
    /// the two planes without any dedup pass. A key present in BOTH
    /// planes (hot shadow over a stale cold entry, e.g. after AOF replay)
    /// is classified hot; a hot entry that is TTL-expired above a live
    /// cold entry is classified cold.
    pub fn cold_only_keys(&self, now_ms: u64) -> impl Iterator<Item = &Bytes> + '_ {
        let cold = self.cold_index.as_ref().into_iter().flat_map(move |ci| {
            ci.iter()
                .filter(move |(key, loc)| self.is_cold_only_alive(key, loc, now_ms))
                .map(|(key, _)| key)
        });
        // Keys mid-spill are in neither plane but exist (#459) — KEYS and
        // RANDOMKEY promise a point-in-time view of the keyspace, so
        // omitting them would hide a live key for the length of the window.
        // Same partition discipline as the cold half: skip anything a hot
        // entry or a cold entry already accounts for.
        let inflight = self
            .spill_inflight
            .iter()
            .filter(move |(key, p)| {
                p.ttl_ms.is_none_or(|ttl| now_ms <= ttl)
                    && !self.data.contains_key(key)
                    && !self
                        .cold_index
                        .as_ref()
                        .is_some_and(|ci| ci.lookup(key).is_some())
            })
            .map(|(key, _)| key);
        cold.chain(inflight)
    }

    /// The cold-only liveness predicate shared by [`Self::cold_only_keys`]
    /// and [`Self::cold_only_keys_from`]: cold entry not TTL-expired AND not
    /// shadowed by a live hot entry. Keeping it in one place keeps the
    /// two-plane partition invariant (#364) from diverging between the
    /// full-walk and range-resume paths.
    #[inline]
    fn is_cold_only_alive(
        &self,
        key: &Bytes,
        loc: &crate::storage::tiered::cold_index::ColdLocation,
        now_ms: u64,
    ) -> bool {
        let cold_alive = loc.ttl_ms.is_none_or(|ttl| now_ms <= ttl);
        if !cold_alive {
            return false;
        }
        !self
            .data
            .get(key.as_ref())
            .is_some_and(|e| !e.is_expired_at(now_ms))
    }

    /// Hash-ordered cold-only keys from `from_h48` in the SCAN cursor's
    /// 48-bit hash space (#368): O(log n) seek into the ordered cold index,
    /// then ascending `(hash48, key)` candidates filtered by
    /// [`Self::is_cold_only_alive`]. SCAN takes the first COUNT — since the
    /// order is ascending, those are exactly the smallest cold candidates —
    /// instead of filtering the entire index on every page.
    ///
    /// KNOWN GAP (#459): unlike [`Self::cold_only_keys`], this does NOT
    /// include keys whose spill is still in flight. The in-flight map is
    /// unordered, so merging it into an ascending `hash48` walk needs a
    /// merge-sort against a plane that mutates under the cursor — real work,
    /// deliberately not done here. A key can therefore be skipped by SCAN
    /// for the milliseconds its spill is queued. That stays within SCAN's
    /// contract (only keys present for the WHOLE iteration are guaranteed),
    /// and `KEYS`/`RANDOMKEY`, whose contracts are point-in-time, do include
    /// them.
    pub fn cold_only_keys_from(
        &self,
        from_h48: u64,
        now_ms: u64,
    ) -> impl Iterator<Item = (u64, &Bytes)> + '_ {
        self.cold_index.as_ref().into_iter().flat_map(move |ci| {
            ci.range_from(from_h48)
                .filter(move |(_, key, loc)| self.is_cold_only_alive(key, loc, now_ms))
                .map(|(h, key, _)| (h, key))
        })
    }

    /// Return a random non-expired key from the database, or None if empty.
    ///
    /// Samples the LOGICAL keyspace: hot-alive entries plus cold-only
    /// spilled keys ([`Self::cold_only_keys`]) — an all-spilled database
    /// must not answer "empty" (#364).
    ///
    /// Two passes — count, then walk to the selected position — so only
    /// the winning key is ever cloned (the previous single-pass version
    /// materialized a `Bytes` copy of EVERY live key per call). Stable
    /// across the passes: `&self` is held throughout and each shard's
    /// database is single-threaded, so neither plane can mutate between
    /// the count and the walk.
    ///
    /// The position comes from the thread RNG, NOT the clock. It used to be
    /// `current_time_ms() % total`, which made every call inside the same
    /// millisecond return the SAME key — a client polling RANDOMKEY in a loop
    /// (the normal way to sample a keyspace) got one name repeated for as long
    /// as the loop ran, and only ~1 distinct key per millisecond of wall time
    /// however many times it asked (moon#629).
    pub fn random_key(&self) -> Option<Bytes> {
        use rand::RngExt;
        let now_ms = self.cached_now_ms;
        let hot_live = self
            .data
            .iter()
            .filter(|(_, e)| !e.is_expired_at(now_ms))
            .count();
        let cold_live = self.cold_only_keys(now_ms).count();
        let total = hot_live + cold_live;
        if total == 0 {
            return None;
        }
        let idx = rand::rng().random_range(0..total);
        if idx < hot_live {
            self.data
                .iter()
                .filter(|(_, e)| !e.is_expired_at(now_ms))
                .nth(idx)
                .map(|(k, _)| Bytes::copy_from_slice(k.as_ref()))
        } else {
            self.cold_only_keys(now_ms).nth(idx - hot_live).cloned()
        }
    }

    /// Set or remove expiration on an existing key.
    ///
    /// Performs lazy expiry check first. Returns `false` if the key does not
    /// exist (or has already expired). Pass 0 to remove expiry.
    pub fn set_expiry(&mut self, key: &[u8], expires_at_ms: u64) -> bool {
        crate::admin::metrics_setup::record_keyspace_change();
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            // moon#541 rides moon#542: HIDE the expired key and queue it for
            // the emitting drain instead of removing it silently here — a
            // physical removal on this path emitted neither the `expired`
            // notification nor the dual-plane DEL, recreating for EXPIRE
            // exactly the divergence #542 closed for reads.
            self.note_lazy_expired(key);
            return false;
        }
        let old_ttl = match self.data.get_mut(key) {
            Some(entry) => {
                let old = entry.expires_at_ms();
                entry.set_expires_at_ms(expires_at_ms);
                old
            }
            None => return false,
        };
        // Latch the fast-path flag once a TTL is set. Persist (0) may
        // clear the per-entry bit but we don't flip the DB-level flag
        // down — the active-expiry scan's self-reset gate is the only
        // authoritative downgrade path (avoids racy decrement logic).
        if expires_at_ms != 0 {
            self.maybe_has_expiring_keys = true;
        }
        // moon#541: retarget the index pair.
        if old_ttl != expires_at_ms {
            if old_ttl != 0 {
                self.expiry_index_remove(old_ttl, key);
            }
            if expires_at_ms != 0 {
                self.expiry_index_insert(expires_at_ms, key);
            }
        }
        true
    }

    /// Count entries that have an expiration set. O(1) via the deadline
    /// index (moon#541) — previously an O(N) scan.
    pub fn expires_count(&self) -> usize {
        self.expiry_index_len()
    }

    /// Check if an entry is expired without requiring &mut self.
    pub(super) fn check_expired(
        data: &DashTable<CompactKey, Entry>,
        key: &[u8],
        now_ms: u64,
    ) -> bool {
        data.get(key).is_some_and(|e| e.is_expired_at(now_ms))
    }

    /// Convenience: set a string value with no expiry.
    pub fn set_string(&mut self, key: &[u8], value: Bytes) {
        self.set(key, Entry::new_string(value));
    }

    /// Convenience: set a string value with an expiry (unix millis).
    pub fn set_string_with_expiry(&mut self, key: &[u8], value: Bytes, expires_at_ms: u64) {
        self.set(key, Entry::new_string_with_expiry(value, expires_at_ms));
    }

    /// Check if a single entry is expired.
    #[allow(dead_code)]
    pub(super) fn is_expired(entry: &Entry) -> bool {
        entry.is_expired_at(current_time_ms())
    }

    /// WRONGTYPE error frame.
    pub(super) fn wrongtype_error() -> Frame {
        Frame::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        ))
    }

    /// Take the next creation ticket from the per-db birth counter.
    ///
    /// Stamped on every entry this database fabricates so a delete+recreate is
    /// observably a different incarnation rather than a fresh `INITIAL_VERSION`
    /// that a WATCHing client mistakes for its own recorded token. See
    /// `Database::birth_counter` for the wrap analysis.
    ///
    /// Consuming a ticket without using it is fine — gaps carry no meaning.
    #[inline]
    pub(crate) fn next_birth_version(&mut self) -> u32 {
        self.birth_counter = Entry::bump_version(self.birth_counter);
        self.birth_counter
    }

    /// Get the version of a key. Returns 0 if not found. No expiry check (WATCH needs raw version).
    pub fn get_version(&self, key: &[u8]) -> u32 {
        self.data.get(key).map(|e| e.version()).unwrap_or(0)
    }

    /// Increment version of a key if it exists.
    pub fn increment_version(&mut self, key: &[u8]) {
        if let Some(entry) = self.data.get_mut(key) {
            entry.increment_version();
        }
    }

    /// Touch access time of a key for LRU tracking (for reads).
    pub fn touch_access(&mut self, key: &[u8]) {
        let now = self.cached_now;
        if let Some(entry) = self.data.get_mut(key) {
            entry.set_last_access(now);
        }
    }
}
