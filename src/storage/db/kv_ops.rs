//! Core keyspace operations: get/set/remove, lazy expiry, expiry writes and versions (split from db/mod.rs; cold promotion, scans and bulk load live in `cold_promote.rs`, `keyspace_scan.rs` and `bulk_load.rs`).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::{DashTable, InsertOrUpdate};
use crate::storage::entry::{Entry, current_time_ms};

use crate::storage::db::{Database, entry_overhead};

/// Outcome of [`Database::remove_expired_at`] (moon#1189).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExpiredRemoval {
    /// The expired entry was removed.
    Removed,
    /// The popped pair no longer described the entry; nothing was removed.
    Stale,
    /// The pair is valid but not due yet; restore it.
    NotYetDue,
}

/// What deleting one key found (moon#1234): `Live` is counted and publishes
/// `del`; `Expired` — only a hot copy whose TTL had passed, reaped — is not
/// counted and publishes `expired`, as redis's `expireIfNeeded` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyDeletion {
    Absent,
    Live,
    Expired,
}

impl Database {
    /// Get an entry by key, performing lazy expiration.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: a single immutable probe classifies the key as live,
    /// expired, or absent. NLL forces one re-probe on the live path
    /// (the cold fallback below mutates `self`, so the first borrow cannot
    /// be returned directly): 2 probes on a live hit, 1 on a miss — down
    /// from 3/2 with the previous expiry-check + `is_some()` + get chain.
    ///
    /// Records the access for the eviction policy (moon#1161) — redis's
    /// `lookupKeyRead`. Metadata commands that redis serves with
    /// `LOOKUP_NOTOUCH` (`TTL`, `TYPE`, `OBJECT`, …) use [`Self::peek`].
    #[inline]
    pub fn get(&mut self, key: &[u8]) -> Option<&Entry> {
        self.lookup(key, true)
    }

    /// [`Self::get`] WITHOUT recording an access (redis `LOOKUP_NOTOUCH`):
    /// same lazy-expiry hiding and cold promotion, but `OBJECT IDLETIME` /
    /// `OBJECT FREQ` do not see this read. For metadata commands and for
    /// internal lookups that must not look like client traffic.
    #[inline]
    pub fn peek(&mut self, key: &[u8]) -> Option<&Entry> {
        self.lookup(key, false)
    }

    fn lookup(&mut self, key: &[u8], touch: bool) -> Option<&Entry> {
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
            KeyState::Live => {
                let entry = self.data.get(key);
                if touch && let Some(e) = entry {
                    e.note_access(crate::storage::eviction::access_tracking(), self.cached_now);
                }
                entry
            }
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

    /// Get a mutable reference to an entry by key, performing lazy expiration and access tracking.
    ///
    /// Returns `None` if the key does not exist or has expired.
    /// Optimized: immutable check for expiry (rare path), then single get_mut
    /// for LRU touch + return. Reduces from 3 lookups to 2 for non-expired keys.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut Entry> {
        // Mutable access is a write intent, counted as one change for
        // `rdb_changes_since_last_save` (the over-count is the safe
        // direction). In production only the active hash-field expiry sweep
        // reaches this (`reap_expired_fields_one_hash_at`), and it mutes the
        // count: expiry is housekeeping (moon#1232 review 5). A command would
        // count by its redis rule instead (`command::keyspace_changes`).
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
        // Single get_mut: touch LRU + return. moon#1161: under an LFU policy
        // a write is an access too (redis `lookupKeyWrite`), so the counter
        // is decayed + incremented before the unconditional stamp below.
        let tracking = crate::storage::eviction::access_tracking();
        let entry = self.data.get_mut(key)?;
        entry.note_access(tracking, now);
        entry.set_last_access(now);
        // moon#926: this hands out a raw `&mut Entry`, the broadest mutable
        // handle there is — stamp the WATCH version with it. A miss returns
        // above (the `?`) without stamping.
        crate::storage::db::stamp_mutation(entry);
        Some(entry)
    }

    /// Insert or replace an entry, tracking memory and version.
    ///
    /// Uses `DashTable::insert_or_update` for a single SIMD probe on both hit
    /// and miss paths. The old `get_mut` + `insert` pattern ran two probes on
    /// miss (PERF-08).
    ///
    /// The probe *count* reduction is structural and holds everywhere. The
    /// wall-clock win is not: measured on 1M keys, moon @ 7678156f, the fused
    /// miss path is ~11% faster on aarch64 (Neoverse-N1) and ~17% *slower* on
    /// x86_64 (Xeon 8481C) than `get_mut` + `insert`, despite issuing strictly
    /// fewer SIMD probes. Do not quote PERF-08 as an unqualified speed-up.
    /// See moon#789 and the module docs of
    /// `tests/perf_v0112_insert_or_update_single_probe.rs`.
    ///
    /// `key` is BORROWED on purpose. Every use below is by reference —
    /// `spill_inflight_forget`, `entry_overhead`, `hash_expiry_index_note_value`,
    /// the upsert (which builds a `CompactKey` only on a miss), `ColdIndex::remove`
    /// and both expiry-index writers all take `&[u8]`, and the `Bytes` is never
    /// moved anywhere. Taking `Bytes` forced a `key.clone()` at every write
    /// command's call site: one `shared_v_clone` on the way in and one
    /// `shared_v_drop` on the way out, per command, producing nothing.
    ///
    /// An overwrite is an ACCESS for the eviction policy (redis `setKey` =
    /// `lookupKeyWrite` + `dbOverwrite`). A command that already looked the
    /// key up with an access-recording read writes through
    /// [`Self::set_looked_up`] instead, so it is not counted twice.
    #[inline]
    pub fn set(&mut self, key: &[u8], entry: Entry) {
        self.set_recording::<true>(key, entry);
    }

    /// [`Self::set`] for a read-modify-write command that has ALREADY read
    /// the key with an access-recording lookup ([`Self::get`]) in this same
    /// command — redis's `lookupKeyWrite` + `dbOverwrite`: the lookup records
    /// the one access (refused commands included, as in redis), and the
    /// overwrite only CARRIES the LFU counter over. Writing through
    /// [`Self::set`] there recorded a second Morris increment per command
    /// (moon#1221 review F3). Everything else is `set`, byte for byte.
    pub fn set_looked_up(&mut self, key: &[u8], entry: Entry) {
        self.set_recording::<false>(key, entry);
    }

    /// The body of [`Self::set`] / [`Self::set_looked_up`]. `RECORD` is a
    /// const so the plain `set` monomorph is exactly the code it always was.
    fn set_recording<const RECORD: bool>(&mut self, key: &[u8], mut entry: Entry) {
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
        // moon#1163: a stream arriving whole is billed at its measured size.
        super::settle_stream_billing(&mut entry);
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
        // moon#1161: under an LFU policy an overwrite keeps (and bumps) the
        // key's frequency, as redis's `lookupKeyWrite` + `dbSetValue` do —
        // otherwise every write reset a hot key to `LFU_INIT_VAL`.
        let tracking = crate::storage::eviction::access_tracking();
        // `set_looked_up` (moon#1221 review F3): the caller's `get` was the
        // access; the overwrite below only carries the counter.
        let record = if RECORD {
            tracking
        } else {
            crate::storage::entry::AccessTracking::Off
        };
        let now_secs = self.cached_now;

        // `insert_or_update` invariant: exactly one of the two closures fires
        // exactly once per call, so the `Cell::take()` below cannot observe
        // a None value. Annotated for the hot-path unwrap ratchet.
        #[allow(clippy::expect_used)]
        // moon#1159: keyed by the borrowed slice — the owned `CompactKey` is
        // built only on a miss. `insert_or_update(CompactKey::from(key), ..)`
        // allocated (and dropped) a heap key block on every overwrite of a
        // key longer than 23 bytes.
        let result = self.data.insert_or_update_slice(
            key,
            |existing: &mut Entry| {
                // Hit path: replace existing entry, bump version.
                let new_entry = entry_cell.take().expect("update closure called once");
                old_cost = entry_overhead(key, existing);
                old_ttl = existing.expires_at_ms();
                let new_version = Entry::bump_version(existing.version());
                let carried_lfu = match tracking {
                    crate::storage::entry::AccessTracking::Lfu { .. } => {
                        existing.note_access(record, now_secs);
                        Some(existing.access_counter())
                    }
                    _ => None,
                };
                *existing = new_entry;
                existing.set_version(new_version);
                if let Some(counter) = carried_lfu {
                    existing.set_access_counter(counter);
                }
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
        // moon#1232: redis counts a flush as the keys it removed (read before
        // the table is swapped out below).
        crate::admin::metrics_setup::record_keyspace_change_by(self.logical_len() as u64);
        // moon#1228: an armed BGSAVE epoch that has not written this database
        // yet keeps the old table as its epoch-start contents (the save then
        // completes with the pre-flush image instead of aborting); otherwise
        // `note_cleared_table` drops it, as this assignment used to. The
        // table's bill is `used_memory` (not `estimated_memory`: the
        // spill-in-flight bytes are not in the table) — without the charges
        // of values UNLINK queued for lazy free (moon#1190), which are not
        // in the table either: with a save armed they are reclaimed first
        // (moon#1228 review 6, N1: the frozen bill carried them for the whole
        // save, on top of the pre-images the trim restores). That is the
        // lazy-free drain's owed work, done here instead of in later ticks.
        if crate::persistence::snapshot_cow::is_armed() && self.lazy_free_reclaimable() {
            let _ = self.reclaim_lazy_free(usize::MAX);
        }
        let old = std::mem::replace(&mut self.data, DashTable::new());
        crate::persistence::snapshot_cow::note_cleared_table(self, old, self.used_memory as u64);
        // moon#1190: the ledger restarts at 0; values still being freed must
        // not be credited against it again.
        self.lazy_free_forget_charges();
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
        // above would otherwise contradict. The retired requests are still in
        // flight and still write their slots: each is superseded, as by a DEL
        // of its key, so a rewrite folded before its completion still writes
        // the flushed key a head DEL (moon#1253).
        self.spill_inflight_supersede_all();
    }

    /// The hot-key sketch (sampling hooks, HOTKEYS command, coordinator
    /// merge). Interior mutability — `&self` suffices for observe/top/clear.
    #[inline]
    pub fn hot_keys(&self) -> &crate::storage::hotkey::HotKeySketch {
        &self.hot_keys
    }

    /// Remove a key and return its entry. No expiry check needed (DEL removes regardless).
    ///
    /// Also removes any COLD (spilled) copy of the key — without this, a DEL
    /// of an evicted key is a no-op and the next GET resurrects the value via
    /// cold read-through (D1, tmp/OFFLOAD-COMPRESSION-REVIEW.md). A cold-only
    /// key returns `None` (no in-RAM entry exists); callers that must COUNT
    /// cold-only removals (DEL/UNLINK) use [`Self::remove_counting_cold`].
    pub fn remove(&mut self, key: &[u8]) -> Option<Entry> {
        let had_cold = self.remove_cold_only(key);
        let hot = self.remove_hot(key);
        // moon#1232: a removal that found nothing is no change (redis counts
        // `DEL missing` as 0).
        if had_cold || hot.is_some() {
            crate::admin::metrics_setup::record_keyspace_change();
        }
        hot
    }

    /// Remove hot + cold copies; returns `true` when a LIVE copy existed, so
    /// DEL/UNLINK count spilled keys as removed (Redis semantics: the key
    /// logically exists). A copy that is already TTL-expired is reclaimed
    /// but NOT counted — DEL of a logically-expired key answers 0: a cold
    /// entry judged from the cached `ColdLocation::ttl_ms` (no disk read),
    /// and a hot entry by the db clock (moon#1234; redis's `expireIfNeeded`
    /// deletes it before DEL looks). The removed hot entry, when present, is
    /// also returned — `DEL` tells an expired one by it.
    pub fn remove_counting_cold(&mut self, key: &[u8]) -> (bool, Option<Entry>) {
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
        let hot_alive = hot.as_ref().is_some_and(|e| !e.is_expired_at(now_ms));
        let live = hot_alive || (had_cold && cold_alive) || inflight_alive;
        // moon#1232: only a LIVE key's removal is a change. An absent key is
        // none, and an expired one is expiry's reap, which redis does not
        // count either (`expireIfNeeded` runs before DEL looks).
        if live {
            crate::admin::metrics_setup::record_keyspace_change();
        }
        (live, hot)
    }

    /// `UNLINK` for one key (moon#1190): [`Self::remove_counting_cold`]'s
    /// answer, but a large hot value is handed to the lazy-free queue instead
    /// of being walked for its ledger cost and dropped inside the command.
    ///
    /// O(1) for the command however big the value: one table probe, the
    /// expiry-index unindex, and a queue push. The value's bytes stay charged
    /// to `used_memory` until the shard tick's drain frees them.
    pub fn unlink(&mut self, key: &[u8]) -> bool {
        self.unlink_key(key) == KeyDeletion::Live
    }

    /// [`Self::unlink`], classified: a hot copy whose TTL had passed is
    /// reaped through the same lazy-free path, as [`KeyDeletion::Expired`].
    pub(crate) fn unlink_key(&mut self, key: &[u8]) -> KeyDeletion {
        let now_ms = self.cached_now_ms;
        let cold_alive = self
            .cold_index
            .as_ref()
            .and_then(|ci| ci.lookup(key))
            .is_some_and(|loc| loc.ttl_ms.is_none_or(|ttl| now_ms <= ttl));
        let inflight_alive = self.spill_inflight_alive(key, now_ms);
        let had_cold = self.remove_cold_only(key);
        let hot = self.remove_hot_lazily(key);
        if hot == Some(false) || (had_cold && cold_alive) || inflight_alive {
            // moon#1232: as `remove_counting_cold` — only a live key counts.
            crate::admin::metrics_setup::record_keyspace_change();
            KeyDeletion::Live
        } else if hot == Some(true) {
            KeyDeletion::Expired
        } else {
            KeyDeletion::Absent
        }
    }

    /// [`Self::remove`] for a server-initiated deletion (active expiry)
    /// whose value may be large: hot and cold copies go, and a large hot
    /// value is freed through the lazy-free queue (moon#1190). Returns
    /// whether a hot entry was removed.
    ///
    /// Not a keyspace change for `rdb_changes_since_last_save` (moon#1232):
    /// redis's expiry (`deleteExpiredKeyAndPropagate`) leaves `dirty` alone.
    pub(crate) fn remove_lazily(&mut self, key: &[u8]) -> bool {
        let _ = self.remove_cold_only(key);
        self.remove_hot_lazily(key).is_some()
    }

    /// [`Self::remove_hot`] without the O(n) parts for a large value: no
    /// `entry_overhead` walk (the drain credits as it frees) and the
    /// hash-field index is unindexed from the value's cached minimum rather
    /// than a scan of its TTL sidecar (a missed pair is harmless there —
    /// stale-early pairs self-heal, see `hash_expiry_index`).
    /// `None` when no hot entry, else whether it had expired (moon#1234).
    fn remove_hot_lazily(&mut self, key: &[u8]) -> Option<bool> {
        let entry = self.data.remove(key)?;
        let expired = entry.is_expired_at(self.cached_now_ms);
        if entry.has_expiry() {
            self.expiry_index_remove(entry.expires_at_ms(), key);
        }
        self.forget_removed_hash_ttl(key, &entry);
        self.lazy_free_or_drop(key.len(), entry, true);
        Some(expired)
    }

    /// Unindex a just-removed entry from the hash-field index. For a value
    /// headed to the lazy-free queue the pair is found from the value's
    /// cached minimum instead of a scan of its TTL sidecar (a missed pair is
    /// harmless there — stale-early pairs self-heal, see `hash_expiry_index`).
    fn forget_removed_hash_ttl(&mut self, key: &[u8], entry: &Entry) {
        if super::lazy_free::lazy_free_weight(entry).is_none() {
            self.hash_expiry_index_forget(key, entry);
            return;
        }
        if let crate::storage::compact_value::RedisValueRef::HashWithTtl { min_expiry_ms, .. } =
            entry.value.as_redis_value()
            && min_expiry_ms != u64::MAX
            && !self.hash_expiry_index.is_empty()
        {
            self.hash_expiry_index
                .remove(&(min_expiry_ms, CompactKey::from(key)));
        }
    }

    /// The active-expiry sweep's removal of an index pair it just POPPED
    /// (moon#1189): ONE table probe decides and removes.
    ///
    /// Replaces `is_key_expired` (a probe) + `remove` (a second probe, plus a
    /// second index search and a `CompactKey` build to retire the pair the
    /// sweep had peeked and cloned).
    ///
    /// - [`ExpiredRemoval::Removed`]: the entry carried exactly `ts` and is
    ///   expired at `now_ms`; it is gone with its cold copy, and a large value
    ///   went to the lazy-free queue (moon#1190). The popped pair WAS its
    ///   index pair, so there is nothing left to unindex.
    /// - [`ExpiredRemoval::Stale`]: the entry is gone or carries another
    ///   deadline — the popped pair was a leftover and is now retired.
    /// - [`ExpiredRemoval::NotYetDue`]: the pair is valid but the entry is
    ///   not expired at `now_ms`; the caller must restore the pair.
    pub(crate) fn remove_expired_at(&mut self, key: &[u8], ts: u64, now_ms: u64) -> ExpiredRemoval {
        let mut not_yet_due = false;
        let outcome = self.data.remove_if(key, |e| {
            if e.expires_at_ms() != ts {
                return false;
            }
            if e.is_expired_at(now_ms) {
                true
            } else {
                not_yet_due = true;
                false
            }
        });
        match outcome {
            crate::storage::dashtable::RemoveIf::Removed(entry) => {
                // No keyspace change counted: expiry is not one in redis
                // (moon#1232, see `remove_lazily`).
                let _ = self.remove_cold_only(key);
                self.forget_removed_hash_ttl(key, &entry);
                self.lazy_free_or_drop(key.len(), entry, true);
                ExpiredRemoval::Removed
            }
            crate::storage::dashtable::RemoveIf::Kept if not_yet_due => ExpiredRemoval::NotYetDue,
            crate::storage::dashtable::RemoveIf::Kept
            | crate::storage::dashtable::RemoveIf::Absent => ExpiredRemoval::Stale,
        }
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

    /// Set or remove expiration on an existing key.
    ///
    /// Performs lazy expiry check first. Returns `false` if the key does not
    /// exist (or has already expired). Pass 0 to remove expiry.
    pub fn set_expiry(&mut self, key: &[u8], expires_at_ms: u64) -> bool {
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
                // moon#926: a TTL change is a modification of the watched key.
                // Verified against redis 8.6.1, which dirties on every
                // SUCCESSFUL expire-family write — `EXPIREAT k <same-ts>` twice
                // aborts a watcher the second time too — but leaves the key
                // alone when `PERSIST` finds no TTL to remove. `expires_at_ms
                // != 0` is the expire family; `old != 0` is the PERSIST that
                // actually removed something.
                if expires_at_ms != 0 || old != 0 {
                    crate::storage::db::stamp_mutation(entry);
                    // moon#1232: the same rule is redis's `dirty` — an
                    // expire-family write counts, a PERSIST that removed
                    // nothing and an EXPIRE of a missing key do not.
                    crate::admin::metrics_setup::record_keyspace_change();
                }
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

    /// Increment version of a key if it exists — the BY-KEY form, which
    /// costs its own hash probe.
    ///
    /// The production write path does NOT come through here: every accessor
    /// that hands out a mutable handle already holds the `&mut Entry` it
    /// probed for and stamps that directly via
    /// [`crate::storage::db::stamp_mutation`], which is where the moon#926
    /// invariant lives. This entry point remains for a caller that has only a
    /// key — and is deliberately implemented in terms of the same helper so
    /// the two can never disagree about what a bump is.
    pub fn increment_version(&mut self, key: &[u8]) {
        if let Some(entry) = self.data.get_mut(key) {
            crate::storage::db::stamp_mutation(entry);
        }
    }

    /// `TOUCH` for one key (moon#1161): answer whether it exists (hot,
    /// cold-only or mid-spill — the same answer as [`Self::exists`]) and
    /// record an access on a live hot entry.
    ///
    /// Unlike an ordinary read this records even when the policy tracks
    /// nothing: touching is the command's whole purpose, so `OBJECT
    /// IDLETIME` restarts from zero as it does on redis. One probe.
    pub fn touch_key(&mut self, key: &[u8]) -> bool {
        let now_ms = self.cached_now_ms;
        match self.data.get(key) {
            None => self.cold_contains_alive(key, now_ms),
            Some(entry) if entry.is_expired_at(now_ms) => {
                // Same as `exists`: hide + defer, never shadow a live cold copy.
                self.note_lazy_expired(key);
                self.cold_contains_alive(key, now_ms)
            }
            Some(entry) => {
                entry.note_access(touch_tracking(), self.cached_now);
                true
            }
        }
    }
}

/// What `TOUCH` records: the policy's tracking, or a plain LRU stamp when the
/// policy tracks nothing (TOUCH is an explicit request to be recorded).
#[inline]
pub(super) fn touch_tracking() -> crate::storage::entry::AccessTracking {
    match crate::storage::eviction::access_tracking() {
        crate::storage::entry::AccessTracking::Off => crate::storage::entry::AccessTracking::Lru,
        tracking => tracking,
    }
}

impl Database {
    /// Touch access time of a key for LRU tracking (for reads).
    pub fn touch_access(&mut self, key: &[u8]) {
        let now = self.cached_now;
        if let Some(entry) = self.data.get_mut(key) {
            entry.set_last_access(now);
        }
    }
}
