//! Cold-tier promotion, the cold-fault channel and the cold existence
//! checks (split from `db/kv_ops.rs`; pure move).

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, RedisValue};

impl Database {
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
    /// Those accessors now reach the same work through
    /// [`Self::promote_cold_known_absent`] -- this method minus the hot
    /// guard below, which their own preamble has already answered -- so the
    /// gap stays closed by the same code, one probe cheaper (moon#942).
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
        // THE HOT GUARD. Not an optimisation -- see
        // [`Self::promote_cold_known_absent`], which is this method without
        // it and is only callable by a caller that has already established
        // the key is not hot.
        if self.data.contains_key(key) {
            return true;
        }
        self.promote_cold_known_absent(key, now_ms)
    }

    /// [`Self::promote_cold_if_present`] for a caller that has ALREADY
    /// established, with its own hot-plane lookup, that `key` is **not**
    /// resident in `self.data`.
    ///
    /// # Precondition (load-bearing for data integrity, not just for speed)
    ///
    /// `key` MUST be absent from the hot plane on entry. The `contains_key`
    /// this skips is not merely a fast path: it is the only thing standing
    /// between [`Self::promote_inflight_if_present`] and a hot value.
    /// That method does **not** re-check residency -- it calls
    /// `Database::set` unconditionally -- so calling this on a key that is
    /// hot AND still carries an in-flight spill record would overwrite the
    /// live value with the older spilled body. (`promote_cold_outcome`, the
    /// on-disk arm, carries its own `contains_key` guard and is safe either
    /// way; the in-flight arm is not.)
    ///
    /// `pub(super)` on purpose: the only caller is `accessors::
    /// Database::settle_not_live`, which reaches here exclusively from
    /// `HotState::Absent` (`self.data.get(key)` just answered `None`) or from
    /// `HotState::Expired` after `remove_hot` has unconditionally removed the
    /// entry. Both arms leave `key` provably absent. Do not widen this
    /// visibility, and do not call it from a path that has not just looked.
    ///
    /// moon#942: this removes the fourth probe from every `get_or_create*`
    /// miss. It is a PROBE-COUNT reduction and makes no throughput claim --
    /// see `storage::db::probe_budget`'s module docs for why this repo does
    /// not treat those as the same thing.
    pub(super) fn promote_cold_known_absent(&mut self, key: &[u8], now_ms: u64) -> bool {
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
    ///
    /// An EXPIRED record is retired too (moon#1255), and the answer is
    /// `false`. The key is logically absent, and every caller then answers it
    /// absent (a read) or creates it afresh (`get_or_create*` via
    /// `settle_not_live`). The record, however, is still its request's
    /// authorization to publish. Left in place, the completion publishes the
    /// expired slot as the key's cold entry behind the new value and logs its
    /// `MOON.SPILLED` after the write, and replaying that marker drops the
    /// acknowledged write. A record whose payload does not rehydrate but has
    /// not expired is left alone: its spill file is the key's only copy.
    pub fn promote_inflight_if_present(&mut self, key: &[u8], now_ms: u64) -> bool {
        if self.spill_inflight_is_empty() {
            return false;
        }
        let Some(entry) = self.spill_inflight_entry(key, now_ms) else {
            if self.spill_inflight_expired(key, now_ms) {
                self.spill_inflight_forget(key);
            }
            return false;
        };
        self.spill_inflight_forget(key);
        // moon#1232: a read's promotion is no keyspace change.
        let _quiet = crate::admin::metrics_setup::mute_keyspace_changes();
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
            // The location a read may use right now: the index entry, or
            // during a gated replay the older authorized copy the gate hands
            // out in its place (moon#1140).
            let still_valid = self
                .cold_location_visible(key)
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
                //
                // moon#898: this is the boundary where a cold value re-enters
                // the HOT keyspace, and therefore where its compact encoding
                // has to be re-derived. The spill body format is canonical per
                // logical type (`Set | SetListpack | SetIntset` all encode as
                // `ValueType::Set`), so `cold_read` can only hand back the FULL
                // form; without this step the default-enabled offload path
                // flattened every container it promoted, permanently — nothing
                // demotes (moon#832). The re-derivation cannot live one layer
                // down in `kv_serde::deserialize_collection`, which the
                // NON-promoting read-through shares: `ValueKind::classify_cold`
                // accepts only the full forms and would answer WRONGTYPE for a
                // compacted one. See `kv_serde::compact_for_promotion`.
                let mut entry = Entry::new_string(Bytes::new()); // placeholder
                entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
                    crate::storage::tiered::kv_serde::compact_for_promotion(redis_value),
                );
                if let Some(ttl) = ttl_ms {
                    entry.set_expires_at_ms(ttl);
                }
                {
                    // moon#1232: bringing a cold value back into RAM changes
                    // nothing in the keyspace — a GET must not count.
                    let _quiet = crate::admin::metrics_setup::mute_keyspace_changes();
                    self.set(key, entry);
                }
                if let Some(ref mut ci) = self.cold_index {
                    ci.remove(key);
                }
                true
            }
            ColdReadOutcome::Expired => {
                // Expired on disk: reclaim the index entry now rather than
                // waiting for the periodic sweep. TTL expiry IS swept --
                // `ColdIndex::sweep_expired` runs alongside `orphan_sweep`
                // in `shard::timers::run_cold_orphan_sweep`, every
                // `cold_orphan_sweep_interval_secs` -- so this is a latency
                // optimisation, not the only reclaim path. Doing it here
                // costs nothing (we have already paid for the read that
                // proved the entry expired) and keeps `# Keyspace` from
                // counting a key that the very next `EXISTS` will deny.
                // Safe to remove unconditionally here -- the revalidation
                // above already confirmed the index still points at
                // `expected_location`, so this can't be clobbering a newer
                // entry.
                if let Some(ref mut ci) = self.cold_index {
                    ci.remove(key);
                }
                // moon#1013: the lazy cold-tier expiry — same signal as the
                // hot drain and the periodic `sweep_expired`.
                crate::tracking::invalidation::invalidate_server_removed(key);
                false
            }
            ColdReadOutcome::Miss => false,
            // moon#875: indexed, but the bytes could not be produced. NOT a
            // miss: the index entry is kept (a transient I/O error must not
            // permanently drop the key; the next read retries) and nothing
            // is fabricated in hot RAM. `read_cold_entry` has already
            // counted and logged the fault with its location. The caller
            // sees "not hot" and the command answers as it would for an
            // absent key — see `ColdReadOutcome::Unreadable` for why the
            // wire reply is not yet an error.
            ColdReadOutcome::Unreadable(fault) => {
                self.note_cold_fault(fault.reason);
                false
            }
        }
    }

    /// Raise the moon#875 cold-fault flag — see the field docs.
    #[inline]
    pub(crate) fn note_cold_fault(
        &self,
        reason: crate::storage::tiered::cold_read::ColdReadFaultReason,
    ) {
        self.cold_fault
            .store(reason as u8 + 1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Is a cold fault pending for the command currently executing?
    #[inline]
    #[must_use]
    pub fn cold_fault_pending(&self) -> bool {
        self.cold_fault.load(std::sync::atomic::Ordering::Relaxed) != 0
    }

    /// Consume the pending cold fault, if any. One relaxed load when none is
    /// pending — the only cost every command pays.
    #[inline]
    pub fn take_cold_fault(
        &self,
    ) -> Option<crate::storage::tiered::cold_read::ColdReadFaultReason> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.cold_fault.load(Relaxed) == 0 {
            return None;
        }
        let code = self.cold_fault.swap(0, Relaxed);
        crate::storage::tiered::cold_read::ColdReadFaultReason::from_code(code.wrapping_sub(1))
    }

    /// The reply for a key that is indexed in the cold tier but whose bytes
    /// could not be read. An ERROR, never nil: "key not found" is a
    /// legitimate answer a client acts on, and nothing above this layer
    /// could otherwise tell a lost cold entry from a key never written.
    /// `IOERR` is Redis's own prefix for a disk read that failed. Static
    /// bytes — no allocation on the reply path.
    #[inline]
    #[must_use]
    pub fn cold_fault_error() -> Frame {
        Frame::Error(Bytes::from_static(
            b"IOERR cold tier: key is indexed but its data could not be read (see server log)",
        ))
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
        // moon#902: `cold_location_visible` is the replay-gated choke point —
        // during an AOF-authority replay a cold entry whose file has not been
        // cut yet must read as absent, or `SETNX`/`LPUSHX`-style existence
        // checks answer from a copy the log is about to rebuild.
        match self.cold_location_visible(key) {
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
}
