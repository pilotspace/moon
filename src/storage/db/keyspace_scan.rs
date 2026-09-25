//! Keyspace size, iteration, scan and random-key selection over the hot and
//! cold tiers (split from `db/kv_ops.rs`; pure move).

use bytes::Bytes;

use crate::storage::compact_key::CompactKey;
use crate::storage::db::Database;

impl Database {
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
}
