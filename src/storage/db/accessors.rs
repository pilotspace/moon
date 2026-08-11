//! Typed accessors: W5 ValueKind/OwnedKind generics, per-type delegators, read-only refs, blocking-hook helpers, streams (split from db/mod.rs).

use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::protocol::Frame;
use crate::storage::bptree::BPTree;
use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::DashTable;
use crate::storage::db_kind::{self, OwnedKind, ValueKind};
use crate::storage::db_read::{HashRef, ListRef, SetRef, SortedSetRef, StreamRef};
use crate::storage::entry::{Entry, RedisValue, current_time_ms};
use crate::storage::intset::Intset;
use crate::storage::stream::Stream as StreamData;

use crate::storage::db::{Database, entry_overhead, list_elem_cost};

impl Database {
    // ── W5: generic typed accessors ─────────────────────────────────────
    //
    // The per-type accessor skeleton (expiry check → cold promote/
    // read-through → compact-encoding upgrade → variant projection) exists
    // once per access shape below; everything genuinely per-type lives in
    // `storage::db_kind` as a `ValueKind`/`OwnedKind` marker impl. The
    // public `get_hash`/`get_or_create_set`/`get_list_ref_if_alive`/…
    // methods are thin delegators, so command-layer call sites are
    // unchanged and dispatch is fully static.

    /// Remove `key` if it is expired at `now_ms`, crediting its memory.
    fn drop_if_expired(&mut self, key: &[u8], now_ms: u64) {
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
    }

    /// Read-only typed access via the kind's `*Ref` view.
    ///
    /// Takes `&self` because this backs BOTH the exclusive-dispatch path
    /// (`&mut Database`, reborrowed) AND the RwLock-shared-read dispatch
    /// path (`&Database` only). The latter cannot promote a cold hit into
    /// hot RAM, so a hot miss falls back to a non-promoting cold
    /// read-through returning the view's `Owned` variant (P0
    /// cold-collection-visibility fix) — the fast path (key present hot)
    /// still costs exactly one probe. Expiry is checked against the
    /// caller's `now_ms` without removing the key or touching LRU.
    pub fn get_ref_if_alive<K: ValueKind>(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<K::Ref<'_>>, Frame> {
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired_at(now_ms) {
                return Ok(None);
            }
            return match K::classify_hot(entry.value.as_redis_value(), now_ms) {
                Ok(r) => Ok(Some(r)),
                Err(db_kind::WrongType) => Err(Self::wrongtype_error()),
            };
        }
        match self.cold_read_only(key, now_ms) {
            Some(v) => match K::classify_cold(v, now_ms) {
                Ok(r) => Ok(Some(r)),
                Err(db_kind::WrongType) => Err(Self::wrongtype_error()),
            },
            None => Ok(None),
        }
    }

    /// Get-or-create typed access to the full (non-compact) encoding.
    ///
    /// Promotes a cold-spilled value back to hot RAM before fabricating an
    /// empty container (P0 fix: a write on an evicted key must not silently
    /// shadow the cold copy — see `Self::promote_cold_if_present`), then
    /// upgrades the kind's compact encoding(s) in place.
    pub fn get_or_create<K: OwnedKind>(&mut self, key: &[u8]) -> Result<K::Mut<'_>, Frame> {
        let now_ms = self.cached_now_ms;
        self.drop_if_expired(key, now_ms);
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = K::new_entry();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let Some(entry) = self.data.get_mut(key) else {
            // Should not happen — insert was just called above. Log and
            // return an error instead of panicking.
            tracing::error!(
                "get_or_create: get_mut returned None after insert for key len={}",
                key.len()
            );
            return Err(Frame::Error(bytes::Bytes::from_static(
                b"ERR internal: lookup failed after insert",
            )));
        };
        K::upgrade(entry);
        match entry.value.as_redis_value_mut() {
            Some(v) => match K::project_mut(v) {
                Ok(m) => Ok(m),
                Err(db_kind::WrongType) => Err(Self::wrongtype_error()),
            },
            None => Err(Self::wrongtype_error()),
        }
    }

    /// Read typed access to the full (non-compact) encoding, promoting a
    /// cold-spilled value back to hot RAM on miss (this accessor takes
    /// `&mut self` — unlike the enum-based `get_ref_if_alive` it can
    /// promote directly instead of decoding a throwaway copy per call) and
    /// upgrading the kind's compact encoding(s) in place when present.
    pub fn get_promoted<K: OwnedKind>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<K::Shared<'_>>, Frame> {
        let now_ms = self.cached_now_ms;
        self.drop_if_expired(key, now_ms);
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        if let Some(entry) = self.data.get_mut(key) {
            K::upgrade(entry);
        }
        match self.data.get(key) {
            None => Ok(None),
            Some(entry) => match K::project_ref(entry.value.as_redis_value()) {
                Ok(r) => Ok(Some(r)),
                Err(db_kind::WrongType) => Err(Self::wrongtype_error()),
            },
        }
    }

    /// Get or create a hash entry. Returns mutable ref to inner HashMap.
    /// Returns Err(WRONGTYPE) if key exists with wrong type.
    ///
    /// New keys start with compact listpack encoding and are upgraded to
    /// full HashMap on first mutable access (eager upgrade).
    pub fn get_or_create_hash(&mut self, key: &[u8]) -> Result<&mut HashMap<Bytes, Bytes>, Frame> {
        self.get_or_create::<db_kind::HashKind>(key)
    }

    /// Get a hash entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encoding to full HashMap if found.
    ///
    /// Promotes a cold-spilled hash back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`, so
    /// unlike the enum-based `get_hash_ref_if_alive` it can promote directly
    /// instead of decoding a throwaway copy on every call.
    pub fn get_hash(&mut self, key: &[u8]) -> Result<Option<&HashMap<Bytes, Bytes>>, Frame> {
        self.get_promoted::<db_kind::HashKind>(key)
    }

    /// Get or create a list entry. Returns mutable ref to inner VecDeque.
    /// New keys start with full encoding. Upgrades compact listpack on access.
    pub fn get_or_create_list(&mut self, key: &[u8]) -> Result<&mut VecDeque<Bytes>, Frame> {
        self.get_or_create::<db_kind::ListKind>(key)
    }

    /// Get a list entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encoding to full VecDeque if found.
    ///
    /// Promotes a cold-spilled list back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        self.get_promoted::<db_kind::ListKind>(key)
    }

    /// Get or create a set entry. Returns mutable ref to inner HashSet.
    /// New keys start with full encoding. Upgrades compact encodings on access.
    pub fn get_or_create_set(&mut self, key: &[u8]) -> Result<&mut HashSet<Bytes>, Frame> {
        self.get_or_create::<db_kind::SetKind>(key)
    }

    /// Get a set entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encodings to full HashSet if found.
    ///
    /// Promotes a cold-spilled set back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    pub fn get_set(&mut self, key: &[u8]) -> Result<Option<&HashSet<Bytes>>, Frame> {
        self.get_promoted::<db_kind::SetKind>(key)
    }

    /// Get or create an intset entry. Creates a new SetIntset if the key doesn't exist.
    /// Returns Err if the key exists but holds a non-set type.
    /// Returns Ok(None) if the key exists but is not an intset (caller should use get_or_create_set).
    /// Returns Ok(Some(&mut Intset)) if the key holds or was created as an intset.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_intset(&mut self, key: &[u8]) -> Result<Option<&mut Intset>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled set before fabricating an empty
            // intset — a promoted value always decodes as `RedisValue::Set`
            // (cold storage never persists the intset compact encoding), so
            // it naturally falls into the `Ok(None)` "not an intset, caller
            // should use get_or_create_set" arm below, which already routes
            // callers (e.g. SADD) to `get_or_create_set` — no fabrication.
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_set_intset();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => Ok(Some(is)),
            Some(RedisValue::Set(_)) | Some(RedisValue::SetListpack(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade an intset entry to a full HashSet and return mutable ref.
    /// Panics if the key doesn't exist or isn't a SetIntset.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is SetIntset; upgrade is infallible
    pub fn upgrade_intset_to_set(&mut self, key: &[u8]) -> &mut HashSet<Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => {
                let set = is.to_hash_set();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(set);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Set(set)) => set,
            _ => unreachable!("upgrade_intset_to_set: expected Set after upgrade"),
        }
    }

    /// Get or create a hash entry as listpack. Creates new keys as HashListpack.
    /// Returns Ok(Some(&mut Listpack)) if the key is a HashListpack.
    /// Returns Ok(None) if the key already holds a full Hash (caller should fall through).
    /// Returns Err(WRONGTYPE) if the key holds a non-hash type.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_hash_listpack(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut crate::storage::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled hash before fabricating an
            // empty listpack — a promoted value always decodes as
            // `RedisValue::Hash` (cold storage never persists the listpack
            // compact encoding), so it naturally falls into the `Ok(None)`
            // "not a listpack, fall through" arm below, which already routes
            // callers (e.g. HSET) to `get_or_create_hash` — no fabrication.
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_hash_listpack();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::HashListpack(lp)) => Ok(Some(lp)),
            // Plain HashMap or TTL-extended hash: caller falls through to the
            // HashMap path.  TTL'd hashes never compact back to listpack.
            Some(RedisValue::Hash(_)) | Some(RedisValue::HashWithTtl { .. }) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade a HashListpack to full Hash. Returns mutable ref to the HashMap.
    /// Panics if the key doesn't exist or isn't a HashListpack.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is HashListpack; upgrade is infallible
    pub fn upgrade_hash_listpack_to_hash(&mut self, key: &[u8]) -> &mut HashMap<Bytes, Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::HashListpack(lp)) => {
                let map = lp.to_hash_map();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(map);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Hash(map)) => map,
            _ => unreachable!("upgrade_hash_listpack_to_hash: expected Hash after upgrade"),
        }
    }

    /// Get or create a list entry as listpack. Creates new keys as ListListpack.
    /// Returns Ok(Some(&mut Listpack)) if the key is a ListListpack.
    /// Returns Ok(None) if the key already holds a full List (caller should fall through).
    /// Returns Err(WRONGTYPE) if the key holds a non-list type.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_list_listpack(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut crate::storage::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            // P0 fix: promote a cold-spilled list before fabricating an
            // empty listpack — a promoted value always decodes as
            // `RedisValue::List` (cold storage never persists the listpack
            // compact encoding), so it naturally falls into the `Ok(None)`
            // "not a listpack, fall through" arm below, which already routes
            // callers (e.g. LPUSH) to `get_or_create_list` — no fabrication.
            self.promote_cold_if_present(key, now_ms);
            if !self.data.contains_key(key) {
                let entry = Entry::new_list_listpack();
                let k = CompactKey::from(key);
                self.used_memory += entry_overhead(key, &entry);
                self.data.insert(k, entry);
            }
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::ListListpack(lp)) => Ok(Some(lp)),
            Some(RedisValue::List(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade a ListListpack to full List. Returns mutable ref to the VecDeque.
    /// Panics if the key doesn't exist or isn't a ListListpack.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is ListListpack; upgrade is infallible
    pub fn upgrade_list_listpack_to_list(&mut self, key: &[u8]) -> &mut VecDeque<Bytes> {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::ListListpack(lp)) => {
                let list = lp.to_vec_deque();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::List(list);
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::List(list)) => list,
            _ => unreachable!("upgrade_list_listpack_to_list: expected List after upgrade"),
        }
    }

    /// Get or create a sorted set entry. Returns mutable refs to both inner structures.
    ///
    /// New keys start with SortedSetBPTree encoding. Legacy SortedSet (BTreeMap)
    /// entries are upgraded to SortedSetBPTree on access for backward compatibility.
    pub fn get_or_create_sorted_set(
        &mut self,
        key: &[u8],
    ) -> Result<(&mut HashMap<Bytes, f64>, &mut BPTree), Frame> {
        self.get_or_create::<db_kind::SortedSetKind>(key)
    }

    /// Get a sorted set entry (read-only). Returns None if key missing, Err if wrong type.
    ///
    /// Promotes a cold-spilled sorted set back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    pub fn get_sorted_set(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(&HashMap<Bytes, f64>, &BPTree)>, Frame> {
        self.get_promoted::<db_kind::SortedSetKind>(key)
    }

    /// Collect keys that have an expiration set.
    pub fn keys_with_expiry(&self) -> Vec<CompactKey> {
        self.data
            .iter()
            .filter(|(_, e)| e.has_expiry())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Collect keys whose value is a `HashWithTtl` (hash with per-field TTLs).
    ///
    /// Used by the active-expiry tick to find hashes that may have expired
    /// fields ready for reaping.  Returns an owned `Vec` so the caller can
    /// iterate and mutate via `get_mut` without holding a borrow on `self.data`.
    pub fn hashes_with_field_expiry(&self) -> Vec<CompactKey> {
        self.data
            .iter()
            .filter(|(_, e)| {
                matches!(
                    e.value.as_redis_value(),
                    crate::storage::compact_value::RedisValueRef::HashWithTtl { .. }
                )
            })
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Check if a key exists and its expiry is in the past.
    pub fn is_key_expired(&self, key: &[u8]) -> bool {
        let now_ms = current_time_ms();
        self.data.get(key).is_some_and(|e| e.is_expired_at(now_ms))
    }

    /// Read-only access to the data map (for SCAN iteration).
    pub fn data(&self) -> &DashTable<CompactKey, Entry> {
        &self.data
    }

    // ---- Read-only methods for RwLock read path ----
    // These take `now_ms` as a parameter and do NOT mutate state:
    // no expired-key removal, no LRU touch.

    /// Read-only get: checks expiry, returns None if expired, but does NOT
    /// remove expired keys or touch LRU. Used with RwLock read path.
    pub fn get_if_alive(&self, key: &[u8], now_ms: u64) -> Option<&Entry> {
        let entry = self.data.get(key)?;
        if entry.is_expired_at(now_ms) {
            return None;
        }
        Some(entry)
    }

    /// Read-only cold storage lookup for evicted keys.
    ///
    /// When `get_if_alive` returns None, call this to check if the key was
    /// spilled to disk by the eviction path. Returns the value as owned Bytes
    /// (read from disk file). Does NOT promote the entry back to RAM.
    ///
    /// WARNING: this method performs synchronous disk I/O. Callers on the
    /// hot path must release any shard read/write guard *before* invoking it.
    /// Use [`Self::cold_lookup_location`] under the guard, then drop the guard,
    /// then call [`crate::storage::tiered::cold_read::read_cold_entry_at`].
    pub fn get_cold_value(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Option<crate::storage::entry::RedisValue> {
        // In-flight plane first (#459): a key mid-spill is in neither hot nor
        // cold, so every `&self` reader that stops here would answer nil for
        // a key `EXISTS` reports as present. No disk read — the payload is in
        // RAM. This is the choke point for the RwLock-shared-read dispatch
        // path (`*_readonly` handlers, which the tokio runtime's GET takes),
        // so it must come before the `cold_shard_dir`/`cold_index` bails
        // below.
        if let Some(value) = self.spill_inflight_value(key, now_ms) {
            return Some(value);
        }
        let shard_dir = self.cold_shard_dir.as_ref()?;
        let ci = self.cold_index.as_ref()?;
        let (value, _ttl) =
            crate::storage::tiered::cold_read::cold_read_through(ci, shard_dir, key, now_ms)?;
        Some(value)
    }

    /// Cheap, in-memory cold-index lookup. Returns the disk location plus a
    /// cloned shard dir path so the caller can drop the shard guard before
    /// performing the disk read.
    pub fn cold_lookup_location(
        &self,
        key: &[u8],
    ) -> Option<(
        crate::storage::tiered::cold_index::ColdLocation,
        std::path::PathBuf,
    )> {
        let shard_dir = self.cold_shard_dir.as_ref()?;
        let ci = self.cold_index.as_ref()?;
        let location = ci.lookup(key)?;
        Some((location, shard_dir.clone()))
    }

    /// Returns `true` if the key is present in the hot in-memory DashTable,
    /// regardless of expiry status.
    ///
    /// Used by the cold-tier orphan sweeper to detect "hot shadow" entries:
    /// a cold entry whose key was later overwritten by a hot `SET` is an
    /// orphan — the hot copy shadows it and the cold file is reclaimable.
    ///
    /// This intentionally does NOT check expiry (an expired hot key still
    /// shadows the cold entry; the hot expiry path will clean it up on next
    /// access). Using the expired-included check avoids a TOCTOU race where
    /// we decide "not hot" and then a concurrent read promotes the expired key.
    #[inline]
    pub fn is_hot(&self, key: &[u8]) -> bool {
        self.data.get(key).is_some()
    }

    /// Read-only existence check: returns false if expired.
    ///
    /// Also counts a cold-only key (spilled by eviction) as existing — see
    /// [`Self::cold_contains_alive`] (P0 cold-collection-visibility fix).
    /// Used by the RwLock-shared-read `EXISTS` dispatch path, which cannot
    /// mutate `self` to promote.
    pub fn exists_if_alive(&self, key: &[u8], now_ms: u64) -> bool {
        match self.data.get(key) {
            Some(e) if !e.is_expired_at(now_ms) => true,
            _ => self.cold_contains_alive(key, now_ms),
        }
    }

    // ---- Enum-based readonly accessors that handle compact encodings ----

    /// Read-only hash access via HashRef enum.
    ///
    /// Handles `Hash` (HashMap), `HashListpack` (compact Listpack), and
    /// `HashWithTtl` (HashMap with per-field TTL sidecar).  For `HashWithTtl`
    /// returns a `HashRef::WithTtl` that filters expired fields on every
    /// field-level operation without mutating the database (lazy expiry).
    ///
    /// Takes `&self` because this backs BOTH the exclusive-dispatch path
    /// (`&mut Database`, reborrowed) AND the RwLock-shared-read dispatch
    /// path (`&Database` only — `hget_readonly`/`hgetall_readonly`/etc.).
    /// The latter cannot promote a cold hit into hot RAM, so a hot miss
    /// falls back to a non-promoting cold read-through returning
    /// `HashRef::Owned`/`OwnedWithTtl` (P0 cold-collection-visibility fix) —
    /// the fast path (key present hot) still costs exactly one probe.
    pub fn get_hash_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<HashRef<'_>>, Frame> {
        self.get_ref_if_alive::<db_kind::HashKind>(key, now_ms)
    }

    /// Read-only list access via ListRef enum. Handles both VecDeque and Listpack.
    ///
    /// See [`Self::get_hash_ref_if_alive`] for why this consults the cold
    /// tier without promoting on a hot miss (P0 cold-collection-visibility
    /// fix).
    pub fn get_list_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<ListRef<'_>>, Frame> {
        self.get_ref_if_alive::<db_kind::ListKind>(key, now_ms)
    }

    /// Read-only set access via SetRef enum. Handles HashSet, Listpack, and Intset.
    ///
    /// See [`Self::get_hash_ref_if_alive`] for why this consults the cold
    /// tier without promoting on a hot miss (P0 cold-collection-visibility
    /// fix).
    pub fn get_set_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<SetRef<'_>>, Frame> {
        self.get_ref_if_alive::<db_kind::SetKind>(key, now_ms)
    }

    /// Read-only sorted set access via SortedSetRef enum. Handles BPTree, Listpack, and Legacy.
    ///
    /// See [`Self::get_hash_ref_if_alive`] for why this consults the cold
    /// tier without promoting on a hot miss (P0 cold-collection-visibility
    /// fix).
    pub fn get_sorted_set_ref_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<SortedSetRef<'_>>, Frame> {
        self.get_ref_if_alive::<db_kind::SortedSetKind>(key, now_ms)
    }

    // ---- Low-level helpers for blocking wakeup hooks ----

    /// Pop the front element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. Handles compact listpack upgrade.
    pub fn list_pop_front(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_or_create_list(key).ok()?;
        let val = list.pop_front()?;
        let empty = list.is_empty();
        // `list`'s borrow of `self` ends above.
        if empty {
            // Whole-key removal recomputes the (now-empty) entry cost via
            // `entry_overhead` -- no separate credit needed for the popped
            // element itself.
            self.remove(key);
        } else {
            self.credit_memory(list_elem_cost(&val));
        }
        Some(val)
    }

    /// Pop the back element from a list. Returns None if key missing/empty/wrong type.
    /// Removes the key if the list becomes empty. Handles compact listpack upgrade.
    pub fn list_pop_back(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_or_create_list(key).ok()?;
        let val = list.pop_back()?;
        let empty = list.is_empty();
        if empty {
            self.remove(key);
        } else {
            self.credit_memory(list_elem_cost(&val));
        }
        Some(val)
    }

    /// Push an element to the front of a list. Creates the list if it does not exist.
    pub fn list_push_front(&mut self, key: &[u8], value: Bytes) {
        // get_or_create_list creates the key if missing
        let cost = list_elem_cost(&value);
        if let Ok(list) = self.get_or_create_list(key) {
            list.push_front(value);
            self.charge_memory(cost);
        }
    }

    /// Push an element to the back of a list. Creates the list if it does not exist.
    pub fn list_push_back(&mut self, key: &[u8], value: Bytes) {
        let cost = list_elem_cost(&value);
        if let Ok(list) = self.get_or_create_list(key) {
            list.push_back(value);
            self.charge_memory(cost);
        }
    }

    /// Pop the minimum element from a sorted set. Returns (member, score) or None.
    /// Removes the key if the sorted set becomes empty.
    pub fn zset_pop_min(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self.get_or_create_sorted_set(key).ok()?;
        let first = tree.iter().next().map(|(s, m)| (s, m.clone()))?;
        let (score, member) = first;
        tree.remove(score, &member);
        members.remove(&member);
        let result = (member, score.0);
        if members.is_empty() {
            self.remove(key);
        }
        Some(result)
    }

    /// Pop the maximum element from a sorted set. Returns (member, score) or None.
    /// Removes the key if the sorted set becomes empty.
    pub fn zset_pop_max(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self.get_or_create_sorted_set(key).ok()?;
        let last = tree.iter_rev().next().map(|(s, m)| (s, m.clone()))?;
        let (score, member) = last;
        tree.remove(score, &member);
        members.remove(&member);
        let result = (member, score.0);
        if members.is_empty() {
            self.remove(key);
        }
        Some(result)
    }

    /// Put a (member, score) pair back into a sorted set.
    ///
    /// The exact inverse of `zset_pop_min` / `zset_pop_max`, used by the
    /// blocking-wakeup undo path when the woken client turns out to be gone
    /// (c10k hardening A2). Memory accounting deliberately mirrors the pops:
    /// they do not `credit_memory` for the removed member, so this does not
    /// `charge_memory` for putting it back — the pair is a no-op on
    /// `used_memory`, which is what keeps the estimate consistent across a
    /// pop/restore cycle.
    pub fn zset_restore(&mut self, key: &[u8], member: Bytes, score: f64) {
        if let Ok((members, tree)) = self.get_or_create_sorted_set(key) {
            if let Some(old) = members.insert(member.clone(), score) {
                tree.remove(ordered_float::OrderedFloat(old), &member);
            }
            tree.insert(ordered_float::OrderedFloat(score), member);
        }
    }

    /// Get or create a stream at the given key. Returns WRONGTYPE if key holds another type.
    pub fn get_or_create_stream(&mut self, key: &[u8]) -> Result<&mut StreamData, Frame> {
        self.get_or_create::<db_kind::StreamKind>(key)
    }

    /// Read-only stream access for the shared-lock read path.
    ///
    /// Checks expiry using `now_ms` but does NOT remove the expired key or
    /// touch LRU (mirrors `get_if_alive` semantics).  Returns `Ok(None)` for
    /// missing or expired keys, `Err(WRONGTYPE)` for non-stream keys.
    ///
    /// Returns `StreamRef` rather than `&StreamData`: this backs both the
    /// exclusive-dispatch path and the RwLock-shared-read dispatch path
    /// (`&Database` only, cannot promote). A hot miss falls back to a
    /// non-promoting cold read-through returning `StreamRef::Owned` (P0
    /// cold-collection-visibility fix) — `StreamRef` derefs to `&StreamData`
    /// so existing call sites (`stream.length`, `stream.range(..)`, ...) are
    /// unaffected. The fast path (key present hot) still costs one probe.
    pub fn get_stream_if_alive(
        &self,
        key: &[u8],
        now_ms: u64,
    ) -> Result<Option<StreamRef<'_>>, Frame> {
        self.get_ref_if_alive::<db_kind::StreamKind>(key, now_ms)
    }

    /// Get a read-only reference to a stream. Returns Ok(None) if key doesn't exist.
    /// Returns WRONGTYPE error if key holds another type.
    ///
    /// Promotes a cold-spilled stream back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    pub fn get_stream(&mut self, key: &[u8]) -> Result<Option<&StreamData>, Frame> {
        self.get_promoted::<db_kind::StreamKind>(key)
    }

    /// Get a mutable reference to an existing stream. Returns Ok(None) if key doesn't exist.
    ///
    /// Promotes a cold-spilled stream back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    pub fn get_stream_mut(&mut self, key: &[u8]) -> Result<Option<&mut StreamData>, Frame> {
        let now_ms = self.cached_now_ms;
        if Self::check_expired(&self.data, key, now_ms) {
            if let Some(entry) = self.data.remove(key) {
                self.used_memory = self.used_memory.saturating_sub(entry_overhead(key, &entry));
            }
        }
        if !self.data.contains_key(key) {
            self.promote_cold_if_present(key, now_ms);
        }
        match self.data.get_mut(key) {
            None => Ok(None),
            Some(entry) => match entry.value.as_redis_value_mut() {
                Some(RedisValue::Stream(s)) => Ok(Some(s.as_mut())),
                Some(_) => Err(Self::wrongtype_error()),
                None => Err(Self::wrongtype_error()),
            },
        }
    }
}
