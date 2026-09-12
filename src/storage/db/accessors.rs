//! Typed accessors: W5 ValueKind/OwnedKind generics, per-type delegators, read-only refs, blocking-hook helpers, streams (split from db/mod.rs).

use bytes::Bytes;
use std::collections::{HashMap, VecDeque};

use crate::protocol::Frame;
use crate::storage::bptree::BPTree;
use crate::storage::compact_key::CompactKey;
use crate::storage::dashtable::DashTable;
use crate::storage::db_kind::{self, OwnedKind, ValueKind};
use crate::storage::db_read::{HashRef, ListRef, SetRef, SortedSetRef, StreamRef};
use crate::storage::entry::{Entry, RedisValue, current_time_ms};
use crate::storage::intset::Intset;
use crate::storage::stream::Stream as StreamData;

use crate::storage::db::{Database, entry_overhead, list_elem_cost, stamp_mutation};

/// Rendered width, in bytes, of the widest member of an intset — what the
/// listpack value threshold would measure once the intset's `i64`s become
/// decimal spellings (moon#899). The intset is sorted, so only its two ends
/// can be the widest; 0 for an empty one. Allocation-free.
#[inline]
fn widest_rendering(is: &Intset) -> usize {
    let mut buf = itoa::Buffer::new();
    let first = is.get(0).map_or(0, |v| buf.format(v).len());
    let last = is
        .get(is.len().wrapping_sub(1))
        .map_or(0, |v| buf.format(v).len());
    first.max(last)
}

/// A live entry sourced from either storage plane (moon#610).
///
/// The hot plane hands back a borrow; the cold plane has to materialise the
/// value, so it hands back an owned `Entry`. Both answer [`Self::entry`],
/// which is all a read-only command needs — and making the two planes
/// indistinguishable at the call site is the point: the bug this type exists
/// to prevent was twenty-five call sites that silently saw only one of them.
pub enum EntryView<'a> {
    /// Resident in the hot plane.
    Hot(&'a Entry),
    /// Materialised from the cold tier. NOT inserted into the hot plane.
    Cold(Entry),
}

impl EntryView<'_> {
    /// The entry, whichever plane it came from.
    #[inline]
    pub fn entry(&self) -> &Entry {
        match self {
            Self::Hot(e) => e,
            Self::Cold(e) => e,
        }
    }
}

/// Deref so a call site that already had an `&Entry` keeps reading exactly as
/// it did. That keeps the moon#610 conversion a one-word rename per handler
/// rather than a rewrite of ten command bodies — the smallest diff that can
/// carry the fix, and the one least likely to change a behaviour by accident.
impl std::ops::Deref for EntryView<'_> {
    type Target = Entry;

    #[inline]
    fn deref(&self) -> &Entry {
        self.entry()
    }
}

/// What ONE hot-plane lookup says about a key (moon#942).
///
/// The accessor skeleton used to spend three DashTable lookups before it
/// handed anything out: a `get` inside `drop_if_expired`, a `contains_key`,
/// then the `get_mut`. The first two answer the same question — "is there a
/// live entry here?" — and this enum is that question asked once.
///
/// `Database::get` (`kv_ops.rs:22`) has used the same shape since it was
/// written; the typed accessors now share it rather than each re-deriving it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum HotState {
    /// Present and not expired at the caller's `now_ms`.
    Live,
    /// Present but past its deadline. Must read as ABSENT, and the entry has
    /// to be removed through `remove_hot` so the expiry index stays in
    /// lock-step (moon#541).
    Expired,
    /// Not in the hot plane. May still be in the cold tier or mid-spill.
    Absent,
}

impl Database {
    /// Classify `key` in the hot plane with EXACTLY ONE DashTable lookup.
    ///
    /// This is the single probe that replaced the `drop_if_expired` +
    /// `contains_key` pair (moon#942). Expiry is *observed* here and acted on
    /// by [`Self::settle_not_live`] — the split exists only because dropping
    /// an expired entry needs `&mut self` and classifying it does not.
    #[inline]
    fn hot_state(&self, key: &[u8], now_ms: u64) -> HotState {
        match self.data.get(key) {
            Some(e) if e.is_expired_at(now_ms) => HotState::Expired,
            Some(_) => HotState::Live,
            None => HotState::Absent,
        }
    }

    /// Resolve a key [`Self::hot_state`] just reported as NOT `Live`. Returns
    /// `true` if the key is resident in the hot plane afterwards.
    ///
    /// Both the expired and the absent arm attempt cold promotion, exactly as
    /// the `drop_if_expired` + `contains_key` + `promote_cold_if_present`
    /// sequence did: dropping an expired HOT copy is not a statement about the
    /// cold plane, and skipping the promotion would let a write on an evicted
    /// key silently shadow the cold copy (moon#459 / the P0 this accessor
    /// family carries).
    ///
    /// The return value replaces the second `contains_key`:
    /// `promote_cold_if_present`'s documented contract is "`true` iff `key` is
    /// present in hot RAM after this call", and `promote_cold_outcome` honours
    /// it on every arm — `Hit` inserts and answers `true`, `Expired` and
    /// `Miss` insert nothing and answer `false`.
    #[inline]
    fn settle_not_live(&mut self, key: &[u8], now_ms: u64, state: HotState) -> bool {
        debug_assert!(
            state != HotState::Live,
            "settle_not_live called on a live key"
        );
        if state == HotState::Expired {
            // Write-path expired drop: the incoming write supersedes the key
            // everywhere, so no #542 hide/queue here — but the removal must
            // still go through `remove_hot` so the expiry index stays in
            // lock-step (moon#541).
            self.remove_hot(key);
        }
        self.promote_cold_if_present(key, now_ms)
    }

    /// Insert a freshly fabricated, empty container at `key` and charge it.
    ///
    /// Stamps the per-db creation ticket so a WATCHing client can tell this
    /// container from the one that occupied the key before (see
    /// `Database::birth_counter`), then bills `entry_overhead` — the two
    /// steps every `get_or_create*` fabrication path used to spell out for
    /// itself. One implementation means one place for the ledger to be right.
    #[inline]
    fn insert_fresh(&mut self, key: &[u8], mut entry: Entry) {
        let version = self.next_birth_version();
        entry.set_version(version);
        self.used_memory += entry_overhead(key, &entry);
        self.data.insert(CompactKey::from(key), entry);
    }

    // ── W5: generic typed accessors ─────────────────────────────────────
    //
    // The per-type accessor skeleton (expiry check → cold promote/
    // read-through → compact-encoding upgrade → variant projection) exists
    // once per access shape below; everything genuinely per-type lives in
    // `storage::db_kind` as a `ValueKind`/`OwnedKind` marker impl. The
    // public `get_hash`/`get_or_create_set`/`get_list_ref_if_alive`/…
    // methods are thin delegators, so command-layer call sites are
    // unchanged and dispatch is fully static.

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
        // moon#942: ONE lookup decides the whole preamble. What used to be a
        // `get` (expiry) + `contains_key` + `get_mut` is now `hot_state` +
        // `get_mut`; the create arm drops a second `contains_key` by reading
        // `promote_cold_if_present`'s own return value instead.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live && !self.settle_not_live(key, now_ms, state) {
            self.insert_fresh(key, K::new_entry());
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
        // moon#926: handing out `K::Mut` IS the mutation as far as WATCH is
        // concerned. See `stamp_mutation` for why this cannot live in the
        // ~60 write handlers instead.
        stamp_mutation(entry);
        // moon#788: a compact→full encoding upgrade changes the entry's real
        // size; charge the difference or the ledger silently desynchronises
        // from the keyspace. Disjoint field borrows: `entry` borrows
        // `self.data`, the counter is a separate field.
        let encoding_delta = K::upgrade(entry);
        self.used_memory = self.used_memory.saturating_add_signed(encoding_delta);
        match entry.value.as_redis_value_mut() {
            Some(v) => match K::project_mut(v) {
                Ok(m) => Ok(m),
                Err(db_kind::WrongType) => Err(Self::wrongtype_error()),
            },
            None => Err(Self::wrongtype_error()),
        }
    }

    /// Mutable typed access to an **existing** key's full (non-compact)
    /// encoding — `get_or_create` minus the fabrication step.
    ///
    /// A missing key answers `Ok(None)` and leaves the keyspace (and
    /// `used_memory`) byte-identical: no entry inserted, no birth version
    /// consumed. Everything else matches `get_or_create` exactly — expired
    /// keys are dropped, a cold-spilled value is promoted back to hot RAM
    /// before it is handed out, the kind's compact encoding is upgraded in
    /// place, and a type mismatch is `Err(WRONGTYPE)`.
    ///
    /// This is what the blocking-pop helpers below take (moon#523/#539): a
    /// pop that finds nothing is a *read* as far as the keyspace is
    /// concerned, and reads never create.
    pub fn get_mut_if_present<K: OwnedKind>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<K::Mut<'_>>, Frame> {
        let now_ms = self.cached_now_ms;
        // moon#942: one lookup for the preamble, one to hand the entry out.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live {
            self.settle_not_live(key, now_ms, state);
        }
        let Some(entry) = self.data.get_mut(key) else {
            return Ok(None);
        };
        // moon#926: a present key is about to be handed out mutably. A MISS
        // returns above without stamping — a pop that finds nothing is a read
        // as far as the keyspace is concerned, and redis does not dirty it.
        stamp_mutation(entry);
        // moon#788: a compact→full encoding upgrade changes the entry's real
        // size; charge the difference or the ledger silently desynchronises
        // from the keyspace. Disjoint field borrows: `entry` borrows
        // `self.data`, the counter is a separate field.
        let encoding_delta = K::upgrade(entry);
        self.used_memory = self.used_memory.saturating_add_signed(encoding_delta);
        match entry.value.as_redis_value_mut() {
            Some(v) => match K::project_mut(v) {
                Ok(m) => Ok(Some(m)),
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
    ///
    /// # This REWRITES the value. Do not call it to read (moon#832).
    ///
    /// The return type is `K::Shared` — a *shared* reference — but getting
    /// one costs an unconditional, one-way `K::upgrade`: nothing in the tree
    /// ever downgrades, so the first call flattens the key's compact encoding
    /// for the rest of its lifetime. Measured on b04e8990: 1000 eight-member
    /// integer sets went 333,055 → 1,149,055 bytes of `used_memory` (3.45×)
    /// after one `SCARD` each; `OBJECT ENCODING` went `intset → hashtable`,
    /// and a three-element list `listpack → linkedlist` after one `LLEN`.
    ///
    /// That is only acceptable for a caller that is *about to mutate* and
    /// therefore needs the full form anyway. A caller that only reads — a
    /// length probe, a membership test, a `-WRONGTYPE` gate — must go through
    /// [`Self::get_ref_if_alive`] (`get_hash_ref_if_alive`,
    /// `get_list_ref_if_alive`, `get_set_ref_if_alive`, …), whose `&self`
    /// receiver makes the rewrite unrepresentable rather than merely
    /// discouraged.
    pub fn get_promoted<K: OwnedKind>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<K::Shared<'_>>, Frame> {
        let now_ms = self.cached_now_ms;
        // moon#942: two lookups, not four. The old shape paid a `get` for
        // expiry, a `contains_key`, a `get_mut` to run the upgrade, and then
        // a FOURTH `get` purely to re-borrow the same entry immutably — the
        // upgrade's `&mut` reborrows to `&` for free.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live {
            self.settle_not_live(key, now_ms, state);
        }
        let Some(entry) = self.data.get_mut(key) else {
            return Ok(None);
        };
        // moon#788: a compact→full encoding upgrade changes the entry's real
        // size; charge the difference or the ledger silently desynchronises
        // from the keyspace. Disjoint field borrows: `entry` borrows
        // `self.data`, the counter is a separate field.
        let encoding_delta = K::upgrade(entry);
        self.used_memory = self.used_memory.saturating_add_signed(encoding_delta);
        match K::project_ref(entry.value.as_redis_value()) {
            Ok(r) => Ok(Some(r)),
            Err(db_kind::WrongType) => Err(Self::wrongtype_error()),
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
    ///
    /// **Flattens the compact encoding (moon#832).** Writers only —
    /// see [`Self::get_promoted`]. Pure reads take
    /// [`Self::get_hash_ref_if_alive`].
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
    ///
    /// **Flattens the compact encoding (moon#832).** Writers only —
    /// see [`Self::get_promoted`]. Pure reads take
    /// [`Self::get_list_ref_if_alive`].
    pub fn get_list(&mut self, key: &[u8]) -> Result<Option<&VecDeque<Bytes>>, Frame> {
        self.get_promoted::<db_kind::ListKind>(key)
    }

    /// Get or create a set entry. Returns mutable ref to inner HashSet.
    /// New keys start with full encoding. Upgrades compact encodings on access.
    pub fn get_or_create_set(
        &mut self,
        key: &[u8],
    ) -> Result<&mut crate::storage::entry::SetValue, Frame> {
        self.get_or_create::<db_kind::SetKind>(key)
    }

    /// Get a set entry (read-only). Returns None if key missing, Err if wrong type.
    /// Upgrades compact encodings to full HashSet if found.
    ///
    /// Promotes a cold-spilled set back to hot RAM on miss (P0
    /// cold-collection-visibility fix) — this accessor takes `&mut self`.
    ///
    /// **Flattens the compact encoding (moon#832).** Writers only —
    /// see [`Self::get_promoted`]. Pure reads take
    /// [`Self::get_set_ref_if_alive`].
    pub fn get_set(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&crate::storage::entry::SetValue>, Frame> {
        self.get_promoted::<db_kind::SetKind>(key)
    }

    /// Get or create an intset entry. Creates a new SetIntset if the key doesn't exist.
    /// Returns Err if the key exists but holds a non-set type.
    /// Returns Ok(None) if the key exists but is not an intset (caller should use get_or_create_set).
    /// Returns Ok(Some(&mut Intset)) if the key holds or was created as an intset.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_intset(&mut self, key: &[u8]) -> Result<Option<&mut Intset>, Frame> {
        let now_ms = self.cached_now_ms;
        // P0 fix: `settle_not_live` promotes a cold-spilled set before this
        // fabricates an empty intset — a promoted value always decodes as
        // `RedisValue::Set` (cold storage never persists the intset compact
        // encoding), so it naturally falls into the `Ok(None)` "not an
        // intset, caller should use get_or_create_set" arm below, which
        // already routes callers (e.g. SADD) to `get_or_create_set` — no
        // fabrication.
        //
        // moon#942: ONE lookup decides the preamble — `hot_state` replaces
        // the `drop_if_expired` + `contains_key` pair, and the create arm
        // reads `promote_cold_if_present`'s own return value instead of
        // re-issuing `contains_key`.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live && !self.settle_not_live(key, now_ms, state) {
            self.insert_fresh(key, Entry::new_set_intset());
        }
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => Ok(Some(is)),
            Some(RedisValue::Set(_)) | Some(RedisValue::SetListpack(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade an intset entry to a full HashSet and return mutable ref.
    /// Panics if the key doesn't exist or isn't a SetIntset.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is SetIntset; upgrade is infallible
    pub fn upgrade_intset_to_set(&mut self, key: &[u8]) -> &mut crate::storage::entry::SetValue {
        let entry = self.data.get_mut(key).unwrap();
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetIntset(is)) => {
                let set = is.to_set_value();
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Set(Box::new(set));
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
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
        // P0 fix: `settle_not_live` promotes a cold-spilled hash before this
        // fabricates an empty listpack — a promoted value always decodes as
        // `RedisValue::Hash` (cold storage never persists the listpack
        // compact encoding), so it naturally falls into the `Ok(None)` "not a
        // listpack, fall through" arm below, which already routes callers
        // (e.g. HSET) to `get_or_create_hash` — no fabrication.
        //
        // moon#942: ONE lookup decides the preamble — `hot_state` replaces
        // the `drop_if_expired` + `contains_key` pair, and the create arm
        // reads `promote_cold_if_present`'s own return value instead of
        // re-issuing `contains_key`.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live && !self.settle_not_live(key, now_ms, state) {
            self.insert_fresh(key, Entry::new_hash_listpack());
        }
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
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
                *entry.value.as_redis_value_mut().unwrap() = RedisValue::Hash(Box::new(map));
            }
            _ => {}
        }
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
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
        // P0 fix: `settle_not_live` promotes a cold-spilled list before this
        // fabricates an empty listpack — a promoted value always decodes as
        // `RedisValue::List` (cold storage never persists the listpack
        // compact encoding), so it naturally falls into the `Ok(None)` "not a
        // listpack, fall through" arm below, which already routes callers
        // (e.g. LPUSH) to `get_or_create_list` — no fabrication.
        //
        // moon#942: ONE lookup decides the preamble — `hot_state` replaces
        // the `drop_if_expired` + `contains_key` pair, and the create arm
        // reads `promote_cold_if_present`'s own return value instead of
        // re-issuing `contains_key`.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live && !self.settle_not_live(key, now_ms, state) {
            self.insert_fresh(key, Entry::new_list_listpack());
        }
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
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
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::List(list)) => list,
            _ => unreachable!("upgrade_list_listpack_to_list: expected List after upgrade"),
        }
    }

    /// Get or create a set entry as a listpack. Creates new keys as `SetListpack`.
    ///
    /// Returns `Ok(Some(&mut Listpack))` when the key is a `SetListpack`,
    /// `Ok(None)` when it already holds the full `IndexSet` form or a
    /// `SetIntset` (the caller falls through to the intset path or to
    /// `get_or_create_set`), and `Err(WRONGTYPE)` for a non-set type.
    ///
    /// This is the accessor `SADD` was missing (moon#787). `SetListpack` was
    /// wired end to end — `Entry::new_set_listpack`, the value codec, the RDB
    /// and AOF writers, and `SetKind::classify_hot`'s `SetRef::Listpack` arm
    /// all handle it — but nothing ever produced one that survived, because
    /// `get_or_create_set` calls `SetKind::upgrade`, which converts a listpack
    /// to the full form unconditionally in the same call. So every string set
    /// was a hashtable from birth, where Redis keeps one in a listpack up to
    /// `set-max-listpack-entries` / `set-max-listpack-value`.
    ///
    /// Mirrors `get_or_create_hash_listpack` exactly, including the cold-tier
    /// rule: cold storage never persists a compact encoding, so a promoted
    /// value always decodes as `RedisValue::Set` and lands in the `Ok(None)`
    /// arm rather than being fabricated over.
    ///
    /// An existing `SetIntset` takes the `intset -> listpack` edge (moon#899)
    /// when `absorb_intset(members, widest_rendering)` says the result still
    /// fits the listpack policy — the edge Redis 7.2+ has and moon lacked, so
    /// `SADD s 1 2 3` then `SADD s abc` went straight to a hashtable where
    /// redis answers `listpack`. The caller decides with the authority's
    /// predicate (`EncodingLimits::fits(Shape::Set, members + 1, ..)`); the
    /// accessor supplies the two facts only it can see cheaply: the member
    /// count, and the rendered width of the widest integer (an intset stores
    /// `i64`s, a listpack stores their decimal spelling, and the value
    /// threshold applies to the spelling). Refused, the intset answers
    /// `Ok(None)` and the caller promotes it to the full form as before.
    ///
    /// Byte transparency across the edge (moon#795): every value in an intset
    /// arrived through `numeric::canonical_i64`, so its `itoa` rendering is
    /// the exact bytes the client sent; a listpack, for its part, integer-
    /// encodes only canonical spellings. `SMEMBERS` after the conversion
    /// returns what was written, and `SISMEMBER +5` still answers 0 against a
    /// stored `5`.
    ///
    /// SELF-ACCOUNTING for the conversion: the intset -> listpack cost swing
    /// is applied to `used_memory` here, so the caller's own before/after
    /// snapshot of the listpack starts from the converted form.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_set_listpack(
        &mut self,
        key: &[u8],
        absorb_intset: impl FnOnce(usize, usize) -> bool,
    ) -> Result<Option<&mut crate::storage::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        // moon#942: ONE lookup decides the preamble. `settle_not_live` still
        // promotes a cold-spilled set before this fabricates an empty
        // listpack, so a promoted value lands in the `Ok(None)` arm below
        // rather than being fabricated over.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live && !self.settle_not_live(key, now_ms, state) {
            self.insert_fresh(key, Entry::new_set_listpack());
        }
        self.absorb_intset_into_listpack(key, absorb_intset);
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SetListpack(lp)) => Ok(Some(lp)),
            Some(RedisValue::Set(_)) | Some(RedisValue::SetIntset(_)) => Ok(None),
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// The `intset -> listpack` edge (moon#899), in place, for a key that
    /// holds a `SetIntset` and whose `absorb(members, widest_rendering)`
    /// answers true; a no-op for anything else. Renders every `i64` with
    /// `itoa` (canonical, so byte-exact — see `get_or_create_set_listpack`)
    /// and applies the cost swing to `used_memory`.
    fn absorb_intset_into_listpack(
        &mut self,
        key: &[u8],
        absorb: impl FnOnce(usize, usize) -> bool,
    ) {
        let Some(entry) = self.data.get_mut(key) else {
            return;
        };
        let Some(RedisValue::SetIntset(is)) = entry.value.as_redis_value_mut() else {
            return;
        };
        if !absorb(is.len(), widest_rendering(is)) {
            return;
        }
        let before = is.estimate_memory();
        let mut lp = crate::storage::listpack::Listpack::new();
        let mut buf = itoa::Buffer::new();
        for v in is.iter() {
            lp.push_back(buf.format(v).as_bytes());
        }
        let after = lp.estimate_memory();
        if let Some(slot) = entry.value.as_redis_value_mut() {
            *slot = RedisValue::SetListpack(lp);
        }
        // Disjoint field borrows: `entry` borrows `self.data`, the ledger is
        // a separate field (same shape as `get_or_create`).
        self.used_memory = self
            .used_memory
            .saturating_add(after)
            .saturating_sub(before);
    }

    /// Upgrade a `SetListpack` to the full `IndexSet` form in place, returning
    /// a mutable ref to it.
    ///
    /// Unlike `upgrade_intset_to_set` and `upgrade_hash_listpack_to_hash`,
    /// which leave the memory swing to the caller, this accessor is
    /// SELF-ACCOUNTING: it delegates to `SetKind::upgrade` — the one
    /// conversion `get_or_create` and `get_promoted` also run — and applies
    /// the delta it returns to `used_memory` here. The listpack -> `IndexSet`
    /// cost swing therefore has exactly one implementation, billing the
    /// entries `Vec` and the index table from their real capacity
    /// (`set_table_bytes`, moon#788/#810); a caller cannot reach for the
    /// retired per-member model by mistake.
    ///
    /// Infallible for a key that exists and holds a set; a key that already
    /// holds the full form is returned unchanged with a zero delta, which is
    /// what makes this safe to call unconditionally from the promotion branch.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is a set; upgrade is infallible
    pub fn upgrade_set_listpack_to_set(
        &mut self,
        key: &[u8],
    ) -> &mut crate::storage::entry::SetValue {
        let entry = self.data.get_mut(key).unwrap();
        let encoding_delta = db_kind::SetKind::upgrade(entry);
        self.used_memory = self.used_memory.saturating_add_signed(encoding_delta);
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::Set(set)) => set,
            _ => unreachable!("upgrade_set_listpack_to_set: expected Set after upgrade"),
        }
    }

    /// Get or create a sorted set entry as a listpack. Creates new keys as
    /// `SortedSetListpack`.
    ///
    /// Returns `Ok(Some(&mut Listpack))` when the key is a `SortedSetListpack`,
    /// `Ok(None)` when it already holds the full `SortedSetBPTree` (or the
    /// legacy `SortedSet`) form — the caller falls through to
    /// `get_or_create_sorted_set` — and `Err(WRONGTYPE)` for a non-zset type.
    ///
    /// This is the accessor `ZADD` was missing (moon#787). `SortedSetListpack`
    /// was wired end to end — `Entry::new_sorted_set_listpack`, the value
    /// codec, the RDB/AOF writers, `DEBUG DIGEST`, and
    /// `SortedSetKind::classify_hot`'s `SortedSetRef::Listpack` arm all handle
    /// it — but nothing ever produced one, so every zset was a `skiplist` from
    /// its first member where Redis keeps one in a listpack up to
    /// `zset-max-listpack-entries` (128) / `zset-max-listpack-value` (64).
    ///
    /// Mirrors `get_or_create_hash_listpack` exactly, including the cold-tier
    /// rule: cold storage never persists a compact encoding, so a promoted
    /// value always decodes as `RedisValue::SortedSetBPTree` and lands in the
    /// `Ok(None)` arm rather than being fabricated over.
    #[allow(clippy::unwrap_used)] // get_mut() after insert guarantees key present
    pub fn get_or_create_zset_listpack(
        &mut self,
        key: &[u8],
    ) -> Result<Option<&mut crate::storage::listpack::Listpack>, Frame> {
        let now_ms = self.cached_now_ms;
        // moon#942: ONE lookup decides the preamble. `settle_not_live` still
        // promotes a cold-spilled zset before this fabricates an empty
        // listpack, so a promoted value lands in the `Ok(None)` arm below
        // rather than being fabricated over.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live && !self.settle_not_live(key, now_ms, state) {
            self.insert_fresh(key, Entry::new_sorted_set_listpack());
        }
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SortedSetListpack(lp)) => Ok(Some(lp)),
            Some(RedisValue::SortedSetBPTree { .. }) | Some(RedisValue::SortedSet { .. }) => {
                Ok(None)
            }
            _ => Err(Self::wrongtype_error()),
        }
    }

    /// Upgrade a `SortedSetListpack` to the full `SortedSetBPTree` form in
    /// place, returning mutable refs to both inner structures.
    ///
    /// SELF-ACCOUNTING, unlike the older `upgrade_*` siblings: it delegates to
    /// `SortedSetKind::upgrade` — the one conversion `get_or_create` and
    /// `get_promoted` also run — and applies the delta it returns to
    /// `used_memory` here. The listpack -> B+tree swing therefore has exactly
    /// one implementation, billing the arena and the `members` table from
    /// their real capacity (`zset_table_bytes`, moon#788/#810); a caller
    /// cannot reach for the retired per-member model by mistake.
    ///
    /// Infallible for a key that exists and holds a zset; a key that already
    /// holds the full form is returned unchanged with a zero delta.
    #[allow(clippy::unwrap_used)] // caller guarantees key exists and is a zset; upgrade is infallible
    pub fn upgrade_zset_listpack_to_bptree(
        &mut self,
        key: &[u8],
    ) -> (&mut HashMap<Bytes, f64>, &mut BPTree) {
        let entry = self.data.get_mut(key).unwrap();
        let encoding_delta = db_kind::SortedSetKind::upgrade(entry);
        self.used_memory = self.used_memory.saturating_add_signed(encoding_delta);
        let entry = self.data.get_mut(key).unwrap();
        // moon#926 — see `stamp_mutation`.
        stamp_mutation(entry);
        match entry.value.as_redis_value_mut() {
            Some(RedisValue::SortedSetBPTree { members, tree }) => (members, tree),
            _ => unreachable!("upgrade_zset_listpack_to_bptree: expected BPTree after upgrade"),
        }
    }

    /// Get or create a sorted set entry. Returns mutable refs to both inner structures.
    ///
    /// New keys start with SortedSetBPTree encoding. Legacy SortedSet (BTreeMap)
    /// and — since moon#787 — `SortedSetListpack` entries are upgraded to
    /// SortedSetBPTree on access.
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
        // moon#541: read the deadline index (deadline order) instead of
        // scanning the whole data map — O(volatile) instead of O(N).
        self.expiry_index.iter().map(|(_, k)| k.clone()).collect()
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
    ///
    /// **HOT PLANE ONLY.** A key that eviction spilled to the cold tier is
    /// absent from `data` and this returns `None` for it — indistinguishable
    /// from "no such key". Any command that would answer differently for a
    /// tiered key than for a missing one must use
    /// [`Self::get_if_alive_any_plane`] instead (moon#610).
    pub fn get_if_alive(&self, key: &[u8], now_ms: u64) -> Option<&Entry> {
        let entry = self.data.get(key)?;
        if entry.is_expired_at(now_ms) {
            return None;
        }
        Some(entry)
    }

    /// [`Self::get_if_alive`], but ALSO consulting the cold tier (moon#610).
    ///
    /// `get_if_alive` probes only the hot plane, so every read-only command
    /// built on it answered as though a TIERED key did not exist: measured on
    /// one instance with identical values and TTLs, `STRLEN` said 0, `TYPE`
    /// said `none`, `TTL` said -2 and `MGET` said nil for a key `EXISTS`
    /// answered 1 for and `GET` served in full. `0` and `-2` are exactly what
    /// a MISSING key returns, so nothing let a client tell the two apart.
    ///
    /// Only `get_readonly` had a cold fallback, hand-written at its own call
    /// site; the other twenty-five call sites did not, and nothing failed.
    /// That is why the fallback belongs to the accessor: a per-call-site one
    /// is how this drifted in the first place.
    ///
    /// The cold entry is materialised, NOT promoted — `&self` cannot mutate
    /// the hot plane, and promotion here would also make a read path
    /// allocate into the keyspace. The same choice `get_ref_if_alive`
    /// already makes for typed collection access.
    ///
    /// WARNING: inherits [`Self::get_cold_value`]'s synchronous disk read on
    /// a cold hit. Callers on the shard event loop must have released any
    /// shard guard first — identical to the existing typed-access path.
    pub fn get_if_alive_any_plane(&self, key: &[u8], now_ms: u64) -> Option<EntryView<'_>> {
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired_at(now_ms) {
                return None;
            }
            return Some(EntryView::Hot(entry));
        }
        // The in-flight spill plane FIRST (#459): a key mid-spill has left
        // hot RAM but has no `cold_index` entry yet, and this is the only
        // accessor that hands it back as a whole `Entry` — deadline included.
        // Reaching it through `get_cold_value` instead yields the value with
        // the TTL dropped, so `TTL` answers -1 ("no expiry") for a key written
        // with `EX`: a different wrong answer, not a fix. Measured, not feared.
        if let Some(entry) = self.spill_inflight_entry(key, now_ms) {
            return Some(EntryView::Cold(entry));
        }
        let value = self.get_cold_value(key, now_ms)?;
        let mut entry = Entry::new_string(bytes::Bytes::new());
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(value);
        // Settled in the cold tier: the deadline rides in the cold index, so
        // restoring it costs no extra disk read.
        if let Some((loc, _)) = self.cold_lookup_location(key) {
            if let Some(ttl) = loc.ttl_ms {
                entry.set_expires_at_ms(ttl);
            }
        }
        Some(EntryView::Cold(entry))
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
        // moon#902: an AOF-authority replay may not read a value from a cold
        // file the log has not cut yet (see `cold_replay_gate`). Outside
        // replay this is the same lookup `cold_read_through` repeats.
        if self.replay_cold_gate_active() {
            self.cold_location_visible(key)?;
        }
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
        // moon#902: replay-gated (see `cold_replay_gate`); a plain lookup
        // outside replay.
        let location = self.cold_location_visible(key)?;
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
    ///
    /// moon#523/#539: the lookup is deliberately NON-creating. This helper
    /// backs the blocking fast path (`try_immediate_pop` → BLPOP/BLMOVE/…),
    /// so a `get_or_create_list` here materialised an empty list on every
    /// miss — a phantom key that EXISTS/TYPE/DBSIZE reported, that a later
    /// RPUSH rejected with WRONGTYPE, and that no path ever removed.
    pub fn list_pop_front(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_mut_if_present::<db_kind::ListKind>(key).ok()??;
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
    ///
    /// Non-creating on a missing key — see [`Self::list_pop_front`].
    pub fn list_pop_back(&mut self, key: &[u8]) -> Option<Bytes> {
        let list = self.get_mut_if_present::<db_kind::ListKind>(key).ok()??;
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
    ///
    /// Non-creating on a missing key — see [`Self::list_pop_front`].
    pub fn zset_pop_min(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self
            .get_mut_if_present::<db_kind::SortedSetKind>(key)
            .ok()??;
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
    ///
    /// Non-creating on a missing key — see [`Self::list_pop_front`].
    pub fn zset_pop_max(&mut self, key: &[u8]) -> Option<(Bytes, f64)> {
        let (members, tree) = self
            .get_mut_if_present::<db_kind::SortedSetKind>(key)
            .ok()??;
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
        // moon#942: one lookup for the preamble, one to hand the stream out.
        let state = self.hot_state(key, now_ms);
        if state != HotState::Live {
            self.settle_not_live(key, now_ms, state);
        }
        match self.data.get_mut(key) {
            None => Ok(None),
            Some(entry) => {
                // moon#926 — see `stamp_mutation`. A miss stamps nothing.
                stamp_mutation(entry);
                match entry.value.as_redis_value_mut() {
                    Some(RedisValue::Stream(s)) => Ok(Some(s.as_mut())),
                    Some(_) => Err(Self::wrongtype_error()),
                    None => Err(Self::wrongtype_error()),
                }
            }
        }
    }
}
