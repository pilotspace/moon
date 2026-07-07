//! Cypher result cache: caches pre-encoded RESP reply bytes for read-only
//! `GRAPH.QUERY` / `GRAPH.RO_QUERY` results.
//!
//! # Design summary (Task #32 design doc, Part A)
//!
//! - Keyed by `(raw query-bytes hash, remaining-args hash)` — mirrors
//!   `PlanCache`'s raw-hash pre-lookup (`planner::hash_query`), so
//!   literal-per-query text (e.g. the bench's `{id: <i>}` point queries)
//!   naturally gets distinct entries without a separate normalization pass.
//! - Invalidated by `NamedGraph::write_gen`, a per-graph monotonic counter
//!   bumped at every mutation site that changes query-visible state (see
//!   `NamedGraph::touch`). A cache hit whose `write_gen_at_insert` doesn't
//!   match the graph's current `write_gen` is treated as a miss — the stale
//!   entry is left in place and naturally overwritten by the next `put()`
//!   for that key (same lazy-eviction philosophy as `PlanCache`).
//! - Stores wire bytes (`bytes::Bytes`), not a `Frame` tree: `FrameVec` is a
//!   *boxed* `SmallVec` (`src/protocol/frame.rs`), so cloning a cached
//!   `Frame::Array` re-allocates the box/smallvec on every hit. Caching the
//!   RESP-encoded `Bytes` instead makes a hit an O(1) atomic refcount bump
//!   (`Bytes::clone()`), replayed via the pre-existing `Frame::PreSerialized`
//!   passthrough variant (already used by the GET fast path) — no new
//!   protocol-layer variant needed.
//! - RESP2 and RESP3 clients must never share a slot: `encode_frame`
//!   branches on `protocol_version` (arrays/maps/booleans/doubles serialize
//!   differently). Each entry carries two independently-populated slots.
//!   A slot is populated lazily, from whichever protocol version first
//!   `put()`s that key — the OTHER slot for an already-cached key is simply
//!   a cache miss (not an auto re-encode: this cache never retains the
//!   source `ExecResult`, only the wire bytes, so there is nothing to
//!   re-encode from). This is a deliberate simplification vs. a design that
//!   would keep the `ExecResult` alive to serve either protocol from one
//!   `put` — it trades a slightly lower hit rate under mixed RESP2/RESP3
//!   traffic against the *same* query text for a strictly smaller resident
//!   footprint (exactly the trade-off caching wire bytes over Frame trees
//!   is for in the first place). A miss is always safe: the caller
//!   re-executes and re-populates.
//!
//! # NOT covered by this cache (deliberately -- see call sites)
//!
//! - `GRAPH.EXPLAIN` / `GRAPH.PROFILE` never consult or populate it (they
//!   already bypass `PlanCache` for the same reason: stable-output debug
//!   surfaces, not a query-result semantics to cache).
//! - `--decay` queries are wall-clock-dependent (`TemporalDecayScorer::now`)
//!   and are never cached regardless of `write_gen` — two decay queries at
//!   different real times against an *unchanged* graph legitimately differ.
//! - Only `Ok` executions are cached; a `TIMEOUT`-truncated or any other
//!   `Err` result never reaches `put()` — gating on `Result::Ok` alone
//!   excludes every truncated-result class without a separate flag.

use bytes::Bytes;
use std::sync::OnceLock;

use crate::graph::fasthash::FxHashMap;

/// Cache key: raw Cypher query-bytes hash + remaining-args hash.
///
/// `query_hash` mirrors `planner::hash_query` applied to the exact client
/// query bytes (not the literal-normalized text) — every distinct literal
/// value naturally gets its own key, which is exactly what a *result* cache
/// wants (a plan cache wants to SHARE across literals; a result cache must
/// NOT, since the results differ).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResultCacheKey {
    pub query_hash: u64,
    pub args_hash: u64,
}

/// One cached query result: up to two lazily-populated wire-byte slots
/// (RESP2 / RESP3), the write-generation this entry was computed against,
/// and an LRU access tick.
struct ResultCacheEntry {
    resp2: OnceLock<Bytes>,
    resp3: OnceLock<Bytes>,
    write_gen_at_insert: u64,
    used: u64,
}

impl ResultCacheEntry {
    fn resident_bytes(&self) -> usize {
        self.resp2.get().map_or(0, Bytes::len) + self.resp3.get().map_or(0, Bytes::len)
    }
}

/// Bounded, per-graph LRU cache of pre-encoded Cypher query reply bytes.
///
/// Wrapped in `parking_lot::Mutex` on `NamedGraph` (same interior-mutability
/// pattern as `plan_cache`) so read handlers reachable through an immutable
/// `&NamedGraph` borrow (`graph_query_readonly` never takes `&mut
/// GraphStore`) can still record hits/insert entries.
pub struct ResultCache {
    entries: FxHashMap<ResultCacheKey, ResultCacheEntry>,
    max_entries: usize,
    max_bytes: usize,
    cur_bytes: usize,
    /// Monotonic access counter backing LRU eviction (mirrors `PlanCache`).
    tick: u64,
}

impl ResultCache {
    /// Create a new result cache. `max_entries` bounds distinct
    /// `(query, args)` keys; `max_bytes` bounds the sum of resident RESP2 +
    /// RESP3 wire bytes across all entries (a soft cap -- enforced after
    /// each `put`, so a single oversized insert can transiently exceed it
    /// by that one entry's size before eviction catches up).
    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: FxHashMap::default(),
            max_entries,
            max_bytes,
            cur_bytes: 0,
            tick: 0,
        }
    }

    /// Look up a cached reply for `key` at the caller's negotiated
    /// `protocol_version` (2 or 3; anything `>= 3` is treated as RESP3).
    ///
    /// Returns `None` when: no entry exists, the entry is stale (its
    /// `write_gen_at_insert` no longer matches `current_write_gen` --
    /// left in place, not evicted eagerly, exactly like a `PlanCache` miss),
    /// or the requested protocol's slot hasn't been populated yet for this
    /// key. Every `None` path is a safe, ordinary cache miss -- callers
    /// re-execute and `put()` the fresh result.
    pub fn get(
        &mut self,
        key: ResultCacheKey,
        current_write_gen: u64,
        protocol_version: u8,
    ) -> Option<Bytes> {
        self.tick += 1;
        let tick = self.tick;
        let entry = self.entries.get_mut(&key)?;
        if entry.write_gen_at_insert != current_write_gen {
            return None;
        }
        entry.used = tick;
        let slot = if protocol_version >= 3 {
            &entry.resp3
        } else {
            &entry.resp2
        };
        slot.get().cloned()
    }

    /// Insert (or update) the wire-encoded reply for `key` at
    /// `protocol_version`, stamped with `write_gen` (the graph's write
    /// generation *captured before the query ran* -- see `NamedGraph::touch`
    /// docs for why this ordering matters for the race guard).
    ///
    /// A key whose existing entry is stale (`write_gen_at_insert` doesn't
    /// match `write_gen`) is replaced wholesale -- both protocol slots reset
    /// -- rather than patched in place: a stale RESP2 slot must never survive
    /// next to a freshly-inserted RESP3 slot under the same key.
    pub fn put(&mut self, key: ResultCacheKey, write_gen: u64, protocol_version: u8, bytes: Bytes) {
        self.tick += 1;
        let tick = self.tick;

        if let Some(existing) = self.entries.get(&key) {
            if existing.write_gen_at_insert != write_gen {
                self.cur_bytes -= existing.resident_bytes();
                self.entries.remove(&key);
            }
        }

        if !self.entries.contains_key(&key) && self.entries.len() >= self.max_entries {
            self.evict_lru();
        }

        let entry = self.entries.entry(key).or_insert_with(|| ResultCacheEntry {
            resp2: OnceLock::new(),
            resp3: OnceLock::new(),
            write_gen_at_insert: write_gen,
            used: tick,
        });
        entry.used = tick;
        entry.write_gen_at_insert = write_gen;

        let len = bytes.len();
        let slot = if protocol_version >= 3 {
            &entry.resp3
        } else {
            &entry.resp2
        };
        // `set` only fails if the slot is already populated (a second
        // `put()` for the same key+protocol before invalidation, e.g. a
        // benign race between two concurrent identical queries on the same
        // shard thread -- can't happen cross-thread since NamedGraph is
        // shard-owned, but a single-threaded re-entrant `put` for the exact
        // same key is still a harmless no-op here).
        if slot.set(bytes).is_ok() {
            self.cur_bytes += len;
        }

        self.enforce_byte_budget();
    }

    /// Evict least-recently-used entries until `cur_bytes <= max_bytes`.
    /// Stops at one remaining entry so a single reply larger than
    /// `max_bytes` doesn't spin the cache empty on every insert.
    fn enforce_byte_budget(&mut self) {
        while self.cur_bytes > self.max_bytes && self.entries.len() > 1 {
            self.evict_lru();
        }
    }

    /// Remove the least-recently-used entry (by `used` tick), if any.
    fn evict_lru(&mut self) {
        let Some(lru_key) = self
            .entries
            .iter()
            .min_by_key(|(_, e)| e.used)
            .map(|(k, _)| *k)
        else {
            return;
        };
        if let Some(evicted) = self.entries.remove(&lru_key) {
            self.cur_bytes -= evicted.resident_bytes();
        }
    }

    /// Number of distinct `(query, args)` keys currently cached.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all cached entries (`FLUSHALL` / `FLUSHDB` / `GRAPH.DELETE`).
    pub fn clear(&mut self) {
        self.entries.clear();
        self.cur_bytes = 0;
    }

    /// Resident bytes: wire-byte payloads plus a per-entry key/bookkeeping
    /// estimate, so the shard's elastic memory budget sees this cache the
    /// same way it already sees `SegmentPropertyIndexes` / `GraphHnsw`
    /// (`csr/storage.rs::resident_bytes` doc comment -- an unaccounted
    /// lazily-built structure is exactly the class of bug those precedents
    /// fixed; this cache must not repeat it).
    pub fn resident_bytes(&self) -> usize {
        self.cur_bytes
            + self.entries.len()
                * (std::mem::size_of::<ResultCacheKey>() + std::mem::size_of::<ResultCacheEntry>())
    }
}

/// Default entry-count cap: modest by design (per-graph, not shard-wide) --
/// a handful of hundred distinct hot queries covers the skewed-access
/// production case the design doc identifies as the real justification
/// (A.5 "Case 2"), without the cache itself becoming a memory-budget risk
/// on graphs with many named indexes.
pub const DEFAULT_MAX_ENTRIES: usize = 256;

/// Default byte budget: ~4 MiB. A 1-hop/2-hop point-query reply is tens to
/// low-hundreds of bytes RESP-encoded, so this comfortably covers
/// `DEFAULT_MAX_ENTRIES` entries at both protocol versions with headroom.
pub const DEFAULT_MAX_BYTES: usize = 4 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    fn key(q: u64, a: u64) -> ResultCacheKey {
        ResultCacheKey {
            query_hash: q,
            args_hash: a,
        }
    }

    #[test]
    fn test_miss_on_empty_cache() {
        let mut cache = ResultCache::new(8, 1024);
        assert_eq!(cache.get(key(1, 1), 0, 2), None);
    }

    #[test]
    fn test_put_then_get_hit_same_protocol() {
        let mut cache = ResultCache::new(8, 1024);
        cache.put(key(1, 1), 5, 2, Bytes::from_static(b"*1\r\n:1\r\n"));
        assert_eq!(
            cache.get(key(1, 1), 5, 2),
            Some(Bytes::from_static(b"*1\r\n:1\r\n"))
        );
    }

    #[test]
    fn test_get_miss_on_write_gen_mismatch() {
        let mut cache = ResultCache::new(8, 1024);
        cache.put(key(1, 1), 5, 2, Bytes::from_static(b"data"));
        // Graph mutated since insert (write_gen advanced) -- stale, must miss.
        assert_eq!(cache.get(key(1, 1), 6, 2), None);
    }

    #[test]
    fn test_different_args_hash_is_different_key() {
        let mut cache = ResultCache::new(8, 1024);
        cache.put(key(1, 1), 0, 2, Bytes::from_static(b"a"));
        cache.put(key(1, 2), 0, 2, Bytes::from_static(b"b"));
        assert_eq!(cache.get(key(1, 1), 0, 2), Some(Bytes::from_static(b"a")));
        assert_eq!(cache.get(key(1, 2), 0, 2), Some(Bytes::from_static(b"b")));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_protocol_version_isolation() {
        let mut cache = ResultCache::new(8, 1024);
        cache.put(key(1, 1), 0, 2, Bytes::from_static(b"resp2-bytes"));
        // Same key, RESP3 slot not yet populated -- must miss, not
        // accidentally return the RESP2 bytes.
        assert_eq!(cache.get(key(1, 1), 0, 3), None);
        cache.put(key(1, 1), 0, 3, Bytes::from_static(b"resp3-bytes"));
        assert_eq!(
            cache.get(key(1, 1), 0, 2),
            Some(Bytes::from_static(b"resp2-bytes"))
        );
        assert_eq!(
            cache.get(key(1, 1), 0, 3),
            Some(Bytes::from_static(b"resp3-bytes"))
        );
    }

    #[test]
    fn test_stale_entry_replaced_resets_both_protocol_slots() {
        let mut cache = ResultCache::new(8, 1024);
        cache.put(key(1, 1), 0, 2, Bytes::from_static(b"old-resp2"));
        cache.put(key(1, 1), 0, 3, Bytes::from_static(b"old-resp3"));
        // Mutation advances write_gen; a fresh RESP2-only put must not leave
        // the OLD resp3 bytes reachable under the new generation.
        cache.put(key(1, 1), 1, 2, Bytes::from_static(b"new-resp2"));
        assert_eq!(
            cache.get(key(1, 1), 1, 2),
            Some(Bytes::from_static(b"new-resp2"))
        );
        assert_eq!(
            cache.get(key(1, 1), 1, 3),
            None,
            "stale resp3 slot must not survive a write_gen bump"
        );
    }

    #[test]
    fn test_lru_eviction_order() {
        let mut cache = ResultCache::new(2, usize::MAX);
        cache.put(key(1, 0), 0, 2, Bytes::from_static(b"a"));
        cache.put(key(2, 0), 0, 2, Bytes::from_static(b"b"));
        // Touch key(1,0) so key(2,0) becomes LRU.
        assert!(cache.get(key(1, 0), 0, 2).is_some());
        cache.put(key(3, 0), 0, 2, Bytes::from_static(b"c"));
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.get(key(2, 0), 0, 2),
            None,
            "key(2,0) was LRU and must have been evicted"
        );
        assert!(cache.get(key(1, 0), 0, 2).is_some());
        assert!(cache.get(key(3, 0), 0, 2).is_some());
    }

    #[test]
    fn test_byte_budget_eviction() {
        // `max_bytes` bounds the wire-payload total (`cur_bytes`); each
        // payload here is 100 bytes, so a 350-byte budget fits at most 3.
        let mut cache = ResultCache::new(usize::MAX, 350);
        let payload = Bytes::from(vec![0u8; 100]);
        for i in 0..5u64 {
            cache.put(key(i, 0), 0, 2, payload.clone());
        }
        assert!(
            cache.len() < 5,
            "byte budget must have evicted at least one entry before max_entries was hit"
        );
        // `resident_bytes()` additionally reports fixed per-entry
        // bookkeeping overhead (key + entry struct) on top of the raw wire
        // payload that `max_bytes` governs -- assert the payload-only
        // component (`len() * 100`) stays within budget, not the
        // overhead-inclusive total.
        assert!(
            cache.len() * 100 <= 350,
            "wire-payload bytes must be within budget: {} entries * 100",
            cache.len()
        );
    }

    #[test]
    fn test_resident_bytes_tracks_puts_and_clear() {
        let mut cache = ResultCache::new(8, 1024);
        assert_eq!(cache.resident_bytes(), 0);
        cache.put(key(1, 1), 0, 2, Bytes::from_static(b"12345"));
        assert!(cache.resident_bytes() >= 5);
        cache.clear();
        assert_eq!(cache.resident_bytes(), 0);
    }

    #[test]
    fn test_clear_empties_cache() {
        let mut cache = ResultCache::new(8, 1024);
        cache.put(key(1, 1), 0, 2, Bytes::from_static(b"x"));
        assert!(!cache.is_empty());
        cache.clear();
        assert!(cache.is_empty());
        assert_eq!(cache.get(key(1, 1), 0, 2), None);
    }

    #[test]
    fn test_single_oversized_entry_does_not_starve_cache_empty() {
        let mut cache = ResultCache::new(8, 10);
        cache.put(key(1, 1), 0, 2, Bytes::from(vec![0u8; 100]));
        // Budget is 10 bytes but the entry is 100 -- must still be retained
        // (the "stop at one remaining entry" guard), not evicted into an
        // infinite loop or a permanently-empty cache.
        assert_eq!(cache.len(), 1);
        assert!(cache.get(key(1, 1), 0, 2).is_some());
    }
}
