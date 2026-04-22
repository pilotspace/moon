//! IP-level connection affinity tracker.
//!
//! Stores two kinds of shard hints per client IP:
//!
//! * **key_shard** — populated when a per-connection `AffinityTracker`
//!   (`src/server/conn/affinity.rs`) converges on a remote shard (≥10/16 ops
//!   target the same foreign shard) and the connection is migrated. This
//!   captures storage locality: the shard that OWNS most of this client's
//!   keyspace.
//! * **pubsub_shard** — populated on `SUBSCRIBE` / `PSUBSCRIBE`. This captures
//!   fan-out locality: the shard that already holds the client's subscriber
//!   registrations.
//!
//! The listener consults `lookup()` for every new accept; it returns the key
//! shard if present, falling back to the pub/sub shard. A single entry can
//! carry both hints when a client both subscribes and does key access; they
//! don't overwrite each other, and the storage hint wins (reconnection
//! lands on the data shard, re-`SUBSCRIBE` then routes fan-out back over
//! SPSC — cheap compared to mis-placing the key hot path).
//!
//! Uses a bounded `HashMap` with LRU-style eviction to prevent unbounded
//! memory growth. Thread-safe via `Arc<RwLock<>>` at the server level.

use std::collections::HashMap;
use std::net::IpAddr;

/// Maximum number of IP entries before eviction.
const MAX_ENTRIES: usize = 16384;

#[derive(Debug, Clone, Copy)]
struct Entry {
    /// Shard that owns most of this IP's keyspace, if the per-connection
    /// sampler converged on one. Preferred over `pubsub_shard` at lookup.
    key_shard: Option<usize>,
    /// Shard where this IP's active `SUBSCRIBE` landed, if any.
    pubsub_shard: Option<usize>,
    /// Monotonic counter for LRU-style eviction.
    lru: u64,
}

/// Tracks preferred shards per client IP for listener-side connection placement.
pub struct AffinityTracker {
    entries: HashMap<IpAddr, Entry>,
    /// Monotonic counter — bumped on every register, used as LRU stamp.
    counter: u64,
    max_entries: usize,
}

impl AffinityTracker {
    pub fn new() -> Self {
        Self {
            entries: HashMap::with_capacity(1024),
            counter: 0,
            max_entries: MAX_ENTRIES,
        }
    }

    /// Record a pub/sub hint: `ip` has active subscriptions on `shard_id`.
    /// Called when a connection enters subscriber mode (first `SUBSCRIBE`).
    pub fn register_pubsub(&mut self, ip: IpAddr, shard_id: usize) {
        self.counter += 1;
        let lru = self.counter;
        self.entries
            .entry(ip)
            .and_modify(|e| {
                e.pubsub_shard = Some(shard_id);
                e.lru = lru;
            })
            .or_insert(Entry {
                key_shard: None,
                pubsub_shard: Some(shard_id),
                lru,
            });
        self.maybe_evict();
    }

    /// Record a key-access hint: this IP's per-connection sampler converged
    /// on `shard_id` as the storage-locality target. Called from the
    /// migration-decision path in the sharded/monoio handlers.
    pub fn register_key(&mut self, ip: IpAddr, shard_id: usize) {
        self.counter += 1;
        let lru = self.counter;
        self.entries
            .entry(ip)
            .and_modify(|e| {
                e.key_shard = Some(shard_id);
                e.lru = lru;
            })
            .or_insert(Entry {
                key_shard: Some(shard_id),
                pubsub_shard: None,
                lru,
            });
        self.maybe_evict();
    }

    /// Look up the preferred shard for `ip`. Prefers the key-access hint
    /// (storage locality) over the pub/sub hint (fan-out locality).
    pub fn lookup(&self, ip: &IpAddr) -> Option<usize> {
        let e = self.entries.get(ip)?;
        e.key_shard.or(e.pubsub_shard)
    }

    /// Return only the pub/sub hint for `ip` (unused by the listener but kept
    /// for symmetry / introspection).
    #[allow(dead_code)]
    pub fn lookup_pubsub(&self, ip: &IpAddr) -> Option<usize> {
        self.entries.get(ip).and_then(|e| e.pubsub_shard)
    }

    /// Clear the pub/sub hint on disconnect when no subscriptions remain. Do
    /// not touch the key hint — key locality persists across reconnects.
    /// If both hints are now empty the entry is removed.
    pub fn remove_pubsub(&mut self, ip: &IpAddr) {
        if let Some(e) = self.entries.get_mut(ip) {
            e.pubsub_shard = None;
            if e.key_shard.is_none() {
                self.entries.remove(ip);
            }
        }
    }

    /// Legacy API — `SUBSCRIBE` register path used to call `register()`.
    /// Preserved as an alias for `register_pubsub()`.
    #[allow(dead_code)]
    pub fn register(&mut self, ip: IpAddr, shard_id: usize) {
        self.register_pubsub(ip, shard_id);
    }

    /// Legacy API — disconnect path used to call `remove()`.
    /// Preserved as an alias for `remove_pubsub()`.
    #[allow(dead_code)]
    pub fn remove(&mut self, ip: &IpAddr) {
        self.remove_pubsub(ip);
    }

    /// Evict oldest entries when over capacity.
    fn maybe_evict(&mut self) {
        if self.entries.len() <= self.max_entries {
            return;
        }
        let target = self.max_entries * 3 / 4;
        let mut lrus: Vec<u64> = self.entries.values().map(|e| e.lru).collect();
        lrus.sort_unstable();
        if let Some(&threshold) = lrus.get(self.entries.len() - target) {
            self.entries.retain(|_, e| e.lru > threshold);
        }
    }
}

impl Default for AffinityTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn pubsub_register_and_lookup() {
        let mut t = AffinityTracker::new();
        t.register_pubsub(ip("192.168.1.1"), 3);
        assert_eq!(t.lookup(&ip("192.168.1.1")), Some(3));
    }

    #[test]
    fn missing_lookup_is_none() {
        let t = AffinityTracker::new();
        assert_eq!(t.lookup(&ip("10.0.0.1")), None);
    }

    #[test]
    fn key_hint_wins_over_pubsub_hint() {
        let mut t = AffinityTracker::new();
        let addr = ip("10.0.0.5");
        t.register_pubsub(addr, 1);
        t.register_key(addr, 3);
        assert_eq!(t.lookup(&addr), Some(3));
        // pub/sub hint still reachable for introspection
        assert_eq!(t.lookup_pubsub(&addr), Some(1));
    }

    #[test]
    fn key_hint_stored_even_without_pubsub() {
        let mut t = AffinityTracker::new();
        let addr = ip("10.0.0.6");
        t.register_key(addr, 7);
        assert_eq!(t.lookup(&addr), Some(7));
        assert_eq!(t.lookup_pubsub(&addr), None);
    }

    #[test]
    fn remove_pubsub_preserves_key_hint() {
        let mut t = AffinityTracker::new();
        let addr = ip("10.0.0.7");
        t.register_key(addr, 4);
        t.register_pubsub(addr, 2);
        t.remove_pubsub(&addr);
        // Key hint must survive pub/sub disconnect.
        assert_eq!(t.lookup(&addr), Some(4));
        assert_eq!(t.lookup_pubsub(&addr), None);
    }

    #[test]
    fn remove_pubsub_drops_entry_when_no_key_hint() {
        let mut t = AffinityTracker::new();
        let addr = ip("10.0.0.8");
        t.register_pubsub(addr, 5);
        t.remove_pubsub(&addr);
        assert_eq!(t.lookup(&addr), None);
    }

    #[test]
    fn overwrite_pubsub_affinity() {
        let mut t = AffinityTracker::new();
        let addr = ip("192.168.1.1");
        t.register_pubsub(addr, 1);
        t.register_pubsub(addr, 5);
        assert_eq!(t.lookup(&addr), Some(5));
    }

    #[test]
    fn legacy_register_and_remove_still_work() {
        let mut t = AffinityTracker::new();
        let addr = ip("192.168.1.42");
        t.register(addr, 2);
        assert_eq!(t.lookup(&addr), Some(2));
        t.remove(&addr);
        assert_eq!(t.lookup(&addr), None);
    }
}
