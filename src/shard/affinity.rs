//! Pub/sub connection affinity tracker.
//!
//! Remembers which shard a client IP last subscribed on, enabling
//! affinity-based routing for reconnections. Uses a bounded HashMap
//! with LRU-style eviction to prevent unbounded memory growth.

use std::collections::HashMap;
use std::net::IpAddr;

/// Maximum number of IP entries before eviction.
const MAX_ENTRIES: usize = 16384;

/// Tracks which shard each client IP last had pub/sub subscriptions on.
///
/// Thread-safe: wrapped in `Arc<RwLock<>>` at the server level.
/// Read contention is minimal (one read per new connection accept).
/// Write contention is minimal (one write per SUBSCRIBE, one per disconnect).
pub struct AffinityTracker {
    /// IP -> (shard_id, last_activity_counter)
    entries: HashMap<IpAddr, (usize, u64)>,
    /// Monotonic counter for LRU ordering
    counter: u64,
    /// Maximum entries before eviction
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

    /// Register that `ip` has active subscriptions on `shard_id`.
    /// Called when a connection enters subscriber mode (first SUBSCRIBE).
    pub fn register(&mut self, ip: IpAddr, shard_id: usize) {
        self.counter += 1;
        self.entries.insert(ip, (shard_id, self.counter));
        self.maybe_evict();
    }

    /// Look up the preferred shard for a client IP.
    /// Returns None if no affinity is recorded.
    pub fn lookup(&self, ip: &IpAddr) -> Option<usize> {
        self.entries.get(ip).map(|(shard_id, _)| *shard_id)
    }

    /// Remove affinity for a client IP (called on disconnect if no subscriptions remain).
    pub fn remove(&mut self, ip: &IpAddr) {
        self.entries.remove(ip);
    }

    /// Evict oldest entries if over capacity.
    fn maybe_evict(&mut self) {
        if self.entries.len() <= self.max_entries {
            return;
        }
        // Find the 25th percentile counter value and remove entries below it
        let target = self.max_entries * 3 / 4;
        let mut counters: Vec<u64> = self.entries.values().map(|(_, c)| *c).collect();
        counters.sort_unstable();
        if let Some(&threshold) = counters.get(self.entries.len() - target) {
            self.entries.retain(|_, (_, c)| *c > threshold);
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
    use std::net::IpAddr;

    #[test]
    fn test_register_and_lookup() {
        let mut tracker = AffinityTracker::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        tracker.register(ip, 3);
        assert_eq!(tracker.lookup(&ip), Some(3));
    }

    #[test]
    fn test_lookup_missing() {
        let tracker = AffinityTracker::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert_eq!(tracker.lookup(&ip), None);
    }

    #[test]
    fn test_remove() {
        let mut tracker = AffinityTracker::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        tracker.register(ip, 2);
        tracker.remove(&ip);
        assert_eq!(tracker.lookup(&ip), None);
    }

    #[test]
    fn test_overwrite_affinity() {
        let mut tracker = AffinityTracker::new();
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        tracker.register(ip, 1);
        tracker.register(ip, 5);
        assert_eq!(tracker.lookup(&ip), Some(5));
    }
}
