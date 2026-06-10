//! Per-shard hot-key detection: SpaceSaving top-K sketch.
//!
//! Each `Database` owns one `HotKeySketch`. Both dispatchers sample 1-in-64
//! keyed commands (`tick()` gates the sampling) and record the primary key
//! into the sketch. `HOTKEYS [COUNT n]` reads the top entries; in multi-shard
//! mode the coordinator merges per-shard results.
//!
//! Interior mutability: the read path (`dispatch_read`) holds only
//! `&Database` and may run concurrently from other shard threads via the
//! cross-shard fast path, so the tick counter is a relaxed atomic and the
//! slot table sits behind a `parking_lot::Mutex` acquired with `try_lock` —
//! a contended sample is simply dropped (sampling tolerates loss; the hot
//! path never blocks).
//!
//! SpaceSaving (Metwally et al. 2005): fixed K slots. A known key increments
//! its counter; an unknown key with free capacity takes a slot at count 1;
//! otherwise it REPLACES the minimum-count slot, inheriting `min + 1`. This
//! overestimates (never underestimates) frequencies and is guaranteed to
//! retain any key whose true frequency exceeds total/K.
//!
//! Memory: K=128 slots × ~40 B ≈ 5 KB per database — L1-resident, so the
//! linear scans below are a few ns. With 1-in-64 sampling the amortized
//! dispatch cost is well under 1% of a SET (A/B verified; 1-in-16 cost 3-5%
//! on uniform-random pipelined workloads).
//!
//! Kill switch: `MOON_NO_HOTKEYS=1` disables sampling at process start
//! (checked once; `tick()` then never fires).

use crate::storage::compact_key::CompactKey;
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};

/// Number of tracked slots. Keys hotter than total_samples/128 are guaranteed
/// to be retained; in practice the top ~dozen are what operators act on.
pub const HOTKEY_CAPACITY: usize = 128;

/// Sample 1 in 2^6 = 64 keyed commands. Chosen by A/B benchmark: at 1-in-16
/// a uniform-random key stream (sketch always full, every observe pays the
/// full O(K) scan) cost 3-5% pipelined throughput; 1-in-64 brings the
/// amortized cost under the 1% gate while a 100k op/s hot key still gets
/// ~1.5k samples/s — orders of magnitude above the detection threshold.
const SAMPLE_SHIFT_MASK: u32 = 0x3F;

/// Multiplier from sampled counts to approximate command counts.
pub const HOTKEY_SAMPLE_RATE: u64 = (SAMPLE_SHIFT_MASK as u64) + 1;

/// Default number of entries returned by `HOTKEYS` without COUNT.
pub const HOTKEY_DEFAULT_COUNT: usize = 10;

fn hotkeys_disabled() -> bool {
    static DISABLED: OnceLock<bool> = OnceLock::new();
    *DISABLED.get_or_init(|| std::env::var_os("MOON_NO_HOTKEYS").is_some_and(|v| v == "1"))
}

/// SpaceSaving top-K frequency sketch over sampled command keys.
pub struct HotKeySketch {
    /// Unsorted (key, estimated_count) slots; at most `HOTKEY_CAPACITY`.
    entries: Mutex<Vec<(CompactKey, u64)>>,
    /// Dispatch tick counter driving 1-in-64 sampling (relaxed — exact
    /// cadence under concurrency doesn't matter, only the ~1/64 rate).
    sample_tick: AtomicU32,
    /// Process-wide kill switch, latched at construction.
    enabled: bool,
}

impl Default for HotKeySketch {
    fn default() -> Self {
        Self::new()
    }
}

impl HotKeySketch {
    pub fn new() -> Self {
        Self {
            // Lazily grows toward HOTKEY_CAPACITY; avoids a 5 KB allocation
            // for the many short-lived Databases created in tests/tools.
            entries: Mutex::new(Vec::new()),
            sample_tick: AtomicU32::new(0),
            enabled: !hotkeys_disabled(),
        }
    }

    /// Advance the sampling counter; returns `true` when this command should
    /// be observed (1 in 64). One relaxed fetch_add — the only per-dispatch cost.
    #[inline]
    pub fn tick(&self) -> bool {
        let prev = self.sample_tick.fetch_add(1, Ordering::Relaxed);
        self.enabled && prev.wrapping_add(1) & SAMPLE_SHIFT_MASK == 0
    }

    /// Record one observation of `key` (SpaceSaving update). Single O(K)
    /// scan — membership check and min tracking share one pass (uniform
    /// random keys hit the worst case on EVERY sampled observe, so the scan
    /// count directly bounds the hot-path cost). Only reached on sampled
    /// ticks. Never blocks: a contended lock drops the sample.
    pub fn observe(&self, key: &[u8]) {
        let Some(mut entries) = self.entries.try_lock() else {
            return;
        };
        let mut min_idx = 0usize;
        let mut min_count = u64::MAX;
        for (i, entry) in entries.iter_mut().enumerate() {
            if entry.0.as_bytes() == key {
                entry.1 += 1;
                return;
            }
            if entry.1 < min_count {
                min_count = entry.1;
                min_idx = i;
            }
        }
        if entries.len() < HOTKEY_CAPACITY {
            entries.push((CompactKey::from(key), 1));
            return;
        }
        // Table full: replace the minimum-count slot, inheriting min + 1.
        entries[min_idx] = (CompactKey::from(key), min_count + 1);
    }

    /// Top `n` entries by estimated count, descending. Counts are sample
    /// counts (multiply by `HOTKEY_SAMPLE_RATE` for an approximate command
    /// rate).
    pub fn top(&self, n: usize) -> Vec<(Bytes, u64)> {
        let entries = self.entries.lock();
        let mut sorted: Vec<(Bytes, u64)> = entries
            .iter()
            .map(|(k, c)| (Bytes::copy_from_slice(k.as_bytes()), *c))
            .collect();
        drop(entries);
        sorted.sort_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        sorted.truncate(n);
        sorted
    }

    /// Number of tracked slots currently occupied.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }

    /// Drop all tracked entries (FLUSHDB/FLUSHALL hygiene).
    pub fn clear(&self) {
        self.entries.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn observe_counts_repeats() {
        let sk = HotKeySketch::new();
        for _ in 0..5 {
            sk.observe(&key("hot"));
        }
        sk.observe(&key("cold"));
        let top = sk.top(2);
        assert_eq!(top[0], (key("hot"), 5));
        assert_eq!(top[1], (key("cold"), 1));
    }

    #[test]
    fn top_orders_descending_and_truncates() {
        let sk = HotKeySketch::new();
        for i in 0..20 {
            let k = key(&format!("k{i:02}"));
            for _ in 0..=i {
                sk.observe(&k);
            }
        }
        let top = sk.top(3);
        assert_eq!(top.len(), 3);
        assert_eq!(top[0], (key("k19"), 20));
        assert_eq!(top[1], (key("k18"), 19));
        assert_eq!(top[2], (key("k17"), 18));
    }

    #[test]
    fn capacity_is_bounded_and_min_is_replaced() {
        let sk = HotKeySketch::new();
        // Fill all slots; "anchor" is genuinely hot.
        for _ in 0..100 {
            sk.observe(&key("anchor"));
        }
        for i in 0..(HOTKEY_CAPACITY - 1) {
            sk.observe(&key(&format!("filler{i}")));
        }
        assert_eq!(sk.len(), HOTKEY_CAPACITY);
        // A new key must evict a count-1 filler, never the hot anchor,
        // and inherit min+1 = 2 (SpaceSaving overestimate).
        sk.observe(&key("newcomer"));
        assert_eq!(sk.len(), HOTKEY_CAPACITY);
        let top = sk.top(HOTKEY_CAPACITY);
        assert_eq!(top[0], (key("anchor"), 100));
        let newcomer = top.iter().find(|(k, _)| k == &key("newcomer"));
        assert_eq!(newcomer, Some(&(key("newcomer"), 2)));
    }

    #[test]
    fn tick_fires_at_sample_rate() {
        let sk = HotKeySketch::new();
        let n = (HOTKEY_SAMPLE_RATE as usize) * 10;
        let fired = (0..n).filter(|_| sk.tick()).count();
        assert_eq!(fired, 10);
    }

    #[test]
    fn long_inline_and_heap_keys_round_trip() {
        let sk = HotKeySketch::new();
        // > 23 bytes forces CompactKey's heap path.
        let long = key("this-key-is-definitely-longer-than-twenty-three-bytes");
        sk.observe(&long);
        sk.observe(&long);
        assert_eq!(sk.top(1)[0], (long, 2));
    }

    #[test]
    fn clear_empties_sketch() {
        let sk = HotKeySketch::new();
        sk.observe(&key("a"));
        assert!(!sk.is_empty());
        sk.clear();
        assert!(sk.is_empty());
        assert_eq!(sk.top(10).len(), 0);
    }

    #[test]
    fn concurrent_observe_is_safe() {
        use std::sync::Arc;
        let sk = Arc::new(HotKeySketch::new());
        let mut handles = Vec::new();
        for t in 0..4 {
            let sk = Arc::clone(&sk);
            handles.push(std::thread::spawn(move || {
                let k = key(&format!("thread{t}"));
                for _ in 0..1000 {
                    sk.observe(&k);
                }
            }));
        }
        for h in handles {
            #[allow(clippy::unwrap_used)] // test-only join
            h.join().unwrap();
        }
        // try_lock drops contended samples by design, so under tight
        // contention a thread may lose ALL its observes — only safety and
        // count plausibility are asserted, not per-thread presence.
        let top = sk.top(4);
        assert!(!top.is_empty());
        for (_, c) in top {
            assert!(c >= 1 && c <= 1000);
        }
    }
}
