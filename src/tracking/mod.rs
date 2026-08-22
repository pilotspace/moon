pub mod invalidation;

use crate::runtime::channel;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::protocol::Frame;

/// Number of clients with a registered invalidation channel, process-wide.
///
/// This is the write-path gate: every successful write checks
/// [`tracking_active`] (one relaxed load) before touching the shared
/// [`TrackingTable`] lock. With no tracking clients the KV hot path pays
/// a single atomic load and nothing else.
static ACTIVE_TRACKERS: AtomicUsize = AtomicUsize::new(0);

/// True when at least one connection has CLIENT TRACKING enabled.
#[inline]
pub fn tracking_active() -> bool {
    ACTIVE_TRACKERS.load(Ordering::Relaxed) > 0
}

/// The process-wide tracking table.
///
/// CLIENT TRACKING must be GLOBAL, not per-shard: a tracked read registers on
/// the reader connection's shard thread while the invalidating write can
/// execute on any other shard (or arrive over the SPSC mesh). Per-shard
/// tables silently dropped every cross-shard invalidation — the table is one
/// shared instance guarded by a mutex, gated off the hot path by
/// [`tracking_active`]. Invalidation senders are cross-thread-safe (flume),
/// so a write on shard A pushes directly into a connection's channel on
/// shard B; the connection's own event loop writes it to the socket.
pub fn global_table() -> std::sync::Arc<parking_lot::Mutex<TrackingTable>> {
    static GLOBAL: std::sync::OnceLock<std::sync::Arc<parking_lot::Mutex<TrackingTable>>> =
        std::sync::OnceLock::new();
    GLOBAL
        .get_or_init(|| std::sync::Arc::new(parking_lot::Mutex::new(TrackingTable::new())))
        .clone()
}

/// Per-client tracking configuration.
#[derive(Debug, Clone)]
pub struct TrackingState {
    pub enabled: bool,
    pub bcast: bool,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    pub redirect: Option<u64>,
    pub prefixes: Vec<Bytes>,
    pub invalidation_tx: Option<channel::MpscSender<Frame>>,
}

impl Default for TrackingState {
    fn default() -> Self {
        Self {
            enabled: false,
            bcast: false,
            optin: false,
            optout: false,
            noloop: false,
            redirect: None,
            prefixes: Vec::new(),
            invalidation_tx: None,
        }
    }
}

/// Per-shard tracking table.
///
/// Two modes:
/// 1. Normal (default): track_key records which clients have read a key.
///    On write, invalidate_key looks up clients and sends invalidation.
/// 2. BCAST: clients register prefixes. On ANY write, check if key matches
///    any registered prefix and invalidate matching clients.
///
/// Table is bounded: max_keys (default 1_000_000). When exceeded, evict oldest
/// entries with fake invalidation.
pub struct TrackingTable {
    /// Normal mode: key -> set of (client_id, noloop)
    key_clients: HashMap<Bytes, Vec<(u64, bool)>>,
    /// REVERSE index of `key_clients`: client_id -> the keys it currently tracks.
    ///
    /// Disconnect used to sweep every entry of `key_clients` looking for the
    /// departing client, holding the process-wide tracking mutex for the whole
    /// walk — so one client hanging up stalled every other shard's invalidation
    /// path, and the stall grew with the table (capped at `max_keys`, one
    /// million). This makes teardown proportional to what the client actually
    /// tracked. Kept exactly in step with `key_clients`: every insertion and
    /// every removal there has a matching update here.
    client_keys: HashMap<u64, std::collections::HashSet<Bytes>>,
    /// BCAST mode: list of (client_id, prefix, noloop)
    bcast_clients: Vec<(u64, Bytes, bool)>,
    /// Client channels: client_id -> MpscSender<Frame>
    client_channels: HashMap<u64, channel::MpscSender<Frame>>,
    /// Redirect map: source_client_id -> target_client_id
    redirects: HashMap<u64, u64>,
    /// Maximum keys tracked (bounded table)
    max_keys: usize,
}

impl TrackingTable {
    pub fn new() -> Self {
        Self::with_max_keys(1_000_000)
    }

    /// Construct with an explicit key cap (tests; production uses `new`).
    pub fn with_max_keys(max_keys: usize) -> Self {
        Self {
            key_clients: HashMap::new(),
            client_keys: HashMap::new(),
            bcast_clients: Vec::new(),
            client_channels: HashMap::new(),
            redirects: HashMap::new(),
            max_keys,
        }
    }

    /// Register a client's invalidation channel.
    pub fn register_client(&mut self, client_id: u64, tx: channel::MpscSender<Frame>) {
        if self.client_channels.insert(client_id, tx).is_none() {
            ACTIVE_TRACKERS.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Register a redirect: invalidations for source go to target.
    pub fn set_redirect(&mut self, source: u64, target: u64) {
        self.redirects.insert(source, target);
    }

    /// Register a BCAST prefix for a client.
    pub fn register_prefix(&mut self, client_id: u64, prefix: Bytes, noloop: bool) {
        self.bcast_clients.push((client_id, prefix, noloop));
    }

    /// Track that a client has read a key (normal mode).
    ///
    /// Enforces the `max_keys` bound (deep-review G1: the documented cap was
    /// dead code, so a long-lived tracking client reading many distinct
    /// never-written keys grew this table without limit). When tracking a NEW
    /// key would exceed the cap, an arbitrary existing entry is evicted and
    /// its `(key, senders)` returned — the caller must push an invalidation
    /// for it so the evicted key's clients drop their cached copy (Redis's
    /// "fake invalidation" on tracking-table eviction). Returns `None` when
    /// no eviction occurred.
    pub fn track_key(
        &mut self,
        client_id: u64,
        key: &Bytes,
        noloop: bool,
    ) -> Option<(Bytes, Vec<channel::MpscSender<Frame>>)> {
        if let Some(clients) = self.key_clients.get_mut(key) {
            if !clients.iter().any(|(id, _)| *id == client_id) {
                clients.push((client_id, noloop));
                self.client_keys
                    .entry(client_id)
                    .or_default()
                    .insert(key.clone());
            }
            return None;
        }

        let evicted = if self.key_clients.len() >= self.max_keys.max(1) {
            // Evict an arbitrary entry (HashMap has no age order; correctness
            // needs only that the evicted key's trackers are told to drop it).
            #[allow(clippy::unwrap_used)] // len >= 1 guaranteed by the branch
            let victim = self.key_clients.keys().next().unwrap().clone();
            let clients = self.key_clients.remove(&victim).unwrap_or_default();
            let mut senders = Vec::new();
            for (cid, _noloop) in clients {
                Self::forget_client_key(&mut self.client_keys, cid, &victim);
                // No noloop skip: cap eviction is not a self-write — every
                // tracker of the victim key must drop its cached copy.
                let target_id = self.redirects.get(&cid).copied().unwrap_or(cid);
                if let Some(tx) = self.client_channels.get(&target_id) {
                    senders.push(tx.clone());
                }
            }
            Some((victim, senders))
        } else {
            None
        };

        self.key_clients
            .insert(key.clone(), vec![(client_id, noloop)]);
        self.client_keys
            .entry(client_id)
            .or_default()
            .insert(key.clone());
        evicted
    }

    /// Get the list of client IDs tracking a given key (for testing).
    pub fn tracked_clients(&self, key: &Bytes) -> Vec<u64> {
        self.key_clients
            .get(key)
            .map(|clients| clients.iter().map(|(id, _)| *id).collect())
            .unwrap_or_default()
    }

    /// Invalidate a key: collect all clients that tracked this key (normal mode)
    /// and all BCAST clients whose prefixes match. Returns list of channels to notify.
    /// Removes the key from the tracking table after collection.
    pub fn invalidate_key(
        &mut self,
        key: &Bytes,
        writer_client_id: u64,
    ) -> Vec<channel::MpscSender<Frame>> {
        let mut to_notify: Vec<channel::MpscSender<Frame>> = Vec::new();

        // Normal mode: check key_clients
        if let Some(clients) = self.key_clients.remove(key) {
            for (cid, noloop) in clients {
                // The key is gone from the forward map, so it must go from the
                // reverse one too -- a stale entry would make the reverse index
                // grow without bound for a client that re-reads an
                // often-invalidated key, and teardown would walk the garbage.
                Self::forget_client_key(&mut self.client_keys, cid, key);
                // NOLOOP: skip if the writer is the same client
                if noloop && cid == writer_client_id {
                    continue;
                }
                let target_id = self.redirects.get(&cid).copied().unwrap_or(cid);
                if let Some(tx) = self.client_channels.get(&target_id) {
                    to_notify.push(tx.clone());
                }
            }
        }

        // BCAST mode: check prefix matches
        for (cid, prefix, noloop) in &self.bcast_clients {
            if key.starts_with(prefix.as_ref()) {
                if *noloop && *cid == writer_client_id {
                    continue;
                }
                let target_id = self.redirects.get(cid).copied().unwrap_or(*cid);
                if let Some(tx) = self.client_channels.get(&target_id) {
                    to_notify.push(tx.clone());
                }
            }
        }

        to_notify
    }

    /// Remove all tracking for a client (on disconnect or TRACKING OFF).
    pub fn untrack_all(&mut self, client_id: u64) {
        // Visit only the keys this client actually tracked. This used to
        // `retain` over the whole table -- O(tracked keys) per disconnect,
        // under the process-wide mutex, regardless of whether the departing
        // client had tracked anything at all.
        if let Some(keys) = self.client_keys.remove(&client_id) {
            for key in keys {
                let Some(clients) = self.key_clients.get_mut(&key) else {
                    continue;
                };
                clients.retain(|(id, _)| *id != client_id);
                if clients.is_empty() {
                    self.key_clients.remove(&key);
                }
            }
        }
        // Remove from bcast_clients
        self.bcast_clients.retain(|(id, _, _)| *id != client_id);
        // Remove channel and redirect
        if self.client_channels.remove(&client_id).is_some() {
            ACTIVE_TRACKERS.fetch_sub(1, Ordering::Relaxed);
        }
        self.redirects.remove(&client_id);
    }

    /// Drop one (client, key) pair from the reverse index, retiring the
    /// client's entry when it has nothing left to track.
    ///
    /// Free function over the map so callers can hold a borrow of the forward
    /// map across the call.
    fn forget_client_key(
        client_keys: &mut HashMap<u64, std::collections::HashSet<Bytes>>,
        client_id: u64,
        key: &Bytes,
    ) {
        if let Some(keys) = client_keys.get_mut(&client_id) {
            keys.remove(key);
            if keys.is_empty() {
                client_keys.remove(&client_id);
            }
        }
    }

    /// Cache-flush invalidation (FLUSHALL/FLUSHDB): every registered client
    /// must drop its whole local cache. Clears the per-key table and returns
    /// every client channel so the caller can push the RESP3 flush
    /// invalidation (`invalidate` + Null payload, the Redis convention).
    pub fn invalidate_all(&mut self) -> Vec<channel::MpscSender<Frame>> {
        self.key_clients.clear();
        self.client_keys.clear();
        self.client_channels.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::channel;

    /// The forward and reverse indexes must describe exactly the same set of
    /// (key, client) pairs. Every test below asserts this after mutating the
    /// table -- a reverse index that drifts is worse than no reverse index,
    /// because teardown then silently leaves a departed client registered on a
    /// key and keeps pushing invalidations into a dead channel.
    fn assert_indexes_agree(table: &TrackingTable) {
        let mut forward: Vec<(u64, Bytes)> = Vec::new();
        for (key, clients) in &table.key_clients {
            assert!(!clients.is_empty(), "empty client list left on {key:?}");
            for (cid, _) in clients {
                forward.push((*cid, key.clone()));
            }
        }
        let mut reverse: Vec<(u64, Bytes)> = Vec::new();
        for (cid, keys) in &table.client_keys {
            assert!(!keys.is_empty(), "empty key set left for client {cid}");
            for key in keys {
                reverse.push((*cid, key.clone()));
            }
        }
        forward.sort();
        reverse.sort();
        assert_eq!(forward, reverse, "forward and reverse indexes disagree");
    }

    fn sender() -> channel::MpscSender<Frame> {
        let (tx, rx) = channel::mpsc_unbounded::<Frame>();
        std::mem::forget(rx); // keep the channel alive for the test's lifetime
        tx
    }

    #[test]
    fn disconnect_untracks_only_the_departing_client() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        table.register_client(2, sender());
        let shared = Bytes::from_static(b"shared");
        let solo = Bytes::from_static(b"solo");
        table.track_key(1, &shared, false);
        table.track_key(2, &shared, false);
        table.track_key(1, &solo, false);
        assert_indexes_agree(&table);

        table.untrack_all(1);

        assert_eq!(table.tracked_clients(&shared), vec![2]);
        assert!(
            !table.key_clients.contains_key(&solo),
            "a key with no trackers left must be dropped, not kept empty"
        );
        assert_indexes_agree(&table);
    }

    #[test]
    fn invalidating_a_key_clears_it_from_the_reverse_index() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        let k = Bytes::from_static(b"k");
        // Track, invalidate, re-track -- ten times. The reverse index must not
        // accumulate: it mirrors the forward map, which holds one entry.
        for _ in 0..10 {
            table.track_key(1, &k, false);
            table.invalidate_key(&k, 99);
        }
        assert!(table.client_keys.is_empty(), "reverse index accumulated");
        table.track_key(1, &k, false);
        assert_eq!(table.client_keys[&1].len(), 1);
        assert_indexes_agree(&table);

        // And teardown after an invalidation must not trip over the gap.
        table.invalidate_key(&k, 99);
        table.untrack_all(1);
        assert_indexes_agree(&table);
    }

    #[test]
    fn cap_eviction_clears_the_reverse_index() {
        let mut table = TrackingTable::with_max_keys(2);
        table.register_client(1, sender());
        table.track_key(1, &Bytes::from_static(b"a"), false);
        table.track_key(1, &Bytes::from_static(b"b"), false);
        // The third key evicts an arbitrary existing one.
        let evicted = table.track_key(1, &Bytes::from_static(b"c"), false);
        assert!(evicted.is_some(), "the cap must evict");
        assert_eq!(table.key_clients.len(), 2, "table must stay at the cap");
        assert_indexes_agree(&table);
    }

    #[test]
    fn flush_invalidation_clears_the_reverse_index() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        table.track_key(1, &Bytes::from_static(b"a"), false);
        table.invalidate_all();
        assert!(
            table.client_keys.is_empty(),
            "FLUSHALL left a stale reverse entry"
        );
        assert_indexes_agree(&table);
        // The client is still registered, so it can track again.
        table.track_key(1, &Bytes::from_static(b"b"), false);
        assert_indexes_agree(&table);
    }

    #[test]
    fn tracking_the_same_key_twice_records_one_reverse_entry() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        let k = Bytes::from_static(b"k");
        table.track_key(1, &k, false);
        table.track_key(1, &k, false);
        assert_eq!(table.tracked_clients(&k), vec![1]);
        assert_eq!(table.client_keys[&1].len(), 1);
        assert_indexes_agree(&table);
    }

    #[test]
    fn disconnecting_an_untracked_client_leaves_the_table_alone() {
        let mut table = TrackingTable::new();
        table.register_client(1, sender());
        let k = Bytes::from_static(b"k");
        table.track_key(1, &k, false);

        table.untrack_all(42); // never tracked, never registered

        assert_eq!(table.tracked_clients(&k), vec![1]);
        assert_indexes_agree(&table);
    }

    /// Cost of ONE client disconnecting, as the tracking table's key count grows.
    ///
    /// `#[ignore]`d: a measurement, not an assertion. Run it explicitly:
    /// `cargo test --release --lib bench_untrack_all_cost_vs_table_size -- --ignored --nocapture`
    ///
    /// The disconnecting client tracks NOTHING, so every microsecond spent is
    /// the sweep over other clients' keys. Flat µs/disconnect means the table
    /// size does not matter; growth means each disconnect is O(table).
    #[test]
    #[ignore = "measurement harness; run explicitly with --nocapture"]
    fn bench_untrack_all_cost_vs_table_size() {
        use std::time::Instant;
        println!("{:<10} {:<16} total", "keys", "µs/disconnect");
        for keys in [1000_usize, 2000, 4000, 8000, 16000] {
            let mut table = TrackingTable::new();
            let (tx, _rx) = channel::mpsc_unbounded::<Frame>();
            table.register_client(1, tx.clone());
            for k in 0..keys {
                table.track_key(1, &Bytes::from(format!("key:{k}")), false);
            }
            // 100 clients that tracked nothing at all connect and disconnect.
            const CHURN: usize = 100;
            for c in 0..CHURN {
                table.register_client(100 + c as u64, tx.clone());
            }
            let t = Instant::now();
            for c in 0..CHURN {
                table.untrack_all(100 + c as u64);
            }
            let el = t.elapsed();
            println!(
                "{:<10} {:<16.3} {:?}",
                keys,
                el.as_secs_f64() * 1e6 / CHURN as f64,
                el
            );
            assert_eq!(table.key_clients.len(), keys, "sweep dropped live entries");
        }
    }

    #[test]
    fn test_new_creates_empty_table() {
        let table = TrackingTable::new();
        assert!(table.key_clients.is_empty());
        assert!(table.bcast_clients.is_empty());
        assert!(table.client_channels.is_empty());
    }

    #[test]
    fn test_track_key_registers_client() {
        let mut table = TrackingTable::new();
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);
        assert_eq!(table.tracked_clients(&key), vec![1]);
    }

    #[test]
    fn test_track_key_idempotent() {
        let mut table = TrackingTable::new();
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);
        table.track_key(1, &key, false);
        assert_eq!(table.tracked_clients(&key), vec![1]);
    }

    #[test]
    fn test_track_key_multiple_clients() {
        let mut table = TrackingTable::new();
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);
        table.track_key(2, &key, false);
        let mut clients = table.tracked_clients(&key);
        clients.sort();
        assert_eq!(clients, vec![1, 2]);
    }

    /// G1 (deep review): the documented max_keys bound was dead code — a
    /// long-lived tracking client reading many distinct never-written keys
    /// grew key_clients without limit. The cap must evict an existing entry
    /// (with invalidation senders so the evicted key's clients drop their
    /// cached copy) instead of growing.
    #[test]
    fn test_track_key_enforces_max_keys_bound() {
        let mut table = TrackingTable::with_max_keys(2);
        let (tx, rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        assert!(
            table
                .track_key(1, &Bytes::from_static(b"k1"), false)
                .is_none()
        );
        assert!(
            table
                .track_key(1, &Bytes::from_static(b"k2"), false)
                .is_none()
        );
        // Re-tracking an existing key never evicts.
        assert!(
            table
                .track_key(1, &Bytes::from_static(b"k2"), false)
                .is_none()
        );

        // Third distinct key: one existing entry must be evicted, with the
        // evicted key's client senders returned for invalidation.
        let evicted = table
            .track_key(1, &Bytes::from_static(b"k3"), false)
            .expect("cap reached: eviction expected");
        assert!(evicted.0 == Bytes::from_static(b"k1") || evicted.0 == Bytes::from_static(b"k2"));
        assert_eq!(evicted.1.len(), 1, "evicted key's tracker must be notified");
        assert_eq!(table.key_clients.len(), 2, "table must stay at the cap");
        assert_eq!(table.tracked_clients(&Bytes::from_static(b"k3")), vec![1]);
        drop(rx);
    }

    #[test]
    fn test_invalidate_key_returns_senders_and_removes() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);

        let senders = table.invalidate_key(&key, 99); // writer is different client
        assert_eq!(senders.len(), 1);
        // Key should be removed after invalidation
        assert!(table.tracked_clients(&key).is_empty());
    }

    #[test]
    fn test_untrack_all_removes_client() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        let key1 = Bytes::from_static(b"foo");
        let key2 = Bytes::from_static(b"bar");
        table.track_key(1, &key1, false);
        table.track_key(1, &key2, false);

        table.untrack_all(1);
        assert!(table.tracked_clients(&key1).is_empty());
        assert!(table.tracked_clients(&key2).is_empty());
        assert!(!table.client_channels.contains_key(&1));
    }

    #[test]
    fn test_bcast_prefix_match() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        table.register_prefix(1, Bytes::from_static(b"user:"), false);

        let key = Bytes::from_static(b"user:123");
        let senders = table.invalidate_key(&key, 99);
        assert_eq!(senders.len(), 1);
    }

    #[test]
    fn test_bcast_prefix_no_match() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        table.register_prefix(1, Bytes::from_static(b"user:"), false);

        let key = Bytes::from_static(b"other:key");
        let senders = table.invalidate_key(&key, 99);
        assert!(senders.is_empty());
    }

    #[test]
    fn test_noloop_skips_self_invalidation() {
        let mut table = TrackingTable::new();
        let (tx, _rx) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx);
        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, true); // noloop = true

        // Writer is the same client (1), should skip
        let senders = table.invalidate_key(&key, 1);
        assert!(senders.is_empty());
    }

    #[test]
    fn test_redirect_sends_to_target() {
        let mut table = TrackingTable::new();
        let (tx1, _rx1) = channel::mpsc_bounded::<Frame>(16);
        let (tx2, rx2) = channel::mpsc_bounded::<Frame>(16);
        table.register_client(1, tx1);
        table.register_client(2, tx2);
        table.set_redirect(1, 2); // redirect client 1's invalidations to client 2

        let key = Bytes::from_static(b"foo");
        table.track_key(1, &key, false);

        let senders = table.invalidate_key(&key, 99);
        assert_eq!(senders.len(), 1);
        // Send a test frame through the returned sender
        let push = invalidation::invalidation_push(std::slice::from_ref(&key));
        senders[0].try_send(push.clone()).unwrap();
        let received = rx2.try_recv().unwrap();
        assert_eq!(received, push);
    }
}
