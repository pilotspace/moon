pub mod invalidation;

use crate::runtime::channel;
use bytes::Bytes;
use std::collections::HashMap;

use crate::protocol::Frame;

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
    /// BCAST mode: list of (client_id, prefix, noloop)
    bcast_clients: Vec<(u64, Bytes, bool)>,
    /// Client channels: client_id -> MpscSender<Frame>
    client_channels: HashMap<u64, channel::MpscSender<Frame>>,
    /// Redirect map: source_client_id -> target_client_id
    redirects: HashMap<u64, u64>,
    /// Maximum keys tracked (bounded table)
    #[allow(dead_code)]
    max_keys: usize,
}

impl TrackingTable {
    pub fn new() -> Self {
        Self {
            key_clients: HashMap::new(),
            bcast_clients: Vec::new(),
            client_channels: HashMap::new(),
            redirects: HashMap::new(),
            max_keys: 1_000_000,
        }
    }

    /// Register a client's invalidation channel.
    pub fn register_client(&mut self, client_id: u64, tx: channel::MpscSender<Frame>) {
        self.client_channels.insert(client_id, tx);
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
    pub fn track_key(&mut self, client_id: u64, key: &Bytes, noloop: bool) {
        let clients = self.key_clients.entry(key.clone()).or_insert_with(Vec::new);
        if !clients.iter().any(|(id, _)| *id == client_id) {
            clients.push((client_id, noloop));
        }
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
        // Remove from key_clients
        self.key_clients.retain(|_, clients| {
            clients.retain(|(id, _)| *id != client_id);
            !clients.is_empty()
        });
        // Remove from bcast_clients
        self.bcast_clients.retain(|(id, _, _)| *id != client_id);
        // Remove channel and redirect
        self.client_channels.remove(&client_id);
        self.redirects.remove(&client_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::channel;

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
        let push = invalidation::invalidation_push(&[key.clone()]);
        senders[0].try_send(push.clone()).unwrap();
        let received = rx2.try_recv().unwrap();
        assert_eq!(received, push);
    }
}
