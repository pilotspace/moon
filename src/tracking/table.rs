//! The process-wide CLIENT TRACKING table: which clients read which keys,
//! the BCAST prefix registrations, redirects, and the invalidation routing
//! that turns a write into deliveries.
//!
//! Moved out of `tracking/mod.rs` unchanged (the 1500-line rule): the struct,
//! its fields and its methods are as they were there, re-exported as
//! `crate::tracking::TrackingTable`. Fields and helpers the sibling modules
//! and `tracking`'s tests reach into are `pub(super)` — the visibility they
//! had as private items of `tracking` itself.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;

use super::{
    ACTIVE_TRACKERS, ClientTrackingView, Delivery, InvalidationTx, PubSubInbox, prefilter,
};

/// Process-wide tracking table (see [`global_table`](super::global_table)).
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
    pub(super) key_clients: HashMap<Bytes, Vec<(u64, bool)>>,
    /// REVERSE index of `key_clients`: client_id -> the keys it currently tracks.
    ///
    /// Disconnect used to sweep every entry of `key_clients` looking for the
    /// departing client, holding the process-wide tracking mutex for the whole
    /// walk — so one client hanging up stalled every other shard's invalidation
    /// path, and the stall grew with the table (capped at `max_keys`, one
    /// million). This makes teardown proportional to what the client actually
    /// tracked. Kept exactly in step with `key_clients`: every insertion and
    /// every removal there has a matching update here.
    pub(super) client_keys: HashMap<u64, HashSet<Bytes>>,
    /// BCAST mode: list of (client_id, prefix, noloop)
    pub(super) bcast_clients: Vec<(u64, Bytes, bool)>,
    /// Client channels: client_id -> its invalidation queue.
    pub(super) client_channels: HashMap<u64, InvalidationTx>,
    /// Redirect map: source_client_id -> target_client_id
    pub(super) redirects: HashMap<u64, u64>,
    /// Pub/sub delivery channels of connections that have subscribed, keyed
    /// by client id — how a REDIRECT reaches a target that never enabled
    /// tracking itself (moon#1048). Registered once per connection, when its
    /// pub/sub channel is created, and removed when it disconnects.
    pub(super) inboxes: HashMap<u64, PubSubInbox>,
    /// Sources whose redirect target was found gone (`broken_redirect` in
    /// `CLIENT TRACKINGINFO`). Cleared when the source re-enables or disables
    /// tracking.
    pub(super) broken: HashSet<u64>,
    /// Whether a client id belongs to a live connection. The client registry
    /// in production; injectable so the routing rules are unit-testable.
    pub(super) is_connected: fn(u64) -> bool,
    /// Maximum keys tracked (bounded table)
    pub(super) max_keys: usize,
    /// This is the process-global table (moon#1166): its key and prefix
    /// changes are mirrored into the lock-free pre-filter. `false` for every
    /// private (test) table.
    pub(super) global: bool,
}

impl Default for TrackingTable {
    fn default() -> Self {
        Self::new()
    }
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
            inboxes: HashMap::new(),
            broken: HashSet::new(),
            is_connected: crate::client_registry::is_registered,
            max_keys,
            global: false,
        }
    }

    /// The process-global table: mirrors its contents into the pre-filter.
    pub(super) fn new_global() -> Self {
        Self {
            global: true,
            ..Self::new()
        }
    }

    /// A key is about to enter `key_clients` (call BEFORE inserting).
    #[inline]
    fn filter_add_key(&self, key: &[u8]) {
        if self.global {
            prefilter::key_added(key);
        }
    }

    /// A key has left `key_clients` (call AFTER removing).
    #[inline]
    fn filter_remove_key(&self, key: &[u8]) {
        if self.global {
            prefilter::key_removed(key);
        }
    }

    /// Replace the connection-liveness probe (tests).
    #[cfg(test)]
    pub(crate) fn with_liveness(mut self, is_connected: fn(u64) -> bool) -> Self {
        self.is_connected = is_connected;
        self
    }

    /// Register a client's invalidation channel.
    pub fn register_client(&mut self, client_id: u64, tx: impl Into<InvalidationTx>) {
        if self.client_channels.insert(client_id, tx.into()).is_none() {
            ACTIVE_TRACKERS.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Set (or, with `None`, clear) where `source`'s invalidations go.
    /// Re-enabling tracking replaces the redirect wholesale, and a fresh
    /// redirect is not broken.
    pub fn set_redirect(&mut self, source: u64, target: Option<u64>) {
        match target {
            Some(t) => {
                self.redirects.insert(source, t);
            }
            None => {
                self.redirects.remove(&source);
            }
        }
        self.broken.remove(&source);
    }

    /// Whether `client_id` has tracking enabled (its channel is registered).
    pub fn is_tracking(&self, client_id: u64) -> bool {
        self.client_channels.contains_key(&client_id)
    }

    /// What `CLIENT LIST`/`CLIENT INFO` report about `client_id`'s tracking,
    /// or `None` when it has tracking off (moon#1078).
    pub fn client_view(&self, client_id: u64) -> Option<ClientTrackingView> {
        if !self.is_tracking(client_id) {
            return None;
        }
        Some(ClientTrackingView {
            redirect: self.redirects.get(&client_id).copied().unwrap_or(0),
            broken_redirect: self.broken.contains(&client_id),
            bcast: self.bcast_clients.iter().any(|(id, _, _)| *id == client_id),
        })
    }

    /// [`Self::client_view`] for every tracking client, for one `CLIENT
    /// LIST`. Proportional to the number of tracking clients, not of
    /// connections.
    pub fn client_views(&self) -> HashMap<u64, ClientTrackingView> {
        // One pass over the BCAST registrations, not one per client: this
        // runs under the tracking mutex every write path waits on.
        let bcast: HashSet<u64> = self.bcast_clients.iter().map(|(id, _, _)| *id).collect();
        self.client_channels
            .keys()
            .map(|&id| {
                let view = ClientTrackingView {
                    redirect: self.redirects.get(&id).copied().unwrap_or(0),
                    broken_redirect: self.broken.contains(&id),
                    bcast: bcast.contains(&id),
                };
                (id, view)
            })
            .collect()
    }

    /// Whether an invalidation for `source` has found its redirect target
    /// gone since tracking was (re-)enabled.
    pub fn is_redirect_broken(&self, source: u64) -> bool {
        self.broken.contains(&source)
    }

    /// Whether `client_id` names a connection that could be a redirect
    /// target right now.
    pub fn client_exists(&self, client_id: u64) -> bool {
        self.client_channels.contains_key(&client_id)
            || self.inboxes.contains_key(&client_id)
            || (self.is_connected)(client_id)
    }

    /// Register the pub/sub channel of a connection that has subscribed.
    pub fn register_inbox(&mut self, client_id: u64, inbox: PubSubInbox) {
        self.inboxes.insert(client_id, inbox);
    }

    /// Drop a disconnecting connection's pub/sub inbox.
    pub fn unregister_inbox(&mut self, client_id: u64) {
        self.inboxes.remove(&client_id);
    }

    /// Register a BCAST prefix for a client. Registering the same prefix
    /// twice is a no-op (re-enabling BCAST adds prefixes, it does not
    /// duplicate them); `noloop` is refreshed.
    pub fn register_prefix(&mut self, client_id: u64, prefix: Bytes, noloop: bool) {
        if let Some(entry) = self
            .bcast_clients
            .iter_mut()
            .find(|(id, p, _)| *id == client_id && *p == prefix)
        {
            entry.2 = noloop;
            return;
        }
        if self.global {
            prefilter::prefixes_added(1);
        }
        self.bcast_clients.push((client_id, prefix, noloop));
    }

    /// Re-enabling tracking replaces NOLOOP for every prefix the client
    /// already has.
    pub fn set_bcast_noloop(&mut self, client_id: u64, noloop: bool) {
        for entry in self.bcast_clients.iter_mut().filter(|e| e.0 == client_id) {
            entry.2 = noloop;
        }
    }

    /// Resolve where one tracker's message goes.
    ///
    /// Without a redirect, the tracker's own channel. With one, the target —
    /// by redis's rules (`sendTrackingMessage`):
    ///
    /// * a subscribed RESP2 target gets a pub/sub `message`;
    /// * a target with its own tracking channel gets the push (RESP3 writes
    ///   it, RESP2 drops it);
    /// * a subscribed RESP3 target gets the push through its pub/sub channel;
    /// * a target that exists but has none of those cannot receive anything —
    ///   redis drops the message too;
    /// * a target that no longer exists breaks the redirect: the source is
    ///   told, and `CLIENT TRACKINGINFO` reports `broken_redirect`.
    ///
    /// Known gap (moon#1078, left open on purpose): a RESP3 target that
    /// neither subscribed nor enabled tracking has no channel moon can reach,
    /// so it gets nothing where redis pushes to it. The two ways to close it
    /// were weighed and neither is taken here:
    ///
    /// * a channel for every RESP3 connection: a connection holding one waits
    ///   in a select that never parks. Measured (monoio on macOS,
    ///   `--shards 1`, `--conn-park-secs 2`, 2000 idle `HELLO 3` connections,
    ///   twice):
    ///   without a channel `parked_clients:2000`; with one (`CLIENT TRACKING
    ///   on`) `parked_clients:0`. Every RESP3 client would lose c1M parking.
    /// * install a channel lazily and wake the target: the target may be
    ///   parked in a cancelable read registered in its OWN shard's
    ///   thread-local idle registry, or task-exited behind a readiness
    ///   watcher, or in a tokio select — none of which another thread can
    ///   wake today except by `shutdown(2)` (`CLIENT KILL`). It needs a new
    ///   shard-mesh message and a wake arm in every park stage on both
    ///   runtimes; that is a change to the c1M park machinery, not to
    ///   tracking, and belongs in its own PR.
    fn route(&mut self, client_id: u64) -> Option<Delivery> {
        let Some(&target) = self.redirects.get(&client_id) else {
            return self
                .client_channels
                .get(&client_id)
                .cloned()
                .map(Delivery::Push);
        };
        let inbox = self.inboxes.get(&target);
        if let Some(inbox) = inbox.filter(|i| !i.resp3) {
            return Some(Delivery::PubSub(inbox.clone()));
        }
        if let Some(tx) = self.client_channels.get(&target) {
            return Some(Delivery::Push(tx.clone()));
        }
        if let Some(inbox) = inbox {
            return Some(Delivery::PubSub(inbox.clone()));
        }
        // Lock order: this probes the client registry (a striped RwLock) while
        // the tracking mutex is held. Nothing takes the tracking mutex while
        // holding a registry stripe — `client_registry::update` closures only
        // touch the entry — so the order cannot invert. Reached only for a
        // redirect whose target has neither an inbox nor a tracking channel.
        if (self.is_connected)(target) {
            return None;
        }
        self.broken.insert(client_id);
        self.client_channels
            .get(&client_id)
            .cloned()
            .map(|source| Delivery::RedirBroken { source, target })
    }

    /// Track that a client has read a key (normal mode).
    ///
    /// Enforces the `max_keys` bound (deep-review G1: the documented cap was
    /// dead code, so a long-lived tracking client reading many distinct
    /// never-written keys grew this table without limit). When tracking a NEW
    /// key would exceed the cap, an arbitrary existing entry is evicted and
    /// its `(key, recipients)` returned — the caller must deliver an
    /// invalidation for it so the evicted key's clients drop their cached copy
    /// (Redis's "fake invalidation" on tracking-table eviction). Returns
    /// `None` when no eviction occurred.
    pub fn track_key(
        &mut self,
        client_id: u64,
        key: &Bytes,
        noloop: bool,
    ) -> Option<(Bytes, Vec<Delivery>)> {
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
            self.filter_remove_key(&victim);
            let mut recipients = Vec::new();
            for (cid, _noloop) in clients {
                Self::forget_client_key(&mut self.client_keys, cid, &victim);
                // No noloop skip: cap eviction is not a self-write — every
                // tracker of the victim key must drop its cached copy.
                if let Some(d) = self.route(cid) {
                    recipients.push(d);
                }
            }
            Some((victim, recipients))
        } else {
            None
        };

        self.filter_add_key(key);
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
    /// and all BCAST clients whose prefixes match, resolved to their delivery
    /// routes. Removes the key from the tracking table after collection.
    pub fn invalidate_key(&mut self, key: &Bytes, writer_client_id: u64) -> Vec<Delivery> {
        let mut to_notify: Vec<Delivery> = Vec::new();

        // Normal mode: check key_clients
        if let Some(clients) = self.key_clients.remove(key) {
            self.filter_remove_key(key);
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
                if let Some(d) = self.route(cid) {
                    to_notify.push(d);
                }
            }
        }

        // BCAST mode: check prefix matches. Collected first, because routing
        // needs `&mut self` (it records broken redirects).
        let mut bcast_hits: smallvec::SmallVec<[u64; 4]> = smallvec::SmallVec::new();
        for (cid, prefix, noloop) in &self.bcast_clients {
            if key.starts_with(prefix.as_ref()) {
                if *noloop && *cid == writer_client_id {
                    continue;
                }
                bcast_hits.push(*cid);
            }
        }
        for cid in bcast_hits {
            if let Some(d) = self.route(cid) {
                to_notify.push(d);
            }
        }

        to_notify
    }

    /// Remove all tracking for a client (on disconnect or TRACKING OFF).
    ///
    /// Leaves the client's pub/sub inbox alone: that belongs to the
    /// connection's subscriptions, not its tracking, and another client may
    /// still redirect to it. [`TrackingTable::unregister_inbox`] drops it.
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
                    self.filter_remove_key(&key);
                }
            }
        }
        // Remove from bcast_clients
        let before = self.bcast_clients.len();
        self.bcast_clients.retain(|(id, _, _)| *id != client_id);
        if self.global {
            prefilter::prefixes_removed(before - self.bcast_clients.len());
        }
        // Remove channel and redirect
        if self.client_channels.remove(&client_id).is_some() {
            ACTIVE_TRACKERS.fetch_sub(1, Ordering::Relaxed);
        }
        self.redirects.remove(&client_id);
        self.broken.remove(&client_id);
    }

    /// Drop one (client, key) pair from the reverse index, retiring the
    /// client's entry when it has nothing left to track.
    ///
    /// Free function over the map so callers can hold a borrow of the forward
    /// map across the call.
    fn forget_client_key(
        client_keys: &mut HashMap<u64, HashSet<Bytes>>,
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
    /// every tracking client's route, so the caller can deliver the flush
    /// invalidation (`invalidate` + Null payload, the Redis convention) — to
    /// the redirect target where there is one.
    pub fn invalidate_all(&mut self) -> Vec<Delivery> {
        let dropped: Vec<Bytes> = if self.global {
            self.key_clients.keys().cloned().collect()
        } else {
            Vec::new()
        };
        self.key_clients.clear();
        for key in &dropped {
            self.filter_remove_key(key);
        }
        self.client_keys.clear();
        let ids: Vec<u64> = self.client_channels.keys().copied().collect();
        ids.into_iter().filter_map(|id| self.route(id)).collect()
    }
}
