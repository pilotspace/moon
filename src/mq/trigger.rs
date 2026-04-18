//! Per-shard trigger registry for debounced MQ callbacks.
//!
//! `TriggerRegistry` stores debounced event triggers keyed by a composite
//! key of `{workspace_id_hex}:{queue_key}`. Triggers fire after a debounce
//! window expires, executing a stored RESP callback command.
//!
//! Timer integration: the event loop's 1ms periodic tick checks
//! `fire_ready()` and dispatches callbacks for entries whose
//! `pending_fire_ms` has elapsed.

use std::collections::HashMap;

use bytes::Bytes;

/// Entry in the trigger registry.
#[derive(Debug, Clone)]
pub struct TriggerEntry {
    /// The queue key that this trigger watches.
    pub queue_key: Bytes,
    /// RESP command to execute when trigger fires.
    pub callback_cmd: Bytes,
    /// Debounce window in milliseconds.
    pub debounce_ms: u64,
    /// Timestamp of last fire (0 = never fired).
    pub last_fire_ms: u64,
    /// Timestamp when next fire is pending (0 = not pending).
    pub pending_fire_ms: u64,
}

/// Per-shard registry for debounced MQ triggers.
///
/// Keyed by composite key `{ws_hex}:{queue_key}` to ensure workspace
/// isolation. Thread-safety provided by caller via `Mutex` on
/// `ShardDatabases`.
#[derive(Debug, Default)]
pub struct TriggerRegistry {
    entries: HashMap<Bytes, TriggerEntry>,
}

impl TriggerRegistry {
    /// Create an empty trigger registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a trigger (insert or replace).
    pub fn register(&mut self, key: Bytes, entry: TriggerEntry) {
        self.entries.insert(key, entry);
    }

    /// Remove a trigger by composite key.
    pub fn remove(&mut self, key: &[u8]) {
        self.entries.remove(key);
    }

    /// Look up a trigger by composite key.
    pub fn get(&self, key: &[u8]) -> Option<&TriggerEntry> {
        self.entries.get(key)
    }

    /// Get a mutable reference to a trigger by composite key.
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut TriggerEntry> {
        self.entries.get_mut(key)
    }

    /// Return keys of entries whose pending fire time has elapsed.
    ///
    /// An entry is ready to fire when `pending_fire_ms > 0` and
    /// `pending_fire_ms <= now_ms`.
    pub fn fire_ready(&self, now_ms: u64) -> Vec<Bytes> {
        self.entries
            .iter()
            .filter(|(_, e)| e.pending_fire_ms > 0 && e.pending_fire_ms <= now_ms)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Mark a trigger as pending to fire at a specific time.
    ///
    /// Sets `pending_fire_ms` to `fire_at_ms`. If the entry does not
    /// exist, this is a no-op.
    pub fn mark_pending(&mut self, key: &[u8], fire_at_ms: u64) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.pending_fire_ms = fire_at_ms;
        }
    }

    /// Mark a trigger as having just fired.
    ///
    /// Sets `last_fire_ms` to `now_ms` and clears `pending_fire_ms`.
    /// If the entry does not exist, this is a no-op.
    pub fn mark_fired(&mut self, key: &[u8], now_ms: u64) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.last_fire_ms = now_ms;
            entry.pending_fire_ms = 0;
        }
    }

    /// Remove all triggers whose composite key starts with `{ws_hex}:`.
    ///
    /// Used when a workspace is dropped to clean up orphaned triggers.
    pub fn remove_by_workspace(&mut self, ws_hex: &str) {
        let prefix = format!("{}:", ws_hex);
        self.entries
            .retain(|k, _| !k.starts_with(prefix.as_bytes()));
    }

    /// Number of registered triggers.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(queue_key: &[u8], callback: &[u8], debounce_ms: u64) -> TriggerEntry {
        TriggerEntry {
            queue_key: Bytes::copy_from_slice(queue_key),
            callback_cmd: Bytes::copy_from_slice(callback),
            debounce_ms,
            last_fire_ms: 0,
            pending_fire_ms: 0,
        }
    }

    #[test]
    fn test_register_and_get() {
        let mut reg = TriggerRegistry::new();
        let key = Bytes::from_static(b"abc123:orders");
        let entry = make_entry(b"orders", b"PUBLISH mq:events new", 1000);
        reg.register(key.clone(), entry);

        let found = reg.get(b"abc123:orders");
        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.queue_key.as_ref(), b"orders");
        assert_eq!(found.debounce_ms, 1000);
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_remove() {
        let mut reg = TriggerRegistry::new();
        reg.register(
            Bytes::from_static(b"ws1:q1"),
            make_entry(b"q1", b"CMD", 100),
        );
        assert_eq!(reg.len(), 1);

        reg.remove(b"ws1:q1");
        assert_eq!(reg.len(), 0);
        assert!(reg.get(b"ws1:q1").is_none());
    }

    #[test]
    fn test_fire_ready_empty() {
        let reg = TriggerRegistry::new();
        assert!(reg.fire_ready(1000).is_empty());
    }

    #[test]
    fn test_fire_ready_with_pending() {
        let mut reg = TriggerRegistry::new();
        let key = Bytes::from_static(b"ws1:q1");
        let mut entry = make_entry(b"q1", b"CMD", 500);
        entry.pending_fire_ms = 900;
        reg.register(key.clone(), entry);

        // Not yet ready at time 800
        assert!(reg.fire_ready(800).is_empty());

        // Ready at exact time 900
        let ready = reg.fire_ready(900);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].as_ref(), b"ws1:q1");

        // Ready at time 1000 (past due)
        let ready = reg.fire_ready(1000);
        assert_eq!(ready.len(), 1);
    }

    #[test]
    fn test_fire_ready_no_pending() {
        let mut reg = TriggerRegistry::new();
        reg.register(
            Bytes::from_static(b"ws1:q1"),
            make_entry(b"q1", b"CMD", 500),
        );
        // pending_fire_ms is 0, so should not fire
        assert!(reg.fire_ready(u64::MAX).is_empty());
    }

    #[test]
    fn test_mark_pending_and_fire_cycle() {
        let mut reg = TriggerRegistry::new();
        let key = Bytes::from_static(b"ws1:q1");
        reg.register(key.clone(), make_entry(b"q1", b"CMD", 500));

        // Initially no pending fires
        assert!(reg.fire_ready(1000).is_empty());

        // Mark pending to fire at 1500
        reg.mark_pending(b"ws1:q1", 1500);
        assert!(reg.fire_ready(1400).is_empty());
        assert_eq!(reg.fire_ready(1500).len(), 1);

        // Mark as fired
        reg.mark_fired(b"ws1:q1", 1500);
        let entry = reg.get(b"ws1:q1").unwrap();
        assert_eq!(entry.last_fire_ms, 1500);
        assert_eq!(entry.pending_fire_ms, 0);

        // After firing, no longer ready
        assert!(reg.fire_ready(2000).is_empty());
    }

    #[test]
    fn test_debounce_window() {
        let mut reg = TriggerRegistry::new();
        let key = Bytes::from_static(b"ws1:q1");
        let entry = make_entry(b"q1", b"CMD", 1000);
        reg.register(key.clone(), entry);

        // Simulate: event at t=100, debounce=1000, so fire at t=1100
        reg.mark_pending(b"ws1:q1", 1100);
        assert!(reg.fire_ready(1099).is_empty());
        assert_eq!(reg.fire_ready(1100).len(), 1);

        // Fire and record
        reg.mark_fired(b"ws1:q1", 1100);

        // New event at t=1200, debounce=1000, fire at t=2200
        reg.mark_pending(b"ws1:q1", 2200);
        assert!(reg.fire_ready(2199).is_empty());
        assert_eq!(reg.fire_ready(2200).len(), 1);
    }

    #[test]
    fn test_remove_by_workspace() {
        let mut reg = TriggerRegistry::new();
        reg.register(
            Bytes::from_static(b"ws_aaa:q1"),
            make_entry(b"q1", b"CMD", 100),
        );
        reg.register(
            Bytes::from_static(b"ws_aaa:q2"),
            make_entry(b"q2", b"CMD", 100),
        );
        reg.register(
            Bytes::from_static(b"ws_bbb:q1"),
            make_entry(b"q1", b"CMD", 100),
        );
        assert_eq!(reg.len(), 3);

        reg.remove_by_workspace("ws_aaa");
        assert_eq!(reg.len(), 1);
        assert!(reg.get(b"ws_aaa:q1").is_none());
        assert!(reg.get(b"ws_aaa:q2").is_none());
        assert!(reg.get(b"ws_bbb:q1").is_some());
    }

    #[test]
    fn test_remove_by_workspace_no_match() {
        let mut reg = TriggerRegistry::new();
        reg.register(
            Bytes::from_static(b"ws_aaa:q1"),
            make_entry(b"q1", b"CMD", 100),
        );
        reg.remove_by_workspace("ws_zzz");
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_mark_pending_nonexistent_key() {
        let mut reg = TriggerRegistry::new();
        // Should be a no-op
        reg.mark_pending(b"nonexistent", 1000);
        assert!(reg.is_empty());
    }

    #[test]
    fn test_mark_fired_nonexistent_key() {
        let mut reg = TriggerRegistry::new();
        // Should be a no-op
        reg.mark_fired(b"nonexistent", 1000);
        assert!(reg.is_empty());
    }

    #[test]
    fn test_get_mut() {
        let mut reg = TriggerRegistry::new();
        reg.register(
            Bytes::from_static(b"ws1:q1"),
            make_entry(b"q1", b"CMD", 100),
        );

        let entry = reg.get_mut(b"ws1:q1").unwrap();
        entry.debounce_ms = 2000;

        assert_eq!(reg.get(b"ws1:q1").unwrap().debounce_ms, 2000);
    }
}
