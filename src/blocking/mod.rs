pub mod wakeup;

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use crate::protocol::Frame;

/// Direction for LMOVE/BLMOVE pop/push operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
}

/// Which blocking command a waiter is executing.
#[derive(Debug)]
pub enum BlockedCommand {
    BLPop,
    BRPop,
    BLMove {
        destination: Bytes,
        wherefrom: Direction,
        whereto: Direction,
    },
    BZPopMin,
    BZPopMax,
    XRead {
        /// (key, last_seen_id) pairs -- read entries > last_seen_id from each stream.
        streams: Vec<(Bytes, crate::storage::stream::StreamId)>,
        count: Option<usize>,
    },
    XReadGroup {
        group: Bytes,
        consumer: Bytes,
        streams: Vec<(Bytes, crate::storage::stream::StreamId)>,
        count: Option<usize>,
        noack: bool,
    },
}

/// A single blocked client waiting for data on a key.
pub struct WaitEntry {
    /// Unique ID shared across all keys this client is waiting on (for dedup).
    pub wait_id: u64,
    /// Which blocking command variant.
    pub cmd: BlockedCommand,
    /// Oneshot sender to deliver the result. Second send attempt returns Err (natural guard).
    pub reply_tx: crate::runtime::channel::OneshotSender<Option<Frame>>,
    /// Absolute deadline (None = block forever, 0 timeout).
    pub deadline: Option<tokio::time::Instant>,
}

/// Per-shard blocking registry. Manages FIFO wait queues keyed by (db_index, key).
///
/// Wrapped in `Rc<RefCell<...>>` by the shard, same pattern as PubSubRegistry.
pub struct BlockingRegistry {
    /// (db_index, key) -> FIFO queue of waiting clients.
    waiters: HashMap<(usize, Bytes), VecDeque<WaitEntry>>,
    /// wait_id -> list of (db_index, key) for cross-key cleanup on wakeup/timeout.
    wait_keys: HashMap<u64, Vec<(usize, Bytes)>>,
    /// Monotonically increasing wait_id counter (lower 48 bits).
    next_id: u64,
    /// Shard ID encoded in upper 16 bits of wait_id for global uniqueness.
    shard_id: usize,
}

impl BlockingRegistry {
    /// Create a new empty registry with shard_id for globally unique wait_ids.
    ///
    /// Wait IDs encode `(shard_id << 48) | counter` so IDs from different shards
    /// never collide, enabling cross-shard BlockCancel to target the correct registry.
    pub fn new(shard_id: usize) -> Self {
        BlockingRegistry {
            waiters: HashMap::new(),
            wait_keys: HashMap::new(),
            next_id: 0,
            shard_id,
        }
    }

    /// Returns and increments the next wait_id.
    /// Upper 16 bits encode shard_id, lower 48 bits are a per-shard counter.
    pub fn next_wait_id(&mut self) -> u64 {
        let id = ((self.shard_id as u64) << 48) | self.next_id;
        self.next_id += 1;
        id
    }

    /// Register a waiter on a specific (db_index, key).
    /// Push to back of the FIFO queue. Also records in wait_keys for cross-key cleanup.
    pub fn register(&mut self, db_index: usize, key: Bytes, entry: WaitEntry) {
        let wait_id = entry.wait_id;
        let queue_key = (db_index, key.clone());

        self.waiters
            .entry(queue_key.clone())
            .or_insert_with(VecDeque::new)
            .push_back(entry);

        self.wait_keys
            .entry(wait_id)
            .or_insert_with(Vec::new)
            .push(queue_key);
    }

    /// Pop the first waiter from the FIFO queue for (db_index, key).
    /// Removes the key from the waiters map if the queue becomes empty.
    pub fn pop_front(&mut self, db_index: usize, key: &Bytes) -> Option<WaitEntry> {
        let queue_key = (db_index, key.clone());
        let entry = {
            let queue = self.waiters.get_mut(&queue_key)?;
            let entry = queue.pop_front()?;
            entry
        };
        // Clean up empty queue
        if self.waiters.get(&queue_key).map_or(true, |q| q.is_empty()) {
            self.waiters.remove(&queue_key);
        }
        Some(entry)
    }

    /// Remove all entries with this wait_id from ALL keys they are registered on.
    /// Used after a waiter is woken or times out to clean up cross-key registrations.
    pub fn remove_wait(&mut self, wait_id: u64) {
        if let Some(keys) = self.wait_keys.remove(&wait_id) {
            for queue_key in keys {
                if let Some(queue) = self.waiters.get_mut(&queue_key) {
                    queue.retain(|e| e.wait_id != wait_id);
                    if queue.is_empty() {
                        self.waiters.remove(&queue_key);
                    }
                }
            }
        }
    }

    /// Check if any waiters exist for this (db_index, key).
    pub fn has_waiters(&self, db_index: usize, key: &Bytes) -> bool {
        self.waiters
            .get(&(db_index, key.clone()))
            .map_or(false, |q| !q.is_empty())
    }

    /// Expire all timed-out waiters. Sends None through their reply channels.
    ///
    /// Two-pass approach: first collect timed-out entries, then clean up.
    pub fn expire_timed_out(&mut self, now: tokio::time::Instant) {
        // Pass 1: collect timed-out (wait_id, reply_tx) pairs
        let mut timed_out: Vec<(u64, tokio::sync::oneshot::Sender<Option<Frame>>)> = Vec::new();
        let mut timed_out_ids: Vec<u64> = Vec::new();

        for queue in self.waiters.values_mut() {
            let mut i = 0;
            while i < queue.len() {
                let is_expired = queue[i]
                    .deadline
                    .map_or(false, |d| d <= now);
                if is_expired {
                    let entry = queue.remove(i).unwrap();
                    timed_out_ids.push(entry.wait_id);
                    timed_out.push((entry.wait_id, entry.reply_tx));
                } else {
                    i += 1;
                }
            }
        }

        // Remove empty queues
        self.waiters.retain(|_, q| !q.is_empty());

        // Send None (timeout) to all timed-out waiters
        for (_wait_id, reply_tx) in timed_out {
            let _ = reply_tx.send(None);
        }

        // Clean up wait_keys for timed-out ids
        // Deduplicate ids first
        timed_out_ids.sort_unstable();
        timed_out_ids.dedup();
        for id in timed_out_ids {
            self.wait_keys.remove(&id);
        }
    }
}

impl Default for BlockingRegistry {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_pop_front() {
        let mut reg = BlockingRegistry::new(0);
        let id = reg.next_wait_id();
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let entry = WaitEntry {
            wait_id: id,
            cmd: BlockedCommand::BLPop,
            reply_tx: tx,
            deadline: None,
        };
        let key = Bytes::from_static(b"mylist");
        reg.register(0, key.clone(), entry);
        assert!(reg.has_waiters(0, &key));

        let popped = reg.pop_front(0, &key);
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().wait_id, id);
        assert!(!reg.has_waiters(0, &key));
    }

    #[test]
    fn test_fifo_order() {
        let mut reg = BlockingRegistry::new(0);
        let key = Bytes::from_static(b"mylist");

        let id1 = reg.next_wait_id();
        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        reg.register(0, key.clone(), WaitEntry {
            wait_id: id1,
            cmd: BlockedCommand::BLPop,
            reply_tx: tx1,
            deadline: None,
        });

        let id2 = reg.next_wait_id();
        let (tx2, _rx2) = tokio::sync::oneshot::channel();
        reg.register(0, key.clone(), WaitEntry {
            wait_id: id2,
            cmd: BlockedCommand::BRPop,
            reply_tx: tx2,
            deadline: None,
        });

        let first = reg.pop_front(0, &key).unwrap();
        assert_eq!(first.wait_id, id1);
        let second = reg.pop_front(0, &key).unwrap();
        assert_eq!(second.wait_id, id2);
    }

    #[test]
    fn test_remove_wait_cross_key() {
        let mut reg = BlockingRegistry::new(0);
        let id = reg.next_wait_id();
        let key1 = Bytes::from_static(b"list1");
        let key2 = Bytes::from_static(b"list2");

        let (tx1, _rx1) = tokio::sync::oneshot::channel();
        reg.register(0, key1.clone(), WaitEntry {
            wait_id: id,
            cmd: BlockedCommand::BLPop,
            reply_tx: tx1,
            deadline: None,
        });
        let (tx2, _rx2) = tokio::sync::oneshot::channel();
        reg.register(0, key2.clone(), WaitEntry {
            wait_id: id,
            cmd: BlockedCommand::BLPop,
            reply_tx: tx2,
            deadline: None,
        });

        assert!(reg.has_waiters(0, &key1));
        assert!(reg.has_waiters(0, &key2));

        reg.remove_wait(id);

        assert!(!reg.has_waiters(0, &key1));
        assert!(!reg.has_waiters(0, &key2));
    }

    #[test]
    fn test_has_waiters_empty() {
        let reg = BlockingRegistry::new(0);
        assert!(!reg.has_waiters(0, &Bytes::from_static(b"nokey")));
    }
}
