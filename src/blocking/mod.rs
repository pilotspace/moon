pub mod wakeup;

use std::collections::{HashMap, VecDeque};

use crate::protocol::Frame;
use bytes::Bytes;

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
    BLMPop {
        dir: Direction,
        count: u32,
    },
    BZPopMin,
    BZPopMax,
    BZMPop {
        min: bool,
        count: u32,
    },
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
    pub deadline: Option<std::time::Instant>,
}

/// Per-shard blocking registry. Manages FIFO wait queues keyed by (db_index, key).
///
/// Wrapped in `Rc<RefCell<...>>` by the shard, same pattern as PubSubRegistry.
pub struct BlockingRegistry {
    /// (db_index, key) -> FIFO queue of waiting clients.
    waiters: HashMap<(usize, Bytes), VecDeque<WaitEntry>>,
    /// wait_id -> list of (db_index, key) for cross-key cleanup on wakeup/timeout.
    wait_keys: HashMap<u64, Vec<(usize, Bytes)>>,
    /// Deadline index (c10k W6): min-heap of (deadline, db_index, key) for
    /// every registered waiter that HAS a deadline. `expire_timed_out` used
    /// to walk EVERY queue of EVERY blocked waiter at its 100 Hz cadence
    /// (~2M entry visits/s/shard at 10k blocked clients, tmp/C10K-REVIEW.md
    /// defect #3); with the heap it touches only queues holding an actually
    /// -due candidate. Entries are lazily invalidated: a waiter served or
    /// cancelled before its deadline leaves a stale heap entry that pops to
    /// a no-op at its original deadline — bounded by registration rate ×
    /// timeout, the same envelope as the queues themselves. Zero-timeout
    /// (block-forever) waiters never enter the heap.
    deadlines: std::collections::BinaryHeap<std::cmp::Reverse<(std::time::Instant, usize, Bytes)>>,
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
            deadlines: std::collections::BinaryHeap::new(),
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

        if let Some(deadline) = entry.deadline {
            self.deadlines
                .push(std::cmp::Reverse((deadline, db_index, key)));
        }

        self.waiters
            .entry(queue_key.clone())
            .or_insert_with(VecDeque::new)
            .push_back(entry);

        // A wait_id appears in `wait_keys` exactly while its client is
        // blocked, and a multi-key BLPOP registers the same id once per key —
        // so the gauge moves on the FIRST registration only.
        match self.wait_keys.entry(wait_id) {
            std::collections::hash_map::Entry::Occupied(mut e) => e.get_mut().push(queue_key),
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(vec![queue_key]);
                crate::admin::metrics_setup::record_client_blocked();
            }
        }
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
            crate::admin::metrics_setup::record_client_unblocked();
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
    /// Heap-driven (c10k W6): pops due deadline-index entries and scans ONLY
    /// the queues they name; every other blocked waiter is untouched. Runs at
    /// 100 Hz from the shard timer, so the no-expiry steady state must stay
    /// O(1). Returns the number of queue entries visited (observability +
    /// test surface — the old implementation visited every blocked waiter).
    pub fn expire_timed_out(&mut self, now: std::time::Instant) -> usize {
        let mut visited = 0usize;
        let mut timed_out: Vec<(u64, crate::runtime::channel::OneshotSender<Option<Frame>>)> =
            Vec::new();
        let mut timed_out_ids: Vec<u64> = Vec::new();

        while self
            .deadlines
            .peek()
            .is_some_and(|std::cmp::Reverse((d, _, _))| *d <= now)
        {
            #[allow(clippy::unwrap_used)] // peek above just proved non-empty
            let std::cmp::Reverse((_, db_index, key)) = self.deadlines.pop().unwrap();
            let queue_key = (db_index, key);
            // A missing queue is a STALE heap entry (waiter served/cancelled
            // before its deadline) — the lazy-invalidation no-op.
            let Some(queue) = self.waiters.get_mut(&queue_key) else {
                continue;
            };
            let mut i = 0;
            while i < queue.len() {
                visited += 1;
                let is_expired = queue[i].deadline.map_or(false, |d| d <= now);
                if is_expired {
                    #[allow(clippy::unwrap_used)] // i < queue.len() by loop guard
                    let entry = queue.remove(i).unwrap();
                    timed_out_ids.push(entry.wait_id);
                    timed_out.push((entry.wait_id, entry.reply_tx));
                } else {
                    i += 1;
                }
            }
            if queue.is_empty() {
                self.waiters.remove(&queue_key);
            }
        }

        // Send None (timeout) to all timed-out waiters
        for (_wait_id, reply_tx) in timed_out {
            let _ = reply_tx.send(None);
        }

        // Clean up wait_keys for timed-out ids
        // Deduplicate ids first
        timed_out_ids.sort_unstable();
        timed_out_ids.dedup();
        for id in timed_out_ids {
            if self.wait_keys.remove(&id).is_some() {
                crate::admin::metrics_setup::record_client_unblocked();
            }
        }
        visited
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
        let (tx, _rx) = crate::runtime::channel::oneshot();
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
        let (tx1, _rx1) = crate::runtime::channel::oneshot();
        reg.register(
            0,
            key.clone(),
            WaitEntry {
                wait_id: id1,
                cmd: BlockedCommand::BLPop,
                reply_tx: tx1,
                deadline: None,
            },
        );

        let id2 = reg.next_wait_id();
        let (tx2, _rx2) = crate::runtime::channel::oneshot();
        reg.register(
            0,
            key.clone(),
            WaitEntry {
                wait_id: id2,
                cmd: BlockedCommand::BRPop,
                reply_tx: tx2,
                deadline: None,
            },
        );

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

        let (tx1, _rx1) = crate::runtime::channel::oneshot();
        reg.register(
            0,
            key1.clone(),
            WaitEntry {
                wait_id: id,
                cmd: BlockedCommand::BLPop,
                reply_tx: tx1,
                deadline: None,
            },
        );
        let (tx2, _rx2) = crate::runtime::channel::oneshot();
        reg.register(
            0,
            key2.clone(),
            WaitEntry {
                wait_id: id,
                cmd: BlockedCommand::BLPop,
                reply_tx: tx2,
                deadline: None,
            },
        );

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

#[cfg(test)]
mod deadline_heap_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn entry(reg: &mut BlockingRegistry, deadline: Option<Instant>) -> (u64, WaitEntry) {
        let id = reg.next_wait_id();
        let (tx, _rx) = crate::runtime::channel::oneshot();
        (
            id,
            WaitEntry {
                wait_id: id,
                cmd: BlockedCommand::BLPop,
                reply_tx: tx,
                deadline,
            },
        )
    }

    /// c10k W6: the 100 Hz sweep must not touch block-forever waiters. The
    /// old implementation walked every queue of every blocked client.
    #[test]
    fn sweep_skips_undeadlined_waiters() {
        let mut reg = BlockingRegistry::new(0);
        let now = Instant::now();
        for i in 0..100u32 {
            let (_, e) = entry(&mut reg, None);
            reg.register(0, Bytes::from(format!("k{i}")), e);
        }
        let (_, due) = entry(&mut reg, Some(now - Duration::from_millis(1)));
        reg.register(0, Bytes::from_static(b"due-key"), due);

        let visited = reg.expire_timed_out(now);
        assert_eq!(visited, 1, "only the due queue's single entry is visited");
        assert!(!reg.has_waiters(0, &Bytes::from_static(b"due-key")));
        assert!(reg.has_waiters(0, &Bytes::from_static(b"k0")));
        assert!(reg.has_waiters(0, &Bytes::from_static(b"k99")));

        // Steady state after the sweep: nothing due, zero visits.
        assert_eq!(reg.expire_timed_out(now), 0);
    }

    /// Shared key: expire only due entries, preserve FIFO of the rest.
    #[test]
    fn shared_key_partial_expiry_preserves_fifo() {
        let mut reg = BlockingRegistry::new(0);
        let now = Instant::now();
        let key = Bytes::from_static(b"shared");
        let (_, e1) = entry(&mut reg, Some(now - Duration::from_millis(5)));
        reg.register(0, key.clone(), e1);
        let (id2, e2) = entry(&mut reg, Some(now + Duration::from_secs(60)));
        reg.register(0, key.clone(), e2);
        let (id3, e3) = entry(&mut reg, None);
        reg.register(0, key.clone(), e3);

        reg.expire_timed_out(now);
        assert_eq!(reg.pop_front(0, &key).unwrap().wait_id, id2);
        assert_eq!(reg.pop_front(0, &key).unwrap().wait_id, id3);
        assert!(reg.pop_front(0, &key).is_none());
    }

    /// A waiter served before its deadline leaves a stale heap entry — the
    /// sweep at its original deadline must be a no-op, not a panic or a
    /// spurious timeout reply.
    #[test]
    fn served_waiter_stale_heap_entry_is_noop() {
        let mut reg = BlockingRegistry::new(0);
        let now = Instant::now();
        let key = Bytes::from_static(b"served");
        let (id, e) = entry(&mut reg, Some(now + Duration::from_millis(10)));
        reg.register(0, key.clone(), e);

        let popped = reg.pop_front(0, &key).expect("served");
        assert_eq!(popped.wait_id, id);
        reg.remove_wait(id);

        let visited = reg.expire_timed_out(now + Duration::from_secs(1));
        assert_eq!(visited, 0, "stale heap entry must no-op");
    }

    /// Multi-key waiter (BLPOP k1 k2) with one shared deadline: both queue
    /// entries removed in one sweep, one visit each.
    #[test]
    fn multikey_waiter_expires_from_all_queues() {
        let mut reg = BlockingRegistry::new(0);
        let now = Instant::now();
        let id = reg.next_wait_id();
        let deadline = Some(now - Duration::from_millis(1));
        for k in [&b"mk1"[..], &b"mk2"[..]] {
            let (tx, _rx) = crate::runtime::channel::oneshot();
            reg.register(
                0,
                Bytes::copy_from_slice(k),
                WaitEntry {
                    wait_id: id,
                    cmd: BlockedCommand::BLPop,
                    reply_tx: tx,
                    deadline,
                },
            );
        }
        let visited = reg.expire_timed_out(now);
        assert_eq!(visited, 2);
        assert!(!reg.has_waiters(0, &Bytes::from_static(b"mk1")));
        assert!(!reg.has_waiters(0, &Bytes::from_static(b"mk2")));
    }
}
