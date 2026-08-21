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

/// Where a blocked `XREAD` starts reading from, as the client wrote it.
///
/// `$` cannot be turned into a number by the connection that parsed it: with
/// more than one shard the stream may live on a different thread entirely, and
/// reading a stale look-alike from the local slice would bind the waiter to
/// the wrong id. So `$` travels as [`StreamSince::Latest`] and is bound by the
/// shard that OWNS the key, at registration time.
///
/// Binding must happen with **no `.await` between the read of `last_id` and
/// `BlockingRegistry::register`** — that is the whole lost-wakeup argument for
/// moon#595. Both binding sites satisfy it by running inside a single
/// synchronous stretch of their shard's event loop:
///
/// * local keys — `handle_blocking_command{,_monoio}` binds inside
///   `with_shard_db` and registers immediately after;
/// * remote keys — the `BlockRegister` handler binds, registers, and re-runs
///   the waker, all in one SPSC message.
///
/// Redis binds `$` the same way, which is observable: block on `$`, then
/// `DEL` the stream and re-`XADD` at a LOWER id, and the waiter still times
/// out rather than being woken (measured against redis-server 8.6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSince {
    /// An explicit id. Deliver entries strictly greater than this.
    Id(crate::storage::stream::StreamId),
    /// The client wrote `$`. Not yet bound to a number.
    ///
    /// A waiter that reaches the waker still carrying `Latest` is a binding
    /// bug, and the waker treats it as "serve nothing" rather than as
    /// `0-0` — mis-binding must cost a missed wakeup, never a replay of the
    /// stream's entire history to a client that asked only for new entries.
    Latest,
}

impl StreamSince {
    /// Bind `Latest` to `last_id`; an explicit id is already bound and is
    /// returned unchanged.
    #[inline]
    pub fn bind(self, last_id: crate::storage::stream::StreamId) -> Self {
        match self {
            StreamSince::Latest => StreamSince::Id(last_id),
            bound => bound,
        }
    }

    /// The bound id, or `None` while still unbound.
    #[inline]
    pub fn id(self) -> Option<crate::storage::stream::StreamId> {
        match self {
            StreamSince::Id(id) => Some(id),
            StreamSince::Latest => None,
        }
    }
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
        /// (key, since) pairs -- read entries strictly after `since` from each
        /// stream. See [`StreamSince`] for why the id is not always known yet.
        streams: Vec<(Bytes, StreamSince)>,
        count: Option<usize>,
    },
    XReadGroup {
        group: Bytes,
        consumer: Bytes,
        streams: Vec<(Bytes, StreamSince)>,
        count: Option<usize>,
        noack: bool,
    },
}

/// Which waker is entitled to serve a given blocked command.
///
/// A waker must never CONSUME a waiter outside its own family. Before moon#535
/// each waker popped whatever was at the front of the queue, let an unhandled
/// command fall through a `_ => (None, None)` arm, and then ran the same
/// unconditional cleanup it uses for a served waiter — `remove_wait` plus
/// `reply_tx.send(None)`. That destroyed the registration and answered the
/// client a null it never earned.
///
/// The mapping is exhaustive on purpose: a new `BlockedCommand` will not
/// compile until it declares the family that may wake it, rather than silently
/// inheriting a `_` arm and becoming edible again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitFamily {
    List,
    ZSet,
    Stream,
}

impl BlockedCommand {
    /// The waker family entitled to serve this command.
    pub fn family(&self) -> WaitFamily {
        match self {
            BlockedCommand::BLPop
            | BlockedCommand::BRPop
            | BlockedCommand::BLMove { .. }
            | BlockedCommand::BLMPop { .. } => WaitFamily::List,
            BlockedCommand::BZPopMin | BlockedCommand::BZPopMax | BlockedCommand::BZMPop { .. } => {
                WaitFamily::ZSet
            }
            BlockedCommand::XRead { .. } | BlockedCommand::XReadGroup { .. } => WaitFamily::Stream,
        }
    }

    /// Bind this command's `$` for `key` against the stream that `db` holds
    /// under it (moon#595).
    ///
    /// Call on the shard that OWNS `key`, immediately before
    /// [`BlockingRegistry::register`] and with no `.await` in between — see
    /// [`StreamSince`] for why that ordering is the correctness argument and
    /// not merely a preference.
    ///
    /// Only `key`'s own entry is bound. A multi-key `XREAD` registers one copy
    /// of this command per key, each on that key's owner, and each copy is
    /// only ever consulted for the key it was registered under — so binding
    /// the siblings here would mean reading them off the wrong shard.
    ///
    /// A missing stream binds to `0-0`, matching Redis: `$` on a key that does
    /// not exist yet means "everything from now on".
    pub fn bind_stream_since(&mut self, db: &mut crate::storage::Database, key: &Bytes) {
        let BlockedCommand::XRead { streams, .. } = self else {
            // XREADGROUP reads by group cursor (`>`), never by `$`.
            return;
        };
        let Some(slot) = streams
            .iter_mut()
            .find(|(k, since)| k == key && matches!(since, StreamSince::Latest))
        else {
            return;
        };
        let last_id = match db.get_stream(key) {
            Ok(Some(stream)) => stream.last_id,
            // Missing key, or a key of the wrong type — either way there is no
            // history to skip. A wrong-typed key is answered as `-WRONGTYPE`
            // before this waiter is ever registered.
            _ => crate::storage::stream::StreamId::ZERO,
        };
        slot.1 = slot.1.bind(last_id);
    }
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

    /// Pop the first waiter of `family` from the FIFO queue for (db_index, key),
    /// leaving every other waiter in place and in order.
    ///
    /// This is what `pop_front` should always have been for the wakers
    /// (moon#535). A key's queue can hold waiters of different families at
    /// once — `BZPOPMIN k` registers on a key that does not exist yet, and a
    /// later `RPUSH k` creates it as a LIST — and the blind `pop_front` handed
    /// the list waker a zset waiter, which it then destroyed.
    ///
    /// Skipping foreign families does not violate FIFO: ordering is only
    /// meaningful among clients competing for the SAME data, and a zset waiter
    /// was never a candidate for a list push. The scan is over one key's queue
    /// and stops at the first match; no allocation.
    pub fn pop_front_of_family(
        &mut self,
        db_index: usize,
        key: &Bytes,
        family: WaitFamily,
    ) -> Option<WaitEntry> {
        let queue_key = (db_index, key.clone());
        let entry = {
            let queue = self.waiters.get_mut(&queue_key)?;
            let idx = queue.iter().position(|e| e.cmd.family() == family)?;
            queue.remove(idx)?
        };
        if self.waiters.get(&queue_key).is_none_or(|q| q.is_empty()) {
            self.waiters.remove(&queue_key);
        }
        Some(entry)
    }

    /// The waiters queued on `(db_index, key)`, in FIFO order, for a caller
    /// that needs to DECIDE before it removes anything (moon#595).
    ///
    /// [`pop_front_of_family`](Self::pop_front_of_family) is the right
    /// primitive for a DESTRUCTIVE wake: a list push has one element and
    /// exactly one waiter may have it, so taking the waiter out and answering
    /// it is the whole operation. `XREAD` is the opposite — the entry stays in
    /// the stream, so an `XADD` wakes EVERY parked reader (measured: two
    /// clients on `XREAD BLOCK 5000 STREAMS k $` both receive the entry from
    /// one `XADD` against redis-server 8.6.1), and a reader this particular
    /// `XADD` cannot serve must stay parked until its own deadline. Popping
    /// first and answering `None` on a miss — what the destructive wakers do —
    /// would unblock those readers with a premature null.
    pub fn waiters_on(&self, db_index: usize, key: &Bytes) -> Option<&VecDeque<WaitEntry>> {
        self.waiters.get(&(db_index, key.clone()))
    }

    /// Remove every waiter on `(db_index, key)` whose id is in `ids`, and hand
    /// the entries back in queue order.
    ///
    /// `ids` MUST be sorted ascending: each one is located with a binary
    /// search, and an unsorted slice makes that search MISS ids that are
    /// present, silently leaving those waiters queued (moon#620).
    ///
    /// Queue order will not do. A `wait_id` is `(shard_id << 48) | counter`,
    /// minted by the registry of the shard the waiter's CONNECTION lives on,
    /// while the queue belongs to the shard that owns the KEY — so ids reach
    /// one queue from several counters and arrive in no particular order.
    /// Sort a copy at the call site.
    ///
    /// The batching is not micro-optimisation. Removing one at a time meant a
    /// scan of the queue to find the entry, plus another inside `remove_wait`,
    /// for each of W waiters — quadratic in the number of clients tailing one
    /// stream, which is the canonical fan-out workload. Measured `XADD` p50
    /// against parked, never-servable `XREAD` waiters, before this change:
    ///
    /// ```text
    /// waiters      0     500    1000    2000    3000
    /// moon      48.9   314.7  1005.2  3511.9  7601.3 us   (24x for 6x waiters)
    /// redis    209.8   878.1   717.9  3032.9  5259.0 us   (6x  for 6x waiters)
    /// ```
    ///
    /// moon starts 4.3x faster than Redis and crosses over to slower at about
    /// 2000 tailers; at 3000 a single `XADD` held the shard thread for 7.6 ms,
    /// which on a thread-per-core runtime stalls every other client on that
    /// shard. This walks each queue once instead.
    pub fn take_waits(
        &mut self,
        db_index: usize,
        key: &Bytes,
        ids: &[u64],
    ) -> smallvec::SmallVec<[WaitEntry; 4]> {
        let mut taken = smallvec::SmallVec::new();
        if ids.is_empty() {
            return taken;
        }
        let queue_key = (db_index, key.clone());
        if let Some(queue) = self.waiters.get_mut(&queue_key) {
            let mut kept = VecDeque::with_capacity(queue.len());
            while let Some(entry) = queue.pop_front() {
                if ids.binary_search(&entry.wait_id).is_ok() {
                    taken.push(entry);
                } else {
                    kept.push_back(entry);
                }
            }
            if kept.is_empty() {
                self.waiters.remove(&queue_key);
            } else {
                *queue = kept;
            }
        }
        // Sibling registrations and the gauge. `wait_keys` names exactly the
        // keys each waiter sits on, so this touches only those queues — and a
        // stream waiter has just the one (see `is_blocking_stream_read`).
        for id in ids {
            let Some(keys) = self.wait_keys.remove(id) else {
                continue;
            };
            crate::admin::metrics_setup::record_client_unblocked();
            for sibling in keys {
                if sibling == queue_key {
                    continue;
                }
                if let Some(queue) = self.waiters.get_mut(&sibling) {
                    queue.retain(|e| e.wait_id != *id);
                    if queue.is_empty() {
                        self.waiters.remove(&sibling);
                    }
                }
            }
        }
        taken
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

    /// moon#535: a waker must be able to take ITS waiter out of a queue that
    /// also holds other families, without disturbing them.
    #[test]
    fn pop_front_of_family_skips_foreign_waiters_and_leaves_them_queued() {
        let mut reg = BlockingRegistry::new(0);
        let key = Bytes::from_static(b"mixed");
        // Order matters: the zset waiter is FIRST, which is exactly the case
        // that used to let the list waker eat it.
        for cmd in [
            BlockedCommand::BZPopMin,
            BlockedCommand::BLPop,
            BlockedCommand::BZPopMax,
        ] {
            let (tx, _rx) = crate::runtime::channel::oneshot();
            let id = reg.next_wait_id();
            reg.register(
                0,
                key.clone(),
                WaitEntry {
                    wait_id: id,
                    cmd,
                    reply_tx: tx,
                    deadline: None,
                },
            );
        }

        // The list waker reaches past the leading zset waiter to its own.
        let got = reg
            .pop_front_of_family(0, &key, WaitFamily::List)
            .expect("the list waiter is there");
        assert_eq!(got.cmd.family(), WaitFamily::List);
        // ...and there is not a second one.
        assert!(reg.pop_front_of_family(0, &key, WaitFamily::List).is_none());

        // Both zset waiters are untouched, still in registration order.
        assert!(reg.has_waiters(0, &key));
        let first = reg
            .pop_front_of_family(0, &key, WaitFamily::ZSet)
            .expect("first zset waiter survived");
        assert!(matches!(first.cmd, BlockedCommand::BZPopMin));
        let second = reg
            .pop_front_of_family(0, &key, WaitFamily::ZSet)
            .expect("second zset waiter survived");
        assert!(matches!(second.cmd, BlockedCommand::BZPopMax));

        // Draining every family empties the queue entirely.
        assert!(!reg.has_waiters(0, &key));
        assert!(
            reg.pop_front_of_family(0, &key, WaitFamily::Stream)
                .is_none()
        );
    }

    /// A queue holding ONLY foreign waiters must report "nothing for me"
    /// rather than handing one over — the wakers loop on this, so a wrong
    /// answer here is an infinite loop on the shard thread, not a wrong reply.
    #[test]
    fn pop_front_of_family_returns_none_when_only_foreign_waiters_remain() {
        let mut reg = BlockingRegistry::new(0);
        let key = Bytes::from_static(b"zsetonly");
        let (tx, _rx) = crate::runtime::channel::oneshot();
        let id = reg.next_wait_id();
        reg.register(
            0,
            key.clone(),
            WaitEntry {
                wait_id: id,
                cmd: BlockedCommand::BZPopMin,
                reply_tx: tx,
                deadline: None,
            },
        );
        assert!(reg.has_waiters(0, &key), "the queue is NOT empty");
        assert!(
            reg.pop_front_of_family(0, &key, WaitFamily::List).is_none(),
            "a non-empty queue with no list waiter must still answer None"
        );
        assert!(
            reg.has_waiters(0, &key),
            "the refused waiter must still be registered"
        );
    }

    /// The family map is the fix's whole contract; pin it so a re-classified
    /// command has to change this test deliberately.
    #[test]
    fn every_blocked_command_declares_its_waker_family() {
        use WaitFamily::*;
        let cases = [
            (BlockedCommand::BLPop, List),
            (BlockedCommand::BRPop, List),
            (
                BlockedCommand::BLMPop {
                    dir: Direction::Left,
                    count: 1,
                },
                List,
            ),
            (BlockedCommand::BZPopMin, ZSet),
            (BlockedCommand::BZPopMax, ZSet),
            (
                BlockedCommand::BZMPop {
                    min: true,
                    count: 1,
                },
                ZSet,
            ),
            (
                BlockedCommand::XRead {
                    streams: Vec::new(),
                    count: None,
                },
                Stream,
            ),
        ];
        for (cmd, want) in cases {
            assert_eq!(cmd.family(), want, "{cmd:?} is classified wrong");
        }
    }
}
