//! Per-database lazy-free queue (moon#1190).
//!
//! Removing a large collection used to cost the shard thread two full passes
//! over it, back to back, while every other connection on the shard waited:
//! the `entry_overhead` ledger walk (`RedisValue::estimate_memory` sums every
//! element) and then the drop itself (one `free` per element). On monoio
//! `UNLINK` did both inline — redis frees such a value on a background
//! thread and answers in O(1).
//!
//! A value handed to this queue is taken apart a bounded number of elements
//! at a time by [`Database::drain_lazy_free`], which the shard's periodic tick
//! calls under a small time budget. Walk and free are FUSED: each element's
//! ledger cost is computed from the element in hand immediately before it is
//! dropped, with the same per-element functions `estimate_memory` sums, so
//! the removal costs one pass instead of two and none of it runs inside the
//! command.
//!
//! ## Accounting
//!
//! An item enqueued by [`Database::unlink`] or the expiry sweep is still
//! CHARGED: `used_memory` keeps counting it until the drain frees it, and is
//! credited element by element as the memory is actually released. That is
//! the truthful reading (the bytes ARE still resident) and it is what keeps
//! the enqueue O(1): the exact total is only known once every element has
//! been looked at. Under `debug_assertions` (and so in every test build) the
//! enqueue computes `entry_overhead` the slow way and the drain asserts the
//! credits add up to exactly it — the equality `RedisValue::estimate_memory`
//! and this decomposition must keep.
//!
//! An item enqueued by eviction is already credited (the eviction loop needs
//! the credit synchronously to know when to stop), so only its drop is
//! deferred.
//!
//! ## Memory pressure (moon#1221 review F1)
//!
//! Charged-but-queued bytes are memory the keyspace has already released.
//! A maxmemory / OOM gate that found the ledger over budget used to act on
//! them as if they were live — evicting live keys (`allkeys-*`), refusing
//! the write (`noeviction`) or finding no victim (`volatile-*`) right after
//! an UNLINK or expiry of a large value, where main had credited the value
//! synchronously and the write fit. Every gate now first calls
//! [`Database::reclaim_lazy_free`], which drains the queue synchronously, in
//! bounded batches, until the overshoot is covered or nothing charged is
//! left — BEFORE it picks a victim or answers OOM. UNLINK stays O(1); the
//! walk is paid only under memory pressure, and never exceeds what main
//! paid inside the UNLINK itself. It is the same drain as the tick's, so
//! each byte is still credited exactly once.
//!
//! `Database::clear` and `recalculate_memory` rebuild `used_memory` from the
//! hot table alone; they first mark every queued item uncharged so the drain
//! cannot credit bytes the rebuild already dropped from the ledger.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::storage::bptree::NodeDrain;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::db::{
    Database, hash_field_cost, hash_ttl_field_cost, legacy_zset_member_cost, list_elem_cost,
    set_member_cost, set_table_bytes, zset_member_cost, zset_table_bytes,
};
use crate::storage::entry::{Entry, RedisValue, SetValue};
use crate::storage::stream::{Stream, StreamId};

/// A collection with MORE than this many elements is freed lazily; anything
/// smaller (and every string, listpack and intset, whose free is O(1)) is
/// freed inline. Redis's `LAZYFREE_THRESHOLD` and UNLINK's historical cut.
pub const LAZY_FREE_THRESHOLD: usize = 64;

/// Shard-thread time one periodic tick may spend draining lazy-free queues.
/// The tick is 1 ms, so a 1M-field hash (~60 ms of frees) is released over
/// ~0.25 s of wall time while no single tick holds the shard for more than
/// this plus one step.
pub const LAZY_FREE_TICK_BUDGET: Duration = Duration::from_micros(250);

/// Elements freed between two clock reads of the drain.
const STEP_ELEMENTS: usize = 256;

/// Elements one batch of the memory-pressure drain frees between two checks
/// of how much it has credited ([`Database::reclaim_lazy_free`]).
const PRESSURE_BATCH_ELEMENTS: usize = 4_096;

/// Largest value whose credited total is checked against `entry_overhead`
/// in debug/test builds (the check itself is an O(n) walk).
#[cfg(debug_assertions)]
const DEBUG_VERIFY_MAX_ELEMENTS: usize = 65_536;

/// Queued items across every database in the process. The tick's fast bail:
/// one relaxed load when nothing is queued anywhere.
static PENDING_ITEMS: AtomicUsize = AtomicUsize::new(0);

/// `true` if any database in the process has lazy-free work queued.
#[inline]
pub fn lazy_free_pending_anywhere() -> bool {
    PENDING_ITEMS.load(Relaxed) != 0
}

/// Element count of `entry` when it is large enough to free lazily.
pub(crate) fn lazy_free_weight(entry: &Entry) -> Option<usize> {
    let n = match entry.value.as_redis_value() {
        RedisValueRef::Hash(m) => m.len(),
        RedisValueRef::HashWithTtl { fields, ttls, .. } => fields.len() + ttls.len(),
        RedisValueRef::List(l) => l.len(),
        RedisValueRef::Set(s) => s.len(),
        RedisValueRef::SortedSet { members, .. } => members.len(),
        RedisValueRef::SortedSetBPTree { members, .. } => members.len(),
        RedisValueRef::Stream(s) => s.entries.len(),
        RedisValueRef::String(_)
        | RedisValueRef::HashListpack(_)
        | RedisValueRef::ListListpack(_)
        | RedisValueRef::SetListpack(_)
        | RedisValueRef::SetIntset(_)
        | RedisValueRef::SortedSetListpack(_) => 0,
    };
    (n > LAZY_FREE_THRESHOLD).then_some(n)
}

/// A value this large leaves behind a container SHELL — the emptied hash
/// table, deque buffer, index vector or B+tree arena — whose single backing
/// allocation is itself huge (~160 MB for a 1M-field hash table). Freeing it
/// is one `madvise(MADV_DONTNEED)` of the whole extent: measured at 29 ms on
/// the shard thread (strace, release build), the one remaining stall of a
/// lazily freed 1M-field UNLINK. Shells of values at least this large are
/// released on the helper thread below instead.
const OFFLOAD_SHELL_ELEMENTS: usize = 65_536;

/// Free an emptied container shell: inline when small, on the
/// `moon-lazyfree` helper thread when large (or inline if that thread could
/// not be started). Its bytes were already credited by the drain.
fn release_shell(work: Work, weight: usize) {
    if weight >= OFFLOAD_SHELL_ELEMENTS
        && let Some(tx) = shell_dropper()
    {
        // A send only fails if the helper is gone; the shell then comes back
        // in the error and is dropped here.
        let _ = tx.send(work);
    }
}

/// The lazily started helper that drops large shells, off every shard
/// thread. One per process, idle (parked in `recv`) when unused.
fn shell_dropper() -> Option<&'static flume::Sender<Work>> {
    static DROPPER: std::sync::OnceLock<Option<flume::Sender<Work>>> = std::sync::OnceLock::new();
    DROPPER
        .get_or_init(|| spawn_shell_dropper("moon-lazyfree"))
        .as_ref()
}

/// Start a shell-dropping thread named `name`; `None` if the OS refused.
/// Split from [`shell_dropper`] so a test can start one from a thread it
/// controls — the process-wide one is spawned once, by whichever shard
/// thread happens to need it first.
fn spawn_shell_dropper(name: &str) -> Option<flume::Sender<Work>> {
    let (tx, rx) = flume::unbounded::<Work>();
    let label = name.to_string();
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            // moon#1221 review INTEG-4: this thread is spawned from whichever
            // shard thread first frees a huge value — a thread pinned to one
            // core — and Linux threads inherit their creator's affinity mask.
            // Re-pin onto the non-shard core set first, as every other
            // auxiliary thread does (`snapshot_stream`, `cdc/read_pool`), or
            // the helper is confined to that shard's core and competes with it
            // for the very frees it exists to take off it. A no-op off Linux,
            // with `MOON_NO_AUX_PIN=1`, or when there is no non-shard core.
            crate::shard::numa::pin_current_aux_thread(&label);
            while let Ok(shell) = rx.recv() {
                drop(shell);
            }
        })
        .ok()
        .map(|_| tx)
}

/// The per-database queue.
#[derive(Default)]
pub(crate) struct LazyFreeQueue {
    items: VecDeque<Item>,
    /// Queued items whose bytes are still counted in `used_memory` — what a
    /// memory gate can win back by draining (moon#1221 review F1). O(1) to
    /// ask; an eviction victim (already credited) never counts.
    charged_items: usize,
}

impl Drop for LazyFreeQueue {
    fn drop(&mut self) {
        // A dropped database frees its queue synchronously; keep the
        // process-wide counter honest.
        if !self.items.is_empty() {
            PENDING_ITEMS.fetch_sub(self.items.len(), Relaxed);
        }
    }
}

struct Item {
    work: Work,
    /// Element count at enqueue — decides where the emptied shell is freed.
    weight: usize,
    /// Credited once the last element is gone: the key, the entry, the
    /// boxes, and the tables charged from capacity — everything the ledger
    /// bills that is not per element.
    tail_credit: usize,
    /// Whether these bytes are still counted in `used_memory`.
    charged: bool,
    /// `entry_overhead` computed the slow way at enqueue (debug/test only,
    /// and only for values up to [`DEBUG_VERIFY_MAX_ELEMENTS`]).
    #[cfg(debug_assertions)]
    expected: Option<usize>,
    /// Credited so far (debug/test only).
    #[cfg(debug_assertions)]
    credited: usize,
}

/// A value taken apart for incremental freeing. Every variant owns its
/// elements (owned iterators), so a step never needs the database.
enum Work {
    Hash(std::collections::hash_map::IntoIter<Bytes, Bytes>),
    HashTtl {
        fields: std::collections::hash_map::IntoIter<Bytes, Bytes>,
        ttls: std::collections::hash_map::IntoIter<Bytes, u64>,
    },
    List(std::collections::vec_deque::IntoIter<Bytes>),
    Set(indexmap::set::IntoIter<Bytes>),
    LegacyZset {
        members: std::collections::hash_map::IntoIter<Bytes, f64>,
        scores: std::collections::btree_map::IntoIter<(OrderedFloat<f64>, Bytes), ()>,
    },
    /// moon#1221 review F4: every member is credited once, from the
    /// `members` map; the B+tree — which holds the other reference to each
    /// member's bytes — is then released node by node (bounded per step).
    /// It used to be emptied by popping its minimum: one rebalancing
    /// `BPTree::remove`, a key copy and a `Bytes` clone per member, 6.0–6.7x
    /// the cost of a plain drop. The arena's emptied allocation is the shell.
    /// A member is credited when its map slot goes, while the tree's
    /// reference still keeps its payload resident until the node phase of
    /// the same item — a bounded number of steps later. (Tree first would
    /// credit nothing for most of the drain: the members map would still
    /// hold every payload.)
    BpZset {
        members: std::collections::hash_map::IntoIter<Bytes, f64>,
        nodes: NodeDrain,
    },
    /// moon#1163: a stream is credited exactly what the ledger BILLED for it
    /// (`Stream::billed_memory`) — each freed entry at its `entry_cost`,
    /// capped by what is left of the bill, and the remainder (groups, PELs,
    /// the fixed part) when the entries are gone. A re-measure would credit
    /// growth nobody charged.
    Stream {
        entries: std::collections::btree_map::IntoIter<StreamId, Vec<(Bytes, Bytes)>>,
        /// The rest of the value (groups, PELs), freed with the finished item.
        _rest: Box<Stream>,
        /// Billed bytes not yet credited.
        remaining: usize,
    },
}

/// Bytes `CompactValue` bills for the `Box<RedisValue>` every collection
/// lives in (mirrors `compact_value::BOXED_REDIS_VALUE_BYTES`).
const BOXED_REDIS_VALUE_BYTES: usize =
    crate::storage::mem_size::size_class(std::mem::size_of::<RedisValue>());

/// Per-entry overhead `entry_overhead` adds on top of key and value.
const ENTRY_SLOT_OVERHEAD: usize = 128;

impl Work {
    /// Take `value` apart. Returns the work and the O(1)-known part of its
    /// ledger cost (boxed payloads + capacity-charged tables).
    fn from_value(value: RedisValue) -> Result<(Work, usize), RedisValue> {
        let boxed = value.boxed_payload_bytes();
        Ok(match value {
            RedisValue::Hash(map) => (Work::Hash((*map).into_iter()), boxed),
            RedisValue::HashWithTtl { fields, ttls, .. } => (
                Work::HashTtl {
                    fields: (*fields).into_iter(),
                    ttls: (*ttls).into_iter(),
                },
                boxed,
            ),
            RedisValue::List(list) => (Work::List(list.into_iter()), boxed),
            RedisValue::Set(set) => {
                let table = set_table_bytes(&set);
                let set: SetValue = *set;
                (Work::Set(set.into_iter()), boxed + table)
            }
            RedisValue::SortedSet { members, scores } => (
                Work::LegacyZset {
                    members: (*members).into_iter(),
                    scores: (*scores).into_iter(),
                },
                boxed,
            ),
            RedisValue::SortedSetBPTree { tree, members } => {
                let table = zset_table_bytes(&members, &tree);
                (
                    Work::BpZset {
                        members: (*members).into_iter(),
                        nodes: (*tree).into_node_drain(),
                    },
                    boxed + table,
                )
            }
            RedisValue::Stream(mut s) => {
                let remaining = s.billed_memory();
                let entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>> =
                    std::mem::take(&mut s.entries);
                (
                    Work::Stream {
                        entries: entries.into_iter(),
                        _rest: s,
                        remaining,
                    },
                    boxed,
                )
            }
            other => return Err(other),
        })
    }

    /// Free up to `budget` elements. Returns (bytes to credit, elements
    /// freed, finished). A finished `Stream` also credits what is left of the
    /// stream (its fixed part and consumer groups), measured now that its
    /// entries are gone.
    fn step(&mut self, budget: usize) -> (usize, usize, bool) {
        let mut credit = 0usize;
        let mut n = 0usize;
        macro_rules! drain {
            ($it:expr, |$x:pat_param| $cost:expr) => {{
                while n < budget {
                    match $it.next() {
                        Some($x) => {
                            credit += $cost;
                            n += 1;
                        }
                        None => break,
                    }
                }
            }};
        }
        let done = match self {
            Work::Hash(it) => {
                drain!(it, |(f, v)| hash_field_cost(&f, &v));
                n < budget
            }
            Work::HashTtl { fields, ttls } => {
                drain!(fields, |(f, v)| hash_field_cost(&f, &v));
                drain!(ttls, |(f, _)| hash_ttl_field_cost(&f));
                n < budget
            }
            Work::List(it) => {
                drain!(it, |e| list_elem_cost(&e));
                n < budget
            }
            Work::Set(it) => {
                drain!(it, |m| set_member_cost(&m));
                n < budget
            }
            Work::LegacyZset { members, scores } => {
                drain!(members, |(m, _)| legacy_zset_member_cost(&m));
                drain!(scores, |_| 0);
                n < budget
            }
            Work::BpZset { members, nodes } => {
                // Members first: each is billed once, whatever the tree holds.
                drain!(members, |(m, _)| zset_member_cost(&m));
                // Then the tree, whose nodes hold the last reference to every
                // member's bytes. Its arena was billed from capacity and is in
                // `tail_credit`, so the nodes credit nothing here. A node is
                // released whole: a step may overshoot by one node.
                let mut finished = false;
                while n < budget {
                    let (released, done) = nodes.drop_nodes(budget - n);
                    n += released;
                    if done {
                        finished = true;
                        break;
                    }
                }
                finished
            }
            // The groups go with `_rest` when the finished item is dropped.
            Work::Stream {
                entries, remaining, ..
            } => {
                drain!(entries, |(_, fields)| {
                    let c = crate::storage::stream::entry_cost(&fields).min(*remaining);
                    *remaining -= c;
                    c
                });
                if n < budget {
                    credit += std::mem::take(remaining);
                    true
                } else {
                    false
                }
            }
        };
        (credit, n, done)
    }
}

impl Database {
    /// Dispose of an entry that has just left the hot table.
    ///
    /// `charged`: its bytes are still in `used_memory` (they are credited as
    /// they are freed) — `false` when the caller already credited them.
    /// A value below [`LAZY_FREE_THRESHOLD`] is credited and dropped inline;
    /// a larger one is queued for [`Self::drain_lazy_free`].
    pub(crate) fn lazy_free_or_drop(&mut self, key_len: usize, entry: Entry, charged: bool) {
        if lazy_free_weight(&entry).is_none() {
            if charged {
                self.used_memory = self
                    .used_memory
                    .saturating_sub(super::entry_overhead_len(key_len, &entry));
            }
            return;
        }
        // The equality the decomposition must keep, checked in debug/test
        // builds. Capped so a debug server UNLINKing a million-field value
        // still returns in O(1) — the property this module exists for.
        #[cfg(debug_assertions)]
        let expected = lazy_free_weight(&entry)
            .is_some_and(|n| n <= DEBUG_VERIFY_MAX_ELEMENTS)
            .then(|| super::entry_overhead_len(key_len, &entry));
        let weight = lazy_free_weight(&entry).unwrap_or(0);
        let entry_fixed = key_len + ENTRY_SLOT_OVERHEAD + BOXED_REDIS_VALUE_BYTES;
        let (work, fixed) = match Work::from_value(entry.value.into_redis_value()) {
            Ok(w) => w,
            // Unreachable: `lazy_free_weight` answers Some only for the
            // decomposable kinds. Keep the ledger honest regardless.
            Err(other) => {
                if charged {
                    self.used_memory = self
                        .used_memory
                        .saturating_sub(entry_fixed + other.estimate_memory());
                }
                return;
            }
        };
        if charged {
            self.lazy_free.charged_items += 1;
        }
        self.lazy_free.items.push_back(Item {
            work,
            weight,
            tail_credit: entry_fixed + fixed,
            charged,
            #[cfg(debug_assertions)]
            expected,
            #[cfg(debug_assertions)]
            credited: 0,
        });
        PENDING_ITEMS.fetch_add(1, Relaxed);
    }

    /// Items queued in this database.
    #[inline]
    pub fn lazy_free_len(&self) -> usize {
        self.lazy_free.items.len()
    }

    /// `true` when draining the queue would lower `used_memory`: some queued
    /// value's bytes are still charged. One field read — the memory gates'
    /// fast bail (moon#1221 review F1).
    #[inline]
    pub fn lazy_free_reclaimable(&self) -> bool {
        self.lazy_free.charged_items != 0
    }

    /// The memory gates' drain (moon#1221 review F1): free queued values NOW,
    /// in batches of [`PRESSURE_BATCH_ELEMENTS`], until at least `excess`
    /// bytes have been credited back to `used_memory` or no queued value is
    /// still charged. Returns the bytes credited.
    ///
    /// Called by every maxmemory / OOM gate that finds the ledger over
    /// budget, BEFORE it evicts or refuses: the queued bytes are memory the
    /// keyspace already released. Items ahead of the first charged one (an
    /// eviction victim, credited when it was evicted) are freed on the way —
    /// work the tick owed anyway. Stops as soon as the overshoot is covered,
    /// so a write that is a few bytes over pays a few thousand elements, not
    /// the whole value; the tick frees the rest. Same drain as the tick's,
    /// so a byte is never credited twice (debug-asserted per item in
    /// [`Self::drain_lazy_free_elements`]), and a huge value's emptied shell
    /// still goes to the `moon-lazyfree` helper.
    pub fn reclaim_lazy_free(&mut self, excess: usize) -> usize {
        let start = self.used_memory;
        while self.lazy_free.charged_items != 0 && start.saturating_sub(self.used_memory) < excess {
            let queued = self.lazy_free.items.len();
            if self.drain_lazy_free_elements(PRESSURE_BATCH_ELEMENTS) == 0
                && self.lazy_free.items.len() == queued
            {
                // A batch that freed nothing and finished nothing cannot
                // happen while an item is queued; never spin on it.
                break;
            }
        }
        debug_assert_eq!(
            self.lazy_free.charged_items,
            self.lazy_free.items.iter().filter(|i| i.charged).count(),
            "lazy-free charged-item count drifted from the queue"
        );
        start.saturating_sub(self.used_memory)
    }

    /// Free queued values until `deadline` (checked every [`STEP_ELEMENTS`]
    /// elements) or the queue is empty. Returns `true` if work remains.
    pub fn drain_lazy_free(&mut self, deadline: Instant) -> bool {
        while !self.lazy_free.items.is_empty() {
            self.drain_lazy_free_elements(STEP_ELEMENTS);
            if Instant::now() >= deadline {
                break;
            }
        }
        !self.lazy_free.items.is_empty()
    }

    /// Free about `max_elements` queued elements (deterministic; tests and
    /// the time-budgeted drain above) — a B+tree node is released whole, so a
    /// call may overshoot by one node (17). Returns the number freed.
    pub fn drain_lazy_free_elements(&mut self, max_elements: usize) -> usize {
        let mut freed = 0usize;
        while freed < max_elements {
            let Some(item) = self.lazy_free.items.front_mut() else {
                break;
            };
            let (mut credit, n, done) = item.work.step(max_elements - freed);
            freed += n;
            if done {
                credit += item.tail_credit;
            }
            if item.charged {
                self.used_memory = self.used_memory.saturating_sub(credit);
                #[cfg(debug_assertions)]
                {
                    item.credited += credit;
                }
            }
            if !done {
                break;
            }
            #[cfg(debug_assertions)]
            if item.charged
                && let Some(expected) = item.expected
            {
                debug_assert_eq!(
                    item.credited, expected,
                    "lazy-free credited {} bytes for a value `entry_overhead` billed at {} \
                     (moon#1190: the per-element decomposition drifted from \
                     RedisValue::estimate_memory)",
                    item.credited, expected
                );
            }
            if let Some(done_item) = self.lazy_free.items.pop_front() {
                if done_item.charged {
                    self.lazy_free.charged_items = self.lazy_free.charged_items.saturating_sub(1);
                }
                release_shell(done_item.work, done_item.weight);
            }
            PENDING_ITEMS.fetch_sub(1, Relaxed);
        }
        freed
    }

    /// The ledger is about to be rebuilt from the hot table alone (`clear`,
    /// `recalculate_memory`): nothing still queued may be credited again.
    pub(super) fn lazy_free_forget_charges(&mut self) {
        for item in &mut self.lazy_free.items {
            item.charged = false;
        }
        self.lazy_free.charged_items = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use ordered_float::OrderedFloat;

    use crate::storage::bptree::BPTree;
    use crate::storage::compact_value::CompactValue;
    use crate::storage::db::Database;
    use crate::storage::entry::{Entry, RedisValue};

    fn zset_entry(n: usize, probe: &Bytes) -> Entry {
        let mut tree = BPTree::new();
        let mut members = HashMap::new();
        for i in 0..n {
            let m = Bytes::from(format!("member-{i:08}").into_bytes());
            tree.insert(OrderedFloat(i as f64), m.clone());
            members.insert(m, i as f64);
        }
        tree.insert(OrderedFloat(-1.0), probe.clone());
        members.insert(probe.clone(), -1.0);
        let mut e = Entry::new_string(Bytes::new());
        e.value = CompactValue::from_redis_value(RedisValue::SortedSetBPTree {
            tree: Box::new(tree),
            members: Box::new(members),
        });
        e
    }

    fn hash_entry(n: usize) -> Entry {
        let mut h = HashMap::new();
        for i in 0..n {
            h.insert(
                Bytes::from(format!("member-{i:08}").into_bytes()),
                Bytes::from_static(b"v"),
            );
        }
        let mut e = Entry::new_string(Bytes::new());
        e.value = CompactValue::from_redis_value(RedisValue::Hash(Box::new(h)));
        e
    }

    // ── moon#1221 review F4 ──────────────────────────────────────────────

    /// A B+tree zset is freed by releasing its `members` map element by
    /// element and its node arena node by node — never by popping the
    /// minimum with one rebalancing `BPTree::remove` (plus a key copy and a
    /// `Bytes` clone) per member, which cost 6.0–6.7x a plain drop.
    #[test]
    fn a_bptree_zset_is_freed_without_a_tree_remove_per_member() {
        let probe = Bytes::from(b"probe-payload-held-by-the-test".to_vec());
        let mut db = Database::new();
        db.set(b"z", zset_entry(5_000, &probe));
        assert!(db.unlink(b"z"));
        let _ = crate::storage::bptree::take_remove_calls();
        let mut steps = 0usize;
        while db.lazy_free_len() != 0 {
            let freed = db.drain_lazy_free_elements(256);
            assert!(
                freed <= 256 + 32,
                "one step freed {freed} elements (budget 256, at most one node over)"
            );
            steps += 1;
            assert!(steps < 10_000, "no progress");
        }
        let removes = crate::storage::bptree::take_remove_calls();
        assert_eq!(
            removes, 0,
            "the drain called BPTree::remove {removes} times"
        );
        assert!(steps >= 5_000 / 256, "the value must span bounded steps");
        assert!(probe.is_unique(), "the drain must free every member");
        assert_eq!(
            db.estimated_memory(),
            0,
            "credited exactly what SET charged"
        );
    }

    /// Measurement, not a gate (`--ignored --nocapture`): shard-thread time
    /// of UNLINK + drain against a plain drop, per kind, N = 100K. Relative
    /// numbers from one process, back to back.
    #[test]
    #[ignore = "measurement; run with --ignored --nocapture"]
    fn measure_lazy_free_cost_against_a_plain_drop() {
        use std::time::{Duration, Instant};
        const N: usize = 100_000;
        fn lazy_cost(e: Entry) -> Duration {
            let mut db = Database::new();
            db.set(b"k", e);
            let t = Instant::now();
            assert!(db.unlink(b"k"));
            while db.lazy_free_len() != 0 {
                db.drain_lazy_free_elements(256);
            }
            t.elapsed()
        }
        fn drop_cost(e: Entry) -> Duration {
            let t = Instant::now();
            drop(e);
            t.elapsed()
        }
        let probe = Bytes::from_static(b"p");
        let zset = || zset_entry(N, &probe);
        let hash = || hash_entry(N);
        for (kind, make) in [
            ("hash", &hash as &dyn Fn() -> Entry),
            ("zset-bptree", &zset as &dyn Fn() -> Entry),
        ] {
            for rep in 0..3 {
                let d = drop_cost(make());
                let l = lazy_cost(make());
                eprintln!(
                    "LAZY-FREE-COST N={N} {kind} rep {rep}: drop {d:?} lazy {l:?} ratio {:.2}",
                    l.as_secs_f64() / d.as_secs_f64().max(1e-9)
                );
            }
        }
    }

    // ── moon#1221 review INTEG-4 ─────────────────────────────────────────

    /// The helper is spawned lazily, from whichever shard thread first frees
    /// a huge value — a thread pinned to ONE core. Linux threads inherit
    /// their creator's affinity mask, so without a re-pin the helper is born
    /// confined to that shard's core and competes with it for every shell it
    /// frees (observed: `Cpus_allowed_list: 0`).
    #[cfg(target_os = "linux")]
    #[test]
    fn the_helper_thread_does_not_inherit_the_spawning_shard_core() {
        fn affinity(tid: &str) -> Option<String> {
            let s = std::fs::read_to_string(format!("/proc/self/task/{tid}/status")).ok()?;
            s.lines()
                .find_map(|l| l.strip_prefix("Cpus_allowed_list:"))
                .map(|v| v.trim().to_string())
        }
        fn thread_affinity(name: &str) -> Option<String> {
            std::fs::read_dir("/proc/self/task")
                .ok()?
                .flatten()
                .map(|t| t.file_name().to_string_lossy().into_owned())
                .find(|tid| {
                    std::fs::read_to_string(format!("/proc/self/task/{tid}/comm"))
                        .is_ok_and(|c| c.trim() == name)
                })
                .and_then(|tid| affinity(&tid))
        }
        crate::shard::numa::init_aux_pinning(1);
        if crate::shard::numa::aux_core_at(0).is_none() {
            eprintln!("SKIPPED: no non-shard core to pin auxiliary threads to");
            return;
        }
        const NAME: &str = "moon-lazyfree-t";
        let outcome = std::thread::spawn(|| {
            crate::shard::numa::pin_to_core(0);
            let me = std::fs::read_link("/proc/thread-self")
                .ok()
                .and_then(|p| p.file_name().map(|f| f.to_string_lossy().into_owned()))
                .unwrap_or_default();
            let mine = affinity(&me);
            if mine.as_deref() != Some("0") {
                return Err(format!("cannot pin the test thread (affinity {mine:?})"));
            }
            let tx = super::spawn_shell_dropper(NAME).expect("spawn the helper");
            // The helper re-pins itself as the first act of its closure,
            // after `spawn` returned: poll until it has, or give up.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
            let mut seen = thread_affinity(NAME);
            while std::time::Instant::now() < deadline && seen.as_deref().is_none_or(|a| a == "0") {
                std::thread::sleep(std::time::Duration::from_millis(10));
                seen = thread_affinity(NAME);
            }
            drop(tx);
            Ok(seen)
        })
        .join()
        .expect("test thread");
        match outcome {
            Err(skip) => eprintln!("SKIPPED: {skip}"),
            Ok(seen) => {
                let seen = seen.expect("the helper thread exists");
                assert_ne!(
                    seen, "0",
                    "moon-lazyfree inherited the spawning shard's single-core affinity"
                );
            }
        }
    }
}
