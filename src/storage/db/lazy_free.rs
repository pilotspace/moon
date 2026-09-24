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
//! `Database::clear` and `recalculate_memory` rebuild `used_memory` from the
//! hot table alone; they first mark every queued item uncharged so the drain
//! cannot credit bytes the rebuild already dropped from the ledger.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::storage::bptree::BPTree;
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
        .get_or_init(|| {
            let (tx, rx) = flume::unbounded::<Work>();
            std::thread::Builder::new()
                .name("moon-lazyfree".to_string())
                .spawn(move || {
                    while let Ok(shell) = rx.recv() {
                        drop(shell);
                    }
                })
                .ok()
                .map(|_| tx)
        })
        .as_ref()
}

/// The per-database queue.
#[derive(Default)]
pub(crate) struct LazyFreeQueue {
    items: VecDeque<Item>,
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
    /// Popped from the tree's minimum, so the arena empties leaf by leaf;
    /// each pop also retires the member's `members` slot (and credits it).
    BpZset {
        tree: Box<BPTree>,
        members: HashMap<Bytes, f64>,
    },
    Stream {
        entries: std::collections::btree_map::IntoIter<StreamId, Vec<(Bytes, Bytes)>>,
        rest: Box<Stream>,
    },
}

/// Bytes `CompactValue` bills for the `Box<RedisValue>` every collection
/// lives in (mirrors `compact_value::BOXED_REDIS_VALUE_BYTES`).
const BOXED_REDIS_VALUE_BYTES: usize =
    crate::storage::mem_size::size_class(std::mem::size_of::<RedisValue>());

/// Per-entry overhead `entry_overhead` adds on top of key and value.
const ENTRY_SLOT_OVERHEAD: usize = 128;

/// `Stream::estimate_memory`'s per-entry term (MUST mirror it; the debug
/// equality check in the drain is what catches a drift).
fn stream_entry_cost(fields: &[(Bytes, Bytes)]) -> usize {
    16 + fields
        .iter()
        .map(|(f, v)| f.len() + v.len() + 48)
        .sum::<usize>()
}

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
                        tree,
                        members: *members,
                    },
                    boxed + table,
                )
            }
            RedisValue::Stream(mut s) => {
                let entries: BTreeMap<StreamId, Vec<(Bytes, Bytes)>> =
                    std::mem::take(&mut s.entries);
                (
                    Work::Stream {
                        entries: entries.into_iter(),
                        rest: s,
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
            Work::BpZset { tree, members } => {
                while n < budget {
                    let Some((score, member)) = tree.iter().next().map(|(s, m)| (s, m.clone()))
                    else {
                        break;
                    };
                    if !tree.remove(score, &member) {
                        // The tree disagrees with its own iterator. Never
                        // loop on it: drop the arena whole (bounded by its
                        // size) and let the members tail below finish.
                        **tree = BPTree::new();
                        break;
                    }
                    if members.remove(&member).is_some() {
                        credit += zset_member_cost(&member);
                    }
                    n += 1;
                }
                if n < budget {
                    // Tree empty: any member it did not index is still billed.
                    let rest = std::mem::take(members);
                    for (m, _) in rest {
                        credit += zset_member_cost(&m);
                    }
                    true
                } else {
                    false
                }
            }
            Work::Stream { entries, rest } => {
                drain!(entries, |(_, fields)| stream_entry_cost(&fields));
                if n < budget {
                    credit += rest.estimate_memory();
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

    /// Free at most `max_elements` queued elements (deterministic; tests and
    /// the time-budgeted drain above). Returns the number freed.
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
    }
}
