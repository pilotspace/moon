//! moon#1295: a large collection's epoch-start image is STREAMED into the
//! snapshot before an in-place write changes it, instead of deep-cloned.
//!
//! # The problem
//!
//! moon's BGSAVE is fork-less copy-on-write: a write to a key the epoch walk
//! has not reached captures the key's epoch-start value first
//! ([`super::capture::capture_key`]). For an in-place write — `HSET` of one
//! field, `LPUSH`, `SADD`, `ZADD` — that value stays in the keyspace, so the
//! capture is a deep clone: one `HSET` on a 5M-field hash held the shard
//! thread 1.2 s (every client of the shard stalled) and pinned +836 MB until
//! the walk passed the key. moon#1269 fixed the removals (the value leaves the
//! keyspace, so it is MOVED); this is the in-place class.
//!
//! # Design: stream the pre-image, the key write-locked meanwhile
//!
//! Above [`STREAM_MIN_ELEMENTS`] elements, a writer that CAN wait does not
//! clone. [`admit_write`] queues a stream request for the key and hands the
//! writer a receiver to await; nothing has changed yet. The shard's
//! persistence tick ([`service`], before the walk) opens a key block for it
//! (`snapshot::key_stream`): the value's epoch-start bytes go straight into
//! the snapshot stream, a byte budget per tick, while the walk is paused and
//! a tombstone pre-image keeps the walk from writing the key again. Writers
//! of the key keep waiting (the value must not change under the stream; other
//! keys are served as usual). When the block closes the waiters are woken,
//! re-run [`admit_write`] and find the key captured — the write runs, and
//! its dispatch hook skips the copy. Cost: O(n) serialization once, which the
//! save paid anyway when the walk reached the key, and no copy: the shard is
//! never held for more than a tick's budget, the RSS spike is gone.
//!
//! Who waits: a connection's own write, on both runtimes, before its local
//! dispatch (a pipelined connection waits in order, so later commands of the
//! same client never overtake it). Who cannot wait — a MULTI/EXEC body, a
//! script's `redis.call`, a routed SPSC leg, a blocking-pop waker, eviction,
//! replica apply — keeps the pre-moon#1295 behaviour on a key not streaming
//! yet (the copy), and on a key that IS streaming finishes the stream
//! synchronously from the value as it still is ([`guard_write`]: the rest of
//! the serialization, inline, still no copy) before it writes. A whole-table
//! change follows the stream ([`note_swap`]) or finishes it from the detached
//! table ([`note_cleared`]); FLUSHALL and a replica resync abort the epoch.
//!
//! # Keys an open TXN holds (moon#1300)
//!
//! A held key is saved at its PRE-transaction image, which the hold keeps
//! (`isolation`). A large one streams from that image ([`Source::Held`]) —
//! the transaction's own writes change the live value, not the source, so
//! nothing waits. A hold released mid-stream hands the image over by move
//! ([`held_released`]), so a commit never waits for, nor drops, the source.
//!
//! # What makes the image exact
//!
//! - The key is in the file exactly once: the block, with the walk's copy
//!   hidden by the tombstone (first capture wins, and the stream starts only
//!   for a key with no capture and still pending).
//! - The value does not change while it streams: every writer waits, or
//!   finishes the stream first. A value with a key TTL is never streamed
//!   (expiry removes keys without a writer). Every chunk re-checks the value
//!   ([`KeyCursor::matches`]); a mismatch fails the save loudly
//!   ("snapshot aborted") rather than publishing a block that does not parse.
//!
//! All state is per shard thread (`thread_local!`), like the rest of
//! `snapshot_cow`: the writers, the tick and the hooks all run on it.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use super::capture::{captured, mark_captured, record_entry, target};
use super::{PENDING, is_armed};
use crate::persistence::snapshot::key_stream::{ChunkLimit, KeyCursor, streamable_len};
use crate::persistence::snapshot::{SnapshotState, Table};
use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::storage::entry::Entry;

/// Collections with at least this many elements are streamed instead of
/// copied. A copy costs ~230 ns per element on the shard thread (measured:
/// 1.17 s for 5M hash fields), so this is ~2 ms — about what a writer waits
/// for a stream of the same size (a tick or two).
pub(crate) const STREAM_MIN_ELEMENTS: usize = 8192;

/// A tick serializes ONE chunk (across every key): at most this many
/// bytes...
const STREAM_TICK_BYTES: usize = 8 << 20;

/// ...for about this long. Serialization is memory-bound (~200 ns an element
/// for a hash of separately allocated fields: two cache misses), so a byte
/// budget alone let one tick run 25 ms. A hash or sorted set re-skips to its
/// position first (~2 ns an element) and may then serialize as long as the
/// skip took: a tick is at most ~2x a full skip (~20 ms at 5M fields).
const STREAM_TICK_TIME: std::time::Duration = std::time::Duration::from_millis(2);

/// Where an active stream reads the value from.
enum Source {
    /// The live keyspace: shard slot `.0` (moved by a SWAPDB).
    Live(usize),
    /// The pre-transaction image an open TXN's hold keeps (moon#1300), of
    /// the key in shard slot `.0`.
    Held(usize),
    /// A held image whose hold was released mid-stream, handed over by move.
    Owned(Entry),
}

/// A key a writer (or the epoch's start, for a held key) waits to stream.
struct Request {
    /// Shard slot the key lives in.
    slot: usize,
    /// Epoch database it is filed under.
    db: usize,
    key: Bytes,
    held: bool,
}

/// The key being streamed: at most one at a time (its block must be
/// contiguous in the file).
struct Active {
    source: Source,
    key: Bytes,
    cursor: KeyCursor,
    /// Bytes a writer that could not wait serialized ([`guard_write`]),
    /// appended to the block by the next [`service`].
    tail: Vec<u8>,
    /// The value changed under the stream (no guard saw it): the next
    /// [`service`] fails the save.
    broken: bool,
}

impl Active {
    fn live_slot(&self) -> Option<usize> {
        match self.source {
            Source::Live(slot) => Some(slot),
            _ => None,
        }
    }

    /// Finish from `current`, the value as it is right before a writer that
    /// cannot wait changes it.
    fn finish_from(&mut self, current: Option<&Entry>) {
        if self.cursor.done() {
            return;
        }
        match current {
            Some(entry) if self.cursor.matches(entry) => {
                self.cursor.write_rest(entry, &mut self.tail)
            }
            _ => self.broken = true,
        }
    }
}

thread_local! {
    /// A request is queued or a stream is active: the gate every hook takes
    /// first (one `Cell<bool>` load otherwise).
    static BUSY: Cell<bool> = const { Cell::new(false) };
    static QUEUE: RefCell<VecDeque<Request>> = const { RefCell::new(VecDeque::new()) };
    static ACTIVE: RefCell<Option<Active>> = const { RefCell::new(None) };
    /// Writers waiting for a stream: woken (sender dropped) whenever a
    /// stream ends or a request is settled; each re-runs [`admit_write`].
    static WAITERS: RefCell<Vec<flume::Sender<()>>> = const { RefCell::new(Vec::new()) };
    /// Test-only threshold override (unit tests run in parallel threads).
    #[cfg(test)]
    static MIN_OVERRIDE: Cell<Option<usize>> = const { Cell::new(None) };
    /// Test-only per-tick byte budget (no time limit while set).
    #[cfg(test)]
    static TICK_OVERRIDE: Cell<Option<usize>> = const { Cell::new(None) };
}

static STREAMED_KEYS: AtomicU64 = AtomicU64::new(0);
static PARKED_WRITES: AtomicU64 = AtomicU64::new(0);

/// INFO `rdb_cow_streamed_keys`: collections whose epoch-start image was
/// streamed instead of copied, since start.
pub(crate) fn streamed_keys() -> u64 {
    STREAMED_KEYS.load(Ordering::Relaxed)
}

/// INFO `rdb_cow_stream_waits`: writes that waited for a stream, since start.
pub(crate) fn parked_writes() -> u64 {
    PARKED_WRITES.load(Ordering::Relaxed)
}

/// Read a test-only knob once: `MOON_TEST_COW_STREAM_*` (no deployment sets
/// them; integration tests lower the threshold and the tick budget).
fn env_knob(cell: &'static OnceLock<Option<usize>>, name: &str) -> Option<usize> {
    *cell.get_or_init(|| std::env::var(name).ok().and_then(|v| v.parse().ok()))
}

fn min_elements() -> usize {
    #[cfg(test)]
    if let Some(n) = MIN_OVERRIDE.with(Cell::get) {
        return n;
    }
    static KNOB: OnceLock<Option<usize>> = OnceLock::new();
    env_knob(&KNOB, "MOON_TEST_COW_STREAM_MIN_ELEMENTS").unwrap_or(STREAM_MIN_ELEMENTS)
}

fn tick_bytes() -> usize {
    #[cfg(test)]
    if let Some(n) = TICK_OVERRIDE.with(Cell::get) {
        return n.max(1);
    }
    static KNOB: OnceLock<Option<usize>> = OnceLock::new();
    env_knob(&KNOB, "MOON_TEST_COW_STREAM_TICK_BYTES")
        .unwrap_or(STREAM_TICK_BYTES)
        .max(1)
}

/// A tick's chunk time ([`STREAM_TICK_TIME`]); none under a test budget.
fn tick_time() -> Option<std::time::Duration> {
    #[cfg(test)]
    if TICK_OVERRIDE.with(Cell::get).is_some() {
        return None;
    }
    Some(STREAM_TICK_TIME)
}

/// Test-only: stream collections of at least `n` elements, `tick` bytes a
/// tick (no time limit), on this thread.
#[cfg(test)]
pub(crate) fn set_knobs_for_test(min: Option<usize>, tick: Option<usize>) {
    MIN_OVERRIDE.with(|c| c.set(min));
    TICK_OVERRIDE.with(|c| c.set(tick));
}

/// Is a request queued or a stream active on this shard?
#[inline]
pub(crate) fn busy() -> bool {
    BUSY.with(Cell::get)
}

fn wait() -> flume::Receiver<()> {
    let (tx, rx) = flume::bounded(1);
    WAITERS.with(|w| w.borrow_mut().push(tx));
    rx
}

fn wake_all() {
    let waiters = WAITERS.with(|w| std::mem::take(&mut *w.borrow_mut()));
    drop(waiters);
}

fn refresh_busy() {
    let busy = ACTIVE.with(|a| a.borrow().is_some()) || QUEUE.with(|q| !q.borrow().is_empty());
    BUSY.with(|b| b.set(busy));
}

/// Must a writer of `(slot, key)` wait? While the key is the active live
/// stream (not finished) or queued for one.
fn key_busy(slot: usize, key: &[u8]) -> bool {
    ACTIVE.with(|a| {
        a.borrow().as_ref().is_some_and(|a| {
            a.live_slot() == Some(slot) && a.key.as_ref() == key && !a.cursor.done()
        })
    }) || QUEUE.with(|q| {
        q.borrow()
            .iter()
            .any(|r| !r.held && r.slot == slot && r.key.as_ref() == key)
    })
}

/// Every key position `cmd args..` may WRITE: the shared key walker's
/// `Write` role (moon#1217), the primary key when it cannot enumerate.
pub(super) fn for_each_written_key(cmd: &[u8], args: &[Frame], mut f: impl FnMut(&[u8])) {
    use crate::acl::keyspec::{KeyPositions, KeyRole, command_key_positions};
    match command_key_positions(cmd, args) {
        KeyPositions::At(positions) | KeyPositions::AtPlusComputed(positions) => {
            for at in positions.iter().filter(|at| at.role == KeyRole::Write) {
                if let Some(key) = args
                    .get(at.idx)
                    .and_then(crate::command::helpers::extract_bytes)
                {
                    f(key);
                }
            }
        }
        KeyPositions::None => {}
        KeyPositions::Unknown => {
            if let Some(key) = crate::server::conn::shared::extract_primary_key(cmd, args) {
                f(key);
            }
        }
    }
}

/// Whole-table writes: they wait for an active stream rather than finish it
/// inline.
fn whole_table(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"FLUSHDB")
        || cmd.eq_ignore_ascii_case(b"FLUSHALL")
        || cmd.eq_ignore_ascii_case(b"SWAPDB")
}

/// A connection is about to run write `cmd args..` in shard slot `slot`.
/// `Some(rx)`: wait (`rx.recv_async()`, which ends when a stream ends) and
/// ask again; `None`: go ahead. One thread-local `bool` load when no save is
/// in flight. Only for a writer that can wait without breaking atomicity —
/// a connection's own command, before its dispatch.
pub(crate) fn admit_write(slot: usize, cmd: &[u8], args: &[Frame]) -> Option<flume::Receiver<()>> {
    if !is_armed() {
        return None;
    }
    crate::shard::slice::with_shard_db_read(slot, |db| admit_write_in(db, slot, cmd, args))
}

/// [`admit_write`] against `db` (= `databases[slot]`).
pub(crate) fn admit_write_in(
    db: &Database,
    slot: usize,
    cmd: &[u8],
    args: &[Frame],
) -> Option<flume::Receiver<()>> {
    if !is_armed() || !crate::command::metadata::is_write(cmd) {
        return None;
    }
    if whole_table(cmd) {
        return busy().then(park);
    }
    let removes = super::removes_only(cmd);
    let mut wait_for = false;
    for_each_written_key(cmd, args, |key| {
        if wait_for {
            return;
        }
        if busy() && key_busy(slot, key) {
            wait_for = true;
            return;
        }
        // A removal hands the value over by move (moon#1269): nothing to
        // stream.
        if removes {
            return;
        }
        let Some(epoch_db) = target(slot, key) else {
            return;
        };
        if captured(epoch_db, key) {
            return;
        }
        let large = db
            .data()
            .get(key)
            .and_then(streamable_len)
            .is_some_and(|n| n >= min_elements());
        if large {
            request(slot, epoch_db, Bytes::copy_from_slice(key), false);
            wait_for = true;
        }
    });
    wait_for.then(park)
}

/// [`admit_write`] until it lets the write through: a connection's write,
/// before its local dispatch, on both runtimes. The wait yields to the shard
/// (other clients, the tick that streams); it ends when the stream ends or
/// the save does.
pub(crate) async fn wait_for_streams(slot: usize, cmd: &[u8], args: &[Frame]) {
    while let Some(rx) = admit_write(slot, cmd, args) {
        let _ = rx.recv_async().await;
    }
}

fn park() -> flume::Receiver<()> {
    PARKED_WRITES.fetch_add(1, Ordering::Relaxed);
    wait()
}

fn request(slot: usize, db: usize, key: Bytes, held: bool) {
    QUEUE.with(|q| {
        q.borrow_mut().push_back(Request {
            slot,
            db,
            key,
            held,
        })
    });
    BUSY.with(|b| b.set(true));
}

/// At the epoch's start (`capture_held_pre_images`): stream `key`'s held
/// pre-transaction image `pre` when it is large. `true` = queued (the key is
/// already marked captured); `false` = the caller copies it as before.
pub(super) fn request_held(slot: usize, db: usize, key: &Bytes, pre: &Entry) -> bool {
    if !streamable_len(pre).is_some_and(|n| n >= min_elements()) {
        return false;
    }
    mark_captured(db, key);
    request(slot, db, key.clone(), true);
    true
}

/// Drop every request and stream (the epoch ended or was replaced) and wake
/// every waiter. An image handed over by move is freed off the shard thread.
pub(super) fn clear() {
    if !busy() && WAITERS.with(|w| w.borrow().is_empty()) {
        return;
    }
    QUEUE.with(|q| q.borrow_mut().clear());
    if let Some(active) = ACTIVE.with(|a| a.borrow_mut().take())
        && let Source::Owned(entry) = active.source
    {
        crate::persistence::snapshot::frozen::dispose(entry);
    }
    BUSY.with(|b| b.set(false));
    wake_all();
}

/// A writer that cannot wait is about to write `key` in shard slot `slot`,
/// whose value is `current` (`None`: no hot entry). Called first thing in
/// every capture ([`super::capture::capture_key`]) and before every hot
/// removal (`Database::remove_hot*`), only while [`busy`]:
///
/// - `key` is the active live stream: finish it from `current` now (the
///   rest of the serialization, inline — still no copy), so the write
///   cannot change a value half-written;
/// - `key` is queued: drop the request (the caller captures as it always
///   did) and wake its waiters, who will find the key captured.
#[cold]
pub(crate) fn guard_write(slot: usize, key: &[u8], current: Option<&Entry>) {
    let finished = ACTIVE.with(|a| {
        let mut a = a.borrow_mut();
        match a.as_mut() {
            Some(a) if a.live_slot() == Some(slot) && a.key.as_ref() == key && !a.cursor.done() => {
                a.finish_from(current);
                true
            }
            _ => false,
        }
    });
    let dropped = QUEUE.with(|q| {
        let mut q = q.borrow_mut();
        let before = q.len();
        q.retain(|r| r.held || r.slot != slot || r.key.as_ref() != key);
        before != q.len()
    });
    if finished || dropped {
        refresh_busy();
        wake_all();
    }
}

/// `SWAPDB a b` is exchanging the tables of shard slots `a` and `b`
/// (`note_swapdb`): the streamed and queued keys move with them.
pub(super) fn note_swap(a: usize, b: usize) {
    if !busy() {
        return;
    }
    let swap = |s: &mut usize| {
        if *s == a {
            *s = b;
        } else if *s == b {
            *s = a;
        }
    };
    ACTIVE.with(|act| {
        if let Some(act) = act.borrow_mut().as_mut() {
            match &mut act.source {
                Source::Live(s) | Source::Held(s) => swap(s),
                Source::Owned(_) => {}
            }
        }
    });
    QUEUE.with(|q| q.borrow_mut().iter_mut().for_each(|r| swap(&mut r.slot)));
}

/// A flush detached shard slot `slot`'s `table` (`note_cleared_table`): a
/// stream of a key in it is finished from the detached table, and requests
/// for its keys are dropped (their keys are gone).
pub(super) fn note_cleared(slot: usize, table: &Table) {
    if !busy() {
        return;
    }
    ACTIVE.with(|a| {
        if let Some(a) = a.borrow_mut().as_mut()
            && a.live_slot() == Some(slot)
        {
            let current = table.get(a.key.as_ref());
            a.finish_from(current);
        }
    });
    QUEUE.with(|q| q.borrow_mut().retain(|r| r.held || r.slot != slot));
    refresh_busy();
    wake_all();
}

/// An open TXN's hold on `(slot, key)` is being released, handing back its
/// pre-transaction image `pre` (moon#1300). A stream reading it takes it by
/// move; a queued request files it as the key's pre-image by move. Returns
/// the image when the snapshot did not take it.
pub(crate) fn held_released(slot: usize, key: &[u8], pre: Option<Entry>) -> Option<Entry> {
    let entry = pre?;
    if !busy() {
        return Some(entry);
    }
    let entry = ACTIVE.with(|a| {
        let mut a = a.borrow_mut();
        match a.as_mut() {
            Some(a)
                if matches!(a.source, Source::Held(s) if s == slot)
                    && a.key.as_ref() == key
                    && !a.cursor.done() =>
            {
                a.source = Source::Owned(entry);
                None
            }
            _ => Some(entry),
        }
    })?;
    let queued = QUEUE.with(|q| {
        let mut q = q.borrow_mut();
        let at = q
            .iter()
            .position(|r| r.held && r.slot == slot && r.key.as_ref() == key)?;
        q.remove(at)
    });
    match queued {
        Some(req) => {
            // Already marked captured at the epoch's start; this is its
            // first (and only) pre-image.
            PENDING.with(|p| p.borrow_mut().push((req.db, req.key, Some(entry), None)));
            refresh_busy();
            None
        }
        None => Some(entry),
    }
}

/// How [`service`] reads a key's current value: `lookup(slot, key, f)` calls
/// `f` with the hot entry of `key` in shard slot `slot`.
pub(crate) type Lookup<'a> = &'a dyn Fn(usize, &[u8], &mut dyn FnMut(Option<&Entry>));

/// The persistence tick, before the walk (also while a test holds the walk):
/// stream up to the tick's byte budget — finish the active block, start the
/// next request. The walk stays paused while a block is open.
pub(crate) fn service(snap: &mut SnapshotState) {
    if !busy() {
        return;
    }
    service_with(snap, &|slot, key, f| {
        crate::shard::slice::with_shard_db_read(slot, |db| f(db.data().get(key)))
    });
}

/// [`service`] with an explicit live-keyspace lookup (tests).
pub(crate) fn service_with(snap: &mut SnapshotState, lookup: Lookup<'_>) {
    // One chunk per tick: `budget` falls to 0 once it is written.
    let mut budget = tick_bytes();
    loop {
        if let Some(active) = ACTIVE.with(|a| a.borrow_mut().take()) {
            match pump(snap, active, &mut budget, lookup) {
                Pump::Closed => {
                    STREAMED_KEYS.fetch_add(1, Ordering::Relaxed);
                    refresh_busy();
                    wake_all();
                    continue;
                }
                Pump::Open(active) => {
                    ACTIVE.with(|a| *a.borrow_mut() = Some(active));
                    return;
                }
                Pump::Broken => {
                    snap.abort("a key streamed into the snapshot changed under it");
                    clear();
                    return;
                }
            }
        }
        let Some(req) = QUEUE.with(|q| q.borrow_mut().pop_front()) else {
            refresh_busy();
            return;
        };
        if let Some(active) = start(snap, req, lookup) {
            ACTIVE.with(|a| *a.borrow_mut() = Some(active));
        }
        // A request settled without a stream: its waiters re-check.
        wake_all();
        refresh_busy();
    }
}

enum Pump {
    Closed,
    Open(Active),
    Broken,
}

/// Read a stream's source and hand it to `f`.
fn with_source(source: &Source, key: &[u8], lookup: Lookup<'_>, f: &mut dyn FnMut(Option<&Entry>)) {
    match source {
        Source::Live(slot) => lookup(*slot, key, f),
        Source::Held(slot) => {
            crate::transaction::isolation::with_held_pre(*slot, key, |pre| f(pre))
        }
        Source::Owned(entry) => f(Some(entry)),
    }
}

fn pump(
    snap: &mut SnapshotState,
    mut active: Active,
    budget: &mut usize,
    lookup: Lookup<'_>,
) -> Pump {
    if active.broken {
        if let Source::Owned(entry) = active.source {
            crate::persistence::snapshot::frozen::dispose(entry);
        }
        return Pump::Broken;
    }
    if !active.tail.is_empty() {
        let tail = std::mem::take(&mut active.tail);
        *budget = budget.saturating_sub(tail.len());
        snap.key_block_write(|out| out.extend_from_slice(&tail));
    }
    if !active.cursor.done() {
        if *budget == 0 {
            return Pump::Open(active);
        }
        let Active {
            source,
            key,
            cursor,
            ..
        } = &mut active;
        let mut ok = false;
        let limit = ChunkLimit {
            bytes: *budget,
            time: tick_time(),
        };
        with_source(source, key, lookup, &mut |entry| {
            let Some(entry) = entry.filter(|e| cursor.matches(e)) else {
                return;
            };
            ok = true;
            snap.key_block_write(|out| {
                cursor.write_some(entry, out, limit);
            });
        });
        if !ok {
            active.broken = true;
            return pump(snap, active, budget, lookup);
        }
        *budget = 0;
        // A save wait's stall watch: the epoch is making progress.
        crate::command::persistence::note_save_progress();
    }
    if !active.cursor.done() {
        return Pump::Open(active);
    }
    snap.close_key_block(active.cursor);
    if let Source::Owned(entry) = active.source {
        crate::persistence::snapshot::frozen::dispose(entry);
    }
    Pump::Closed
}

/// Open the block for `req`, or settle it without one.
fn start(snap: &mut SnapshotState, req: Request, lookup: Lookup<'_>) -> Option<Active> {
    let mut opened = None;
    let mut copy_held = None;
    let mut open = |entry: Option<&Entry>| {
        let Some(entry) = entry else { return };
        match snap.open_key_block(req.db, &req.key, entry) {
            Some(cursor) => opened = Some(cursor),
            // A held image must reach the file: copied, as before moon#1295.
            None if req.held && snap.is_key_pending(req.db, &req.key) => {
                copy_held = Some(entry.clone())
            }
            None => {}
        }
    };
    if req.held {
        crate::transaction::isolation::with_held_pre(req.slot, &req.key, |pre| open(pre));
    } else {
        lookup(req.slot, &req.key, &mut open);
    }
    if let Some(entry) = copy_held {
        record_entry(req.db, req.key.clone(), entry, None);
    }
    match opened {
        Some(cursor) => {
            // Its first capture: no later write of the key copies it.
            mark_captured(req.db, &req.key);
            Some(Active {
                source: if req.held {
                    Source::Held(req.slot)
                } else {
                    Source::Live(req.slot)
                },
                key: req.key,
                cursor,
                tail: Vec::new(),
                broken: false,
            })
        }
        None => {
            // Nothing to stream: the range is written, the key has a
            // pre-image already, or it is gone. A capture would be dropped
            // (or the key needs none), so mark it captured: the waiters'
            // re-check then lets them through.
            if !snap.is_key_pending(req.db, &req.key) || snap.has_pre_image(req.db, &req.key) {
                mark_captured(req.db, &req.key);
            }
            None
        }
    }
}
