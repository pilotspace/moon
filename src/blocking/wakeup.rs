use bytes::Bytes;

use crate::blocking::{BlockedCommand, BlockingRegistry, Direction};
use crate::command::sorted_set::format_score_bytes;
use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

pub use crate::blocking::stream_wake::{stream_register_error, try_wake_stream_waiter};

/// The ready keys of one write, in the order it wrote them. Most writes name
/// one or two; only a wide multi-key write spills to the heap.
pub type ReadyKeys = smallvec::SmallVec<[Bytes; 2]>;

/// Can `cmd` leave a LIST, SORTED SET or STREAM behind — the only values a
/// blocked client can be served from?
///
/// Redis's `signalKeyAsReady` returns at once unless some client is blocked
/// on the TYPE of value the write produced; this is the same early return
/// taken from the command table instead of the value: a write whose ACL
/// categories are only `@string`, `@hash` or `@set` (bitmaps and
/// HyperLogLogs are `@string`) produces a string, hash or set, which nobody
/// can be blocked on. Without it, one waiter parked anywhere on a shard — the
/// normal state of a queue server — made every `MSET` walk its keys: 489K →
/// 407K ops/s for a 10-key `MSET` in the moon#1069 review.
///
/// Kept: `@list`, `@sortedset`, `@stream` (which is where `ZADD`, `LMOVE`,
/// `XADD`, every `*STORE` of a zset and `BLMOVE` live), and `@generic`, which
/// in this table holds everything that moves or rebuilds a value of any type
/// — `RENAME`, `COPY`, `MOVE`, `RESTORE`, `SORT ... STORE` — and the `GEO*`
/// commands (`@geo` in redis; a geo set is a zset). Only `WRITE` commands: a
/// read never makes a key ready. Scripts are not in this list because they do
/// not signal as a whole: each `redis.call` inside one is checked on its own
/// command ([`note_script_write`]).
#[inline]
pub fn may_ready_a_key(cmd: &[u8]) -> bool {
    use crate::command::metadata::{self, AclCategories as C, CommandFlags};
    metadata::lookup(cmd).is_some_and(|m| {
        m.flags.contains(CommandFlags::WRITE)
            && [C::LIST, C::SORTEDSET, C::STREAM, C::GENERIC]
                .iter()
                .any(|family| m.acl_categories.contains(*family))
    })
}

/// The gate every write→waiter hook opens with: [`may_ready_a_key`], widened
/// to EVERY write while an `XREADGROUP` is parked on this shard (moon#1086).
///
/// A group reader is owed an answer not only when its stream gains entries
/// but when any write deletes it (`DEL`, `UNLINK`, `RENAME` away, `MOVE`
/// away), retypes it (`SET`, `RESTORE ... REPLACE`, ...) or destroys its
/// group: redis marks the reader `unblock_on_nokey` and answers it `-NOGROUP`
/// / `-WRONGTYPE` at once (measured against redis-server 8.6.1). Those writes
/// are `@string`, `@hash`, `@set` as often as not, which the cheap gate
/// rightly skips for list, zset and `XREAD` waiters. So the widening costs
/// one thread-local read, and only a shard that actually has a group reader
/// parked walks the keys of a `SET`.
#[inline]
pub fn may_wake(cmd: &[u8]) -> bool {
    may_ready_a_key(cmd)
        || (crate::blocking::group_readers_parked_here()
            && crate::command::metadata::lookup(cmd).is_some_and(|m| {
                m.flags
                    .contains(crate::command::metadata::CommandFlags::WRITE)
            }))
}

/// Call `f` on every key a write command may have CREATED or GROWN — the keys
/// a blocked client could now be served from (moon#1059, moon#1069) — in
/// argument order, BORROWED from `args`.
///
/// This is redis's `signalKeyAsReady`, raised from the keyspace write itself
/// (`dbAdd`, `setKey`, `lmoveHandlePush`, ...), expressed through the one key
/// walker every other consumer shares ([`command_key_positions`]): the
/// positions a command may MODIFY. Keyed on what a command WRITES, never on a
/// list of command names. The list it replaced — `LPUSH`/`RPUSH`/`LMOVE`/
/// `RPOPLPUSH`/`ZADD`/`XADD` — was the model of the system, and every writer
/// it did not name (`RENAME`, `COPY`, `SORT ... STORE`, `ZUNIONSTORE`,
/// `ZRANGESTORE`, `ZINCRBY`, `GEOADD`, `RESTORE`, ...) left a client blocked
/// beside data it could have had.
///
/// Over-inclusive on purpose, in the direction that is merely wasted work: a
/// key that was DELETED (`RENAME`'s source, `LMOVE`'s emptied source) or that
/// now holds the wrong type for its waiters is offered too, and the wakers
/// find nothing to serve and leave every waiter parked exactly where it was.
/// Missing a key is a waiter asleep until its own timeout.
///
/// Callers gate this with [`may_ready_a_key`] (and a registry that has a
/// waiter at all); it does no gating of its own.
///
/// `MOVE` and `COPY ... DB n` write a key in ANOTHER database; that half is
/// [`cross_db_write_target`], raised by the two-database intercepts.
///
/// [`command_key_positions`]: crate::acl::keyspec::command_key_positions
pub fn for_each_written_key(cmd: &[u8], args: &[Frame], mut f: impl FnMut(&Bytes)) {
    use crate::acl::keyspec::{KeyPositions, KeyRole, command_key_positions};
    let positions = match command_key_positions(cmd, args) {
        // `AtPlusComputed` is `SORT ... BY w_*`: the computed names are only
        // ever READ, and the `STORE` destination is still named.
        KeyPositions::At(idx) | KeyPositions::AtPlusComputed(idx) => idx,
        KeyPositions::None | KeyPositions::Unknown => return,
    };
    for at in &positions {
        if at.role == KeyRole::Write
            && let Some(key) = args
                .get(at.idx)
                .and_then(crate::command::helpers::extract_bytes)
        {
            f(key);
        }
    }
}

/// [`for_each_written_key`], collected (each key once, in order). For the
/// callers that must RECORD the keys for a wake that happens later — an
/// `EXEC` body, a script — and only after [`may_ready_a_key`].
pub fn written_keys(cmd: &[u8], args: &[Frame]) -> ReadyKeys {
    let mut out = ReadyKeys::new();
    for_each_written_key(cmd, args, |k| {
        if !out.contains(k) {
            out.push(k.clone());
        }
    });
    out
}

/// The database and key a successful `MOVE` or `COPY ... DB n` wrote into, or
/// `None` for anything else — including a same-db `COPY`, whose destination
/// [`written_keys`] already names in the command's own database.
pub fn cross_db_write_target(
    cmd: &[u8],
    args: &[Frame],
    db_index: usize,
    db_count: usize,
) -> Option<(usize, Bytes)> {
    use crate::command::keyspace::move_cmd as ksmv;
    if cmd.eq_ignore_ascii_case(b"MOVE") {
        let (key, dst_db) = ksmv::parse_move_args(args, db_count).ok()?;
        return (dst_db != db_index).then_some((dst_db, key));
    }
    if cmd.eq_ignore_ascii_case(b"COPY") {
        let ca = ksmv::parse_copy_db_args(args, db_index, db_count)?.ok()?;
        return Some((ca.dst_db, ca.dst_key));
    }
    None
}

/// The keys a write touched that have a client parked on them in `db_index`,
/// in argument order — decided against the registry alone, so a caller can
/// ask it BEFORE paying for an exclusive database guard (moon#942).
///
/// Cheapest first: while nothing on this shard is blocked it is one map
/// emptiness check (the steady state of every non-queue workload); a write
/// that cannot produce a list, zset or stream stops at [`may_ready_a_key`];
/// and the keys that remain are probed BORROWED — only a key that really has
/// a waiter is cloned into the result.
pub fn ready_keys(
    registry: &BlockingRegistry,
    db_index: usize,
    cmd: &[u8],
    args: &[Frame],
) -> ReadyKeys {
    let mut keys = ReadyKeys::new();
    if !registry.has_any_waiters() || !may_wake(cmd) {
        return keys;
    }
    for_each_written_key(cmd, args, |k| {
        if registry.has_waiters(db_index, k) && !keys.contains(k) {
            keys.push(k.clone());
        }
    });
    keys
}

/// What a wake attempt consumed from the datastore, so it can be put back if
/// the woken client turns out to be gone.
///
/// c10k hardening A2: every wake path used to `pop` first and only then
/// `let _ = reply_tx.send(...)`. A failed send — the overwhelmingly common
/// case for a client that RST'd or timed out while its registration was still
/// live on this shard — silently destroyed the popped element: not delivered,
/// not requeued, not offered to the next FIFO waiter. Redis guarantees an
/// element is either delivered or stays in the key.
///
/// Two defences, because neither alone is sufficient:
///   1. `is_disconnected()` is checked BEFORE touching the datastore, so a
///      known-dead waiter never causes a mutation at all (the common case);
///   2. this undo covers the residual race — the receiver can drop between
///      that check and the send.
///
/// moon#1019 adds a third user: a shard that pops for a waiter registered on
/// several threads and then LOSES the waiter's claim puts the element back
/// with this, in the same synchronous stretch as the pop.
///
/// Every user runs it in the SAME synchronous stretch as the pop, so nothing
/// else can have touched the key in between. That is the whole reason it may
/// exist at all: a put-back after an await would land on top of other
/// clients' logged writes while the pop itself was never logged, and the
/// master would diverge from its AOF and replicas (moon#1023).
///
/// moon#1056 NARROWS the A2 guarantee, deliberately. A won serve is now
/// LOGGED (AOF + replication, `blocking::pop_log`) before its reply is sent,
/// so the waiter's `fsync_barrier` on the owner covers the record. Once the
/// record exists, this undo is never applied: a put-back would leave memory
/// disagreeing with the AOF and every replica. So a waiter whose receiver
/// drops in the window between that log and the send — reachable only for a
/// REMOTE waiter whose connection task is torn down without settling its
/// claim; a local waiter's receiver lives on this thread and is checked just
/// before — loses the element: consumed, logged, delivered to nobody, exactly
/// like a client that disconnects right after its reply was written. The
/// "delivered or still in the key" guarantee holds in full only when no
/// durability plane is active (nothing logged, so the undo still runs).
pub(crate) enum WakeUndo {
    /// Values popped from the FRONT of the key, in pop order.
    ListFront(smallvec::SmallVec<[bytes::Bytes; 4]>),
    /// Values popped from the BACK of the key, in pop order.
    ListBack(smallvec::SmallVec<[bytes::Bytes; 4]>),
    /// BLMOVE: `value` left the key via `wherefrom` and was pushed onto
    /// `destination` via `whereto`. Undoing means reversing BOTH halves.
    Moved {
        destination: Bytes,
        wherefrom: Direction,
        whereto: Direction,
        value: Bytes,
    },
    /// (member, score) pairs popped from the sorted set at the key.
    Zset(smallvec::SmallVec<[(bytes::Bytes, f64); 4]>),
}

/// The absolute TTL of `key` (ms), or 0 when it has none or is absent.
///
/// Read BEFORE a pop that may be put back: a pop that empties the key removes
/// it, TTL and all, and the put-back recreates it from nothing.
pub(crate) fn expiry_of(db: &mut Database, key: &Bytes) -> u64 {
    db.get(key)
        .filter(|e| e.has_expiry())
        .map_or(0, |e| e.expires_at_ms())
}

impl WakeUndo {
    /// [`restore`](Self::restore), then give the key back the TTL it had
    /// before the pop — `expires_at_ms` from [`expiry_of`], 0 for none.
    ///
    /// A pop that emptied the key removed it, and `restore` recreates it
    /// through the create-on-push path with NO TTL; without this the master
    /// would hold forever a key its replicas expire. A key that survived the
    /// pop still carries its TTL, and is left alone.
    pub(crate) fn restore_keeping_ttl(self, db: &mut Database, key: &Bytes, expires_at_ms: u64) {
        self.restore(db, key);
        if expires_at_ms != 0 && expiry_of(db, key) == 0 {
            db.set_expiry(key, expires_at_ms);
        }
    }

    /// Put everything back exactly where it came from.
    pub(crate) fn restore(self, db: &mut Database, key: &Bytes) {
        match self {
            // Pops came off the front in order [v0, v1, ..]; pushing them
            // back front-first in REVERSE order restores the original
            // sequence (push v_n first, v0 last => v0 ends up at the front).
            WakeUndo::ListFront(vals) => {
                for v in vals.into_iter().rev() {
                    db.list_push_front(key, v);
                }
            }
            WakeUndo::ListBack(vals) => {
                for v in vals.into_iter().rev() {
                    db.list_push_back(key, v);
                }
            }
            WakeUndo::Moved {
                destination,
                wherefrom,
                whereto,
                value,
            } => {
                // Take back the element that was pushed onto the destination.
                // It is still at the end it was pushed to — this runs on the
                // shard thread with no await in between. The value check is a
                // guard, not a branch any caller expects to take: if the end
                // ever held something else, the move STANDS (the element is
                // misplaced by the client's own command, never lost) rather
                // than taking another client's element.
                let moved = match whereto {
                    Direction::Left => db.list_pop_front(&destination),
                    Direction::Right => db.list_pop_back(&destination),
                };
                match moved {
                    Some(v) if v == value => match wherefrom {
                        Direction::Left => db.list_push_front(key, v),
                        Direction::Right => db.list_push_back(key, v),
                    },
                    Some(other) => match whereto {
                        Direction::Left => db.list_push_front(&destination, other),
                        Direction::Right => db.list_push_back(&destination, other),
                    },
                    None => {}
                }
            }
            // Sorted sets are order-free: reinsertion order is irrelevant.
            WakeUndo::Zset(pairs) => {
                for (member, score) in pairs {
                    db.zset_restore(key, member, score);
                }
            }
        }
    }
}

/// Serve the list waiters parked on `key` — and then every key those serves
/// made ready in turn (see [`wake_keys`]). Returns true if a blocked client
/// was answered. A `key` that is not a list serves nobody.
///
/// The caller must hold mutable borrows on both the registry and the database.
pub fn try_wake_list_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    let mut worklist = ReadyKeys::new();
    let mut budget = crate::blocking::pop_log::wake_budget();
    let served = serve_list_key(registry, db, db_index, key, &mut worklist, 0, &mut budget);
    served | drain_ready(registry, db, db_index, &mut worklist, 0, &mut budget)
}

/// Serve a whole batch of ready keys in one database — the keys a write, an
/// `EXEC` or a script made ready — the way redis's `handleClientsBlockedOnKeys`
/// drains `server.ready_keys`. Returns true if a blocked client was answered.
///
/// **One FIFO worklist, seeded with the WHOLE batch.** Each key's waiters are
/// served in turn, and a `BLMOVE`/`BRPOPLPUSH` served on the way PUSHES onto
/// its destination — as much a write as the `LPUSH` that woke it, so the
/// destination is appended to the END of the worklist (moon#1059). It is served
/// only after every key already queued ahead of it, which is redis's
/// batch-by-batch order and is observable: with `BLMOVE a c`, `BLMOVE b c` and
/// `BRPOP c` parked, `MULTI; RPUSH a x; RPUSH b y; EXEC` hands the `BRPOP` `y`
/// and leaves `c = [x]` (redis-server 8.6.1). Draining each seed's chain before
/// the next seed ran served `c` between the two moves and handed it `x`.
///
/// A key is not queued twice while it is still PENDING (redis dedups
/// `server.ready_keys` the same way), but one served earlier in the walk is
/// queued again when a later hop pushes back onto it (`BLMOVE a b`,
/// `BLMOVE b a`, `BLPOP a`: the element returns to `a` and its next waiter
/// takes it, as in redis).
///
/// **Bounded without a cap.** Past the seeds, a key joins the worklist only
/// when a move waiter has just been SERVED, and a served waiter leaves the
/// registry in the same step; nothing registers during this synchronous
/// stretch. So the walk queues at most one key per waiter parked when it
/// began, even through cycles. Every destination is in this database and on
/// this shard: a move across shards is refused before it can park
/// (`cross_shard_move_refusal`).
pub fn wake_keys(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    seeds: impl IntoIterator<Item = Bytes>,
) -> bool {
    wake_keys_budgeted(
        registry,
        db,
        db_index,
        seeds,
        &mut crate::blocking::pop_log::wake_budget(),
    )
}

/// [`wake_keys`] drawing every pop it logs from the caller's backpressure
/// `budget` (moon#1056): a wake pass that serves many waiters stalls the
/// shard thread for at most one bound in total when the AOF writer is
/// saturated, not one bound per served pop.
pub(crate) fn wake_keys_budgeted(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    seeds: impl IntoIterator<Item = Bytes>,
    budget: &mut std::time::Duration,
) -> bool {
    let mut worklist = ReadyKeys::new();
    for key in seeds {
        if !worklist.contains(&key) {
            worklist.push(key);
        }
    }
    drain_ready(registry, db, db_index, &mut worklist, 0, budget)
}

/// Serve `worklist[next..]` in order, appending the destinations the serves
/// make ready. See [`wake_keys`].
fn drain_ready(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    worklist: &mut ReadyKeys,
    mut next: usize,
    budget: &mut std::time::Duration,
) -> bool {
    let mut served = false;
    while let Some(key) = worklist.get(next).cloned() {
        next += 1;
        served |= serve_ready_key(registry, db, db_index, &key, worklist, next, budget);
    }
    served
}

/// One ready key's turn: run the waker of the type the key NOW holds, and
/// only that one.
///
/// A key holds one type, so every other family's waiters stay parked where
/// they were — redis's behaviour for a key that became the wrong type under a
/// waiter (measured against redis-server 8.6.1: `RENAME` a zset onto a key a
/// `BLPOP` is parked on leaves the `BLPOP` parked until its own timeout).
/// Dispatching on the type also keeps a hot list's queue of `BLPOP`s from
/// being rescanned by the zset and stream wakers on every push. Read-only
/// probes (moon#832); an absent key, or one of a type no waiter can use,
/// serves nobody.
fn serve_ready_key(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    worklist: &mut ReadyKeys,
    pending_from: usize,
    budget: &mut std::time::Duration,
) -> bool {
    let now_ms = db.now_ms();
    let served = if matches!(db.get_list_ref_if_alive(key, now_ms), Ok(Some(_))) {
        serve_list_key(registry, db, db_index, key, worklist, pending_from, budget)
    } else if matches!(db.get_sorted_set_ref_if_alive(key, now_ms), Ok(Some(_))) {
        serve_zset_key(registry, db, db_index, key, budget)
    } else if matches!(db.get_stream_if_alive(key, now_ms), Ok(Some(_))) {
        return crate::blocking::stream_wake::try_wake_stream_waiter_budgeted(
            registry, db, db_index, key, budget,
        );
    } else {
        false
    };
    // moon#1086: the key is not a stream (any more). A group reader parked on
    // it is owed `-NOGROUP` (the key is gone) or `-WRONGTYPE` (it holds
    // something else) now; the stream waker decides both. Plain `XREAD`
    // waiters stay parked, as in redis.
    if registry.has_group_readers() {
        served
            | crate::blocking::stream_wake::try_wake_stream_waiter_budgeted(
                registry, db, db_index, key, budget,
            )
    } else {
        served
    }
}

/// One key's turn in [`try_wake_list_waiter`]: pop the key's list waiters
/// (FIFO) and execute each one's pop while the key has data. The destination
/// of every move served here is appended to `worklist` unless it is already
/// pending there (`worklist[pending_from..]`).
fn serve_list_key(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    worklist: &mut ReadyKeys,
    pending_from: usize,
    budget: &mut std::time::Duration,
) -> bool {
    // Loop: try waiters until one succeeds (oneshot receiver may be dropped = skip)
    // moon#535: pop only waiters THIS waker can serve. The old blind
    // `pop_front` handed us waiters of every family, and the cleanup below —
    // `remove_wait` + `send(None)` — runs for every waiter we pop, so an
    // unservable one was destroyed rather than left for its own waker.
    //
    // The loop condition moved from `has_waiters` to the pop itself: a queue
    // holding only foreign waiters is not empty, so the old condition would
    // now spin forever.
    // A key that holds nothing has nothing for anyone: leave every waiter
    // parked without touching the queue. Every producer calls this after a
    // successful push, so the key exists; this is the cheap exit for the
    // callers that cannot promise that. (A waiter popped below for a pop that
    // yields nothing is put back too — never answered nil, which redis never
    // sends a timeout-0 waiter.)
    //
    // moon#1069: and a key that is not a LIST has nothing for a list waiter —
    // checked here, before any waiter is popped, because this is now raised
    // for every key a write touched, not only for keys a push just grew. The
    // `BLMOVE` arm below answers `-WRONGTYPE` for a wrong-typed DESTINATION
    // before it pops; without this, a `SET` on a `BLMOVE`'s SOURCE would have
    // reached that arm and unblocked the client with the destination's error,
    // where redis (`serveClientsBlockedOnKey` matches the key's type to the
    // waiter's first) leaves it parked. Read-only probe (moon#832).
    if !db.exists(key) {
        return false;
    }
    let now_ms = db.now_ms();
    if !matches!(db.get_list_ref_if_alive(key, now_ms), Ok(Some(_))) {
        return false;
    }
    let mut served = false;
    while let Some(waiter) =
        registry.pop_front_of_family(db_index, key, crate::blocking::WaitFamily::List)
    {
        // A2: never mutate the datastore on behalf of a waiter whose client
        // is already gone — nor, since moon#1019, one that another shard has
        // already served or that has given up. Reap the registration and move
        // to the next waiter with the key untouched.
        if waiter.is_settled() {
            registry.remove_wait(waiter.wait_id);
            continue;
        }
        let crate::blocking::WaitEntry {
            wait_id,
            cmd,
            reply_tx,
            claim,
            deadline,
        } = waiter;

        // Execute the pop based on command type
        // The TTL the key has before this pop, in case the pop empties it
        // and has to be put back.
        let expires_at_ms = expiry_of(db, key);
        let (result, undo) = match &cmd {
            BlockedCommand::BLPop => {
                // Pop from left, return [key, value]
                match db.list_pop_front(key) {
                    Some(v) => (
                        Some(Frame::Array(framevec![
                            Frame::BulkString(key.clone()),
                            Frame::BulkString(v.clone()),
                        ])),
                        Some(WakeUndo::ListFront(smallvec::smallvec![v])),
                    ),
                    None => (None, None),
                }
            }
            BlockedCommand::BRPop => {
                // Pop from right, return [key, value]
                match db.list_pop_back(key) {
                    Some(v) => (
                        Some(Frame::Array(framevec![
                            Frame::BulkString(key.clone()),
                            Frame::BulkString(v.clone()),
                        ])),
                        Some(WakeUndo::ListBack(smallvec::smallvec![v])),
                    ),
                    None => (None, None),
                }
            }
            BlockedCommand::BLMove {
                destination,
                wherefrom,
                whereto,
            } => {
                // moon#556: a destination of the wrong type is the client's
                // ERROR, never a reason to consume the element. Redis checks
                // it before popping (`serveClientBlockedOnList`) and unblocks
                // the waiter with `-WRONGTYPE`; moon used to pop, hand the
                // value to the client in its reply, and lose it on the way to
                // the destination — `list_push_*` swallows a wrong-typed
                // target in an `if let Ok(list)`.
                //
                // `destination == key` is the rotate form: same key, same
                // type, nothing to check.
                // moon#570: this shard owns `key` (the source) — it is the
                // shard the waiter registered on. It cannot push to a
                // destination another shard owns; doing so wrote the element
                // into THIS shard's slice under the destination's name, where
                // a normally-routed read of the destination never looks. The
                // client got the element in its reply and the keyspace lost
                // it.
                //
                // Unreachable in practice: `immediate_scan` refuses the same
                // pair before the waiter is ever registered, so no `BLMove`
                // with a remote destination should reach this arm. It is
                // checked again here because this is the LAST place that can
                // still decline to consume the element — every other defence
                // sits upstream of the pop, and a silent regression upstream
                // would be acked data loss, the failure mode this whole path
                // exists to prevent. Comparing the two key hashes (rather
                // than this shard's id) makes the answer independent of which
                // shard runs it.
                let cross_shard_err = crate::command::list::cross_shard_move_refusal(
                    key,
                    destination,
                    crate::command::connection::shard_count(),
                );
                let dest_err = if cross_shard_err.is_some() {
                    cross_shard_err
                } else if destination == key {
                    None
                } else {
                    // moon#832: type probe only — when it fails nothing moves,
                    // so it must not flatten the destination's encoding.
                    let now_ms = db.now_ms();
                    db.get_list_ref_if_alive(destination, now_ms).err()
                };
                if let Some(err) = dest_err {
                    // No undo: nothing was popped.
                    (Some(err), None)
                } else {
                    let val = match wherefrom {
                        Direction::Left => db.list_pop_front(key),
                        Direction::Right => db.list_pop_back(key),
                    };
                    match val {
                        Some(v) => {
                            // Push to destination
                            match whereto {
                                Direction::Left => db.list_push_front(destination, v.clone()),
                                Direction::Right => db.list_push_back(destination, v.clone()),
                            }
                            (
                                Some(Frame::BulkString(v.clone())),
                                Some(WakeUndo::Moved {
                                    destination: destination.clone(),
                                    wherefrom: *wherefrom,
                                    whereto: *whereto,
                                    value: v,
                                }),
                            )
                        }
                        None => (None, None),
                    }
                }
            }
            BlockedCommand::BLMPop { dir, count } => {
                let mut popped = smallvec::SmallVec::<[Bytes; 4]>::new();
                let n = *count as usize;
                for _ in 0..n {
                    let val = match dir {
                        Direction::Left => db.list_pop_front(key),
                        Direction::Right => db.list_pop_back(key),
                    };
                    match val {
                        Some(v) => popped.push(v),
                        None => break,
                    }
                }
                if popped.is_empty() {
                    (None, None)
                } else {
                    let elem_vec: Vec<Frame> =
                        popped.iter().cloned().map(Frame::BulkString).collect();
                    let undo = match dir {
                        Direction::Left => WakeUndo::ListFront(popped),
                        Direction::Right => WakeUndo::ListBack(popped),
                    };
                    (
                        Some(Frame::Array(framevec![
                            Frame::BulkString(key.clone()),
                            Frame::Array(elem_vec.into()),
                        ])),
                        Some(undo),
                    )
                }
            }
            // Unreachable since moon#535: `pop_front_of_family(List)` cannot
            // hand us a zset or stream waiter. Kept as a total match rather
            // than an `unreachable!()` — a panic here would take the shard
            // down, and answering "no data" is the safe direction.
            _ => (None, None),
        };

        // Nothing to pop: the key holds another type (a waiter of this
        // family parked on it while it was absent, then another client
        // created it as something else), or it emptied under a caller that
        // runs every waker. This waker has nothing for the waiter, so it must
        // not answer it — nil is a reply redis never sends a timeout-0 waiter.
        // Put it back where it was and stop; every other waiter of this
        // family would find the same nothing.
        let Some(result) = result else {
            registry.requeue_front(
                db_index,
                key,
                crate::blocking::WaitEntry {
                    wait_id,
                    cmd,
                    reply_tx,
                    deadline,
                    claim,
                },
            );
            break;
        };

        // Clean up all other key registrations for this wait_id
        registry.remove_wait(wait_id);

        // moon#1059: a move that goes through made its destination ready.
        // Only a move that actually MOVED — its undo says so; a `-WRONGTYPE`
        // answer carries none — and not the rotate form, whose destination is
        // the key this loop is still serving.
        let moved_to = match &undo {
            Some(WakeUndo::Moved { destination, .. }) if destination != key => {
                Some(destination.clone())
            }
            _ => None,
        };

        let delivered = deliver(
            db,
            db_index,
            key,
            &cmd,
            reply_tx,
            claim.as_ref(),
            result,
            undo,
            expires_at_ms,
            budget,
        );
        if delivered.served() {
            served = true;
            if let Some(dest) = moved_to
                && !worklist[pending_from..].contains(&dest)
            {
                worklist.push(dest);
            }
            // moon#1056: the AOF writer could not take this pop's record
            // within the backpressure bound. Every further serve here would
            // wait out a bound of its own on the shard thread; leave the rest
            // parked beside their data instead (a later write to the key, or
            // their own timeout, reaches them).
            if delivered == Delivered::ServedAofLost {
                break;
            }
            // moon#1019: one push can carry several elements, and
            // Redis keeps serving the key's waiters while it has data. Stopping
            // after the first left the rest parked next to data they could
            // have had — and a waiter parked on several keys was offered each
            // of them exactly once by `register_group`, so it stayed parked
            // even when a key it was registered on still held elements.
            if !db.exists(key) {
                break;
            }
        }
    }
    served
}

/// How [`deliver`] ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Delivered {
    /// The waiter has its reply, and the pop is logged wherever logging is on.
    Served,
    /// The waiter was answered, but the AOF append of the pop was lost (the
    /// writer stayed saturated past its bound). The pop stands and the waiter
    /// was told so with an error instead of the element.
    ServedAofLost,
    /// Nobody took the serve: a lost claim, or a waiter gone before the reply
    /// could be sent.
    NotServed,
}

impl Delivered {
    /// A waiter was answered.
    #[inline]
    pub(crate) fn served(self) -> bool {
        !matches!(self, Delivered::NotServed)
    }
}

/// The tail every destructive wake shares, once the element is popped and the
/// reply built: decide whether this waiter gets it, log the pop, and put it
/// back if nobody takes it. A waker with nothing to hand a waiter never gets
/// here — it leaves the waiter parked instead.
///
/// * moon#1019: a waiter registered on several threads is served by exactly
///   one of them — whichever wins its [`ClaimToken`](crate::blocking::ClaimToken).
///   The claim is attempted with the element already in hand, so a lost claim
///   restores it here, in the same synchronous stretch as the pop, where no
///   other client can have observed the round trip.
/// * moon#1056: a won serve is LOGGED here, by the shard that popped, before
///   the reply leaves ([`crate::blocking::pop_log`]). This is the owner's
///   thread and the pop's own synchronous stretch, so the record lands in the
///   owner's AOF and replication stream in the same order as the owner's
///   other writes. The waiter logs nothing.
/// * A2: a failed send (the receiver dropped after the liveness check)
///   restores the element and moves on instead of destroying it — when the
///   pop reached no durability plane. Once it has been logged, putting the
///   element back would leave memory disagreeing with the AOF and the
///   replicas, so the serve stands, exactly as it does for a client that
///   disconnects after its reply is sent (moon#1023). A local waiter cannot
///   reach that case: its receiver lives on this thread and was checked just
///   before; a remote one only when its task is torn down without settling.
///   That element is then consumed and delivered to nobody — the deliberate
///   price of logging BEFORE the send so the waiter's fsync barrier covers
///   the record (see [`WakeUndo`]).
/// * `budget` is the wake pass's shared AOF backpressure budget
///   ([`crate::blocking::pop_log::wake_budget`]).
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver(
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    cmd: &BlockedCommand,
    reply_tx: crate::runtime::channel::OneshotSender<Option<Frame>>,
    claim: Option<&crate::blocking::ClaimToken>,
    frame: Frame,
    undo: Option<WakeUndo>,
    expires_at_ms: u64,
    budget: &mut std::time::Duration,
) -> Delivered {
    let won = claim.is_none_or(crate::blocking::ClaimToken::try_claim);
    if !won || reply_tx.is_disconnected() {
        if let Some(undo) = undo {
            undo.restore_keeping_ttl(db, key, expires_at_ms);
        }
        return Delivered::NotServed;
    }
    let mut frame = frame;
    let mut logged = false;
    let mut aof_lost = false;
    if let Some(u) = undo.as_ref()
        && crate::blocking::pop_log::has_work()
        && let Some(record) = crate::blocking::pop_log::served_pop_record(cmd, key, u)
    {
        match crate::blocking::pop_log::log_pop(db_index, &record, budget) {
            crate::blocking::pop_log::PopLog::Unlogged => {}
            crate::blocking::pop_log::PopLog::Logged => logged = true,
            crate::blocking::pop_log::PopLog::AofLost => {
                logged = true;
                aof_lost = true;
                frame = Frame::Error(Bytes::from_static(
                    crate::shard::spsc_handler::AOF_APPEND_LOST_ERR,
                ));
            }
        }
    }
    if reply_tx.send(Some(frame)).is_ok() {
        return if aof_lost {
            Delivered::ServedAofLost
        } else {
            Delivered::Served
        };
    }
    if !logged && let Some(undo) = undo {
        undo.restore_keeping_ttl(db, key, expires_at_ms);
    }
    Delivered::NotServed
}

/// Called after ZADD successfully adds elements to a sorted set key.
/// Pops the first waiter (FIFO) and executes ZPOPMIN or ZPOPMAX.
/// Returns true if a blocked client was woken.
pub fn try_wake_zset_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    serve_zset_key(
        registry,
        db,
        db_index,
        key,
        &mut crate::blocking::pop_log::wake_budget(),
    )
}

/// [`try_wake_zset_waiter`] drawing its pop logging from the caller's
/// backpressure `budget`, so one wake pass stalls the shard thread for at
/// most one bound however many waiters it serves.
fn serve_zset_key(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    budget: &mut std::time::Duration,
) -> bool {
    // moon#535: pop only waiters THIS waker can serve. The old blind
    // `pop_front` handed us waiters of every family, and the cleanup below —
    // `remove_wait` + `send(None)` — runs for every waiter we pop, so an
    // unservable one was destroyed rather than left for its own waker.
    //
    // The loop condition moved from `has_waiters` to the pop itself: a queue
    // holding only foreign waiters is not empty, so the old condition would
    // now spin forever.
    // A key that holds nothing has nothing for anyone: leave every waiter
    // parked without touching the queue. Every producer calls this after a
    // successful push, so the key exists; this is the cheap exit for the
    // callers that cannot promise that. (A waiter popped below for a pop that
    // yields nothing is put back too — never answered nil, which redis never
    // sends a timeout-0 waiter.)
    if !db.exists(key) {
        return false;
    }
    let mut served = false;
    while let Some(waiter) =
        registry.pop_front_of_family(db_index, key, crate::blocking::WaitFamily::ZSet)
    {
        // A2 / moon#1019: see try_wake_list_waiter — never pop for a waiter
        // nobody can use a serve from.
        if waiter.is_settled() {
            registry.remove_wait(waiter.wait_id);
            continue;
        }
        let crate::blocking::WaitEntry {
            wait_id,
            cmd,
            reply_tx,
            claim,
            deadline,
        } = waiter;

        // The TTL the key has before this pop, in case the pop empties it
        // and has to be put back.
        let expires_at_ms = expiry_of(db, key);
        let (result, undo) = match &cmd {
            BlockedCommand::BZPopMin => match db.zset_pop_min(key) {
                Some((member, score)) => (
                    Some(Frame::Array(framevec![
                        Frame::BulkString(key.clone()),
                        Frame::BulkString(member.clone()),
                        Frame::BulkString(format_score_bytes(score)),
                    ])),
                    Some(WakeUndo::Zset(smallvec::smallvec![(member, score)])),
                ),
                None => (None, None),
            },
            BlockedCommand::BZPopMax => match db.zset_pop_max(key) {
                Some((member, score)) => (
                    Some(Frame::Array(framevec![
                        Frame::BulkString(key.clone()),
                        Frame::BulkString(member.clone()),
                        Frame::BulkString(format_score_bytes(score)),
                    ])),
                    Some(WakeUndo::Zset(smallvec::smallvec![(member, score)])),
                ),
                None => (None, None),
            },
            BlockedCommand::BZMPop { min, count } => {
                let n = *count as usize;
                let mut popped = smallvec::SmallVec::<[(Bytes, f64); 4]>::new();
                for _ in 0..n {
                    let entry = if *min {
                        db.zset_pop_min(key)
                    } else {
                        db.zset_pop_max(key)
                    };
                    match entry {
                        Some(pair) => popped.push(pair),
                        None => break,
                    }
                }
                if popped.is_empty() {
                    (None, None)
                } else {
                    let elem_vec: Vec<Frame> = popped
                        .iter()
                        .map(|(member, score)| {
                            Frame::Array(framevec![
                                Frame::BulkString(member.clone()),
                                Frame::BulkString(format_score_bytes(*score)),
                            ])
                        })
                        .collect();
                    (
                        Some(Frame::Array(framevec![
                            Frame::BulkString(key.clone()),
                            Frame::Array(elem_vec.into()),
                        ])),
                        Some(WakeUndo::Zset(popped)),
                    )
                }
            }
            // Unreachable since moon#535 — see try_wake_list_waiter.
            _ => (None, None),
        };
        // Nothing to pop: see try_wake_list_waiter — leave the waiter parked.
        let Some(result) = result else {
            registry.requeue_front(
                db_index,
                key,
                crate::blocking::WaitEntry {
                    wait_id,
                    cmd,
                    reply_tx,
                    deadline,
                    claim,
                },
            );
            break;
        };

        registry.remove_wait(wait_id);

        // Claim / log / A2 / keep-serving-while-data: see try_wake_list_waiter.
        let delivered = deliver(
            db,
            db_index,
            key,
            &cmd,
            reply_tx,
            claim.as_ref(),
            result,
            undo,
            expires_at_ms,
            budget,
        );
        if delivered.served() {
            served = true;
            if delivered == Delivered::ServedAofLost || !db.exists(key) {
                break;
            }
        }
    }
    served
}

/// Serve every waiter `key` can now satisfy, and the moves they chain into —
/// [`wake_keys`] for a batch of one. Returns true if a blocked client was
/// answered.
pub fn wake_key(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    wake_keys(registry, db, db_index, std::iter::once(key.clone()))
}

/// The one write→waiter hook: after a write command succeeded in `db_index`,
/// serve whoever is blocked on a key it wrote. Returns true if a blocked
/// client was answered.
///
/// Every dispatch site that runs a write calls THIS (or, where it must decide
/// before taking a database guard, [`ready_keys`] then [`wake_keys`]) — never a
/// hand-rolled subset. It replaces a hook keyed on six "producer" command
/// names, and the lesson of its history is that a site or a command the hook
/// does not reach looks perfectly healthy in review and in CI: `XADD` on a
/// locally owned stream (moon#595), every write inside `MULTI`/`EXEC`
/// (moon#606), and then every writer that was not a push (moon#1069).
///
/// The caller keeps its own success gate (`is_write && !error`, or just
/// `!error`) — it differs per path and is not part of the mapping.
pub fn wake_written_keys(
    registry: &std::cell::RefCell<BlockingRegistry>,
    db: &mut Database,
    db_index: usize,
    cmd: &[u8],
    args: &[Frame],
) -> bool {
    let mut reg = registry.borrow_mut();
    let keys = ready_keys(&reg, db_index, cmd, args);
    !keys.is_empty() && wake_keys(&mut reg, db, db_index, keys)
}

/// Split recorded `(db, key)` wakes into one ordered batch per database, each
/// in first-recorded order, keeping only keys that have a waiter.
///
/// Per database is enough for redis's order: waiters in different databases
/// never compete for data, and a move never leaves its database.
fn batches_by_db(
    registry: &BlockingRegistry,
    recorded: impl IntoIterator<Item = (usize, Bytes)>,
) -> smallvec::SmallVec<[(usize, ReadyKeys); 1]> {
    let mut batches: smallvec::SmallVec<[(usize, ReadyKeys); 1]> = smallvec::SmallVec::new();
    for (db_index, key) in recorded {
        if !registry.has_waiters(db_index, &key) {
            continue;
        }
        match batches.iter_mut().find(|(db, _)| *db == db_index) {
            Some((_, keys)) => keys.push(key),
            None => batches.push((db_index, smallvec::smallvec![key])),
        }
    }
    batches
}

/// Serve the waiters on each `(db, key)` a write recorded for later — the
/// deferred form of [`wake_written_keys`], for an executor that could not
/// reach the registry while it ran (a `MULTI` body, the multi-key
/// coordinator, `SWAPDB`).
///
/// Every recorded key of a database is ONE batch for [`wake_keys`], as redis
/// serves its whole ready list after the command: EXEC is one command, so
/// every waiter sees the whole transaction applied, never half of it, and the
/// moves the keys chain into run after all of them. Must run on the shard
/// that owns the keys — the one the body ran on — and outside any borrow of
/// that shard's slice.
pub fn wake_recorded(
    registry: &std::cell::RefCell<BlockingRegistry>,
    recorded: impl IntoIterator<Item = (usize, Bytes)>,
) {
    // A body of ten thousand writes on a shard nobody is blocked on pays one
    // check, not ten thousand lookups.
    if !registry.borrow().has_any_waiters() {
        return;
    }
    let mut reg = registry.borrow_mut();
    // One backpressure budget for the whole transaction's wakes, across its
    // databases: see `pop_log::wake_budget`.
    let mut budget = crate::blocking::pop_log::wake_budget();
    for (db_index, keys) in batches_by_db(&reg, recorded) {
        crate::shard::slice::with_shard_db(db_index, |db| {
            wake_keys_budgeted(&mut reg, db, db_index, keys, &mut budget);
        });
    }
}

/// [`wake_written_keys`] for a write whose LOCAL leg ran somewhere with no
/// write tail — the multi-key coordinator, which runs a same-shard `COPY`
/// (and this shard's slice of every spanning write) straight through the
/// dispatcher, and an immediately served `BLMOVE`. Remote legs are Execute
/// messages and are woken on their owners like any other write.
///
/// Only keys with a waiter in THIS shard's registry are served, and a waiter
/// only ever parks on the shard that owns its key, so a key written on
/// another shard is never touched here. Call outside any borrow of this
/// shard's slice.
pub fn wake_written_keys_on_shard(
    registry: &std::cell::RefCell<BlockingRegistry>,
    db_index: usize,
    cmd: &[u8],
    args: &[Frame],
) {
    let keys = ready_keys(&registry.borrow(), db_index, cmd, args);
    wake_ready_keys_on_shard(registry, db_index, keys);
}

/// Serve `keys` — decided earlier by [`ready_keys`] — in `db_index` of this
/// shard. For a write path that must decide the keys while it still holds its
/// write guard but may only WAKE once its own record is logged (moon#1056): a
/// wake logs each pop it performs as it performs it, so running it before the
/// write that fed it is logged would put the pop ahead of that write in the
/// AOF and the replication stream. Call outside any borrow of this shard's
/// slice.
pub fn wake_ready_keys_on_shard(
    registry: &std::cell::RefCell<BlockingRegistry>,
    db_index: usize,
    keys: ReadyKeys,
) {
    if keys.is_empty() {
        return;
    }
    let mut reg = registry.borrow_mut();
    crate::shard::slice::with_shard_db(db_index, |db| {
        wake_keys(&mut reg, db, db_index, keys);
    });
}

/// `SWAPDB a b` just exchanged two of this shard's databases: every key a
/// client is parked on in either of them may now hold data. Redis serves them
/// the same way (`swapdb` -> `scanDatabaseForReadyKeys`, measured against
/// redis-server 8.6.1: a `BLPOP k` parked in db 3 is served by `SWAPDB 0 3`
/// when db 0 held `k`). Call outside any borrow of this shard's slice.
pub fn wake_swapped_dbs(registry: &std::cell::RefCell<BlockingRegistry>, a: usize, b: usize) {
    if a == b || !registry.borrow().has_any_waiters() {
        return;
    }
    let recorded: Vec<(usize, Bytes)> = {
        let reg = registry.borrow();
        [a, b]
            .into_iter()
            .flat_map(|db| reg.waited_keys(db).into_iter().map(move |k| (db, k)))
            .collect()
    };
    wake_recorded(registry, recorded);
}

thread_local! {
    /// Set when this shard removed keys in a way no write hook names a key
    /// for — a flush, a `MOVE` out of the source database, active expiry —
    /// while an `XREADGROUP` was parked here. Drained by
    /// [`recheck_group_readers`] on the shard's 10 ms blocking tick.
    static GROUP_READER_RECHECK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A deletion the ready-key hooks cannot see — `FLUSHDB`, `FLUSHALL`, the
/// source half of a `MOVE`, an expired key — just ran on this shard.
///
/// Redis signals every deleted key (`signalDeletedKeyAsReady`), and a parked
/// `XREADGROUP` whose stream went with it is answered `-NOGROUP` at once
/// (moon#1086). These deletions name no key to [`ready_keys`] (a flush names
/// none, a `MOVE` names the key it wrote in the OTHER database, an expiry is
/// no command at all), so instead they mark the shard for a recheck of its
/// parked group readers, which the blocking tick runs within 10 ms. One
/// thread-local read when no group reader is parked; nothing else.
#[inline]
pub fn note_unsignalled_removal() {
    if crate::blocking::group_readers_parked_here() {
        GROUP_READER_RECHECK.with(|c| c.set(true));
    }
}

/// Run the recheck [`note_unsignalled_removal`] asked for: offer every key a
/// group reader is parked on to the stream waker, which answers the readers
/// whose stream or group is gone and serves any that have entries. Called
/// from the shard's blocking tick, outside any borrow of its slice.
pub fn recheck_group_readers(registry: &std::cell::RefCell<BlockingRegistry>) {
    if !GROUP_READER_RECHECK.with(|c| c.replace(false)) {
        return;
    }
    let keys = {
        let reg = registry.borrow();
        if !reg.has_group_readers() {
            return;
        }
        reg.group_reader_keys()
    };
    let mut reg = registry.borrow_mut();
    let mut budget = crate::blocking::pop_log::wake_budget();
    for (db_index, key) in keys {
        crate::shard::slice::with_shard_db(db_index, |db| {
            crate::blocking::stream_wake::try_wake_stream_waiter_budgeted(
                &mut reg,
                db,
                db_index,
                &key,
                &mut budget,
            );
        });
    }
}

/// How a script's writes reach the clients blocked on the keys they touched.
pub enum ScriptWakes<'a> {
    /// Serve them when the script returns, from this shard's registry.
    Serve(&'a std::cell::RefCell<BlockingRegistry>),
    /// Leave them recorded for the caller — a script queued in `MULTI`, whose
    /// writes are served with the rest of the transaction's after `EXEC`.
    /// `armed` is the caller's "is anyone blocked on this shard" snapshot;
    /// `false` records nothing at all.
    Defer { armed: bool },
}

thread_local! {
    /// The keys the script running on this shard thread has written so far,
    /// with the db each was written in — the script's half of redis's ready
    /// set, served once the script returns (see [`note_script_write`]).
    /// Per-shard-thread state, never shared, like `pending_flush::PENDING`.
    /// Drained, never replaced, so its capacity is reused across scripts.
    static SCRIPT_WRITES: std::cell::RefCell<Vec<(usize, Bytes)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    /// Whether the running script's writes are worth recording: was anyone
    /// blocked on this shard when it started? Nothing can register while a
    /// script runs — it is one synchronous stretch — so the snapshot stays
    /// exact for the whole script.
    static SCRIPT_WAKES_ARMED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Start a script: clear any record a script that unwound past its completion
/// left behind, and arm recording iff someone can be woken. Called by
/// [`crate::scripting::pending_flush::run_and_complete`], the one path every
/// script entry point takes.
pub fn begin_script_writes(wakes: &ScriptWakes<'_>) {
    let armed = match wakes {
        ScriptWakes::Serve(registry) => registry.borrow().has_any_waiters(),
        ScriptWakes::Defer { armed } => *armed,
    };
    SCRIPT_WRITES.with(|w| w.borrow_mut().clear());
    SCRIPT_WAKES_ARMED.with(|a| a.set(armed));
}

/// Record the keys a successful `redis.call` write inside a script touched.
///
/// A script reaches the keyspace from `scripting::bridge` through one
/// `&mut Database`, with no registry in scope and no way to serve a waiter
/// without breaking the script's atomicity — so, like a flush the script
/// issues, the wake is recorded here and finished one frame up by
/// [`crate::scripting::pending_flush::run_and_complete`] (or, for a script
/// queued in `MULTI`, by the EXEC executor's own recorded wakes).
///
/// One `Cell` load when nobody is blocked; nothing is recorded for a command
/// that cannot produce a list, zset or stream ([`may_ready_a_key`]); a key is
/// recorded once per script.
pub fn note_script_write(db_index: usize, cmd: &[u8], args: &[Frame]) {
    if !SCRIPT_WAKES_ARMED.with(std::cell::Cell::get) || !may_wake(cmd) {
        return;
    }
    SCRIPT_WRITES.with(|w| {
        let mut w = w.borrow_mut();
        for_each_written_key(cmd, args, |key| {
            if !w.iter().any(|(db, k)| *db == db_index && k == key) {
                w.push((db_index, key.clone()));
            }
        });
    });
}

/// [`note_script_write`] for the key a script's `MOVE` / `COPY ... DB n`
/// wrote into ANOTHER database (moon#1068) — the script half of
/// [`cross_db_write_target`]. The caller passes it only for a `:1` reply.
///
/// Served with the script's other ready keys once it returns, which is after
/// the bridge has logged the command: a pop the wake performs is logged as it
/// happens, so it must follow the write that fed it (moon#1056).
pub fn note_script_cross_db_write(dst_db: usize, key: &Bytes) {
    if !SCRIPT_WAKES_ARMED.with(std::cell::Cell::get) {
        return;
    }
    SCRIPT_WRITES.with(|w| {
        let mut w = w.borrow_mut();
        if !w.iter().any(|(db, k)| *db == dst_db && k == key) {
            w.push((dst_db, key.clone()));
        }
    });
}

/// Take everything [`note_script_write`] recorded and disarm, leaving the
/// thread clean for the next script. The thread's buffer keeps its capacity;
/// the returned `Vec` allocates only when something was recorded.
pub fn take_script_writes() -> Vec<(usize, Bytes)> {
    SCRIPT_WAKES_ARMED.with(|a| a.set(false));
    SCRIPT_WRITES.with(|w| w.borrow_mut().drain(..).collect())
}

/// Serve the waiters on the keys a finished script wrote, as one batch per
/// database ([`wake_keys`]), on the shard whose databases are `databases` —
/// for a caller that already holds this shard's slice, where
/// [`wake_recorded`]'s own slice borrow would re-enter it.
pub fn wake_script_writes(
    registry: &std::cell::RefCell<BlockingRegistry>,
    databases: &crate::shard::db_plane::ShardDbSet,
) {
    let recorded = take_script_writes();
    if recorded.is_empty() {
        return;
    }
    let mut reg = registry.borrow_mut();
    for (db_index, keys) in batches_by_db(&reg, recorded) {
        let mut db = databases.write(db_index);
        wake_keys(&mut reg, &mut db, db_index, keys);
    }
}

/// Wake the waiters on the key a `MOVE` or `COPY ... DB n` just wrote into
/// `dst` (database `dst_db`), given the command's reply. The two-database
/// intercepts run before, and instead of, every generic write tail, so this is
/// their half of [`wake_written_keys`]: redis signals the key from the
/// destination database's `dbAdd`.
///
/// Only `:1` wrote anything; `:0` (source missing, destination occupied
/// without `REPLACE`) and every error leave `dst` as it was.
pub fn wake_cross_db_write(
    registry: &std::cell::RefCell<BlockingRegistry>,
    dst: &mut Database,
    dst_db: usize,
    key: &Bytes,
    reply: &Frame,
) -> bool {
    if !matches!(reply, Frame::Integer(1)) {
        return false;
    }
    let mut reg = registry.borrow_mut();
    if !reg.has_waiters(dst_db, key) {
        return false;
    }
    wake_key(&mut reg, dst, dst_db, key)
}

/// [`wake_cross_db_write`] for a caller that no longer holds the destination
/// database — one that logs the `MOVE`/`COPY ... DB n` first and wakes after
/// (moon#1056: a pop the wake performs is logged as it happens, so it must
/// follow the write that fed it). Call outside any borrow of this shard's
/// slice.
pub fn wake_cross_db_write_on_shard(
    registry: &std::cell::RefCell<BlockingRegistry>,
    dst_db: usize,
    key: &Bytes,
    reply: &Frame,
) -> bool {
    if !matches!(reply, Frame::Integer(1)) || !registry.borrow().has_waiters(dst_db, key) {
        return false;
    }
    crate::shard::slice::with_shard_db(dst_db, |dst| {
        wake_cross_db_write(registry, dst, dst_db, key, reply)
    })
}

#[cfg(test)]
mod dead_waiter_tests {
    use super::*;
    use crate::blocking::WaitEntry;
    use crate::storage::Database;

    fn register(
        reg: &mut BlockingRegistry,
        key: &Bytes,
        cmd: BlockedCommand,
    ) -> crate::runtime::channel::OneshotReceiver<Option<Frame>> {
        let wait_id = reg.next_wait_id();
        let (tx, rx) = crate::runtime::channel::oneshot();
        reg.register(
            0,
            key.clone(),
            WaitEntry {
                wait_id,
                cmd,
                reply_tx: tx,
                deadline: None,
                claim: None,
            },
        );
        rx
    }

    fn list_len(db: &mut Database, key: &Bytes) -> usize {
        db.get_list(key).ok().flatten().map_or(0, |l| l.len())
    }

    /// A2: the woken client is already gone. The element must stay in the
    /// list — the old code popped first, then dropped the value on the floor
    /// when `reply_tx.send` failed.
    #[test]
    fn dead_list_waiter_does_not_consume_element() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let key = Bytes::from_static(b"mylist");

        let rx = register(&mut reg, &key, BlockedCommand::BLPop);
        drop(rx); // client vanished (RST / CLIENT KILL / timeout cleanup)

        db.list_push_back(&key, Bytes::from_static(b"v1"));
        let woke = try_wake_list_waiter(&mut reg, &mut db, 0, &key);

        assert!(!woke, "a dead waiter is not a wakeup");
        assert_eq!(list_len(&mut db, &key), 1, "element must survive");
    }

    /// A2 + FIFO: a dead head-of-queue waiter must yield the element to the
    /// next live waiter, not swallow it.
    #[test]
    fn dead_waiter_yields_element_to_next_live_waiter() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let key = Bytes::from_static(b"mylist");

        let dead = register(&mut reg, &key, BlockedCommand::BLPop);
        drop(dead);
        let live = register(&mut reg, &key, BlockedCommand::BLPop);

        db.list_push_back(&key, Bytes::from_static(b"v1"));
        let woke = try_wake_list_waiter(&mut reg, &mut db, 0, &key);

        assert!(woke, "the live waiter must be served");
        assert_eq!(list_len(&mut db, &key), 0, "element was delivered");
        assert!(
            matches!(live.try_recv(), Ok(Some(Frame::Array(_)))),
            "live waiter received the element"
        );
    }

    /// A2 for BLMOVE: a dead waiter must not leave the element stranded in
    /// the destination list (the pop AND the push both have to be undone).
    #[test]
    fn dead_blmove_waiter_does_not_move_element() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let src = Bytes::from_static(b"src");
        let dst = Bytes::from_static(b"dst");

        let rx = register(
            &mut reg,
            &src,
            BlockedCommand::BLMove {
                destination: dst.clone(),
                wherefrom: Direction::Left,
                whereto: Direction::Right,
            },
        );
        drop(rx);

        db.list_push_back(&src, Bytes::from_static(b"v1"));
        let woke = try_wake_list_waiter(&mut reg, &mut db, 0, &src);

        assert!(!woke);
        assert_eq!(list_len(&mut db, &src), 1, "source keeps the element");
        assert_eq!(list_len(&mut db, &dst), 0, "destination untouched");
    }

    /// moon#556: a woken BLMOVE whose DESTINATION holds the wrong type is
    /// answered `-WRONGTYPE`, and the element stays in the source.
    ///
    /// Pre-fix the pop happened first and the push was swallowed by
    /// `list_push_*`'s `if let Ok(list)`: the client received the element in
    /// its reply while the element left the keyspace entirely — neither in the
    /// source nor in the destination.
    #[test]
    fn woken_blmove_with_wrongtype_destination_keeps_the_element() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let src = Bytes::from_static(b"src");
        let dst = Bytes::from_static(b"dst");
        db.set(
            &dst,
            crate::storage::entry::Entry::new_string(Bytes::from_static(b"iam-a-string")),
        );

        let rx = register(
            &mut reg,
            &src,
            BlockedCommand::BLMove {
                destination: dst.clone(),
                wherefrom: Direction::Left,
                whereto: Direction::Right,
            },
        );

        db.list_push_back(&src, Bytes::from_static(b"v1"));
        let woke = try_wake_list_waiter(&mut reg, &mut db, 0, &src);

        assert!(woke, "the waiter was answered, so it is no longer blocked");
        match rx.try_recv() {
            Ok(Some(Frame::Error(e))) => assert!(
                e.starts_with(b"WRONGTYPE"),
                "expected WRONGTYPE, got {:?}",
                String::from_utf8_lossy(&e)
            ),
            other => panic!("expected a WRONGTYPE error, got {other:?}"),
        }
        assert_eq!(list_len(&mut db, &src), 1, "source keeps the element");
        assert_eq!(
            db.get(b"dst")
                .and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec)),
            Some(b"iam-a-string".to_vec()),
            "destination is untouched"
        );
    }

    /// A2 for BLMPOP: every popped element must be restored, in order.
    #[test]
    fn dead_blmpop_waiter_restores_all_elements_in_order() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let key = Bytes::from_static(b"mylist");

        let rx = register(
            &mut reg,
            &key,
            BlockedCommand::BLMPop {
                dir: Direction::Left,
                count: 3,
            },
        );
        drop(rx);

        for v in [&b"a"[..], b"b", b"c"] {
            db.list_push_back(&key, Bytes::copy_from_slice(v));
        }
        let woke = try_wake_list_waiter(&mut reg, &mut db, 0, &key);

        assert!(!woke);
        let list: Vec<Bytes> = db
            .get_list(&key)
            .ok()
            .flatten()
            .map(|l| l.iter().cloned().collect())
            .unwrap_or_default();
        assert_eq!(
            list,
            vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c")
            ],
            "order must be preserved, not reversed"
        );
    }

    /// A2 for the zset family.
    #[test]
    fn dead_zset_waiter_does_not_consume_member() {
        let mut reg = BlockingRegistry::new(0);
        let mut db = Database::new();
        let key = Bytes::from_static(b"myzset");

        let rx = register(&mut reg, &key, BlockedCommand::BZPopMin);
        drop(rx);

        db.zset_restore(&key, Bytes::from_static(b"m1"), 1.5);
        let woke = try_wake_zset_waiter(&mut reg, &mut db, 0, &key);

        assert!(!woke);
        assert_eq!(
            db.zset_pop_min(&key),
            Some((Bytes::from_static(b"m1"), 1.5)),
            "member must survive a dead waiter"
        );
    }
}

#[cfg(test)]
mod cross_shard_wait_id_tests {
    use super::*;
    use crate::blocking::{StreamSince, WaitEntry};
    use crate::storage::Database;
    use crate::storage::stream::StreamId;

    /// A `wait_id` as minted by the client's OWN shard: `shard_id << 48`.
    fn id_from_shard(shard: u64) -> u64 {
        shard << 48
    }

    fn xadd(db: &mut Database, key: &str, id: &str) {
        let args: Vec<Frame> = [key, id, "f", "v"]
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect();
        let reply = crate::command::stream::xadd(db, &args);
        assert!(
            matches!(reply, Frame::BulkString(_)),
            "seed XADD {id} failed: {reply:?}"
        );
    }

    /// Register a stream reader with an EXPLICIT wait_id, bypassing
    /// `next_wait_id`. That is not a shortcut: the id a waiter carries is
    /// minted by the registry of the shard its CONNECTION lives on, while the
    /// queue it lands in belongs to the shard that owns the KEY.
    fn park(
        reg: &mut BlockingRegistry,
        key: &Bytes,
        wait_id: u64,
        since: StreamId,
    ) -> crate::runtime::channel::OneshotReceiver<Option<Frame>> {
        let (tx, rx) = crate::runtime::channel::oneshot();
        reg.register(
            0,
            key.clone(),
            WaitEntry {
                wait_id,
                cmd: BlockedCommand::XRead {
                    streams: vec![(key.clone(), StreamSince::Id(since))],
                    count: None,
                },
                reply_tx: tx,
                deadline: None,
                claim: None,
            },
        );
        rx
    }

    fn ids_in(reply: Option<Frame>) -> Vec<String> {
        let Some(Frame::Array(streams)) = reply else {
            panic!("expected a woken reply, got {reply:?}");
        };
        let Some(Frame::Array(pair)) = streams.first().cloned() else {
            panic!("expected one stream pair");
        };
        let Some(Frame::Array(entries)) = pair.get(1).cloned() else {
            panic!("expected an entries array");
        };
        entries
            .iter()
            .map(|e| match e {
                Frame::Array(fields) => match fields.first() {
                    Some(Frame::BulkString(id)) => String::from_utf8_lossy(id).into_owned(),
                    other => panic!("expected an entry id, got {other:?}"),
                },
                other => panic!("expected an entry, got {other:?}"),
            })
            .collect()
    }

    /// moon#620: one `XADD` must wake BOTH parked readers, and each must get
    /// the entries ITS OWN cursor asked for — even when the two readers'
    /// `wait_id`s reach the owning shard's queue in descending order.
    ///
    /// `wait_id` is `(shard_id << 48) | counter`, minted by the registry of the
    /// shard the CONNECTION is pinned to, while the waiter is queued on the
    /// shard that owns the KEY. So a reader on shard 3 that parks before a
    /// reader on shard 1 puts a LARGER id ahead of a smaller one — queue order
    /// is not id order, and any code that assumes it is silently mispairs
    /// replies or drops a wakeup entirely.
    #[test]
    fn descending_wait_ids_wake_both_readers_with_their_own_entries() {
        let mut reg = BlockingRegistry::new(2);
        let mut db = Database::new();
        let key = Bytes::from_static(b"s");

        xadd(&mut db, "s", "1-1");
        xadd(&mut db, "s", "5-1");

        // Reader on shard 3 parks first (bigger id), reader on shard 1 second.
        // Distinct cursors, so a mispaired reply is visible in the CONTENT and
        // not only in a debug assertion.
        let early = park(&mut reg, &key, id_from_shard(3), StreamId { ms: 1, seq: 1 });
        let late = park(&mut reg, &key, id_from_shard(1), StreamId { ms: 5, seq: 1 });

        xadd(&mut db, "s", "7-1");
        let woke = try_wake_stream_waiter(&mut reg, &mut db, 0, &key);

        assert!(woke, "the XADD must wake the parked readers");
        assert_eq!(
            ids_in(early.try_recv().expect("shard-3 reader was not woken")),
            vec!["5-1".to_string(), "7-1".to_string()],
            "the reader bound at 1-1 must receive both later entries"
        );
        assert_eq!(
            ids_in(late.try_recv().expect("shard-1 reader was not woken")),
            vec!["7-1".to_string()],
            "the reader bound at 5-1 must receive only the new entry"
        );
    }

    /// The dead-waiter path (A2) crosses the same pairing. A departed reader
    /// whose id sorts AFTER a live sibling's contributes a `None` decision; if
    /// decisions were matched by position, that `None` would land on the live
    /// reader and swallow its wakeup.
    #[test]
    fn a_dead_reader_with_a_higher_wait_id_does_not_swallow_a_live_siblings_wakeup() {
        let mut reg = BlockingRegistry::new(2);
        let mut db = Database::new();
        let key = Bytes::from_static(b"s");

        xadd(&mut db, "s", "1-1");

        let dead = park(&mut reg, &key, id_from_shard(3), StreamId { ms: 1, seq: 1 });
        drop(dead); // client vanished (RST / CLIENT KILL / timeout cleanup)
        let live = park(&mut reg, &key, id_from_shard(1), StreamId { ms: 1, seq: 1 });

        xadd(&mut db, "s", "7-1");
        let woke = try_wake_stream_waiter(&mut reg, &mut db, 0, &key);

        assert!(woke, "the live reader must still be served");
        assert_eq!(
            ids_in(live.try_recv().expect("live reader was not woken")),
            vec!["7-1".to_string()],
        );
    }
}

/// The mapping [`wake_written_keys`] owns: which keys a write makes ready, and
/// what serving them does — including the moves a serve chains into.
///
/// Until moon#623 this was open-coded at eight dispatch sites, so a site could
/// disagree with its siblings and the disagreement showed up only as a
/// routing-dependent hang; until moon#1069 it named six commands. These pin
/// the single copy.
#[cfg(test)]
mod wake_written_keys_tests {
    use super::*;
    use crate::blocking::WaitEntry;
    use crate::storage::Database;
    use std::cell::RefCell;

    fn args(parts: &[&str]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect()
    }

    fn park_blpop(
        reg: &mut BlockingRegistry,
        key: &Bytes,
    ) -> crate::runtime::channel::OneshotReceiver<Option<Frame>> {
        let (tx, rx) = crate::runtime::channel::oneshot();
        reg.register(
            0,
            key.clone(),
            WaitEntry {
                wait_id: 1,
                cmd: BlockedCommand::BLPop,
                reply_tx: tx,
                deadline: None,
                claim: None,
            },
        );
        rx
    }

    fn served(rx: &crate::runtime::channel::OneshotReceiver<Option<Frame>>) -> Option<Frame> {
        rx.try_recv().ok().flatten()
    }

    #[test]
    fn lpush_wakes_the_reader_blocked_on_the_key_it_pushed() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let key = Bytes::from_static(b"wp:list");
        let rx = park_blpop(&mut reg.borrow_mut(), &key);

        let argv = args(&["wp:list", "v"]);
        assert_eq!(
            crate::command::list::lpush(&mut db, &argv),
            Frame::Integer(1)
        );
        assert!(
            wake_written_keys(&reg, &mut db, 0, b"LPUSH", &argv),
            "LPUSH must wake the BLPOP parked on its key"
        );
        assert!(served(&rx).is_some(), "the parked reader got no reply");
    }

    /// `LMOVE src dst ...` makes `dst` non-empty, so `dst` is the key that can
    /// satisfy a waiter — not `args[0]` (moon#520). A site that reads the
    /// source instead wakes nobody and leaves the real waiter parked.
    #[test]
    fn lmove_wakes_on_its_destination_not_its_source() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let src = Bytes::from_static(b"wp:src");
        let dst = Bytes::from_static(b"wp:dst");

        assert_eq!(
            crate::command::list::lpush(&mut db, &args(&["wp:src", "v"])),
            Frame::Integer(1)
        );
        let on_src = park_blpop(&mut reg.borrow_mut(), &src);
        let on_dst = park_blpop(&mut reg.borrow_mut(), &dst);

        let argv = args(&["wp:src", "wp:dst", "LEFT", "LEFT"]);
        let moved = crate::command::list::lmove(&mut db, &argv);
        assert!(matches!(moved, Frame::BulkString(_)), "LMOVE: {moved:?}");

        assert!(
            wake_written_keys(&reg, &mut db, 0, b"LMOVE", &argv),
            "LMOVE must wake the waiter on its DESTINATION"
        );
        assert!(served(&on_dst).is_some(), "destination waiter left parked");
        assert!(
            served(&on_src).is_none(),
            "the source key is now empty — its waiter must stay parked"
        );
    }

    #[test]
    fn a_non_producer_wakes_nobody_and_leaves_the_waiter_parked() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let key = Bytes::from_static(b"wp:list");
        let rx = park_blpop(&mut reg.borrow_mut(), &key);

        let argv = args(&["wp:list", "v"]);
        assert!(
            !wake_written_keys(&reg, &mut db, 0, b"GET", &argv),
            "GET writes nothing"
        );
        assert!(served(&rx).is_none(), "a non-producer answered a waiter");
    }

    #[test]
    fn xadd_wakes_a_stream_reader() {
        use crate::blocking::StreamSince;
        use crate::storage::stream::StreamId;

        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let key = Bytes::from_static(b"wp:stream");
        let (tx, rx) = crate::runtime::channel::oneshot();
        reg.borrow_mut().register(
            0,
            key.clone(),
            WaitEntry {
                wait_id: 1,
                cmd: BlockedCommand::XRead {
                    streams: vec![(key.clone(), StreamSince::Id(StreamId { ms: 0, seq: 0 }))],
                    count: None,
                },
                reply_tx: tx,
                deadline: None,
                claim: None,
            },
        );

        let argv = args(&["wp:stream", "1-1", "f", "v"]);
        assert!(matches!(
            crate::command::stream::xadd(&mut db, &argv),
            Frame::BulkString(_)
        ));
        assert!(
            wake_written_keys(&reg, &mut db, 0, b"XADD", &argv),
            "XADD must wake the XREAD parked on its key"
        );
        assert!(served(&rx).is_some(), "stream reader left parked");
    }

    fn park(
        reg: &RefCell<BlockingRegistry>,
        key: &str,
        wait_id: u64,
        cmd: BlockedCommand,
    ) -> crate::runtime::channel::OneshotReceiver<Option<Frame>> {
        let (tx, rx) = crate::runtime::channel::oneshot();
        reg.borrow_mut().register(
            0,
            Bytes::copy_from_slice(key.as_bytes()),
            WaitEntry {
                wait_id,
                cmd,
                reply_tx: tx,
                deadline: None,
                claim: None,
            },
        );
        rx
    }

    fn blmove(dst: &str) -> BlockedCommand {
        BlockedCommand::BLMove {
            destination: Bytes::copy_from_slice(dst.as_bytes()),
            wherefrom: Direction::Left,
            whereto: Direction::Right,
        }
    }

    fn keys(v: &[&str]) -> ReadyKeys {
        v.iter()
            .map(|k| Bytes::copy_from_slice(k.as_bytes()))
            .collect()
    }

    /// moon#1069: the ready keys are the positions a command WRITES, from the
    /// shared key walker — the destination of every `*STORE`, both keys of a
    /// rename, and nothing for a read.
    #[test]
    fn written_keys_are_the_write_positions_of_any_command() {
        let cases: &[(&str, &[&str], &[&str])] = &[
            ("RENAME", &["s", "d"], &["s", "d"]),
            ("COPY", &["s", "d"], &["d"]),
            ("SORT", &["s", "STORE", "d"], &["d"]),
            ("SORT", &["s", "BY", "w_*", "STORE", "d"], &["d"]),
            ("ZUNIONSTORE", &["d", "2", "a", "b"], &["d"]),
            ("ZRANGESTORE", &["d", "s", "0", "-1"], &["d"]),
            ("ZINCRBY", &["z", "1", "m"], &["z"]),
            ("GEOADD", &["g", "1", "2", "p"], &["g"]),
            ("RESTORE", &["k", "0", "payload"], &["k"]),
            ("LMOVE", &["s", "d", "LEFT", "LEFT"], &["s", "d"]),
            ("EVAL", &["return 1", "1", "k"], &["k"]),
            ("GET", &["k"], &[]),
            ("ZRANGE", &["k", "0", "-1"], &[]),
        ];
        for (cmd, argv, want) in cases {
            assert_eq!(
                written_keys(cmd.as_bytes(), &args(argv)),
                keys(want),
                "{cmd} {argv:?}"
            );
        }
    }

    #[test]
    fn cross_db_target_names_the_other_database_only() {
        let t =
            |cmd: &str, argv: &[&str]| cross_db_write_target(cmd.as_bytes(), &args(argv), 0, 16);
        assert_eq!(t("MOVE", &["k", "3"]), Some((3, Bytes::from_static(b"k"))));
        assert_eq!(
            t("COPY", &["s", "d", "DB", "3"]),
            Some((3, Bytes::from_static(b"d")))
        );
        assert_eq!(t("MOVE", &["k", "0"]), None, "same-db MOVE writes nothing");
        assert_eq!(
            t("COPY", &["s", "d"]),
            None,
            "same-db COPY is written_keys'"
        );
        assert_eq!(t("COPY", &["s", "d", "DB", "0"]), None);
        assert_eq!(t("RENAME", &["s", "d"]), None);
    }

    /// moon#1069: a key created by something other than a push wakes its
    /// waiter — here the destination of a `RENAME`.
    #[test]
    fn a_rename_destination_wakes_its_waiter() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let rx = park(&reg, "d", 1, BlockedCommand::BLPop);
        // The keyspace effect of `RENAME s d` on a one-element list.
        let _ = crate::command::list::lpush(&mut db, &args(&["d", "v"]));
        assert!(wake_written_keys(
            &reg,
            &mut db,
            0,
            b"RENAME",
            &args(&["s", "d"])
        ));
        assert!(
            served(&rx).is_some(),
            "the waiter on the destination stayed parked"
        );
        assert!(!db.exists(b"d"), "the waiter must have taken the element");
    }

    /// moon#1059: a `BLMOVE` served by a wake pushes onto its destination, and
    /// the destination's own waiter is served in the same pass.
    #[test]
    fn a_wake_served_move_feeds_the_destination_waiter() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let mover = park(&reg, "src", 1, blmove("dst"));
        let popper = park(&reg, "dst", 2, BlockedCommand::BLPop);
        let _ = crate::command::list::rpush(&mut db, &args(&["src", "x"]));
        assert!(try_wake_list_waiter(
            &mut reg.borrow_mut(),
            &mut db,
            0,
            &Bytes::from_static(b"src")
        ));
        assert_eq!(
            served(&mover),
            Some(Frame::BulkString(Bytes::from_static(b"x")))
        );
        assert!(
            served(&popper).is_some(),
            "BLPOP on the destination left parked"
        );
        assert!(
            !db.exists(b"dst"),
            "the element must have reached the BLPOP"
        );
        assert!(!reg.borrow().has_any_waiters());
    }

    /// A key served earlier in the walk becomes ready AGAIN when a later hop
    /// pushes back onto it — `a -> b`, `b -> a`, then `BLPOP a` takes it.
    /// Measured against redis-server 8.6.1: all three are answered `x`.
    #[test]
    fn a_move_cycle_serves_every_waiter_and_terminates() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let ab = park(&reg, "a", 1, blmove("b"));
        let ba = park(&reg, "b", 2, blmove("a"));
        let pop_a = park(&reg, "a", 3, BlockedCommand::BLPop);
        let _ = crate::command::list::rpush(&mut db, &args(&["a", "x"]));
        assert!(try_wake_list_waiter(
            &mut reg.borrow_mut(),
            &mut db,
            0,
            &Bytes::from_static(b"a")
        ));
        assert!(served(&ab).is_some());
        assert!(served(&ba).is_some());
        assert!(
            served(&pop_a).is_some(),
            "the element came back to `a` unserved"
        );
        assert!(!db.exists(b"a") && !db.exists(b"b"));
    }

    /// A ring of movers with one element and no final consumer ends once every
    /// mover has been served: the walk is bounded by the waiters parked when it
    /// began, not by a cap.
    #[test]
    fn a_ring_of_movers_is_bounded_by_its_waiters() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let ring = ["r0", "r1", "r2", "r3"];
        let rxs: Vec<_> = (0..ring.len())
            .map(|i| {
                park(
                    &reg,
                    ring[i],
                    i as u64 + 1,
                    blmove(ring[(i + 1) % ring.len()]),
                )
            })
            .collect();
        let _ = crate::command::list::rpush(&mut db, &args(&["r0", "x"]));
        assert!(try_wake_list_waiter(
            &mut reg.borrow_mut(),
            &mut db,
            0,
            &Bytes::from_static(b"r0")
        ));
        for rx in &rxs {
            assert!(served(rx).is_some(), "a mover in the ring was skipped");
        }
        // Four hops of one element: it ends on `r0` again.
        assert!(db.exists(b"r0"));
        assert!(!reg.borrow().has_any_waiters());
    }

    /// The source-type guard: a `SET` on a `BLMOVE`'s source must leave the
    /// mover parked, even when its DESTINATION is of the wrong type — redis
    /// matches the key's type to the waiter before looking at the destination.
    #[test]
    fn a_wrong_typed_source_leaves_a_mover_parked() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        db.set_string(b"dst", Bytes::from_static(b"str"));
        let rx = park(&reg, "src", 1, blmove("dst"));
        db.set_string(b"src", Bytes::from_static(b"str"));
        assert!(!wake_written_keys(
            &reg,
            &mut db,
            0,
            b"SET",
            &args(&["src", "str"])
        ));
        assert!(
            served(&rx).is_none(),
            "a string source answered a list waiter"
        );
        assert!(reg.borrow().has_waiters(0, &Bytes::from_static(b"src")));
    }

    /// Script writes are recorded per key (a run of writes to one key once)
    /// and taken exactly once.
    #[test]
    fn script_writes_are_recorded_then_taken_once() {
        begin_script_writes(&ScriptWakes::Defer { armed: true });
        note_script_write(0, b"RPUSH", &args(&["q", "a"]));
        note_script_write(0, b"RPUSH", &args(&["q", "b"]));
        note_script_write(2, b"ZADD", &args(&["z", "1", "m"]));
        note_script_write(0, b"GET", &args(&["q"]));
        assert_eq!(
            take_script_writes(),
            vec![(0, Bytes::from_static(b"q")), (2, Bytes::from_static(b"z"))]
        );
        assert!(take_script_writes().is_empty());
    }

    /// moon#1068: a script's `MOVE` / `COPY ... DB n` records the key in the
    /// DESTINATION db, once, among the script's other ready keys — and only
    /// while the script's wakes are armed.
    #[test]
    fn script_cross_db_writes_record_the_destination() {
        let q = Bytes::from_static(b"q");
        begin_script_writes(&ScriptWakes::Defer { armed: true });
        note_script_write(0, b"RPUSH", &args(&["q", "a"]));
        note_script_cross_db_write(9, &q);
        note_script_cross_db_write(9, &q);
        assert_eq!(take_script_writes(), vec![(0, q.clone()), (9, q.clone())]);

        begin_script_writes(&ScriptWakes::Defer { armed: false });
        note_script_cross_db_write(9, &q);
        assert!(take_script_writes().is_empty(), "recorded while disarmed");
    }

    /// A script that starts while nobody on the shard is blocked records
    /// nothing, and a write that cannot produce a list, zset or stream is
    /// never recorded even when armed.
    #[test]
    fn script_writes_are_recorded_only_when_someone_can_wake() {
        let reg = RefCell::new(BlockingRegistry::new(0));
        begin_script_writes(&ScriptWakes::Serve(&reg));
        note_script_write(0, b"RPUSH", &args(&["q", "a"]));
        assert!(take_script_writes().is_empty(), "recorded with no waiter");

        begin_script_writes(&ScriptWakes::Defer { armed: false });
        note_script_write(0, b"RPUSH", &args(&["q", "a"]));
        assert!(take_script_writes().is_empty(), "recorded while disarmed");

        let _rx = park(&reg, "q", 1, BlockedCommand::BLPop);
        begin_script_writes(&ScriptWakes::Serve(&reg));
        note_script_write(0, b"SET", &args(&["q", "a"]));
        note_script_write(0, b"HSET", &args(&["q", "f", "v"]));
        assert!(
            take_script_writes().is_empty(),
            "a string/hash write recorded"
        );
        // Taking disarms: a stray write after the script is not recorded.
        note_script_write(0, b"RPUSH", &args(&["q", "a"]));
        assert!(take_script_writes().is_empty(), "recorded after the take");
    }

    /// The command-table gate: only a write whose family can leave a list,
    /// zset or stream behind (or that moves a value of any type) can make a
    /// key ready.
    #[test]
    fn only_list_zset_stream_and_generic_writes_may_ready_a_key() {
        for cmd in [
            "RPUSH",
            "LPUSH",
            "LMOVE",
            "BLMOVE",
            "RPOPLPUSH",
            "LINSERT",
            "ZADD",
            "ZINCRBY",
            "ZUNIONSTORE",
            "ZRANGESTORE",
            "GEOADD",
            "GEOSEARCHSTORE",
            "XADD",
            "RENAME",
            "COPY",
            "MOVE",
            "RESTORE",
            "SORT",
        ] {
            assert!(may_ready_a_key(cmd.as_bytes()), "{cmd} must be walked");
        }
        for cmd in [
            "SET",
            "MSET",
            "MSETNX",
            "APPEND",
            "INCR",
            "SETBIT",
            "BITOP",
            "PFADD",
            "PFMERGE",
            "HSET",
            "SADD",
            "SUNIONSTORE",
            "GET",
            "LRANGE",
            "ZRANGE",
            "XRANGE",
            "EVAL",
            "NOPE",
        ] {
            assert!(!may_ready_a_key(cmd.as_bytes()), "{cmd} must not be walked");
        }
    }

    /// redis serves the ready keys of one command as ONE batch, in the order
    /// they became ready, and the keys a served move pushes onto join the END
    /// of it: `BLMOVE a c`, `BLMOVE b c`, `BRPOP c`, then `RPUSH a x; RPUSH b
    /// y` in one EXEC. Both moves are served before `c` is, so `BRPOP` takes
    /// the LAST element pushed (`y`) and `c` keeps `x`. Measured against
    /// redis-server 8.6.1; serving `c` right after `a` gave `BRPOP` `x`.
    #[test]
    fn one_batch_serves_every_seeded_key_before_the_moves_they_feed() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let w1 = park(&reg, "a", 1, blmove("c"));
        let w2 = park(&reg, "b", 2, blmove("c"));
        let w3 = park(&reg, "c", 3, BlockedCommand::BRPop);
        let _ = crate::command::list::rpush(&mut db, &args(&["a", "x"]));
        let _ = crate::command::list::rpush(&mut db, &args(&["b", "y"]));
        assert!(wake_keys(
            &mut reg.borrow_mut(),
            &mut db,
            0,
            keys(&["a", "b"])
        ));
        let bulk = |v: &'static [u8]| Frame::BulkString(Bytes::from_static(v));
        assert_eq!(served(&w1), Some(bulk(b"x")));
        assert_eq!(served(&w2), Some(bulk(b"y")));
        assert_eq!(
            served(&w3),
            Some(Frame::Array(vec![bulk(b"c"), bulk(b"y")].into())),
            "BRPOP must take the element the LAST move pushed"
        );
        assert_eq!(
            crate::command::list::lrange(&mut db, &args(&["c", "0", "-1"])),
            Frame::Array(vec![bulk(b"x")].into())
        );
        assert!(!reg.borrow().has_any_waiters());
    }

    /// `wake_key` on a list key serves its `BLPOP` and leaves a `BZPOPMIN`
    /// parked ahead of it in the same queue, unanswered and in place. (That it
    /// does so WITHOUT scanning the queue for zset and stream waiters is a
    /// cost, not a behaviour, and is not observable here.)
    #[test]
    fn wake_key_leaves_other_families_parked_in_place() {
        let mut db = Database::new();
        let reg = RefCell::new(BlockingRegistry::new(0));
        let zpop = park(&reg, "k", 1, BlockedCommand::BZPopMin);
        let lpop = park(&reg, "k", 2, BlockedCommand::BLPop);
        let _ = crate::command::list::rpush(&mut db, &args(&["k", "v"]));
        assert!(wake_key(
            &mut reg.borrow_mut(),
            &mut db,
            0,
            &Bytes::from_static(b"k")
        ));
        assert!(served(&lpop).is_some(), "the list waiter was not served");
        assert!(
            served(&zpop).is_none(),
            "a zset waiter answered from a list"
        );
        let reg = reg.borrow();
        let queue = reg
            .waiters_on(0, &Bytes::from_static(b"k"))
            .expect("the zset waiter is still queued");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].wait_id, 1);
    }
}
