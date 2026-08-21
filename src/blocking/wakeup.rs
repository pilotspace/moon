use bytes::Bytes;

use crate::blocking::{BlockedCommand, BlockingRegistry, Direction};
use crate::command::sorted_set::format_score_bytes;
use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

/// Does `cmd` PUSH onto a list, and so possibly satisfy a blocked
/// `BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH` waiter?
///
/// This decision is open-coded at eight dispatch sites (two connection
/// handlers and six arms of the SPSC handler), which is why it is a function
/// and not a literal at each one: a producer that one site wakes on and
/// another does not is a routing-dependent hang — the waiter returns instantly
/// or blocks to its timeout depending on which shard owns the key, which reads
/// as a flake rather than as a bug. `RPOPLPUSH` is `LMOVE ... RIGHT LEFT` and
/// so must answer identically here (moon#520).
#[inline]
pub fn is_list_producer(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"LPUSH")
        || cmd.eq_ignore_ascii_case(b"RPUSH")
        || cmd.eq_ignore_ascii_case(b"LMOVE")
        || cmd.eq_ignore_ascii_case(b"RPOPLPUSH")
}

/// Does `cmd` write data that could satisfy SOME blocked waiter — list, zset
/// or stream?
///
/// The gate in front of every wakeup ladder, and it is a function for the same
/// reason [`is_list_producer`] is: it was open-coded at ten dispatch sites,
/// eight of which said `is_list_producer(cmd) || ZADD || XADD` while the two
/// connection handlers said only `is_list_producer(cmd) || ZADD`. That
/// difference is half of moon#595 — with the missing `XADD`, a stream reader
/// blocked on a key its OWN shard owned could never be woken, because the
/// local write path did not consider stream waiters to exist. It reads as a
/// routing-dependent hang: the same `XADD` wakes or does not wake depending on
/// whether the writer's connection happens to live on the key's shard.
///
/// Producers that gain a blocking consumer later must be added HERE, once.
#[inline]
pub fn is_producer(cmd: &[u8]) -> bool {
    is_list_producer(cmd) || cmd.eq_ignore_ascii_case(b"ZADD") || cmd.eq_ignore_ascii_case(b"XADD")
}

/// Index of the argument naming the key a producer WRITES to — the key a
/// blocked client is waiting on.
///
/// `LMOVE`/`RPOPLPUSH` push to their DESTINATION (`args[1]`); everything else
/// that shares these wakeup guards (`LPUSH`, `RPUSH`, `ZADD`, `XADD`) writes
/// to `args[0]`. Waking on `LMOVE`'s source is a no-op dressed as a wakeup:
/// the element left that key.
#[inline]
pub fn producer_wake_key_index(cmd: &[u8]) -> usize {
    if cmd.eq_ignore_ascii_case(b"LMOVE") || cmd.eq_ignore_ascii_case(b"RPOPLPUSH") {
        1
    } else {
        0
    }
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
enum WakeUndo {
    /// Values popped from the FRONT of the key, in pop order.
    ListFront(smallvec::SmallVec<[bytes::Bytes; 4]>),
    /// Values popped from the BACK of the key, in pop order.
    ListBack(smallvec::SmallVec<[bytes::Bytes; 4]>),
    /// BLMOVE: one value left the key via `wherefrom` and was pushed onto
    /// `destination` via `whereto`. Undoing means reversing BOTH halves.
    Moved {
        destination: Bytes,
        wherefrom: Direction,
        whereto: Direction,
    },
    /// (member, score) pairs popped from the sorted set at the key.
    Zset(smallvec::SmallVec<[(bytes::Bytes, f64); 4]>),
}

impl WakeUndo {
    /// Put everything back exactly where it came from.
    fn restore(self, db: &mut Database, key: &Bytes) {
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
            } => {
                // Take back the element we just pushed onto the destination.
                // It is still at the end we pushed it to: this runs on the
                // shard thread with no await in between, so nothing else can
                // have touched the list.
                let moved = match whereto {
                    Direction::Left => db.list_pop_front(&destination),
                    Direction::Right => db.list_pop_back(&destination),
                };
                if let Some(v) = moved {
                    match wherefrom {
                        Direction::Left => db.list_push_front(key, v),
                        Direction::Right => db.list_push_back(key, v),
                    }
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

/// Called after LPUSH/RPUSH successfully adds elements to a list key.
/// Pops the first waiter (FIFO) and executes the appropriate pop operation.
/// Returns true if a blocked client was woken (element was consumed by the waiter).
///
/// The caller must hold mutable borrows on both the registry and the database.
pub fn try_wake_list_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
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
    while let Some(waiter) =
        registry.pop_front_of_family(db_index, key, crate::blocking::WaitFamily::List)
    {
        let crate::blocking::WaitEntry {
            wait_id,
            cmd,
            reply_tx,
            ..
        } = waiter;

        // A2: never mutate the datastore on behalf of a waiter whose client
        // is already gone. Reap the registration and move to the next waiter
        // with the key untouched.
        if reply_tx.is_disconnected() {
            registry.remove_wait(wait_id);
            continue;
        }

        // Execute the pop based on command type
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
                    db.get_list(destination).err()
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
                                Some(Frame::BulkString(v)),
                                Some(WakeUndo::Moved {
                                    destination: destination.clone(),
                                    wherefrom: *wherefrom,
                                    whereto: *whereto,
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

        // Clean up all other key registrations for this wait_id
        registry.remove_wait(wait_id);

        if let Some(frame) = result {
            if reply_tx.send(Some(frame)).is_ok() {
                return true;
            }
            // A2 residual race: the receiver dropped between the liveness
            // check above and this send. Put the data back and offer it to
            // the next waiter instead of destroying it.
            if let Some(undo) = undo {
                undo.restore(db, key);
            }
            continue;
        }
        // If pop returned None (list became empty -- shouldn't happen in single-threaded
        // model but handle gracefully), try next waiter
        let _ = reply_tx.send(None);
    }
    false
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
    // moon#535: pop only waiters THIS waker can serve. The old blind
    // `pop_front` handed us waiters of every family, and the cleanup below —
    // `remove_wait` + `send(None)` — runs for every waiter we pop, so an
    // unservable one was destroyed rather than left for its own waker.
    //
    // The loop condition moved from `has_waiters` to the pop itself: a queue
    // holding only foreign waiters is not empty, so the old condition would
    // now spin forever.
    while let Some(waiter) =
        registry.pop_front_of_family(db_index, key, crate::blocking::WaitFamily::ZSet)
    {
        let crate::blocking::WaitEntry {
            wait_id,
            cmd,
            reply_tx,
            ..
        } = waiter;

        // A2: see try_wake_list_waiter — never pop for a dead client.
        if reply_tx.is_disconnected() {
            registry.remove_wait(wait_id);
            continue;
        }

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

        registry.remove_wait(wait_id);

        if let Some(frame) = result {
            if reply_tx.send(Some(frame)).is_ok() {
                return true;
            }
            // A2 residual race — restore and offer to the next waiter.
            if let Some(undo) = undo {
                undo.restore(db, key);
            }
            continue;
        }
        let _ = reply_tx.send(None);
    }
    false
}

/// Which waker a producer command's write should raise, or `None` when the
/// command is not a producer at all.
///
/// Pairs with [`producer_wake_key_index`]: together they are everything a
/// write site needs to raise the right wake, so a new execution path can hook
/// in without re-deriving the command-to-waker mapping and getting it subtly
/// wrong. `EXEC` was such a path — it ran producers through its own executor
/// and reached none of the existing hooks, so a `MULTI ; LPUSH k v ; EXEC`
/// left a client blocked on `k` asleep until its own timeout (moon#606).
pub fn producer_family(cmd: &[u8]) -> Option<crate::blocking::WaitFamily> {
    if !is_producer(cmd) {
        return None;
    }
    Some(if is_list_producer(cmd) {
        crate::blocking::WaitFamily::List
    } else if cmd.eq_ignore_ascii_case(b"ZADD") {
        crate::blocking::WaitFamily::ZSet
    } else {
        crate::blocking::WaitFamily::Stream
    })
}

/// Raise `family`'s waker for `key`. Returns true if a client was answered.
pub fn wake_family(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
    family: crate::blocking::WaitFamily,
) -> bool {
    match family {
        crate::blocking::WaitFamily::List => try_wake_list_waiter(registry, db, db_index, key),
        crate::blocking::WaitFamily::ZSet => try_wake_zset_waiter(registry, db, db_index, key),
        crate::blocking::WaitFamily::Stream => try_wake_stream_waiter(registry, db, db_index, key),
    }
}

/// Called after `XADD` adds an entry to a stream key, and again right after a
/// remote `BlockRegister` lands, to serve whatever stream readers that key now
/// has parked on it.
///
/// Returns true if at least one blocked client was answered.
///
/// # Why this is not shaped like the list and zset wakers
///
/// Those wakers CONSUME: a pushed element belongs to exactly one waiter, so
/// `pop_front_of_family` + answer + `return true` is the whole operation, and
/// a waiter they cannot serve means the key really is empty.
///
/// Stream reads are non-destructive, and both halves of that matter
/// (both measured against redis-server 8.6.1):
///
/// * **one `XADD` wakes EVERY parked `XREAD`.** Two clients on
///   `XREAD BLOCK 5000 STREAMS k $` each receive the entry from a single
///   `XADD`. Stopping at the first served waiter would have left the second
///   parked until its deadline.
/// * **a waiter this `XADD` cannot serve must stay parked.** The pre-#595
///   code ran `remove_wait` + `reply_tx.send(None)` for every waiter it
///   popped, servable or not — so an `XADD` at an id BELOW a `$`-bound
///   reader's cursor, or an `XREADGROUP` whose entries a sibling consumer
///   just took, unblocked that reader with a premature null. That same
///   `send(None)` is what would have fired on the re-check the
///   `BlockRegister` handler runs immediately after registering, making a
///   remote `XREAD BLOCK` answer null the instant it was registered.
///
/// So this walks the key's stream-family waiters in FIFO order, decides each
/// one against the store while it is still queued ([`peek_wait`]), and only
/// removes the ones it can actually answer ([`take_wait`]). Nothing is ever
/// answered `None` here; a waiter that is not served stays registered and is
/// released by its own deadline, its client's disconnect, or a later `XADD`.
///
/// [`peek_wait`]: BlockingRegistry::peek_wait
/// [`take_wait`]: BlockingRegistry::take_wait
pub fn try_wake_stream_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    // Decide first, mutate second. One pass over the queue, with every waiter
    // still in place, so a decision of "cannot serve" costs nothing and leaves
    // FIFO order untouched.
    //
    // Queue order is NOT `wait_id` order (moon#620). An id is
    // `(shard_id << 48) | counter`, minted by the registry of the shard the
    // waiter's CONNECTION lives on, while the queue belongs to the shard that
    // owns the KEY — so a reader on shard 3 that parks before a reader on
    // shard 1 puts the larger id first. Everything downstream of here treats
    // the two orders as independent.
    let mut decisions: smallvec::SmallVec<[(u64, Option<Frame>); 4]> = smallvec::SmallVec::new();
    {
        let Some(queue) = registry.waiters_on(db_index, key) else {
            return false;
        };
        for entry in queue
            .iter()
            .filter(|e| e.cmd.family() == crate::blocking::WaitFamily::Stream)
        {
            // c10k A2: a client that already went away must not consume the
            // wake a live sibling needs. There is nothing to undo on this
            // path — `XREAD` mutates nothing, and `XREADGROUP`'s delivery
            // leaves the entries in the stream and only records them in the
            // PEL of a consumer that vanished, which is precisely Redis's
            // dead-consumer state (recoverable via `XAUTOCLAIM`).
            if entry.reply_tx.is_disconnected() {
                decisions.push((entry.wait_id, None));
            } else if let Some(frame) = serve_stream_waiter(&entry.cmd, db, key) {
                decisions.push((entry.wait_id, Some(frame)));
            }
            // Anything else stays REGISTERED and is released by its own
            // deadline, its client's disconnect, or a later XADD. It is never
            // answered `None` here.
        }
    }
    if decisions.is_empty() {
        return false;
    }

    // `take_waits` looks each id up with a binary search, so it must be handed
    // a SORTED slice — queue order will not do (see above). An unsorted slice
    // makes the search miss ids that are present, which silently leaves those
    // waiters parked until their own deadline: the lost wakeup moon#620 was
    // filed for.
    let mut ids: smallvec::SmallVec<[u64; 4]> = decisions.iter().map(|(id, _)| *id).collect();
    ids.sort_unstable();

    let mut woke = false;
    // Pair each returned entry with its decision BY `wait_id`, never by
    // position: `take_waits` hands entries back in queue order while `ids` is
    // sorted, and a positional pairing would hand one reader the entries
    // computed for another's cursor.
    for entry in registry.take_waits(db_index, key, &ids) {
        let Some(slot) = decisions.iter_mut().find(|(id, _)| *id == entry.wait_id) else {
            debug_assert!(false, "take_waits returned an entry we did not ask for");
            continue;
        };
        let Some(frame) = slot.1.take() else {
            continue; // the disconnected client — removed, nothing to send
        };
        if entry.reply_tx.send(Some(frame)).is_ok() {
            woke = true;
        }
        // A failed send is the residual A2 race — the receiver dropped between
        // the check above and here. Nothing to restore: the entries are still
        // in the stream.
    }
    woke
}

/// The error a blocking stream read owes its client IMMEDIATELY, decided on
/// the shard that owns `key` (moon#595).
///
/// Registering is the wrong answer to a question the keyspace has already
/// settled. `-WRONGTYPE` and `XREADGROUP`'s two errors are permanent for as
/// long as the key is what it is: a group that does not exist cannot start
/// existing because someone `XADD`s to the stream, so a waiter parked on that
/// hope would burn its whole budget and then answer the null array.
///
/// It runs HERE, in the `BlockRegister` handler, and not only in the client's
/// own pre-registration scan, because that scan can see only the keys its
/// shard owns. Without this, `XREADGROUP GROUP nope c BLOCK 800 STREAMS k >`
/// answered `-NOGROUP` in 0.000 s when `k` hashed to the client's own shard
/// and parked for the full 800 ms when it did not — the same command, two
/// answers, decided by a hash.
///
/// `None` means "nothing settled; park".
pub fn stream_register_error(
    db: &mut Database,
    key: &Bytes,
    cmd: &BlockedCommand,
) -> Option<Frame> {
    // Wrong type is wrong type for both stream readers.
    if let Some(err) = db.get_stream(key).err() {
        return Some(err);
    }
    let BlockedCommand::XReadGroup { group, .. } = cmd else {
        // A plain XREAD on a missing key is not an error — that is exactly the
        // `$`-on-a-future-stream case, and it must park.
        return None;
    };
    let Ok(Some(stream)) = db.get_stream(key) else {
        return Some(Frame::Error(Bytes::from_static(
            b"ERR The XREADGROUP subcommand requires the key to exist.",
        )));
    };
    if !stream.groups.contains_key(group.as_ref()) {
        return Some(Frame::Error(Bytes::from_static(
            b"NOGROUP No such consumer group for key name",
        )));
    }
    None
}

/// The reply a parked stream reader is owed by the current state of `key`, or
/// `None` if this key cannot serve it yet.
///
/// Split out of [`try_wake_stream_waiter`] so the "can I serve this?" question
/// is answerable against a borrowed [`WaitEntry`], which is what keeps an
/// unservable waiter in the queue.
fn serve_stream_waiter(cmd: &BlockedCommand, db: &mut Database, key: &Bytes) -> Option<Frame> {
    use crate::command::stream::format_entry;
    use crate::storage::stream::StreamId;

    // Each arm builds its own frames: `range` hands back BORROWED field lists
    // while `read_group_new` hands back owned ones, so there is no common
    // `entries` type to carry out of the match.
    let entry_frames: Vec<Frame> = match cmd {
        BlockedCommand::XRead { streams, count } => {
            // `find` rather than an index: a multi-key XREAD registers the
            // same command on several keys and only this key's cursor applies.
            //
            // `StreamSince::Latest` here means `$` was never bound to a
            // number. That is a binding bug, not a client state — and it
            // resolves to "serve nothing" deliberately: treating it as `0-0`
            // would replay the stream's whole history to a client that asked
            // only for what arrives next.
            let since = streams
                .iter()
                .find(|(k, _)| k == key)
                .and_then(|(_, since)| since.id())?;
            let start = if since.seq == u64::MAX {
                StreamId {
                    ms: since.ms.saturating_add(1),
                    seq: 0,
                }
            } else {
                StreamId {
                    ms: since.ms,
                    seq: since.seq.saturating_add(1),
                }
            };
            let stream = db.get_stream(key).ok()??;
            let entries = stream.range(start, StreamId::MAX, *count);
            if entries.is_empty() {
                return None;
            }
            entries
                .into_iter()
                .map(|(id, fields)| format_entry(id, fields))
                .collect()
        }
        BlockedCommand::XReadGroup {
            group,
            consumer,
            count,
            noack,
            ..
        } => {
            let stream = db.get_stream_mut(key).ok()??;
            // Only reaches the store when a live waiter is actually waiting on
            // it, so the PEL side effect never happens on behalf of a client
            // that has already gone (checked by the caller).
            let entries = stream
                .read_group_new(group, consumer, *count, *noack)
                .ok()?;
            if entries.is_empty() {
                return None;
            }
            entries
                .iter()
                .map(|(id, fields)| format_entry(*id, fields))
                .collect()
        }
        // Unreachable since moon#535: `family()` routes only the two stream
        // commands here, and `family_wait_ids` filtered on it.
        _ => return None,
    };
    // Only the stream that actually had entries appears, which is both what
    // Redis answers a woken reader and what moon#594 made the non-blocking
    // XREAD do.
    Some(Frame::Array(framevec![Frame::Array(framevec![
        Frame::BulkString(key.clone()),
        Frame::Array(entry_frames.into()),
    ])]))
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
            dst.clone(),
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
