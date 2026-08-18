use bytes::Bytes;

use crate::blocking::{BlockedCommand, BlockingRegistry, Direction};
use crate::command::sorted_set::format_score_bytes;
use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

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

/// Called after XADD successfully adds an entry to a stream key.
/// Pops the first waiter (FIFO) and checks if the stream has entries > last_seen_id.
/// For XRead: delivers entries from stream.range(last_seen_id+1.., count).
/// For XReadGroup with >: delivers via stream.read_group_new().
/// Returns true if a blocked client was woken.
pub fn try_wake_stream_waiter(
    registry: &mut BlockingRegistry,
    db: &mut Database,
    db_index: usize,
    key: &Bytes,
) -> bool {
    use crate::command::stream::format_entry;
    use crate::storage::stream::StreamId;

    // moon#535: pop only waiters THIS waker can serve. The old blind
    // `pop_front` handed us waiters of every family, and the cleanup below —
    // `remove_wait` + `send(None)` — runs for every waiter we pop, so an
    // unservable one was destroyed rather than left for its own waker.
    //
    // The loop condition moved from `has_waiters` to the pop itself: a queue
    // holding only foreign waiters is not empty, so the old condition would
    // now spin forever.
    while let Some(waiter) =
        registry.pop_front_of_family(db_index, key, crate::blocking::WaitFamily::Stream)
    {
        let crate::blocking::WaitEntry {
            wait_id,
            cmd,
            reply_tx,
            ..
        } = waiter;

        // A2: skip waiters whose client is already gone. Unlike the list and
        // zset paths there is no undo here, and none is needed: XRead is a
        // non-destructive range read, and XReadGroup's delivery leaves the
        // entries in the stream — only the PEL records them as delivered to a
        // consumer that vanished, which is exactly Redis's dead-consumer
        // semantics (recoverable via XAUTOCLAIM). The pre-check still matters
        // so a dead waiter does not consume the wake that a live one needs.
        if reply_tx.is_disconnected() {
            registry.remove_wait(wait_id);
            continue;
        }

        let result = match &cmd {
            BlockedCommand::XRead { streams, count } => {
                // Find this key's last_seen_id in the streams list
                let last_seen = streams.iter().find(|(k, _)| k == key).map(|(_, id)| *id);
                if let Some(last_id) = last_seen {
                    if let Ok(Some(stream)) = db.get_stream(key) {
                        let start = if last_id.seq == u64::MAX {
                            StreamId {
                                ms: last_id.ms.saturating_add(1),
                                seq: 0,
                            }
                        } else {
                            StreamId {
                                ms: last_id.ms,
                                seq: last_id.seq.saturating_add(1),
                            }
                        };
                        let entries = stream.range(start, StreamId::MAX, *count);
                        if !entries.is_empty() {
                            let entry_frames: Vec<crate::protocol::Frame> = entries
                                .iter()
                                .map(|(id, fields)| format_entry(*id, fields))
                                .collect();
                            Some(crate::protocol::Frame::Array(framevec![
                                crate::protocol::Frame::Array(framevec![
                                    crate::protocol::Frame::BulkString(key.clone()),
                                    crate::protocol::Frame::Array(entry_frames.into()),
                                ])
                            ]))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            BlockedCommand::XReadGroup {
                group,
                consumer,
                count,
                noack,
                ..
            } => {
                if let Ok(Some(stream)) = db.get_stream_mut(key) {
                    match stream.read_group_new(group, consumer, *count, *noack) {
                        Ok(entries) if !entries.is_empty() => {
                            let entry_frames: Vec<crate::protocol::Frame> = entries
                                .iter()
                                .map(|(id, fields)| format_entry(*id, fields))
                                .collect();
                            Some(crate::protocol::Frame::Array(framevec![
                                crate::protocol::Frame::Array(framevec![
                                    crate::protocol::Frame::BulkString(key.clone()),
                                    crate::protocol::Frame::Array(entry_frames.into()),
                                ])
                            ]))
                        }
                        _ => None,
                    }
                } else {
                    None
                }
            }
            // Unreachable since moon#535 — see try_wake_list_waiter.
            _ => None,
        };

        // Clean up all other key registrations for this wait_id
        registry.remove_wait(wait_id);

        if let Some(frame) = result {
            let _ = reply_tx.send(Some(frame));
            return true;
        }
        let _ = reply_tx.send(None);
    }
    false
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
