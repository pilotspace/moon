use bytes::Bytes;

use crate::blocking::{BlockedCommand, BlockingRegistry, Direction};
use crate::command::sorted_set::format_score_bytes;
use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
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
    while registry.has_waiters(db_index, key) {
        let waiter = match registry.pop_front(db_index, key) {
            Some(w) => w,
            None => return false,
        };
        let wait_id = waiter.wait_id;

        // Execute the pop based on command type
        let result = match &waiter.cmd {
            BlockedCommand::BLPop => {
                // Pop from left, return [key, value]
                let val = db.list_pop_front(key);
                val.map(|v| {
                    Frame::Array(framevec![
                        Frame::BulkString(key.clone()),
                        Frame::BulkString(v),
                    ])
                })
            }
            BlockedCommand::BRPop => {
                // Pop from right, return [key, value]
                let val = db.list_pop_back(key);
                val.map(|v| {
                    Frame::Array(framevec![
                        Frame::BulkString(key.clone()),
                        Frame::BulkString(v),
                    ])
                })
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
                val.map(|v| {
                    // Push to destination
                    match whereto {
                        Direction::Left => db.list_push_front(destination, v.clone()),
                        Direction::Right => db.list_push_back(destination, v.clone()),
                    }
                    Frame::BulkString(v)
                })
            }
            BlockedCommand::BLMPop { dir, count } => {
                let mut elems = smallvec::SmallVec::<[Frame; 16]>::new();
                let n = *count as usize;
                for _ in 0..n {
                    let val = match dir {
                        Direction::Left => db.list_pop_front(key),
                        Direction::Right => db.list_pop_back(key),
                    };
                    match val {
                        Some(v) => elems.push(Frame::BulkString(v)),
                        None => break,
                    }
                }
                if elems.is_empty() {
                    None
                } else {
                    let elem_vec: Vec<Frame> = elems.into_vec();
                    Some(Frame::Array(framevec![
                        Frame::BulkString(key.clone()),
                        Frame::Array(elem_vec.into()),
                    ]))
                }
            }
            _ => None, // BZPopMin/BZPopMax don't watch list keys
        };

        // Clean up all other key registrations for this wait_id
        registry.remove_wait(wait_id);

        if let Some(frame) = result {
            let _ = waiter.reply_tx.send(Some(frame));
            return true;
        }
        // If pop returned None (list became empty -- shouldn't happen in single-threaded
        // model but handle gracefully), try next waiter
        let _ = waiter.reply_tx.send(None);
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
    while registry.has_waiters(db_index, key) {
        let waiter = match registry.pop_front(db_index, key) {
            Some(w) => w,
            None => return false,
        };
        let wait_id = waiter.wait_id;

        let result = match &waiter.cmd {
            BlockedCommand::BZPopMin => db.zset_pop_min(key).map(|(member, score)| {
                Frame::Array(framevec![
                    Frame::BulkString(key.clone()),
                    Frame::BulkString(member),
                    Frame::BulkString(format_score_bytes(score)),
                ])
            }),
            BlockedCommand::BZPopMax => db.zset_pop_max(key).map(|(member, score)| {
                Frame::Array(framevec![
                    Frame::BulkString(key.clone()),
                    Frame::BulkString(member),
                    Frame::BulkString(format_score_bytes(score)),
                ])
            }),
            BlockedCommand::BZMPop { min, count } => {
                let n = *count as usize;
                let mut elems = smallvec::SmallVec::<[Frame; 16]>::new();
                for _ in 0..n {
                    let popped = if *min {
                        db.zset_pop_min(key)
                    } else {
                        db.zset_pop_max(key)
                    };
                    match popped {
                        Some((member, score)) => {
                            elems.push(Frame::Array(framevec![
                                Frame::BulkString(member),
                                Frame::BulkString(format_score_bytes(score)),
                            ]));
                        }
                        None => break,
                    }
                }
                if elems.is_empty() {
                    None
                } else {
                    let elem_vec: Vec<Frame> = elems.into_vec();
                    Some(Frame::Array(framevec![
                        Frame::BulkString(key.clone()),
                        Frame::Array(elem_vec.into()),
                    ]))
                }
            }
            _ => None, // List commands don't watch zset keys
        };

        registry.remove_wait(wait_id);

        if let Some(frame) = result {
            let _ = waiter.reply_tx.send(Some(frame));
            return true;
        }
        let _ = waiter.reply_tx.send(None);
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

    while registry.has_waiters(db_index, key) {
        let waiter = match registry.pop_front(db_index, key) {
            Some(w) => w,
            None => return false,
        };
        let wait_id = waiter.wait_id;

        let result = match &waiter.cmd {
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
            _ => None, // List/zset commands don't watch stream keys
        };

        // Clean up all other key registrations for this wait_id
        registry.remove_wait(wait_id);

        if let Some(frame) = result {
            let _ = waiter.reply_tx.send(Some(frame));
            return true;
        }
        let _ = waiter.reply_tx.send(None);
    }
    false
}
