use bytes::Bytes;

use crate::blocking::{BlockedCommand, BlockingRegistry, Direction};
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
                    Frame::Array(vec![
                        Frame::BulkString(key.clone()),
                        Frame::BulkString(v),
                    ])
                })
            }
            BlockedCommand::BRPop => {
                // Pop from right, return [key, value]
                let val = db.list_pop_back(key);
                val.map(|v| {
                    Frame::Array(vec![
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
            BlockedCommand::BZPopMin => {
                // Pop min, return [key, member, score]
                db.zset_pop_min(key).map(|(member, score)| {
                    Frame::Array(vec![
                        Frame::BulkString(key.clone()),
                        Frame::BulkString(member),
                        Frame::BulkString(Bytes::from(format_score(score))),
                    ])
                })
            }
            BlockedCommand::BZPopMax => {
                // Pop max, return [key, member, score]
                db.zset_pop_max(key).map(|(member, score)| {
                    Frame::Array(vec![
                        Frame::BulkString(key.clone()),
                        Frame::BulkString(member),
                        Frame::BulkString(Bytes::from(format_score(score))),
                    ])
                })
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

/// Format a float score the same way Redis does (integer if whole, otherwise full precision).
fn format_score(score: f64) -> String {
    if score == score.floor() && score.abs() < i64::MAX as f64 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}
