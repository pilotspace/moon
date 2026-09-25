//! What a collection write adds to `rdb_changes_since_last_save` (moon#1232).
//!
//! redis's `server.dirty` is bumped by each command, by the command's own
//! rule, and the `--save` trigger reads it. moon counted string writes at the
//! storage funnels only, so every collection write counted 0 — `HSET`,
//! `RPUSH`, `SADD`, `ZADD`, `XADD` never armed a `--save` rule.
//!
//! A collection command runs through [`counted`]: its storage-funnel calls
//! (the removal of a key it emptied, a create it abandoned) are muted, and its
//! count is taken from its reply by the redis rule in [`Rule`]. Every rule
//! below was measured against redis-server 7.0.15 (`INFO persistence`
//! `rdb_changes_since_last_save` across one command), and the reviewer's table
//! is pinned end to end by `tests/perf_ws19_save_rules.rs`.
//!
//! The few whose count is not in the reply — `ZADD`'s rescored members,
//! `LTRIM`'s trimmed elements, `SMOVE`'s destination, `PFADD`'s changed
//! registers — are [`Rule::ByHandler`]: the handler records it itself with
//! `record_keyspace_changes`, which the mute does not silence. Such a handler
//! is only ever reached through its wrapped dispatch arm.
//!
//! A blocking pop answered at once counts as its non-blocking twin
//! ([`blocking_pop_changes`]); a PARKED waiter served later by a push counts
//! nothing, as in redis 7.0.15, where only the push is counted.
//!
//! Known, bounded differences (all measured on redis-server 7.0.15):
//!
//! - `XREADGROUP` / `XCLAIM` / `XAUTOCLAIM` do not count the consumer they
//!   create (redis adds 1 when that consumer is served something, so the
//!   command already counts at least one), and an `XREADGROUP` history read
//!   (an id other than `>`) of an empty PEL counts 0 (redis 1 per stream);
//! - `SORT ... STORE` counts 1 where redis counts the stored length;
//! - `ZINCRBY` of a NEW member by 0 counts 0 (redis 1);
//! - `XGROUP DELCONSUMER` of a missing consumer counts 1 (redis 0);
//! - `SETBIT` of a bit that already had the value counts 1 (redis 0);
//! - `PFCOUNT` counts 0; redis counts 1 when a single-key count rewrites the
//!   HLL's cached cardinality (after a `PFADD` changed a register) — moon
//!   keeps no such cache, so its `PFCOUNT` writes nothing.
//!
//! Hot path: one match on the reply after the handler; no allocation.

use crate::admin::metrics_setup::{mute_keyspace_changes, record_keyspace_changes};
use crate::protocol::Frame;
use crate::storage::Database;

/// How a collection command's reply (and arguments) give its redis `dirty`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rule {
    /// One per field-value pair once the command succeeds (`HSET`, `HMSET`).
    Pairs,
    /// One per pushed element once the command succeeds (`LPUSH`, `RPUSH`).
    Pushed,
    /// [`Rule::Pushed`], but only when the key existed — the reply, the new
    /// length, is 0 otherwise (`LPUSHX`, `RPUSHX`).
    PushedIfExists,
    /// One when the command succeeded with a non-null reply (`HINCRBY`,
    /// `HINCRBYFLOAT`, `LSET`, `XADD`, `XSETID`, `LMOVE`, `RPOPLPUSH`).
    One,
    /// One when the integer reply is positive (`HSETNX`, `LINSERT`,
    /// `RENAMENX`).
    Positive,
    /// `RENAME`: [`Rule::One`], but `RENAME k k` changes nothing (redis 0).
    Rename,
    /// The integer reply (`SADD`, `SREM`, `HDEL`, `LREM`, `ZREM`,
    /// `ZREMRANGEBY*`, `XDEL`, `XTRIM`, `XACK`).
    Int,
    /// The elements popped: a bulk reply is one, an array its length
    /// (`LPOP`, `RPOP`, `SPOP`).
    Popped,
    /// `ZPOPMIN` / `ZPOPMAX`: a flat member-score array, two frames per
    /// popped member.
    PoppedPairs,
    /// `LMPOP` / `ZMPOP` / `BLMPOP` / `BZMPOP`: `[key, [elements]]`.
    MultiPopped,
    /// `XREADGROUP`: one per stream that delivered entries.
    Streams,
    /// `XCLAIM`: one per claimed entry.
    Claimed,
    /// `XAUTOCLAIM`: `[cursor, [claimed], [deleted]]` — one per claimed and
    /// per deleted entry.
    AutoClaimed,
    /// The field-TTL commands (`HEXPIRE` family, `HPERSIST`): one per field
    /// whose reply code is 1 (changed) or 2 (deleted by a past deadline).
    FieldCodes,
    /// `HGETDEL`: one per field it returned (and so deleted).
    NonNull,
    /// `HGETEX`: one per returned field when a TTL option is given.
    HGetEx,
    /// `XGROUP`: by subcommand.
    XGroup,
    /// `ZINCRBY key increment member`: see [`zincr_changes`].
    ZIncr,
    /// The handler records its own count.
    ByHandler,
}

/// Run a collection write with the storage funnels muted, then count it by
/// `rule`.
#[inline]
pub(crate) fn counted(
    db: &mut Database,
    args: &[Frame],
    rule: Rule,
    handler: fn(&mut Database, &[Frame]) -> Frame,
) -> Frame {
    let reply = {
        let _quiet = mute_keyspace_changes();
        handler(db, args)
    };
    record_keyspace_changes(changes(rule, args, &reply));
    reply
}

/// A `*STORE` whose destination is `args[0]`, for the stores that build the
/// destination through a collection accessor rather than the `set` funnel
/// (`ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`, `ZRANGESTORE`). redis counts
/// one when it stores a non-empty result, and one when an empty result
/// deletes a destination that existed.
#[inline]
pub(crate) fn counted_store(
    db: &mut Database,
    args: &[Frame],
    handler: fn(&mut Database, &[Frame]) -> Frame,
) -> Frame {
    let existed = args
        .first()
        .and_then(bulk)
        .is_some_and(|dst| db.exists(dst));
    let reply = {
        let _quiet = mute_keyspace_changes();
        handler(db, args)
    };
    let stored = match reply {
        Frame::Integer(n) => n > 0 || existed,
        _ => false,
    };
    record_keyspace_changes(u64::from(stored));
    reply
}

/// The redis `dirty` delta of one command, from its arguments and reply.
pub(crate) fn changes(rule: Rule, args: &[Frame], reply: &Frame) -> u64 {
    if is_failure(reply) {
        return 0;
    }
    match rule {
        Rule::Pairs => (args.len().saturating_sub(1) / 2) as u64,
        Rule::Pushed => args.len().saturating_sub(1) as u64,
        Rule::PushedIfExists => match reply {
            Frame::Integer(n) if *n > 0 => args.len().saturating_sub(1) as u64,
            _ => 0,
        },
        Rule::One => 1,
        Rule::Rename => u64::from(args.first().and_then(bulk) != args.get(1).and_then(bulk)),
        Rule::Positive => match reply {
            Frame::Integer(n) if *n > 0 => 1,
            _ => 0,
        },
        Rule::Int => match reply {
            Frame::Integer(n) if *n > 0 => *n as u64,
            _ => 0,
        },
        Rule::Popped => match reply {
            Frame::Array(items) | Frame::Set(items) => items.len() as u64,
            _ => 1,
        },
        Rule::PoppedPairs => match reply {
            Frame::Array(items) => (items.len() / 2) as u64,
            _ => 0,
        },
        Rule::MultiPopped => nth_len(reply, 1),
        Rule::Streams => match reply {
            Frame::Array(streams) => streams.iter().filter(|s| nth_len(s, 1) > 0).count() as u64,
            _ => 0,
        },
        Rule::Claimed => match reply {
            Frame::Array(items) => items.len() as u64,
            _ => 0,
        },
        Rule::AutoClaimed => nth_len(reply, 1) + nth_len(reply, 2),
        Rule::FieldCodes => match reply {
            Frame::Array(codes) => codes
                .iter()
                .filter(|c| matches!(c, Frame::Integer(1 | 2)))
                .count() as u64,
            _ => 0,
        },
        Rule::NonNull => non_null(reply),
        Rule::HGetEx => {
            let sets_ttl = args.get(1).and_then(bulk).is_some_and(|opt| {
                [&b"EX"[..], b"PX", b"EXAT", b"PXAT", b"PERSIST"]
                    .iter()
                    .any(|o| opt.eq_ignore_ascii_case(o))
            });
            if sets_ttl { non_null(reply) } else { 0 }
        }
        Rule::XGroup => {
            let Some(sub) = args.first().and_then(bulk) else {
                return 0;
            };
            if sub.eq_ignore_ascii_case(b"CREATE")
                || sub.eq_ignore_ascii_case(b"SETID")
                || sub.eq_ignore_ascii_case(b"DELCONSUMER")
            {
                1
            } else if sub.eq_ignore_ascii_case(b"DESTROY")
                || sub.eq_ignore_ascii_case(b"CREATECONSUMER")
            {
                changes(Rule::Positive, args, reply)
            } else {
                0
            }
        }
        Rule::ZIncr => {
            let increment = args
                .get(1)
                .and_then(bulk)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|t| t.parse::<f64>().ok())
                .unwrap_or(f64::NAN);
            zincr_changes(increment, reply)
        }
        Rule::ByHandler => 0,
    }
}

/// A blocking pop served at once (`BLPOP`, `BRPOP`, `BZPOPMIN`, `BZPOPMAX`,
/// `BLMOVE`, `BRPOPLPUSH`: one element; `BLMPOP`, `BZMPOP`: the elements
/// popped), as its non-blocking twin counts.
pub(crate) fn blocking_pop_changes(cmd: &[u8], reply: &Frame) -> u64 {
    if cmd.eq_ignore_ascii_case(b"BLMPOP") || cmd.eq_ignore_ascii_case(b"BZMPOP") {
        changes(Rule::MultiPopped, &[], reply)
    } else {
        changes(Rule::One, &[], reply)
    }
}

/// `ZINCRBY` / `ZADD ... INCR`: one when the score moved. redis counts a
/// NEW member added with increment 0 too; its reply ("0") cannot tell it from
/// an existing member whose score is 0, so that one case counts 0 here.
pub(crate) fn zincr_changes(increment: f64, reply: &Frame) -> u64 {
    if is_failure(reply) || increment == 0.0 {
        0
    } else {
        1
    }
}

/// An error, or a null of either kind: the command changed nothing.
#[inline]
fn is_failure(reply: &Frame) -> bool {
    matches!(reply, Frame::Error(_) | Frame::Null | Frame::NullArray)
}

#[inline]
fn bulk(f: &Frame) -> Option<&[u8]> {
    match f {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// Length of the array at position `i` of an array reply, 0 otherwise.
#[inline]
fn nth_len(reply: &Frame, i: usize) -> u64 {
    match reply {
        Frame::Array(items) => match items.get(i) {
            Some(Frame::Array(inner)) => inner.len() as u64,
            _ => 0,
        },
        _ => 0,
    }
}

#[inline]
fn non_null(reply: &Frame) -> u64 {
    match reply {
        Frame::Array(items) => items
            .iter()
            .filter(|f| !matches!(f, Frame::Null | Frame::NullArray))
            .count() as u64,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::admin::metrics_setup::keyspace_changes_on_this_thread;
    use crate::command::{DispatchResult, dispatch};

    fn run(db: &mut Database, parts: &[&str]) -> Frame {
        let (cmd, rest) = parts.split_first().expect("a command");
        let args: Vec<Frame> = rest
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect();
        let mut selected = 0usize;
        match dispatch(db, cmd.as_bytes(), &args, &mut selected, 16) {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        }
    }

    /// `(name, setup, measured, redis 7.0.15's rdb_changes_since_last_save
    /// delta across `measured`)`. Each row runs on a fresh database. The
    /// expectations are redis's, measured command by command with the same
    /// setup; `tests/perf_ws19_save_rules.rs` re-checks the table against a
    /// live `redis-server` (`dirty_count_oracle_redis_agrees`).
    type Case = (
        &'static str,
        &'static [&'static [&'static str]],
        &'static [&'static str],
        u64,
    );

    const CASES: &[Case] = &[
        ("SET new key", &[], &["SET", "a", "1"], 1),
        (
            "MSET 3 keys",
            &[],
            &["MSET", "b", "1", "c", "2", "d", "3"],
            3,
        ),
        ("DEL missing key", &[], &["DEL", "nope"], 0),
        (
            "DEL 2 of 3",
            &[&["SET", "d1", "1"], &["SET", "d2", "1"]],
            &["DEL", "d1", "d2", "d3"],
            2,
        ),
        ("UNLINK missing", &[], &["UNLINK", "nope"], 0),
        ("EXPIRE missing key", &[], &["EXPIRE", "nope", "10"], 0),
        (
            "EXPIRE existing",
            &[&["SET", "e", "v"]],
            &["EXPIRE", "e", "100"],
            1,
        ),
        (
            "PERSIST without TTL",
            &[&["SET", "p", "v"]],
            &["PERSIST", "p"],
            0,
        ),
        (
            "RENAME",
            &[&["SET", "rn", "v"]],
            &["RENAME", "rn", "rn2"],
            1,
        ),
        (
            "RENAME k k",
            &[&["SET", "rs", "v"]],
            &["RENAME", "rs", "rs"],
            0,
        ),
        (
            "RENAMENX refused",
            &[&["SET", "a", "1"], &["SET", "b", "1"]],
            &["RENAMENX", "a", "b"],
            0,
        ),
        ("GET", &[&["SET", "g", "v"]], &["GET", "g"], 0),
        (
            "HSET 3 new fields",
            &[],
            &["HSET", "h", "f1", "1", "f2", "2", "f3", "3"],
            3,
        ),
        (
            "HSET same value",
            &[&["HSET", "h", "f", "1"]],
            &["HSET", "h", "f", "1"],
            1,
        ),
        (
            "HSETNX existing",
            &[&["HSET", "h", "f", "v"]],
            &["HSETNX", "h", "f", "w"],
            0,
        ),
        (
            "HINCRBY by 0",
            &[&["HSET", "h", "f", "1"]],
            &["HINCRBY", "h", "f", "0"],
            1,
        ),
        (
            "HINCRBY not a number",
            &[&["HSET", "h", "f", "x"]],
            &["HINCRBY", "h", "f", "1"],
            0,
        ),
        (
            "HDEL 2 of 3, emptying nothing",
            &[&["HSET", "h", "a", "1", "b", "2", "c", "3"]],
            &["HDEL", "h", "a", "b", "x"],
            2,
        ),
        (
            "HDEL every field",
            &[&["HSET", "h", "a", "1", "b", "2"]],
            &["HDEL", "h", "a", "b"],
            2,
        ),
        ("HGET", &[&["HSET", "h", "f", "v"]], &["HGET", "h", "f"], 0),
        ("RPUSH 5", &[], &["RPUSH", "l", "a", "b", "c", "d", "e"], 5),
        ("LPUSHX missing", &[], &["LPUSHX", "l", "a", "b"], 0),
        (
            "LPUSHX existing, 2",
            &[&["RPUSH", "l", "a"]],
            &["LPUSHX", "l", "b", "c"],
            2,
        ),
        (
            "LINSERT pivot missing",
            &[&["RPUSH", "l", "a"]],
            &["LINSERT", "l", "BEFORE", "z", "x"],
            0,
        ),
        (
            "LPOP count 3 of 2",
            &[&["RPUSH", "l", "a", "b"]],
            &["LPOP", "l", "3"],
            2,
        ),
        ("LPOP missing", &[], &["LPOP", "l"], 0),
        (
            "LREM every element",
            &[&["RPUSH", "l", "a", "a"]],
            &["LREM", "l", "0", "a"],
            2,
        ),
        (
            "LTRIM 3 of 5",
            &[&["RPUSH", "l", "a", "b", "c", "d", "e"]],
            &["LTRIM", "l", "1", "2"],
            3,
        ),
        (
            "LTRIM everything",
            &[&["RPUSH", "l", "a", "b"]],
            &["LTRIM", "l", "5", "10"],
            2,
        ),
        (
            "LTRIM nothing",
            &[&["RPUSH", "l", "a", "b"]],
            &["LTRIM", "l", "0", "-1"],
            0,
        ),
        (
            "LMOVE",
            &[&["RPUSH", "l", "a", "b"]],
            &["LMOVE", "l", "l2", "LEFT", "RIGHT"],
            1,
        ),
        (
            "LMPOP 2",
            &[&["RPUSH", "l", "a", "b", "c"]],
            &["LMPOP", "1", "l", "LEFT", "COUNT", "2"],
            2,
        ),
        (
            "LRANGE",
            &[&["RPUSH", "l", "a"]],
            &["LRANGE", "l", "0", "-1"],
            0,
        ),
        (
            "SADD 2 new of 3",
            &[&["SADD", "s", "a"]],
            &["SADD", "s", "a", "b", "c"],
            2,
        ),
        (
            "SPOP count 5 of 3",
            &[&["SADD", "s", "a", "b", "c"]],
            &["SPOP", "s", "5"],
            3,
        ),
        (
            "SMOVE to a new set",
            &[&["SADD", "s", "a"]],
            &["SMOVE", "s", "d", "a"],
            2,
        ),
        (
            "SMOVE, member already there",
            &[&["SADD", "s", "a"], &["SADD", "d", "a"]],
            &["SMOVE", "s", "d", "a"],
            1,
        ),
        (
            "SINTERSTORE empty, no destination",
            &[&["SADD", "a", "x"], &["SADD", "b", "y"]],
            &["SINTERSTORE", "d", "a", "b"],
            0,
        ),
        (
            "SINTERSTORE empty over a destination",
            &[&["SADD", "a", "x"], &["SADD", "b", "y"], &["SET", "d", "1"]],
            &["SINTERSTORE", "d", "a", "b"],
            1,
        ),
        (
            "SUNIONSTORE",
            &[&["SADD", "a", "x"], &["SADD", "b", "y"]],
            &["SUNIONSTORE", "d", "a", "b"],
            1,
        ),
        ("ZADD 2", &[], &["ZADD", "z", "1", "a", "2", "b"], 2),
        (
            "ZADD same score",
            &[&["ZADD", "z", "1", "a"]],
            &["ZADD", "z", "1", "a"],
            0,
        ),
        (
            "ZADD rescore + new",
            &[&["ZADD", "z", "1", "a"]],
            &["ZADD", "z", "2", "a", "3", "b"],
            2,
        ),
        (
            "ZADD NX existing",
            &[&["ZADD", "z", "1", "a"]],
            &["ZADD", "z", "NX", "5", "a"],
            0,
        ),
        (
            "ZADD INCR",
            &[&["ZADD", "z", "1", "a"]],
            &["ZADD", "z", "INCR", "2", "a"],
            1,
        ),
        (
            "ZINCRBY 0",
            &[&["ZADD", "z", "1", "a"]],
            &["ZINCRBY", "z", "0", "a"],
            0,
        ),
        (
            "ZPOPMIN 2",
            &[&["ZADD", "z", "1", "a", "2", "b", "3", "c"]],
            &["ZPOPMIN", "z", "2"],
            2,
        ),
        ("ZPOPMIN missing", &[], &["ZPOPMIN", "z"], 0),
        (
            "ZUNIONSTORE",
            &[&["ZADD", "a", "1", "x"], &["ZADD", "b", "1", "y"]],
            &["ZUNIONSTORE", "d", "2", "a", "b"],
            1,
        ),
        (
            "ZINTERSTORE empty, no destination",
            &[&["ZADD", "a", "1", "x"], &["ZADD", "b", "1", "y"]],
            &["ZINTERSTORE", "d", "2", "a", "b"],
            0,
        ),
        (
            "ZINTERSTORE empty over a destination",
            &[
                &["ZADD", "a", "1", "x"],
                &["ZADD", "b", "1", "y"],
                &["SET", "d", "1"],
            ],
            &["ZINTERSTORE", "d", "2", "a", "b"],
            1,
        ),
        ("XADD", &[], &["XADD", "st", "*", "f", "v"], 1),
        (
            "XADD NOMKSTREAM missing",
            &[],
            &["XADD", "st", "NOMKSTREAM", "*", "f", "v"],
            0,
        ),
        (
            "XTRIM 2",
            &[
                &["XADD", "st", "1-1", "f", "v"],
                &["XADD", "st", "1-2", "f", "v"],
                &["XADD", "st", "1-3", "f", "v"],
            ],
            &["XTRIM", "st", "MAXLEN", "1"],
            2,
        ),
        (
            "XGROUP CREATE",
            &[&["XADD", "st", "1-1", "f", "v"]],
            &["XGROUP", "CREATE", "st", "g", "0"],
            1,
        ),
        (
            "XGROUP DESTROY missing",
            &[&["XADD", "st", "1-1", "f", "v"]],
            &["XGROUP", "DESTROY", "st", "g"],
            0,
        ),
        (
            "XREADGROUP 2 entries",
            &[
                &["XADD", "st", "1-1", "f", "v"],
                &["XADD", "st", "1-2", "f", "v"],
                &["XGROUP", "CREATE", "st", "g", "0"],
                &["XGROUP", "CREATECONSUMER", "st", "g", "c"],
            ],
            &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "st", ">"],
            1,
        ),
        (
            "XACK 1 of 2",
            &[
                &["XADD", "st", "1-1", "f", "v"],
                &["XGROUP", "CREATE", "st", "g", "0"],
                &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "st", ">"],
            ],
            &["XACK", "st", "g", "1-1", "9-9"],
            1,
        ),
        ("PFADD 2 to a new key", &[], &["PFADD", "hll", "a", "b"], 3),
        (
            "PFADD, no register changes",
            &[&["PFADD", "hll", "a"]],
            &["PFADD", "hll", "a"],
            0,
        ),
        (
            "GEOADD 2",
            &[],
            &["GEOADD", "g", "13.36", "38.11", "a", "15.08", "37.50", "b"],
            2,
        ),
        (
            "GEOADD same position",
            &[&["GEOADD", "g", "13.36", "38.11", "a"]],
            &["GEOADD", "g", "13.36", "38.11", "a"],
            0,
        ),
        (
            "GEOSEARCHSTORE 2",
            &[&["GEOADD", "g", "13.36", "38.11", "a", "15.08", "37.50", "b"]],
            &[
                "GEOSEARCHSTORE",
                "gd",
                "g",
                "FROMLONLAT",
                "15",
                "37",
                "BYRADIUS",
                "200",
                "km",
            ],
            2,
        ),
        (
            "GEORADIUS STORE 2",
            &[&["GEOADD", "g", "13.36", "38.11", "a", "15.08", "37.50", "b"]],
            &["GEORADIUS", "g", "15", "37", "200", "km", "STORE", "gd"],
            2,
        ),
        (
            "GEOSEARCHSTORE empty, destination existed",
            &[&["GEOADD", "g", "13.36", "38.11", "a"], &["SET", "gd", "x"]],
            &[
                "GEOSEARCHSTORE",
                "gd",
                "g",
                "FROMLONLAT",
                "0",
                "0",
                "BYRADIUS",
                "1",
                "km",
            ],
            1,
        ),
        (
            "GEOSEARCHSTORE empty, no destination",
            &[&["GEOADD", "g", "13.36", "38.11", "a"]],
            &[
                "GEOSEARCHSTORE",
                "gd",
                "g",
                "FROMLONLAT",
                "0",
                "0",
                "BYRADIUS",
                "1",
                "km",
            ],
            0,
        ),
        (
            "FLUSHDB 3 keys",
            &[&["SET", "a", "1"], &["SET", "b", "1"], &["SET", "c", "1"]],
            &["FLUSHDB"],
            3,
        ),
        ("FLUSHDB empty", &[], &["FLUSHDB"], 0),
    ];

    #[test]
    fn collection_writes_count_as_redis_7_0_15_does() {
        let mut wrong = Vec::new();
        for &(name, setup, measured, want) in CASES {
            let mut db = Database::new();
            for cmd in setup {
                run(&mut db, cmd);
            }
            let before = keyspace_changes_on_this_thread();
            let reply = run(&mut db, measured);
            let got = keyspace_changes_on_this_thread() - before;
            if got != want {
                wrong.push(format!(
                    "{name}: redis {want}, moon {got} (reply {reply:?})"
                ));
            }
        }
        assert!(
            wrong.is_empty(),
            "dirty counts differ from redis 7.0.15:\n{}",
            wrong.join("\n")
        );
    }

    /// The mute silences the funnels only: a command's own count still lands,
    /// and the funnels count again once the guard is gone.
    #[test]
    fn a_muted_funnel_is_silent_and_the_mute_nests() {
        use crate::admin::metrics_setup::{
            mute_keyspace_changes, record_keyspace_change, record_keyspace_changes,
        };
        let before = keyspace_changes_on_this_thread();
        {
            let _outer = mute_keyspace_changes();
            {
                let _inner = mute_keyspace_changes();
                record_keyspace_change();
            }
            record_keyspace_change();
            record_keyspace_changes(2);
        }
        record_keyspace_change();
        assert_eq!(keyspace_changes_on_this_thread() - before, 3);
    }

    /// Expiry is not a change in redis: neither the lazy reap nor the active
    /// sweep's removal counts.
    #[test]
    fn expiry_reaps_do_not_count() {
        const AT: &str = "9999999999999";
        let mut db = Database::new();
        run(&mut db, &["SET", "k", "v"]);
        run(&mut db, &["SET", "j", "v", "PXAT", AT]);
        let before = keyspace_changes_on_this_thread();
        assert!(db.remove_lazily(b"k"));
        assert!(matches!(
            db.remove_expired_at(b"j", AT.parse().unwrap_or(0), u64::MAX),
            crate::storage::db::ExpiredRemoval::Removed
        ));
        assert_eq!(keyspace_changes_on_this_thread() - before, 0);
    }
}
