use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::SortedSetRef;
use crate::storage::listpack::ListpackRef;

use crate::command::helpers::{err, err_wrong_args, extract_bytes};

use std::collections::HashMap;

use super::{
    AggregateOp, clamp_nan_to_zero, format_score, format_score_bytes, glob_match, lex_in_range,
    lex_rank_window, parse_bounded_count, parse_lex_bound, parse_numkeys, parse_score_bound,
    score_rank_window, zrange_by_lex, zrange_by_rank, zrange_by_score, zrange_from_entries,
};

// ---------------------------------------------------------------------------
// Mutable-path read commands (take &mut Database because `command::dispatch`
// hands every handler a `&mut Database` — NOT because they mutate)
//
// moon#928 — the mutable dispatch path reads through the SHARED one
//
// Every handler in this section is a pure read, and every one used to reach
// the zset through `Database::get_sorted_set`, whose `get_promoted` core
// calls `SortedSetKind::upgrade` unconditionally. The return type is the
// forcing function: `(&HashMap<Bytes, f64>, &BPTree)` is the FULL B+tree
// form, which a `SortedSetListpack` cannot satisfy, so satisfying it means
// converting — and the conversion is one-way, because nothing in the tree
// ever downgrades (moon#832). A single `ZCARD` taken on the mutable path
// (inside MULTI/EXEC, inside a Lua script, or from `try_inline_dispatch`)
// therefore flattened a `listpack` zset to a `skiplist` for the rest of its
// life, and whether that happened depended on which of moon's three dispatch
// paths the command took rather than on what the command did.
//
// moon#853 fixed exactly this for the set and list families and predicted
// this one in writing: "the moment #793 makes `SortedSetListpack` reachable,
// every `get_sorted_set` read caller becomes the same defect". moon#878 made
// it reachable.
//
// Measured on the pre-fix binary at ab91a23e (`--shards 1`, macOS host,
// `used_memory` ledger — accounting, not throughput): 1000 eight-member
// zsets went 293,055 -> 4,749,055 bytes (16.21x) after ONE `ZCARD` each
// through MULTI/EXEC, `OBJECT ENCODING` going `listpack -> skiplist`. All
// seventeen reads below flattened; the same reads on a bare connection (the
// `dispatch_read` path) kept `listpack`, which is the negative control
// proving the probe measures the dispatch path and not `OBJECT ENCODING`.
// That put a ceiling on moon#878's +38.0% ZADD win: a write-only benchmark
// measured an encoding the first read destroyed.
//
// The fix is to take the read through `&Database`, via the `SortedSetRef`
// view that already backs `dispatch_read`. That view is the right shape for
// a reason the type system enforces rather than documents: it classifies all
// four forms (`BPTree`, `Listpack`, `Legacy`, and the `Owned` decode of a
// cold-tier hit) instead of demanding one, and a SHARED borrow cannot reach
// `K::upgrade` at all. It also collapses the two implementations of every
// one of these commands into one, which removes the divergence class #610
// came from — the answer no longer depends on the dispatch path.
//
// What is deliberately NOT preserved: the mutable path used to reclaim an
// expired key and to promote a cold-tier hit back into hot RAM as a side
// effect of the read. Neither is a correctness property —
// `get_sorted_set_ref_if_alive` still treats an expired key as absent and
// still reads the cold tier through, answering from `SortedSetRef::Owned`
// (pinned by `tests/zset_read_cold_tier_928.rs`, which compares every read
// against a hot zset holding the same members) — and the hash, list and set
// families have shipped this exact trade since moon#853. Active expiry and
// every write path still reclaim and still promote.
// ---------------------------------------------------------------------------

/// ZSCORE key member — the score of one member.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block above).
pub fn zscore(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zscore_readonly(db, args, now_ms)
}

/// ZCARD key — cardinality of a sorted set.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block above).
pub fn zcard(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zcard_readonly(db, args, now_ms)
}

/// Parse the optional `WITHSCORE` of `ZRANK`/`ZREVRANK` (Redis 7.2).
///
/// The token is SINGULAR here, unlike the `WITHSCORES` that `ZRANGE` and
/// friends take, and Redis does not accept the plural as a synonym — so
/// neither does this (moon#521).
///
/// Redis distinguishes the two failure modes and they are not interchangeable:
/// a bad THIRD token is `ERR syntax error`, and only a FOURTH argument is an
/// arity error (`zrankGenericCommand` checks `argc > 4` first, then compares
/// `argv[3]` against "withscore").
#[inline]
fn parse_withscore(cmd: &'static str, args: &[Frame]) -> Result<bool, Frame> {
    match args.len() {
        2 => Ok(false),
        3 => match extract_bytes(&args[2]) {
            Some(opt) if opt.eq_ignore_ascii_case(b"WITHSCORE") => Ok(true),
            _ => Err(err("ERR syntax error")),
        },
        _ => Err(err_wrong_args(cmd)),
    }
}

/// The MISS reply for `ZRANK`/`ZREVRANK`.
///
/// `WITHSCORE` changes the null TYPE as well as the hit type: measured on
/// redis-server 8.6.1, the miss is `*-1` with the option and `$-1` without.
/// A statically-typed client decodes the two differently, so answering the
/// wrong one is a decode error client-side, not a cosmetic difference.
#[inline]
fn rank_miss(withscore: bool) -> Frame {
    if withscore {
        Frame::NullArray
    } else {
        Frame::Null
    }
}

/// The HIT reply for `ZRANK`/`ZREVRANK`.
///
/// The score is a `Frame::Double`, not a pre-formatted bulk string: that is
/// Redis's `addReplyDouble`, which serialises as a bulk string under RESP2 and
/// as a real `,double` under RESP3. Moon's serializer downgrades `Double` to
/// the identical RESP2 bytes (both go through `write!("{}", f)`), so this
/// costs nothing on RESP2 and gets RESP3 right for free.
#[inline]
fn rank_hit(rank: usize, score: f64, withscore: bool) -> Frame {
    if withscore {
        Frame::Array(framevec![Frame::Integer(rank as i64), Frame::Double(score)])
    } else {
        Frame::Integer(rank as i64)
    }
}

/// ZRANK key member [WITHSCORE] — score-ascending position of a member.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block above).
pub fn zrank(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrank_readonly(db, args, now_ms)
}

/// ZREVRANK key member [WITHSCORE] — score-descending position of a member.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block above).
pub fn zrevrank(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrevrank_readonly(db, args, now_ms)
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count] — incremental scan.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zscan(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zscan_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// Range commands
// ---------------------------------------------------------------------------

/// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES].
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zrange(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrange_readonly(db, args, now_ms)
}

/// ZREVRANGE key start stop [WITHSCORES] — reverse rank range.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zrevrange(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrevrange_readonly(db, args, now_ms)
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count].
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zrangebyscore(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrangebyscore_readonly(db, args, now_ms)
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count].
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zrevrangebyscore(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrevrangebyscore_readonly(db, args, now_ms)
}

/// ZCOUNT key min max — members inside a score range.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zcount(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zcount_readonly(db, args, now_ms)
}

/// ZLEXCOUNT key min max — members inside a lexicographic range.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zlexcount(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zlexcount_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// ZSCORE (read-only).
pub fn zscore_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("ZSCORE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZSCORE"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZSCORE"),
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match zref.score(member) {
            Some(score) => Frame::BulkString(Bytes::from(format_score(score))),
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// ZCARD (read-only).
pub fn zcard_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("ZCARD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZCARD"),
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => Frame::Integer(zref.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZRANK (read-only). Takes the same optional `WITHSCORE` as `zrank` — the
/// option is parsed in BOTH, because both are reachable: which one answers is
/// decided by shard routing, not by the command (moon#521).
pub fn zrank_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    let withscore = match parse_withscore("ZRANK", args) {
        Ok(w) => w,
        Err(e) => return e,
    };
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZRANK"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZRANK"),
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match ascending_rank(&zref, member) {
            Some((rank, score)) => rank_hit(rank, score, withscore),
            None => rank_miss(withscore),
        },
        Ok(None) => rank_miss(withscore),
        Err(e) => e,
    }
}

/// ZREVRANK (read-only). See `zrank_readonly` for why the option is parsed
/// here too.
pub fn zrevrank_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    let withscore = match parse_withscore("ZREVRANK", args) {
        Ok(w) => w,
        Err(e) => return e,
    };
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZREVRANK"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZREVRANK"),
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match ascending_rank(&zref, member) {
            Some((rank, score)) => rank_hit(zref.len() - 1 - rank, score, withscore),
            None => rank_miss(withscore),
        },
        Ok(None) => rank_miss(withscore),
        Err(e) => e,
    }
}

/// Compare a borrowed listpack entry with `other` as bytes, without
/// allocating: an integer entry is rendered into a stack buffer (its
/// canonical spelling — the only one the encoder integer-encodes).
fn cmp_lp_ref(entry: ListpackRef<'_>, other: &[u8]) -> std::cmp::Ordering {
    match entry {
        ListpackRef::Str(s) => s.cmp(other),
        ListpackRef::Integer(v) => {
            let mut buf = itoa::Buffer::new();
            buf.format(v).as_bytes().cmp(other)
        }
    }
}

/// `(ascending rank, score)` of `member`, or `None` when it is absent.
///
/// A B+tree (hot, or the cold-tier `Owned` decode) answers with one
/// descent. A listpack keeps INSERTION order, so its rank is the number of
/// pairs that sort strictly before `(score, member)` — ONE borrowed pass,
/// scores read with the same `as_score` rule ZSCORE uses (moon#1174 §4).
/// It used to decode every pair into owned `Bytes`, parse every score,
/// sort the lot and `copy_from_slice` the probe, on every ZRANK.
fn ascending_rank(zref: &SortedSetRef<'_>, member: &[u8]) -> Option<(usize, f64)> {
    let score = zref.score(member)?;
    match zref {
        SortedSetRef::Listpack(lp) => {
            let target = OrderedFloat(score);
            let rank = lp
                .iter_pair_refs()
                .filter(|(m, s)| {
                    let s = OrderedFloat(s.as_score().unwrap_or(0.0));
                    s.cmp(&target).then_with(|| cmp_lp_ref(*m, member)).is_lt()
                })
                .count();
            Some((rank, score))
        }
        _ => match zref.any_tree() {
            Some(tree) => tree.rank(OrderedFloat(score), member).map(|r| (r, score)),
            // Legacy: the sorted decode is the only order it has.
            None => zref
                .entries_sorted()
                .iter()
                .position(|(m, s)| OrderedFloat(*s) == OrderedFloat(score) && m.as_ref() == member)
                .map(|r| (r, score)),
        },
    }
}

/// ZRANGE (read-only).
pub fn zrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("ZRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZRANGE"),
    };
    let min_arg = match extract_bytes(&args[1]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZRANGE"),
    };
    let max_arg = match extract_bytes(&args[2]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZRANGE"),
    };
    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"BYSCORE") {
            by_score = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"BYLEX") {
            by_lex = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"REV") {
            rev = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGE"),
                };
                let cnt_b = match extract_bytes(&args[i + 2]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGE"),
                };
                limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
                if limit_offset.is_none() || limit_count.is_none() {
                    return err("ERR value is not an integer or out of range");
                }
                i += 3;
            } else {
                return err_wrong_args("ZRANGE");
            }
        } else {
            // moon#967: an unrecognised token is `ERR syntax error`, not
            // something to step over. Skipping it silently turned a
            // mis-spelled option into a DIFFERENT, successful command.
            return err("ERR syntax error");
        }
    }
    if by_score && by_lex {
        return err("ERR BYSCORE and BYLEX options are not compatible");
    }
    // moon#967: the only pairing checked used to be BYSCORE+BYLEX.
    if by_lex && withscores {
        return err("ERR syntax error, WITHSCORES not supported in combination with BYLEX");
    }
    if limit_offset.is_some() && !by_score && !by_lex {
        return err(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        );
    }
    // moon#961. With REV on a BYSCORE/BYLEX range, Redis takes the arguments
    // MAX first: `ZRANGE k 3 1 BYSCORE REV`. Every range helper below documents
    // the opposite contract — "all callers pass (min, max) in semantic order
    // regardless of rev" — and `rev` there only reverses iteration. So the swap
    // belongs here, at the one call site that receives the user's order.
    //
    // An index range is NOT swapped: `ZRANGE k 0 -1 REV` keeps start/stop and
    // simply walks backwards.
    let (min_arg, max_arg) = if rev && (by_score || by_lex) {
        (max_arg, min_arg)
    } else {
        (min_arg, max_arg)
    };
    // moon#1060: Redis parses the range grammar unconditionally, before ever
    // looking the key up — a malformed bound against a MISSING key is a parse
    // error, not an empty array. `zrange_by_score`/`zrange_by_lex` below
    // already validate, but only on the `Ok(Some(zref))` arm; `Ok(None)`
    // short-circuited straight to `[]` without calling them. Validate here
    // too, matching `ZCOUNT`/`ZLEXCOUNT` and the already-correct
    // `ZRANGEBYLEX`/`ZREVRANGEBYLEX`.
    if by_score {
        if let Err(e) = parse_score_bound(&min_arg) {
            return e;
        }
        if let Err(e) = parse_score_bound(&max_arg) {
            return e;
        }
    } else if by_lex {
        if let Err(e) = parse_lex_bound(&min_arg) {
            return e;
        }
        if let Err(e) = parse_lex_bound(&max_arg) {
            return e;
        }
    }
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match (&zref, zref.members_map(), zref.bptree()) {
                (_, Some(members), Some(scores)) => {
                    if by_score {
                        zrange_by_score(
                            members,
                            scores,
                            &min_arg,
                            &max_arg,
                            rev,
                            withscores,
                            limit_offset,
                            limit_count,
                        )
                    } else if by_lex {
                        zrange_by_lex(
                            scores,
                            &min_arg,
                            &max_arg,
                            rev,
                            withscores,
                            members,
                            limit_offset,
                            limit_count,
                        )
                    } else {
                        zrange_by_rank(scores, &min_arg, &max_arg, rev, withscores)
                    }
                }
                _ => {
                    // Listpack fallback: convert to sorted entries and apply range logic
                    let entries = zref.entries_sorted();
                    zrange_from_entries(
                        &entries,
                        &min_arg,
                        &max_arg,
                        by_score,
                        by_lex,
                        rev,
                        withscores,
                        limit_offset,
                        limit_count,
                    )
                }
            }
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZREVRANGE (read-only).
pub fn zrevrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("ZREVRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZREVRANGE"),
    };
    let start_arg = match extract_bytes(&args[1]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZREVRANGE"),
    };
    let stop_arg = match extract_bytes(&args[2]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZREVRANGE"),
    };
    let withscores = args.len() > 3
        && extract_bytes(&args[3])
            .map(|b| b.eq_ignore_ascii_case(b"WITHSCORES"))
            .unwrap_or(false);
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match zref.bptree() {
            Some(scores) => zrange_by_rank(scores, &start_arg, &stop_arg, true, withscores),
            None => {
                let entries = zref.entries_sorted();
                zrange_from_entries(
                    &entries, &start_arg, &stop_arg, false, false, true, withscores, None, None,
                )
            }
        },
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZRANGEBYSCORE (read-only).
pub fn zrangebyscore_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("ZRANGEBYSCORE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZRANGEBYSCORE"),
    };
    let min_arg = match extract_bytes(&args[1]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZRANGEBYSCORE"),
    };
    let max_arg = match extract_bytes(&args[2]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZRANGEBYSCORE"),
    };
    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGEBYSCORE"),
                };
                let cnt_b = match extract_bytes(&args[i + 2]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGEBYSCORE"),
                };
                limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
                if limit_offset.is_none() || limit_count.is_none() {
                    return err("ERR value is not an integer or out of range");
                }
                i += 3;
            } else {
                return err_wrong_args("ZRANGEBYSCORE");
            }
        } else {
            // moon#967: an unrecognised token is `ERR syntax error`, not
            // something to step over. Skipping it silently turned a
            // mis-spelled option into a DIFFERENT, successful command.
            return err("ERR syntax error");
        }
    }
    // moon#1060: parse the score grammar before the key lookup — see the
    // identical comment in `zrange_readonly`.
    if let Err(e) = parse_score_bound(&min_arg) {
        return e;
    }
    if let Err(e) = parse_score_bound(&max_arg) {
        return e;
    }
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match (zref.members_map(), zref.bptree()) {
            (Some(members), Some(scores)) => zrange_by_score(
                members,
                scores,
                &min_arg,
                &max_arg,
                false,
                withscores,
                limit_offset,
                limit_count,
            ),
            _ => {
                let entries = zref.entries_sorted();
                zrange_from_entries(
                    &entries,
                    &min_arg,
                    &max_arg,
                    true,
                    false,
                    false,
                    withscores,
                    limit_offset,
                    limit_count,
                )
            }
        },
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZREVRANGEBYSCORE (read-only).
pub fn zrevrangebyscore_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("ZREVRANGEBYSCORE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZREVRANGEBYSCORE"),
    };
    let max_arg = match extract_bytes(&args[1]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZREVRANGEBYSCORE"),
    };
    let min_arg = match extract_bytes(&args[2]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZREVRANGEBYSCORE"),
    };
    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZREVRANGEBYSCORE"),
                };
                let cnt_b = match extract_bytes(&args[i + 2]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZREVRANGEBYSCORE"),
                };
                limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
                if limit_offset.is_none() || limit_count.is_none() {
                    return err("ERR value is not an integer or out of range");
                }
                i += 3;
            } else {
                return err_wrong_args("ZREVRANGEBYSCORE");
            }
        } else {
            // moon#967: an unrecognised token is `ERR syntax error`, not
            // something to step over. Skipping it silently turned a
            // mis-spelled option into a DIFFERENT, successful command.
            return err("ERR syntax error");
        }
    }
    // moon#1060: same defect as `ZRANGEBYSCORE`, found in the same sweep — the
    // sibling command's Ok(None) arm also short-circuited to `[]` without
    // validating min/max first. Parse before the key lookup.
    if let Err(e) = parse_score_bound(&min_arg) {
        return e;
    }
    if let Err(e) = parse_score_bound(&max_arg) {
        return e;
    }
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match (zref.members_map(), zref.bptree()) {
            (Some(members), Some(scores)) => zrange_by_score(
                members,
                scores,
                &min_arg,
                &max_arg,
                true,
                withscores,
                limit_offset,
                limit_count,
            ),
            _ => {
                let entries = zref.entries_sorted();
                zrange_from_entries(
                    &entries,
                    &min_arg,
                    &max_arg,
                    true,
                    false,
                    true,
                    withscores,
                    limit_offset,
                    limit_count,
                )
            }
        },
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZCOUNT (read-only).
pub fn zcount_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("ZCOUNT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZCOUNT"),
    };
    let min_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZCOUNT"),
    };
    let max_bytes = match extract_bytes(&args[2]) {
        Some(b) => b,
        None => return err_wrong_args("ZCOUNT"),
    };
    let min_bound = match parse_score_bound(min_bytes) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let max_bound = match parse_score_bound(max_bytes) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match (&zref, zref.any_tree()) {
            (_, Some(scores)) => {
                // moon#1170: two O(log N) order-statistic descents instead of
                // walking every in-range entry (25 ms on a 1M-member zset).
                let (lo, hi) = score_rank_window(scores, &min_bound, &max_bound);
                Frame::Integer(hi.saturating_sub(lo) as i64)
            }
            (SortedSetRef::Listpack(lp), None) => {
                // moon#1174 §4: one borrowed pass over the scores — no
                // decode, no sort, no allocation.
                let count = lp
                    .iter_pair_refs()
                    .filter(|(_, s)| {
                        let s = s.as_score().unwrap_or(0.0);
                        min_bound.includes(s) && max_bound.includes_upper(s)
                    })
                    .count();
                Frame::Integer(count as i64)
            }
            (_, None) => {
                let entries = zref.entries_sorted();
                let count = entries
                    .iter()
                    .filter(|(_, s)| min_bound.includes(*s) && max_bound.includes_upper(*s))
                    .count();
                Frame::Integer(count as i64)
            }
        },
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZLEXCOUNT (read-only).
pub fn zlexcount_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("ZLEXCOUNT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZLEXCOUNT"),
    };
    let min_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZLEXCOUNT"),
    };
    let max_bytes = match extract_bytes(&args[2]) {
        Some(b) => b,
        None => return err_wrong_args("ZLEXCOUNT"),
    };
    let min_bound = match parse_lex_bound(min_bytes) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let max_bound = match parse_lex_bound(max_bytes) {
        Ok(b) => b,
        Err(e) => return e,
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match (&zref, zref.any_tree()) {
            (SortedSetRef::Listpack(lp), None) => {
                // moon#1174 §4: the count does not depend on order, so one
                // borrowed pass over the members answers it — no decode, no
                // sort, no allocation.
                let count = lp
                    .iter_pair_refs()
                    .filter(|(m, _)| match m {
                        ListpackRef::Str(b) => lex_in_range(b, &min_bound, &max_bound),
                        ListpackRef::Integer(v) => {
                            let mut buf = itoa::Buffer::new();
                            lex_in_range(buf.format(*v).as_bytes(), &min_bound, &max_bound)
                        }
                    })
                    .count();
                Frame::Integer(count as i64)
            }
            (_, Some(scores)) => {
                // moon#1170: O(log N) when every score is equal (the lex
                // commands' precondition); the scan is kept for mixed scores.
                let count = match lex_rank_window(scores, &min_bound, &max_bound) {
                    Some((lo, hi)) => hi.saturating_sub(lo),
                    None => scores
                        .iter()
                        .filter(|(_, member)| lex_in_range(member, &min_bound, &max_bound))
                        .count(),
                };
                Frame::Integer(count as i64)
            }
            (_, None) => {
                let entries = zref.entries_sorted();
                let count = entries
                    .iter()
                    .filter(|(member, _)| lex_in_range(member, &min_bound, &max_bound))
                    .count();
                Frame::Integer(count as i64)
            }
        },
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZSCAN (read-only).
pub fn zscan_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("ZSCAN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZSCAN"),
    };
    let cursor_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZSCAN"),
    };
    let cursor: usize = match std::str::from_utf8(cursor_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => return err("ERR invalid cursor"),
    };
    // One parser for the whole family — see `command::scan_options`. ZSCAN's
    // own copy was the strictest of the eight (it alone refused a non-numeric
    // COUNT) and still answered `wrong number of arguments` where Redis
    // answers `syntax error` for a dangling MATCH.
    let opts = match crate::command::scan_options::parse_scan_options(
        crate::command::scan_options::ScanKind::SortedSet,
        &args[2..],
    ) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let pattern = opts.pattern;
    let scan_count = opts.count;
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            let mut all_members: Vec<(Bytes, f64)> = match zref.members_map() {
                Some(members) => members.iter().map(|(m, s)| (m.clone(), *s)).collect(),
                None => zref.entries_sorted(),
            };
            all_members.sort_by(|a, b| a.0.cmp(&b.0));
            let mut result_items = Vec::new();
            let mut pos = cursor;
            let mut returned = 0;
            while pos < all_members.len() && returned < scan_count {
                let (ref member, score) = all_members[pos];
                let matches = match pattern {
                    Some(p) => glob_match(p, member),
                    None => true,
                };
                if matches {
                    result_items.push(Frame::BulkString(member.clone()));
                    result_items.push(Frame::BulkString(Bytes::from(format_score(score))));
                    returned += 1;
                }
                pos += 1;
            }
            let next_cursor = if pos >= all_members.len() {
                Bytes::from_static(b"0")
            } else {
                Bytes::from(pos.to_string())
            };
            Frame::Array(framevec![
                Frame::BulkString(next_cursor),
                Frame::Array(result_items.into())
            ])
        }
        Ok(None) => Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"0")),
            Frame::Array(framevec![])
        ]),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// Shared helpers for ZDIFF / ZUNION / ZINTER (non-STORE variants)
// ---------------------------------------------------------------------------

/// Parse `numkeys k1 [k2 ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]` args.
fn parse_setop_args(
    args: &[Frame],
    cmd_name: &str,
    supports_weights: bool,
) -> Result<(Vec<Bytes>, Vec<f64>, AggregateOp, bool), Frame> {
    // Redis checks ARITY first, so `ZUNION 0` — which never names a key — is
    // an arity error and never reaches the `numkeys` rules below (moon#969).
    // These commands are declared `-3`: numkeys plus at least one key.
    if args.len() < 2 {
        return Err(err_wrong_args(cmd_name));
    }
    let numkeys_bytes = match extract_bytes(&args[0]) {
        Some(b) => b,
        None => return Err(err_wrong_args(cmd_name)),
    };
    let numkeys = parse_numkeys(numkeys_bytes, cmd_name)?;

    // Past the arity floor, a `numkeys` that overruns the key list is
    // `syntax error` (moon#969).
    if args.len() < 1 + numkeys {
        return Err(err("ERR syntax error"));
    }

    let keys: Vec<Bytes> = (0..numkeys)
        .map(|j| {
            extract_bytes(&args[1 + j])
                .cloned()
                .unwrap_or_else(Bytes::new)
        })
        .collect();

    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = AggregateOp::Sum;
    let mut withscores = false;

    let mut i = 1 + numkeys;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if supports_weights && opt.eq_ignore_ascii_case(b"WEIGHTS") {
            for w in 0..numkeys {
                // Too few weights to cover the key list is `syntax error`
                // (moon#969), not an arity error.
                if i + 1 + w >= args.len() {
                    return Err(err("ERR syntax error"));
                }
                let wb = match extract_bytes(&args[i + 1 + w]) {
                    Some(b) => b,
                    None => return Err(err("ERR syntax error")),
                };
                // `"nan"` parses in Rust; Redis's `getDoubleFromObjectOrReply`
                // rejects it after `strtod` (moon#969). Infinities stay legal.
                let wval: f64 = match std::str::from_utf8(wb)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|v| !v.is_nan())
                {
                    Some(v) => v,
                    None => return Err(err("ERR weight value is not a float")),
                };
                weights[w] = wval;
            }
            i += 1 + numkeys;
        } else if supports_weights && opt.eq_ignore_ascii_case(b"AGGREGATE") {
            if i + 1 >= args.len() {
                return Err(err("ERR syntax error"));
            }
            let agg_b = match extract_bytes(&args[i + 1]) {
                Some(b) => b.as_ref(),
                None => return Err(err("ERR syntax error")),
            };
            aggregate = if agg_b.eq_ignore_ascii_case(b"SUM") {
                AggregateOp::Sum
            } else if agg_b.eq_ignore_ascii_case(b"MIN") {
                AggregateOp::Min
            } else if agg_b.eq_ignore_ascii_case(b"MAX") {
                AggregateOp::Max
            } else {
                return Err(err("ERR syntax error"));
            };
            i += 2;
        } else if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else {
            // moon#967: an unrecognised token is `ERR syntax error`, not
            // something to step over. Skipping it silently turned a
            // mis-spelled option into a DIFFERENT, successful command.
            return Err(err("ERR syntax error"));
        }
    }

    Ok((keys, weights, aggregate, withscores))
}

// `collect_source_sets` — the `&mut Database` twin that reached every source
// through `get_sorted_set` — is gone with moon#928. It was the twelfth of the
// fourteen flattening call sites, and the one that hit FOUR commands at once
// (ZDIFF / ZUNION / ZINTER / ZINTERCARD), flattening every key named in a
// multi-key read, not just the one the caller asked about.

/// Collect the source sorted sets of ZDIFF / ZUNION / ZINTER / ZINTERCARD.
///
/// Handles ALL encodings via `get_sorted_set_ref_if_alive` — the BPTree-only
/// accessor would treat a listpack zset as missing. `BPTree`/`Legacy` borrow
/// their map (no clone); a listpack is small by definition (bounded by
/// `EncodingLimits::zset_entries`), so materializing an owned map for one is
/// bounded too. The `&Database` receiver is what makes the compact encoding
/// survive the read: a shared borrow cannot reach `SortedSetKind::upgrade`.
fn collect_source_sets_readonly<'a>(
    db: &'a Database,
    keys: &[Bytes],
    now_ms: u64,
) -> Result<Vec<std::borrow::Cow<'a, HashMap<Bytes, f64>>>, Frame> {
    use std::borrow::Cow;
    let mut source_data: Vec<Cow<'a, HashMap<Bytes, f64>>> = Vec::with_capacity(keys.len());
    for key in keys {
        // get_sorted_set_ref_if_alive handles ALL encodings (BPTree, Listpack
        // from RDB load, Legacy) — the BPTree-only accessor would silently
        // treat a listpack zset as missing. BPTree/Legacy borrow their map
        // (no clone); listpacks are small by definition, so materializing an
        // owned map for them is bounded.
        match db.get_sorted_set_ref_if_alive(key, now_ms) {
            Ok(Some(zref)) => match zref.members_map() {
                Some(m) => source_data.push(Cow::Borrowed(m)),
                None => source_data.push(Cow::Owned(zref.entries_sorted().into_iter().collect())),
            },
            Ok(None) => {
                source_data.push(Cow::Owned(HashMap::new()));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(source_data)
}

/// Format a result map into a Frame::Array, optionally with scores.
fn result_map_to_frame(result: &HashMap<Bytes, f64>, withscores: bool) -> Frame {
    let mut entries: Vec<(&Bytes, f64)> = result.iter().map(|(m, s)| (m, *s)).collect();
    entries.sort_by(|a, b| {
        a.1.partial_cmp(&b.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    let cap = if withscores {
        entries.len() * 2
    } else {
        entries.len()
    };
    let mut frames = Vec::with_capacity(cap);
    for (member, score) in entries {
        frames.push(Frame::BulkString(member.clone()));
        if withscores {
            frames.push(Frame::BulkString(format_score_bytes(score)));
        }
    }
    Frame::Array(frames.into())
}

// ---------------------------------------------------------------------------
// ZDIFF numkeys key [key ...] [WITHSCORES]
// ---------------------------------------------------------------------------

/// ZDIFF numkeys key [key ...] [WITHSCORES] — set difference.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zdiff(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zdiff_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
// ---------------------------------------------------------------------------

/// ZUNION numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES].
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zunion(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zunion_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES]
// ---------------------------------------------------------------------------

/// ZINTER numkeys key [key ...] [WEIGHTS ...] [AGGREGATE ...] [WITHSCORES].
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zinter(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zinter_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// ZINTERCARD numkeys key [key ...] [LIMIT n]
// ---------------------------------------------------------------------------

/// ZINTERCARD numkeys key [key ...] [LIMIT n] — intersection size.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zintercard(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zintercard_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// ZMSCORE key member [member ...]
// ---------------------------------------------------------------------------

/// ZMSCORE key member [member ...] — scores of several members.
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zmscore(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zmscore_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// ZRANDMEMBER key [count [WITHSCORES]]
// ---------------------------------------------------------------------------

/// ZRANDMEMBER key [count [WITHSCORES]] — random member(s).
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928 — see the block at the top of the
/// mutable-path section).
pub fn zrandmember(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrandmember_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// Read-only twins for the shared-lock (dispatch_read) path
// ---------------------------------------------------------------------------

/// ZDIFF numkeys key [key …] [WITHSCORES] — read-only twin.
pub fn zdiff_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    let (keys, _, _, withscores) = match parse_setop_args(args, "ZDIFF", false) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let source_data = match collect_source_sets_readonly(db, &keys, now_ms) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut result_map: HashMap<Bytes, f64> = HashMap::new();
    if let Some(first) = source_data.first() {
        'outer: for (member, score) in first.iter() {
            for src in source_data.iter().skip(1) {
                if src.contains_key(member) {
                    continue 'outer;
                }
            }
            result_map.insert(member.clone(), *score);
        }
    }
    result_map_to_frame(&result_map, withscores)
}

/// ZUNION numkeys key [key …] [WEIGHTS …] [AGGREGATE …] [WITHSCORES] — read-only twin.
pub fn zunion_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    let (keys, weights, aggregate, withscores) = match parse_setop_args(args, "ZUNION", true) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let source_data = match collect_source_sets_readonly(db, &keys, now_ms) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut result_map: HashMap<Bytes, f64> = HashMap::new();
    for (idx, src) in source_data.iter().enumerate() {
        for (member, score) in src.iter() {
            let weighted = clamp_nan_to_zero(*score * weights[idx]);
            result_map
                .entry(member.clone())
                .and_modify(|existing| {
                    *existing = match aggregate {
                        AggregateOp::Sum => clamp_nan_to_zero(*existing + weighted),
                        AggregateOp::Min => existing.min(weighted),
                        AggregateOp::Max => existing.max(weighted),
                    };
                })
                .or_insert(weighted);
        }
    }
    result_map_to_frame(&result_map, withscores)
}

/// ZINTER numkeys key [key …] [WEIGHTS …] [AGGREGATE …] [WITHSCORES] — read-only twin.
pub fn zinter_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    let (keys, weights, aggregate, withscores) = match parse_setop_args(args, "ZINTER", true) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let source_data = match collect_source_sets_readonly(db, &keys, now_ms) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let mut result_map: HashMap<Bytes, f64> = HashMap::new();
    if let Some(first) = source_data.first() {
        for (member, score) in first.iter() {
            let weighted = clamp_nan_to_zero(*score * weights[0]);
            let mut final_score = weighted;
            let mut in_all = true;
            for (idx, src) in source_data.iter().enumerate().skip(1) {
                match src.get(member) {
                    Some(s) => {
                        let ws = clamp_nan_to_zero(*s * weights[idx]);
                        final_score = match aggregate {
                            AggregateOp::Sum => clamp_nan_to_zero(final_score + ws),
                            AggregateOp::Min => final_score.min(ws),
                            AggregateOp::Max => final_score.max(ws),
                        };
                    }
                    None => {
                        in_all = false;
                        break;
                    }
                }
            }
            if in_all {
                result_map.insert(member.clone(), final_score);
            }
        }
    }
    result_map_to_frame(&result_map, withscores)
}

/// ZINTERCARD numkeys key [key …] [LIMIT limit] — read-only twin.
pub fn zintercard_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    // Arity before everything else — `ZINTERCARD 0` names no key and is an
    // arity error on Redis, not a numkeys error (moon#969).
    if args.len() < 2 {
        return err_wrong_args("ZINTERCARD");
    }
    let numkeys_bytes = match extract_bytes(&args[0]) {
        Some(b) => b,
        None => return err_wrong_args("ZINTERCARD"),
    };
    let numkeys = match parse_numkeys(numkeys_bytes, "ZINTERCARD") {
        Ok(n) => n,
        Err(e) => return e,
    };
    if args.len() < 1 + numkeys {
        return err("ERR syntax error");
    }
    let keys: Vec<Bytes> = (0..numkeys)
        .map(|j| {
            extract_bytes(&args[1 + j])
                .cloned()
                .unwrap_or_else(Bytes::new)
        })
        .collect();
    let mut limit: usize = 0;
    let mut i = 1 + numkeys;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 1 >= args.len() {
                return err("ERR syntax error");
            }
            let lb = match extract_bytes(&args[i + 1]) {
                Some(b) => b,
                None => return err("ERR syntax error"),
            };
            // `getPositiveLongFromObject(…, "LIMIT can't be negative")`: one
            // message for a negative AND for bytes that are not a number
            // (moon#969). `limit: usize` used to fail a negative into the
            // generic integer error.
            limit = match parse_bounded_count(lb, 0, "ERR LIMIT can't be negative") {
                Ok(v) => v,
                Err(e) => return e,
            };
            i += 2;
        } else {
            // moon#967: an unrecognised token is `ERR syntax error`, not
            // something to step over. Skipping it silently turned a
            // mis-spelled option into a DIFFERENT, successful command.
            return err("ERR syntax error");
        }
    }
    let source_data = match collect_source_sets_readonly(db, &keys, now_ms) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if source_data.iter().any(|s| s.is_empty()) {
        return Frame::Integer(0);
    }
    let mut indices: Vec<usize> = (0..source_data.len()).collect();
    indices.sort_by_key(|&i| source_data[i].len());
    let smallest_idx = indices[0];
    let mut count: i64 = 0;
    for member in source_data[smallest_idx].keys() {
        let mut in_all = true;
        for &idx in indices.iter().skip(1) {
            if !source_data[idx].contains_key(member) {
                in_all = false;
                break;
            }
        }
        if in_all {
            count += 1;
            if limit > 0 && count >= limit as i64 {
                break;
            }
        }
    }
    Frame::Integer(count)
}

/// Where ZRANDMEMBER draws from: the B+tree's order statistics (O(log N) per
/// pick), or a listpack decoded once, unsorted (bounded by
/// `zset-max-listpack-entries`).
enum RandPool<'z> {
    Tree(&'z crate::storage::bptree::BPTree),
    Flat(Vec<(Bytes, f64)>),
}

impl RandPool<'_> {
    fn get(&self, i: usize) -> Option<(&Bytes, f64)> {
        match self {
            RandPool::Tree(t) => t.get_by_rank(i).map(|(s, m)| (m, s.0)),
            RandPool::Flat(v) => v.get(i).map(|(m, s)| (m, *s)),
        }
    }
}

/// ZRANDMEMBER key [count [WITHSCORES]] — read-only twin.
///
/// moon#1171: picks by POSITION. A B+tree zset resolves each position with
/// one `get_by_rank` descent through the subtree counts — O(log N) per
/// member where the old code collected all N `(member, score)` pairs into a
/// Vec first (17 ms for one member of a 1M zset, 264x redis). A listpack is
/// decoded once without the sort `entries_sorted` does. Distribution is
/// unchanged: a uniform position is a uniform member.
pub fn zrandmember_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    use rand::RngExt;
    let Some(key) = args.first().and_then(extract_bytes) else {
        return err_wrong_args("ZRANDMEMBER");
    };
    // redis parses `count [WITHSCORES]` before it looks the key up: a bad
    // count is an error on a missing or wrong-typed key too.
    let with_count = match args.get(1..) {
        Some(tail) if !tail.is_empty() => {
            match crate::command::helpers::parse_rand_count(tail, b"WITHSCORES") {
                Ok(parsed) => Some(parsed),
                Err(e) => return e,
            }
        }
        _ => None,
    };
    // Ref accessor: handles every encoding (BPTree, Listpack from RDB load,
    // Legacy) — the BPTree-only accessor would treat a listpack zset as missing.
    let zref = match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(z)) => z,
        Ok(None) => {
            return if with_count.is_none() {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };
    let len = zref.len();
    if len == 0 {
        return if with_count.is_none() {
            Frame::Null
        } else {
            Frame::Array(framevec![])
        };
    }
    let pool = match zref.any_tree() {
        Some(t) => RandPool::Tree(t),
        None => RandPool::Flat(zref.entries_unordered()),
    };
    let mut rng = rand::rng();
    let Some((count, withscores)) = with_count else {
        return match pool.get(rng.random_range(0..len)) {
            Some((member, _)) => Frame::BulkString(member.clone()),
            None => Frame::Null,
        };
    };
    if count == 0 {
        return Frame::Array(framevec![]);
    }
    let push = |result: &mut Vec<Frame>, member: &Bytes, score: f64| {
        result.push(Frame::BulkString(member.clone()));
        if withscores {
            result.push(Frame::BulkString(format_score_bytes(score)));
        }
    };
    if count > 0 {
        let n = std::cmp::min(count as usize, len);
        let mut result = Vec::with_capacity(if withscores { n * 2 } else { n });
        if n == len {
            // The whole zset with no sampling at all — Redis's CASE 2
            // ("count >= size: return the whole zset"), which walks it with
            // `zuiNext`, whose iterator starts at the skiplist TAIL / the
            // listpack's last pair: DESCENDING (score, member) order.
            // Verified on redis-server 7.0.15 (`ZRANDMEMBER z 100` on a
            // 60-member zset answers its highest score first).
            match &pool {
                RandPool::Tree(t) => {
                    for (score, member) in t.iter_rev() {
                        push(&mut result, member, score.0);
                    }
                }
                RandPool::Flat(_) => {
                    for (member, score) in zref.entries_sorted().into_iter().rev() {
                        push(&mut result, &member, score);
                    }
                }
            }
            return Frame::Array(result.into());
        }
        // n distinct positions in O(n), in random order.
        for i in rand::seq::index::sample(&mut rng, len, n) {
            if let Some((member, score)) = pool.get(i) {
                push(&mut result, member, score);
            }
        }
        Frame::Array(result.into())
    } else {
        // Negative count: allow duplicates — exactly |COUNT| of them (Redis
        // contract). The DoS guard refuses extreme counts loudly instead of
        // silently truncating.
        let n = count.unsigned_abs() as usize;
        if n > crate::command::RAND_DUP_COUNT_MAX {
            return Frame::Error(Bytes::from_static(crate::command::ERR_RAND_COUNT_RANGE));
        }
        let mut result = Vec::with_capacity(if withscores { n * 2 } else { n });
        for _ in 0..n {
            if let Some((member, score)) = pool.get(rng.random_range(0..len)) {
                push(&mut result, member, score);
            }
        }
        Frame::Array(result.into())
    }
}

/// ZMSCORE key member [member …] — read-only twin.
pub fn zmscore_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("ZMSCORE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZMSCORE"),
    };
    // Ref accessor: handles every encoding (BPTree, Listpack from RDB load,
    // Legacy) — the BPTree-only accessor would treat a listpack zset as missing.
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            let mut result = Vec::with_capacity(args.len() - 1);
            for arg in &args[1..] {
                let member = match extract_bytes(arg) {
                    Some(m) => m,
                    None => {
                        result.push(Frame::Null);
                        continue;
                    }
                };
                match zref.score(member) {
                    Some(score) => {
                        result.push(Frame::BulkString(format_score_bytes(score)));
                    }
                    None => result.push(Frame::Null),
                }
            }
            Frame::Array(result.into())
        }
        Ok(None) => {
            let mut result = Vec::with_capacity(args.len() - 1);
            for _ in &args[1..] {
                result.push(Frame::Null);
            }
            Frame::Array(result.into())
        }
        Err(e) => e,
    }
}
