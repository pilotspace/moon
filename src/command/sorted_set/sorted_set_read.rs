use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::SortedSetRef;

use crate::command::helpers::{err, err_wrong_args, extract_bytes};

use super::{
    format_score, glob_match, lex_in_range, parse_lex_bound, parse_score_bound, zrange_by_lex,
    zrange_by_rank, zrange_by_score, zrange_from_entries,
};

// ---------------------------------------------------------------------------
// Mutable-path read commands (take &mut Database for historical reasons)
// ---------------------------------------------------------------------------

/// ZSCORE key member
pub fn zscore(db: &mut Database, args: &[Frame]) -> Frame {
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

    match db.get_sorted_set(key) {
        Ok(Some((members, _scores))) => match members.get(member) {
            Some(score) => Frame::BulkString(Bytes::from(format_score(*score))),
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// ZCARD key
pub fn zcard(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("ZCARD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZCARD"),
    };

    match db.get_sorted_set(key) {
        Ok(Some((members, _scores))) => Frame::Integer(members.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZRANK key member
pub fn zrank(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("ZRANK");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZRANK"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZRANK"),
    };

    match db.get_sorted_set(key) {
        Ok(Some((members, scores))) => match members.get(member) {
            Some(score) => match scores.rank(OrderedFloat(*score), member) {
                Some(rank) => Frame::Integer(rank as i64),
                None => Frame::Null,
            },
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// ZREVRANK key member
pub fn zrevrank(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("ZREVRANK");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZREVRANK"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZREVRANK"),
    };

    match db.get_sorted_set(key) {
        Ok(Some((members, scores))) => match members.get(member) {
            Some(score) => match scores.rev_rank(OrderedFloat(*score), member) {
                Some(rev_rank) => Frame::Integer(rev_rank as i64),
                None => Frame::Null,
            },
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// ZSCAN key cursor [MATCH pattern] [COUNT count]
pub fn zscan(db: &mut Database, args: &[Frame]) -> Frame {
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

    // Parse optional MATCH and COUNT
    let mut pattern: Option<&[u8]> = None;
    let mut scan_count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            if i + 1 < args.len() {
                pattern = extract_bytes(&args[i + 1]).map(|b| b.as_ref());
                i += 2;
            } else {
                return err_wrong_args("ZSCAN");
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            if i + 1 < args.len() {
                let count_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZSCAN"),
                };
                scan_count = match std::str::from_utf8(count_b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(c) => c,
                    None => return err("ERR value is not an integer or out of range"),
                };
                i += 2;
            } else {
                return err_wrong_args("ZSCAN");
            }
        } else {
            i += 1;
        }
    }

    match db.get_sorted_set(key) {
        Ok(Some((members, _scores))) => {
            // Collect all members sorted for deterministic cursor
            let mut all_members: Vec<(&Bytes, &f64)> = members.iter().collect();
            all_members.sort_by(|a, b| a.0.cmp(b.0));

            let mut result_items = Vec::new();
            let mut pos = cursor;
            let mut returned = 0;

            while pos < all_members.len() && returned < scan_count {
                let (member, score) = all_members[pos];
                let matches = match pattern {
                    Some(p) => glob_match(p, member),
                    None => true,
                };
                if matches {
                    result_items.push(Frame::BulkString(member.clone()));
                    result_items.push(Frame::BulkString(Bytes::from(format_score(*score))));
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
                Frame::Array(result_items.into()),
            ])
        }
        Ok(None) => Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"0")),
            Frame::Array(framevec![]),
        ]),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// Range commands
// ---------------------------------------------------------------------------

/// ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
pub fn zrange(db: &mut Database, args: &[Frame]) -> Frame {
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

    // Parse optional flags
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
            i += 1;
        }
    }

    if by_score && by_lex {
        return err("ERR BYSCORE and BYLEX options are not compatible");
    }

    // LIMIT is only valid with BYSCORE or BYLEX
    if limit_offset.is_some() && !by_score && !by_lex {
        return err(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        );
    }

    match db.get_sorted_set(key) {
        Ok(Some((members, scores))) => {
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
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZREVRANGE key start stop [WITHSCORES]
pub fn zrevrange(db: &mut Database, args: &[Frame]) -> Frame {
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

    match db.get_sorted_set(key) {
        Ok(Some((_members, scores))) => {
            zrange_by_rank(scores, &start_arg, &stop_arg, true, withscores)
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
pub fn zrangebyscore(db: &mut Database, args: &[Frame]) -> Frame {
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
            i += 1;
        }
    }

    match db.get_sorted_set(key) {
        Ok(Some((members, scores))) => zrange_by_score(
            members,
            scores,
            &min_arg,
            &max_arg,
            false,
            withscores,
            limit_offset,
            limit_count,
        ),
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
pub fn zrevrangebyscore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("ZREVRANGEBYSCORE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZREVRANGEBYSCORE"),
    };
    // NOTE: arg order is max then min (reversed from ZRANGEBYSCORE)
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
            i += 1;
        }
    }

    match db.get_sorted_set(key) {
        Ok(Some((members, scores))) => {
            // Use min_arg and max_arg but in correct order for the score filter,
            // then reverse the result
            zrange_by_score(
                members,
                scores,
                &min_arg,
                &max_arg,
                true,
                withscores,
                limit_offset,
                limit_count,
            )
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// ZCOUNT key min max
pub fn zcount(db: &mut Database, args: &[Frame]) -> Frame {
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

    match db.get_sorted_set(key) {
        Ok(Some((_members, scores))) => {
            let range_min = OrderedFloat(min_bound.value());
            let range_max = OrderedFloat(max_bound.value());
            let count = scores
                .range(range_min, range_max)
                .filter(|(score, _)| {
                    min_bound.includes(score.0) && max_bound.includes_upper(score.0)
                })
                .count();
            Frame::Integer(count as i64)
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZLEXCOUNT key min max
pub fn zlexcount(db: &mut Database, args: &[Frame]) -> Frame {
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

    match db.get_sorted_set(key) {
        Ok(Some((_members, scores))) => {
            let count = scores
                .iter()
                .filter(|(_, member)| lex_in_range(member, &min_bound, &max_bound))
                .count();
            Frame::Integer(count as i64)
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
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

/// ZRANK (read-only).
pub fn zrank_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("ZRANK");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZRANK"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZRANK"),
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            // For BPTree/Legacy variants, use the optimized rank
            match (&zref, zref.score(member)) {
                (SortedSetRef::BPTree { tree, .. }, Some(score)) => {
                    match tree.rank(OrderedFloat(score), member) {
                        Some(rank) => Frame::Integer(rank as i64),
                        None => Frame::Null,
                    }
                }
                (SortedSetRef::Listpack(_), Some(score)) => {
                    // For listpack, compute rank from sorted entries
                    let entries = zref.entries_sorted();
                    let target_score = OrderedFloat(score);
                    let target_member = Bytes::copy_from_slice(member);
                    match entries
                        .iter()
                        .position(|(m, s)| OrderedFloat(*s) == target_score && *m == target_member)
                    {
                        Some(rank) => Frame::Integer(rank as i64),
                        None => Frame::Null,
                    }
                }
                _ => Frame::Null,
            }
        }
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// ZREVRANK (read-only).
pub fn zrevrank_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("ZREVRANK");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZREVRANK"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZREVRANK"),
    };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match (&zref, zref.score(member)) {
            (SortedSetRef::BPTree { tree, .. }, Some(score)) => {
                match tree.rev_rank(OrderedFloat(score), member) {
                    Some(rev_rank) => Frame::Integer(rev_rank as i64),
                    None => Frame::Null,
                }
            }
            (SortedSetRef::Listpack(_), Some(score)) => {
                let entries = zref.entries_sorted();
                let target_score = OrderedFloat(score);
                let target_member = Bytes::copy_from_slice(member);
                match entries
                    .iter()
                    .position(|(m, s)| OrderedFloat(*s) == target_score && *m == target_member)
                {
                    Some(rank) => Frame::Integer((entries.len() - 1 - rank) as i64),
                    None => Frame::Null,
                }
            }
            _ => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
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
            i += 1;
        }
    }
    if by_score && by_lex {
        return err("ERR BYSCORE and BYLEX options are not compatible");
    }
    if limit_offset.is_some() && !by_score && !by_lex {
        return err(
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
        );
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
            i += 1;
        }
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
            i += 1;
        }
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
        Ok(Some(zref)) => match zref.bptree() {
            Some(scores) => {
                let range_min = OrderedFloat(min_bound.value());
                let range_max = OrderedFloat(max_bound.value());
                let count = scores
                    .range(range_min, range_max)
                    .filter(|(score, _)| {
                        min_bound.includes(score.0) && max_bound.includes_upper(score.0)
                    })
                    .count();
                Frame::Integer(count as i64)
            }
            None => {
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
        Ok(Some(zref)) => match zref.bptree() {
            Some(scores) => {
                let count = scores
                    .iter()
                    .filter(|(_, member)| lex_in_range(member, &min_bound, &max_bound))
                    .count();
                Frame::Integer(count as i64)
            }
            None => {
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
    let mut pattern: Option<&[u8]> = None;
    let mut scan_count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            if i + 1 < args.len() {
                pattern = extract_bytes(&args[i + 1]).map(|b| b.as_ref());
                i += 2;
            } else {
                return err_wrong_args("ZSCAN");
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            if i + 1 < args.len() {
                let count_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZSCAN"),
                };
                scan_count = match std::str::from_utf8(count_b)
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(c) => c,
                    None => return err("ERR value is not an integer or out of range"),
                };
                i += 2;
            } else {
                return err_wrong_args("ZSCAN");
            }
        } else {
            i += 1;
        }
    }
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
