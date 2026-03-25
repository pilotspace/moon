use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::HashMap;

use crate::storage::bptree::BPTree;

use crate::protocol::Frame;
use crate::storage::db::SortedSetRef;
use crate::storage::Database;

/// Helper: return ERR wrong number of arguments for a given command.
fn err_wrong_args(cmd: &str) -> Frame {
    Frame::Error(Bytes::from(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd
    )))
}

/// Helper: return a generic error.
fn err(msg: &str) -> Frame {
    Frame::Error(Bytes::from(msg.to_string()))
}

/// Helper: extract &Bytes from a BulkString or SimpleString frame.
fn extract_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// Format a float score for Redis output (strip trailing zeros, but keep at least one decimal).
fn format_score(score: f64) -> String {
    if score == f64::INFINITY {
        "inf".to_string()
    } else if score == f64::NEG_INFINITY {
        "-inf".to_string()
    } else {
        // Use ryu or manual formatting to match Redis behavior
        let s = format!("{}", score);
        s
    }
}

// ---------------------------------------------------------------------------
// Internal helpers -- CRITICAL for dual structure consistency
// ---------------------------------------------------------------------------

/// Add or update a member in the sorted set. Returns true if the member is new.
fn zadd_member(
    members: &mut HashMap<Bytes, f64>,
    scores: &mut BPTree,
    member: Bytes,
    score: f64,
) -> bool {
    // Remove old entry if exists (MUST remove from both)
    let is_new = if let Some(old_score) = members.remove(&member) {
        scores.remove(OrderedFloat(old_score), &member);
        false
    } else {
        true
    };
    members.insert(member.clone(), score);
    scores.insert(OrderedFloat(score), member);
    is_new
}

/// Remove a member from the sorted set. Returns true if the member existed.
fn zrem_member(
    members: &mut HashMap<Bytes, f64>,
    scores: &mut BPTree,
    member: &[u8],
) -> bool {
    if let Some(score) = members.remove(member) {
        scores.remove(OrderedFloat(score), member);
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------
// Score boundary parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum ScoreBound {
    Inclusive(f64),
    Exclusive(f64),
    NegInf,
    PosInf,
}

impl ScoreBound {
    fn value(&self) -> f64 {
        match self {
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => *v,
            ScoreBound::NegInf => f64::NEG_INFINITY,
            ScoreBound::PosInf => f64::INFINITY,
        }
    }

    fn includes(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(v) => score >= *v || (score - *v).abs() < f64::EPSILON,
            ScoreBound::Exclusive(v) => score > *v,
        }
    }

    fn includes_upper(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(v) => score <= *v || (score - *v).abs() < f64::EPSILON,
            ScoreBound::Exclusive(v) => score < *v,
        }
    }
}

fn parse_score_bound(s: &[u8]) -> Result<ScoreBound, Frame> {
    let s_str = std::str::from_utf8(s)
        .map_err(|_| err("ERR min or max is not a float"))?;
    if s_str == "-inf" {
        return Ok(ScoreBound::NegInf);
    }
    if s_str == "+inf" || s_str == "inf" {
        return Ok(ScoreBound::PosInf);
    }
    if let Some(rest) = s_str.strip_prefix('(') {
        let val: f64 = rest
            .parse()
            .map_err(|_| err("ERR min or max is not a float"))?;
        if val.is_nan() {
            return Err(err("ERR min or max is not a float"));
        }
        return Ok(ScoreBound::Exclusive(val));
    }
    let val: f64 = s_str
        .parse()
        .map_err(|_| err("ERR min or max is not a float"))?;
    if val.is_nan() {
        return Err(err("ERR min or max is not a float"));
    }
    Ok(ScoreBound::Inclusive(val))
}

// ---------------------------------------------------------------------------
// Lex boundary parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum LexBound {
    Inclusive(Bytes),
    Exclusive(Bytes),
    NegInf,
    PosInf,
}

fn parse_lex_bound(s: &[u8]) -> Result<LexBound, Frame> {
    if s == b"-" {
        return Ok(LexBound::NegInf);
    }
    if s == b"+" {
        return Ok(LexBound::PosInf);
    }
    if s.is_empty() {
        return Err(err(
            "ERR min or max not valid string range item",
        ));
    }
    match s[0] {
        b'[' => Ok(LexBound::Inclusive(Bytes::copy_from_slice(&s[1..]))),
        b'(' => Ok(LexBound::Exclusive(Bytes::copy_from_slice(&s[1..]))),
        _ => Err(err(
            "ERR min or max not valid string range item",
        )),
    }
}

fn lex_in_range(member: &[u8], min: &LexBound, max: &LexBound) -> bool {
    let above_min = match min {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Inclusive(v) => member >= v.as_ref(),
        LexBound::Exclusive(v) => member > v.as_ref(),
    };
    let below_max = match max {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Inclusive(v) => member <= v.as_ref(),
        LexBound::Exclusive(v) => member < v.as_ref(),
    };
    above_min && below_max
}

// ---------------------------------------------------------------------------
// Core commands
// ---------------------------------------------------------------------------

/// ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
pub fn zadd(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("ZADD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZADD"),
    };

    // Parse flags
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut i = 1;

    while i < args.len() {
        let arg = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => break,
        };
        if arg.eq_ignore_ascii_case(b"NX") { nx = true; i += 1; }
        else if arg.eq_ignore_ascii_case(b"XX") { xx = true; i += 1; }
        else if arg.eq_ignore_ascii_case(b"GT") { gt = true; i += 1; }
        else if arg.eq_ignore_ascii_case(b"LT") { lt = true; i += 1; }
        else if arg.eq_ignore_ascii_case(b"CH") { ch = true; i += 1; }
        else { break; }
    }

    // NX and XX are mutually exclusive
    if nx && xx {
        return err("ERR XX and NX options at the same time are not compatible");
    }
    // NX and GT/LT are not compatible
    if nx && (gt || lt) {
        return err("ERR GT, LT, and NX options at the same time are not compatible");
    }

    // Remaining args must be score member pairs
    let remaining = &args[i..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return err_wrong_args("ZADD");
    }

    let (members, scores) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut added = 0i64;
    let mut changed = 0i64;

    let mut j = 0;
    while j < remaining.len() {
        let score_bytes = match extract_bytes(&remaining[j]) {
            Some(b) => b,
            None => return err_wrong_args("ZADD"),
        };
        let member = match extract_bytes(&remaining[j + 1]) {
            Some(b) => b.clone(),
            None => return err_wrong_args("ZADD"),
        };

        let score_str = match std::str::from_utf8(score_bytes) {
            Ok(s) => s,
            Err(_) => return err("ERR value is not a valid float"),
        };
        let score: f64 = match score_str.parse() {
            Ok(v) => v,
            Err(_) => return err("ERR value is not a valid float"),
        };
        if score.is_nan() {
            return err("ERR value is not a valid float");
        }

        let existing_score = members.get(&member).copied();

        let should_update = match existing_score {
            None => !xx,  // New member: add unless XX
            Some(old) => {
                if nx {
                    false  // NX: never update existing
                } else if gt && lt {
                    false  // GT+LT together: never update (mutually exclusive)
                } else if gt {
                    score > old
                } else if lt {
                    score < old
                } else {
                    true  // No flags: always update
                }
            }
        };

        if should_update {
            let is_new = zadd_member(members, scores, member, score);
            if is_new {
                added += 1;
                changed += 1;
            } else if existing_score.is_some() && (existing_score.unwrap() - score).abs() > f64::EPSILON {
                changed += 1;
            }
        }

        j += 2;
    }

    if ch {
        Frame::Integer(changed)
    } else {
        Frame::Integer(added)
    }
}

/// ZREM key member [member ...]
pub fn zrem(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("ZREM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("ZREM"),
    };

    let (members, scores) = match db.get_or_create_sorted_set(&key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut removed = 0i64;
    for arg in &args[1..] {
        let member = match extract_bytes(arg) {
            Some(b) => b,
            None => return err_wrong_args("ZREM"),
        };
        if zrem_member(members, scores, member) {
            removed += 1;
        }
    }

    // Remove key if empty
    if members.is_empty() {
        db.remove(&key);
    }

    Frame::Integer(removed)
}

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
        Ok(Some((members, _scores))) => {
            match members.get(member) {
                Some(score) => Frame::BulkString(Bytes::from(format_score(*score))),
                None => Frame::Null,
            }
        }
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

/// ZINCRBY key increment member
pub fn zincrby(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("ZINCRBY");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZINCRBY"),
    };
    let incr_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("ZINCRBY"),
    };
    let member = match extract_bytes(&args[2]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZINCRBY"),
    };

    let incr_str = match std::str::from_utf8(incr_bytes) {
        Ok(s) => s,
        Err(_) => return err("ERR value is not a valid float"),
    };
    let increment: f64 = match incr_str.parse() {
        Ok(v) => v,
        Err(_) => return err("ERR value is not a valid float"),
    };
    if increment.is_nan() {
        return err("ERR value is not a valid float");
    }

    let (members, scores) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let current = members.get(&member).copied().unwrap_or(0.0);
    let new_score = current + increment;

    zadd_member(members, scores, member, new_score);

    Frame::BulkString(Bytes::from(format_score(new_score)))
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
        Ok(Some((members, scores))) => {
            match members.get(member) {
                Some(score) => {
                    match scores.rank(OrderedFloat(*score), member) {
                        Some(rank) => Frame::Integer(rank as i64),
                        None => Frame::Null,
                    }
                }
                None => Frame::Null,
            }
        }
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
        Ok(Some((members, scores))) => {
            match members.get(member) {
                Some(score) => {
                    match scores.rev_rank(OrderedFloat(*score), member) {
                        Some(rev_rank) => Frame::Integer(rev_rank as i64),
                        None => Frame::Null,
                    }
                }
                None => Frame::Null,
            }
        }
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// ZPOPMIN key [count]
pub fn zpopmin(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("ZPOPMIN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("ZPOPMIN"),
    };

    let count = if args.len() == 2 {
        let count_bytes = match extract_bytes(&args[1]) {
            Some(b) => b,
            None => return err_wrong_args("ZPOPMIN"),
        };
        match std::str::from_utf8(count_bytes).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(c) if c >= 0 => c as usize,
            _ => return err("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };

    let (members, scores) = match db.get_or_create_sorted_set(&key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut result = Vec::new();
    for _ in 0..count {
        let first = scores.iter().next().map(|(s, m)| (s, m.clone()));
        match first {
            Some((score, member)) => {
                scores.remove(score, &member);
                members.remove(&member);
                result.push(Frame::BulkString(member));
                result.push(Frame::BulkString(Bytes::from(format_score(score.0))));
            }
            None => break,
        }
    }

    // Remove key if empty
    if members.is_empty() {
        db.remove(&key);
    }

    Frame::Array(result)
}

/// ZPOPMAX key [count]
pub fn zpopmax(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("ZPOPMAX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("ZPOPMAX"),
    };

    let count = if args.len() == 2 {
        let count_bytes = match extract_bytes(&args[1]) {
            Some(b) => b,
            None => return err_wrong_args("ZPOPMAX"),
        };
        match std::str::from_utf8(count_bytes).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(c) if c >= 0 => c as usize,
            _ => return err("ERR value is not an integer or out of range"),
        }
    } else {
        1
    };

    let (members, scores) = match db.get_or_create_sorted_set(&key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut result = Vec::new();
    for _ in 0..count {
        let last = scores.iter_rev().next().map(|(s, m)| (s, m.clone()));
        match last {
            Some((score, member)) => {
                scores.remove(score, &member);
                members.remove(&member);
                result.push(Frame::BulkString(member));
                result.push(Frame::BulkString(Bytes::from(format_score(score.0))));
            }
            None => break,
        }
    }

    // Remove key if empty
    if members.is_empty() {
        db.remove(&key);
    }

    Frame::Array(result)
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
            None => { i += 1; continue; }
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

            Frame::Array(vec![
                Frame::BulkString(next_cursor),
                Frame::Array(result_items),
            ])
        }
        Ok(None) => {
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(vec![]),
            ])
        }
        Err(e) => e,
    }
}

/// Simple glob pattern matcher (same approach as key.rs).
fn glob_match(pattern: &[u8], input: &[u8]) -> bool {
    let mut pi = 0;
    let mut ii = 0;
    let mut star_p = None;
    let mut star_i = 0;

    while ii < input.len() {
        if pi < pattern.len() && (pattern[pi] == b'?' || pattern[pi] == input[ii]) {
            pi += 1;
            ii += 1;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_p = Some(pi);
            star_i = ii;
            pi += 1;
        } else if let Some(sp) = star_p {
            pi = sp + 1;
            star_i += 1;
            ii = star_i;
        } else {
            return false;
        }
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
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
            None => { i += 1; continue; }
        };
        if opt.eq_ignore_ascii_case(b"BYSCORE") { by_score = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"BYLEX") { by_lex = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"REV") { rev = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"WITHSCORES") { withscores = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGE"),
                };
                let cnt_b = match extract_bytes(&args[i + 2]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGE"),
                };
                limit_offset = std::str::from_utf8(off_b)
                    .ok()
                    .and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b)
                    .ok()
                    .and_then(|s| s.parse().ok());
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
        return err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
    }

    match db.get_sorted_set(key) {
        Ok(Some((members, scores))) => {
            if by_score {
                zrange_by_score(members, scores, &min_arg, &max_arg, rev, withscores, limit_offset, limit_count)
            } else if by_lex {
                zrange_by_lex(scores, &min_arg, &max_arg, rev, withscores, members, limit_offset, limit_count)
            } else {
                zrange_by_rank(scores, &min_arg, &max_arg, rev, withscores)
            }
        }
        Ok(None) => Frame::Array(vec![]),
        Err(e) => e,
    }
}

fn zrange_by_rank(
    scores: &BPTree,
    min_arg: &[u8],
    max_arg: &[u8],
    rev: bool,
    withscores: bool,
) -> Frame {
    let total = scores.len() as i64;
    if total == 0 {
        return Frame::Array(vec![]);
    }

    let start_raw: i64 = match std::str::from_utf8(min_arg).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return err("ERR value is not an integer or out of range"),
    };
    let stop_raw: i64 = match std::str::from_utf8(max_arg).ok().and_then(|s| s.parse().ok()) {
        Some(v) => v,
        None => return err("ERR value is not an integer or out of range"),
    };

    // Normalize negative indices
    let start = if start_raw < 0 { (total + start_raw).max(0) } else { start_raw.min(total) };
    let stop = if stop_raw < 0 { (total + stop_raw).max(0) } else { stop_raw.min(total - 1) };

    if start > stop {
        return Frame::Array(vec![]);
    }

    let mut result = Vec::new();

    if rev {
        // Reverse: rank 0 = highest score
        let rev_start = (total - 1 - stop) as usize;
        let rev_stop = (total - 1 - start) as usize;
        let entries = scores.range_by_rank(rev_start, rev_stop);
        for (score, member) in entries.into_iter().rev() {
            result.push(Frame::BulkString(member.clone()));
            if withscores {
                result.push(Frame::BulkString(Bytes::from(format_score(score.0))));
            }
        }
    } else {
        let entries = scores.range_by_rank(start as usize, stop as usize);
        for (score, member) in entries {
            result.push(Frame::BulkString(member.clone()));
            if withscores {
                result.push(Frame::BulkString(Bytes::from(format_score(score.0))));
            }
        }
    }

    Frame::Array(result)
}

fn zrange_by_score(
    members: &HashMap<Bytes, f64>,
    scores: &BPTree,
    min_arg: &[u8],
    max_arg: &[u8],
    rev: bool,
    withscores: bool,
    limit_offset: Option<i64>,
    limit_count: Option<i64>,
) -> Frame {
    let (min_bound, max_bound) = if rev {
        // With REV, max comes first in args
        let max_b = match parse_score_bound(min_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let min_b = match parse_score_bound(max_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        (min_b, max_b)
    } else {
        let min_b = match parse_score_bound(min_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let max_b = match parse_score_bound(max_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        (min_b, max_b)
    };

    let _ = members; // not directly needed; scores has all data

    // Use BPTree range to get entries in the score range, then apply bound filtering
    let range_min = OrderedFloat(min_bound.value());
    let range_max = OrderedFloat(max_bound.value());
    // Ensure min <= max for BPTree range call
    let (range_lo, range_hi) = if range_min <= range_max {
        (range_min, range_max)
    } else {
        (range_max, range_min)
    };

    let mut entries: Vec<(f64, &Bytes)> = Vec::new();
    for (score, member) in scores.range(range_lo, range_hi) {
        let s = score.0;
        if min_bound.includes(s) && max_bound.includes_upper(s) {
            entries.push((s, member));
        }
    }

    if rev {
        entries.reverse();
    }

    // Apply LIMIT
    let offset = limit_offset.unwrap_or(0).max(0) as usize;
    let count = limit_count.unwrap_or(-1);
    let limited: Vec<_> = if count < 0 {
        entries.into_iter().skip(offset).collect()
    } else {
        entries.into_iter().skip(offset).take(count as usize).collect()
    };

    let mut result = Vec::new();
    for (score, member) in limited {
        result.push(Frame::BulkString(member.clone()));
        if withscores {
            result.push(Frame::BulkString(Bytes::from(format_score(score))));
        }
    }

    Frame::Array(result)
}

fn zrange_by_lex(
    scores: &BPTree,
    min_arg: &[u8],
    max_arg: &[u8],
    rev: bool,
    withscores: bool,
    members: &HashMap<Bytes, f64>,
    limit_offset: Option<i64>,
    limit_count: Option<i64>,
) -> Frame {
    let (min_bound, max_bound) = if rev {
        let max_b = match parse_lex_bound(min_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let min_b = match parse_lex_bound(max_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        (min_b, max_b)
    } else {
        let min_b = match parse_lex_bound(min_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let max_b = match parse_lex_bound(max_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        (min_b, max_b)
    };

    let mut entries: Vec<&Bytes> = Vec::new();
    for (_, member) in scores.iter() {
        if lex_in_range(member, &min_bound, &max_bound) {
            entries.push(member);
        }
    }

    if rev {
        entries.reverse();
    }

    // Apply LIMIT
    let offset = limit_offset.unwrap_or(0).max(0) as usize;
    let count = limit_count.unwrap_or(-1);
    let limited: Vec<_> = if count < 0 {
        entries.into_iter().skip(offset).collect()
    } else {
        entries.into_iter().skip(offset).take(count as usize).collect()
    };

    let mut result = Vec::new();
    for member in limited {
        result.push(Frame::BulkString(member.clone()));
        if withscores {
            if let Some(score) = members.get(member) {
                result.push(Frame::BulkString(Bytes::from(format_score(*score))));
            }
        }
    }

    Frame::Array(result)
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
        Ok(None) => Frame::Array(vec![]),
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
            None => { i += 1; continue; }
        };
        if opt.eq_ignore_ascii_case(b"WITHSCORES") { withscores = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGEBYSCORE"),
                };
                let cnt_b = match extract_bytes(&args[i + 2]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGEBYSCORE"),
                };
                limit_offset = std::str::from_utf8(off_b)
                    .ok()
                    .and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b)
                    .ok()
                    .and_then(|s| s.parse().ok());
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
        Ok(Some((members, scores))) => {
            zrange_by_score(members, scores, &min_arg, &max_arg, false, withscores, limit_offset, limit_count)
        }
        Ok(None) => Frame::Array(vec![]),
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
            None => { i += 1; continue; }
        };
        if opt.eq_ignore_ascii_case(b"WITHSCORES") { withscores = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZREVRANGEBYSCORE"),
                };
                let cnt_b = match extract_bytes(&args[i + 2]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZREVRANGEBYSCORE"),
                };
                limit_offset = std::str::from_utf8(off_b)
                    .ok()
                    .and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b)
                    .ok()
                    .and_then(|s| s.parse().ok());
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
            zrange_by_score(members, scores, &min_arg, &max_arg, true, withscores, limit_offset, limit_count)
        }
        Ok(None) => Frame::Array(vec![]),
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
                .filter(|(score, _)| min_bound.includes(score.0) && max_bound.includes_upper(score.0))
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

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn zunionstore(db: &mut Database, args: &[Frame]) -> Frame {
    zstore_impl(db, args, false)
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn zinterstore(db: &mut Database, args: &[Frame]) -> Frame {
    zstore_impl(db, args, true)
}

fn zstore_impl(db: &mut Database, args: &[Frame], intersect: bool) -> Frame {
    let cmd_name = if intersect { "ZINTERSTORE" } else { "ZUNIONSTORE" };
    if args.len() < 3 {
        return err_wrong_args(cmd_name);
    }
    let dest = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args(cmd_name),
    };
    let numkeys_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args(cmd_name),
    };
    let numkeys: usize = match std::str::from_utf8(numkeys_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) => n,
        None => return err("ERR value is not an integer or out of range"),
    };

    if numkeys == 0 || args.len() < 2 + numkeys {
        return err_wrong_args(cmd_name);
    }

    // Collect source keys
    let source_keys: Vec<Bytes> = (0..numkeys)
        .map(|j| {
            extract_bytes(&args[2 + j])
                .cloned()
                .unwrap_or_else(|| Bytes::new())
        })
        .collect();

    // Parse WEIGHTS and AGGREGATE
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = AggregateOp::Sum;
    let mut i = 2 + numkeys;

    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => { i += 1; continue; }
        };
        if opt.eq_ignore_ascii_case(b"WEIGHTS") {
            for w in 0..numkeys {
                if i + 1 + w >= args.len() {
                    return err_wrong_args(cmd_name);
                }
                let wb = match extract_bytes(&args[i + 1 + w]) {
                    Some(b) => b,
                    None => return err_wrong_args(cmd_name),
                };
                let wval: f64 = match std::str::from_utf8(wb)
                    .ok()
                    .and_then(|s| s.parse().ok())
                {
                    Some(v) => v,
                    None => return err("ERR weight value is not a float"),
                };
                weights[w] = wval;
            }
            i += 1 + numkeys;
        } else if opt.eq_ignore_ascii_case(b"AGGREGATE") {
            if i + 1 >= args.len() {
                return err_wrong_args(cmd_name);
            }
            let agg_b = match extract_bytes(&args[i + 1]) {
                Some(b) => b.as_ref(),
                None => return err_wrong_args(cmd_name),
            };
            aggregate = if agg_b.eq_ignore_ascii_case(b"SUM") {
                AggregateOp::Sum
            } else if agg_b.eq_ignore_ascii_case(b"MIN") {
                AggregateOp::Min
            } else if agg_b.eq_ignore_ascii_case(b"MAX") {
                AggregateOp::Max
            } else {
                return err("ERR syntax error");
            };
            i += 2;
        } else {
            i += 1;
        }
    }

    // Read all source sets into a temporary structure
    let mut source_data: Vec<HashMap<Bytes, f64>> = Vec::with_capacity(numkeys);
    for key in &source_keys {
        match db.get_sorted_set(key) {
            Ok(Some((members, _))) => {
                source_data.push(members.clone());
            }
            Ok(None) => {
                source_data.push(HashMap::new());
            }
            Err(e) => return e,
        }
    }

    // Compute result
    let mut result_map: HashMap<Bytes, f64> = HashMap::new();

    if intersect {
        // Start with first set's members
        if let Some(first) = source_data.first() {
            for (member, score) in first {
                let weighted = *score * weights[0];
                let mut final_score = weighted;
                let mut in_all = true;

                for (idx, src) in source_data.iter().enumerate().skip(1) {
                    match src.get(member) {
                        Some(s) => {
                            let ws = *s * weights[idx];
                            final_score = match aggregate {
                                AggregateOp::Sum => final_score + ws,
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
    } else {
        // Union: all members from all sets
        for (idx, src) in source_data.iter().enumerate() {
            for (member, score) in src {
                let weighted = *score * weights[idx];
                result_map
                    .entry(member.clone())
                    .and_modify(|existing| {
                        *existing = match aggregate {
                            AggregateOp::Sum => *existing + weighted,
                            AggregateOp::Min => existing.min(weighted),
                            AggregateOp::Max => existing.max(weighted),
                        };
                    })
                    .or_insert(weighted);
            }
        }
    }

    let result_size = result_map.len() as i64;

    // Remove destination key first, then create new sorted set
    db.remove(&dest);

    if !result_map.is_empty() {
        let (members, scores) = match db.get_or_create_sorted_set(&dest) {
            Ok(pair) => pair,
            Err(e) => return e,
        };

        for (member, score) in result_map {
            zadd_member(members, scores, member, score);
        }
    }

    Frame::Integer(result_size)
}

#[derive(Debug, Clone, Copy)]
enum AggregateOp {
    Sum,
    Min,
    Max,
}

/// Fallback range helper for listpack-encoded sorted sets.
/// Operates on pre-sorted `Vec<(Bytes, f64)>` entries.
#[allow(clippy::too_many_arguments)]
fn zrange_from_entries(
    entries: &[(Bytes, f64)],
    min_arg: &[u8],
    max_arg: &[u8],
    by_score: bool,
    by_lex: bool,
    rev: bool,
    withscores: bool,
    limit_offset: Option<i64>,
    limit_count: Option<i64>,
) -> Frame {
    let total = entries.len() as i64;
    if total == 0 {
        return Frame::Array(vec![]);
    }

    if by_score {
        let min_bound = match parse_score_bound(min_arg) { Ok(b) => b, Err(e) => return e };
        let max_bound = match parse_score_bound(max_arg) { Ok(b) => b, Err(e) => return e };
        let mut filtered: Vec<&(Bytes, f64)> = entries.iter()
            .filter(|(_, s)| min_bound.includes(*s) && max_bound.includes_upper(*s))
            .collect();
        if rev { filtered.reverse(); }
        let offset = limit_offset.unwrap_or(0).max(0) as usize;
        let count = limit_count.map(|c| if c < 0 { filtered.len() } else { c as usize }).unwrap_or(filtered.len());
        let result: Vec<Frame> = filtered.into_iter().skip(offset).take(count).flat_map(|(member, score)| {
            let mut v = vec![Frame::BulkString(member.clone())];
            if withscores { v.push(Frame::BulkString(Bytes::from(format_score(*score)))); }
            v
        }).collect();
        Frame::Array(result)
    } else if by_lex {
        let min_bound = match parse_lex_bound(min_arg) { Ok(b) => b, Err(e) => return e };
        let max_bound = match parse_lex_bound(max_arg) { Ok(b) => b, Err(e) => return e };
        let mut filtered: Vec<&(Bytes, f64)> = entries.iter()
            .filter(|(member, _)| lex_in_range(member, &min_bound, &max_bound))
            .collect();
        if rev { filtered.reverse(); }
        let offset = limit_offset.unwrap_or(0).max(0) as usize;
        let count = limit_count.map(|c| if c < 0 { filtered.len() } else { c as usize }).unwrap_or(filtered.len());
        let result: Vec<Frame> = filtered.into_iter().skip(offset).take(count).flat_map(|(member, score)| {
            let mut v = vec![Frame::BulkString(member.clone())];
            if withscores { v.push(Frame::BulkString(Bytes::from(format_score(*score)))); }
            v
        }).collect();
        Frame::Array(result)
    } else {
        // By rank
        let start_raw: i64 = match std::str::from_utf8(min_arg).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return err("ERR value is not an integer or out of range"),
        };
        let stop_raw: i64 = match std::str::from_utf8(max_arg).ok().and_then(|s| s.parse().ok()) {
            Some(v) => v,
            None => return err("ERR value is not an integer or out of range"),
        };
        let start = if start_raw < 0 { (total + start_raw).max(0) as usize } else { start_raw as usize };
        let stop = if stop_raw < 0 { (total + stop_raw).max(0) as usize } else { (stop_raw as usize).min(entries.len().saturating_sub(1)) };
        if start > stop || start >= entries.len() {
            return Frame::Array(vec![]);
        }
        let slice: Vec<&(Bytes, f64)> = if rev {
            entries[start..=stop].iter().rev().collect()
        } else {
            entries[start..=stop].iter().collect()
        };
        let result: Vec<Frame> = slice.into_iter().flat_map(|(member, score)| {
            let mut v = vec![Frame::BulkString(member.clone())];
            if withscores { v.push(Frame::BulkString(Bytes::from(format_score(*score)))); }
            v
        }).collect();
        Frame::Array(result)
    }
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// ZSCORE (read-only).
pub fn zscore_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 { return err_wrong_args("ZSCORE"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZSCORE") };
    let member = match extract_bytes(&args[1]) { Some(b) => b, None => return err_wrong_args("ZSCORE") };
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
    if args.len() != 1 { return err_wrong_args("ZCARD"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZCARD") };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => Frame::Integer(zref.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZRANK (read-only).
pub fn zrank_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 { return err_wrong_args("ZRANK"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZRANK") };
    let member = match extract_bytes(&args[1]) { Some(b) => b, None => return err_wrong_args("ZRANK") };
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
                    match entries.iter().position(|(m, s)| OrderedFloat(*s) == target_score && *m == target_member) {
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
    if args.len() != 2 { return err_wrong_args("ZREVRANK"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZREVRANK") };
    let member = match extract_bytes(&args[1]) { Some(b) => b, None => return err_wrong_args("ZREVRANK") };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match (&zref, zref.score(member)) {
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
                    match entries.iter().position(|(m, s)| OrderedFloat(*s) == target_score && *m == target_member) {
                        Some(rank) => Frame::Integer((entries.len() - 1 - rank) as i64),
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

/// ZRANGE (read-only).
pub fn zrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 { return err_wrong_args("ZRANGE"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZRANGE") };
    let min_arg = match extract_bytes(&args[1]) { Some(b) => b.clone(), None => return err_wrong_args("ZRANGE") };
    let max_arg = match extract_bytes(&args[2]) { Some(b) => b.clone(), None => return err_wrong_args("ZRANGE") };
    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) { Some(b) => b.as_ref(), None => { i += 1; continue; } };
        if opt.eq_ignore_ascii_case(b"BYSCORE") { by_score = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"BYLEX") { by_lex = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"REV") { rev = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"WITHSCORES") { withscores = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) { Some(b) => b, None => return err_wrong_args("ZRANGE") };
                let cnt_b = match extract_bytes(&args[i + 2]) { Some(b) => b, None => return err_wrong_args("ZRANGE") };
                limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
                if limit_offset.is_none() || limit_count.is_none() { return err("ERR value is not an integer or out of range"); }
                i += 3;
            } else { return err_wrong_args("ZRANGE"); }
        } else { i += 1; }
    }
    if by_score && by_lex { return err("ERR BYSCORE and BYLEX options are not compatible"); }
    if limit_offset.is_some() && !by_score && !by_lex { return err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"); }
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match (&zref, zref.members_map(), zref.bptree()) {
                (_, Some(members), Some(scores)) => {
                    if by_score { zrange_by_score(members, scores, &min_arg, &max_arg, rev, withscores, limit_offset, limit_count) }
                    else if by_lex { zrange_by_lex(scores, &min_arg, &max_arg, rev, withscores, members, limit_offset, limit_count) }
                    else { zrange_by_rank(scores, &min_arg, &max_arg, rev, withscores) }
                }
                _ => {
                    // Listpack fallback: convert to sorted entries and apply range logic
                    let entries = zref.entries_sorted();
                    zrange_from_entries(&entries, &min_arg, &max_arg, by_score, by_lex, rev, withscores, limit_offset, limit_count)
                }
            }
        }
        Ok(None) => Frame::Array(vec![]),
        Err(e) => e,
    }
}

/// ZREVRANGE (read-only).
pub fn zrevrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 { return err_wrong_args("ZREVRANGE"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZREVRANGE") };
    let start_arg = match extract_bytes(&args[1]) { Some(b) => b.clone(), None => return err_wrong_args("ZREVRANGE") };
    let stop_arg = match extract_bytes(&args[2]) { Some(b) => b.clone(), None => return err_wrong_args("ZREVRANGE") };
    let withscores = args.len() > 3
        && extract_bytes(&args[3]).map(|b| b.eq_ignore_ascii_case(b"WITHSCORES")).unwrap_or(false);
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match zref.bptree() {
                Some(scores) => zrange_by_rank(scores, &start_arg, &stop_arg, true, withscores),
                None => {
                    let entries = zref.entries_sorted();
                    zrange_from_entries(&entries, &start_arg, &stop_arg, false, false, true, withscores, None, None)
                }
            }
        }
        Ok(None) => Frame::Array(vec![]),
        Err(e) => e,
    }
}

/// ZRANGEBYSCORE (read-only).
pub fn zrangebyscore_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 { return err_wrong_args("ZRANGEBYSCORE"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZRANGEBYSCORE") };
    let min_arg = match extract_bytes(&args[1]) { Some(b) => b.clone(), None => return err_wrong_args("ZRANGEBYSCORE") };
    let max_arg = match extract_bytes(&args[2]) { Some(b) => b.clone(), None => return err_wrong_args("ZRANGEBYSCORE") };
    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) { Some(b) => b.as_ref(), None => { i += 1; continue; } };
        if opt.eq_ignore_ascii_case(b"WITHSCORES") { withscores = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) { Some(b) => b, None => return err_wrong_args("ZRANGEBYSCORE") };
                let cnt_b = match extract_bytes(&args[i + 2]) { Some(b) => b, None => return err_wrong_args("ZRANGEBYSCORE") };
                limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
                if limit_offset.is_none() || limit_count.is_none() { return err("ERR value is not an integer or out of range"); }
                i += 3;
            } else { return err_wrong_args("ZRANGEBYSCORE"); }
        } else { i += 1; }
    }
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match (zref.members_map(), zref.bptree()) {
                (Some(members), Some(scores)) => zrange_by_score(members, scores, &min_arg, &max_arg, false, withscores, limit_offset, limit_count),
                _ => {
                    let entries = zref.entries_sorted();
                    zrange_from_entries(&entries, &min_arg, &max_arg, true, false, false, withscores, limit_offset, limit_count)
                }
            }
        }
        Ok(None) => Frame::Array(vec![]),
        Err(e) => e,
    }
}

/// ZREVRANGEBYSCORE (read-only).
pub fn zrevrangebyscore_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 3 { return err_wrong_args("ZREVRANGEBYSCORE"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZREVRANGEBYSCORE") };
    let max_arg = match extract_bytes(&args[1]) { Some(b) => b.clone(), None => return err_wrong_args("ZREVRANGEBYSCORE") };
    let min_arg = match extract_bytes(&args[2]) { Some(b) => b.clone(), None => return err_wrong_args("ZREVRANGEBYSCORE") };
    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) { Some(b) => b.as_ref(), None => { i += 1; continue; } };
        if opt.eq_ignore_ascii_case(b"WITHSCORES") { withscores = true; i += 1; }
        else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) { Some(b) => b, None => return err_wrong_args("ZREVRANGEBYSCORE") };
                let cnt_b = match extract_bytes(&args[i + 2]) { Some(b) => b, None => return err_wrong_args("ZREVRANGEBYSCORE") };
                limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
                if limit_offset.is_none() || limit_count.is_none() { return err("ERR value is not an integer or out of range"); }
                i += 3;
            } else { return err_wrong_args("ZREVRANGEBYSCORE"); }
        } else { i += 1; }
    }
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match (zref.members_map(), zref.bptree()) {
                (Some(members), Some(scores)) => zrange_by_score(members, scores, &min_arg, &max_arg, true, withscores, limit_offset, limit_count),
                _ => {
                    let entries = zref.entries_sorted();
                    zrange_from_entries(&entries, &min_arg, &max_arg, true, false, true, withscores, limit_offset, limit_count)
                }
            }
        }
        Ok(None) => Frame::Array(vec![]),
        Err(e) => e,
    }
}

/// ZCOUNT (read-only).
pub fn zcount_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 { return err_wrong_args("ZCOUNT"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZCOUNT") };
    let min_bytes = match extract_bytes(&args[1]) { Some(b) => b, None => return err_wrong_args("ZCOUNT") };
    let max_bytes = match extract_bytes(&args[2]) { Some(b) => b, None => return err_wrong_args("ZCOUNT") };
    let min_bound = match parse_score_bound(min_bytes) { Ok(b) => b, Err(e) => return e };
    let max_bound = match parse_score_bound(max_bytes) { Ok(b) => b, Err(e) => return e };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match zref.bptree() {
                Some(scores) => {
                    let range_min = OrderedFloat(min_bound.value());
                    let range_max = OrderedFloat(max_bound.value());
                    let count = scores.range(range_min, range_max).filter(|(score, _)| min_bound.includes(score.0) && max_bound.includes_upper(score.0)).count();
                    Frame::Integer(count as i64)
                }
                None => {
                    let entries = zref.entries_sorted();
                    let count = entries.iter().filter(|(_, s)| min_bound.includes(*s) && max_bound.includes_upper(*s)).count();
                    Frame::Integer(count as i64)
                }
            }
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZLEXCOUNT (read-only).
pub fn zlexcount_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 { return err_wrong_args("ZLEXCOUNT"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZLEXCOUNT") };
    let min_bytes = match extract_bytes(&args[1]) { Some(b) => b, None => return err_wrong_args("ZLEXCOUNT") };
    let max_bytes = match extract_bytes(&args[2]) { Some(b) => b, None => return err_wrong_args("ZLEXCOUNT") };
    let min_bound = match parse_lex_bound(min_bytes) { Ok(b) => b, Err(e) => return e };
    let max_bound = match parse_lex_bound(max_bytes) { Ok(b) => b, Err(e) => return e };
    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => {
            match zref.bptree() {
                Some(scores) => {
                    let count = scores.iter().filter(|(_, member)| lex_in_range(member, &min_bound, &max_bound)).count();
                    Frame::Integer(count as i64)
                }
                None => {
                    let entries = zref.entries_sorted();
                    let count = entries.iter().filter(|(member, _)| lex_in_range(member, &min_bound, &max_bound)).count();
                    Frame::Integer(count as i64)
                }
            }
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// ZSCAN (read-only).
pub fn zscan_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 { return err_wrong_args("ZSCAN"); }
    let key = match extract_bytes(&args[0]) { Some(k) => k, None => return err_wrong_args("ZSCAN") };
    let cursor_bytes = match extract_bytes(&args[1]) { Some(b) => b, None => return err_wrong_args("ZSCAN") };
    let cursor: usize = match std::str::from_utf8(cursor_bytes).ok().and_then(|s| s.parse().ok()) {
        Some(c) => c,
        None => return err("ERR invalid cursor"),
    };
    let mut pattern: Option<&[u8]> = None;
    let mut scan_count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) { Some(b) => b.as_ref(), None => { i += 1; continue; } };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            if i + 1 < args.len() {
                pattern = extract_bytes(&args[i + 1]).map(|b| b.as_ref());
                i += 2;
            } else { return err_wrong_args("ZSCAN"); }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            if i + 1 < args.len() {
                let count_b = match extract_bytes(&args[i + 1]) { Some(b) => b, None => return err_wrong_args("ZSCAN") };
                scan_count = match std::str::from_utf8(count_b).ok().and_then(|s| s.parse().ok()) {
                    Some(c) => c,
                    None => return err("ERR value is not an integer or out of range"),
                };
                i += 2;
            } else { return err_wrong_args("ZSCAN"); }
        } else { i += 1; }
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
                let matches = match pattern { Some(p) => glob_match(p, member), None => true };
                if matches {
                    result_items.push(Frame::BulkString(member.clone()));
                    result_items.push(Frame::BulkString(Bytes::from(format_score(score))));
                    returned += 1;
                }
                pos += 1;
            }
            let next_cursor = if pos >= all_members.len() { Bytes::from_static(b"0") } else { Bytes::from(pos.to_string()) };
            Frame::Array(vec![Frame::BulkString(next_cursor), Frame::Array(result_items)])
        }
        Ok(None) => Frame::Array(vec![Frame::BulkString(Bytes::from_static(b"0")), Frame::Array(vec![])]),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn run_zadd(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zadd(db, &frames)
    }

    fn run_zrem(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrem(db, &frames)
    }

    fn run_zscore(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zscore(db, &frames)
    }

    fn run_zcard(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zcard(db, &frames)
    }

    fn run_zincrby(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zincrby(db, &frames)
    }

    fn run_zrank(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrank(db, &frames)
    }

    fn run_zrevrank(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrevrank(db, &frames)
    }

    fn run_zpopmin(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zpopmin(db, &frames)
    }

    fn run_zpopmax(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zpopmax(db, &frames)
    }

    fn run_zscan(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zscan(db, &frames)
    }

    fn run_zrange(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrange(db, &frames)
    }

    fn run_zrevrange(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrevrange(db, &frames)
    }

    fn run_zrangebyscore(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrangebyscore(db, &frames)
    }

    fn run_zrevrangebyscore(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrevrangebyscore(db, &frames)
    }

    fn run_zcount(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zcount(db, &frames)
    }

    fn run_zlexcount(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zlexcount(db, &frames)
    }

    fn run_zunionstore(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zunionstore(db, &frames)
    }

    fn run_zinterstore(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zinterstore(db, &frames)
    }

    fn setup_zset(db: &mut Database, key: &[u8], items: &[(&[u8], &[u8])]) {
        for (score, member) in items {
            run_zadd(db, &[key, score, member]);
        }
    }

    // ---- ZADD tests ----

    #[test]
    fn test_zadd_basic() {
        let mut db = Database::new();
        let result = run_zadd(&mut db, &[b"zs", b"1.0", b"a", b"2.0", b"b"]);
        assert_eq!(result, Frame::Integer(2));

        // Adding same member again should return 0 (not new)
        let result = run_zadd(&mut db, &[b"zs", b"3.0", b"a"]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_zadd_nx() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"zs", b"1.0", b"a"]);

        // NX: should not update existing
        let result = run_zadd(&mut db, &[b"zs", b"NX", b"5.0", b"a", b"2.0", b"b"]);
        assert_eq!(result, Frame::Integer(1)); // only b added

        // a should still have score 1.0
        let score = run_zscore(&mut db, &[b"zs", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("1")));
    }

    #[test]
    fn test_zadd_xx() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"zs", b"1.0", b"a"]);

        // XX: should only update existing, not add new
        let result = run_zadd(&mut db, &[b"zs", b"XX", b"5.0", b"a", b"2.0", b"b"]);
        assert_eq!(result, Frame::Integer(0)); // b not added (XX), a updated but not new

        let score = run_zscore(&mut db, &[b"zs", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("5")));
        let score = run_zscore(&mut db, &[b"zs", b"b"]);
        assert_eq!(score, Frame::Null);
    }

    #[test]
    fn test_zadd_gt() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"zs", b"5.0", b"a"]);

        // GT: only update if new score > old
        run_zadd(&mut db, &[b"zs", b"GT", b"3.0", b"a"]);
        let score = run_zscore(&mut db, &[b"zs", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("5"))); // not updated

        run_zadd(&mut db, &[b"zs", b"GT", b"10.0", b"a"]);
        let score = run_zscore(&mut db, &[b"zs", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("10"))); // updated
    }

    #[test]
    fn test_zadd_lt() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"zs", b"5.0", b"a"]);

        // LT: only update if new score < old
        run_zadd(&mut db, &[b"zs", b"LT", b"10.0", b"a"]);
        let score = run_zscore(&mut db, &[b"zs", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("5"))); // not updated

        run_zadd(&mut db, &[b"zs", b"LT", b"2.0", b"a"]);
        let score = run_zscore(&mut db, &[b"zs", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("2"))); // updated
    }

    #[test]
    fn test_zadd_ch_flag() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"zs", b"1.0", b"a"]);

        // CH: return changed count (added + updated), not just added
        let result = run_zadd(&mut db, &[b"zs", b"CH", b"5.0", b"a", b"2.0", b"b"]);
        assert_eq!(result, Frame::Integer(2)); // a changed + b added
    }

    #[test]
    fn test_zadd_nan_rejected() {
        let mut db = Database::new();
        let result = run_zadd(&mut db, &[b"zs", b"nan", b"a"]);
        match result {
            Frame::Error(_) => {}
            _ => panic!("Expected error for NaN"),
        }
    }

    #[test]
    fn test_zadd_nx_xx_conflict() {
        let mut db = Database::new();
        let result = run_zadd(&mut db, &[b"zs", b"NX", b"XX", b"1.0", b"a"]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"ERR XX and NX")),
            _ => panic!("Expected error"),
        }
    }

    // ---- ZREM tests ----

    #[test]
    fn test_zrem_basic() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zrem(&mut db, &[b"zs", b"a", b"c"]);
        assert_eq!(result, Frame::Integer(2));

        let card = run_zcard(&mut db, &[b"zs"]);
        assert_eq!(card, Frame::Integer(1));
    }

    #[test]
    fn test_zrem_nonexistent() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a")]);

        let result = run_zrem(&mut db, &[b"zs", b"x"]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_zrem_auto_delete_empty() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a")]);

        run_zrem(&mut db, &[b"zs", b"a"]);
        assert!(!db.exists(b"zs"));
    }

    // ---- ZSCORE tests ----

    #[test]
    fn test_zscore_existing() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"3.5", b"member")]);
        let result = run_zscore(&mut db, &[b"zs", b"member"]);
        assert_eq!(result, Frame::BulkString(Bytes::from("3.5")));
    }

    #[test]
    fn test_zscore_missing() {
        let mut db = Database::new();
        let result = run_zscore(&mut db, &[b"zs", b"x"]);
        assert_eq!(result, Frame::Null);
    }

    // ---- ZCARD tests ----

    #[test]
    fn test_zcard() {
        let mut db = Database::new();
        assert_eq!(run_zcard(&mut db, &[b"zs"]), Frame::Integer(0));

        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b")]);
        assert_eq!(run_zcard(&mut db, &[b"zs"]), Frame::Integer(2));
    }

    // ---- ZINCRBY tests ----

    #[test]
    fn test_zincrby() {
        let mut db = Database::new();
        // Creates member with score = increment when non-existent
        let result = run_zincrby(&mut db, &[b"zs", b"5", b"a"]);
        assert_eq!(result, Frame::BulkString(Bytes::from("5")));

        let result = run_zincrby(&mut db, &[b"zs", b"3", b"a"]);
        assert_eq!(result, Frame::BulkString(Bytes::from("8")));

        // Verify dual structure consistency
        let (members, scores) = db.get_sorted_set(b"zs").unwrap().unwrap();
        assert_eq!(members.len(), scores.len());
        assert_eq!(*members.get(&Bytes::from_static(b"a")).unwrap(), 8.0);
    }

    // ---- ZRANK / ZREVRANK tests ----

    #[test]
    fn test_zrank() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        assert_eq!(run_zrank(&mut db, &[b"zs", b"a"]), Frame::Integer(0));
        assert_eq!(run_zrank(&mut db, &[b"zs", b"b"]), Frame::Integer(1));
        assert_eq!(run_zrank(&mut db, &[b"zs", b"c"]), Frame::Integer(2));
        assert_eq!(run_zrank(&mut db, &[b"zs", b"x"]), Frame::Null);
    }

    #[test]
    fn test_zrevrank() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        assert_eq!(run_zrevrank(&mut db, &[b"zs", b"a"]), Frame::Integer(2));
        assert_eq!(run_zrevrank(&mut db, &[b"zs", b"b"]), Frame::Integer(1));
        assert_eq!(run_zrevrank(&mut db, &[b"zs", b"c"]), Frame::Integer(0));
    }

    // ---- ZPOPMIN / ZPOPMAX tests ----

    #[test]
    fn test_zpopmin() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zpopmin(&mut db, &[b"zs"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"a"));
                assert_eq!(arr[1], bulk(b"1"));
            }
            _ => panic!("Expected array"),
        }

        // Verify removal
        assert_eq!(run_zcard(&mut db, &[b"zs"]), Frame::Integer(2));
    }

    #[test]
    fn test_zpopmin_count() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zpopmin(&mut db, &[b"zs", b"2"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 4); // 2 pairs
                assert_eq!(arr[0], bulk(b"a"));
                assert_eq!(arr[2], bulk(b"b"));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zpopmax() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zpopmax(&mut db, &[b"zs"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"c"));
                assert_eq!(arr[1], bulk(b"3"));
            }
            _ => panic!("Expected array"),
        }
    }

    // ---- ZSCAN tests ----

    #[test]
    fn test_zscan_basic() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b")]);

        let result = run_zscan(&mut db, &[b"zs", b"0"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"0")); // cursor 0 = done
                match &arr[1] {
                    Frame::Array(items) => {
                        assert_eq!(items.len(), 4); // 2 members * 2 (member + score)
                    }
                    _ => panic!("Expected inner array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zscan_empty() {
        let mut db = Database::new();
        let result = run_zscan(&mut db, &[b"zs", b"0"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr[0], bulk(b"0"));
                match &arr[1] {
                    Frame::Array(items) => assert!(items.is_empty()),
                    _ => panic!("Expected inner array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    // ---- Dual structure consistency test ----

    #[test]
    fn test_dual_structure_consistency() {
        let mut db = Database::new();
        // Series of operations
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);
        run_zadd(&mut db, &[b"zs", b"5", b"a"]); // update
        run_zrem(&mut db, &[b"zs", b"b"]);
        run_zincrby(&mut db, &[b"zs", b"10", b"d"]);

        let (members, scores) = db.get_sorted_set(b"zs").unwrap().unwrap();
        assert_eq!(members.len(), scores.len());

        // Verify all members in HashMap have matching entries in BPTree
        for (member, score) in members.iter() {
            assert!(scores.contains(OrderedFloat(*score), member));
        }
    }

    // ---- WRONGTYPE test ----

    #[test]
    fn test_zadd_wrongtype() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));

        let result = run_zadd(&mut db, &[b"str", b"1.0", b"a"]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            _ => panic!("Expected WRONGTYPE error"),
        }
    }

    // ---- ZRANGE tests ----

    #[test]
    fn test_zrange_by_rank() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zrange(&mut db, &[b"zs", b"0", b"1"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"a"));
                assert_eq!(arr[1], bulk(b"b"));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrange_by_rank_negative() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zrange(&mut db, &[b"zs", b"0", b"-1"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // all elements
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrange_withscores() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b")]);

        let result = run_zrange(&mut db, &[b"zs", b"0", b"-1", b"WITHSCORES"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 4); // 2 * (member + score)
                assert_eq!(arr[0], bulk(b"a"));
                assert_eq!(arr[1], bulk(b"1"));
                assert_eq!(arr[2], bulk(b"b"));
                assert_eq!(arr[3], bulk(b"2"));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrange_byscore() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c"), (b"4", b"d")]);

        let result = run_zrange(&mut db, &[b"zs", b"2", b"3", b"BYSCORE"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"b"));
                assert_eq!(arr[1], bulk(b"c"));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrange_byscore_inf() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zrange(&mut db, &[b"zs", b"-inf", b"+inf", b"BYSCORE"]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrange_byscore_exclusive() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zrange(&mut db, &[b"zs", b"(1", b"3", b"BYSCORE"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"b"));
                assert_eq!(arr[1], bulk(b"c"));
            }
            _ => panic!("Expected array"),
        }
    }

    // ---- ZREVRANGE tests ----

    #[test]
    fn test_zrevrange() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zrevrange(&mut db, &[b"zs", b"0", b"1"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"c"));
                assert_eq!(arr[1], bulk(b"b"));
            }
            _ => panic!("Expected array"),
        }
    }

    // ---- ZRANGEBYSCORE tests ----

    #[test]
    fn test_zrangebyscore_basic() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        let result = run_zrangebyscore(&mut db, &[b"zs", b"-inf", b"+inf"]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrangebyscore_exclusive() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        // (1 means exclusive of 1
        let result = run_zrangebyscore(&mut db, &[b"zs", b"(1", b"(3"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], bulk(b"b"));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrangebyscore_withscores() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b")]);

        let result = run_zrangebyscore(&mut db, &[b"zs", b"-inf", b"+inf", b"WITHSCORES"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 4);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_zrangebyscore_limit() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c"), (b"4", b"d")]);

        let result = run_zrangebyscore(&mut db, &[b"zs", b"-inf", b"+inf", b"LIMIT", b"1", b"2"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], bulk(b"b"));
                assert_eq!(arr[1], bulk(b"c"));
            }
            _ => panic!("Expected array"),
        }
    }

    // ---- ZREVRANGEBYSCORE tests ----

    #[test]
    fn test_zrevrangebyscore() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        // Note: args are max then min
        let result = run_zrevrangebyscore(&mut db, &[b"zs", b"+inf", b"-inf"]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], bulk(b"c"));
                assert_eq!(arr[1], bulk(b"b"));
                assert_eq!(arr[2], bulk(b"a"));
            }
            _ => panic!("Expected array"),
        }
    }

    // ---- ZCOUNT tests ----

    #[test]
    fn test_zcount() {
        let mut db = Database::new();
        setup_zset(&mut db, b"zs", &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c")]);

        assert_eq!(run_zcount(&mut db, &[b"zs", b"-inf", b"+inf"]), Frame::Integer(3));
        assert_eq!(run_zcount(&mut db, &[b"zs", b"1", b"2"]), Frame::Integer(2));
        assert_eq!(run_zcount(&mut db, &[b"zs", b"(1", b"3"]), Frame::Integer(2));
        assert_eq!(run_zcount(&mut db, &[b"zs", b"(1", b"(3"]), Frame::Integer(1));
    }

    #[test]
    fn test_zcount_empty() {
        let mut db = Database::new();
        assert_eq!(run_zcount(&mut db, &[b"zs", b"-inf", b"+inf"]), Frame::Integer(0));
    }

    // ---- ZLEXCOUNT tests ----

    #[test]
    fn test_zlexcount() {
        let mut db = Database::new();
        // All same score for lex operations
        setup_zset(&mut db, b"zs", &[(b"0", b"a"), (b"0", b"b"), (b"0", b"c"), (b"0", b"d")]);

        assert_eq!(run_zlexcount(&mut db, &[b"zs", b"-", b"+"]), Frame::Integer(4));
        assert_eq!(run_zlexcount(&mut db, &[b"zs", b"[b", b"[c"]), Frame::Integer(2));
        assert_eq!(run_zlexcount(&mut db, &[b"zs", b"(a", b"[c"]), Frame::Integer(2));
    }

    // ---- ZUNIONSTORE tests ----

    #[test]
    fn test_zunionstore_basic() {
        let mut db = Database::new();
        setup_zset(&mut db, b"z1", &[(b"1", b"a"), (b"2", b"b")]);
        setup_zset(&mut db, b"z2", &[(b"3", b"b"), (b"4", b"c")]);

        let result = run_zunionstore(&mut db, &[b"out", b"2", b"z1", b"z2"]);
        assert_eq!(result, Frame::Integer(3));

        // b should be 2+3=5
        let score = run_zscore(&mut db, &[b"out", b"b"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("5")));
    }

    #[test]
    fn test_zunionstore_weights() {
        let mut db = Database::new();
        setup_zset(&mut db, b"z1", &[(b"1", b"a")]);
        setup_zset(&mut db, b"z2", &[(b"2", b"a")]);

        run_zunionstore(&mut db, &[b"out", b"2", b"z1", b"z2", b"WEIGHTS", b"2", b"3"]);
        // a = 1*2 + 2*3 = 8
        let score = run_zscore(&mut db, &[b"out", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("8")));
    }

    #[test]
    fn test_zunionstore_aggregate_min() {
        let mut db = Database::new();
        setup_zset(&mut db, b"z1", &[(b"1", b"a")]);
        setup_zset(&mut db, b"z2", &[(b"2", b"a")]);

        run_zunionstore(&mut db, &[b"out", b"2", b"z1", b"z2", b"AGGREGATE", b"MIN"]);
        let score = run_zscore(&mut db, &[b"out", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("1")));
    }

    #[test]
    fn test_zunionstore_aggregate_max() {
        let mut db = Database::new();
        setup_zset(&mut db, b"z1", &[(b"1", b"a")]);
        setup_zset(&mut db, b"z2", &[(b"2", b"a")]);

        run_zunionstore(&mut db, &[b"out", b"2", b"z1", b"z2", b"AGGREGATE", b"MAX"]);
        let score = run_zscore(&mut db, &[b"out", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("2")));
    }

    // ---- ZINTERSTORE tests ----

    #[test]
    fn test_zinterstore_basic() {
        let mut db = Database::new();
        setup_zset(&mut db, b"z1", &[(b"1", b"a"), (b"2", b"b")]);
        setup_zset(&mut db, b"z2", &[(b"3", b"b"), (b"4", b"c")]);

        let result = run_zinterstore(&mut db, &[b"out", b"2", b"z1", b"z2"]);
        assert_eq!(result, Frame::Integer(1)); // only b in both

        let score = run_zscore(&mut db, &[b"out", b"b"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("5"))); // 2+3
    }

    #[test]
    fn test_zinterstore_weights() {
        let mut db = Database::new();
        setup_zset(&mut db, b"z1", &[(b"1", b"a")]);
        setup_zset(&mut db, b"z2", &[(b"2", b"a")]);

        run_zinterstore(&mut db, &[b"out", b"2", b"z1", b"z2", b"WEIGHTS", b"2", b"3"]);
        // a = 1*2 + 2*3 = 8
        let score = run_zscore(&mut db, &[b"out", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("8")));
    }
}
