mod sorted_set_read;
mod sorted_set_write;

pub use sorted_set_read::*;
pub use sorted_set_write::*;

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::HashMap;

use crate::storage::bptree::BPTree;

use crate::framevec;
use crate::protocol::Frame;

use super::helpers::err;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Format a float score for Redis output (strip trailing zeros, but keep at least one decimal).
pub(super) fn format_score(score: f64) -> String {
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
pub(super) fn zadd_member(
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
pub(super) fn zrem_member(
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
pub(super) enum ScoreBound {
    Inclusive(f64),
    Exclusive(f64),
    NegInf,
    PosInf,
}

impl ScoreBound {
    pub(super) fn value(&self) -> f64 {
        match self {
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => *v,
            ScoreBound::NegInf => f64::NEG_INFINITY,
            ScoreBound::PosInf => f64::INFINITY,
        }
    }

    pub(super) fn includes(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(v) => score >= *v || (score - *v).abs() < f64::EPSILON,
            ScoreBound::Exclusive(v) => score > *v,
        }
    }

    pub(super) fn includes_upper(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(v) => score <= *v || (score - *v).abs() < f64::EPSILON,
            ScoreBound::Exclusive(v) => score < *v,
        }
    }
}

pub(super) fn parse_score_bound(s: &[u8]) -> Result<ScoreBound, Frame> {
    let s_str = std::str::from_utf8(s).map_err(|_| err("ERR min or max is not a float"))?;
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
pub(super) enum LexBound {
    Inclusive(Bytes),
    Exclusive(Bytes),
    NegInf,
    PosInf,
}

pub(super) fn parse_lex_bound(s: &[u8]) -> Result<LexBound, Frame> {
    if s == b"-" {
        return Ok(LexBound::NegInf);
    }
    if s == b"+" {
        return Ok(LexBound::PosInf);
    }
    if s.is_empty() {
        return Err(err("ERR min or max not valid string range item"));
    }
    match s[0] {
        b'[' => Ok(LexBound::Inclusive(Bytes::copy_from_slice(&s[1..]))),
        b'(' => Ok(LexBound::Exclusive(Bytes::copy_from_slice(&s[1..]))),
        _ => Err(err("ERR min or max not valid string range item")),
    }
}

pub(super) fn lex_in_range(member: &[u8], min: &LexBound, max: &LexBound) -> bool {
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
// Shared range helpers
// ---------------------------------------------------------------------------

pub(super) fn zrange_by_rank(
    scores: &BPTree,
    min_arg: &[u8],
    max_arg: &[u8],
    rev: bool,
    withscores: bool,
) -> Frame {
    let total = scores.len() as i64;
    if total == 0 {
        return Frame::Array(framevec![]);
    }

    let start_raw: i64 = match std::str::from_utf8(min_arg)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return err("ERR value is not an integer or out of range"),
    };
    let stop_raw: i64 = match std::str::from_utf8(max_arg)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(v) => v,
        None => return err("ERR value is not an integer or out of range"),
    };

    // Normalize negative indices
    let start = if start_raw < 0 {
        (total + start_raw).max(0)
    } else {
        start_raw.min(total)
    };
    let stop = if stop_raw < 0 {
        (total + stop_raw).max(0)
    } else {
        stop_raw.min(total - 1)
    };

    if start > stop {
        return Frame::Array(framevec![]);
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

    Frame::Array(result.into())
}

pub(super) fn zrange_by_score(
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
        entries
            .into_iter()
            .skip(offset)
            .take(count as usize)
            .collect()
    };

    let mut result = Vec::new();
    for (score, member) in limited {
        result.push(Frame::BulkString(member.clone()));
        if withscores {
            result.push(Frame::BulkString(Bytes::from(format_score(score))));
        }
    }

    Frame::Array(result.into())
}

pub(super) fn zrange_by_lex(
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
        entries
            .into_iter()
            .skip(offset)
            .take(count as usize)
            .collect()
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

    Frame::Array(result.into())
}

/// Simple glob pattern matcher (same approach as key.rs).
pub(super) fn glob_match(pattern: &[u8], input: &[u8]) -> bool {
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

/// Fallback range helper for listpack-encoded sorted sets.
/// Operates on pre-sorted `Vec<(Bytes, f64)>` entries.
#[allow(clippy::too_many_arguments)]
pub(super) fn zrange_from_entries(
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
        return Frame::Array(framevec![]);
    }

    if by_score {
        let min_bound = match parse_score_bound(min_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let max_bound = match parse_score_bound(max_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let mut filtered: Vec<&(Bytes, f64)> = entries
            .iter()
            .filter(|(_, s)| min_bound.includes(*s) && max_bound.includes_upper(*s))
            .collect();
        if rev {
            filtered.reverse();
        }
        let offset = limit_offset.unwrap_or(0).max(0) as usize;
        let count = limit_count
            .map(|c| if c < 0 { filtered.len() } else { c as usize })
            .unwrap_or(filtered.len());
        let result: Vec<Frame> = filtered
            .into_iter()
            .skip(offset)
            .take(count)
            .flat_map(|(member, score)| {
                let mut v = vec![Frame::BulkString(member.clone())];
                if withscores {
                    v.push(Frame::BulkString(Bytes::from(format_score(*score))));
                }
                v
            })
            .collect();
        Frame::Array(result.into())
    } else if by_lex {
        let min_bound = match parse_lex_bound(min_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let max_bound = match parse_lex_bound(max_arg) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let mut filtered: Vec<&(Bytes, f64)> = entries
            .iter()
            .filter(|(member, _)| lex_in_range(member, &min_bound, &max_bound))
            .collect();
        if rev {
            filtered.reverse();
        }
        let offset = limit_offset.unwrap_or(0).max(0) as usize;
        let count = limit_count
            .map(|c| if c < 0 { filtered.len() } else { c as usize })
            .unwrap_or(filtered.len());
        let result: Vec<Frame> = filtered
            .into_iter()
            .skip(offset)
            .take(count)
            .flat_map(|(member, score)| {
                let mut v = vec![Frame::BulkString(member.clone())];
                if withscores {
                    v.push(Frame::BulkString(Bytes::from(format_score(*score))));
                }
                v
            })
            .collect();
        Frame::Array(result.into())
    } else {
        // By rank
        let start_raw: i64 = match std::str::from_utf8(min_arg)
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) => v,
            None => return err("ERR value is not an integer or out of range"),
        };
        let stop_raw: i64 = match std::str::from_utf8(max_arg)
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) => v,
            None => return err("ERR value is not an integer or out of range"),
        };
        let start = if start_raw < 0 {
            (total + start_raw).max(0) as usize
        } else {
            start_raw as usize
        };
        let stop = if stop_raw < 0 {
            (total + stop_raw).max(0) as usize
        } else {
            (stop_raw as usize).min(entries.len().saturating_sub(1))
        };
        if start > stop || start >= entries.len() {
            return Frame::Array(framevec![]);
        }
        let slice: Vec<&(Bytes, f64)> = if rev {
            entries[start..=stop].iter().rev().collect()
        } else {
            entries[start..=stop].iter().collect()
        };
        let result: Vec<Frame> = slice
            .into_iter()
            .flat_map(|(member, score)| {
                let mut v = vec![Frame::BulkString(member.clone())];
                if withscores {
                    v.push(Frame::BulkString(Bytes::from(format_score(*score))));
                }
                v
            })
            .collect();
        Frame::Array(result.into())
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
        setup_zset(
            &mut db,
            b"zs",
            &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c"), (b"4", b"d")],
        );

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
        setup_zset(
            &mut db,
            b"zs",
            &[(b"1", b"a"), (b"2", b"b"), (b"3", b"c"), (b"4", b"d")],
        );

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

        assert_eq!(
            run_zcount(&mut db, &[b"zs", b"-inf", b"+inf"]),
            Frame::Integer(3)
        );
        assert_eq!(run_zcount(&mut db, &[b"zs", b"1", b"2"]), Frame::Integer(2));
        assert_eq!(
            run_zcount(&mut db, &[b"zs", b"(1", b"3"]),
            Frame::Integer(2)
        );
        assert_eq!(
            run_zcount(&mut db, &[b"zs", b"(1", b"(3"]),
            Frame::Integer(1)
        );
    }

    #[test]
    fn test_zcount_empty() {
        let mut db = Database::new();
        assert_eq!(
            run_zcount(&mut db, &[b"zs", b"-inf", b"+inf"]),
            Frame::Integer(0)
        );
    }

    // ---- ZLEXCOUNT tests ----

    #[test]
    fn test_zlexcount() {
        let mut db = Database::new();
        // All same score for lex operations
        setup_zset(
            &mut db,
            b"zs",
            &[(b"0", b"a"), (b"0", b"b"), (b"0", b"c"), (b"0", b"d")],
        );

        assert_eq!(
            run_zlexcount(&mut db, &[b"zs", b"-", b"+"]),
            Frame::Integer(4)
        );
        assert_eq!(
            run_zlexcount(&mut db, &[b"zs", b"[b", b"[c"]),
            Frame::Integer(2)
        );
        assert_eq!(
            run_zlexcount(&mut db, &[b"zs", b"(a", b"[c"]),
            Frame::Integer(2)
        );
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

        run_zunionstore(
            &mut db,
            &[b"out", b"2", b"z1", b"z2", b"WEIGHTS", b"2", b"3"],
        );
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

        run_zinterstore(
            &mut db,
            &[b"out", b"2", b"z1", b"z2", b"WEIGHTS", b"2", b"3"],
        );
        // a = 1*2 + 2*3 = 8
        let score = run_zscore(&mut db, &[b"out", b"a"]);
        assert_eq!(score, Frame::BulkString(Bytes::from("8")));
    }
}
