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

/// Zero-alloc version of `format_score` — returns `Bytes` directly.
pub(crate) fn format_score_bytes(score: f64) -> Bytes {
    if score == f64::INFINITY {
        Bytes::from_static(b"inf")
    } else if score == f64::NEG_INFINITY {
        Bytes::from_static(b"-inf")
    } else {
        use std::fmt::Write;
        let mut buf = String::with_capacity(24);
        let _ = write!(buf, "{}", score);
        Bytes::from(buf)
    }
}

/// Aggregate operation for ZUNION/ZINTER/ZUNIONSTORE/ZINTERSTORE.
#[derive(Debug, Clone, Copy)]
pub(super) enum AggregateOp {
    Sum,
    Min,
    Max,
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
    // All callers pass (min, max) in semantic order regardless of rev.
    // The rev flag only affects iteration direction (entries.reverse below).
    let min_bound = match parse_score_bound(min_arg) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let max_bound = match parse_score_bound(max_arg) {
        Ok(b) => b,
        Err(e) => return e,
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
    let min_bound = match parse_lex_bound(min_arg) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let max_bound = match parse_lex_bound(max_arg) {
        Ok(b) => b,
        Err(e) => return e,
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

    #[test]
    fn test_zmpop_huge_count_no_abort() {
        // A huge COUNT must not drive an unbounded Vec::with_capacity ->
        // allocator abort. This site was MISSED by the audit finders and
        // caught by the Batch A allocation sweep (unlike LMPOP it lacked a
        // .min(card) cap).
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2", b"b"]);
        match zmpop(
            &mut db,
            &[
                bulk(b"1"),
                bulk(b"z"),
                bulk(b"MIN"),
                bulk(b"COUNT"),
                bulk(b"5000000000"),
            ],
        ) {
            Frame::Array(items) => {
                assert_eq!(items.len(), 2, "ZMPOP returns [key, popped]");
                match &items[1] {
                    Frame::Array(popped) => {
                        assert_eq!(popped.len(), 2, "only 2 members exist to pop")
                    }
                    other => panic!("expected popped array, got {other:?}"),
                }
            }
            other => panic!("expected array, got {other:?}"),
        }
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
        db.set_string(b"str", Bytes::from_static(b"val"));

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

    // -----------------------------------------------------------------
    // WS6 — container-growth memory accounting (src/storage/db.rs).
    // -----------------------------------------------------------------

    #[test]
    fn test_estimated_memory_rises_with_zadd_growth() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"member-000"]);
        let one = db.estimated_memory();
        for i in 1..50 {
            let m = format!("member-{i:03}");
            run_zadd(&mut db, &[b"z", b"1", m.as_bytes()]);
        }
        let many = db.estimated_memory();
        assert!(
            many > one,
            "estimated_memory must rise as members are added: one={one} many={many}"
        );
        assert!(
            many - one > 49 * 10,
            "growth must be at least proportional to the added member payload"
        );
    }

    #[test]
    fn test_estimated_memory_zincrby_score_only_update_is_free() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"member-000"]);
        // moon#897: ZINCRBY no longer promotes a small zset, so this test
        // has to cross the entry threshold on purpose to reach the B+tree
        // cost model it is about. (Before #897 the single ZINCRBY below did
        // the promoting, and that is precisely the defect.)
        for i in 1..=crate::storage::db::EncodingLimits::moon_defaults().zset_entries {
            let m = format!("filler-{i:04}");
            run_zadd(&mut db, &[b"z", b"1", m.as_bytes()]);
        }
        run_zincrby(&mut db, &[b"z", b"5", b"member-000"]);
        assert_eq!(encoding_of(&mut db, b"z"), "skiplist");
        let after_promote = db.estimated_memory();
        // ZINCRBY on an EXISTING member only changes the score -- the
        // SortedSetBPTree cost model doesn't factor in score values, so
        // this must be a zero-byte delta (no phantom growth per call).
        run_zincrby(&mut db, &[b"z", b"5", b"member-000"]);
        let after_incr = db.estimated_memory();
        assert_eq!(
            after_promote, after_incr,
            "ZINCRBY on an existing member must not change estimated_memory"
        );
    }

    #[test]
    fn test_estimated_memory_falls_with_zrem() {
        // moon#897: ZREM keeps a small zset in its listpack, so the ledger
        // claim is now made TWICE — once on each encoding. The listpack leg
        // is the one #897 created; the B+tree leg is the original test, with
        // the promotion made deliberate instead of riding on the first ZREM.
        for over_threshold in [false, true] {
            let mut db = Database::new();
            let n: usize = if over_threshold {
                crate::storage::db::EncodingLimits::moon_defaults().zset_entries + 20
            } else {
                50
            };
            for i in 0..n {
                let m = format!("member-{i:03}");
                run_zadd(&mut db, &[b"z", b"1", m.as_bytes()]);
            }
            run_zrem(&mut db, &[b"z", b"member-000"]);
            assert_eq!(
                encoding_of(&mut db, b"z"),
                if over_threshold {
                    "skiplist"
                } else {
                    "listpack"
                },
                "n={n}: ZREM must not change the encoding either way"
            );
            let grown = db.estimated_memory();
            for i in 1..n - 1 {
                let m = format!("member-{i:03}");
                run_zrem(&mut db, &[b"z", m.as_bytes()]);
            }
            let drained = db.estimated_memory();
            if over_threshold {
                assert!(
                    drained < grown,
                    "n={n}: estimated_memory must fall as members are removed: \
                     grown={grown} drained={drained}"
                );
            } else {
                // A listpack's cost is its ONE heap buffer, billed from
                // `Vec::capacity` (moon#788), and draining entries out of a
                // `Vec` does not return capacity — so the ledger is right to
                // keep reporting it while the key lives, and it must not
                // GROW. Same shape as HDEL's listpack arm in
                // `db::hash_delete_field`, which credits the same
                // before/after difference. The bytes come back at
                // `db.remove` below, which is the assertion that matters.
                assert!(
                    drained <= grown,
                    "n={n}: draining a listpack must never charge more: \
                     grown={grown} drained={drained}"
                );
            }

            let last = format!("member-{:03}", n - 1);
            run_zrem(&mut db, &[b"z", last.as_bytes()]);
            assert_eq!(
                db.estimated_memory(),
                0,
                "n={n}: estimated_memory must return to zero once the sorted \
                 set is fully drained"
            );
        }
    }

    #[test]
    fn test_estimated_memory_zpopmin_credits_removed_member() {
        let mut db = Database::new();
        for i in 0..10 {
            let m = format!("member-{i:03}");
            run_zadd(&mut db, &[b"z", b"1", m.as_bytes()]);
        }
        let args = [bulk(b"z")];
        // moon#787 / moon#832: the first ZPOPMIN promotes the listpack to the
        // B+tree form and is charged for it; snapshot after that swing.
        zpopmin(&mut db, &args);
        assert_eq!(encoding_of(&mut db, b"z"), "skiplist");
        let grown = db.estimated_memory();
        zpopmin(&mut db, &args);
        let after = db.estimated_memory();
        assert!(
            after < grown,
            "ZPOPMIN must credit the removed member: grown={grown} after={after}"
        );
    }

    // --- ZRANK/ZREVRANK WITHSCORE (Redis 7.2, moon#521) -------------------
    //
    // Oracle, redis-server 8.6.1:
    //
    //   ZADD z 1 m
    //   ZRANK z m WITHSCORE      -> 1) (integer) 0   2) "1"
    //   ZRANK absent-key m WITHSCORE   -> *-1   (null ARRAY, not $-1)
    //   ZREVRANK absent-key m WITHSCORE -> *-1
    //   ZRANK z m                -> :0
    //   ZRANK z nosuch           -> $-1  (still the null BULK without the option)
    //
    // The option changes the null TYPE as well as the hit type, and the token
    // is singular — `WITHSCORE`, not the `WITHSCORES` that ZRANGE takes.

    /// Both entry points are exercised: `zrank` (the &mut Database path taken
    /// by the write dispatcher) and `zrank_readonly` (the shared-read path).
    /// The bug was an arity check, and there is one in each — fixing only the
    /// one a unit test happens to call leaves the other answering the error on
    /// whichever routing path reaches it.
    fn both_zrank(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        let via_mut = zrank(db, &frames);
        let via_ro = zrank_readonly(db, &frames, 0);
        assert_eq!(
            via_mut, via_ro,
            "zrank and zrank_readonly must agree for {args:?}"
        );
        via_mut
    }

    fn both_zrevrank(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        let via_mut = zrevrank(db, &frames);
        let via_ro = zrevrank_readonly(db, &frames, 0);
        assert_eq!(
            via_mut, via_ro,
            "zrevrank and zrevrank_readonly must agree for {args:?}"
        );
        via_mut
    }

    #[test]
    fn test_zrank_withscore_hit_is_rank_and_score() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"m", b"2", b"n"]);
        assert_eq!(
            both_zrank(&mut db, &[b"z", b"m", b"WITHSCORE"]),
            Frame::Array(framevec![Frame::Integer(0), Frame::Double(1.0)])
        );
        assert_eq!(
            both_zrank(&mut db, &[b"z", b"n", b"WITHSCORE"]),
            Frame::Array(framevec![Frame::Integer(1), Frame::Double(2.0)])
        );
        // Lowercase and mixed case are the same option.
        assert_eq!(
            both_zrank(&mut db, &[b"z", b"m", b"withscore"]),
            Frame::Array(framevec![Frame::Integer(0), Frame::Double(1.0)])
        );
    }

    #[test]
    fn test_zrevrank_withscore_hit_is_rank_and_score() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"m", b"2", b"n"]);
        assert_eq!(
            both_zrevrank(&mut db, &[b"z", b"n", b"WITHSCORE"]),
            Frame::Array(framevec![Frame::Integer(0), Frame::Double(2.0)])
        );
        assert_eq!(
            both_zrevrank(&mut db, &[b"z", b"m", b"WITHSCORE"]),
            Frame::Array(framevec![Frame::Integer(1), Frame::Double(1.0)])
        );
    }

    #[test]
    fn test_zrank_withscore_miss_is_null_array() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"m"]);
        assert_eq!(
            both_zrank(&mut db, &[b"nosuchkey", b"m", b"WITHSCORE"]),
            Frame::NullArray,
            "absent KEY with WITHSCORE is the null array"
        );
        assert_eq!(
            both_zrank(&mut db, &[b"z", b"nosuchmember", b"WITHSCORE"]),
            Frame::NullArray,
            "absent MEMBER with WITHSCORE is the null array"
        );
        assert_eq!(
            both_zrevrank(&mut db, &[b"nosuchkey", b"m", b"WITHSCORE"]),
            Frame::NullArray
        );
        assert_eq!(
            both_zrevrank(&mut db, &[b"z", b"nosuchmember", b"WITHSCORE"]),
            Frame::NullArray
        );
    }

    /// The fence: WITHOUT the option nothing moves — the hit is a bare Integer
    /// and the miss is the null BULK. Without this half, "make every ZRANK
    /// miss `*-1`" would pass the test above while breaking every existing
    /// client.
    #[test]
    fn test_zrank_without_withscore_is_unchanged() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"m"]);
        assert_eq!(both_zrank(&mut db, &[b"z", b"m"]), Frame::Integer(0));
        assert_eq!(both_zrank(&mut db, &[b"z", b"nosuch"]), Frame::Null);
        assert_eq!(both_zrank(&mut db, &[b"nosuchkey", b"m"]), Frame::Null);
        assert_eq!(both_zrevrank(&mut db, &[b"z", b"m"]), Frame::Integer(0));
        assert_eq!(both_zrevrank(&mut db, &[b"z", b"nosuch"]), Frame::Null);
        assert_eq!(both_zrevrank(&mut db, &[b"nosuchkey", b"m"]), Frame::Null);
    }

    /// A bad third token is a SYNTAX error, and only a FOURTH argument is an
    /// arity error — Redis distinguishes the two and so must Moon. In
    /// particular the PLURAL `WITHSCORES` (which ZRANGE takes) is not a
    /// synonym here.
    #[test]
    fn test_zrank_rejects_bad_option_as_syntax_error() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"m"]);
        for bad in [b"WITHSCORES".as_ref(), b"NOPE".as_ref()] {
            match both_zrank(&mut db, &[b"z", b"m", bad]) {
                Frame::Error(e) => assert_eq!(
                    e,
                    Bytes::from_static(b"ERR syntax error"),
                    "bad option must be a syntax error, not an arity error"
                ),
                other => panic!("expected syntax error, got {other:?}"),
            }
        }
        match both_zrank(&mut db, &[b"z", b"m", b"WITHSCORE", b"extra"]) {
            Frame::Error(e) => assert!(
                String::from_utf8_lossy(&e).contains("wrong number of arguments"),
                "a fourth argument is an ARITY error"
            ),
            other => panic!("expected arity error, got {other:?}"),
        }
        match both_zrevrank(&mut db, &[b"z", b"m", b"WITHSCORES"]) {
            Frame::Error(e) => assert_eq!(e, Bytes::from_static(b"ERR syntax error")),
            other => panic!("expected syntax error, got {other:?}"),
        }
    }

    // ── #787: ZADD must reach the listpack encoding ───────────────────────
    //
    // Redis keeps a sorted set in a listpack until it exceeds
    // zset-max-listpack-entries (128) or zset-max-listpack-value (64); moon's
    // `SortedSetListpack` variant is wired end to end EXCEPT that nothing ever
    // created one, so every zset was a `skiplist` from its first member.
    // Verified against a redis 8.6.1 oracle:
    //
    //     127.0.0.1:7799> zadd z 1 a 2 b 3 c
    //     (integer) 3
    //     127.0.0.1:7799> object encoding z
    //     "listpack"
    //
    // Probe choice (moon#832): `OBJECT ENCODING` reads `entry.value` through
    // `Database::get`, which does not route through `get_promoted`, so asking
    // about the encoding cannot itself flatten it.

    /// What `OBJECT ENCODING <key>` actually replies — asserted through the
    /// real command handler, not a private field, so the test checks the
    /// user-visible answer that diverged from Redis.
    fn encoding_of(db: &mut Database, key: &[u8]) -> String {
        match crate::command::key::object(db, &[bulk(b"ENCODING"), bulk(key)]) {
            Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
            other => panic!("OBJECT ENCODING did not reply a bulk string: {other:?}"),
        }
    }

    /// ZSCORE through the shared-read dispatch twin — the one that reaches a
    /// zset through `get_sorted_set_ref_if_alive`, which classifies every
    /// encoding instead of forcing the owned one.
    ///
    /// The mutable twin `zscore` goes through `get_sorted_set`, whose
    /// `OwnedKind::upgrade` promotes a listpack to the B+tree form on the
    /// FIRST call — that is the moon#832 ceiling, not a blessed behaviour,
    /// and it is why these tests read through the twin that keeps the
    /// encoding. `now_ms = 0` is safe: none of these keys carry a TTL.
    fn ro_zscore(db: &Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zscore_readonly(db, &frames, 0)
    }

    #[test]
    fn zadd_small_zset_stays_listpack() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "a 3-member zset is far below zset-max-listpack-entries (128); \
             redis 8.6.1 reports `listpack` here"
        );
    }

    #[test]
    fn zadd_promotes_past_the_entry_threshold() {
        let mut db = Database::new();
        for i in 0..crate::storage::db::EncodingLimits::moon_defaults().zset_entries {
            let m = format!("m{i:04}");
            run_zadd(&mut db, &[b"z", b"1", m.as_bytes()]);
        }
        // Exactly at the threshold it is still a listpack (Redis: 128 is
        // inclusive) — the half of the boundary the pre-fix binary gets
        // wrong, so the test cannot pass vacuously.
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "exactly zset-max-listpack-entries members must still be a listpack"
        );
        run_zadd(&mut db, &[b"z", b"1", b"one-more"]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "skiplist",
            "past zset-max-listpack-entries the zset must promote to a skiplist"
        );
        assert_eq!(
            run_zcard(&mut db, &[b"z"]),
            Frame::Integer(
                crate::storage::db::EncodingLimits::moon_defaults().zset_entries as i64 + 1
            ),
            "the promotion must not lose a member"
        );
        assert_eq!(
            run_zscore(&mut db, &[b"z", b"m0000"]),
            Frame::BulkString(Bytes::from_static(b"1"))
        );
    }

    #[test]
    fn zadd_promotes_on_an_oversized_member() {
        let mut db = Database::new();
        let at_limit = vec![b'y'; crate::storage::db::EncodingLimits::moon_defaults().zset_value];
        run_zadd(&mut db, &[b"z", b"1", b"small", b"2", &at_limit]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "a member of exactly zset-max-listpack-value bytes fits a listpack"
        );
        let big = vec![b'x'; crate::storage::db::EncodingLimits::moon_defaults().zset_value + 1];
        run_zadd(&mut db, &[b"z", b"3", &big]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "skiplist",
            "a member longer than zset-max-listpack-value must promote"
        );
        assert_eq!(run_zcard(&mut db, &[b"z"]), Frame::Integer(3));
        assert_eq!(
            run_zscore(&mut db, &[b"z", &big]),
            Frame::BulkString(Bytes::from_static(b"3"))
        );
        assert_eq!(
            run_zscore(&mut db, &[b"z", &at_limit]),
            Frame::BulkString(Bytes::from_static(b"2"))
        );
    }

    #[test]
    fn listpack_zset_answers_reads_identically() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"3", b"c", b"1", b"a", b"2", b"b"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        // Read through the twins that classify every encoding — the mutable
        // twins would promote to the B+tree form on the FIRST call and leave
        // the rest of this test measuring a skiplist.
        assert_eq!(zcard_readonly(&db, &[bulk(b"z")], 0), Frame::Integer(3));
        assert_eq!(
            ro_zscore(&db, &[b"z", b"b"]),
            Frame::BulkString(Bytes::from_static(b"2"))
        );
        assert_eq!(ro_zscore(&db, &[b"z", b"zz"]), Frame::Null);
        // Rank is score order, not insertion order.
        assert_eq!(
            zrank_readonly(&db, &[bulk(b"z"), bulk(b"a")], 0),
            Frame::Integer(0)
        );
        assert_eq!(
            zrank_readonly(&db, &[bulk(b"z"), bulk(b"c")], 0),
            Frame::Integer(2)
        );
        // ZRANGE 0 -1 WITHSCORES, score-ordered — matches the oracle's
        // `zrange z 0 -1 withscores` => a 1 b 2 c 3.
        match zrange_readonly(
            &db,
            &[bulk(b"z"), bulk(b"0"), bulk(b"-1"), bulk(b"WITHSCORES")],
            0,
        ) {
            Frame::Array(items) => {
                let got: Vec<Bytes> = items
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.clone(),
                        other => panic!("expected bulk, got {other:?}"),
                    })
                    .collect();
                assert_eq!(
                    got,
                    vec![
                        Bytes::from_static(b"a"),
                        Bytes::from_static(b"1"),
                        Bytes::from_static(b"b"),
                        Bytes::from_static(b"2"),
                        Bytes::from_static(b"c"),
                        Bytes::from_static(b"3"),
                    ]
                );
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "reads through the shared-read twins must not change the encoding"
        );
    }

    #[test]
    fn listpack_zset_updates_a_duplicate_member_in_place() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        // A repeated member is an UPDATE, not an insert: ZADD returns 0 added.
        assert_eq!(run_zadd(&mut db, &[b"z", b"10", b"a"]), Frame::Integer(0));
        // The update happens IN the listpack — no promotion, matching the
        // oracle, which still reports `listpack` after `zadd z 10 a`.
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        assert_eq!(
            ro_zscore(&db, &[b"z", b"a"]),
            Frame::BulkString(Bytes::from_static(b"10"))
        );
        // Cardinality and rank are unchanged by an in-place update — oracle
        // `zrange z 0 -1 withscores` => `b 2 c 3 a 10`.
        assert_eq!(
            zcard_readonly(&db, &[bulk(b"z")], 0),
            Frame::Integer(3),
            "an update must not add a member"
        );
        assert_eq!(
            zrank_readonly(&db, &[bulk(b"z"), bulk(b"a")], 0),
            Frame::Integer(2),
            "the new score re-orders `a` to last"
        );
        // …and reading did not silently promote it.
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
    }

    #[test]
    fn listpack_zset_normalises_scores_like_redis() {
        // Scores round-trip through the listpack as their canonical rendering,
        // exactly as the BPTree form does. Oracle (redis 8.6.1):
        //     3.0    -> 3        1e3 -> 1000
        //     3.5000 -> 3.5      inf -> inf
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"3.0", b"m"]);
        run_zadd(&mut db, &[b"z", b"1e3", b"n"]);
        run_zadd(&mut db, &[b"z", b"3.5000", b"o"]);
        run_zadd(&mut db, &[b"z", b"inf", b"p"]);
        run_zadd(&mut db, &[b"z", b"-inf", b"q"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        for (member, want) in [
            (&b"m"[..], &b"3"[..]),
            (b"n", b"1000"),
            (b"o", b"3.5"),
            (b"p", b"inf"),
            (b"q", b"-inf"),
        ] {
            assert_eq!(
                ro_zscore(&db, &[b"z", member]),
                Frame::BulkString(Bytes::copy_from_slice(want)),
                "score rendering for member {:?}",
                String::from_utf8_lossy(member)
            );
        }
        // A score that needs full precision must survive the string round trip.
        run_zadd(&mut db, &[b"z", b"1.0000000000000002", b"r"]);
        assert_eq!(
            ro_zscore(&db, &[b"z", b"r"]),
            Frame::BulkString(Bytes::from_static(b"1.0000000000000002"))
        );
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
    }

    /// `zset_score::render_score` (what the listpack stores) and
    /// `format_score_bytes` (what `ZSCORE` replies) must be byte-identical,
    /// or a listpack zset and a skiplist zset would answer differently.
    #[test]
    fn listpack_score_rendering_matches_zscore_rendering() {
        use crate::storage::zset_score::{ScoreBuf, render_score};
        let mut buf = ScoreBuf::new();
        for v in [
            0.0,
            -0.0,
            3.0,
            3.5,
            1e3,
            1e21,
            1e300,
            1e-7,
            0.1 + 0.2,
            1.0000000000000002,
            f64::MAX,
            f64::MIN_POSITIVE,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            render_score(v, &mut buf);
            assert_eq!(&buf[..], &format_score_bytes(v)[..], "score {v:?}");
        }
    }

    #[test]
    fn listpack_zset_honours_zadd_flags() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"5", b"a"]);
        // NX must not overwrite an existing member.
        assert_eq!(
            run_zadd(&mut db, &[b"z", b"NX", b"9", b"a"]),
            Frame::Integer(0)
        );
        assert_eq!(
            ro_zscore(&db, &[b"z", b"a"]),
            Frame::BulkString(Bytes::from_static(b"5"))
        );
        // XX must not create a missing member.
        assert_eq!(
            run_zadd(&mut db, &[b"z", b"XX", b"1", b"zz"]),
            Frame::Integer(0)
        );
        assert_eq!(ro_zscore(&db, &[b"z", b"zz"]), Frame::Null);
        // GT only raises.
        run_zadd(&mut db, &[b"z", b"GT", b"3", b"a"]);
        assert_eq!(
            ro_zscore(&db, &[b"z", b"a"]),
            Frame::BulkString(Bytes::from_static(b"5"))
        );
        run_zadd(&mut db, &[b"z", b"GT", b"7", b"a"]);
        assert_eq!(
            ro_zscore(&db, &[b"z", b"a"]),
            Frame::BulkString(Bytes::from_static(b"7"))
        );
        // LT only lowers.
        run_zadd(&mut db, &[b"z", b"LT", b"9", b"a"]);
        assert_eq!(
            ro_zscore(&db, &[b"z", b"a"]),
            Frame::BulkString(Bytes::from_static(b"7"))
        );
        run_zadd(&mut db, &[b"z", b"LT", b"2", b"a"]);
        assert_eq!(
            ro_zscore(&db, &[b"z", b"a"]),
            Frame::BulkString(Bytes::from_static(b"2"))
        );
        // CH counts changed, not added.
        assert_eq!(
            run_zadd(&mut db, &[b"z", b"CH", b"4", b"a"]),
            Frame::Integer(1)
        );
        assert_eq!(
            run_zadd(&mut db, &[b"z", b"CH", b"4", b"a"]),
            Frame::Integer(0)
        );
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
    }

    /// moon#814 / #820 on the listpack path. `ZADD` is all-or-nothing in Redis
    /// and it does not create the key when the command errors. The listpack
    /// branch was first written ABOVE the validation pre-pass — creating the
    /// key, writing the valid prefix, then erroring from inside the mutation
    /// loop — which is exactly the regression #820 had just fixed. It now
    /// sits below the pre-pass; this pins the reply-level half (the ledger
    /// half is in `ledger_consistency_788`).
    #[test]
    fn zadd_listpack_path_is_all_or_nothing_on_a_bad_score() {
        let mut db = Database::new();
        // Fresh key: the error must not create it.
        let r = run_zadd(&mut db, &[b"z", b"1", b"a", b"2", b"b", b"notafloat", b"c"]);
        assert!(
            matches!(&r, Frame::Error(e) if e.starts_with(b"ERR value is not a valid float")),
            "a bad score anywhere must be the float error, got {r:?}"
        );
        assert_eq!(
            db.logical_len(),
            0,
            "an erroring ZADD must not create the key (Redis: EXISTS z -> 0)"
        );
        assert_eq!(zcard_readonly(&db, &[bulk(b"z")], 0), Frame::Integer(0));

        // Existing listpack zset: the valid prefix must not be written either.
        run_zadd(&mut db, &[b"z", b"1", b"a"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        let r = run_zadd(&mut db, &[b"z", b"2", b"b", b"notafloat", b"c"]);
        assert!(matches!(r, Frame::Error(_)), "got {r:?}");
        assert_eq!(
            zcard_readonly(&db, &[bulk(b"z")], 0),
            Frame::Integer(1),
            "the valid prefix `2 b` must NOT have been written"
        );
        assert_eq!(ro_zscore(&db, &[b"z", b"b"]), Frame::Null);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
    }

    #[test]
    fn listpack_zset_upgrades_transparently_on_a_non_zadd_write() {
        // The zset commands that still go through the owned accessor upgrade
        // a listpack to the BPTree form in place. That upgrade must be
        // lossless — it is the safety net under ZPOPMIN, ZRANGESTORE and
        // friends on a key ZADD created as a listpack. ZREM and ZINCRBY were
        // in this set until moon#897 and have their own guards below;
        // ZPOPMIN stands in for the rest here.
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2.5", b"b", b"3", b"c"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        assert_eq!(
            zpopmin(&mut db, &[bulk(b"z")]),
            Frame::Array(
                [
                    Frame::BulkString(Bytes::from_static(b"a")),
                    Frame::BulkString(Bytes::from_static(b"1")),
                ]
                .into_iter()
                .collect()
            )
        );
        assert_eq!(encoding_of(&mut db, b"z"), "skiplist");
        assert_eq!(run_zcard(&mut db, &[b"z"]), Frame::Integer(2));
        assert_eq!(
            run_zscore(&mut db, &[b"z", b"c"]),
            Frame::BulkString(Bytes::from_static(b"3"))
        );
        // `b`'s non-integral score survived the upgrade; `a` is the one
        // ZPOPMIN took.
        assert_eq!(
            run_zscore(&mut db, &[b"z", b"b"]),
            Frame::BulkString(Bytes::from_static(b"2.5"))
        );
        assert_eq!(run_zscore(&mut db, &[b"z", b"a"]), Frame::Null);
        // A listpack that then grows past the threshold via ZADD promotes
        // with its non-integral score intact.
        run_zadd(&mut db, &[b"z2", b"2.5", b"b"]);
        for i in 0..=crate::storage::db::EncodingLimits::moon_defaults().zset_entries {
            let m = format!("m{i:04}");
            run_zadd(&mut db, &[b"z2", b"1", m.as_bytes()]);
        }
        assert_eq!(encoding_of(&mut db, b"z2"), "skiplist");
        assert_eq!(
            run_zscore(&mut db, &[b"z2", b"b"]),
            Frame::BulkString(Bytes::from_static(b"2.5"))
        );
    }

    // ── #897: the SECONDARY writes must not flatten a small zset ─────────
    //
    // moon#787 made ZADD build a listpack. Every OTHER zset write took the
    // eager `get_or_create_sorted_set`, which upgrades on ACCESS — so ONE
    // `ZINCRBY` or `ZREM` flattened a three-member zset to `skiplist`, and
    // because nothing demotes (moon#832) it stayed flat for the key's
    // lifetime. Measured against redis 8.6.1 on the same host, one shard:
    //
    //   zadd z 1 a; zadd z 2 b; zadd z 3 c   -> both `listpack`
    //   zincrby z 5 b                        -> moon `skiplist`, redis `listpack`
    //   zrem z b                             -> moon `skiplist`, redis `listpack`
    //
    // Mutation-red evidence: pointing either branch below at
    // `get_or_create_sorted_set` — i.e. deleting the `get_or_create_
    // zset_listpack` match arm, which is the shipped behaviour of
    // `f7c83769` — turns every `"listpack"` assertion here red.

    #[test]
    fn zincrby_keeps_a_small_zset_in_its_listpack() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        // Existing member: an in-place score replacement.
        assert_eq!(
            run_zincrby(&mut db, &[b"z", b"5", b"b"]),
            Frame::BulkString(Bytes::from_static(b"7"))
        );
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "ZINCRBY on a 3-member zset must not promote (redis 8.6.1: listpack)"
        );
        assert_eq!(
            ro_zscore(&db, &[b"z", b"b"]),
            Frame::BulkString(Bytes::from_static(b"7"))
        );
        // ...and the new score re-orders it, read back through the twin that
        // classifies every encoding.
        assert_eq!(
            zrank_readonly(&db, &[bulk(b"z"), bulk(b"b")], 0),
            Frame::Integer(2)
        );

        // A brand-new member: an append, still in the listpack.
        assert_eq!(
            run_zincrby(&mut db, &[b"z", b"2.5", b"fresh"]),
            Frame::BulkString(Bytes::from_static(b"2.5"))
        );
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        assert_eq!(zcard_readonly(&db, &[bulk(b"z")], 0), Frame::Integer(4));
        assert_eq!(
            ro_zscore(&db, &[b"z", b"fresh"]),
            Frame::BulkString(Bytes::from_static(b"2.5"))
        );

        // A missing key: ZINCRBY creates it compact, as redis does.
        assert_eq!(
            run_zincrby(&mut db, &[b"new", b"9", b"m"]),
            Frame::BulkString(Bytes::from_static(b"9"))
        );
        assert_eq!(encoding_of(&mut db, b"new"), "listpack");
    }

    #[test]
    fn zrem_keeps_a_small_zset_in_its_listpack() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2.5", b"b", b"3", b"c"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        assert_eq!(run_zrem(&mut db, &[b"z", b"b"]), Frame::Integer(1));
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "ZREM on a 3-member zset must not promote (redis 8.6.1: listpack)"
        );
        assert_eq!(zcard_readonly(&db, &[bulk(b"z")], 0), Frame::Integer(2));
        assert_eq!(ro_zscore(&db, &[b"z", b"b"]), Frame::Null);
        assert_eq!(
            ro_zscore(&db, &[b"z", b"c"]),
            Frame::BulkString(Bytes::from_static(b"3"))
        );

        // Absent and duplicate members are counted exactly once each.
        assert_eq!(
            run_zrem(&mut db, &[b"z", b"ghost", b"c", b"c"]),
            Frame::Integer(1)
        );
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");

        // The last member deletes the key, as on the B+tree arm.
        assert_eq!(run_zrem(&mut db, &[b"z", b"a"]), Frame::Integer(1));
        assert_eq!(db.logical_len(), 0, "an emptied zset must delete its key");
        assert_eq!(
            db.estimated_memory(),
            0,
            "the listpack ZREM path must credit the whole entry back"
        );
        // ZREM against a key that never existed still answers 0 and creates
        // nothing.
        assert_eq!(run_zrem(&mut db, &[b"nosuch", b"a"]), Frame::Integer(0));
        assert_eq!(db.logical_len(), 0);
    }

    /// `remove_pair` matches the FIELD half of each pair only. For a zset
    /// listpack that half is the MEMBER — so `ZREM z 7` must not delete the
    /// member whose SCORE renders as `7`.
    #[test]
    fn zrem_listpack_never_matches_a_score() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"7", b"a", b"8", b"b"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        assert_eq!(run_zrem(&mut db, &[b"z", b"7"]), Frame::Integer(0));
        assert_eq!(zcard_readonly(&db, &[bulk(b"z")], 0), Frame::Integer(2));
        assert_eq!(
            ro_zscore(&db, &[b"z", b"a"]),
            Frame::BulkString(Bytes::from_static(b"7"))
        );
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
    }

    /// Both sides of BOTH thresholds. A zset that genuinely outgrows the
    /// policy must STILL promote — the fix is "stop promoting early", not
    /// "stop promoting".
    #[test]
    fn zincrby_still_promotes_when_a_threshold_is_genuinely_crossed() {
        let max = crate::storage::db::EncodingLimits::moon_defaults().zset_entries;

        // Entry count. Build to exactly `max`, then let ZINCRBY add one.
        let mut db = Database::new();
        for i in 0..max {
            let m = format!("m{i:04}");
            run_zadd(&mut db, &[b"z", b"1", m.as_bytes()]);
        }
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        // Touching an EXISTING member at the threshold does not grow it.
        run_zincrby(&mut db, &[b"z", b"1", b"m0007"]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "exactly zset-max-listpack-entries members is still a listpack"
        );
        // A NEW member crosses it.
        run_zincrby(&mut db, &[b"z", b"1", b"over-the-line"]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "skiplist",
            "the {}th member must promote",
            max + 1
        );
        assert_eq!(
            run_zcard(&mut db, &[b"z"]),
            Frame::Integer(max as i64 + 1),
            "the promotion must not lose a member"
        );
        assert_eq!(
            run_zscore(&mut db, &[b"z", b"m0007"]),
            Frame::BulkString(Bytes::from_static(b"2")),
            "the in-listpack increment must survive the promotion"
        );

        // Member size. Exactly at the limit fits; one byte over promotes.
        let value_max = crate::storage::db::EncodingLimits::moon_defaults().zset_value;
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a"]);
        let at_limit = vec![b'y'; value_max];
        run_zincrby(&mut db, &[b"z", b"1", &at_limit]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "listpack",
            "a member of exactly zset-max-listpack-value bytes fits"
        );
        let too_big = vec![b'x'; value_max + 1];
        run_zincrby(&mut db, &[b"z", b"1", &too_big]);
        assert_eq!(
            encoding_of(&mut db, b"z"),
            "skiplist",
            "a member longer than zset-max-listpack-value must promote"
        );
        assert_eq!(
            run_zscore(&mut db, &[b"z", &too_big]),
            Frame::BulkString(Bytes::from_static(b"1"))
        );
        assert_eq!(
            run_zscore(&mut db, &[b"z", &at_limit]),
            Frame::BulkString(Bytes::from_static(b"1"))
        );
    }

    /// A zset that is ALREADY the full form stays there: neither command
    /// demotes (moon#832 has no inverse, and neither does Redis).
    #[test]
    fn secondary_writes_never_demote_a_promoted_zset() {
        let max = crate::storage::db::EncodingLimits::moon_defaults().zset_entries;
        let mut db = Database::new();
        for i in 0..=max {
            let m = format!("m{i:04}");
            run_zadd(&mut db, &[b"z", b"1", m.as_bytes()]);
        }
        assert_eq!(encoding_of(&mut db, b"z"), "skiplist");
        run_zincrby(&mut db, &[b"z", b"5", b"m0000"]);
        assert_eq!(encoding_of(&mut db, b"z"), "skiplist");
        // Down to well under the threshold: still a skiplist, as in Redis.
        for i in 0..max {
            let m = format!("m{i:04}");
            run_zrem(&mut db, &[b"z", m.as_bytes()]);
        }
        assert_eq!(encoding_of(&mut db, b"z"), "skiplist");
        assert_eq!(run_zcard(&mut db, &[b"z"]), Frame::Integer(1));
    }

    /// The moon#863 hazard, stated for ZINCRBY: a listpack stores the score
    /// as its RENDERED text, so every score ZINCRBY can produce must survive
    /// `render_score` -> `parse_score`. `inf`/`-inf` are the ones that bit
    /// #863 on the replication path.
    #[test]
    fn zincrby_scores_round_trip_through_the_listpack() {
        for (incr, want) in [
            (&b"inf"[..], &b"inf"[..]),
            (b"-inf", b"-inf"),
            (b"1e300", b"1e300"),
            (b"1e-300", b"1e-300"),
            (b"0.1", b"0.1"),
            (b"3.0", b"3"),
            (b"1e3", b"1000"),
            (b"1.0000000000000002", b"1.0000000000000002"),
            (b"-7.5", b"-7.5"),
        ] {
            let mut db = Database::new();
            run_zadd(&mut db, &[b"z", b"1", b"other"]);
            let reply = run_zincrby(&mut db, &[b"z", b"0", b"m"]);
            assert_eq!(reply, Frame::BulkString(Bytes::from_static(b"0")));
            let reply = run_zincrby(&mut db, &[b"z", incr, b"m"]);
            assert_eq!(
                encoding_of(&mut db, b"z"),
                "listpack",
                "incr {:?} must stay compact",
                String::from_utf8_lossy(incr)
            );
            // The stored text and the reply must agree, and ZSCORE must read
            // the identical value back out of the listpack.
            let want_frame = Frame::BulkString(Bytes::copy_from_slice(
                &format_score_bytes(std::str::from_utf8(want).unwrap().parse::<f64>().unwrap())[..],
            ));
            assert_eq!(
                reply,
                want_frame,
                "ZINCRBY reply for {:?}",
                String::from_utf8_lossy(incr)
            );
            assert_eq!(
                ro_zscore(&db, &[b"z", b"m"]),
                want_frame,
                "ZSCORE round-trip for {:?}",
                String::from_utf8_lossy(incr)
            );
            // A second ZINCRBY reads the stored text back as an f64 and must
            // land on the same value — the step that would silently return
            // 0.0 if `parse_score` had refused what `render_score` wrote.
            assert_eq!(
                run_zincrby(&mut db, &[b"z", b"0", b"m"]),
                want_frame,
                "re-increment by zero for {:?}",
                String::from_utf8_lossy(incr)
            );
        }
    }

    /// `inf + -inf` is NaN, and `render_score(NaN)` writes `NaN`, which
    /// `parse_score` refuses — a NaN in a listpack would read back as `0.0`
    /// (the moon#863 shape). The listpack arm must therefore refuse the case
    /// and hand it to the B+tree arm, which is byte-for-byte what ZINCRBY
    /// did before moon#897.
    ///
    /// NOTE: moon's reply here (`NaN`) diverges from redis 8.6.1, which
    /// answers `ERR resulting score is not a number (NaN)` and leaves the
    /// score untouched. That divergence is PRE-EXISTING and deliberately
    /// unchanged by moon#897; this test pins moon's current behaviour so the
    /// listpack path cannot silently make it worse.
    #[test]
    fn zincrby_nan_never_reaches_the_listpack() {
        for (first, second) in [(&b"inf"[..], &b"-inf"[..]), (b"-inf", b"inf")] {
            let mut db = Database::new();
            run_zadd(&mut db, &[b"z", b"1", b"other"]);
            assert_eq!(
                run_zincrby(&mut db, &[b"z", first, b"m"]),
                Frame::BulkString(Bytes::copy_from_slice(first))
            );
            assert_eq!(encoding_of(&mut db, b"z"), "listpack");
            // The NaN step stands down to the owned accessor, which promotes.
            let reply = run_zincrby(&mut db, &[b"z", second, b"m"]);
            assert_eq!(
                reply,
                Frame::BulkString(Bytes::from_static(b"NaN")),
                "moon's pre-#897 reply for a NaN result, unchanged"
            );
            assert_eq!(
                encoding_of(&mut db, b"z"),
                "skiplist",
                "the NaN case falls through to the B+tree arm"
            );
            // The critical half: `m` must NOT read back as 0.0 out of a
            // listpack. Whatever moon stores, it is not a silently-zeroed
            // score.
            assert_ne!(
                ro_zscore(&db, &[b"z", b"m"]),
                Frame::BulkString(Bytes::from_static(b"0")),
                "a NaN must never round-trip through a listpack as 0"
            );
        }
    }

    /// moon#795: compact encodings are not byte-transparent for
    /// numeric-looking strings unless every writer is careful. A MEMBER's
    /// bytes must survive both new arms.
    #[test]
    fn secondary_writes_preserve_numeric_looking_member_bytes() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"000000012345"]);
        run_zadd(&mut db, &[b"z", b"2", b"+5"]);
        run_zadd(&mut db, &[b"z", b"3", b"5"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        run_zincrby(&mut db, &[b"z", b"10", b"000000012345"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        assert_eq!(
            ro_zscore(&db, &[b"z", b"000000012345"]),
            Frame::BulkString(Bytes::from_static(b"11")),
            "`000000012345` must not be read as `12345`"
        );
        assert_eq!(
            ro_zscore(&db, &[b"z", b"+5"]),
            Frame::BulkString(Bytes::from_static(b"2")),
            "`+5` must not be read as `5`"
        );
        assert_eq!(
            ro_zscore(&db, &[b"z", b"5"]),
            Frame::BulkString(Bytes::from_static(b"3"))
        );
        assert_eq!(run_zrem(&mut db, &[b"z", b"+5"]), Frame::Integer(1));
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        assert_eq!(
            ro_zscore(&db, &[b"z", b"5"]),
            Frame::BulkString(Bytes::from_static(b"3")),
            "removing `+5` must not remove `5`"
        );
        assert_eq!(zcard_readonly(&db, &[bulk(b"z")], 0), Frame::Integer(2));
    }

    /// The `used_memory` ledger must stay EXACT across the two new arms.
    ///
    /// `ledger_consistency_788::sorted_set_mutations_keep_the_ledger_exact`
    /// makes this claim for the B+tree form only — its fixture is 300
    /// members, so ZINCRBY and ZREM take the fall-through there and the
    /// listpack arms are invisible to it. The check is the same one:
    /// the running ledger against a full recompute. moon#814's failure mode
    /// (a delete crediting bytes that were never charged, driving
    /// `used_memory` down without bound until `--maxmemory` can never fire)
    /// is what an inexact delta here would reopen.
    #[test]
    fn listpack_secondary_writes_keep_the_ledger_exact() {
        fn exact(db: &mut Database, step: &str) {
            let running = db.estimated_memory();
            db.recalculate_memory();
            let recomputed = db.estimated_memory();
            assert_eq!(
                running, recomputed,
                "{step}: running ledger {running} B != full recompute \
                 {recomputed} B — the listpack arm charged the wrong delta"
            );
        }

        let mut db = Database::new();
        for i in 0..8u32 {
            let m = format!("m:{i:04}");
            run_zadd(&mut db, &[b"z", i.to_string().as_bytes(), m.as_bytes()]);
        }
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        exact(&mut db, "after 8 ZADD into a listpack");

        run_zincrby(&mut db, &[b"z", b"5", b"m:0001"]); // existing member
        run_zincrby(&mut db, &[b"z", b"5", b"brand-new"]); // new member
        run_zincrby(&mut db, &[b"z", b"1e300", b"long-score"]); // long rendering
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        exact(&mut db, "after ZINCRBY on a listpack");

        run_zrem(&mut db, &[b"z", b"m:0002", b"absent"]);
        assert_eq!(encoding_of(&mut db, b"z"), "listpack");
        exact(&mut db, "after ZREM on a listpack");

        // Crossing the threshold BY a ZINCRBY: the promotion swing has to be
        // billed exactly once, by the accessor.
        for i in 0..crate::storage::db::EncodingLimits::moon_defaults().zset_entries as u32 {
            let m = format!("bulk:{i:04}");
            run_zadd(&mut db, &[b"z2", b"1", m.as_bytes()]);
        }
        assert_eq!(encoding_of(&mut db, b"z2"), "listpack");
        exact(&mut db, "at the threshold, still a listpack");
        run_zincrby(&mut db, &[b"z2", b"1", b"crosses"]);
        assert_eq!(encoding_of(&mut db, b"z2"), "skiplist");
        exact(&mut db, "after ZINCRBY promoted the zset");

        // Drain to empty on the listpack arm, then confirm the ledger is back
        // to exactly where the other key leaves it.
        let mark = db.estimated_memory();
        for i in 0..10u32 {
            let m = format!("d:{i:04}");
            run_zadd(&mut db, &[b"drain", b"1", m.as_bytes()]);
        }
        assert_eq!(encoding_of(&mut db, b"drain"), "listpack");
        for i in 0..10u32 {
            let m = format!("d:{i:04}");
            run_zrem(&mut db, &[b"drain", m.as_bytes()]);
        }
        assert_eq!(
            db.estimated_memory(),
            mark,
            "ZREM-to-empty auto-removed the key but left bytes charged"
        );
        exact(&mut db, "after draining a listpack zset to empty");
    }

    /// WRONGTYPE must still come out of both new arms, and neither may
    /// clobber the value it refused.
    #[test]
    fn secondary_writes_reject_a_wrong_type_key() {
        for run in [
            (|db: &mut Database| run_zincrby(db, &[b"str", b"1", b"a"]))
                as fn(&mut Database) -> Frame,
            |db: &mut Database| run_zrem(db, &[b"str", b"a"]),
        ] {
            let mut db = Database::new();
            db.set(
                b"str",
                crate::storage::entry::Entry::new_string(Bytes::from_static(b"v")),
            );
            match run(&mut db) {
                Frame::Error(e) => assert!(
                    e.starts_with(b"WRONGTYPE"),
                    "expected WRONGTYPE, got {:?}",
                    String::from_utf8_lossy(&e)
                ),
                other => panic!("expected WRONGTYPE error, got {other:?}"),
            }
            assert_eq!(encoding_of(&mut db, b"str"), "embstr");
        }
    }

    #[test]
    fn zadd_listpack_rejects_a_wrong_type_key() {
        let mut db = Database::new();
        db.set(
            b"str",
            crate::storage::entry::Entry::new_string(Bytes::from_static(b"v")),
        );
        match run_zadd(&mut db, &[b"str", b"1", b"a"]) {
            Frame::Error(e) => assert!(
                e.starts_with(b"WRONGTYPE"),
                "expected WRONGTYPE, got {:?}",
                String::from_utf8_lossy(&e)
            ),
            other => panic!("expected WRONGTYPE error, got {other:?}"),
        }
    }
}

#[cfg(test)]
mod zadd_listpack_batch_tests {
    use crate::protocol::Frame;
    use crate::storage::Database;
    use bytes::Bytes;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// moon#865, the sorted-set arm. A zset listpack stores TWO entries per
    /// member (member then score), so it wraps the header's u16 element count
    /// at 32,768 members -- half the list/set threshold. Before the batch
    /// guard, every pair was pushed and only then was the entry count compared
    /// with zset-max-listpack-entries.
    #[test]
    fn zadd_one_call_past_u16_keeps_every_member() {
        let mut db = Database::new();
        const N: usize = 40_000; // 40k members = 80k listpack entries
        let owned: Vec<Vec<u8>> = (0..N).map(|i| format!("m{i:07}").into_bytes()).collect();
        let scores: Vec<Vec<u8>> = (0..N).map(|i| format!("{i}").into_bytes()).collect();
        let mut args: Vec<Frame> = Vec::with_capacity(N * 2 + 1);
        args.push(bs(b"bigz"));
        for i in 0..N {
            args.push(bs(&scores[i]));
            args.push(bs(&owned[i]));
        }

        assert_eq!(
            crate::command::sorted_set::zadd(&mut db, &args),
            Frame::Integer(N as i64),
            "ZADD under-reported the members it accepted"
        );
        assert_eq!(
            crate::command::sorted_set::zcard(&mut db, &[bs(b"bigz")]),
            Frame::Integer(N as i64),
            "ZCARD lost members to the u16 wrap"
        );
        assert_ne!(
            crate::command::sorted_set::zscore(&mut db, &[bs(b"bigz"), bs(&owned[N - 1])]),
            Frame::Null,
            "last member unreachable"
        );
    }

    /// The guard must not disable the encoding it protects.
    #[test]
    fn small_batches_still_reach_the_listpack_and_accumulate() {
        for n in [1usize, 8, 64, 65] {
            let mut db = Database::new();
            let mut args: Vec<Frame> = Vec::with_capacity(n * 2 + 1);
            args.push(bs(b"z"));
            for i in 0..n {
                args.push(bs(format!("{i}").as_bytes()));
                args.push(bs(format!("m{i:05}").as_bytes()));
            }
            assert_eq!(
                crate::command::sorted_set::zadd(&mut db, &args),
                Frame::Integer(n as i64),
                "ZADD n={n}"
            );
            assert_eq!(
                crate::command::sorted_set::zcard(&mut db, &[bs(b"z")]),
                Frame::Integer(n as i64),
                "ZCARD n={n}"
            );
        }

        let mut db = Database::new();
        for c in 0..400 {
            let mut args: Vec<Frame> = Vec::with_capacity(201);
            args.push(bs(b"acc"));
            for i in 0..100 {
                let idx = c * 100 + i;
                args.push(bs(format!("{idx}").as_bytes()));
                args.push(bs(format!("m{idx:07}").as_bytes()));
            }
            crate::command::sorted_set::zadd(&mut db, &args);
        }
        assert_eq!(
            crate::command::sorted_set::zcard(&mut db, &[bs(b"acc")]),
            Frame::Integer(40_000)
        );
    }
}
