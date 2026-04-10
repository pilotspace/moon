use bytes::Bytes;
use std::collections::HashMap;

use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err, err_wrong_args, extract_bytes};

use super::{format_score, zadd_member, zrem_member};

// ---------------------------------------------------------------------------
// Write commands (mutate the database)
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
        if arg.eq_ignore_ascii_case(b"NX") {
            nx = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"XX") {
            xx = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"GT") {
            gt = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"LT") {
            lt = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"CH") {
            ch = true;
            i += 1;
        } else {
            break;
        }
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
    if remaining.is_empty() || !remaining.len().is_multiple_of(2) {
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
            None => !xx, // New member: add unless XX
            Some(old) => {
                if nx {
                    false // NX: never update existing
                } else if gt && lt {
                    false // GT+LT together: never update (mutually exclusive)
                } else if gt {
                    score > old
                } else if lt {
                    score < old
                } else {
                    true // No flags: always update
                }
            }
        };

        if should_update {
            let is_new = zadd_member(members, scores, member, score);
            if is_new {
                added += 1;
                changed += 1;
            } else if existing_score.is_some_and(|es| (es - score).abs() > f64::EPSILON) {
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
        match std::str::from_utf8(count_bytes)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
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

    Frame::Array(result.into())
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
        match std::str::from_utf8(count_bytes)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
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

    Frame::Array(result.into())
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn zunionstore(db: &mut Database, args: &[Frame]) -> Frame {
    zstore_impl(db, args, false)
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn zinterstore(db: &mut Database, args: &[Frame]) -> Frame {
    zstore_impl(db, args, true)
}

#[derive(Debug, Clone, Copy)]
enum AggregateOp {
    Sum,
    Min,
    Max,
}

fn zstore_impl(db: &mut Database, args: &[Frame], intersect: bool) -> Frame {
    let cmd_name = if intersect {
        "ZINTERSTORE"
    } else {
        "ZUNIONSTORE"
    };
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
            None => {
                i += 1;
                continue;
            }
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
                let wval: f64 = match std::str::from_utf8(wb).ok().and_then(|s| s.parse().ok()) {
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
