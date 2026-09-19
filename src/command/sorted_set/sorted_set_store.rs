//! The sorted-set STORE family: `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`
//! and `ZRANGESTORE`.
//!
//! Split out of `sorted_set_write.rs` when `ZDIFFSTORE` and the
//! `ZREMRANGEBY*` trio (moon#959) took that file past the 1500-line rule.
//! Every command here reads one or more SOURCE zsets and REPLACES a
//! destination, which is a different shape from the in-place writes that
//! stay in the write half. Tests stay in `mod.rs`.

use bytes::Bytes;
use std::collections::HashMap;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{zset_member_cost, zset_table_bytes};

use crate::command::helpers::{err, err_wrong_args, extract_bytes};

use super::{
    AggregateOp, clamp_nan_to_zero, parse_lex_bound, parse_numkeys, parse_score_bound, zadd_member,
    zrange_by_lex, zrange_by_rank, zrange_by_score,
};

/// A rank bound as `zrange_by_rank` parses it: a base-10 `i64`, or `None`.
fn parse_rank(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Which set operation a `Z*STORE` command computes over its sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetOp {
    Union,
    Inter,
    /// Members of the FIRST source that are absent from every other one,
    /// keeping the first source's scores. Takes no `WEIGHTS`/`AGGREGATE`:
    /// Redis's `zunionInterDiffGenericCommand` recognises those tokens only
    /// when `op != SET_OP_DIFF`, so on `ZDIFFSTORE` they are `ERR syntax
    /// error` like any other unknown token (verified against redis-server
    /// 8.6.1, moon#959).
    Diff,
}

impl SetOp {
    /// The registered command name, for the arity and `numkeys` messages.
    fn name(self) -> &'static str {
        match self {
            SetOp::Union => "ZUNIONSTORE",
            SetOp::Inter => "ZINTERSTORE",
            SetOp::Diff => "ZDIFFSTORE",
        }
    }
}

/// ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn zunionstore(db: &mut Database, args: &[Frame]) -> Frame {
    zstore_impl(db, args, SetOp::Union)
}

/// ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
pub fn zinterstore(db: &mut Database, args: &[Frame]) -> Frame {
    zstore_impl(db, args, SetOp::Inter)
}

/// ZDIFFSTORE destination numkeys key [key ...] (moon#959)
///
/// Stores in `destination` the members of the first source absent from every
/// other source, with the first source's scores. Replies the cardinality of
/// `destination`, deleting it when the difference is empty. Shares the
/// `numkeys` contract and the option loop of its siblings — including the
/// two-class `numkeys` split (moon#969) — with `WEIGHTS`/`AGGREGATE` refused
/// as `syntax error`, which is what redis 8.6.1 answers.
pub fn zdiffstore(db: &mut Database, args: &[Frame]) -> Frame {
    zstore_impl(db, args, SetOp::Diff)
}

fn zstore_impl(db: &mut Database, args: &[Frame], op: SetOp) -> Frame {
    let cmd_name = op.name();
    if args.len() < 3 {
        return err_wrong_args(cmd_name);
    }
    let dest = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args(cmd_name),
    };
    let numkeys_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args(cmd_name),
    };
    let numkeys = match parse_numkeys(numkeys_bytes, cmd_name) {
        Ok(n) => n,
        Err(e) => return e,
    };

    // A `numkeys` that overruns the key list is `syntax error`, not an arity
    // error (moon#969) — Redis's arity check already passed above, and
    // `zunionInterDiffGenericCommand` answers `shared.syntaxerr` here.
    if args.len() < 2 + numkeys {
        return err("ERR syntax error");
    }

    // Collect source keys
    let source_keys: Vec<Bytes> = (0..numkeys)
        .map(|j| {
            extract_bytes(&args[2 + j])
                .cloned()
                .unwrap_or_else(|| Bytes::new())
        })
        .collect();

    // The sources are read BEFORE the options are parsed, because that is
    // the order `zunionInterDiffGenericCommand` takes: it looks every source
    // up (and refuses a wrong type) before it looks at `WEIGHTS`, so
    // `ZUNIONSTORE d 1 <string-key> BOGUS` is `WRONGTYPE` on redis 8.6.1, not
    // `syntax error`. Parsing the options first inverted that (moon#959).
    //
    // Each source is read through the SHARED-borrow view, not `get_sorted_set`: that
    // accessor's `get_promoted` core upgrades a listpack source to the B+tree
    // form as a side effect of READING it — the moon#928 defect, which the
    // read-only set-operation family (`collect_source_sets_readonly`) already
    // left behind. A `&Database` borrow cannot reach `SortedSetKind::upgrade`
    // at all, and `get_sorted_set_ref_if_alive` classifies every encoding
    // (B+tree, listpack, legacy, and a cold-tier hit read through as `Owned`)
    // rather than demanding one. Each source is copied into an owned map
    // exactly as before, because the destination write below needs `db`
    // mutably; a listpack's copy is bounded by `zset-max-listpack-entries`.
    let now_ms = db.now_ms();
    let mut source_data: Vec<HashMap<Bytes, f64>> = Vec::with_capacity(numkeys);
    for key in &source_keys {
        match db.get_sorted_set_ref_if_alive(key, now_ms) {
            Ok(Some(zref)) => match zref.members_map() {
                Some(members) => source_data.push(members.clone()),
                None => source_data.push(zref.entries_sorted().into_iter().collect()),
            },
            Ok(None) => {
                source_data.push(HashMap::new());
            }
            Err(e) => return e,
        }
    }

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
        // `WEIGHTS` and `AGGREGATE` are not tokens ZDIFFSTORE knows — Redis
        // only matches them `if (op != SET_OP_DIFF)`, so on a diff they fall
        // through to the unknown-token arm and are `syntax error` (moon#959).
        let takes_weights = op != SetOp::Diff;
        if takes_weights && opt.eq_ignore_ascii_case(b"WEIGHTS") {
            for w in 0..numkeys {
                // Too few weights to cover the key list is `syntax error` on
                // Redis, not an arity error (moon#969).
                if i + 1 + w >= args.len() {
                    return err("ERR syntax error");
                }
                let wb = match extract_bytes(&args[i + 1 + w]) {
                    Some(b) => b,
                    None => return err("ERR syntax error"),
                };
                // `"nan"` PARSES in Rust where C's `strtod` + `isnan` check in
                // `getDoubleFromObjectOrReply` rejects it (moon#969), so a NaN
                // weight sailed through and poisoned every aggregated score.
                // Infinities stay legal, as they are on Redis.
                let wval: f64 = match std::str::from_utf8(wb)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|v| !v.is_nan())
                {
                    Some(v) => v,
                    None => return err("ERR weight value is not a float"),
                };
                weights[w] = wval;
            }
            i += 1 + numkeys;
        } else if takes_weights && opt.eq_ignore_ascii_case(b"AGGREGATE") {
            if i + 1 >= args.len() {
                return err("ERR syntax error");
            }
            let agg_b = match extract_bytes(&args[i + 1]) {
                Some(b) => b.as_ref(),
                None => return err("ERR syntax error"),
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
            // moon#967 rewrote every OTHER zset option loop to reject an
            // unrecognised token and missed this one, so `ZUNIONSTORE d 1 k
            // BOGUS` stepped over `BOGUS` and answered a DIFFERENT, successful
            // command. Redis: `ERR syntax error`.
            return err("ERR syntax error");
        }
    }

    // Compute result
    let mut result_map: HashMap<Bytes, f64> = HashMap::new();

    if op == SetOp::Diff {
        // Members of the first source that no later source contains, with the
        // first source's scores untouched — the same walk `zdiff_readonly`
        // makes. No weight applies: the option loop refused `WEIGHTS`.
        if let Some(first) = source_data.first() {
            'outer: for (member, score) in first {
                for src in source_data.iter().skip(1) {
                    if src.contains_key(member) {
                        continue 'outer;
                    }
                }
                result_map.insert(member.clone(), *score);
            }
        }
    } else if op == SetOp::Inter {
        // Start with first set's members
        if let Some(first) = source_data.first() {
            for (member, score) in first {
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
    } else {
        // Union: all members from all sets
        for (idx, src) in source_data.iter().enumerate() {
            for (member, score) in src {
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
    }

    let result_size = result_map.len() as i64;

    // Remove destination key first, then create new sorted set
    db.remove(dest);

    if !result_map.is_empty() {
        let (members, scores) = match db.get_or_create_sorted_set(dest) {
            Ok(pair) => pair,
            Err(e) => return e,
        };

        // `dest` was just removed/recreated above, so every member here is
        // new -- charge each unconditionally (O(1) per member, no full
        // recompute of the destination sorted set).
        let mut mem_charge: usize = 0;
        let table_before = zset_table_bytes(members, scores);
        for (member, score) in result_map {
            mem_charge += zset_member_cost(&member);
            zadd_member(members, scores, member, score);
        }
        let table_after = zset_table_bytes(members, scores);
        // `members`/`scores`' borrow of `db` ends above.
        db.charge_memory(mem_charge);
        db.adjust_memory(table_before, table_after);
    }

    Frame::Integer(result_size)
}

// ---------------------------------------------------------------------------
// ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
// ---------------------------------------------------------------------------

/// ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]
///
/// Stores the result of a ZRANGE into `dst`, replacing it. Returns the cardinality of `dst`.
pub fn zrangestore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("ZRANGESTORE");
    }
    let dst = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("ZRANGESTORE"),
    };
    let src = match extract_bytes(&args[1]) {
        Some(k) => k,
        None => return err_wrong_args("ZRANGESTORE"),
    };
    let min_arg = match extract_bytes(&args[2]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZRANGESTORE"),
    };
    let max_arg = match extract_bytes(&args[3]) {
        Some(b) => b.clone(),
        None => return err_wrong_args("ZRANGESTORE"),
    };

    // Parse optional flags (same as ZRANGE but no WITHSCORES)
    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;

    let mut i = 4;
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
        } else if opt.eq_ignore_ascii_case(b"LIMIT") {
            if i + 2 < args.len() {
                let off_b = match extract_bytes(&args[i + 1]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGESTORE"),
                };
                let cnt_b = match extract_bytes(&args[i + 2]) {
                    Some(b) => b,
                    None => return err_wrong_args("ZRANGESTORE"),
                };
                limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
                limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
                if limit_offset.is_none() || limit_count.is_none() {
                    return err("ERR value is not an integer or out of range");
                }
                i += 3;
            } else {
                return err_wrong_args("ZRANGESTORE");
            }
        } else {
            return err("ERR syntax error");
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

    // moon#1102: Redis parses the range grammar unconditionally, before it
    // looks the source up — a malformed bound is a parse error whether the
    // source is missing, a zset or another type. The helpers below validate
    // too, but only on the `Ok(Some(_))` arm: a missing source used to fall
    // through to "store the empty result" and DELETE `dst`, and a wrong-type
    // source answered WRONGTYPE. Same shape as `zrange_readonly` (moon#1060).
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
    } else if parse_rank(&min_arg).is_none() || parse_rank(&max_arg).is_none() {
        return err("ERR value is not an integer or out of range");
    }

    // Run ZRANGE on src, collecting (member, score) pairs
    let entries: Vec<(Bytes, f64)> = match db.get_sorted_set(src) {
        Ok(Some((members, scores))) => {
            let frame = if by_score {
                zrange_by_score(
                    members,
                    scores,
                    &min_arg,
                    &max_arg,
                    rev,
                    true,
                    limit_offset,
                    limit_count,
                )
            } else if by_lex {
                zrange_by_lex(
                    scores,
                    &min_arg,
                    &max_arg,
                    rev,
                    true,
                    members,
                    limit_offset,
                    limit_count,
                )
            } else {
                zrange_by_rank(scores, &min_arg, &max_arg, rev, true)
            };
            // Parse the Frame::Array([member, score, member, score, ...]) into Vec<(Bytes, f64)>
            match frame {
                Frame::Array(arr) => {
                    let mut result = Vec::with_capacity(arr.len() / 2);
                    let mut idx = 0;
                    while idx + 1 < arr.len() {
                        if let (Frame::BulkString(m), Frame::BulkString(s)) =
                            (&arr[idx], &arr[idx + 1])
                        {
                            if let Ok(score) = std::str::from_utf8(s).unwrap_or("0").parse::<f64>()
                            {
                                result.push((m.clone(), score));
                            }
                        }
                        idx += 2;
                    }
                    result
                }
                Frame::Error(_) => return frame,
                _ => Vec::with_capacity(0),
            }
        }
        Ok(None) => Vec::with_capacity(0),
        Err(e) => return e,
    };

    let count = entries.len() as i64;

    // Replace dst with the result
    db.remove(dst);

    if !entries.is_empty() {
        let (dst_members, dst_scores) = match db.get_or_create_sorted_set(dst) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        // `dst` was just removed/recreated above, so every entry is new.
        let mut mem_charge: usize = 0;
        let table_before = zset_table_bytes(dst_members, dst_scores);
        for (member, score) in entries {
            mem_charge += zset_member_cost(&member);
            zadd_member(dst_members, dst_scores, member, score);
        }
        let table_after = zset_table_bytes(dst_members, dst_scores);
        // `dst_members`/`dst_scores`' borrow of `db` ends above.
        db.charge_memory(mem_charge);
        db.adjust_memory(table_before, table_after);
    }

    Frame::Integer(count)
}
