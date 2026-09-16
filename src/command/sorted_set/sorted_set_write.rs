use bytes::Bytes;
use std::collections::HashMap;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{Shape, zset_member_cost, zset_table_bytes};
use crate::storage::listpack::PairUpdate;
use crate::storage::zset_score::{ScoreBuf, render_score};

use crate::command::helpers::{all_args_are_bytes, err, err_wrong_args, extract_bytes};
use crate::command::sorted_set::work_budget;

use super::{
    AggregateOp, clamp_nan_to_zero, format_score, format_score_bytes, zadd_member, zrange_by_lex,
    zrange_by_rank, zrange_by_score, zrem_member, zset_insert_absent, zset_update_existing,
};

// ---------------------------------------------------------------------------
// Write commands (mutate the database)
// ---------------------------------------------------------------------------

/// How many `score member` pairs of one `ZADD` are carried from the moon#814
/// validation pre-pass to the mutation loop in a stack array.
///
/// A fixed array rather than a `SmallVec` because `src/command/` forbids the
/// heap allocation a spill would make: past this many pairs the loop re-parses
/// exactly as it always did, which is correct, just not free. 32 pairs covers
/// the benchmark's one and every batch an application realistically sends, and
/// costs 256 bytes of a shard thread's stack.
const ZADD_INLINE_PAIRS: usize = 32;

/// Parse one `score member` pair of a `ZADD`.
///
/// The single source of truth for what `ZADD` accepts. The validation pre-pass
/// calls it for every pair, and the mutation loop calls it again for any batch
/// too large to cache — and that shared definition is load-bearing rather than
/// tidy: the moment the two disagree — the pre-pass accepting something the
/// loop then rejects — moon#814 returns, because the loop's error arms return
/// from inside the `table_before … charge_memory()` window and strand the
/// charge for every member already inserted.
#[inline]
fn parse_zadd_pair<'a>(
    score_arg: &Frame,
    member_arg: &'a Frame,
) -> Result<(f64, &'a Bytes), Frame> {
    let (Some(score_bytes), Some(member)) = (extract_bytes(score_arg), extract_bytes(member_arg))
    else {
        return Err(err_wrong_args("ZADD"));
    };
    let Ok(score_str) = std::str::from_utf8(score_bytes) else {
        return Err(err("ERR value is not a valid float"));
    };
    work_budget::note_arg_score_parse();
    let Ok(score) = score_str.parse::<f64>() else {
        return Err(err("ERR value is not a valid float"));
    };
    if score.is_nan() {
        return Err(err("ERR value is not a valid float"));
    }
    Ok((score, member))
}

/// The pair at `idx`, taking the score from the pre-pass's cache when it fits
/// and re-parsing when it does not (moon#942).
///
/// Neither arm can fail for a batch the pre-pass accepted; both are kept as
/// real `Result`s anyway, because a bare `unwrap` here would be the one place
/// the validation and the mutation could silently diverge — which is exactly
/// the moon#814 shape.
#[inline]
fn resolved_pair<'a>(
    pair: &'a [Frame],
    idx: usize,
    cache: Option<&[f64]>,
) -> Result<(f64, &'a Bytes), Frame> {
    let [score_arg, member_arg] = pair else {
        return Err(err_wrong_args("ZADD"));
    };
    match cache {
        Some(scores) => {
            let (Some(member), Some(score)) = (extract_bytes(member_arg), scores.get(idx)) else {
                return Err(err_wrong_args("ZADD"));
            };
            Ok((*score, member))
        }
        None => parse_zadd_pair(score_arg, member_arg),
    }
}

// A zset listpack is `[member, score, member, score, …]` — the same
// field/value layout a hash listpack has — so the member/score update is
// `Listpack::update_pair_value`: ONE borrowed scan that keeps the byte
// offsets it walked past and writes the new score where it stopped.
//
// It used to be a local `listpack_zset_find` returning a pair ORDINAL,
// followed by `replace_at`, which walked back to that ordinal from the head.
// Two scans to change one score, which is the defect moon#799 fixed for HSET
// and left standing for the sorted set (moon#942). Nothing materialises for
// the entries walked past on either the lookup or the write.
//
// An unparseable stored score is in-memory corruption (every writer goes
// through `render_score`); the closures below read it as 0.0 so the member is
// still FOUND and updated in place rather than duplicated.

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

    // moon#814: validate EVERY pair BEFORE touching the keyspace.
    //
    // The mutation loop below runs inside the `table_before … charge_memory()`
    // window, so returning an error from inside it left members inserted with
    // their charge never applied. Because `db.remove` later credits an
    // `entry_overhead` recomputed from the CURRENT value, a delete afterwards
    // credited back memory that was never charged — driving `used_memory`
    // monotonically DOWN, without bound, on a path any unprivileged client can
    // trigger, until `--maxmemory` can never fire. Measured before this fix: a
    // 100-pair ZADD with a bad tail billed 3,846 B against 21,541 B of real
    // content, and 25 churn rounds drifted the ledger by 42,400 B each.
    //
    // Validating first is also Redis parity: real Redis's ZADD is
    // all-or-nothing, and it does not create the key when the command errors —
    // which is why this sits above `get_or_create_sorted_set`.
    //
    // The pre-pass KEEPS what it decodes (moon#942). It used to throw every
    // `f64` away and let the mutation loop re-run `parse_zadd_pair` over the
    // same bytes — `str::parse::<f64>` twice per pair, where Redis's
    // `zaddGenericCommand` parses once into its own `scores` array.
    //
    // A fixed stack array, not a `SmallVec`: `src/command/` forbids the
    // allocation a spill would make, so a batch that does not fit simply
    // re-parses in the loop exactly as before.
    let pair_count = remaining.len() / 2;
    let mut scores_cache = [0f64; ZADD_INLINE_PAIRS];
    let cached = pair_count <= ZADD_INLINE_PAIRS;
    for (idx, pair) in remaining.chunks_exact(2).enumerate() {
        let [score_arg, member_arg] = pair else {
            // `chunks_exact(2)` yields nothing else; the guard above already
            // rejected an odd tail.
            return err_wrong_args("ZADD");
        };
        match parse_zadd_pair(score_arg, member_arg) {
            Ok((score, _)) => {
                if cached {
                    scores_cache[idx] = score;
                }
            }
            Err(e) => return e,
        }
    }
    // What the two mutation loops below read instead of re-parsing. `None` for
    // an oversized batch, which re-parses exactly as it always did.
    let cache: Option<&[f64]> = if cached {
        Some(&scores_cache[..pair_count])
    } else {
        None
    };

    // Listpack path for small sorted sets (moon#787). Redis keeps a zset in a
    // listpack until it exceeds zset-max-listpack-entries (128) or
    // zset-max-listpack-value (64); moon reported `skiplist` from the first
    // member because no accessor ever produced a surviving
    // `SortedSetListpack`. Only the MEMBER is measured against the value
    // threshold, as in Redis (a score is stored as its rendering, and an
    // extreme one like 1e300 renders long, but that is not what the threshold
    // governs). Verified against a redis 8.6.1 oracle: a 64-byte member is
    // `listpack`, a 65-byte member is `skiplist`.
    //
    // This branch sits BELOW the moon#814 pre-pass and ABOVE
    // `get_or_create_sorted_set`, for the same reason the pre-pass does:
    // every pair is already proven parseable, so nothing in the loop can
    // return from inside the `before … adjust_memory` window with members
    // pushed and never charged, and an erroring ZADD never creates the key.
    // The first version of this branch was inserted ABOVE the pre-pass and
    // re-introduced moon#814 on the listpack path; `ledger_consistency_788`
    // now pins both the ledger and the all-or-nothing reply for it.
    // The entry gate, from the ONE authority (moon#896): the longest MEMBER
    // in the batch against `zset-max-listpack-value`, the batch size against
    // `zset-max-listpack-entries`. The same predicate bounds a batch far
    // below the listpack header's u16 range (moon#865) — the upgrade check
    // runs AFTER the loop below, too late to stop a batch already too big.
    let limits = db.encoding_limits();
    let max_member = remaining
        .chunks_exact(2)
        .map(|pair| extract_bytes(&pair[1]).map_or(0, |m| m.len()))
        .max()
        .unwrap_or(0);
    // `remaining.len()` is listpack ENTRIES (score and member per pair); the
    // policy is in MEMBERS, and the shape converts. Passing the entry count
    // here was moon#896: a bulk ZADD of 65 pairs (argv 130) refused the
    // listpack path that the same zset built one pair at a time stayed on
    // until 128.
    if limits.fits(
        Shape::SortedSet,
        Shape::SortedSet.items_in(remaining.len()),
        max_member,
    ) {
        match db.get_or_create_zset_listpack(key) {
            Ok(Some(lp)) => {
                let mut added = 0i64;
                let mut changed = 0i64;
                // Listpack `estimate_memory()` is O(1) (capacity-based), so a
                // before/after snapshot is cheap — no per-member formula.
                let before = lp.estimate_memory();
                // Does ANYTHING in this command consult the score already
                // stored (moon#942)? A listpack keeps a score as canonical
                // decimal text, so decoding one is a real `str::parse::<f64>`,
                // and the plain `ZADD z <score> <member>` — the benchmark's
                // shape, and most applications' — consults it for nothing:
                // `should_update` is unconditionally true and the `changed`
                // tally it feeds is not what the command replies. `NX` is on
                // this side of the line too, because it refuses on PRESENCE,
                // which `update_pair_value` established by finding the pair.
                let consults_old = ch || gt || lt;
                for (idx, pair) in remaining.chunks_exact(2).enumerate() {
                    let (score, member) = match resolved_pair(pair, idx, cache) {
                        Ok(parsed) => parsed,
                        Err(e) => return e,
                    };

                    // Store the canonical rendering, not the raw argument:
                    // `ZADD z 3.0 m` must answer `ZSCORE` with `3`, and
                    // `render_score` is round-trip exact, so `as_score`
                    // recovers the identical f64. Rendered ONCE per member and
                    // reused by every arm below.
                    let mut rendered = ScoreBuf::new();
                    render_score(score, &mut rendered);

                    // ONE scan (moon#942), decided from what is already there.
                    // `old_score` comes back out through the closure because
                    // the `CH` tally needs it after the write.
                    let mut old_score = 0.0f64;
                    let outcome = lp.update_pair_value(member, |current| {
                        if !consults_old {
                            if nx {
                                // The member is present, which is all NX needs.
                                return None;
                            }
                            // Redis's `zsetAdd` re-inserts only
                            // `if (score != curscore)`. Comparing the RENDERED
                            // bytes decides the same question without decoding
                            // the stored score — and decides it the way moon
                            // already behaved, byte for byte: writing bytes
                            // that are already there changes nothing, so
                            // declining here is observationally identical and
                            // skips an `encode_entry` plus a `write_entry`.
                            //
                            // (Byte equality is not score equality at exactly
                            // one point — `-0` and `0` are different bytes for
                            // scores that compare equal — and taking the BYTE
                            // answer there is what preserves moon's existing
                            // behaviour rather than quietly adopting Redis's.)
                            if current.eq_bytes(&rendered) {
                                return None;
                            }
                            work_budget::note_listpack_score_write();
                            return Some(&rendered[..]);
                        }
                        work_budget::note_stored_score_parse();
                        let old = current.as_score().unwrap_or(0.0);
                        old_score = old;
                        let should_update = if nx {
                            false // NX: never update existing
                        } else if gt && lt {
                            false // GT+LT together: never update
                        } else if gt {
                            score > old
                        } else if lt {
                            score < old
                        } else {
                            true // No flags: always update
                        };
                        if !should_update {
                            return None;
                        }
                        work_budget::note_listpack_score_write();
                        Some(&rendered[..])
                    });

                    match outcome {
                        PairUpdate::Replaced(_) => {
                            // `changed` is only ever REPLIED under `CH`, and
                            // `CH` is on the `consults_old` side, so
                            // `old_score` is the real stored score whenever
                            // this tally can be read.
                            if (old_score - score).abs() > f64::EPSILON {
                                changed += 1;
                            }
                        }
                        // The member is there and either a flag refused the
                        // write or the bytes were already the ones this call
                        // would have written.
                        PairUpdate::Unchanged => {}
                        PairUpdate::Absent => {
                            // New member: add unless XX.
                            if !xx {
                                work_budget::note_listpack_score_write();
                                lp.push_back(member);
                                lp.push_back(&rendered);
                                added += 1;
                                changed += 1;
                            }
                        }
                    }
                }
                let after = lp.estimate_memory();
                // The upgrade check, from the same authority as the gate:
                // it converts `lp.len()` (member AND score entries) to
                // members itself.
                let should_upgrade = !limits.listpack_fits(Shape::SortedSet, lp);
                // Empty means the accessor FABRICATED this container and `XX`
                // then refused every member of the batch — moon reaches the
                // keyspace through `get_or_create_*`, where Redis checks
                // `if (zobj == NULL) { if (xx) goto reply_to_client; }` and
                // creates nothing. `ZREM` already carries this rule, which is
                // why a drained zset never survives; without it here,
                // `ZADD <random> XX 1 m` grows the keyspace without bound and
                // moves `DEBUG DIGEST` away from the master's.
                let is_empty = lp.is_empty();
                // `lp`'s borrow of `db` ends here — safe to call back into
                // `db` for accounting from this point on.
                db.adjust_memory(before, after);
                if is_empty {
                    db.remove(key);
                    // Nothing was added and nothing changed, so both tallies
                    // are zero and the reply is the same either way.
                    return Frame::Integer(0);
                }
                if should_upgrade {
                    // One-time cost-model swing (listpack -> B+tree + members
                    // map). The accessor bills it itself through
                    // `SortedSetKind::upgrade`, so the arena and the table are
                    // charged from their real capacity (moon#788/#810).
                    db.upgrade_zset_listpack_to_bptree(key);
                }
                return if ch {
                    Frame::Integer(changed)
                } else {
                    Frame::Integer(added)
                };
            }
            // Already a full BPTree (or the legacy form): fall through.
            Ok(None) => {}
            Err(e) => return e, // WRONGTYPE
        }
    }

    let (members, scores) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut added = 0i64;
    let mut changed = 0i64;
    // Net O(1) byte charge across all members in this call — see the WS6
    // accounting note above `entry_overhead` in storage/db.rs. A brand-new
    // member costs `zset_member_cost`, which is the member BUFFER only: the
    // `members` table and the B+tree arena are charged separately from their
    // real capacity by the snapshot below. An existing member's score-only
    // update costs nothing (scores are inline `f64`s in slots already billed).
    let mut mem_charge: usize = 0;
    // moon#788: the B+tree arena and the `members` table are charged from
    // their REAL capacity, snapshotted around the mutation (O(1), three
    // `capacity()` reads). The old per-member `+ 80` billed a fictitious
    // 80-byte node against an arena whose slot is ~800 B and whose minimum
    // allocation is four of them.
    let table_before = zset_table_bytes(members, scores);

    for (idx, pair) in remaining.chunks_exact(2).enumerate() {
        let (score, member) = match resolved_pair(pair, idx, cache) {
            Ok(parsed) => parsed,
            Err(e) => return e,
        };

        // ONE hash lookup answers everything this loop asks of an EXISTING
        // member (moon#942): whether it is there, what its score was, and
        // where the new one goes. The flag decision rides inside the closure,
        // so the score never has to be looked up a second time to write it.
        let mut accepted = false;
        let existing_score: Option<f64> = zset_update_existing(members, scores, member, |old| {
            let should_update = if nx {
                false // NX: never update existing
            } else if gt && lt {
                false // GT+LT together: never update (mutually exclusive)
            } else if gt {
                score > old
            } else if lt {
                score < old
            } else {
                true // No flags: always update
            };
            accepted = should_update;
            if should_update { Some(score) } else { None }
        });

        match existing_score {
            Some(old) => {
                // `accepted` is the closure's own decision, read back rather
                // than re-derived: the flag logic has ONE spelling, so the
                // write and the `CH` tally can never disagree about it.
                // `changed` then reports whether the score MOVED, on the same
                // epsilon rule this command has always used.
                if accepted && (old - score).abs() > f64::EPSILON {
                    changed += 1;
                }
            }
            None => {
                // New member: add unless XX.
                if !xx {
                    mem_charge += zset_member_cost(member);
                    zset_insert_absent(members, scores, member.clone(), score);
                    added += 1;
                    changed += 1;
                }
            }
        }
    }

    let table_after = zset_table_bytes(members, scores);
    // Same rule as the listpack arm and as `ZREM`: an empty container here
    // means the accessor fabricated it and `XX` refused the whole batch, and
    // Redis creates nothing in that case. A 65-byte member skips the listpack
    // gate entirely, so this arm leaks a ghost key without it.
    let is_empty = members.is_empty();
    // `members`/`scores`' borrow of `db` ends above.
    db.charge_memory(mem_charge);
    db.adjust_memory(table_before, table_after);
    if is_empty {
        db.remove(key);
        return Frame::Integer(0);
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
        Some(k) => k,
        None => return err_wrong_args("ZREM"),
    };

    // moon#823: refuse a non-argument-shaped frame BEFORE the mutation window
    // opens. The loop below bails on the first one `extract_bytes` rejects,
    // and by then it has already written part of the command — a partial write
    // that is applied on the master and, because propagation is gated on the
    // reply not being an error, never reaches the AOF or a replica.
    if !all_args_are_bytes(&args[1..]) {
        return err_wrong_args("ZREM");
    }

    // Listpack path (moon#897). ZREM used to reach straight for the eager
    // `get_or_create_sorted_set`, which upgrades on ACCESS — so one ZREM
    // flattened a three-member zset to `skiplist`, and because nothing
    // demotes (moon#832) it stayed that way for the key's lifetime. Redis
    // deletes from the listpack in place and never converts on a removal;
    // measured against redis 8.6.1, a 3-member zset is `listpack` on both
    // sides after this branch and was `skiplist` on moon's before it.
    //
    // No entry gate: a removal cannot grow the container, so it cannot cross
    // a threshold. Nor is there an upgrade check afterwards — moon, like
    // Redis, has no demotion, and a listpack that fitted before a ZREM fits
    // after it. The only post-condition is the empty-key delete, which is
    // the same rule the B+tree arm below applies.
    match db.get_or_create_zset_listpack(key) {
        Ok(Some(lp)) => {
            // Listpack `estimate_memory()` is O(1) (capacity-based), so a
            // before/after snapshot is cheap — no per-member formula.
            let before = lp.estimate_memory();
            let mut removed = 0i64;
            for arg in &args[1..] {
                let Some(member) = extract_bytes(arg) else {
                    // Unreachable: `all_args_are_bytes` above proved every
                    // one. Kept as a real match, as the B+tree loop does.
                    return err_wrong_args("ZREM");
                };
                // `remove_pair` matches on the FIELD half only — for a zset
                // listpack that is the member, never the score — and drains
                // both entries in one scan. `ZREM z 7` must not delete the
                // member whose SCORE is 7.
                if lp.remove_pair(member) {
                    removed += 1;
                }
            }
            let after = lp.estimate_memory();
            let is_empty = lp.is_empty();
            // `lp`'s borrow of `db` ends here.
            db.adjust_memory(before, after);
            if is_empty {
                db.remove(key);
            }
            return Frame::Integer(removed);
        }
        // Already the full B+tree form (or a cold-promoted value, which never
        // decodes compact): fall through.
        Ok(None) => {}
        Err(e) => return e, // WRONGTYPE
    }

    let (members, scores) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut removed = 0i64;
    let mut credit: usize = 0;
    let table_before = zset_table_bytes(members, scores);
    for arg in &args[1..] {
        let member = match extract_bytes(arg) {
            Some(b) => b,
            None => return err_wrong_args("ZREM"),
        };
        if zrem_member(members, scores, member) {
            removed += 1;
            credit += zset_member_cost(member);
        }
    }
    let is_empty = members.is_empty();
    let table_after = zset_table_bytes(members, scores);
    // `members`/`scores`' borrow of `db` ends above.
    db.credit_memory(credit);
    // Unconditional, even when the set just went empty: `db.remove` below
    // credits `entry_overhead` recomputed from the CURRENT value, and a
    // hashbrown table's reported `capacity()` shrinks as entries are erased.
    // Skipping the adjust here left the shrink uncredited — measured 1504 B
    // stranded per create/drain cycle, which accumulates without bound.
    db.adjust_memory(table_before, table_after);

    // Remove key if empty
    if is_empty {
        db.remove(key);
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

    // Listpack path (moon#897). ZINCRBY is the leaderboard primitive, and
    // before this it took the eager `get_or_create_sorted_set`: one ZINCRBY
    // flattened a three-member zset to `skiplist` permanently (nothing
    // demotes — moon#832). Redis increments inside the listpack and converts
    // only when a threshold is genuinely crossed.
    //
    // The entry gate comes from the ONE authority (moon#896): ONE logical
    // item, its MEMBER measured against `zset-max-listpack-value`. Only the
    // member is measured, as in Redis and as `zadd` documents — the score is
    // stored as its rendering, and an extreme one renders long, but that is
    // not what the threshold governs. The CARDINALITY bound is enforced by
    // the upgrade check after the mutation, exactly as `zadd` does it, so
    // adding the 129th member promotes.
    let limits = db.encoding_limits();
    if limits.fits(Shape::SortedSet, 1, member.len()) {
        match db.get_or_create_zset_listpack(key) {
            Ok(Some(lp)) => {
                // Listpack `estimate_memory()` is O(1) (capacity-based).
                let before = lp.estimate_memory();
                // ONE scan (moon#942): the walk that finds the member carries
                // the byte offsets its score is rewritten at, so there is no
                // second walk back to an ordinal.
                //
                // moon#863's hazard, stated: a listpack stores the score as
                // its RENDERED text, and `render_score(NaN)` writes `NaN`,
                // which `parse_score` refuses — the score would read back as
                // 0.0 and the member would silently change value. `increment`
                // is already proven non-NaN above, so this is reachable only
                // as `±inf + ∓inf`, which in turn means the member already
                // exists (a fresh member starts at 0.0) — so declining here
                // never leaves a key created-and-abandoned.
                //
                // Declining hands the case to the B+tree arm below, which is
                // byte-for-byte what EVERY ZINCRBY did before this branch
                // existed. moon's reply there (`NaN`) diverges from redis
                // 8.6.1, which answers
                // `ERR resulting score is not a number (NaN)` and leaves the
                // score untouched — a real, PRE-EXISTING divergence that this
                // change deliberately does not alter, and that a NaN must
                // never reach a listpack in the meantime.
                let outcome = lp.update_pair_value(&member, |current| {
                    work_budget::note_stored_score_parse();
                    let new_score = current.as_score().unwrap_or(0.0) + increment;
                    if new_score.is_nan() {
                        return None;
                    }
                    // One stack buffer; `render_score` is byte-identical to
                    // `format_score_bytes`, so the stored text and the reply
                    // below agree, and `render_score -> parse_score` is exact
                    // (`storage::zset_score`), so ZSCORE recovers this f64.
                    let mut rendered = ScoreBuf::new();
                    render_score(new_score, &mut rendered);
                    Some(rendered)
                });

                let stored = match outcome {
                    // The bytes handed back are the ones written, so the
                    // reply below does not render the score a second time.
                    PairUpdate::Replaced(rendered) => Some(rendered),
                    PairUpdate::Absent => {
                        // A fresh member starts at 0.0 and `increment` is
                        // already proven non-NaN, so this rendering can never
                        // be the NaN the arm above guards against.
                        let mut rendered = ScoreBuf::new();
                        render_score(increment, &mut rendered);
                        lp.push_back(&member);
                        lp.push_back(&rendered);
                        Some(rendered)
                    }
                    // NaN. The listpack was not touched, and this is the
                    // answer (moon#960): redis 8.6.1 replies
                    // `ERR resulting score is not a number (NaN)` and leaves
                    // the score alone. Returning here rather than falling
                    // through also keeps the encoding intact — the B+tree arm
                    // below opens with the EAGER `get_or_create_sorted_set`,
                    // so falling through would flatten the listpack
                    // permanently (moon#832: nothing demotes) as a side
                    // effect of a command that errors and stores nothing.
                    PairUpdate::Unchanged => {
                        return err("ERR resulting score is not a number (NaN)");
                    }
                };

                if let Some(rendered) = stored {
                    let after = lp.estimate_memory();
                    // The upgrade check, from the same authority as the gate:
                    // it converts `lp.len()` (member AND score entries) to
                    // members itself — the moon#896 unit.
                    let should_upgrade = !limits.listpack_fits(Shape::SortedSet, lp);
                    // `lp`'s borrow of `db` ends here.
                    db.adjust_memory(before, after);
                    if should_upgrade {
                        // Self-accounting: the accessor bills the one-time
                        // listpack -> B+tree swing itself (moon#788/#810).
                        db.upgrade_zset_listpack_to_bptree(key);
                    }
                    // Reply with the bytes we STORED, not a second rendering:
                    // one copy out of the stack buffer instead of the
                    // `format_score` -> `String` allocation the B+tree arm
                    // below still pays (`src/command/` is a no-`String`
                    // path). `render_score` is pinned byte-identical to
                    // `format_score_bytes` by
                    // `listpack_score_rendering_matches_zscore_rendering`, so
                    // this is the same text either way — and it is now the
                    // same text a later ZSCORE reads out of the listpack, by
                    // construction rather than by two formatters agreeing.
                    return Frame::BulkString(Bytes::copy_from_slice(&rendered));
                }
            }
            // Already the full B+tree form (or a cold-promoted value, which
            // never decodes compact): fall through.
            Ok(None) => {}
            Err(e) => return e, // WRONGTYPE
        }
    }

    let (members, scores) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let member_cost = zset_member_cost(&member);
    let table_before = zset_table_bytes(members, scores);
    // ONE hash lookup (moon#942): the same lookup that reads the current score
    // writes `current + increment` back through the slot it found. It used to
    // be `members.get` followed by `zadd_member`'s own `remove` and `insert` —
    // three hashes of one member for one command.
    let mut new_score = increment;
    // moon#960. `increment` is already proven non-NaN above, so the only way
    // here is `±inf + ∓inf` — which means the member exists. Returning `None`
    // from the closure is `zset_update_existing`'s "leave it alone", so the
    // old score survives and neither map is touched.
    let mut reached_nan = false;
    let is_new = zset_update_existing(members, scores, &member, |current| {
        let candidate = current + increment;
        if candidate.is_nan() {
            reached_nan = true;
            return None;
        }
        new_score = candidate;
        Some(candidate)
    })
    .is_none();
    if reached_nan {
        return err("ERR resulting score is not a number (NaN)");
    }
    if is_new {
        // A member that was not there starts at 0.0, so its new score is the
        // increment itself — already in `new_score`.
        zset_insert_absent(members, scores, member, new_score);
    }
    let table_after = zset_table_bytes(members, scores);
    // `members`/`scores`' borrow of `db` ends above.
    if is_new {
        db.charge_memory(member_cost);
    }
    db.adjust_memory(table_before, table_after);

    Frame::BulkString(Bytes::from(format_score(new_score)))
}

/// ZPOPMIN key [count]
pub fn zpopmin(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("ZPOPMIN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
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

    let (members, scores) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut result = Vec::new();
    let mut credit: usize = 0;
    let table_before = zset_table_bytes(members, scores);
    for _ in 0..count {
        let first = scores.iter().next().map(|(s, m)| (s, m.clone()));
        match first {
            Some((score, member)) => {
                scores.remove(score, &member);
                members.remove(&member);
                credit += zset_member_cost(&member);
                result.push(Frame::BulkString(member));
                result.push(Frame::BulkString(Bytes::from(format_score(score.0))));
            }
            None => break,
        }
    }
    let is_empty = members.is_empty();
    let table_after = zset_table_bytes(members, scores);
    // `members`/`scores`' borrow of `db` ends above.
    db.credit_memory(credit);
    // Unconditional, even when the set just went empty: `db.remove` below
    // credits `entry_overhead` recomputed from the CURRENT value, and a
    // hashbrown table's reported `capacity()` shrinks as entries are erased.
    // Skipping the adjust here left the shrink uncredited — measured 1504 B
    // stranded per create/drain cycle, which accumulates without bound.
    db.adjust_memory(table_before, table_after);

    // Remove key if empty
    if is_empty {
        db.remove(key);
    }

    Frame::Array(result.into())
}

/// ZPOPMAX key [count]
pub fn zpopmax(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("ZPOPMAX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
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

    let (members, scores) = match db.get_or_create_sorted_set(key) {
        Ok(pair) => pair,
        Err(e) => return e,
    };

    let mut result = Vec::new();
    let mut credit: usize = 0;
    let table_before = zset_table_bytes(members, scores);
    for _ in 0..count {
        let last = scores.iter_rev().next().map(|(s, m)| (s, m.clone()));
        match last {
            Some((score, member)) => {
                scores.remove(score, &member);
                members.remove(&member);
                credit += zset_member_cost(&member);
                result.push(Frame::BulkString(member));
                result.push(Frame::BulkString(Bytes::from(format_score(score.0))));
            }
            None => break,
        }
    }
    let is_empty = members.is_empty();
    let table_after = zset_table_bytes(members, scores);
    // `members`/`scores`' borrow of `db` ends above.
    db.credit_memory(credit);
    // Unconditional, even when the set just went empty: `db.remove` below
    // credits `entry_overhead` recomputed from the CURRENT value, and a
    // hashbrown table's reported `capacity()` shrinks as entries are erased.
    // Skipping the adjust here left the shrink uncredited — measured 1504 B
    // stranded per create/drain cycle, which accumulates without bound.
    db.adjust_memory(table_before, table_after);

    // Remove key if empty
    if is_empty {
        db.remove(key);
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
        Some(k) => k,
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

// ---------------------------------------------------------------------------
// ZMPOP numkeys key [key ...] MIN|MAX [COUNT n]
// ---------------------------------------------------------------------------

/// ZMPOP numkeys key [key ...] MIN|MAX [COUNT n]
///
/// Pops elements from the first non-empty sorted set. Returns [key, [[m, s], ...]].
pub fn zmpop(db: &mut Database, args: &[Frame]) -> Frame {
    use crate::framevec;
    if args.len() < 3 {
        return err_wrong_args("ZMPOP");
    }
    let numkeys_bytes = match extract_bytes(&args[0]) {
        Some(b) => b,
        None => return err_wrong_args("ZMPOP"),
    };
    let numkeys: usize = match std::str::from_utf8(numkeys_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(n) if n > 0 => n,
        _ => return err("ERR numkeys can't be non-positive value"),
    };

    if args.len() < 1 + numkeys + 1 {
        return err_wrong_args("ZMPOP");
    }

    let keys: Vec<Bytes> = (0..numkeys)
        .map(|j| {
            extract_bytes(&args[1 + j])
                .cloned()
                .unwrap_or_else(Bytes::new)
        })
        .collect();

    // Parse MIN|MAX
    let direction_bytes = match extract_bytes(&args[1 + numkeys]) {
        Some(b) => b.as_ref(),
        None => return err_wrong_args("ZMPOP"),
    };
    let is_min = if direction_bytes.eq_ignore_ascii_case(b"MIN") {
        true
    } else if direction_bytes.eq_ignore_ascii_case(b"MAX") {
        false
    } else {
        return err("ERR syntax error");
    };

    // Parse optional COUNT
    let mut pop_count: usize = 1;
    let mut i = 2 + numkeys;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"COUNT") {
            if i + 1 >= args.len() {
                return err_wrong_args("ZMPOP");
            }
            let cb = match extract_bytes(&args[i + 1]) {
                Some(b) => b,
                None => return err_wrong_args("ZMPOP"),
            };
            pop_count = match std::str::from_utf8(cb).ok().and_then(|s| s.parse().ok()) {
                Some(c) if c > 0 => c,
                _ => return err("ERR value is not an integer or out of range"),
            };
            i += 2;
        } else {
            i += 1;
        }
    }

    // Iterate keys, find first non-empty
    for key in &keys {
        let card = match db.get_sorted_set(key) {
            Ok(Some((members, _))) => members.len(),
            Ok(None) => 0,
            Err(e) => return e,
        };
        if card == 0 {
            continue;
        }

        let (members, scores) = match db.get_or_create_sorted_set(key) {
            Ok(pair) => pair,
            Err(e) => return e,
        };

        // DoS guard: bound the pre-size by the set's cardinality so a huge
        // COUNT can't drive an unbounded Vec::with_capacity -> allocator abort
        // (matches LMPOP's count.min(list_len); loop already breaks when empty).
        let mut popped = Vec::with_capacity(pop_count.min(card));
        let mut credit: usize = 0;
        let table_before = zset_table_bytes(members, scores);
        for _ in 0..pop_count {
            let entry = if is_min {
                scores.iter().next().map(|(s, m)| (s, m.clone()))
            } else {
                scores.iter_rev().next().map(|(s, m)| (s, m.clone()))
            };
            match entry {
                Some((score, member)) => {
                    scores.remove(score, &member);
                    members.remove(&member);
                    credit += zset_member_cost(&member);
                    popped.push(Frame::Array(framevec![
                        Frame::BulkString(member),
                        Frame::BulkString(format_score_bytes(score.0)),
                    ]));
                }
                None => break,
            }
        }
        let is_empty = members.is_empty();
        let table_after = zset_table_bytes(members, scores);
        // `members`/`scores`' borrow of `db` ends above.
        db.credit_memory(credit);
        // Unconditional — see the note in `zrem`.
        db.adjust_memory(table_before, table_after);

        if is_empty {
            db.remove(key);
        }

        return Frame::Array(framevec![
            Frame::BulkString(key.clone()),
            Frame::Array(popped.into()),
        ]);
    }

    // No key held anything: ZMPOP's miss is a null ARRAY (moon#482).
    Frame::NullArray
}
