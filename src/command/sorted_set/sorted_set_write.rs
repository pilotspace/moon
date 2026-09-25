use bytes::Bytes;
use ordered_float::OrderedFloat;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{Shape, SortedSetRef, zset_member_cost, zset_table_bytes};
use crate::storage::listpack::PairUpdate;
use crate::storage::owned_bytes::detach;
use crate::storage::zset_score::{ScoreBuf, render_score};

use crate::command::helpers::{all_args_are_bytes, err, err_wrong_args, extract_bytes};
use crate::command::sorted_set::work_budget;

use super::{
    LexBound, ScoreBound, format_score, format_score_bytes, lex_in_range, parse_bounded_count,
    parse_lex_bound, parse_score_bound, rank_window, zrem_member, zset_insert_absent,
    zset_update_existing,
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

/// ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
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
    let mut incr = false;
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
        } else if arg.eq_ignore_ascii_case(b"INCR") {
            incr = true;
            i += 1;
        } else {
            break;
        }
    }

    // NX and XX are mutually exclusive
    if nx && xx {
        return err("ERR XX and NX options at the same time are not compatible");
    }
    // GT, LT and NX are pairwise incompatible (moon#969). The old guard only
    // caught `nx && (gt || lt)`, so `GT LT` was ACCEPTED and then silently did
    // nothing — Redis's `zaddGenericCommand` rejects all three pairings, and
    // its message says "and/or".
    if (gt && nx) || (lt && nx) || (gt && lt) {
        return err("ERR GT, LT, and/or NX options at the same time are not compatible");
    }

    // Remaining args must be score member pairs
    let remaining = &args[i..];
    if remaining.is_empty() {
        return err_wrong_args("ZADD");
    }
    // An ODD tail is a different class from NO tail (moon#969): Redis fails
    // `ZADD k 1 a 2` in `zaddGenericCommand` with `syntax error`, and only a
    // command with no pairs at all (`ZADD k NX`) trips `commandCheckArity`.
    if !remaining.len().is_multiple_of(2) {
        return err("ERR syntax error");
    }

    // `INCR` (moon#959): ZINCRBY's arithmetic under ZADD's flags, replying the
    // new score as a bulk string, or nil when a flag refused the write. Redis
    // checks the pair count AFTER the parity and flag-pairing rules above, so
    // `ZADD k INCR 1` is `syntax error` and `ZADD k INCR NX XX 1 a 2 b` is the
    // NX/XX error — both verified on redis 8.6.1. The increment is parsed by
    // the ONE parser every `score member` pair goes through, so a NaN or a
    // non-float is refused with `ZADD`'s own message before the keyspace is
    // touched.
    if incr {
        if remaining.len() != 2 {
            return err("ERR INCR option supports a single increment-element pair");
        }
        let (increment, member) = match parse_zadd_pair(&remaining[0], &remaining[1]) {
            Ok(pair) => pair,
            Err(e) => return e,
        };
        // `CH` has no effect on the INCR reply, as on Redis.
        return zincr_member(db, key, increment, member, IncrFlags { nx, xx, gt, lt });
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
                        // `gt && lt` no longer reaches here: the guard above
                        // rejects that pairing outright (moon#969). The arm
                        // that used to sit between `nx` and `gt` answered
                        // `false` — a SILENT no-op for a command Redis
                        // refuses — and is gone rather than left unreachable,
                        // so relaxing the guard cannot quietly resurrect it.
                        let should_update = if nx {
                            false // NX: never update existing
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
                            //
                            // moon#792: EXACTLY, as Redis's `zsetAdd` does
                            // (`if (score != curscore)`). An absolute
                            // `f64::EPSILON` window called any move smaller
                            // than ~2.2e-16 "unchanged" REGARDLESS of
                            // magnitude, so rescoring 1e-10 to 1.0000001e-10 —
                            // a change of six significant figures — replied 0
                            // while the stored score really did move, and the
                            // next read disagreed with the reply. Neither side
                            // can be NaN: `parse_zadd_pair` rejects a NaN
                            // score, so `!=` is total here.
                            if old_score != score {
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
            // The second of the two `gt && lt` fallthroughs moon#969 left
            // standing (the listpack arm has the other). The guard at the top
            // of `zadd` now rejects the pairing, so this arm was both dead and
            // wrong; it is removed rather than left unreachable.
            let should_update = if nx {
                false // NX: never update existing
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
                //
                // moon#792, the B+tree half: `changed` reports whether the
                // score MOVED, compared EXACTLY as Redis's `zsetAdd` does. The
                // old absolute `f64::EPSILON` window disagreed with
                // `zset_update_existing`, which decides on `to_bits()` — so a
                // sub-epsilon rescore really was written to both structures
                // and then reported as no change. Neither side can be NaN
                // (`parse_zadd_pair` rejects a NaN score).
                if accepted && old != score {
                    changed += 1;
                }
            }
            None => {
                // New member: add unless XX.
                if !xx {
                    mem_charge += zset_member_cost(member);
                    // moon#1160: an exact-size copy, not the request's slice.
                    zset_insert_absent(members, scores, detach(member), score);
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

// ---------------------------------------------------------------------------
// ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX (moon#959)
// ---------------------------------------------------------------------------

/// The window a `ZREMRANGEBY*` command deletes, parsed BEFORE the keyspace is
/// touched so a bad bound never fabricates or reclaims a key — Redis parses
/// the range first and only then looks the key up, so `ZREMRANGEBYSCORE
/// nokey a 1` is `min or max is not a float` and not `0`.
enum RemRange {
    Rank(i64, i64),
    Score(ScoreBound, ScoreBound),
    Lex(LexBound, LexBound),
}

impl RemRange {
    /// The members of a score-sorted decode that fall inside the window.
    /// Borrowed from `entries`, which is the caller's own copy, so the
    /// listpack they came from can be mutated while these are consumed.
    fn select<'a>(&self, entries: &'a [(Bytes, f64)]) -> Vec<&'a Bytes> {
        match self {
            RemRange::Rank(start, stop) => match rank_window(*start, *stop, entries.len()) {
                Some((lo, hi)) => entries[lo..=hi].iter().map(|(m, _)| m).collect(),
                None => Vec::new(),
            },
            RemRange::Score(min, max) => entries
                .iter()
                .filter(|(_, s)| min.includes(*s) && max.includes_upper(*s))
                .map(|(m, _)| m)
                .collect(),
            RemRange::Lex(min, max) => entries
                .iter()
                .filter(|(m, _)| lex_in_range(m, min, max))
                .map(|(m, _)| m)
                .collect(),
        }
    }
}

/// ZREMRANGEBYRANK key start stop
pub fn zremrangebyrank(db: &mut Database, args: &[Frame]) -> Frame {
    let (key, min_b, max_b) = match zremrange_args(args, "ZREMRANGEBYRANK") {
        Ok(v) => v,
        Err(e) => return e,
    };
    // A rank index is read with a NULL message on Redis, so the generic
    // integer error is the right class here (moon#969 documents the same for
    // ZRANGE's indices).
    let parse = |b: &[u8]| -> Result<i64, Frame> {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| err("ERR value is not an integer or out of range"))
    };
    let (start, stop) = match (parse(min_b), parse(max_b)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    zremrange_impl(db, key, RemRange::Rank(start, stop))
}

/// ZREMRANGEBYSCORE key min max
pub fn zremrangebyscore(db: &mut Database, args: &[Frame]) -> Frame {
    let (key, min_b, max_b) = match zremrange_args(args, "ZREMRANGEBYSCORE") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (min, max) = match (parse_score_bound(min_b), parse_score_bound(max_b)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    zremrange_impl(db, key, RemRange::Score(min, max))
}

/// ZREMRANGEBYLEX key min max
pub fn zremrangebylex(db: &mut Database, args: &[Frame]) -> Frame {
    let (key, min_b, max_b) = match zremrange_args(args, "ZREMRANGEBYLEX") {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (min, max) = match (parse_lex_bound(min_b), parse_lex_bound(max_b)) {
        (Ok(a), Ok(b)) => (a, b),
        (Err(e), _) | (_, Err(e)) => return e,
    };
    zremrange_impl(db, key, RemRange::Lex(min, max))
}

/// The `key min max` shape all three share: exactly three arguments (their
/// registered arity is 4), every one a bulk string.
fn zremrange_args<'a>(
    args: &'a [Frame],
    cmd: &'static str,
) -> Result<(&'a Bytes, &'a Bytes, &'a Bytes), Frame> {
    if args.len() != 3 {
        return Err(err_wrong_args(cmd));
    }
    match (
        extract_bytes(&args[0]),
        extract_bytes(&args[1]),
        extract_bytes(&args[2]),
    ) {
        (Some(k), Some(min), Some(max)) => Ok((k, min, max)),
        _ => Err(err_wrong_args(cmd)),
    }
}

/// Delete every member inside `range` and reply how many went.
///
/// The same two-arm shape as `zrem` (moon#897): a listpack is trimmed in
/// place and never converted — a removal cannot cross a threshold upward —
/// and the B+tree arm credits each member's cost and the table shrink exactly
/// as `zrem` does. A key that drains to empty is removed on both arms, which
/// is also what reclaims the empty container `get_or_create_zset_listpack`
/// fabricates for a missing key, so `ZREMRANGEBYRANK nokey 0 1` answers `0`
/// and leaves no key behind.
///
/// The victims are materialised before the first removal on both arms: a
/// listpack keeps insertion order, so the window is decided on a score-sorted
/// decode (bounded by `zset-max-listpack-entries`), and a B+tree cannot be
/// mutated while its iterator is live. The B+tree list holds `Bytes` handles,
/// which are reference-count bumps rather than copies.
fn zremrange_impl(db: &mut Database, key: &[u8], range: RemRange) -> Frame {
    match db.get_or_create_zset_listpack(key) {
        Ok(Some(lp)) => {
            // Listpack `estimate_memory()` is O(1) (capacity-based).
            let before = lp.estimate_memory();
            let entries = SortedSetRef::Listpack(&*lp).entries_sorted();
            let mut removed = 0i64;
            for member in range.select(&entries) {
                // `remove_pair` matches the FIELD half only — the member,
                // never the score — and drains both entries in one scan.
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

    let victims: Vec<Bytes> = match &range {
        RemRange::Rank(start, stop) => match rank_window(*start, *stop, scores.len()) {
            Some((lo, hi)) => scores
                .range_by_rank(lo, hi)
                .into_iter()
                .map(|(_, m)| m.clone())
                .collect(),
            None => Vec::new(),
        },
        RemRange::Score(min, max) => {
            // `BPTree::range` wants `lo <= hi`; a reversed pair is an empty
            // window on Redis (`ZREMRANGEBYSCORE k 3 1` removes nothing), and
            // the bound filters keep an exclusive or infinite edge exact.
            let lo = OrderedFloat(min.value());
            let hi = OrderedFloat(max.value());
            if lo > hi {
                Vec::new()
            } else {
                scores
                    .range(lo, hi)
                    .filter(|(s, _)| min.includes(s.0) && max.includes_upper(s.0))
                    .map(|(_, m)| m.clone())
                    .collect()
            }
        }
        RemRange::Lex(min, max) => scores
            .iter()
            .filter(|(_, m)| lex_in_range(m, min, max))
            .map(|(_, m)| m.clone())
            .collect(),
    };

    let mut removed = 0i64;
    let mut credit: usize = 0;
    let table_before = zset_table_bytes(members, scores);
    for member in &victims {
        if zrem_member(members, scores, member) {
            removed += 1;
            credit += zset_member_cost(member);
        }
    }
    let is_empty = members.is_empty();
    let table_after = zset_table_bytes(members, scores);
    // `members`/`scores`' borrow of `db` ends above.
    db.credit_memory(credit);
    // Unconditional, as in `zrem`: `db.remove` credits `entry_overhead`
    // recomputed from the CURRENT value, and a shrunken table's capacity
    // must be credited here or it is stranded.
    db.adjust_memory(table_before, table_after);
    if is_empty {
        db.remove(key);
    }
    Frame::Integer(removed)
}

// ---------------------------------------------------------------------------
// ZINCRBY, and the arithmetic core it shares with `ZADD ... INCR`
// ---------------------------------------------------------------------------

/// The `ZADD` flags that bear on an increment (moon#959). All false for a
/// plain `ZINCRBY`.
#[derive(Debug, Clone, Copy, Default)]
struct IncrFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
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
        Some(b) => b,
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

    zincr_member(db, key, increment, member, IncrFlags::default())
}

/// Add `increment` to `member`'s score, creating the member at `increment`
/// when it is absent, and reply the new score — or nil when a flag refused
/// the write.
///
/// The decision order is Redis's `zsetAdd` with `ZADD_IN_INCR`, verified on
/// redis 8.6.1: for a PRESENT member, `NX` refuses before the sum is even
/// formed; then a NaN sum is `ERR resulting score is not a number (NaN)` with
/// nothing written; then `GT`/`LT` refuse a sum that does not move the score
/// the right way (`GT` with a zero increment is a refusal). For an ABSENT
/// member only `XX` refuses; `GT`/`LT` never block a first insert. A refusal
/// on a key this call had to fabricate leaves no key behind.
fn zincr_member(
    db: &mut Database,
    key: &[u8],
    increment: f64,
    member: &Bytes,
    flags: IncrFlags,
) -> Frame {
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
                // Why the closure declined, when it did. `Unchanged` alone
                // cannot say: it is a NaN sum for a plain ZINCRBY and a flag
                // refusal under `ZADD ... INCR`, and the two reply
                // differently.
                let mut reached_nan = false;
                let mut refused = false;
                // ONE scan (moon#942): the walk that finds the member carries
                // the byte offsets its score is rewritten at, so there is no
                // second walk back to an ordinal.
                //
                // moon#863's hazard, stated: a listpack stores the score as
                // its RENDERED text, and `render_score(NaN)` writes `NaN`,
                // which `parse_score` refuses — the score would read back as
                // 0.0 and the member would silently change value. `increment`
                // is already proven non-NaN by every caller, so this is
                // reachable only as `±inf + ∓inf`, which in turn means the
                // member already exists (a fresh member starts at 0.0) — so
                // declining here never leaves a key created-and-abandoned.
                let outcome = lp.update_pair_value(member, |current| {
                    if flags.nx {
                        // NX: the member is present, which is all it needs.
                        refused = true;
                        return None;
                    }
                    work_budget::note_stored_score_parse();
                    let old = current.as_score().unwrap_or(0.0);
                    let new_score = old + increment;
                    if new_score.is_nan() {
                        reached_nan = true;
                        return None;
                    }
                    if (flags.gt && new_score <= old) || (flags.lt && new_score >= old) {
                        refused = true;
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
                        if flags.xx {
                            // XX: never create. The container may be one
                            // this call fabricated; the empty check below
                            // reclaims it.
                            refused = true;
                            None
                        } else {
                            // A fresh member starts at 0.0 and `increment` is
                            // already proven non-NaN, so this rendering can
                            // never be the NaN the arm above guards against.
                            let mut rendered = ScoreBuf::new();
                            render_score(increment, &mut rendered);
                            lp.push_back(member);
                            lp.push_back(&rendered);
                            Some(rendered)
                        }
                    }
                    // NaN, or a flag refusal: the listpack was not touched.
                    PairUpdate::Unchanged => None,
                };

                let after = lp.estimate_memory();
                // The upgrade check, from the same authority as the gate:
                // it converts `lp.len()` (member AND score entries) to
                // members itself — the moon#896 unit.
                let should_upgrade =
                    stored.is_some() && !limits.listpack_fits(Shape::SortedSet, lp);
                let is_empty = lp.is_empty();
                // `lp`'s borrow of `db` ends here.
                db.adjust_memory(before, after);
                if reached_nan {
                    // This is the answer (moon#960): redis 8.6.1 replies
                    // `ERR resulting score is not a number (NaN)` and leaves
                    // the score alone. Returning here rather than falling
                    // through also keeps the encoding intact — the B+tree arm
                    // below opens with the EAGER `get_or_create_sorted_set`,
                    // so falling through would flatten the listpack
                    // permanently (moon#832: nothing demotes) as a side
                    // effect of a command that errors and stores nothing.
                    return err("ERR resulting score is not a number (NaN)");
                }
                if is_empty {
                    // Only reachable as an `XX` refusal on a fabricated
                    // container — the same rule `zadd` applies.
                    db.remove(key);
                }
                if refused {
                    return Frame::Null;
                }
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
                if let Some(rendered) = stored {
                    return Frame::BulkString(Bytes::copy_from_slice(&rendered));
                }
                // `stored` is `None` exactly when `reached_nan || refused`,
                // both returned above. Kept as a real match rather than an
                // `unwrap`, as the mutation loops in `zadd` are.
                return Frame::Null;
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

    let member_cost = zset_member_cost(member);
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
    let mut refused = false;
    let is_new = zset_update_existing(members, scores, member, |current| {
        if flags.nx {
            refused = true;
            return None;
        }
        let candidate = current + increment;
        if candidate.is_nan() {
            reached_nan = true;
            return None;
        }
        if (flags.gt && candidate <= current) || (flags.lt && candidate >= current) {
            refused = true;
            return None;
        }
        new_score = candidate;
        Some(candidate)
    })
    .is_none();
    if reached_nan {
        return err("ERR resulting score is not a number (NaN)");
    }
    let inserted = is_new && !flags.xx;
    if inserted {
        // A member that was not there starts at 0.0, so its new score is the
        // increment itself — already in `new_score`.
        // moon#1160: an exact-size copy, not the request's slice.
        zset_insert_absent(members, scores, detach(member), new_score);
    }
    let is_empty = members.is_empty();
    let table_after = zset_table_bytes(members, scores);
    // `members`/`scores`' borrow of `db` ends above.
    if inserted {
        db.charge_memory(member_cost);
    }
    db.adjust_memory(table_before, table_after);
    if is_empty {
        // `XX` refused the only member a fabricated container would have had.
        db.remove(key);
    }
    if refused || (is_new && flags.xx) {
        return Frame::Null;
    }

    Frame::BulkString(format_score_bytes(new_score))
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
        // moon#969: Redis reads this with `getPositiveLongFromObject`, which
        // carries its OWN message for every failure — non-numeric and
        // negative alike. moon answered the generic integer error, a
        // different exception type in every client that maps them.
        match parse_bounded_count(
            count_bytes,
            0,
            "ERR value is out of range, must be positive",
        ) {
            Ok(c) => c,
            Err(e) => return e,
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
        // moon#969: Redis reads this with `getPositiveLongFromObject`, which
        // carries its OWN message for every failure — non-numeric and
        // negative alike. moon answered the generic integer error, a
        // different exception type in every client that maps them.
        match parse_bounded_count(
            count_bytes,
            0,
            "ERR value is out of range, must be positive",
        ) {
            Ok(c) => c,
            Err(e) => return e,
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
    // ZMPOP does NOT take the two-class split the ZUNIONSTORE family takes:
    // Redis reads it with `getRangeLongFromObject(…, 1, LONG_MAX, …,
    // "numkeys should be greater than 0")`, one message for every failure
    // (moon#969). Verified against redis-server 8.6.1, including `notanint`.
    let numkeys =
        match parse_bounded_count(numkeys_bytes, 1, "ERR numkeys should be greater than 0") {
            Ok(n) => n,
            Err(e) => return e,
        };

    if args.len() < 1 + numkeys + 1 {
        return err("ERR syntax error");
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
            // A dangling `COUNT` is `syntax error` (moon#969), the same class
            // the bare-token arm below already answers.
            if i + 1 >= args.len() {
                return err("ERR syntax error");
            }
            let cb = match extract_bytes(&args[i + 1]) {
                Some(b) => b,
                None => return err("ERR syntax error"),
            };
            pop_count = match parse_bounded_count(cb, 1, "ERR count should be greater than 0") {
                Ok(c) => c,
                Err(e) => return e,
            };
            i += 2;
        } else {
            // moon#967. ZMPOP MUTATES, so argument validation has to complete
            // before anything is popped: `ZMPOP 1 k MIN MAX` used to take the
            // first direction and pop, leaving the caller with a keyspace
            // change from a command Redis rejects outright.
            return err("ERR syntax error");
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
