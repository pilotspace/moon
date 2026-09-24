mod sorted_set_lex;
mod sorted_set_read;
mod sorted_set_store;
mod sorted_set_write;
mod work_budget;

pub use sorted_set_lex::*;
pub use sorted_set_read::*;
pub use sorted_set_store::*;
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

/// Format a float score for Redis output, exactly as `ZSCORE` replies it.
///
/// Delegates to [`crate::storage::zset_score::render_score`] (moon#942). The
/// two used to be independent transcriptions of the same three rules, and
/// `listpack_score_rendering_matches_zscore_rendering` existed to catch them
/// drifting apart — a test that can only ever notice the drift AFTER a client
/// has read a score back differently from a listpack than from a B+tree.
/// There is now ONE renderer, so they agree by construction, and that test
/// keeps standing as the guard on this delegation.
pub(super) fn format_score(score: f64) -> String {
    let mut buf = crate::storage::zset_score::ScoreBuf::new();
    crate::storage::zset_score::render_score(score, &mut buf);
    // `render_score` writes ASCII only — `inf`, `-inf`, an `itoa` rendering,
    // or `core::fmt`'s `f64` Display — so the fallback is unreachable. One
    // allocation, the same count `format!` made.
    std::str::from_utf8(&buf).unwrap_or("0").to_owned()
}

/// `Bytes` version of [`format_score`], for the reply paths that want no
/// intermediate `String`.
pub(crate) fn format_score_bytes(score: f64) -> Bytes {
    if score == f64::INFINITY {
        return Bytes::from_static(b"inf");
    }
    if score == f64::NEG_INFINITY {
        return Bytes::from_static(b"-inf");
    }
    let mut buf = crate::storage::zset_score::ScoreBuf::new();
    crate::storage::zset_score::render_score(score, &mut buf);
    Bytes::copy_from_slice(&buf)
}

/// Aggregate operation for ZUNION/ZINTER/ZUNIONSTORE/ZINTERSTORE.
#[derive(Debug, Clone, Copy)]
pub(super) enum AggregateOp {
    Sum,
    Min,
    Max,
}

// ---------------------------------------------------------------------------
// Argument validation shared by the read and write halves (moon#969)
// ---------------------------------------------------------------------------
//
// The error CLASS matters beyond the wording. redis-py maps each class to a
// distinct exception type, so a client that branches on the exception takes
// the WRONG branch when moon answers an arity error where Redis answers a
// syntax error — and retries a request that can never succeed.

/// `ERR at least 1 input key is needed for '<cmd>' command`.
///
/// Redis interpolates the command's registered (lower-case) name here, exactly
/// as `err_wrong_args` does for the arity message and for the same reason:
/// clients string-match the result.
fn err_at_least_one_key(cmd: &str) -> Frame {
    const PREFIX: &str = "ERR at least 1 input key is needed for '";
    const SUFFIX: &str = "' command";
    let mut msg = String::with_capacity(PREFIX.len() + cmd.len() + SUFFIX.len());
    msg.push_str(PREFIX);
    msg.extend(cmd.chars().map(|c| c.to_ascii_lowercase()));
    msg.push_str(SUFFIX);
    Frame::Error(Bytes::from(msg))
}

/// The `numkeys` contract of the set-operation family — ZUNIONSTORE,
/// ZINTERSTORE, ZUNION, ZINTER, ZDIFF, ZINTERCARD.
///
/// Redis's `zunionInterDiffGenericCommand` answers in TWO classes, and the
/// split is the whole point: `getLongFromObjectOrReply(…, NULL)` reports
/// `ERR value is not an integer or out of range` for bytes that are not a
/// number, and only then does `if (setnum < 1)` report `at least 1 input key
/// is needed …`. Parsing straight into a `usize` collapsed the two — `-1` came
/// back as the integer error, and `0` came back as an ARITY error, a third
/// class again. Verified against redis-server 8.6.1.
pub(super) fn parse_numkeys(arg: &[u8], cmd: &str) -> Result<usize, Frame> {
    let n: i64 = match std::str::from_utf8(arg).ok().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return Err(err("ERR value is not an integer or out of range")),
    };
    if n < 1 {
        return Err(err_at_least_one_key(cmd));
    }
    Ok(n as usize)
}

/// A count argument Redis reads with `getRangeLongFromObject` /
/// `getPositiveLongFromObject`: ONE bespoke message for EVERY failure, whether
/// the bytes were not a number at all or the number was below `min`.
///
/// Deliberately the opposite shape to [`parse_numkeys`]. ZPOPMIN/ZPOPMAX's
/// `count`, ZMPOP's `numkeys` and `COUNT`, and ZINTERCARD's `LIMIT` each carry
/// a message Redis hands to that one call site; moon answered the generic
/// integer error at all of them.
///
/// Note what this is NOT for. A ZRANGE **rank index**, or a
/// `LIMIT offset count` pair, is read with `NULL` as the message, so the
/// generic `ERR value is not an integer or out of range` is the CORRECT reply
/// there — and those sites accept negatives. They are already right and do not
/// come through here (moon#969 cites them; the oracle says otherwise).
pub(super) fn parse_bounded_count(arg: &[u8], min: i64, msg: &str) -> Result<usize, Frame> {
    match std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= min => Ok(n as usize),
        _ => Err(err(msg)),
    }
}

// ---------------------------------------------------------------------------
// Internal helpers -- CRITICAL for dual structure consistency
// ---------------------------------------------------------------------------

/// Look up a member and, if `decide` returns a new score, move it — in ONE
/// hash lookup (moon#942).
///
/// Returns `None` when the member is absent, in which case NOTHING was written
/// and `decide` was never called; the caller adds it with [`zset_insert_absent`]
/// if it wants to. Otherwise `Some(old_score)`, whether or not the score moved.
///
/// This is the shape Redis's `zsetAdd` has: ONE `dictFind`, then the new score
/// written through the entry it found. moon hashed the member three times for
/// one `ZADD` — `members.get` for the flag decision, then `members.remove` and
/// `members.insert` inside `zadd_member` — and cloned the `Bytes` twice, on a
/// path `src/command/` forbids cloning on at all.
///
/// The B+tree is touched only when the score actually moves, which is Redis's
/// own `if (score != curscore)` and matters more here than it does there:
/// `BPTree::remove` builds a `Bytes::copy_from_slice(member)` to form its
/// lookup key, so a no-op re-score used to cost an allocation as well as a
/// tree delete and a tree insert.
///
/// The comparison is BITWISE, deliberately. `-0.0 == 0.0` is true while the two
/// render differently (`-0` and `0`), and moon has always stored whichever one
/// the client sent; `to_bits` keeps that, where `==` would silently start
/// answering `0` to a client that wrote `-0`. NaN cannot reach here — `ZADD`
/// and `ZINCRBY` both reject it — so `to_bits` has no NaN-payload hazard.
pub(super) fn zset_update_existing(
    members: &mut HashMap<Bytes, f64>,
    scores: &mut BPTree,
    member: &Bytes,
    decide: impl FnOnce(f64) -> Option<f64>,
) -> Option<f64> {
    work_budget::note_member_lookup();
    let slot = members.get_mut(member.as_ref() as &[u8])?;
    let old = *slot;
    let Some(new) = decide(old) else {
        return Some(old);
    };
    if new.to_bits() != old.to_bits() {
        *slot = new;
        work_budget::note_bptree_score_write();
        // MUST move in both structures.
        scores.remove(OrderedFloat(old), member);
        scores.insert(OrderedFloat(new), member.clone());
    }
    Some(old)
}

/// Insert a member the caller has already PROVEN absent, into both structures.
/// One hash lookup.
pub(super) fn zset_insert_absent(
    members: &mut HashMap<Bytes, f64>,
    scores: &mut BPTree,
    member: Bytes,
    score: f64,
) {
    work_budget::note_member_lookup();
    work_budget::note_bptree_score_write();
    members.insert(member.clone(), score);
    scores.insert(OrderedFloat(score), member);
}

/// Add or update a member in the sorted set. Returns true if the member is new.
///
/// Unconditional overwrite — the shape `ZUNIONSTORE`, `ZINTERSTORE` and
/// `ZRANGESTORE` want, where the destination is being built and no flag has a
/// say. `ZADD` and `ZINCRBY` call [`zset_update_existing`] directly, because
/// their decision depends on the score they are about to displace.
pub(super) fn zadd_member(
    members: &mut HashMap<Bytes, f64>,
    scores: &mut BPTree,
    member: Bytes,
    score: f64,
) -> bool {
    if zset_update_existing(members, scores, &member, |_| Some(score)).is_some() {
        return false;
    }
    zset_insert_absent(members, scores, member, score);
    true
}

/// Remove a member from the sorted set. Returns true if the member existed.
pub(super) fn zrem_member(
    members: &mut HashMap<Bytes, f64>,
    scores: &mut BPTree,
    member: &[u8],
) -> bool {
    work_budget::note_member_lookup();
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

    /// Does `score` satisfy this bound used as the LOWER end of a range?
    ///
    /// Compares exactly. Both sides are doubles parsed from the same wire
    /// text, so there is no rounding here for a tolerance to absorb — and an
    /// absolute `f64::EPSILON` tolerance is not a tolerance at all below
    /// magnitude 1, where it spans many ulps (moon#966).
    ///
    /// An infinite bound is directional: `-inf` as a minimum admits every
    /// score, but `+inf` as a minimum admits only `+inf` itself, which is what
    /// makes `ZRANGEBYSCORE k +inf -inf` empty rather than everything
    /// (moon#961).
    pub(super) fn includes(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => true,
            ScoreBound::PosInf => score == f64::INFINITY,
            ScoreBound::Inclusive(v) => score >= *v,
            ScoreBound::Exclusive(v) => score > *v,
        }
    }

    /// Does `score` satisfy this bound used as the UPPER end of a range?
    /// The mirror of [`ScoreBound::includes`]; see its note.
    pub(super) fn includes_upper(&self, score: f64) -> bool {
        match self {
            ScoreBound::NegInf => score == f64::NEG_INFINITY,
            ScoreBound::PosInf => true,
            ScoreBound::Inclusive(v) => score <= *v,
            ScoreBound::Exclusive(v) => score < *v,
        }
    }

    /// Used as the LOWER end of a range: the ascending rank of the first
    /// entry this bound admits, i.e. how many entries it excludes from
    /// below. O(log N) (moon#1170).
    ///
    /// `includes` is monotone over the tree's order — false for a prefix,
    /// true for the rest, for every variant including the directional
    /// infinities of moon#961 — so its negation is exactly the prefix
    /// predicate `BPTree::count_while` partitions on. The rank therefore
    /// agrees with the per-entry filter the scan used to apply, by
    /// construction rather than by a second transcription of the four cases.
    pub(super) fn lower_rank(&self, tree: &BPTree) -> usize {
        tree.count_while(|score, _| !self.includes(score))
    }

    /// Used as the UPPER end of a range: one past the ascending rank of the
    /// last entry this bound admits. `includes_upper` is true for a prefix.
    pub(super) fn upper_rank(&self, tree: &BPTree) -> usize {
        tree.count_while(|score, _| self.includes_upper(score))
    }
}

/// `[lo, hi)` in ascending rank space for a score range — empty (`lo >= hi`)
/// when the bounds cross, exactly as the old filter-everything scan found
/// nothing for `ZRANGEBYSCORE k 3 1`.
pub(super) fn score_rank_window(
    tree: &BPTree,
    min: &ScoreBound,
    max: &ScoreBound,
) -> (usize, usize) {
    (min.lower_rank(tree), max.upper_rank(tree))
}

/// Redis's convention for a score that arithmetic turned into NaN: `0.0`.
///
/// `zunionInterAggregate` clamps after every aggregation step — *"The result of
/// adding two doubles is NaN when one variable is +inf and the other is -inf.
/// When these numbers are added, we maintain the convention of the result being
/// 0.0"* — and the weight multiply clamps separately, because `inf * 0` is NaN
/// before any aggregation happens. Both points need it (moon#960).
///
/// This is the AGGREGATE rule only. `ZINCRBY` takes the opposite one: an
/// explicit single-key increment that reaches NaN is a user error, answered
/// with `ERR resulting score is not a number (NaN)` and no mutation.
#[inline]
pub(super) fn clamp_nan_to_zero(score: f64) -> f64 {
    if score.is_nan() { 0.0 } else { score }
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

/// Does `member` satisfy `min` used as the lower lex bound? The first half of
/// [`lex_in_range`], split out because it is the monotone predicate a rank
/// seek needs.
fn lex_above_min(member: &[u8], min: &LexBound) -> bool {
    match min {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Inclusive(v) => member >= v.as_ref(),
        LexBound::Exclusive(v) => member > v.as_ref(),
    }
}

/// The second half of [`lex_in_range`].
fn lex_below_max(member: &[u8], max: &LexBound) -> bool {
    match max {
        LexBound::NegInf => false,
        LexBound::PosInf => true,
        LexBound::Inclusive(v) => member <= v.as_ref(),
        LexBound::Exclusive(v) => member < v.as_ref(),
    }
}

/// `[lo, hi)` in ascending rank space for a lex range — but ONLY when every
/// member carries the same score, which is the precondition Redis documents
/// for the lex commands: then the `(score, member)` order IS the member
/// order and both halves of [`lex_in_range`] are monotone over it. `None`
/// otherwise, and the callers keep the full scan, whose answer for mixed
/// scores is the one moon has always given.
///
/// "Same score" is numeric equality, the tree's own notion: `OrderedFloat`
/// ranks `-0.0` and `0.0` as one score, so a zset holding both is still
/// member-ordered. O(log N) for the check (first and last rank) and the two
/// seeks (moon#1170).
pub(super) fn lex_rank_window(
    tree: &BPTree,
    min: &LexBound,
    max: &LexBound,
) -> Option<(usize, usize)> {
    let len = tree.len();
    if len == 0 {
        return Some((0, 0));
    }
    let first = tree.get_by_rank(0)?.0;
    let last = tree.get_by_rank(len - 1)?.0;
    if first != last {
        return None;
    }
    let lo = tree.count_while(|_, m| !lex_above_min(m, min));
    let hi = tree.count_while(|_, m| lex_below_max(m, max));
    Some((lo, hi))
}

/// Resolve `LIMIT offset count` against a rank window `[lo, hi)`: the
/// `(first_rank_to_emit, how_many)` pair, or `None` for an empty reply.
///
/// moon#967: a negative offset returns nothing; a negative count means "no
/// limit". The window is walked from the LOW end for an ascending range and
/// from the HIGH end for a REV one, so for REV the first rank emitted is
/// `hi - 1 - offset` and the walk goes down.
fn limit_window(
    lo: usize,
    hi: usize,
    rev: bool,
    limit_offset: Option<i64>,
    limit_count: Option<i64>,
) -> Option<(usize, usize)> {
    let raw_offset = limit_offset.unwrap_or(0);
    if raw_offset < 0 || hi <= lo {
        return None;
    }
    let avail = hi - lo;
    let offset = usize::try_from(raw_offset).unwrap_or(usize::MAX);
    if offset >= avail {
        return None;
    }
    let left = avail - offset;
    let n = match limit_count {
        Some(c) if c >= 0 => left.min(usize::try_from(c).unwrap_or(usize::MAX)),
        _ => left,
    };
    if n == 0 {
        return None;
    }
    let first = if rev { hi - 1 - offset } else { lo + offset };
    Some((first, n))
}

/// Emit `n` entries walking from ascending rank `first` — upwards, or
/// downwards for `rev` — as a ZRANGE-family reply. One descent plus a leaf
/// walk: O(log N + n) (moon#1170).
fn emit_ranks(tree: &BPTree, first: usize, n: usize, rev: bool, withscores: bool) -> Frame {
    let mut result = Vec::with_capacity(if withscores { n.saturating_mul(2) } else { n });
    let mut push = |score: OrderedFloat<f64>, member: &Bytes| {
        result.push(Frame::BulkString(member.clone()));
        if withscores {
            result.push(Frame::BulkString(format_score_bytes(score.0)));
        }
    };
    if rev {
        for (score, member) in tree.iter_rev_from_rank(first).take(n) {
            push(score, member);
        }
    } else {
        for (score, member) in tree.iter_from_rank(first).take(n) {
            push(score, member);
        }
    }
    Frame::Array(result.into())
}

// ---------------------------------------------------------------------------
// Shared range helpers
// ---------------------------------------------------------------------------

/// Resolve a `start stop` rank pair the way Redis's `zremrangeGenericCommand`
/// does, returning the inclusive window or `None` when it is empty.
///
/// A negative index counts from the end. A START still negative after that is
/// clamped to 0; a STOP still negative is NOT, so `start > stop` reports the
/// window empty — which is what makes `ZREMRANGEBYRANK z -10 -6` on a
/// five-member zset remove nothing (redis 8.6.1 answers `(integer) 0`).
///
/// `zrange_by_rank` and `zrange_from_entries` below (used by `ZRANGE`,
/// `ZREVRANGE` and `ZRANGESTORE`) now call this same helper instead of
/// clamping the STOP as well — moon#959 introduced it only for
/// `ZREMRANGEBYRANK` and left the older range commands with the STOP-clamp
/// bug, which moon#1001 closed by sharing this helper everywhere a rank
/// window is resolved.
pub(super) fn rank_window(start_raw: i64, stop_raw: i64, total: usize) -> Option<(usize, usize)> {
    let len = total as i64;
    let mut start = if start_raw < 0 {
        len.saturating_add(start_raw)
    } else {
        start_raw
    };
    let mut stop = if stop_raw < 0 {
        len.saturating_add(stop_raw)
    } else {
        stop_raw
    };
    if start < 0 {
        start = 0;
    }
    if start > stop || start >= len {
        return None;
    }
    if stop >= len {
        stop = len - 1;
    }
    Some((start as usize, stop as usize))
}

pub(super) fn zrange_by_rank(
    scores: &BPTree,
    min_arg: &[u8],
    max_arg: &[u8],
    rev: bool,
    withscores: bool,
) -> Frame {
    let total = scores.len();
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

    // moon#1001: `rank_window` carries the exact redis rule (a STOP still
    // negative after `len + stop` is NOT clamped to 0), shared with
    // `zrange_from_entries` and `ZREMRANGEBYRANK` so the three cannot drift.
    let (start, stop) = match rank_window(start_raw, stop_raw, total) {
        Some(w) => w,
        None => return Frame::Array(framevec![]),
    };

    // One descent to the window's first entry, then the leaf chain — where
    // `range_by_rank` used to re-descend from the root for every element
    // (moon#1170). REV counts ranks from the HIGH end: rev rank `start` is
    // ascending rank `total - 1 - start`, walked downwards.
    let n = stop - start + 1;
    if rev {
        emit_ranks(scores, total - 1 - start, n, true, withscores)
    } else {
        emit_ranks(scores, start, n, false, withscores)
    }
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

    // moon#1170: O(log N + count), independent of the offset. The bounds become a rank
    // window by two order-statistic descents, LIMIT becomes arithmetic on
    // that window, and only the `count` entries actually replied are
    // visited. The old shape collected EVERY in-range entry into a Vec,
    // reversed it for REV, and only then applied LIMIT — 45 ms for
    // `LIMIT 0 10` on a 1M-member zset.
    let (lo, hi) = score_rank_window(scores, &min_bound, &max_bound);
    match limit_window(lo, hi, rev, limit_offset, limit_count) {
        Some((first, n)) => emit_ranks(scores, first, n, rev, withscores),
        None => Frame::Array(framevec![]),
    }
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

    let _ = members; // the tree carries the score; no second lookup needed

    // moon#1170: when every member shares one score (the documented
    // precondition of the lex commands) the member order is the tree order,
    // so the bounds are a rank window like a score range.
    if let Some((lo, hi)) = lex_rank_window(scores, &min_bound, &max_bound) {
        return match limit_window(lo, hi, rev, limit_offset, limit_count) {
            Some((first, n)) => emit_ranks(scores, first, n, rev, withscores),
            None => Frame::Array(framevec![]),
        };
    }

    // Mixed scores: the lex filter is not monotone over the tree order, so
    // this keeps the full scan and its long-standing answer — but lazily,
    // with no intermediate Vec of every match, and it stops as soon as
    // LIMIT is satisfied. Walking the tree backwards and filtering yields
    // exactly the old "collect ascending, then reverse" sequence.
    // moon#967: a negative LIMIT offset returns nothing.
    let raw_offset = limit_offset.unwrap_or(0);
    if raw_offset < 0 {
        return Frame::Array(framevec![]);
    }
    let offset = usize::try_from(raw_offset).unwrap_or(usize::MAX);
    let take = match limit_count {
        Some(c) if c >= 0 => usize::try_from(c).unwrap_or(usize::MAX),
        _ => usize::MAX,
    };
    let mut result = Vec::new();
    let mut push = |score: OrderedFloat<f64>, member: &Bytes| {
        result.push(Frame::BulkString(member.clone()));
        if withscores {
            result.push(Frame::BulkString(format_score_bytes(score.0)));
        }
    };
    let in_range = |m: &Bytes| lex_in_range(m, &min_bound, &max_bound);
    if rev {
        for (score, member) in scores
            .iter_rev()
            .filter(|(_, m)| in_range(m))
            .skip(offset)
            .take(take)
        {
            push(score, member);
        }
    } else {
        for (score, member) in scores
            .iter()
            .filter(|(_, m)| in_range(m))
            .skip(offset)
            .take(take)
        {
            push(score, member);
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
        // moon#967: a negative LIMIT offset returns nothing (see above).
        let raw_offset = limit_offset.unwrap_or(0);
        if raw_offset < 0 {
            return Frame::Array(framevec![]);
        }
        let offset = raw_offset as usize;
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
        // moon#967: a negative LIMIT offset returns nothing (see above).
        let raw_offset = limit_offset.unwrap_or(0);
        if raw_offset < 0 {
            return Frame::Array(framevec![]);
        }
        let offset = raw_offset as usize;
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
        // moon#1001: normalize with the same `rank_window` helper
        // `zrange_by_rank` uses against the B+tree, so the listpack answer
        // and the B+tree answer cannot drift apart, and a STOP still
        // negative after `len + stop` is NOT clamped to 0 (redis leaves it
        // negative so `start > stop` reports the window empty).
        let (start, stop) = match rank_window(start_raw, stop_raw, entries.len()) {
            Some(w) => w,
            None => return Frame::Array(framevec![]),
        };
        // REV counts ranks from the HIGH-score end. `entries` is
        // score-ascending, so the window [start, stop] in the reversed order
        // is [total-1-stop, total-1-start] here, walked backwards — the same
        // mapping `zrange_by_rank` performs.
        //
        // Reversing the ASCENDING slice instead (what this did before
        // moon#928) is only correct when the window covers the whole zset,
        // which is why `ZREVRANGE z 0 -1` looked fine: on {a:1, b:2, c:3},
        // `ZREVRANGE z 0 1` answered [b, a] where redis 8.6.1 answers [c, b].
        // Reachable only through the compact-encoding branch, which is why
        // moon#928 — the change that stops a read flattening a listpack — is
        // what surfaced it.
        let slice: Vec<&(Bytes, f64)> = if rev {
            let lo = entries.len() - 1 - stop;
            let hi = entries.len() - 1 - start;
            entries[lo..=hi].iter().rev().collect()
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
mod range_rank_tests;

#[cfg(test)]
mod rand_member_tests;

#[cfg(test)]
mod listpack_count_tests;

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

    /// A refused `ZADD` must not leave an empty zset behind.
    ///
    /// moon's `ZADD` reaches the keyspace through `get_or_create_*`, which
    /// FABRICATES the container before the loop can discover that `XX` refuses
    /// every member of the batch. Redis short-circuits first —
    /// `if (zobj == NULL) { if (xx) goto reply_to_client; }` — so
    /// `ZADD ghost XX 1 m` answers 0 and creates nothing.
    ///
    /// Verified against a live redis 8.6.1: after that one command moon
    /// answered `EXISTS ghost` 1, `TYPE ghost` `zset`, `DBSIZE` 1, `KEYS *`
    /// `ghost` and a non-empty `DEBUG DIGEST`, against 0 / `none` / 0 / empty
    /// / the zero digest. `ZCARD` agreed at 0 on both, which is why nothing
    /// caught it.
    ///
    /// This is unbounded keyspace growth on a path any unprivileged client can
    /// drive — one entry plus a fabricated container per call, none of which
    /// ever appears to hold anything — and it diverges the digest, so a
    /// replica or a reloaded RDB disagrees with its master. It is the sorted
    /// set's copy of the empty-container class, and `ZREM` already carries the
    /// fix: drop the key when the container ends up empty.
    ///
    /// Both encodings, because the 65-byte member skips the listpack gate
    /// entirely and lands on `get_or_create_sorted_set`, which fabricates too.
    #[test]
    fn a_refused_zadd_leaves_no_empty_zset_behind() {
        for member in [b"m".to_vec(), vec![b'y'; 70]] {
            let mut db = Database::new();
            let floor = db.estimated_memory();
            assert_eq!(
                zadd(
                    &mut db,
                    &[bulk(b"ghost"), bulk(b"XX"), bulk(b"1"), bulk(&member)]
                ),
                Frame::Integer(0),
                "XX on a missing key adds nothing"
            );
            assert_eq!(
                crate::command::key::exists(&mut db, &[bulk(b"ghost")]),
                Frame::Integer(0),
                "XX on a missing key must not create it (member {} bytes)",
                member.len()
            );
            assert_eq!(
                crate::command::key::type_cmd(&mut db, &[bulk(b"ghost")]),
                Frame::SimpleString(bytes::Bytes::from_static(b"none")),
                "and TYPE must still say none"
            );
            assert_eq!(
                db.estimated_memory(),
                floor,
                "a refused ZADD must charge nothing at all"
            );
        }

        // The same refusal on a key that DOES exist must leave it untouched.
        let mut db = Database::new();
        assert_eq!(
            zadd(&mut db, &[bulk(b"live"), bulk(b"1"), bulk(b"a")]),
            Frame::Integer(1)
        );
        assert_eq!(
            zadd(
                &mut db,
                &[bulk(b"live"), bulk(b"XX"), bulk(b"2"), bulk(b"never")]
            ),
            Frame::Integer(0)
        );
        assert_eq!(
            crate::command::key::exists(&mut db, &[bulk(b"live")]),
            Frame::Integer(1),
            "an existing zset must survive a refused ZADD"
        );
        assert_eq!(
            zcard(&mut db, &[bulk(b"live")]),
            Frame::Integer(1),
            "and keep the member it had"
        );
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

    /// moon#1001. A STOP still negative after `len + stop` must NOT be
    /// clamped to 0 — redis leaves it negative so `start > stop` reports the
    /// window empty. `rank_window` (moon#959) already carries this rule;
    /// `zrange_by_rank` and `zrange_from_entries` did not use it. Verified
    /// against a live redis-server 8.6.1 on a five-member zset:
    /// `ZRANGE r8 -10 -6` -> `*0`, `ZREVRANGE r8 -10 -6` -> `*0`,
    /// `ZRANGE r8 -10 -6 REV` -> `*0`, `ZRANGESTORE r9 r8 -10 -6` -> `0`.
    #[test]
    fn still_negative_stop_is_not_clamped_to_zero() {
        for member_of in [
            |c: char| Bytes::from(c.to_string()),
            |c: char| Bytes::from(format!("{c}{}", "x".repeat(70))), // forces B+tree
        ] {
            let mut db = Database::new();
            let members: Vec<Bytes> = ('a'..='e').map(member_of).collect();
            for (score, member) in members.iter().enumerate() {
                run_zadd(
                    &mut db,
                    &[b"r8", (score + 1).to_string().as_bytes(), member],
                );
            }
            let first = members[0].clone();
            let last = members[4].clone();

            assert_eq!(
                zrange(&mut db, &[bulk(b"r8"), bulk(b"-10"), bulk(b"-6")]),
                Frame::Array(framevec![]),
                "ZRANGE r8 -10 -6 on {members:?}"
            );
            assert_eq!(
                zrevrange(&mut db, &[bulk(b"r8"), bulk(b"-10"), bulk(b"-6")]),
                Frame::Array(framevec![]),
                "ZREVRANGE r8 -10 -6 on {members:?}"
            );
            assert_eq!(
                zrange(
                    &mut db,
                    &[bulk(b"r8"), bulk(b"-10"), bulk(b"-6"), bulk(b"REV")]
                ),
                Frame::Array(framevec![]),
                "ZRANGE r8 -10 -6 REV on {members:?}"
            );
            assert_eq!(
                zrangestore(
                    &mut db,
                    &[bulk(b"r9"), bulk(b"r8"), bulk(b"-10"), bulk(b"-6")]
                ),
                Frame::Integer(0),
                "ZRANGESTORE r9 r8 -10 -6 on {members:?}"
            );

            // Control, taken from the same oracle session: a stop of exactly
            // `-len` normalises to rank 0 without any clamping, so it was
            // already correct pre-fix and must stay so (verified against a
            // live redis-server 8.6.1: `ZRANGE r8 -10 -5` -> `1) "a"`).
            assert_eq!(
                zrange(&mut db, &[bulk(b"r8"), bulk(b"-10"), bulk(b"-5")]),
                Frame::Array(framevec![Frame::BulkString(first.clone())]),
                "ZRANGE r8 -10 -5 (stop == -len normalises to rank 0)"
            );
            assert_eq!(
                zrange(&mut db, &[bulk(b"r8"), bulk(b"-1"), bulk(b"-3")]),
                Frame::Array(framevec![]),
                "ZRANGE r8 -1 -3 (start > stop pre-existing) stays empty"
            );

            // A single-member zset must not leak the same bug at len == 1.
            let mut db1 = Database::new();
            run_zadd(&mut db1, &[b"e1", b"1", &first]);
            assert_eq!(
                zrange(&mut db1, &[bulk(b"e1"), bulk(b"-10"), bulk(b"-6")]),
                Frame::Array(framevec![]),
                "ZRANGE e1 -10 -6 on a single-member zset"
            );
            assert_eq!(
                zrange(&mut db1, &[bulk(b"e1"), bulk(b"-1"), bulk(b"-1")]),
                Frame::Array(framevec![Frame::BulkString(first.clone())]),
                "ZRANGE e1 -1 -1 still returns the sole member"
            );
            let _ = &last;
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

    // ── #928: a READ on the mutable dispatch path must not flatten a
    //         listpack zset ────────────────────────────────────────────────
    //
    // Every handler below is a pure read, and every one used to reach the
    // zset through `Database::get_sorted_set`, whose `get_promoted` core
    // calls `SortedSetKind::upgrade` unconditionally. That conversion is
    // one-way -- nothing in the tree ever downgrades (moon#832) -- so a
    // single ZCARD taken on the mutable path (inside MULTI/EXEC, inside a
    // Lua script, or from `try_inline_dispatch`) permanently flattened a
    // `listpack` zset to a `skiplist` for the rest of its life.
    //
    // Measured on the pre-fix binary at ab91a23e (`--shards 1`, macOS host,
    // `used_memory` ledger -- accounting, not throughput): 1000 eight-member
    // zsets went 293,055 -> 4,749,055 bytes (16.21x) after ONE `ZCARD` each
    // taken through MULTI/EXEC, and `OBJECT ENCODING` went
    // `listpack -> skiplist`. All 17 reads flattened; the same reads on a
    // bare connection (the `dispatch_read` path) kept `listpack`, which is
    // the negative control proving the probe measures the dispatch path.
    //
    // Probe choice: `OBJECT ENCODING` reads `entry.value` through
    // `Database::get`, which does not route through `get_promoted`, so asking
    // about the encoding cannot itself flatten it. Every case asserts the
    // fixture is `listpack` BEFORE the read -- a fixture that was never
    // compact would make the post-read assertion vacuous.

    fn run_zmscore(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zmscore(db, &frames)
    }

    fn run_zrandmember(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zrandmember(db, &frames)
    }

    fn run_zdiff(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zdiff(db, &frames)
    }

    fn run_zunion(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zunion(db, &frames)
    }

    fn run_zinter(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zinter(db, &frames)
    }

    fn run_zintercard(db: &mut Database, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        zintercard(db, &frames)
    }

    /// A `Frame::Array` of bulk strings — the reply shape most of the range
    /// commands answer without `WITHSCORES`.
    fn bulk_array(members: &[&[u8]]) -> Frame {
        Frame::Array(members.iter().map(|m| bulk(m)).collect::<Vec<_>>().into())
    }

    /// The shared fixture: three members, far below `zset_entries` (128) and
    /// `zset_value` (64), so redis 8.6.1 reports `listpack` for it too.
    fn listpack_zset(db: &mut Database) {
        run_zadd(db, &[b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        assert_eq!(
            encoding_of(db, b"z"),
            "listpack",
            "fixture must start as a listpack or the case proves nothing"
        );
    }

    /// The post-read assertion every case below shares.
    fn assert_still_listpack(db: &mut Database, what: &str) {
        assert_eq!(
            encoding_of(db, b"z"),
            "listpack",
            "{what} on the mutable dispatch path flattened the zset to a \
             skiplist (moon#928) -- reads must not rewrite the encoding"
        );
    }

    #[test]
    fn zscore_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zscore(&mut db, &[b"z", b"b"]),
            Frame::BulkString(Bytes::from_static(b"2"))
        );
        assert_eq!(run_zscore(&mut db, &[b"z", b"nope"]), Frame::Null);
        assert_still_listpack(&mut db, "ZSCORE");
    }

    #[test]
    fn zcard_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(run_zcard(&mut db, &[b"z"]), Frame::Integer(3));
        assert_still_listpack(&mut db, "ZCARD");
    }

    #[test]
    fn zrank_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(run_zrank(&mut db, &[b"z", b"a"]), Frame::Integer(0));
        assert_eq!(run_zrank(&mut db, &[b"z", b"c"]), Frame::Integer(2));
        assert_eq!(run_zrank(&mut db, &[b"z", b"nope"]), Frame::Null);
        assert_still_listpack(&mut db, "ZRANK");
    }

    #[test]
    fn zrevrank_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(run_zrevrank(&mut db, &[b"z", b"a"]), Frame::Integer(2));
        assert_eq!(run_zrevrank(&mut db, &[b"z", b"c"]), Frame::Integer(0));
        assert_eq!(run_zrevrank(&mut db, &[b"z", b"nope"]), Frame::Null);
        assert_still_listpack(&mut db, "ZREVRANK");
    }

    #[test]
    fn zscan_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zscan(&mut db, &[b"z", b"0"]),
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(framevec![
                    Frame::BulkString(Bytes::from_static(b"a")),
                    Frame::BulkString(Bytes::from_static(b"1")),
                    Frame::BulkString(Bytes::from_static(b"b")),
                    Frame::BulkString(Bytes::from_static(b"2")),
                    Frame::BulkString(Bytes::from_static(b"c")),
                    Frame::BulkString(Bytes::from_static(b"3")),
                ]),
            ])
        );
        assert_still_listpack(&mut db, "ZSCAN");
    }

    #[test]
    fn zrange_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zrange(&mut db, &[b"z", b"0", b"-1"]),
            bulk_array(&[b"a", b"b", b"c"])
        );
        assert_still_listpack(&mut db, "ZRANGE");
    }

    #[test]
    fn zrevrange_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zrevrange(&mut db, &[b"z", b"0", b"-1"]),
            bulk_array(&[b"c", b"b", b"a"])
        );
        // A PARTIAL rev window is the case the whole-range one hides: ranks
        // count from the HIGH-score end, so `0 1` is [c, b], not [b, a].
        // Verified against redis 8.6.1: `zrevrange z 0 1` => c, b.
        assert_eq!(
            run_zrevrange(&mut db, &[b"z", b"0", b"1"]),
            bulk_array(&[b"c", b"b"])
        );
        assert_eq!(
            run_zrevrange(&mut db, &[b"z", b"1", b"1"]),
            bulk_array(&[b"b"])
        );
        assert_eq!(
            run_zrevrange(&mut db, &[b"z", b"-2", b"-1"]),
            bulk_array(&[b"b", b"a"])
        );
        assert_eq!(
            run_zrevrange(&mut db, &[b"z", b"5", b"10"]),
            Frame::Array(framevec![])
        );
        assert_still_listpack(&mut db, "ZREVRANGE");
    }

    #[test]
    fn zrangebyscore_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zrangebyscore(&mut db, &[b"z", b"-inf", b"+inf"]),
            bulk_array(&[b"a", b"b", b"c"])
        );
        assert_still_listpack(&mut db, "ZRANGEBYSCORE");
    }

    #[test]
    fn zrevrangebyscore_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zrevrangebyscore(&mut db, &[b"z", b"+inf", b"-inf"]),
            bulk_array(&[b"c", b"b", b"a"])
        );
        assert_still_listpack(&mut db, "ZREVRANGEBYSCORE");
    }

    #[test]
    fn zcount_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zcount(&mut db, &[b"z", b"-inf", b"+inf"]),
            Frame::Integer(3)
        );
        assert_eq!(run_zcount(&mut db, &[b"z", b"2", b"3"]), Frame::Integer(2));
        assert_eq!(run_zcount(&mut db, &[b"z", b"(2", b"3"]), Frame::Integer(1));
        assert_still_listpack(&mut db, "ZCOUNT");
    }

    #[test]
    fn zlexcount_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zlexcount(&mut db, &[b"z", b"-", b"+"]),
            Frame::Integer(3)
        );
        assert_eq!(
            run_zlexcount(&mut db, &[b"z", b"[b", b"+"]),
            Frame::Integer(2)
        );
        assert_still_listpack(&mut db, "ZLEXCOUNT");
    }

    #[test]
    fn zmscore_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zmscore(&mut db, &[b"z", b"a", b"nope", b"c"]),
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"1")),
                Frame::Null,
                Frame::BulkString(Bytes::from_static(b"3")),
            ])
        );
        assert_still_listpack(&mut db, "ZMSCORE");
    }

    #[test]
    fn zrandmember_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        // Random member, deterministic membership: whatever comes back must
        // be one of the three, and the count form must return all three.
        match run_zrandmember(&mut db, &[b"z"]) {
            Frame::BulkString(b) => assert!(
                [&b"a"[..], b"b", b"c"].contains(&b.as_ref()),
                "ZRANDMEMBER returned {b:?}, not a member of the fixture"
            ),
            other => panic!("expected a bulk string, got {other:?}"),
        }
        match run_zrandmember(&mut db, &[b"z", b"3"]) {
            Frame::Array(items) => assert_eq!(items.len(), 3),
            other => panic!("expected an array, got {other:?}"),
        }
        assert_still_listpack(&mut db, "ZRANDMEMBER");
    }

    #[test]
    fn zdiff_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zdiff(&mut db, &[b"1", b"z"]),
            bulk_array(&[b"a", b"b", b"c"])
        );
        assert_still_listpack(&mut db, "ZDIFF");
    }

    #[test]
    fn zunion_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zunion(&mut db, &[b"1", b"z"]),
            bulk_array(&[b"a", b"b", b"c"])
        );
        assert_still_listpack(&mut db, "ZUNION");
    }

    #[test]
    fn zinter_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(
            run_zinter(&mut db, &[b"1", b"z"]),
            bulk_array(&[b"a", b"b", b"c"])
        );
        assert_still_listpack(&mut db, "ZINTER");
    }

    #[test]
    fn zintercard_read_keeps_a_small_zset_listpack() {
        let mut db = Database::new();
        listpack_zset(&mut db);
        assert_eq!(run_zintercard(&mut db, &[b"1", b"z"]), Frame::Integer(3));
        assert_still_listpack(&mut db, "ZINTERCARD");
    }

    /// Route a command name to its mutable-path handler — the same handlers
    /// `command::dispatch` calls.
    fn dispatch_named(db: &mut Database, name: &str, args: &[&[u8]]) -> Frame {
        let frames: Vec<Frame> = args.iter().map(|a| bulk(a)).collect();
        match name {
            "ZSCORE" => zscore(db, &frames),
            "ZCARD" => zcard(db, &frames),
            "ZRANK" => zrank(db, &frames),
            "ZREVRANK" => zrevrank(db, &frames),
            "ZSCAN" => zscan(db, &frames),
            "ZRANGE" => zrange(db, &frames),
            "ZREVRANGE" => zrevrange(db, &frames),
            "ZRANGEBYSCORE" => zrangebyscore(db, &frames),
            "ZREVRANGEBYSCORE" => zrevrangebyscore(db, &frames),
            "ZCOUNT" => zcount(db, &frames),
            "ZLEXCOUNT" => zlexcount(db, &frames),
            "ZMSCORE" => zmscore(db, &frames),
            "ZDIFF" => zdiff(db, &frames),
            "ZUNION" => zunion(db, &frames),
            "ZINTER" => zinter(db, &frames),
            "ZINTERCARD" => zintercard(db, &frames),
            other => panic!("dispatch_named has no arm for {other}"),
        }
    }

    /// Same members, both encodings, byte-identical replies.
    ///
    /// This is the semantic half of moon#928: routing the mutable handlers
    /// through the shared-borrow implementation only preserves the encoding
    /// if the two implementations answer the same. A zset that genuinely
    /// exceeds `zset_entries`/`zset_value` is a B+tree and must still read
    /// correctly from that form, so the same fixture is built twice — once
    /// small enough to stay a listpack, once promoted past the value
    /// threshold and then trimmed back to the identical three members.
    #[test]
    fn listpack_and_skiplist_zsets_answer_every_read_identically() {
        let deterministic: &[&[&[u8]]] = &[
            &[b"ZSCORE", b"z", b"b"],
            &[b"ZSCORE", b"z", b"nope"],
            &[b"ZCARD", b"z"],
            &[b"ZRANK", b"z", b"b"],
            &[b"ZRANK", b"z", b"b", b"WITHSCORE"],
            &[b"ZREVRANK", b"z", b"b"],
            &[b"ZSCAN", b"z", b"0"],
            &[b"ZRANGE", b"z", b"0", b"-1"],
            &[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"],
            &[b"ZRANGE", b"z", b"(1", b"+inf", b"BYSCORE"],
            &[b"ZRANGE", b"z", b"[a", b"[b", b"BYLEX"],
            &[b"ZRANGE", b"z", b"0", b"-1", b"REV"],
            // Partial REV windows — the arm a whole-range `0 -1` cannot
            // distinguish, and the one that was wrong for listpacks until
            // moon#928 surfaced it.
            &[b"ZRANGE", b"z", b"0", b"1", b"REV"],
            &[b"ZREVRANGE", b"z", b"0", b"-1", b"WITHSCORES"],
            &[b"ZREVRANGE", b"z", b"0", b"1"],
            &[b"ZREVRANGE", b"z", b"1", b"1"],
            &[b"ZREVRANGE", b"z", b"-2", b"-1"],
            &[b"ZREVRANGE", b"z", b"5", b"10"],
            &[b"ZREVRANGE", b"z", b"-100", b"100"],
            &[b"ZRANGEBYSCORE", b"z", b"-inf", b"+inf", b"WITHSCORES"],
            &[b"ZRANGEBYSCORE", b"z", b"2", b"3", b"LIMIT", b"1", b"1"],
            &[b"ZREVRANGEBYSCORE", b"z", b"+inf", b"-inf"],
            &[b"ZCOUNT", b"z", b"(1", b"3"],
            &[b"ZLEXCOUNT", b"z", b"[b", b"+"],
            &[b"ZMSCORE", b"z", b"a", b"nope", b"c"],
            &[b"ZDIFF", b"1", b"z", b"WITHSCORES"],
            &[b"ZUNION", b"1", b"z", b"WITHSCORES"],
            &[b"ZINTER", b"1", b"z", b"WITHSCORES"],
            &[b"ZINTERCARD", b"1", b"z"],
        ];

        let mut lp = Database::new();
        run_zadd(&mut lp, &[b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        assert_eq!(encoding_of(&mut lp, b"z"), "listpack");

        // The same three members in the B+tree form: one oversized member
        // forces the promotion, then ZREM takes it away again. Promotion is
        // one-way, so what is left is the identical content as a skiplist.
        let mut bt = Database::new();
        run_zadd(&mut bt, &[b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        let oversized =
            vec![b'x'; crate::storage::db::EncodingLimits::moon_defaults().zset_value + 1];
        run_zadd(&mut bt, &[b"z", b"9", &oversized]);
        run_zrem(&mut bt, &[b"z", &oversized]);
        assert_eq!(
            encoding_of(&mut bt, b"z"),
            "skiplist",
            "the control fixture must be a B+tree or this test compares two listpacks"
        );

        let mut divergences: Vec<String> = Vec::new();
        for argv in deterministic {
            let name = String::from_utf8_lossy(argv[0]).into_owned();
            let got_lp = dispatch_named(&mut lp, &name, &argv[1..]);
            let got_bt = dispatch_named(&mut bt, &name, &argv[1..]);
            if got_lp != got_bt {
                divergences.push(format!(
                    "{}: listpack {got_lp:?} != skiplist {got_bt:?}",
                    argv.iter()
                        .map(|a| String::from_utf8_lossy(a).into_owned())
                        .collect::<Vec<_>>()
                        .join(" ")
                ));
            }
        }
        assert!(
            divergences.is_empty(),
            "{} read(s) answer differently depending on the encoding:\n  {}",
            divergences.len(),
            divergences.join("\n  ")
        );
        // The whole point: none of the reads above moved either encoding.
        assert_eq!(encoding_of(&mut lp, b"z"), "listpack");
        assert_eq!(encoding_of(&mut bt, b"z"), "skiplist");
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
    /// (the moon#863 shape). The listpack arm must therefore refuse the case.
    ///
    /// moon#960 changed what "refuse" means. It used to mean "fall through to
    /// the B+tree arm", which replied `NaN`, stored a NaN, and — because that
    /// arm opens with the EAGER `get_or_create_sorted_set` — flattened the
    /// encoding for the key's lifetime (moon#832: nothing demotes), all from a
    /// command that should not have written anything. It now means what redis
    /// 8.6.1 does: `ERR resulting score is not a number (NaN)`, score
    /// untouched, encoding untouched.
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

            let reply = run_zincrby(&mut db, &[b"z", second, b"m"]);
            match reply {
                Frame::Error(ref e) => assert_eq!(
                    e.as_ref(),
                    b"ERR resulting score is not a number (NaN)".as_ref()
                ),
                other => panic!("expected the NaN error, got {other:?}"),
            }
            assert_eq!(
                encoding_of(&mut db, b"z"),
                "listpack",
                "a command that stores nothing must not promote the encoding"
            );
            // The original invariant, now satisfied outright: the member keeps
            // the score it had, so there is no NaN to round-trip as 0.
            assert_eq!(
                ro_zscore(&db, &[b"z", b"m"]),
                Frame::BulkString(Bytes::copy_from_slice(first)),
                "the NaN step must leave the previous score in place"
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

    // ---- moon#967: unknown option tokens ----------------------------------
    //
    // Every zset option loop ended in a bare `} else { i += 1; }`, so a token
    // the parser did not recognise was SKIPPED rather than rejected. Redis
    // answers `ERR syntax error`. Verified against redis 8.6.1.

    #[test]
    fn an_unrecognised_option_token_is_a_syntax_error() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"k", b"1", b"a", b"2", b"b"]);

        let syntax = Frame::Error(Bytes::from_static(b"ERR syntax error"));

        assert_eq!(
            zrange(
                &mut db,
                &[bulk(b"k"), bulk(b"0"), bulk(b"-1"), bulk(b"BOGUS")]
            ),
            syntax
        );
        assert_eq!(
            zrangebyscore(
                &mut db,
                &[bulk(b"k"), bulk(b"-inf"), bulk(b"+inf"), bulk(b"BOGUS")]
            ),
            syntax
        );
        assert_eq!(
            zrevrangebyscore(
                &mut db,
                &[bulk(b"k"), bulk(b"+inf"), bulk(b"-inf"), bulk(b"BOGUS")]
            ),
            syntax
        );

        // ZINTERCARD is the sharp one: the trailing `1` was meant as LIMIT 1.
        // Skipping both tokens answered the UNBOUNDED cardinality, so a client
        // asking for a bounded intersection silently got an unbounded one.
        assert_eq!(
            zintercard(
                &mut db,
                &[bulk(b"1"), bulk(b"k"), bulk(b"BOGUS"), bulk(b"1")]
            ),
            syntax
        );

        // ZMPOP MUTATES, so accepting a bogus token is not even side-effect
        // free: `MIN MAX` used to take the first direction and pop.
        assert_eq!(
            zmpop(
                &mut db,
                &[bulk(b"1"), bulk(b"k"), bulk(b"MIN"), bulk(b"MAX")]
            ),
            syntax
        );
        assert_eq!(
            run_zcard(&mut db, &[b"k"]),
            Frame::Integer(2),
            "a rejected ZMPOP must not have popped anything"
        );

        // The pairing ZRANGE never checked: only BYSCORE+BYLEX was rejected.
        match zrange(
            &mut db,
            &[
                bulk(b"k"),
                bulk(b"-"),
                bulk(b"+"),
                bulk(b"BYLEX"),
                bulk(b"WITHSCORES"),
            ],
        ) {
            Frame::Error(ref e) => assert_eq!(
                e.as_ref(),
                b"ERR syntax error, WITHSCORES not supported in combination with BYLEX".as_ref()
            ),
            other => panic!("expected the BYLEX+WITHSCORES error, got {other:?}"),
        }

        // Every option the parsers DO know still works.
        assert_eq!(
            zrange(&mut db, &[bulk(b"k"), bulk(b"0"), bulk(b"-1")]),
            Frame::Array(framevec![bulk(b"a"), bulk(b"b")])
        );
    }

    // ---- moon#969 / moon#792: option semantics and error classes ----------
    //
    // Every expectation below was taken from a live redis-server 8.6.1 on a
    // second port, command by command, BEFORE any of it was changed.

    /// A member short enough to stay in the listpack encoding.
    const LP_MEMBER: &[u8] = b"m";
    /// A member past `zset-max-listpack-value` (64), which forces the B+tree.
    /// The two `ZADD` mutation loops are SEPARATE code, so every claim about
    /// flags or `CH` has to be made twice.
    const BT_MEMBER: &[u8] =
        b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn error_text(f: &Frame) -> String {
        match f {
            Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[test]
    fn gt_lt_and_nx_are_pairwise_incompatible() {
        let mut db = Database::new();
        const MSG: &str = "ERR GT, LT, and/or NX options at the same time are not compatible";

        // `GT LT` is the pairing the old guard missed: it was ACCEPTED, and
        // then the mutation loops answered it with a silent no-op.
        for flags in [
            [&b"GT"[..], &b"LT"[..]],
            [&b"GT"[..], &b"NX"[..]],
            [&b"LT"[..], &b"NX"[..]],
        ] {
            let reply = run_zadd(&mut db, &[b"k", flags[0], flags[1], b"1", b"m"]);
            assert_eq!(error_text(&reply), MSG, "flags {flags:?}");
        }
        assert_eq!(
            run_zadd(&mut db, &[b"k", b"GT", b"LT", b"NX", b"1", b"m"]),
            Frame::Error(Bytes::from_static(MSG.as_bytes()))
        );

        // Rejected before the keyspace is touched, on BOTH encodings.
        assert_eq!(run_zcard(&mut db, &[b"k"]), Frame::Integer(0));
        for member in [LP_MEMBER, BT_MEMBER] {
            run_zadd(&mut db, &[b"z", b"5", member]);
            let reply = run_zadd(&mut db, &[b"z", b"GT", b"LT", b"9", member]);
            assert_eq!(error_text(&reply), MSG);
            assert_eq!(
                run_zscore(&mut db, &[b"z", member]),
                Frame::BulkString(Bytes::from_static(b"5")),
                "a rejected GT+LT must not have rescored the member"
            );
        }

        // The pairing that is still legal on its own keeps working.
        assert_eq!(
            run_zadd(&mut db, &[b"z", b"GT", b"9", LP_MEMBER]),
            Frame::Integer(0)
        );
        assert_eq!(
            run_zscore(&mut db, &[b"z", LP_MEMBER]),
            Frame::BulkString(Bytes::from_static(b"9"))
        );
    }

    #[test]
    fn an_odd_score_member_tail_is_a_syntax_error() {
        let mut db = Database::new();
        // Redis splits these two: NO pairs at all fails `commandCheckArity`,
        // an ODD tail fails inside `zaddGenericCommand`.
        assert_eq!(
            error_text(&run_zadd(&mut db, &[b"k", b"1", b"a", b"2"])),
            "ERR syntax error"
        );
        assert_eq!(
            error_text(&run_zadd(&mut db, &[b"k", b"CH", b"1"])),
            "ERR syntax error"
        );
        assert_eq!(
            error_text(&run_zadd(&mut db, &[b"k", b"NX"])),
            "ERR wrong number of arguments for 'zadd' command"
        );
        assert_eq!(run_zcard(&mut db, &[b"k"]), Frame::Integer(0));
    }

    #[test]
    fn a_nan_weight_is_not_a_float() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"src", b"1", b"a"]);
        let nan_args =
            |extra: &[&[u8]]| -> Vec<Frame> { extra.iter().map(|a| bulk(a)).collect::<Vec<_>>() };

        // Rust parses "nan"; C's `strtod` + `isnan` check does not.
        for w in [&b"nan"[..], &b"-nan"[..], &b"NaN"[..]] {
            assert_eq!(
                error_text(&zunionstore(
                    &mut db,
                    &nan_args(&[b"d", b"1", b"src", b"WEIGHTS", w])
                )),
                "ERR weight value is not a float",
                "ZUNIONSTORE weight {}",
                String::from_utf8_lossy(w)
            );
            assert_eq!(
                error_text(&zunion(&mut db, &nan_args(&[b"1", b"src", b"WEIGHTS", w]))),
                "ERR weight value is not a float",
                "ZUNION weight {}",
                String::from_utf8_lossy(w)
            );
        }
        // …and the destination was never written.
        assert_eq!(run_zcard(&mut db, &[b"d"]), Frame::Integer(0));

        // An INFINITE weight stays legal, exactly as on Redis.
        assert_eq!(
            zunionstore(
                &mut db,
                &nan_args(&[b"d", b"1", b"src", b"WEIGHTS", b"inf"])
            ),
            Frame::Integer(1)
        );
    }

    #[test]
    fn count_and_numkeys_errors_carry_the_class_redis_uses() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2", b"b"]);
        let f = |args: &[&[u8]]| -> Vec<Frame> { args.iter().map(|a| bulk(a)).collect() };

        // `getPositiveLongFromObject` — one message for every failure.
        for bad in [&b"notanint"[..], &b"-1"[..], &b"1.5"[..]] {
            assert_eq!(
                error_text(&zpopmin(&mut db, &f(&[b"z", bad]))),
                "ERR value is out of range, must be positive"
            );
            assert_eq!(
                error_text(&zpopmax(&mut db, &f(&[b"z", bad]))),
                "ERR value is out of range, must be positive"
            );
        }
        assert_eq!(run_zcard(&mut db, &[b"z"]), Frame::Integer(2));

        // The set-operation family SPLITS: not-a-number is the generic integer
        // error, a number below 1 names the command.
        assert_eq!(
            error_text(&zunionstore(&mut db, &f(&[b"d", b"notanint", b"z"]))),
            "ERR value is not an integer or out of range"
        );
        for bad in [&b"0"[..], &b"-1"[..]] {
            assert_eq!(
                error_text(&zunionstore(&mut db, &f(&[b"d", bad, b"z"]))),
                "ERR at least 1 input key is needed for 'zunionstore' command"
            );
            assert_eq!(
                error_text(&zinterstore(&mut db, &f(&[b"d", bad, b"z"]))),
                "ERR at least 1 input key is needed for 'zinterstore' command"
            );
            assert_eq!(
                error_text(&zunion(&mut db, &f(&[bad, b"z"]))),
                "ERR at least 1 input key is needed for 'zunion' command"
            );
            assert_eq!(
                error_text(&zinter(&mut db, &f(&[bad, b"z"]))),
                "ERR at least 1 input key is needed for 'zinter' command"
            );
            assert_eq!(
                error_text(&zdiff(&mut db, &f(&[bad, b"z"]))),
                "ERR at least 1 input key is needed for 'zdiff' command"
            );
            assert_eq!(
                error_text(&zintercard(&mut db, &f(&[bad, b"z"]))),
                "ERR at least 1 input key is needed for 'zintercard' command"
            );
        }
        // Arity is checked FIRST, so a form that names no key at all never
        // reaches the numkeys rules.
        assert_eq!(
            error_text(&zunion(&mut db, &f(&[b"0"]))),
            "ERR wrong number of arguments for 'zunion' command"
        );
        assert_eq!(
            error_text(&zintercard(&mut db, &f(&[b"0"]))),
            "ERR wrong number of arguments for 'zintercard' command"
        );

        // ZMPOP does NOT split — `getRangeLongFromObject` with one message.
        for bad in [&b"0"[..], &b"-1"[..], &b"notanint"[..]] {
            assert_eq!(
                error_text(&zmpop(&mut db, &f(&[bad, b"z", b"MIN"]))),
                "ERR numkeys should be greater than 0"
            );
        }
        for bad in [&b"0"[..], &b"-1"[..], &b"notanint"[..]] {
            assert_eq!(
                error_text(&zmpop(&mut db, &f(&[b"1", b"z", b"MIN", b"COUNT", bad]))),
                "ERR count should be greater than 0"
            );
        }
        assert_eq!(
            run_zcard(&mut db, &[b"z"]),
            Frame::Integer(2),
            "a rejected ZMPOP must not have popped"
        );

        // ZINTERCARD's LIMIT has its own message too.
        for bad in [&b"-1"[..], &b"notanint"[..]] {
            assert_eq!(
                error_text(&zintercard(&mut db, &f(&[b"1", b"z", b"LIMIT", bad]))),
                "ERR LIMIT can't be negative"
            );
        }
        assert_eq!(
            zintercard(&mut db, &f(&[b"1", b"z", b"LIMIT", b"0"])),
            Frame::Integer(2)
        );

        // A numkeys that overruns the key list, a short WEIGHTS list, a
        // dangling AGGREGATE and an unknown trailing token are all
        // `syntax error` — NOT arity errors.
        let syntax = "ERR syntax error";
        assert_eq!(
            error_text(&zunionstore(&mut db, &f(&[b"d", b"2", b"z"]))),
            syntax
        );
        assert_eq!(error_text(&zunion(&mut db, &f(&[b"2", b"z"]))), syntax);
        assert_eq!(error_text(&zintercard(&mut db, &f(&[b"2", b"z"]))), syntax);
        assert_eq!(
            error_text(&zmpop(&mut db, &f(&[b"2", b"z", b"MIN"]))),
            syntax
        );
        assert_eq!(
            error_text(&zunionstore(&mut db, &f(&[b"d", b"1", b"z", b"WEIGHTS"]))),
            syntax
        );
        assert_eq!(
            error_text(&zunion(&mut db, &f(&[b"1", b"z", b"WEIGHTS"]))),
            syntax
        );
        assert_eq!(
            error_text(&zunionstore(&mut db, &f(&[b"d", b"1", b"z", b"AGGREGATE"]))),
            syntax
        );
        // moon#967 rewrote every zset option loop but this one.
        assert_eq!(
            error_text(&zunionstore(&mut db, &f(&[b"d", b"1", b"z", b"BOGUS"]))),
            syntax
        );
        assert_eq!(
            error_text(&zintercard(&mut db, &f(&[b"1", b"z", b"LIMIT"]))),
            syntax
        );
        assert_eq!(
            error_text(&zmpop(&mut db, &f(&[b"1", b"z", b"MIN", b"COUNT"]))),
            syntax
        );
    }

    /// moon#969 cites four ZRANGE-family sites as wrong. They are NOT: Redis
    /// reads a rank index and a `LIMIT offset count` with
    /// `getLongFromObjectOrReply(…, NULL)`, whose message is exactly the
    /// generic one moon already answers. This test pins them so the moon#969
    /// sweep cannot "fix" them into a divergence.
    #[test]
    fn rank_and_limit_parses_keep_the_generic_integer_error() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"z", b"1", b"a", b"2", b"b"]);
        let f = |args: &[&[u8]]| -> Vec<Frame> { args.iter().map(|a| bulk(a)).collect() };
        const GENERIC: &str = "ERR value is not an integer or out of range";

        assert_eq!(
            error_text(&zrange(&mut db, &f(&[b"z", b"notanint", b"5"]))),
            GENERIC
        );
        assert_eq!(
            error_text(&zrange(&mut db, &f(&[b"z", b"0", b"notanint"]))),
            GENERIC
        );
        assert_eq!(
            error_text(&zrange(&mut db, &f(&[b"z", b"1.5", b"2"]))),
            GENERIC
        );
        assert_eq!(
            error_text(&zrevrange(&mut db, &f(&[b"z", b"notanint", b"5"]))),
            GENERIC
        );
        assert_eq!(
            error_text(&zrangebyscore(
                &mut db,
                &f(&[b"z", b"0", b"5", b"LIMIT", b"notanint", b"5"])
            )),
            GENERIC
        );
        assert_eq!(
            error_text(&zrevrangebyscore(
                &mut db,
                &f(&[b"z", b"5", b"0", b"LIMIT", b"notanint", b"5"])
            )),
            GENERIC
        );
        assert_eq!(
            error_text(&zrandmember(&mut db, &f(&[b"z", b"notanint"]))),
            GENERIC
        );
        assert_eq!(
            error_text(&zrangestore(&mut db, &f(&[b"d", b"z", b"notanint", b"5"]))),
            GENERIC
        );
    }

    /// moon#792. `CH` counted a rescore only when the score moved by MORE than
    /// an absolute `f64::EPSILON`, so a real change smaller than ~2.2e-16 was
    /// reported as no change — while the stored score really did move, which
    /// the `ZSCORE` assertions below prove. Redis's `zsetAdd` compares
    /// EXACTLY (`score != curscore`).
    #[test]
    fn ch_counts_a_sub_epsilon_rescore() {
        // `nextafter(1.0)` — the smallest representable move from 1.0, whose
        // distance is EXACTLY `f64::EPSILON` and so failed the old `>` test.
        const NUDGED: &[u8] = b"1.0000000000000002";

        for member in [LP_MEMBER, BT_MEMBER] {
            let mut db = Database::new();
            assert_eq!(run_zadd(&mut db, &[b"z", b"1", member]), Frame::Integer(1));
            assert_eq!(
                run_zadd(&mut db, &[b"z", b"CH", NUDGED, member]),
                Frame::Integer(1),
                "CH must count a sub-epsilon rescore ({})",
                if member == LP_MEMBER {
                    "listpack"
                } else {
                    "bptree"
                }
            );
            assert_eq!(
                run_zscore(&mut db, &[b"z", member]),
                Frame::BulkString(Bytes::from_static(NUDGED)),
                "and the score really did move"
            );
            // Re-writing the SAME score is still no change.
            assert_eq!(
                run_zadd(&mut db, &[b"z", b"CH", NUDGED, member]),
                Frame::Integer(0)
            );
        }
    }

    /// moon#967. Redis defines a negative LIMIT offset as "return nothing".
    /// moon parsed it as a plain i64 and clamped it to 0 with `.max(0)`,
    /// returning a non-empty result.
    #[test]
    fn a_negative_limit_offset_returns_nothing() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"k", b"1", b"a", b"2", b"b"]);

        let empty = Frame::Array(framevec![]);
        assert_eq!(
            zrangebyscore(
                &mut db,
                &[
                    bulk(b"k"),
                    bulk(b"-inf"),
                    bulk(b"+inf"),
                    bulk(b"LIMIT"),
                    bulk(b"-1"),
                    bulk(b"2"),
                ]
            ),
            empty
        );
        // A zero offset is not negative and still returns the range.
        assert_eq!(
            zrangebyscore(
                &mut db,
                &[
                    bulk(b"k"),
                    bulk(b"-inf"),
                    bulk(b"+inf"),
                    bulk(b"LIMIT"),
                    bulk(b"0"),
                    bulk(b"2"),
                ]
            ),
            Frame::Array(framevec![bulk(b"a"), bulk(b"b")])
        );
    }

    // ---- moon#960: arithmetic that produces NaN ---------------------------
    //
    // Redis has TWO different rules here and moon had neither. Both were read
    // off a live redis-server 8.6.1 oracle:
    //
    //   ZINCRBY  -> `ERR resulting score is not a number (NaN)`, score UNTOUCHED
    //   aggregate-> clamp the NaN to 0.0 and store it, no error
    //
    // The asymmetry is deliberate in Redis: an explicit single-key increment
    // is a user error, whereas an aggregate combining two sets is not, so
    // `zunionInterAggregate` keeps "the convention of the result being 0.0".

    #[test]
    fn zincrby_to_nan_errors_and_leaves_the_score_untouched() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"k", b"inf", b"m"]);

        let got = zincrby(&mut db, &[bulk(b"k"), bulk(b"-inf"), bulk(b"m")]);
        match got {
            Frame::Error(ref e) => assert_eq!(
                e.as_ref(),
                b"ERR resulting score is not a number (NaN)".as_ref(),
                "error text must match redis 8.6.1 exactly"
            ),
            other => panic!("expected the NaN error, got {other:?}"),
        }

        // "Untouched" is the half that makes this a data-integrity fix rather
        // than a cosmetic one: the member must still hold its old score, and
        // must not have been dropped or rewritten as 0.
        assert_eq!(
            run_zscore(&mut db, &[b"k", b"m"]),
            Frame::BulkString(Bytes::from_static(b"inf"))
        );
        assert_eq!(run_zcard(&mut db, &[b"k"]), Frame::Integer(1));
    }

    #[test]
    fn an_aggregate_that_reaches_nan_clamps_to_zero() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"s1", b"inf", b"m"]);
        run_zadd(&mut db, &[b"s2", b"-inf", b"m"]);

        // inf + -inf across two sets: Redis stores 0, not NaN, and does not error.
        let n = zunionstore(&mut db, &[bulk(b"d"), bulk(b"2"), bulk(b"s1"), bulk(b"s2")]);
        assert_eq!(n, Frame::Integer(1), "ZUNIONSTORE must succeed, not error");
        assert_eq!(
            run_zscore(&mut db, &[b"d", b"m"]),
            Frame::BulkString(Bytes::from_static(b"0")),
            "a NaN aggregate result is stored as 0"
        );

        // The weight multiply is the other way in: inf * 0 is also NaN, and
        // Redis clamps that too, before any aggregation happens.
        let n2 = zunionstore(
            &mut db,
            &[
                bulk(b"d2"),
                bulk(b"1"),
                bulk(b"s1"),
                bulk(b"WEIGHTS"),
                bulk(b"0"),
            ],
        );
        assert_eq!(n2, Frame::Integer(1));
        assert_eq!(
            run_zscore(&mut db, &[b"d2", b"m"]),
            Frame::BulkString(Bytes::from_static(b"0")),
            "inf * 0 is NaN and must be clamped at the weight multiply"
        );
    }

    // ---- moon#966 / moon#961: score-range bound comparison ----------------
    //
    // Both were reproduced against a live redis-server 8.6.1 oracle before
    // this test was written. `ScoreBound` is the single decision point for
    // every BYSCORE range, so both live here.

    /// moon#966. `f64::EPSILON` is the gap between 1.0 and the next double —
    /// a RELATIVE quantity. Used as an ABSOLUTE tolerance it swallows real
    /// differences at every magnitude below 1: one ulp at 0.5 is 1.11e-16,
    /// which is under EPSILON, so two distinct doubles compare "equal" and a
    /// member strictly outside the range is reported inside it.
    ///
    /// Probe at magnitude 1 and this cannot fail — one ulp there IS EPSILON,
    /// so `< EPSILON` is false. The magnitudes below are load-bearing.
    #[test]
    fn a_bound_excludes_a_score_one_ulp_outside_it() {
        // 0.5000000000000001 is the next double after 0.5.
        let just_above_half = 0.5_f64 + f64::EPSILON / 2.0;
        assert!(just_above_half > 0.5, "test premise: distinct doubles");

        assert!(
            !ScoreBound::Inclusive(0.5).includes_upper(just_above_half),
            "a score strictly greater than the inclusive upper bound must be excluded"
        );
        assert!(
            !ScoreBound::Inclusive(0.5).includes(0.5 - f64::EPSILON / 4.0),
            "a score strictly less than the inclusive lower bound must be excluded"
        );

        // The same at a magnitude where the absolute tolerance is millions of ulps.
        let tiny = 1.0e-10_f64;
        assert!(
            !ScoreBound::Inclusive(tiny).includes_upper(tiny + f64::EPSILON / 2.0),
            "the tolerance must not scale with how small the bound is"
        );

        // Exact equality still belongs to an inclusive bound.
        assert!(ScoreBound::Inclusive(0.5).includes(0.5));
        assert!(ScoreBound::Inclusive(0.5).includes_upper(0.5));
    }

    /// moon#961. `NegInf`/`PosInf` answered `true` in BOTH directions, so an
    /// infinite bound admitted everything regardless of which end it sat on.
    /// `ZRANGEBYSCORE k +inf -inf` returned the whole set; Redis returns empty.
    #[test]
    fn an_infinite_bound_respects_the_end_it_sits_on() {
        // -inf as a LOWER bound admits everything; as an UPPER bound it admits
        // only -inf itself.
        assert!(ScoreBound::NegInf.includes(-1.0e300));
        assert!(ScoreBound::NegInf.includes(f64::NEG_INFINITY));
        assert!(!ScoreBound::NegInf.includes_upper(0.0));
        assert!(ScoreBound::NegInf.includes_upper(f64::NEG_INFINITY));

        // +inf as an UPPER bound admits everything; as a LOWER bound only +inf.
        assert!(ScoreBound::PosInf.includes_upper(1.0e300));
        assert!(ScoreBound::PosInf.includes_upper(f64::INFINITY));
        assert!(!ScoreBound::PosInf.includes(0.0));
        assert!(ScoreBound::PosInf.includes(f64::INFINITY));
    }

    /// moon#961, the other half. `zrange_by_score`'s contract is stated on the
    /// function itself: *"All callers pass (min, max) in semantic order
    /// regardless of rev"*. `ZRANGE` was the caller that did not — it handed
    /// argv straight through, so the Redis invocation (max first when `REV` is
    /// set) arrived as an inverted range and matched nothing.
    #[test]
    fn zrange_byscore_rev_reads_its_bounds_max_first() {
        let mut db = Database::new();
        run_zadd(&mut db, &[b"k", b"1", b"a", b"2", b"b", b"3", b"c"]);

        // The Redis spelling: max, then min.
        let got = zrange(
            &mut db,
            &[
                bulk(b"k"),
                bulk(b"3"),
                bulk(b"1"),
                bulk(b"BYSCORE"),
                bulk(b"REV"),
            ],
        );
        assert_eq!(
            got,
            Frame::Array(framevec![bulk(b"c"), bulk(b"b"), bulk(b"a")]),
            "ZRANGE k 3 1 BYSCORE REV must walk the range in reverse"
        );

        // Without REV the order is the plain one, and the bounds are min-first.
        let fwd = zrange(
            &mut db,
            &[bulk(b"k"), bulk(b"1"), bulk(b"3"), bulk(b"BYSCORE")],
        );
        assert_eq!(
            fwd,
            Frame::Array(framevec![bulk(b"a"), bulk(b"b"), bulk(b"c")])
        );

        // REV on a plain index range does NOT swap: start/stop stay start/stop.
        let by_rank = zrange(
            &mut db,
            &[bulk(b"k"), bulk(b"0"), bulk(b"-1"), bulk(b"REV")],
        );
        assert_eq!(
            by_rank,
            Frame::Array(framevec![bulk(b"c"), bulk(b"b"), bulk(b"a")]),
            "an index range with REV reverses output but keeps start/stop order"
        );
    }

    /// The `+inf -inf` inversion end to end: an empty answer, not the whole set.
    #[test]
    fn an_inverted_infinite_range_matches_nothing() {
        let lo = ScoreBound::PosInf;
        let hi = ScoreBound::NegInf;
        for score in [-1.0e300, -1.0, 0.0, 1.0, 1.0e300] {
            assert!(
                !(lo.includes(score) && hi.includes_upper(score)),
                "score {score} must not fall inside [+inf, -inf]"
            );
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

/// moon#959 — the six commands that used to be `unknown command`, and
/// `ZADD ... INCR`. Every expectation below was read off redis-server 8.6.1
/// on the wire (`/tmp/z959/oracle_vs_control.txt` in the PR) before the code
/// was written; the reply bytes are what these assert, not `COMMAND INFO`.
///
/// Dispatch-path coverage, stated per CLAUDE.md's three-path rule:
/// * `command::dispatch` (the mutable path MULTI/EXEC, Lua and every write
///   take) — every `call(...)` below goes through it.
/// * `command::dispatch_read` (the shared-lock path a bare read takes) —
///   `call_read(...)` for the two lex reads, plus the prefilter check in
///   `dispatch_read_serves_the_lex_reads`.
/// * `server::conn::try_inline_dispatch` inlines exactly `GET` and a plain
///   `SET` (`blocking.rs`); every other command falls through to generic
///   dispatch, so there is no arm for a zset command to be missing from.
#[cfg(test)]
mod missing_commands_959_tests {
    use super::*;
    use crate::command::{DispatchResult, dispatch, dispatch_read, is_dispatch_read_supported};
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn argv(args: &[&str]) -> Vec<Frame> {
        args.iter().map(|a| bs(a.as_bytes())).collect()
    }

    /// Through the real mutable dispatch table, so a missing arm shows up as
    /// `unknown command` rather than as a handler that was never reached.
    fn call(db: &mut Database, cmd: &str, args: &[&str]) -> Frame {
        let mut selected = 0usize;
        match dispatch(db, cmd.as_bytes(), &argv(args), &mut selected, 16) {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => panic!("unexpected Quit for {cmd}: {f:?}"),
        }
    }

    /// Through the shared-lock read table.
    fn call_read(db: &Database, cmd: &str, args: &[&str]) -> Frame {
        let mut selected = 0usize;
        let now_ms = db.now_ms();
        match dispatch_read(db, cmd.as_bytes(), &argv(args), now_ms, &mut selected, 16) {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => panic!("unexpected Quit for {cmd}: {f:?}"),
        }
    }

    fn seed(db: &mut Database, key: &str, pairs: &[(&str, &str)]) {
        let mut a = vec![key];
        for (s, m) in pairs {
            a.push(s);
            a.push(m);
        }
        let n = call(db, "ZADD", &a);
        assert_eq!(n, Frame::Integer(pairs.len() as i64), "seeding {key}");
    }

    const FIVE: &[(&str, &str)] = &[("1", "a"), ("2", "b"), ("3", "c"), ("4", "d"), ("5", "e")];
    const LEX: &[(&str, &str)] = &[("0", "a"), ("0", "b"), ("0", "c"), ("0", "d"), ("0", "e")];

    /// A member past `zset-max-listpack-value` (64) forces the B+tree form.
    /// All `z`s so it sorts AFTER every fixture member under a lex range.
    const LONG: &str = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";

    fn strings(frame: &Frame) -> Vec<String> {
        match frame {
            Frame::Array(items) => items
                .iter()
                .map(|f| match f {
                    Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                    other => panic!("not a bulk string: {other:?}"),
                })
                .collect(),
            other => panic!("not an array: {other:?}"),
        }
    }

    fn range(db: &mut Database, key: &str) -> Vec<String> {
        strings(&call(db, "ZRANGE", &[key, "0", "-1"]))
    }

    fn err_text(frame: &Frame) -> String {
        match frame {
            Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    fn encoding_of(db: &mut Database, key: &str) -> String {
        match crate::command::key::object(db, &[bs(b"ENCODING"), bs(key.as_bytes())]) {
            Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
            other => panic!("OBJECT ENCODING did not reply a bulk string: {other:?}"),
        }
    }

    fn exists(db: &mut Database, key: &str) -> bool {
        call(db, "EXISTS", &[key]) == Frame::Integer(1)
    }

    fn ledger_exact(db: &mut Database, step: &str) {
        let running = db.estimated_memory();
        db.recalculate_memory();
        let recomputed = db.estimated_memory();
        assert_eq!(
            running, recomputed,
            "{step}: ledger {running} != recount {recomputed}"
        );
    }

    // ── the tripwire: nothing here is `unknown command` any more ────────

    #[test]
    fn all_six_are_dispatched_and_the_negative_control_is_not() {
        let mut db = Database::new();
        seed(&mut db, "z", FIVE);
        for (cmd, args) in [
            ("ZRANGEBYLEX", &["z", "-", "+"][..]),
            ("ZREVRANGEBYLEX", &["z", "+", "-"][..]),
            ("ZREMRANGEBYRANK", &["z", "0", "0"][..]),
            ("ZREMRANGEBYSCORE", &["z", "0", "0"][..]),
            ("ZREMRANGEBYLEX", &["z", "[zz", "[zz"][..]),
            ("ZDIFFSTORE", &["d", "1", "z"][..]),
        ] {
            let reply = call(&mut db, cmd, args);
            assert!(
                !matches!(&reply, Frame::Error(e) if e.starts_with(b"ERR unknown command")),
                "{cmd} is still unknown to dispatch: {reply:?}"
            );
            // Lower-case, as redis-py sends it.
            let reply = call(&mut db, &cmd.to_ascii_lowercase(), args);
            assert!(
                !matches!(&reply, Frame::Error(e) if e.starts_with(b"ERR unknown command")),
                "{cmd} (lower-case) is still unknown to dispatch: {reply:?}"
            );
        }
        // The negative control: the same shape the issue used, still refused.
        let reply = call(&mut db, "ZNOTACOMMAND", &["z"]);
        assert!(
            err_text(&reply).starts_with("ERR unknown command"),
            "{reply:?}"
        );
    }

    #[test]
    fn registry_carries_the_six_with_the_sortedset_category() {
        use crate::command::metadata::{AclCategories, CommandFlags, lookup};
        for (name, write, arity) in [
            ("ZRANGEBYLEX", false, -4),
            ("ZREVRANGEBYLEX", false, -4),
            ("ZREMRANGEBYRANK", true, 4),
            ("ZREMRANGEBYSCORE", true, 4),
            ("ZREMRANGEBYLEX", true, 4),
            ("ZDIFFSTORE", true, -4),
        ] {
            let meta = lookup(name.as_bytes()).unwrap_or_else(|| panic!("{name} not registered"));
            assert_eq!(meta.arity, arity, "{name} arity");
            assert_eq!(
                meta.flags.contains(CommandFlags::WRITE),
                write,
                "{name} write flag"
            );
            assert_eq!(
                meta.flags.contains(CommandFlags::READONLY),
                !write,
                "{name} read flag"
            );
            assert!(
                meta.acl_categories.contains(AclCategories::SORTEDSET),
                "{name} must be @sortedset"
            );
            assert_eq!(meta.first_key, 1, "{name} first key");
        }
    }

    #[test]
    fn dispatch_read_serves_the_lex_reads() {
        let mut db = Database::new();
        seed(&mut db, "lex", LEX);
        assert!(is_dispatch_read_supported(b"ZRANGEBYLEX"));
        assert!(is_dispatch_read_supported(b"ZREVRANGEBYLEX"));
        assert_eq!(
            strings(&call_read(&db, "ZRANGEBYLEX", &["lex", "[b", "(d"])),
            ["b", "c"]
        );
        assert_eq!(
            strings(&call_read(&db, "ZREVRANGEBYLEX", &["lex", "(d", "[b"])),
            ["c", "b"]
        );
        // The read path answered from the listpack without flattening it.
        assert_eq!(encoding_of(&mut db, "lex"), "listpack");
    }

    // ── ZRANGEBYLEX / ZREVRANGEBYLEX ─────────────────────────────────────

    #[test]
    fn zrangebylex_bounds_and_limit_match_the_oracle() {
        for promote in [false, true] {
            let mut db = Database::new();
            seed(&mut db, "lex", LEX);
            if promote {
                call(&mut db, "ZADD", &["lex", "0", LONG]);
                assert_eq!(encoding_of(&mut db, "lex"), "skiplist");
            }
            let r = |db: &mut Database, a: &[&str]| strings(&call(db, "ZRANGEBYLEX", a));
            let tail: &[&str] = if promote { &[LONG] } else { &[] };
            let mut all = vec!["a", "b", "c", "d", "e"];
            all.extend_from_slice(tail);
            assert_eq!(r(&mut db, &["lex", "-", "+"]), all, "promote={promote}");
            assert_eq!(r(&mut db, &["lex", "[b", "(d"]), ["b", "c"]);
            assert_eq!(r(&mut db, &["lex", "(b", "[d"]), ["c", "d"]);
            assert_eq!(r(&mut db, &["lex", "[c", "[c"]), ["c"]);
            // `(` and `[` alone are exclusive/inclusive EMPTY strings: every
            // member is > "" and none is <= "".
            assert!(r(&mut db, &["lex", "(", "["]).is_empty());
            assert_eq!(
                r(&mut db, &["lex", "-", "+", "LIMIT", "1", "2"]),
                ["b", "c"]
            );
            assert!(r(&mut db, &["lex", "-", "+", "LIMIT", "-1", "2"]).is_empty());
            assert!(r(&mut db, &["lex", "-", "+", "LIMIT", "0", "0"]).is_empty());
            let mut from_b = vec!["b", "c", "d", "e"];
            from_b.extend_from_slice(tail);
            assert_eq!(r(&mut db, &["lex", "-", "+", "LIMIT", "1", "-1"]), from_b);
            // Reversed bounds are an empty range, not an error.
            assert!(r(&mut db, &["lex", "+", "-"]).is_empty());
            // Lower-case option token.
            assert_eq!(
                r(&mut db, &["lex", "-", "+", "limit", "1", "2"]),
                ["b", "c"]
            );
            assert!(r(&mut db, &["nokey", "-", "+"]).is_empty());
        }
    }

    #[test]
    fn zrevrangebylex_takes_max_then_min_and_walks_backwards() {
        let mut db = Database::new();
        seed(&mut db, "lex", LEX);
        let r = |db: &mut Database, a: &[&str]| strings(&call(db, "ZREVRANGEBYLEX", a));
        assert_eq!(r(&mut db, &["lex", "+", "-"]), ["e", "d", "c", "b", "a"]);
        assert_eq!(r(&mut db, &["lex", "[d", "(b"]), ["d", "c"]);
        assert_eq!(r(&mut db, &["lex", "(d", "[b"]), ["c", "b"]);
        assert!(r(&mut db, &["lex", "-", "+"]).is_empty());
        assert_eq!(
            r(&mut db, &["lex", "+", "-", "LIMIT", "1", "2"]),
            ["d", "c"]
        );
        assert_eq!(
            r(&mut db, &["lex", "+", "-", "LIMIT", "1", "-1"]),
            ["d", "c", "b", "a"]
        );
        assert!(r(&mut db, &["lex", "+", "-", "LIMIT", "-1", "2"]).is_empty());
    }

    #[test]
    fn zrangebylex_error_surface_matches_the_oracle() {
        let mut db = Database::new();
        seed(&mut db, "lex", LEX);
        call(&mut db, "SET", &["str", "v"]);
        let e = |db: &mut Database, cmd: &str, a: &[&str]| err_text(&call(db, cmd, a));
        for cmd in ["ZRANGEBYLEX", "ZREVRANGEBYLEX"] {
            let lc = cmd.to_ascii_lowercase();
            assert_eq!(
                e(&mut db, cmd, &["lex", "-"]),
                format!("ERR wrong number of arguments for '{lc}' command")
            );
            assert_eq!(
                e(&mut db, cmd, &["lex", "a", "b"]),
                "ERR min or max not valid string range item"
            );
            assert_eq!(
                e(&mut db, cmd, &["lex", "", "+"]),
                "ERR min or max not valid string range item"
            );
            // The grammar is checked BEFORE the key: a missing key with a bad
            // bound is still an error, not an empty array.
            assert_eq!(
                e(&mut db, cmd, &["nokey", "a", "b"]),
                "ERR min or max not valid string range item"
            );
            assert_eq!(
                e(&mut db, cmd, &["lex", "-", "+", "WITHSCORES"]),
                "ERR syntax error, WITHSCORES not supported in combination with BYLEX"
            );
            // ... and the WITHSCORES refusal outranks a bad bound. (The
            // first draft had these the other way round; the oracle sweep of
            // the built binary caught it, which is why every row is sent.)
            assert_eq!(
                e(&mut db, cmd, &["lex", "a", "b", "WITHSCORES"]),
                "ERR syntax error, WITHSCORES not supported in combination with BYLEX"
            );
            assert_eq!(
                e(&mut db, cmd, &["lex", "-", "+", "LIMIT", "1"]),
                "ERR syntax error"
            );
            assert_eq!(
                e(&mut db, cmd, &["lex", "-", "+", "BOGUS"]),
                "ERR syntax error"
            );
            assert_eq!(
                e(&mut db, cmd, &["lex", "-", "+", "LIMIT", "notanint", "1"]),
                "ERR value is not an integer or out of range"
            );
            assert_eq!(
                e(&mut db, cmd, &["lex", "-", "+", "LIMIT", "1", "notanint"]),
                "ERR value is not an integer or out of range"
            );
            // The option loop runs first: a dangling LIMIT beats a bad bound.
            assert_eq!(
                e(&mut db, cmd, &["lex", "a", "b", "LIMIT", "1"]),
                "ERR syntax error"
            );
            assert!(e(&mut db, cmd, &["str", "-", "+"]).starts_with("WRONGTYPE"));
        }
    }

    // ── ZREMRANGEBYRANK ──────────────────────────────────────────────────

    #[test]
    fn zremrangebyrank_normalises_ranks_like_redis() {
        for promote in [false, true] {
            let cases: &[(&str, &str, i64, &[&str])] = &[
                ("0", "0", 1, &["b", "c", "d", "e"]),
                ("-2", "-1", 2, &["a", "b", "c"]),
                ("3", "1", 0, &["a", "b", "c", "d", "e"]),
                ("0", "100", 5, &[]),
                ("-100", "1", 2, &["c", "d", "e"]),
                ("5", "10", 0, &["a", "b", "c", "d", "e"]),
                ("-1", "-3", 0, &["a", "b", "c", "d", "e"]),
                // A stop still negative after normalisation is NOT clamped
                // to 0: redis 8.6.1 removes nothing here.
                ("-10", "-6", 0, &["a", "b", "c", "d", "e"]),
                ("2", "-2", 2, &["a", "b", "e"]),
                ("0", "-1", 5, &[]),
            ];
            for (start, stop, removed, left) in cases {
                let mut db = Database::new();
                seed(&mut db, "r", FIVE);
                if promote {
                    // Promote WITHOUT changing the membership under test.
                    call(&mut db, "ZADD", &["r", "9", LONG]);
                    call(&mut db, "ZREM", &["r", LONG]);
                    assert_eq!(encoding_of(&mut db, "r"), "skiplist");
                } else {
                    assert_eq!(encoding_of(&mut db, "r"), "listpack");
                }
                assert_eq!(
                    call(&mut db, "ZREMRANGEBYRANK", &["r", start, stop]),
                    Frame::Integer(*removed),
                    "ZREMRANGEBYRANK r {start} {stop} promote={promote}"
                );
                if left.is_empty() {
                    assert!(!exists(&mut db, "r"), "drained key must be gone");
                } else {
                    assert_eq!(
                        range(&mut db, "r"),
                        *left,
                        "{start} {stop} promote={promote}"
                    );
                    // A removal never converts the encoding (moon#897).
                    assert_eq!(
                        encoding_of(&mut db, "r"),
                        if promote { "skiplist" } else { "listpack" }
                    );
                }
                ledger_exact(
                    &mut db,
                    &format!("ZREMRANGEBYRANK {start} {stop} promote={promote}"),
                );
            }
        }
    }

    #[test]
    fn zremrangebyrank_error_surface_and_missing_key() {
        let mut db = Database::new();
        seed(&mut db, "r", FIVE);
        call(&mut db, "SET", &["str", "v"]);
        let e = |db: &mut Database, a: &[&str]| err_text(&call(db, "ZREMRANGEBYRANK", a));
        for bad in [
            &["r", "notanint", "1"][..],
            &["r", "1", "notanint"],
            &["r", "1.5", "2"],
        ] {
            assert_eq!(
                e(&mut db, bad),
                "ERR value is not an integer or out of range"
            );
        }
        for bad in [&["r", "1"][..], &["r", "1", "2", "3"], &["r"]] {
            assert_eq!(
                e(&mut db, bad),
                "ERR wrong number of arguments for 'zremrangebyrank' command"
            );
        }
        assert!(e(&mut db, &["str", "0", "1"]).starts_with("WRONGTYPE"));
        assert_eq!(
            range(&mut db, "r").len(),
            5,
            "no error may have removed anything"
        );
        // A bad index on a MISSING key is still the integer error (the range
        // is parsed before the lookup), and a good one answers 0 and creates
        // nothing.
        assert_eq!(
            e(&mut db, &["nokey", "x", "1"]),
            "ERR value is not an integer or out of range"
        );
        assert_eq!(
            call(&mut db, "ZREMRANGEBYRANK", &["nokey", "0", "1"]),
            Frame::Integer(0)
        );
        assert!(!exists(&mut db, "nokey"));
        ledger_exact(&mut db, "after the error surface");
    }

    // ── ZREMRANGEBYSCORE ─────────────────────────────────────────────────

    #[test]
    fn zremrangebyscore_bounds_match_the_oracle() {
        for promote in [false, true] {
            let cases: &[(&str, &str, i64, &[&str])] = &[
                ("2", "3", 2, &["a", "d", "e"]),
                ("(2", "3", 1, &["a", "b", "d", "e"]),
                ("-inf", "+inf", 5, &[]),
                ("3", "1", 0, &["a", "b", "c", "d", "e"]),
                ("+inf", "-inf", 0, &["a", "b", "c", "d", "e"]),
                ("(1", "(1", 0, &["a", "b", "c", "d", "e"]),
                ("(1", "2", 1, &["a", "c", "d", "e"]),
                ("(5", "inf", 0, &["a", "b", "c", "d", "e"]),
                ("5", "inf", 1, &["a", "b", "c", "d"]),
            ];
            for (min, max, removed, left) in cases {
                let mut db = Database::new();
                seed(&mut db, "s", FIVE);
                if promote {
                    call(&mut db, "ZADD", &["s", "9", LONG]);
                    call(&mut db, "ZREM", &["s", LONG]);
                    assert_eq!(encoding_of(&mut db, "s"), "skiplist");
                }
                assert_eq!(
                    call(&mut db, "ZREMRANGEBYSCORE", &["s", min, max]),
                    Frame::Integer(*removed),
                    "ZREMRANGEBYSCORE s {min} {max} promote={promote}"
                );
                if left.is_empty() {
                    assert!(!exists(&mut db, "s"));
                } else {
                    assert_eq!(range(&mut db, "s"), *left, "{min} {max} promote={promote}");
                }
                ledger_exact(
                    &mut db,
                    &format!("ZREMRANGEBYSCORE {min} {max} promote={promote}"),
                );
            }
        }
    }

    #[test]
    fn zremrangebyscore_error_surface_and_missing_key() {
        let mut db = Database::new();
        seed(&mut db, "s", FIVE);
        call(&mut db, "SET", &["str", "v"]);
        let e = |db: &mut Database, a: &[&str]| err_text(&call(db, "ZREMRANGEBYSCORE", a));
        for bad in [&["s", "nan", "1"][..], &["s", "a", "1"], &["s", "1", "a"]] {
            assert_eq!(e(&mut db, bad), "ERR min or max is not a float");
        }
        for bad in [&["s", "1"][..], &["s", "1", "2", "3"]] {
            assert_eq!(
                e(&mut db, bad),
                "ERR wrong number of arguments for 'zremrangebyscore' command"
            );
        }
        assert!(e(&mut db, &["str", "0", "1"]).starts_with("WRONGTYPE"));
        assert_eq!(
            e(&mut db, &["nokey", "x", "1"]),
            "ERR min or max is not a float"
        );
        assert_eq!(
            call(&mut db, "ZREMRANGEBYSCORE", &["nokey", "0", "1"]),
            Frame::Integer(0)
        );
        assert!(!exists(&mut db, "nokey"));
        assert_eq!(range(&mut db, "s").len(), 5);
    }

    // ── ZREMRANGEBYLEX ───────────────────────────────────────────────────

    #[test]
    fn zremrangebylex_bounds_match_the_oracle() {
        for promote in [false, true] {
            let cases: &[(&str, &str, i64, &[&str])] = &[
                ("[b", "(d", 2, &["a", "d", "e"]),
                ("-", "+", 5, &[]),
                ("+", "-", 0, &["a", "b", "c", "d", "e"]),
                ("(c", "+", 2, &["a", "b", "c"]),
                ("[zz", "[zz", 0, &["a", "b", "c", "d", "e"]),
            ];
            for (min, max, removed, left) in cases {
                let mut db = Database::new();
                seed(&mut db, "l", LEX);
                if promote {
                    call(&mut db, "ZADD", &["l", "0", LONG]);
                    call(&mut db, "ZREM", &["l", LONG]);
                    assert_eq!(encoding_of(&mut db, "l"), "skiplist");
                }
                assert_eq!(
                    call(&mut db, "ZREMRANGEBYLEX", &["l", min, max]),
                    Frame::Integer(*removed),
                    "ZREMRANGEBYLEX l {min} {max} promote={promote}"
                );
                if left.is_empty() {
                    assert!(!exists(&mut db, "l"));
                } else {
                    assert_eq!(range(&mut db, "l"), *left, "{min} {max} promote={promote}");
                }
                ledger_exact(
                    &mut db,
                    &format!("ZREMRANGEBYLEX {min} {max} promote={promote}"),
                );
            }
        }
    }

    #[test]
    fn zremrangebylex_error_surface_and_missing_key() {
        let mut db = Database::new();
        seed(&mut db, "l", LEX);
        call(&mut db, "SET", &["str", "v"]);
        let e = |db: &mut Database, a: &[&str]| err_text(&call(db, "ZREMRANGEBYLEX", a));
        assert_eq!(
            e(&mut db, &["l", "a", "b"]),
            "ERR min or max not valid string range item"
        );
        assert_eq!(
            e(&mut db, &["l", "", "+"]),
            "ERR min or max not valid string range item"
        );
        for bad in [&["l", "-"][..], &["l", "-", "+", "x"]] {
            assert_eq!(
                e(&mut db, bad),
                "ERR wrong number of arguments for 'zremrangebylex' command"
            );
        }
        assert!(e(&mut db, &["str", "-", "+"]).starts_with("WRONGTYPE"));
        assert_eq!(
            e(&mut db, &["nokey", "x", "1"]),
            "ERR min or max not valid string range item"
        );
        assert_eq!(
            call(&mut db, "ZREMRANGEBYLEX", &["nokey", "-", "+"]),
            Frame::Integer(0)
        );
        assert!(!exists(&mut db, "nokey"));
        assert_eq!(range(&mut db, "l").len(), 5);
    }

    // ── ZDIFFSTORE ───────────────────────────────────────────────────────

    fn scored(db: &mut Database, key: &str) -> Vec<String> {
        strings(&call(db, "ZRANGE", &[key, "0", "-1", "WITHSCORES"]))
    }

    #[test]
    fn zdiffstore_computes_the_difference_with_first_source_scores() {
        let mut db = Database::new();
        seed(&mut db, "z", FIVE);
        seed(&mut db, "z2", &[("1", "a"), ("2", "b")]);
        seed(&mut db, "z3", &[("2", "b"), ("9", "x")]);
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["d1", "2", "z", "z2"]),
            Frame::Integer(3)
        );
        assert_eq!(scored(&mut db, "d1"), ["c", "3", "d", "4", "e", "5"]);
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["d2", "1", "z"]),
            Frame::Integer(5)
        );
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["d3", "2", "z", "nokey"]),
            Frame::Integer(5)
        );
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["d8", "3", "z", "z2", "z3"]),
            Frame::Integer(3)
        );
        assert_eq!(scored(&mut db, "d8"), ["c", "3", "d", "4", "e", "5"]);
        // Sources are read before the destination is replaced, so a
        // destination that is also a source is diffed from its OLD content.
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["z3", "2", "z", "z3"]),
            Frame::Integer(4)
        );
        assert_eq!(
            scored(&mut db, "z3"),
            ["a", "1", "c", "3", "d", "4", "e", "5"]
        );
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["z2", "1", "z2"]),
            Frame::Integer(2)
        );
        assert_eq!(scored(&mut db, "z2"), ["a", "1", "b", "2"]);
        // Lower-case, as a client library sends it.
        assert_eq!(
            call(&mut db, "zdiffstore", &["d9", "1", "z"]),
            Frame::Integer(5)
        );
        // Reading a listpack source did not flatten it.
        assert_eq!(encoding_of(&mut db, "z"), "listpack");
        ledger_exact(&mut db, "after the ZDIFFSTORE happy paths");
    }

    #[test]
    fn zdiffstore_empty_result_deletes_the_destination() {
        let mut db = Database::new();
        seed(&mut db, "z", FIVE);
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["d4", "2", "nokey", "z"]),
            Frame::Integer(0)
        );
        assert!(!exists(&mut db, "d4"));
        // Even a destination of another type is replaced — by nothing.
        call(&mut db, "SET", &["d5", "x"]);
        assert_eq!(
            call(&mut db, "ZDIFFSTORE", &["d5", "2", "z", "z"]),
            Frame::Integer(0)
        );
        assert!(!exists(&mut db, "d5"));
        ledger_exact(&mut db, "after an empty ZDIFFSTORE");
    }

    #[test]
    fn zdiffstore_error_surface_matches_the_oracle() {
        let mut db = Database::new();
        seed(&mut db, "z", FIVE);
        call(&mut db, "SET", &["str", "v"]);
        let e = |db: &mut Database, a: &[&str]| err_text(&call(db, "ZDIFFSTORE", a));
        // The two-class numkeys split (moon#969).
        assert_eq!(
            e(&mut db, &["d", "0", "z"]),
            "ERR at least 1 input key is needed for 'zdiffstore' command"
        );
        assert_eq!(
            e(&mut db, &["d", "-1", "z"]),
            "ERR at least 1 input key is needed for 'zdiffstore' command"
        );
        assert_eq!(
            e(&mut db, &["d", "notanint", "z"]),
            "ERR value is not an integer or out of range"
        );
        // Arity first: no key named at all.
        for bad in [&["d", "1"][..], &["d"], &["d", "0"]] {
            assert_eq!(
                e(&mut db, bad),
                "ERR wrong number of arguments for 'zdiffstore' command"
            );
        }
        // numkeys overrunning the key list, and every option token: ZDIFFSTORE
        // takes none, so WEIGHTS/AGGREGATE are as unknown as BOGUS.
        assert_eq!(e(&mut db, &["d", "2", "z"]), "ERR syntax error");
        for opts in [
            &["WEIGHTS", "1"][..],
            &["AGGREGATE", "SUM"],
            &["WITHSCORES"],
            &["BOGUS"],
        ] {
            let mut a = vec!["d", "1", "z"];
            a.extend_from_slice(opts);
            assert_eq!(e(&mut db, &a), "ERR syntax error", "{opts:?}");
        }
        assert!(
            !exists(&mut db, "d"),
            "no error may have created the destination"
        );
        // WRONGTYPE from either position, and it outranks an option error:
        // Redis looks the sources up before it parses the options.
        assert!(e(&mut db, &["d", "2", "str", "z"]).starts_with("WRONGTYPE"));
        assert!(e(&mut db, &["d", "2", "z", "str"]).starts_with("WRONGTYPE"));
        assert!(e(&mut db, &["d", "1", "str", "BOGUS"]).starts_with("WRONGTYPE"));
        // ... while a numkeys error or an overrun is decided before the lookup.
        assert_eq!(
            e(&mut db, &["d", "0", "str", "BOGUS"]),
            "ERR at least 1 input key is needed for 'zdiffstore' command"
        );
        assert_eq!(e(&mut db, &["d", "2", "str"]), "ERR syntax error");
        // The destination's type is irrelevant until the write.
        assert_eq!(e(&mut db, &["str", "1", "z", "BOGUS"]), "ERR syntax error");
        assert!(!exists(&mut db, "d"));
    }

    /// The precedence fix above applies to the whole family, since the three
    /// share one implementation: `ZUNIONSTORE d 1 <string> BOGUS` is
    /// WRONGTYPE on redis 8.6.1, and was `syntax error` on moon.
    #[test]
    fn zunionstore_wrongtype_outranks_an_option_error() {
        let mut db = Database::new();
        seed(&mut db, "z", FIVE);
        call(&mut db, "SET", &["str", "v"]);
        for (cmd, opts) in [
            ("ZUNIONSTORE", &["BOGUS"][..]),
            ("ZUNIONSTORE", &["WEIGHTS", "nan"]),
            ("ZINTERSTORE", &["WEIGHTS", "1"]),
        ] {
            let mut a = vec!["d", "2", "z", "str"];
            a.extend_from_slice(opts);
            assert!(
                err_text(&call(&mut db, cmd, &a)).starts_with("WRONGTYPE"),
                "{cmd} {opts:?}"
            );
        }
        // A well-typed source with a bad option is still the option's error.
        assert_eq!(
            err_text(&call(&mut db, "ZUNIONSTORE", &["d", "1", "z", "BOGUS"])),
            "ERR syntax error"
        );
        assert_eq!(
            err_text(&call(
                &mut db,
                "ZUNIONSTORE",
                &["d", "1", "z", "WEIGHTS", "nan"]
            )),
            "ERR weight value is not a float"
        );
        // And reading a listpack source through the store family leaves it a
        // listpack (the moon#928 defect, closed for this family too).
        assert_eq!(
            call(&mut db, "ZUNIONSTORE", &["u", "1", "z"]),
            Frame::Integer(5)
        );
        assert_eq!(encoding_of(&mut db, "z"), "listpack");
    }

    // ── ZADD ... INCR ────────────────────────────────────────────────────

    fn bulk_text(frame: &Frame) -> String {
        match frame {
            Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("expected a bulk string, got {other:?}"),
        }
    }

    #[test]
    fn zadd_incr_replies_the_new_score_on_both_encodings() {
        for promote in [false, true] {
            let mut db = Database::new();
            let key = "i";
            if promote {
                call(&mut db, "ZADD", &[key, "1", LONG]);
                assert_eq!(encoding_of(&mut db, key), "skiplist");
            }
            let incr = |db: &mut Database, a: &[&str]| call(db, "ZADD", a);
            assert_eq!(bulk_text(&incr(&mut db, &[key, "INCR", "5", "a"])), "5");
            assert_eq!(bulk_text(&incr(&mut db, &[key, "INCR", "2.5", "a"])), "7.5");
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "INCR", "1e3", "big"])),
                "1000"
            );
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "INCR", "0.1", "big"])),
                "1000.1"
            );
            // Option order does not matter, and CH has no say in the reply.
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "CH", "INCR", "1", "a"])),
                "8.5"
            );
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "INCR", "CH", "1", "a"])),
                "9.5"
            );
            assert_eq!(bulk_text(&incr(&mut db, &[key, "incr", "1", "a"])), "10.5");
            assert_eq!(bulk_text(&call(&mut db, "ZSCORE", &[key, "a"])), "10.5");
            if !promote {
                assert_eq!(encoding_of(&mut db, key), "listpack");
            }
            ledger_exact(&mut db, &format!("after ZADD INCR promote={promote}"));
        }
    }

    #[test]
    fn zadd_incr_honours_nx_xx_gt_lt_like_redis() {
        for promote in [false, true] {
            let mut db = Database::new();
            let key = "i";
            if promote {
                call(&mut db, "ZADD", &[key, "1", LONG]);
            }
            let incr = |db: &mut Database, a: &[&str]| call(db, "ZADD", a);
            assert_eq!(bulk_text(&incr(&mut db, &[key, "INCR", "5", "a"])), "5");
            // NX: refuses a present member, admits a new one.
            assert_eq!(incr(&mut db, &[key, "NX", "INCR", "1", "a"]), Frame::Null);
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "NX", "INCR", "1", "newm"])),
                "1"
            );
            // XX: refuses a new member, admits a present one.
            assert_eq!(
                incr(&mut db, &[key, "XX", "INCR", "1", "nope"]),
                Frame::Null
            );
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "XX", "INCR", "1", "a"])),
                "6"
            );
            // GT/LT: only a move in the right direction; zero is a refusal.
            assert_eq!(incr(&mut db, &[key, "GT", "INCR", "-1", "a"]), Frame::Null);
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "GT", "INCR", "1", "a"])),
                "7"
            );
            assert_eq!(incr(&mut db, &[key, "LT", "INCR", "1", "a"]), Frame::Null);
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "LT", "INCR", "-1", "a"])),
                "6"
            );
            assert_eq!(incr(&mut db, &[key, "GT", "INCR", "0", "a"]), Frame::Null);
            assert_eq!(incr(&mut db, &[key, "LT", "INCR", "0", "a"]), Frame::Null);
            // GT/LT never block a first insert; XX+GT does.
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "GT", "INCR", "1", "zz"])),
                "1"
            );
            assert_eq!(
                bulk_text(&incr(&mut db, &[key, "LT", "INCR", "1", "yy"])),
                "1"
            );
            assert_eq!(
                incr(&mut db, &[key, "XX", "GT", "INCR", "1", "qq"]),
                Frame::Null
            );
            // A refusal wrote nothing.
            assert_eq!(bulk_text(&call(&mut db, "ZSCORE", &[key, "a"])), "6");
            assert_eq!(call(&mut db, "ZSCORE", &[key, "qq"]), Frame::Null);
            ledger_exact(
                &mut db,
                &format!("after flagged ZADD INCR promote={promote}"),
            );
        }
    }

    #[test]
    fn zadd_incr_refusal_on_a_missing_key_creates_nothing() {
        let mut db = Database::new();
        assert_eq!(
            call(&mut db, "ZADD", &["i3", "XX", "INCR", "1", "a"]),
            Frame::Null
        );
        assert!(!exists(&mut db, "i3"));
        // The B+tree arm too: a member too long for a listpack.
        assert_eq!(
            call(&mut db, "ZADD", &["i4", "XX", "INCR", "1", LONG]),
            Frame::Null
        );
        assert!(!exists(&mut db, "i4"));
        assert_eq!(
            bulk_text(&call(&mut db, "ZADD", &["i3", "NX", "INCR", "1", "a"])),
            "1"
        );
        ledger_exact(&mut db, "after refused ZADD INCR on missing keys");
    }

    #[test]
    fn zadd_incr_error_surface_matches_the_oracle() {
        let mut db = Database::new();
        seed(&mut db, "i", &[("1", "a")]);
        call(&mut db, "SET", &["str", "v"]);
        let e = |db: &mut Database, a: &[&str]| err_text(&call(db, "ZADD", a));
        assert_eq!(
            e(&mut db, &["i", "INCR", "1", "a", "2", "b"]),
            "ERR INCR option supports a single increment-element pair"
        );
        assert_eq!(
            e(&mut db, &["i", "INCR"]),
            "ERR wrong number of arguments for 'zadd' command"
        );
        // Parity is checked before the pair count ...
        assert_eq!(e(&mut db, &["i", "INCR", "1"]), "ERR syntax error");
        assert_eq!(
            e(&mut db, &["i", "INCR", "1", "a", "2"]),
            "ERR syntax error"
        );
        // ... and the flag pairings before both.
        assert_eq!(
            e(&mut db, &["i", "INCR", "NX", "XX", "1", "a", "2", "b"]),
            "ERR XX and NX options at the same time are not compatible"
        );
        assert_eq!(
            e(&mut db, &["i", "INCR", "GT", "LT", "1", "a"]),
            "ERR GT, LT, and/or NX options at the same time are not compatible"
        );
        assert_eq!(
            e(&mut db, &["i", "INCR", "nan", "a"]),
            "ERR value is not a valid float"
        );
        assert_eq!(
            e(&mut db, &["i", "INCR", "notafloat", "a"]),
            "ERR value is not a valid float"
        );
        assert!(e(&mut db, &["str", "INCR", "1", "a"]).starts_with("WRONGTYPE"));
        // inf + -inf is NaN: refused with the ZINCRBY message, score untouched.
        assert_eq!(
            bulk_text(&call(&mut db, "ZADD", &["i", "INCR", "inf", "a"])),
            "inf"
        );
        assert_eq!(
            e(&mut db, &["i", "INCR", "-inf", "a"]),
            "ERR resulting score is not a number (NaN)"
        );
        assert_eq!(bulk_text(&call(&mut db, "ZSCORE", &["i", "a"])), "inf");
        assert_eq!(
            encoding_of(&mut db, "i"),
            "listpack",
            "an erroring INCR must not flatten"
        );
        // NX outranks the NaN check: the sum is never formed for a present
        // member under NX.
        assert_eq!(
            call(&mut db, "ZADD", &["i", "NX", "INCR", "-inf", "a"]),
            Frame::Null
        );
    }

    /// The plain ZINCRBY went through the refactored core; its contract is
    /// unchanged.
    #[test]
    fn zincrby_is_unchanged_by_the_shared_core() {
        let mut db = Database::new();
        assert_eq!(bulk_text(&call(&mut db, "ZINCRBY", &["z", "5", "a"])), "5");
        assert_eq!(
            bulk_text(&call(&mut db, "ZINCRBY", &["z", "-2.5", "a"])),
            "2.5"
        );
        assert_eq!(
            bulk_text(&call(&mut db, "ZINCRBY", &["z", "inf", "a"])),
            "inf"
        );
        assert_eq!(
            err_text(&call(&mut db, "ZINCRBY", &["z", "-inf", "a"])),
            "ERR resulting score is not a number (NaN)"
        );
        assert_eq!(
            err_text(&call(&mut db, "ZINCRBY", &["z", "nan", "a"])),
            "ERR value is not a valid float"
        );
        assert_eq!(encoding_of(&mut db, "z"), "listpack");
        assert_eq!(bulk_text(&call(&mut db, "ZINCRBY", &["z", "1", LONG])), "1");
        assert_eq!(encoding_of(&mut db, "z"), "skiplist");
        assert_eq!(
            bulk_text(&call(&mut db, "ZINCRBY", &["z", "1", "a"])),
            "inf"
        );
        ledger_exact(&mut db, "after ZINCRBY through the shared core");
    }

    #[test]
    fn rank_window_follows_the_redis_rule() {
        assert_eq!(rank_window(0, 0, 5), Some((0, 0)));
        assert_eq!(rank_window(-2, -1, 5), Some((3, 4)));
        assert_eq!(rank_window(3, 1, 5), None);
        assert_eq!(rank_window(0, 100, 5), Some((0, 4)));
        assert_eq!(rank_window(-100, 1, 5), Some((0, 1)));
        assert_eq!(rank_window(5, 10, 5), None);
        assert_eq!(rank_window(-1, -3, 5), None);
        assert_eq!(rank_window(-10, -6, 5), None);
        assert_eq!(rank_window(2, -2, 5), Some((2, 3)));
        assert_eq!(rank_window(0, -1, 0), None);
        assert_eq!(rank_window(i64::MIN, i64::MAX, 5), Some((0, 4)));
        assert_eq!(rank_window(i64::MAX, i64::MAX, 5), None);
    }
}

/// moon#1060 — `ZRANGEBYSCORE`, `ZRANGE ... BYSCORE`/`BYLEX` and (found in the
/// same sweep) `ZREVRANGEBYSCORE` used to look the key up BEFORE validating
/// the min/max grammar, so a malformed bound against a MISSING key answered
/// `[]` where Redis parses the grammar unconditionally and answers a parse
/// error. `ZRANGEBYLEX`/`ZREVRANGEBYLEX` (moon#959) already had the order
/// right — they anchor the "still correct" side of every table below.
///
/// Dispatch-path coverage, per CLAUDE.md's three-path rule: every command
/// here delegates to a `*_readonly` function called from BOTH
/// `command::dispatch` (`call` below) and `command::dispatch_read` (`call_read`
/// below); `server::conn::try_inline_dispatch` inlines only `GET`/plain `SET`
/// and stands down to generic dispatch for everything else, so there is no
/// third arm for this family to be missing from.
#[cfg(test)]
mod missing_key_bound_validation_1060_tests {
    use super::*;
    use crate::command::{DispatchResult, dispatch, dispatch_read};
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn argv(args: &[&str]) -> Vec<Frame> {
        args.iter().map(|a| bs(a.as_bytes())).collect()
    }

    fn call(db: &mut Database, cmd: &str, args: &[&str]) -> Frame {
        let mut selected = 0usize;
        match dispatch(db, cmd.as_bytes(), &argv(args), &mut selected, 16) {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => panic!("unexpected Quit for {cmd}: {f:?}"),
        }
    }

    fn call_read(db: &Database, cmd: &str, args: &[&str]) -> Frame {
        let mut selected = 0usize;
        let now_ms = db.now_ms();
        match dispatch_read(db, cmd.as_bytes(), &argv(args), now_ms, &mut selected, 16) {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => panic!("unexpected Quit for {cmd}: {f:?}"),
        }
    }

    fn err_text(frame: &Frame) -> String {
        match frame {
            Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    /// Every row: (command, args against a key that does not exist, the
    /// error Redis gives). Measured against redis-server 8.6.1, raw socket.
    const BAD_BOUND_ON_MISSING_KEY: &[(&str, &[&str], &str)] = &[
        (
            "ZRANGEBYSCORE",
            &["nokey", "a", "b"],
            "ERR min or max is not a float",
        ),
        (
            "ZREVRANGEBYSCORE",
            &["nokey", "a", "b"],
            "ERR min or max is not a float",
        ),
        (
            "ZRANGE",
            &["nokey", "a", "b", "BYSCORE"],
            "ERR min or max is not a float",
        ),
        (
            "ZRANGE",
            &["nokey", "a", "b", "BYLEX"],
            "ERR min or max not valid string range item",
        ),
        (
            "ZRANGE",
            &["nokey", "b", "a", "BYSCORE", "REV"],
            "ERR min or max is not a float",
        ),
    ];

    #[test]
    fn bad_bound_on_a_missing_key_is_a_parse_error_not_empty_through_dispatch() {
        for (cmd, args, want) in BAD_BOUND_ON_MISSING_KEY {
            let mut db = Database::new();
            let reply = call(&mut db, cmd, args);
            assert_eq!(err_text(&reply), *want, "{cmd} {args:?} through dispatch()");
        }
    }

    #[test]
    fn bad_bound_on_a_missing_key_is_a_parse_error_not_empty_through_dispatch_read() {
        for (cmd, args, want) in BAD_BOUND_ON_MISSING_KEY {
            let db = Database::new();
            let reply = call_read(&db, cmd, args);
            assert_eq!(
                err_text(&reply),
                *want,
                "{cmd} {args:?} through dispatch_read()"
            );
        }
    }

    /// Negative control: the SAME commands, on the SAME missing key, with a
    /// grammatically valid bound, still answer the ordinary empty result —
    /// this fix must not turn a merely-absent key into an error.
    #[test]
    fn valid_bound_on_a_missing_key_still_answers_empty() {
        let mut db = Database::new();
        assert_eq!(
            call(&mut db, "ZRANGEBYSCORE", &["nokey", "0", "10"]),
            Frame::Array(framevec![])
        );
        assert_eq!(
            call(&mut db, "ZREVRANGEBYSCORE", &["nokey", "10", "0"]),
            Frame::Array(framevec![])
        );
        assert_eq!(
            call(&mut db, "ZRANGE", &["nokey", "0", "10", "BYSCORE"]),
            Frame::Array(framevec![])
        );
        assert_eq!(
            call(&mut db, "ZRANGE", &["nokey", "-", "+", "BYLEX"]),
            Frame::Array(framevec![])
        );
    }

    /// The already-correct siblings (moon#959) validate the SAME way; this
    /// pins that the fix did not have to touch them and that they still do.
    #[test]
    fn already_correct_siblings_are_unchanged() {
        let mut db = Database::new();
        assert_eq!(
            err_text(&call(&mut db, "ZRANGEBYLEX", &["nokey", "bad", "bound"])),
            "ERR min or max not valid string range item"
        );
        assert_eq!(
            err_text(&call(&mut db, "ZREVRANGEBYLEX", &["nokey", "bad", "bound"])),
            "ERR min or max not valid string range item"
        );
    }

    /// On a key that EXISTS, the bad bound already errored before this fix —
    /// this pins that the fix did not change that arm.
    #[test]
    fn bad_bound_on_an_existing_key_is_unchanged() {
        let mut db = Database::new();
        assert_eq!(call(&mut db, "ZADD", &["z", "1", "m"]), Frame::Integer(1));
        for (cmd, args, want) in BAD_BOUND_ON_MISSING_KEY {
            let args: Vec<&str> = std::iter::once(&"z")
                .chain(args.iter().skip(1))
                .copied()
                .collect();
            let reply = call(&mut db, cmd, &args);
            assert_eq!(err_text(&reply), *want, "{cmd} {args:?} on an existing key");
        }
    }
}

/// moon#1102 — `ZRANGESTORE` had the moon#1060 defect: it looked the SOURCE key
/// up before validating the range grammar, so a malformed bound against a
/// missing source answered `:0` — and, worse, went on to DELETE the
/// destination, which a parse error must never touch. Redis parses the whole
/// grammar (rank, `BYSCORE` or `BYLEX` bounds) before its key lookup, so the
/// error wins over a missing or wrong-type source.
///
/// Dispatch-path coverage: `ZRANGESTORE` is a write, reached only through
/// `command::dispatch` (`call` below). `command::dispatch_read` serves reads
/// and `server::conn::try_inline_dispatch` inlines only `GET`/plain `SET`, so
/// neither has an arm to be missing from.
#[cfg(test)]
mod zrangestore_bound_validation_1102_tests {
    use super::*;
    use crate::command::{DispatchResult, dispatch};
    use crate::storage::Database;

    fn call(db: &mut Database, args: &[&str]) -> Frame {
        let cmd = args[0];
        let argv: Vec<Frame> = args[1..]
            .iter()
            .map(|a| Frame::BulkString(Bytes::copy_from_slice(a.as_bytes())))
            .collect();
        let mut selected = 0usize;
        match dispatch(db, cmd.as_bytes(), &argv, &mut selected, 16) {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => panic!("unexpected Quit for {args:?}: {f:?}"),
        }
    }

    fn err_text(frame: &Frame) -> String {
        match frame {
            Frame::Error(e) => String::from_utf8_lossy(e).into_owned(),
            other => panic!("expected an error reply, got {other:?}"),
        }
    }

    const NOT_A_FLOAT: &str = "ERR min or max is not a float";
    const NOT_A_LEX_ITEM: &str = "ERR min or max not valid string range item";
    const NOT_AN_INT: &str = "ERR value is not an integer or out of range";

    /// Every row: (arguments after `ZRANGESTORE dst src`, the error Redis
    /// gives when `src` does not exist). Measured against redis-server 8.6.1
    /// over a raw socket.
    const BAD_RANGE: &[(&[&str], &str)] = &[
        (&["a", "b", "BYSCORE"], NOT_A_FLOAT),
        (&["0", "b", "BYSCORE"], NOT_A_FLOAT),
        (&["(1", "(a", "BYSCORE"], NOT_A_FLOAT),
        (&["nan", "1", "BYSCORE"], NOT_A_FLOAT),
        (&["a", "b", "BYSCORE", "REV"], NOT_A_FLOAT),
        (&["a", "b", "BYSCORE", "LIMIT", "0", "1"], NOT_A_FLOAT),
        (
            &["a", "b", "BYSCORE", "REV", "LIMIT", "0", "1"],
            NOT_A_FLOAT,
        ),
        (&["a", "b", "BYLEX"], NOT_A_LEX_ITEM),
        (&["b", "a", "BYLEX", "REV"], NOT_A_LEX_ITEM),
        (&["a", "b", "BYLEX", "LIMIT", "0", "1"], NOT_A_LEX_ITEM),
        (
            &["a", "b", "BYLEX", "REV", "LIMIT", "0", "1"],
            NOT_A_LEX_ITEM,
        ),
        (&["a", "b"], NOT_AN_INT),
        (&["0", "b"], NOT_AN_INT),
        (&["a", "1"], NOT_AN_INT),
        (&["a", "b", "REV"], NOT_AN_INT),
        (&["1.5", "2"], NOT_AN_INT),
        (&["99999999999999999999", "1"], NOT_AN_INT),
    ];

    fn zrangestore(db: &mut Database, src: &str, range: &[&str]) -> Frame {
        let mut args = vec!["ZRANGESTORE", "dst", src];
        args.extend_from_slice(range);
        call(db, &args)
    }

    #[test]
    fn bad_range_on_a_missing_source_is_a_parse_error() {
        for (range, want) in BAD_RANGE {
            let mut db = Database::new();
            let reply = zrangestore(&mut db, "nokey", range);
            assert_eq!(err_text(&reply), *want, "ZRANGESTORE dst nokey {range:?}");
        }
    }

    /// The parse error must leave the destination alone. Before the fix the
    /// missing-source arm fell through to "store the empty result", which
    /// deletes `dst` — a malformed command destroyed data.
    #[test]
    fn bad_range_on_a_missing_source_does_not_delete_the_destination() {
        for (range, _) in BAD_RANGE {
            let mut db = Database::new();
            let _ = call(&mut db, &["SET", "dst", "keep"]);
            let _ = zrangestore(&mut db, "nokey", range);
            assert_eq!(
                call(&mut db, &["EXISTS", "dst"]),
                Frame::Integer(1),
                "ZRANGESTORE dst nokey {range:?} must not delete dst"
            );
        }
    }

    /// Redis answers the parse error even when the source holds another type:
    /// the grammar is checked before the key is looked up at all.
    #[test]
    fn bad_range_on_a_wrong_type_source_is_a_parse_error_not_wrongtype() {
        for (range, want) in BAD_RANGE {
            let mut db = Database::new();
            let _ = call(&mut db, &["SET", "str", "x"]);
            let reply = zrangestore(&mut db, "str", range);
            assert_eq!(err_text(&reply), *want, "ZRANGESTORE dst str {range:?}");
        }
    }

    /// On an existing zset the bad range already errored before the fix; this
    /// pins that the reordering did not change that arm.
    #[test]
    fn bad_range_on_an_existing_source_is_unchanged() {
        for (range, want) in BAD_RANGE {
            let mut db = Database::new();
            assert_eq!(call(&mut db, &["ZADD", "z", "1", "m"]), Frame::Integer(1));
            let reply = zrangestore(&mut db, "z", range);
            assert_eq!(err_text(&reply), *want, "ZRANGESTORE dst z {range:?}");
        }
    }

    /// Negative controls: a VALID range on a missing source is still the
    /// ordinary empty store (`:0`, and the destination is removed, as Redis
    /// does), and a valid range on a wrong-type source is still WRONGTYPE.
    #[test]
    fn valid_range_on_a_missing_or_wrong_type_source_is_unchanged() {
        for range in [
            &["0", "1", "BYSCORE"][..],
            &["(0", "+inf", "BYSCORE", "REV", "LIMIT", "0", "1"][..],
            &["-", "+", "BYLEX"][..],
            &["[a", "(b", "BYLEX", "LIMIT", "0", "1"][..],
            &["0", "-1"][..],
            &["0", "-1", "REV"][..],
        ] {
            let mut db = Database::new();
            let _ = call(&mut db, &["SET", "dst", "keep"]);
            assert_eq!(
                zrangestore(&mut db, "nokey", range),
                Frame::Integer(0),
                "ZRANGESTORE dst nokey {range:?}"
            );
            assert_eq!(
                call(&mut db, &["EXISTS", "dst"]),
                Frame::Integer(0),
                "an empty ZRANGESTORE result removes dst ({range:?})"
            );
            let _ = call(&mut db, &["SET", "str", "x"]);
            assert!(
                err_text(&zrangestore(&mut db, "str", range)).starts_with("WRONGTYPE"),
                "ZRANGESTORE dst str {range:?}"
            );
        }
    }

    /// Option errors are still reported before the range is parsed, as in
    /// Redis: `LIMIT` without `BYSCORE`/`BYLEX` and a non-integer `LIMIT`.
    #[test]
    fn option_errors_still_win_over_the_range() {
        let mut db = Database::new();
        assert_eq!(
            err_text(&zrangestore(
                &mut db,
                "nokey",
                &["a", "b", "LIMIT", "0", "1"]
            )),
            "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"
        );
        assert_eq!(
            err_text(&zrangestore(
                &mut db,
                "nokey",
                &["a", "b", "BYSCORE", "LIMIT", "x", "1"]
            )),
            NOT_AN_INT
        );
        assert_eq!(
            err_text(&zrangestore(
                &mut db,
                "nokey",
                &["a", "b", "BYSCORE", "WITHSCORES"]
            )),
            "ERR syntax error"
        );
    }
}
