//! Master-side rewrite of a non-deterministic write to the EFFECT it had, for
//! propagation to the AOF and to replicas (moon#825).
//!
//! A command whose result depends on the RNG, the clock, or a condition
//! evaluated against the clock does not reproduce itself when replayed:
//! `SPOP` re-rolls and removes a different member, `XADD key *` re-reads the
//! clock and assigns a different ID, `EXPIRE key 100 NX` restarts the
//! countdown at apply time. The client was told one thing, the AOF replays
//! another, and nothing is logged. Redis's rule is to propagate the effect,
//! never the command, and this module is where moon applies it.
//!
//! The sibling [`crate::replication::expire_rewrite`] is a pure function of
//! the *frame* — enough for `EXPIRE key 100`, whose effect is computable from
//! the arguments plus the clock. The families here are not: which member
//! `SPOP` popped, which ID `XADD` assigned, which fields `HEXPIRE` actually
//! touched — every one of those facts lives only in the REPLY. So this is a
//! pure function of `(frame, reply, now_ms)`, evaluated on the shard that
//! executed the command, where all three are in scope.
//!
//! | command | reply | propagated as |
//! |---|---|---|
//! | `SPOP key [count]` | member / members | `SREM key <members>` (`SREM` drops an emptied set) |
//! | `XADD key … * f v` / `ms-*` | assigned ID | `XADD key … <id> f v` |
//! | `EXPIRE`/`PEXPIRE key ttl [NX\|XX\|GT\|LT]` | `:1` | `PEXPIREAT key <abs>` (`DEL key` for a past TTL) |
//! | `HEXPIRE`/`HPEXPIRE key ttl [cond] FIELDS n …` | per-field codes | `HPEXPIREAT key <abs> FIELDS m <fields set or deleted>` |
//! | `HGETEX key EX\|PX ttl FIELDS n …` | per-field values | `HPEXPIREAT key <abs> FIELDS m <fields that exist>` |
//! | `RESTORE key ttl payload …` | `+OK` | `RESTORE key <abs> payload … ABSTTL` |
//!
//! A reply that proves nothing was written (`SPOP` on a missing key, a
//! refused `EXPIRE … NX`, an `HEXPIRE` whose every field was skipped) yields
//! [`Propagation::Skip`]: a no-op must reach neither plane. Everything else
//! is [`Propagation::Verbatim`], and the caller then applies the frame-only
//! expire rewrite as before.
//!
//! `now_ms` MUST be the executing shard's cached clock (`current_time_ms()`).
//! `CachedClock::update` writes that thread-local and `Database::now_ms` from
//! the same read, so the absolute deadline computed here is bit-identical to
//! the one the handler stored.

use crate::protocol::{Frame, FrameVec};
use bytes::Bytes;

/// What to propagate for a write that answered `reply`.
#[derive(Debug)]
pub enum Propagation {
    /// The command reproduces itself: propagate the frame as-is (the caller
    /// still runs the frame-only expire rewrite on it).
    Verbatim,
    /// Propagate this deterministic effect instead of the command.
    Rewritten(Frame),
    /// The command wrote nothing: propagate nothing.
    Skip,
}

#[inline]
fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

#[inline]
fn bulk_u64(n: u64) -> Frame {
    let mut b = itoa::Buffer::new();
    bulk(b.format(n).as_bytes())
}

#[inline]
fn arg_bytes(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.trim().parse::<i64>().ok()
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    std::str::from_utf8(bytes)
        .ok()?
        .trim()
        .parse::<usize>()
        .ok()
}

#[inline]
fn eq_ignore_ascii(hay: &[u8], upper_needle: &[u8]) -> bool {
    hay.len() == upper_needle.len() && hay.eq_ignore_ascii_case(upper_needle)
}

/// `now_ms + ttl` (seconds or milliseconds) with the same saturating i128
/// arithmetic the hash-field TTL handlers use, so a clamped deadline matches
/// what they stored.
#[inline]
fn abs_saturating(now_ms: u64, ttl: i64, millis: bool) -> u64 {
    let rel: i128 = if millis {
        ttl as i128
    } else {
        (ttl as i128).saturating_mul(1000)
    };
    (now_ms as i128)
        .saturating_add(rel)
        .clamp(0, u64::MAX as i128) as u64
}

/// Rewrite a non-deterministic write to its deterministic effect.
///
/// `frame` is the command as executed, `reply` the frame it answered (RESP2
/// shape — the rewrite runs before any RESP3 conversion, though the families
/// here reply identically under both), `now_ms` the executing shard's clock.
pub fn rewrite_effect_for_propagation(frame: &Frame, reply: &Frame, now_ms: u64) -> Propagation {
    // An error reply means nothing was written. Callers gate on this already;
    // the check here keeps the contract local.
    if matches!(reply, Frame::Error(_)) {
        return Propagation::Skip;
    }
    let args = match frame {
        Frame::Array(a) => a,
        _ => return Propagation::Verbatim,
    };
    let Some(cmd) = args.first().and_then(arg_bytes) else {
        return Propagation::Verbatim;
    };

    if eq_ignore_ascii(cmd, b"SPOP") {
        rewrite_spop(args, reply)
    } else if eq_ignore_ascii(cmd, b"XADD") {
        rewrite_xadd(args, reply)
    } else if eq_ignore_ascii(cmd, b"EXPIRE") {
        rewrite_expire(args, reply, now_ms, false)
    } else if eq_ignore_ascii(cmd, b"PEXPIRE") {
        rewrite_expire(args, reply, now_ms, true)
    } else if eq_ignore_ascii(cmd, b"HEXPIRE") {
        rewrite_hexpire(args, reply, now_ms, false)
    } else if eq_ignore_ascii(cmd, b"HPEXPIRE") {
        rewrite_hexpire(args, reply, now_ms, true)
    } else if eq_ignore_ascii(cmd, b"HGETEX") {
        rewrite_hgetex(args, reply, now_ms)
    } else if eq_ignore_ascii(cmd, b"RESTORE") {
        rewrite_restore(args, now_ms)
    } else {
        Propagation::Verbatim
    }
}

/// `SPOP key [count]` -> `SREM key <popped…>`.
///
/// The reply is the popped member (`BulkString`) or members (`Array`, or a
/// RESP3 `Set` if a caller converts first). `SREM` removes the key when the
/// set empties, exactly as `SPOP` did, so one form covers the drain case too.
fn rewrite_spop(args: &FrameVec, reply: &Frame) -> Propagation {
    if args.len() < 2 {
        return Propagation::Verbatim;
    }
    let mut out: Vec<Frame> = Vec::with_capacity(3);
    out.push(bulk(b"SREM"));
    out.push(args[1].clone());
    match reply {
        Frame::BulkString(_) => out.push(reply.clone()),
        Frame::Array(members) | Frame::Set(members) => {
            out.extend(
                members
                    .iter()
                    .filter(|m| matches!(m, Frame::BulkString(_)))
                    .cloned(),
            );
        }
        _ => return Propagation::Skip,
    }
    if out.len() == 2 {
        // Null / empty array: nothing popped, nothing to propagate.
        return Propagation::Skip;
    }
    Propagation::Rewritten(Frame::Array(FrameVec::from_vec(out)))
}

/// `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] <*|ms-*> f v …`
/// -> the same command with the ID the master actually assigned.
///
/// Walks the options exactly as the handler does, so the ID is found at the
/// same position the handler read it from. Explicit IDs propagate verbatim;
/// a `Null` reply (`NOMKSTREAM` on a missing key) wrote nothing.
fn rewrite_xadd(args: &FrameVec, reply: &Frame) -> Propagation {
    let Frame::BulkString(id) = reply else {
        return Propagation::Skip;
    };
    let mut idx = 2;
    while idx < args.len() {
        let Some(arg) = arg_bytes(&args[idx]) else {
            return Propagation::Verbatim;
        };
        if eq_ignore_ascii(arg, b"NOMKSTREAM") {
            idx += 1;
        } else if eq_ignore_ascii(arg, b"MAXLEN") || eq_ignore_ascii(arg, b"MINID") {
            idx += 1;
            if idx < args.len()
                && matches!(arg_bytes(&args[idx]), Some(m) if m == b"~" || m == b"=")
            {
                idx += 1;
            }
            idx += 1; // threshold
        } else {
            break;
        }
    }
    if idx >= args.len() {
        return Propagation::Verbatim;
    }
    let Some(id_arg) = arg_bytes(&args[idx]) else {
        return Propagation::Verbatim;
    };
    if !id_arg.contains(&b'*') {
        return Propagation::Verbatim;
    }
    let mut out = args.to_vec();
    out[idx] = Frame::BulkString(id.clone());
    Propagation::Rewritten(Frame::Array(FrameVec::from_vec(out)))
}

/// `EXPIRE`/`PEXPIRE key ttl [NX|XX|GT|LT]`, reply-gated.
///
/// `:0` means the key was missing or a condition refused the set — nothing
/// changed. `:1` with a positive TTL becomes `PEXPIREAT key <abs>`; `:1` with
/// a non-positive TTL deleted the key and becomes `DEL key`. Both forms are
/// unconditional, which is right: the master already evaluated the condition.
fn rewrite_expire(args: &FrameVec, reply: &Frame, now_ms: u64, millis: bool) -> Propagation {
    match reply {
        Frame::Integer(0) => return Propagation::Skip,
        Frame::Integer(1) => {}
        _ => return Propagation::Verbatim,
    }
    if args.len() < 3 {
        return Propagation::Verbatim;
    }
    let Some(ttl) = arg_bytes(&args[2]).and_then(parse_i64) else {
        return Propagation::Verbatim;
    };
    if ttl <= 0 {
        return Propagation::Rewritten(Frame::Array(FrameVec::from_vec(vec![
            bulk(b"DEL"),
            args[1].clone(),
        ])));
    }
    let delta = if millis {
        Some(ttl as u64)
    } else {
        (ttl as u64).checked_mul(1000)
    };
    let Some(abs) = delta.and_then(|d| now_ms.checked_add(d)) else {
        return Propagation::Verbatim;
    };
    Propagation::Rewritten(Frame::Array(FrameVec::from_vec(vec![
        bulk(b"PEXPIREAT"),
        args[1].clone(),
        bulk_u64(abs),
    ])))
}

/// Locate `FIELDS n f…` starting the scan at `from`; returns the field
/// frames, or `None` when the layout is not the one the handler accepted.
fn fields_after(args: &FrameVec, from: usize) -> Option<&[Frame]> {
    let mut pos = from;
    while pos < args.len() {
        if arg_bytes(&args[pos]).is_some_and(|t| eq_ignore_ascii(t, b"FIELDS")) {
            let n = arg_bytes(args.get(pos + 1)?).and_then(parse_usize)?;
            let first = pos + 2;
            return args.get(first..first.checked_add(n)?);
        }
        pos += 1;
    }
    None
}

/// `HPEXPIREAT key <abs> FIELDS m <fields>` — the shared effect form for the
/// three relative hash-field-TTL commands. `None` fields means skip.
fn hpexpireat(key: &Frame, abs: u64, fields: Vec<Frame>) -> Propagation {
    if fields.is_empty() {
        return Propagation::Skip;
    }
    let mut out = Vec::with_capacity(5 + fields.len());
    out.push(bulk(b"HPEXPIREAT"));
    out.push(key.clone());
    out.push(bulk_u64(abs));
    out.push(bulk(b"FIELDS"));
    out.push(bulk_u64(fields.len() as u64));
    out.extend(fields);
    Propagation::Rewritten(Frame::Array(FrameVec::from_vec(out)))
}

/// `HEXPIRE`/`HPEXPIRE key ttl [NX|XX|GT|LT] FIELDS n f…`, reply-gated per
/// field: code `1` (deadline set) and `2` (past deadline, field deleted) are
/// effects; `0` (missing field or condition refused) is not. A replayed
/// `HPEXPIREAT` with the same absolute deadline reproduces both codes.
fn rewrite_hexpire(args: &FrameVec, reply: &Frame, now_ms: u64, millis: bool) -> Propagation {
    let Frame::Array(codes) = reply else {
        return Propagation::Verbatim;
    };
    if args.len() < 5 {
        return Propagation::Verbatim;
    }
    let Some(ttl) = arg_bytes(&args[2]).and_then(parse_i64) else {
        return Propagation::Verbatim;
    };
    let Some(fields) = fields_after(args, 3) else {
        return Propagation::Verbatim;
    };
    if fields.len() != codes.len() {
        return Propagation::Verbatim;
    }
    let keep: Vec<Frame> = fields
        .iter()
        .zip(codes.iter())
        .filter(|(_, code)| matches!(code, Frame::Integer(1) | Frame::Integer(2)))
        .map(|(f, _)| f.clone())
        .collect();
    hpexpireat(&args[1], abs_saturating(now_ms, ttl, millis), keep)
}

/// `HGETEX key EX|PX ttl FIELDS n f…` -> `HPEXPIREAT` on the fields that
/// exist (a `Null` reply slot is a missing field, untouched). `EXAT`, `PXAT`,
/// `PERSIST` and the bare read are already deterministic: verbatim.
fn rewrite_hgetex(args: &FrameVec, reply: &Frame, now_ms: u64) -> Propagation {
    let Frame::Array(values) = reply else {
        return Propagation::Verbatim;
    };
    if args.len() < 6 {
        return Propagation::Verbatim;
    }
    let Some(mode) = arg_bytes(&args[2]) else {
        return Propagation::Verbatim;
    };
    let millis = if eq_ignore_ascii(mode, b"EX") {
        false
    } else if eq_ignore_ascii(mode, b"PX") {
        true
    } else {
        return Propagation::Verbatim;
    };
    let Some(ttl) = arg_bytes(&args[3]).and_then(parse_i64) else {
        return Propagation::Verbatim;
    };
    let Some(fields) = fields_after(args, 4) else {
        return Propagation::Verbatim;
    };
    if fields.len() != values.len() {
        return Propagation::Verbatim;
    }
    let keep: Vec<Frame> = fields
        .iter()
        .zip(values.iter())
        .filter(|(_, v)| matches!(v, Frame::BulkString(_)))
        .map(|(f, _)| f.clone())
        .collect();
    hpexpireat(&args[1], abs_saturating(now_ms, ttl, millis), keep)
}

/// `RESTORE key ttl payload [REPLACE] [IDLETIME s] [FREQ f]` with a relative
/// `ttl` -> the same command with `<now + ttl>` and `ABSTTL` appended.
/// `ttl 0` (no expiry) and an existing `ABSTTL` are already absolute.
fn rewrite_restore(args: &FrameVec, now_ms: u64) -> Propagation {
    if args.len() < 4 {
        return Propagation::Verbatim;
    }
    let Some(ttl) = arg_bytes(&args[2]).and_then(parse_i64) else {
        return Propagation::Verbatim;
    };
    if ttl <= 0 {
        return Propagation::Verbatim;
    }
    if args[4..]
        .iter()
        .any(|a| arg_bytes(a).is_some_and(|o| eq_ignore_ascii(o, b"ABSTTL")))
    {
        return Propagation::Verbatim;
    }
    // The handler saturates; a deadline past u64::MAX is not reachable from
    // an i64 ttl added to a real clock, so `checked_add` failing is a
    // verbatim fallback rather than a wrong deadline.
    let Some(abs) = now_ms.checked_add(ttl as u64) else {
        return Propagation::Verbatim;
    };
    let mut out = args.to_vec();
    out[2] = bulk_u64(abs);
    out.push(bulk(b"ABSTTL"));
    Propagation::Rewritten(Frame::Array(FrameVec::from_vec(out)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(parts: &[&[u8]]) -> Frame {
        Frame::Array(FrameVec::from_vec(parts.iter().map(|p| bulk(p)).collect()))
    }

    /// Same as [`cmd`] from `&str` literals — every element has one type, so
    /// a slice of forms of different lengths can be iterated in one loop.
    fn cmd_s(parts: &[&str]) -> Frame {
        Frame::Array(FrameVec::from_vec(
            parts.iter().map(|p| bulk(p.as_bytes())).collect(),
        ))
    }

    fn parts_of(f: &Frame) -> Vec<Vec<u8>> {
        match f {
            Frame::Array(a) => a
                .iter()
                .map(|x| arg_bytes(x).expect("bulk").to_vec())
                .collect(),
            _ => panic!("not an array"),
        }
    }

    fn rewritten(p: Propagation) -> Vec<Vec<u8>> {
        match p {
            Propagation::Rewritten(f) => parts_of(&f),
            other => panic!("expected Rewritten, got {other:?}"),
        }
    }

    fn strs(v: &[Vec<u8>]) -> Vec<&str> {
        v.iter().map(|b| std::str::from_utf8(b).unwrap()).collect()
    }

    const NOW: u64 = 1_700_000_000_000;

    #[test]
    fn spop_single_becomes_srem_of_the_popped_member() {
        let p = rewrite_effect_for_propagation(&cmd(&[b"SPOP", b"s"]), &bulk(b"f"), NOW);
        assert_eq!(strs(&rewritten(p)), ["SREM", "s", "f"]);
    }

    #[test]
    fn spop_count_becomes_srem_of_every_popped_member() {
        let reply = Frame::Array(FrameVec::from_vec(vec![bulk(b"a"), bulk(b"c")]));
        let p = rewrite_effect_for_propagation(&cmd(&[b"spop", b"s", b"2"]), &reply, NOW);
        assert_eq!(strs(&rewritten(p)), ["SREM", "s", "a", "c"]);
    }

    #[test]
    fn spop_on_missing_or_empty_is_skipped() {
        let p = rewrite_effect_for_propagation(&cmd(&[b"SPOP", b"s"]), &Frame::Null, NOW);
        assert!(matches!(p, Propagation::Skip));
        let empty = Frame::Array(FrameVec::from_vec(vec![]));
        let p = rewrite_effect_for_propagation(&cmd(&[b"SPOP", b"s", b"3"]), &empty, NOW);
        assert!(matches!(p, Propagation::Skip));
    }

    #[test]
    fn xadd_star_takes_the_assigned_id() {
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"XADD", b"st", b"*", b"f", b"v"]),
            &bulk(b"1700000000000-0"),
            NOW,
        );
        assert_eq!(
            strs(&rewritten(p)),
            ["XADD", "st", "1700000000000-0", "f", "v"]
        );
    }

    #[test]
    fn xadd_partial_id_and_options_keep_their_position() {
        let p = rewrite_effect_for_propagation(
            &cmd(&[
                b"XADD",
                b"st",
                b"NOMKSTREAM",
                b"MAXLEN",
                b"~",
                b"100",
                b"5-*",
                b"f",
                b"v",
            ]),
            &bulk(b"5-3"),
            NOW,
        );
        assert_eq!(
            strs(&rewritten(p)),
            [
                "XADD",
                "st",
                "NOMKSTREAM",
                "MAXLEN",
                "~",
                "100",
                "5-3",
                "f",
                "v"
            ]
        );
    }

    #[test]
    fn xadd_explicit_id_is_verbatim_and_nomkstream_miss_is_skipped() {
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"XADD", b"st", b"7-1", b"f", b"v"]),
            &bulk(b"7-1"),
            NOW,
        );
        assert!(matches!(p, Propagation::Verbatim));
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"XADD", b"st", b"NOMKSTREAM", b"*", b"f", b"v"]),
            &Frame::Null,
            NOW,
        );
        assert!(matches!(p, Propagation::Skip));
    }

    #[test]
    fn expire_with_condition_becomes_pexpireat_only_when_it_took() {
        let f = cmd(&[b"EXPIRE", b"k", b"100", b"NX"]);
        let p = rewrite_effect_for_propagation(&f, &Frame::Integer(1), NOW);
        assert_eq!(strs(&rewritten(p)), ["PEXPIREAT", "k", "1700000100000"]);
        let p = rewrite_effect_for_propagation(&f, &Frame::Integer(0), NOW);
        assert!(matches!(p, Propagation::Skip));
    }

    #[test]
    fn pexpire_gt_and_past_ttl_delete() {
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"PEXPIRE", b"k", b"500", b"GT"]),
            &Frame::Integer(1),
            NOW,
        );
        assert_eq!(strs(&rewritten(p)), ["PEXPIREAT", "k", "1700000000500"]);
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"EXPIRE", b"k", b"-1", b"LT"]),
            &Frame::Integer(1),
            NOW,
        );
        assert_eq!(strs(&rewritten(p)), ["DEL", "k"]);
    }

    #[test]
    fn hexpire_keeps_only_the_fields_it_touched() {
        let reply = Frame::Array(FrameVec::from_vec(vec![
            Frame::Integer(1),
            Frame::Integer(0),
            Frame::Integer(2),
        ]));
        let p = rewrite_effect_for_propagation(
            &cmd(&[
                b"HEXPIRE", b"h", b"10", b"NX", b"FIELDS", b"3", b"a", b"b", b"c",
            ]),
            &reply,
            NOW,
        );
        assert_eq!(
            strs(&rewritten(p)),
            ["HPEXPIREAT", "h", "1700000010000", "FIELDS", "2", "a", "c"]
        );
    }

    #[test]
    fn hpexpire_with_nothing_touched_is_skipped() {
        let reply = Frame::Array(FrameVec::from_vec(vec![Frame::Integer(0)]));
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"HPEXPIRE", b"h", b"10", b"FIELDS", b"1", b"a"]),
            &reply,
            NOW,
        );
        assert!(matches!(p, Propagation::Skip));
    }

    #[test]
    fn hgetex_ex_becomes_hpexpireat_on_existing_fields() {
        let reply = Frame::Array(FrameVec::from_vec(vec![bulk(b"v"), Frame::Null]));
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"HGETEX", b"h", b"EX", b"5", b"FIELDS", b"2", b"a", b"zz"]),
            &reply,
            NOW,
        );
        assert_eq!(
            strs(&rewritten(p)),
            ["HPEXPIREAT", "h", "1700000005000", "FIELDS", "1", "a"]
        );
    }

    #[test]
    fn hgetex_absolute_persist_and_plain_are_verbatim() {
        let reply = Frame::Array(FrameVec::from_vec(vec![bulk(b"v")]));
        let forms: [&[&str]; 3] = [
            &["HGETEX", "h", "PXAT", "5", "FIELDS", "1", "a"],
            &["HGETEX", "h", "PERSIST", "FIELDS", "1", "a"],
            &["HGETEX", "h", "FIELDS", "1", "a"],
        ];
        for form in forms {
            let p = rewrite_effect_for_propagation(&cmd_s(form), &reply, NOW);
            assert!(matches!(p, Propagation::Verbatim), "{form:?}");
        }
    }

    #[test]
    fn restore_relative_ttl_becomes_absttl() {
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"RESTORE", b"k", b"1000", b"payload", b"REPLACE"]),
            &Frame::SimpleString(Bytes::from_static(b"OK")),
            NOW,
        );
        assert_eq!(
            strs(&rewritten(p)),
            [
                "RESTORE",
                "k",
                "1700000001000",
                "payload",
                "REPLACE",
                "ABSTTL"
            ]
        );
    }

    #[test]
    fn restore_without_ttl_or_already_absolute_is_verbatim() {
        let ok = Frame::SimpleString(Bytes::from_static(b"OK"));
        let p =
            rewrite_effect_for_propagation(&cmd(&[b"RESTORE", b"k", b"0", b"payload"]), &ok, NOW);
        assert!(matches!(p, Propagation::Verbatim));
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"RESTORE", b"k", b"1700000001000", b"payload", b"ABSTTL"]),
            &ok,
            NOW,
        );
        assert!(matches!(p, Propagation::Verbatim));
    }

    #[test]
    fn error_reply_is_skipped_and_unrelated_commands_are_verbatim() {
        let p = rewrite_effect_for_propagation(
            &cmd(&[b"SPOP", b"s"]),
            &Frame::Error(Bytes::from_static(b"WRONGTYPE")),
            NOW,
        );
        assert!(matches!(p, Propagation::Skip));
        let forms: [&[&str]; 4] = [
            &["SET", "k", "v"],
            &["SREM", "s", "a"],
            &["INCRBYFLOAT", "k", "1.5"],
            &["HEXPIREAT", "h", "1", "FIELDS", "1", "a"],
        ];
        for form in forms {
            let p = rewrite_effect_for_propagation(&cmd_s(form), &Frame::Integer(1), NOW);
            assert!(matches!(p, Propagation::Verbatim), "{form:?}");
        }
    }
}
