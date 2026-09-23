//! The propagated EFFECT of a consumer-group read or claim (moon#1130).
//!
//! `XREADGROUP ... >`, `XCLAIM` and `XAUTOCLAIM` decide what they do from the
//! clock: a read stamps every entry it delivers with the delivery time, and a
//! claim takes only the entries idle for `min-idle-time`. Logged verbatim, a
//! replay — a restart, or a replica applying the stream — re-decides both at
//! REPLAY time: the pending entries' idle times restart from zero, and a
//! claim that followed finds its entry only a few ms idle and takes nothing,
//! so the entry goes back to its old owner.
//!
//! Redis never propagates these commands; it propagates what they did
//! (`streamPropagateXCLAIM`, `streamPropagateGroupID`,
//! `streamPropagateConsumerCreation`): `XCLAIM key group consumer 0 <id>
//! TIME <ms> RETRYCOUNT <n> FORCE JUSTID LASTID <id>` per entry, and the
//! group's cursor. `min-idle-time 0` means nothing is re-decided, `TIME`
//! restores the delivery time, `FORCE` recreates a pending entry the replaying
//! side does not have. Moon's `XCLAIM` honours every one of those options
//! (moon#1104), and the blocking `XREADGROUP` already logs that shape
//! ([`crate::blocking::stream_log`]).
//!
//! Here the effect is derived from `(frame, reply, now_ms)` only — the
//! propagation sites hold the reply, not the database — which is exact for
//! these commands because what they did is in the reply:
//!
//! | command | propagated as |
//! |---|---|
//! | `XREADGROUP ... >` (per stream served) | `XCLAIM key g c 0 <ids> TIME now RETRYCOUNT 1 FORCE JUSTID LASTID <last>` |
//! | `XREADGROUP ... NOACK ... >` (per stream served) | `XGROUP CREATECONSUMER key g c`, `XGROUP SETID key g <last>` |
//! | `XREADGROUP` history read (explicit id) / nothing served | nothing |
//! | `XCLAIM key g c <min-idle> ids ... [opts]` | `XCLAIM key g c 0 <claimed ids> TIME <t> [RETRYCOUNT n] FORCE [JUSTID] [LASTID x]` |
//! | `XAUTOCLAIM key g c <min-idle> <start> ...` | `XCLAIM key g c 0 <claimed ids> <deleted ids> TIME now [JUSTID]` |
//!
//! Notes on each:
//!
//! * A `>` read delivers only entries past the group's cursor, stamps them
//!   `now` with count 1, and moves the cursor to the last one — so one
//!   `XCLAIM` per stream carrying every id (redis writes one per id) restores
//!   the PEL, and its `LASTID` restores the cursor, raise-only. Redis also
//!   writes an `XGROUP SETID` there; `LASTID` makes it redundant.
//! * A `NOACK` read has no PEL; it moves the cursor and may create the
//!   consumer. Whether the consumer existed before is not in the reply, so
//!   `CREATECONSUMER` is written for every NOACK read that delivered — a
//!   no-op on replay when it already exists. (redis writes it only when the
//!   read created it; the replayed state is the same.)
//! * `XCLAIM`'s claimed ids are its reply. A claim's delivery count is
//!   either `RETRYCOUNT` or the old count plus one (unless `JUSTID`); the
//!   old count is not in the reply, so the record keeps the command's own
//!   `RETRYCOUNT` / `JUSTID` and the replay, starting from the same state,
//!   arrives at the same count. An `XCLAIM` that claimed nothing but raised
//!   the cursor with `LASTID` is written as `XCLAIM key g c 0 0-0 LASTID x`:
//!   `0-0` is never an entry, so it claims nothing and only raises the
//!   cursor, exactly as the command did.
//! * `XAUTOCLAIM`'s reply names both the entries it claimed and the ones it
//!   found deleted and dropped from the PEL. An `XCLAIM` of a deleted entry
//!   drops its pending entry too, so both go in one record.
//!
//! Checked against redis-server 8.6.1's AOF for the same sequences: redis
//! writes one `XCLAIM` per id inside `MULTI`/`EXEC` (moon's log never carries
//! `MULTI`; the records of one command are appended back to back), its read
//! records carry `LASTID <cursor before the read>` plus `XGROUP SETID ...
//! ENTRIESREAD n` (moon keeps no entries-read counter), and a claim that takes
//! nothing — which still creates its consumer — writes no record at all, as
//! here.
//!
//! Residuals, by construction — none involves entry data:
//!
//! * An `XCLAIM` also drops the pending entry of any requested id whose
//!   stream entry was deleted, and those ids are not in its reply (nor
//!   distinguishable there from ids it skipped as not idle enough). Such a
//!   stale pending entry survives a replay until the next claim that touches
//!   it drops it.
//! * A `NOACK` read that delivered nothing but created its consumer: redis
//!   writes `XGROUP CREATECONSUMER`; here the reply cannot tell a new consumer
//!   from an old one, and logging one per empty poll is not worth it. The
//!   consumer reappears on its first read that delivers.

use bytes::Bytes;

use crate::protocol::{Frame, FrameVec};
use crate::replication::effect_rewrite::Propagation;
use crate::storage::stream::StreamId;

/// Effect records of one command, in log order.
pub type StreamEffects = smallvec::SmallVec<[Frame; 2]>;

#[inline]
fn arg(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

#[inline]
fn is(a: &[u8], upper: &[u8]) -> bool {
    a.eq_ignore_ascii_case(upper)
}

#[inline]
fn stat(s: &'static [u8]) -> Frame {
    Frame::BulkString(Bytes::from_static(s))
}

fn num(n: u64) -> Frame {
    let mut b = itoa::Buffer::new();
    Frame::BulkString(Bytes::copy_from_slice(b.format(n).as_bytes()))
}

fn id_frame(id: StreamId) -> Frame {
    // `ms-seq` is at most 41 bytes.
    let mut out = Vec::with_capacity(41);
    let mut b = itoa::Buffer::new();
    out.extend_from_slice(b.format(id.ms).as_bytes());
    out.push(b'-');
    out.extend_from_slice(b.format(id.seq).as_bytes());
    Frame::BulkString(Bytes::from(out))
}

/// A stream id as the handlers' strict parser takes it: `ms` or `ms-seq`,
/// never the range shorthands `-` / `+` nor the auto forms `*` / `ms-*`.
fn strict_id(b: &[u8]) -> Option<StreamId> {
    if matches!(b, b"-" | b"+" | b"*") || b.ends_with(b"-*") {
        return None;
    }
    StreamId::parse(b, 0).ok()
}

/// Redis's `string2ll` accept-set, as the `XCLAIM` handler parses numbers.
fn parse_ll(b: &[u8]) -> Option<i64> {
    let digits = b.strip_prefix(b"-").unwrap_or(b);
    if digits.is_empty()
        || !digits.iter().all(u8::is_ascii_digit)
        || (digits.len() > 1 && digits[0] == b'0')
    {
        return None;
    }
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// The id of one element of a claim/read reply: an entry `[id, fields]`
/// (fields may be null for a deleted entry in a history read) or a bare id
/// (`JUSTID`).
fn element_id(e: &Frame) -> Option<StreamId> {
    match e {
        Frame::Array(pair) => pair.first().and_then(arg).and_then(|b| strict_id(b)),
        Frame::BulkString(b) => strict_id(b),
        _ => None,
    }
}

/// `XCLAIM key group consumer 0 <ids> <tail>`.
fn xclaim_record(
    key: &Frame,
    group: &Frame,
    consumer: &Frame,
    ids: &[StreamId],
    tail: &[Frame],
) -> Frame {
    let mut v: Vec<Frame> = Vec::with_capacity(5 + ids.len() + tail.len());
    v.push(stat(b"XCLAIM"));
    v.push(key.clone());
    v.push(group.clone());
    v.push(consumer.clone());
    v.push(stat(b"0"));
    v.extend(ids.iter().map(|&id| id_frame(id)));
    v.extend_from_slice(tail);
    Frame::Array(FrameVec::from_vec(v))
}

/// `XREADGROUP GROUP g c [COUNT n] [BLOCK ms] [NOACK] STREAMS k.. id..` ->
/// per `>` stream the reply served, the records of what it delivered.
pub(crate) fn rewrite_xreadgroup(args: &FrameVec, reply: &Frame, now_ms: u64) -> Propagation {
    // A RESP2 reply is `[[key, entries], ...]`, a RESP3 one the map
    // `{key: entries}`. Either way the streams come in argument order (a `>`
    // stream with nothing new is dropped), so the argument index below only
    // ever moves forward — which also pairs a key named twice with its id.
    let pairs: smallvec::SmallVec<[(&Frame, &Frame); 4]> = match reply {
        Frame::Array(streams) => streams
            .iter()
            .filter_map(|s| match s {
                Frame::Array(p) if p.len() == 2 => Some((&p[0], &p[1])),
                _ => None,
            })
            .collect(),
        Frame::Map(streams) => streams.iter().map(|(k, v)| (k, v)).collect(),
        // Nothing served: nothing moved, nothing to log.
        Frame::Null | Frame::NullArray => return Propagation::Skip,
        _ => return Propagation::Verbatim,
    };
    if args.len() < 4 || !arg(&args[1]).is_some_and(|a| is(a, b"GROUP")) {
        return Propagation::Verbatim;
    }
    let (group, consumer) = (&args[2], &args[3]);
    let mut noack = false;
    let mut i = 4;
    loop {
        let Some(opt) = args.get(i).and_then(arg) else {
            return Propagation::Verbatim;
        };
        if is(opt, b"COUNT") || is(opt, b"BLOCK") {
            i += 2;
        } else if is(opt, b"NOACK") {
            noack = true;
            i += 1;
        } else if is(opt, b"STREAMS") {
            i += 1;
            break;
        } else {
            return Propagation::Verbatim;
        }
    }
    let rest = args.len().saturating_sub(i);
    if rest == 0 || rest % 2 != 0 {
        return Propagation::Verbatim;
    }
    let n = rest / 2;
    let (keys, ids) = (&args[i..i + n], &args[i + n..]);

    let mut out = StreamEffects::new();
    let mut at = 0;
    for (name, entries) in pairs {
        let Some(name) = arg(name) else {
            return Propagation::Verbatim;
        };
        while at < n && arg(&keys[at]) != Some(name) {
            at += 1;
        }
        if at == n {
            return Propagation::Verbatim;
        }
        let (key, id) = (&keys[at], &ids[at]);
        at += 1;
        // A history read (explicit id) moves nothing: redis logs nothing.
        if arg(id).is_none_or(|a| a.as_ref() != b">") {
            continue;
        }
        let Frame::Array(entries) = entries else {
            continue;
        };
        let delivered: smallvec::SmallVec<[StreamId; 8]> =
            entries.iter().filter_map(element_id).collect();
        let Some(&last) = delivered.last() else {
            continue;
        };
        if noack {
            out.push(Frame::Array(FrameVec::from_vec(vec![
                stat(b"XGROUP"),
                stat(b"CREATECONSUMER"),
                key.clone(),
                group.clone(),
                consumer.clone(),
            ])));
            out.push(Frame::Array(FrameVec::from_vec(vec![
                stat(b"XGROUP"),
                stat(b"SETID"),
                key.clone(),
                group.clone(),
                id_frame(last),
            ])));
        } else {
            // `read_group_new` stamps every entry `current_time_ms()` with
            // count 1 — the cached clock `now_ms` is read from.
            out.push(xclaim_record(
                key,
                group,
                consumer,
                &delivered,
                &[
                    stat(b"TIME"),
                    num(now_ms),
                    stat(b"RETRYCOUNT"),
                    stat(b"1"),
                    stat(b"FORCE"),
                    stat(b"JUSTID"),
                    stat(b"LASTID"),
                    id_frame(last),
                ],
            ));
        }
    }
    if out.is_empty() {
        Propagation::Skip
    } else {
        Propagation::Records(out)
    }
}

/// `XCLAIM key group consumer min-idle id... [IDLE ms] [TIME ms]
/// [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]` -> the claim it made, with
/// `min-idle 0` and the delivery time it stamped.
pub(crate) fn rewrite_xclaim(args: &FrameVec, reply: &Frame, now_ms: u64) -> Propagation {
    let Frame::Array(claimed) = reply else {
        return Propagation::Verbatim;
    };
    if args.len() < 6 {
        return Propagation::Verbatim;
    }
    // Options start at the first argument that is not a strict id, exactly
    // as the handler parses them.
    let mut j = 5;
    while j < args.len()
        && args
            .get(j)
            .and_then(arg)
            .and_then(|b| strict_id(b))
            .is_some()
    {
        j += 1;
    }
    let now = now_ms as i64;
    let mut delivery_time: Option<i64> = None;
    let mut retry_count: Option<u64> = None;
    let mut justid = false;
    let mut last_id: Option<&Frame> = None;
    while j < args.len() {
        let Some(opt) = arg(&args[j]) else {
            return Propagation::Verbatim;
        };
        let value = args.get(j + 1).and_then(arg);
        if is(opt, b"FORCE") {
        } else if is(opt, b"JUSTID") {
            justid = true;
        } else if is(opt, b"IDLE") && value.is_some() {
            let Some(idle) = value.and_then(|b| parse_ll(b)) else {
                return Propagation::Verbatim;
            };
            delivery_time = Some(now.saturating_sub(idle));
            j += 1;
        } else if is(opt, b"TIME") && value.is_some() {
            let Some(t) = value.and_then(|b| parse_ll(b)) else {
                return Propagation::Verbatim;
            };
            delivery_time = Some(t);
            j += 1;
        } else if is(opt, b"RETRYCOUNT") && value.is_some() {
            let Some(n) = value.and_then(|b| parse_ll(b)) else {
                return Propagation::Verbatim;
            };
            // A negative count is redis's "not given": it increments.
            retry_count = u64::try_from(n).ok();
            j += 1;
        } else if is(opt, b"LASTID") && value.is_some() {
            last_id = args.get(j + 1);
            j += 1;
        } else {
            return Propagation::Verbatim;
        }
        j += 1;
    }
    // `Stream::xclaim`'s clamp: a negative or future time stamps now.
    let time = match delivery_time {
        Some(t) if t >= 0 && t <= now => t as u64,
        _ => now_ms,
    };
    let ids: smallvec::SmallVec<[StreamId; 8]> = claimed.iter().filter_map(element_id).collect();
    if ids.is_empty() {
        return match last_id {
            // Raise-only cursor move, and nothing claimed: `0-0` is never
            // an entry (see the module docs).
            Some(last) => Propagation::Rewritten(xclaim_record(
                &args[1],
                &args[2],
                &args[3],
                &[StreamId::ZERO],
                &[stat(b"LASTID"), last.clone()],
            )),
            None => Propagation::Skip,
        };
    }
    let mut tail: smallvec::SmallVec<[Frame; 8]> = smallvec::SmallVec::new();
    tail.push(stat(b"TIME"));
    tail.push(num(time));
    if let Some(n) = retry_count {
        tail.push(stat(b"RETRYCOUNT"));
        tail.push(num(n));
    }
    // Every claimed id was pending, or was created by the command's own
    // FORCE; the replaying side, in the same state, needs FORCE for the
    // latter and ignores it for the former.
    tail.push(stat(b"FORCE"));
    if justid {
        tail.push(stat(b"JUSTID"));
    }
    if let Some(last) = last_id {
        tail.push(stat(b"LASTID"));
        tail.push(last.clone());
    }
    Propagation::Rewritten(xclaim_record(&args[1], &args[2], &args[3], &ids, &tail))
}

/// `XAUTOCLAIM key group consumer min-idle start [COUNT n] [JUSTID]` -> one
/// `XCLAIM` of the entries it claimed and the deleted ones it dropped.
pub(crate) fn rewrite_xautoclaim(args: &FrameVec, reply: &Frame, now_ms: u64) -> Propagation {
    let Frame::Array(parts) = reply else {
        return Propagation::Verbatim;
    };
    if args.len() < 6 || parts.len() < 2 {
        return Propagation::Verbatim;
    }
    let (Frame::Array(claimed), deleted) = (&parts[1], parts.get(2)) else {
        return Propagation::Verbatim;
    };
    // A bare-id reply is the JUSTID form, which leaves the count alone; an
    // entry reply incremented it. Taken from the reply, so the record says
    // what the handler actually did.
    let justid = claimed.iter().any(|e| matches!(e, Frame::BulkString(_)));
    let mut ids: smallvec::SmallVec<[StreamId; 8]> =
        claimed.iter().filter_map(element_id).collect();
    if let Some(Frame::Array(deleted)) = deleted {
        ids.extend(deleted.iter().filter_map(element_id));
    }
    if ids.is_empty() {
        return Propagation::Skip;
    }
    let mut tail: smallvec::SmallVec<[Frame; 3]> = smallvec::SmallVec::new();
    tail.push(stat(b"TIME"));
    tail.push(num(now_ms));
    if justid {
        tail.push(stat(b"JUSTID"));
    }
    Propagation::Rewritten(xclaim_record(&args[1], &args[2], &args[3], &ids, &tail))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

    fn b(s: &str) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    fn cmd(parts: &[&str]) -> FrameVec {
        FrameVec::from_vec(parts.iter().map(|p| b(p)).collect())
    }

    fn flat(f: &Frame) -> String {
        let Frame::Array(items) = f else {
            return format!("{f:?}");
        };
        items
            .iter()
            .map(|i| match i {
                Frame::BulkString(b) => String::from_utf8_lossy(b).into_owned(),
                other => format!("{other:?}"),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Run `parts` (command name first) against `db` through its handler.
    fn run(db: &mut Database, parts: &FrameVec) -> Frame {
        let name = match &parts[0] {
            Frame::BulkString(n) => n.to_ascii_uppercase(),
            _ => panic!("no command name"),
        };
        let args = &parts[1..];
        use crate::command::stream as s;
        match name.as_slice() {
            b"XGROUP" => s::xgroup(db, args),
            b"XADD" => s::xadd(db, args),
            b"XDEL" => s::xdel(db, args),
            b"XREADGROUP" => s::xreadgroup(db, args),
            b"XCLAIM" => s::xclaim(db, args),
            b"XAUTOCLAIM" => s::xautoclaim(db, args),
            other => panic!("unexpected {}", String::from_utf8_lossy(other)),
        }
    }

    /// Two databases in the same state: `setup` runs on both.
    fn twins(setup: &[&[&str]]) -> (Database, Database) {
        let (mut a, mut r) = (Database::new(), Database::new());
        for c in setup {
            run(&mut a, &cmd(c));
            run(&mut r, &cmd(c));
        }
        (a, r)
    }

    const SETUP: &[&[&str]] = &[
        &["XGROUP", "CREATE", "s", "g", "$", "MKSTREAM"],
        &["XADD", "s", "1-1", "f", "v"],
        &["XADD", "s", "1-2", "f", "v"],
        &["XGROUP", "CREATE", "t", "g", "$", "MKSTREAM"],
        &["XADD", "t", "2-1", "f", "v"],
    ];

    /// `(id, consumer, delivery_time, count)` of every pending entry, the
    /// cursor, and the consumer names — the whole group state.
    fn state(db: &mut Database, key: &str) -> String {
        let s = db.get_stream(key.as_bytes()).unwrap().unwrap();
        let g = &s.groups[b"g".as_ref()];
        let pel: Vec<String> = g
            .pel
            .iter()
            .map(|(id, pe)| {
                format!(
                    "{}-{}:{}:{}:{}",
                    id.ms,
                    id.seq,
                    String::from_utf8_lossy(&pe.consumer),
                    pe.delivery_time,
                    pe.delivery_count
                )
            })
            .collect();
        let mut consumers: Vec<String> = g
            .consumers
            .keys()
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect();
        consumers.sort();
        format!(
            "last={:?} consumers={consumers:?} pel={pel:?}",
            g.last_delivered_id
        )
    }

    /// Run `c` on `live`, derive its records at `now` and apply them to
    /// `replay`, returning the records as text.
    fn propagate(live: &mut Database, replay: &mut Database, c: &[&str], now: u64) -> Vec<String> {
        let frame = cmd(c);
        let reply = run(live, &frame);
        let recs: Vec<Frame> =
            match crate::replication::effect_rewrite::rewrite_effect_for_propagation(
                &Frame::Array(frame.clone()),
                &reply,
                now,
            ) {
                Propagation::Skip => vec![],
                Propagation::Rewritten(f) => vec![f],
                Propagation::Records(r) => r.into_iter().collect(),
                Propagation::Verbatim => panic!("{c:?} propagated verbatim"),
            };
        for r in &recs {
            let Frame::Array(parts) = r else { panic!() };
            assert!(
                !matches!(run(replay, parts), Frame::Error(_)),
                "{}",
                flat(r)
            );
        }
        recs.iter().map(flat).collect()
    }

    fn delivery_time(db: &mut Database, key: &str, ms: u64, seq: u64) -> u64 {
        let s = db.get_stream(key.as_bytes()).unwrap().unwrap();
        s.groups[b"g".as_ref()].pel[&StreamId { ms, seq }].delivery_time
    }

    #[test]
    fn a_read_is_one_forced_claim_per_stream_that_replays_exactly() {
        let (mut live, mut replay) = twins(SETUP);
        let c = [
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "COUNT",
            "5",
            "STREAMS",
            "s",
            "t",
            ">",
            ">",
        ];
        let frame = cmd(&c);
        let reply = run(&mut live, &frame);
        let now = delivery_time(&mut live, "s", 1, 1);
        // Re-run the derivation on the reply the live read gave.
        let recs = match rewrite_xreadgroup(&frame, &reply, now) {
            Propagation::Records(r) => r,
            other => panic!("{other:?}"),
        };
        let got: Vec<String> = recs.iter().map(flat).collect();
        assert_eq!(
            got,
            vec![
                format!("XCLAIM s g c 0 1-1 1-2 TIME {now} RETRYCOUNT 1 FORCE JUSTID LASTID 1-2"),
                format!("XCLAIM t g c 0 2-1 TIME {now} RETRYCOUNT 1 FORCE JUSTID LASTID 2-1"),
            ]
        );
        for r in &recs {
            let Frame::Array(parts) = r else { panic!() };
            run(&mut replay, parts);
        }
        for k in ["s", "t"] {
            assert_eq!(state(&mut live, k), state(&mut replay, k), "{k}");
        }
    }

    /// Each record reaches the log as its own serialized command: the
    /// framed AOF replays one command per record, so two streams' records
    /// fused into one payload would lose the second.
    #[test]
    fn a_multi_stream_read_serializes_one_record_per_stream() {
        let (mut live, _) = twins(SETUP);
        let frame = cmd(&[
            "XREADGROUP",
            "GROUP",
            "g",
            "c",
            "STREAMS",
            "s",
            "t",
            ">",
            ">",
        ]);
        let reply = run(&mut live, &frame);
        let log = crate::persistence::aof::serialize_effect_for_log(&Frame::Array(frame), &reply);
        assert_eq!(log.len(), 2, "{log:?}");
        for (rec, key) in log.iter().zip(["s", "t"]) {
            let text = String::from_utf8_lossy(rec);
            assert!(text.starts_with("*"), "{text}");
            assert_eq!(text.matches("XCLAIM").count(), 1, "{text}");
            assert!(
                text.contains(&format!("\r\n${}\r\n{key}\r\n", key.len())),
                "{text}"
            );
        }
    }

    #[test]
    fn a_noack_read_logs_the_consumer_and_the_cursor() {
        let (mut live, mut replay) = twins(SETUP);
        let got = propagate(
            &mut live,
            &mut replay,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "n",
                "NOACK",
                "STREAMS",
                "s",
                ">",
            ],
            7,
        );
        assert_eq!(
            got,
            vec!["XGROUP CREATECONSUMER s g n", "XGROUP SETID s g 1-2"]
        );
        assert_eq!(state(&mut live, "s"), state(&mut replay, "s"));
    }

    #[test]
    fn reads_that_move_nothing_log_nothing() {
        let (mut live, mut replay) = twins(SETUP);
        propagate(
            &mut live,
            &mut replay,
            &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
            5,
        );
        // Nothing new: the null reply.
        assert!(
            propagate(
                &mut live,
                &mut replay,
                &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
                5
            )
            .is_empty()
        );
        // History reads, served or not.
        for id in ["0", "1-1", "9-9"] {
            assert!(
                propagate(
                    &mut live,
                    &mut replay,
                    &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", id],
                    5
                )
                .is_empty(),
                "history read from {id} logged something"
            );
        }
        // A history stream next to a `>` stream: only the `>` one logs.
        let got = propagate(
            &mut live,
            &mut replay,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "STREAMS",
                "s",
                "t",
                "0",
                ">",
            ],
            5,
        );
        assert_eq!(
            got,
            vec!["XCLAIM t g c 0 2-1 TIME 5 RETRYCOUNT 1 FORCE JUSTID LASTID 2-1"]
        );
    }

    #[test]
    fn a_resp3_map_reply_is_read_the_same_way() {
        let frame = cmd(&["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"]);
        let entry = Frame::Array(FrameVec::from_vec(vec![
            b("1-1"),
            Frame::Array(FrameVec::new()),
        ]));
        let reply = Frame::Map(vec![(
            b("s"),
            Frame::Array(FrameVec::from_vec(vec![entry])),
        )]);
        let Propagation::Records(r) = rewrite_xreadgroup(&frame, &reply, 9) else {
            panic!("no records");
        };
        assert_eq!(
            flat(&r[0]),
            "XCLAIM s g c 0 1-1 TIME 9 RETRYCOUNT 1 FORCE JUSTID LASTID 1-1"
        );
    }

    #[test]
    fn a_claim_logs_what_it_took_with_min_idle_zero() {
        let (mut live, mut replay) = twins(SETUP);
        propagate(
            &mut live,
            &mut replay,
            &["XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"],
            100,
        );
        // Every live-side delivery time is the real clock; the replay's is
        // what the record said. Line them up before comparing.
        let (_, mut replay) = {
            let mut r = Database::new();
            for c in SETUP {
                run(&mut r, &cmd(c));
            }
            let t = delivery_time(&mut live, "s", 1, 1);
            run(
                &mut r,
                &cmd(&[
                    "XCLAIM",
                    "s",
                    "g",
                    "c1",
                    "0",
                    "1-1",
                    "1-2",
                    "TIME",
                    &t.to_string(),
                    "RETRYCOUNT",
                    "1",
                    "FORCE",
                    "JUSTID",
                    "LASTID",
                    "1-2",
                ]),
            );
            ((), r)
        };
        assert_eq!(state(&mut live, "s"), state(&mut replay, "s"));

        // min-idle unmet: claims nothing, logs nothing.
        assert!(
            propagate(
                &mut live,
                &mut replay,
                &["XCLAIM", "s", "g", "c2", "999999999", "1-1"],
                100
            )
            .is_empty()
        );
        // min-idle 0 claims; the record carries min-idle 0 and the time.
        let now = crate::storage::entry::current_time_ms();
        let got = propagate(
            &mut live,
            &mut replay,
            &[
                "XCLAIM",
                "s",
                "g",
                "c2",
                "0",
                "1-1",
                "JUSTID",
                "RETRYCOUNT",
                "7",
            ],
            now,
        );
        assert_eq!(
            got,
            vec![format!(
                "XCLAIM s g c2 0 1-1 TIME {now} RETRYCOUNT 7 FORCE JUSTID"
            )]
        );
        // Without JUSTID / RETRYCOUNT the count increments — on replay too.
        let got = propagate(
            &mut live,
            &mut replay,
            &["XCLAIM", "s", "g", "c3", "0", "1-2", "IDLE", "50"],
            now,
        );
        assert_eq!(
            got,
            vec![format!("XCLAIM s g c3 0 1-2 TIME {} FORCE", now - 50)]
        );
        let s = |db: &mut Database| {
            let st = db.get_stream(b"s").unwrap().unwrap();
            let g = &st.groups[b"g".as_ref()];
            g.pel
                .iter()
                .map(|(id, pe)| (*id, pe.consumer.clone(), pe.delivery_count))
                .collect::<Vec<_>>()
        };
        assert_eq!(s(&mut live), s(&mut replay));
    }

    #[test]
    fn a_claim_of_nothing_that_raises_the_cursor_logs_only_that() {
        let (mut live, mut replay) = twins(SETUP);
        let got = propagate(
            &mut live,
            &mut replay,
            &["XCLAIM", "s", "g", "c", "0", "1-1", "LASTID", "1-2"],
            3,
        );
        assert_eq!(got, vec!["XCLAIM s g c 0 0-0 LASTID 1-2"]);
        assert_eq!(state(&mut live, "s"), state(&mut replay, "s"));
    }

    #[test]
    fn an_autoclaim_logs_claimed_and_deleted_in_one_claim() {
        let (mut live, mut replay) = twins(SETUP);
        let read = [
            "XCLAIM",
            "s",
            "g",
            "c1",
            "0",
            "1-1",
            "1-2",
            "TIME",
            "10",
            "RETRYCOUNT",
            "1",
            "FORCE",
            "JUSTID",
        ];
        run(&mut live, &cmd(&read));
        run(&mut replay, &cmd(&read));
        run(&mut live, &cmd(&["XDEL", "s", "1-2"]));
        run(&mut replay, &cmd(&["XDEL", "s", "1-2"]));
        let now = crate::storage::entry::current_time_ms();
        let got = propagate(
            &mut live,
            &mut replay,
            &["XAUTOCLAIM", "s", "g", "c2", "0", "0"],
            now,
        );
        assert_eq!(got, vec![format!("XCLAIM s g c2 0 1-1 1-2 TIME {now}")]);
        let live_state = state(&mut live, "s");
        assert!(
            live_state.contains("1-1:c2:") && !live_state.contains("1-2:"),
            "{live_state}"
        );
        // Delivery count 2 on both sides; times differ only by the clock.
        let pel = |db: &mut Database| {
            let st = db.get_stream(b"s").unwrap().unwrap();
            st.groups[b"g".as_ref()]
                .pel
                .iter()
                .map(|(id, pe)| (*id, pe.consumer.clone(), pe.delivery_count))
                .collect::<Vec<_>>()
        };
        assert_eq!(pel(&mut live), pel(&mut replay));
        // Nothing idle enough: nothing logged.
        assert!(
            propagate(
                &mut live,
                &mut replay,
                &["XAUTOCLAIM", "s", "g", "c3", "999999999", "0"],
                now
            )
            .is_empty()
        );
    }
}
