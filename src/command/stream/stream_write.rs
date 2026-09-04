//! Stream write command handlers: XADD, XDEL, XTRIM, XACK, XCLAIM, XAUTOCLAIM, XGROUP, XREADGROUP, XSETID.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::storage::stream::StreamId;

use super::format_entry;
use crate::command::helpers::{err_wrong_args, extract_bytes};

/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] id field value [field value ...]
pub fn xadd(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 4 {
        return err_wrong_args("XADD");
    }

    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XADD"),
    };

    let mut idx = 1;
    let mut nomkstream = false;
    let mut trim_strategy: Option<(&[u8], bool, &Bytes)> = None; // (MAXLEN|MINID, approximate, threshold)

    // Parse options before the ID
    while idx < args.len() {
        let arg = match extract_bytes(&args[idx]) {
            Some(a) => a,
            None => break,
        };

        if arg.eq_ignore_ascii_case(b"NOMKSTREAM") {
            nomkstream = true;
            idx += 1;
        } else if arg.eq_ignore_ascii_case(b"MAXLEN") || arg.eq_ignore_ascii_case(b"MINID") {
            let strategy = arg.as_ref();
            idx += 1;
            if idx >= args.len() {
                return err_wrong_args("XADD");
            }
            let next = match extract_bytes(&args[idx]) {
                Some(a) => a,
                None => return err_wrong_args("XADD"),
            };
            let (approximate, threshold_arg) = if next.as_ref() == b"~" {
                idx += 1;
                if idx >= args.len() {
                    return err_wrong_args("XADD");
                }
                (
                    true,
                    match extract_bytes(&args[idx]) {
                        Some(a) => a,
                        None => return err_wrong_args("XADD"),
                    },
                )
            } else if next.as_ref() == b"=" {
                idx += 1;
                if idx >= args.len() {
                    return err_wrong_args("XADD");
                }
                (
                    false,
                    match extract_bytes(&args[idx]) {
                        Some(a) => a,
                        None => return err_wrong_args("XADD"),
                    },
                )
            } else {
                (false, next)
            };
            trim_strategy = Some((strategy, approximate, threshold_arg));
            idx += 1;
        } else {
            break; // Must be the ID
        }
    }

    // idx now points to the ID
    if idx >= args.len() {
        return err_wrong_args("XADD");
    }
    let id_arg = match extract_bytes(&args[idx]) {
        Some(a) => a,
        None => return err_wrong_args("XADD"),
    };
    idx += 1;

    // Parse field-value pairs (remaining args)
    let remaining = args.len() - idx;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return err_wrong_args("XADD");
    }

    let mut fields = Vec::with_capacity(remaining / 2);
    while idx + 1 < args.len() {
        let field = match extract_bytes(&args[idx]) {
            Some(f) => f.clone(),
            None => return err_wrong_args("XADD"),
        };
        let value = match extract_bytes(&args[idx + 1]) {
            Some(v) => v.clone(),
            None => return err_wrong_args("XADD"),
        };
        fields.push((field, value));
        idx += 2;
    }

    // moon#823: resolve the ID BEFORE the key can be created.
    //
    // `get_or_create_stream` inserts the entry, charges `entry_overhead` and
    // burns a birth version. Every ID error below used to return AFTER that
    // call, so a rejected `XADD` left an empty stream in the keyspace —
    // charged, `DBSIZE`-visible, and, because propagation is gated on the
    // reply not being an error, never written to the AOF or sent to a replica.
    // It vanished on the next restart. Measured: all five of `bogus`, `0-0`,
    // `1-1-1`, `abc-1` and `-5` created the key; 3,000 rejected XADDs cost
    // 1.46 MB that nothing ever credits back, on a command that only ever
    // answers an error. Real Redis parses the ID first and creates nothing.
    //
    // `last_id` is peeked from the existing stream (`0-0` when there is none),
    // which is exactly what `validate_explicit_id` compares against, so the
    // pre-resolution and the stream's own rule cannot disagree — they are the
    // same function.
    let last_id = match db.get_stream(key) {
        Ok(Some(s)) => s.last_id,
        Ok(None) => {
            // NOMKSTREAM: if key doesn't exist, return Null.
            if nomkstream {
                return Frame::Null;
            }
            StreamId { ms: 0, seq: 0 }
        }
        Err(e) => return e,
    };

    // `*` is resolved from the shard clock once the stream exists; it cannot
    // fail, so it is the one form that may stay below the create.
    let resolved_id = if id_arg.as_ref() == b"*" {
        None
    } else {
        match StreamId::parse(id_arg, 0) {
            Ok(parsed) => {
                // "ms-*" takes the sequence from last_id.
                let s_str = std::str::from_utf8(id_arg).unwrap_or("");
                if s_str.ends_with("-*") {
                    let ms = parsed.ms;
                    let seq = if ms == last_id.ms {
                        last_id.seq + 1
                    } else if ms > last_id.ms {
                        0
                    } else {
                        return Frame::Error(Bytes::from_static(
                            b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
                        ));
                    };
                    Some(StreamId { ms, seq })
                } else {
                    match crate::storage::stream::validate_explicit_id_against(last_id, parsed) {
                        Ok(id) => Some(id),
                        Err(e) => return Frame::Error(Bytes::from(e)),
                    }
                }
            }
            Err(e) => return Frame::Error(Bytes::from(e)),
        }
    };

    // Get or create the stream — from here on nothing may return an error.
    let stream = match db.get_or_create_stream(key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    let id = match resolved_id {
        Some(id) => id,
        None => stream.next_auto_id(),
    };

    let result_id = stream.add(id, fields);

    // Apply trim if specified
    if let Some((strategy, approximate, threshold_bytes)) = trim_strategy {
        if strategy.eq_ignore_ascii_case(b"MAXLEN") {
            if let Ok(s) = std::str::from_utf8(threshold_bytes) {
                if let Ok(maxlen) = s.parse::<u64>() {
                    stream.trim_maxlen(maxlen, approximate);
                }
            }
        } else if strategy.eq_ignore_ascii_case(b"MINID") {
            if let Ok(minid) = StreamId::parse(threshold_bytes, 0) {
                stream.trim_minid(minid, approximate);
            }
        }
    }

    Frame::BulkString(result_id.to_bytes())
}

/// XTRIM key MAXLEN|MINID [=|~] threshold
pub fn xtrim(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("XTRIM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XTRIM"),
    };
    let strategy = match extract_bytes(&args[1]) {
        Some(s) => s,
        None => return err_wrong_args("XTRIM"),
    };

    let mut idx = 2;
    let approximate;
    if let Some(modifier) = extract_bytes(&args[idx]) {
        if modifier.as_ref() == b"~" {
            approximate = true;
            idx += 1;
        } else if modifier.as_ref() == b"=" {
            approximate = false;
            idx += 1;
        } else {
            approximate = false;
        }
    } else {
        approximate = false;
    }

    if idx >= args.len() {
        return err_wrong_args("XTRIM");
    }
    let threshold = match extract_bytes(&args[idx]) {
        Some(t) => t,
        None => return err_wrong_args("XTRIM"),
    };

    let stream = match db.get_stream_mut(key) {
        Ok(Some(s)) => s,
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
    };

    let removed = if strategy.eq_ignore_ascii_case(b"MAXLEN") {
        match std::str::from_utf8(threshold) {
            Ok(s) => match s.parse::<u64>() {
                Ok(maxlen) => stream.trim_maxlen(maxlen, approximate),
                Err(_) => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
                }
            },
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        }
    } else if strategy.eq_ignore_ascii_case(b"MINID") {
        match StreamId::parse(threshold, 0) {
            Ok(minid) => stream.trim_minid(minid, approximate),
            Err(e) => return Frame::Error(Bytes::from(e)),
        }
    } else {
        return Frame::Error(Bytes::from_static(
            b"ERR syntax error, XTRIM requires MAXLEN or MINID",
        ));
    };

    Frame::Integer(removed as i64)
}

/// XDEL key id [id ...]
pub fn xdel(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("XDEL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XDEL"),
    };

    let mut ids = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let id_bytes = match extract_bytes(arg) {
            Some(b) => b,
            None => return err_wrong_args("XDEL"),
        };
        match StreamId::parse(id_bytes, 0) {
            Ok(id) => ids.push(id),
            Err(e) => return Frame::Error(Bytes::from(e)),
        }
    }

    let stream = match db.get_stream_mut(key) {
        Ok(Some(s)) => s,
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
    };

    let deleted = stream.delete(&ids);
    Frame::Integer(deleted as i64)
}

/// XGROUP subcommand handler.
/// XGROUP CREATE key group id [MKSTREAM]
/// XGROUP DESTROY key group
/// XGROUP SETID key group id
/// XGROUP CREATECONSUMER key group consumer
/// XGROUP DELCONSUMER key group consumer
pub fn xgroup(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("XGROUP");
    }
    let subcmd = match extract_bytes(&args[0]) {
        Some(s) => s,
        None => return err_wrong_args("XGROUP"),
    };
    // moon#670: an unknown subcommand is refused with Redis's shape BEFORE any
    // arity check, and from the SAME table the `MULTI` queue gate consults. An
    // arity error here reads to a client as "the subcommand exists, you called
    // it wrong", which is how `XGROUP BOGUS` used to answer.
    if let Some(help) = crate::command::help_text::help_if_requested("XGROUP", subcmd) {
        return help;
    }
    if !crate::command::metadata::is_known_subcommand(b"XGROUP", subcmd) {
        {
            return crate::command::helpers::err_unknown_subcommand("XGROUP", subcmd);
        }
    }

    if subcmd.eq_ignore_ascii_case(b"CREATE") {
        // XGROUP CREATE key group id [MKSTREAM]
        if args.len() < 4 {
            return err_wrong_args("XGROUP CREATE");
        }
        let key = match extract_bytes(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("XGROUP CREATE"),
        };
        let group_name = match extract_bytes(&args[2]) {
            Some(g) => g.clone(),
            None => return err_wrong_args("XGROUP CREATE"),
        };
        let id_arg = match extract_bytes(&args[3]) {
            Some(i) => i,
            None => return err_wrong_args("XGROUP CREATE"),
        };

        let mkstream = args.len() > 4
            && extract_bytes(&args[4]).map_or(false, |a| a.eq_ignore_ascii_case(b"MKSTREAM"));

        // Resolve $ to last_id
        let last_delivered_id = if id_arg.as_ref() == b"$" {
            match db.get_stream(key) {
                Ok(Some(s)) => s.last_id,
                Ok(None) => StreamId::ZERO,
                Err(e) => return e,
            }
        } else if id_arg.as_ref() == b"0" || id_arg.as_ref() == b"0-0" {
            StreamId::ZERO
        } else {
            match StreamId::parse(id_arg, 0) {
                Ok(id) => id,
                Err(e) => return Frame::Error(Bytes::from(e)),
            }
        };

        // Check if stream exists; create if MKSTREAM
        let stream = match db.get_stream_mut(key) {
            Ok(Some(s)) => s,
            Ok(None) => {
                if mkstream {
                    match db.get_or_create_stream(key) {
                        Ok(s) => s,
                        Err(e) => return e,
                    }
                } else {
                    return Frame::Error(Bytes::from_static(
                        b"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    ));
                }
            }
            Err(e) => return e,
        };

        match stream.create_group(group_name, last_delivered_id) {
            Ok(()) => Frame::SimpleString(Bytes::from_static(b"OK")),
            Err(e) => Frame::Error(Bytes::from(e)),
        }
    } else if subcmd.eq_ignore_ascii_case(b"DESTROY") {
        if args.len() < 3 {
            return err_wrong_args("XGROUP DESTROY");
        }
        let key = match extract_bytes(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("XGROUP DESTROY"),
        };
        let group_name = match extract_bytes(&args[2]) {
            Some(g) => g,
            None => return err_wrong_args("XGROUP DESTROY"),
        };
        let stream = match db.get_stream_mut(key) {
            Ok(Some(s)) => s,
            Ok(None) => return Frame::Integer(0),
            Err(e) => return e,
        };
        if stream.destroy_group(group_name) {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        }
    } else if subcmd.eq_ignore_ascii_case(b"SETID") {
        if args.len() < 4 {
            return err_wrong_args("XGROUP SETID");
        }
        let key = match extract_bytes(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("XGROUP SETID"),
        };
        let group_name = match extract_bytes(&args[2]) {
            Some(g) => g,
            None => return err_wrong_args("XGROUP SETID"),
        };
        let id_arg = match extract_bytes(&args[3]) {
            Some(i) => i,
            None => return err_wrong_args("XGROUP SETID"),
        };

        let id = if id_arg.as_ref() == b"$" {
            match db.get_stream(key) {
                Ok(Some(s)) => s.last_id,
                Ok(None) => StreamId::ZERO,
                Err(e) => return e,
            }
        } else {
            match StreamId::parse(id_arg, 0) {
                Ok(id) => id,
                Err(e) => return Frame::Error(Bytes::from(e)),
            }
        };

        let stream = match db.get_stream_mut(key) {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR The XGROUP subcommand requires the key to exist.",
                ));
            }
            Err(e) => return e,
        };
        match stream.set_group_id(group_name, id) {
            Ok(()) => Frame::SimpleString(Bytes::from_static(b"OK")),
            Err(e) => Frame::Error(Bytes::from(e)),
        }
    } else if subcmd.eq_ignore_ascii_case(b"CREATECONSUMER") {
        if args.len() < 4 {
            return err_wrong_args("XGROUP CREATECONSUMER");
        }
        let key = match extract_bytes(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("XGROUP CREATECONSUMER"),
        };
        let group_name = match extract_bytes(&args[2]) {
            Some(g) => g,
            None => return err_wrong_args("XGROUP CREATECONSUMER"),
        };
        let consumer_name = match extract_bytes(&args[3]) {
            Some(c) => c.clone(),
            None => return err_wrong_args("XGROUP CREATECONSUMER"),
        };
        let stream = match db.get_stream_mut(key) {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR The XGROUP subcommand requires the key to exist.",
                ));
            }
            Err(e) => return e,
        };
        match stream.create_consumer(group_name, consumer_name) {
            Ok(true) => Frame::Integer(1),
            Ok(false) => Frame::Integer(0),
            Err(e) => Frame::Error(Bytes::from(e)),
        }
    } else if subcmd.eq_ignore_ascii_case(b"DELCONSUMER") {
        if args.len() < 4 {
            return err_wrong_args("XGROUP DELCONSUMER");
        }
        let key = match extract_bytes(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("XGROUP DELCONSUMER"),
        };
        let group_name = match extract_bytes(&args[2]) {
            Some(g) => g,
            None => return err_wrong_args("XGROUP DELCONSUMER"),
        };
        let consumer_name = match extract_bytes(&args[3]) {
            Some(c) => c,
            None => return err_wrong_args("XGROUP DELCONSUMER"),
        };
        let stream = match db.get_stream_mut(key) {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR The XGROUP subcommand requires the key to exist.",
                ));
            }
            Err(e) => return e,
        };
        match stream.delete_consumer(group_name, consumer_name) {
            Ok(count) => Frame::Integer(count as i64),
            Err(e) => Frame::Error(Bytes::from(e)),
        }
    } else {
        Frame::Error(Bytes::from_static(
            b"ERR 'XGROUP' command 'UNKNOWN' not recognized",
        ))
    }
}

/// XREADGROUP GROUP group consumer [COUNT count] [NOACK] STREAMS key [key ...] id [id ...]
pub fn xreadgroup(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("XREADGROUP");
    }

    let mut idx = 0;

    // Expect GROUP keyword
    let first = match extract_bytes(&args[idx]) {
        Some(a) => a,
        None => return err_wrong_args("XREADGROUP"),
    };
    if !first.eq_ignore_ascii_case(b"GROUP") {
        return Frame::Error(Bytes::from_static(b"ERR 'GROUP' keyword expected"));
    }
    idx += 1;

    if idx + 1 >= args.len() {
        return err_wrong_args("XREADGROUP");
    }
    let group = match extract_bytes(&args[idx]) {
        Some(g) => g.clone(),
        None => return err_wrong_args("XREADGROUP"),
    };
    idx += 1;
    let consumer = match extract_bytes(&args[idx]) {
        Some(c) => c.clone(),
        None => return err_wrong_args("XREADGROUP"),
    };
    idx += 1;

    let mut count: Option<usize> = None;
    let mut noack = false;

    // Parse options
    while idx < args.len() {
        let arg = match extract_bytes(&args[idx]) {
            Some(a) => a,
            None => break,
        };
        if arg.eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return err_wrong_args("XREADGROUP");
            }
            if let Some(n) = extract_bytes(&args[idx]) {
                if let Ok(s) = std::str::from_utf8(n) {
                    if let Ok(v) = s.parse::<usize>() {
                        count = Some(v);
                    }
                }
            }
            idx += 1;
        } else if arg.eq_ignore_ascii_case(b"BLOCK") {
            idx += 1; // skip timeout value (handled in shard for blocking)
            if idx >= args.len() {
                return err_wrong_args("XREADGROUP");
            }
            idx += 1;
        } else if arg.eq_ignore_ascii_case(b"NOACK") {
            noack = true;
            idx += 1;
        } else if arg.eq_ignore_ascii_case(b"STREAMS") {
            idx += 1;
            break;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR Unrecognized XREADGROUP option"));
        }
    }

    // Parse keys and IDs
    let remaining = args.len() - idx;
    if remaining == 0 || !remaining.is_multiple_of(2) {
        return err_wrong_args("XREADGROUP");
    }

    let num_streams = remaining / 2;
    let keys_start = idx;
    let ids_start = idx + num_streams;

    // Redis emits a stream in the reply when it was SERVED, and the two modes
    // are served on different conditions (`xreadCommand`):
    //
    //   * HISTORY mode (an explicit ID, not `>`) is always served — the stream
    //     name and its entry array go out before Redis knows whether the
    //     consumer's PEL slice has anything in it, so an empty PEL answers
    //     `[["s", []]]`, not a null. Moon used to answer the null and a client
    //     iterating the stream list got a decode error where Redis gives it
    //     zero iterations (moon#526).
    //   * `>` mode is served only when there ARE new entries; a stream with
    //     nothing new is DROPPED from the reply rather than rendered as an
    //     empty entry list.
    //
    // Only when nothing at all was served does the reply become the null array.
    let mut results = Vec::new();

    for i in 0..num_streams {
        let key = match extract_bytes(&args[keys_start + i]) {
            Some(k) => k,
            None => return err_wrong_args("XREADGROUP"),
        };
        let id_bytes = match extract_bytes(&args[ids_start + i]) {
            Some(b) => b,
            None => return err_wrong_args("XREADGROUP"),
        };

        let is_new = id_bytes.as_ref() == b">";

        let stream = match db.get_stream_mut(key) {
            Ok(Some(s)) => s,
            Ok(None) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR The XREADGROUP subcommand requires the key to exist.",
                ));
            }
            Err(e) => return e,
        };

        let entries = if is_new {
            match stream.read_group_new(&group, &consumer, count, noack) {
                Ok(e) => e,
                Err(e) => return Frame::Error(Bytes::from(e)),
            }
        } else {
            let start = if id_bytes.as_ref() == b"0" || id_bytes.as_ref() == b"0-0" {
                StreamId::ZERO
            } else {
                match StreamId::parse(id_bytes, 0) {
                    Ok(id) => id,
                    Err(e) => return Frame::Error(Bytes::from(e)),
                }
            };
            match stream.read_group_pending(&group, &consumer, start, count) {
                Ok(e) => e,
                Err(e) => return Frame::Error(Bytes::from(e)),
            }
        };

        // `>` with nothing new is not served at all; history always is.
        if is_new && entries.is_empty() {
            continue;
        }
        let entry_frames: Vec<Frame> = entries
            .iter()
            .map(|(id, fields)| format_entry(*id, fields))
            .collect();
        results.push(Frame::Array(framevec![
            Frame::BulkString(key.clone()),
            Frame::Array(entry_frames.into()),
        ]));
    }

    if !results.is_empty() {
        Frame::Array(results.into())
    } else {
        // Null ARRAY, like `XREAD`: the reply is an array of streams, so its
        // "nothing" is a missing array. Measured against redis-server 8.6.1,
        // which answers `*-1` here (moon#482). Reachable only when every
        // requested stream was a `>` read that found nothing — a history read
        // always contributes a (possibly empty) entry (moon#526).
        //
        // This site sits in `stream_write.rs` while its twin sits in
        // `stream_read.rs`, which is exactly why the first sweep for #482
        // missed it — the audit walked the read file. Caught in review.
        Frame::NullArray
    }
}

/// XACK key group id [id ...]
pub fn xack(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("XACK");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XACK"),
    };
    let group = match extract_bytes(&args[1]) {
        Some(g) => g,
        None => return err_wrong_args("XACK"),
    };

    let mut ids = Vec::with_capacity(args.len() - 2);
    for arg in &args[2..] {
        let id_bytes = match extract_bytes(arg) {
            Some(b) => b,
            None => return err_wrong_args("XACK"),
        };
        match StreamId::parse(id_bytes, 0) {
            Ok(id) => ids.push(id),
            Err(e) => return Frame::Error(Bytes::from(e)),
        }
    }

    let stream = match db.get_stream_mut(key) {
        Ok(Some(s)) => s,
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
    };

    // Convert group Bytes to owned for the borrow
    let group_owned = group.clone();
    match stream.xack(&group_owned, &ids) {
        Ok(count) => Frame::Integer(count as i64),
        Err(e) => Frame::Error(Bytes::from(e)),
    }
}

/// XCLAIM key group consumer min-idle-time id [id ...]
pub fn xclaim(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 5 {
        return err_wrong_args("XCLAIM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XCLAIM"),
    };
    let group = match extract_bytes(&args[1]) {
        Some(g) => g.clone(),
        None => return err_wrong_args("XCLAIM"),
    };
    let consumer = match extract_bytes(&args[2]) {
        Some(c) => c.clone(),
        None => return err_wrong_args("XCLAIM"),
    };
    let min_idle_bytes = match extract_bytes(&args[3]) {
        Some(b) => b,
        None => return err_wrong_args("XCLAIM"),
    };
    let min_idle = match std::str::from_utf8(min_idle_bytes) {
        Ok(s) => match s.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        },
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };

    let mut ids = Vec::new();
    for arg in &args[4..] {
        let id_bytes = match extract_bytes(arg) {
            Some(b) => b,
            None => continue, // Skip options like JUSTID, FORCE, etc.
        };
        match StreamId::parse(id_bytes, 0) {
            Ok(id) => ids.push(id),
            Err(_) => continue, // Skip unrecognized args (could be options)
        }
    }

    let stream = match db.get_stream_mut(key) {
        Ok(Some(s)) => s,
        Ok(None) => return Frame::Array(framevec![]),
        Err(e) => return e,
    };

    match stream.xclaim(&group, &consumer, min_idle, &ids) {
        Ok(entries) => {
            let frames: Vec<Frame> = entries
                .iter()
                .map(|(id, fields)| format_entry(*id, fields))
                .collect();
            Frame::Array(frames.into())
        }
        Err(e) => Frame::Error(Bytes::from(e)),
    }
}

/// XAUTOCLAIM key group consumer min-idle-time start [COUNT count]
pub fn xautoclaim(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 5 {
        return err_wrong_args("XAUTOCLAIM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XAUTOCLAIM"),
    };
    let group = match extract_bytes(&args[1]) {
        Some(g) => g.clone(),
        None => return err_wrong_args("XAUTOCLAIM"),
    };
    let consumer = match extract_bytes(&args[2]) {
        Some(c) => c.clone(),
        None => return err_wrong_args("XAUTOCLAIM"),
    };
    let min_idle_bytes = match extract_bytes(&args[3]) {
        Some(b) => b,
        None => return err_wrong_args("XAUTOCLAIM"),
    };
    let min_idle = match std::str::from_utf8(min_idle_bytes) {
        Ok(s) => match s.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        },
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let start_bytes = match extract_bytes(&args[4]) {
        Some(b) => b,
        None => return err_wrong_args("XAUTOCLAIM"),
    };
    let start = match StreamId::parse(start_bytes, 0) {
        Ok(id) => id,
        Err(e) => return Frame::Error(Bytes::from(e)),
    };

    let mut count: usize = 100; // default
    if args.len() >= 7 {
        if let Some(opt) = extract_bytes(&args[5]) {
            if opt.eq_ignore_ascii_case(b"COUNT") {
                if let Some(n) = extract_bytes(&args[6]) {
                    if let Ok(s) = std::str::from_utf8(n) {
                        if let Ok(v) = s.parse::<usize>() {
                            count = v;
                        }
                    }
                }
            }
        }
    }

    let stream = match db.get_stream_mut(key) {
        Ok(Some(s)) => s,
        Ok(None) => return Frame::Error(Bytes::from_static(b"ERR no such key")),
        Err(e) => return e,
    };

    match stream.xautoclaim(&group, &consumer, min_idle, start, count) {
        Ok((next_id, claimed, deleted)) => {
            let claimed_frames: Vec<Frame> = claimed
                .iter()
                .map(|(id, fields)| format_entry(*id, fields))
                .collect();
            let deleted_frames: Vec<Frame> = deleted
                .iter()
                .map(|id| Frame::BulkString(id.to_bytes()))
                .collect();
            Frame::Array(framevec![
                Frame::BulkString(next_id.to_bytes()),
                Frame::Array(claimed_frames.into()),
                Frame::Array(deleted_frames.into()),
            ])
        }
        Err(e) => Frame::Error(Bytes::from(e)),
    }
}

/// XSETID key last-id [ENTRIESADDED entries-added]
///
/// Sets the last delivered ID of a stream without adding entries.
pub fn xsetid(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("XSETID");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XSETID"),
    };
    let id_str = match extract_bytes(&args[1]) {
        Some(s) => s,
        None => return err_wrong_args("XSETID"),
    };
    let id = match StreamId::parse(id_str, 0) {
        Ok(id) => id,
        Err(e) => return Frame::Error(Bytes::from_static(e.as_bytes())),
    };

    match db.get_stream_mut(key) {
        Ok(Some(stream)) => {
            stream.last_id = id;
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        Ok(None) => Frame::Error(Bytes::from_static(b"ERR no such key")),
        Err(e) => e,
    }
}
