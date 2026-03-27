//! Stream command handlers: XADD, XLEN, XRANGE, XREVRANGE, XTRIM, XDEL, XREAD.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::storage::stream::StreamId;
fn extract_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

fn err_wrong_args(cmd: &str) -> Frame {
    Frame::Error(Bytes::from(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd
    )))
}

/// Format a stream entry as a RESP nested array: [id, [field, value, ...]]
pub(crate) fn format_entry(id: StreamId, fields: &[(Bytes, Bytes)]) -> Frame {
    let mut field_frames = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        field_frames.push(Frame::BulkString(f.clone()));
        field_frames.push(Frame::BulkString(v.clone()));
    }
    Frame::Array(framevec![
        Frame::BulkString(id.to_bytes()),
        Frame::Array(field_frames.into()),
    ])
}

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
    if remaining == 0 || remaining % 2 != 0 {
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

    // NOMKSTREAM: if key doesn't exist, return Null
    if nomkstream {
        match db.get_stream(key) {
            Ok(None) => return Frame::Null,
            Err(e) => return e,
            Ok(Some(_)) => {} // exists, proceed
        }
    }

    // Get or create the stream
    let stream = match db.get_or_create_stream(key) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // Generate or validate ID
    let id = if id_arg.as_ref() == b"*" {
        stream.next_auto_id()
    } else {
        // Check for "ms-*" pattern (auto-seq for explicit ms)
        match StreamId::parse(id_arg, 0) {
            Ok(parsed) => {
                // For "ms-*", we need auto-seq: if ms == last_id.ms, seq = last_id.seq + 1
                let s_str = std::str::from_utf8(id_arg).unwrap_or("");
                if s_str.ends_with("-*") {
                    let ms = parsed.ms;
                    let seq = if ms == stream.last_id.ms {
                        stream.last_id.seq + 1
                    } else if ms > stream.last_id.ms {
                        0
                    } else {
                        return Frame::Error(Bytes::from_static(
                            b"ERR The ID specified in XADD is equal or smaller than the target stream top item",
                        ));
                    };
                    StreamId { ms, seq }
                } else {
                    match stream.validate_explicit_id(parsed) {
                        Ok(id) => id,
                        Err(e) => return Frame::Error(Bytes::from(e)),
                    }
                }
            }
            Err(e) => return Frame::Error(Bytes::from(e)),
        }
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

/// XLEN key
pub fn xlen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("XLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XLEN"),
    };
    match db.get_stream(key) {
        Ok(Some(stream)) => Frame::Integer(stream.length as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// XRANGE key start end [COUNT count]
pub fn xrange(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("XRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XRANGE"),
    };
    let start_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("XRANGE"),
    };
    let end_bytes = match extract_bytes(&args[2]) {
        Some(b) => b,
        None => return err_wrong_args("XRANGE"),
    };

    let start = match StreamId::parse(start_bytes, 0) {
        Ok(id) => id,
        Err(e) => return Frame::Error(Bytes::from(e)),
    };
    let end = match StreamId::parse(end_bytes, u64::MAX) {
        Ok(id) => id,
        Err(e) => return Frame::Error(Bytes::from(e)),
    };

    let mut count = None;
    if args.len() >= 5 {
        if let Some(c) = extract_bytes(&args[3]) {
            if c.eq_ignore_ascii_case(b"COUNT") {
                if let Some(n) = extract_bytes(&args[4]) {
                    if let Ok(s) = std::str::from_utf8(n) {
                        if let Ok(v) = s.parse::<usize>() {
                            count = Some(v);
                        }
                    }
                }
            }
        }
    }

    match db.get_stream(key) {
        Ok(Some(stream)) => {
            let entries = stream.range(start, end, count);
            let frames: Vec<Frame> = entries
                .into_iter()
                .map(|(id, fields)| format_entry(id, fields))
                .collect();
            Frame::Array(frames.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// XREVRANGE key end start [COUNT count]
/// Note: argument order is reversed compared to XRANGE.
pub fn xrevrange(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("XREVRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XREVRANGE"),
    };
    let end_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("XREVRANGE"),
    };
    let start_bytes = match extract_bytes(&args[2]) {
        Some(b) => b,
        None => return err_wrong_args("XREVRANGE"),
    };

    let end = match StreamId::parse(end_bytes, u64::MAX) {
        Ok(id) => id,
        Err(e) => return Frame::Error(Bytes::from(e)),
    };
    let start = match StreamId::parse(start_bytes, 0) {
        Ok(id) => id,
        Err(e) => return Frame::Error(Bytes::from(e)),
    };

    let mut count = None;
    if args.len() >= 5 {
        if let Some(c) = extract_bytes(&args[3]) {
            if c.eq_ignore_ascii_case(b"COUNT") {
                if let Some(n) = extract_bytes(&args[4]) {
                    if let Ok(s) = std::str::from_utf8(n) {
                        if let Ok(v) = s.parse::<usize>() {
                            count = Some(v);
                        }
                    }
                }
            }
        }
    }

    match db.get_stream(key) {
        Ok(Some(stream)) => {
            let entries = stream.range_rev(start, end, count);
            let frames: Vec<Frame> = entries
                .into_iter()
                .map(|(id, fields)| format_entry(id, fields))
                .collect();
            Frame::Array(frames.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
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

/// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
/// BLOCK is parsed but ignored in this plan (Plan 02 handles blocking).
pub fn xread(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("XREAD");
    }

    let mut idx = 0;
    let mut count: Option<usize> = None;

    // Parse options
    while idx < args.len() {
        let arg = match extract_bytes(&args[idx]) {
            Some(a) => a,
            None => break,
        };

        if arg.eq_ignore_ascii_case(b"COUNT") {
            idx += 1;
            if idx >= args.len() {
                return err_wrong_args("XREAD");
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
            idx += 1; // Skip the BLOCK arg
            if idx >= args.len() {
                return err_wrong_args("XREAD");
            }
            idx += 1; // Skip the milliseconds value (ignored in this plan)
        } else if arg.eq_ignore_ascii_case(b"STREAMS") {
            idx += 1;
            break;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR Unrecognized XREAD option"));
        }
    }

    // After STREAMS keyword: remaining args are keys... followed by ids...
    let remaining = args.len() - idx;
    if remaining == 0 || remaining % 2 != 0 {
        return err_wrong_args("XREAD");
    }

    let num_streams = remaining / 2;
    let keys_start = idx;
    let ids_start = idx + num_streams;

    let mut results = Vec::new();
    let mut has_entries = false;

    for i in 0..num_streams {
        let key = match extract_bytes(&args[keys_start + i]) {
            Some(k) => k,
            None => return err_wrong_args("XREAD"),
        };
        let id_bytes = match extract_bytes(&args[ids_start + i]) {
            Some(b) => b,
            None => return err_wrong_args("XREAD"),
        };

        // Resolve "$" to stream's last_id
        let start_id = if id_bytes.as_ref() == b"$" {
            match db.get_stream(key) {
                Ok(Some(stream)) => stream.last_id,
                Ok(None) => StreamId::ZERO,
                Err(e) => return e,
            }
        } else {
            match StreamId::parse(id_bytes, 0) {
                Ok(id) => id,
                Err(e) => return Frame::Error(Bytes::from(e)),
            }
        };

        // Read entries with ID > start_id
        let next_id = StreamId {
            ms: start_id.ms,
            seq: start_id.seq.saturating_add(1),
        };
        // Handle overflow: if seq was u64::MAX, increment ms
        let next_id = if start_id.seq == u64::MAX {
            StreamId {
                ms: start_id.ms.saturating_add(1),
                seq: 0,
            }
        } else {
            next_id
        };

        match db.get_stream(key) {
            Ok(Some(stream)) => {
                let entries = stream.range(next_id, StreamId::MAX, count);
                if !entries.is_empty() {
                    has_entries = true;
                }
                let entry_frames: Vec<Frame> = entries
                    .into_iter()
                    .map(|(id, fields)| format_entry(id, fields))
                    .collect();
                results.push(Frame::Array(framevec![
                    Frame::BulkString(key.clone()),
                    Frame::Array(entry_frames.into()),
                ]));
            }
            Ok(None) => {
                // Stream doesn't exist, no entries
                results.push(Frame::Array(framevec![
                    Frame::BulkString(key.clone()),
                    Frame::Array(framevec![]),
                ]));
            }
            Err(e) => return e,
        }
    }

    if has_entries {
        Frame::Array(results.into())
    } else {
        Frame::Null
    }
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
    if remaining == 0 || remaining % 2 != 0 {
        return err_wrong_args("XREADGROUP");
    }

    let num_streams = remaining / 2;
    let keys_start = idx;
    let ids_start = idx + num_streams;

    let mut results = Vec::new();
    let mut has_entries = false;

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

        if !entries.is_empty() {
            has_entries = true;
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

    if has_entries {
        Frame::Array(results.into())
    } else {
        Frame::Null
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

/// XPENDING key group [start end count [consumer]]
pub fn xpending(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("XPENDING");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("XPENDING"),
    };
    let group = match extract_bytes(&args[1]) {
        Some(g) => g.clone(),
        None => return err_wrong_args("XPENDING"),
    };

    let stream = match db.get_stream(key) {
        Ok(Some(s)) => s,
        Ok(None) => {
            // No stream => no group
            return Frame::Error(Bytes::from_static(b"ERR no such key"));
        }
        Err(e) => return e,
    };

    if args.len() == 2 {
        // Summary form
        match stream.xpending_summary(&group) {
            Ok(summary) => {
                if summary.is_empty() {
                    // No pending entries
                    Frame::Array(framevec![
                        Frame::Integer(0),
                        Frame::Null,
                        Frame::Null,
                        Frame::Null,
                    ])
                } else {
                    let (_, min_id, max_id, consumers) = &summary[0];
                    let total: u64 = consumers.iter().map(|(_, c)| c).sum();
                    let consumer_frames: Vec<Frame> = consumers
                        .iter()
                        .map(|(name, count)| {
                            Frame::Array(framevec![
                                Frame::BulkString(name.clone()),
                                Frame::BulkString(Bytes::from(count.to_string())),
                            ])
                        })
                        .collect();
                    Frame::Array(framevec![
                        Frame::Integer(total as i64),
                        Frame::BulkString(min_id.to_bytes()),
                        Frame::BulkString(max_id.to_bytes()),
                        Frame::Array(consumer_frames.into()),
                    ])
                }
            }
            Err(e) => Frame::Error(Bytes::from(e)),
        }
    } else if args.len() >= 5 {
        // Detail form: XPENDING key group start end count [consumer]
        // Handle IDLE option (Redis 6.2+): XPENDING key group IDLE min-idle start end count [consumer]
        let mut detail_idx = 2;
        let mut _min_idle: u64 = 0;
        if let Some(arg) = extract_bytes(&args[detail_idx]) {
            if arg.eq_ignore_ascii_case(b"IDLE") {
                detail_idx += 1;
                if detail_idx >= args.len() {
                    return err_wrong_args("XPENDING");
                }
                if let Some(idle_bytes) = extract_bytes(&args[detail_idx]) {
                    if let Ok(s) = std::str::from_utf8(idle_bytes) {
                        _min_idle = s.parse::<u64>().unwrap_or(0);
                    }
                }
                detail_idx += 1;
            }
        }

        if detail_idx + 2 >= args.len() {
            return err_wrong_args("XPENDING");
        }

        let start_bytes = match extract_bytes(&args[detail_idx]) {
            Some(b) => b,
            None => return err_wrong_args("XPENDING"),
        };
        let end_bytes = match extract_bytes(&args[detail_idx + 1]) {
            Some(b) => b,
            None => return err_wrong_args("XPENDING"),
        };
        let count_bytes = match extract_bytes(&args[detail_idx + 2]) {
            Some(b) => b,
            None => return err_wrong_args("XPENDING"),
        };

        let start = match StreamId::parse(start_bytes, 0) {
            Ok(id) => id,
            Err(e) => return Frame::Error(Bytes::from(e)),
        };
        let end = match StreamId::parse(end_bytes, u64::MAX) {
            Ok(id) => id,
            Err(e) => return Frame::Error(Bytes::from(e)),
        };
        let count = match std::str::from_utf8(count_bytes) {
            Ok(s) => match s.parse::<usize>() {
                Ok(c) => c,
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

        let consumer_filter = if detail_idx + 3 < args.len() {
            extract_bytes(&args[detail_idx + 3])
        } else {
            None
        };

        match stream.xpending_detail(&group, start, end, count, consumer_filter) {
            Ok(details) => {
                let frames: Vec<Frame> = details
                    .into_iter()
                    .map(|(id, consumer, idle, delivery_count)| {
                        Frame::Array(framevec![
                            Frame::BulkString(id.to_bytes()),
                            Frame::BulkString(consumer),
                            Frame::Integer(idle as i64),
                            Frame::Integer(delivery_count as i64),
                        ])
                    })
                    .collect();
                Frame::Array(frames.into())
            }
            Err(e) => Frame::Error(Bytes::from(e)),
        }
    } else {
        err_wrong_args("XPENDING")
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

/// XINFO STREAM key | XINFO GROUPS key | XINFO CONSUMERS key group
pub fn xinfo(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("XINFO");
    }
    let subcmd = match extract_bytes(&args[0]) {
        Some(s) => s,
        None => return err_wrong_args("XINFO"),
    };
    let key = match extract_bytes(&args[1]) {
        Some(k) => k,
        None => return err_wrong_args("XINFO"),
    };

    if subcmd.eq_ignore_ascii_case(b"STREAM") {
        let stream = match db.get_stream(key) {
            Ok(Some(s)) => s,
            Ok(None) => return Frame::Error(Bytes::from_static(b"ERR no such key")),
            Err(e) => return e,
        };

        let first_entry = stream
            .entries
            .iter()
            .next()
            .map(|(&id, fields)| format_entry(id, fields));
        let last_entry = stream
            .entries
            .iter()
            .next_back()
            .map(|(&id, fields)| format_entry(id, fields));

        Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"length")),
            Frame::Integer(stream.length as i64),
            Frame::BulkString(Bytes::from_static(b"radix-tree-keys")),
            Frame::Integer(stream.entries.len() as i64),
            Frame::BulkString(Bytes::from_static(b"radix-tree-nodes")),
            Frame::Integer(stream.entries.len() as i64),
            Frame::BulkString(Bytes::from_static(b"last-generated-id")),
            Frame::BulkString(stream.last_id.to_bytes()),
            Frame::BulkString(Bytes::from_static(b"groups")),
            Frame::Integer(stream.groups.len() as i64),
            Frame::BulkString(Bytes::from_static(b"first-entry")),
            first_entry.unwrap_or(Frame::Null),
            Frame::BulkString(Bytes::from_static(b"last-entry")),
            last_entry.unwrap_or(Frame::Null),
        ])
    } else if subcmd.eq_ignore_ascii_case(b"GROUPS") {
        let stream = match db.get_stream(key) {
            Ok(Some(s)) => s,
            Ok(None) => return Frame::Error(Bytes::from_static(b"ERR no such key")),
            Err(e) => return e,
        };

        let groups: Vec<Frame> = stream
            .groups
            .iter()
            .map(|(name, group)| {
                Frame::Array(framevec![
                    Frame::BulkString(Bytes::from_static(b"name")),
                    Frame::BulkString(name.clone()),
                    Frame::BulkString(Bytes::from_static(b"consumers")),
                    Frame::Integer(group.consumers.len() as i64),
                    Frame::BulkString(Bytes::from_static(b"pending")),
                    Frame::Integer(group.pel.len() as i64),
                    Frame::BulkString(Bytes::from_static(b"last-delivered-id")),
                    Frame::BulkString(group.last_delivered_id.to_bytes()),
                ])
            })
            .collect();
        Frame::Array(groups.into())
    } else if subcmd.eq_ignore_ascii_case(b"CONSUMERS") {
        if args.len() < 3 {
            return err_wrong_args("XINFO CONSUMERS");
        }
        let group_name = match extract_bytes(&args[2]) {
            Some(g) => g,
            None => return err_wrong_args("XINFO CONSUMERS"),
        };
        let stream = match db.get_stream(key) {
            Ok(Some(s)) => s,
            Ok(None) => return Frame::Error(Bytes::from_static(b"ERR no such key")),
            Err(e) => return e,
        };
        let group = match stream.groups.get(group_name) {
            Some(g) => g,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"NOGROUP No such consumer group for key name",
                ));
            }
        };

        let now = crate::storage::entry::current_time_ms();
        let consumers: Vec<Frame> = group
            .consumers
            .iter()
            .map(|(name, c)| {
                let idle = now.saturating_sub(c.seen_time);
                Frame::Array(framevec![
                    Frame::BulkString(Bytes::from_static(b"name")),
                    Frame::BulkString(name.clone()),
                    Frame::BulkString(Bytes::from_static(b"pending")),
                    Frame::Integer(c.pending.len() as i64),
                    Frame::BulkString(Bytes::from_static(b"idle")),
                    Frame::Integer(idle as i64),
                ])
            })
            .collect();
        Frame::Array(consumers.into())
    } else {
        Frame::Error(Bytes::from_static(
            b"ERR 'XINFO' command 'UNKNOWN' not recognized",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::Database;

    fn make_args(parts: &[&[u8]]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
            .collect()
    }

    #[test]
    fn test_xadd_auto_id() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"*", b"name", b"alice"]);
        let result = xadd(&mut db, &args);
        match result {
            Frame::BulkString(id) => {
                assert!(id.as_ref().contains(&b'-'));
            }
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_xadd_explicit_id() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"1-1", b"name", b"alice"]);
        let result = xadd(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from("1-1")));

        let args = make_args(&[b"mystream", b"2-0", b"name", b"bob"]);
        let result = xadd(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from("2-0")));
    }

    #[test]
    fn test_xadd_nomkstream_nonexistent() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"NOMKSTREAM", b"*", b"f", b"v"]);
        let result = xadd(&mut db, &args);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_xadd_nomkstream_existing() {
        let mut db = Database::new();
        // Create stream first
        let args = make_args(&[b"mystream", b"*", b"f", b"v"]);
        xadd(&mut db, &args);
        // NOMKSTREAM on existing should work
        let args = make_args(&[b"mystream", b"NOMKSTREAM", b"*", b"f2", b"v2"]);
        let result = xadd(&mut db, &args);
        match result {
            Frame::BulkString(_) => {}
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_xadd_with_maxlen() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }
        // Add with MAXLEN 5
        let args = make_args(&[b"mystream", b"MAXLEN", b"5", b"11-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let len_args = make_args(&[b"mystream"]);
        let result = xlen(&mut db, &len_args);
        assert_eq!(result, Frame::Integer(5));
    }

    #[test]
    fn test_xlen() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &args), Frame::Integer(0));

        let args = make_args(&[b"mystream", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);
        let args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &args), Frame::Integer(1));
    }

    #[test]
    fn test_xrange_full() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let val = format!("v{}", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", val.as_bytes()]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"-", b"+"]);
        let result = xrange(&mut db, &args);
        match result {
            Frame::Array(entries) => assert_eq!(entries.len(), 3),
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xrange_with_count() {
        let mut db = Database::new();
        for i in 1..=5 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"-", b"+", b"COUNT", b"2"]);
        let result = xrange(&mut db, &args);
        match result {
            Frame::Array(entries) => assert_eq!(entries.len(), 2),
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xrevrange() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"+", b"-"]);
        let result = xrevrange(&mut db, &args);
        match result {
            Frame::Array(entries) => {
                assert_eq!(entries.len(), 3);
                // First entry should be highest ID (3-0)
                if let Frame::Array(ref inner) = entries[0] {
                    assert_eq!(inner[0], Frame::BulkString(Bytes::from("3-0")));
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xtrim_maxlen() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"MAXLEN", b"5"]);
        let result = xtrim(&mut db, &args);
        assert_eq!(result, Frame::Integer(5));

        let len_args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &len_args), Frame::Integer(5));
    }

    #[test]
    fn test_xtrim_approximate() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        // Approximate trim with ~ -- small excess may not trigger
        let args = make_args(&[b"mystream", b"MAXLEN", b"~", b"9"]);
        let result = xtrim(&mut db, &args);
        // With 10 entries and maxlen 9, approximate should NOT trim (excess < 10%)
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_xdel() {
        let mut db = Database::new();
        for i in 1..=5 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"2-0", b"4-0"]);
        let result = xdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(2));

        let len_args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &len_args), Frame::Integer(3));
    }

    #[test]
    fn test_xdel_nonexistent_ids() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"mystream", b"99-0"]);
        let result = xdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_xread_single_stream() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        // Read entries after 1-0
        let args = make_args(&[b"STREAMS", b"mystream", b"1-0"]);
        let result = xread(&mut db, &args);
        match result {
            Frame::Array(streams) => {
                assert_eq!(streams.len(), 1);
                if let Frame::Array(ref inner) = streams[0] {
                    assert_eq!(inner[0], Frame::BulkString(Bytes::from("mystream")));
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 2); // entries 2-0 and 3-0
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xread_dollar_id() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        // Read from $: should return Null since no new entries
        let args = make_args(&[b"STREAMS", b"mystream", b"$"]);
        let result = xread(&mut db, &args);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_xread_multiple_streams() {
        let mut db = Database::new();
        let args = make_args(&[b"s1", b"1-0", b"f", b"v1"]);
        xadd(&mut db, &args);
        let args = make_args(&[b"s2", b"1-0", b"f", b"v2"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"STREAMS", b"s1", b"s2", b"0-0", b"0-0"]);
        let result = xread(&mut db, &args);
        match result {
            Frame::Array(streams) => {
                assert_eq!(streams.len(), 2);
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xread_with_count() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"COUNT", b"3", b"STREAMS", b"mystream", b"0-0"]);
        let result = xread(&mut db, &args);
        match result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 3);
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xrange_nonexistent() {
        let mut db = Database::new();
        let args = make_args(&[b"nostream", b"-", b"+"]);
        let result = xrange(&mut db, &args);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    #[test]
    fn test_xdel_nonexistent_key() {
        let mut db = Database::new();
        let args = make_args(&[b"nostream", b"1-0"]);
        let result = xdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));
    }

    // ---- Consumer group tests (Plan 02) ----

    /// Helper: set up a stream with entries 1-0..n-0 and a consumer group.
    fn setup_stream_with_group(db: &mut Database, key: &[u8], n: u64, group: &[u8]) {
        for i in 1..=n {
            let id = format!("{}-0", i);
            let val = format!("v{}", i);
            let args = make_args(&[key, id.as_bytes(), b"f", val.as_bytes()]);
            xadd(db, &args);
        }
        // XGROUP CREATE key group 0 MKSTREAM
        let args = make_args(&[b"CREATE", key, group, b"0", b"MKSTREAM"]);
        let result = xgroup(db, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_xgroup_create_and_destroy() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"CREATE", b"mystream", b"mygroup", b"0", b"MKSTREAM"]);
        assert_eq!(
            xgroup(&mut db, &args),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );

        // Creating same group again should error
        let args = make_args(&[b"CREATE", b"mystream", b"mygroup", b"0", b"MKSTREAM"]);
        match xgroup(&mut db, &args) {
            Frame::Error(_) => {}
            other => panic!("Expected Error, got {:?}", other),
        }

        // Destroy
        let args = make_args(&[b"DESTROY", b"mystream", b"mygroup"]);
        assert_eq!(xgroup(&mut db, &args), Frame::Integer(1));

        // Destroy again
        let args = make_args(&[b"DESTROY", b"mystream", b"mygroup"]);
        assert_eq!(xgroup(&mut db, &args), Frame::Integer(0));
    }

    #[test]
    fn test_xgroup_create_mkstream_nonexistent() {
        let mut db = Database::new();
        // Without MKSTREAM on nonexistent key
        let args = make_args(&[b"CREATE", b"nostream", b"mygroup", b"0"]);
        match xgroup(&mut db, &args) {
            Frame::Error(_) => {}
            other => panic!("Expected Error, got {:?}", other),
        }
        // With MKSTREAM on nonexistent key
        let args = make_args(&[b"CREATE", b"nostream", b"mygroup", b"0", b"MKSTREAM"]);
        assert_eq!(
            xgroup(&mut db, &args),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
    }

    #[test]
    fn test_xreadgroup_new_entries() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // XREADGROUP GROUP g alice STREAMS s >
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                assert_eq!(streams.len(), 1);
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 3);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }

        // Reading > again should return Null (all delivered)
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        assert_eq!(xreadgroup(&mut db, &args), Frame::Null);
    }

    #[test]
    fn test_xreadgroup_pending_replay() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read all new entries
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // Read pending entries with 0
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b"0"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 3); // same 3 entries
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xack_removes_from_pel() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read new entries
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // ACK entries 1-0 and 2-0
        let args = make_args(&[b"s", b"g", b"1-0", b"2-0"]);
        let result = xack(&mut db, &args);
        assert_eq!(result, Frame::Integer(2));

        // ACK non-pending entry (silently skip)
        let args = make_args(&[b"s", b"g", b"99-0"]);
        let result = xack(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));

        // Pending replay should only show 3-0 now
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b"0"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 1);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xpending_summary() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read entries for alice
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        let args = make_args(&[b"s", b"g"]);
        let result = xpending(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[0], Frame::Integer(3)); // 3 pending
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xpending_detail() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read entries for alice
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        let args = make_args(&[b"s", b"g", b"-", b"+", b"10"]);
        let result = xpending(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // 3 detail entries
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xclaim_transfers_ownership() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // alice reads all
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // bob claims 1-0 with min-idle 0 (immediate claim)
        let args = make_args(&[b"s", b"g", b"bob", b"0", b"1-0"]);
        let result = xclaim(&mut db, &args);
        match &result {
            Frame::Array(entries) => {
                assert_eq!(entries.len(), 1);
            }
            other => panic!("Expected Array, got {:?}", other),
        }

        // alice's pending replay should now have only 2
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b"0"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 2);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xautoclaim_idle_entries() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // alice reads all
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // Auto-claim with min_idle 0 (all entries are idle enough)
        let args = make_args(&[b"s", b"g", b"bob", b"0", b"0-0"]);
        let result = xautoclaim(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // [next_id, claimed, deleted]
                if let Frame::Array(ref claimed) = arr[1] {
                    assert_eq!(claimed.len(), 3); // all 3 claimed
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xinfo_stream() {
        let mut db = Database::new();
        let args = make_args(&[b"s", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"STREAM", b"s"]);
        let result = xinfo(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                // Should have key-value pairs: length, radix-tree-keys, etc.
                assert!(arr.len() >= 10);
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"length")));
                assert_eq!(arr[1], Frame::Integer(1));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xinfo_groups() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 1, b"g1");

        let args = make_args(&[b"GROUPS", b"s"]);
        let result = xinfo(&mut db, &args);
        match &result {
            Frame::Array(groups) => {
                assert_eq!(groups.len(), 1);
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xinfo_consumers() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read as alice to auto-create consumer
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        let args = make_args(&[b"CONSUMERS", b"s", b"g"]);
        let result = xinfo(&mut db, &args);
        match &result {
            Frame::Array(consumers) => {
                assert_eq!(consumers.len(), 1); // alice
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xreadgroup_with_count() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 5, b"g");

        // Read with COUNT 2
        let args = make_args(&[
            b"GROUP", b"g", b"alice", b"COUNT", b"2", b"STREAMS", b"s", b">",
        ]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 2);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xgroup_setid() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 5, b"g");

        // Read 2 entries
        let args = make_args(&[
            b"GROUP", b"g", b"alice", b"COUNT", b"2", b"STREAMS", b"s", b">",
        ]);
        xreadgroup(&mut db, &args);

        // Set ID back to 0, so next > read delivers all again
        let args = make_args(&[b"SETID", b"s", b"g", b"0"]);
        assert_eq!(
            xgroup(&mut db, &args),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );

        // Reading > should deliver from 1-0 again
        let args = make_args(&[b"GROUP", b"g", b"bob", b"STREAMS", b"s", b">"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 5); // all 5 entries
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }
}
