//! Stream read command handlers: XLEN, XRANGE, XREVRANGE, XREAD, XINFO, XPENDING.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::db::Database;
use crate::storage::stream::StreamId;

use crate::command::helpers::{err_wrong_args, extract_bytes};
use super::format_entry;

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
    if remaining == 0 || !remaining.is_multiple_of(2) {
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
