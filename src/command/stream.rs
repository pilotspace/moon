//! Stream command handlers: XADD, XLEN, XRANGE, XREVRANGE, XTRIM, XDEL, XREAD.

use bytes::Bytes;

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
fn format_entry(id: StreamId, fields: &[(Bytes, Bytes)]) -> Frame {
    let mut field_frames = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        field_frames.push(Frame::BulkString(f.clone()));
        field_frames.push(Frame::BulkString(v.clone()));
    }
    Frame::Array(vec![
        Frame::BulkString(id.to_bytes()),
        Frame::Array(field_frames),
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
            Frame::Array(frames)
        }
        Ok(None) => Frame::Array(vec![]),
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
            Frame::Array(frames)
        }
        Ok(None) => Frame::Array(vec![]),
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
                    return Frame::Error(Bytes::from_static(b"ERR value is not an integer or out of range"))
                }
            },
            Err(_) => {
                return Frame::Error(Bytes::from_static(b"ERR value is not an integer or out of range"))
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
            return Frame::Error(Bytes::from_static(
                b"ERR Unrecognized XREAD option",
            ));
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
                results.push(Frame::Array(vec![
                    Frame::BulkString(key.clone()),
                    Frame::Array(entry_frames),
                ]));
            }
            Ok(None) => {
                // Stream doesn't exist, no entries
                results.push(Frame::Array(vec![
                    Frame::BulkString(key.clone()),
                    Frame::Array(vec![]),
                ]));
            }
            Err(e) => return e,
        }
    }

    if has_entries {
        Frame::Array(results)
    } else {
        Frame::Null
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
        assert_eq!(result, Frame::Array(vec![]));
    }

    #[test]
    fn test_xdel_nonexistent_key() {
        let mut db = Database::new();
        let args = make_args(&[b"nostream", b"1-0"]);
        let result = xdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));
    }
}
