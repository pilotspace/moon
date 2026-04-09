use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err_wrong_args, extract_bytes};

/// HGET key field
///
/// Returns the value associated with field in the hash at key, or Null.
pub fn hget(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGET"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HGET"),
    };
    match db.get_hash(key) {
        Ok(Some(map)) => match map.get(field) {
            Some(v) => Frame::BulkString(v.clone()),
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// HMGET key field [field ...]
///
/// Returns values for multiple fields. Null for missing fields.
pub fn hmget(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HMGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMGET"),
    };
    let map_opt = match db.get_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut results = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let field = match extract_bytes(arg) {
            Some(f) => f,
            None => {
                results.push(Frame::Null);
                continue;
            }
        };
        match &map_opt {
            Some(map) => match map.get(field) {
                Some(v) => results.push(Frame::BulkString(v.clone())),
                None => results.push(Frame::Null),
            },
            None => results.push(Frame::Null),
        }
    }
    Frame::Array(results.into())
}

/// HGETALL key
///
/// Returns all field-value pairs as alternating elements in an array.
pub fn hgetall(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HGETALL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGETALL"),
    };
    match db.get_hash(key) {
        Ok(Some(map)) => {
            let mut result = Vec::with_capacity(map.len() * 2);
            for (field, value) in map {
                result.push(Frame::BulkString(field.clone()));
                result.push(Frame::BulkString(value.clone()));
            }
            Frame::Array(result.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HEXISTS key field
///
/// Returns 1 if field exists in hash, 0 otherwise.
pub fn hexists(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HEXISTS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HEXISTS"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HEXISTS"),
    };
    match db.get_hash(key) {
        Ok(Some(map)) => {
            if map.contains_key(field) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HLEN key
///
/// Returns the number of fields in the hash, or 0 if key missing.
pub fn hlen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HLEN"),
    };
    match db.get_hash(key) {
        Ok(Some(map)) => Frame::Integer(map.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HKEYS key
///
/// Returns all field names in the hash.
pub fn hkeys(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HKEYS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HKEYS"),
    };
    match db.get_hash(key) {
        Ok(Some(map)) => {
            let fields: Vec<Frame> = map.keys().map(|k| Frame::BulkString(k.clone())).collect();
            Frame::Array(fields.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HVALS key
///
/// Returns all values in the hash.
pub fn hvals(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HVALS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HVALS"),
    };
    match db.get_hash(key) {
        Ok(Some(map)) => {
            let values: Vec<Frame> = map.values().map(|v| Frame::BulkString(v.clone())).collect();
            Frame::Array(values.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Incrementally iterates hash fields using a cursor.
pub fn hscan(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HSCAN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSCAN"),
    };

    // Parse cursor
    let cursor: usize = match extract_bytes(&args[1]) {
        Some(c) => match std::str::from_utf8(c).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
        },
        None => return err_wrong_args("HSCAN"),
    };

    // Parse optional MATCH and COUNT
    let mut match_pattern: Option<&[u8]> = None;
    let mut count: usize = 10;

    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            i += 1;
            if i < args.len() {
                match_pattern = extract_bytes(&args[i]).map(|b| b.as_ref());
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i < args.len() {
                if let Some(v) = extract_bytes(&args[i]) {
                    if let Some(c) = std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        if c > 0 {
                            count = c;
                        }
                    }
                }
            }
        }
        i += 1;
    }

    // Get hash fields (read-only first to collect field names)
    let map = match db.get_hash(key) {
        Ok(Some(m)) => m,
        Ok(None) => {
            // Key missing -- return cursor 0 with empty array
            return Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(framevec![]),
            ]);
        }
        Err(e) => return e,
    };

    // Sort fields for deterministic iteration
    let mut fields: Vec<(&Bytes, &Bytes)> = map.iter().collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));

    let total = fields.len();
    let mut results = Vec::with_capacity(count * 2);
    let mut pos = cursor;
    let mut checked = 0;

    while pos < total && checked < count {
        let (field, value) = fields[pos];
        pos += 1;
        checked += 1;

        // MATCH filter on field name
        if let Some(pattern) = match_pattern {
            if !crate::command::key::glob_match(pattern, field) {
                continue;
            }
        }

        results.push(Frame::BulkString(field.clone()));
        results.push(Frame::BulkString(value.clone()));
    }

    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        let mut ibuf = itoa::Buffer::new();
        Bytes::copy_from_slice(ibuf.format(pos).as_bytes())
    };

    Frame::Array(framevec![
        Frame::BulkString(next_cursor),
        Frame::Array(results.into()),
    ])
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// HGET (read-only).
pub fn hget_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGET"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HGET"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => match href.get_field(field) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// HMGET (read-only).
pub fn hmget_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HMGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMGET"),
    };
    let href_opt = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut results = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let field = match extract_bytes(arg) {
            Some(f) => f,
            None => {
                results.push(Frame::Null);
                continue;
            }
        };
        match &href_opt {
            Some(href) => match href.get_field(field) {
                Some(v) => results.push(Frame::BulkString(v)),
                None => results.push(Frame::Null),
            },
            None => results.push(Frame::Null),
        }
    }
    Frame::Array(results.into())
}

/// HGETALL (read-only).
pub fn hgetall_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HGETALL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGETALL"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let entries = href.entries();
            let mut result = Vec::with_capacity(entries.len() * 2);
            for (field, value) in entries {
                result.push(Frame::BulkString(field));
                result.push(Frame::BulkString(value));
            }
            Frame::Array(result.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HLEN (read-only).
pub fn hlen_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HLEN"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => Frame::Integer(href.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HKEYS (read-only).
pub fn hkeys_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HKEYS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HKEYS"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let fields: Vec<Frame> = href
                .entries()
                .into_iter()
                .map(|(k, _)| Frame::BulkString(k))
                .collect();
            Frame::Array(fields.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HVALS (read-only).
pub fn hvals_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HVALS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HVALS"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let values: Vec<Frame> = href
                .entries()
                .into_iter()
                .map(|(_, v)| Frame::BulkString(v))
                .collect();
            Frame::Array(values.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HEXISTS (read-only).
pub fn hexists_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HEXISTS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HEXISTS"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HEXISTS"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            if href.get_field(field).is_some() {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HSCAN (read-only).
pub fn hscan_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HSCAN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSCAN"),
    };
    let cursor: usize = match extract_bytes(&args[1]) {
        Some(c) => match std::str::from_utf8(c).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
        },
        None => return err_wrong_args("HSCAN"),
    };
    let mut match_pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            i += 1;
            if i < args.len() {
                match_pattern = extract_bytes(&args[i]).map(|b| b.as_ref());
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i < args.len() {
                if let Some(v) = extract_bytes(&args[i]) {
                    if let Some(c) = std::str::from_utf8(v)
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                    {
                        if c > 0 {
                            count = c;
                        }
                    }
                }
            }
        }
        i += 1;
    }
    let entries = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let mut e = href.entries();
            e.sort_by(|a, b| a.0.cmp(&b.0));
            e
        }
        Ok(None) => {
            return Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(framevec![]),
            ]);
        }
        Err(e) => return e,
    };
    let total = entries.len();
    let mut results = Vec::new();
    let mut pos = cursor;
    let mut checked = 0;
    while pos < total && checked < count {
        let (ref field, ref value) = entries[pos];
        pos += 1;
        checked += 1;
        if let Some(pattern) = match_pattern {
            if !crate::command::key::glob_match(pattern, field) {
                continue;
            }
        }
        results.push(Frame::BulkString(field.clone()));
        results.push(Frame::BulkString(value.clone()));
    }
    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        let mut ibuf = itoa::Buffer::new();
        Bytes::copy_from_slice(ibuf.format(pos).as_bytes())
    };
    Frame::Array(framevec![
        Frame::BulkString(next_cursor),
        Frame::Array(results.into()),
    ])
}
