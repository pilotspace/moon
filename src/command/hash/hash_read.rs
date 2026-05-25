use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err_wrong_args, extract_bytes};

/// HGET key field
///
/// Returns the value associated with field in the hash at key, or Null.
/// Skips fields whose per-field TTL has expired (lazy expiry via `HashRef::WithTtl`).
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
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => match href.get_field(field) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// HMGET key field [field ...]
///
/// Returns values for multiple fields. Null for missing or expired fields.
pub fn hmget(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HMGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMGET"),
    };
    let now_ms = db.now_ms();
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

/// HGETALL key
///
/// Returns all live field-value pairs as alternating elements in an array.
/// Expired fields are omitted.
pub fn hgetall(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HGETALL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGETALL"),
    };
    let now_ms = db.now_ms();
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

/// HEXISTS key field
///
/// Returns 1 if field exists and has not expired, 0 otherwise.
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
    let now_ms = db.now_ms();
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

/// HLEN key
///
/// Returns the number of live fields in the hash, or 0 if key missing.
///
/// O(1) for plain `Hash` and `HashListpack`.
/// O(N) for `HashWithTtl` (filters expired fields on each call).
pub fn hlen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HLEN"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => Frame::Integer(href.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HKEYS key
///
/// Returns all live field names in the hash. Expired fields are omitted.
pub fn hkeys(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HKEYS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HKEYS"),
    };
    let now_ms = db.now_ms();
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

/// HVALS key
///
/// Returns all live values in the hash. Values of expired fields are omitted.
pub fn hvals(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HVALS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HVALS"),
    };
    let now_ms = db.now_ms();
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

/// HSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Incrementally iterates hash fields using a cursor. Expired fields are omitted.
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

    // Collect live (field, value) pairs; HashRef::entries() filters expired fields.
    let now_ms = db.now_ms();
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
    let mut results = Vec::with_capacity(count * 2);
    let mut pos = cursor;
    let mut checked = 0;

    while pos < total && checked < count {
        let (ref field, ref value) = entries[pos];
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

// ---------------------------------------------------------------------------
// HRANDFIELD key [count [WITHVALUES]]
// ---------------------------------------------------------------------------

/// HRANDFIELD key [count [WITHVALUES]]
///
/// Returns random fields from the hash. Expired fields are never returned.
pub fn hrandfield(db: &mut Database, args: &[Frame]) -> Frame {
    use rand::seq::IndexedRandom;
    if args.is_empty() || args.len() > 3 {
        return err_wrong_args("HRANDFIELD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HRANDFIELD"),
    };
    // Collect only live (non-expired) entries via HashRef::entries().
    let now_ms = db.now_ms();
    let entries = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => href.entries(),
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };
    if entries.is_empty() {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(framevec![])
        };
    }
    let mut rng = rand::rng();
    if args.len() == 1 {
        return if let Some((field, _)) = entries.choose(&mut rng) {
            Frame::BulkString(field.clone())
        } else {
            Frame::Null
        };
    }
    let count_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("HRANDFIELD"),
    };
    let count: i64 = match std::str::from_utf8(count_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let with_values = if args.len() == 3 {
        let opt = match extract_bytes(&args[2]) {
            Some(b) => b,
            None => return err_wrong_args("HRANDFIELD"),
        };
        if opt.eq_ignore_ascii_case(b"WITHVALUES") {
            true
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    } else {
        false
    };
    if count == 0 {
        return Frame::Array(framevec![]);
    }
    if count > 0 {
        let n = std::cmp::min(count as usize, entries.len());
        let indices: Vec<usize> = (0..entries.len()).collect();
        let chosen: Vec<usize> = indices.as_slice().sample(&mut rng, n).copied().collect();
        if with_values {
            let mut result = Vec::with_capacity(n * 2);
            for &idx in &chosen {
                result.push(Frame::BulkString(entries[idx].0.clone()));
                result.push(Frame::BulkString(entries[idx].1.clone()));
            }
            Frame::Array(result.into())
        } else {
            let result: Vec<Frame> = chosen
                .iter()
                .map(|&idx| Frame::BulkString(entries[idx].0.clone()))
                .collect();
            Frame::Array(result.into())
        }
    } else {
        // Negative count: allow duplicates. Cap to entries.len() to prevent OOM on i64::MIN.
        let n = std::cmp::min(count.unsigned_abs() as usize, entries.len() * 10);
        if with_values {
            let mut result = Vec::with_capacity(n * 2);
            for _ in 0..n {
                if let Some((field, value)) = entries.choose(&mut rng) {
                    result.push(Frame::BulkString(field.clone()));
                    result.push(Frame::BulkString(value.clone()));
                }
            }
            Frame::Array(result.into())
        } else {
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                if let Some((field, _)) = entries.choose(&mut rng) {
                    result.push(Frame::BulkString(field.clone()));
                }
            }
            Frame::Array(result.into())
        }
    }
}

/// HRANDFIELD readonly path
pub fn hrandfield_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    use rand::seq::IndexedRandom;
    if args.is_empty() || args.len() > 3 {
        return err_wrong_args("HRANDFIELD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HRANDFIELD"),
    };
    let href = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(h)) => h,
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };
    let entries = href.entries();
    if entries.is_empty() {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(framevec![])
        };
    }
    let mut rng = rand::rng();
    if args.len() == 1 {
        return if let Some((field, _)) = entries.choose(&mut rng) {
            Frame::BulkString(field.clone())
        } else {
            Frame::Null
        };
    }
    let count_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("HRANDFIELD"),
    };
    let count: i64 = match std::str::from_utf8(count_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let with_values = if args.len() == 3 {
        let opt = match extract_bytes(&args[2]) {
            Some(b) => b,
            None => return err_wrong_args("HRANDFIELD"),
        };
        if opt.eq_ignore_ascii_case(b"WITHVALUES") {
            true
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    } else {
        false
    };
    if count == 0 {
        return Frame::Array(framevec![]);
    }
    if count > 0 {
        let n = std::cmp::min(count as usize, entries.len());
        let indices: Vec<usize> = (0..entries.len()).collect();
        let chosen: Vec<usize> = indices.as_slice().sample(&mut rng, n).copied().collect();
        if with_values {
            let mut result = Vec::with_capacity(n * 2);
            for &idx in &chosen {
                result.push(Frame::BulkString(entries[idx].0.clone()));
                result.push(Frame::BulkString(entries[idx].1.clone()));
            }
            Frame::Array(result.into())
        } else {
            let result: Vec<Frame> = chosen
                .iter()
                .map(|&idx| Frame::BulkString(entries[idx].0.clone()))
                .collect();
            Frame::Array(result.into())
        }
    } else {
        // Negative count: allow duplicates. Cap to prevent OOM on extreme values.
        let n = std::cmp::min(count.unsigned_abs() as usize, entries.len() * 10);
        if with_values {
            let mut result = Vec::with_capacity(n * 2);
            for _ in 0..n {
                if let Some((field, value)) = entries.choose(&mut rng) {
                    result.push(Frame::BulkString(field.clone()));
                    result.push(Frame::BulkString(value.clone()));
                }
            }
            Frame::Array(result.into())
        } else {
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                if let Some((field, _)) = entries.choose(&mut rng) {
                    result.push(Frame::BulkString(field.clone()));
                }
            }
            Frame::Array(result.into())
        }
    }
}
