use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

/// Helper: return ERR wrong number of arguments for a given command.
fn err_wrong_args(cmd: &str) -> Frame {
    Frame::Error(Bytes::from(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd
    )))
}

/// Helper: extract &Bytes from a BulkString or SimpleString frame.
fn extract_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// Helper: OK response.
fn ok() -> Frame {
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// HSET key field value [field value ...]
///
/// Sets field-value pairs in the hash stored at key. Returns the number
/// of fields that were newly added (not updated).
pub fn hset(db: &mut Database, args: &[Frame]) -> Frame {
    // Need at least key + one field-value pair, and args count must be odd (key + pairs)
    if args.len() < 3 || args.len() % 2 == 0 {
        return err_wrong_args("HSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSET"),
    };
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut new_count: i64 = 0;
    let mut i = 1;
    while i < args.len() {
        let field = match extract_bytes(&args[i]) {
            Some(f) => f.clone(),
            None => return err_wrong_args("HSET"),
        };
        let value = match extract_bytes(&args[i + 1]) {
            Some(v) => v.clone(),
            None => return err_wrong_args("HSET"),
        };
        if map.insert(field, value).is_none() {
            new_count += 1;
        }
        i += 2;
    }
    Frame::Integer(new_count)
}

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

/// HDEL key field [field ...]
///
/// Removes fields from the hash. Returns the number of fields removed.
/// If the hash becomes empty, the key is removed entirely.
pub fn hdel(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HDEL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("HDEL"),
    };
    let map = match db.get_or_create_hash(&key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut count: i64 = 0;
    for arg in &args[1..] {
        if let Some(field) = extract_bytes(arg) {
            if map.remove(field).is_some() {
                count += 1;
            }
        }
    }
    // Remove key if hash is now empty
    if map.is_empty() {
        db.remove(&key);
    }
    Frame::Integer(count)
}

/// HMSET key field value [field value ...]
///
/// Sets multiple field-value pairs. Legacy command, always returns OK.
pub fn hmset(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 || args.len() % 2 == 0 {
        return err_wrong_args("HMSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMSET"),
    };
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut i = 1;
    while i < args.len() {
        let field = match extract_bytes(&args[i]) {
            Some(f) => f.clone(),
            None => return err_wrong_args("HMSET"),
        };
        let value = match extract_bytes(&args[i + 1]) {
            Some(v) => v.clone(),
            None => return err_wrong_args("HMSET"),
        };
        map.insert(field, value);
        i += 2;
    }
    ok()
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
    Frame::Array(results)
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
            Frame::Array(result)
        }
        Ok(None) => Frame::Array(vec![]),
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
            let fields: Vec<Frame> = map
                .keys()
                .map(|k| Frame::BulkString(k.clone()))
                .collect();
            Frame::Array(fields)
        }
        Ok(None) => Frame::Array(vec![]),
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
            let values: Vec<Frame> = map
                .values()
                .map(|v| Frame::BulkString(v.clone()))
                .collect();
            Frame::Array(values)
        }
        Ok(None) => Frame::Array(vec![]),
        Err(e) => e,
    }
}

/// HINCRBY key field increment
///
/// Increments the integer value of a hash field by the given number.
pub fn hincrby(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("HINCRBY");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HINCRBY"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f.clone(),
        None => return err_wrong_args("HINCRBY"),
    };
    let increment: i64 = match extract_bytes(&args[2]) {
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            }
        },
        None => return err_wrong_args("HINCRBY"),
    };
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let current = match map.get(&field) {
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => n,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR hash value is not an integer",
                ))
            }
        },
        None => 0,
    };
    let new_value = current + increment;
    map.insert(field, Bytes::from(new_value.to_string()));
    Frame::Integer(new_value)
}

/// HINCRBYFLOAT key field increment
///
/// Increments the float value of a hash field by the given amount.
/// Returns the new value as a bulk string.
pub fn hincrbyfloat(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("HINCRBYFLOAT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HINCRBYFLOAT"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f.clone(),
        None => return err_wrong_args("HINCRBYFLOAT"),
    };
    let increment: f64 = match extract_bytes(&args[2]) {
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not a valid float",
                ))
            }
        },
        None => return err_wrong_args("HINCRBYFLOAT"),
    };
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let current: f64 = match map.get(&field) {
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR hash value is not a valid float",
                ))
            }
        },
        None => 0.0,
    };
    let new_value = current + increment;
    // Format like Redis: integer-like floats get no decimal, otherwise trim trailing zeros
    let formatted = format_float(new_value);
    map.insert(field, Bytes::from(formatted.clone()));
    Frame::BulkString(Bytes::from(formatted))
}

/// Format a float value in Redis style.
/// If the value is an exact integer, format without decimal point.
/// Otherwise, format with necessary precision, trimming trailing zeros.
fn format_float(v: f64) -> String {
    if v == v.floor() && v.is_finite() {
        // Check if it fits in i64 range for clean integer formatting
        if v >= i64::MIN as f64 && v <= i64::MAX as f64 {
            return format!("{}", v as i64);
        }
    }
    // Use enough precision and trim trailing zeros
    let s = format!("{:.17}", v);
    let s = s.trim_end_matches('0');
    // Don't leave trailing dot
    let s = s.trim_end_matches('.');
    s.to_string()
}

/// HSETNX key field value
///
/// Sets field only if it does not already exist. Returns 1 if set, 0 if not.
pub fn hsetnx(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("HSETNX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSETNX"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f.clone(),
        None => return err_wrong_args("HSETNX"),
    };
    let value = match extract_bytes(&args[2]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("HSETNX"),
    };
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    if map.contains_key(&field) {
        Frame::Integer(0)
    } else {
        map.insert(field, value);
        Frame::Integer(1)
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
            None => {
                return Frame::Error(Bytes::from_static(b"ERR invalid cursor"))
            }
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
            return Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(vec![]),
            ]);
        }
        Err(e) => return e,
    };

    // Sort fields for deterministic iteration
    let mut fields: Vec<(&Bytes, &Bytes)> = map.iter().collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));

    let total = fields.len();
    let mut results = Vec::new();
    let mut pos = cursor;
    let mut checked = 0;

    while pos < total && checked < count {
        let (field, value) = fields[pos];
        pos += 1;
        checked += 1;

        // MATCH filter on field name
        if let Some(pattern) = match_pattern {
            if !super::key::glob_match(pattern, field) {
                continue;
            }
        }

        results.push(Frame::BulkString(field.clone()));
        results.push(Frame::BulkString(value.clone()));
    }

    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        Bytes::from(pos.to_string())
    };

    Frame::Array(vec![
        Frame::BulkString(next_cursor),
        Frame::Array(results),
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
    match db.get_hash_if_alive(key, now_ms) {
        Ok(Some(map)) => match map.get(field) {
            Some(v) => Frame::BulkString(v.clone()),
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
    let map_opt = match db.get_hash_if_alive(key, now_ms) {
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
    Frame::Array(results)
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
    match db.get_hash_if_alive(key, now_ms) {
        Ok(Some(map)) => {
            let mut result = Vec::with_capacity(map.len() * 2);
            for (field, value) in map {
                result.push(Frame::BulkString(field.clone()));
                result.push(Frame::BulkString(value.clone()));
            }
            Frame::Array(result)
        }
        Ok(None) => Frame::Array(vec![]),
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
    match db.get_hash_if_alive(key, now_ms) {
        Ok(Some(map)) => Frame::Integer(map.len() as i64),
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
    match db.get_hash_if_alive(key, now_ms) {
        Ok(Some(map)) => {
            let fields: Vec<Frame> = map.keys().map(|k| Frame::BulkString(k.clone())).collect();
            Frame::Array(fields)
        }
        Ok(None) => Frame::Array(vec![]),
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
    match db.get_hash_if_alive(key, now_ms) {
        Ok(Some(map)) => {
            let values: Vec<Frame> = map.values().map(|v| Frame::BulkString(v.clone())).collect();
            Frame::Array(values)
        }
        Ok(None) => Frame::Array(vec![]),
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
    match db.get_hash_if_alive(key, now_ms) {
        Ok(Some(map)) => {
            if map.contains_key(field) { Frame::Integer(1) } else { Frame::Integer(0) }
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
            None => { i += 1; continue; }
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
                    if let Some(c) = std::str::from_utf8(v).ok().and_then(|s| s.parse::<usize>().ok()) {
                        if c > 0 { count = c; }
                    }
                }
            }
        }
        i += 1;
    }
    let map = match db.get_hash_if_alive(key, now_ms) {
        Ok(Some(m)) => m,
        Ok(None) => {
            return Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(vec![]),
            ]);
        }
        Err(e) => return e,
    };
    let mut fields: Vec<(&Bytes, &Bytes)> = map.iter().collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));
    let total = fields.len();
    let mut results = Vec::new();
    let mut pos = cursor;
    let mut checked = 0;
    while pos < total && checked < count {
        let (field, value) = fields[pos];
        pos += 1;
        checked += 1;
        if let Some(pattern) = match_pattern {
            if !super::key::glob_match(pattern, field) {
                continue;
            }
        }
        results.push(Frame::BulkString(field.clone()));
        results.push(Frame::BulkString(value.clone()));
    }
    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        Bytes::from(pos.to_string())
    };
    Frame::Array(vec![
        Frame::BulkString(next_cursor),
        Frame::Array(results),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

    fn b(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn make_args(parts: &[&[u8]]) -> Vec<Frame> {
        parts.iter().map(|p| b(p)).collect()
    }

    #[test]
    fn test_hset_new_fields() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1", b"f2", b"v2"]);
        let result = hset(&mut db, &args);
        assert_eq!(result, Frame::Integer(2));
    }

    #[test]
    fn test_hset_update_existing() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1"]);
        hset(&mut db, &args);

        // Update f1, add f2 -- should return 1 (only f2 is new)
        let args = make_args(&[b"myhash", b"f1", b"v1_new", b"f2", b"v2"]);
        let result = hset(&mut db, &args);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_hset_wrong_args() {
        let mut db = Database::new();
        // No field-value pair
        let args = make_args(&[b"myhash"]);
        let result = hset(&mut db, &args);
        assert!(matches!(result, Frame::Error(_)));

        // Even number of args (key + field without value)
        let args = make_args(&[b"myhash", b"f1"]);
        let result = hset(&mut db, &args);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_hget_existing() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash", b"f1"]);
        let result = hget(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"v1")));
    }

    #[test]
    fn test_hget_missing_field() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash", b"nope"]);
        let result = hget(&mut db, &args);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_hget_missing_key() {
        let mut db = Database::new();
        let args = make_args(&[b"nokey", b"f1"]);
        let result = hget(&mut db, &args);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_hdel_fields() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash", b"f1", b"f2", b"nonexistent"]);
        let result = hdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(2));

        // Check f3 still exists
        let args = make_args(&[b"myhash", b"f3"]);
        let result = hget(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"v3")));
    }

    #[test]
    fn test_hdel_removes_empty_hash() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash", b"f1"]);
        hdel(&mut db, &args);

        // Key should be gone
        assert!(!db.exists(b"myhash"));
    }

    #[test]
    fn test_hmset_hmget() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1", b"f2", b"v2"]);
        let result = hmset(&mut db, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        let args = make_args(&[b"myhash", b"f1", b"f2", b"f3"]);
        let result = hmget(&mut db, &args);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"v1")));
                assert_eq!(arr[1], Frame::BulkString(Bytes::from_static(b"v2")));
                assert_eq!(arr[2], Frame::Null);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_hmget_missing_key() {
        let mut db = Database::new();
        let args = make_args(&[b"nokey", b"f1", b"f2"]);
        let result = hmget(&mut db, &args);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], Frame::Null);
                assert_eq!(arr[1], Frame::Null);
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_hgetall() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1", b"f2", b"v2"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash"]);
        let result = hgetall(&mut db, &args);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 4); // 2 field-value pairs
                // Check that both f1->v1 and f2->v2 are present (order may vary)
                let mut pairs: Vec<(Bytes, Bytes)> = Vec::new();
                for chunk in arr.chunks(2) {
                    if let (Frame::BulkString(k), Frame::BulkString(v)) = (&chunk[0], &chunk[1]) {
                        pairs.push((k.clone(), v.clone()));
                    }
                }
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                assert_eq!(pairs[0].0.as_ref(), b"f1");
                assert_eq!(pairs[0].1.as_ref(), b"v1");
                assert_eq!(pairs[1].0.as_ref(), b"f2");
                assert_eq!(pairs[1].1.as_ref(), b"v2");
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_hgetall_missing() {
        let mut db = Database::new();
        let args = make_args(&[b"nokey"]);
        let result = hgetall(&mut db, &args);
        assert_eq!(result, Frame::Array(vec![]));
    }

    #[test]
    fn test_hexists() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash", b"f1"]);
        assert_eq!(hexists(&mut db, &args), Frame::Integer(1));

        let args = make_args(&[b"myhash", b"nope"]);
        assert_eq!(hexists(&mut db, &args), Frame::Integer(0));

        let args = make_args(&[b"nokey", b"f1"]);
        assert_eq!(hexists(&mut db, &args), Frame::Integer(0));
    }

    #[test]
    fn test_hlen() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1", b"f2", b"v2"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash"]);
        assert_eq!(hlen(&mut db, &args), Frame::Integer(2));

        let args = make_args(&[b"nokey"]);
        assert_eq!(hlen(&mut db, &args), Frame::Integer(0));
    }

    #[test]
    fn test_hkeys_hvals() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"v1", b"f2", b"v2"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash"]);
        let keys_result = hkeys(&mut db, &args);
        match keys_result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                let mut keys: Vec<Bytes> = arr
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.clone(),
                        _ => panic!("Expected BulkString"),
                    })
                    .collect();
                keys.sort();
                assert_eq!(keys[0].as_ref(), b"f1");
                assert_eq!(keys[1].as_ref(), b"f2");
            }
            _ => panic!("Expected Array"),
        }

        let vals_result = hvals(&mut db, &args);
        match vals_result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                let mut vals: Vec<Bytes> = arr
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.clone(),
                        _ => panic!("Expected BulkString"),
                    })
                    .collect();
                vals.sort();
                assert_eq!(vals[0].as_ref(), b"v1");
                assert_eq!(vals[1].as_ref(), b"v2");
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_hkeys_hvals_missing() {
        let mut db = Database::new();
        let args = make_args(&[b"nokey"]);
        assert_eq!(hkeys(&mut db, &args), Frame::Array(vec![]));
        assert_eq!(hvals(&mut db, &args), Frame::Array(vec![]));
    }

    #[test]
    fn test_hincrby() {
        let mut db = Database::new();
        // Increment non-existing field (defaults to 0)
        let args = make_args(&[b"myhash", b"counter", b"5"]);
        let result = hincrby(&mut db, &args);
        assert_eq!(result, Frame::Integer(5));

        // Increment again
        let args = make_args(&[b"myhash", b"counter", b"3"]);
        let result = hincrby(&mut db, &args);
        assert_eq!(result, Frame::Integer(8));

        // Negative increment
        let args = make_args(&[b"myhash", b"counter", b"-2"]);
        let result = hincrby(&mut db, &args);
        assert_eq!(result, Frame::Integer(6));
    }

    #[test]
    fn test_hincrby_non_integer_field() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"notanumber"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash", b"f1", b"1"]);
        let result = hincrby(&mut db, &args);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_hincrbyfloat() {
        let mut db = Database::new();
        // Increment non-existing field
        let args = make_args(&[b"myhash", b"price", b"10.5"]);
        let result = hincrbyfloat(&mut db, &args);
        assert_eq!(
            result,
            Frame::BulkString(Bytes::from("10.5"))
        );

        // Increment again
        let args = make_args(&[b"myhash", b"price", b"0.5"]);
        let result = hincrbyfloat(&mut db, &args);
        assert_eq!(
            result,
            Frame::BulkString(Bytes::from("11"))
        );
    }

    #[test]
    fn test_hincrbyfloat_non_float_field() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"f1", b"notafloat"]);
        hset(&mut db, &args);

        let args = make_args(&[b"myhash", b"f1", b"1.0"]);
        let result = hincrbyfloat(&mut db, &args);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_hsetnx() {
        let mut db = Database::new();
        // Set new field
        let args = make_args(&[b"myhash", b"f1", b"v1"]);
        let result = hsetnx(&mut db, &args);
        assert_eq!(result, Frame::Integer(1));

        // Try to set same field again -- should not overwrite
        let args = make_args(&[b"myhash", b"f1", b"v2"]);
        let result = hsetnx(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));

        // Verify original value is preserved
        let args = make_args(&[b"myhash", b"f1"]);
        let result = hget(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"v1")));
    }

    #[test]
    fn test_hscan_basic() {
        let mut db = Database::new();
        let args = make_args(&[b"myhash", b"a", b"1", b"b", b"2", b"c", b"3"]);
        hset(&mut db, &args);

        // Scan from cursor 0
        let args = make_args(&[b"myhash", b"0"]);
        let result = hscan(&mut db, &args);
        match result {
            Frame::Array(outer) => {
                assert_eq!(outer.len(), 2);
                // Cursor should be "0" since all fields fit
                assert_eq!(outer[0], Frame::BulkString(Bytes::from_static(b"0")));
                match &outer[1] {
                    Frame::Array(inner) => {
                        // 3 fields * 2 (field + value) = 6 elements
                        assert_eq!(inner.len(), 6);
                    }
                    _ => panic!("Expected inner Array"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_hscan_with_count() {
        let mut db = Database::new();
        // Add more fields than default count
        let args = make_args(&[
            b"myhash", b"a", b"1", b"b", b"2", b"c", b"3", b"d", b"4", b"e", b"5",
        ]);
        hset(&mut db, &args);

        // Scan with COUNT 2
        let args = make_args(&[b"myhash", b"0", b"COUNT", b"2"]);
        let result = hscan(&mut db, &args);
        match result {
            Frame::Array(outer) => {
                assert_eq!(outer.len(), 2);
                // Cursor should NOT be "0" since not all fields were scanned
                assert_ne!(outer[0], Frame::BulkString(Bytes::from_static(b"0")));
                match &outer[1] {
                    Frame::Array(inner) => {
                        // At most 2 fields * 2 = 4 elements
                        assert!(inner.len() <= 4);
                    }
                    _ => panic!("Expected inner Array"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_hscan_with_match() {
        let mut db = Database::new();
        let args = make_args(&[
            b"myhash", b"name", b"alice", b"age", b"30", b"nickname", b"ali",
        ]);
        hset(&mut db, &args);

        // Scan with MATCH "n*"
        let args = make_args(&[b"myhash", b"0", b"MATCH", b"n*"]);
        let result = hscan(&mut db, &args);
        match result {
            Frame::Array(outer) => {
                assert_eq!(outer.len(), 2);
                match &outer[1] {
                    Frame::Array(inner) => {
                        // Should match "name" and "nickname" but not "age"
                        // Each match = 2 elements (field + value)
                        assert_eq!(inner.len(), 4);
                        // All field names should start with 'n'
                        for chunk in inner.chunks(2) {
                            if let Frame::BulkString(field) = &chunk[0] {
                                assert!(field[0] == b'n');
                            }
                        }
                    }
                    _ => panic!("Expected inner Array"),
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_hscan_missing_key() {
        let mut db = Database::new();
        let args = make_args(&[b"nokey", b"0"]);
        let result = hscan(&mut db, &args);
        match result {
            Frame::Array(outer) => {
                assert_eq!(outer.len(), 2);
                assert_eq!(outer[0], Frame::BulkString(Bytes::from_static(b"0")));
                assert_eq!(outer[1], Frame::Array(vec![]));
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_wrongtype_error() {
        let mut db = Database::new();
        // Set a string key
        db.set_string(
            Bytes::from_static(b"mystring"),
            Bytes::from_static(b"hello"),
        );

        // Try hash operations on string key
        let args = make_args(&[b"mystring", b"f1", b"v1"]);
        let result = hset(&mut db, &args);
        assert!(matches!(result, Frame::Error(ref e) if e.starts_with(b"WRONGTYPE")));

        let args = make_args(&[b"mystring", b"f1"]);
        let result = hget(&mut db, &args);
        assert!(matches!(result, Frame::Error(ref e) if e.starts_with(b"WRONGTYPE")));

        let args = make_args(&[b"mystring"]);
        let result = hgetall(&mut db, &args);
        assert!(matches!(result, Frame::Error(ref e) if e.starts_with(b"WRONGTYPE")));
    }
}
