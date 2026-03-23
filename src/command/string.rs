use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::entry::{current_time_ms, Entry, RedisValue};
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

/// GET command handler.
pub fn get(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("GET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GET"),
    };
    match db.get(key) {
        Some(entry) => match &entry.value {
            RedisValue::String(v) => Frame::BulkString(v.clone()),
            _ => Frame::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        None => Frame::Null,
    }
}

/// SET command handler with EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET options.
pub fn set(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("SET"),
    };
    let value = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("SET"),
    };

    let mut expires_at_ms: u64 = 0;
    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut get_old = false;

    // Parse options from args[2..] using zero-alloc case-insensitive comparison
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
        };
        if opt.eq_ignore_ascii_case(b"EX") {
            i += 1;
            if i >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[i]) {
                Some(secs) => expires_at_ms = current_time_ms() + (secs as u64) * 1000,
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else if opt.eq_ignore_ascii_case(b"PX") {
            i += 1;
            if i >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[i]) {
                Some(ms) => expires_at_ms = current_time_ms() + ms as u64,
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else if opt.eq_ignore_ascii_case(b"EXAT") {
            i += 1;
            if i >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[i]) {
                Some(ts) => {
                    expires_at_ms = (ts as u64) * 1000;
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else if opt.eq_ignore_ascii_case(b"PXAT") {
            i += 1;
            if i >= args.len() {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[i]) {
                Some(ts_ms) => {
                    expires_at_ms = ts_ms as u64;
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else if opt.eq_ignore_ascii_case(b"NX") {
            nx = true;
        } else if opt.eq_ignore_ascii_case(b"XX") {
            xx = true;
        } else if opt.eq_ignore_ascii_case(b"KEEPTTL") {
            keepttl = true;
        } else if opt.eq_ignore_ascii_case(b"GET") {
            get_old = true;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        i += 1;
    }

    // Get old value if needed
    let old_value = if get_old {
        db.get(&key).map(|e| match &e.value {
            RedisValue::String(v) => Frame::BulkString(v.clone()),
            _ => Frame::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        })
    } else {
        None
    };

    // NX + XX both set: contradictory, return nil (or old value if GET)
    if nx && xx {
        return if get_old {
            old_value.unwrap_or(Frame::Null)
        } else {
            Frame::Null
        };
    }

    // NX: only set if not exists
    if nx && db.exists(&key) {
        return if get_old {
            old_value.unwrap_or(Frame::Null)
        } else {
            Frame::Null
        };
    }

    // XX: only set if exists
    if xx && !db.exists(&key) {
        return if get_old {
            Frame::Null
        } else {
            Frame::Null
        };
    }

    // Determine final expiry
    let final_expires_at_ms = if expires_at_ms > 0 {
        expires_at_ms
    } else if keepttl {
        // Preserve existing TTL
        db.get(&key).map(|e| e.expires_at_ms).unwrap_or(0)
    } else {
        0
    };

    let entry = Entry {
        value: RedisValue::String(value),
        expires_at_ms: final_expires_at_ms,
        metadata: 0,
    };
    // Set last_access from db cached time (version will be set by db.set)
    let mut entry = entry;
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key, entry);

    if get_old {
        old_value.unwrap_or(Frame::Null)
    } else {
        ok()
    }
}

/// MGET command handler.
pub fn mget(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("MGET");
    }
    let mut results = Vec::with_capacity(args.len());
    for arg in args {
        let key = match extract_bytes(arg) {
            Some(k) => k,
            None => {
                results.push(Frame::Null);
                continue;
            }
        };
        match db.get(key) {
            Some(entry) => match &entry.value {
                RedisValue::String(v) => results.push(Frame::BulkString(v.clone())),
                _ => results.push(Frame::Null),
            },
            None => results.push(Frame::Null),
        }
    }
    Frame::Array(results)
}

/// MSET command handler.
pub fn mset(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() % 2 != 0 {
        return err_wrong_args("MSET");
    }
    for pair in args.chunks(2) {
        let key = match extract_bytes(&pair[0]) {
            Some(k) => k.clone(),
            None => return err_wrong_args("MSET"),
        };
        let value = match extract_bytes(&pair[1]) {
            Some(v) => v.clone(),
            None => return err_wrong_args("MSET"),
        };
        db.set_string(key, value);
    }
    ok()
}

/// INCR command handler.
pub fn incr(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("INCR");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("INCR"),
    };
    incrby_internal(db, key, 1)
}

/// DECR command handler.
pub fn decr(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DECR");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("DECR"),
    };
    incrby_internal(db, key, -1)
}

/// INCRBY command handler.
pub fn incrby(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("INCRBY");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("INCRBY"),
    };
    let delta = match parse_i64(&args[1]) {
        Some(d) => d,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    incrby_internal(db, key, delta)
}

/// DECRBY command handler.
pub fn decrby(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("DECRBY");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("DECRBY"),
    };
    let delta = match parse_i64(&args[1]) {
        Some(d) => d,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    incrby_internal(db, key, -delta)
}

/// Internal helper for INCR/DECR/INCRBY/DECRBY.
fn incrby_internal(db: &mut Database, key: &Bytes, delta: i64) -> Frame {
    // Get current value and existing expiry
    let (current, existing_expiry_ms) = match db.get(key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms;
            match &entry.value {
                RedisValue::String(v) => {
                    let s = match std::str::from_utf8(v) {
                        Ok(s) => s,
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not an integer or out of range",
                            ))
                        }
                    };
                    match s.parse::<i64>() {
                        Ok(n) => (n, expiry),
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not an integer or out of range",
                            ))
                        }
                    }
                }
                _ => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }
        None => (0, 0),
    };

    let new_val = match current.checked_add(delta) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR increment or decrement would overflow",
            ))
        }
    };

    // Store new value preserving existing TTL
    let mut entry = Entry {
        value: RedisValue::String(Bytes::from(new_val.to_string())),
        expires_at_ms: existing_expiry_ms,
        metadata: 0,
    };
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key.clone(), entry);

    Frame::Integer(new_val)
}

/// INCRBYFLOAT command handler.
pub fn incrbyfloat(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("INCRBYFLOAT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("INCRBYFLOAT"),
    };
    let increment = match parse_f64(&args[1]) {
        Some(f) => f,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not a valid float",
            ))
        }
    };
    if increment.is_nan() {
        return Frame::Error(Bytes::from_static(
            b"ERR increment would produce NaN or Infinity",
        ));
    }

    // Get current value and existing expiry
    let (current, existing_expiry_ms) = match db.get(key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms;
            match &entry.value {
                RedisValue::String(v) => {
                    let s = match std::str::from_utf8(v) {
                        Ok(s) => s,
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not a valid float",
                            ))
                        }
                    };
                    match s.parse::<f64>() {
                        Ok(f) => (f, expiry),
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not a valid float",
                            ))
                        }
                    }
                }
                _ => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }
        None => (0.0, 0),
    };

    let result = current + increment;
    if result.is_nan() || result.is_infinite() {
        return Frame::Error(Bytes::from_static(
            b"ERR increment would produce NaN or Infinity",
        ));
    }

    let formatted = format_float(result);

    let mut entry = Entry {
        value: RedisValue::String(Bytes::from(formatted.clone())),
        expires_at_ms: existing_expiry_ms,
        metadata: 0,
    };
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key.clone(), entry);

    Frame::BulkString(Bytes::from(formatted))
}

/// APPEND command handler.
pub fn append(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("APPEND");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("APPEND"),
    };
    let append_val = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("APPEND"),
    };

    // Check if key exists, get existing data + expiry
    let (existing_data, existing_expiry_ms) = match db.get(&key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms;
            match &entry.value {
                RedisValue::String(v) => (Some(v.clone()), expiry),
                _ => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ))
                }
            }
        }
        None => (None, 0),
    };

    let new_val = match existing_data {
        Some(existing) => {
            let mut combined = Vec::with_capacity(existing.len() + append_val.len());
            combined.extend_from_slice(&existing);
            combined.extend_from_slice(&append_val);
            Bytes::from(combined)
        }
        None => append_val,
    };

    let new_len = new_val.len() as i64;
    let mut entry = Entry {
        value: RedisValue::String(new_val),
        expires_at_ms: existing_expiry_ms,
        metadata: 0,
    };
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key, entry);

    Frame::Integer(new_len)
}

/// STRLEN command handler.
pub fn strlen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("STRLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("STRLEN"),
    };
    match db.get(key) {
        Some(entry) => match &entry.value {
            RedisValue::String(v) => Frame::Integer(v.len() as i64),
            _ => Frame::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        None => Frame::Integer(0),
    }
}

/// SETNX command handler (legacy wrapper).
pub fn setnx(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("SETNX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("SETNX"),
    };
    let value = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("SETNX"),
    };
    if db.exists(&key) {
        Frame::Integer(0)
    } else {
        db.set_string(key, value);
        Frame::Integer(1)
    }
}

/// SETEX command handler (legacy wrapper).
/// Args: key seconds value
pub fn setex(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("SETEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("SETEX"),
    };
    let seconds = match parse_i64(&args[1]) {
        Some(s) => s,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    if seconds <= 0 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'SETEX' command",
        ));
    }
    let value = match extract_bytes(&args[2]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("SETEX"),
    };
    db.set_string_with_expiry(key, value, current_time_ms() + (seconds as u64) * 1000);
    ok()
}

/// PSETEX command handler (legacy wrapper).
/// Args: key milliseconds value
pub fn psetex(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("PSETEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("PSETEX"),
    };
    let millis = match parse_i64(&args[1]) {
        Some(m) => m,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    if millis <= 0 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'PSETEX' command",
        ));
    }
    let value = match extract_bytes(&args[2]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("PSETEX"),
    };
    db.set_string_with_expiry(key, value, current_time_ms() + millis as u64);
    ok()
}

/// GETSET command handler (legacy).
pub fn getset(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("GETSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("GETSET"),
    };
    let value = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("GETSET"),
    };

    let old = db.get(&key).map(|e| match &e.value {
        RedisValue::String(v) => Frame::BulkString(v.clone()),
        _ => Frame::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
    });

    // GETSET removes TTL (sets new entry without expiry)
    db.set_string(key, value);

    old.unwrap_or(Frame::Null)
}

/// GETDEL command handler.
pub fn getdel(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("GETDEL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GETDEL"),
    };
    match db.remove(key) {
        Some(entry) => match entry.value {
            RedisValue::String(v) => Frame::BulkString(v),
            _ => Frame::Error(Bytes::from_static(b"WRONGTYPE Operation against a key holding the wrong kind of value")),
        },
        None => Frame::Null,
    }
}

/// GETEX command handler.
pub fn getex(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("GETEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("GETEX"),
    };

    // First, get value (returns None if key doesn't exist or expired)
    let value = match db.get(&key) {
        Some(entry) => match &entry.value {
            RedisValue::String(v) => v.clone(),
            _ => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ))
            }
        },
        None => return Frame::Null,
    };

    // Parse options using zero-alloc case-insensitive comparison
    if args.len() > 1 {
        let opt = match extract_bytes(&args[1]) {
            Some(b) => b.as_ref(),
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        if opt.eq_ignore_ascii_case(b"PERSIST") {
            if let Some(entry) = db.get_mut(&key) {
                entry.expires_at_ms = 0;
            }
        } else if opt.eq_ignore_ascii_case(b"EX") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(secs) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.expires_at_ms = current_time_ms() + (secs as u64) * 1000;
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else if opt.eq_ignore_ascii_case(b"PX") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(ms) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.expires_at_ms = current_time_ms() + ms as u64;
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else if opt.eq_ignore_ascii_case(b"EXAT") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(ts) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.expires_at_ms = (ts as u64) * 1000;
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else if opt.eq_ignore_ascii_case(b"PXAT") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(ts_ms) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.expires_at_ms = ts_ms as u64;
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ))
                }
            }
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    Frame::BulkString(value)
}

// --- Internal helpers ---

/// Parse a Frame argument as i64.
fn parse_i64(frame: &Frame) -> Option<i64> {
    let b = extract_bytes(frame)?;
    let s = std::str::from_utf8(b).ok()?;
    s.parse::<i64>().ok()
}

/// Parse a Frame argument as positive i64 (> 0).
fn parse_positive_i64(frame: &Frame) -> Option<i64> {
    let v = parse_i64(frame)?;
    if v > 0 {
        Some(v)
    } else {
        None
    }
}

/// Parse a Frame argument as f64.
fn parse_f64(frame: &Frame) -> Option<f64> {
    let b = extract_bytes(frame)?;
    let s = std::str::from_utf8(b).ok()?;
    s.parse::<f64>().ok()
}

/// Format a float result, stripping trailing zeros after decimal point.
fn format_float(val: f64) -> String {
    let s = format!("{}", val);
    if s.contains('.') {
        let trimmed = s.trim_end_matches('0');
        let trimmed = trimmed.trim_end_matches('.');
        trimmed.to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::current_time_ms;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn make_db() -> Database {
        Database::new()
    }

    // --- GET/SET tests ---

    #[test]
    fn test_get_set_roundtrip() {
        let mut db = make_db();
        let result = set(&mut db, &[bs(b"key"), bs(b"value")]);
        assert_eq!(result, ok());
        let result = get(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"value")));
    }

    #[test]
    fn test_get_wrongtype_on_hash() {
        let mut db = make_db();
        db.set(Bytes::from_static(b"myhash"), Entry::new_hash());
        let result = get(&mut db, &[bs(b"myhash")]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            _ => panic!("Expected WRONGTYPE error"),
        }
    }

    #[test]
    fn test_get_missing() {
        let mut db = make_db();
        let result = get(&mut db, &[bs(b"nonexistent")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_get_wrong_arity() {
        let mut db = make_db();
        let result = get(&mut db, &[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_set_wrong_arity() {
        let mut db = make_db();
        let result = set(&mut db, &[bs(b"key")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_set_nx_exists() {
        let mut db = make_db();
        set(&mut db, &[bs(b"key"), bs(b"old")]);
        let result = set(&mut db, &[bs(b"key"), bs(b"new"), bs(b"NX")]);
        assert_eq!(result, Frame::Null);
        // Key unchanged
        let result = get(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"old")));
    }

    #[test]
    fn test_set_nx_not_exists() {
        let mut db = make_db();
        let result = set(&mut db, &[bs(b"key"), bs(b"val"), bs(b"NX")]);
        assert_eq!(result, ok());
        let result = get(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"val")));
    }

    #[test]
    fn test_set_xx_missing() {
        let mut db = make_db();
        let result = set(&mut db, &[bs(b"key"), bs(b"val"), bs(b"XX")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_set_xx_exists() {
        let mut db = make_db();
        set(&mut db, &[bs(b"key"), bs(b"old")]);
        let result = set(&mut db, &[bs(b"key"), bs(b"new"), bs(b"XX")]);
        assert_eq!(result, ok());
        let result = get(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"new")));
    }

    #[test]
    fn test_set_ex() {
        let mut db = make_db();
        let result = set(&mut db, &[bs(b"key"), bs(b"val"), bs(b"EX"), bs(b"10")]);
        assert_eq!(result, ok());
        let entry = db.get(b"key").unwrap();
        assert!(entry.has_expiry());
    }

    #[test]
    fn test_set_px() {
        let mut db = make_db();
        let result = set(
            &mut db,
            &[bs(b"key"), bs(b"val"), bs(b"PX"), bs(b"10000")],
        );
        assert_eq!(result, ok());
        let entry = db.get(b"key").unwrap();
        assert!(entry.has_expiry());
    }

    #[test]
    fn test_set_get_option() {
        let mut db = make_db();
        set(&mut db, &[bs(b"key"), bs(b"old")]);
        let result = set(&mut db, &[bs(b"key"), bs(b"new"), bs(b"GET")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"old")));
        let result = get(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"new")));
    }

    #[test]
    fn test_set_get_option_missing() {
        let mut db = make_db();
        let result = set(&mut db, &[bs(b"key"), bs(b"val"), bs(b"GET")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_set_keepttl() {
        let mut db = make_db();
        // Set key with expiry
        set(&mut db, &[bs(b"key"), bs(b"val"), bs(b"EX"), bs(b"100")]);
        let exp1 = db.get(b"key").unwrap().expires_at_ms;
        assert!(exp1 > 0);
        // Set with KEEPTTL
        set(&mut db, &[bs(b"key"), bs(b"newval"), bs(b"KEEPTTL")]);
        let exp2 = db.get(b"key").unwrap().expires_at_ms;
        assert!(exp2 > 0);
    }

    #[test]
    fn test_set_case_insensitive_options() {
        let mut db = make_db();
        let result = set(&mut db, &[bs(b"key"), bs(b"val"), bs(b"ex"), bs(b"10")]);
        assert_eq!(result, ok());
        assert!(db.get(b"key").unwrap().has_expiry());
    }

    // --- MGET/MSET tests ---

    #[test]
    fn test_mget_with_missing() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"k1"), Bytes::from_static(b"v1"));
        db.set_string(Bytes::from_static(b"k3"), Bytes::from_static(b"v3"));

        let result = mget(&mut db, &[bs(b"k1"), bs(b"k2"), bs(b"k3")]);
        assert_eq!(
            result,
            Frame::Array(vec![
                Frame::BulkString(Bytes::from_static(b"v1")),
                Frame::Null,
                Frame::BulkString(Bytes::from_static(b"v3")),
            ])
        );
    }

    #[test]
    fn test_mget_wrong_arity() {
        let mut db = make_db();
        let result = mget(&mut db, &[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_mset_atomic() {
        let mut db = make_db();
        let result = mset(
            &mut db,
            &[bs(b"k1"), bs(b"v1"), bs(b"k2"), bs(b"v2"), bs(b"k3"), bs(b"v3")],
        );
        assert_eq!(result, ok());
        assert_eq!(
            get(&mut db, &[bs(b"k1")]),
            Frame::BulkString(Bytes::from_static(b"v1"))
        );
        assert_eq!(
            get(&mut db, &[bs(b"k2")]),
            Frame::BulkString(Bytes::from_static(b"v2"))
        );
        assert_eq!(
            get(&mut db, &[bs(b"k3")]),
            Frame::BulkString(Bytes::from_static(b"v3"))
        );
    }

    #[test]
    fn test_mset_odd_args() {
        let mut db = make_db();
        let result = mset(&mut db, &[bs(b"k1"), bs(b"v1"), bs(b"k2")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_mset_empty() {
        let mut db = make_db();
        let result = mset(&mut db, &[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- INCR/DECR tests ---

    #[test]
    fn test_incr_new_key() {
        let mut db = make_db();
        let result = incr(&mut db, &[bs(b"counter")]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_incr_existing() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"counter"), Bytes::from_static(b"10"));
        let result = incr(&mut db, &[bs(b"counter")]);
        assert_eq!(result, Frame::Integer(11));
    }

    #[test]
    fn test_incr_non_integer() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"abc"));
        let result = incr(&mut db, &[bs(b"key")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_incr_overflow() {
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"key"),
            Bytes::from(i64::MAX.to_string()),
        );
        let result = incr(&mut db, &[bs(b"key")]);
        match result {
            Frame::Error(e) => {
                assert!(e.starts_with(b"ERR increment or decrement would overflow"));
            }
            _ => panic!("Expected overflow error"),
        }
    }

    #[test]
    fn test_decr() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"counter"), Bytes::from_static(b"10"));
        let result = decr(&mut db, &[bs(b"counter")]);
        assert_eq!(result, Frame::Integer(9));
    }

    #[test]
    fn test_decrby() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"counter"), Bytes::from_static(b"10"));
        let result = decrby(&mut db, &[bs(b"counter"), bs(b"3")]);
        assert_eq!(result, Frame::Integer(7));
    }

    #[test]
    fn test_incrby() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"counter"), Bytes::from_static(b"10"));
        let result = incrby(&mut db, &[bs(b"counter"), bs(b"5")]);
        assert_eq!(result, Frame::Integer(15));
    }

    #[test]
    fn test_incr_preserves_ttl() {
        let mut db = make_db();
        let exp_ms = current_time_ms() + 100_000;
        db.set(
            Bytes::from_static(b"counter"),
            Entry::new_string_with_expiry(Bytes::from_static(b"5"), exp_ms),
        );
        let result = incr(&mut db, &[bs(b"counter")]);
        assert_eq!(result, Frame::Integer(6));
        let entry = db.get(b"counter").unwrap();
        assert!(entry.has_expiry());
    }

    // --- INCRBYFLOAT tests ---

    #[test]
    fn test_incrbyfloat_basic() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"10.5"));
        let result = incrbyfloat(&mut db, &[bs(b"key"), bs(b"0.1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from("10.6")));
    }

    #[test]
    fn test_incrbyfloat_trailing_zeros() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"10"));
        let result = incrbyfloat(&mut db, &[bs(b"key"), bs(b"1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from("11")));
    }

    #[test]
    fn test_incrbyfloat_new_key() {
        let mut db = make_db();
        let result = incrbyfloat(&mut db, &[bs(b"key"), bs(b"2.5")]);
        assert_eq!(result, Frame::BulkString(Bytes::from("2.5")));
    }

    #[test]
    fn test_incrbyfloat_negative() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"10"));
        let result = incrbyfloat(&mut db, &[bs(b"key"), bs(b"-5")]);
        assert_eq!(result, Frame::BulkString(Bytes::from("5")));
    }

    #[test]
    fn test_incrbyfloat_non_numeric() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"abc"));
        let result = incrbyfloat(&mut db, &[bs(b"key"), bs(b"1.0")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- format_float tests ---

    #[test]
    fn test_format_float_no_trailing_zeros() {
        assert_eq!(format_float(3.14), "3.14");
        assert_eq!(format_float(3.0), "3");
        assert_eq!(format_float(3.1), "3.1");
        assert_eq!(format_float(10.0), "10");
        assert_eq!(format_float(0.5), "0.5");
    }

    // --- APPEND tests ---

    #[test]
    fn test_append_new_key() {
        let mut db = make_db();
        let result = append(&mut db, &[bs(b"key"), bs(b"hello")]);
        assert_eq!(result, Frame::Integer(5));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from_static(b"hello"))
        );
    }

    #[test]
    fn test_append_existing() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"hello"));
        let result = append(&mut db, &[bs(b"key"), bs(b" world")]);
        assert_eq!(result, Frame::Integer(11));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from("hello world"))
        );
    }

    #[test]
    fn test_append_preserves_ttl() {
        let mut db = make_db();
        let exp_ms = current_time_ms() + 100_000;
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string_with_expiry(Bytes::from_static(b"hello"), exp_ms),
        );
        append(&mut db, &[bs(b"key"), bs(b" world")]);
        let entry = db.get(b"key").unwrap();
        assert!(entry.has_expiry());
    }

    // --- STRLEN tests ---

    #[test]
    fn test_strlen_existing() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"hello"));
        let result = strlen(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::Integer(5));
    }

    #[test]
    fn test_strlen_missing() {
        let mut db = make_db();
        let result = strlen(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- SETNX tests ---

    #[test]
    fn test_setnx_new() {
        let mut db = make_db();
        let result = setnx(&mut db, &[bs(b"key"), bs(b"val")]);
        assert_eq!(result, Frame::Integer(1));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from_static(b"val"))
        );
    }

    #[test]
    fn test_setnx_exists() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"old"));
        let result = setnx(&mut db, &[bs(b"key"), bs(b"new")]);
        assert_eq!(result, Frame::Integer(0));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from_static(b"old"))
        );
    }

    // --- SETEX tests ---

    #[test]
    fn test_setex() {
        let mut db = make_db();
        let result = setex(&mut db, &[bs(b"key"), bs(b"10"), bs(b"val")]);
        assert_eq!(result, ok());
        let entry = db.get(b"key").unwrap();
        assert!(entry.has_expiry());
        match &entry.value {
            RedisValue::String(v) => assert_eq!(v.as_ref(), b"val"),
            _ => panic!("unexpected type"),
        }
    }

    #[test]
    fn test_setex_invalid_time() {
        let mut db = make_db();
        let result = setex(&mut db, &[bs(b"key"), bs(b"0"), bs(b"val")]);
        assert!(matches!(result, Frame::Error(_)));
        let result = setex(&mut db, &[bs(b"key"), bs(b"-1"), bs(b"val")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- PSETEX tests ---

    #[test]
    fn test_psetex() {
        let mut db = make_db();
        let result = psetex(&mut db, &[bs(b"key"), bs(b"10000"), bs(b"val")]);
        assert_eq!(result, ok());
        let entry = db.get(b"key").unwrap();
        assert!(entry.has_expiry());
    }

    #[test]
    fn test_psetex_invalid_time() {
        let mut db = make_db();
        let result = psetex(&mut db, &[bs(b"key"), bs(b"0"), bs(b"val")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- GETSET tests ---

    #[test]
    fn test_getset() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"old"));
        let result = getset(&mut db, &[bs(b"key"), bs(b"new")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"old")));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from_static(b"new"))
        );
    }

    #[test]
    fn test_getset_missing() {
        let mut db = make_db();
        let result = getset(&mut db, &[bs(b"key"), bs(b"val")]);
        assert_eq!(result, Frame::Null);
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from_static(b"val"))
        );
    }

    #[test]
    fn test_getset_removes_ttl() {
        let mut db = make_db();
        let exp_ms = current_time_ms() + 100_000;
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string_with_expiry(Bytes::from_static(b"old"), exp_ms),
        );
        getset(&mut db, &[bs(b"key"), bs(b"new")]);
        let entry = db.get(b"key").unwrap();
        assert!(!entry.has_expiry());
    }

    // --- GETDEL tests ---

    #[test]
    fn test_getdel() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        let result = getdel(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"val")));
        assert_eq!(get(&mut db, &[bs(b"key")]), Frame::Null);
    }

    #[test]
    fn test_getdel_missing() {
        let mut db = make_db();
        let result = getdel(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::Null);
    }

    // --- GETEX tests ---

    #[test]
    fn test_getex_no_options() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        let result = getex(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"val")));
    }

    #[test]
    fn test_getex_missing() {
        let mut db = make_db();
        let result = getex(&mut db, &[bs(b"key")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_getex_persist() {
        let mut db = make_db();
        let exp_ms = current_time_ms() + 100_000;
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), exp_ms),
        );
        let result = getex(&mut db, &[bs(b"key"), bs(b"PERSIST")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"val")));
        let entry = db.get(b"key").unwrap();
        assert!(!entry.has_expiry());
    }

    #[test]
    fn test_getex_ex() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"val"));
        let result = getex(&mut db, &[bs(b"key"), bs(b"EX"), bs(b"10")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"val")));
        let entry = db.get(b"key").unwrap();
        assert!(entry.has_expiry());
    }
}
