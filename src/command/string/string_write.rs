use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::{Entry, current_time_ms};

use super::{format_float, parse_f64, parse_i64, parse_positive_i64};
use crate::command::helpers::{err_wrong_args, extract_bytes, ok};

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

    // Fast path: plain SET key value (no options) — skip option parsing entirely
    if args.len() == 2 {
        let mut entry = Entry::new_string(value);
        entry.set_last_access(db.now());
        entry.set_access_counter(5);
        db.set(key, entry);
        return ok();
    }

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
                    ));
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
                    ));
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
                    ));
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
                    ));
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
        db.get(&key).map(|e| match e.value.as_bytes_owned() {
            Some(v) => Frame::BulkString(v),
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
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
        return if get_old { Frame::Null } else { Frame::Null };
    }

    // Determine final expiry
    let base_ts = db.base_timestamp();
    let final_expires_at_ms = if expires_at_ms > 0 {
        expires_at_ms
    } else if keepttl {
        // Preserve existing TTL
        db.get(&key).map(|e| e.expires_at_ms(base_ts)).unwrap_or(0)
    } else {
        0
    };

    let mut entry = if final_expires_at_ms > 0 {
        Entry::new_string_with_expiry(value, final_expires_at_ms, base_ts)
    } else {
        Entry::new_string(value)
    };
    // Set last_access from db cached time (version will be set by db.set)
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key, entry);

    if get_old {
        old_value.unwrap_or(Frame::Null)
    } else {
        ok()
    }
}

/// MSET command handler.
pub fn mset(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || !args.len().is_multiple_of(2) {
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
            ));
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
            ));
        }
    };
    incrby_internal(db, key, -delta)
}

/// Internal helper for INCR/DECR/INCRBY/DECRBY.
fn incrby_internal(db: &mut Database, key: &Bytes, delta: i64) -> Frame {
    let base_ts = db.base_timestamp();
    // Get current value and existing expiry
    let (current, existing_expiry_ms) = match db.get(key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms(base_ts);
            match entry.value.as_bytes() {
                Some(v) => {
                    let s = match std::str::from_utf8(v) {
                        Ok(s) => s,
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not an integer or out of range",
                            ));
                        }
                    };
                    match s.parse::<i64>() {
                        Ok(n) => (n, expiry),
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not an integer or out of range",
                            ));
                        }
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
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
            ));
        }
    };

    // Store new value preserving existing TTL
    let mut entry = if existing_expiry_ms > 0 {
        Entry::new_string_with_expiry(
            Bytes::from(new_val.to_string()),
            existing_expiry_ms,
            base_ts,
        )
    } else {
        Entry::new_string(Bytes::from(new_val.to_string()))
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
        None => return Frame::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };
    if increment.is_nan() {
        return Frame::Error(Bytes::from_static(
            b"ERR increment would produce NaN or Infinity",
        ));
    }

    let base_ts = db.base_timestamp();
    // Get current value and existing expiry
    let (current, existing_expiry_ms) = match db.get(key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms(base_ts);
            match entry.value.as_bytes() {
                Some(v) => {
                    let s = match std::str::from_utf8(v) {
                        Ok(s) => s,
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not a valid float",
                            ));
                        }
                    };
                    match s.parse::<f64>() {
                        Ok(f) => (f, expiry),
                        Err(_) => {
                            return Frame::Error(Bytes::from_static(
                                b"ERR value is not a valid float",
                            ));
                        }
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
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

    let mut entry = if existing_expiry_ms > 0 {
        Entry::new_string_with_expiry(Bytes::from(formatted.clone()), existing_expiry_ms, base_ts)
    } else {
        Entry::new_string(Bytes::from(formatted.clone()))
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

    let base_ts = db.base_timestamp();
    // Check if key exists, get existing data + expiry
    let (existing_data, existing_expiry_ms) = match db.get(&key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms(base_ts);
            match entry.value.as_bytes_owned() {
                Some(v) => (Some(v), expiry),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
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
    let mut entry = if existing_expiry_ms > 0 {
        Entry::new_string_with_expiry(new_val, existing_expiry_ms, base_ts)
    } else {
        Entry::new_string(new_val)
    };
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key, entry);

    Frame::Integer(new_len)
}

/// SETRANGE key offset value — overwrite part of string at offset.
/// Zero-pads if offset exceeds current length. Creates key if missing.
/// Returns new string length.
pub fn setrange(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("SETRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("SETRANGE"),
    };
    let offset = match parse_i64(&args[1]) {
        Some(v) if v >= 0 => v as usize,
        _ => return Frame::Error(Bytes::from_static(b"ERR offset is out of range")),
    };
    let value = match extract_bytes(&args[2]) {
        Some(v) => v,
        None => return err_wrong_args("SETRANGE"),
    };

    // Redis compatibility: SETRANGE with empty value on missing key returns 0 without creating key
    if value.is_empty() {
        return match db.get(&key) {
            Some(entry) => match entry.value.as_bytes() {
                Some(v) => Frame::Integer(v.len() as i64),
                None => Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            },
            None => Frame::Integer(0),
        };
    }

    // Redis limits string size to 512MB
    let required = match offset.checked_add(value.len()) {
        Some(r) if r <= 512 * 1024 * 1024 => r,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR string exceeds maximum allowed size (512MB)",
            ));
        }
    };

    let base_ts = db.base_timestamp();
    let (existing_data, existing_expiry_ms) = match db.get(&key) {
        Some(entry) => {
            let expiry = entry.expires_at_ms(base_ts);
            match entry.value.as_bytes() {
                Some(v) => (Some(v.to_vec()), expiry),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            }
        }
        None => (None, 0),
    };

    let mut buf = existing_data.unwrap_or_default();
    // Extend with zero bytes if needed
    if required > buf.len() {
        buf.resize(required, 0);
    }
    // Overwrite at offset
    buf[offset..offset + value.len()].copy_from_slice(value);

    let new_len = buf.len() as i64;
    let new_val = Bytes::from(buf);
    let mut entry = if existing_expiry_ms > 0 {
        Entry::new_string_with_expiry(new_val, existing_expiry_ms, base_ts)
    } else {
        Entry::new_string(new_val)
    };
    entry.set_last_access(db.now());
    entry.set_access_counter(5);
    db.set(key, entry);

    Frame::Integer(new_len)
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
            ));
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
            ));
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

    let old = db.get(&key).map(|e| match e.value.as_bytes_owned() {
        Some(v) => Frame::BulkString(v),
        None => Frame::Error(Bytes::from_static(
            b"WRONGTYPE Operation against a key holding the wrong kind of value",
        )),
    });

    // GETSET removes TTL (sets new entry without expiry)
    db.set_string(key, value);

    old.unwrap_or(Frame::Null)
}
