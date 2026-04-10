use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::current_time_ms;

use super::{parse_i64, parse_positive_i64};
use crate::command::helpers::{err_wrong_args, extract_bytes};

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
        Some(entry) => {
            crate::admin::metrics_setup::record_keyspace_hit();
            match entry.value.as_bytes_owned() {
                Some(v) => Frame::BulkString(v),
                None => Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            }
        }
        None => {
            crate::admin::metrics_setup::record_keyspace_miss();
            Frame::Null
        }
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
            Some(entry) => {
                crate::admin::metrics_setup::record_keyspace_hit();
                match entry.value.as_bytes_owned() {
                    Some(v) => results.push(Frame::BulkString(v)),
                    None => results.push(Frame::Null),
                }
            }
            None => {
                crate::admin::metrics_setup::record_keyspace_miss();
                results.push(Frame::Null);
            }
        }
    }
    Frame::Array(results.into())
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
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => Frame::Integer(v.len() as i64),
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Frame::Integer(0),
    }
}

/// GETRANGE key start end — return substring of string value.
/// Negative indices count from the end. Out-of-range clamped to string bounds.
/// Returns empty string for reversed range or missing key.
pub fn getrange(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("GETRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GETRANGE"),
    };
    let start = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let end = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    match db.get(key) {
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => getrange_slice(v, start, end),
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Frame::BulkString(Bytes::new()),
    }
}

/// GETRANGE (read-only).
pub fn getrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("GETRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GETRANGE"),
    };
    let start = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let end = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    match db.get_if_alive(key, now_ms) {
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => getrange_slice(v, start, end),
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Frame::BulkString(Bytes::new()),
    }
}

/// Shared logic for GETRANGE/SUBSTR: resolve negative indices, clamp, slice.
fn getrange_slice(v: &[u8], start: i64, end: i64) -> Frame {
    let len = v.len() as i64;
    if len == 0 {
        return Frame::BulkString(Bytes::new());
    }
    // Resolve negative indices
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    // Clamp
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e {
        return Frame::BulkString(Bytes::new());
    }
    Frame::BulkString(Bytes::copy_from_slice(&v[s as usize..=e as usize]))
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
        Some(entry) => match entry.value.as_bytes_owned() {
            Some(v) => Frame::BulkString(v),
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
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
        Some(entry) => match entry.value.as_bytes_owned() {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        },
        None => return Frame::Null,
    };

    // Parse options using zero-alloc case-insensitive comparison
    if args.len() > 1 {
        let base_ts = db.base_timestamp();
        let opt = match extract_bytes(&args[1]) {
            Some(b) => b.as_ref(),
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        if opt.eq_ignore_ascii_case(b"PERSIST") {
            if let Some(entry) = db.get_mut(&key) {
                entry.set_expires_at_ms(base_ts, 0);
            }
        } else if opt.eq_ignore_ascii_case(b"EX") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(secs) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.set_expires_at_ms(base_ts, current_time_ms() + (secs as u64) * 1000);
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
                }
            }
        } else if opt.eq_ignore_ascii_case(b"PX") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(ms) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.set_expires_at_ms(base_ts, current_time_ms() + ms as u64);
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
                }
            }
        } else if opt.eq_ignore_ascii_case(b"EXAT") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(ts) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.set_expires_at_ms(base_ts, (ts as u64) * 1000);
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
                }
            }
        } else if opt.eq_ignore_ascii_case(b"PXAT") {
            if args.len() < 3 {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
            match parse_positive_i64(&args[2]) {
                Some(ts_ms) => {
                    if let Some(entry) = db.get_mut(&key) {
                        entry.set_expires_at_ms(base_ts, ts_ms as u64);
                    }
                }
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
                }
            }
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    Frame::BulkString(value)
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// GET (read-only): uses get_if_alive instead of get.
pub fn get_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("GET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("GET"),
    };
    match db.get_if_alive(key, now_ms) {
        Some(entry) => match entry.value.as_bytes_owned() {
            Some(v) => Frame::BulkString(v),
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => {
            // Cold storage fallback: key may have been evicted to NVMe
            if let Some(value) = db.get_cold_value(key, now_ms) {
                match value {
                    crate::storage::entry::RedisValue::String(v) => Frame::BulkString(v),
                    _ => Frame::Error(Bytes::from_static(
                        b"WRONGTYPE Operation against a key holding the wrong kind of value",
                    )),
                }
            } else {
                Frame::Null
            }
        }
    }
}

/// MGET (read-only).
pub fn mget_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
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
        match db.get_if_alive(key, now_ms) {
            Some(entry) => match entry.value.as_bytes_owned() {
                Some(v) => results.push(Frame::BulkString(v)),
                None => results.push(Frame::Null),
            },
            None => results.push(Frame::Null),
        }
    }
    Frame::Array(results.into())
}

/// STRLEN (read-only).
pub fn strlen_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("STRLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("STRLEN"),
    };
    match db.get_if_alive(key, now_ms) {
        Some(entry) => match entry.value.as_bytes() {
            Some(v) => Frame::Integer(v.len() as i64),
            None => Frame::Error(Bytes::from_static(
                b"WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
        },
        None => Frame::Integer(0),
    }
}
