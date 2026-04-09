use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{LISTPACK_MAX_ELEMENT_SIZE, LISTPACK_MAX_ENTRIES};

use crate::command::helpers::{err_wrong_args, extract_bytes, ok};

/// HSET key field value [field value ...]
///
/// Sets field-value pairs in the hash stored at key. Returns the number
/// of fields that were newly added (not updated).
pub fn hset(db: &mut Database, args: &[Frame]) -> Frame {
    // Need at least key + one field-value pair, and args count must be odd (key + pairs)
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return err_wrong_args("HSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSET"),
    };

    // Check if any field or value exceeds LISTPACK_MAX_ELEMENT_SIZE
    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        // Try listpack path for small hashes
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                let mut count = 0i64;
                let mut i = 1;
                while i < args.len() {
                    let field = match extract_bytes(&args[i]) {
                        Some(f) => f,
                        None => return err_wrong_args("HSET"),
                    };
                    let value = match extract_bytes(&args[i + 1]) {
                        Some(v) => v,
                        None => return err_wrong_args("HSET"),
                    };
                    // Search for existing field in listpack pairs
                    let mut found = false;
                    let mut idx = 0;
                    for (f, _v) in lp.iter_pairs() {
                        if f.as_bytes() == field.as_ref() {
                            // Replace value at idx*2+1
                            lp.replace_at(idx * 2 + 1, value);
                            found = true;
                            break;
                        }
                        idx += 1;
                    }
                    if !found {
                        lp.push_back(field);
                        lp.push_back(value);
                        count += 1;
                    }
                    i += 2;
                }
                // Check threshold: lp.len()/2 fields > LISTPACK_MAX_ENTRIES
                if lp.len() / 2 > LISTPACK_MAX_ENTRIES {
                    db.upgrade_hash_listpack_to_hash(key);
                }
                return Frame::Integer(count);
            }
            Ok(None) => {
                // Already a full HashMap -- fall through to standard path
            }
            Err(e) => return e,
        }
    }

    // Full HashMap path (large elements or already upgraded)
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
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return err_wrong_args("HMSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMSET"),
    };

    // Check if any field or value exceeds LISTPACK_MAX_ELEMENT_SIZE
    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                let mut i = 1;
                while i < args.len() {
                    let field = match extract_bytes(&args[i]) {
                        Some(f) => f,
                        None => return err_wrong_args("HMSET"),
                    };
                    let value = match extract_bytes(&args[i + 1]) {
                        Some(v) => v,
                        None => return err_wrong_args("HMSET"),
                    };
                    let mut found = false;
                    let mut idx = 0;
                    for (f, _v) in lp.iter_pairs() {
                        if f.as_bytes() == field.as_ref() {
                            lp.replace_at(idx * 2 + 1, value);
                            found = true;
                            break;
                        }
                        idx += 1;
                    }
                    if !found {
                        lp.push_back(field);
                        lp.push_back(value);
                    }
                    i += 2;
                }
                if lp.len() / 2 > LISTPACK_MAX_ENTRIES {
                    db.upgrade_hash_listpack_to_hash(key);
                }
                return ok();
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }

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
                ));
            }
        },
        None => return err_wrong_args("HINCRBY"),
    };
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let current = match map.get(&field) {
        Some(v) => match std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR hash value is not an integer")),
        },
        None => 0,
    };
    let new_value = current + increment;
    let mut ibuf = itoa::Buffer::new();
    map.insert(
        field,
        Bytes::copy_from_slice(ibuf.format(new_value).as_bytes()),
    );
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
            None => return Frame::Error(Bytes::from_static(b"ERR value is not a valid float")),
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
                return Frame::Error(Bytes::from_static(b"ERR hash value is not a valid float"));
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
pub(super) fn format_float(v: f64) -> String {
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
