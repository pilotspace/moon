mod string_read;
mod string_write;

pub use string_read::*;
pub use string_write::*;

// --- Shared helpers used by both string_read and string_write ---

use crate::protocol::Frame;

use super::helpers::extract_bytes;

/// Parse a Frame argument as i64.
pub(crate) fn parse_i64(frame: &Frame) -> Option<i64> {
    let b = extract_bytes(frame)?;
    let s = std::str::from_utf8(b).ok()?;
    s.parse::<i64>().ok()
}

/// Parse a Frame argument as positive i64 (> 0).
pub(crate) fn parse_positive_i64(frame: &Frame) -> Option<i64> {
    let v = parse_i64(frame)?;
    if v > 0 { Some(v) } else { None }
}

/// Parse a Frame argument as f64.
pub(crate) fn parse_f64(frame: &Frame) -> Option<f64> {
    let b = extract_bytes(frame)?;
    let s = std::str::from_utf8(b).ok()?;
    s.parse::<f64>().ok()
}

/// Format a float result, stripping trailing zeros after decimal point.
pub(crate) fn format_float(val: f64) -> String {
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
    use bytes::Bytes;
    use crate::framevec;
    use crate::protocol::Frame;
    use crate::storage::Database;
    use crate::storage::entry::{Entry, current_time_ms};

    use super::super::helpers::ok;

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
        let result = set(&mut db, &[bs(b"key"), bs(b"val"), bs(b"PX"), bs(b"10000")]);
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
        assert!(db.get(b"key").unwrap().has_expiry());
        // Set with KEEPTTL
        set(&mut db, &[bs(b"key"), bs(b"newval"), bs(b"KEEPTTL")]);
        assert!(db.get(b"key").unwrap().has_expiry());
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
            Frame::Array(framevec![
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
            &[
                bs(b"k1"),
                bs(b"v1"),
                bs(b"k2"),
                bs(b"v2"),
                bs(b"k3"),
                bs(b"v3"),
            ],
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
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"counter"),
            Entry::new_string_with_expiry(Bytes::from_static(b"5"), exp_ms, base_ts),
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
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string_with_expiry(Bytes::from_static(b"hello"), exp_ms, base_ts),
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
        assert_eq!(entry.value.as_bytes().unwrap(), b"val");
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
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string_with_expiry(Bytes::from_static(b"old"), exp_ms, base_ts),
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
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string_with_expiry(Bytes::from_static(b"val"), exp_ms, base_ts),
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

    // --- GETRANGE tests ---

    #[test]
    fn test_getrange_basic() {
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"Hello, World!"),
        );
        let result = getrange(&mut db, &[bs(b"key"), bs(b"0"), bs(b"4")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"Hello")));
    }

    #[test]
    fn test_getrange_negative_end() {
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"Hello, World!"),
        );
        let result = getrange(&mut db, &[bs(b"key"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            result,
            Frame::BulkString(Bytes::from_static(b"Hello, World!"))
        );
    }

    #[test]
    fn test_getrange_negative_both() {
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"Hello, World!"),
        );
        // len=13, -6 = 7, -1 = 12 → "World!"
        let result = getrange(&mut db, &[bs(b"key"), bs(b"-6"), bs(b"-1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"World!")));
    }

    #[test]
    fn test_getrange_middle() {
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"Hello, World!"),
        );
        let result = getrange(&mut db, &[bs(b"key"), bs(b"7"), bs(b"-1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"World!")));
    }

    #[test]
    fn test_getrange_out_of_range() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"Hello"));
        // end beyond string length — clamped
        let result = getrange(&mut db, &[bs(b"key"), bs(b"0"), bs(b"100")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"Hello")));
    }

    #[test]
    fn test_getrange_reversed() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"Hello"));
        let result = getrange(&mut db, &[bs(b"key"), bs(b"3"), bs(b"1")]);
        assert_eq!(result, Frame::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_missing_key() {
        let mut db = make_db();
        let result = getrange(&mut db, &[bs(b"key"), bs(b"0"), bs(b"-1")]);
        assert_eq!(result, Frame::BulkString(Bytes::new()));
    }

    #[test]
    fn test_getrange_wrongtype() {
        let mut db = make_db();
        db.set(Bytes::from_static(b"myhash"), Entry::new_hash());
        let result = getrange(&mut db, &[bs(b"myhash"), bs(b"0"), bs(b"-1")]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            _ => panic!("Expected WRONGTYPE error"),
        }
    }

    #[test]
    fn test_getrange_wrong_arity() {
        let mut db = make_db();
        let result = getrange(&mut db, &[bs(b"key"), bs(b"0")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_getrange_empty_string() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::new());
        let result = getrange(&mut db, &[bs(b"key"), bs(b"0"), bs(b"-1")]);
        assert_eq!(result, Frame::BulkString(Bytes::new()));
    }

    // --- SETRANGE tests ---

    #[test]
    fn test_setrange_basic() {
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"Hello, World!"),
        );
        let result = setrange(&mut db, &[bs(b"key"), bs(b"7"), bs(b"Redis")]);
        assert_eq!(result, Frame::Integer(13));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from("Hello, Redis!"))
        );
    }

    #[test]
    fn test_setrange_zero_pad() {
        let mut db = make_db();
        let result = setrange(&mut db, &[bs(b"key"), bs(b"5"), bs(b"hi")]);
        assert_eq!(result, Frame::Integer(7));
        let val = get(&mut db, &[bs(b"key")]);
        match val {
            Frame::BulkString(b) => {
                assert_eq!(b.len(), 7);
                assert_eq!(&b[..5], &[0, 0, 0, 0, 0]);
                assert_eq!(&b[5..], b"hi");
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_setrange_extend() {
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"key"), Bytes::from_static(b"Hello"));
        let result = setrange(&mut db, &[bs(b"key"), bs(b"5"), bs(b" World")]);
        assert_eq!(result, Frame::Integer(11));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from("Hello World"))
        );
    }

    #[test]
    fn test_setrange_negative_offset() {
        let mut db = make_db();
        let result = setrange(&mut db, &[bs(b"key"), bs(b"-1"), bs(b"hi")]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"ERR offset")),
            _ => panic!("Expected offset error"),
        }
    }

    #[test]
    fn test_setrange_preserves_ttl() {
        let mut db = make_db();
        let exp_ms = current_time_ms() + 100_000;
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"key"),
            Entry::new_string_with_expiry(Bytes::from_static(b"Hello"), exp_ms, base_ts),
        );
        setrange(&mut db, &[bs(b"key"), bs(b"0"), bs(b"Jello")]);
        let entry = db.get(b"key").unwrap();
        assert!(entry.has_expiry());
        assert_eq!(entry.value.as_bytes().unwrap(), b"Jello");
    }

    #[test]
    fn test_setrange_wrongtype() {
        let mut db = make_db();
        db.set(Bytes::from_static(b"myhash"), Entry::new_hash());
        let result = setrange(&mut db, &[bs(b"myhash"), bs(b"0"), bs(b"hi")]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            _ => panic!("Expected WRONGTYPE error"),
        }
    }

    #[test]
    fn test_setrange_wrong_arity() {
        let mut db = make_db();
        let result = setrange(&mut db, &[bs(b"key"), bs(b"0")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_setrange_at_zero_new_key() {
        let mut db = make_db();
        let result = setrange(&mut db, &[bs(b"key"), bs(b"0"), bs(b"Hello")]);
        assert_eq!(result, Frame::Integer(5));
        assert_eq!(
            get(&mut db, &[bs(b"key")]),
            Frame::BulkString(Bytes::from_static(b"Hello"))
        );
    }

    // --- SUBSTR (alias for GETRANGE) ---

    #[test]
    fn test_substr_alias() {
        // SUBSTR uses the same getrange function, verify it produces identical results
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"msg"),
            Bytes::from_static(b"Hello, World!"),
        );
        // SUBSTR with same args as GETRANGE should produce identical output
        let getrange_result = getrange(&mut db, &[bs(b"msg"), bs(b"0"), bs(b"4")]);
        let substr_result = getrange(&mut db, &[bs(b"msg"), bs(b"0"), bs(b"4")]);
        assert_eq!(getrange_result, substr_result);
        assert_eq!(
            substr_result,
            Frame::BulkString(Bytes::from_static(b"Hello"))
        );
    }

    #[test]
    fn test_setrange_empty_value_missing_key() {
        // Redis: SETRANGE on missing key with empty value returns 0, does NOT create key
        let mut db = make_db();
        let result = setrange(&mut db, &[bs(b"nokey"), bs(b"5"), bs(b"")]);
        assert_eq!(result, Frame::Integer(0));
        assert_eq!(get(&mut db, &[bs(b"nokey")]), Frame::Null);
    }

    #[test]
    fn test_setrange_empty_value_existing_key() {
        // Redis: SETRANGE on existing key with empty value returns current length
        let mut db = make_db();
        db.set_string(Bytes::from_static(b"mykey"), Bytes::from_static(b"Hello"));
        let result = setrange(&mut db, &[bs(b"mykey"), bs(b"5"), bs(b"")]);
        assert_eq!(result, Frame::Integer(5));
        // Key unchanged
        assert_eq!(
            get(&mut db, &[bs(b"mykey")]),
            Frame::BulkString(Bytes::from_static(b"Hello"))
        );
    }

    #[test]
    fn test_substr_negative_indices() {
        let mut db = make_db();
        db.set_string(
            Bytes::from_static(b"msg"),
            Bytes::from_static(b"Hello, World!"),
        );
        // SUBSTR with negative indices (alias behavior)
        let result = getrange(&mut db, &[bs(b"msg"), bs(b"-6"), bs(b"-1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"World!")));
    }
}
