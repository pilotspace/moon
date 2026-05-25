mod hash_read;
mod hash_write;

pub use hash_read::*;
pub use hash_write::*;

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::framevec;
    use crate::protocol::Frame;
    use crate::storage::Database;

    use super::*;

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
        assert_eq!(result, Frame::Array(framevec![]));
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
        assert_eq!(hkeys(&mut db, &args), Frame::Array(framevec![]));
        assert_eq!(hvals(&mut db, &args), Frame::Array(framevec![]));
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
        assert_eq!(result, Frame::BulkString(Bytes::from("10.5")));

        // Increment again
        let args = make_args(&[b"myhash", b"price", b"0.5"]);
        let result = hincrbyfloat(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from("11")));
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
            b"myhash",
            b"name",
            b"alice",
            b"age",
            b"30",
            b"nickname",
            b"ali",
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
                assert_eq!(outer[1], Frame::Array(framevec![]));
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

    // ──────────────────────────────────────────────────────────────────────────
    // Phase 196 — HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT write commands.
    // RED tests; will go GREEN once handlers + HashWithTtl-aware HSET land.
    // ──────────────────────────────────────────────────────────────────────────

    fn seed_hash(db: &mut Database, key: &[u8], pairs: &[(&[u8], &[u8])]) {
        let mut args: Vec<&[u8]> = vec![key];
        for (f, v) in pairs {
            args.push(f);
            args.push(v);
        }
        let frames = make_args(&args);
        let r = hset(db, &frames);
        assert!(matches!(r, Frame::Integer(_)), "seed hset must succeed");
    }

    #[test]
    fn test_hexpire_sets_ttl_on_existing_field() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let args = make_args(&[b"h", b"3600", b"FIELDS", b"1", b"f"]);
        let r = hexpire(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(1)]));
        assert!(db.hash_get_field_ttl_ms(b"h", b"f").is_some());
    }

    #[test]
    fn test_hexpire_returns_zero_on_missing_field() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let args = make_args(&[b"h", b"60", b"FIELDS", b"1", b"nope"]);
        let r = hexpire(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(0)]));
    }

    #[test]
    fn test_hexpire_returns_neg2_when_nx_and_ttl_exists() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let args = make_args(&[b"h", b"60", b"FIELDS", b"1", b"f"]);
        assert_eq!(
            hexpire(&mut db, &args),
            Frame::Array(framevec![Frame::Integer(1)])
        );

        let args = make_args(&[b"h", b"120", b"NX", b"FIELDS", b"1", b"f"]);
        let r = hexpire(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(-2)]));
    }

    #[test]
    fn test_hexpire_returns_neg2_when_xx_and_no_ttl() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let args = make_args(&[b"h", b"60", b"XX", b"FIELDS", b"1", b"f"]);
        let r = hexpire(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(-2)]));
    }

    #[test]
    fn test_hexpire_returns_neg2_when_gt_and_new_lt_current() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        // Seed a 1-hour TTL.
        let args = make_args(&[b"h", b"3600", b"FIELDS", b"1", b"f"]);
        assert_eq!(
            hexpire(&mut db, &args),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        // GT with a smaller TTL should be rejected.
        let args = make_args(&[b"h", b"60", b"GT", b"FIELDS", b"1", b"f"]);
        let r = hexpire(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(-2)]));
    }

    #[test]
    fn test_hexpire_returns_neg2_when_lt_and_new_gt_current() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let args = make_args(&[b"h", b"60", b"FIELDS", b"1", b"f"]);
        assert_eq!(
            hexpire(&mut db, &args),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        let args = make_args(&[b"h", b"3600", b"LT", b"FIELDS", b"1", b"f"]);
        let r = hexpire(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(-2)]));
    }

    #[test]
    fn test_hexpireat_returns_two_when_expiry_in_past() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        // 1970-01-01T00:00:01 — guaranteed in the past.
        let args = make_args(&[b"h", b"1", b"FIELDS", b"1", b"f"]);
        let r = hexpireat(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(2)]));
        // Field must be gone.
        let r = hget(&mut db, &make_args(&[b"h", b"f"]));
        assert_eq!(r, Frame::Null);
    }

    #[test]
    fn test_hexpire_wrong_type_returns_wrongtype() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"s"), Bytes::from_static(b"x"));
        let args = make_args(&[b"s", b"60", b"FIELDS", b"1", b"f"]);
        let r = hexpire(&mut db, &args);
        assert!(matches!(r, Frame::Error(ref e) if e.starts_with(b"WRONGTYPE")));
    }

    #[test]
    fn test_hexpire_missing_key_returns_zero_per_field() {
        let mut db = Database::new();
        let args = make_args(&[b"nokey", b"60", b"FIELDS", b"2", b"a", b"b"]);
        let r = hexpire(&mut db, &args);
        assert_eq!(
            r,
            Frame::Array(framevec![Frame::Integer(0), Frame::Integer(0)])
        );
    }

    #[test]
    fn test_hexpire_numfields_mismatch_returns_error() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        // numfields says 2 but only 1 field name follows.
        let args = make_args(&[b"h", b"60", b"FIELDS", b"2", b"f"]);
        let r = hexpire(&mut db, &args);
        assert!(matches!(r, Frame::Error(_)), "got {:?}", r);
    }

    #[test]
    fn test_hexpire_nx_xx_conflict_returns_error() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let args = make_args(&[b"h", b"60", b"NX", b"XX", b"FIELDS", b"1", b"f"]);
        let r = hexpire(&mut db, &args);
        assert!(matches!(r, Frame::Error(_)), "got {:?}", r);
    }

    #[test]
    fn test_hexpire_numfields_zero_returns_error() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let args = make_args(&[b"h", b"60", b"FIELDS", b"0"]);
        let r = hexpire(&mut db, &args);
        assert!(matches!(r, Frame::Error(_)), "got {:?}", r);
    }

    #[test]
    fn test_hpexpire_millisecond_precision() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let now = db.now_ms();
        let args = make_args(&[b"h", b"123456", b"FIELDS", b"1", b"f"]);
        let r = hpexpire(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(1)]));
        let got = db.hash_get_field_ttl_ms(b"h", b"f").expect("ttl set");
        // Should be approximately now + 123456 ms.
        assert!(
            (got as i128 - (now as i128 + 123456)).abs() < 100,
            "expected ~{} got {}",
            now + 123456,
            got
        );
    }

    #[test]
    fn test_hexpireat_absolute_seconds() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        // Far future: 2099-01-01 ≈ 4_070_908_800 seconds.
        let args = make_args(&[b"h", b"4070908800", b"FIELDS", b"1", b"f"]);
        let r = hexpireat(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(1)]));
        assert_eq!(
            db.hash_get_field_ttl_ms(b"h", b"f"),
            Some(4_070_908_800_u64 * 1000)
        );
    }

    #[test]
    fn test_hpexpireat_absolute_millis() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let abs_ms = 4_070_908_800_000_u64;
        let s = abs_ms.to_string();
        let args = make_args(&[b"h", s.as_bytes(), b"FIELDS", b"1", b"f"]);
        let r = hpexpireat(&mut db, &args);
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(1)]));
        assert_eq!(db.hash_get_field_ttl_ms(b"h", b"f"), Some(abs_ms));
    }

    // ── HSET / HSETNX / HMSET / HDEL / HINCRBY on HashWithTtl ────────────────

    #[test]
    fn test_hset_works_on_ttl_encoded_hash() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1")]);
        // Promote to HashWithTtl via HEXPIRE.
        let args = make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"]);
        assert_eq!(
            hexpire(&mut db, &args),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        // HSET on a different field should work — currently regresses to WRONGTYPE pre-fix.
        let r = hset(&mut db, &make_args(&[b"h", b"b", b"2"]));
        assert_eq!(r, Frame::Integer(1));
        assert_eq!(
            hget(&mut db, &make_args(&[b"h", b"b"])),
            Frame::BulkString(Bytes::from_static(b"2"))
        );
    }

    #[test]
    fn test_hset_clears_ttl_on_overwrite() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1"), (b"b", b"2")]);
        // Set TTLs on both fields.
        assert_eq!(
            hexpire(
                &mut db,
                &make_args(&[b"h", b"60", b"FIELDS", b"2", b"a", b"b"])
            ),
            Frame::Array(framevec![Frame::Integer(1), Frame::Integer(1)])
        );
        // Overwrite 'a' via HSET → its TTL must be cleared.
        let r = hset(&mut db, &make_args(&[b"h", b"a", b"NEW"]));
        assert_eq!(r, Frame::Integer(0));
        assert_eq!(db.hash_get_field_ttl_ms(b"h", b"a"), None);
        // 'b' TTL must still be intact.
        assert!(db.hash_get_field_ttl_ms(b"h", b"b").is_some());
    }

    #[test]
    fn test_hset_clears_ttl_downgrades_when_last() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        // Overwrite the only TTL'd field; storage should downgrade to plain Hash.
        hset(&mut db, &make_args(&[b"h", b"a", b"NEW"]));
        // No TTL anywhere. HEXPIRE with XX must return -2 (no current TTL).
        let r = hexpire(
            &mut db,
            &make_args(&[b"h", b"60", b"XX", b"FIELDS", b"1", b"a"]),
        );
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(-2)]));
    }

    #[test]
    fn test_hsetnx_works_on_ttl_encoded_hash() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        // HSETNX a new field should succeed without WRONGTYPE.
        let r = hsetnx(&mut db, &make_args(&[b"h", b"b", b"2"]));
        assert_eq!(r, Frame::Integer(1));
    }

    #[test]
    fn test_hsetnx_does_not_clear_ttl_when_field_exists() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        // HSETNX on existing field — must NOT overwrite, must NOT clear TTL.
        let r = hsetnx(&mut db, &make_args(&[b"h", b"a", b"NEW"]));
        assert_eq!(r, Frame::Integer(0));
        assert!(db.hash_get_field_ttl_ms(b"h", b"a").is_some());
    }

    #[test]
    fn test_hmset_works_on_ttl_encoded_hash() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        // HMSET should not WRONGTYPE.
        let r = hmset(&mut db, &make_args(&[b"h", b"b", b"2", b"c", b"3"]));
        assert!(matches!(r, Frame::SimpleString(ref s) if s.as_ref() == b"OK"));
    }

    #[test]
    fn test_hmset_clears_ttl_on_overwrite() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        hmset(&mut db, &make_args(&[b"h", b"a", b"NEW"]));
        assert_eq!(db.hash_get_field_ttl_ms(b"h", b"a"), None);
    }

    #[test]
    fn test_hdel_works_on_ttl_encoded_hash() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1"), (b"b", b"2")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        // HDEL on non-TTL'd field must work, not WRONGTYPE.
        let r = hdel(&mut db, &make_args(&[b"h", b"b"]));
        assert_eq!(r, Frame::Integer(1));
    }

    #[test]
    fn test_hdel_removes_ttl_sidecar_entry() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1"), (b"b", b"2")]);
        assert_eq!(
            hexpire(
                &mut db,
                &make_args(&[b"h", b"60", b"FIELDS", b"2", b"a", b"b"])
            ),
            Frame::Array(framevec![Frame::Integer(1), Frame::Integer(1)])
        );
        // Delete 'a' — its TTL entry must also be removed.
        hdel(&mut db, &make_args(&[b"h", b"a"]));
        assert_eq!(db.hash_get_field_ttl_ms(b"h", b"a"), None);
        // 'b' TTL still present.
        assert!(db.hash_get_field_ttl_ms(b"h", b"b").is_some());
    }

    #[test]
    fn test_hdel_downgrades_when_last_ttl_removed() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"a", b"1"), (b"b", b"2")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"a"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        hdel(&mut db, &make_args(&[b"h", b"a"]));
        // No TTLs left — encoding should downgrade to plain Hash. Indirect check:
        // HEXPIRE with XX on the surviving field must return -2 (no TTL).
        let r = hexpire(
            &mut db,
            &make_args(&[b"h", b"60", b"XX", b"FIELDS", b"1", b"b"]),
        );
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(-2)]));
    }

    #[test]
    fn test_hincrby_preserves_ttl() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"c", b"10")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"c"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        let ttl_before = db.hash_get_field_ttl_ms(b"h", b"c");
        let r = hincrby(&mut db, &make_args(&[b"h", b"c", b"5"]));
        assert_eq!(r, Frame::Integer(15));
        assert_eq!(db.hash_get_field_ttl_ms(b"h", b"c"), ttl_before);
    }

    #[test]
    fn test_hincrbyfloat_preserves_ttl() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"c", b"10.5")]);
        assert_eq!(
            hexpire(&mut db, &make_args(&[b"h", b"60", b"FIELDS", b"1", b"c"])),
            Frame::Array(framevec![Frame::Integer(1)])
        );
        let ttl_before = db.hash_get_field_ttl_ms(b"h", b"c");
        let r = hincrbyfloat(&mut db, &make_args(&[b"h", b"c", b"1.5"]));
        assert!(matches!(r, Frame::BulkString(_)));
        assert_eq!(db.hash_get_field_ttl_ms(b"h", b"c"), ttl_before);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Phase 197 — Lazy + active expiration for hash fields with per-field TTL.
    // RED tests: must FAIL until the helpers + read-command refactor land.
    // ──────────────────────────────────────────────────────────────────────────

    /// Set an absolute-ms expiry on `field` inside hash at `key`.
    fn expire_field_at(db: &mut Database, key: &[u8], field: &[u8], abs_ms: u64) {
        let s = abs_ms.to_string();
        let r = hpexpireat(
            db,
            &make_args(&[key, s.as_bytes(), b"FIELDS", b"1", field]),
        );
        assert_eq!(
            r,
            Frame::Array(framevec![Frame::Integer(1)]),
            "hpexpireat must succeed for test setup"
        );
    }

    #[test]
    fn test_hget_skips_expired_field() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"alive", b"yes")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        assert_eq!(hget(&mut db, &make_args(&[b"h", b"f"])), Frame::Null);
        assert_eq!(
            hget(&mut db, &make_args(&[b"h", b"alive"])),
            Frame::BulkString(Bytes::copy_from_slice(b"yes"))
        );
    }

    #[test]
    fn test_hgetall_omits_expired_fields() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        let result = hgetall(&mut db, &make_args(&[b"h"]));
        match result {
            Frame::Array(ref items) => {
                let keys: Vec<&[u8]> = items
                    .iter()
                    .step_by(2)
                    .filter_map(|fr| {
                        if let Frame::BulkString(b) = fr { Some(b.as_ref()) } else { None }
                    })
                    .collect();
                assert!(!keys.iter().any(|s| *s == b"f"), "expired field must be absent");
                assert!(keys.iter().any(|s| *s == b"g"), "live field must be present");
            }
            _ => panic!("expected Array from HGETALL"),
        }
    }

    #[test]
    fn test_hexists_returns_zero_for_expired_field() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        assert_eq!(hexists(&mut db, &make_args(&[b"h", b"f"])), Frame::Integer(0));
    }

    #[test]
    fn test_hlen_excludes_expired_fields() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        assert_eq!(hlen(&mut db, &make_args(&[b"h"])), Frame::Integer(1));
    }

    #[test]
    fn test_hmget_returns_nil_for_expired_field() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        let result = hmget(&mut db, &make_args(&[b"h", b"f", b"g"]));
        assert_eq!(
            result,
            Frame::Array(
                vec![
                    Frame::Null,
                    Frame::BulkString(Bytes::copy_from_slice(b"w")),
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_hkeys_omits_expired_fields() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        let result = hkeys(&mut db, &make_args(&[b"h"]));
        match result {
            Frame::Array(ref items) => {
                let found: Vec<&[u8]> = items
                    .iter()
                    .filter_map(|fr| {
                        if let Frame::BulkString(b) = fr { Some(b.as_ref()) } else { None }
                    })
                    .collect();
                assert!(!found.iter().any(|s| *s == b"f"));
                assert!(found.iter().any(|s| *s == b"g"));
            }
            _ => panic!("expected Array from HKEYS"),
        }
    }

    #[test]
    fn test_hvals_omits_expired_fields() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        let result = hvals(&mut db, &make_args(&[b"h"]));
        match result {
            Frame::Array(ref items) => {
                let vals: Vec<&[u8]> = items
                    .iter()
                    .filter_map(|fr| {
                        if let Frame::BulkString(b) = fr { Some(b.as_ref()) } else { None }
                    })
                    .collect();
                assert!(!vals.iter().any(|s| *s == b"v"), "expired value must be absent");
                assert!(vals.iter().any(|s| *s == b"w"), "live value must be present");
            }
            _ => panic!("expected Array from HVALS"),
        }
    }

    #[test]
    fn test_hscan_omits_expired_fields() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        let result = hscan(&mut db, &make_args(&[b"h", b"0"]));
        if let Frame::Array(ref outer) = result {
            if let Frame::Array(ref inner) = outer[1] {
                let keys: Vec<&[u8]> = inner
                    .iter()
                    .step_by(2)
                    .filter_map(|fr| {
                        if let Frame::BulkString(b) = fr { Some(b.as_ref()) } else { None }
                    })
                    .collect();
                assert!(!keys.iter().any(|s| *s == b"f"), "expired field must not appear");
                assert!(keys.iter().any(|s| *s == b"g"), "live field must appear");
                return;
            }
        }
        panic!("unexpected HSCAN return shape: {:?}", result);
    }

    #[test]
    fn test_hrandfield_never_returns_expired_field() {
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        assert_eq!(hrandfield(&mut db, &make_args(&[b"h"])), Frame::Null);
        assert_eq!(
            hrandfield(&mut db, &make_args(&[b"h", b"1"])),
            Frame::Array(framevec![])
        );
    }

    #[test]
    fn test_active_tick_reaps_expired_fields() {
        use crate::server::expiration::expire_cycle_direct;
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        expire_cycle_direct(&mut db);
        assert_eq!(hget(&mut db, &make_args(&[b"h", b"f"])), Frame::Null);
        assert_eq!(hlen(&mut db, &make_args(&[b"h"])), Frame::Integer(1));
        assert_eq!(
            hget(&mut db, &make_args(&[b"h", b"g"])),
            Frame::BulkString(Bytes::copy_from_slice(b"w"))
        );
    }

    #[test]
    fn test_active_tick_downgrades_hash_when_last_ttl_drained() {
        use crate::server::expiration::expire_cycle_direct;
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v"), (b"g", b"w")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        expire_cycle_direct(&mut db);
        // After reaping the only TTL'd field, encoding must downgrade to plain Hash.
        // HEXPIRE XX on the surviving field must return -2 (no TTL).
        let r = hexpire(
            &mut db,
            &make_args(&[b"h", b"60", b"XX", b"FIELDS", b"1", b"g"]),
        );
        assert_eq!(r, Frame::Array(framevec![Frame::Integer(-2)]));
    }

    #[test]
    fn test_active_tick_deletes_hash_when_all_fields_expired() {
        use crate::server::expiration::expire_cycle_direct;
        let mut db = Database::new();
        seed_hash(&mut db, b"h", &[(b"f", b"v")]);
        let exp = db.now_ms() + 1_000;
        expire_field_at(&mut db, b"h", b"f", exp);
        db.set_cached_now_ms_for_test(exp + 1);
        expire_cycle_direct(&mut db);
        assert_eq!(hgetall(&mut db, &make_args(&[b"h"])), Frame::Array(framevec![]));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn test_hlen_o1_for_plain_hash_unchanged_complexity() {
        // Plain Hash (no TTL sidecar): HLEN must remain O(1) via HashMap::len().
        let mut db = Database::new();
        seed_hash(
            &mut db,
            b"h",
            &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")],
        );
        assert_eq!(hlen(&mut db, &make_args(&[b"h"])), Frame::Integer(3));
    }
}
