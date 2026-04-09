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
}
