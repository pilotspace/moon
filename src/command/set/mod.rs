mod set_read;
mod set_write;

use bytes::Bytes;
use std::collections::HashSet;

use crate::protocol::Frame;
use crate::storage::Database;

use super::helpers::extract_bytes;

// ---------------------------------------------------------------------------
// Shared helpers (used by both set_read and set_write)
// ---------------------------------------------------------------------------

/// Helper: parse an integer from a frame.
pub(crate) fn parse_int(frame: &Frame) -> Option<i64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Helper: simple glob match (reused pattern from key.rs).
pub(crate) fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0;
    let mut si = 0;
    let mut star_pi = usize::MAX;
    let mut star_si = usize::MAX;

    while si < string.len() {
        if pi < pattern.len() && pattern[pi] == b'\\' {
            pi += 1;
            if pi < pattern.len() && pattern[pi] == string[si] {
                pi += 1;
                si += 1;
                continue;
            }
        } else if pi < pattern.len() && pattern[pi] == b'?' {
            pi += 1;
            si += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == string[si] {
            pi += 1;
            si += 1;
            continue;
        }

        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
            continue;
        }

        return false;
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Collect sets from database as cloned HashSets to avoid borrow conflicts.
/// Returns Err(WRONGTYPE) if any key is the wrong type.
pub(crate) fn collect_sets(
    db: &mut Database,
    keys: &[&Bytes],
) -> Result<Vec<Option<HashSet<Bytes>>>, Frame> {
    let mut sets = Vec::with_capacity(keys.len());
    for key in keys {
        match db.get_set(key) {
            Ok(Some(set)) => sets.push(Some(set.clone())),
            Ok(None) => sets.push(None),
            Err(e) => return Err(e),
        }
    }
    Ok(sets)
}

// ---------------------------------------------------------------------------
// Re-exports: read operations
// ---------------------------------------------------------------------------
pub use set_read::scard;
pub use set_read::scard_readonly;
pub use set_read::sdiff;
pub use set_read::sdiff_readonly;
pub use set_read::sinter;
pub use set_read::sinter_readonly;
pub use set_read::sismember;
pub use set_read::sismember_readonly;
pub use set_read::smembers;
pub use set_read::smembers_readonly;
pub use set_read::smismember;
pub use set_read::smismember_readonly;
pub use set_read::srandmember;
pub use set_read::srandmember_readonly;
pub use set_read::sscan;
pub use set_read::sscan_readonly;
pub use set_read::sunion;
pub use set_read::sunion_readonly;

// ---------------------------------------------------------------------------
// Re-exports: write operations
// ---------------------------------------------------------------------------
pub use set_write::sadd;
pub use set_write::sdiffstore;
pub use set_write::sinterstore;
pub use set_write::spop;
pub use set_write::srem;
pub use set_write::sunionstore;

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn setup_set(db: &mut Database, key: &[u8], members: &[&[u8]]) {
        for m in members {
            sadd(db, &[bs(key), bs(m)]);
        }
    }

    // --- SADD / SREM tests ---

    #[test]
    fn test_sadd_basic() {
        let mut db = Database::new();
        let result = sadd(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(3));

        // Adding duplicates
        let result = sadd(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"d")]);
        assert_eq!(result, Frame::Integer(1)); // only "d" is new
    }

    #[test]
    fn test_srem_basic() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = srem(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"d")]);
        assert_eq!(result, Frame::Integer(1)); // only "a" removed

        // Remove remaining to trigger auto-delete
        let result = srem(&mut db, &[bs(b"myset"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(2));
        assert!(!db.exists(b"myset")); // key should be deleted
    }

    // --- SMEMBERS / SCARD tests ---

    #[test]
    fn test_smembers_and_scard() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = scard(&mut db, &[bs(b"myset")]);
        assert_eq!(result, Frame::Integer(3));

        let result = smembers(&mut db, &[bs(b"myset")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("expected array"),
        }

        // Missing key
        let result = scard(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Integer(0));

        let result = smembers(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // --- SISMEMBER / SMISMEMBER tests ---

    #[test]
    fn test_sismember() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        assert_eq!(
            sismember(&mut db, &[bs(b"myset"), bs(b"a")]),
            Frame::Integer(1)
        );
        assert_eq!(
            sismember(&mut db, &[bs(b"myset"), bs(b"c")]),
            Frame::Integer(0)
        );
        assert_eq!(
            sismember(&mut db, &[bs(b"missing"), bs(b"a")]),
            Frame::Integer(0)
        );
    }

    #[test]
    fn test_smismember() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        let result = smismember(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"c"), bs(b"b")]);
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::Integer(1),
                Frame::Integer(0),
                Frame::Integer(1)
            ])
        );
    }

    // --- SINTER tests ---

    #[test]
    fn test_sinter() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sinter(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                let members: HashSet<Bytes> = arr
                    .into_iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b,
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(members.len(), 2);
                assert!(members.contains(&Bytes::from_static(b"b")));
                assert!(members.contains(&Bytes::from_static(b"c")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sinter_missing_key() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);

        let result = sinter(&mut db, &[bs(b"s1"), bs(b"missing")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // --- SUNION tests ---

    #[test]
    fn test_sunion() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sunion(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                let members: HashSet<Bytes> = arr
                    .into_iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b,
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(members.len(), 3);
                assert!(members.contains(&Bytes::from_static(b"a")));
                assert!(members.contains(&Bytes::from_static(b"b")));
                assert!(members.contains(&Bytes::from_static(b"c")));
            }
            _ => panic!("expected array"),
        }
    }

    // --- SDIFF tests ---

    #[test]
    fn test_sdiff() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sdiff(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"a")));
            }
            _ => panic!("expected array"),
        }
    }

    // --- SINTERSTORE / SUNIONSTORE / SDIFFSTORE tests ---

    #[test]
    fn test_sinterstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sinterstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(2));

        let result = scard(&mut db, &[bs(b"dest")]);
        assert_eq!(result, Frame::Integer(2));
    }

    #[test]
    fn test_sunionstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sunionstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_sdiffstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sdiffstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(1));
    }

    // --- SRANDMEMBER tests ---

    #[test]
    fn test_srandmember_single() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = srandmember(&mut db, &[bs(b"myset")]);
        match result {
            Frame::BulkString(_) => {} // any member is fine
            _ => panic!("expected bulkstring"),
        }

        // Missing key
        let result = srandmember(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_srandmember_positive_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        // Request more than set size: should return all distinct
        let result = srandmember(&mut db, &[bs(b"myset"), bs(b"10")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // min(10, 3) = 3
                // Check uniqueness by extracting bytes into a HashSet
                let set: HashSet<Bytes> = arr
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.clone(),
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(set.len(), 3);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_srandmember_negative_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a"]);

        // Negative count: may have duplicates, always returns abs(count) elements
        let result = srandmember(&mut db, &[bs(b"myset"), bs(b"-5")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 5); // always returns 5 elements
            }
            _ => panic!("expected array"),
        }
    }

    // --- SPOP tests ---

    #[test]
    fn test_spop_single() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = spop(&mut db, &[bs(b"myset")]);
        match result {
            Frame::BulkString(_) => {}
            _ => panic!("expected bulkstring"),
        }

        assert_eq!(scard(&mut db, &[bs(b"myset")]), Frame::Integer(2));
    }

    #[test]
    fn test_spop_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = spop(&mut db, &[bs(b"myset"), bs(b"2")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("expected array"),
        }

        assert_eq!(scard(&mut db, &[bs(b"myset")]), Frame::Integer(1));
    }

    #[test]
    fn test_spop_all_auto_delete() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        let _ = spop(&mut db, &[bs(b"myset"), bs(b"10")]);
        assert!(!db.exists(b"myset")); // key should be removed
    }

    #[test]
    fn test_spop_missing_key() {
        let mut db = Database::new();
        let result = spop(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Null);

        let result = spop(&mut db, &[bs(b"missing"), bs(b"3")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // --- SSCAN tests ---

    #[test]
    fn test_sscan_basic() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = sscan(&mut db, &[bs(b"myset"), bs(b"0")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                // Should return cursor "0" (all scanned) and array of members
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"0")));
                match &arr[1] {
                    Frame::Array(members) => assert_eq!(members.len(), 3),
                    _ => panic!("expected inner array"),
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sscan_with_match() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"apple", b"banana", b"apricot"]);

        let result = sscan(&mut db, &[bs(b"myset"), bs(b"0"), bs(b"MATCH"), bs(b"ap*")]);
        match result {
            Frame::Array(arr) => match &arr[1] {
                Frame::Array(members) => {
                    assert_eq!(members.len(), 2);
                }
                _ => panic!("expected inner array"),
            },
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sscan_missing_key() {
        let mut db = Database::new();
        let result = sscan(&mut db, &[bs(b"missing"), bs(b"0")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"0")));
                assert_eq!(arr[1], Frame::Array(framevec![]));
            }
            _ => panic!("expected array"),
        }
    }

    // --- WRONGTYPE tests ---

    #[test]
    fn test_wrongtype_on_string_key() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"mystr"), Bytes::from_static(b"hello"));

        let result = sadd(&mut db, &[bs(b"mystr"), bs(b"a")]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            _ => panic!("expected WRONGTYPE error"),
        }
    }

    // --- Wrong args tests ---

    #[test]
    fn test_wrong_args() {
        let mut db = Database::new();
        match sadd(&mut db, &[bs(b"key")]) {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number")),
            _ => panic!("expected error"),
        }
        match srem(&mut db, &[bs(b"key")]) {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number")),
            _ => panic!("expected error"),
        }
    }
}
