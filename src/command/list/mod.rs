mod list_read;
mod list_write;

use crate::protocol::Frame;

use super::helpers::extract_bytes;

// ---------------------------------------------------------------------------
// Shared helpers (used by both list_read and list_write)
// ---------------------------------------------------------------------------

/// Helper: parse i64 from a frame.
pub(crate) fn parse_i64(frame: &Frame) -> Option<i64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

/// Helper: resolve a Redis index (possibly negative) to a usize position within a list of given length.
/// Returns None if the resolved index is out of bounds.
pub(crate) fn resolve_index(index: i64, len: usize) -> Option<usize> {
    let resolved = if index < 0 { len as i64 + index } else { index };
    if resolved < 0 || resolved >= len as i64 {
        None
    } else {
        Some(resolved as usize)
    }
}

// ---------------------------------------------------------------------------
// Re-exports: read operations
// ---------------------------------------------------------------------------
pub use list_read::lindex;
pub use list_read::lindex_readonly;
pub use list_read::llen;
pub use list_read::llen_readonly;
pub use list_read::lpos;
pub use list_read::lpos_readonly;
pub use list_read::lrange;
pub use list_read::lrange_readonly;

// ---------------------------------------------------------------------------
// Re-exports: write operations
// ---------------------------------------------------------------------------
pub use list_write::linsert;
pub use list_write::lmove;
pub use list_write::lpop;
pub use list_write::lpush;
pub use list_write::lrem;
pub use list_write::lset;
pub use list_write::ltrim;
pub use list_write::rpop;
pub use list_write::rpush;

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::framevec;
    use crate::storage::Database;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn setup_list(db: &mut Database, key: &[u8], elements: &[&[u8]]) {
        for elem in elements {
            rpush(db, &[bs(key), bs(elem)]);
        }
    }

    // --- LPUSH tests ---

    #[test]
    fn test_lpush_basic() {
        let mut db = Database::new();
        let result = lpush(&mut db, &[bs(b"mylist"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(3));
        // LPUSH a b c -> [c, b, a] (each pushed to front in order)
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(range, Frame::Array(framevec![bs(b"c"), bs(b"b"), bs(b"a")]));
    }

    #[test]
    fn test_lpush_wrong_args() {
        let mut db = Database::new();
        let result = lpush(&mut db, &[bs(b"mylist")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- RPUSH tests ---

    #[test]
    fn test_rpush_basic() {
        let mut db = Database::new();
        let result = rpush(&mut db, &[bs(b"mylist"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(3));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(range, Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")]));
    }

    // --- LPOP tests ---

    #[test]
    fn test_lpop_single() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"a")));
    }

    #[test]
    fn test_lpop_with_count() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lpop(&mut db, &[bs(b"mylist"), bs(b"2")]);
        assert_eq!(result, Frame::Array(framevec![bs(b"a"), bs(b"b")]));
    }

    #[test]
    fn test_lpop_empty() {
        let mut db = Database::new();
        let result = lpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_lpop_removes_empty_list() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a"]);
        lpop(&mut db, &[bs(b"mylist")]);
        assert!(!db.exists(b"mylist"));
    }

    // --- RPOP tests ---

    #[test]
    fn test_rpop_single() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = rpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
    }

    #[test]
    fn test_rpop_with_count() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = rpop(&mut db, &[bs(b"mylist"), bs(b"2")]);
        assert_eq!(result, Frame::Array(framevec![bs(b"c"), bs(b"b")]));
    }

    #[test]
    fn test_rpop_empty() {
        let mut db = Database::new();
        let result = rpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Null);
    }

    // --- LLEN tests ---

    #[test]
    fn test_llen() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = llen(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_llen_missing() {
        let mut db = Database::new();
        let result = llen(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- LRANGE tests ---

    #[test]
    fn test_lrange_positive_indices() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"d"]);
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"1"), bs(b"2")]);
        assert_eq!(result, Frame::Array(framevec![bs(b"b"), bs(b"c")]));
    }

    #[test]
    fn test_lrange_negative_indices() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"d"]);
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"-3"), bs(b"-1")]);
        assert_eq!(
            result,
            Frame::Array(framevec![bs(b"b"), bs(b"c"), bs(b"d")])
        );
    }

    #[test]
    fn test_lrange_out_of_range_clamping() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"100")]);
        assert_eq!(
            result,
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")])
        );
    }

    #[test]
    fn test_lrange_missing_key() {
        let mut db = Database::new();
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // --- LINDEX tests ---

    #[test]
    fn test_lindex_positive() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lindex(&mut db, &[bs(b"mylist"), bs(b"1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"b")));
    }

    #[test]
    fn test_lindex_negative() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lindex(&mut db, &[bs(b"mylist"), bs(b"-1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
    }

    #[test]
    fn test_lindex_out_of_range() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lindex(&mut db, &[bs(b"mylist"), bs(b"10")]);
        assert_eq!(result, Frame::Null);
    }

    // --- LSET tests ---

    #[test]
    fn test_lset_valid() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lset(&mut db, &[bs(b"mylist"), bs(b"1"), bs(b"x")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        let val = lindex(&mut db, &[bs(b"mylist"), bs(b"1")]);
        assert_eq!(val, Frame::BulkString(Bytes::from_static(b"x")));
    }

    #[test]
    fn test_lset_out_of_range() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lset(&mut db, &[bs(b"mylist"), bs(b"10"), bs(b"x")]);
        assert!(
            matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"ERR index out of range"))
        );
    }

    // --- LINSERT tests ---

    #[test]
    fn test_linsert_before() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = linsert(&mut db, &[bs(b"mylist"), bs(b"BEFORE"), bs(b"b"), bs(b"x")]);
        assert_eq!(result, Frame::Integer(4));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"a"), bs(b"x"), bs(b"b"), bs(b"c")])
        );
    }

    #[test]
    fn test_linsert_after() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = linsert(&mut db, &[bs(b"mylist"), bs(b"AFTER"), bs(b"b"), bs(b"x")]);
        assert_eq!(result, Frame::Integer(4));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"x"), bs(b"c")])
        );
    }

    #[test]
    fn test_linsert_pivot_not_found() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = linsert(&mut db, &[bs(b"mylist"), bs(b"BEFORE"), bs(b"z"), bs(b"x")]);
        assert_eq!(result, Frame::Integer(-1));
    }

    #[test]
    fn test_linsert_missing_key() {
        let mut db = Database::new();
        let result = linsert(&mut db, &[bs(b"mylist"), bs(b"BEFORE"), bs(b"a"), bs(b"x")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- LREM tests ---

    #[test]
    fn test_lrem_from_head() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"a", b"c", b"a"]);
        let result = lrem(&mut db, &[bs(b"mylist"), bs(b"2"), bs(b"a")]);
        assert_eq!(result, Frame::Integer(2));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(range, Frame::Array(framevec![bs(b"b"), bs(b"c"), bs(b"a")]));
    }

    #[test]
    fn test_lrem_from_tail() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"a", b"c", b"a"]);
        let result = lrem(&mut db, &[bs(b"mylist"), bs(b"-2"), bs(b"a")]);
        assert_eq!(result, Frame::Integer(2));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(range, Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")]));
    }

    #[test]
    fn test_lrem_all() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"a", b"c", b"a"]);
        let result = lrem(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"a")]);
        assert_eq!(result, Frame::Integer(3));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(range, Frame::Array(framevec![bs(b"b"), bs(b"c")]));
    }

    #[test]
    fn test_lrem_removes_empty_list() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"a"]);
        lrem(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"a")]);
        assert!(!db.exists(b"mylist"));
    }

    // --- LTRIM tests ---

    #[test]
    fn test_ltrim_subrange() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"d", b"e"]);
        let result = ltrim(&mut db, &[bs(b"mylist"), bs(b"1"), bs(b"3")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(range, Frame::Array(framevec![bs(b"b"), bs(b"c"), bs(b"d")]));
    }

    #[test]
    fn test_ltrim_to_empty() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        ltrim(&mut db, &[bs(b"mylist"), bs(b"5"), bs(b"10")]);
        assert!(!db.exists(b"mylist"));
    }

    // --- LPOS tests ---

    #[test]
    fn test_lpos_basic() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b")]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_lpos_not_found() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"z")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_lpos_with_rank() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b"), bs(b"RANK"), bs(b"2")]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_lpos_with_count() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b"), bs(b"COUNT"), bs(b"0")]);
        assert_eq!(
            result,
            Frame::Array(framevec![Frame::Integer(1), Frame::Integer(3)])
        );
    }

    #[test]
    fn test_lpos_with_count_limited() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b"), bs(b"COUNT"), bs(b"1")]);
        assert_eq!(result, Frame::Array(framevec![Frame::Integer(1)]));
    }

    #[test]
    fn test_lpos_with_maxlen() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        // MAXLEN 2 only scans first 2 elements
        let result = lpos(
            &mut db,
            &[
                bs(b"mylist"),
                bs(b"b"),
                bs(b"MAXLEN"),
                bs(b"2"),
                bs(b"COUNT"),
                bs(b"0"),
            ],
        );
        assert_eq!(result, Frame::Array(framevec![Frame::Integer(1)]));
    }

    // --- WRONGTYPE test ---

    #[test]
    fn test_wrongtype_on_string_key() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"mykey"), Bytes::from_static(b"val"));
        let result = lpush(&mut db, &[bs(b"mykey"), bs(b"a")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"WRONGTYPE")));
    }

    // --- LMOVE tests ---

    #[test]
    fn test_lmove_left_left() {
        let mut db = Database::new();
        setup_list(&mut db, b"src", &[b"a", b"b", b"c"]);
        let result = lmove(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"LEFT"), bs(b"LEFT")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"a")));
        // src should be [b, c], dst should be [a]
        let src_list = db.get_list(b"src").unwrap().unwrap();
        assert_eq!(src_list.len(), 2);
        let dst_list = db.get_list(b"dst").unwrap().unwrap();
        assert_eq!(dst_list.len(), 1);
        assert_eq!(dst_list[0], Bytes::from_static(b"a"));
    }

    #[test]
    fn test_lmove_right_right() {
        let mut db = Database::new();
        setup_list(&mut db, b"src", &[b"a", b"b", b"c"]);
        let result = lmove(
            &mut db,
            &[bs(b"src"), bs(b"dst"), bs(b"RIGHT"), bs(b"RIGHT")],
        );
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
    }

    #[test]
    fn test_lmove_empty_source() {
        let mut db = Database::new();
        let result = lmove(
            &mut db,
            &[bs(b"nosrc"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT")],
        );
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_lmove_same_key() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        // Rotate: pop right, push left -> c moves to front
        let result = lmove(
            &mut db,
            &[bs(b"mylist"), bs(b"mylist"), bs(b"RIGHT"), bs(b"LEFT")],
        );
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
        let list = db.get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list[0], Bytes::from_static(b"c"));
        assert_eq!(list[1], Bytes::from_static(b"a"));
        assert_eq!(list[2], Bytes::from_static(b"b"));
    }

    #[test]
    fn test_lmove_wrongtype_source() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
        let result = lmove(&mut db, &[bs(b"str"), bs(b"dst"), bs(b"LEFT"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"WRONGTYPE")));
    }

    #[test]
    fn test_lmove_wrongtype_destination() {
        let mut db = Database::new();
        setup_list(&mut db, b"src", &[b"a"]);
        db.set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
        let result = lmove(&mut db, &[bs(b"src"), bs(b"str"), bs(b"LEFT"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"WRONGTYPE")));
    }

    #[test]
    fn test_lmove_wrong_args() {
        let mut db = Database::new();
        let result = lmove(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_lmove_invalid_direction() {
        let mut db = Database::new();
        let result = lmove(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"UP"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"ERR syntax")));
    }
}
