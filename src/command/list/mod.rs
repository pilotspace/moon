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
pub use list_write::lmpop;
pub use list_write::lpop;
pub use list_write::lpush;
pub use list_write::lpushx;
pub use list_write::lrem;
pub use list_write::lset;
pub use list_write::ltrim;
pub use list_write::rpop;
pub use list_write::rpoplpush;
pub use list_write::rpush;
pub use list_write::rpushx;

// ---------------------------------------------------------------------------
// Cross-shard routing rule for the list MOVE family (moon#570)
// ---------------------------------------------------------------------------

/// The reply a list MOVE owes its client when its two keys are owned by two
/// different shards.
///
/// Keeps the `CROSSSLOT` prefix every Redis client already recognises as
/// "co-locate these keys", and names the remedy moon actually supports.
pub const CROSS_SHARD_MOVE_ERROR: &[u8] =
    b"CROSSSLOT Keys in request don't hash to the same shard; \
     co-locate source and destination with a {hash} tag";

/// Is this list MOVE (`LMOVE`/`RPOPLPUSH`/`BLMOVE`/`BRPOPLPUSH`) impossible to
/// execute without losing the element, and therefore owed a refusal?
///
/// moon is shared-nothing across shards: a shard can pop only from keys it
/// owns and push only to keys it owns. A move whose source and destination
/// hash to different shards has no shard that can do both halves, and moon has
/// no cross-shard commit to split it across two. Before this check the pop and
/// the push both ran on the SOURCE's owner: the element was handed to the
/// client as the reply and written into that shard's slice under the
/// destination's name, where every normally-routed read of the destination —
/// which goes to the DESTINATION's owner — is blind to it. The client was
/// told the move succeeded and the element was gone (moon#570).
///
/// Refusing is the only answer that cannot lose the element:
///
/// * it is decided from the two key names alone, before anything is popped, so
///   there is no window in which the element exists in neither list and no
///   undo to get wrong under a race, a timeout, or a crash;
/// * splitting the move across a shard hop would trade the loss for a
///   non-atomic `LMOVE` — an intermediate state Redis never exposes — plus two
///   independent AOF records with no shared commit point.
///
/// This mirrors what moon already answers for every other operation it cannot
/// perform atomically across shards: a MULTI/EXEC body spanning shards, a
/// script spanning shards, and `MSETNX` are all `CROSSSLOT`, and cross-shard
/// `COPY` degrades to an error naming `{hash}` tags for the types it cannot
/// carry. `{hash}` tags collapse the pair onto one shard and the move works
/// exactly as it does at `--shards 1`.
///
/// Both keys are hashed with the SAME `key_to_shard` the routing layer uses,
/// so the answer is independent of which shard happens to ask.
#[must_use]
pub fn cross_shard_move_refusal(
    source: &[u8],
    destination: &[u8],
    num_shards: usize,
) -> Option<Frame> {
    if num_shards <= 1 {
        return None;
    }
    // The rotate form (`LMOVE k k L R`) is one key: never cross-shard, and
    // hashing it twice would be wasted work on the hot path.
    if source == destination {
        return None;
    }
    let src_shard = crate::shard::dispatch::key_to_shard(source, num_shards);
    let dst_shard = crate::shard::dispatch::key_to_shard(destination, num_shards);
    (src_shard != dst_shard)
        .then(|| Frame::Error(bytes::Bytes::from_static(CROSS_SHARD_MOVE_ERROR)))
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framevec;
    use crate::storage::Database;
    use bytes::Bytes;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// moon#570: the routing rule that keeps a cross-shard move from eating
    /// the element.
    #[test]
    fn cross_shard_move_refusal_only_fires_across_a_shard_boundary() {
        use crate::shard::dispatch::key_to_shard;

        // A single-shard server has no boundary to cross.
        assert!(cross_shard_move_refusal(b"src", b"dst", 1).is_none());

        // Find a genuinely cross-shard pair and a genuinely co-located one,
        // rather than trusting two literals to hash where this test wants.
        let n = 4;
        let base = key_to_shard(b"src", n);
        let far = (0..1000)
            .map(|i| format!("dst{i}"))
            .find(|k| key_to_shard(k.as_bytes(), n) != base)
            .expect("a key on another shard must exist");
        let near = (0..1000)
            .map(|i| format!("dst{i}"))
            .find(|k| key_to_shard(k.as_bytes(), n) == base)
            .expect("a key on the same shard must exist");

        let refused = cross_shard_move_refusal(b"src", far.as_bytes(), n);
        match refused {
            Some(Frame::Error(e)) => assert!(
                e.starts_with(b"CROSSSLOT"),
                "clients key off the CROSSSLOT prefix, got {:?}",
                String::from_utf8_lossy(&e)
            ),
            other => panic!("cross-shard pair must be refused, got {other:?}"),
        }
        assert!(
            cross_shard_move_refusal(b"src", near.as_bytes(), n).is_none(),
            "a co-located pair must NOT be refused — {{hash}} tags are the documented remedy"
        );

        // The rotate form is one key: same shard by construction, and it must
        // not depend on the hash comparison to notice.
        assert!(cross_shard_move_refusal(b"k", b"k", 64).is_none());

        // A `{hash}` tag co-locates regardless of the rest of the name.
        assert!(cross_shard_move_refusal(b"{t}:src", b"{t}:dst", 8).is_none());
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
        db.set_string(b"mykey", Bytes::from_static(b"val"));
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
        db.set_string(b"str", Bytes::from_static(b"val"));
        let result = lmove(&mut db, &[bs(b"str"), bs(b"dst"), bs(b"LEFT"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"WRONGTYPE")));
    }

    #[test]
    fn test_lmove_wrongtype_destination() {
        let mut db = Database::new();
        setup_list(&mut db, b"src", &[b"a"]);
        db.set_string(b"str", Bytes::from_static(b"val"));
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

    // -----------------------------------------------------------------
    // WS6 — container-growth memory accounting (src/storage/db.rs).
    // -----------------------------------------------------------------

    #[test]
    fn test_estimated_memory_rises_with_rpush_growth() {
        let mut db = Database::new();
        let big = vec![b'v'; 200]; // forces the full-VecDeque path
        rpush(&mut db, &[bs(b"l"), bs(&big)]);
        let one = db.estimated_memory();
        for _ in 1..50 {
            rpush(&mut db, &[bs(b"l"), bs(&big)]);
        }
        let many = db.estimated_memory();
        assert!(
            many > one + 49 * 200,
            "estimated_memory must rise proportionally with RPUSH growth: \
             one={one} many={many}"
        );
    }

    #[test]
    fn test_estimated_memory_falls_with_lpop() {
        let mut db = Database::new();
        let big = vec![b'v'; 200];
        for _ in 0..50 {
            rpush(&mut db, &[bs(b"l"), bs(&big)]);
        }
        let grown = db.estimated_memory();
        for _ in 0..49 {
            lpop(&mut db, &[bs(b"l")]);
        }
        let drained = db.estimated_memory();
        assert!(
            drained < grown,
            "estimated_memory must fall as elements are popped: grown={grown} drained={drained}"
        );
        // Pop the last element -- key removed entirely, cost returns to zero.
        lpop(&mut db, &[bs(b"l")]);
        assert_eq!(
            db.estimated_memory(),
            0,
            "estimated_memory must return to zero once the list is fully drained"
        );
    }

    #[test]
    fn test_estimated_memory_lset_overwrite_nets_correct_delta() {
        let mut db = Database::new();
        let small = vec![b'a'; 10];
        let big = vec![b'b'; 5_000];
        rpush(&mut db, &[bs(b"l"), bs(&small)]);
        let small_mem = db.estimated_memory();

        lset(&mut db, &[bs(b"l"), bs(b"0"), bs(&big)]);
        let big_mem = db.estimated_memory();
        assert!(
            big_mem > small_mem + 4_000,
            "LSET growing an element must charge the net delta: \
             small={small_mem} big={big_mem}"
        );

        lset(&mut db, &[bs(b"l"), bs(b"0"), bs(&small)]);
        let shrunk_mem = db.estimated_memory();
        assert!(
            shrunk_mem < big_mem,
            "LSET shrinking an element must credit the net delta: \
             big={big_mem} shrunk={shrunk_mem}"
        );
    }

    #[test]
    fn test_estimated_memory_ltrim_credits_dropped_elements() {
        let mut db = Database::new();
        let big = vec![b'v'; 200];
        for _ in 0..50 {
            rpush(&mut db, &[bs(b"l"), bs(&big)]);
        }
        let grown = db.estimated_memory();
        // Keep only the first 5 elements -- 45 must be credited back.
        ltrim(&mut db, &[bs(b"l"), bs(b"0"), bs(b"4")]);
        let trimmed = db.estimated_memory();
        assert!(
            trimmed < grown,
            "LTRIM must credit dropped elements: grown={grown} trimmed={trimmed}"
        );
        assert!(
            grown - trimmed > 40 * 200,
            "LTRIM's credit must be proportional to the ~45 dropped elements"
        );
    }

    // --- LPOP/RPOP count validation ordering (moon#527) ---
    //
    // Oracle: redis-server 8.6.1, raw socket, `--shards 1`.
    //
    //   LPOP nokey abc        -> -ERR value is out of range, must be positive
    //   LPOP nokey -1         -> -ERR value is out of range, must be positive
    //   LPOP existingkey abc  -> -ERR value is out of range, must be positive
    //   LPOP nokey 2          -> *-1        (well-formed count, absent key)
    //
    // Redis parses the optional count BEFORE `lookupKeyWrite`, so an argument
    // error never depends on whether the key happens to exist.

    const COUNT_ERR: &[u8] = b"ERR value is out of range, must be positive";

    fn err_bytes(f: &Frame) -> Bytes {
        match f {
            Frame::Error(e) => e.clone(),
            other => panic!("expected an error frame, got {other:?}"),
        }
    }

    #[test]
    fn test_lpop_bad_count_on_missing_key_is_an_error() {
        let mut db = Database::new();
        assert_eq!(
            err_bytes(&lpop(&mut db, &[bs(b"nokey"), bs(b"abc")])),
            Bytes::from_static(COUNT_ERR)
        );
        assert_eq!(
            err_bytes(&lpop(&mut db, &[bs(b"nokey"), bs(b"-1")])),
            Bytes::from_static(COUNT_ERR)
        );
    }

    #[test]
    fn test_rpop_bad_count_on_missing_key_is_an_error() {
        let mut db = Database::new();
        assert_eq!(
            err_bytes(&rpop(&mut db, &[bs(b"nokey"), bs(b"abc")])),
            Bytes::from_static(COUNT_ERR)
        );
        assert_eq!(
            err_bytes(&rpop(&mut db, &[bs(b"nokey"), bs(b"-1")])),
            Bytes::from_static(COUNT_ERR)
        );
    }

    #[test]
    fn test_lpop_rpop_bad_count_error_text_matches_redis() {
        let mut db = Database::new();
        setup_list(&mut db, b"existing", &[b"a", b"b"]);
        assert_eq!(
            err_bytes(&lpop(&mut db, &[bs(b"existing"), bs(b"abc")])),
            Bytes::from_static(COUNT_ERR)
        );
        assert_eq!(
            err_bytes(&lpop(&mut db, &[bs(b"existing"), bs(b"-1")])),
            Bytes::from_static(COUNT_ERR)
        );
        assert_eq!(
            err_bytes(&rpop(&mut db, &[bs(b"existing"), bs(b"abc")])),
            Bytes::from_static(COUNT_ERR)
        );
        assert_eq!(
            err_bytes(&rpop(&mut db, &[bs(b"existing"), bs(b"-1")])),
            Bytes::from_static(COUNT_ERR)
        );
    }

    /// A WELL-FORMED count against an absent key must still answer the null
    /// ARRAY (`*-1`) — the #482 contract this reorder must not regress.
    #[test]
    fn test_lpop_rpop_good_count_on_missing_key_still_null_array() {
        let mut db = Database::new();
        assert_eq!(
            lpop(&mut db, &[bs(b"nokey"), bs(b"2")]),
            Frame::NullArray,
            "LPOP nokey 2"
        );
        assert_eq!(
            rpop(&mut db, &[bs(b"nokey"), bs(b"2")]),
            Frame::NullArray,
            "RPOP nokey 2"
        );
        // No-count form's miss stays the null STRING.
        assert_eq!(lpop(&mut db, &[bs(b"nokey")]), Frame::Null, "LPOP nokey");
        assert_eq!(rpop(&mut db, &[bs(b"nokey")]), Frame::Null, "RPOP nokey");
        // A zero count on an absent key is still a miss, not an empty array.
        assert_eq!(
            lpop(&mut db, &[bs(b"nokey"), bs(b"0")]),
            Frame::NullArray,
            "LPOP nokey 0"
        );
        // ...and a zero count on a PRESENT key is the empty array.
        setup_list(&mut db, b"present", &[b"a"]);
        assert_eq!(
            lpop(&mut db, &[bs(b"present"), bs(b"0")]),
            Frame::Array(framevec![]),
            "LPOP present 0"
        );
    }

    /// Validating the count before the lookup must NOT create the key.
    #[test]
    fn test_lpop_bad_count_does_not_create_key() {
        let mut db = Database::new();
        let _ = lpop(&mut db, &[bs(b"ghost"), bs(b"abc")]);
        let _ = lpop(&mut db, &[bs(b"ghost2"), bs(b"2")]);
        assert_eq!(llen(&mut db, &[bs(b"ghost")]), Frame::Integer(0));
        assert_eq!(llen(&mut db, &[bs(b"ghost2")]), Frame::Integer(0));
    }
}
