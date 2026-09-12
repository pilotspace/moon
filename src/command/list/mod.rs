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
    // ── moon#832: a read on the MUTABLE path must not flatten the encoding ──
    //
    // Red on b04e8990: LLEN/LINDEX/LRANGE/LPOS reached the list through
    // `Database::get_list` -> `get_promoted` -> `ListKind::upgrade`, an
    // unconditional one-way conversion. `RPUSH l a b c` then `LLEN l` on the
    // mutable path left the key a `linkedlist` forever (verified against a
    // live server: `listpack` after RPUSH, `listpack` after a plain LLEN,
    // `linkedlist` after the same LLEN inside MULTI/EXEC).

    fn encoding_of_832(db: &mut Database, key: &[u8]) -> &'static str {
        db.get(key)
            .map(|e| e.value.as_redis_value().encoding_name())
            .unwrap_or("<missing>")
    }

    #[test]
    fn read_on_mutable_path_keeps_listpack_encoding() {
        #[allow(clippy::type_complexity)]
        let handlers: &[(&str, fn(&mut Database))] = &[
            ("LLEN", |db| {
                llen(db, &[bs(b"l")]);
            }),
            ("LINDEX", |db| {
                lindex(db, &[bs(b"l"), bs(b"0")]);
            }),
            ("LRANGE", |db| {
                lrange(db, &[bs(b"l"), bs(b"0"), bs(b"-1")]);
            }),
            ("LPOS", |db| {
                lpos(db, &[bs(b"l"), bs(b"b")]);
            }),
        ];
        for (name, call) in handlers {
            let mut db = Database::new();
            rpush(&mut db, &[bs(b"l"), bs(b"a"), bs(b"b"), bs(b"c")]);
            assert_eq!(
                encoding_of_832(&mut db, b"l"),
                "listpack",
                "{name}: fixture must start compact, or the test proves nothing"
            );
            call(&mut db);
            assert_eq!(
                encoding_of_832(&mut db, b"l"),
                "listpack",
                "{name} flattened the list: a read on the mutable dispatch path rewrote the encoding (moon#832)"
            );
        }
    }

    /// The reads must still ANSWER correctly straight off the listpack.
    #[test]
    fn read_on_mutable_path_still_answers_from_the_listpack() {
        let mut db = Database::new();
        rpush(&mut db, &[bs(b"l"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(llen(&mut db, &[bs(b"l")]), Frame::Integer(3));
        assert_eq!(lindex(&mut db, &[bs(b"l"), bs(b"1")]), bs(b"b"));
        assert_eq!(lpos(&mut db, &[bs(b"l"), bs(b"c")]), Frame::Integer(2));
        let Frame::Array(items) = lrange(&mut db, &[bs(b"l"), bs(b"0"), bs(b"-1")]) else {
            panic!("LRANGE must answer an array");
        };
        assert_eq!(items.len(), 3);
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");
    }

    /// The list's elements, read through the SHARED-read twin.
    ///
    /// `lrange` on the mutable path is `get_promoted` and would flatten the
    /// very encoding under test on the first call, leaving every later
    /// assertion measuring a `linkedlist` (moon#832). `now_ms = 0` is safe:
    /// no key here carries a TTL.
    fn ro_elements(db: &Database, key: &[u8]) -> Vec<Vec<u8>> {
        match lrange_readonly(db, &[bs(key), bs(b"0"), bs(b"-1")], 0) {
            Frame::Array(items) => items
                .iter()
                .map(|f| match f {
                    Frame::BulkString(b) => b.to_vec(),
                    other => panic!("expected bulk, got {other:?}"),
                })
                .collect(),
            other => panic!("LRANGE did not reply an array: {other:?}"),
        }
    }

    // ── moon#897: LSET / LPOP / RPOP must not flatten the list they touch ──
    //
    // Measured on f7c83769 against a redis 8.6.1 oracle, same host, one
    // shard: a three-element list built one `RPUSH` at a time reported
    // `listpack`, then ONE `LSET`, `LPOP` or `RPOP` reported `linkedlist`
    // where redis still reported `listpack`. Nothing demotes (moon#832), so
    // the promotion is permanent — and `LPOP` is the QUEUE primitive, so a
    // small work queue flattened on its first pop and never came back.
    //
    // Two mechanisms, both gone from the compact path: `get_or_create_list`
    // (whose `ListKind::upgrade` materialises the `VecDeque` unconditionally)
    // and, in the pops, a `db.get_list(key)` EXISTENCE probe — `get_promoted`
    // — that flattened the list before the pop had even started.
    //
    // These guards are proven able to FAIL by MUTATION, not by removal:
    // making `list_route` answer `ListRoute::Full` for a listpack (the
    // pre-moon#897 verdict) turns every `assert_eq!(…, "listpack")` below red
    // while the value assertions stay green — which is the point, since the
    // values were never wrong.

    #[test]
    fn lset_keeps_a_small_listpack_list_a_listpack() {
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
        assert_eq!(
            encoding_of_832(&mut db, b"l"),
            "listpack",
            "fixture precondition"
        );

        assert_eq!(
            lset(&mut db, &[bs(b"l"), bs(b"1"), bs(b"B")]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            encoding_of_832(&mut db, b"l"),
            "listpack",
            "moon#897: LSET flattened a 3-element listpack list; redis 8.6.1 keeps it a listpack"
        );
        assert_eq!(
            lrange_readonly(&db, &[bs(b"l"), bs(b"0"), bs(b"-1")], 0),
            Frame::Array(framevec![bs(b"a"), bs(b"B"), bs(b"c")])
        );

        // A negative index resolves the same way and is equally in-place.
        assert_eq!(
            lset(&mut db, &[bs(b"l"), bs(b"-1"), bs(b"C")]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");
        assert_eq!(
            lrange_readonly(&db, &[bs(b"l"), bs(b"0"), bs(b"-1")], 0),
            Frame::Array(framevec![bs(b"a"), bs(b"B"), bs(b"C")])
        );
    }

    #[test]
    fn lpop_and_rpop_keep_a_small_listpack_list_a_listpack() {
        // `LPOP` is the one moon#897 names; `RPOP` is the same state writer
        // from the other end and took the same two flattening accessors.
        for (name, popped, rest) in [
            ("LPOP", &b"a"[..], vec![b"b".to_vec(), b"c".to_vec()]),
            ("RPOP", &b"c"[..], vec![b"a".to_vec(), b"b".to_vec()]),
        ] {
            let mut db = Database::new();
            setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
            assert_eq!(encoding_of_832(&mut db, b"l"), "listpack", "{name} fixture");

            let got = if name == "LPOP" {
                lpop(&mut db, &[bs(b"l")])
            } else {
                rpop(&mut db, &[bs(b"l")])
            };
            assert_eq!(
                got,
                Frame::BulkString(Bytes::copy_from_slice(popped)),
                "{name} returned the wrong element"
            );
            assert_eq!(
                encoding_of_832(&mut db, b"l"),
                "listpack",
                "moon#897: {name} flattened a 3-element listpack list; \
                 redis 8.6.1 keeps it a listpack"
            );
            assert_eq!(ro_elements(&db, b"l"), rest, "{name} left the wrong list");
        }
    }

    #[test]
    fn lpop_with_a_count_keeps_the_listpack_and_its_reply_shape() {
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
        assert_eq!(
            lpop(&mut db, &[bs(b"l"), bs(b"2")]),
            Frame::Array(framevec![bs(b"a"), bs(b"b")])
        );
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");
        assert_eq!(ro_elements(&db, b"l"), vec![b"c".to_vec()]);

        // COUNT 0 is an EMPTY array on a live list and must touch nothing.
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
        assert_eq!(
            lpop(&mut db, &[bs(b"l"), bs(b"0")]),
            Frame::Array(framevec![])
        );
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");
        assert_eq!(
            ro_elements(&db, b"l"),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );

        // COUNT past the length drains it and deletes the key.
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
        assert_eq!(
            lpop(&mut db, &[bs(b"l"), bs(b"99")]),
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")])
        );
        assert_eq!(encoding_of_832(&mut db, b"l"), "<missing>");

        // RPOP with a count pops from the back, in back-to-front order.
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
        assert_eq!(
            rpop(&mut db, &[bs(b"l"), bs(b"2")]),
            Frame::Array(framevec![bs(b"c"), bs(b"b")])
        );
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");
    }

    #[test]
    fn list_secondary_writes_still_promote_past_the_threshold() {
        // The fix must not disable the encoding policy it preserves. Both
        // sides of the entry boundary, because a guard that only checks the
        // compact side cannot tell "preserved" from "never promotes".
        let limits = crate::storage::db::EncodingLimits::moon_defaults();
        let n = limits.list_entries;
        for (count, want) in [(n, "listpack"), (n + 1, "linkedlist")] {
            for op in ["LSET", "LPOP", "RPOP"] {
                let mut db = Database::new();
                let owned: Vec<Vec<u8>> = (0..count)
                    .map(|i| format!("e{i:05}").into_bytes())
                    .collect();
                for e in &owned {
                    rpush(&mut db, &[bs(b"l"), bs(e)]);
                }
                assert_eq!(
                    encoding_of_832(&mut db, b"l"),
                    want,
                    "{op} fixture at {count} elements"
                );
                match op {
                    "LSET" => {
                        lset(&mut db, &[bs(b"l"), bs(b"0"), bs(b"REPLACED")]);
                    }
                    "LPOP" => {
                        lpop(&mut db, &[bs(b"l")]);
                    }
                    _ => {
                        rpop(&mut db, &[bs(b"l")]);
                    }
                }
                assert_eq!(
                    encoding_of_832(&mut db, b"l"),
                    want,
                    "{op} changed the encoding at {count} elements"
                );
            }
        }

        // The element-size dimension is LSET's alone: the count is unchanged
        // by a replacement, but the replacement element can be too long. moon's
        // list value threshold is 64 B — the same one its own `RPUSH` gate
        // applies — so a 64 B element stays compact and a 65 B one promotes.
        // (redis's list has no element limit at all; moon's list policy has
        // diverged from redis's `list-max-listpack-size -2` byte budget since
        // long before this change, and flipping it is a separate decision.)
        for (len, want) in [
            (limits.set_value, "listpack"),
            (limits.set_value + 1, "linkedlist"),
        ] {
            let mut db = Database::new();
            setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
            assert_eq!(encoding_of_832(&mut db, b"l"), "listpack", "fixture");
            let big = vec![b'v'; len];
            assert_eq!(
                lset(&mut db, &[bs(b"l"), bs(b"1"), bs(&big)]),
                Frame::SimpleString(Bytes::from_static(b"OK"))
            );
            assert_eq!(
                encoding_of_832(&mut db, b"l"),
                want,
                "LSET of a {len} B element must leave the list a {want}"
            );
            // Promoted or not, the value must be there and be exact.
            assert_eq!(
                lindex_readonly(&db, &[bs(b"l"), bs(b"1")], 0),
                Frame::BulkString(Bytes::copy_from_slice(&big)),
                "LSET lost the element it wrote at {len} B"
            );
            assert_eq!(
                ro_elements(&db, b"l"),
                vec![b"a".to_vec(), big.clone(), b"c".to_vec()]
            );
        }
    }

    #[test]
    fn list_secondary_writes_hold_byte_transparency() {
        // moon#795/#903: the compact encodings are NOT byte-transparent for
        // numeric-looking strings unless every value entering one goes through
        // the canonical-integer rule. `+5`, `000000012345` and `-0` must
        // survive an LPOP of an unrelated element — and must go IN through
        // LSET and come back out unchanged.
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"victim", b"+5", b"000000012345", b"-0"]);
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack", "fixture");
        assert_eq!(
            lpop(&mut db, &[bs(b"l")]),
            Frame::BulkString(Bytes::from_static(b"victim"))
        );
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");
        assert_eq!(
            ro_elements(&db, b"l"),
            vec![b"+5".to_vec(), b"000000012345".to_vec(), b"-0".to_vec()],
            "an element's bytes changed across LPOP"
        );

        // LSET writes one in.
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
        for (i, spelling) in [&b"+5"[..], &b"000000012345"[..], &b"-0"[..]]
            .iter()
            .enumerate()
        {
            assert_eq!(
                lset(
                    &mut db,
                    &[bs(b"l"), bs(i.to_string().as_bytes()), bs(spelling)]
                ),
                Frame::SimpleString(Bytes::from_static(b"OK"))
            );
        }
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");
        assert_eq!(
            ro_elements(&db, b"l"),
            vec![b"+5".to_vec(), b"000000012345".to_vec(), b"-0".to_vec()],
            "LSET did not write a numeric-looking element back verbatim"
        );
    }

    #[test]
    fn list_secondary_write_semantics_are_unchanged_by_the_routing() {
        // Every non-encoding answer must be exactly what it was — and what
        // redis 8.6.1 answers.

        // LPOP / RPOP on a missing key: `Null` bare, `NullArray` with a count.
        let mut db = Database::new();
        assert_eq!(lpop(&mut db, &[bs(b"nokey")]), Frame::Null);
        assert_eq!(lpop(&mut db, &[bs(b"nokey"), bs(b"2")]), Frame::NullArray);
        assert_eq!(rpop(&mut db, &[bs(b"nokey")]), Frame::Null);
        assert_eq!(rpop(&mut db, &[bs(b"nokey"), bs(b"2")]), Frame::NullArray);
        assert_eq!(encoding_of_832(&mut db, b"nokey"), "<missing>");

        // A bad count is refused BEFORE the key is looked at, so the same
        // malformed command answers the same way whether the key exists or
        // not (moon#527).
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a"]);
        for key in [&b"l"[..], &b"nokey"[..]] {
            for bad in [&b"-1"[..], &b"xyz"[..]] {
                match lpop(&mut db, &[bs(key), bs(bad)]) {
                    Frame::Error(e) => assert_eq!(
                        &e[..],
                        b"ERR value is out of range, must be positive",
                        "LPOP {} {}",
                        String::from_utf8_lossy(key),
                        String::from_utf8_lossy(bad)
                    ),
                    other => panic!("expected the count error, got {other:?}"),
                }
            }
        }
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");

        // Emptying a list deletes its key, from either end.
        for op in ["LPOP", "RPOP"] {
            let mut db = Database::new();
            setup_list(&mut db, b"l", &[b"only"]);
            let got = if op == "LPOP" {
                lpop(&mut db, &[bs(b"l")])
            } else {
                rpop(&mut db, &[bs(b"l")])
            };
            assert_eq!(got, Frame::BulkString(Bytes::from_static(b"only")));
            assert_eq!(
                encoding_of_832(&mut db, b"l"),
                "<missing>",
                "{op} left the key behind after emptying it"
            );
        }

        // LSET error strings and their ORDER.
        let mut db = Database::new();
        setup_list(&mut db, b"l", &[b"a", b"b", b"c"]);
        for ix in [&b"9"[..], &b"-9"[..]] {
            match lset(&mut db, &[bs(b"l"), bs(ix), bs(b"x")]) {
                Frame::Error(e) => assert_eq!(&e[..], b"ERR index out of range"),
                other => panic!("expected index error, got {other:?}"),
            }
        }
        match lset(&mut db, &[bs(b"l"), bs(b"abc"), bs(b"x")]) {
            Frame::Error(e) => assert_eq!(&e[..], b"ERR value is not an integer or out of range"),
            other => panic!("expected index-parse error, got {other:?}"),
        }
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack");

        // WRONGTYPE, decided by the `&self` router before any mutation.
        let mut db = Database::new();
        crate::command::string::set(&mut db, &[bs(b"str"), bs(b"v")]);
        for got in [
            lpop(&mut db, &[bs(b"str")]),
            rpop(&mut db, &[bs(b"str")]),
            lset(&mut db, &[bs(b"str"), bs(b"0"), bs(b"x")]),
        ] {
            match got {
                Frame::Error(e) => assert!(
                    e.starts_with(b"WRONGTYPE"),
                    "expected WRONGTYPE, got {:?}",
                    String::from_utf8_lossy(&e)
                ),
                other => panic!("expected WRONGTYPE, got {other:?}"),
            }
        }
    }

    /// moon#830, fixed for free by the moon#897 routing.
    ///
    /// `LSET` used to reach for `get_or_create_list` BEFORE it could answer
    /// "no such key", so the error came back AND an empty list was left in the
    /// keyspace — charged, `DBSIZE`-visible, and never propagated to the AOF
    /// or a replica (propagation is gated on the reply not being an error).
    /// The non-creating `&self` router the encoding fix needs answers the
    /// question without the create half.
    ///
    /// This assertion is red on the pre-fix tree, and is what makes the #830
    /// claim in the PR body checkable rather than asserted.
    #[test]
    fn lset_on_a_missing_key_does_not_create_it() {
        let mut db = Database::new();
        match lset(&mut db, &[bs(b"ghost"), bs(b"0"), bs(b"v")]) {
            Frame::Error(e) => assert_eq!(&e[..], b"ERR no such key"),
            other => panic!("expected 'ERR no such key', got {other:?}"),
        }
        assert_eq!(
            encoding_of_832(&mut db, b"ghost"),
            "<missing>",
            "moon#830: LSET fabricated the key it had just said did not exist"
        );
        assert_eq!(
            db.logical_len(),
            0,
            "moon#830: the fabricated key is DBSIZE-visible"
        );
        assert_eq!(
            db.estimated_memory(),
            0,
            "moon#830: the fabricated key was charged to the ledger"
        );
    }

    /// The post-pop upgrade check is NOT decoration — the `SREM` twin of this
    /// argument, for the list arm. See
    /// `srem_promotes_a_listpack_that_a_tightened_policy_no_longer_fits`.
    ///
    /// Mutating `pop_listpack`'s check to `let should_upgrade = false;` turns
    /// this red and leaves every other list test green.
    #[test]
    fn lpop_promotes_a_listpack_that_a_tightened_policy_no_longer_fits() {
        let loose = crate::storage::db::EncodingLimits::moon_defaults();
        let tight = crate::storage::db::EncodingLimits {
            list_entries: 4,
            ..loose
        };

        let mut db = Database::new();
        for i in 0..20u32 {
            rpush(&mut db, &[bs(b"l"), bs(format!("e{i:03}").as_bytes())]);
        }
        assert_eq!(encoding_of_832(&mut db, b"l"), "listpack", "fixture");
        db.set_encoding_limits(tight);
        assert_eq!(
            encoding_of_832(&mut db, b"l"),
            "listpack",
            "the setter must not re-encode an existing container"
        );
        assert_eq!(
            lpop(&mut db, &[bs(b"l")]),
            Frame::BulkString(Bytes::from_static(b"e000"))
        );
        assert_eq!(
            encoding_of_832(&mut db, b"l"),
            "linkedlist",
            "LPOP left a 19-element listpack under a 4-element policy"
        );
        assert_eq!(ro_elements(&db, b"l").len(), 19);

        // A container that DOES fit the tightened policy is left alone.
        let mut db = Database::new();
        db.set_encoding_limits(tight);
        setup_list(&mut db, b"t", &[b"a", b"b", b"c"]);
        assert_eq!(encoding_of_832(&mut db, b"t"), "listpack");
        lpop(&mut db, &[bs(b"t")]);
        assert_eq!(encoding_of_832(&mut db, b"t"), "listpack");
    }
}

#[cfg(test)]
mod listpack_batch_overflow_tests {
    use crate::protocol::Frame;
    use crate::storage::Database;
    use bytes::Bytes;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// moon#865. The listpack header counts elements in a `u16`. Before the
    /// fix, one command's entries were all pushed before the entry count was
    /// compared with the entry threshold, so a batch past 65_536 wrapped the
    /// count and the container silently forgot everything before the wrap --
    /// while the server acknowledged the write.
    ///
    /// Measured pre-fix: `LLEN` returned 4464 (= 70_000 - 65_536).
    #[test]
    fn rpush_one_call_past_u16_keeps_every_element() {
        let mut db = Database::new();
        const N: usize = 70_000;
        let owned: Vec<Vec<u8>> = (0..N).map(|i| format!("e{i:07}").into_bytes()).collect();
        let mut args: Vec<Frame> = Vec::with_capacity(N + 1);
        args.push(bs(b"biglist"));
        args.extend(owned.iter().map(|m| bs(m)));

        assert_eq!(
            crate::command::list::rpush(&mut db, &args),
            Frame::Integer(N as i64),
            "RPUSH under-reported the elements it accepted"
        );
        assert_eq!(
            crate::command::list::llen(&mut db, &[bs(b"biglist")]),
            Frame::Integer(N as i64),
            "LLEN lost elements to the u16 wrap"
        );
    }

    /// Measured pre-fix: `LPUSH` reported 4464 for the same reason.
    #[test]
    fn lpush_one_call_past_u16_keeps_every_element() {
        let mut db = Database::new();
        const N: usize = 70_000;
        let owned: Vec<Vec<u8>> = (0..N).map(|i| format!("e{i:07}").into_bytes()).collect();
        let mut args: Vec<Frame> = Vec::with_capacity(N + 1);
        args.push(bs(b"biglist"));
        args.extend(owned.iter().map(|m| bs(m)));

        assert_eq!(
            crate::command::list::lpush(&mut db, &args),
            Frame::Integer(N as i64),
            "LPUSH under-reported"
        );
        assert_eq!(
            crate::command::list::llen(&mut db, &[bs(b"biglist")]),
            Frame::Integer(N as i64),
            "LLEN lost elements to the u16 wrap"
        );
    }

    /// A hash stores two listpack entries per field, so it wraps at 32_768
    /// fields. Measured pre-fix: `HLEN` returned 7232 for 40_000 fields.
    #[test]
    fn hset_one_call_past_u16_keeps_every_field() {
        let mut db = Database::new();
        const N: usize = 40_000;
        let mut owned: Vec<Vec<u8>> = Vec::with_capacity(N * 2);
        for i in 0..N {
            owned.push(format!("f{i:07}").into_bytes());
            owned.push(format!("v{i:07}").into_bytes());
        }
        let mut args: Vec<Frame> = Vec::with_capacity(N * 2 + 1);
        args.push(bs(b"bighash"));
        args.extend(owned.iter().map(|m| bs(m)));

        assert_eq!(
            crate::command::hash::hset(&mut db, &args),
            Frame::Integer(N as i64),
            "HSET under-reported"
        );
        assert_eq!(
            crate::command::hash::hlen(&mut db, &[bs(b"bighash")]),
            Frame::Integer(N as i64),
            "HLEN lost fields to the u16 wrap"
        );
    }

    /// The batch guard must not change behaviour for a batch that legitimately
    /// belongs in a listpack, nor for one that straddles the upgrade point --
    /// otherwise the fix is just "never use the encoding".
    #[test]
    fn small_and_straddling_batches_are_unchanged() {
        for n in [1usize, 8, 128, 129, 200] {
            let mut db = Database::new();
            let owned: Vec<Vec<u8>> = (0..n).map(|i| format!("e{i:05}").into_bytes()).collect();
            let mut args: Vec<Frame> = Vec::with_capacity(n + 1);
            args.push(bs(b"l"));
            args.extend(owned.iter().map(|m| bs(m)));
            assert_eq!(
                crate::command::list::rpush(&mut db, &args),
                Frame::Integer(n as i64),
                "RPUSH n={n}"
            );
            assert_eq!(
                crate::command::list::llen(&mut db, &[bs(b"l")]),
                Frame::Integer(n as i64),
                "LLEN n={n}"
            );
        }
    }

    /// Many small calls must still reach the same total: the guard is on the
    /// per-command batch, so repeated RPUSHes have to upgrade the container
    /// rather than pile into the listpack.
    #[test]
    fn many_small_calls_accumulate_past_the_listpack_ceiling() {
        let mut db = Database::new();
        const CALLS: usize = 700;
        const PER: usize = 100;
        for c in 0..CALLS {
            let owned: Vec<Vec<u8>> = (0..PER)
                .map(|i| format!("e{:07}", c * PER + i).into_bytes())
                .collect();
            let mut args: Vec<Frame> = Vec::with_capacity(PER + 1);
            args.push(bs(b"l"));
            args.extend(owned.iter().map(|m| bs(m)));
            crate::command::list::rpush(&mut db, &args);
        }
        assert_eq!(
            crate::command::list::llen(&mut db, &[bs(b"l")]),
            Frame::Integer((CALLS * PER) as i64)
        );
    }
}

/// moon#942/#788: the `used_memory` ledger across the list encoding ladder.
///
/// These guards live here, next to the commands, because moon#942's list work
/// moves WHERE two charges are applied — the pop's element credit (out of a
/// re-probe, into the borrow that popped it) and the push-if-exists gate (off
/// the flattening `get_promoted`, onto the `&self` router). A memory delta
/// that MOVES is a memory delta that can be dropped, and moon#814 is the
/// recorded consequence: a charge stranded on one branch drives `used_memory`
/// monotonically DOWN, without bound, on a path any unprivileged client can
/// drive, until `--maxmemory` can never fire.
///
/// The oracle is `recalculate_memory` — the running ledger is asserted against
/// a from-scratch recompute of the whole keyspace at every rung.
#[cfg(test)]
mod ledger_and_encoding_942 {
    use crate::protocol::Frame;
    use crate::storage::Database;
    use bytes::Bytes;

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    /// The running ledger must equal a full recompute. `step` names the rung.
    fn assert_ledger_exact(db: &mut Database, step: &str) {
        let running = db.estimated_memory();
        db.recalculate_memory();
        let recomputed = db.estimated_memory();
        assert_eq!(
            running, recomputed,
            "{step}: running ledger {running} B != full recompute {recomputed} B — \
             a list mutation site charged the wrong delta (or none at all)"
        );
    }

    /// `OBJECT ENCODING key`, read through the path that does not flatten.
    fn encoding_of(db: &mut Database, key: &[u8]) -> String {
        match crate::command::key::object(db, &[bs(b"ENCODING"), bs(key)]) {
            Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
            other => panic!("OBJECT ENCODING answered {other:?}"),
        }
    }

    /// One key walked up the whole ladder and back down to the floor:
    /// absent -> listpack -> the 128-element threshold -> linkedlist ->
    /// drained to empty -> gone.
    ///
    /// Mutation check: delete the `db.credit_memory(credit)` line from
    /// `pop_eager` and the "drained the linkedlist" rung goes red — measured
    /// at a 7,569 B running ledger against a 7,513 B recompute.
    #[test]
    fn the_list_ladder_keeps_the_ledger_exact() {
        let limits = crate::storage::db::EncodingLimits::moon_defaults();
        let mut db = Database::new();
        let floor = db.estimated_memory();
        let push = [bs(b"l"), bs(b"xxxxxxxx")];
        let pop = [bs(b"l")];

        // absent -> listpack
        assert_eq!(
            crate::command::list::lpush(&mut db, &push),
            Frame::Integer(1)
        );
        assert_eq!(encoding_of(&mut db, b"l"), "listpack", "fixture");
        assert_ledger_exact(&mut db, "absent -> listpack");

        // listpack steady state, both ends
        for _ in 0..10 {
            crate::command::list::lpush(&mut db, &push);
            crate::command::list::rpush(&mut db, &push);
        }
        assert_eq!(encoding_of(&mut db, b"l"), "listpack", "fixture");
        assert_ledger_exact(&mut db, "listpack steady state");

        // a pop off each end, still a listpack (moon#897)
        assert!(matches!(
            crate::command::list::lpop(&mut db, &pop),
            Frame::BulkString(_)
        ));
        assert!(matches!(
            crate::command::list::rpop(&mut db, &pop),
            Frame::BulkString(_)
        ));
        assert_eq!(
            encoding_of(&mut db, b"l"),
            "listpack",
            "moon#897: a pop must not flatten the list"
        );
        assert_ledger_exact(&mut db, "listpack after a pop from each end");

        // AT the entry threshold: inclusive, so exactly `list_entries`
        // elements is still a listpack.
        // `llen` on the MUTABLE path is `get_promoted` and would flatten the
        // very encoding this rung is about (moon#832) — the length has to
        // come from the read-only twin. `now_ms = 0` is safe: no key here
        // carries a TTL.
        let have = match crate::command::list::llen_readonly(&db, &pop, 0) {
            Frame::Integer(n) => n as usize,
            other => panic!("LLEN answered {other:?}"),
        };
        for _ in have..limits.list_entries {
            crate::command::list::rpush(&mut db, &push);
        }
        assert_eq!(
            encoding_of(&mut db, b"l"),
            "listpack",
            "fixture: list-max-listpack-size is inclusive"
        );
        assert_ledger_exact(&mut db, "listpack at the entry threshold");

        // ONE past it: the promotion, and the one-time cost-model swing.
        crate::command::list::rpush(&mut db, &push);
        assert_eq!(
            encoding_of(&mut db, b"l"),
            "linkedlist",
            "fixture: one element past the threshold must promote"
        );
        assert_ledger_exact(&mut db, "listpack -> linkedlist");

        // linkedlist steady state, pushes and pops and the X forms
        crate::command::list::lpush(&mut db, &push);
        assert_ledger_exact(&mut db, "linkedlist LPUSH");
        crate::command::list::rpushx(&mut db, &push);
        assert_ledger_exact(&mut db, "linkedlist RPUSHX");
        crate::command::list::lpushx(&mut db, &push);
        assert_ledger_exact(&mut db, "linkedlist LPUSHX");
        assert!(matches!(
            crate::command::list::rpop(&mut db, &pop),
            Frame::BulkString(_)
        ));
        assert_ledger_exact(&mut db, "linkedlist RPOP");

        // Drain it dry one element at a time. The LAST pop must delete the
        // key, not leave an empty container behind.
        loop {
            match crate::command::list::lpop(&mut db, &pop) {
                Frame::BulkString(_) => {}
                Frame::Null => break,
                other => panic!("LPOP answered {other:?}"),
            }
        }
        assert_ledger_exact(&mut db, "drained the linkedlist");
        assert!(
            db.data().get(b"l").is_none(),
            "the pop that empties a list must remove the key"
        );
        assert_eq!(
            db.estimated_memory(),
            floor,
            "the full ladder must return the ledger to its floor"
        );
    }

    /// The same drain-to-empty on the COMPACT encoding, which takes the other
    /// pop branch entirely, plus a multi-element `LPOP key count`.
    ///
    /// Mutation check: replace `pop_listpack`'s `if empty` with `if false` and
    /// the "a counted pop that empties a listpack must remove the key" rung
    /// goes red.
    ///
    /// NOT `db.adjust_memory(before, after)`: that line was tried first and
    /// the test stayed green, because `Listpack::estimate_memory` bills the
    /// jemalloc size class of the buffer's CAPACITY and nothing shrinks a
    /// listpack's buffer on removal — so `before == after` on every pop and
    /// the call is a genuine no-op there. The running ledger and
    /// `recalculate_memory` agree for the same reason, which is why this
    /// rung needs the emptiness guard to carry it.
    #[test]
    fn draining_a_listpack_keeps_the_ledger_exact_and_removes_the_key() {
        let mut db = Database::new();
        let floor = db.estimated_memory();
        let push = [bs(b"l"), bs(b"xxxxxxxx")];

        for _ in 0..12 {
            crate::command::list::rpush(&mut db, &push);
        }
        assert_eq!(encoding_of(&mut db, b"l"), "listpack", "fixture");

        // The counted form, off both ends.
        assert!(matches!(
            crate::command::list::lpop(&mut db, &[bs(b"l"), bs(b"3")]),
            Frame::Array(_)
        ));
        assert!(matches!(
            crate::command::list::rpop(&mut db, &[bs(b"l"), bs(b"3")]),
            Frame::Array(_)
        ));
        assert_eq!(
            encoding_of(&mut db, b"l"),
            "listpack",
            "moon#897: a counted pop must not flatten the list either"
        );
        assert_ledger_exact(&mut db, "counted listpack pop");

        // A count LARGER than the list drains it and removes the key.
        assert!(matches!(
            crate::command::list::lpop(&mut db, &[bs(b"l"), bs(b"1000")]),
            Frame::Array(_)
        ));
        assert!(
            db.data().get(b"l").is_none(),
            "a counted pop that empties a listpack must remove the key"
        );
        assert_ledger_exact(&mut db, "drained the listpack");
        assert_eq!(db.estimated_memory(), floor, "back to the floor");

        // And the miss forms, which must not fabricate anything.
        assert_eq!(
            crate::command::list::lpop(&mut db, &[bs(b"l")]),
            Frame::Null
        );
        assert_eq!(
            crate::command::list::lpop(&mut db, &[bs(b"l"), bs(b"2")]),
            Frame::NullArray
        );
        assert_eq!(
            crate::command::list::lpushx(&mut db, &push),
            Frame::Integer(0)
        );
        assert!(
            db.data().get(b"l").is_none(),
            "a miss must fabricate nothing"
        );
        assert_eq!(db.estimated_memory(), floor, "a miss must charge nothing");
    }

    /// moon#795/#903 byte transparency across a pop: an element that LOOKS
    /// numeric goes into a listpack under the canonical-integer rule and must
    /// come back out as the exact bytes the client wrote.
    ///
    /// This is the guard on rewriting `listpack_pop_end` to decode through the
    /// BORROWED `ListpackRef` instead of the owning `ListpackEntry`: the two
    /// render an integer entry by different routes (`itoa` vs `to_string`) and
    /// must not be able to disagree.
    #[test]
    fn a_listpack_pop_returns_the_exact_bytes_that_were_pushed() {
        // `007` and `+7` are NOT canonical, so they are stored as strings;
        // `7`, `-0`… wait, `-0` is not canonical either. Both i64 limits ARE.
        let cases: [&[u8]; 8] = [
            b"7",
            b"007",
            b"+7",
            b"-0",
            b"0",
            b"-9223372036854775808",
            b"9223372036854775807",
            b"xxxxxxxx",
        ];
        for probe in cases {
            // Front and back, so both ends of `listpack_pop_end` are asked.
            for front in [true, false] {
                let mut db = Database::new();
                crate::command::list::rpush(&mut db, &[bs(b"l"), bs(probe)]);
                assert_eq!(
                    encoding_of(&mut db, b"l"),
                    "listpack",
                    "fixture: a one-element list must be a listpack"
                );
                let got = if front {
                    crate::command::list::lpop(&mut db, &[bs(b"l")])
                } else {
                    crate::command::list::rpop(&mut db, &[bs(b"l")])
                };
                assert_eq!(
                    got,
                    Frame::BulkString(Bytes::copy_from_slice(probe)),
                    "pop (front={front}) of {:?} did not round-trip its bytes \
                     (moon#795/#903)",
                    String::from_utf8_lossy(probe)
                );
            }
        }
    }

    /// moon#832, and the guard the LPUSHX/RPUSHX gate swap is pinned against:
    /// these two STILL flatten a listpack-encoded list, and must keep doing
    /// exactly that until someone changes it deliberately.
    ///
    /// moon#897 made `LPOP`, `RPOP` and `LSET` encoding-preserving and left
    /// `LPUSHX`, `RPUSHX`, `LINSERT`, `LREM`, `LTRIM` and `LMOVE` behind. All
    /// six still reach `get_or_create_list`, whose `ListKind::upgrade`
    /// materialises the `VecDeque` unconditionally and never comes back —
    /// redis 8.x keeps a small list a `listpack` through every one of them.
    /// That is a real divergence and it is NOT this change's to fix: moon#942
    /// is a probe budget, and swapping a two-probe flattening gate for a
    /// one-probe `&self` one must leave the OUTCOME byte for byte alone.
    ///
    /// So this test asserts the divergence, deliberately. If someone makes
    /// `LPUSHX` encoding-preserving, this test is the thing that says so out
    /// loud instead of letting an encoding change ride along inside a
    /// performance commit.
    #[test]
    fn lpushx_and_rpushx_still_flatten_a_listpack_moon832() {
        for (name, f) in [
            (
                "LPUSHX",
                crate::command::list::lpushx as fn(&mut Database, &[Frame]) -> Frame,
            ),
            ("RPUSHX", crate::command::list::rpushx),
        ] {
            let mut db = Database::new();
            let push = [bs(b"l"), bs(b"xxxxxxxx")];
            for _ in 0..3 {
                crate::command::list::rpush(&mut db, &push);
            }
            assert_eq!(encoding_of(&mut db, b"l"), "listpack", "fixture ({name})");

            assert_eq!(f(&mut db, &push), Frame::Integer(4), "{name} must push");
            assert_eq!(
                encoding_of(&mut db, b"l"),
                "linkedlist",
                "{name} is expected to flatten (moon#832, still open). A \
                 `listpack` here means the encoding changed — which may well \
                 be the right thing to do, but not silently and not inside a \
                 probe-budget commit"
            );
            assert_ledger_exact(&mut db, "after the X-form flattened the list");
        }
    }

    /// The X forms must also leave a missing key missing and a wrong-typed key
    /// untouched — the two arms the gate swap moves off `get_promoted`.
    #[test]
    fn the_x_forms_fabricate_nothing_and_reject_a_wrong_type() {
        for (name, f) in [
            (
                "LPUSHX",
                crate::command::list::lpushx as fn(&mut Database, &[Frame]) -> Frame,
            ),
            ("RPUSHX", crate::command::list::rpushx),
        ] {
            let mut db = Database::new();
            let floor = db.estimated_memory();
            assert_eq!(
                f(&mut db, &[bs(b"nope"), bs(b"v")]),
                Frame::Integer(0),
                "{name} on a missing key answers 0"
            );
            assert!(
                db.data().get(b"nope").is_none(),
                "{name} on a missing key must not fabricate it (moon#830)"
            );
            assert_eq!(db.estimated_memory(), floor, "{name} miss charged memory");

            crate::command::string::set(&mut db, &[bs(b"str"), bs(b"v")]);
            let before = db.estimated_memory();
            let r = f(&mut db, &[bs(b"str"), bs(b"v")]);
            assert!(
                matches!(&r, Frame::Error(e) if e.starts_with(b"WRONGTYPE")),
                "{name} on a string must answer WRONGTYPE, got {r:?}"
            );
            assert_eq!(
                db.estimated_memory(),
                before,
                "{name} WRONGTYPE must charge nothing"
            );
            assert_ledger_exact(&mut db, "after a refused X form");
        }
    }

    /// `used_memory` that `Database::list_pop_front`/`list_pop_back` STRAND
    /// every time they remove the last element of a list — one
    /// `list_elem_cost` for the element they just handed back.
    ///
    /// **This is an open bug, pinned at its measured value, not a tolerance.**
    /// `accessors.rs:1192-1222` credits `list_elem_cost(&val)` on the `else`
    /// branch and NOT on the `if empty` branch, on the stated theory that
    /// "whole-key removal recomputes the (now-empty) entry cost via
    /// `entry_overhead`". It does not: `entry_overhead` is computed from the
    /// CURRENT value, which no longer holds the element, so the push-time
    /// charge is never given back. Measured on an otherwise EMPTY database:
    /// one `RPUSH k e` + one `list_pop_front` leaves `used_memory` at 56 B
    /// against a from-scratch recompute of 0 B, on BOTH encodings, and ten
    /// create/drain cycles leave 560 B. It accumulates without bound on a
    /// keyspace that is empty.
    ///
    /// Everything that drains a list through the BLOCKING family reaches it —
    /// `LMOVE`, `RPOPLPUSH`, and the `BLPOP`/`BRPOP`/`BLMOVE`/`BRPOPLPUSH`
    /// immediate and wakeup paths — i.e. the reliable-queue pattern, which is
    /// precisely the workload that drains a list to empty over and over. The
    /// drift is UPWARD, so the consequence is `--maxmemory` and eviction
    /// firing on a server that is actually empty, rather than moon#814's
    /// never-firing direction.
    ///
    /// The fix is one line in each of the two accessors — credit the element
    /// unconditionally, exactly as `pop_eager` in this module already does —
    /// but `src/storage/db/accessors.rs` is not this change's to edit. When it
    /// lands, this constant goes to 0 and that is the signal, not a
    /// regression.
    const STRANDED_BY_LIST_POP: usize = 56;

    /// EVERY list writer that can remove the last element must remove the KEY
    /// with it, on BOTH encodings.
    ///
    /// A container left alive holding nothing is not a cosmetic defect. It is
    /// `EXISTS`/`TYPE`/`DBSIZE`-visible, it survives into the AOF and onto a
    /// replica, redis has no such state to compare against, and — because
    /// `db.remove` credits an `entry_overhead` recomputed from the CURRENT
    /// value — a later delete credits back memory that was charged against a
    /// value that no longer exists. moon#830 is the recorded shape on the
    /// creation side (`LSET missing` fabricating a charged empty list) and
    /// moon#814 is what the ledger does afterwards.
    ///
    /// This enumerates the STATE WRITERS, not the command names: the entry
    /// gate is "does this call path remove elements", so `LPOP` appears twice
    /// (bare and counted) and `LMOVE`/`RPOPLPUSH` appear because they drain a
    /// source through a different accessor entirely (`Database::list_pop_*`)
    /// than the pops do. A writer added later that empties a list and is not
    /// listed here is exactly the gap this test cannot see, so it is listed by
    /// the operation it performs and not by the module it lives in.
    #[test]
    fn every_list_writer_that_empties_a_list_removes_the_key() {
        type Drain = fn(&mut Database);

        // (name, how to empty a 2-element list at key `l`, the ledger drift
        // that writer leaves behind). Every drift here is a BUG pinned at its
        // measured value, not a tolerance: see the two LMOVE rows.
        let cases: [(&str, Drain, usize); 9] = [
            (
                "LPOP x2",
                |db| {
                    crate::command::list::lpop(db, &[bs(b"l")]);
                    crate::command::list::lpop(db, &[bs(b"l")]);
                },
                0,
            ),
            (
                "RPOP x2",
                |db| {
                    crate::command::list::rpop(db, &[bs(b"l")]);
                    crate::command::list::rpop(db, &[bs(b"l")]);
                },
                0,
            ),
            (
                "LPOP count",
                |db| {
                    crate::command::list::lpop(db, &[bs(b"l"), bs(b"9")]);
                },
                0,
            ),
            (
                "RPOP count",
                |db| {
                    crate::command::list::rpop(db, &[bs(b"l"), bs(b"9")]);
                },
                0,
            ),
            (
                "LREM all",
                |db| {
                    crate::command::list::lrem(db, &[bs(b"l"), bs(b"0"), bs(b"e")]);
                },
                0,
            ),
            (
                "LTRIM to an empty range",
                |db| {
                    crate::command::list::ltrim(db, &[bs(b"l"), bs(b"5"), bs(b"1")]);
                },
                0,
            ),
            (
                "LMPOP",
                |db| {
                    crate::command::list::lmpop(
                        db,
                        &[bs(b"1"), bs(b"l"), bs(b"LEFT"), bs(b"COUNT"), bs(b"9")],
                    );
                },
                0,
            ),
            (
                "LMOVE draining the source",
                |db| {
                    for _ in 0..2 {
                        crate::command::list::lmove(
                            db,
                            &[bs(b"l"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT")],
                        );
                    }
                },
                STRANDED_BY_LIST_POP,
            ),
            (
                "RPOPLPUSH draining the source",
                |db| {
                    for _ in 0..2 {
                        crate::command::list::rpoplpush(db, &[bs(b"l"), bs(b"dst")]);
                    }
                },
                STRANDED_BY_LIST_POP,
            ),
        ];

        for (name, drain, drift) in cases {
            // Both encodings: a 2-element listpack, and a list that has been
            // pushed past `list-max-listpack-size` and trimmed back to 2, so
            // it is a `linkedlist` holding the same two elements. Nothing
            // demotes (moon#832), which is what makes the second reachable.
            for compact in [true, false] {
                let mut db = Database::new();
                if compact {
                    crate::command::list::rpush(&mut db, &[bs(b"l"), bs(b"e"), bs(b"e")]);
                } else {
                    let owned: Vec<Vec<u8>> = (0..200).map(|_| b"e".to_vec()).collect();
                    let mut args: Vec<Frame> = Vec::with_capacity(201);
                    args.push(bs(b"l"));
                    args.extend(owned.iter().map(|m| bs(m)));
                    crate::command::list::rpush(&mut db, &args);
                    crate::command::list::ltrim(&mut db, &[bs(b"l"), bs(b"0"), bs(b"1")]);
                }
                let want = if compact { "listpack" } else { "linkedlist" };
                assert_eq!(
                    encoding_of(&mut db, b"l"),
                    want,
                    "fixture ({name}, compact={compact})"
                );
                assert_eq!(
                    crate::command::list::llen_readonly(&db, &[bs(b"l")], 0),
                    Frame::Integer(2),
                    "fixture ({name}, compact={compact}): two elements"
                );

                drain(&mut db);

                assert!(
                    db.data().get(b"l").is_none(),
                    "{name} (compact={compact}) emptied the list and left the \
                     KEY alive — an empty container that EXISTS, TYPE and \
                     DBSIZE report, that reaches the AOF and a replica, and \
                     that redis has no counterpart for"
                );
                assert_eq!(
                    crate::command::list::llen_readonly(&db, &[bs(b"l")], 0),
                    Frame::Integer(0),
                    "{name} (compact={compact}): LLEN after the key is gone"
                );
                let running = db.estimated_memory();
                db.recalculate_memory();
                let recomputed = db.estimated_memory();
                assert_eq!(
                    running - recomputed,
                    drift,
                    "{name} (compact={compact}): running ledger {running} B vs \
                     full recompute {recomputed} B, expected a drift of \
                     {drift} B"
                );
            }
        }
    }
}
