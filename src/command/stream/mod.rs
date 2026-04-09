//! Stream command handlers: XADD, XLEN, XRANGE, XREVRANGE, XTRIM, XDEL, XREAD,
//! XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO.

mod stream_read;
mod stream_write;

pub use stream_read::*;
pub use stream_write::*;

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::stream::StreamId;

/// Format a stream entry as a RESP nested array: [id, [field, value, ...]]
pub(crate) fn format_entry(id: StreamId, fields: &[(Bytes, Bytes)]) -> Frame {
    let mut field_frames = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        field_frames.push(Frame::BulkString(f.clone()));
        field_frames.push(Frame::BulkString(v.clone()));
    }
    Frame::Array(framevec![
        Frame::BulkString(id.to_bytes()),
        Frame::Array(field_frames.into()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::Database;

    fn make_args(parts: &[&[u8]]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
            .collect()
    }

    #[test]
    fn test_xadd_auto_id() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"*", b"name", b"alice"]);
        let result = xadd(&mut db, &args);
        match result {
            Frame::BulkString(id) => {
                assert!(id.as_ref().contains(&b'-'));
            }
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_xadd_explicit_id() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"1-1", b"name", b"alice"]);
        let result = xadd(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from("1-1")));

        let args = make_args(&[b"mystream", b"2-0", b"name", b"bob"]);
        let result = xadd(&mut db, &args);
        assert_eq!(result, Frame::BulkString(Bytes::from("2-0")));
    }

    #[test]
    fn test_xadd_nomkstream_nonexistent() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"NOMKSTREAM", b"*", b"f", b"v"]);
        let result = xadd(&mut db, &args);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_xadd_nomkstream_existing() {
        let mut db = Database::new();
        // Create stream first
        let args = make_args(&[b"mystream", b"*", b"f", b"v"]);
        xadd(&mut db, &args);
        // NOMKSTREAM on existing should work
        let args = make_args(&[b"mystream", b"NOMKSTREAM", b"*", b"f2", b"v2"]);
        let result = xadd(&mut db, &args);
        match result {
            Frame::BulkString(_) => {}
            other => panic!("Expected BulkString, got {:?}", other),
        }
    }

    #[test]
    fn test_xadd_with_maxlen() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }
        // Add with MAXLEN 5
        let args = make_args(&[b"mystream", b"MAXLEN", b"5", b"11-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let len_args = make_args(&[b"mystream"]);
        let result = xlen(&mut db, &len_args);
        assert_eq!(result, Frame::Integer(5));
    }

    #[test]
    fn test_xlen() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &args), Frame::Integer(0));

        let args = make_args(&[b"mystream", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);
        let args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &args), Frame::Integer(1));
    }

    #[test]
    fn test_xrange_full() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let val = format!("v{}", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", val.as_bytes()]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"-", b"+"]);
        let result = xrange(&mut db, &args);
        match result {
            Frame::Array(entries) => assert_eq!(entries.len(), 3),
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xrange_with_count() {
        let mut db = Database::new();
        for i in 1..=5 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"-", b"+", b"COUNT", b"2"]);
        let result = xrange(&mut db, &args);
        match result {
            Frame::Array(entries) => assert_eq!(entries.len(), 2),
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xrevrange() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"+", b"-"]);
        let result = xrevrange(&mut db, &args);
        match result {
            Frame::Array(entries) => {
                assert_eq!(entries.len(), 3);
                // First entry should be highest ID (3-0)
                if let Frame::Array(ref inner) = entries[0] {
                    assert_eq!(inner[0], Frame::BulkString(Bytes::from("3-0")));
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xtrim_maxlen() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"MAXLEN", b"5"]);
        let result = xtrim(&mut db, &args);
        assert_eq!(result, Frame::Integer(5));

        let len_args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &len_args), Frame::Integer(5));
    }

    #[test]
    fn test_xtrim_approximate() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        // Approximate trim with ~ -- small excess may not trigger
        let args = make_args(&[b"mystream", b"MAXLEN", b"~", b"9"]);
        let result = xtrim(&mut db, &args);
        // With 10 entries and maxlen 9, approximate should NOT trim (excess < 10%)
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_xdel() {
        let mut db = Database::new();
        for i in 1..=5 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"mystream", b"2-0", b"4-0"]);
        let result = xdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(2));

        let len_args = make_args(&[b"mystream"]);
        assert_eq!(xlen(&mut db, &len_args), Frame::Integer(3));
    }

    #[test]
    fn test_xdel_nonexistent_ids() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"mystream", b"99-0"]);
        let result = xdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_xread_single_stream() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        // Read entries after 1-0
        let args = make_args(&[b"STREAMS", b"mystream", b"1-0"]);
        let result = xread(&mut db, &args);
        match result {
            Frame::Array(streams) => {
                assert_eq!(streams.len(), 1);
                if let Frame::Array(ref inner) = streams[0] {
                    assert_eq!(inner[0], Frame::BulkString(Bytes::from("mystream")));
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 2); // entries 2-0 and 3-0
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xread_dollar_id() {
        let mut db = Database::new();
        for i in 1..=3 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        // Read from $: should return Null since no new entries
        let args = make_args(&[b"STREAMS", b"mystream", b"$"]);
        let result = xread(&mut db, &args);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_xread_multiple_streams() {
        let mut db = Database::new();
        let args = make_args(&[b"s1", b"1-0", b"f", b"v1"]);
        xadd(&mut db, &args);
        let args = make_args(&[b"s2", b"1-0", b"f", b"v2"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"STREAMS", b"s1", b"s2", b"0-0", b"0-0"]);
        let result = xread(&mut db, &args);
        match result {
            Frame::Array(streams) => {
                assert_eq!(streams.len(), 2);
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xread_with_count() {
        let mut db = Database::new();
        for i in 1..=10 {
            let id = format!("{}-0", i);
            let args = make_args(&[b"mystream", id.as_bytes(), b"f", b"v"]);
            xadd(&mut db, &args);
        }

        let args = make_args(&[b"COUNT", b"3", b"STREAMS", b"mystream", b"0-0"]);
        let result = xread(&mut db, &args);
        match result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 3);
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xrange_nonexistent() {
        let mut db = Database::new();
        let args = make_args(&[b"nostream", b"-", b"+"]);
        let result = xrange(&mut db, &args);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    #[test]
    fn test_xdel_nonexistent_key() {
        let mut db = Database::new();
        let args = make_args(&[b"nostream", b"1-0"]);
        let result = xdel(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));
    }

    // ---- Consumer group tests (Plan 02) ----

    /// Helper: set up a stream with entries 1-0..n-0 and a consumer group.
    fn setup_stream_with_group(db: &mut Database, key: &[u8], n: u64, group: &[u8]) {
        for i in 1..=n {
            let id = format!("{}-0", i);
            let val = format!("v{}", i);
            let args = make_args(&[key, id.as_bytes(), b"f", val.as_bytes()]);
            xadd(db, &args);
        }
        // XGROUP CREATE key group 0 MKSTREAM
        let args = make_args(&[b"CREATE", key, group, b"0", b"MKSTREAM"]);
        let result = xgroup(db, &args);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_xgroup_create_and_destroy() {
        let mut db = Database::new();
        let args = make_args(&[b"mystream", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"CREATE", b"mystream", b"mygroup", b"0", b"MKSTREAM"]);
        assert_eq!(
            xgroup(&mut db, &args),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );

        // Creating same group again should error
        let args = make_args(&[b"CREATE", b"mystream", b"mygroup", b"0", b"MKSTREAM"]);
        match xgroup(&mut db, &args) {
            Frame::Error(_) => {}
            other => panic!("Expected Error, got {:?}", other),
        }

        // Destroy
        let args = make_args(&[b"DESTROY", b"mystream", b"mygroup"]);
        assert_eq!(xgroup(&mut db, &args), Frame::Integer(1));

        // Destroy again
        let args = make_args(&[b"DESTROY", b"mystream", b"mygroup"]);
        assert_eq!(xgroup(&mut db, &args), Frame::Integer(0));
    }

    #[test]
    fn test_xgroup_create_mkstream_nonexistent() {
        let mut db = Database::new();
        // Without MKSTREAM on nonexistent key
        let args = make_args(&[b"CREATE", b"nostream", b"mygroup", b"0"]);
        match xgroup(&mut db, &args) {
            Frame::Error(_) => {}
            other => panic!("Expected Error, got {:?}", other),
        }
        // With MKSTREAM on nonexistent key
        let args = make_args(&[b"CREATE", b"nostream", b"mygroup", b"0", b"MKSTREAM"]);
        assert_eq!(
            xgroup(&mut db, &args),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
    }

    #[test]
    fn test_xreadgroup_new_entries() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // XREADGROUP GROUP g alice STREAMS s >
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                assert_eq!(streams.len(), 1);
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 3);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }

        // Reading > again should return Null (all delivered)
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        assert_eq!(xreadgroup(&mut db, &args), Frame::Null);
    }

    #[test]
    fn test_xreadgroup_pending_replay() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read all new entries
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // Read pending entries with 0
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b"0"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 3); // same 3 entries
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xack_removes_from_pel() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read new entries
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // ACK entries 1-0 and 2-0
        let args = make_args(&[b"s", b"g", b"1-0", b"2-0"]);
        let result = xack(&mut db, &args);
        assert_eq!(result, Frame::Integer(2));

        // ACK non-pending entry (silently skip)
        let args = make_args(&[b"s", b"g", b"99-0"]);
        let result = xack(&mut db, &args);
        assert_eq!(result, Frame::Integer(0));

        // Pending replay should only show 3-0 now
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b"0"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 1);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xpending_summary() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read entries for alice
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        let args = make_args(&[b"s", b"g"]);
        let result = xpending(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[0], Frame::Integer(3)); // 3 pending
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xpending_detail() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read entries for alice
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        let args = make_args(&[b"s", b"g", b"-", b"+", b"10"]);
        let result = xpending(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // 3 detail entries
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xclaim_transfers_ownership() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // alice reads all
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // bob claims 1-0 with min-idle 0 (immediate claim)
        let args = make_args(&[b"s", b"g", b"bob", b"0", b"1-0"]);
        let result = xclaim(&mut db, &args);
        match &result {
            Frame::Array(entries) => {
                assert_eq!(entries.len(), 1);
            }
            other => panic!("Expected Array, got {:?}", other),
        }

        // alice's pending replay should now have only 2
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b"0"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 2);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xautoclaim_idle_entries() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // alice reads all
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        // Auto-claim with min_idle 0 (all entries are idle enough)
        let args = make_args(&[b"s", b"g", b"bob", b"0", b"0-0"]);
        let result = xautoclaim(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // [next_id, claimed, deleted]
                if let Frame::Array(ref claimed) = arr[1] {
                    assert_eq!(claimed.len(), 3); // all 3 claimed
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xinfo_stream() {
        let mut db = Database::new();
        let args = make_args(&[b"s", b"1-0", b"f", b"v"]);
        xadd(&mut db, &args);

        let args = make_args(&[b"STREAM", b"s"]);
        let result = xinfo(&mut db, &args);
        match &result {
            Frame::Array(arr) => {
                // Should have key-value pairs: length, radix-tree-keys, etc.
                assert!(arr.len() >= 10);
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"length")));
                assert_eq!(arr[1], Frame::Integer(1));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xinfo_groups() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 1, b"g1");

        let args = make_args(&[b"GROUPS", b"s"]);
        let result = xinfo(&mut db, &args);
        match &result {
            Frame::Array(groups) => {
                assert_eq!(groups.len(), 1);
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xinfo_consumers() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 3, b"g");

        // Read as alice to auto-create consumer
        let args = make_args(&[b"GROUP", b"g", b"alice", b"STREAMS", b"s", b">"]);
        xreadgroup(&mut db, &args);

        let args = make_args(&[b"CONSUMERS", b"s", b"g"]);
        let result = xinfo(&mut db, &args);
        match &result {
            Frame::Array(consumers) => {
                assert_eq!(consumers.len(), 1); // alice
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xreadgroup_with_count() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 5, b"g");

        // Read with COUNT 2
        let args = make_args(&[
            b"GROUP", b"g", b"alice", b"COUNT", b"2", b"STREAMS", b"s", b">",
        ]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 2);
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_xgroup_setid() {
        let mut db = Database::new();
        setup_stream_with_group(&mut db, b"s", 5, b"g");

        // Read 2 entries
        let args = make_args(&[
            b"GROUP", b"g", b"alice", b"COUNT", b"2", b"STREAMS", b"s", b">",
        ]);
        xreadgroup(&mut db, &args);

        // Set ID back to 0, so next > read delivers all again
        let args = make_args(&[b"SETID", b"s", b"g", b"0"]);
        assert_eq!(
            xgroup(&mut db, &args),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );

        // Reading > should deliver from 1-0 again
        let args = make_args(&[b"GROUP", b"g", b"bob", b"STREAMS", b"s", b">"]);
        let result = xreadgroup(&mut db, &args);
        match &result {
            Frame::Array(streams) => {
                if let Frame::Array(ref inner) = streams[0] {
                    if let Frame::Array(ref entries) = inner[1] {
                        assert_eq!(entries.len(), 5); // all 5 entries
                    } else {
                        panic!("Expected entries array");
                    }
                }
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }
}
