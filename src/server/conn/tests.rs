//! Tests for inline dispatch (monoio runtime).
//!
//! Extracted from `server/connection.rs` (Plan 48-02).

use super::*;
use crate::persistence::aof::AofMessage;
use crate::runtime::channel;
use crate::storage::Database;
use crate::storage::entry::Entry;
use bytes::{Bytes, BytesMut};

/// Helper: create a single-shard, single-database ShardDatabases for testing
/// and initialize the thread-local ShardSlice so dispatch paths work.
///
/// Each test must call this before any dispatch or write_db calls.
/// Uses `reset_test_shard` (test-only) so multiple tests on the same OS thread
/// each get a fresh, empty shard state.
fn make_dbs() -> std::sync::Arc<crate::shard::shared_databases::ShardDatabases> {
    let (shared, mut inits) =
        crate::shard::shared_databases::ShardDatabases::new(vec![vec![Database::new()]]);
    let init = inits.remove(0);
    crate::shard::slice::reset_test_shard(crate::shard::slice::ShardSlice::new(init));
    shared
}

/// Helper: default runtime config for inline dispatch tests.
fn make_rt_config() -> parking_lot::RwLock<crate::config::RuntimeConfig> {
    parking_lot::RwLock::new(crate::config::RuntimeConfig::default())
}

#[test]
fn test_inline_get_hit() {
    let dbs = make_dbs();
    crate::shard::slice::with_shard_db(0, |db| {
        db.set(
            Bytes::from_static(b"foo"),
            Entry::new_string(Bytes::from_static(b"bar")),
        );
    });
    let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(result, 1);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$3\r\nbar\r\n");
}

/// Byte-parity guard for the inline GET hit across the `CompactValue` SSO
/// boundary (12B inline / 13B heap) and up to a large value. The reply must be
/// exactly `$<len>\r\n<bytes>\r\n` for every size — this pins the response
/// framing so the no-copy refactor (writing the value straight from the borrow
/// into `write_buf` instead of via an intermediate `Vec`) cannot change a byte.
#[test]
fn test_inline_get_hit_byte_parity_sizes() {
    for &size in &[0usize, 1, 12, 13, 65536] {
        let dbs = make_dbs();
        let value: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        crate::shard::slice::with_shard_db(0, |db| {
            db.set(
                Bytes::from_static(b"k"),
                Entry::new_string(Bytes::from(value.clone())),
            );
        });
        let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"[..]);
        let mut write_buf = BytesMut::new();
        let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
        let rt_config = make_rt_config();

        let result = try_inline_dispatch(
            &mut read_buf,
            &mut write_buf,
            &dbs,
            0,
            0,
            &aof_pool,
            &None,
            0,
            1,
            false,
            &rt_config,
        );

        let mut expected = Vec::new();
        expected.extend_from_slice(b"$");
        expected.extend_from_slice(size.to_string().as_bytes());
        expected.extend_from_slice(b"\r\n");
        expected.extend_from_slice(&value);
        expected.extend_from_slice(b"\r\n");

        assert_eq!(
            result, 1,
            "size {size}: expected exactly one command inlined"
        );
        assert!(
            read_buf.is_empty(),
            "size {size}: read_buf not fully consumed"
        );
        assert_eq!(
            &write_buf[..],
            &expected[..],
            "size {size}: reply byte mismatch"
        );
    }
}

#[test]
fn test_inline_get_miss() {
    let dbs = make_dbs();
    let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(result, 1);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$-1\r\n");
}

#[test]
fn test_inline_set_falls_through_when_writes_disabled() {
    // SET is rejected when can_inline_writes=false (tracking/MULTI/restricted ACL).
    let dbs = make_dbs();
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(result, 0, "SET should fall through inline dispatch");
    assert_eq!(read_buf.len(), original_len, "buffer should be untouched");
    assert!(write_buf.is_empty(), "no response should be written");
}

#[test]
fn test_inline_set_executes_when_writes_enabled() {
    // Plain SET is inlined when can_inline_writes=true.
    let dbs = make_dbs();
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        true,
        &rt_config,
    );
    assert_eq!(result, 1, "SET should be inlined");
    assert!(read_buf.is_empty(), "buffer should be consumed");
    assert_eq!(&write_buf[..], b"+OK\r\n");

    // Verify the key was actually set
    crate::shard::slice::with_shard_db(0, |db| {
        // Test assertion: SET was just issued with a live TTL, so the key must
        // exist and hold a string-typed value; an absent or wrong-typed value
        // would indicate the inline SET path regressed.
        #[allow(clippy::expect_used, clippy::unwrap_used)]
        let entry = db.get_if_alive(b"foo", 0).expect("key should exist");
        #[allow(clippy::unwrap_used)]
        let value_bytes = entry.value.as_bytes().unwrap();
        assert_eq!(value_bytes, b"bar");
    });
}

#[test]
fn test_inline_set_with_options_falls_through() {
    // SET with extra args (NX/XX/EX/PX) is NOT inlined — only plain *3 SET.
    let dbs = make_dbs();
    let cmd = b"*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\nEX\r\n$2\r\n10\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        true,
        &rt_config,
    );
    assert_eq!(result, 0, "SET with options should fall through");
    assert_eq!(read_buf.len(), original_len);
}

#[test]
fn test_inline_fallthrough() {
    let dbs = make_dbs();
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
    let mut read_buf = BytesMut::from(&ping_cmd[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(result, 0);
    assert_eq!(read_buf.len(), original_len);
    assert!(write_buf.is_empty());
}

#[test]
fn test_inline_mixed_batch() {
    let dbs = make_dbs();
    crate::shard::slice::with_shard_db(0, |db| {
        db.set(
            Bytes::from_static(b"foo"),
            Entry::new_string(Bytes::from_static(b"bar")),
        );
    });
    // GET foo followed by PING
    let mut read_buf = BytesMut::new();
    read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    read_buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    // Inline loop should process GET but leave PING
    let total = try_inline_dispatch_loop(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(total, 1);
    assert_eq!(&write_buf[..], b"$3\r\nbar\r\n");
    assert_eq!(&read_buf[..], b"*1\r\n$4\r\nPING\r\n");
}

#[test]
fn test_inline_case_insensitive() {
    let dbs = make_dbs();
    crate::shard::slice::with_shard_db(0, |db| {
        db.set(
            Bytes::from_static(b"foo"),
            Entry::new_string(Bytes::from_static(b"baz")),
        );
    });
    let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n"[..]);
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(result, 1);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$3\r\nbaz\r\n");
}

#[test]
fn test_inline_partial() {
    let dbs = make_dbs();
    // Partial command: missing key data
    let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\n"[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(result, 0);
    assert_eq!(read_buf.len(), original_len);
    assert!(write_buf.is_empty());
}

#[test]
fn test_inline_set_with_aof_falls_through_when_writes_disabled() {
    // SET falls through when can_inline_writes=false even with AOF.
    let dbs = make_dbs();
    let (aof_sender, _aof_receiver) = channel::mpsc_bounded::<AofMessage>(16);
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = Some(
        crate::persistence::aof::AofWriterPool::top_level(aof_sender),
    );
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();
    let rt_config = make_rt_config();

    // With can_inline_writes=false, SET falls through
    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(
        result, 0,
        "SET should fall through inline dispatch when writes disabled"
    );
    assert_eq!(read_buf.len(), original_len);
    assert!(write_buf.is_empty());
}

#[test]
fn test_inline_multiple_gets() {
    let dbs = make_dbs();
    crate::shard::slice::with_shard_db(0, |db| {
        db.set(
            Bytes::from_static(b"a"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        db.set(
            Bytes::from_static(b"b"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
    });
    let mut read_buf = BytesMut::new();
    read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n");
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let total = try_inline_dispatch_loop(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(total, 2);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$1\r\n1\r\n$1\r\n2\r\n");
}

/// task #59 (coverage-gap fix, review round 3): plain `GET key` for a key
/// that only lives in the cold tier is served by THIS inline fast path
/// first (before the general async `dispatch_read` branch in
/// `handler_monoio` ever runs) -- redis-benchmark and the task's original
/// repro both hit this path. `try_inline_dispatch` is synchronous and
/// cannot `.await` the off-shard-thread pool read, so doing the blocking
/// `pread` here would reproduce the exact ~1.9s inline stall the task
/// targets. It must instead DECLINE (return the "not inlined" sentinel,
/// bytes unconsumed) and let the caller fall through to the async path.
///
/// This test proves it deterministically: inject a large synthetic delay
/// into the cold-read path, call `try_inline_dispatch` on a `GET` of a key
/// that IS cold-indexed, and assert it returns almost instantly (nowhere
/// near the injected delay) with the sentinel `0` and the input bytes
/// completely unconsumed -- i.e. it never touched the slow path at all,
/// rather than merely "returned before the delay elapsed" (which a buggy
/// spawn-and-abandon implementation could also satisfy).
#[test]
fn test_inline_get_declines_for_cold_key_instead_of_blocking() {
    let _guard = crate::storage::tiered::cold_read::TEST_DELAY_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    crate::storage::tiered::cold_read::TEST_INJECT_DELAY_MS
        .store(1_000, std::sync::atomic::Ordering::Relaxed);

    let tmp = tempfile::tempdir().unwrap();
    let dbs = make_dbs();
    crate::shard::slice::with_shard_db(0, |db| {
        let manifest_path = tmp.path().join("shard.manifest");
        let mut manifest = crate::persistence::manifest::ShardManifest::create(&manifest_path)
            .expect("create manifest");
        let mut cold_index = crate::storage::tiered::cold_index::ColdIndex::new();
        let entry = Entry::new_string(Bytes::from_static(b"cold-value-on-disk"));
        crate::storage::tiered::kv_spill::spill_to_datafile(
            tmp.path(),
            70,
            b"coldkey",
            &entry,
            0,
            &mut manifest,
            Some(&mut cold_index),
        )
        .expect("spill");
        db.cold_shard_dir = Some(tmp.path().to_path_buf());
        db.cold_index = Some(cold_index);
    });

    let cmd = b"*2\r\n$3\r\nGET\r\n$7\r\ncoldkey\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let original = read_buf.clone();
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let start = std::time::Instant::now();
    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    let elapsed = start.elapsed();

    crate::storage::tiered::cold_read::TEST_INJECT_DELAY_MS
        .store(0, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        result, 0,
        "a cold GET must decline inlining (sentinel 0), not answer it with a blocking read"
    );
    assert_eq!(
        read_buf, original,
        "declining must leave the command bytes completely unconsumed so the fall-through \
         parser sees the exact same command"
    );
    assert!(
        write_buf.is_empty(),
        "declining must not have written any response bytes"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(200),
        "must return almost immediately -- a 1000ms injected cold-read delay must never be \
         paid on this synchronous inline path; got {elapsed:?}"
    );
}

/// Genuine miss (key absent from BOTH tiers -- no cold index configured at
/// all) must still be answered inline with a fast `$-1`, unaffected by the
/// cold-GET bail-out above (which only fires when `cold_lookup_location`
/// actually finds an entry).
#[test]
fn test_inline_get_genuine_miss_still_answers_inline() {
    let dbs = make_dbs();
    let cmd = b"*2\r\n$3\r\nGET\r\n$7\r\nnokeyat\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let mut write_buf = BytesMut::new();
    let aof_pool: Option<std::sync::Arc<crate::persistence::aof::AofWriterPool>> = None;
    let rt_config = make_rt_config();

    let result = try_inline_dispatch(
        &mut read_buf,
        &mut write_buf,
        &dbs,
        0,
        0,
        &aof_pool,
        &None,
        0,
        1,
        false,
        &rt_config,
    );
    assert_eq!(result, 1);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$-1\r\n");
}

/// c10k W2 regression guard: `active_cross_txn` must stay boxed. Unboxed,
/// CrossStoreTxn's inline SmallVecs put ~2.2 KB into EVERY connection's task
/// future (tmp/C10K-REVIEW.md §2). If this assert fires, something re-inlined
/// large state into ConnectionState — box it instead.
#[test]
fn connection_state_stays_small() {
    let sz = std::mem::size_of::<crate::server::conn::core::ConnectionState>();
    assert!(
        sz <= 768,
        "ConnectionState is {sz} B — keep bulky fields boxed (was 2.7 KB before c10k W2)"
    );
}
