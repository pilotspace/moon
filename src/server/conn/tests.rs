//! Tests for inline dispatch (monoio runtime).
//!
//! Extracted from `server/connection.rs` (Plan 48-02).

use super::*;
use crate::persistence::aof::AofMessage;
use crate::runtime::channel;
use crate::storage::Database;
use crate::storage::entry::Entry;
use bytes::{Bytes, BytesMut};
use std::cell::RefCell;
use std::rc::Rc;

/// Helper: create a single-shard, single-database ShardDatabases for testing.
fn make_dbs() -> std::sync::Arc<crate::shard::shared_databases::ShardDatabases> {
    crate::shard::shared_databases::ShardDatabases::new(vec![vec![Database::new()]])
}

#[test]
fn test_inline_get_hit() {
    let dbs = make_dbs();
    {
        let mut guard = dbs.write_db(0, 0);
        guard.set(
            Bytes::from_static(b"foo"),
            Entry::new_string(Bytes::from_static(b"bar")),
        );
    }
    let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
    let mut write_buf = BytesMut::new();
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(result, 1);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$3\r\nbar\r\n");
}

#[test]
fn test_inline_get_miss() {
    let dbs = make_dbs();
    let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"[..]);
    let mut write_buf = BytesMut::new();
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(result, 1);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$-1\r\n");
}

#[test]
fn test_inline_set_falls_through() {
    // SET is a write command — inline fast-path intentionally rejects it
    // (must go through normal dispatch for ACL, replication, tracking, etc.)
    let dbs = make_dbs();
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(result, 0, "SET should fall through inline dispatch");
    assert_eq!(read_buf.len(), original_len, "buffer should be untouched");
    assert!(write_buf.is_empty(), "no response should be written");
}

#[test]
fn test_inline_fallthrough() {
    let dbs = make_dbs();
    let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
    let mut read_buf = BytesMut::from(&ping_cmd[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(result, 0);
    assert_eq!(read_buf.len(), original_len);
    assert!(write_buf.is_empty());
}

#[test]
fn test_inline_mixed_batch() {
    let dbs = make_dbs();
    {
        let mut guard = dbs.write_db(0, 0);
        guard.set(
            Bytes::from_static(b"foo"),
            Entry::new_string(Bytes::from_static(b"bar")),
        );
    }
    // GET foo followed by PING
    let mut read_buf = BytesMut::new();
    read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    read_buf.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    let mut write_buf = BytesMut::new();
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    // Inline loop should process GET but leave PING
    let total = try_inline_dispatch_loop(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(total, 1);
    assert_eq!(&write_buf[..], b"$3\r\nbar\r\n");
    assert_eq!(&read_buf[..], b"*1\r\n$4\r\nPING\r\n");
}

#[test]
fn test_inline_case_insensitive() {
    let dbs = make_dbs();
    {
        let mut guard = dbs.write_db(0, 0);
        guard.set(
            Bytes::from_static(b"foo"),
            Entry::new_string(Bytes::from_static(b"baz")),
        );
    }
    let mut read_buf = BytesMut::from(&b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n"[..]);
    let mut write_buf = BytesMut::new();
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
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
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(result, 0);
    assert_eq!(read_buf.len(), original_len);
    assert!(write_buf.is_empty());
}

#[test]
fn test_inline_set_with_aof_falls_through() {
    // SET is a write command — inline fast-path intentionally rejects it
    // even when AOF is configured.
    let dbs = make_dbs();
    let (aof_sender, _aof_receiver) = channel::mpsc_bounded::<AofMessage>(16);
    let aof_tx: Option<channel::MpscSender<AofMessage>> = Some(aof_sender);
    let cmd = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    let mut read_buf = BytesMut::from(&cmd[..]);
    let original_len = read_buf.len();
    let mut write_buf = BytesMut::new();

    let result = try_inline_dispatch(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(result, 0, "SET should fall through inline dispatch");
    assert_eq!(read_buf.len(), original_len);
    assert!(write_buf.is_empty());
}

#[test]
fn test_inline_multiple_gets() {
    let dbs = make_dbs();
    {
        let mut guard = dbs.write_db(0, 0);
        guard.set(
            Bytes::from_static(b"a"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        guard.set(
            Bytes::from_static(b"b"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
    }
    let mut read_buf = BytesMut::new();
    read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n");
    read_buf.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$1\r\nb\r\n");
    let mut write_buf = BytesMut::new();
    let aof_tx: Option<channel::MpscSender<AofMessage>> = None;

    let total = try_inline_dispatch_loop(&mut read_buf, &mut write_buf, &dbs, 0, 0, &aof_tx, 0, 1);
    assert_eq!(total, 2);
    assert!(read_buf.is_empty());
    assert_eq!(&write_buf[..], b"$1\r\n1\r\n$1\r\n2\r\n");
}
