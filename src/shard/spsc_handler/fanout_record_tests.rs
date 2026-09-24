//! moon#1177: what `wal_append_and_fanout_bytes` does with a routed write's
//! record — every cross-shard write in the SPSC arms goes through it.

use super::*;
use crate::persistence::aof::{AofMessage, AofWriterPool, FsyncPolicy};
use crate::replication::backlog::{ReplicationBacklog, SharedBacklog};
use crate::runtime::channel::mpsc_bounded;
use std::time::Duration;

/// The AOF writer receives the caller's own allocation. The slice-taking form
/// copied every record (`Bytes::copy_from_slice`: a malloc + memcpy on the
/// shard thread and a free on the writer's) although the arm already owned it.
#[test]
fn the_aof_pool_receives_the_callers_record_without_a_copy() {
    let (tx, rx) = mpsc_bounded::<AofMessage>(8);
    let pool = AofWriterPool::top_level_with_policy(tx, FsyncPolicy::EverySec, Duration::ZERO);
    let record = bytes::Bytes::from(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec());
    let origin = record.as_ptr();
    let backlog: SharedBacklog = Arc::new(parking_lot::Mutex::new(None));
    let mut budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;

    assert!(wal_append_and_fanout_bytes(
        record,
        0,
        &mut None,
        &backlog,
        &mut Vec::new(),
        &None,
        0,
        Some(&pool),
        false,
        &mut budget,
    ));

    match rx.try_recv() {
        Ok(AofMessage::Append { bytes, .. }) => assert_eq!(
            bytes.as_ptr(),
            origin,
            "the record reached the AOF writer as a copy, not as the caller's allocation"
        ),
        other => panic!("expected one Append, got {:?}", other.is_ok()),
    }
}

/// With no replica registered on this shard and none ever attaching, a write
/// never touches the replication backlog's mutex. The comment above the lock
/// claimed "no lock acquire" for this case while the code locked on every
/// write. Held here by this thread: a write that tries to lock it cannot
/// finish, which the bounded join turns into a failure instead of a hang.
#[test]
fn a_write_with_no_replica_never_takes_the_backlog_lock() {
    if crate::replication::state::fanout_hint_active() {
        // The hint is process-global and sticky; another test in this binary
        // (a replica activation) turned it on, so "no replica ever attached"
        // cannot be set up in this process. Nothing to observe.
        eprintln!("fanout hint already active in this test process; skipping");
        return;
    }
    let backlog: SharedBacklog =
        Arc::new(parking_lot::Mutex::new(Some(ReplicationBacklog::new(1024))));
    let held = backlog.lock();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let backlog2 = Arc::clone(&backlog);
    let worker = std::thread::spawn(move || {
        let (tx, _rx) = mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level_with_policy(tx, FsyncPolicy::EverySec, Duration::ZERO);
        let mut budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
        let ok = wal_append_and_fanout_bytes(
            bytes::Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
            0,
            &mut None,
            &backlog2,
            &mut Vec::new(),
            &None,
            0,
            Some(&pool),
            false,
            &mut budget,
        );
        let _ = done_tx.send(ok);
    });
    let finished = done_rx.recv_timeout(Duration::from_secs(2));
    drop(held);
    let _ = worker.join();
    assert_eq!(
        finished,
        Ok(true),
        "the write blocked on the replication backlog mutex with no replica anywhere"
    );
    assert_eq!(
        backlog.lock().as_ref().map(|b| b.end_offset()),
        Some(0),
        "and appended nothing to it"
    );
}
