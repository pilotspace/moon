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

/// The backlog gate, every row (moon#1177): a write appends to the
/// replication backlog, taking its mutex, iff a replica is registered on this
/// shard or one has begun attaching somewhere. The pure predicate is testable
/// whatever the process-global hint holds; reading the hint directly made the
/// integration test below vacuous whenever a `replication::state` test had
/// already set it in the same `cargo test --lib` process.
#[test]
fn the_backlog_gate_opens_only_for_a_replica_or_the_fanout_hint() {
    // (replicas_empty, fanout_hint) -> wanted
    let table = [
        ((true, false), false),
        ((true, true), true),
        ((false, false), true),
        ((false, true), true),
    ];
    for ((replicas_empty, fanout_hint), wanted) in table {
        assert_eq!(
            backlog_append_wanted(replicas_empty, fanout_hint),
            wanted,
            "replicas_empty={replicas_empty} fanout_hint={fanout_hint}"
        );
    }
}

/// One routed write through the real body, hint passed in (never read from
/// the process-global), while this thread HOLDS the backlog mutex: a write
/// that tries to lock it cannot finish, and the bounded wait turns that into
/// a failure instead of a hang. Returns whether the write finished, and the
/// backlog's end offset afterwards.
fn write_with_backlog_held(
    fanout_hint: bool,
) -> (Result<bool, std::sync::mpsc::RecvTimeoutError>, Option<u64>) {
    let backlog: SharedBacklog =
        Arc::new(parking_lot::Mutex::new(Some(ReplicationBacklog::new(1024))));
    let held = backlog.lock();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let backlog2 = Arc::clone(&backlog);
    let worker = std::thread::spawn(move || {
        let (tx, _rx) = mpsc_bounded::<AofMessage>(8);
        let pool = AofWriterPool::top_level_with_policy(tx, FsyncPolicy::EverySec, Duration::ZERO);
        let mut budget = crate::persistence::aof::AOF_SPSC_BACKPRESSURE_BOUND;
        let ok = wal_append_and_fanout_hinted(
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
            fanout_hint,
        );
        let _ = done_tx.send(ok);
    });
    // Long enough for a write that does not lock to finish on a loaded box;
    // a write that does lock waits here the whole time.
    let finished = done_rx.recv_timeout(Duration::from_secs(2));
    drop(held);
    let _ = worker.join();
    let end = backlog.lock().as_ref().map(|b| b.end_offset());
    (finished, end)
}

/// With no replica registered on this shard and none ever attaching, a write
/// never touches the replication backlog's mutex. The comment above the lock
/// claimed "no lock acquire" for this case while the code locked on every
/// write.
#[test]
fn a_write_with_no_replica_never_takes_the_backlog_lock() {
    let (finished, end) = write_with_backlog_held(false);
    assert_eq!(
        finished,
        Ok(true),
        "the write blocked on the replication backlog mutex with no replica anywhere"
    );
    assert_eq!(end, Some(0), "and appended nothing to it");
}

/// The other side of the gate: once a replica has begun attaching (the hint),
/// a write with no replica registered on this shard yet still appends, so the
/// backlog holds every record a partial resync may ask for. The write waits
/// for the mutex this thread holds, then appends once it is released.
#[test]
fn a_write_under_the_fanout_hint_appends_to_the_backlog() {
    let (finished, end) = write_with_backlog_held(true);
    assert_eq!(
        finished,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        "the write must take the backlog mutex (it finished while this thread held it)"
    );
    assert_eq!(
        end,
        Some(b"*1\r\n$4\r\nPING\r\n".len() as u64),
        "the record is in the backlog"
    );
}
