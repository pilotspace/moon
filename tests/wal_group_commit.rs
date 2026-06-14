//! ADD task `wal-group-commit` §4 TESTS — failing-first (RED) suite for the
//! group-commit batching seam frozen in §3.
//!
//! These are RED until §5 BUILD creates `src/persistence/aof/group_commit.rs` with:
//!   - const `AOF_GROUP_COMMIT_MAX_BATCH` / `AOF_GROUP_COMMIT_MAX_BYTES`
//!   - `struct GroupCommitBatch { data, deferred_control }`
//!   - `fn collect_group_commit_batch(first, try_next, max_batch, max_bytes) -> GroupCommitBatch`
//!   - `trait GroupCommitSink { write_all; sync }`  (the sync-capable realization of the frozen
//!     `<W: Write>` + "flush()+sync_data()" — non-behavioral: the durability behavior is unchanged)
//!   - `struct CommitOutcome { synced, write_failed, fsync_failed }`
//!   - `fn commit_group_commit_batch(sink, batch, do_fsync) -> CommitOutcome`
//! Before build the `use` below is UNRESOLVED → this crate fails to compile. That compile
//! failure IS the red signal (the other test crates build independently).
//!
//! These pin the PURE seam (collect) + the durability invariant (commit: one fsync, ack
//! AFTER fsync, the 5 reject mappings). The end-to-end durability scenarios
//! (every_acked_write_survives_crash, rewrite-during-writes) are proven by the integration
//! crash-matrix (`crash_matrix_per_shard_aof.rs` + a new concurrent-writers SIGKILL test added
//! at build), NOT here — a unit mock cannot prove on-disk survival across a real kill.
//!
//! Running: cargo test --test wal_group_commit

use bytes::Bytes;
use moon::persistence::aof::group_commit::{
    AOF_GROUP_COMMIT_MAX_BATCH, AOF_GROUP_COMMIT_MAX_BYTES, CommitOutcome, GroupCommitBatch,
    GroupCommitSink, collect_group_commit_batch, commit_group_commit_batch,
};
use moon::persistence::aof::{AofAck, AofMessage};
use moon::runtime::channel::{OneshotReceiver, oneshot};
use std::collections::VecDeque;

// --- helpers ---------------------------------------------------------------

fn append(data: &[u8]) -> AofMessage {
    AofMessage::Append {
        lsn: 0,
        bytes: Bytes::copy_from_slice(data),
    }
}

fn append_sync(data: &[u8]) -> (AofMessage, OneshotReceiver<AofAck>) {
    let (tx, rx) = oneshot::<AofAck>();
    (
        AofMessage::AppendSync {
            lsn: 0,
            bytes: Bytes::copy_from_slice(data),
            ack: tx,
        },
        rx,
    )
}

/// A `GroupCommitSink` that records writes + counts fsyncs, with optional fault
/// injection. `synced_at_write_count` proves the single fsync happens AFTER all
/// writes (ack-after-fsync ordering).
struct CountingSink {
    writes: Vec<Vec<u8>>,
    sync_calls: usize,
    synced_at_write_count: Option<usize>,
    fail_write_at: Option<usize>,
    fail_sync: bool,
}

impl CountingSink {
    fn new() -> Self {
        CountingSink {
            writes: Vec::new(),
            sync_calls: 0,
            synced_at_write_count: None,
            fail_write_at: None,
            fail_sync: false,
        }
    }
}

impl GroupCommitSink for CountingSink {
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        if Some(self.writes.len()) == self.fail_write_at {
            return Err(std::io::Error::other("injected write failure"));
        }
        self.writes.push(buf.to_vec());
        Ok(())
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.sync_calls += 1;
        self.synced_at_write_count = Some(self.writes.len());
        if self.fail_sync {
            return Err(std::io::Error::other("injected sync failure"));
        }
        Ok(())
    }
}

// --- collect: the pure batching seam --------------------------------------

/// batch_straddles_control — a control message FIRST yields an empty batch with
/// the control deferred (the writer handles it directly, no batch).
#[test]
fn collect_first_control_makes_no_batch() {
    let batch = collect_group_commit_batch(
        AofMessage::Shutdown,
        || None,
        AOF_GROUP_COMMIT_MAX_BATCH,
        AOF_GROUP_COMMIT_MAX_BYTES,
    );
    assert!(
        batch.data.is_empty(),
        "a control message as `first` must not open a data batch"
    );
    assert!(
        matches!(batch.deferred_control, Some(AofMessage::Shutdown)),
        "the control message is deferred for the caller to handle after commit"
    );
}

/// M3 / batch_straddles_control — the drain stops AT a control message,
/// preserving order, and never pulls data queued after the control.
#[test]
fn collect_stops_at_control_preserving_order() {
    let mut q: VecDeque<AofMessage> = VecDeque::new();
    let (sync_msg, _rx) = append_sync(b"second");
    q.push_back(sync_msg);
    q.push_back(AofMessage::Shutdown);
    q.push_back(append(b"after-control")); // must remain queued

    let batch = collect_group_commit_batch(
        append(b"first"),
        || q.pop_front(),
        AOF_GROUP_COMMIT_MAX_BATCH,
        AOF_GROUP_COMMIT_MAX_BYTES,
    );

    assert_eq!(
        batch.data.len(),
        2,
        "first(Append) + the AppendSync, then STOP at Shutdown"
    );
    assert!(matches!(batch.deferred_control, Some(AofMessage::Shutdown)));
    assert_eq!(
        q.len(),
        1,
        "the data message after the control must NOT be consumed"
    );
}

/// batch_cap_exceeded (count) — the drain stops at max_batch; the rest stay
/// queued for the next batch (never an unbounded drain).
#[test]
fn collect_respects_max_batch() {
    let mut q: VecDeque<AofMessage> = VecDeque::new();
    for _ in 0..10 {
        q.push_back(append(b"x"));
    }
    let batch = collect_group_commit_batch(append(b"first"), || q.pop_front(), 3, AOF_GROUP_COMMIT_MAX_BYTES);
    assert_eq!(
        batch.data.len(),
        3,
        "max_batch=3 bounds the batch to first + 2 drained"
    );
    assert!(batch.deferred_control.is_none());
    assert_eq!(q.len(), 8, "the remaining 8 stay queued for the next batch");
}

/// batch_cap_exceeded (bytes) — a soft byte cap stops the drain once the
/// accumulated payload reaches max_bytes (a message is never split).
#[test]
fn collect_respects_max_bytes() {
    let mut q: VecDeque<AofMessage> = VecDeque::new();
    for _ in 0..10 {
        q.push_back(append(&[0u8; 100]));
    }
    // first=100 → +100=200 (<250) → +100=300 (≥250, include then stop) ⇒ 3 messages.
    let batch = collect_group_commit_batch(append(&[0u8; 100]), || q.pop_front(), AOF_GROUP_COMMIT_MAX_BATCH, 250);
    assert_eq!(
        batch.data.len(),
        3,
        "byte cap stops the drain once accumulated ≥ max_bytes (whole-message granularity)"
    );
}

// --- commit: the durability invariant -------------------------------------

/// M1 — K queued AppendSync writes are made durable by exactly ONE fsync, and
/// every waiter is acked Synced; the fsync happens AFTER all K writes.
#[test]
fn commit_one_fsync_many_acks() {
    let (m1, rx1) = append_sync(b"a");
    let (m2, rx2) = append_sync(b"b");
    let (m3, rx3) = append_sync(b"c");
    let mut batch = GroupCommitBatch {
        data: vec![m1, m2, m3],
        deferred_control: None,
    };
    let mut sink = CountingSink::new();

    let outcome = commit_group_commit_batch(&mut sink, &mut batch, true);

    assert_eq!(sink.sync_calls, 1, "exactly ONE fsync for the whole batch");
    assert_eq!(sink.writes.len(), 3, "all three payloads written");
    assert_eq!(
        sink.synced_at_write_count,
        Some(3),
        "the fsync runs AFTER all writes (ack-after-fsync ordering)"
    );
    assert_eq!(
        outcome,
        CommitOutcome {
            synced: 3,
            write_failed: false,
            fsync_failed: false
        }
    );
    assert_eq!(rx1.try_recv(), Ok(AofAck::Synced));
    assert_eq!(rx2.try_recv(), Ok(AofAck::Synced));
    assert_eq!(rx3.try_recv(), Ok(AofAck::Synced));
}

/// batch_write_failed — a write_all failure mid-batch acks WriteFailed and NO
/// waiter in the batch is acked Synced (no false durability claim).
#[test]
fn commit_write_fail_acks_write_failed() {
    let (m1, rx1) = append_sync(b"a");
    let (m2, rx2) = append_sync(b"b");
    let mut batch = GroupCommitBatch {
        data: vec![m1, m2],
        deferred_control: None,
    };
    let mut sink = CountingSink::new();
    sink.fail_write_at = Some(1); // the 2nd write fails

    let outcome = commit_group_commit_batch(&mut sink, &mut batch, true);

    assert!(outcome.write_failed, "the batch must report a write failure");
    assert_eq!(outcome.synced, 0, "no waiter may be acked Synced on a write failure");
    assert_ne!(rx1.try_recv(), Ok(AofAck::Synced));
    assert_ne!(rx2.try_recv(), Ok(AofAck::Synced));
    assert_eq!(rx2.try_recv(), Ok(AofAck::WriteFailed));
}

/// batch_fsync_failed (+ ack_before_fsync) — when the single fsync fails, ALL
/// AppendSync waiters are acked FsyncFailed and NONE Synced. That none are
/// Synced proves no ack was emitted before the fsync result was known.
#[test]
fn commit_fsync_fail_acks_fsync_failed() {
    let (m1, rx1) = append_sync(b"a");
    let (m2, rx2) = append_sync(b"b");
    let mut batch = GroupCommitBatch {
        data: vec![m1, m2],
        deferred_control: None,
    };
    let mut sink = CountingSink::new();
    sink.fail_sync = true;

    let outcome = commit_group_commit_batch(&mut sink, &mut batch, true);

    assert!(outcome.fsync_failed, "the batch must report an fsync failure");
    assert_eq!(outcome.synced, 0, "no waiter may be acked Synced when the fsync fails");
    assert_eq!(rx1.try_recv(), Ok(AofAck::FsyncFailed));
    assert_eq!(rx2.try_recv(), Ok(AofAck::FsyncFailed));
}

/// M4 everysec_path_unchanged — under do_fsync=false the commit performs NO
/// fsync; an Append-only batch is written in order and acks nothing.
#[test]
fn commit_everysec_no_fsync() {
    let mut batch = GroupCommitBatch {
        data: vec![append(b"x"), append(b"y")],
        deferred_control: None,
    };
    let mut sink = CountingSink::new();

    let outcome = commit_group_commit_batch(&mut sink, &mut batch, false);

    assert_eq!(sink.sync_calls, 0, "everysec (do_fsync=false) must not fsync per batch");
    assert_eq!(sink.writes.len(), 2, "bytes are still written in order");
    assert_eq!(outcome.synced, 0, "no AppendSync waiters in an everysec batch");
}

/// M2 barrier_property_preserved — a single batch fsync makes preceding
/// fire-and-forget Append bytes durable too (ordered-channel H1-BARRIER): the
/// fsync runs after the appends are written, and the trailing AppendSync barrier
/// is acked Synced.
#[test]
fn commit_barrier_covers_preceding_appends() {
    let (barrier, rx) = append_sync(b""); // zero-length AppendSync barrier
    let mut batch = GroupCommitBatch {
        data: vec![append(b"one"), append(b"two"), barrier],
        deferred_control: None,
    };
    let mut sink = CountingSink::new();

    let outcome = commit_group_commit_batch(&mut sink, &mut batch, true);

    assert_eq!(sink.sync_calls, 1, "one fsync covers the appends + the barrier");
    assert_eq!(
        sink.synced_at_write_count,
        Some(3),
        "the fsync runs after the two appends AND the barrier are written"
    );
    assert_eq!(outcome.synced, 1, "the barrier AppendSync is acked");
    assert_eq!(rx.try_recv(), Ok(AofAck::Synced));
}
