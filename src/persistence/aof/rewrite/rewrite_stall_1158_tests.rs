//! moon#1158: a per-shard rewrite dispatched while a writer is still
//! draining the previous rewrite's overflow must never be lost.
//!
//! The previous rewrite committed (terminal `shard_done`) and released the
//! BGREWRITEAOF in-progress flag BEFORE every writer had finished its
//! post-fold overflow drain. On a slow disk that drain takes seconds (the
//! soak: 1.8M spilled appends, 14 s), so the auto-rewrite monitor dispatched
//! the next rewrite into the channel of a writer still inside
//! `RewriteOverflow::finish_framed`. The drain consumed the `RewritePerShard`
//! and dropped it as "redundant": its countdown never reached zero, the flag
//! stayed set forever, no rewrite ever ran again, and the incr AOF grew
//! until the disk filled.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use ringbuf::HeapRb;
use ringbuf::traits::Split;

use crate::command::persistence::bgrewriteaof_start_sharded_with_flag;
use crate::persistence::aof::rewrite::RewriteOverflow;
use crate::persistence::aof::{
    AofMessage, AofWriterPool, DEFAULT_AOF_FSYNC_TIMEOUT, FoldEpoch, FsyncPolicy,
    PerShardRewriteCoord,
};
use crate::persistence::aof_manifest::AofManifest;
use crate::protocol::Frame;
use crate::runtime::channel;
use crate::shard::dispatch::ShardMessage;
use crate::shard::shared_databases::ShardDatabases;
use crate::storage::Database;

fn append(lsn: u64, payload: &'static [u8]) -> AofMessage {
    AofMessage::Append {
        lsn,
        db: 0,
        bytes: Bytes::from_static(payload),
        epoch: FoldEpoch::INITIAL,
    }
}

fn manifest_at(dir: &std::path::Path, shards: u16) -> (Arc<parking_lot::Mutex<AofManifest>>, u64) {
    let m = AofManifest::initialize_multi(dir, shards).expect("manifest");
    let seq = m.seq;
    (Arc::new(parking_lot::Mutex::new(m)), seq)
}

/// A `RewritePerShard` for shard 0 of `coord`, exactly as
/// `try_send_rewrite_per_shard` builds it.
fn rewrite_request(coord: &Arc<PerShardRewriteCoord>) -> AofMessage {
    let (shard_dbs, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
    let (prod, _cons) = HeapRb::<ShardMessage>::new(1).split();
    AofMessage::RewritePerShard {
        shard_dbs,
        coord: coord.clone(),
        fold_producer: Arc::new(parking_lot::Mutex::new(prod)),
        fold_notifier: Arc::new(channel::Notify::new()),
        overflow: Arc::new(RewriteOverflow::new()),
    }
}

/// The root cause. Every writer holds its `RewritePerShard` (and so the
/// coordinator) until it has finished the post-fold overflow drain and is
/// back in its recv loop. The in-progress flag, which gates the next
/// dispatch, must stay set until the LAST writer lets go, not merely until
/// the terminal `shard_done` commits: a rewrite dispatched in between lands
/// in the channel of a writer still draining.
#[test]
fn the_in_progress_flag_is_held_until_every_writer_has_finished_its_drain() {
    static IN_PROGRESS: AtomicBool = AtomicBool::new(false);
    let tmp = tempfile::tempdir().expect("tempdir");
    let (manifest, seq) = manifest_at(tmp.path(), 2);
    let coord = PerShardRewriteCoord::with_in_progress_flag(manifest, seq, 2usize, &IN_PROGRESS);
    let new_seq = coord.new_seq();
    // bgrewriteaof_start_sharded's CAS.
    IN_PROGRESS.store(true, Ordering::SeqCst);

    // The fan-out hands each writer a clone; the dispatcher's handle drops.
    let writer_a = coord.clone();
    let writer_b = coord.clone();
    drop(coord);

    // Both writers fold; B's decrement is terminal and commits.
    writer_a.shard_done();
    writer_b.shard_done();
    assert_eq!(
        *writer_b.outcome.lock(),
        Some(new_seq),
        "the rewrite committed"
    );

    // Both writers are now in `finish_framed`, draining the spill window.
    assert!(
        IN_PROGRESS.load(Ordering::SeqCst),
        "moon#1158: the flag was released while writers still drain the \
         rewrite's overflow — the next dispatch lands mid-drain and is lost"
    );
    drop(writer_a);
    assert!(
        IN_PROGRESS.load(Ordering::SeqCst),
        "writer B is still draining"
    );
    drop(writer_b);
    assert!(
        !IN_PROGRESS.load(Ordering::SeqCst),
        "the last writer to finish must release the flag"
    );
}

/// Defence in depth, and the exact soak interleaving: rewrite N's writer is
/// in `finish_framed` with a spilled overflow when rewrite N+1's request is
/// in its channel. The drain must not swallow the request silently — that
/// wedges N+1's countdown (and the flag) forever. It must resolve it: abort
/// that rewrite loudly so the countdown closes, every barrier waiter wakes,
/// and the monitor can dispatch again. The drained appends are unaffected.
#[test]
fn a_rewrite_request_surfacing_in_the_overflow_drain_is_resolved_not_swallowed() {
    static IN_PROGRESS: AtomicBool = AtomicBool::new(false);
    let tmp = tempfile::tempdir().expect("tempdir");
    let (manifest, seq) = manifest_at(tmp.path(), 1);
    let next = PerShardRewriteCoord::with_in_progress_flag(manifest, seq, 1, &IN_PROGRESS);
    IN_PROGRESS.store(true, Ordering::SeqCst);

    // Rewrite N's fold window: the channel saturated and appends spilled.
    let ovf = RewriteOverflow::new();
    ovf.arm();
    let (tx, rx) = channel::mpsc_bounded::<AofMessage>(8);
    tx.try_send(append(1, b"chan-a")).expect("send");
    assert!(ovf.try_spill(append(2, b"spill-b")).is_ok());
    // Rewrite N committed and released the flag; the monitor dispatched N+1.
    tx.try_send(rewrite_request(&next)).expect("send rewrite");

    let path = tmp.path().join("incr.aof");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("incr");
    let mut db_ctx = 0usize;
    ovf.finish_framed(&rx, &mut file, &mut db_ctx, FoldEpoch::INITIAL)
        .expect("drain");
    assert!(
        ovf.in_progress_at_finish_for_test().is_some(),
        "the post-fold drain ran"
    );

    assert_eq!(
        next.remaining.load(Ordering::Acquire),
        0,
        "moon#1158: the drain swallowed rewrite N+1 — its countdown never \
         closes, so the in-progress flag is held forever"
    );
    assert_eq!(
        *next.outcome.lock(),
        Some(seq),
        "the unrunnable request must abort, keeping the old generation"
    );
    assert!(next.failed.load(Ordering::Acquire));
    drop(next);
    assert!(
        !IN_PROGRESS.load(Ordering::SeqCst),
        "the aborted request must release the flag so the monitor retries"
    );

    let data = std::fs::read(&path).expect("read incr");
    for rec in [&b"chan-a"[..], &b"spill-b"[..]] {
        assert!(
            data.windows(rec.len()).any(|w| w == rec),
            "drained appends are still written"
        );
    }
}

/// Two per-shard writer channels plus the fold channels a pool needs to
/// reach the fan-out loop. Shard 1's receiver is dropped, so the fan-out
/// fails AFTER shard 0 has received its `RewritePerShard`.
fn half_dead_pool(
    dir: &std::path::Path,
) -> (Arc<AofWriterPool>, channel::MpscReceiver<AofMessage>) {
    AofManifest::initialize_multi(dir, 2).expect("manifest");
    let (tx0, rx0) = channel::mpsc_bounded::<AofMessage>(4);
    let (tx1, rx1) = channel::mpsc_bounded::<AofMessage>(4);
    drop(rx1);
    let mut producers = Vec::new();
    let mut notifiers = Vec::new();
    for _ in 0..2 {
        let (prod, cons) = HeapRb::<ShardMessage>::new(1).split();
        // The consumer is never polled: the writers never fold in this test.
        std::mem::forget(cons);
        producers.push(Arc::new(parking_lot::Mutex::new(prod)));
        notifiers.push(Arc::new(channel::Notify::new()));
    }
    let pool = AofWriterPool::per_shard_with_fold_channels(
        vec![tx0, tx1],
        FsyncPolicy::EverySec,
        DEFAULT_AOF_FSYNC_TIMEOUT,
        dir.to_path_buf(),
        producers,
        notifiers,
    );
    (pool, rx0)
}

fn two_shard_dbs() -> Arc<ShardDatabases> {
    let (shard_dbs, _inits) =
        ShardDatabases::new(vec![vec![Database::new()], vec![Database::new()]]);
    shard_dbs
}

/// A fan-out that fails part-way has already handed the rewrite to the
/// writers before the failure; they fold (or roll back) and drain exactly
/// like a successful rewrite. The command handler must therefore not clear
/// the flag on the error path — it would let the next rewrite be dispatched
/// into a writer that is still draining this one (the moon#1158 loss). The
/// flag goes when the last writer that received the rewrite lets go of it.
#[test]
fn a_partially_failed_fan_out_leaves_the_flag_to_the_writers_that_received_it() {
    static IN_PROGRESS: AtomicBool = AtomicBool::new(false);
    let tmp = tempfile::tempdir().expect("tempdir");
    let (pool, rx0) = half_dead_pool(tmp.path());

    let frame = bgrewriteaof_start_sharded_with_flag(&pool, two_shard_dbs(), &IN_PROGRESS);
    assert!(
        matches!(frame, Frame::Error(_)),
        "shard 1 is gone: {frame:?}"
    );

    let delivered = rx0.try_recv().expect("shard 0 received the rewrite first");
    assert!(
        IN_PROGRESS.load(Ordering::SeqCst),
        "moon#1158: the failed fan-out cleared the flag while shard 0 still \
         holds the rewrite — the next dispatch can land in its drain"
    );
    // Shard 0 finishes (folds, drains) and drops its request.
    drop(delivered);
    assert!(
        !IN_PROGRESS.load(Ordering::SeqCst),
        "the last holder must release the flag"
    );
}

/// The other side of the ownership rule: a fan-out that fails before any
/// coordinator exists (no manifest) has no writer to release the flag, so
/// the flag must be released on that path.
#[test]
fn a_rewrite_that_never_reached_a_writer_releases_the_flag() {
    static IN_PROGRESS: AtomicBool = AtomicBool::new(false);
    let tmp = tempfile::tempdir().expect("tempdir");
    let (tx0, _rx0) = channel::mpsc_bounded::<AofMessage>(4);
    let (tx1, _rx1) = channel::mpsc_bounded::<AofMessage>(4);
    // No manifest on disk: the fan-out refuses before building a coordinator.
    let pool = AofWriterPool::per_shard_with_base_dir(
        vec![tx0, tx1],
        FsyncPolicy::EverySec,
        DEFAULT_AOF_FSYNC_TIMEOUT,
        tmp.path().to_path_buf(),
    );
    let frame = bgrewriteaof_start_sharded_with_flag(&pool, two_shard_dbs(), &IN_PROGRESS);
    assert!(matches!(frame, Frame::Error(_)), "no manifest: {frame:?}");
    assert!(
        !IN_PROGRESS.load(Ordering::SeqCst),
        "nothing holds the rewrite"
    );
}

/// The same loss on the TopLevel tokio writer (`--shards 1`): it released
/// the flag right after the fold and only THEN drained the fold window's
/// channel backlog + spilled overflow. A rewrite dispatched in that window
/// was consumed by the drain and dropped, and nothing ever cleared the flag
/// its dispatcher had set. The drain must run while the flag is still held.
#[cfg(feature = "runtime-tokio")]
#[tokio::test]
async fn the_toplevel_tokio_writer_drains_before_releasing_the_flag() {
    use crate::command::persistence::AOF_REWRITE_IN_PROGRESS;
    use crate::persistence::aof::aof_writer_task;
    use crate::runtime::cancel::CancellationToken;

    let tmp = tempfile::tempdir().expect("tempdir");
    let (tx, rx) = channel::mpsc_bounded::<AofMessage>(16);
    let cancel = CancellationToken::new();
    // No fold channels: the fold aborts at once, but the writer still runs
    // the release + drain sequence under test.
    let writer = tokio::spawn(aof_writer_task(
        rx,
        tmp.path().join("appendonly.aof"),
        FsyncPolicy::EverySec,
        cancel.clone(),
        None,
    ));
    let overflow = Arc::new(RewriteOverflow::new());
    let (shard_dbs, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);
    // bgrewriteaof_start_sharded's CAS (this writer releases the global flag).
    AOF_REWRITE_IN_PROGRESS.store(true, Ordering::SeqCst);
    tx.try_send(AofMessage::RewriteSharded(shard_dbs, overflow.clone()))
        .expect("send rewrite");
    // Disconnect rather than send Shutdown: a Shutdown queued behind the
    // rewrite is consumed by the post-fold drain, and the writer then exits
    // on disconnect anyway.
    drop(tx);
    tokio::time::timeout(std::time::Duration::from_secs(30), writer)
        .await
        .expect("writer exits")
        .expect("writer task");

    assert_eq!(
        overflow.in_progress_at_finish_for_test(),
        Some(true),
        "moon#1158: the TopLevel tokio writer released the flag before its \
         post-fold drain — a rewrite dispatched then is consumed by the drain"
    );
    assert!(
        !AOF_REWRITE_IN_PROGRESS.load(Ordering::SeqCst),
        "the rewrite released the flag once it finished"
    );
}
