//! moon#1188 — a segment rotation fsyncs the old segment on the sync agent,
//! not inline on the shard thread, and the next segment never exists before
//! the old one is durable (the mid-chain tear policy's invariant).

use super::*;
use crate::persistence::wal_v3::record::WalRecordType;

// ---------------------------------------------------------------------
// moon#1188 — segment rotation fsyncs on the sync agent, not inline.
// ---------------------------------------------------------------------

/// A controllable fsync backend: blocks while closed, counts calls,
/// optionally fails.
struct FsyncGate {
    open: parking_lot::Mutex<bool>,
    cv: parking_lot::Condvar,
    calls: std::sync::atomic::AtomicUsize,
    fail: std::sync::atomic::AtomicBool,
}

impl FsyncGate {
    fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            open: parking_lot::Mutex::new(false),
            cv: parking_lot::Condvar::new(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            fail: std::sync::atomic::AtomicBool::new(false),
        })
    }
    fn open(&self) {
        *self.open.lock() = true;
        self.cv.notify_all();
    }
    fn close(&self) {
        *self.open.lock() = false;
    }
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn agent(self: &std::sync::Arc<Self>) -> crate::persistence::wal_v3::sync_agent::WalSyncAgent {
        let gate = std::sync::Arc::clone(self);
        crate::persistence::wal_v3::sync_agent::WalSyncAgent::spawn_with_backend(0, move |f| {
            gate.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut open = gate.open.lock();
            // Released by `open()` — or once the test dropped its handle: a
            // failing test unwinds and drops the writer, which joins this
            // agent, so an fsync held in the gate forever would hang the
            // test binary instead of reporting the failure.
            while !*open && std::sync::Arc::strong_count(&gate) > 1 {
                gate.cv
                    .wait_for(&mut open, std::time::Duration::from_millis(10));
            }
            drop(open);
            if gate.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(std::io::Error::other("injected fsync failure"));
            }
            f.sync_data()
        })
        .expect("spawn gated agent")
    }
}

fn replayed_lsns(wal_dir: &std::path::Path) -> Vec<u64> {
    let mut lsns = Vec::new();
    crate::persistence::wal_v3::replay::replay_wal_v3_dir_until_with_salvage(
        wal_dir,
        0,
        None,
        false,
        &mut |r| lsns.push(r.lsn),
        &mut |_| {},
    )
    .expect("replay must succeed (no mid-chain tear)");
    lsns
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !f() {
        assert!(std::time::Instant::now() < deadline, "condition never held");
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// The rotation's fsync of the old segment runs on the agent, the
/// shard-side call returns while it is in flight, and the next segment
/// does not exist until the agent reports the old one durable. At HEAD
/// `935c555` the rotation ran `sync_data()` inline and created the next
/// segment before returning.
#[test]
fn test_1188_rotation_offloads_fsync_and_next_segment_waits_for_it() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    writer.install_sync_agent_for_test(gate.agent());

    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    // Everything buffered overflows the 512-byte segment: rotation. The
    // gate is CLOSED — an inline fsync would still return, so what
    // proves the offload is that the agent got the old segment's fsync
    // and the next segment does not exist yet.
    writer.request_sync().unwrap();
    wait_until(|| gate.calls() == 1);
    let seg2 = WalSegment::segment_path(&wal_dir, 2);
    assert!(
        writer.rotation_pending(),
        "rotation must wait for the agent"
    );
    assert!(
        !seg2.exists(),
        "next segment created before the old one is durable"
    );
    assert_eq!(writer.current_segment_sequence(), 1);
    assert_eq!(writer.rotation_counts(), (1, 0));

    // Appends keep landing — in memory — while the fsync is in flight.
    for _ in 0..5 {
        writer.append(WalRecordType::Command, b"SET k2 v2");
    }
    writer.flush_if_needed().unwrap();
    assert!(!seg2.exists());

    gate.open();
    wait_until(|| {
        writer.flush_if_needed().unwrap();
        !writer.rotation_pending()
    });
    assert!(seg2.exists(), "rotation must complete once durable");
    assert_eq!(writer.current_segment_sequence(), 2);
    // The request that arrived mid-rotation was re-issued for segment 2.
    wait_until(|| gate.calls() >= 2);
    writer.wait_durable(35, WAIT_DURABLE_TIMEOUT).unwrap();
    assert_eq!(replayed_lsns(&wal_dir), (1..=35).collect::<Vec<_>>());
}

/// SIGKILL while the old segment's fsync is in flight: the next segment
/// does not exist, so the (possibly torn) old segment is the FINAL one
/// and replay keeps its valid prefix — never a mid-chain tear.
#[test]
fn test_1188_kill_during_pending_rotation_leaves_a_final_segment_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.request_sync().unwrap();
    wait_until(|| gate.calls() == 1);
    for _ in 0..5 {
        writer.append(WalRecordType::Command, b"SET lost v");
    }
    writer.flush_if_needed().unwrap();
    // "Kill": the process state vanishes without a flush or a drop.
    std::mem::forget(writer);
    assert!(!WalSegment::segment_path(&wal_dir, 2).exists());
    // Records 1..=30 reached the old segment's page cache; 31..=35 were
    // only in process memory, and a kill loses them. HEAD `935c555` loses
    // these five too (a buffer under 4 KiB waits for the 1 s sync timer),
    // but a pending rotation holds EVERY append in memory — up to
    // PENDING_ROTATION_MAX_BUFFER or PENDING_ROTATION_MAX_AGE — where HEAD
    // wrote 4 KiB and more to the page cache each tick, so a kill in the
    // window loses more of the WAL-only planes (workspace, MQ, temporal)
    // than at HEAD; still within the everysec second (moon#1221 review R3).
    // `test_1221_r3_pending_rotation_is_bounded_in_time` pins the bound.
    assert_eq!(replayed_lsns(&wal_dir), (1..=30).collect::<Vec<_>>());
    gate.open(); // release the leaked agent thread
}

/// A poisoned agent never publishes the watermark, and the pending rotation
/// must not stand in for it (moon#1221 review R2): after a failed fsync a
/// retry succeeds without proving anything, so the rotation does not fsync
/// again — it fails loudly and keeps the next segment unopened while under
/// the memory bound, and past the bound opens the next segment WITHOUT any
/// durability claim so memory stays bounded. The data is kept either way.
#[test]
fn test_1188_poisoned_rotation_fails_loud_and_stays_bounded() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    gate.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.flush_write().unwrap();
    assert!(writer.rotation_pending());
    gate.open();
    wait_until(|| writer.sync_agent.as_ref().is_some_and(|a| a.is_poisoned()));
    assert!(writer.flush_if_needed().is_err(), "must fail loudly");
    assert!(writer.rotation_pending());
    assert!(!WalSegment::segment_path(&wal_dir, 2).exists());
    assert_eq!(writer.rotation_counts(), (1, 0), "no inline fsync retry");

    // Past the memory bound: the next segment opens, with no claim.
    let big = vec![0x5Au8; 64 * 1024];
    let mut last = 30;
    while writer.buffered_bytes() <= PENDING_ROTATION_MAX_BUFFER {
        last = writer.append(WalRecordType::Command, &big);
    }
    assert!(writer.flush_if_needed().is_err(), "still failing loudly");
    assert!(!writer.rotation_pending());
    assert!(WalSegment::segment_path(&wal_dir, 2).exists());
    assert_eq!(writer.rotations_degraded(), 1);
    assert_eq!(writer.buffered_bytes(), 0, "the buffer left process memory");
    assert!(writer.sync_agent.as_ref().unwrap().durable_lsn() < 30);
    assert!(
        writer
            .wait_durable(1, std::time::Duration::from_millis(50))
            .is_err()
    );
    assert!(writer.request_sync().is_err());
    assert_eq!(replayed_lsns(&wal_dir), (1..=last).collect::<Vec<_>>());
    assert_eq!(gate.calls(), 1, "a poisoned WAL never fsyncs again");
}

/// Memory stays bounded when the disk is the bottleneck: past
/// PENDING_ROTATION_MAX_BUFFER of buffered appends the rotation is
/// completed inline.
#[test]
fn test_1188_pending_rotation_buffer_is_bounded() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.flush_write().unwrap();
    assert!(writer.rotation_pending());
    let big = vec![0x5Au8; 64 * 1024];
    let mut appended = 30u64;
    let mut peak = 0usize;
    while writer.rotation_counts().1 == 0 {
        assert!(appended < 1_000, "the memory bound never engaged");
        writer.append(WalRecordType::Command, &big);
        appended += 1;
        peak = peak.max(writer.buffered_bytes());
        writer.flush_if_needed().unwrap();
    }
    // The old segment was completed inline once the buffer crossed the
    // bound, and the buffer went to disk.
    assert!(peak <= PENDING_ROTATION_MAX_BUFFER + big.len() + 64);
    assert!(writer.buffered_bytes() < big.len());
    gate.open();
    writer.flush_sync().unwrap();
    assert_eq!(replayed_lsns(&wal_dir), (1..=appended).collect::<Vec<_>>());
}

/// `wait_durable` for an LSN past a pending rotation waits for the old
/// segment, opens the next, and returns only once `lsn` is durable.
#[test]
fn test_1188_wait_durable_spans_a_pending_rotation() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.flush_write().unwrap();
    assert!(writer.rotation_pending());
    let lsn = writer.append(WalRecordType::Command, b"SET last v");
    let opener = {
        let gate = std::sync::Arc::clone(&gate);
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(50));
            gate.open();
        })
    };
    writer.wait_durable(lsn, WAIT_DURABLE_TIMEOUT).unwrap();
    opener.join().unwrap();
    assert!(!writer.rotation_pending());
    assert!(WalSegment::segment_path(&wal_dir, 2).exists());
    assert!(writer.sync_agent.as_ref().unwrap().durable_lsn() >= lsn);
    assert_eq!(replayed_lsns(&wal_dir), (1..=lsn).collect::<Vec<_>>());
}

/// The inline `flush_sync` (shutdown) completes a pending rotation first
/// and leaves every record durable, in order, with no gap.
#[test]
fn test_1188_flush_sync_completes_a_pending_rotation() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    gate.open();
    writer.install_sync_agent_for_test(gate.agent());
    for round in 0..6u64 {
        for _ in 0..25 {
            writer.append(WalRecordType::Command, b"SET k v");
        }
        if round % 2 == 0 {
            writer.request_sync().unwrap();
        } else {
            writer.flush_if_needed().unwrap();
        }
    }
    writer.flush_sync().unwrap();
    assert!(!writer.rotation_pending());
    assert_eq!(replayed_lsns(&wal_dir), (1..=150).collect::<Vec<_>>());
    // Contiguous chain: no sequence gap.
    let max = writer.current_segment_sequence();
    for seq in 1..=max {
        assert!(
            WalSegment::segment_path(&wal_dir, seq).exists(),
            "gap at {seq}"
        );
    }
}

// ---------------------------------------------------------------------
// moon#1221 review R1 — a rotated segment's header `base_lsn` is the LSN
// of its first record (refs moon#1188).
// ---------------------------------------------------------------------

/// `(header base_lsn, first record's LSN)` of segment `seq`; the second is
/// `None` for a header-only segment.
fn header_and_first_lsn(wal_dir: &std::path::Path, seq: u64) -> (u64, Option<u64>) {
    let data = std::fs::read(WalSegment::segment_path(wal_dir, seq)).unwrap();
    let base = u64::from_le_bytes(data[28..36].try_into().unwrap());
    let first = (data.len() > WAL_V3_HEADER_SIZE)
        .then(|| read_wal_v3_record(&data[WAL_V3_HEADER_SIZE..]).map(|r| r.lsn))
        .flatten();
    (base, first)
}

/// The reviewer's reproduction: records appended while the old segment's
/// fsync is in flight land in the NEXT segment, so its header must name the
/// first of them (`upto + 1`), not `next_lsn` at the moment it was opened.
/// At the PR head the header read 36 for a segment whose first record is 31.
#[test]
fn test_1221_r1_rotated_segment_header_names_its_first_record() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.request_sync().unwrap();
    wait_until(|| gate.calls() == 1);
    assert!(writer.rotation_pending());
    for _ in 0..5 {
        writer.append(WalRecordType::Command, b"SET k2 v2");
    }
    gate.open();
    wait_until(|| {
        writer.flush_if_needed().unwrap();
        !writer.rotation_pending()
    });
    writer.flush_sync().unwrap();
    let (base, first) = header_and_first_lsn(&wal_dir, 2);
    assert_eq!(
        first,
        Some(31),
        "segment 2 must start at the first buffered record"
    );
    assert_eq!(
        Some(base),
        first,
        "segment 2 header base_lsn must be its first record's LSN"
    );
}

/// Every segment a rotation opens names its first record: with appends
/// buffered during the in-flight fsync, with none, and back to back (the
/// buffer released by one rotation overflows the new segment at once).
#[test]
fn test_1221_r1_every_rotated_segment_header_names_its_first_record() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    writer.install_sync_agent_for_test(gate.agent());
    let finish = |writer: &mut WalWriterV3| {
        wait_until(|| {
            writer.flush_if_needed().unwrap();
            !writer.rotation_pending()
        })
    };

    // (a) 5 appends buffered while segment 1's fsync is in flight.
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.request_sync().unwrap();
    assert!(writer.rotation_pending());
    for _ in 0..5 {
        writer.append(WalRecordType::Command, b"SET a v");
    }
    gate.open();
    finish(&mut writer);

    // (b) no append while segment 2's fsync is in flight.
    for _ in 0..20 {
        writer.append(WalRecordType::Command, b"SET b v");
    }
    writer.flush_write().unwrap();
    assert!(writer.rotation_pending());
    finish(&mut writer);
    writer.append(WalRecordType::Command, b"SET b2 v");
    writer.flush_write().unwrap();

    // (c) back to back: more than a segment buffered while segment 3's
    // fsync is in flight, so completing the rotation rotates again.
    gate.close();
    for _ in 0..20 {
        writer.append(WalRecordType::Command, b"SET c v");
    }
    writer.flush_write().unwrap();
    assert!(writer.rotation_pending());
    for _ in 0..40 {
        writer.append(WalRecordType::Command, b"SET d v");
    }
    writer.request_sync().unwrap();
    gate.open();
    finish(&mut writer);
    for _ in 0..3 {
        writer.append(WalRecordType::Command, b"SET e v");
    }
    writer.flush_sync().unwrap();

    let last = writer.current_lsn() - 1;
    assert_eq!(replayed_lsns(&wal_dir), (1..=last).collect::<Vec<_>>());
    let max = writer.current_segment_sequence();
    assert!(
        max >= 5,
        "the scenario must rotate at least 4 times, got {max}"
    );
    for seq in 1..=max {
        let (base, first) = header_and_first_lsn(&wal_dir, seq);
        match first {
            Some(first) => assert_eq!(base, first, "segment {seq}: header base_lsn"),
            // Only the active segment can be header-only: it names the
            // next LSN to be assigned.
            None => {
                assert_eq!(seq, max, "sealed segment {seq} holds no record");
                assert_eq!(base, writer.current_lsn(), "segment {seq}: header base_lsn");
            }
        }
    }
}

// ---------------------------------------------------------------------
// moon#1221 review R2 — an fsync that failed is never followed by a
// durability claim.
// ---------------------------------------------------------------------

/// The reviewer's reproduction. The agent's fsync of segment 1 fails and
/// poisons it; at the PR head the next tick's fallback fsynced the same file
/// inline — the kernel had already reported the error to the agent's fd, so
/// that fsync succeeds whatever reached the disk — and published LSN 30:
/// `wait_durable(30)` returned Ok and the checkpoint's log-before-data rule
/// accepted pages whose WAL fsync failed.
#[test]
fn test_1221_r2_fallback_never_publishes_a_failed_agent_fsync() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    gate.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.flush_write().unwrap();
    assert!(writer.rotation_pending());
    gate.open();
    wait_until(|| writer.sync_agent.as_ref().is_some_and(|a| a.is_poisoned()));
    let polled = writer.flush_if_needed();
    let watermark = writer.sync_agent.as_ref().unwrap().durable_lsn();
    assert!(
        watermark < 30,
        "the fallback published the failed segment as durable: watermark {watermark}"
    );
    let waited = writer.wait_durable(30, std::time::Duration::from_millis(50));
    assert!(
        waited.is_err(),
        "wait_durable(30) returned Ok after the fsync covering it failed"
    );
    assert!(polled.is_err(), "a poisoned rotation must fail loudly");
    assert!(
        writer.rotation_pending() && !WalSegment::segment_path(&wal_dir, 2).exists(),
        "a poisoned rotation under the memory bound neither retries the fsync nor opens the next segment"
    );
}

/// Poison is checked BEFORE the watermark: once any fsync on the WAL failed,
/// no LSN is reported durable again, even one a genuine fsync covered — the
/// contract the module docs state ("fail every subsequent wait_durable").
/// At the PR head the fast path returned Ok for any LSN under the watermark.
#[test]
fn test_1221_r2_poison_is_checked_before_the_watermark() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer =
        WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    gate.open();
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..10 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.request_sync().unwrap();
    writer.wait_durable(10, WAIT_DURABLE_TIMEOUT).unwrap();
    gate.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    for _ in 0..10 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.request_sync().unwrap();
    wait_until(|| writer.sync_agent.as_ref().is_some_and(|a| a.is_poisoned()));
    assert!(
        writer
            .wait_durable(5, std::time::Duration::from_millis(50))
            .is_err(),
        "a poisoned WAL must fail every durability wait, below the watermark too"
    );
    assert!(writer.request_sync().is_err());
}

/// The finer race behind R2: the old segment's fsync is still in flight on
/// the agent when the memory bound makes the writer complete the rotation
/// with its own inline fsync. If the agent's fsync then fails, the kernel
/// reported the error to the agent's fd — the writer's inline fsync proved
/// nothing — so the writer must not have published the watermark over it.
/// At the PR head the inline completion published LSN 30 at once, and
/// `wait_durable(30)` returned Ok after the agent's fsync failed.
#[test]
fn test_1221_r2_inline_completion_never_publishes_over_an_in_flight_agent_fsync() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    gate.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.flush_write().unwrap();
    wait_until(|| gate.calls() == 1); // the agent is inside the (gated) fsync
    let big = vec![0x5Au8; 64 * 1024];
    while writer.rotation_counts().1 == 0 {
        writer.append(WalRecordType::Command, &big);
        writer.flush_if_needed().unwrap();
    }
    // The memory bound completed segment 1's rotation inline.
    assert!(
        writer.sync_agent.as_ref().unwrap().durable_lsn() < 30,
        "the inline completion published over an agent fsync still in flight"
    );
    gate.open(); // ... which now fails
    wait_until(|| writer.sync_agent.as_ref().is_some_and(|a| a.is_poisoned()));
    assert!(
        writer
            .wait_durable(30, std::time::Duration::from_millis(50))
            .is_err(),
        "wait_durable(30) returned Ok after the agent's fsync of segment 1 failed"
    );
}

/// `flush_sync` (shutdown, the inline fallback) on a WAL whose agent fsync
/// failed: at the PR head it completed the pending rotation with an inline
/// fsync of the same file and published every LSN durable. It must keep the
/// data (page cache) but claim nothing and fail loudly.
#[test]
fn test_1221_r2_flush_sync_on_a_poisoned_wal_keeps_the_data_and_claims_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    gate.fail.store(true, std::sync::atomic::Ordering::SeqCst);
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.flush_write().unwrap();
    for _ in 0..5 {
        writer.append(WalRecordType::Command, b"SET k2 v2");
    }
    gate.open();
    wait_until(|| writer.sync_agent.as_ref().is_some_and(|a| a.is_poisoned()));
    let flushed = writer.flush_sync();
    let watermark = writer.sync_agent.as_ref().unwrap().durable_lsn();
    assert!(
        watermark < 30,
        "flush_sync published a WAL whose fsync failed: watermark {watermark}"
    );
    assert!(
        flushed.is_err(),
        "flush_sync on a poisoned WAL must fail loudly"
    );
    assert!(
        writer
            .wait_durable(35, std::time::Duration::from_millis(50))
            .is_err()
    );
    // Nothing is dropped: every record reached a segment file.
    assert!(!writer.rotation_pending());
    assert_eq!(replayed_lsns(&wal_dir), (1..=35).collect::<Vec<_>>());
}

/// A failed INLINE fsync (no agent: the writer's own thread) is never
/// retried into a durability claim: at the PR head a second `flush_sync`
/// fsynced again — which succeeds once the kernel has reported the error —
/// and returned Ok, and `wait_durable` with no agent returned Ok with it.
#[test]
fn test_1221_r2_a_failed_inline_fsync_is_never_retried_into_a_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer =
        WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE, WalBounds::DEFAULT).unwrap();
    writer.sync_agent_unavailable = true; // inline fsync only
    for _ in 0..10 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.inline_fsync_fault = true;
    assert!(writer.flush_sync().is_err());
    assert!(writer.durability_poisoned());
    assert!(
        writer.flush_sync().is_err(),
        "a retried fsync reported the WAL durable"
    );
    assert!(writer.request_sync().is_err());
    assert!(
        writer
            .wait_durable(10, std::time::Duration::from_millis(50))
            .is_err()
    );
    // The data is kept — in the page cache, never claimed.
    assert_eq!(replayed_lsns(&wal_dir), (1..=10).collect::<Vec<_>>());
}

/// ... and with an agent running, the inline failure poisons it too: it
/// stops fsyncing and never publishes again.
#[test]
fn test_1221_r2_a_failed_inline_fsync_poisons_the_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer =
        WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    gate.open();
    writer.install_sync_agent_for_test(gate.agent());
    for _ in 0..10 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.inline_fsync_fault = true;
    assert!(writer.flush_sync().is_err());
    let agent = writer.sync_agent.as_ref().unwrap();
    assert!(
        agent.is_poisoned(),
        "the agent must share the writer's poison"
    );
    assert_eq!(agent.durable_lsn(), 0);
    for _ in 0..10 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    assert!(writer.request_sync().is_err());
    assert!(
        writer
            .wait_durable(1, std::time::Duration::from_millis(50))
            .is_err()
    );
    assert_eq!(gate.calls(), 0, "a poisoned WAL never fsyncs again");
}

// ---------------------------------------------------------------------
// moon#1221 review R3 — the in-memory window of a pending rotation.
// ---------------------------------------------------------------------

/// While a rotation is pending every append stays in process memory — the
/// next segment does not exist yet — so a SIGKILL loses it. The window is
/// bounded in time as well as bytes: past the age bound the rotation
/// completes with an inline fsync and the buffer reaches the page cache,
/// even while the agent's fsync is stuck — and without claiming anything
/// over that fsync still in flight.
#[test]
fn test_1221_r3_pending_rotation_is_bounded_in_time() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");
    let mut writer = WalWriterV3::new(0, &wal_dir, 512, WalBounds::DEFAULT).unwrap();
    let gate = FsyncGate::new();
    writer.install_sync_agent_for_test(gate.agent());
    writer.set_pending_rotation_max_age_for_test(std::time::Duration::from_millis(50));
    for _ in 0..30 {
        writer.append(WalRecordType::Command, b"SET k v");
    }
    writer.flush_write().unwrap();
    assert!(writer.rotation_pending());
    wait_until(|| gate.calls() == 1); // the agent's fsync is stuck in the gate
    for _ in 0..5 {
        writer.append(WalRecordType::Command, b"SET k2 v2");
    }
    std::thread::sleep(std::time::Duration::from_millis(60));
    writer.flush_if_needed().unwrap();
    assert!(
        !writer.rotation_pending(),
        "the age bound must complete the rotation"
    );
    assert_eq!(writer.rotation_counts(), (1, 1));
    // What a SIGKILL would leave now: every record, in the page cache.
    assert_eq!(replayed_lsns(&wal_dir), (1..=35).collect::<Vec<_>>());
    assert_eq!(
        writer.sync_agent.as_ref().unwrap().durable_lsn(),
        0,
        "nothing published over the agent's fsync still in flight"
    );
    gate.open();
    writer.wait_durable(35, WAIT_DURABLE_TIMEOUT).unwrap();
}

/// Overwrite segment `seq`'s header `base_lsn` (bytes 28..36).
fn set_header_base(wal_dir: &std::path::Path, seq: u64, base: u64) {
    let path = WalSegment::segment_path(wal_dir, seq);
    let mut data = std::fs::read(&path).unwrap();
    data[28..36].copy_from_slice(&base.to_le_bytes());
    std::fs::write(&path, &data).unwrap();
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap().flatten() {
        std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
    }
}

/// A WAL directory written by the unreleased moon#1188 build (headers that
/// overstate `base_lsn`: the LSN after the records appended during the
/// in-flight fsync) stays correct under this fix's readers — the recyclers
/// only ever free LESS than with exact headers, and never a segment holding
/// a record at or past `redo_lsn`.
#[test]
fn test_1221_r1_an_overstated_header_only_makes_recycling_conservative() {
    let tmp = tempfile::tempdir().unwrap();
    let exact = tmp.path().join("exact");
    let mut firsts = Vec::new();
    let last_lsn;
    {
        let mut writer = WalWriterV3::new(0, &exact, 512, WalBounds::UNBOUNDED).unwrap();
        for _ in 0..6 {
            for _ in 0..20 {
                writer.append(WalRecordType::Command, b"SET k v");
            }
            writer.flush_sync().unwrap();
        }
        last_lsn = writer.current_lsn() - 1;
        for seq in 1..=writer.current_segment_sequence() {
            let (base, first) = header_and_first_lsn(&exact, seq);
            assert_eq!(Some(base), first.or(Some(last_lsn + 1)), "segment {seq}");
            firsts.push(base);
        }
    }
    assert!(firsts.len() >= 5, "fixture must span several segments");
    // The PR head's header for segment N lay in [first_N, first_{N+1}].
    let overstated = tmp.path().join("overstated");
    copy_dir(&exact, &overstated);
    for (i, pair) in firsts.windows(2).enumerate().skip(1) {
        set_header_base(&overstated, i as u64 + 1, (pair[0] + pair[1]) / 2);
    }

    let remaining = |dir: &std::path::Path| -> Vec<u64> {
        let mut seqs: Vec<u64> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter_map(|e| e.file_name().to_str()?.strip_suffix(".wal")?.parse().ok())
            .collect();
        seqs.sort_unstable();
        seqs
    };
    let last = *firsts.last().unwrap();
    for redo in firsts
        .iter()
        .flat_map(|&f| [f - 1, f, f + 1])
        .chain([last + 5])
    {
        for aggressive in [false, true] {
            let mut kept = Vec::new();
            for (name, src) in [("exact", &exact), ("overstated", &overstated)] {
                let dir = tmp.path().join(format!("run-{name}-{redo}-{aggressive}"));
                copy_dir(src, &dir);
                let mut writer = WalWriterV3::new(0, &dir, 512, WalBounds::UNBOUNDED).unwrap();
                if aggressive {
                    writer.recycle_aggressive(redo).unwrap();
                } else {
                    writer.recycle_segments_before(redo).unwrap();
                }
                let left = remaining(&dir);
                // Never freed: a segment holding a record at or past redo.
                for (seq, &first) in firsts.iter().enumerate() {
                    let seq = seq as u64 + 1;
                    let next_first = firsts.get(seq as usize).copied().unwrap_or(last_lsn + 1);
                    if next_first > redo && first < next_first {
                        assert!(
                            left.contains(&seq),
                            "{name}: segment {seq} (LSNs {first}..{next_first}) freed at redo {redo}"
                        );
                    }
                }
                kept.push(left);
            }
            assert!(
                kept[0].iter().all(|seq| kept[1].contains(seq)),
                "an overstated header freed a segment exact headers keep (redo {redo}): {kept:?}"
            );
        }
    }
}
