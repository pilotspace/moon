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
    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn agent(self: &std::sync::Arc<Self>) -> crate::persistence::wal_v3::sync_agent::WalSyncAgent {
        let gate = std::sync::Arc::clone(self);
        crate::persistence::wal_v3::sync_agent::WalSyncAgent::spawn_with_backend(0, move |f| {
            gate.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut open = gate.open.lock();
            while !*open {
                gate.cv.wait(&mut open);
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
    // only in memory — exactly what a kill loses today.
    assert_eq!(replayed_lsns(&wal_dir), (1..=30).collect::<Vec<_>>());
    gate.open(); // release the leaked agent thread
}

/// A poisoned agent never publishes the watermark: the pending rotation
/// falls back to the inline fsync (the pre-moon#1188 path) instead of
/// buffering forever.
#[test]
fn test_1188_poisoned_agent_completes_rotation_inline() {
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
    writer.flush_if_needed().unwrap();
    assert!(!writer.rotation_pending());
    assert!(WalSegment::segment_path(&wal_dir, 2).exists());
    assert_eq!(writer.rotation_counts(), (1, 1));
    // Durability requests keep failing loudly after the poison.
    assert!(writer.request_sync().is_err());
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
