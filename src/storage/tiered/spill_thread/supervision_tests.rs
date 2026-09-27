//! moon#1265: the spill thread under supervision — a panic is captured at
//! the thread boundary, the channels survive it, the thread is respawned on
//! them, and a crash loop spends a bounded budget and then closes them.

use std::time::{Duration, Instant};

use super::fault::{PanicPlan, PanicPoint};
use super::supervisor::Verdict;
use super::*;
use crate::persistence::kv_page::ValueType;

fn request(key: &str, file_id: u64, dir: &std::path::Path) -> SpillRequest {
    SpillRequest {
        key: Bytes::copy_from_slice(key.as_bytes()),
        db_index: 0,
        value_bytes: Bytes::from_static(b"v"),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
        file_id,
        shard_dir: dir.to_path_buf(),
    }
}

fn heap(dir: &std::path::Path, file_id: u64) -> std::path::PathBuf {
    dir.join("data").join(format!("heap-{file_id:06}.mpf"))
}

fn wait_dead(st: &SpillThread) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !st.is_dead() {
        assert!(Instant::now() < deadline, "the thread never died");
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn wait_completion(st: &SpillThread, req_id: u64) -> Vec<SpillCompletion> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut got = Vec::new();
    loop {
        got.extend(st.drain_completions());
        if got
            .iter()
            .any(|c| c.entries.iter().any(|e| e.req_file_id == req_id))
        {
            return got;
        }
        assert!(Instant::now() < deadline, "no completion for {req_id}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A flush that wrote its file and panicked before announcing it: the
/// request is lost with the thread (its file unlisted on disk), a request
/// sent afterwards through a sender cloned BEFORE the death — a connection's
/// — still queues, and the respawned thread serves it on the same channel.
#[test]
fn a_panicked_thread_is_respawned_on_the_same_channels() {
    let tmp = tempfile::tempdir().unwrap();
    let st = SpillThread::with_fault(3, Some(PanicPlan::times(PanicPoint::AfterWrite, 1)));
    let conn_sender = st.sender();
    let restarts_before = spill_thread_restarts_total();

    conn_sender.send(request("lost", 1, tmp.path())).unwrap();
    wait_dead(&st);
    assert!(heap(tmp.path(), 1).exists(), "written, never announced");
    assert!(st.drain_completions().is_empty(), "no completion for it");
    assert_eq!(st.done_below(), 0, "the watermark never covered it");

    let now = 10_000;
    let death = st.take_death(true, now).expect("a fresh death");
    assert_eq!(death.verdict, Verdict::RespawnAt(now + 100));
    assert!(
        !conn_sender.is_disconnected(),
        "the channel outlives the thread"
    );
    conn_sender.send(request("queued", 2, tmp.path())).unwrap();
    assert!(
        st.try_submit_reclaim(super::super::reclaim_io::ReclaimJob::Read {
            db_index: 0,
            file_id: 1,
            shard_dir: tmp.path().to_path_buf(),
        })
        .is_err(),
        "no reclaim job is queued for a thread that is not running"
    );

    assert_eq!(st.respawn_if_due(now + 99), Respawn::NotDue);
    assert_eq!(st.respawn_if_due(now + 100), Respawn::Respawned);
    assert!(!st.is_dead());
    assert!(spill_thread_restarts_total() > restarts_before);
    let got = wait_completion(&st, 2);
    assert!(
        got.iter()
            .all(|c| c.entries.iter().all(|e| e.req_file_id == 2)),
        "only the queued request completes; the lost one never does"
    );
    assert!(heap(tmp.path(), 2).exists());
    let _ = st.shutdown();
}

/// A completion sent just before the panic is in the channel for the drain
/// that follows the death; only the watermark did not move.
#[test]
fn a_completion_sent_before_the_panic_reaches_the_drain() {
    let tmp = tempfile::tempdir().unwrap();
    let st = SpillThread::with_fault(4, Some(PanicPlan::times(PanicPoint::AfterSend, 1)));
    st.sender().send(request("sent", 5, tmp.path())).unwrap();
    wait_dead(&st);
    let was_dead = st.is_dead();
    let got = st.drain_completions();
    assert!(
        got.iter()
            .any(|c| c.success && c.entries.iter().any(|e| e.req_file_id == 5))
    );
    assert_eq!(st.done_below(), 0, "died before publishing the watermark");
    assert!(st.take_death(was_dead, 0).is_some());
    let _ = st.shutdown();
}

/// A crash loop: five respawns inside the window, and the sixth death
/// degrades the shard. Degrading closes the channels, so a connection's
/// sender sees a disconnect (eviction then takes the no-spill path) and
/// nothing can be queued — and pinned in RAM — for a thread that never runs.
#[test]
fn a_crash_loop_spends_the_budget_then_closes_the_channels() {
    let st = SpillThread::with_fault(5, Some(PanicPlan::times(PanicPoint::Start, 1_000)));
    let conn_sender = st.sender();
    let mut now = 50_000u64;
    let mut backoffs = Vec::new();
    loop {
        wait_dead(&st);
        match st
            .take_death(true, now)
            .expect("each death is seen once")
            .verdict
        {
            Verdict::RespawnAt(due) => {
                backoffs.push(due - now);
                now = due;
                assert_eq!(st.respawn_if_due(now), Respawn::Respawned);
                now += 1;
            }
            Verdict::Degrade => break,
        }
    }
    assert_eq!(backoffs, vec![100, 200, 400, 800, 1_600]);
    assert!(st.is_degraded() && st.is_dead());
    assert!(spill_threads_degraded() >= 1);
    assert_eq!(st.respawn_if_due(u64::MAX), Respawn::NotDue, "terminal");
    assert!(st.take_queued_requests().is_empty());
    st.stop_spilling();
    assert!(conn_sender.is_disconnected());
    let _ = st.shutdown();
}

/// Any panic payload — `&str` or a formatted `String` — is kept for the
/// shard's log line; the injected one is surfaced by `take_death`.
#[test]
fn the_panic_message_is_captured_at_the_thread_boundary() {
    let slot = supervision::ExitSlot::default();
    let bg = slot.clone();
    std::thread::spawn(move || {
        let Err(p) = std::panic::catch_unwind(|| panic!("boom {}", 7));
        bg.record_panic(p.as_ref());
    })
    .join()
    .unwrap();
    assert_eq!(slot.take().as_deref(), Some("boom 7"));

    let st = SpillThread::exited_for_test();
    let d = st.take_death(true, 0).unwrap();
    assert!(d.panic.unwrap().contains("Start"));
    let _ = st.shutdown();
}
