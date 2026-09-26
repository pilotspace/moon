//! moon#1264 (a): `SHUTDOWN ABORT` against the SHUTDOWN save loop, on a
//! virtual clock (the `sleep` advances it and plays the other client).

use std::cell::{Cell, RefCell};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::tests::BGSAVE_TEST_LOCK;
use super::*;

fn args(words: &[&'static str]) -> Vec<Frame> {
    words
        .iter()
        .map(|w| Frame::BulkString(Bytes::from_static(w.as_bytes())))
        .collect()
}

fn text(frame: &Frame) -> String {
    match frame {
        Frame::Error(e) => format!("-{}", String::from_utf8_lossy(e)),
        Frame::SimpleString(s) => format!("+{}", String::from_utf8_lossy(s)),
        other => format!("{other:?}"),
    }
}

/// Run `shutdown_save_within` to completion; `on_sleep(n)` runs at the n-th
/// poll (from 1), as another client would between two polls.
fn run_shutdown(on_sleep: impl Fn(usize)) -> Result<(), Frame> {
    let (tx, _rx) = crate::runtime::channel::watch(0u64);
    let budget = Duration::from_millis(SHUTDOWN_SAVE_DEADLINE_MS);
    let clock = Cell::new(Instant::now());
    let polls = Cell::new(0usize);
    let sleep = |d: Duration| {
        clock.set(clock.get() + d);
        polls.set(polls.get() + 1);
        on_sleep(polls.get());
        std::future::ready(())
    };
    let mut fut = std::pin::pin!(shutdown_save_within(&tx, 1, sleep, || clock.get(), budget));
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(r) => r,
        Poll::Pending => panic!("every sleep is ready at once"),
    }
}

/// Replies, word for word, of redis-server 7.0.15 with no shutdown in
/// progress (measured): the abort error keeps its period; `ABORT` with any
/// other modifier and `SAVE` with `NOSAVE` are syntax errors; a repeated
/// modifier is accepted.
#[test]
fn shutdown_arguments_parse_as_redis_7_0_15_does() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    let err = |words: &[&'static str]| match parse_shutdown_args(&args(words)) {
        Err(f) => text(&f),
        Ok(mode) => format!("{mode:?}"),
    };
    assert_eq!(err(&["ABORT"]), "-ERR No shutdown in progress.");
    assert_eq!(err(&["abort"]), "-ERR No shutdown in progress.");
    for combo in [
        &["ABORT", "NOSAVE"][..],
        &["NOSAVE", "ABORT"],
        &["SAVE", "ABORT"],
        &["FORCE", "ABORT"],
        &["ABORT", "NOW"],
        &["SAVE", "NOSAVE"],
        &["BOGUS"],
    ] {
        assert_eq!(err(combo), "-ERR syntax error", "{combo:?}");
    }
    assert_eq!(err(&["NOSAVE", "NOSAVE"]), "NoSave");
    assert_eq!(err(&["SAVE", "SAVE", "NOW"]), "Save");
    assert_eq!(err(&["NOW", "FORCE"]), "Default");
    assert_eq!(err(&[]), "Default");
}

/// A SHUTDOWN waiting for a save someone else started: ABORT from another
/// client answers `+OK`, the SHUTDOWN answers redis's error and starts no
/// save of its own, and a second ABORT finds no shutdown in progress.
#[test]
fn abort_cancels_a_shutdown_waiting_for_a_running_save() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    SAVE_IN_PROGRESS.store(true, Ordering::SeqCst);
    BGSAVE_SHARDS_REMAINING.store(0, Ordering::SeqCst);
    let abort_reply = RefCell::new(None);
    let reply = run_shutdown(|poll| {
        if poll == 3 {
            *abort_reply.borrow_mut() = Some(parse_shutdown_args(&args(&["ABORT"])));
        }
    });
    let started_own_save = BGSAVE_SHARDS_REMAINING.load(Ordering::SeqCst) != 0;
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);

    let abort_reply = abort_reply.into_inner().expect("the abort ran");
    assert_eq!(
        text(&abort_reply.expect_err("ABORT never shuts down")),
        "+OK"
    );
    assert_eq!(
        reply.map_err(|f| text(&f)),
        Err("-ERR Errors trying to SHUTDOWN. Check logs.".to_string())
    );
    assert!(
        !started_own_save,
        "the aborted SHUTDOWN started its own save"
    );
    assert_eq!(shutdown_abort::pending_for_test(), 0);
    assert_eq!(
        parse_shutdown_args(&args(&["ABORT"])).map_err(|f| text(&f)),
        Err("-ERR No shutdown in progress.".to_string())
    );
}

/// A SHUTDOWN waiting for its OWN save is cancelled the same way; the save
/// it started runs on as an ordinary BGSAVE.
#[test]
fn abort_cancels_a_shutdown_waiting_for_its_own_save() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    let abort_reply = RefCell::new(None);
    let reply = run_shutdown(|poll| {
        if poll == 2 {
            *abort_reply.borrow_mut() = Some(parse_shutdown_args(&args(&["ABORT"])));
        }
    });
    let own_save_running = SAVE_IN_PROGRESS.load(Ordering::SeqCst);
    if own_save_running {
        bgsave_shard_done(true);
    }
    assert!(own_save_running, "the SHUTDOWN had started its own save");
    let abort_reply = abort_reply.into_inner().expect("the abort ran");
    assert_eq!(text(&abort_reply.expect_err("ABORT")), "+OK");
    assert_eq!(
        reply.map_err(|f| text(&f)),
        Err("-ERR Errors trying to SHUTDOWN. Check logs.".to_string())
    );
    assert_eq!(shutdown_abort::pending_for_test(), 0);
}

/// An ABORT that found no shutdown does not cancel the next one.
#[test]
fn an_abort_with_no_shutdown_pending_cancels_nothing_later() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    assert!(parse_shutdown_args(&args(&["ABORT"])).is_err());
    let reply = run_shutdown(|poll| {
        if poll == 2 {
            bgsave_shard_done(true);
        }
    });
    assert_eq!(reply.map_err(|f| text(&f)), Ok(()));
}

/// Review F2: the SHUTDOWN's own save completes and, in that same poll,
/// another client's ABORT lands. Abort and commit are one decision: the
/// ABORT answered `+OK`, so the SHUTDOWN must not exit.
///
/// Red before the fix (REVIEW-WS21 `review_ws21_abort_race`): the ABORT
/// answered `+OK` and the SHUTDOWN returned `Ok(())` — the server exited.
#[test]
fn an_abort_that_answered_ok_stops_the_shutdown_whose_save_just_completed() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    let abort_reply = RefCell::new(None);
    let reply = run_shutdown(|poll| {
        if poll == 1 {
            // The shard finishes the SHUTDOWN's own save during this sleep...
            bgsave_shard_done(true);
            // ...and a second client's SHUTDOWN ABORT arrives before the poll.
            *abort_reply.borrow_mut() = Some(parse_shutdown_args(&args(&["ABORT"])));
        }
    });
    let abort_reply = abort_reply.into_inner().expect("the abort ran");
    assert_eq!(text(&abort_reply.expect_err("ABORT")), "+OK");
    assert_eq!(
        reply.map_err(|f| text(&f)),
        Err("-ERR Errors trying to SHUTDOWN. Check logs.".to_string()),
        "SHUTDOWN ABORT answered +OK, yet the SHUTDOWN it cancelled went ahead"
    );
    assert_eq!(shutdown_abort::pending_for_test(), 0);
}

/// The other order: the SHUTDOWN commits first; an ABORT after that finds
/// no shutdown in progress (the server is exiting), and never `+OK`.
#[test]
fn an_abort_after_the_commit_finds_no_shutdown_in_progress() {
    let _guard = BGSAVE_TEST_LOCK.lock();
    SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    let reply = run_shutdown(|poll| {
        if poll == 1 {
            bgsave_shard_done(true);
        }
    });
    assert_eq!(reply.map_err(|f| text(&f)), Ok(()));
    assert_eq!(shutdown_abort::pending_for_test(), 0);
    assert_eq!(
        parse_shutdown_args(&args(&["ABORT"])).map_err(|f| text(&f)),
        Err("-ERR No shutdown in progress.".to_string())
    );
}
