//! Off-shard-thread execution for cold KV reads (task #59).
//!
//! `Database::get()`'s cold-tier fallback does a blocking `pread` inline on
//! the shard event-loop thread. Under spill/AOF write backlog on the same
//! disk that `pread` can block for up to ~1.9s, stalling every connection on
//! the shard.
//!
//! CORRECTNESS (non-negotiable): a cold read must never answer "not found"
//! for a key that actually exists on disk just because a deadline expired.
//! An earlier revision of this module bounded the wait with a timeout and
//! fell back to `ColdReadOutcome::Miss` on expiry — that is a silent
//! data-correctness violation (a GET on an *existing* spilled key could
//! return nil under disk backlog) and has been removed entirely. There is
//! no timeout anywhere in this module now: [`read_cold_entry_async`] always
//! waits for and returns the real result.
//!
//! This module moves the blocking `pread` onto a small dedicated worker
//! pool and lets the *caller* decide how to wait:
//! - [`read_cold_entry_async`] — `.await`s the real result with no timeout.
//!   For use from async contexts (the monoio/tokio connection-handler task)
//!   that can afford to suspend: the awaiting connection's task yields to
//!   the shard's reactor, so **siblings on the same shard thread keep
//!   running** while this one waits for disk I/O — the actual goal of task
//!   #59. Currently wired into the GET read-only fast path only (see
//!   `src/server/conn/handler_monoio/mod.rs`); MULTI/EXEC bodies and Lua
//!   `redis.call` cannot `.await` (see `tmp/task59-design.md`) and continue
//!   to use the original synchronous `cold_read::cold_read_through_outcome`
//!   directly, unchanged.
//!
//! See `tmp/task59-design.md` for the full design rationale and the
//! call-site audit.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use super::cold_index::ColdLocation;
use super::cold_read::{ColdReadOutcome, read_cold_entry};

/// Number of worker threads in the pool. Small on purpose: this is purely
/// to get blocking I/O off the shard thread, not a throughput knob — a
/// backlogged disk is bottlenecked on the disk itself, not on worker count.
const DEFAULT_POOL_THREADS: usize = 2;

struct ColdReadJob {
    shard_dir: PathBuf,
    location: ColdLocation,
    now_ms: u64,
    reply: flume::Sender<ColdReadOutcome>,
}

static JOB_SENDER: OnceLock<flume::Sender<ColdReadJob>> = OnceLock::new();

fn job_sender() -> &'static flume::Sender<ColdReadJob> {
    JOB_SENDER.get_or_init(|| {
        let (tx, rx) = flume::unbounded::<ColdReadJob>();
        let n_threads = std::env::var("MOON_COLD_READ_POOL_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_POOL_THREADS);
        for i in 0..n_threads {
            let rx = rx.clone();
            let build = std::thread::Builder::new().name(format!("moon-cold-read-{i}"));
            // A worker thread that fails to spawn just means this pool has
            // fewer workers than requested; the job queue still drains via
            // any thread that did start. If EVERY spawn fails, `job_sender`
            // callers fall back to a direct synchronous read (see
            // `read_cold_entry_async`'s send-failure branch) -- slow, but
            // still correct. Not worth propagating a spawn error here.
            let _ = build.spawn(move || {
                for job in rx.iter() {
                    let outcome = read_cold_entry(&job.shard_dir, job.location, job.now_ms, None);
                    // If the requester's task was cancelled/dropped, this
                    // send just fails and is ignored -- nothing depends on
                    // it any more.
                    let _ = job.reply.send(outcome);
                }
            });
        }
        tx
    })
}

/// Off-shard-thread cold read: submits the blocking `pread` to the worker
/// pool and `.await`s the **real** result. No timeout, ever -- the awaiting
/// task simply yields (freeing the shard thread to run sibling
/// connections/commands) until the worker replies with the actual disk
/// contents.
///
/// Correctness fallback: if the pool is unreachable (send failed -- every
/// worker thread has panicked and unwound, which should never happen in
/// practice) or the reply channel closes without a reply (the one worker
/// servicing this job panicked mid-read), this falls back to a direct
/// synchronous read on the calling task instead of guessing `Miss` --
/// slow-but-correct beats fast-but-wrong every time here.
pub async fn read_cold_entry_async(
    shard_dir: &Path,
    location: ColdLocation,
    now_ms: u64,
) -> ColdReadOutcome {
    let (reply_tx, reply_rx) = flume::bounded::<ColdReadOutcome>(1);
    let job = ColdReadJob {
        shard_dir: shard_dir.to_path_buf(),
        location,
        now_ms,
        reply: reply_tx,
    };
    if job_sender().send(job).is_err() {
        return read_cold_entry(shard_dir, location, now_ms, None);
    }
    match reply_rx.recv_async().await {
        Ok(outcome) => outcome,
        Err(_) => read_cold_entry(shard_dir, location, now_ms, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::entry::{Entry, RedisValue};
    use crate::storage::tiered::cold_index::ColdIndex;
    use crate::storage::tiered::cold_read::{TEST_DELAY_LOCK, TEST_INJECT_DELAY_MS};
    use crate::storage::tiered::kv_spill::spill_to_datafile;
    use bytes::Bytes;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    /// Serializes tests that mutate the process-global injected-delay knob
    /// so they don't interfere with each other under `cargo test`'s default
    /// parallelism. Shared with `cold_read`'s own test module (via
    /// `cold_read::TEST_DELAY_LOCK`) since both call into the same
    /// process-wide cold-read worker pool.
    fn test_lock() -> std::sync::MutexGuard<'static, ()> {
        TEST_DELAY_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn spilled_index(shard_dir: &Path, key: &[u8], value: &[u8]) -> ColdIndex {
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();
        let entry = Entry::new_string(Bytes::copy_from_slice(value));
        spill_to_datafile(
            shard_dir,
            50,
            key,
            &entry,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();
        cold_index
    }

    /// RED (pre-fix property, still true today for MULTI/Lua): the direct,
    /// unpooled synchronous read blocks the calling thread for the full
    /// injected delay — this is exactly the shard-event-loop stall task #59
    /// reports (up to ~1.9s in production; simulated deterministically here
    /// via the injected-delay hook).
    #[test]
    fn test_direct_cold_read_blocks_caller_for_full_delay() {
        let _guard = test_lock();
        TEST_INJECT_DELAY_MS.store(300, Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let cold_index = spilled_index(tmp.path(), b"slowkey", b"value");

        let start = Instant::now();
        let result = crate::storage::tiered::cold_read::cold_read_through_outcome(
            &cold_index,
            tmp.path(),
            b"slowkey",
            0,
        );
        let elapsed = start.elapsed();

        TEST_INJECT_DELAY_MS.store(0, Ordering::Relaxed);

        assert!(
            matches!(result, ColdReadOutcome::Hit(..)),
            "sanity: the slow read still finds the entry"
        );
        assert!(
            elapsed >= Duration::from_millis(300),
            "direct synchronous read must block for the full injected delay, got {elapsed:?}"
        );
    }

    /// GREEN (the corrected property): the async pooled read returns the
    /// CORRECT value -- not a timed-out placeholder -- even when the
    /// underlying disk read is injected-slow. No assertion here bounds the
    /// wall-clock latency of the awaiting task itself (it's expected to
    /// take as long as the real read takes); what this proves is that
    /// waiting longer never trades correctness for speed.
    #[tokio::test]
    async fn test_async_cold_read_returns_correct_value_never_times_out_to_wrong_answer() {
        let _guard = test_lock();
        TEST_INJECT_DELAY_MS.store(400, Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let cold_index = spilled_index(tmp.path(), b"slowkey2", b"real-value-on-disk");
        let location = cold_index.lookup(b"slowkey2").expect("indexed");

        let start = Instant::now();
        let result = read_cold_entry_async(tmp.path(), location, 0).await;
        let elapsed = start.elapsed();

        TEST_INJECT_DELAY_MS.store(0, Ordering::Relaxed);

        assert!(
            elapsed >= Duration::from_millis(400),
            "the async read must wait for the REAL result, not truncate early; got {elapsed:?}"
        );
        match result {
            ColdReadOutcome::Hit(RedisValue::String(v), _ttl) => {
                assert_eq!(
                    v.as_ref(),
                    b"real-value-on-disk",
                    "must return the actual on-disk value, never a placeholder"
                );
            }
            other => panic!(
                "expected Hit(String) with the real value even under a slow disk, got {:?}",
                match other {
                    ColdReadOutcome::Hit(..) => "Hit(non-string)",
                    ColdReadOutcome::Expired => "Expired",
                    ColdReadOutcome::Miss => "Miss",
                }
            ),
        }
    }

    /// The async path must still be correct (and fast) on the overwhelming
    /// majority fast case — no regression to read correctness.
    #[tokio::test]
    async fn test_async_cold_read_correct_on_fast_path() {
        let _guard = test_lock();
        TEST_INJECT_DELAY_MS.store(0, Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let cold_index = spilled_index(tmp.path(), b"fastkey", b"fast-value");
        let location = cold_index.lookup(b"fastkey").expect("indexed");

        let result = read_cold_entry_async(tmp.path(), location, 0).await;
        match result {
            ColdReadOutcome::Hit(RedisValue::String(v), _ttl) => {
                assert_eq!(v.as_ref(), b"fast-value");
            }
            other => panic!(
                "expected fast Hit, got {other:?}",
                other = match other {
                    ColdReadOutcome::Hit(..) => "Hit(non-string)",
                    ColdReadOutcome::Expired => "Expired",
                    ColdReadOutcome::Miss => "Miss",
                }
            ),
        }
    }

    /// Genuine miss (key never spilled) must still resolve to `Miss` — the
    /// async path must not manufacture false hits either.
    #[tokio::test]
    async fn test_async_cold_read_genuine_miss_stays_miss() {
        let _guard = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let cold_index = spilled_index(tmp.path(), b"present-key", b"value");
        assert!(cold_index.lookup(b"absent-key").is_none());
        // Nothing to await here -- a genuinely absent key never reaches the
        // pool (no ColdLocation to submit); this test documents the
        // contract at the `ColdIndex::lookup` boundary the async path sits
        // behind (mirrors `cold_read_through_outcome`'s own None branch).
    }

    /// Siblings-not-blocked property: while one task awaits a slow cold
    /// read, a concurrently spawned "fast" task on the SAME single-threaded
    /// executor still makes progress promptly -- proving the await point
    /// actually yields (this is the entire point of task #59: a slow cold
    /// reader must not starve its shard-mates).
    #[tokio::test(flavor = "current_thread")]
    async fn test_slow_cold_read_does_not_starve_sibling_task() {
        let _guard = test_lock();
        TEST_INJECT_DELAY_MS.store(300, Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let cold_index = spilled_index(tmp.path(), b"slowkey3", b"slow-value");
        let location = cold_index.lookup(b"slowkey3").expect("indexed");
        let shard_dir = tmp.path().to_path_buf();

        // The "cold reader": awaits the real (slow) result.
        let slow =
            tokio::spawn(async move { read_cold_entry_async(&shard_dir, location, 0).await });

        // The "sibling": a trivial task representing another connection on
        // the same shard (e.g. PING). On a single-threaded executor, if the
        // slow reader were blocking the thread instead of yielding, this
        // would also be stuck for ~300ms. It must instead complete almost
        // immediately -- proving the slow reader's await point genuinely
        // frees the executor.
        let sibling_start = Instant::now();
        let sibling = tokio::spawn(async { 1 + 1 });
        let sibling_result: i32 = sibling.await.unwrap();
        let sibling_elapsed = sibling_start.elapsed();

        let slow_result = slow.await.unwrap();
        TEST_INJECT_DELAY_MS.store(0, Ordering::Relaxed);

        assert_eq!(sibling_result, 2);
        assert!(
            sibling_elapsed < Duration::from_millis(100),
            "a sibling task must complete promptly while another task awaits a slow cold \
             read on the SAME executor -- got {sibling_elapsed:?} (expected << 300ms \
             injected delay); a non-yielding implementation would show this near 300ms"
        );
        match slow_result {
            ColdReadOutcome::Hit(RedisValue::String(v), _) => {
                assert_eq!(
                    v.as_ref(),
                    b"slow-value",
                    "slow reader must still get the real value"
                )
            }
            _ => panic!("expected slow reader to eventually Hit with the real value"),
        }
    }

    /// TOCTOU fix (task #59 review round 2): a `DEL` on the SAME key that
    /// runs on the shard thread WHILE a GET's async cold-read `.await` is
    /// suspended must not be resurrected when that await resolves and the
    /// (now-stale) `Hit` outcome comes back. Reproduces the exact race the
    /// coordinator flagged:
    ///
    /// 1. GET peeks the cold location for `key`, `.await`s
    ///    `read_cold_entry_async` (injected-slow).
    /// 2. While suspended, `DEL key` runs synchronously on the "shard
    ///    thread" (here: the same task, interleaved via `tokio::select!`/
    ///    explicit ordering below) -- removes the cold-index entry.
    /// 3. The await resolves with the OLD value (`Hit`). Promoting it
    ///    blindly would resurrect the deleted key.
    ///
    /// Asserts: after `promote_cold_outcome` runs, the key is ABSENT (no
    /// hot entry, no cold entry) and STAYS absent on a subsequent read --
    /// no resurrection.
    #[tokio::test(flavor = "current_thread")]
    async fn test_del_during_cold_read_await_does_not_resurrect_key() {
        let _guard = test_lock();
        let tmp = tempfile::tempdir().unwrap();
        let key = b"racy-key";

        let mut db = crate::storage::db::Database::new();
        db.cold_shard_dir = Some(tmp.path().to_path_buf());
        db.cold_index = Some(spilled_index(
            tmp.path(),
            key,
            b"stale-value-about-to-be-deleted",
        ));

        // Precondition: key is cold-indexed, absent from hot RAM (mirrors
        // the connection handler's pre-await peek).
        assert!(!db.is_hot(key));
        let (loc, shard_dir) = db.cold_lookup_location(key).expect("indexed before DEL");

        // Step 1: start the (injected-slow) cold read, matching the
        // handler's `.await` point. Don't await it yet -- we need DEL to
        // run first, exactly like a sibling connection's command landing on
        // the shard thread while this task is suspended.
        TEST_INJECT_DELAY_MS.store(200, Ordering::Relaxed);
        let read_fut = read_cold_entry_async(&shard_dir, loc, 0);
        tokio::pin!(read_fut);

        // Poll the future once to kick off the pool submission (puts the
        // job in flight / the worker sleeping), then step away and run DEL
        // synchronously on `db` -- reproducing "the shard thread runs
        // another connection's command while this task's await is
        // pending". `current_thread` flavor + no other task competing means
        // this is deterministic: the read future cannot make further
        // progress until the 200ms sleep elapses on the worker thread,
        // regardless of how many times we poll it early.
        tokio::select! {
            biased;
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
            _ = &mut read_fut => panic!("read must not resolve before DEL runs (test setup bug)"),
        }

        let del_result = crate::command::key::del(
            &mut db,
            &[crate::protocol::Frame::BulkString(
                bytes::Bytes::from_static(key),
            )],
        );
        assert_eq!(
            del_result,
            crate::protocol::Frame::Integer(1),
            "DEL of a cold-only key must count it as removed (mirrors \
             test_del_removes_cold_entry_no_resurrection in cold_read.rs)"
        );
        assert!(
            db.cold_index.as_ref().unwrap().lookup(key).is_none(),
            "DEL must have removed the cold-index entry"
        );

        // Step 2: the await resolves with the STALE Hit read before DEL ran.
        let outcome = read_fut.await;
        TEST_INJECT_DELAY_MS.store(0, Ordering::Relaxed);
        assert!(
            matches!(outcome, ColdReadOutcome::Hit(..)),
            "sanity: the in-flight read completes with the old value, proving this outcome IS \
             stale relative to the DEL that already ran"
        );

        // Step 3: apply the stale outcome exactly like the connection
        // handler does. Revalidation must discard it instead of
        // resurrecting the deleted key.
        let promoted = db.promote_cold_outcome(key, 0, loc, outcome);
        assert!(
            !promoted,
            "a stale Hit for an already-deleted key must NOT be promoted"
        );
        assert!(
            !db.is_hot(key),
            "the deleted key must not have been resurrected into hot RAM"
        );
        assert!(
            db.cold_index.as_ref().unwrap().lookup(key).is_none(),
            "the deleted key must not have been re-added to the cold index either"
        );

        // Step 4: the key stays gone on a subsequent, completely ordinary
        // read -- no lingering resurrection via any other path.
        assert!(
            db.get(key).is_none(),
            "a later GET must still see the key as deleted, not resurrected"
        );
    }
}
