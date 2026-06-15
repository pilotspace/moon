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
    let batch = collect_group_commit_batch(
        append(b"first"),
        || q.pop_front(),
        3,
        AOF_GROUP_COMMIT_MAX_BYTES,
    );
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
    let batch = collect_group_commit_batch(
        append(&[0u8; 100]),
        || q.pop_front(),
        AOF_GROUP_COMMIT_MAX_BATCH,
        250,
    );
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

    assert!(
        outcome.write_failed,
        "the batch must report a write failure"
    );
    assert_eq!(
        outcome.synced, 0,
        "no waiter may be acked Synced on a write failure"
    );
    // The ack oneshot is single-use (flume bounded-1): read each receiver ONCE
    // into a local, then assert on the captured value. (The original suite read
    // rx2.try_recv() twice, draining it before the second assertion — a
    // use-after-consume defect that no correct impl could satisfy.)
    let r1 = rx1.try_recv();
    let r2 = rx2.try_recv();
    assert_ne!(r1, Ok(AofAck::Synced));
    assert_ne!(r2, Ok(AofAck::Synced));
    assert_eq!(r2, Ok(AofAck::WriteFailed));
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

    assert!(
        outcome.fsync_failed,
        "the batch must report an fsync failure"
    );
    assert_eq!(
        outcome.synced, 0,
        "no waiter may be acked Synced when the fsync fails"
    );
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

    assert_eq!(
        sink.sync_calls, 0,
        "everysec (do_fsync=false) must not fsync per batch"
    );
    assert_eq!(sink.writes.len(), 2, "bytes are still written in order");
    assert_eq!(
        outcome.synced, 0,
        "no AppendSync waiters in an everysec batch"
    );
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

    assert_eq!(
        sink.sync_calls, 1,
        "one fsync covers the appends + the barrier"
    );
    assert_eq!(
        sink.synced_at_write_count,
        Some(3),
        "the fsync runs after the two appends AND the barrier are written"
    );
    assert_eq!(outcome.synced, 1, "the barrier AppendSync is acked");
    assert_eq!(rx.try_recv(), Ok(AofAck::Synced));
}

// ===========================================================================
// INTEGRATION — real server, SIGKILL, replay (the §4 durability scenarios).
//
// A unit mock cannot prove on-disk survival across a real kill, so these spawn
// the release binary under `appendfsync=always` and drive CONCURRENT durable
// writers (the cell group commit lives in), then assert every acked write
// survives a SIGKILL and nothing is double-applied on replay.
//
// `#[ignore]` — require the release binary at ./target/release/moon + redis-cli
// on PATH. Run (monoio default, matches CI release build):
//   cargo build --release
//   cargo test --release --test wal_group_commit -- --ignored
// Tokio runtime:
//   cargo build --release --no-default-features \
//     --features runtime-tokio,jemalloc,graph,text-index
//   cargo test --release --no-default-features \
//     --features runtime-tokio,jemalloc,graph,text-index \
//     --test wal_group_commit -- --ignored
// ===========================================================================
#[cfg(unix)]
mod integration {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpStream;
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    fn unique_port() -> u16 {
        use std::net::TcpListener;
        let l = TcpListener::bind("127.0.0.1:0").expect("bind port 0");
        let p = l.local_addr().expect("local addr").port();
        drop(l);
        p
    }

    fn unique_dir(suffix: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "moon-wal-gc-{}-{}-{}",
            std::process::id(),
            suffix,
            nanos
        ))
    }

    fn start_moon(port: u16, dir: &std::path::Path, shards: u16, fsync: &str) -> Child {
        Command::new("./target/release/moon")
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "yes",
                "--appendfsync",
                fsync,
                "--dir",
            ])
            .arg(dir)
            .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("create stdout log"))
            .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("create stderr log"))
            .spawn()
            .expect("spawn moon — run `cargo build --release` first")
    }

    fn wait_for_port(port: u16) {
        for _ in 0..80 {
            if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
                std::thread::sleep(Duration::from_millis(200));
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("moon did not start within 8s on port {}", port);
    }

    fn sigkill(child: &mut Child) {
        let pid = child.id() as i32;
        // SAFETY: `pid` is this test's own freshly-spawned child; SIGKILL has no
        // userspace side effects beyond terminating it. We then reap it.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = child.wait();
    }

    /// Read one RESP reply line and return it trimmed (e.g. ":42", "+OK", "-ERR ...").
    fn read_reply_line(reader: &mut BufReader<TcpStream>) -> Option<String> {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => None,
            Ok(_) => Some(line.trim_end_matches("\r\n").to_string()),
            Err(_) => None,
        }
    }

    /// Open a connection and issue `n` sequential `INCR key` commands, each one
    /// sent only AFTER the previous reply is read (so under `appendfsync=always`
    /// every counted reply observed an fsync). Returns the count of integer
    /// replies received — the number of ACKED increments for this key.
    fn incr_acked(port: u16, key: &str, n: usize) -> usize {
        let stream = match TcpStream::connect(format!("127.0.0.1:{}", port)) {
            Ok(s) => s,
            Err(_) => return 0,
        };
        stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
        let mut writer = stream.try_clone().expect("clone stream");
        let mut reader = BufReader::new(stream);
        let cmd = format!("*2\r\n$4\r\nINCR\r\n${}\r\n{}\r\n", key.len(), key);
        let mut acked = 0usize;
        for _ in 0..n {
            if writer.write_all(cmd.as_bytes()).is_err() {
                break;
            }
            if writer.flush().is_err() {
                break;
            }
            match read_reply_line(&mut reader) {
                Some(l) if l.starts_with(':') => acked += 1,
                _ => break, // error / disconnect — stop counting at the first non-ack
            }
        }
        acked
    }

    /// GET an integer counter; returns its value or -1 on miss/parse failure.
    fn get_int(port: u16, key: &str) -> i64 {
        let out = Command::new("redis-cli")
            .args(["-p", &port.to_string(), "GET", key])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("redis-cli GET");
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(-1)
    }

    /// M2 every_acked_write_survives_crash — N concurrent connections each drive
    /// M durable INCRs on a distinct counter under `appendfsync=always` (so the
    /// writer coalesces concurrent AppendSyncs into group-committed fsyncs). All
    /// threads are joined (every counted INCR is acked ⇒ fsynced) BEFORE the
    /// SIGKILL, so recovery MUST show each counter == its acked count — every
    /// acked write durable, none lost, none double-applied (INCR is non-idempotent).
    fn concurrent_writers_survive_sigkill(shards: u16, tag: &str) {
        const WRITERS: usize = 16;
        const PER_WRITER: usize = 60;

        let port = unique_port();
        let dir = unique_dir(tag);
        std::fs::create_dir_all(&dir).expect("create test dir");

        let mut child = start_moon(port, &dir, shards, "always");
        wait_for_port(port);

        // Fan out WRITERS concurrent durable-write loops; collect per-key acked counts.
        let handles: Vec<_> = (0..WRITERS)
            .map(|i| {
                let key = format!("gc:{{{}}}:{}", i, i);
                std::thread::spawn(move || (key.clone(), incr_acked(port, &key, PER_WRITER)))
            })
            .collect();
        let acked: Vec<(String, usize)> = handles
            .into_iter()
            .map(|h| h.join().expect("writer thread"))
            .collect();

        // Every counted INCR was acked ⇒ fsynced under Always. Kill WITHOUT a
        // quiescing sleep: the durability contract is that each ack already saw disk.
        sigkill(&mut child);

        // -- recover --
        let mut child2 = start_moon(port, &dir, shards, "always");
        wait_for_port(port);

        let mut wrong: Vec<String> = Vec::new();
        for (key, want) in &acked {
            let got = get_int(port, key);
            if got != *want as i64 {
                wrong.push(format!("{}: want={} got={}", key, want, got));
            }
        }
        sigkill(&mut child2);

        assert!(
            wrong.is_empty(),
            "group-commit durability ({} shards): {} counters wrong after SIGKILL+recovery. \
             A loss under-counts, a double-apply over-counts. Sample: {:?}",
            shards,
            wrong.len(),
            wrong.iter().take(8).collect::<Vec<_>>(),
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// M2 — TopLevel path (shards=1): all durable writes converge on ONE writer
    /// (the `commit_group_commit_batch` path), maximizing batch coalescing.
    #[test]
    #[ignore]
    fn concurrent_writers_all_acked_survive_sigkill_top_level() {
        concurrent_writers_survive_sigkill(1, "concurrent-s1");
    }

    /// M2 — PerShard path (shards=4): writes spread across per-shard framed
    /// writers; recovery merges. Exercises the production default layout.
    #[test]
    #[ignore]
    fn concurrent_writers_all_acked_survive_sigkill_per_shard() {
        concurrent_writers_survive_sigkill(4, "concurrent-s4");
    }

    /// M4 lone_writer_no_added_latency — with exactly ONE writer (C=1) every
    /// durable write is a batch of 1: it fsyncs immediately and is acked Synced.
    /// A SIGKILL with no quiescing sleep must leave every acked INCR on disk.
    #[test]
    #[ignore]
    fn lone_writer_fsyncs_immediately() {
        let port = unique_port();
        let dir = unique_dir("lone");
        std::fs::create_dir_all(&dir).expect("create test dir");

        let mut child = start_moon(port, &dir, 1, "always");
        wait_for_port(port);

        let acked = incr_acked(port, "lone:counter", 200);
        assert!(acked > 0, "lone writer made no progress");
        sigkill(&mut child);

        let mut child2 = start_moon(port, &dir, 1, "always");
        wait_for_port(port);
        let got = get_int(port, "lone:counter");
        sigkill(&mut child2);

        assert_eq!(
            got, acked as i64,
            "lone (C=1) writer: counter after SIGKILL+recovery = {} (expected {} acked)",
            got, acked,
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// M3 control_message_breaks_the_batch — BGREWRITEAOF while many durable
    /// writes are in flight. The Rewrite control must flush the in-progress batch
    /// (write + fsync + ack) BEFORE it is handled, and post-rewrite replay must
    /// lose no acked write. Writers are joined (all acked ⇒ durable) before the
    /// kill, so each counter MUST recover exactly.
    #[test]
    #[ignore]
    fn rewrite_during_active_writes_no_loss() {
        const WRITERS: usize = 12;
        const PER_WRITER: usize = 80;

        let port = unique_port();
        let dir = unique_dir("rewrite");
        std::fs::create_dir_all(&dir).expect("create test dir");

        let mut child = start_moon(port, &dir, 4, "always");
        wait_for_port(port);

        // Fire BGREWRITEAOF repeatedly from a side thread while writers run, so a
        // Rewrite control message lands mid-drain on at least one writer.
        let rewrite_port = port;
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop2 = stop.clone();
        let rewriter = std::thread::spawn(move || {
            while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                let _ = Command::new("redis-cli")
                    .args(["-p", &rewrite_port.to_string(), "BGREWRITEAOF"])
                    .output();
                std::thread::sleep(Duration::from_millis(40));
            }
        });

        let handles: Vec<_> = (0..WRITERS)
            .map(|i| {
                let key = format!("rw:{{{}}}:{}", i, i);
                std::thread::spawn(move || (key.clone(), incr_acked(port, &key, PER_WRITER)))
            })
            .collect();
        let acked: Vec<(String, usize)> = handles
            .into_iter()
            .map(|h| h.join().expect("writer thread"))
            .collect();

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = rewriter.join();

        sigkill(&mut child);

        let mut child2 = start_moon(port, &dir, 4, "always");
        wait_for_port(port);
        let mut wrong: Vec<String> = Vec::new();
        for (key, want) in &acked {
            let got = get_int(port, key);
            if got != *want as i64 {
                wrong.push(format!("{}: want={} got={}", key, want, got));
            }
        }
        sigkill(&mut child2);

        assert!(
            wrong.is_empty(),
            "BGREWRITEAOF during active durable writes: {} counters wrong after recovery. \
             The control message must flush its batch before the rewrite (no loss, no double-apply). \
             Sample: {:?}",
            wrong.len(),
            wrong.iter().take(8).collect::<Vec<_>>(),
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
