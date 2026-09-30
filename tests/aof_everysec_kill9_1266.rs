//! moon#1266: what `appendfsync everysec` loses of its ACKNOWLEDGED writes to
//! a process crash (`kill -9`).
//!
//! redis writes its AOF buffer with `write(2)` before the event loop sends the
//! replies of that iteration; only the `fsync` is deferred (to a background
//! thread, at most once per second). On a healthy disk a process crash
//! therefore loses nothing it acknowledged: the bytes are already in the
//! kernel page cache, which a SIGKILL does not touch. (On a slow disk redis
//! postpones that write for up to 2 s while replies still go out — see
//! `docs/production-guide.md`.)
//!
//! moon acknowledges a write once its record is queued to the shard's AOF
//! writer. Before moon#1266 Option 3 the write(2) could lag that ack by:
//! - the monoio writer's park-free poll step (`wait/16`, 3 ms while writing,
//!   up to 50 ms for the first write after an idle second);
//! - the tokio writer's user-space `BufWriter` tail (up to 8 KiB per shard);
//! - an inline everysec `fdatasync` on the writer thread, during which the
//!   channel did not drain at all.
//!
//! Option 3 (this fix) shortens that lag to one 100 µs poll step or one
//! thread wake-up — but the ack still does not wait for the `write(2)`, so a
//! writer thread that is descheduled, or whose `write(2)` stalls (a VM's I/O
//! jitter, dirty-page throttling, a journal commit behind the everysec
//! fsync), for longer than the kill delay still loses the last acked writes.
//! Closing that window needs the write before the reply (moon#1266 1A, WS46).
//!
//! Each case below acks N SETs, kills the server with SIGKILL ~1 ms after the
//! last ack (`MOON_1266_KILL_DELAY_US`, default 1000), restarts it on the same
//! `--dir`, and counts acked keys that did not come back. Every rep's count
//! is printed. The assertion is the Option-3 property: the MEDIAN rep loses
//! nothing and at most a quarter of the reps lose anything (before the fix
//! the median rep lost 1-1,100 keys in every cell). `MOON_1266_STRICT=1`
//! asserts 0 lost in every rep — the bar for 1A.
//!
//! What this does NOT cover: power loss / OS crash (the page cache is lost
//! there; everysec's bound is then ~1 s plus a slow fsync). Set
//! `MOON_1266_KILL_DELAY_US=0` to measure a kill right at the ack.
//!
//! ```text
//! MOON_BIN=/path/to/moon MOON_DISK_FREE_MIN_PCT=0 \
//!   cargo test --test aof_everysec_kill9_1266 -- --include-ignored --nocapture
//! ```
//! `MOON_1266_REPS` (default 20) sets the repetitions per case.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Acked SETs per rep (the WAVE2-PLAN WS40 durability leg: 10,000).
const N: usize = 10_000;
/// Commands per pipelined write in the pipelined shape.
const PIPELINE: usize = 100;

fn reps() -> usize {
    std::env::var("MOON_1266_REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
}

fn kill_delay() -> Duration {
    Duration::from_micros(
        std::env::var("MOON_1266_KILL_DELAY_US")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1000),
    )
}

fn start_moon(port: u16, dir: &std::path::Path, shards: usize) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "everysec",
            "--auto-aof-rewrite-percentage",
            "0",
            "--disk-free-min-pct",
            "0",
        ])
        .arg("--dir")
        .arg(dir)
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (build it first, or set MOON_BIN)")
}

/// A connection that answers PING, retried: a connection accepted during the
/// bootstrap -> per-shard listener handover can be reset.
fn ready_conn(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_nodelay(true);
            let _ = s.set_read_timeout(Some(Duration::from_secs(10)));
            let mut buf = [0u8; 64];
            if s.write_all(b"*1\r\n$4\r\nPING\r\n").is_ok()
                && let Ok(n) = s.read(&mut buf)
                && buf[..n].starts_with(b"+PONG")
            {
                return s;
            }
        }
        assert!(
            Instant::now() < deadline,
            "server on {port} never answered PING"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Read exactly `n` single-line replies; returns how many were `+OK`.
fn read_status_replies(s: &mut TcpStream, n: usize, spill: &mut Vec<u8>) -> usize {
    let mut chunk = [0u8; 65536];
    let (mut seen, mut ok) = (0usize, 0usize);
    loop {
        while seen < n {
            let Some(pos) = spill.windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            if spill[0] == b'+' {
                ok += 1;
            } else {
                panic!(
                    "unexpected reply: {:?}",
                    String::from_utf8_lossy(&spill[..pos])
                );
            }
            spill.drain(..pos + 2);
            seen += 1;
        }
        if seen == n {
            return ok;
        }
        let got = s.read(&mut chunk).expect("read replies");
        assert!(got > 0, "server closed after {seen}/{n} replies");
        spill.extend_from_slice(&chunk[..got]);
    }
}

fn set_cmd(key: &str) -> Vec<u8> {
    format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n$1\r\nv\r\n", key.len()).into_bytes()
}

/// Issue `n` SETs (`pipeline` per write), returning once every one is acked.
fn acked_sets(s: &mut TcpStream, prefix: &str, n: usize, pipeline: usize) {
    let mut spill = Vec::new();
    let mut i = 0usize;
    while i < n {
        let end = (i + pipeline).min(n);
        let mut wire = Vec::with_capacity((end - i) * 40);
        for j in i..end {
            wire.extend_from_slice(&set_cmd(&format!("{prefix}:{j}")));
        }
        s.write_all(&wire).expect("write SETs");
        let ok = read_status_replies(s, end - i, &mut spill);
        assert_eq!(ok, end - i, "every SET must be acked +OK");
        i = end;
    }
}

/// `INFO persistence` says loading:0 (the AOF has been replayed).
fn wait_loaded(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut s = ready_conn(port);
        s.write_all(b"*2\r\n$4\r\nINFO\r\n$11\r\npersistence\r\n")
            .expect("INFO");
        let mut buf = vec![0u8; 16384];
        let n = s.read(&mut buf).unwrap_or(0);
        if String::from_utf8_lossy(&buf[..n]).contains("loading:0") {
            return;
        }
        assert!(Instant::now() < deadline, "server never finished loading");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Number of `{prefix}:{0..n}` keys that do not EXIST.
fn count_missing(port: u16, prefix: &str, n: usize) -> usize {
    let mut s = ready_conn(port);
    let mut missing = 0usize;
    let mut chunk = [0u8; 65536];
    let mut i = 0usize;
    while i < n {
        let end = (i + 500).min(n);
        let mut wire = Vec::new();
        for j in i..end {
            let key = format!("{prefix}:{j}");
            wire.extend_from_slice(
                format!("*2\r\n$6\r\nEXISTS\r\n${}\r\n{key}\r\n", key.len()).as_bytes(),
            );
        }
        s.write_all(&wire).expect("write EXISTS");
        let mut raw = Vec::new();
        let mut seen = 0usize;
        while seen < end - i {
            let got = s.read(&mut chunk).expect("read EXISTS");
            assert!(got > 0, "server closed during EXISTS");
            raw.extend_from_slice(&chunk[..got]);
            seen = raw.windows(2).filter(|w| w == b"\r\n").count();
        }
        missing += raw
            .split(|&b| b == b'\n')
            .filter(|l| l.starts_with(b":0"))
            .count();
        i = end;
    }
    missing
}

#[derive(Clone, Copy)]
enum Shape {
    /// One SET per round trip.
    Unpipelined,
    /// [`PIPELINE`] SETs per write.
    Pipelined,
    /// ONE SET after the writer has idled >1 s (its escalated wait): the
    /// "first write after idle" case the post-idle poll step governs.
    LoneAfterIdle,
}

/// One rep: fresh server, (idle), N acked SETs, SIGKILL `kill_delay` after the
/// last ack, restart, count what is missing.
fn one_rep(shards: usize, shape: Shape, rep: usize) -> usize {
    let dir = tempfile::tempdir().expect("tempdir");
    let prefix = format!("r{rep}");
    let (mut server, port) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards));
    let mut s = ready_conn(port);
    // The writer's idle wait escalates 50 ms -> 250 ms -> 1 s; by 1.5 s of
    // idle it sits at the top step, the worst case for the first write.
    std::thread::sleep(Duration::from_millis(1500));
    let n = match shape {
        Shape::Unpipelined => {
            acked_sets(&mut s, &prefix, N, 1);
            N
        }
        Shape::Pipelined => {
            acked_sets(&mut s, &prefix, N, PIPELINE);
            N
        }
        Shape::LoneAfterIdle => {
            acked_sets(&mut s, &prefix, 1, 1);
            1
        }
    };
    let delay = kill_delay();
    if !delay.is_zero() {
        // Busy-wait: a sleep this short overshoots by the timer slack.
        let t = Instant::now();
        while t.elapsed() < delay {
            std::hint::spin_loop();
        }
    }
    server.kill_now();
    common::wait_for_port_down(port);

    let (_server2, port2) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards));
    wait_loaded(port2);
    count_missing(port2, &prefix, n)
}

fn run_case(shards: usize, shape: Shape, label: &str) {
    let reps = reps();
    let mut lost = Vec::with_capacity(reps);
    for rep in 0..reps {
        lost.push(one_rep(shards, shape, rep));
    }
    let total: usize = lost.iter().sum();
    let bin = common::find_moon_binary();
    eprintln!(
        "moon#1266 {label} shards={shards} kill_delay={:?} bin={} lost per rep: {lost:?} \
         (total {total}, max {})",
        kill_delay(),
        bin.display(),
        lost.iter().max().copied().unwrap_or(0)
    );
    if std::env::var("MOON_1266_STRICT").as_deref() == Ok("1") {
        assert_eq!(
            total, 0,
            "{label} shards={shards}: {total} acknowledged SETs were lost to kill -9 under \
             appendfsync everysec (per rep: {lost:?})"
        );
        return;
    }
    let mut sorted = lost.clone();
    sorted.sort_unstable();
    let median = sorted[sorted.len() / 2];
    let lossy = lost.iter().filter(|&&l| l > 0).count();
    assert!(
        median == 0 && lossy * 4 <= reps,
        "{label} shards={shards}: the median rep lost {median} acked SETs and {lossy} of {reps} \
         reps lost some, to a kill -9 1 ms after the last ack (per rep: {lost:?})"
    );
}

#[test]
#[ignore]
fn everysec_unpipelined_sets_survive_kill9_s1() {
    run_case(1, Shape::Unpipelined, "unpipelined");
}

#[test]
#[ignore]
fn everysec_unpipelined_sets_survive_kill9_s4() {
    run_case(4, Shape::Unpipelined, "unpipelined");
}

#[test]
#[ignore]
fn everysec_pipelined_sets_survive_kill9_s1() {
    run_case(1, Shape::Pipelined, "pipelined");
}

#[test]
#[ignore]
fn everysec_pipelined_sets_survive_kill9_s4() {
    run_case(4, Shape::Pipelined, "pipelined");
}

#[test]
#[ignore]
fn everysec_lone_set_after_idle_survives_kill9_s1() {
    run_case(1, Shape::LoneAfterIdle, "lone-after-idle");
}

#[test]
#[ignore]
fn everysec_lone_set_after_idle_survives_kill9_s4() {
    run_case(4, Shape::LoneAfterIdle, "lone-after-idle");
}

// ---------------------------------------------------------------------------
// The fsync itself, held open with `MOON_TEST_AOF_SYNC_GATE=<path>` (every AOF
// data fsync waits while <path> exists).
// ---------------------------------------------------------------------------

fn start_moon_gated(
    port: u16,
    dir: &std::path::Path,
    shards: usize,
    policy: &str,
    gate: &std::path::Path,
) -> Child {
    Command::new(common::find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            policy,
            "--auto-aof-rewrite-percentage",
            "0",
            "--disk-free-min-pct",
            "0",
            // A held fsync must not turn into a timeout reply here.
            "--aof-fsync-timeout-ms",
            "0",
        ])
        .arg("--dir")
        .arg(dir)
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .env("MOON_TEST_AOF_SYNC_GATE", gate)
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon (build it first, or set MOON_BIN)")
}

fn info_u64(port: u16, field: &str) -> Option<u64> {
    let mut s = ready_conn(port);
    s.write_all(b"*2\r\n$4\r\nINFO\r\n$11\r\npersistence\r\n")
        .expect("INFO");
    let mut buf = vec![0u8; 16384];
    let n = s.read(&mut buf).unwrap_or(0);
    String::from_utf8_lossy(&buf[..n])
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{field}:")))
        .and_then(|v| v.trim().parse().ok())
}

/// `appendfsync always` acknowledges a write only after its fsync: while the
/// fsync is held, no reply; once released, `+OK`.
fn always_acks_only_after_the_fsync(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let gate = dir.path().join("sync.gate");
    let (_server, port) = common::spawn_listening_guarded(|port| {
        start_moon_gated(port, dir.path(), shards, "always", &gate)
    });
    let mut s = ready_conn(port);
    let mut spill = Vec::new();
    acked_sets(&mut s, "before", 20, 1);

    std::fs::write(&gate, b"").expect("create gate");
    // Give any batch fsync already past the gate check time to finish.
    std::thread::sleep(Duration::from_millis(50));
    for i in 0..8 {
        s.write_all(&set_cmd(&format!("held:{i}")))
            .expect("write SET");
        s.set_read_timeout(Some(Duration::from_millis(400)))
            .expect("timeout");
        let mut buf = [0u8; 64];
        match s.read(&mut buf) {
            Ok(n) => panic!(
                "shards={shards}: `always` acknowledged {:?} while its fsync was held",
                String::from_utf8_lossy(&buf[..n])
            ),
            Err(e) => assert!(
                matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ),
                "read failed: {e}"
            ),
        }
        std::fs::remove_file(&gate).expect("release gate");
        s.set_read_timeout(Some(Duration::from_secs(10)))
            .expect("timeout");
        assert_eq!(
            read_status_replies(&mut s, 1, &mut spill),
            1,
            "the held SET is acked once its fsync returns"
        );
        std::fs::write(&gate, b"").expect("re-create gate");
    }
    std::fs::remove_file(&gate).expect("release gate");
}

#[test]
#[ignore]
fn always_acks_only_after_the_fsync_s1() {
    always_acks_only_after_the_fsync(1);
}

#[test]
#[ignore]
fn always_acks_only_after_the_fsync_s4() {
    always_acks_only_after_the_fsync(4);
}

/// moon#1266 Option 3: under everysec a held (slow) fsync runs on the
/// writer's agent thread — writes keep being acknowledged AND written to
/// the kernel meanwhile, the next deadline is postponed (INFO
/// `aof_delayed_fsync`), and a kill -9 during the held fsync loses nothing.
fn everysec_writer_keeps_writing_while_the_fsync_is_held(shards: usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let gate = dir.path().join("sync.gate");
    let (mut server, port) = common::spawn_listening_guarded(|port| {
        start_moon_gated(port, dir.path(), shards, "everysec", &gate)
    });
    let mut s = ready_conn(port);
    std::fs::write(&gate, b"").expect("create gate");
    // Write for 2.5 s: the first deadline's fsync is held, so the second
    // (and later) deadlines find it in flight.
    let started = Instant::now();
    let mut n = 0usize;
    let mut slowest = Duration::ZERO;
    while started.elapsed() < Duration::from_millis(2500) {
        let t = Instant::now();
        acked_sets(&mut s, &format!("g:{n}"), 50, 50);
        slowest = slowest.max(t.elapsed());
        n += 1;
    }
    let delayed = info_u64(port, "aof_delayed_fsync");
    assert!(
        delayed.is_some_and(|d| d >= 1),
        "shards={shards}: INFO aof_delayed_fsync = {delayed:?}: the held fsync never met a \
         later deadline (no agent, or the gate did not bite)"
    );
    assert!(
        slowest < Duration::from_millis(900),
        "shards={shards}: a 50-SET pipeline took {slowest:?} to be acked while the fsync was held"
    );
    server.kill_now();
    common::wait_for_port_down(port);
    std::fs::remove_file(&gate).expect("release gate");

    let (_server2, port2) =
        common::spawn_listening_guarded(|port| start_moon(port, dir.path(), shards));
    wait_loaded(port2);
    let mut missing = 0usize;
    for b in 0..n {
        missing += count_missing(port2, &format!("g:{b}"), 50);
    }
    eprintln!(
        "moon#1266 held-fsync shards={shards}: {} acked SETs, aof_delayed_fsync={delayed:?}, \
         slowest ack {slowest:?}, lost {missing}",
        n * 50
    );
    assert_eq!(
        missing, 0,
        "shards={shards}: {missing} acked SETs lost to kill -9 during a held everysec fsync"
    );
}

#[test]
#[ignore]
fn everysec_writer_keeps_writing_while_the_fsync_is_held_s1() {
    everysec_writer_keeps_writing_while_the_fsync_is_held(1);
}

#[test]
#[ignore]
fn everysec_writer_keeps_writing_while_the_fsync_is_held_s4() {
    everysec_writer_keeps_writing_while_the_fsync_is_held(4);
}
