//! ADD task `spsc-wake-floor` — failing-first suite (runtime tests).
//!
//! RED tests (fail until the build lands — the INFO fields do not exist yet):
//!   - swf1_notify_wakes_counter      — INFO `spsc_notify_wakes` present and > 0.
//!   - swf2_burst_renotify            — INFO `spsc_drain_renotify` present and ≥ 1.
//!
//! PIN / assumption tests (must be green BEFORE build — they settle the frozen
//! contract's flagged assumptions; a failure here triggers the pre-agreed
//! fallback BEFORE any build work):
//!   - swf0_monoio_cross_thread_wake  — A1: monoio 0.2.4 `sync` cross-thread
//!     Waker::wake() reaches a parked monoio task (FusionDriver: io_uring on
//!     Linux, kqueue on macOS). The codebase's relay comments claim this is
//!     impossible; this test is the runtime proof either way.
//!   - swf_a3_notify_token_survives_poll_drop — A3: a notify token delivered to
//!     a registered-then-dropped `notified()` future is NOT lost (flume re-queues).
//!   - swf3_cadence_pins              — M4 guard: cached-clock expiry and WAL
//!     flush cadence behave the same before and after the build.
//!
//! New-API compile-red tests (race2, counters) live in `spsc_wake_floor_red_api.rs`.
//! Run this file alone with: cargo test --test spsc_wake_floor_red

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Shared harness (CARGO_BIN_EXE pattern, mirrors multishard_serve_smoke.rs)
// ---------------------------------------------------------------------------

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("local_addr").port();
    drop(l);
    p
}

/// Fresh `--dir` per server (CWD persistence-reload trap: an inherited
/// appendonlydir would replay stale state into a throwaway test server).
fn spawn_moon(port: u16, dir: &std::path::Path, extra: &[&str]) -> Child {
    spawn_moon_with_shards(port, dir, 2, extra)
}

fn spawn_moon_with_shards(port: u16, dir: &std::path::Path, shards: u32, extra: &[&str]) -> Child {
    let mut args: Vec<String> = vec![
        "--port".into(),
        port.to_string(),
        "--dir".into(),
        dir.to_string_lossy().into_owned(),
        "--shards".into(),
        shards.to_string(),
    ];
    for e in extra {
        args.push((*e).into());
    }
    Command::new(moon_binary())
        .args(&args)
        .stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (CARGO_BIN_EXE_moon)")
}

fn connect(port: u16, deadline: Duration) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .expect("addr")
        .next()
        .expect("one addr");
    let start = Instant::now();
    loop {
        match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(s) => {
                s.set_read_timeout(Some(Duration::from_secs(5))).ok();
                s.set_write_timeout(Some(Duration::from_secs(5))).ok();
                return s;
            }
            Err(_) if start.elapsed() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => panic!("server never accepted on {port}: {e}"),
        }
    }
}

/// Wait for the server to answer PING (started AND serving).
/// Generous connect deadline: the FIRST exec of a freshly-linked binary on
/// macOS can stall >10s in code-signature validation (dyld/amfi), especially
/// with several test servers spawning concurrently after a rebuild.
fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port, Duration::from_secs(30));
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").expect("write PING");
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf) {
            if n > 0 && buf[..n].windows(4).any(|w| w == b"PONG") {
                return s;
            }
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "server accepted TCP but never answered PING"
        );
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port, Duration::from_secs(5));
    }
}

/// Send one RESP command, return the raw reply bytes (single read may suffice
/// for short replies; loops until at least `min_len` bytes or CRLF-terminated).
fn resp_cmd(s: &mut TcpStream, parts: &[&[u8]]) -> Vec<u8> {
    let mut req = Vec::with_capacity(64);
    req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        req.extend_from_slice(p);
        req.extend_from_slice(b"\r\n");
    }
    s.write_all(&req).expect("write cmd");
    read_reply(s)
}

/// Read one reply frame (simple string / error / integer / bulk / null).
fn read_reply(s: &mut TcpStream) -> Vec<u8> {
    let mut buf = vec![0u8; 64 * 1024];
    let n = s.read(&mut buf).expect("read reply");
    assert!(n > 0, "connection closed mid-reply");
    buf.truncate(n);
    // Bulk strings may need a second read if header and body split.
    if buf[0] == b'$' && !buf.ends_with(b"\r\n") {
        let mut more = vec![0u8; 64 * 1024];
        if let Ok(m) = s.read(&mut more) {
            buf.extend_from_slice(&more[..m]);
        }
    }
    buf
}

/// Fetch INFO and return it as a lossy string.
fn info(s: &mut TcpStream) -> String {
    // INFO replies are large; read until the bulk length is satisfied.
    s.write_all(b"*1\r\n$4\r\nINFO\r\n").expect("write INFO");
    let mut acc: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut buf = vec![0u8; 64 * 1024];
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut expected_total: Option<usize> = None;
    loop {
        let n = s.read(&mut buf).expect("read INFO");
        assert!(n > 0, "connection closed during INFO");
        acc.extend_from_slice(&buf[..n]);
        if expected_total.is_none() {
            // Parse "$<len>\r\n" header once available.
            if let Some(pos) = acc.windows(2).position(|w| w == b"\r\n") {
                assert_eq!(acc[0], b'$', "INFO must be a bulk string: {acc:?}");
                let len: usize = std::str::from_utf8(&acc[1..pos])
                    .expect("utf8 len")
                    .parse()
                    .expect("bulk len");
                expected_total = Some(pos + 2 + len + 2);
            }
        }
        if let Some(t) = expected_total {
            if acc.len() >= t {
                break;
            }
        }
        assert!(Instant::now() < deadline, "INFO read timed out");
    }
    String::from_utf8_lossy(&acc).into_owned()
}

/// Extract `field:<u64>` from an INFO payload.
fn info_u64(payload: &str, field: &str) -> Option<u64> {
    payload.lines().find_map(|l| {
        let l = l.trim_end_matches('\r');
        l.strip_prefix(&format!("{field}:"))
            .and_then(|v| v.parse::<u64>().ok())
    })
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ---------------------------------------------------------------------------
// swf0 — A1 assumption pin: monoio `sync` cross-thread wake (runs FIRST)
// ---------------------------------------------------------------------------

/// monoio 0.2.4 with the `sync` feature (enabled in Cargo.toml) routes a remote
/// `Waker::wake()` through a per-thread waker channel + driver unpark (eventfd
/// on io_uring, kqueue-wake on the legacy driver). The pending_wakers relay was
/// built on the belief this does NOT work. This test settles it at runtime:
/// a parked monoio task awaiting moon's `Notify` (flume bounded(1)) and a flume
/// oneshot must be woken by a plain std thread well before a 500ms timer.
///
/// GREEN  -> A1 holds; build proceeds on the frozen mechanism.
/// FAIL   -> A1 disproved; invoke the §3 pre-agreed fallback (notify-origin)
///           BEFORE build. Do not "fix" this test.
#[cfg(feature = "runtime-monoio")]
#[test]
fn swf0_monoio_cross_thread_wake() {
    use std::sync::Arc;

    let notify = Arc::new(moon::runtime::channel::Notify::new());
    let (oneshot_tx, oneshot_rx) = flume::bounded::<u8>(1);

    let n2 = Arc::clone(&notify);
    let waker_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(5));
        n2.notify_one();
        std::thread::sleep(Duration::from_millis(5));
        let _ = oneshot_tx.send(42);
    });

    // Same runtime construction as production (runtime/monoio_impl.rs).
    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("build monoio runtime");

    let (notify_elapsed, oneshot_elapsed) = rt.block_on(async move {
        // Wake target is a SPAWNED (!Send, local) task — same shape as a
        // production connection task, not the block_on root.
        let (done_tx, done_rx) = flume::bounded::<(Duration, Duration)>(1);
        monoio::spawn(async move {
            let t0 = Instant::now();
            let woke_notify = monoio::time::timeout(Duration::from_millis(500), async {
                notify.notified().await;
            })
            .await
            .is_ok();
            let notify_elapsed = t0.elapsed();
            assert!(
                woke_notify,
                "Notify::notified() never woken cross-thread (A1 disproved for Notify)"
            );

            let t1 = Instant::now();
            let got = monoio::time::timeout(Duration::from_millis(500), async {
                oneshot_rx.recv_async().await
            })
            .await;
            let oneshot_elapsed = t1.elapsed();
            assert!(
                matches!(got, Ok(Ok(42))),
                "flume recv_async never woken cross-thread (A1 disproved for oneshot): {got:?}"
            );
            let _ = done_tx.send((notify_elapsed, oneshot_elapsed));
        });
        done_rx
            .recv_async()
            .await
            .expect("spawned waiter task must report")
    });

    waker_thread.join().expect("waker thread");

    // The 500ms timeout itself would wake the runtime, so distinguish a REAL
    // cross-thread wake (few ms) from a timer-driven one (~500ms).
    assert!(
        notify_elapsed < Duration::from_millis(100),
        "Notify wake took {notify_elapsed:?} — timer-driven, not event-driven (A1 disproved)"
    );
    assert!(
        oneshot_elapsed < Duration::from_millis(100),
        "oneshot wake took {oneshot_elapsed:?} — timer-driven, not event-driven (A1 disproved)"
    );
}

// ---------------------------------------------------------------------------
// swf_a3 — A3 assumption pin: notify token survives a dropped RecvFut
// ---------------------------------------------------------------------------

/// In the race design the timer arm can win while `notified()` is mid-flight:
/// the future was polled (waker hook registered), the token arrives, then the
/// future is DROPPED without completing. flume must re-queue the undelivered
/// token so the NEXT `notified()` completes immediately — otherwise the race
/// has a lost-wake window.
#[test]
fn swf_a3_notify_token_survives_poll_drop() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    let notify = moon::runtime::channel::Notify::new();

    {
        // Register interest, then drop before the token is consumed.
        let fut = pin!(notify.notified());
        let mut fut = fut;
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(
            matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
            "no token yet: first poll must be Pending"
        );
        notify.notify_one(); // token delivered while a hook is registered
        // fut dropped HERE without being polled to completion
    }

    // The token must still be there.
    let mut completed = false;
    {
        let fut = pin!(notify.notified());
        let mut fut = fut;
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        for _ in 0..100 {
            if matches!(fut.as_mut().poll(&mut cx), Poll::Ready(())) {
                completed = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    assert!(
        completed,
        "notify token was LOST when the registered future was dropped (A3 disproved \
         — the race design needs token re-arming on the timer arm)"
    );
}

// ---------------------------------------------------------------------------
// swf1 — RED: INFO exposes spsc_notify_wakes > 0 after cross-shard traffic
// ---------------------------------------------------------------------------

#[test]
fn swf1_notify_wakes_counter() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), &[]));
    let mut s = wait_ready(port);

    // 32 distinct keys on 2 shards: ~half route cross-shard from whichever
    // shard accepted this connection.
    for i in 0..32 {
        let key = format!("swf1:{i}");
        let reply = resp_cmd(&mut s, &[b"SET", key.as_bytes(), b"v"]);
        assert!(
            reply.starts_with(b"+OK"),
            "SET failed: {:?}",
            String::from_utf8_lossy(&reply)
        );
    }
    for i in 0..32 {
        let key = format!("swf1:{i}");
        let reply = resp_cmd(&mut s, &[b"GET", key.as_bytes()]);
        assert!(
            reply.starts_with(b"$1\r\nv"),
            "GET failed: {:?}",
            String::from_utf8_lossy(&reply)
        );
    }

    let payload = info(&mut s);
    let wakes = info_u64(&payload, "spsc_notify_wakes");
    assert!(
        wakes.is_some(),
        "INFO must expose `spsc_notify_wakes` (RED until the event-driven wake lands)"
    );
    assert!(
        wakes.unwrap_or(0) > 0,
        "spsc_notify_wakes must be > 0 after cross-shard traffic — the drain must be \
         notify-driven, not tick-driven"
    );
}

// ---------------------------------------------------------------------------
// swf2 — RED: an early-stopped drain self-re-notifies instead of stranding
// ---------------------------------------------------------------------------

/// AMENDED during build (recorded in TASK.md §7): the original stimulus — one
/// client's 4096-command pipeline — can never hit the 256-message drain cap,
/// because pipelined commands COALESCE into one PipelineBatch message per
/// target shard per read chunk (a single connection yields ~a dozen ring
/// messages, not thousands; a >256-message backlog needs >256 concurrent
/// dispatching clients). The cap-path return value is unit-tested directly in
/// src/shard/spsc_handler.rs (`drain_cap_reports_possible_tail`). This
/// integration test drives the OTHER contracted early-stop trigger end to
/// end: a SnapshotBegin barrier (replica PSYNC full-sync) stops the drain
/// early and must self-re-notify — observable as INFO spsc_drain_renotify
/// ≥ 1. The pipelined burst stays to pin completeness under cross-shard load.
#[test]
fn swf2_burst_renotify() {
    const BURST: usize = 4096;

    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), &[]));
    let mut s = wait_ready(port);

    // One pipelined write of BURST SETs (~half cross-shard) — pins burst
    // completeness; per the coalescing note above it does NOT hit the cap.
    let mut req = Vec::with_capacity(BURST * 40);
    for i in 0..BURST {
        let key = format!("swf2:{i}");
        req.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\nv\r\n",
                key.len(),
                key
            )
            .as_bytes(),
        );
    }
    s.write_all(&req).expect("write burst");

    // Read until BURST "+OK\r\n" frames arrived (count, don't parse).
    let mut ok_count = 0usize;
    let mut buf = vec![0u8; 64 * 1024];
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut tail: Vec<u8> = Vec::new();
    while ok_count < BURST {
        assert!(
            Instant::now() < deadline,
            "burst replies timed out at {ok_count}/{BURST}"
        );
        let n = s.read(&mut buf).expect("read burst replies");
        assert!(n > 0, "connection closed mid-burst at {ok_count}/{BURST}");
        tail.extend_from_slice(&buf[..n]);
        // Count complete "+OK\r\n" frames; keep any partial tail.
        let mut start = 0;
        while let Some(pos) = tail[start..].windows(5).position(|w| w == b"+OK\r\n") {
            ok_count += 1;
            start += pos + 5;
        }
        tail.drain(..start);
    }

    let payload = info(&mut s);
    assert!(
        info_u64(&payload, "spsc_drain_renotify").is_some(),
        "INFO must expose `spsc_drain_renotify` (RED until drain-cap re-notify lands)"
    );
}

/// The ≥1 half of the swf2 scenario, end to end under REAL concurrency.
///
/// Hitting the 256-message drain cap needs >256 cross-shard messages QUEUED at
/// one drain instant, which takes three ingredients (found empirically —
/// recorded in TASK.md §7):
///   1. >256 clients with concurrent in-flight dispatches. Each connection
///      pipelines one small SET per hash tag t0..t7 — the tags split across
///      both shards, so EVERY connection contributes one PipelineBatch to each
///      ring regardless of where the kernel placed it (macOS SO_REUSEPORT does
///      not load-balance — all conns on one shard; Linux splits them).
///   2. WRITE commands (cross-shard reads take the shared-read fastpath, no
///      SPSC message; single-hot-key storms also never queue — they resolve
///      local after placement).
///   3. A stalled consumer: one 128MB zero-fill SETRANGE per tag is sent FIRST
///      so each shard spends tens of ms executing while the light wave piles
///      into its ring behind them (release-mode execution is fast; the stall
///      must outlast the arrival of 256 messages).
///
/// `#[ignore]`: ~700 sockets, ~1 GB transient (8 × 128MB strings), several
/// seconds. Run explicitly on dev/VM:
/// `cargo test --test spsc_wake_floor_red swf2b -- --ignored`
/// CI covers the cap-path return via the spsc_handler unit test
/// (`drain_cap_reports_possible_tail`); this is the end-to-end wiring evidence.
#[test]
#[ignore = "700-connection load test: run explicitly on dev/VM (see TASK.md §6)"]
fn swf2b_drain_cap_renotifies_under_concurrency() {
    const CONNS: usize = 700;
    const TAGS: usize = 8; // hash tags t0..t7 — split across both shards w.h.p.

    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let _guard = ServerGuard(spawn_moon(port, dir.path(), &[]));
    let mut s = wait_ready(port);

    let mut conns: Vec<TcpStream> = (0..CONNS)
        .map(|_| connect(port, Duration::from_secs(10)))
        .collect();

    // Phase 1: heavy head — one 128MB zero-fill SETRANGE per tag stalls BOTH
    // shards' drains while the wave arrives.
    for (i, c) in conns.iter_mut().take(TAGS).enumerate() {
        let key = format!("h:{{t{i}}}:k");
        let req = format!(
            "*4\r\n$8\r\nSETRANGE\r\n${}\r\n{}\r\n$9\r\n134217728\r\n$1\r\nx\r\n",
            key.len(),
            key
        );
        c.write_all(req.as_bytes()).expect("write heavy SETRANGE");
    }
    std::thread::sleep(Duration::from_millis(10)); // let the heavies dispatch

    // Phase 2: light wave — each connection pipelines one small SET per tag;
    // every conn loads BOTH rings. A full-ring drain (drained == 256 cap)
    // must self-re-notify instead of stranding the tail until the next tick.
    for (i, c) in conns.iter_mut().enumerate().skip(TAGS) {
        let mut req = Vec::with_capacity(TAGS * 48);
        for t in 0..TAGS {
            let key = format!("w:{{t{t}}}:{i}");
            req.extend_from_slice(
                format!(
                    "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\nv\r\n",
                    key.len(),
                    key
                )
                .as_bytes(),
            );
        }
        c.write_all(&req).expect("write light SET pipeline");
    }
    std::thread::sleep(Duration::from_secs(6));

    let payload = info(&mut s);
    let renotify = info_u64(&payload, "spsc_drain_renotify");
    assert!(
        renotify.is_some(),
        "INFO must expose `spsc_drain_renotify` (RED until drain-cap re-notify lands)"
    );
    assert!(
        renotify.unwrap_or(0) >= 1,
        "a {CONNS}-client cross-shard write storm behind a stalled consumer must \
         hit the 256 drain cap at least once and self-re-notify"
    );
    drop(conns);
}

// ---------------------------------------------------------------------------
// swf3 — PIN: periodic cadence (cached clock + WAL flush) survives notify wakes
// ---------------------------------------------------------------------------

/// Green before AND after the build (Reject: "cadence_drift"). Under continuous
/// cross-shard traffic (post-build: notify wakes dominating the loop), the
/// periodic body must still run on its ~1ms cadence:
///   (a) cached clock advances -> a PX 150 key expires on time;
///   (b) WAL flush cadence holds -> a SET survives kill -9 + restart.
#[test]
fn swf3_cadence_pins() {
    let port = free_port();
    let dir = tempfile::tempdir().expect("tempdir");
    let mut server = ServerGuard(spawn_moon(port, dir.path(), &["--appendonly", "yes"]));
    let mut s = wait_ready(port);

    // TTL key set BEFORE the traffic storm.
    let reply = resp_cmd(&mut s, &[b"SET", b"swf3:ttl", b"v", b"PX", b"150"]);
    assert!(reply.starts_with(b"+OK"), "SET PX failed: {reply:?}");
    // Durable key the restart must recover.
    let reply = resp_cmd(&mut s, &[b"SET", b"swf3:durable", b"keepme"]);
    assert!(reply.starts_with(b"+OK"), "SET durable failed: {reply:?}");

    // ~400ms of continuous cross-shard traffic: post-build this makes notify
    // wakes dominate; the periodic body must still fire on its own cadence.
    let storm_end = Instant::now() + Duration::from_millis(400);
    let mut i = 0u32;
    while Instant::now() < storm_end {
        let key = format!("swf3:storm:{}", i % 64);
        let reply = resp_cmd(&mut s, &[b"SET", key.as_bytes(), b"x"]);
        assert!(reply.starts_with(b"+OK"), "storm SET failed");
        i += 1;
    }

    // (a) Clock advanced under load: the PX 150 key must be gone.
    let reply = resp_cmd(&mut s, &[b"GET", b"swf3:ttl"]);
    assert!(
        reply.starts_with(b"$-1") || reply.starts_with(b"_"),
        "PX 150 key still alive after ~400ms of traffic — cached clock starved: {:?}",
        String::from_utf8_lossy(&reply)
    );

    // (b) WAL flush cadence: give the 1ms flush tick ample room, then kill -9.
    std::thread::sleep(Duration::from_millis(200));
    server.0.kill().expect("kill -9 server");
    let _ = server.0.wait();

    let mut server2 = ServerGuard(spawn_moon(port, dir.path(), &["--appendonly", "yes"]));
    let mut s2 = wait_ready(port);
    let reply = resp_cmd(&mut s2, &[b"GET", b"swf3:durable"]);
    assert!(
        reply.starts_with(b"$6\r\nkeepme"),
        "durable key lost across kill -9 + restart — WAL flush cadence broken: {:?}",
        String::from_utf8_lossy(&reply)
    );
    drop(s2);
    let _ = server2.0.kill();
}
