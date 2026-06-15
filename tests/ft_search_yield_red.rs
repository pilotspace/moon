//! ADD task `ft-search-off-eventloop` §4 TESTS — runtime suite (behavioral).
//!
//! RED driver (fails until §5 BUILD lands — the INFO counter does not exist yet):
//!   - m1_heavy_search_yields_cooperatively — after a heavy FT.SEARCH, INFO field
//!     `ft_search_cooperative_yields_total` is PRESENT and > 0 (the deterministic
//!     C5 proxy: the local slice relinquished to the shard event loop ≥ once).
//!     RED now (field absent ⇒ parsed as None). Mirrors `spsc_wake_floor_red::swf1`.
//!
//! GREEN-PINS (must be green BEFORE and AFTER build — the yield must not change them):
//!   - m2_topk_known_neighbors_keys_resolved — FT.SEARCH returns the known nearest
//!     doc KEY first, resolved via key_hash_to_key (never a synthetic `vec:<id>`).
//!     Guards G-IDENTITY at the wire.
//!   - m3_write_visible_and_consistent — a write is visible to the NEXT search
//!     (write not lost; the M3 After-clause). The true mid-search interleave
//!     isolation is guarded by the reused green-pins `ft_search_concurrent_readers.rs`
//!     (G10) + the §3 G-ISOLATION contract; update-time index dedup is a separate,
//!     pre-existing, out-of-scope Moon behavior and is NOT asserted here.
//!   - m4_ft_basic_correctness_smoke — FT.CREATE/HSET/FT.SEARCH/FT.INFO num_docs
//!     basic correctness (a fast guard the async-ripple build didn't break FT.*).
//!
//! CORROBORATION (ignored — jitter-sensitive, run by hand at verify on a quiesced VM):
//!   - m1b_colocated_ping_progress_during_search — wall-clock co-located PING relief.
//!
//! FT.* needs the default `graph` feature → whole file is `#[cfg(feature="graph")]`
//! (mirrors `ft_search_concurrent_readers.rs`). The compile-red API pins live in
//! `ft_search_yield_red_api.rs`.
//!
//! Run:  cargo test --test ft_search_yield_red --features graph -- --test-threads=1
#![cfg(feature = "graph")]

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Shared harness (CARGO_BIN_EXE spawn pattern, mirrors spsc_wake_floor_red.rs)
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

/// Fresh unique `--dir` per server (CWD persistence-reload trap: an inherited
/// `appendonlydir` would replay stale indexes into a throwaway test server).
fn fresh_dir(port: u16) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("moon-ftyield-{port}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).expect("create fresh dir");
    d
}

/// Spawn `moon --port P --shards 1 --appendonly no --dir <fresh>` so FT.SEARCH and
/// the co-located commands share ONE event loop (the intra-shard stall under test).
fn spawn_single_shard(port: u16, dir: &std::path::Path) -> Child {
    Command::new(moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "no",
            "--dir",
            &dir.to_string_lossy(),
        ])
        // Pin a SMALL brute-force yield chunk so a 1000-vector search crosses it
        // (=> the yield fires) deterministically and fast. The PRODUCTION default
        // is tuned much coarser for throughput (monoio's timer-yield has a per-yield
        // cost — see tmp/bench_ftsearch/RESULTS.md); this test exercises the yield
        // MECHANISM, not the tuning, so it pins the chunk rather than inserting
        // tens of thousands of vectors. The `yields > 0` assertion is unchanged.
        .env("MOON_FT_YIELD_CHUNK", "256")
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

/// Send one RESP command (binary-safe), read one short reply frame.
fn resp_cmd(s: &mut TcpStream, parts: &[&[u8]]) -> Vec<u8> {
    write_cmd(s, parts);
    let mut buf = vec![0u8; 64 * 1024];
    let n = s.read(&mut buf).expect("read reply");
    assert!(n > 0, "connection closed mid-reply");
    buf.truncate(n);
    buf
}

fn write_cmd(s: &mut TcpStream, parts: &[&[u8]]) {
    let mut req = Vec::with_capacity(64);
    req.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        req.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        req.extend_from_slice(p);
        req.extend_from_slice(b"\r\n");
    }
    s.write_all(&req).expect("write cmd");
}

/// Send a command whose reply may span several TCP segments (FT.SEARCH arrays):
/// drain until a brief read-idle. Fully consumes the reply so the socket is clean
/// for the next command.
fn cmd_drain(s: &mut TcpStream, parts: &[&[u8]]) -> Vec<u8> {
    write_cmd(s, parts);
    s.set_read_timeout(Some(Duration::from_millis(400))).ok();
    let mut acc = Vec::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => acc.extend_from_slice(&buf[..n]),
            Err(_) => break, // read timeout ⇒ reply drained
        }
    }
    s.set_read_timeout(Some(Duration::from_secs(5))).ok();
    acc
}

/// Fetch INFO and return it as a lossy string.
fn info(s: &mut TcpStream) -> String {
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
            if let Some(pos) = acc.windows(2).position(|w| w == b"\r\n") {
                assert_eq!(acc[0], b'$', "INFO must be a bulk string");
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
            .and_then(|v| v.trim().parse::<u64>().ok())
    })
}

/// 4×f32 little-endian vector blob (matches the DIM 4 / TYPE FLOAT32 schema).
fn vec4_bytes(v: [f32; 4]) -> Vec<u8> {
    let mut b = Vec::with_capacity(16);
    for f in v {
        b.extend_from_slice(&f.to_le_bytes());
    }
    b
}

/// FT.SEARCH match count = first element of the reply array (Int or Bulk form).
fn ft_search_count(reply: &[u8]) -> Option<i64> {
    let nl = reply.windows(2).position(|w| w == b"\r\n")?;
    let rest = &reply[nl + 2..];
    match rest.first()? {
        b':' => {
            let end = rest.windows(2).position(|w| w == b"\r\n")?;
            std::str::from_utf8(&rest[1..end]).ok()?.trim().parse().ok()
        }
        b'$' => {
            let hdr = rest.windows(2).position(|w| w == b"\r\n")?;
            let body = &rest[hdr + 2..];
            let end = body.windows(2).position(|w| w == b"\r\n")?;
            std::str::from_utf8(&body[..end]).ok()?.trim().parse().ok()
        }
        _ => None,
    }
}

fn create_idx(s: &mut TcpStream) {
    let r = resp_cmd(
        s,
        &[
            b"FT.CREATE",
            b"idx",
            b"ON",
            b"HASH",
            b"PREFIX",
            b"1",
            b"doc:",
            b"SCHEMA",
            b"vec",
            b"VECTOR",
            b"HNSW",
            b"6",
            b"DIM",
            b"4",
            b"TYPE",
            b"FLOAT32",
            b"DISTANCE_METRIC",
            b"L2",
        ],
    );
    assert!(
        r.windows(2).any(|w| w == b"OK"),
        "FT.CREATE must return OK, got {:?}",
        String::from_utf8_lossy(&r)
    );
}

fn hset_vec(s: &mut TcpStream, key: &[u8], v: [f32; 4]) {
    let blob = vec4_bytes(v);
    let r = resp_cmd(s, &[b"HSET", key, b"vec", &blob]);
    assert!(
        r.first() == Some(&b':'),
        "HSET must return an integer, got {:?}",
        String::from_utf8_lossy(&r)
    );
}

const Q: [f32; 4] = [1.0, 0.0, 0.0, 0.0];

fn ft_search(s: &mut TcpStream) -> Vec<u8> {
    let q = vec4_bytes(Q);
    cmd_drain(
        s,
        &[
            b"FT.SEARCH",
            b"idx",
            b"*=>[KNN 10 @vec $q]",
            b"PARAMS",
            b"2",
            b"q",
            &q,
            b"DIALECT",
            b"2",
        ],
    )
}

struct ServerGuard(Child);
impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// ---------------------------------------------------------------------------
// m1 — RED: a heavy FT.SEARCH must yield cooperatively (deterministic C5 proxy)
// ---------------------------------------------------------------------------

#[test]
fn m1_heavy_search_yields_cooperatively() {
    let port = free_port();
    let dir = fresh_dir(port);
    let guard = ServerGuard(spawn_single_shard(port, &dir));
    let mut s = wait_ready(port);

    create_idx(&mut s);

    // Heavy index: enough mutable vectors that one brute-force scan exceeds the
    // bounded per-chunk cap ⇒ the yielding seam relinquishes ≥ once per search.
    for i in 0..1000u32 {
        let key = format!("doc:{i}");
        let a = (i % 97) as f32 / 97.0;
        let b = (i % 31) as f32 / 31.0;
        hset_vec(&mut s, key.as_bytes(), [a, b, 1.0 - a, 1.0 - b]);
    }

    // One heavy search; reply must be an array (not an error frame).
    let reply = ft_search(&mut s);
    assert!(
        reply.first() == Some(&b'*'),
        "FT.SEARCH must return an array, got {:?}",
        String::from_utf8_lossy(&reply[..reply.len().min(120)])
    );

    let payload = info(&mut s);
    let yields = info_u64(&payload, "ft_search_cooperative_yields_total");
    assert!(
        yields.is_some_and(|n| n > 0),
        "M1 (RED until build): a heavy FT.SEARCH over a 1-shard loop must yield \
         cooperatively — INFO `ft_search_cooperative_yields_total` present and > 0. \
         Got {yields:?}. Until the §5 build adds the yielding seam + the INFO counter, \
         the search runs synchronously and this field is absent."
    );

    drop(guard);
}

// ---------------------------------------------------------------------------
// m1b — corroboration (ignored: jitter-sensitive; run by hand at verify)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "wall-clock co-located latency — jitter-sensitive; run on a quiesced VM at verify"]
fn m1b_colocated_ping_progress_during_search() {
    let port = free_port();
    let dir = fresh_dir(port);
    let guard = ServerGuard(spawn_single_shard(port, &dir));
    let mut setup = wait_ready(port);
    create_idx(&mut setup);
    for i in 0..2000u32 {
        let key = format!("doc:{i}");
        let a = (i % 97) as f32 / 97.0;
        hset_vec(
            &mut setup,
            key.as_bytes(),
            [a, 1.0 - a, a * 0.5, 1.0 - a * 0.5],
        );
    }

    // Connection A fires a continuous stream of heavy searches; connection B
    // measures co-located PING RTT during that window.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_a = stop.clone();
    let searcher = std::thread::spawn(move || {
        let mut a = connect(port, Duration::from_secs(5));
        while !stop_a.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = ft_search(&mut a);
        }
    });

    let mut b = connect(port, Duration::from_secs(5));
    let mut max_rtt = Duration::ZERO;
    for _ in 0..200 {
        let t0 = Instant::now();
        let _ = resp_cmd(&mut b, &[b"PING"]);
        max_rtt = max_rtt.max(t0.elapsed());
        std::thread::sleep(Duration::from_millis(2));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = searcher.join();

    assert!(
        max_rtt < Duration::from_millis(25),
        "M1b: co-located PING max RTT during heavy FT.SEARCH was {max_rtt:?} — the \
         search is monopolizing the event loop (expected < 25ms once the slice yields)"
    );
    drop(guard);
}

// ---------------------------------------------------------------------------
// m2 — GREEN PIN: known nearest neighbor, key resolved (G-IDENTITY)
// ---------------------------------------------------------------------------

#[test]
fn m2_topk_known_neighbors_keys_resolved() {
    let port = free_port();
    let dir = fresh_dir(port);
    let guard = ServerGuard(spawn_single_shard(port, &dir));
    let mut s = wait_ready(port);

    create_idx(&mut s);
    hset_vec(&mut s, b"doc:a", [1.0, 0.0, 0.0, 0.0]); // exact match to Q
    hset_vec(&mut s, b"doc:b", [0.0, 1.0, 0.0, 0.0]);
    hset_vec(&mut s, b"doc:c", [0.0, 0.0, 1.0, 0.0]);
    hset_vec(&mut s, b"doc:d", [0.0, 0.0, 0.0, 1.0]);
    std::thread::sleep(Duration::from_millis(80)); // let auto-index settle

    let reply = ft_search(&mut s);
    let count = ft_search_count(&reply).expect("FT.SEARCH count parses");
    assert!(
        count >= 1,
        "expected at least the exact-match doc, got {count}"
    );
    assert!(
        reply.windows(5).any(|w| w == b"doc:a"),
        "G-IDENTITY: the exact-match key `doc:a` must be in the result, got {:?}",
        String::from_utf8_lossy(&reply)
    );
    assert!(
        !reply.windows(4).any(|w| w == b"vec:"),
        "G-IDENTITY: keys must resolve via key_hash_to_key, never a synthetic `vec:<id>`: {:?}",
        String::from_utf8_lossy(&reply)
    );
    drop(guard);
}

// ---------------------------------------------------------------------------
// m3 — GREEN PIN: write visible to next search, update not duplicated
// ---------------------------------------------------------------------------

#[test]
fn m3_write_visible_and_consistent() {
    let port = free_port();
    let dir = fresh_dir(port);
    let guard = ServerGuard(spawn_single_shard(port, &dir));
    let mut s = wait_ready(port);

    create_idx(&mut s);
    hset_vec(&mut s, b"doc:a", [1.0, 0.0, 0.0, 0.0]);
    hset_vec(&mut s, b"doc:b", [0.0, 1.0, 0.0, 0.0]);
    std::thread::sleep(Duration::from_millis(60));
    let c0 = ft_search_count(&ft_search(&mut s)).expect("count");
    assert_eq!(c0, 2, "two docs indexed");

    // A new write is visible to the NEXT search (not lost) — the M3 After-clause:
    // the post-write doc IS visible to the next search. (The true mid-search
    // interleave isolation is guarded by the reused `ft_search_concurrent_readers.rs`
    // G10 pin + the §3 G-ISOLATION contract; update-time index dedup is a separate,
    // pre-existing, out-of-scope Moon behavior and is NOT asserted here.)
    hset_vec(&mut s, b"doc:c", [0.0, 0.0, 1.0, 0.0]);
    std::thread::sleep(Duration::from_millis(60));
    let c1 = ft_search_count(&ft_search(&mut s)).expect("count");
    assert_eq!(
        c1, 3,
        "the new doc is visible to a subsequent search (write not lost)"
    );
    drop(guard);
}

// ---------------------------------------------------------------------------
// m4 — GREEN PIN: FT.* basic correctness smoke (no-regression)
// ---------------------------------------------------------------------------

#[test]
fn m4_ft_basic_correctness_smoke() {
    let port = free_port();
    let dir = fresh_dir(port);
    let guard = ServerGuard(spawn_single_shard(port, &dir));
    let mut s = wait_ready(port);

    create_idx(&mut s);
    for i in 0..10u32 {
        let key = format!("doc:{i}");
        let a = i as f32 / 10.0;
        hset_vec(&mut s, key.as_bytes(), [a, 1.0 - a, 0.0, 0.0]);
    }
    std::thread::sleep(Duration::from_millis(80));

    let info_reply = cmd_drain(&mut s, &[b"FT.INFO", b"idx"]);
    assert!(
        info_reply.windows(8).any(|w| w == b"num_docs"),
        "FT.INFO must report num_docs, got {:?}",
        String::from_utf8_lossy(&info_reply)
    );
    let count = ft_search_count(&ft_search(&mut s)).expect("count");
    assert_eq!(
        count, 10,
        "FT.SEARCH must see all 10 indexed docs, got {count}"
    );
    drop(guard);
}
