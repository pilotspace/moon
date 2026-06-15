//! ADD task `ft-yield-costfree-monoio` — VERIFY-phase end-to-end FT.SEARCH QPS A/B
//! (scenario 2). Same binary, same data, same queries; the ONLY difference is the
//! brute-force yield chunk (sweeps 256/512/1024, gates on the shipped knee 512):
//!   - treatment: MOON_FT_YIELD_CHUNK=512     -> shipped default (self-pipe ~39 yields/query)
//!   - control:   MOON_FT_YIELD_CHUNK=1e9     -> one chunk = sync scan (no yield)
//!
//! If treatment QPS is within ~5% of the sync control, the cost-free yield's
//! overhead is negligible and the deferred throughput is reclaimed.
//!
//! #[ignore] — spawns servers + loads 20k×384d vectors; run explicitly on the VM:
//!   cargo test --test ft_yield_chunk_ab -- --ignored --nocapture

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const DIM: usize = 384;
const N_VECS: usize = 20_000;
const M_QUERIES: usize = 300;
const WARMUP: usize = 30;
const REPS: usize = 3;

fn moon_binary() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let p = l.local_addr().expect("addr").port();
    drop(l);
    p
}

fn fresh_dir(tag: &str, port: u16) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("moon-ftab-{tag}-{port}"));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).expect("create dir");
    d
}

fn spawn(port: u16, dir: &std::path::Path, chunk: Option<&str>) -> Child {
    let mut cmd = Command::new(moon_binary());
    cmd.args([
        "--port",
        &port.to_string(),
        "--shards",
        "1",
        "--appendonly",
        "no",
        "--dir",
        &dir.to_string_lossy(),
    ]);
    if let Some(c) = chunk {
        cmd.env("MOON_FT_YIELD_CHUNK", c);
    }
    cmd.stdout(std::fs::File::create(dir.join("out.log")).unwrap())
        .stderr(std::fs::File::create(dir.join("err.log")).unwrap())
        .spawn()
        .expect("spawn moon")
}

fn connect(port: u16) -> TcpStream {
    let addr = format!("127.0.0.1:{port}")
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let start = Instant::now();
    loop {
        if let Ok(s) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            s.set_read_timeout(Some(Duration::from_secs(30))).ok();
            s.set_write_timeout(Some(Duration::from_secs(30))).ok();
            return s;
        }
        if start.elapsed() > Duration::from_secs(30) {
            panic!("server never accepted on {port}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_ready(port: u16) -> TcpStream {
    let mut s = connect(port);
    let start = Instant::now();
    loop {
        s.write_all(b"PING\r\n").unwrap();
        let mut buf = [0u8; 64];
        if let Ok(n) = s.read(&mut buf) {
            if n > 0 && buf[..n].windows(4).any(|w| w == b"PONG") {
                return s;
            }
        }
        assert!(start.elapsed() < Duration::from_secs(15), "no PING");
        std::thread::sleep(Duration::from_millis(100));
        s = connect(port);
    }
}

fn encode(parts: &[&[u8]]) -> Vec<u8> {
    let mut r = Vec::new();
    r.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        r.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        r.extend_from_slice(p);
        r.extend_from_slice(b"\r\n");
    }
    r
}

/// Deterministic vector bytes (LCG — no rand dep, identical across arms).
fn vec_bytes(seed: &mut u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(DIM * 4);
    for _ in 0..DIM {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let f = ((*seed >> 33) as f32) / ((1u64 << 31) as f32); // ~[0,2)
        b.extend_from_slice(&f.to_le_bytes());
    }
    b
}

fn read_until_pong(s: &mut TcpStream) {
    let mut acc: Vec<u8> = Vec::with_capacity(1 << 20);
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = s.read(&mut buf).expect("read");
        assert!(n > 0, "closed mid-stream");
        acc.extend_from_slice(&buf[..n]);
        if acc.ends_with(b"+PONG\r\n") {
            return;
        }
    }
}

fn create_idx(s: &mut TcpStream) {
    let dim = DIM.to_string();
    let req = encode(&[
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
        b"8", // 4 attr pairs: DIM, TYPE, DISTANCE_METRIC, COMPACT_THRESHOLD
        b"DIM",
        dim.as_bytes(),
        b"TYPE",
        b"FLOAT32",
        b"DISTANCE_METRIC",
        b"L2",
        b"COMPACT_THRESHOLD",
        b"100000", // > N_VECS => stays brute-force (no compaction)
    ]);
    s.write_all(&req).unwrap();
    let mut buf = [0u8; 256];
    let n = s.read(&mut buf).unwrap();
    assert!(
        buf[..n].windows(2).any(|w| w == b"OK"),
        "FT.CREATE: {:?}",
        String::from_utf8_lossy(&buf[..n])
    );
}

fn load_vectors(s: &mut TcpStream) {
    let mut seed = 0x1234_5678_9abc_def0u64;
    let batch = 1000usize;
    let mut written = 0;
    while written < N_VECS {
        let upto = (written + batch).min(N_VECS);
        let mut req = Vec::new();
        for i in written..upto {
            let key = format!("doc:{i}");
            let blob = vec_bytes(&mut seed);
            req.extend_from_slice(&encode(&[b"HSET", key.as_bytes(), b"vec", &blob]));
        }
        req.extend_from_slice(b"PING\r\n");
        s.write_all(&req).unwrap();
        read_until_pong(s);
        written = upto;
    }
}

/// Pipeline M FT.SEARCH KNN queries + a trailing PING; time write→PONG. QPS = M/elapsed.
fn bench_qps(s: &mut TcpStream, query_blob: &[u8], m: usize) -> f64 {
    let one = encode(&[
        b"FT.SEARCH",
        b"idx",
        b"*=>[KNN 10 @vec $q]",
        b"PARAMS",
        b"2",
        b"q",
        query_blob,
        b"DIALECT",
        b"2",
    ]);
    let mut req = Vec::with_capacity(one.len() * m + 16);
    for _ in 0..m {
        req.extend_from_slice(&one);
    }
    req.extend_from_slice(b"PING\r\n");

    let start = Instant::now();
    s.write_all(&req).unwrap();
    read_until_pong(s);
    let elapsed = start.elapsed();
    m as f64 / elapsed.as_secs_f64()
}

struct Guard(Child);
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Best-of-REPS QPS for one arm (fresh server per arm; best-of reduces VM jitter).
fn measure_arm(chunk: Option<&str>, tag: &str) -> f64 {
    let port = free_port();
    let dir = fresh_dir(tag, port);
    let _g = Guard(spawn(port, &dir, chunk));
    let mut s = wait_ready(port);
    create_idx(&mut s);
    load_vectors(&mut s);

    let mut qseed = 0xfeed_face_dead_beefu64;
    let query_blob = vec_bytes(&mut qseed);

    // warmup
    let _ = bench_qps(&mut s, &query_blob, WARMUP);

    let mut best = 0.0f64;
    for _ in 0..REPS {
        let q = bench_qps(&mut s, &query_blob, M_QUERIES);
        if q > best {
            best = q;
        }
    }
    best
}

#[test]
#[ignore = "VERIFY-phase FT.SEARCH QPS A/B — spawns servers + loads 20k×384d; run with --ignored on the VM"]
fn scenario2_qps_within_5pct_of_sync_control() {
    let sync = measure_arm(Some("1000000000"), "sync"); // control: one chunk, never yields
    let chunks = [("256", "256"), ("512", "512"), ("1024", "1024")];

    println!(
        "\n========== FT.SEARCH QPS A/B (brute-force {N_VECS}x{DIM}d L2, KNN10, release) =========="
    );
    println!("control (sync, chunk=1e9):  {sync:>10.1} qps   [self-pipe overhead vs sync]");
    let mut at_default = 0.0f64; // shipped knee = 512
    for (c, tag) in chunks {
        let q = measure_arm(Some(c), tag);
        let ratio = q / sync;
        let overhead = (1.0 - ratio) * 100.0;
        if c == "512" {
            at_default = ratio;
        }
        println!("self-pipe chunk={c:<5}      {q:>10.1} qps   {overhead:+.2}%  (gap ~{c} vecs)");
    }
    println!(
        "=====================================================================================\n"
    );

    assert!(
        at_default >= 0.95,
        "scenario 2: self-pipe@512 (shipped knee) QPS must be within ~5% of the sync control \
         at {DIM}d; got {:+.2}% overhead",
        (1.0 - at_default) * 100.0
    );
}
