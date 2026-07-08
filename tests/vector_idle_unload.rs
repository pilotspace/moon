//! WS3: vector segment idle-unload integration test.
//!
//! Proves the HOT->WARM idle-unload path end to end against a REAL server
//! process:
//!   1. An immutable (HOT) segment with an exact-rerank f16 sidecar is
//!      created by FT.COMPACT.
//!   2. Left untouched past `--engine-offload-idle-secs`, it demotes to the
//!      WARM (mmap-backed) tier automatically — `--segment-warm-after` is
//!      set far higher so ONLY the idle criterion can trigger this,
//!      isolating the new code path from the pre-existing age-based one.
//!   3. FT.INFO's `graph_segments` / `warm_segments` counters track the
//!      transition.
//!   4. FT.SEARCH after the transition returns the SAME nearest neighbor
//!      with an exact (near-zero) rerank distance — proving the f16 sidecar
//!      was carried over to the WARM tier instead of silently dropped
//!      (the pre-WS3 code path always passed `None` for `vectors_data`,
//!      which would have degraded this to a quantized ADC-only distance).
//!
//! Run (monoio default, matches CI):
//!   cargo build --release
//!   cargo test --release --test vector_idle_unload -- --ignored
//!
//! tokio runtime:
//!   cargo build --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index
//!   cargo test --release --no-default-features \
//!     --features runtime-tokio,jemalloc,graph,text-index \
//!     --test vector_idle_unload -- --ignored

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

use std::io::{BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const DIM: usize = 16;

// ---------------------------------------------------------------------------
// Binary resolution (pattern: tests/crash_recovery_vector_durability.rs)
// ---------------------------------------------------------------------------

fn find_moon_binary() -> PathBuf {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = PathBuf::from(bin);
        if p.exists() {
            return p;
        }
    }
    PathBuf::from(env!("CARGO_BIN_EXE_moon"))
}

fn unique_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

fn unique_dir(suffix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-idleunload-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

struct ServerGuard {
    child: Child,
    dir_marker: String,
}

impl ServerGuard {
    fn new(child: Child, dir: &Path) -> Self {
        Self {
            child,
            dir_marker: dir.to_string_lossy().into_owned(),
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            sigkill(&mut self.child);
        } else {
            let _ = self.child.wait();
        }
        let _ = Command::new("pkill")
            .args(["-9", "-f"])
            .arg(&self.dir_marker)
            .output();
    }
}

#[cfg(unix)]
fn sigkill(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(pid, libc::SIGKILL);
    }
    let _ = child.wait();
}

#[cfg(not(unix))]
fn sigkill(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Spawn moon with disk-offload at its default (`enable`) — required for the
/// HOT->WARM transition tick to run at all (`server_config.disk_offload_enabled()`
/// gates it in `event_loop.rs`). `--segment-warm-after` is set far above the
/// test's timeout so age-based demotion never fires; `--engine-offload-idle-secs`
/// is set low so idleness is the ONLY thing that can trigger the transition
/// within this test.
fn spawn_moon(port: u16, dir: &Path, idle_secs: u64) -> ServerGuard {
    std::fs::create_dir_all(dir).expect("create test dir");
    let child = Command::new(find_moon_binary())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            "1",
            "--appendonly",
            "yes",
            "--segment-warm-after",
            "3600",
            "--engine-offload-idle-secs",
            &idle_secs.to_string(),
            "--disk-free-min-pct",
            "0",
            "--dir",
        ])
        .arg(dir)
        .env("RUST_LOG", "moon=info")
        .stdout(
            std::fs::File::options()
                .append(true)
                .create(true)
                .open(dir.join("moon.stdout.log"))
                .expect("stdout log"),
        )
        .stderr(
            std::fs::File::options()
                .append(true)
                .create(true)
                .open(dir.join("moon.stderr.log"))
                .expect("stderr log"),
        )
        .spawn()
        .expect("spawn moon (run `cargo build --release` first, or set MOON_BIN)");
    ServerGuard::new(child, dir)
}

// ---------------------------------------------------------------------------
// Minimal RESP client (pattern: tests/crash_recovery_vector_durability.rs)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Clone)]
enum V {
    Simple(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Arr(Vec<V>),
    Null,
}

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(port: u16) -> Self {
        let addr = format!("127.0.0.1:{port}")
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap();
        let start = Instant::now();
        let stream = loop {
            match TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(s) => break s,
                Err(_) if start.elapsed() < Duration::from_secs(30) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("server never accepted on port {port}: {e}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(20)))
            .unwrap();
        let writer = stream.try_clone().unwrap();
        Client {
            reader: BufReader::new(stream),
            writer,
        }
    }

    fn encode(args: &[&[u8]]) -> Vec<u8> {
        let mut out = format!("*{}\r\n", args.len()).into_bytes();
        for a in args {
            out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            out.extend_from_slice(a);
            out.extend_from_slice(b"\r\n");
        }
        out
    }

    fn read_line(&mut self) -> String {
        let mut line = Vec::new();
        let mut b = [0u8; 1];
        loop {
            self.reader.read_exact(&mut b).expect("read byte");
            if b[0] == b'\n' {
                break;
            }
            if b[0] != b'\r' {
                line.push(b[0]);
            }
        }
        String::from_utf8_lossy(&line).into_owned()
    }

    fn parse(&mut self) -> V {
        let line = self.read_line();
        let (t, rest) = line.split_at(1);
        match t {
            "+" => V::Simple(rest.to_string()),
            "-" => V::Err(rest.to_string()),
            ":" => V::Int(rest.parse().expect("int")),
            "$" => {
                let n: i64 = rest.parse().expect("bulk len");
                if n < 0 {
                    return V::Null;
                }
                let mut buf = vec![0u8; n as usize + 2];
                self.reader.read_exact(&mut buf).expect("bulk body");
                buf.truncate(n as usize);
                V::Bulk(buf)
            }
            "*" => {
                let n: i64 = rest.parse().expect("arr len");
                if n < 0 {
                    return V::Null;
                }
                V::Arr((0..n).map(|_| self.parse()).collect())
            }
            other => panic!("unexpected RESP type {other:?} (line {line:?})"),
        }
    }

    fn cmd(&mut self, args: &[&[u8]]) -> V {
        self.writer.write_all(&Self::encode(args)).expect("send");
        self.parse()
    }

    fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> Vec<V> {
        let mut buf = Vec::new();
        for c in cmds {
            let refs: Vec<&[u8]> = c.iter().map(|a| a.as_slice()).collect();
            buf.extend_from_slice(&Self::encode(&refs));
        }
        self.writer.write_all(&buf).expect("send pipeline");
        cmds.iter().map(|_| self.parse()).collect()
    }
}

fn wait_ready(port: u16) -> Client {
    let mut c = Client::connect(port);
    let start = Instant::now();
    loop {
        match c.cmd(&[b"PING"]) {
            V::Simple(s) if s == "PONG" => return c,
            _ if start.elapsed() < Duration::from_secs(30) => {
                std::thread::sleep(Duration::from_millis(100));
            }
            other => panic!("server never answered PING: {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Vector fixtures — deterministic splitmix64-style hash per (seed, dim).
// ---------------------------------------------------------------------------

fn fixture_vec(seed: u32, dim: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(dim * 4);
    for d in 0..dim {
        let mut h = (seed as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add((d as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9))
            .wrapping_add(0x94D0_49BB_1331_11EB);
        h ^= h >> 33;
        h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
        h ^= h >> 33;
        h = h.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
        h ^= h >> 33;
        let v = ((h >> 40) as f32 / (1u64 << 24) as f32) * 10.0;
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// FT.* helpers
// ---------------------------------------------------------------------------

fn ft_create(c: &mut Client, idx: &str, dim: usize, compact_threshold: u32) {
    let prefix = format!("{idx}:");
    let params: Vec<Vec<u8>> = vec![
        b"TYPE".to_vec(),
        b"FLOAT32".to_vec(),
        b"DIM".to_vec(),
        dim.to_string().into_bytes(),
        b"DISTANCE_METRIC".to_vec(),
        b"L2".to_vec(),
        b"QUANTIZATION".to_vec(),
        b"SQ8".to_vec(),
        b"COMPACT_THRESHOLD".to_vec(),
        compact_threshold.to_string().into_bytes(),
    ];
    let mut args: Vec<Vec<u8>> = vec![
        b"FT.CREATE".to_vec(),
        idx.as_bytes().to_vec(),
        b"ON".to_vec(),
        b"HASH".to_vec(),
        b"PREFIX".to_vec(),
        b"1".to_vec(),
        prefix.into_bytes(),
        b"SCHEMA".to_vec(),
        b"vec".to_vec(),
        b"VECTOR".to_vec(),
        b"HNSW".to_vec(),
        params.len().to_string().into_bytes(),
    ];
    args.extend(params);
    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
    let r = c.cmd(&refs);
    assert_eq!(r, V::Simple("OK".into()), "FT.CREATE {idx} failed: {r:?}");
}

fn hset_batch(c: &mut Client, prefix: &str, ids: &[u32]) {
    let cmds: Vec<Vec<Vec<u8>>> = ids
        .iter()
        .map(|&i| {
            let key = format!("{prefix}{i}");
            vec![
                b"HSET".to_vec(),
                key.into_bytes(),
                b"vec".to_vec(),
                fixture_vec(i, DIM),
            ]
        })
        .collect();
    let replies = c.pipeline(&cmds);
    for (i, r) in replies.iter().enumerate() {
        assert!(matches!(r, V::Int(_)), "HSET batch #{i} failed: {r:?}");
    }
}

fn ft_compact(c: &mut Client, idx: &str) {
    let r = c.cmd(&[b"FT.COMPACT", idx.as_bytes()]);
    assert_eq!(r, V::Simple("OK".into()), "FT.COMPACT {idx} failed: {r:?}");
}

/// KNN 1 search; returns `(key, distance)` for the top hit. The score field
/// is always named `__vec_score` regardless of any `AS <alias>` clause (see
/// `ft_search/response.rs::extract_score_from_fields`).
fn search_top1(c: &mut Client, idx: &str, blob: &[u8]) -> (String, f64) {
    let query = "*=>[KNN 1 @vec $B]".to_string();
    let r = c.cmd(&[
        b"FT.SEARCH",
        idx.as_bytes(),
        query.as_bytes(),
        b"PARAMS",
        b"2",
        b"B",
        blob,
        b"DIALECT",
        b"2",
    ]);
    let V::Arr(items) = r else {
        panic!("FT.SEARCH {idx} reply not array: {r:?}");
    };
    assert!(items.len() >= 3, "expected at least one hit, got {items:?}");
    let key = match &items[1] {
        V::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("expected bulk key, got {other:?}"),
    };
    // Fields array: [field1_name, field1_val, ...] — find "__vec_score".
    let V::Arr(fields) = &items[2] else {
        panic!("expected fields array, got {:?}", items[2]);
    };
    let mut dist = f64::NAN;
    for pair in fields.chunks(2) {
        if let [V::Bulk(name), V::Bulk(val)] = pair
            && name.as_slice() == b"__vec_score"
        {
            dist = String::from_utf8_lossy(val).parse().unwrap_or(f64::NAN);
        }
    }
    (key, dist)
}

fn ft_info_int(c: &mut Client, idx: &str, field: &str) -> i64 {
    let r = c.cmd(&[b"FT.INFO", idx.as_bytes()]);
    let V::Arr(items) = r else {
        panic!("FT.INFO {idx} reply not array: {r:?}");
    };
    for pair in items.chunks(2) {
        if let [V::Bulk(k), V::Int(n)] = pair
            && k.as_slice() == field.as_bytes()
        {
            return *n;
        }
    }
    panic!("field {field} not found in FT.INFO {idx} reply: {items:?}");
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[test]
#[ignore] // Spawns a real server process; run explicitly with `-- --ignored`.
fn hot_segment_idle_unloads_and_preserves_recall() {
    let port = unique_port();
    let dir = unique_dir("s1");
    let idx = "idleidx";

    let _guard = spawn_moon(port, &dir, 2);
    let mut c = wait_ready(port);

    ft_create(&mut c, idx, DIM, 100);
    // Below COMPACT_THRESHOLD so auto-compact never fires -- the explicit
    // FT.COMPACT below must produce exactly one HOT segment for the
    // graph_segments==1 assertion to hold.
    let ids: Vec<u32> = (0..90).collect();
    hset_batch(&mut c, &format!("{idx}:"), &ids);
    ft_compact(&mut c, idx);

    // Baseline: HOT segment present with an exact-rerank sidecar, no WARM
    // segments yet.
    assert_eq!(
        ft_info_int(&mut c, idx, "graph_segments"),
        1,
        "expected exactly one HOT segment after FT.COMPACT"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "segments_with_exact_rerank"),
        1,
        "freshly compacted segment must carry the f16 sidecar"
    );
    assert_eq!(ft_info_int(&mut c, idx, "warm_segments"), 0);

    let probe = fixture_vec(7, DIM);
    let (key_before, dist_before) = search_top1(&mut c, idx, &probe);
    assert_eq!(key_before, format!("{idx}:7"), "self-match must find id 7");
    assert!(
        dist_before < 1e-3,
        "exact self-match distance should be ~0 pre-transition, got {dist_before}"
    );

    // Idle out the segment: stop querying for longer than
    // --engine-offload-idle-secs (2s) and let the warm-check tick (adapted
    // down to ~1s by the idle threshold, see event_loop.rs) demote it.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if ft_info_int(&mut c, idx, "warm_segments") >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "segment never transitioned to warm tier within 30s"
        );
        std::thread::sleep(Duration::from_millis(500));
    }

    assert_eq!(
        ft_info_int(&mut c, idx, "graph_segments"),
        0,
        "HOT segment should be gone after warm transition"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "warm_segments_with_exact_rerank"),
        1,
        "the f16 sidecar must have been carried over to the WARM tier"
    );

    // Recall + exactness after the transition: same key, same near-zero
    // distance. If the sidecar had been dropped (pre-WS3 behavior), the
    // WARM segment would answer with a quantized SQ8 ADC estimate instead
    // of the true distance, which is measurably non-zero for a self-match.
    let (key_after, dist_after) = search_top1(&mut c, idx, &probe);
    assert_eq!(
        key_after, key_before,
        "top-1 result must be unchanged across the HOT->WARM transition"
    );
    assert!(
        dist_after < 1e-3,
        "exact self-match distance should still be ~0 post-transition \
         (sidecar preserved), got {dist_after}"
    );
}
