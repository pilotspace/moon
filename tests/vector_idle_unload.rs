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
    spawn_moon_full(port, dir, 3600, idle_secs)
}

/// Same as [`spawn_moon`] but with both thresholds explicit -- used by the
/// resurrection tests to isolate the WARM-only path (idle disabled, age low)
/// from the COLD-only path (age high, idle low).
fn spawn_moon_full(port: u16, dir: &Path, warm_after_secs: u64, idle_secs: u64) -> ServerGuard {
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
            &warm_after_secs.to_string(),
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

/// WARM-only spawn: idle disabled entirely (`--engine-offload-idle-secs 0`),
/// so only the age criterion (`--segment-warm-after`) can demote a segment --
/// used to isolate the WARM-tier resurrection test from the COLD path.
fn spawn_moon_warm_only(port: u16, dir: &Path, warm_after_secs: u64) -> ServerGuard {
    spawn_moon_full(port, dir, warm_after_secs, 0)
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
    hset_batch_dim(c, prefix, ids, DIM);
}

/// Same as [`hset_batch`] but with an explicit dimension, for tests that use
/// a bigger vector width than the file-level `DIM` constant (16) -- e.g. the
/// RSS proxy test, which needs a large-enough dataset for the HNSW+TQ codes
/// to actually dominate the process's resident set.
fn hset_batch_dim(c: &mut Client, prefix: &str, ids: &[u32], dim: usize) {
    let cmds: Vec<Vec<Vec<u8>>> = ids
        .iter()
        .map(|&i| {
            let key = format!("{prefix}{i}");
            vec![
                b"HSET".to_vec(),
                key.into_bytes(),
                b"vec".to_vec(),
                fixture_vec(i, dim),
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

/// Round 2 (WS3): idle now routes straight to the COLD (`unloaded_segments`)
/// tier, not the WARM tier -- WARM (`--segment-warm-after`, age-only) does
/// not reduce RSS (see CHANGELOG "known limitation" from round 1); COLD
/// drops everything in-memory and is what actually frees memory. This test
/// supersedes the round-1 version, which polled `warm_segments` (now stays 0
/// for a purely-idle transition -- see the doc comment on
/// `VectorIndex::try_warm_transitions_idle`).
#[test]
#[ignore] // Spawns a real server process; run explicitly with `-- --ignored`.
fn hot_segment_idle_unloads_to_cold_and_preserves_recall() {
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

    // Baseline: HOT segment present with an exact-rerank sidecar, nothing in
    // WARM or COLD yet. num_docs must already be correct here.
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
    assert_eq!(ft_info_int(&mut c, idx, "unloaded_segments"), 0);
    assert_eq!(
        ft_info_int(&mut c, idx, "num_docs"),
        90,
        "num_docs must be correct pre-transition"
    );

    let probe = fixture_vec(7, DIM);
    let (key_before, dist_before) = search_top1(&mut c, idx, &probe);
    assert_eq!(key_before, format!("{idx}:7"), "self-match must find id 7");
    assert!(
        dist_before < 1e-3,
        "exact self-match distance should be ~0 pre-transition, got {dist_before}"
    );

    // Idle out the segment: stop querying for longer than
    // --engine-offload-idle-secs (2s) and let the warm-check tick (adapted
    // down to ~1s by the idle threshold, see event_loop.rs) demote it
    // straight to COLD.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if ft_info_int(&mut c, idx, "unloaded_segments") >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "segment never transitioned to the COLD tier within 30s"
        );
        std::thread::sleep(Duration::from_millis(500));
    }

    // Post-unload: HOT is gone, WARM never got touched (idle bypasses it
    // entirely and goes straight to COLD), COLD holds the segment.
    assert_eq!(
        ft_info_int(&mut c, idx, "graph_segments"),
        0,
        "HOT segment should be gone after the unload transition"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "warm_segments"),
        0,
        "idle transitions bypass WARM entirely -- straight to COLD"
    );
    assert_eq!(ft_info_int(&mut c, idx, "unloaded_segments"), 1);
    assert_eq!(
        ft_info_int(&mut c, idx, "unloaded_segments_with_exact_rerank"),
        1,
        "the COLD stub must remember it had a sidecar, without reloading"
    );
    // num_docs must stay correct with the only segment fully unloaded --
    // this is the num_docs regression a naive COLD implementation would
    // reintroduce (the same bug fixed for WARM in round 1).
    assert_eq!(
        ft_info_int(&mut c, idx, "num_docs"),
        90,
        "num_docs must stay correct while the segment is COLD (unloaded)"
    );

    // Touch it: FT.SEARCH must transparently reload the COLD segment,
    // single-flight, synchronously, promoting it into WARM.
    let (key_after, dist_after) = search_top1(&mut c, idx, &probe);
    assert_eq!(
        key_after, key_before,
        "top-1 result must be unchanged across the HOT->COLD->reload round trip"
    );
    assert!(
        dist_after < 1e-3,
        "exact self-match distance should still be ~0 after reload \
         (sidecar preserved through the COLD stub), got {dist_after}"
    );

    // After the touch, the segment must have been promoted COLD -> WARM.
    assert_eq!(
        ft_info_int(&mut c, idx, "unloaded_segments"),
        0,
        "COLD segment must be promoted out of `unloaded` on first touch"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "warm_segments"),
        1,
        "reloaded segment lands in WARM (mirrors the age-based tier's destination)"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "warm_segments_with_exact_rerank"),
        1,
        "reload must restore the sidecar, not silently fall back to ADC-only"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "num_docs"),
        90,
        "num_docs must still be correct after reload"
    );
}

/// Adversarial review #1 (CRITICAL): a HDEL that lands while a segment is
/// COLD must not resurrect the doc once the segment reloads. Before this
/// fix, `tombstone_key_in_index` never walked `warm`/`unloaded`, so the
/// delete was silently dropped -- the deleted doc would resurface (and
/// `num_docs` would over-count) the moment a search touched the index again.
#[test]
#[ignore] // Spawns a real server process; run explicitly with `-- --ignored`.
fn hdel_during_cold_does_not_resurrect() {
    let port = unique_port();
    let dir = unique_dir("s2");
    let idx = "colddelidx";

    let _guard = spawn_moon(port, &dir, 2);
    let mut c = wait_ready(port);

    ft_create(&mut c, idx, DIM, 100);
    let ids: Vec<u32> = (0..30).collect();
    hset_batch(&mut c, &format!("{idx}:"), &ids);
    ft_compact(&mut c, idx);

    assert_eq!(ft_info_int(&mut c, idx, "graph_segments"), 1);
    assert_eq!(ft_info_int(&mut c, idx, "num_docs"), 30);

    let probe = fixture_vec(7, DIM);
    let (key_before, dist_before) = search_top1(&mut c, idx, &probe);
    assert_eq!(key_before, format!("{idx}:7"), "self-match must find id 7");
    assert!(dist_before < 1e-3, "exact self-match pre-delete");

    // Idle the segment straight to COLD.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if ft_info_int(&mut c, idx, "unloaded_segments") >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "segment never transitioned to COLD within 30s"
        );
        std::thread::sleep(Duration::from_millis(500));
    }

    // HDEL (via DEL on the whole hash key) while the segment is COLD --
    // must reach the stub without forcing a reload.
    let r = c.cmd(&[b"DEL", format!("{idx}:7").as_bytes()]);
    assert_eq!(r, V::Int(1), "DEL must report the key existed");

    // num_docs must reflect the delete immediately -- no reload triggered by
    // DEL alone (the tombstone is queued in the COLD stub, see
    // `UnloadedSegment::mark_deleted_by_key_hash`).
    assert_eq!(ft_info_int(&mut c, idx, "unloaded_segments"), 1);
    assert_eq!(
        ft_info_int(&mut c, idx, "num_docs"),
        29,
        "num_docs must drop immediately even while the segment stays COLD"
    );

    // Touch it: triggers the reload. The deleted doc must NOT resurrect.
    let (key_after, _dist_after) = search_top1(&mut c, idx, &probe);
    assert_ne!(
        key_after,
        format!("{idx}:7"),
        "deleted doc must not resurrect after a COLD -> reload round trip"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "unloaded_segments"),
        0,
        "reload promotes COLD -> WARM"
    );
    assert_eq!(ft_info_int(&mut c, idx, "warm_segments"), 1);
    assert_eq!(
        ft_info_int(&mut c, idx, "num_docs"),
        29,
        "num_docs must remain correct after reload"
    );
}

/// Same shape as [`hdel_during_cold_does_not_resurrect`] but for the WARM
/// tier directly (idle disabled -- only the age criterion fires, so the
/// segment demotes to WARM and is never touched by the COLD path at all).
/// `WarmSearchSegment` previously had no tombstone mechanism whatsoever; a
/// HDEL against an already-WARM segment was a silent no-op.
#[test]
#[ignore] // Spawns a real server process; run explicitly with `-- --ignored`.
fn hdel_during_warm_does_not_resurrect() {
    let port = unique_port();
    let dir = unique_dir("s3");
    let idx = "warmdelidx";

    let _guard = spawn_moon_warm_only(port, &dir, 2);
    let mut c = wait_ready(port);

    ft_create(&mut c, idx, DIM, 100);
    let ids: Vec<u32> = (0..30).collect();
    hset_batch(&mut c, &format!("{idx}:"), &ids);
    ft_compact(&mut c, idx);

    assert_eq!(ft_info_int(&mut c, idx, "graph_segments"), 1);

    let probe = fixture_vec(7, DIM);
    let (key_before, dist_before) = search_top1(&mut c, idx, &probe);
    assert_eq!(key_before, format!("{idx}:7"));
    assert!(dist_before < 1e-3);

    // Age it to WARM (idle disabled -- only the age criterion can fire).
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if ft_info_int(&mut c, idx, "warm_segments") >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "segment never transitioned to WARM within 30s"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    assert_eq!(
        ft_info_int(&mut c, idx, "unloaded_segments"),
        0,
        "idle is disabled here -- must never visit COLD"
    );

    // HDEL while WARM -- must take effect immediately against the resident
    // WarmSearchSegment (its own interior tombstone set), no reload needed.
    let r = c.cmd(&[b"DEL", format!("{idx}:7").as_bytes()]);
    assert_eq!(r, V::Int(1));

    assert_eq!(
        ft_info_int(&mut c, idx, "num_docs"),
        29,
        "num_docs must reflect the delete immediately against a resident WARM segment"
    );

    let (key_after, _dist_after) = search_top1(&mut c, idx, &probe);
    assert_ne!(
        key_after,
        format!("{idx}:7"),
        "deleted doc must not appear in WARM-tier search results"
    );
    assert_eq!(
        ft_info_int(&mut c, idx, "warm_segments"),
        1,
        "still WARM -- a delete alone doesn't force any tier change"
    );
}

/// RSS proof: a real, measurably-sized dataset (large enough that the
/// HNSW+TQ codes dominate the process's resident set, unlike the tiny
/// DIM=16/90-vector fixture used above) must show a REAL RSS drop once the
/// segment idles into COLD -- unlike the round-1 WARM tier, which measured
/// flat-to-+1.1% (see CHANGELOG). This is the process-level proxy requested
/// alongside the FT.INFO counter checks above.
#[test]
#[ignore] // Spawns a real server process; run explicitly with `-- --ignored`.
fn cold_unload_reduces_process_rss() {
    // Adversarial review #5 tightened the assertion below to a 15% floor
    // (was a plain `<`). At the ORIGINAL 256d/3,000-vector fixture the hot
    // tier (codes + graph + f16 sidecar) is only a few MB against a ~99MB
    // process baseline (runtime/allocator/etc.) -- nowhere near 15% of RSS
    // even when fully freed, so the tightened assertion failed even with
    // COLD correctly freeing 100% of its own data. Scaled up so the hot
    // tier is large enough (~35+ MB) relative to the baseline for a 15%
    // floor to be a meaningful, reliably-passing signal instead of a flaky
    // one -- still small enough to compact well under this test's 90s
    // deadline.
    const BIG_DIM: usize = 768;
    const N: u32 = 15000;

    let port = unique_port();
    let dir = unique_dir("rss");
    let idx = "rssidx";

    let guard = spawn_moon(port, &dir, 2);
    let mut c = wait_ready(port);

    ft_create(&mut c, idx, BIG_DIM, 20000); // COMPACT_THRESHOLD > N: no auto-compact
    let ids: Vec<u32> = (0..N).collect();
    // Batch the HSETs to keep the pipeline reasonable.
    for chunk in ids.chunks(200) {
        hset_batch_dim(&mut c, &format!("{idx}:"), chunk, BIG_DIM);
    }
    ft_compact(&mut c, idx);
    // Background compaction (see CLAUDE.md "FT background compaction"):
    // FT.COMPACT returns OK immediately but the HNSW build runs on a worker
    // thread -- poll rather than assert immediately. 15,000 x 768d measured
    // >90s on a `release-fast` (not fully-optimized `release`) binary under
    // this dev machine's load. Bumped 240s -> 600s after observing this
    // exact test blow a 240s deadline while OTHER heavy `cargo test`/`cargo
    // build` jobs were running concurrently on the same (shared, load-average
    // 6-10) dev machine -- an isolated run (nothing else competing for CPU)
    // reliably produces a HOT segment in single-digit seconds, so 240s is
    // fine in isolation but not resilient to this machine's background
    // contention. 600s is a generous ceiling since this test is `--ignored`
    // (not part of the default `cargo test` run) and correctness matters
    // more than speed here.
    let compact_deadline = Instant::now() + Duration::from_secs(600);
    loop {
        if ft_info_int(&mut c, idx, "graph_segments") >= 1 {
            break;
        }
        assert!(
            Instant::now() < compact_deadline,
            "background compaction never produced a HOT segment within 600s"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(ft_info_int(&mut c, idx, "graph_segments"), 1);
    assert_eq!(ft_info_int(&mut c, idx, "num_docs"), N as i64);
    // Idle-window race guard: `spawn_moon` uses a 2s idle threshold, and
    // idleness is measured from segment creation (compaction), not from
    // this poll loop returning. If the compaction itself (plus this poll's
    // own round-trips) ever takes longer than 2s -- entirely plausible
    // under this dev machine's background load -- the segment could already
    // be eligible for (or mid-) a COLD transition by the time RSS_BEFORE is
    // sampled below, silently corrupting the before/after delta into a
    // false pass or a spurious failure. Fail loudly instead.
    assert_eq!(
        ft_info_int(&mut c, idx, "unloaded_segments"),
        0,
        "segment already went COLD before RSS_BEFORE could be sampled -- \
         idle-window race (compaction took longer than the 2s idle \
         threshold); this corrupts the RSS delta measurement below"
    );

    let rss_kb = |pid: u32| -> u64 {
        let out = Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .expect("run ps");
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(0)
    };

    let pid = guard.child.id();
    std::thread::sleep(Duration::from_millis(500)); // let jemalloc settle post-compact
    let rss_before = rss_kb(pid);
    assert!(rss_before > 0, "must be able to read server RSS via ps");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if ft_info_int(&mut c, idx, "unloaded_segments") >= 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "segment never transitioned to COLD within 30s"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    // Let jemalloc's background reclaim thread (1s dirty-page decay, see
    // CLAUDE.md's `_RJEM_MALLOC_CONF`) return freed pages to the OS.
    std::thread::sleep(Duration::from_secs(3));
    let rss_after_unload = rss_kb(pid);

    // Tightened per adversarial review #5: a plain `<` passes even for a
    // regression that only frees the HNSW graph (leaving codes+sidecar
    // resident) or otherwise partially unloads. The production 40K x 768d
    // SQ8 measurement (CHANGELOG) showed a real -26.2% drop; require at
    // least -15% here so a partial-free regression fails loudly while still
    // tolerating this test's smaller (256d x 3,000-vector) CI-scale fixture
    // and jemalloc arena noise.
    let threshold = (rss_before as f64 * 0.85) as i64;
    assert!(
        (rss_after_unload as i64) < threshold,
        "RSS must drop by at least 15% after COLD unload: before={rss_before}KB \
         after={rss_after_unload}KB threshold={threshold}KB (round-1 WARM measured \
         a FLAT/+1.1% non-result here -- COLD must actually free the \
         codes+graph+sidecar, not just e.g. the graph alone)"
    );

    // Touch it: reload restores full residency (roughly back to pre-unload).
    let probe = fixture_vec(1, BIG_DIM);
    let _ = search_top1(&mut c, idx, &probe);
    assert_eq!(ft_info_int(&mut c, idx, "unloaded_segments"), 0);
    assert_eq!(ft_info_int(&mut c, idx, "warm_segments"), 1);
    assert_eq!(ft_info_int(&mut c, idx, "num_docs"), N as i64);

    drop(guard);
}
