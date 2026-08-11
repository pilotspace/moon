//! Wave-1 durability recovery matrix (#54, #452): e2e legs for the
//! 2026-08 durability fixes.
//!
//! The unit-level red/green tests live next to their fixes
//! (`persistence::aof::rewrite_overflow`, `persistence::wal_v3::replay`,
//! `replication::reason_del`); this suite adds the end-to-end shape the
//! single-client crash matrix cannot produce: **pipelined write pressure
//! against a live rewrite fold**. A serialized `redis-cli` loop never gets
//! more than one append in flight, so the 10k writer channel never fills —
//! raw-socket pipelining in 5k-command bursts during a BGREWRITEAOF fold is
//! what historically overflowed the channel and silently dropped acked
//! writes (#452.1).
//!
//! Crash-suite conventions: real binaries (`MOON_BIN` to pin), SIGKILL,
//! `--disk-free-min-pct 0`, `#[ignore]` (run with `-- --ignored`).

mod common;

use std::io::{Read, Write};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-recovery-w1-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

fn start_moon(port: u16, dir: &std::path::Path, shards: usize) -> Child {
    let port_s = port.to_string();
    let shards_s = shards.to_string();
    let mut cmd = Command::new(common::find_moon_binary());
    cmd.args([
        "--port",
        &port_s,
        "--shards",
        &shards_s,
        "--appendonly",
        "yes",
        "--appendfsync",
        "everysec",
        "--disk-free-min-pct",
        "0",
        // Auto-rewrite must stay OFF: a post-flood auto fold would capture
        // the dropped keys from live memory into a fresh base and heal the
        // loss before the SIGKILL — masking exactly the bug under test.
        "--auto-aof-rewrite-percentage",
        "0",
    ])
    .arg("--dir")
    .arg(dir);
    cmd.stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (cargo build first; MOON_BIN to override)")
}

fn cli(port: u16, args: &[&str]) -> String {
    let mut full = vec!["-p".to_string(), port.to_string()];
    full.extend(args.iter().map(|s| s.to_string()));
    let out = Command::new("redis-cli")
        .args(&full)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("redis-cli");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Send `count` pipelined `SET <prefix><start+i> v` commands in one burst and
/// strictly count `+OK` acks — any non-OK reply fails the harness loudly so
/// error replies can never masquerade as (or mask) lost writes.
fn pipeline_set_burst(
    sock: &mut std::net::TcpStream,
    prefix: &str,
    start: usize,
    count: usize,
    value: &str,
) -> usize {
    let mut batch = Vec::with_capacity(count * (64 + value.len()));
    for i in start..start + count {
        let key = format!("{prefix}{i}");
        batch.extend_from_slice(
            format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            )
            .as_bytes(),
        );
    }
    sock.write_all(&batch).expect("pipeline write");

    let mut acked = 0usize;
    let mut buf = [0u8; 65536];
    let mut pending: Vec<u8> = Vec::new();
    while acked < count {
        let n = sock.read(&mut buf).expect("pipeline read");
        assert!(n > 0, "server closed the connection mid-pipeline");
        pending.extend_from_slice(&buf[..n]);
        while let Some(pos) = pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = pending.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            assert_eq!(
                line, "+OK",
                "harness requires every pipelined SET to be acked +OK, got {line:?}"
            );
            acked += 1;
        }
    }
    acked
}

/// #452.1 e2e: a BGREWRITEAOF fold racing sustained pipelined writes must
/// not lose a single acked SET across SIGKILL + recovery.
///
/// Shape: preload enough keys that the fold's snapshot+RDB write takes real
/// time, then flood >channel-capacity pipelined bursts from two connections
/// while BGREWRITEAOF folds. During the fold the writer thread is out of its
/// recv loop, so a 30k in-flight burst overflows the 10k channel — pre-fix
/// the overflow either failed loud (-MOONERR on the local/inline leg, which
/// this harness's strict +OK assert catches) or dropped silently (the
/// cross-shard fire-and-forget leg, which the exact DBSIZE assert catches).
/// Merge-base A/B on the dev host: main failed 1-2 of 3 runs (5.6k drops
/// observed per hit); the fixed binary is stably green. The race is
/// PROBABILISTIC on fast disks (the fold can win against the burst
/// boundaries) — the deterministic drop/spill contract is pinned unit-level
/// in `persistence::aof::rewrite_overflow`; this test is the whole-stack
/// regression net and the recovery-exactness proof. The 2.5s quiesce before
/// SIGKILL keeps the everysec window out of the equation: every counted ack
/// is durability-due by kill time.
#[test]
#[ignore] // Spawns real binaries + SIGKILL; run explicitly (crash-suite convention).
fn rewrite_under_pipelined_load_loses_no_acked_writes() {
    // All keys carry the {x} hash tag: the whole load lands on ONE shard, so
    // its writer channel (10k) takes the full append pressure instead of
    // having it halved across shards. Preload sizing measured on the dev
    // host: 400k x 256B ≈ 110MB RDB ≈ 200-400ms fold — while a pipelined
    // client sustains >500k SET/s, so the 60k flood below lands well inside
    // the fold window and stacks 6x the channel capacity.
    // BURST must exceed the writer channel capacity (10k): a round-trip
    // gated client can never have more appends in flight than one burst, so
    // a 10k burst tops out at exactly the channel cap and can never
    // overflow it. 30k in flight against a non-draining (folding) writer
    // guarantees ~20k channel-overflow appends per mid-fold burst.
    const PRELOAD: usize = 600_000;
    const FLOOD_BURSTS: usize = 7;
    const BURST: usize = 30_000;

    let dir = unique_dir("rewrite-flood");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut child, port) = common::spawn_listening(|p| start_moon(p, &dir, 2));

    let mut sock = std::net::TcpStream::connect(("127.0.0.1", port)).expect("connect");
    sock.set_nodelay(true).ok();

    // Preload: make the fold's snapshot big enough to take real time.
    let preload_val = "x".repeat(384);
    let mut acked = 0usize;
    for burst in 0..(PRELOAD / BURST) {
        acked += pipeline_set_burst(&mut sock, "pre:{x}:", burst * BURST, BURST, &preload_val);
    }
    assert_eq!(acked, PRELOAD);

    // Flood continuously from a dedicated thread and fire BGREWRITEAOF
    // mid-flood: starting the flood first removes the start-latency
    // variance that otherwise lets the fold win the race before the first
    // burst lands (observed: a subprocess redis-cli + connect delay is
    // enough for a ~300ms fold to finish unpressured).
    // TWO flood connections: shard-round-robin accept places one on each
    // shard, so one of them is always CROSS-shard from {x}'s owner — the
    // fire-and-forget/bounded-blocking SPSC leg where the pre-fix drop was
    // silent-or-MOONERR, independent of accept-order luck.
    let flood_a = std::thread::spawn(move || {
        let mut fsock = std::net::TcpStream::connect(("127.0.0.1", port)).expect("flood connect");
        fsock.set_nodelay(true).ok();
        let mut acked = 0usize;
        for burst in 0..FLOOD_BURSTS {
            acked += pipeline_set_burst(&mut fsock, "ka:{x}:", burst * BURST, BURST, "v");
        }
        acked
    });
    let flood_b = std::thread::spawn(move || {
        let mut fsock = std::net::TcpStream::connect(("127.0.0.1", port)).expect("flood connect");
        fsock.set_nodelay(true).ok();
        let mut acked = 0usize;
        for burst in 0..FLOOD_BURSTS {
            acked += pipeline_set_burst(&mut fsock, "kb:{x}:", burst * BURST, BURST, "v");
        }
        acked
    });
    std::thread::sleep(Duration::from_millis(150));
    let reply = cli(port, &["BGREWRITEAOF"]);
    assert!(
        reply.contains("started") || reply.contains("in progress"),
        "BGREWRITEAOF must start, got {reply:?}"
    );
    let flood_acked =
        flood_a.join().expect("flood thread a") + flood_b.join().expect("flood thread b");
    assert_eq!(flood_acked, 2 * FLOOD_BURSTS * BURST);

    // Diagnostics (not an assert: older binaries lack the field): how much
    // the rewrite overflow actually spilled this run.
    let spilled = cli(port, &["INFO", "persistence"])
        .lines()
        .find(|l| l.starts_with("aof_rewrite_overflow_spilled:"))
        .map(|l| l.to_string())
        .unwrap_or_else(|| "aof_rewrite_overflow_spilled:<absent>".into());
    eprintln!("[recovery_matrix_w1] {spilled}");

    // Quiesce past the everysec window + post-fold drains, then crash.
    std::thread::sleep(Duration::from_millis(2500));
    common::sigkill(&mut child);

    let (mut child2, port2) = common::spawn_listening(|p| start_moon(p, &dir, 2));
    let dbsize: usize = cli(port2, &["DBSIZE"])
        .parse()
        .expect("DBSIZE must be numeric after recovery");
    let expected = PRELOAD + 2 * FLOOD_BURSTS * BURST;
    // Spot-check the flood tail (the records most likely to sit in the
    // overflow window) before the aggregate assert, for a sharper failure.
    for i in [0usize, BURST * 3 + 17, FLOOD_BURSTS * BURST - 1] {
        for prefix in ["ka", "kb"] {
            assert_eq!(
                cli(port2, &["GET", &format!("{prefix}:{{x}}:{i}")]),
                "v",
                "acked flood key {prefix}:{{x}}:{i} lost across rewrite + SIGKILL recovery"
            );
        }
    }
    assert_eq!(
        dbsize,
        expected,
        "acked writes lost across rewrite-under-load + SIGKILL recovery \
         (missing {} of {expected})",
        expected - dbsize.min(expected)
    );
    common::sigkill(&mut child2);
    let _ = std::fs::remove_dir_all(&dir);
}
