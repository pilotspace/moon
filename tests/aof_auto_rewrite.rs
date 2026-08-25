//! #433: automatic AOF rewrite (`auto-aof-rewrite-percentage` /
//! `auto-aof-rewrite-min-size`, Redis parity) + un-gated multi-shard
//! BGREWRITEAOF.
//!
//! The AOF is append-only; before #433 nothing ever compacted it — it grew
//! with write volume, not dataset size (observed: 4.8 GB appendonlydir for a
//! 2.43 GB dataset, +1 GB/day), ending in `MOONERR diskfull`. And on the
//! default multi-shard config the manual escape hatch was gated off too
//! (`BGREWRITEAOF` refused unless `--experimental-per-shard-rewrite`).
//!
//! These tests pin the new contract:
//!   1. Multi-shard `BGREWRITEAOF` works WITHOUT the experimental flag (the
//!      per-shard fan-out is the default; crash matrix green).
//!   2. The AOF rewrites ITSELF when it grows `percentage`% over its
//!      post-rewrite size and is at least `min-size` bytes — no operator
//!      action, observable as a seq>1 base file replacing the old generation.
//!   3. `auto-aof-rewrite-percentage 0` disables the trigger (Redis parity).
//!   4. Exactness: an auto rewrite must not drop or double-apply acked
//!      INCRs across the rewrite boundary (SIGKILL + recovery, same
//!      non-idempotent-counter technique as the crash matrix).
//!   5. `INFO persistence` reports real `aof_enabled` / `aof_base_size` /
//!      `aof_current_size` instead of hardcoded zeros (#432).
//!
//! Spawns real server binaries; needs `redis-cli` on PATH. Every server gets
//! `--disk-free-min-pct 0` (crash-harness convention — see
//! gotcha-diskfull-guard-gutted-crash-tests) and every INCR reply is parsed
//! strictly so an error reply fails loudly instead of under-counting.

mod common;

use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn unique_dir(suffix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-aof-auto-rewrite-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

/// Spawn moon with the given shard count and extra flags. Deliberately does
/// NOT pass `--experimental-per-shard-rewrite`: the un-gated default is part
/// of the contract under test.
fn start_moon(port: u16, dir: &std::path::Path, shards: usize, extra: &[&str]) -> Child {
    let port_s = port.to_string();
    let shards_s = shards.to_string();
    let mut args: Vec<&str> = vec![
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
    ];
    args.extend_from_slice(extra);
    let mut cmd = Command::new(common::find_moon_binary());
    cmd.args(&args).arg("--dir").arg(dir);
    cmd.stdout(std::fs::File::create(dir.join("moon.stdout.log")).expect("stdout log"))
        .stderr(std::fs::File::create(dir.join("moon.stderr.log")).expect("stderr log"))
        .spawn()
        .expect("spawn moon (cargo build first; MOON_BIN to override)")
}

fn spawn_listening(
    dir: &std::path::Path,
    shards: usize,
    extra: &[&str],
) -> (common::ServerGuard, u16) {
    common::spawn_listening_guarded(|port| start_moon(port, dir, shards, extra))
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

/// Strict INCR: panics on a non-numeric reply so error replies (diskfull,
/// readonly, …) can never masquerade as lost writes.
fn incr(port: u16, key: &str) -> i64 {
    let reply = cli(port, &["INCR", key]);
    reply.parse().unwrap_or_else(|_| {
        panic!("INCR {key} not acked with a number — harness cannot count it: {reply:?}")
    })
}

/// True when a base RDB with seq > `min_seq_exclusive` exists anywhere under
/// appendonlydir (covers both the TopLevel `appendonlydir/moon.aof.N.base.rdb`
/// and PerShard `appendonlydir/shard-K/moon.aof.N.base.rdb` layouts).
fn max_base_seq(dir: &std::path::Path) -> u64 {
    fn scan(p: &std::path::Path, max: &mut u64) {
        let Ok(entries) = std::fs::read_dir(p) else {
            return;
        };
        for e in entries.flatten() {
            let path = e.path();
            if path.is_dir() {
                scan(&path, max);
            } else if let Some(name) = path.file_name().and_then(|n| n.to_str())
                && let Some(rest) = name.strip_prefix("moon.aof.")
                && let Some(seq_str) = rest.strip_suffix(".base.rdb")
                && let Ok(seq) = seq_str.parse::<u64>()
            {
                *max = (*max).max(seq);
            }
        }
    }
    let mut max = 0;
    scan(&dir.join("appendonlydir"), &mut max);
    max
}

fn wait_for_base_seq_above(dir: &std::path::Path, floor: u64, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if max_base_seq(dir) > floor {
            return true;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

/// Layout-aware "a rewrite compacted the AOF" tracker. Manifest layouts
/// (monoio: both shard counts; tokio: shards>=2) advance to a seq>1 base
/// file — visible forever after. The tokio TopLevel writer instead compacts
/// its legacy flat `appendonly.aof` IN PLACE, and the file starts regrowing
/// immediately — so the shrink-below-high-water signal is only observable
/// while it happens. Callers must therefore `sample()` DURING the write
/// stream too, not only after it.
struct CompactionTracker {
    legacy: std::path::PathBuf,
    high_water: u64,
    seen: bool,
}

impl CompactionTracker {
    fn new(dir: &std::path::Path) -> Self {
        Self {
            legacy: dir.join("appendonly.aof"),
            high_water: 0,
            seen: false,
        }
    }

    fn sample(&mut self, dir: &std::path::Path) -> bool {
        if self.seen {
            return true;
        }
        if max_base_seq(dir) > 1 {
            self.seen = true;
            return true;
        }
        if let Ok(md) = std::fs::metadata(&self.legacy) {
            let len = md.len();
            if self.high_water > 1024 && len + 1024 < self.high_water {
                // Shrank well below the high-water mark: in-place rewrite.
                self.seen = true;
                return true;
            }
            self.high_water = self.high_water.max(len);
        }
        false
    }

    fn wait(&mut self, dir: &std::path::Path, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.sample(dir) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    }
}

/// One-shot convenience over [`CompactionTracker`]; unused since the
/// mid-stream-sampling refactor but kept as the documented entry point for
/// future post-hoc waits.
#[allow(dead_code)]
fn wait_for_compaction(dir: &std::path::Path, timeout: Duration) -> bool {
    CompactionTracker::new(dir).wait(dir, timeout)
}

/// Contract 1: multi-shard BGREWRITEAOF works by DEFAULT (no experimental
/// flag) and compacts — plus exactness across SIGKILL+recovery.
#[test]
#[ignore] // Spawns real binaries + SIGKILL; run explicitly (crash-suite convention).
fn manual_bgrewriteaof_ungated_on_multi_shard() {
    const N: i64 = 200;
    let dir = unique_dir("ungated");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut child, port) = spawn_listening(&dir, 2, &[]);

    for _ in 0..N {
        incr(port, "cnt:{a}");
        incr(port, "cnt:{b}");
    }
    let reply = cli(port, &["BGREWRITEAOF"]);
    assert!(
        reply.contains("started"),
        "multi-shard BGREWRITEAOF must be un-gated by default, got: {reply:?}"
    );
    assert!(
        wait_for_base_seq_above(&dir, 1, Duration::from_secs(10)),
        "no compacted (seq>1) base appeared after manual BGREWRITEAOF"
    );
    // Post-rewrite writes + quiesce past the everysec window, then crash.
    for _ in 0..50 {
        incr(port, "cnt:{a}");
    }
    std::thread::sleep(Duration::from_millis(2000));
    child.kill_now();

    let (mut child2, port2) = spawn_listening(&dir, 2, &[]);
    assert_eq!(cli(port2, &["GET", "cnt:{a}"]), (N + 50).to_string());
    assert_eq!(cli(port2, &["GET", "cnt:{b}"]), N.to_string());
    child2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Contract 2 + 4 (multi-shard): the AOF auto-rewrites once it exceeds the
/// growth threshold, with INCR-exact recovery across SIGKILL.
#[test]
#[ignore] // Spawns real binaries + SIGKILL; run explicitly (crash-suite convention).
fn auto_rewrite_triggers_on_growth_multi_shard() {
    const N: i64 = 400;
    let dir = unique_dir("auto2");
    std::fs::create_dir_all(&dir).unwrap();
    // Tiny thresholds so ~800 INCR records (~30 bytes each) cross quickly.
    let (mut child, port) = spawn_listening(
        &dir,
        2,
        &[
            "--auto-aof-rewrite-min-size",
            "4096",
            "--auto-aof-rewrite-percentage",
            "50",
        ],
    );

    for _ in 0..N {
        incr(port, "cnt:{a}");
        incr(port, "cnt:{b}");
    }
    assert!(
        wait_for_base_seq_above(&dir, 1, Duration::from_secs(20)),
        "auto rewrite never fired: no seq>1 base under {} (incr grew past \
         min-size+percentage but nothing compacted)",
        dir.display()
    );
    // Keep writing across/after the rewrite, quiesce, crash, recover exact.
    for _ in 0..100 {
        incr(port, "cnt:{a}");
    }
    std::thread::sleep(Duration::from_millis(2000));
    child.kill_now();

    let (mut child2, port2) = spawn_listening(&dir, 2, &[]);
    assert_eq!(
        cli(port2, &["GET", "cnt:{a}"]),
        (N + 100).to_string(),
        "auto rewrite dropped or double-applied acked INCRs for cnt:{{a}}"
    );
    assert_eq!(cli(port2, &["GET", "cnt:{b}"]), N.to_string());
    child2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Contract 2 (single-shard / TopLevel layout): same trigger, legacy rewrite
/// path.
#[test]
#[ignore] // Spawns real binaries + SIGKILL; run explicitly (crash-suite convention).
fn auto_rewrite_triggers_on_growth_single_shard() {
    const N: i64 = 500;
    let dir = unique_dir("auto1");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut child, port) = spawn_listening(
        &dir,
        1,
        &[
            "--auto-aof-rewrite-min-size",
            "4096",
            "--auto-aof-rewrite-percentage",
            "50",
        ],
    );
    // Sample DURING the stream: the tokio TopLevel layout compacts its flat
    // file in place mid-stream and regrows it — a post-hoc poll misses it.
    let mut tracker = CompactionTracker::new(&dir);
    for i in 0..N {
        incr(port, "cnt:solo");
        if i % 10 == 0 {
            tracker.sample(&dir);
        }
    }
    assert!(
        tracker.wait(&dir, Duration::from_secs(20)),
        "auto rewrite never fired on the shards=1 layout (no seq>1 base and \
         no in-place shrink of appendonly.aof)"
    );
    std::thread::sleep(Duration::from_millis(2000));
    child.kill_now();
    let (mut child2, port2) = spawn_listening(&dir, 1, &[]);
    assert_eq!(cli(port2, &["GET", "cnt:solo"]), N.to_string());
    child2.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Contract 3: percentage 0 disables the trigger entirely.
#[test]
#[ignore] // Spawns real binaries; run explicitly (crash-suite convention).
fn auto_rewrite_percentage_zero_disables() {
    let dir = unique_dir("disabled");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut child, port) = spawn_listening(
        &dir,
        2,
        &[
            "--auto-aof-rewrite-min-size",
            "1024",
            "--auto-aof-rewrite-percentage",
            "0",
        ],
    );
    for _ in 0..300 {
        incr(port, "cnt:{a}");
    }
    // Well past several monitor ticks: nothing may compact.
    std::thread::sleep(Duration::from_millis(4000));
    assert_eq!(
        max_base_seq(&dir),
        1,
        "auto-aof-rewrite-percentage 0 must disable automatic rewrites"
    );
    child.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Contract 5 (#432): INFO persistence must report the real AOF state.
#[test]
#[ignore] // Spawns real binaries; run explicitly (crash-suite convention).
fn info_persistence_reports_real_aof_fields() {
    let dir = unique_dir("info");
    std::fs::create_dir_all(&dir).unwrap();
    let (mut child, port) = spawn_listening(&dir, 2, &[]);
    for _ in 0..50 {
        incr(port, "cnt:{a}");
    }
    // everysec: give the writer a flush window before sampling sizes.
    std::thread::sleep(Duration::from_millis(1500));
    let info = cli(port, &["INFO", "persistence"]);
    assert!(
        info.contains("aof_enabled:1"),
        "appendonly=yes must report aof_enabled:1 (#432), got:\n{info}"
    );
    assert!(
        info.contains("aof_rewrite_in_progress:0"),
        "no rewrite is running, got:\n{info}"
    );
    let current = info
        .lines()
        .find_map(|l| l.strip_prefix("aof_current_size:"))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or_else(|| panic!("missing/unparseable aof_current_size in:\n{info}"));
    let base = info
        .lines()
        .find_map(|l| l.strip_prefix("aof_base_size:"))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or_else(|| panic!("missing/unparseable aof_base_size in:\n{info}"));
    assert!(
        current > base,
        "50 INCRs were appended, aof_current_size ({current}) must exceed \
         aof_base_size ({base})"
    );
    child.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}
