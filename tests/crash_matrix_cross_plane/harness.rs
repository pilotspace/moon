//! Spawn/crash/restart harness + the curated config-cell matrix.
//!
//! Reuses `tests/common::{reserve_port, spawn_listening, find_moon_binary,
//! sigkill, wait_for_port_down}` (kernel M3 brief, Stage 1) instead of a
//! seventh hand-rolled copy of these primitives.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use crate::common;

/// One curated config cell. `label` feeds the `cross_plane_<config>_<cell>`
/// test-fn naming convention the brief specifies (avoids the `g1_` prefix
/// collision documented in `crash_recovery_graph_durability.rs`).
#[derive(Clone, Copy)]
pub struct Config {
    pub appendonly: bool,
    pub disk_offload: bool,
    pub shards: u32,
    pub label: &'static str,
}

impl Config {
    /// Production cell A — the config the brief calls out as "the config
    /// that matters most": checkpoint-backed, AOF durability, single shard.
    pub const PROD_S1: Config = Config {
        appendonly: true,
        disk_offload: true,
        shards: 1,
        label: "prod_s1",
    };
    /// Production cell B — same durability contract, 4 shards (the
    /// cross-shard axis for the production config).
    pub const PROD_S4: Config = Config {
        appendonly: true,
        disk_offload: true,
        shards: 4,
        label: "prod_s4",
    };
    /// Legacy cell A — no checkpoint/snapshot protocol at all
    /// (`segment_holds_plane_history` is the ONLY thing preventing WAL
    /// recycle from eating plane history in this mode). Brief: legacy cells
    /// run at shards=1 only ("no cross-shard novelty for this suite's
    /// purpose").
    pub const LEGACY_YES_S1: Config = Config {
        appendonly: true,
        disk_offload: false,
        shards: 1,
        label: "legacy_yes_s1",
    };
    /// Legacy cell B — `appendonly=no` skips the WAL writer entirely
    /// (`persistence_dir` is only constructed when `appendonly=="yes" ||
    /// save.is_some()`, `src/config.rs`), so NO plane has any durability
    /// contract here, not just KV. Assertions in this cell are deliberately
    /// "must survive a crash without corrupting/hanging", never "must
    /// recover data" — see the brief's "vacuous by design, not a bug" note.
    pub const LEGACY_NO_S1: Config = Config {
        appendonly: false,
        disk_offload: false,
        shards: 1,
        label: "legacy_no_s1",
    };
    /// Spot-check cell — disk-offload enabled but no durability backstop
    /// (`appendonly=no`, no `--save`): `disk_offload_spill_inert` in
    /// `src/config.rs` documents this as a loud no-op, not a crash, and
    /// `persistence_dir` is still None so the WAL writer never exists here
    /// either. Same "vacuous by design" contract as `LEGACY_NO_S1`.
    pub const SPOT_OFFLOAD_NO_S1: Config = Config {
        appendonly: false,
        disk_offload: true,
        shards: 1,
        label: "spot_offload_no_s1",
    };
    pub const SPOT_OFFLOAD_NO_S4: Config = Config {
        appendonly: false,
        disk_offload: true,
        shards: 4,
        label: "spot_offload_no_s4",
    };
    /// Spot-check cell — legacy mode at shards=4 (the brief's curation
    /// excludes legacy from the full plane×workload matrix at shards=4, but
    /// still wants 1-2 representative scenarios here).
    pub const SPOT_LEGACY_S4: Config = Config {
        appendonly: true,
        disk_offload: false,
        shards: 4,
        label: "spot_legacy_s4",
    };
    pub const SPOT_LEGACY_NO_S4: Config = Config {
        appendonly: false,
        disk_offload: false,
        shards: 4,
        label: "spot_legacy_no_s4",
    };

    fn appendonly_arg(&self) -> &'static str {
        if self.appendonly { "yes" } else { "no" }
    }

    fn disk_offload_arg(&self) -> &'static str {
        if self.disk_offload {
            "enable"
        } else {
            "disable"
        }
    }

    /// True for the two cells with NO durability contract on ANY plane
    /// (`persistence_dir` never constructed — see `LEGACY_NO_S1`'s doc).
    pub fn no_durability_contract(&self) -> bool {
        !self.appendonly
    }
}

// ---------------------------------------------------------------------------
// Spawn / crash / restart
// ---------------------------------------------------------------------------

pub fn unique_dir(suffix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "moon-xplane-{}-{}-{}",
        std::process::id(),
        suffix,
        nanos
    ))
}

/// Kills the wrapped child on drop (SIGKILL + waitpid via `common::sigkill`),
/// plus a best-effort `pkill -9 -f <dir>` backstop keyed on this test's
/// unique temp dir (never a bare port/process-name pattern — see the
/// SIGTERM+SO_REUSEPORT / leaked-busy-poller gotchas in CLAUDE.md).
pub struct ServerGuard {
    pub child: Child,
    dir_marker: String,
}

impl ServerGuard {
    pub fn new(child: Child, dir: &Path) -> Self {
        Self {
            child,
            dir_marker: dir.to_string_lossy().into_owned(),
        }
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            common::sigkill(&mut self.child);
        } else {
            let _ = self.child.wait();
        }
        let _ = Command::new("pkill")
            .args(["-9", "-f"])
            .arg(&self.dir_marker)
            .output();
    }
}

/// Spawn moon under `cfg`, with `extra` appended verbatim (WAL-size overrides
/// for the Pass-C kill point, `--maxmemory`/`--maxmemory-policy` for the
/// spilled-KV scenario, etc). Fresh `--dir` per test, `--disk-free-min-pct 0`
/// (DEL/FLUSH are write-flagged by the diskfull guard and this repo's dev
/// volumes routinely sit near the 5% line). Returns the guard and the port
/// actually bound (`spawn_listening` may respawn on a fresh port internally
/// after losing a bind race).
pub fn spawn_moon_on(dir: &Path, cfg: &Config, extra: &[&str]) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).expect("create test dir");
    let (child, port) = common::spawn_listening(|port| {
        let mut args: Vec<String> = vec![
            "--port".into(),
            port.to_string(),
            "--dir".into(),
            dir.to_string_lossy().into_owned(),
            "--shards".into(),
            cfg.shards.to_string(),
            "--appendonly".into(),
            cfg.appendonly_arg().into(),
            "--disk-offload".into(),
            cfg.disk_offload_arg().into(),
            "--disk-free-min-pct".into(),
            "0".into(),
        ];
        for &e in extra {
            args.push(e.into());
        }
        Command::new(common::find_moon_binary())
            .args(&args)
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
            .unwrap_or_else(|e| {
                panic!(
                    "spawn moon (dir={}) failed: {e}. Build with `cargo build \
                     --release` or set MOON_BIN.",
                    dir.display()
                )
            })
    });
    (ServerGuard::new(child, dir), port)
}

/// SIGKILL + wait for the port to actually stop accepting (SO_REUSEPORT
/// makes a plain bind-check useless).
pub fn crash(mut guard: ServerGuard, port: u16) {
    common::sigkill(&mut guard.child);
    drop(guard); // idempotent (already reaped) + pkill backstop
    common::wait_for_port_down(port);
}

// ---------------------------------------------------------------------------
// WAL v3 durability-wait primitives
// ---------------------------------------------------------------------------

/// Scan every shard's `wal-v3/` dir for `needle`. Used instead of a
/// per-shard variant because this suite deliberately does not pin write
/// routing to a specific shard index at shards=4 (hash-tag co-location picks
/// *some* shard, and the suite doesn't need to know which).
pub fn wait_for_wal_v3_bytes_any_shard(dir: &Path, num_shards: u32, needle: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if wal_v3_contains_bytes_any_shard(dir, num_shards, needle) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no shard's wal-v3/ dir under {} ever contained marker {:?} — \
             writes are not reaching the WAL (or --appendonly no, where no \
             WAL writer exists at all)",
            dir.display(),
            String::from_utf8_lossy(needle),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn wal_v3_contains_bytes_any_shard(dir: &Path, num_shards: u32, needle: &[u8]) -> bool {
    for shard_id in 0..num_shards {
        let wal_dir = dir.join(format!("shard-{shard_id}")).join("wal-v3");
        if let Ok(entries) = std::fs::read_dir(&wal_dir) {
            for entry in entries.flatten() {
                if let Ok(bytes) = std::fs::read(entry.path())
                    && bytes.windows(needle.len()).any(|w| w == needle)
                {
                    return true;
                }
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// AOF durability-wait primitives (KV plane, incl. vector-HSET — brief §1.2:
// "vector durability rides the same path as any other HSET, normal Command
// WAL/AOF replay"). Confirmed empirically on the VM: plain SET commands
// NEVER appear in `wal-v3`, only in the AOF incr file; `wal-v3` is
// exclusively graph/vector-index-lifecycle/WS/MQ/temporal EFFECT records.
// Layout differs by shard count (confirmed via a live `--shards 4` smoke
// spawn): shards==1 uses a FLAT `appendonlydir/moon.aof.1.incr.aof` (no
// `shard-0/` subdirectory at all); shards>1 uses
// `appendonlydir/shard-<N>/moon.aof.1.incr.aof` per shard. Scans both shapes
// unconditionally rather than branching on `num_shards` so a config change
// can never silently point this at the wrong layout.
// ---------------------------------------------------------------------------

pub fn wait_for_aof_bytes_any_shard(dir: &Path, needle: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if aof_contains_bytes_any_shard(dir, needle) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no AOF file under {}/appendonlydir ever contained marker {:?} — \
             writes are not reaching the AOF (or --appendonly no, where no \
             AOF writer exists at all)",
            dir.display(),
            String::from_utf8_lossy(needle),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Wait for EVERY needle in `needles` to appear (independently, possibly in
/// different files) under `dir/appendonlydir`. Use this instead of a single
/// concatenated needle when a command's arguments are logged as SEPARATE
/// RESP bulk strings (e.g. `SET key val` — the AOF never contains the
/// literal substring `"key=val"`, only `key` and `val` as distinct frames).
pub fn wait_for_aof_bytes_all_any_shard(dir: &Path, needles: &[&[u8]]) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if needles
            .iter()
            .all(|needle| aof_contains_bytes_any_shard(dir, needle))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no AOF file under {}/appendonlydir ever contained all of {:?} — \
             writes are not reaching the AOF (or --appendonly no, where no \
             AOF writer exists at all)",
            dir.display(),
            needles
                .iter()
                .map(|n| String::from_utf8_lossy(n).into_owned())
                .collect::<Vec<_>>(),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn aof_contains_bytes_any_shard(dir: &Path, needle: &[u8]) -> bool {
    let aof_root = dir.join("appendonlydir");
    if scan_aof_dir_for_bytes(&aof_root, needle) {
        return true;
    }
    if let Ok(entries) = std::fs::read_dir(&aof_root) {
        for entry in entries.flatten() {
            if entry.path().is_dir() && scan_aof_dir_for_bytes(&entry.path(), needle) {
                return true;
            }
        }
    }
    false
}

fn scan_aof_dir_for_bytes(dir: &Path, needle: &[u8]) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "aof")
            && let Ok(bytes) = std::fs::read(&path)
            && bytes.windows(needle.len()).any(|w| w == needle)
        {
            return true;
        }
    }
    false
}

pub fn wal_v3_total_bytes(dir: &Path, num_shards: u32) -> u64 {
    let mut total = 0u64;
    for shard_id in 0..num_shards {
        let wal_dir = dir.join(format!("shard-{shard_id}")).join("wal-v3");
        if let Ok(entries) = std::fs::read_dir(&wal_dir) {
            for entry in entries.flatten() {
                total += std::fs::metadata(entry.path())
                    .map(|m| m.len())
                    .unwrap_or(0);
            }
        }
    }
    total
}

/// Poll the shard-0 control file (`shard-0/shard-0.control`) until its
/// content changes from `before` — the moment a checkpoint Finalize has
/// written a new `last_checkpoint_lsn` (pattern:
/// `crash_recovery_graph_durability.rs`'s g5 scenario).
pub fn control_file_path(dir: &Path) -> PathBuf {
    dir.join("shard-0").join("shard-0.control")
}

pub fn wait_for_control_file_change(dir: &Path, before: &Option<Vec<u8>>, timeout: Duration) {
    let path = control_file_path(dir);
    let deadline = Instant::now() + timeout;
    loop {
        let now = std::fs::read(&path).ok();
        if now.is_some() && &now != before {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "control file {} never advanced — checkpoint Finalize did not run \
             within {timeout:?}",
            path.display(),
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Recursively count `heap-*.mpf` cold-tier data files anywhere under
/// `dir` (kv_spill's on-disk format, `src/storage/tiered/kv_spill.rs` —
/// `<disk-offload-dir>/shard-<n>/data/heap-<file_id>.mpf`;
/// `effective_disk_offload_dir` falls back to `--dir` when
/// `--disk-offload-dir` is not set, which is this harness's default, so a
/// plain recursive walk from the test's unique dir finds them without
/// needing to know the exact shard layout). A hard precondition for
/// every disk-offload scenario: without any heap files on disk, filler
/// pressure never forced a spill and the scenario silently degenerates to
/// ordinary AOF-replay coverage (already proven by `kv_isolated`) — proven
/// empirically in review round 3 (task #52 P0) against a too-small filler.
/// Pattern: `crash_recovery_cold_del_resurrection.rs::count_heap_files`.
pub fn count_heap_mpf_files(dir: &Path) -> usize {
    fn walk(p: &Path, acc: &mut usize) {
        let Ok(rd) = std::fs::read_dir(p) else {
            return;
        };
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, acc);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("heap-") && n.ends_with(".mpf"))
            {
                *acc += 1;
            }
        }
    }
    let mut acc = 0;
    walk(dir, &mut acc);
    acc
}

/// Gate for RED cells (kernel M3 brief, coordinator review round 3): the
/// suite's own execution convention is `cargo test ... --ignored` (every
/// test needs a built release binary, so ALL of them carry `#[ignore]`) —
/// but `--ignored` RUNS every ignored test, so annotating a RED cell with
/// `#[ignore = "RED: ..."]` does NOT exclude it from the default run; it
/// just documents intent while the cell still executes and fails. Every RED
/// cell calls this FIRST and returns early unless `MOON_CRASH_MATRIX_RED=1`
/// is set, so the default `--ignored` run is green-only (a clean tripwire
/// for regressions) while RED reproductions stay one env var away on
/// demand. `#[ignore]` on RED cells stays bare (or the generic "requires a
/// release binary" reason) — this guard is the actual RED/GREEN gate now.
pub fn red_guard(reason: &str) -> bool {
    if std::env::var("MOON_CRASH_MATRIX_RED").is_err() {
        eprintln!("SKIP: RED cell ({reason}) — set MOON_CRASH_MATRIX_RED=1 to run");
        return false;
    }
    true
}

/// Cheap xorshift PRNG seeded from the wall clock — good enough for "pick a
/// random marker index to kill after" jitter; no external `rand` crate
/// dependency needed for this.
pub fn jitter_u64(seed_salt: u64) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x = nanos ^ seed_salt ^ (std::process::id() as u64).wrapping_mul(0x9E3779B97F4A7C15);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// Random value in `[lo, hi)` using [`jitter_u64`].
pub fn jitter_range(seed_salt: u64, lo: u64, hi: u64) -> u64 {
    assert!(hi > lo);
    lo + jitter_u64(seed_salt) % (hi - lo)
}
