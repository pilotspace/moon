//! moon#1281 kill -9 matrix: under `--appendonly no` a cold deletion is
//! durable no later than the next successful snapshot, at every crash point.
//!
//! One pristine directory is built per run of this suite (cold probes
//! inherited from an `--appendonly yes` phase whose AOF is then removed, as in
//! `crash_recovery_cold_no_aof.rs`). Each iteration copies it, boots it
//! without an AOF, DELs the EVEN probes (their spill files still back the odd
//! probes and the fillers), and kills the server at one of these points:
//!
//! | point | what ran before the kill -9 | even probes after restart |
//! |---|---|---|
//! | `NoSave` | nothing | may come back (no snapshot names the DEL) |
//! | `DuringSave` | `BGSAVE` + 0-40 ms | old or new snapshot per shard: absent, or the ORIGINAL value |
//! | `AfterPublish` | `BGSAVE` completed | absent |
//! | `DuringSweep` | `BGSAVE` completed + 0-2.5 s (the post-save sweep, its unlinks and manifest commit) | absent |
//! | `SecondGeneration` | `BGSAVE`, kill, restart, `BGSAVE`, kill | absent |
//!
//! Every point: every ODD probe comes back with its value (nothing lost), and
//! no probe ever reads a value it never had.
//!
//!   MOON_BIN=target/release/moon cargo test --release \
//!     --test crash_matrix_cold_graves_1281 -- --ignored --test-threads 1
//! Knobs: `MOON_TEST_COLD_DEL_SHARDS` (1 or 4, default 4),
//! `MOON_TEST_MATRIX_RUNS` (iterations, default 20), `MOON_TEST_SEED`.
//!
//! Requires: built release binary, `redis-cli` on PATH.

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;
mod crash_recovery_cold_support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crash_recovery_cold_support::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KillPoint {
    NoSave,
    DuringSave,
    AfterPublish,
    DuringSweep,
    SecondGeneration,
}

const POINTS: [KillPoint; 5] = [
    KillPoint::NoSave,
    KillPoint::DuringSave,
    KillPoint::AfterPublish,
    KillPoint::DuringSweep,
    KillPoint::SecondGeneration,
];

const SAVE: [&str; 2] = ["--save", "3600 100000000"];

/// xorshift64*: deterministic from `MOON_TEST_SEED`, printed on failure.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("create copy dir");
    for e in std::fs::read_dir(from).expect("read pristine dir") {
        let e = e.expect("dir entry");
        let dst = to.join(e.file_name());
        if e.file_type().expect("file type").is_dir() {
            copy_dir(&e.path(), &dst);
        } else {
            std::fs::copy(e.path(), &dst).expect("copy file");
        }
    }
}

/// `(index, value)` of every probe that reads non-nil, by one `MGET`. Not
/// through `redis_cmd`: it trims the output, which drops nil lines at either
/// end and would shift every index after a leading nil.
fn probes_present(port: u16) -> Vec<(usize, String)> {
    let keys: Vec<String> = (0..PROBE_COUNT).map(probe_key).collect();
    let out = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "MGET"])
        .args(&keys)
        .output()
        .expect("redis-cli MGET");
    let text = String::from_utf8_lossy(&out.stdout);
    let body = text.strip_suffix('\n').unwrap_or(&text);
    let lines: Vec<&str> = body.split('\n').collect();
    assert_eq!(
        lines.len(),
        PROBE_COUNT,
        "MGET answered {} lines: {:?}",
        lines.len(),
        &text[..text.len().min(200)]
    );
    lines
        .iter()
        .enumerate()
        .filter(|(_, v)| !v.is_empty())
        .map(|(i, v)| (i, (*v).to_string()))
        .collect()
}

/// BGSAVE and wait until it completed successfully.
fn bgsave_and_wait(port: u16) {
    let lastsave = || integer_reply(&redis_cmd(port, &["LASTSAVE"])).unwrap_or(0);
    let before = lastsave();
    std::thread::sleep(Duration::from_millis(1_100));
    redis_cmd(port, &["BGSAVE"]);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let info = redis_cmd(port, &["INFO", "persistence"]);
        if info.contains("rdb_bgsave_in_progress:0") && lastsave() > before {
            assert!(
                info.contains("rdb_last_bgsave_status:ok"),
                "BGSAVE failed: {info}"
            );
            return;
        }
        assert!(Instant::now() < deadline, "BGSAVE never completed: {info}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn pristine_dir() -> PathBuf {
    let port = common::reserve_port();
    let dir = unique_dir("graves-pristine");
    std::fs::create_dir_all(&dir).expect("create pristine dir");
    let mut s1 = start_moon(port, &dir, 3600);
    wait_for_port(port);
    spill_probes(port, &dir);
    let _ = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "SHUTDOWN", "NOSAVE"])
        .output();
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline && s1.as_mut().try_wait().ok().flatten().is_none() {
        std::thread::sleep(Duration::from_millis(100));
    }
    s1.kill_now();
    wait_for_port_down(port);
    let _ = std::fs::remove_file(dir.join("appendonly.aof"));
    let _ = std::fs::remove_dir_all(dir.join("appendonlydir"));
    for log in ["moon.stdout.log", "moon.stderr.log"] {
        let _ = std::fs::remove_file(dir.join(log));
    }
    dir
}

/// One iteration; returns (resurrected even probes, lost or wrong probes).
fn iteration(pristine: &Path, point: KillPoint, rng: &mut Rng, i: usize) -> (usize, Vec<String>) {
    let port = common::reserve_port();
    let dir = unique_dir(&format!("graves-{i}"));
    copy_dir(pristine, &dir);
    let mut server = start_moon_alive_with(port, &dir, 1, "no", &SAVE);
    let val = probe_value();
    let before = probes_present(port);
    assert!(
        before.iter().any(|(_, v)| *v == val),
        "precondition failed: no probe inherited"
    );
    let evens: Vec<String> = (0..PROBE_COUNT).step_by(2).map(probe_key).collect();
    let mut del: Vec<&str> = vec!["DEL"];
    del.extend(evens.iter().map(String::as_str));
    redis_cmd(port, &del);

    match point {
        KillPoint::NoSave => {}
        KillPoint::DuringSave => {
            redis_cmd(port, &["BGSAVE"]);
            std::thread::sleep(Duration::from_millis(rng.below(40)));
        }
        KillPoint::AfterPublish => bgsave_and_wait(port),
        KillPoint::DuringSweep => {
            bgsave_and_wait(port);
            std::thread::sleep(Duration::from_millis(rng.below(2_500)));
        }
        KillPoint::SecondGeneration => {
            bgsave_and_wait(port);
            server.kill_now();
            wait_for_port_down(port);
            server = start_moon_alive_with(port, &dir, 1, "no", &SAVE);
            std::thread::sleep(Duration::from_millis(rng.below(1_500)));
            bgsave_and_wait(port);
        }
    }
    server.kill_now();
    wait_for_port_down(port);

    let mut server2 = start_moon_alive_with(port, &dir, 3600, "no", &SAVE);
    let after = probes_present(port);
    server2.kill_now();
    wait_for_port_down(port);

    let odd_before: Vec<usize> = before
        .iter()
        .filter(|(i, _)| i % 2 == 1)
        .map(|(i, _)| *i)
        .collect();
    let mut wrong = Vec::new();
    for i in odd_before {
        if !after.iter().any(|(j, v)| *j == i && *v == val) {
            wrong.push(format!("odd {} lost", probe_key(i)));
        }
    }
    for (i, v) in &after {
        if *v != val {
            wrong.push(format!("{} reads a value it never had", probe_key(*i)));
        }
    }
    let evens_back = after.iter().filter(|(i, _)| i % 2 == 0).count();
    let must_stay_deleted = matches!(
        point,
        KillPoint::AfterPublish | KillPoint::DuringSweep | KillPoint::SecondGeneration
    );
    let resurrected = if must_stay_deleted { evens_back } else { 0 };
    let bad = resurrected > 0 || !wrong.is_empty();
    let keep: Vec<String> = if bad {
        vec![format!("{point:?}")]
    } else {
        Vec::new()
    };
    finish(&dir, &keep);
    (resurrected, wrong)
}

#[test]
#[ignore]
fn cold_deletes_survive_every_kill_point_without_an_aof() {
    let runs: usize = std::env::var("MOON_TEST_MATRIX_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20);
    let seed: u64 = std::env::var("MOON_TEST_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(1)
        })
        | 1;
    let mut rng = Rng(seed);
    let pristine = pristine_dir();
    let mut failures = Vec::new();
    let mut total_resurrected = 0usize;
    for i in 0..runs {
        let point = POINTS[i % POINTS.len()];
        let (resurrected, wrong) = iteration(&pristine, point, &mut rng, i);
        eprintln!(
            "run {i:>3} {point:?}: resurrected {resurrected}, lost/wrong {}",
            wrong.len()
        );
        total_resurrected += resurrected;
        if resurrected > 0 || !wrong.is_empty() {
            failures.push(format!(
                "run {i} {point:?}: {resurrected} resurrected, {wrong:?}"
            ));
        }
    }
    let _ = std::fs::remove_dir_all(&pristine);
    assert!(
        failures.is_empty(),
        "seed {seed}, shards {}: {total_resurrected} deleted cold probes resurrected across \
         {runs} runs; failures: {failures:#?}",
        shards()
    );
}
