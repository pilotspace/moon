//! moon#1228 review 6 (the reviewer's real-server property test, adopted):
//! seed four databases with mixed types, record the image, hold a BGSAVE open
//! (`MOON_TEST_SNAPSHOT_HOLD_FILE`) and run random writes — SET, DEL, UNLINK,
//! expiries, MOVE, COPY, SWAPDB, FLUSHDB [ASYNC], grow-then-FLUSHDB — with the
//! hold toggled so the walk stops at random points; finish the save, and
//! restore the file ALONE (kill -9, restart, `--appendonly no`): it must be
//! exactly the epoch-start image.
//!
//! 3 seeds at `--shards 1` and 3 at `--shards 4` by default;
//! `MOON_TEST_BGSAVE_PROP_SEEDS`, `MOON_TEST_BGSAVE_PROP_SEED_START` and
//! `MOON_TEST_BGSAVE_PROP_SHARDS` ("1,4") run others (the reviewer ran 80).
//! Ports come from `common::spawn_listening`.
//!
//! Pin the binary: `MOON_BIN=<moon> cargo test --test perf_ws16_bgsave_prop`.

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, encode};

// ---------------------------------------------------------------- RESP ----

#[derive(Debug, Clone, PartialEq)]
enum V {
    S(String),
    I(i64),
    Nil,
    E(String),
    A(Vec<V>),
}

fn parse_one(b: &[u8], i: &mut usize) -> V {
    let end = (*i..b.len() - 1)
        .find(|&j| &b[j..j + 2] == b"\r\n")
        .unwrap();
    let line = std::str::from_utf8(&b[*i + 1..end]).unwrap().to_string();
    let tag = b[*i];
    *i = end + 2;
    match tag {
        b'+' => V::S(line),
        b'-' => V::E(line),
        b':' => V::I(line.parse().unwrap()),
        b'$' => {
            let n: i64 = line.parse().unwrap();
            if n < 0 {
                return V::Nil;
            }
            let s = String::from_utf8_lossy(&b[*i..*i + n as usize]).into_owned();
            *i += n as usize + 2;
            V::S(s)
        }
        b'*' => {
            let n: i64 = line.parse().unwrap();
            if n < 0 {
                return V::Nil;
            }
            V::A((0..n).map(|_| parse_one(b, i)).collect())
        }
        b'_' => V::Nil,
        other => panic!("unexpected RESP tag {other}"),
    }
}

fn parse_all(s: &str) -> Vec<V> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut out = Vec::new();
    while i < b.len() {
        out.push(parse_one(b, &mut i));
    }
    out
}

fn strs(v: &V) -> Vec<String> {
    match v {
        V::A(items) => items
            .iter()
            .map(|x| match x {
                V::S(s) => s.clone(),
                V::I(i) => i.to_string(),
                other => format!("{other:?}"),
            })
            .collect(),
        other => vec![format!("{other:?}")],
    }
}

fn pipe(c: &mut Conn, cmds: &[Vec<String>]) -> Vec<V> {
    let mut out = Vec::new();
    for chunk in cmds.chunks(500) {
        let mut buf = Vec::new();
        for cmd in chunk {
            let parts: Vec<&str> = cmd.iter().map(String::as_str).collect();
            buf.extend_from_slice(&encode(&parts));
        }
        c.sock.write_all(&buf).unwrap();
        let reply = c.read_replies_within(chunk.len(), Duration::from_secs(120));
        let parsed = parse_all(&reply);
        assert_eq!(parsed.len(), chunk.len());
        out.extend(parsed);
    }
    out
}

fn cmd(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|p| (*p).to_string()).collect()
}

type Image = BTreeMap<(usize, String), String>;

/// Canonical image of databases `0..n_dbs`: type, absolute expiry, sorted content.
fn dump(c: &mut Conn, n_dbs: usize) -> Image {
    let mut out = Image::new();
    for db in 0..n_dbs {
        assert!(c.send(&["SELECT", &db.to_string()]).starts_with("+OK"));
        let keys = strs(&parse_all(&c.send(&["KEYS", "*"]))[0]);
        let meta: Vec<Vec<String>> = keys
            .iter()
            .flat_map(|k| [cmd(&["TYPE", k]), cmd(&["PEXPIRETIME", k])])
            .collect();
        let meta = pipe(c, &meta);
        let mut reads = Vec::new();
        let mut kinds = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            let ty = match &meta[2 * i] {
                V::S(s) => s.clone(),
                other => format!("{other:?}"),
            };
            let exp = match &meta[2 * i + 1] {
                V::I(n) => *n,
                other => panic!("PEXPIRETIME {k}: {other:?}"),
            };
            let read = match ty.as_str() {
                "string" => cmd(&["GET", k]),
                "hash" => cmd(&["HGETALL", k]),
                "list" => cmd(&["LRANGE", k, "0", "-1"]),
                "set" => cmd(&["SMEMBERS", k]),
                "zset" => cmd(&["ZRANGE", k, "0", "-1", "WITHSCORES"]),
                _ => cmd(&["TYPE", k]),
            };
            reads.push(read);
            kinds.push((k.clone(), ty, exp));
        }
        let bodies = pipe(c, &reads);
        for ((k, ty, exp), body) in kinds.into_iter().zip(bodies) {
            let mut items = match &body {
                V::S(s) => vec![s.clone()],
                other => strs(other),
            };
            match ty.as_str() {
                "hash" => {
                    let mut pairs: Vec<String> = items
                        .chunks(2)
                        .map(|p| format!("{}={}", p[0], p[1]))
                        .collect();
                    pairs.sort();
                    items = pairs;
                }
                "set" => items.sort(),
                _ => {}
            }
            out.insert((db, k), format!("{ty}|exp={exp}|{items:?}"));
        }
    }
    let _ = c.send(&["SELECT", "0"]);
    out
}

// ------------------------------------------------------------ servers ----

struct DirGuard(PathBuf);
impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn hold_file(dir: &Path) -> PathBuf {
    dir.join("snapshot.hold")
}

fn spawn(dir: &Path, shards: usize, extra: &[&str]) -> (ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    let bin = common::find_moon_binary();
    let extra: Vec<String> = extra.iter().map(|s| (*s).to_string()).collect();
    let (child, port) = common::spawn_listening(|port| {
        std::process::Command::new(&bin)
            .env("MOON_TEST_SNAPSHOT_HOLD_FILE", hold_file(dir))
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--appendonly",
                "no",
                "--save",
                "",
                "--disk-free-min-pct",
                "0",
            ])
            .args(&extra)
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    });
    let guard = ServerGuard::new(child);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let mut c = Conn::open(port);
        if c.send(&["PING"]).starts_with("+PONG") {
            break;
        }
        assert!(Instant::now() < deadline, "server never answered PING");
        std::thread::sleep(Duration::from_millis(20));
    }
    (guard, port)
}

fn info(c: &mut Conn, section: &str, field: &str) -> String {
    let s = c.send(&["INFO", section]);
    let prefix = format!("{field}:");
    s.lines()
        .find_map(|l| l.strip_prefix(prefix.as_str()))
        .map(|v| v.trim().to_string())
        .unwrap_or_else(|| panic!("no {field} in INFO {section}"))
}

fn in_progress(c: &mut Conn) -> bool {
    info(c, "persistence", "rdb_bgsave_in_progress") == "1"
}

fn wait_armed(dir: &Path, shards: usize, c: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !(0..shards).all(|s| dir.join(format!("shard-{s}.rrdshard.tmp")).exists()) {
        assert!(in_progress(c), "the held save ended before arming");
        assert!(Instant::now() < deadline, "never armed");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn wait_done(c: &mut Conn) {
    let deadline = Instant::now() + Duration::from_secs(300);
    while in_progress(c) {
        assert!(Instant::now() < deadline, "BGSAVE never finished");
        std::thread::sleep(Duration::from_millis(5));
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const N_DBS: usize = 4;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn seed_keyspace(c: &mut Conn, rng: &mut Rng) -> Vec<u64> {
    let far = (now_ms() + 1_000_000_000).to_string();
    let mut sizes = Vec::new();
    for db in 0..N_DBS {
        let n = [0u64, 100, 2000, 12_000][rng.below(4) as usize];
        sizes.push(n);
        let mut cmds = vec![cmd(&["SELECT", &db.to_string()])];
        for i in 0..n {
            let k = format!("s{db}:{i:05}");
            let v = format!("v{db}.{i}");
            match i % 7 {
                0 | 1 => cmds.push(cmd(&["SET", &k, &v])),
                2 => cmds.push(cmd(&["SET", &k, &v, "PXAT", &far])),
                3 => cmds.push(cmd(&["HSET", &k, "f1", &v, "f2", "x"])),
                4 => cmds.push(cmd(&["RPUSH", &k, "a", &v, "c"])),
                5 => {
                    cmds.push(cmd(&["SADD", &k, "m1", &v]));
                    cmds.push(cmd(&["PEXPIREAT", &k, &far]));
                }
                _ => cmds.push(cmd(&["ZADD", &k, "1", "a", "2", &v])),
            }
        }
        let replies = pipe(c, &cmds);
        assert!(
            !replies.iter().any(|r| matches!(r, V::E(_))),
            "seed refused"
        );
    }
    let _ = c.send(&["SELECT", "0"]);
    sizes
}

#[derive(Default, Debug)]
struct E2eCoverage {
    seeds: u64,
    ops: u64,
    flushes: u64,
    grown_flushes: u64,
    swaps: u64,
    moves: u64,
    hold_releases: u64,
    walk_progress_samples: u64,
}

/// One seed: seed, record the image, arm a held save, run random writes with
/// the hold toggled so the walk stops at random points, finish the save, and
/// restore the file alone (kill -9, restart, `--appendonly no`).
fn e2e_seed(shards: usize, seed: u64, cov: &mut E2eCoverage) {
    let dir = common::unique_test_dir(&format!("ws16-bgsave-prop-s{shards}-{seed}"));
    let _cleanup = DirGuard(dir.clone());
    let (mut server, port) = spawn(&dir, shards, &["--disk-offload", "disable"]);
    let mut c = Conn::open(port);
    let mut probe = Conn::open(port);
    let mut rng = Rng(seed);
    let sizes = seed_keyspace(&mut c, &mut rng);
    let expected = dump(&mut c, N_DBS);
    std::fs::write(hold_file(&dir), b"").unwrap();
    assert!(c.send(&["BGSAVE"]).contains("Background saving started"));
    wait_armed(&dir, shards, &mut probe);
    let now = now_ms();
    let far = (now + 500_000_000).to_string();
    let mut selected = 0usize;
    let mut rounds = 0;
    'outer: loop {
        rounds += 1;
        let mut batch: Vec<Vec<String>> = Vec::new();
        for _ in 0..1 + rng.below(10) {
            cov.ops += 1;
            let db = rng.below(N_DBS as u64) as usize;
            if db != selected {
                batch.push(cmd(&["SELECT", &db.to_string()]));
                selected = db;
            }
            let src = rng.below(N_DBS as u64) as usize;
            let k = if rng.below(10) < 8 && sizes[src] > 0 {
                format!("s{src}:{:05}", rng.below(sizes[src]))
            } else {
                format!("n:{}", rng.below(300))
            };
            let other = rng.below(N_DBS as u64).to_string();
            match rng.below(100) {
                0..=24 => batch.push(cmd(&["SET", &k, &format!("w{}", rng.below(999))])),
                25..=27 => batch.push(cmd(&["SET", &k, "px", "PXAT", &far])),
                28..=37 => batch.push(cmd(&["DEL", &k])),
                38..=40 => batch.push(cmd(&["UNLINK", &k])),
                41..=43 => batch.push(cmd(&["INCR", &k])),
                44..=46 => batch.push(cmd(&["HSET", &k, "f1", "h"])),
                47..=49 => batch.push(cmd(&["RPUSH", &k, "z"])),
                50..=52 => batch.push(cmd(&["SADD", &k, "m9"])),
                53..=55 => batch.push(cmd(&["ZADD", &k, "5", "q"])),
                56..=58 => batch.push(cmd(&[
                    "PEXPIREAT",
                    &k,
                    &(now_ms() + 1 + rng.below(30)).to_string(),
                ])),
                59..=61 => batch.push(cmd(&["PEXPIREAT", &k, &(now - 1000).to_string()])),
                62..=63 => batch.push(cmd(&["PERSIST", &k])),
                64..=71 => {
                    cov.moves += 1;
                    batch.push(cmd(&["MOVE", &k, &other]));
                }
                72..=73 => batch.push(cmd(&["COPY", &k, &k, "DB", &other, "REPLACE"])),
                74..=80 => {
                    cov.swaps += 1;
                    let a = rng.below(N_DBS as u64).to_string();
                    batch.push(cmd(&["SWAPDB", &a, &other]));
                }
                81..=88 => {
                    cov.flushes += 1;
                    if rng.below(3) == 0 {
                        batch.push(cmd(&["FLUSHDB", "ASYNC"]));
                    } else {
                        batch.push(cmd(&["FLUSHDB"]));
                    }
                }
                _ => {
                    // Grow the selected database, then flush it — ALONE, so
                    // no second grown table can wait for the same drain.
                    let replies = pipe(&mut c, &batch);
                    assert!(
                        !replies
                            .iter()
                            .any(|r| matches!(r, V::E(e) if e.contains("OOM"))),
                        "refused: {replies:?}"
                    );
                    batch.clear();
                    let tag = rng.next();
                    let grow: Vec<Vec<String>> = (0..3000)
                        .map(|j| cmd(&["SET", &format!("g{tag:x}:{j}"), "gv"]))
                        .collect();
                    let _ = pipe(&mut c, &grow);
                    std::thread::sleep(Duration::from_millis(3)); // a drain runs
                    cov.grown_flushes += 1;
                    let _ = c.send(&["FLUSHDB"]);
                    std::thread::sleep(Duration::from_millis(3));
                }
            }
        }
        let replies = pipe(&mut c, &batch);
        for r in &replies {
            if let V::E(e) = r {
                // Cross-shard MOVE/COPY targets are the only expected refusals.
                assert!(!e.contains("OOM"), "refused: {e}");
            }
        }
        // Let the walk run a little, at a random point, then hold it again.
        if rng.below(3) == 0 {
            cov.hold_releases += 1;
            let _ = std::fs::remove_file(hold_file(&dir));
            std::thread::sleep(Duration::from_micros(200 + rng.below(3000)));
            std::fs::write(hold_file(&dir), b"").unwrap();
        }
        if !in_progress(&mut probe) {
            break 'outer;
        }
        cov.walk_progress_samples += 1;
        if rounds > 400 {
            break;
        }
    }
    let _ = std::fs::remove_file(hold_file(&dir));
    wait_done(&mut probe);
    assert_eq!(
        info(&mut probe, "persistence", "rdb_last_bgsave_status"),
        "ok",
        "shards {shards} seed {seed}: the save failed"
    );
    assert_eq!(info(&mut probe, "persistence", "current_cow_size"), "0");
    server.kill_now();
    common::wait_for_port_down(port);
    let (_server2, port2) = spawn(&dir, shards, &["--disk-offload", "disable"]);
    let mut c2 = Conn::open(port2);
    let got = dump(&mut c2, N_DBS);
    if got != expected {
        let missing: Vec<_> = expected.keys().filter(|k| !got.contains_key(*k)).collect();
        let extra: Vec<_> = got.keys().filter(|k| !expected.contains_key(*k)).collect();
        let wrong: Vec<_> = expected
            .iter()
            .filter(|(k, v)| got.get(*k).is_some_and(|g| g != *v))
            .map(|(k, v)| (k.clone(), v.clone(), got[k].clone()))
            .collect();
        panic!(
            "shards {shards} seed {seed}: the restored file is not the epoch-start image: {} \
             missing {:?}, {} extra {:?}, {} wrong {:?}",
            missing.len(),
            &missing[..missing.len().min(8)],
            extra.len(),
            &extra[..extra.len().min(8)],
            wrong.len(),
            &wrong[..wrong.len().min(4)],
        );
    }
    cov.seeds += 1;
}

#[test]
fn the_restored_file_is_the_epoch_start_image_over_random_workloads() {
    let seeds: u64 = std::env::var("MOON_TEST_BGSAVE_PROP_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let start: u64 = std::env::var("MOON_TEST_BGSAVE_PROP_SEED_START")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let shard_list = std::env::var("MOON_TEST_BGSAVE_PROP_SHARDS").unwrap_or_else(|_| "1,4".into());
    let mut cov = E2eCoverage::default();
    for shards in shard_list
        .split(',')
        .map(|s| s.trim().parse::<usize>().unwrap())
    {
        for seed in start..start + seeds {
            e2e_seed(shards, seed, &mut cov);
        }
    }
    eprintln!("bgsave property coverage: {cov:#?}");
}

fn lastsave(c: &mut Conn) -> i64 {
    match &parse_all(&c.send(&["LASTSAVE"]))[0] {
        V::I(n) => *n,
        other => panic!("LASTSAVE: {other:?}"),
    }
}

fn dbsize(c: &mut Conn, db: usize) -> i64 {
    let r = c.pipeline(&[&["SELECT", &db.to_string()], &["DBSIZE"]]);
    match parse_all(&r).pop() {
        Some(V::I(n)) => n,
        other => panic!("DBSIZE: {other:?}"),
    }
}

/// Review 6 (S2, the reviewer's proof, adopted): db 1 grows by 10 MiB during a
/// held save; then ONE pipeline — or one MULTI — flushes db 1 and db 2 (db 2
/// did not grow). Only one grown table waits for the drain, so the save must
/// complete. It used to fail on the second freeze whatever that table held
/// ("status err"). Also checks `current_cow_size` back at 0, that the next
/// BGSAVE completes, and the restored file.
#[test]
fn a_pipelined_flush_of_a_grown_and_an_ungrown_db_keeps_the_save() {
    let mut outcomes = Vec::new();
    for use_multi in [false, true] {
        let dir = common::unique_test_dir(&format!("ws16-bgsave-two-flushes-{use_multi}"));
        let _cleanup = DirGuard(dir.clone());
        let (mut server, port) = spawn(&dir, 1, &["--disk-offload", "disable"]);
        let mut c = Conn::open(port);
        for db in 0..3 {
            let mut cmds = vec![cmd(&["SELECT", &db.to_string()])];
            for j in 0..200 {
                cmds.push(cmd(&["SET", &format!("d{db}:{j}"), "v"]));
            }
            let _ = pipe(&mut c, &cmds);
        }
        assert!(c.send(&["SELECT", "0"]).starts_with("+OK"));
        assert!(c.send(&["BGSAVE"]).contains("started"));
        wait_done(&mut c);
        assert_eq!(info(&mut c, "persistence", "rdb_last_bgsave_status"), "ok");
        let t1 = lastsave(&mut c);
        std::thread::sleep(Duration::from_millis(1100));

        std::fs::write(hold_file(&dir), b"").unwrap();
        assert!(c.send(&["BGSAVE"]).contains("started"));
        wait_armed(&dir, 1, &mut c);
        let big = "x".repeat(1 << 20);
        let mut cmds = vec![cmd(&["SELECT", "1"])];
        for j in 0..10 {
            cmds.push(cmd(&["SET", &format!("big:{j}"), &big]));
        }
        let _ = pipe(&mut c, &cmds);
        std::thread::sleep(Duration::from_millis(20)); // drains run; the walk is held
        let changes_before: i64 = info(&mut c, "persistence", "rdb_changes_since_last_save")
            .parse()
            .unwrap();
        let reply = if use_multi {
            c.pipeline(&[
                &["MULTI"],
                &["SELECT", "1"],
                &["FLUSHDB"],
                &["SELECT", "2"],
                &["FLUSHDB"],
                &["EXEC"],
            ])
        } else {
            c.pipeline(&[
                &["SELECT", "1"],
                &["FLUSHDB"],
                &["SELECT", "2"],
                &["FLUSHDB"],
            ])
        };
        let changes_after: i64 = info(&mut c, "persistence", "rdb_changes_since_last_save")
            .parse()
            .unwrap();
        std::fs::remove_file(hold_file(&dir)).unwrap();
        wait_done(&mut c);
        let status = info(&mut c, "persistence", "rdb_last_bgsave_status");
        let t2 = lastsave(&mut c);
        let cow = info(&mut c, "persistence", "current_cow_size");
        eprintln!(
            "multi={use_multi}: reply {reply:?}; status {status}; LASTSAVE {t1} -> {t2}; \
             current_cow_size {cow}; rdb_changes_since_last_save {changes_before} -> \
             {changes_after} (removed 210 + 200 keys)"
        );
        // moon#1232: the flushes count their 410 removed keys, once.
        assert_eq!(
            changes_after - changes_before,
            410,
            "the flushes count their keys, once"
        );
        assert_eq!(cow, "0");
        // Recovery after whatever happened: the next save completes.
        assert!(c.send(&["SELECT", "0"]).starts_with("+OK"));
        assert!(c.send(&["BGSAVE"]).contains("started"));
        wait_done(&mut c);
        assert_eq!(info(&mut c, "persistence", "rdb_last_bgsave_status"), "ok");
        assert!(lastsave(&mut c) >= t1);
        server.kill_now();
        common::wait_for_port_down(port);
        let (_s2, port2) = spawn(&dir, 1, &["--disk-offload", "disable"]);
        let mut c2 = Conn::open(port2);
        assert_eq!(
            (dbsize(&mut c2, 0), dbsize(&mut c2, 1), dbsize(&mut c2, 2)),
            (200, 0, 0)
        );
        if status == "err" {
            assert_eq!(t2, t1, "a failed save must not move LASTSAVE");
        }
        outcomes.push((use_multi, status));
    }
    assert!(
        outcomes.iter().all(|(_, s)| s == "ok"),
        "ONE grown table (db 1) and an un-grown one (db 2) flushed in one tick failed the save: \
         (multi, status) = {outcomes:?}"
    );
}
