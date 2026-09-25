//! WS8 x WS15 guard, adopted from the PR #1233 integration review
//! (`review3_integ_spanning_cold_del`): a spanning DEL of cold keys in a
//! non-zero database stays deleted across BGREWRITEAOF and kill -9.
//!
//! WS8 (moon#1184) sends a spanning `DEL`/`UNLINK` to each owner shard as ONE
//! merged `DEL k1 k2 …` MultiExecute leg; WS15 (moon#1215) records every cold
//! slot a delete leaves on disk in the owner's dead-slot ledger and re-emits a
//! head `SELECT <db>` + `DEL` in the next AOF generation. This drives both at
//! once, in a NON-ZERO database, with same-named decoys in db 0:
//!
//!   db 0: probe:i = Z… (decoys, never touched) ; db 3: probe:i = P…
//!   filler in db 3 -> probes spill cold
//!   db 3: spanning DEL of the even probes (half as DEL, half as UNLINK)
//!   BGREWRITEAOF -> every shard's new incr head must carry its OWN keys' DELs
//!   SIGKILL -> restart -> db 3 even probes absent, odd intact, db 0 intact.
//!
//! A head DEL replayed in the wrong db would delete db 0 decoys; a remote
//! owner that did not record its dead slots would resurrect db 3 probes.
//!
//! Green on the PR head (`d4a2fd3`); a guard for the ledger's recording and
//! head-DEL paths as they change. Runs at `--shards 4` (spanning legs, the
//! per-shard fold) and `--shards 1` (no spanning, db 3 on the TopLevel fold
//! on monoio / the flat-file fold on tokio).
//!
//! `MOON_BIN=<moon> cargo test --test perf_ws15_spanning_cold_del -- --nocapture`

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::process::Command;
use std::time::{Duration, Instant};

use common::Conn;

const PROBES: usize = 200;
const PROBE_LEN: usize = 500;
const FILLER: usize = 16_000;
const FILLER_LEN: usize = 600;

fn start(dir: &std::path::Path, shards: usize) -> (common::ServerGuard, u16) {
    let off = dir.join("off");
    std::fs::create_dir_all(&off).unwrap();
    let bin = common::find_moon_binary();
    let dir = dir.to_path_buf();
    let (child, port) = common::spawn_listening(move |port| {
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                &shards.to_string(),
                "--maxmemory",
                "8388608",
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                off.to_str().unwrap(),
                "--appendonly",
                "yes",
                "--cold-orphan-sweep-interval-secs",
                "3600",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().unwrap(),
            ])
            .stdout(std::fs::File::create(dir.join(format!("out.{port}.log"))).unwrap())
            .stderr(std::fs::File::create(dir.join(format!("err.{port}.log"))).unwrap())
            .spawn()
            .expect("spawn moon")
    });
    (common::ServerGuard::new(child), port)
}

fn filler_db3(port: u16) {
    let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
    let val = "F".repeat(FILLER_LEN);
    let mut buf = common::encode(&["SELECT", "3"]);
    for i in 0..FILLER {
        buf.extend_from_slice(&common::encode(&["SET", &format!("filler:{i}"), &val]));
    }
    s.write_all(&buf).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    let mut got = 0usize;
    let mut chunk = [0u8; 65536];
    while got < FILLER + 1 {
        let n = s.read(&mut chunk).unwrap();
        assert!(n > 0, "filler conn closed after {got}");
        got += chunk[..n].iter().filter(|&&b| b == b'\n').count();
    }
}

fn heap_files(dir: &std::path::Path) -> usize {
    fn walk(p: &std::path::Path, acc: &mut usize) {
        for e in std::fs::read_dir(p).into_iter().flatten().flatten() {
            let path = e.path();
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
    let mut n = 0;
    walk(&dir.join("off"), &mut n);
    n
}

/// Newest `moon.aof.<seq>.incr.aof` under each AOF directory (per shard, or
/// the TopLevel one) — `(dir, seq, bytes)`.
fn newest_incrs(dir: &std::path::Path) -> Vec<(String, u64, Vec<u8>)> {
    let aof = dir.join("appendonlydir");
    let mut dirs = vec![aof.clone()];
    for e in std::fs::read_dir(&aof).into_iter().flatten().flatten() {
        if e.path().is_dir() {
            dirs.push(e.path());
        }
    }
    let mut out = Vec::new();
    for d in dirs {
        let mut best: Option<(u64, std::path::PathBuf)> = None;
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(seq) = name
                .strip_prefix("moon.aof.")
                .and_then(|r| r.strip_suffix(".incr.aof"))
                .and_then(|s| s.parse::<u64>().ok())
                && best.as_ref().is_none_or(|(b, _)| seq > *b)
            {
                best = Some((seq, e.path()));
            }
        }
        if let Some((seq, p)) = best {
            out.push((d.display().to_string(), seq, std::fs::read(p).unwrap()));
        }
    }
    // tokio `--shards 1`: one legacy flat file; a rewrite republishes it with
    // an RDB preamble (`MOON` magic) followed by the generation head.
    let flat = dir.join("appendonly.aof");
    if let Ok(bytes) = std::fs::read(&flat) {
        let seq = u64::from(bytes.starts_with(b"MOON"));
        out.push((flat.display().to_string(), seq, bytes));
    }
    out
}

fn get(c: &mut Conn, key: &str) -> Option<String> {
    let r = c.send(&["GET", key]);
    if r.starts_with("$-1") {
        return None;
    }
    let body = r.split("\r\n").nth(1).unwrap_or_default();
    Some(body.to_string())
}

#[test]
fn spanning_del_of_cold_keys_in_db3_stays_deleted_across_rewrite_and_kill9_at_4_shards() {
    run(4);
}

#[test]
fn del_of_cold_keys_in_db3_stays_deleted_across_rewrite_and_kill9_at_1_shard() {
    run(1);
}

fn run(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws15-span-cold-del-{shards}"));
    std::fs::create_dir_all(&dir).unwrap();
    let (mut server, port) = start(&dir, shards);
    let mut c = Conn::open(port);
    let decoy = "Z".repeat(PROBE_LEN);
    let live = "P".repeat(PROBE_LEN);
    for i in 0..PROBES {
        assert_eq!(c.send(&["SET", &format!("probe:{i}"), &decoy]), "+OK\r\n");
    }
    assert_eq!(c.send(&["SELECT", "3"]), "+OK\r\n");
    for i in 0..PROBES {
        assert_eq!(c.send(&["SET", &format!("probe:{i}"), &live]), "+OK\r\n");
    }
    filler_db3(port);
    std::thread::sleep(Duration::from_secs(8));
    let heaps = heap_files(&dir);
    assert!(heaps > 0, "precondition: nothing spilled");

    // Spanning deletes in db 3: 50 keys per command (every command spans all
    // owners at --shards 4), even probes 0..100 via DEL, 100..200 via UNLINK.
    let even: Vec<String> = (0..PROBES)
        .filter(|i| i % 2 == 0)
        .map(|i| format!("probe:{i}"))
        .collect();
    let mut removed = 0i64;
    for (n, chunk) in even.chunks(25).enumerate() {
        let verb = if n < 2 { "DEL" } else { "UNLINK" };
        let mut args = vec![verb];
        args.extend(chunk.iter().map(String::as_str));
        let r = c.send(&args);
        removed += r
            .trim_start_matches(':')
            .trim()
            .parse::<i64>()
            .unwrap_or_else(|_| panic!("{verb} reply {r:?}"));
    }
    assert_eq!(
        removed, 100,
        "every even db-3 probe existed and was removed"
    );
    assert_eq!(get(&mut c, "probe:0"), None);

    // BGREWRITEAOF and wait until every generation is cut.
    let before: Vec<u64> = newest_incrs(&dir).iter().map(|(_, s, _)| *s).collect();
    let r = c.send(&["BGREWRITEAOF"]);
    assert!(r.starts_with('+'), "BGREWRITEAOF: {r:?}");
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let info = c.send(&["INFO", "persistence"]);
        let now: Vec<u64> = newest_incrs(&dir).iter().map(|(_, s, _)| *s).collect();
        let advanced = now.len() == before.len()
            && now.iter().zip(&before).filter(|(a, b)| a > b).count() == shards;
        if info.contains("aof_rewrite_in_progress:0") && advanced {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "rewrite never finished: {now:?} vs {before:?}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // Every new generation's head: which db-3 probes does it DELete?
    let mut head_dels = 0usize;
    let mut gens_with_dels = 0usize;
    for (d, seq, bytes) in newest_incrs(&dir) {
        let text = String::from_utf8_lossy(&bytes);
        let n = even
            .iter()
            .filter(|k| text.contains(&format!("\r\n{k}\r\n")))
            .count();
        let selects_3 = text.contains("$6\r\nSELECT\r\n$1\r\n3\r\n");
        eprintln!("{d} seq {seq}: {n} even probes in head, SELECT 3: {selects_3}");
        head_dels += n;
        if n > 0 {
            gens_with_dels += 1;
            assert!(selects_3, "{d}: head DELs without SELECT 3");
        }
    }
    eprintln!(
        "heap files {heaps}; head DEL mentions {head_dels}; generations with DELs {gens_with_dels}"
    );

    std::thread::sleep(Duration::from_secs(2));
    server.kill_now();
    common::wait_for_port_down(port);
    drop(c);
    let (_server2, port2) = start(&dir, shards);
    let mut c2 = Conn::open(port2);
    let mut back = Vec::new();
    let mut lost_odd = 0usize;
    let mut lost_decoys = 0usize;
    for i in 0..PROBES {
        if get(&mut c2, &format!("probe:{i}")).as_deref() != Some(decoy.as_str()) {
            lost_decoys += 1;
        }
    }
    assert_eq!(c2.send(&["SELECT", "3"]), "+OK\r\n");
    for i in 0..PROBES {
        let got = get(&mut c2, &format!("probe:{i}"));
        if i % 2 == 0 {
            if got.is_some() {
                back.push(i);
            }
        } else if got.as_deref() != Some(live.as_str()) {
            lost_odd += 1;
        }
    }
    eprintln!(
        "after restart: {} deleted db-3 probes back, {lost_odd} live db-3 probes lost, \
         {lost_decoys} db-0 decoys lost",
        back.len()
    );
    assert!(
        back.is_empty() && lost_odd == 0 && lost_decoys == 0,
        "{} of 100 DELeted db-3 probes came back (first {:?}); {lost_odd} live db-3 probes \
         and {lost_decoys} db-0 decoys lost; head DEL mentions {head_dels} in {gens_with_dels} \
         generations; dir {}",
        back.len(),
        back.first(),
        dir.display()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
