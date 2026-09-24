//! moon#1225: a list MOVE on a live server never pops an element it cannot
//! place, when one endpoint is a cold-tier list whose bytes cannot be read.
//!
//! The moon#875 method, on a real server: a list is tiered to its own heap
//! file through eviction, the file is moved away under the RUNNING server
//! (the index still names it), and the list commands that act on a
//! "does the key exist?" answer before replying are driven at it:
//!
//! * `LMOVE` / `RPOPLPUSH` / `BLMOVE` (served on the spot) with the faulted
//!   list as DESTINATION — pre-fix the source was popped, the push's `-IOERR`
//!   swallowed, and the client told the element had moved while it existed
//!   nowhere (the success also reached the AOF);
//! * the same with the faulted list as SOURCE;
//! * `LMPOP` with the faulted list FIRST — pre-fix it popped the later key,
//!   then answered `-IOERR`: the element was gone on the primary;
//! * `BLMOVE` queued inside `MULTI` (rewritten to `LMOVE` at `EXEC`).
//!
//! Every refusal must be `-IOERR`, and every element must still be where it
//! was. Moving the file back must heal the faulted list with its original
//! contents — nothing was fabricated over it.
//!
//! Run with (pin the binary; `find_moon_binary` otherwise falls back to an
//! artifact of unknown provenance):
//!   MOON_BIN=/path/to/moon cargo test --test perf_ws10_list_cold_fault

#![cfg(unix)]
#![allow(clippy::unwrap_used)]

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use common::{Conn, ServerGuard, find_moon_binary};

const MAXMEMORY_BYTES: usize = 8 * 1024 * 1024;
const FILLER_VALUE_LEN: usize = 1024;
const FILLER_PER_ROUND: usize = 100;
const MAX_FILLER_ROUNDS: usize = 1000;

fn offload_dir(dir: &Path) -> PathBuf {
    dir.join("off")
}

fn spawn(dir: &Path, shards: usize) -> (ServerGuard, u16) {
    std::fs::create_dir_all(offload_dir(dir)).unwrap();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        Command::new(find_moon_binary())
            .args([
                "--port",
                &port.to_string(),
                "--dir",
                &dir.to_string_lossy(),
                "--shards",
                &shards.to_string(),
                "--disk-offload",
                "enable",
                "--disk-offload-dir",
                &offload_dir(dir).to_string_lossy(),
                "--appendonly",
                "yes",
                "--appendfsync",
                "everysec",
                "--maxmemory",
                &MAXMEMORY_BYTES.to_string(),
                "--maxmemory-policy",
                "allkeys-lru",
                "--disk-free-min-pct",
                "0",
                "--protected-mode",
                "no",
            ])
            .stdout(common::server_stderr(dir))
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (build it first, or set MOON_BIN)")
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 7];
            if s.write_all(b"PING\r\n").is_ok()
                && s.read_exact(&mut buf).is_ok()
                && buf.starts_with(b"+PONG")
            {
                return (guard, port);
            }
        }
        assert!(Instant::now() < deadline, "moon never answered PING");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn accepted(reply: &str) -> bool {
    if reply.starts_with('-') {
        assert!(
            reply.contains("OOM") || reply.contains("backpressure"),
            "unexpected error while filling: {reply:?}"
        );
        return false;
    }
    true
}

fn write_filler(c: &mut Conn, round: usize) {
    let val = "f".repeat(FILLER_VALUE_LEN);
    let keys: Vec<String> = (0..FILLER_PER_ROUND)
        .map(|i| format!("filler:{round:04}:{i:03}"))
        .collect();
    let cmds: Vec<Vec<&str>> = keys.iter().map(|k| vec!["SET", k.as_str(), &val]).collect();
    let refs: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
    let replies = c.pipeline(&refs);
    let _ = replies
        .split("\r\n")
        .filter(|l| !l.is_empty())
        .filter(|l| accepted(l))
        .count();
}

/// Every heap file under `<off>/shard-*/data/` whose bytes contain `needle`.
fn heap_files_holding(off: &Path, needle: &[u8]) -> Vec<PathBuf> {
    let mut hits = Vec::new();
    let Ok(shards) = std::fs::read_dir(off) else {
        return hits;
    };
    for shard in shards.flatten() {
        let Ok(files) = std::fs::read_dir(shard.path().join("data")) else {
            continue;
        };
        for f in files.flatten() {
            let p = f.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !(name.starts_with("heap-") && name.ends_with(".mpf")) {
                continue;
            }
            if let Ok(bytes) = std::fs::read(&p)
                && bytes.windows(needle.len()).any(|w| w == needle)
            {
                hits.push(p);
            }
        }
    }
    hits.sort();
    hits
}

fn fill_until_spilled(c: &mut Conn, off: &Path, key: &str) -> PathBuf {
    let deadline = Instant::now() + Duration::from_secs(120);
    for round in 0..MAX_FILLER_ROUNDS {
        write_filler(c, round);
        if let Some(first) = heap_files_holding(off, key.as_bytes()).first() {
            return first.clone();
        }
        assert!(
            Instant::now() < deadline,
            "{key} did not reach disk in 120s"
        );
    }
    panic!("{key} did not reach disk after {MAX_FILLER_ROUNDS} filler rounds");
}

/// `LRANGE key 0 -1` as a list of element strings; panics on an error reply.
fn lrange(c: &mut Conn, key: &str) -> Vec<String> {
    let raw = c.send(&["LRANGE", key, "0", "-1"]);
    assert!(raw.starts_with('*'), "LRANGE {key} answered {raw:?}");
    raw.split("\r\n")
        .skip(1)
        .filter(|l| !l.is_empty() && !l.starts_with('$'))
        .map(str::to_string)
        .collect()
}

fn assert_ioerr(reply: &str, what: &str) {
    assert!(
        reply.starts_with("-IOERR"),
        "{what}: an endpoint is indexed but unreadable — the reply must be -IOERR, \
         got {reply:?} (moon#1225)"
    );
}

fn list_moves_never_lose_an_element(shards: usize) {
    let dir = common::unique_test_dir(&format!("ws10-list-cold-fault-s{shards}"));
    let off = offload_dir(&dir);
    let (server, port) = spawn(&dir, shards);
    let mut c = Conn::open(port);

    // One hash tag: every key on one shard, so the moves are not refused as
    // cross-shard at --shards 4.
    let faulted = "{w10}:faulted-list-1225";
    let src = "{w10}:src";
    let src2 = "{w10}:src2";
    let faulted_items = ["cold-1225-a", "cold-1225-b"];

    let mut rpush = vec!["RPUSH", faulted];
    rpush.extend(faulted_items);
    assert_eq!(c.send(&rpush), ":2\r\n");
    let file = fill_until_spilled(&mut c, &off, faulted);
    // Let the spill's completion land: until it does the payload is still
    // served from the in-flight plane in RAM and the file is not consulted.
    std::thread::sleep(Duration::from_secs(1));
    // Written AFTER the spill: the most recently used keys, so no eviction
    // below can take them (and a tiered source would be readable anyway).
    assert_eq!(c.send(&["RPUSH", src, "s1", "s2", "s3"]), ":3\r\n");
    assert_eq!(c.send(&["RPUSH", src2, "t1", "t2"]), ":2\r\n");
    assert_eq!(
        c.send(&["EXISTS", faulted]),
        ":1\r\n",
        "the tiered list must still exist before the damage"
    );

    // Damage under the RUNNING server: the index still names the file.
    let parked = dir.join("parked-heap.mpf");
    std::fs::rename(&file, &parked).unwrap();
    // The premise, checked rather than assumed: the list IS faulted now. A
    // plain read of it answers -IOERR on every binary (moon#875).
    assert_ioerr(
        &c.send(&["LLEN", faulted]),
        "precondition: LLEN of the damaged list",
    );

    let src_items = vec!["s1".to_string(), "s2".into(), "s3".into()];
    let src2_items = vec!["t1".to_string(), "t2".into()];

    for (what, cmd) in [
        (
            "LMOVE onto the faulted list",
            vec!["LMOVE", src, faulted, "LEFT", "LEFT"],
        ),
        (
            "LMOVE onto the faulted list (right)",
            vec!["LMOVE", src, faulted, "RIGHT", "RIGHT"],
        ),
        (
            "RPOPLPUSH onto the faulted list",
            vec!["RPOPLPUSH", src, faulted],
        ),
        (
            "BLMOVE onto the faulted list",
            vec!["BLMOVE", src, faulted, "LEFT", "RIGHT", "0"],
        ),
        (
            "BRPOPLPUSH onto the faulted list",
            vec!["BRPOPLPUSH", src, faulted, "0"],
        ),
        (
            "LMOVE from the faulted list",
            vec!["LMOVE", faulted, src, "LEFT", "LEFT"],
        ),
        (
            "RPOPLPUSH from the faulted list",
            vec!["RPOPLPUSH", faulted, src],
        ),
        (
            "LMPOP with the faulted list first",
            vec!["LMPOP", "2", faulted, src2, "LEFT"],
        ),
        (
            "LMPOP COUNT with the faulted list first",
            vec!["LMPOP", "2", faulted, src2, "RIGHT", "COUNT", "5"],
        ),
    ] {
        let reply = c.send(&cmd);
        eprintln!("[1225 s{shards}] {what}: {:?}", reply.trim_end());
        assert_ioerr(&reply, what);
        assert_eq!(
            lrange(&mut c, src),
            src_items,
            "{what}: the source lost an element"
        );
        assert_eq!(
            lrange(&mut c, src2),
            src2_items,
            "{what}: LMPOP's later key lost an element"
        );
        assert_eq!(c.send(&["PING"]), "+PONG\r\n", "{what}: the fault leaked");
    }

    // Inside MULTI a BLMOVE is queued as LMOVE and executed at EXEC.
    assert_eq!(c.send(&["MULTI"]), "+OK\r\n");
    assert_eq!(
        c.send(&["BLMOVE", src, faulted, "LEFT", "LEFT", "0"]),
        "+QUEUED\r\n"
    );
    let exec = c.send(&["EXEC"]);
    eprintln!("[1225 s{shards}] MULTI BLMOVE: {:?}", exec.trim_end());
    assert!(
        exec.starts_with("*1\r\n-IOERR"),
        "MULTI/BLMOVE onto the faulted list: {exec:?}"
    );
    assert_eq!(
        lrange(&mut c, src),
        src_items,
        "MULTI/BLMOVE lost an element"
    );

    // Heal: the file comes back, the faulted list reads back UNCHANGED (no
    // refused command fabricated or shadowed it), and a move now succeeds.
    std::fs::rename(&parked, &file).unwrap();
    assert_eq!(
        lrange(&mut c, faulted),
        faulted_items
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        "the faulted list must come back with its original contents"
    );
    assert_eq!(
        c.send(&["LMOVE", src, faulted, "LEFT", "RIGHT"]),
        "$2\r\ns1\r\n"
    );
    assert_eq!(
        lrange(&mut c, faulted).last().map(String::as_str),
        Some("s1")
    );
    drop(server);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn list_moves_never_lose_an_element_1_shard() {
    list_moves_never_lose_an_element(1);
}

#[test]
fn list_moves_never_lose_an_element_4_shards() {
    list_moves_never_lose_an_element(4);
}
