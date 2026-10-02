//! R2b round 3: what a boot does with a damaged or foreign AOF.
//!
//! - **F-B** — a record torn by a crash at the end of the AOF (the flat
//!   `appendonly.aof`, the single-shard incr, a per-shard incr) is CUT before
//!   the writer appends: the writer used to append behind the torn bytes,
//!   whose declared length then swallowed every later record, so each write
//!   acknowledged after that boot was lost at the next one (reviewer TK: a
//!   `kill -9` during the append of a 400 MB `SET`; TT: an edited torn tail).
//!   The cut bytes are saved in `<file>.torn-<offset>`.
//! - **F-B** — mid-stream corruption (not a tail) refuses the boot and
//!   leaves the file as it was (reviewer E2: tokio served 12 of 20 keys and
//!   lost a later acknowledged write at the next boot; redis 7.2.7 refuses).
//!
//! Every server runs `--appendfsync always`: an acknowledged write is on disk.
//!
//! ```text
//! MOON_BIN=<moon> cargo test --release --test aof_boot_damage_r2b3 -- --include-ignored
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

fn boot_with(bin: &Path, dir: &Path, shards: usize, extra: &[&str]) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    common::spawn_listening_guarded(|port| {
        Command::new(bin)
            .args(["--port", &port.to_string(), "--shards", &shards.to_string()])
            .arg("--dir")
            .arg(dir)
            .args([
                "--disk-free-min-pct",
                "0",
                "--appendonly",
                "yes",
                "--appendfsync",
                "always",
                "--maxmemory",
                "0",
            ])
            .args(extra)
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon")
    })
}

fn boot(dir: &Path, shards: usize, extra: &[&str]) -> (common::ServerGuard, u16) {
    boot_with(&common::find_moon_binary(), dir, shards, extra)
}

fn ready(port: u16) -> Conn {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let mut c = Conn::open(port);
            if c.send(&["PING"]).starts_with("+PONG") {
                return c;
            }
        }
        assert!(Instant::now() < deadline, "moon never answered PING");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Boot `bin` on `dir` expecting it to refuse: returns its exit code and
/// stderr.
fn refused(bin: &Path, dir: &Path, args: &[&str]) -> (Option<i32>, String) {
    let mut child = Command::new(bin)
        .args(["--port", &common::reserve_port().to_string(), "--dir"])
        .arg(dir)
        .args(["--disk-free-min-pct", "0"])
        .args(args)
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon");
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        if Instant::now() >= deadline {
            common::sigkill(&mut child);
            panic!("moon booted (or hung) on {}", dir.display());
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
    (status.code(), log)
}

/// Every AOF file under `dir` (flat file, incr files), with its bytes.
fn aof_files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    aof_paths(dir)
        .into_iter()
        .map(|p| {
            let b = std::fs::read(&p).unwrap();
            (p, b)
        })
        .collect()
}

/// Every AOF file under `dir` (flat file, incr files).
fn aof_paths(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            let name = p.file_name().unwrap().to_string_lossy().to_string();
            if name == "appendonly.aof" || name.ends_with(".incr.aof") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

/// The AOF file that holds `needle`.
fn file_with(dir: &Path, needle: &[u8]) -> (PathBuf, Vec<u8>) {
    aof_files(dir)
        .into_iter()
        .find(|(_, b)| b.windows(needle.len()).any(|w| w == needle))
        .unwrap_or_else(|| panic!("no AOF file holds {:?}", String::from_utf8_lossy(needle)))
}

fn torn_sidecars(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.to_string_lossy().contains(".torn-") {
                out.push(p);
            }
        }
    }
    out
}

fn seed(port: u16) {
    let mut c = ready(port);
    for i in 1..=20 {
        assert_eq!(
            c.send(&["SET", &format!("k{i}"), &format!("v{i}")]),
            "+OK\r\n"
        );
    }
}

/// After the torn boot: a write acknowledged then must survive the next
/// crash and boot, with every earlier key.
fn later_write_survives(dir: &Path, shards: usize, extra: &[&str], what: &str) {
    let (mut srv, port) = boot(dir, shards, extra);
    let mut c = ready(port);
    assert_eq!(
        c.send(&["DBSIZE"]),
        ":20\r\n",
        "{what}: boot 2 keeps the prefix"
    );
    assert_eq!(c.send(&["SET", "after-torn", "acked"]), "+OK\r\n");
    assert_eq!(c.send(&["INCR", "ctr"]), ":1\r\n");
    drop(c);
    srv.kill_now();
    let (mut srv, port) = boot(dir, shards, extra);
    let mut c = ready(port);
    assert_eq!(
        c.send(&["GET", "after-torn"]),
        "$5\r\nacked\r\n",
        "{what}: the write acknowledged after the torn boot is kept"
    );
    assert_eq!(c.send(&["GET", "ctr"]), "$1\r\n1\r\n", "{what}");
    assert_eq!(c.send(&["DBSIZE"]), ":22\r\n", "{what}");
    drop(c);
    srv.kill_now();
    assert!(
        !torn_sidecars(dir).is_empty(),
        "{what}: the cut bytes are saved"
    );
}

/// TT: a torn record appended to the live AOF file after a crash.
fn edited_torn_tail(shards: usize, extra: &[&str]) {
    let dir = common::unique_test_dir(&format!("r2b3-tt-s{shards}"));
    let (mut srv, port) = boot(&dir, shards, extra);
    seed(port);
    srv.kill_now();
    let (path, _) = file_with(&dir, b"k20");
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\ntorn")
        .unwrap();
    later_write_survives(&dir, shards, extra, &format!("TT s{shards} {extra:?}"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// TK: a real `kill -9` while a large `SET` is being appended.
fn killed_during_a_large_append(shards: usize) {
    const BIG: usize = 128 << 20;
    let dir = common::unique_test_dir(&format!("r2b3-tk-s{shards}"));
    let (mut srv, port) = boot(&dir, shards, &[]);
    seed(port);
    let total = |d: &Path| -> u64 {
        aof_paths(d)
            .iter()
            .map(|p| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum()
    };
    let before = total(&dir);
    let sender = std::thread::spawn(move || {
        let mut s = std::net::TcpStream::connect(("127.0.0.1", port)).unwrap();
        let mut req = format!("*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n${BIG}\r\n").into_bytes();
        req.resize(req.len() + BIG, b'x');
        req.extend_from_slice(b"\r\n");
        let _ = s.write_all(&req);
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    while total(&dir) < before + (16 << 20) {
        assert!(
            Instant::now() < deadline,
            "the large SET never reached the AOF"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    srv.kill_now();
    let _ = sender.join();
    let grown = total(&dir) - before;
    assert!(
        grown < BIG as u64,
        "precondition: the kill landed inside the append ({grown} bytes written)"
    );
    later_write_survives(&dir, shards, &[], &format!("TK s{shards}"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// E2: one corrupt framing byte mid-file refuses the boot, untouched.
fn corruption_mid_file(shards: usize) {
    let dir = common::unique_test_dir(&format!("r2b3-e2-s{shards}"));
    let (mut srv, port) = boot(&dir, shards, &[]);
    seed(port);
    srv.kill_now();
    let needle = b"*3\r\n$3\r\nSET\r\n$3\r\nk13\r\n";
    let (path, mut bytes) = file_with(&dir, needle);
    let at = bytes
        .windows(needle.len())
        .position(|w| w == needle)
        .unwrap();
    bytes[at] = b'?';
    std::fs::write(&path, &bytes).unwrap();
    let shards_s = shards.to_string();
    let (code, log) = refused(
        &common::find_moon_binary(),
        &dir,
        &["--shards", &shards_s, "--appendonly", "yes"],
    );
    assert_ne!(code, Some(0), "E2 s{shards}: refused: {log:.3000}");
    assert!(
        log.to_lowercase().contains("refus"),
        "E2 s{shards}: the refusal says so: {log:.3000}"
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "E2 s{shards}: the damaged file is left as it was"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_torn_tail_is_cut_and_later_writes_survive_1_shard() {
    edited_torn_tail(1, &[]);
}

/// The v2 recovery path (`--disk-offload disable`).
#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_torn_tail_is_cut_and_later_writes_survive_1_shard_v2_recovery() {
    edited_torn_tail(1, &["--disk-offload", "disable"]);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_torn_tail_is_cut_and_later_writes_survive_4_shards() {
    edited_torn_tail(4, &[]);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_kill_during_a_large_append_keeps_later_writes_1_shard() {
    killed_during_a_large_append(1);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_kill_during_a_large_append_keeps_later_writes_4_shards() {
    killed_during_a_large_append(4);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn mid_file_corruption_refuses_the_boot_1_shard() {
    corruption_mid_file(1);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn mid_file_corruption_refuses_the_boot_4_shards() {
    corruption_mid_file(4);
}
