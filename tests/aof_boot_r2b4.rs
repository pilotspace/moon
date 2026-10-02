//! R2b round 4: boot/replay fixes, against real servers.
//!
//! - **F2** — a write whose logged effect names more items than the client
//!   protocol's element cap (`SPOP s 1060000` was logged as ONE `SREM` of
//!   1,060,000 members) made the next boot refuse the AOF as "mid-file
//!   corruption" (and the truncation it advised dropped every later write).
//!   The effect is now logged in chunks and the replay parser has no element
//!   cap; the restart keeps the pop and the later write.
//!
//! - **F5** — a boot that failed after its AOF writers opened the files and
//!   before the replay cut a torn tail appended `MOON.TS … CLOSE` behind the
//!   torn bytes (every later boot then refused the file). A failed boot now
//!   leaves the AOF as it found it.
//!
//! ```text
//! MOON_BIN=<moon> cargo test --release --test aof_boot_r2b4 -- --include-ignored
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

mod common;

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::Conn;

fn boot(dir: &Path, shards: usize) -> (common::ServerGuard, u16) {
    std::fs::create_dir_all(dir).unwrap();
    let bin = common::find_moon_binary();
    common::spawn_listening_guarded(|port| {
        Command::new(&bin)
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
            .env("MOON_DISK_FREE_MIN_PCT", "0")
            .stdout(Stdio::null())
            .stderr(common::server_stderr(dir))
            .spawn()
            .expect("spawn moon (set MOON_BIN)")
    })
}

fn ready(port: u16) -> Conn {
    let deadline = Instant::now() + Duration::from_secs(120);
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

/// F2: SADD 1.1M members, SPOP 1,060,000, SET after, kill -9, restart.
fn large_spop_survives_a_restart(shards: usize) {
    const MEMBERS: usize = 1_100_000;
    const POP: usize = 1_060_000;
    let dir = common::unique_test_dir(&format!("r2b4-spop-s{shards}"));
    let (mut srv, port) = boot(&dir, shards);
    let mut c = ready(port);
    let mut names: Vec<String> = Vec::with_capacity(10_002);
    for start in (0..MEMBERS).step_by(10_000) {
        names.clear();
        names.push("SADD".into());
        names.push("{s}set".into());
        names.extend((start..start + 10_000).map(|i| format!("m{i}")));
        let parts: Vec<&str> = names.iter().map(String::as_str).collect();
        assert_eq!(c.send(&parts), ":10000\r\n");
    }
    let popped = c.send_within(
        &["SPOP", "{s}set", &POP.to_string()],
        Duration::from_secs(120),
    );
    assert!(popped.starts_with(&format!("*{POP}\r\n")), "SPOP answered");
    assert_eq!(c.send(&["SET", "after", "1"]), "+OK\r\n");
    drop(c);
    srv.kill_now();

    let (mut srv, port) = boot(&dir, shards);
    let mut c = ready(port);
    assert_eq!(
        c.send(&["SCARD", "{s}set"]),
        format!(":{}\r\n", MEMBERS - POP),
        "s{shards}: the pop survives the restart"
    );
    assert_eq!(
        c.send(&["GET", "after"]),
        "$1\r\n1\r\n",
        "s{shards}: the write after it survives"
    );
    drop(c);
    srv.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_huge_spop_replays_after_a_restart_1_shard() {
    large_spop_survives_a_restart(1);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_huge_spop_replays_after_a_restart_4_shards() {
    large_spop_survives_a_restart(4);
}

/// Every AOF file under `dir` (flat file, incr files), with its bytes.
fn aof_files(dir: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
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
                let b = std::fs::read(&p).unwrap();
                out.push((p, b));
            }
        }
    }
    out.sort();
    out
}

/// Boot `dir` expecting a refusal: the exit code.
fn refused(dir: &Path, shards: usize) -> Option<i32> {
    let mut child = Command::new(common::find_moon_binary())
        .args(["--port", &common::reserve_port().to_string(), "--dir"])
        .arg(dir)
        .args([
            "--shards",
            &shards.to_string(),
            "--disk-free-min-pct",
            "0",
            "--appendonly",
            "yes",
        ])
        .env("MOON_DISK_FREE_MIN_PCT", "0")
        .stdout(Stdio::null())
        .stderr(common::server_stderr(dir))
        .spawn()
        .expect("spawn moon");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(s) = child.try_wait().unwrap() {
            return s.code();
        }
        if Instant::now() >= deadline {
            common::sigkill(&mut child);
            panic!("moon booted (or hung) on {}", dir.display());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// F5: a boot that fails after its writers opened the AOF, and before the
/// replay cut the torn tail (here: an unprovable cold file-id seed), used to
/// append `MOON.TS … CLOSE` behind the torn bytes — every later boot then
/// refused the file as mid-file corruption. Now the failed boot leaves the
/// AOF as it found it, and the next good boot cuts the tail and keeps the
/// later writes.
fn failed_boot_appends_nothing(shards: usize) {
    let dir = common::unique_test_dir(&format!("r2b4-f5-s{shards}"));
    let (mut srv, port) = boot(&dir, shards);
    let mut c = ready(port);
    for i in 0..10 {
        assert_eq!(c.send(&["SET", &format!("k{i}"), "v"]), "+OK\r\n");
    }
    drop(c);
    srv.kill_now();
    let (path, _) = aof_files(&dir)
        .into_iter()
        .find(|(_, b)| b.windows(2).any(|w| w == b"k9"))
        .expect("the AOF that holds k9");
    use std::io::Write as _;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"*3\r\n$3\r\nSE")
        .unwrap();
    // An unprovable cold file-id seed: shard-0/data is a regular file.
    let data = dir.join("shard-0").join("data");
    let aside = dir.join("shard-0").join("data.aside");
    if data.exists() {
        std::fs::rename(&data, &aside).unwrap();
    }
    std::fs::write(&data, b"not a directory").unwrap();
    let before = aof_files(&dir);
    let code = refused(&dir, shards);
    assert_ne!(code, Some(0), "s{shards}: the boot failed");
    assert_eq!(
        aof_files(&dir),
        before,
        "s{shards}: the failed boot appended nothing to the AOF"
    );
    std::fs::remove_file(&data).unwrap();
    if aside.exists() {
        std::fs::rename(&aside, &data).unwrap();
    }
    let (mut srv, port) = boot(&dir, shards);
    let mut c = ready(port);
    assert_eq!(c.send(&["DBSIZE"]), ":10\r\n", "s{shards}");
    assert_eq!(c.send(&["SET", "after", "1"]), "+OK\r\n");
    drop(c);
    srv.kill_now();
    let (mut srv, port) = boot(&dir, shards);
    let mut c = ready(port);
    assert_eq!(c.send(&["GET", "after"]), "$1\r\n1\r\n", "s{shards}");
    assert_eq!(c.send(&["DBSIZE"]), ":11\r\n", "s{shards}");
    drop(c);
    srv.kill_now();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_failed_boot_leaves_a_torn_aof_as_it_found_it_1_shard() {
    failed_boot_appends_nothing(1);
}

#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_failed_boot_leaves_a_torn_aof_as_it_found_it_4_shards() {
    failed_boot_appends_nothing(4);
}

/// F6: a dir holding BOTH a manifest with data and a flat `appendonly.aof`
/// with data (what the pre-fix F-H bug left: a stale monoio manifest and a
/// newer tokio-era file) is refused, and neither file is retired — the
/// previous advice ("boot it with the monoio build") retired the flat file
/// and its writes.
#[test]
#[ignore = "spawns moon; set MOON_BIN"]
fn a_dir_with_a_manifest_and_a_flat_file_with_data_is_refused_untouched() {
    for shards in [1usize, 4] {
        let dir = common::unique_test_dir(&format!("r2b4-f6-s{shards}"));
        let (mut srv, port) = boot(&dir, shards);
        let mut c = ready(port);
        assert_eq!(c.send(&["SET", "manifest-era", "1"]), "+OK\r\n");
        drop(c);
        srv.kill_now();
        assert!(dir.join("appendonlydir").exists(), "a manifest exists");
        // The tokio-era flat file (a single-shard dir only has one at s1;
        // the s4 case stands for any leftover file holding data).
        std::fs::write(
            dir.join("appendonly.aof"),
            b"*3\r\n$3\r\nSET\r\n$9\r\nflat-era1\r\n$1\r\n1\r\n",
        )
        .unwrap();
        let before = aof_files(&dir);
        let code = refused(&dir, shards);
        assert_eq!(code, Some(2), "s{shards}");
        let log = std::fs::read_to_string(dir.join("server.err")).unwrap_or_default();
        assert!(log.contains("BOTH"), "s{shards}: {log:.2000}");
        assert_eq!(
            aof_files(&dir),
            before,
            "s{shards}: nothing retired or written"
        );
        assert!(dir.join("appendonly.aof").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
