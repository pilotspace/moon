//! moon#1190 end to end: `UNLINK` of a large collection must not stall the
//! shard.
//!
//! Before the fix, `UNLINK` of a 1M-field hash on monoio walked the value for
//! its ledger cost and then dropped it inline — ~2M frees on the shard
//! thread, with every other connection on the shard waiting behind it, i.e.
//! exactly what `DEL` does. Now the command only unlinks the key and the
//! shard tick frees the value in bounded slices.
//!
//! Measured the way the issue asks: PING round-trip latency on the SAME shard
//! (`--shards 1`) while a 1M-field hash is removed, once with `DEL` (the
//! synchronous control, same binary, same run) and once with `UNLINK`. The
//! assertions are relative, so they hold on a debug or release binary and on
//! a loaded host: UNLINK's reply and the worst PING during the UNLINK window
//! must each be a small fraction of DEL's.
//!
//! Binary: `MOON_BIN` if set, else the binary cargo built for this invocation.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::Conn;

const FIELDS: usize = 1_000_000;
const PAIRS_PER_HSET: usize = 1_000;

struct Moon {
    _guard: common::ServerGuard,
    port: u16,
}

fn spawn_moon() -> Moon {
    let bin = common::find_moon_binary();
    let (guard, port) = common::spawn_listening_guarded(|port| {
        let dir = common::unique_test_dir(&format!("ws1-unlink-{port}"));
        let _ = std::fs::create_dir_all(&dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                "1",
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-offload",
                "disable",
                "--maxmemory",
                "0",
                "--disk-free-min-pct",
                "0",
                "--dir",
                dir.to_str().expect("utf8 dir"),
            ])
            .stdout(Stdio::null())
            .stderr(common::server_stderr(&dir))
            .spawn()
            .expect("spawn moon")
    });
    let mut probe = Conn::open(port);
    let info = probe.send(&["INFO", "server"]);
    assert!(
        info.contains("moon_version"),
        "port {port} is held by something that is not moon: {info:?}"
    );
    Moon {
        _guard: guard,
        port,
    }
}

/// Fill `key` with `FIELDS` fields via pipelined multi-pair HSETs.
fn fill_hash(c: &mut Conn, key: &str) {
    let fields: Vec<String> = (0..FIELDS).map(|i| format!("f{i:07}")).collect();
    for chunk in fields.chunks(PAIRS_PER_HSET * 16) {
        let cmds: Vec<Vec<&str>> = chunk
            .chunks(PAIRS_PER_HSET)
            .map(|pairs| {
                let mut cmd = vec!["HSET", key];
                for f in pairs {
                    cmd.push(f.as_str());
                    cmd.push("value-bytes");
                }
                cmd
            })
            .collect();
        let cmds: Vec<&[&str]> = cmds.iter().map(Vec::as_slice).collect();
        let reply = c.pipeline(&cmds);
        assert!(
            !reply.contains('-'),
            "HSET failed: {}",
            &reply[..reply.len().min(200)]
        );
    }
    let len = c.send(&["HLEN", key]);
    assert_eq!(len.trim(), format!(":{FIELDS}"), "fixture hash incomplete");
}

/// Remove `key` with `cmd` while a second connection PINGs in a loop.
/// Returns (reply latency of `cmd`, worst PING round trip in the window).
fn remove_under_ping_load(port: u16, cmd: &str, key: &str) -> (Duration, Duration) {
    let stop = Arc::new(AtomicBool::new(false));
    let pinger = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
            s.set_nodelay(true).expect("nodelay");
            let mut worst = Duration::ZERO;
            let mut buf = [0u8; 7];
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                s.write_all(b"*1\r\n$4\r\nPING\r\n").expect("ping");
                s.read_exact(&mut buf).expect("pong");
                assert_eq!(&buf, b"+PONG\r\n", "not a PONG");
                worst = worst.max(t.elapsed());
            }
            worst
        })
    };
    // Let the pinger reach steady state, then remove, then keep pinging long
    // enough to cover the lazy drain (a 1M-field hash is ~0.25 s of slices on
    // release, longer on debug).
    std::thread::sleep(Duration::from_millis(200));
    let mut c = Conn::open(port);
    let t = Instant::now();
    let reply = c.send(&[cmd, key]);
    let cmd_latency = t.elapsed();
    assert_eq!(reply.trim(), ":1", "{cmd} did not remove the key");
    std::thread::sleep(Duration::from_millis(1_500));
    stop.store(true, Ordering::Relaxed);
    let worst_ping = pinger.join().expect("pinger");
    (cmd_latency, worst_ping)
}

#[test]
fn unlink_of_a_million_field_hash_does_not_stall_the_shard() {
    let moon = spawn_moon();
    let mut c = Conn::open(moon.port);
    fill_hash(&mut c, "big:del");
    fill_hash(&mut c, "big:unlink");

    let (del_ms, del_ping) = remove_under_ping_load(moon.port, "DEL", "big:del");
    let (unlink_ms, unlink_ping) = remove_under_ping_load(moon.port, "UNLINK", "big:unlink");
    eprintln!(
        "moon#1190: DEL {del_ms:?} (worst PING {del_ping:?}) vs UNLINK {unlink_ms:?} \
         (worst PING {unlink_ping:?}) on a {FIELDS}-field hash"
    );

    assert!(
        del_ms >= Duration::from_millis(20),
        "control too fast to discriminate: DEL of {FIELDS} fields took {del_ms:?}"
    );
    assert!(
        unlink_ms * 5 < del_ms,
        "UNLINK ({unlink_ms:?}) is not O(1): DEL of the same hash took {del_ms:?}"
    );
    assert!(
        unlink_ping * 3 < del_ping,
        "worst same-shard PING during UNLINK ({unlink_ping:?}) is not a small fraction of \
         the DEL window's ({del_ping:?}) — the free is still inline"
    );
    // Memory is eventually released and credited.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let info = c.send(&["INFO", "memory"]);
        let used: usize = info
            .lines()
            .find_map(|l| l.strip_prefix("used_memory:"))
            .and_then(|v| v.trim().parse().ok())
            .expect("used_memory in INFO");
        if used < 16 * 1024 * 1024 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "used_memory still {used} bytes 30 s after UNLINK — the lazy free never finished"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}
