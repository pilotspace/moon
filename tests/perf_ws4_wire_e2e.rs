//! End-to-end wire-path checks for the WS4 changes (moon#1164, moon#1179):
//! resumable parsing, reads sized from the parse hint, direct reads into the
//! connection read buffer (monoio), and the batch-end shrink hysteresis.
//!
//! Every case drives a real server over a raw socket, because what is under
//! test is how bytes arrive: in one write, in many, or one at a time. The same
//! file runs against whichever runtime the binary was built with, so the
//! monoio (default) and tokio (`--no-default-features --features
//! runtime-tokio,jemalloc`) handlers are both covered.

mod common;

use common::{Conn, encode};

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Moon {
    child: Child,
    port: u16,
    tmp_dir: std::path::PathBuf,
}

impl Drop for Moon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.tmp_dir);
    }
}

fn spawn_moon(shards: &str) -> Moon {
    let bin = std::path::PathBuf::from(env!("CARGO_BIN_EXE_moon"));
    let (child, port) = common::spawn_listening(|port| {
        let tmp_dir = std::env::temp_dir().join(format!("moon-ws4-wire-{port}"));
        let _ = std::fs::create_dir_all(&tmp_dir);
        Command::new(&bin)
            .args([
                "--port",
                &port.to_string(),
                "--shards",
                shards,
                "--admin-port",
                "0",
                "--appendonly",
                "no",
                "--disk-free-min-pct",
                "0",
                "--dir",
                tmp_dir.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn moon")
    });
    let tmp_dir = std::env::temp_dir().join(format!("moon-ws4-wire-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(sock) = std::net::TcpStream::connect(("127.0.0.1", moon.port)) {
            drop(sock);
            let mut c = Conn::open(moon.port);
            if c.send(&["PING"]) == "+PONG\r\n" {
                return moon;
            }
        }
        assert!(Instant::now() < deadline, "moon never became ready");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn bulk(v: &str) -> String {
    format!("${}\r\n{v}\r\n", v.len())
}

/// moon#1164: one large multibulk arriving in 64 KiB writes — the review's
/// reproduction — is answered, and the element count is exact.
#[test]
fn large_multibulk_in_64k_writes_is_answered() {
    let moon = spawn_moon("2");
    let mut c = Conn::open(moon.port);
    let n = 200_000usize;
    let mut payload = format!("*{}\r\n$5\r\nRPUSH\r\n$4\r\nbigl\r\n", n + 2).into_bytes();
    for i in 0..n {
        let e = format!("{}", i % 10);
        payload.extend_from_slice(format!("${}\r\n{e}\r\n", e.len()).as_bytes());
    }
    for chunk in payload.chunks(64 * 1024) {
        c.sock.write_all(chunk).expect("write");
    }
    assert_eq!(c.read_replies(1), format!(":{n}\r\n"));
    assert_eq!(c.send(&["LLEN", "bigl"]), format!(":{n}\r\n"));
    assert_eq!(c.send(&["LINDEX", "bigl", "123457"]), bulk("7"));
}

/// Generic-path (5-argument, never inlined) SETs of large values, one per
/// batch and back to back — the regrow/shrink pattern the hysteresis targets —
/// plus a 1 MiB value, all read back byte-exact.
#[test]
fn generic_path_large_values_round_trip() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    for i in 0..24 {
        let v: String = std::iter::repeat_n(char::from(b'a' + (i % 26) as u8), 65_536).collect();
        let k = format!("big:{i}");
        assert_eq!(c.send(&["SET", &k, &v, "EX", "100"]), "+OK\r\n");
    }
    for i in 0..24 {
        let v: String = std::iter::repeat_n(char::from(b'a' + (i % 26) as u8), 65_536).collect();
        assert_eq!(c.send(&["GET", &format!("big:{i}")]), bulk(&v));
    }
    let huge: String = (0..(1 << 20))
        .map(|i| char::from(b'A' + (i % 23) as u8))
        .collect();
    assert_eq!(c.send(&["SET", "huge", &huge, "EX", "100"]), "+OK\r\n");
    assert_eq!(c.send(&["STRLEN", "huge"]), format!(":{}\r\n", 1 << 20));
    assert_eq!(c.send(&["GETRANGE", "huge", "1000000", "1000004"]), {
        let s: String = (1_000_000..=1_000_004)
            .map(|i| char::from(b'A' + (i % 23) as u8))
            .collect();
        bulk(&s)
    });
}

/// A command delivered ONE BYTE PER WRITE: every read is a parse attempt on a
/// one-byte-longer prefix — the resumable parser's worst case for staleness.
#[test]
fn command_dribbled_one_byte_per_write() {
    let moon = spawn_moon("1");
    let mut c = Conn::open(moon.port);
    c.sock.set_nodelay(true).expect("nodelay");
    let mut cmd = encode(&["RPUSH", "drib", "a", "bb", "ccc", "dddd", "eeeee"]);
    cmd.extend_from_slice(&encode(&["LRANGE", "drib", "0", "-1"]));
    for b in &cmd {
        c.sock.write_all(std::slice::from_ref(b)).expect("write");
        std::thread::sleep(Duration::from_micros(200));
    }
    let replies = c.read_replies(2);
    assert_eq!(
        replies,
        format!(
            ":5\r\n*5\r\n{}{}{}{}{}",
            bulk("a"),
            bulk("bb"),
            bulk("ccc"),
            bulk("dddd"),
            bulk("eeeee")
        )
    );
}

/// A pipeline much larger than one read, mixing inline-eligible (`SET k v`,
/// `GET k`) and generic (`SET k v EX`) commands across two shards, answered in
/// order.
#[test]
fn pipeline_larger_than_a_read_is_answered_in_order() {
    let moon = spawn_moon("2");
    let mut c = Conn::open(moon.port);
    let n = 3000usize;
    let mut cmds: Vec<Vec<String>> = Vec::with_capacity(3 * n);
    for i in 0..n {
        let k = format!("pk:{i}");
        let v = format!("value-{i}-{}", "x".repeat(i % 40));
        if i % 2 == 0 {
            cmds.push(vec!["SET".into(), k.clone(), v.clone()]);
        } else {
            cmds.push(vec![
                "SET".into(),
                k.clone(),
                v.clone(),
                "EX".into(),
                "100".into(),
            ]);
        }
        cmds.push(vec!["GET".into(), k]);
    }
    let refs: Vec<Vec<&str>> = cmds
        .iter()
        .map(|c| c.iter().map(|s| s.as_str()).collect())
        .collect();
    let slices: Vec<&[&str]> = refs.iter().map(|c| c.as_slice()).collect();
    let got = c.pipeline(&slices);
    let mut want = String::new();
    for i in 0..n {
        want.push_str("+OK\r\n");
        want.push_str(&bulk(&format!("value-{i}-{}", "x".repeat(i % 40))));
    }
    assert_eq!(got, want);
}
