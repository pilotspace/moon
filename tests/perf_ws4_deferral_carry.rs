//! moon#1179 item 5: a pipeline deferral carries the PARSED tail into the next
//! batch instead of re-encoding it into the read buffer and re-parsing it.
//!
//! Deferrals (#438 early-flush commands, #507 commands that must wait for this
//! batch's pending remote writes) used to serialize every unconsumed frame back
//! in front of `read_buf` on EVERY deferral — ~n²/2d frame round trips for an
//! n-frame batch deferring every d commands. The frames are now kept as they
//! are. This suite pins that the replies are exactly what in-order execution
//! gives, with the deferral path demonstrably exercised (the moon#507 counter
//! moves), including a carried tail followed by a command whose bytes arrive
//! in a later write, and transactions and a blocking pop inside the tail.
//!
//! Runs against whichever runtime the binary was built with: the monoio
//! handler by default, the tokio one under `--no-default-features --features
//! runtime-tokio,jemalloc`.

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
        let tmp_dir = std::env::temp_dir().join(format!("moon-ws4-defer-{port}"));
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
    let tmp_dir = std::env::temp_dir().join(format!("moon-ws4-defer-{port}"));
    let moon = Moon {
        child,
        port,
        tmp_dir,
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", moon.port)).is_ok()
            && Conn::open(moon.port).send(&["PING"]) == "+PONG\r\n"
        {
            return moon;
        }
        assert!(Instant::now() < deadline, "moon never became ready");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn defers(port: u16) -> u64 {
    let info = Conn::open(port).send(&["INFO", "stats"]);
    info.split("\r\n")
        .find_map(|l| l.strip_prefix("total_pipeline_remote_defer:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("INFO stats has no total_pipeline_remote_defer: {info:?}"))
}

fn bulk(v: &str) -> String {
    format!("${}\r\n{v}\r\n", v.len())
}

/// A long pipeline deferring every other command: `SET` to keys spread over 4
/// shards, each followed by `DBSIZE`, which must wait for the pending remote
/// writes (#507) and therefore cuts the batch. Every `DBSIZE` must see exactly
/// the writes before it.
#[test]
fn deferral_every_other_command_answers_in_order() {
    let moon = spawn_moon("4");
    let mut c = Conn::open(moon.port);
    assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n");
    let base = defers(moon.port);

    let n = 400usize;
    let keys: Vec<String> = (0..n).map(|i| format!("dk:{i}")).collect();
    let vals: Vec<String> = (0..n).map(|i| format!("v{i}")).collect();
    let mut cmds: Vec<Vec<&str>> = Vec::with_capacity(2 * n);
    let mut want = String::new();
    for i in 0..n {
        cmds.push(vec!["SET", &keys[i], &vals[i]]);
        cmds.push(vec!["DBSIZE"]);
        want.push_str("+OK\r\n");
        want.push_str(&format!(":{}\r\n", i + 1));
    }
    let slices: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
    assert_eq!(c.pipeline(&slices), want);

    let fired = defers(moon.port) - base;
    assert!(
        fired >= (n as u64) / 4,
        "only {fired} deferrals for {n} SET+DBSIZE pairs over 4 shards — the \
         carried-tail path was not exercised, so this test would prove nothing"
    );
}

/// Carried tail + the rest of the batch arriving LATER: the pipeline's last
/// command is cut mid-frame; its remainder is written after the deferred tail
/// has been carried. Order and content must be unchanged.
#[test]
fn carried_tail_then_split_command() {
    let moon = spawn_moon("4");
    let mut c = Conn::open(moon.port);
    assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n");
    let mut first = Vec::new();
    let mut want = String::new();
    for i in 0..64 {
        first.extend_from_slice(&encode(&["SET", &format!("sk:{i}"), "x"]));
        first.extend_from_slice(&encode(&["DBSIZE"]));
        first.extend_from_slice(&encode(&["ECHO", &format!("e{i}")]));
        want.push_str("+OK\r\n");
        want.push_str(&format!(":{}\r\n", i + 1));
        want.push_str(&bulk(&format!("e{i}")));
    }
    let tail = encode(&["MGET", "sk:0", "sk:63", "nokey"]);
    let (head, rest) = tail.split_at(tail.len() / 2);
    first.extend_from_slice(head);
    c.sock.write_all(&first).expect("write");
    std::thread::sleep(Duration::from_millis(50));
    c.sock.write_all(rest).expect("write");
    want.push_str(&format!("*3\r\n{}{}$-1\r\n", bulk("x"), bulk("x")));
    assert_eq!(c.read_replies(64 * 3 + 1), want);
}

/// A transaction and a blocking pop inside a deferred tail: MULTI queues and
/// EXEC runs in order; BLPOP (a #438 early-flush command) behind pending
/// remote writes defers and then pops the element pushed earlier in the batch.
#[test]
fn transaction_and_blocking_pop_in_a_carried_tail() {
    let moon = spawn_moon("4");
    let mut c = Conn::open(moon.port);
    assert_eq!(c.send(&["FLUSHALL"]), "+OK\r\n");
    // MULTI keys share a hash tag (moon refuses a cross-shard EXEC); the
    // untagged `x:*` writes are what leave remote work pending.
    let got = c.pipeline(&[
        &["SET", "{t}a", "1"],
        &["SET", "{t}b", "2"],
        &["SET", "x:1", "1"],
        &["SET", "x:2", "2"],
        &["DBSIZE"],
        &["MULTI"],
        &["INCR", "{t}a"],
        &["GET", "{t}b"],
        &["EXEC"],
        &["RPUSH", "{t}list", "elem"],
        &["SET", "x:3", "3"],
        &["BLPOP", "{t}list", "1"],
        &["DBSIZE"],
        &["PING"],
    ]);
    let want = format!(
        "+OK\r\n+OK\r\n+OK\r\n+OK\r\n:4\r\n+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n:2\r\n{}:1\r\n+OK\r\n*2\r\n{}{}:5\r\n+PONG\r\n",
        bulk("2"),
        bulk("{t}list"),
        bulk("elem"),
    );
    assert_eq!(got, want);
}
