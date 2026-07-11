//! AOF multi-db attribution under interleaved connections (durability gate).
//!
//! Red condition this locks in: the live AOF used to persist the CLIENT's
//! literal `SELECT` commands (SELECT is W-flagged) while bare writes carried
//! no db context of their own. Two interleaved connections on different dbs
//! then corrupted recovery: `conn A (db0): SET a1` / `conn B: SELECT 2, SET
//! b1` / `conn A: SET a2` replays `a2` into db2 — the stream's last literal
//! SELECT wins, not the db the master actually executed in. Caught by the
//! v0.7 R1 consistency/durability gates (task #35); pre-existing since the
//! first multi-db AOF.
//!
//! The fix mirrors Redis: the AOF WRITER tracks the stream's current db and
//! prepends `SELECT <db>` whenever a record's execution db differs; client
//! SELECT commands are connection-state only and never persisted.
//!
//! Black-box over a real `moon` process (needs a prebuilt binary):
//!
//! ```text
//! MOON_BIN=./target/release/moon \
//!   cargo test --test aof_multidb_kill9 -- --ignored --nocapture
//! ```

#![cfg(any(feature = "runtime-monoio", feature = "runtime-tokio"))]

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn moon_bin() -> String {
    std::env::var("MOON_BIN").unwrap_or_else(|_| "./target/release/moon".to_string())
}

fn start_moon(port: u16, dir: &str, shards: usize) -> Child {
    Command::new(moon_bin())
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--dir",
            dir,
            "--appendonly",
            "yes",
            "--disk-free-min-pct",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
}

fn sigkill(child: &mut Child) {
    #[cfg(unix)]
    {
        let pid = child.id() as i32;
        // SAFETY: kill(2) with a pid we own from Child::id; SIGKILL is the
        // whole point of the crash test.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
        let _ = child.wait();
    }
    #[cfg(not(unix))]
    {
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// One persistent connection; sends inline commands and reads one reply each.
struct Conn {
    reader: BufReader<TcpStream>,
}

impl Conn {
    fn connect(addr: &str) -> Self {
        let stream = TcpStream::connect(addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("timeout");
        Conn {
            reader: BufReader::new(stream),
        }
    }

    fn cmd(&mut self, line: &str) -> String {
        self.reader
            .get_mut()
            .write_all(format!("{}\r\n", line).as_bytes())
            .expect("write");
        let mut reply = String::new();
        self.reader.read_line(&mut reply).expect("read");
        // Consume the bulk payload line for $-replies.
        if reply.starts_with('$') && !reply.starts_with("$-1") {
            let mut payload = String::new();
            self.reader.read_line(&mut payload).expect("read payload");
            return payload.trim_end().to_string();
        }
        reply.trim_end().to_string()
    }
}

fn wait_ready(addr: &str) {
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(addr) {
            drop(stream);
            let mut c = Conn::connect(addr);
            if c.cmd("PING") == "+PONG" {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("server at {addr} never became ready");
}

fn unique_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

/// Interleave db0/db2 writes from two connections, kill -9, restart, assert
/// every key recovered into the db it was written to.
fn run_case(shards: usize) {
    let port = unique_port();
    let addr = format!("127.0.0.1:{port}");
    let dir = tempfile::tempdir().expect("tempdir");
    let dir_path = dir.path().to_str().expect("utf8 dir").to_string();

    let mut server = start_moon(port, &dir_path, shards);
    wait_ready(&addr);

    let mut conn_a = Conn::connect(&addr); // stays on db0
    let mut conn_b = Conn::connect(&addr);
    assert_eq!(conn_b.cmd("SELECT 2"), "+OK");

    // Interleave so the AOF stream alternates db context between records.
    for i in 0..20 {
        assert_eq!(conn_a.cmd(&format!("SET a{i} v{i}")), "+OK");
        assert_eq!(conn_b.cmd(&format!("SET b{i} v{i}")), "+OK");
    }
    // The poison pattern from the gate: a db0 write AFTER db2 activity.
    assert_eq!(conn_a.cmd("SET tail v-tail"), "+OK");

    // everysec fsync window.
    std::thread::sleep(Duration::from_millis(1600));
    sigkill(&mut server);

    let mut server = start_moon(port, &dir_path, shards);
    wait_ready(&addr);

    let mut c0 = Conn::connect(&addr);
    let mut c2 = Conn::connect(&addr);
    assert_eq!(c2.cmd("SELECT 2"), "+OK");

    let db0 = c0.cmd("DBSIZE");
    let db2 = c2.cmd("DBSIZE");
    // Spot keys BEFORE the sizes so a failure message shows placement.
    assert_eq!(
        c0.cmd("GET tail"),
        "v-tail",
        "db0 tail write recovered into the wrong db (shards={shards}; db0={db0}, db2={db2})"
    );
    assert_eq!(c0.cmd("GET a0"), "v0", "a0 missing from db0");
    assert_eq!(c0.cmd("GET a19"), "v19", "a19 missing from db0");
    assert_eq!(c2.cmd("GET b0"), "v0", "b0 missing from db2");
    assert_eq!(c2.cmd("GET b19"), "v19", "b19 missing from db2");
    assert_eq!(c2.cmd("GET a5"), "$-1", "db0 key leaked into db2");
    assert_eq!(c0.cmd("GET b5"), "$-1", "db2 key leaked into db0");
    assert_eq!(db0, ":21", "db0 size after recovery (shards={shards})");
    assert_eq!(db2, ":20", "db2 size after recovery (shards={shards})");

    sigkill(&mut server);
}

/// TopLevel AOF writer (shards=1).
#[test]
#[ignore]
fn aof_multidb_kill9_recovers_toplevel() {
    run_case(1);
}

/// PerShard framed AOF writers (shards=2) — exercises the SPSC/wal_append
/// fanout leg's db threading as well.
#[test]
#[ignore]
fn aof_multidb_kill9_recovers_per_shard() {
    run_case(2);
}
