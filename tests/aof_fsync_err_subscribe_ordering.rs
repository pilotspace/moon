//! FIX-W2-4 r3 — behavioral test: AOF_FSYNC_ERR canonical constant propagates
//! to the wire under appendfsync=always when a write precedes SUBSCRIBE.
//!
//! This test exercises the SUBSCRIBE ordering fix from handler_single.rs:875-918
//! (and the parallel paths in handler_monoio / handler_sharded):
//!
//!   1. Connect to a server with `--appendonly yes --appendfsync always`.
//!   2. Issue `SET k v` (a write that must be fsync-ack'd before +OK).
//!   3. Issue `SUBSCRIBE chan` on the same connection immediately after.
//!   4. The writer returns FsyncFailed (injected via MOON_TEST_AOF_FSYNC_FAIL=1).
//!
//! Expected outcome: the first response received by the client is exactly the
//! canonical `ERR AOF fsync failed; write not durable` string — NOT +OK followed
//! by the subscribe confirmation. This proves:
//!   (a) The AOF_FSYNC_ERR constant is used (not a hard-coded divergent string).
//!   (b) The WRITEFAIL response lands BEFORE the SUBSCRIBE slot (ordering fix).
//!
//! Run:
//!   cargo build --release
//!   cargo test --release --test aof_fsync_err_subscribe_ordering -- --ignored
//!
//! Requires the release binary at ./target/release/moon.
//! The MOON_TEST_AOF_FSYNC_FAIL=1 env var is passed to the child process.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use std::{fs, thread};

/// Start moon with fault-injected AOF fsync failure. Returns (child, dir).
fn spawn_moon_with_fsync_fail(port: u16, shards: u16) -> (Child, PathBuf) {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "moon-w24-subscribe-{}-{}-{}",
        std::process::id(),
        port,
        nanos
    ));
    fs::create_dir_all(&dir).expect("create temp dir");

    let stderr_log = dir.join("moon.stderr.log");
    let stdout_log = dir.join("moon.stdout.log");

    let child = Command::new("./target/release/moon")
        .args([
            "--port",
            &port.to_string(),
            "--shards",
            &shards.to_string(),
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
            "--dir",
        ])
        .arg(&dir)
        .env("MOON_TEST_AOF_FSYNC_FAIL", "1")
        .env("RUST_LOG", "warn")
        .stdout(fs::File::create(&stdout_log).expect("create stdout log"))
        .stderr(fs::File::create(&stderr_log).expect("create stderr log"))
        .spawn()
        .expect("spawn moon — run `cargo build --release` first");

    (child, dir)
}

/// Wait until the port is accepting connections, or panic after timeout.
fn wait_for_port(port: u16, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!("moon did not accept connections on port {port} within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Send a raw RESP command over a TCP stream without waiting for a response.
fn send_resp(stream: &mut TcpStream, args: &[&str]) {
    let mut buf = format!("*{}\r\n", args.len());
    for arg in args {
        buf.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    stream.write_all(buf.as_bytes()).expect("write RESP command");
}

/// Read one complete RESP response (simple, error, or bulk) from the stream.
/// Returns the raw first line (e.g. "+OK", "-ERR ...", ":3", etc.).
fn read_resp_first_line(reader: &mut BufReader<TcpStream>) -> String {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("read RESP response line");
    line.trim_end_matches("\r\n").to_string()
}

/// Core assertion: SET + SUBSCRIBE with AOF fault injection must yield
/// the canonical AOF_FSYNC_ERR as the first response, not +OK.
///
/// Tests both single-shard (handler_single path) and the sharded paths.
fn assert_aof_fsync_err_before_subscribe_ok(port: u16, shards: u16) {
    let (mut child, dir) = spawn_moon_with_fsync_fail(port, shards);

    // Give moon up to 5 s to bind the port.
    wait_for_port(port, Duration::from_secs(5));

    let result = std::panic::catch_unwind(|| {
        let stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .ok();
        let stream_clone = stream.try_clone().expect("clone stream");
        let mut reader = BufReader::new(stream_clone);
        let mut writer = stream;

        // Pipeline SET then SUBSCRIBE on the same connection.
        // Under appendfsync=always the handler must await the fsync ack
        // for SET before flushing +OK, and before the SUBSCRIBE response.
        send_resp(&mut writer, &["SET", "testkey", "testval"]);
        send_resp(&mut writer, &["SUBSCRIBE", "testchan"]);

        // Read the first response — must be the AOF error, NOT +OK.
        let first = read_resp_first_line(&mut reader);

        // The canonical constant value: "ERR AOF fsync failed; write not durable"
        // A `-` prefix means it's a RESP error frame.
        assert!(
            first.starts_with('-'),
            "first response must be a RESP error frame (starts with '-'); got: {first:?}"
        );
        assert!(
            first.contains("AOF fsync failed"),
            "error frame must contain 'AOF fsync failed' (canonical AOF_FSYNC_ERR); got: {first:?}"
        );
        assert!(
            first.contains("write not durable"),
            "error frame must contain 'write not durable'; got: {first:?}"
        );
    });

    child.kill().ok();
    child.wait().ok();
    fs::remove_dir_all(&dir).ok();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

/// Single-shard path (handler_single): SET write followed by SUBSCRIBE.
/// The AOF_FSYNC_ERR must be the first response, proving the WRITEFAIL
/// frame lands before the SUBSCRIBE confirmation slot.
#[test]
#[ignore]
fn aof_fsync_err_propagates_before_subscribe_single_shard() {
    assert_aof_fsync_err_before_subscribe_ok(17501, 1);
}

/// Multi-shard path (handler_sharded): same ordering guarantee under PerShard
/// AOF layout. Uses --shards 2 so the PerShard writer path is exercised.
#[test]
#[ignore]
fn aof_fsync_err_propagates_before_subscribe_multi_shard() {
    assert_aof_fsync_err_before_subscribe_ok(17502, 2);
}
