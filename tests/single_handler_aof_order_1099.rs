//! moon#1099 — `handler_single` (the library entry that
//! `listener::run_with_shutdown` and `moon::server::handle_connection` drive)
//! must log a write in the stretch that applies it.
//!
//! It used to buffer a pipelined batch's AOF records and send them after the
//! batch. Another connection's write that was applied later, but whose batch
//! finished first, was then logged first, and replay put the older value back.
//!
//! Each test writes through a real in-process server with `appendonly yes`,
//! restarts on the same directory, and compares the replayed value with the
//! value the live server held just before shutdown.
//!
//! Requires `runtime-tokio` (`run_with_shutdown` exists only there).
#![cfg(feature = "runtime-tokio")]

use std::path::Path;
use std::time::Duration;

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::server::listener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod common;

async fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

/// Start `run_with_shutdown` on `dir`; returns the port, the token, and the
/// server task so the caller can wait for a clean stop before restarting.
async fn start(
    dir: &Path,
    appendfsync: &str,
) -> (u16, CancellationToken, tokio::task::JoinHandle<()>) {
    let port = free_port().await;
    let token = CancellationToken::new();
    let server_token = token.clone();
    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        appendonly: "yes".to_string(),
        appendfsync: appendfsync.to_string(),
        dir: dir.to_string_lossy().to_string(),
        appendfilename: "appendonly.aof".to_string(),
        dbfilename: "dump.rdb".to_string(),
        save: None,
        shards: 0,
        protected_mode: "no".to_string(),
        disk_offload: "disable".to_string(),
        disk_free_min_pct: 0,
        maxmemory: Some(0),
        ..Default::default()
    };
    let task = tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });
    common::await_listening(port, Duration::from_secs(10))
        .await
        .unwrap();
    (port, token, task)
}

async fn stop(token: CancellationToken, task: tokio::task::JoinHandle<()>) {
    token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    // The AOF writer is its own task; give it a moment to finish its final sync.
    tokio::time::sleep(Duration::from_millis(200)).await;
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).unwrap();
    let cfg = redis::AsyncConnectionConfig::new()
        .set_response_timeout(Some(Duration::from_secs(30)));
    client
        .get_multiplexed_async_connection_with_config(&cfg)
        .await
        .unwrap()
}

async fn get(port: u16, key: &str) -> Option<String> {
    let mut c = connect(port).await;
    redis::cmd("GET").arg(key).query_async(&mut c).await.unwrap()
}

fn resp(args: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n{}\r\n", a.len(), a).as_bytes());
    }
    out
}

/// Read from `s` until the buffer holds `n` complete top-level RESP lines
/// counted the cheap way: the replies used here are `+OK`, `+QUEUED`, one
/// one-element array (`*1` + `+OK`) and an integer, i.e. `n` CRLF-terminated
/// lines in total.
async fn read_lines(s: &mut TcpStream, n: usize) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while buf.windows(2).filter(|w| w == b"\r\n").count() < n {
        let r = tokio::time::timeout_at(deadline, s.read(&mut chunk))
            .await
            .expect("reply within 15s")
            .unwrap();
        assert!(r > 0, "server closed the connection: {:?}", String::from_utf8_lossy(&buf));
        buf.extend_from_slice(&chunk[..r]);
    }
    String::from_utf8_lossy(&buf).into_owned()
}

/// An await inside the batch: `MULTI / SET k 1 / EXEC / WAIT 1 1500` arrive
/// in one read. EXEC applies `SET k 1`, then WAIT parks for 1.5s (no replica
/// ever acks). A second connection's `SET k 2` lands in that window and is
/// acknowledged under `appendfsync always`. The live value is 2, so the log
/// must replay to 2.
#[tokio::test]
async fn exec_then_wait_logs_the_exec_before_a_later_write() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    let (port, token, task) = start(dir, "always").await;

    let mut a = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    let mut batch = resp(&["MULTI"]);
    batch.extend(resp(&["SET", "k", "1"]));
    batch.extend(resp(&["EXEC"]));
    batch.extend(resp(&["WAIT", "1", "1500"]));
    a.write_all(&batch).await.unwrap();

    // Let EXEC run and WAIT park.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut b = connect(port).await;
    let ok: String = redis::cmd("SET")
        .arg("k")
        .arg("2")
        .query_async(&mut b)
        .await
        .unwrap();
    assert_eq!(ok, "OK");

    // +OK, +QUEUED, *1, +OK, :0
    let replies = read_lines(&mut a, 5).await;
    assert!(replies.ends_with(":0\r\n"), "WAIT reply: {replies:?}");

    let live = get(port, "k").await;
    assert_eq!(live.as_deref(), Some("2"), "live value before restart");
    drop(a);
    drop(b);
    stop(token, task).await;

    let (port2, token2, task2) = start(dir, "always").await;
    let replayed = get(port2, "k").await;
    stop(token2, task2).await;
    assert_eq!(
        replayed.as_deref(),
        Some("2"),
        "moon#1099: replay must end on the last applied write; the EXEC's \
         record was logged after a write that followed it"
    );
}

/// No await needed: connections run on a multi-thread runtime and share
/// `Arc<Vec<RwLock<Database>>>`. When the record is sent after the db lock is
/// released, two connections can apply in one order and log in the other.
/// Several writers APPEND to the same keys in pipelined batches. APPEND does
/// not commute, so each key's value spells out the order its appends were
/// applied in, and the replayed value must equal the live one byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_replay_to_the_live_values() {
    const WRITERS: usize = 8;
    const ROUNDS: usize = 200;
    const PIPELINE: usize = 16;
    const KEYS: usize = 4;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let (port, token, task) = start(dir, "everysec").await;

    let mut handles = Vec::new();
    for w in 0..WRITERS {
        handles.push(tokio::spawn(async move {
            let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
            for r in 0..ROUNDS {
                let mut batch = Vec::new();
                for p in 0..PIPELINE {
                    let key = format!("key{}", (r + p) % KEYS);
                    let val = format!("w{w}r{r}p{p};");
                    batch.extend(resp(&["APPEND", &key, &val]));
                }
                s.write_all(&batch).await.unwrap();
                let got = read_lines(&mut s, PIPELINE).await;
                assert!(
                    got.split("\r\n").filter(|l| !l.is_empty()).all(|l| l.starts_with(':')),
                    "every APPEND acked with its length: {got:?}"
                );
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let mut live = Vec::new();
    for k in 0..KEYS {
        live.push(get(port, &format!("key{k}")).await);
    }
    // everysec: let the writer drain everything before the cancel's final sync.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    stop(token, task).await;

    let (port2, token2, task2) = start(dir, "everysec").await;
    let mut replayed = Vec::new();
    for k in 0..KEYS {
        replayed.push(get(port2, &format!("key{k}")).await);
    }
    stop(token2, task2).await;
    for (k, (r, l)) in replayed.iter().zip(&live).enumerate() {
        assert!(
            r == l,
            "moon#1099: key{k} replayed in a different order than it was applied \
             (live {} bytes, replayed {} bytes, first difference at byte {:?})",
            l.as_ref().map_or(0, String::len),
            r.as_ref().map_or(0, String::len),
            l.as_deref()
                .unwrap_or("")
                .bytes()
                .zip(r.as_deref().unwrap_or("").bytes())
                .position(|(a, b)| a != b),
        );
    }
}
