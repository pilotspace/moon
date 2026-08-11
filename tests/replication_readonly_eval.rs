//! Task #38: a read-only replica must reject a CLIENT-issued `EVAL`/
//! `EVALSHA` script that attempts a write, at the first offending
//! `redis.call`/`redis.pcall` inside it — matching upstream Redis, which
//! lets a script run right up until it tries to mutate state, then aborts
//! with `-READONLY` rather than rejecting `EVAL` outright (a read-only
//! script must still be served on a replica; e.g. `SCRIPT LOAD`+`EVALSHA`
//! doing only `GET`s is a normal read workload against a replica).
//!
//! Mirrors `tests/replication_readonly_ws_mq.rs`'s harness pattern exactly
//! (same real-binary spawn, same `assert_readonly` helper shape) since the
//! enforcement point (`scripting::bridge::make_redis_call_fn`) is inside the
//! same sharded connection-handler code the WS/MQ gate lives next to, and is
//! equally invisible to a synthetic single-shard harness.
//!
//! Run against a monoio (default-feature) build:
//! ```text
//! cargo build --release
//! MOON_BIN=./target/release/moon cargo test --test replication_readonly_eval -- --ignored --nocapture
//! ```
//!
//! Run against a tokio+jemalloc build (handler_sharded parity):
//! ```text
//! cargo build --release --no-default-features --features runtime-tokio,jemalloc
//! MOON_BIN=./target/release/moon cargo test --test replication_readonly_eval -- --ignored --nocapture
//! ```

mod common;

use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn moon_bin() -> std::path::PathBuf {
    common::find_moon_binary()
}

fn start_moon(port: u16, dir: &str) -> Child {
    let port_s = port.to_string();
    Command::new(moon_bin())
        .args([
            "--port",
            &port_s,
            "--shards",
            "1",
            "--dir",
            dir,
            "--disk-free-min-pct",
            "0",
            "--appendonly",
            "no",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("Failed to start moon (set MOON_BIN to a built binary)")
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/")).expect("client open");
    for _ in 0..50 {
        if let Ok(con) = client.get_multiplexed_async_connection().await {
            return con;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("failed to connect to moon on port {port}");
}

fn assert_readonly<T: std::fmt::Debug>(result: redis::RedisResult<T>, label: &str) {
    assert!(
        result.is_err(),
        "{label} on a read-only replica should return READONLY error, got: {result:?}"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.to_uppercase().contains("READONLY"),
        "{label} error should say READONLY, got: {err}"
    );
}

/// Kill-on-drop guard: the many `assert_eq!`/`.expect()` calls below used to
/// run entirely before the manual `child.kill()` at the end of the test, so
/// any one of them panicking orphaned the server (task:
/// test/harness-hygiene-sweep). See tests/bgsave_startup_race.rs for the
/// same pattern.
struct MoonGuard(Option<Child>);

impl Drop for MoonGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[tokio::test]
#[ignore = "spawns a real moon binary — set MOON_BIN, see module docs"]
async fn test_readonly_replica_blocks_writing_eval_and_evalsha() {
    let dir = tempfile::tempdir().unwrap();
    let (child, port) = common::spawn_listening(|p| start_moon(p, dir.path().to_str().unwrap()));
    let child = MoonGuard(Some(child));
    let mut con = connect(port).await;

    // --- Master: a writing EVAL succeeds, proving the harness/script path
    // itself works before we assert the replica rejects it. ---
    let eval_write_on_master: redis::RedisResult<String> = redis::cmd("EVAL")
        .arg("return redis.call('SET', KEYS[1], ARGV[1])")
        .arg(1)
        .arg("eval-key")
        .arg("eval-value")
        .query_async(&mut con)
        .await;
    assert_eq!(eval_write_on_master, Ok("OK".to_string()));

    let get_on_master: String = redis::cmd("GET")
        .arg("eval-key")
        .query_async(&mut con)
        .await
        .expect("GET after EVAL SET should succeed on master");
    assert_eq!(get_on_master, "eval-value");

    // Load a script for the EVALSHA leg below.
    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return redis.call('SET', KEYS[1], ARGV[1])")
        .query_async(&mut con)
        .await
        .expect("SCRIPT LOAD should succeed on master");

    // A read-only script also succeeds on master (sanity for the replica
    // assertion below — proves EVAL itself, not just writes, is reachable).
    let eval_read_on_master: redis::RedisResult<redis::Value> = redis::cmd("EVAL")
        .arg("return redis.call('GET', KEYS[1])")
        .arg(1)
        .arg("eval-key")
        .query_async(&mut con)
        .await;
    assert!(
        eval_read_on_master.is_ok(),
        "read-only EVAL should succeed on master: {eval_read_on_master:?}"
    );

    // --- Become a read-only replica of a non-existent master (same
    // pattern as tests/replication_test.rs::test_readonly_replica and
    // tests/replication_readonly_ws_mq.rs). ---
    let repl: String = redis::cmd("REPLICAOF")
        .arg("127.0.0.1")
        .arg("9999")
        .query_async(&mut con)
        .await
        .expect("REPLICAOF should succeed");
    assert_eq!(repl, "OK");

    // --- A writing EVAL must be rejected -READONLY once it hits the first
    // write redis.call — not silently executed and diverging from master. ---
    let eval_write_on_replica: redis::RedisResult<String> = redis::cmd("EVAL")
        .arg("return redis.call('SET', KEYS[1], ARGV[1])")
        .arg(1)
        .arg("blocked-eval-key")
        .arg("blocked-eval-value")
        .query_async(&mut con)
        .await;
    assert_readonly(eval_write_on_replica, "EVAL SET");

    // Same for EVALSHA against the pre-loaded script.
    let evalsha_write_on_replica: redis::RedisResult<String> = redis::cmd("EVALSHA")
        .arg(&sha)
        .arg(1)
        .arg("blocked-evalsha-key")
        .arg("blocked-evalsha-value")
        .query_async(&mut con)
        .await;
    assert_readonly(evalsha_write_on_replica, "EVALSHA SET");

    // The rejected writes must not have landed.
    let blocked_key_missing: Option<String> = redis::cmd("GET")
        .arg("blocked-eval-key")
        .query_async(&mut con)
        .await
        .expect("GET should succeed (read) on a read-only replica");
    assert_eq!(
        blocked_key_missing, None,
        "a write rejected with READONLY must not have been applied"
    );

    // --- A read-only script must still be served on the replica (matches
    // upstream Redis: EVAL itself is not blanket-blocked, only its writes). ---
    let eval_read_on_replica: redis::RedisResult<redis::Value> = redis::cmd("EVAL")
        .arg("return redis.call('GET', KEYS[1])")
        .arg(1)
        .arg("eval-key")
        .query_async(&mut con)
        .await;
    assert!(
        eval_read_on_replica.is_ok(),
        "read-only EVAL must still be served on a read-only replica, got: {eval_read_on_replica:?}"
    );

    drop(child); // MoonGuard SIGKILLs + reaps
}
