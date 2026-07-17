//! Wave B readonly-enforcement black-box tests (task #34 follow-up, see
//! `.planning/reviews/wave-b-ws-mq-scope-2026-07-12.md` finding #2): a
//! read-only replica must reject client-issued `WS`/`MQ`/`TEMPORAL.*`
//! writes with `-READONLY`, exactly like every other write command already
//! covered by `tests/replication_test.rs::test_readonly_replica`. Read-only
//! subcommands (`WS LIST`/`WS INFO`/`WS AUTH`, `MQ DLQLEN`) must still be
//! served.
//!
//! This spawns the REAL `moon` binary rather than hand-wiring a listener:
//! `WS`/`MQ` are intercepted only in the sharded handlers (handler_monoio /
//! handler_sharded) — the synthetic single-shard `listener::run_with_shutdown`
//! harness used by `replication_test.rs` never wires them up (see the module
//! doc on `tests/workspace_integration.rs`), so it cannot exercise this fix.
//!
//! Spawning the real binary is also the only way to verify the monoio
//! dispatch path locally: CI's `cargo test` only builds/runs the
//! `runtime-tokio` feature set, so a green CI run alone would not catch a
//! monoio-only intercept-order regression (see the "monoio intercept-order
//! bugs are CI-blind" project gotcha).
//!
//! Run against a monoio (default-feature) build:
//! ```text
//! cargo build --release
//! MOON_BIN=./target/release/moon cargo test --test replication_readonly_ws_mq -- --ignored --nocapture
//! ```
//!
//! Run against a tokio+jemalloc build (handler_sharded parity):
//! ```text
//! cargo build --release --no-default-features --features runtime-tokio,jemalloc
//! MOON_BIN=./target/release/moon cargo test --test replication_readonly_ws_mq -- --ignored --nocapture
//! ```

mod common;

use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn moon_bin() -> std::path::PathBuf {
    common::find_moon_binary()
}

/// Spawn moon single-shard, AOF disabled, matching the repo harness rule of
/// always passing `--disk-free-min-pct 0` (shared-checkout filesystems hover
/// near the diskfull guard's floor).
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

/// Connect via the `redis` crate, retrying briefly: `spawn_listening` only
/// guarantees the TCP listener has bound, not that the shard/ACL/config
/// machinery behind it has finished initializing.
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

/// Assert a command was rejected with `-READONLY` (redis crate formats the
/// error as `ReadOnly: ...`, hence the case-insensitive substring check —
/// matches `test_readonly_replica`'s existing assertion style).
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
async fn test_readonly_replica_blocks_ws_mq_temporal_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (child, port) = common::spawn_listening(|p| start_moon(p, dir.path().to_str().unwrap()));
    let child = MoonGuard(Some(child));
    let mut con = connect(port).await;

    // --- Master: mutating subcommands succeed, so the replica assertions
    // below prove the READONLY gate — not that these commands are simply
    // broken end to end. ---
    let ws_id: String = redis::cmd("WS")
        .arg("CREATE")
        .arg("wave-b-test")
        .query_async(&mut con)
        .await
        .expect("WS CREATE should succeed on master");
    assert!(!ws_id.is_empty(), "WS CREATE should return a workspace id");

    let mq_create: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("wave-b-queue")
        .query_async(&mut con)
        .await
        .expect("MQ CREATE should succeed on master");
    assert_eq!(mq_create, "OK");

    let snapshot: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut con)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should succeed on master");
    assert_eq!(snapshot, "OK");

    // Sanity: the read-only subcommands also work on master.
    let list_on_master: redis::RedisResult<redis::Value> =
        redis::cmd("WS").arg("LIST").query_async(&mut con).await;
    assert!(list_on_master.is_ok(), "WS LIST should succeed on master");

    // --- Become a read-only replica of a non-existent master (same
    // pattern as tests/replication_test.rs::test_readonly_replica). ---
    let repl: String = redis::cmd("REPLICAOF")
        .arg("127.0.0.1")
        .arg("9999")
        .query_async(&mut con)
        .await
        .expect("REPLICAOF should succeed");
    assert_eq!(repl, "OK");

    // --- Writes must now be rejected with -READONLY. ---
    let ws_create: redis::RedisResult<String> = redis::cmd("WS")
        .arg("CREATE")
        .arg("blocked-ws")
        .query_async(&mut con)
        .await;
    assert_readonly(ws_create, "WS CREATE");

    let ws_drop: redis::RedisResult<String> = redis::cmd("WS")
        .arg("DROP")
        .arg(&ws_id)
        .query_async(&mut con)
        .await;
    assert_readonly(ws_drop, "WS DROP");

    let mq_create: redis::RedisResult<String> = redis::cmd("MQ")
        .arg("CREATE")
        .arg("blocked-queue")
        .query_async(&mut con)
        .await;
    assert_readonly(mq_create, "MQ CREATE");

    let mq_push: redis::RedisResult<String> = redis::cmd("MQ")
        .arg("PUSH")
        .arg("wave-b-queue")
        .arg("field")
        .arg("value")
        .query_async(&mut con)
        .await;
    assert_readonly(mq_push, "MQ PUSH");

    let temporal_snapshot: redis::RedisResult<String> = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut con)
        .await;
    assert_readonly(temporal_snapshot, "TEMPORAL.SNAPSHOT_AT");

    // --- Read-only subcommands must still be served on the replica. ---
    let list_on_replica: redis::RedisResult<redis::Value> =
        redis::cmd("WS").arg("LIST").query_async(&mut con).await;
    assert!(
        list_on_replica.is_ok(),
        "WS LIST must still be served on a read-only replica, got: {list_on_replica:?}"
    );

    let dlqlen_on_replica: redis::RedisResult<i64> = redis::cmd("MQ")
        .arg("DLQLEN")
        .arg("wave-b-queue")
        .query_async(&mut con)
        .await;
    assert!(
        dlqlen_on_replica.is_ok(),
        "MQ DLQLEN must still be served on a read-only replica, got: {dlqlen_on_replica:?}"
    );

    drop(child); // MoonGuard SIGKILLs + reaps
}
