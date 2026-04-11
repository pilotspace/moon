//! Integration tests for blocking list commands: BLPOP, BRPOP, BLMOVE,
//! BLMPOP, BRPOPLPUSH, BZPOPMIN, BZPOPMAX.
//!
//! Requires a running moon server in sharded mode on port 16479:
//!   ./target/release/moon --port 16479 --shards 1
//!
//! Run with: bash scripts/run-blocking-tests.sh
//!
//! Compiles under both `runtime-tokio` and `runtime-monoio` feature gates.

use std::time::Duration;

use redis::AsyncCommands;

const MOON_PORT: u16 = 16479;

/// Skip test if moon server is not running on MOON_PORT.
macro_rules! require_moon_server {
    () => {
        let client = redis::Client::open(format!("redis://127.0.0.1:{}/", MOON_PORT)).unwrap();
        if client.get_multiplexed_async_connection().await.is_err() {
            eprintln!("SKIP: moon server not running on port {}", MOON_PORT);
            return;
        }
    };
}

/// Get a multiplexed connection (good for non-blocking commands).
async fn get_conn() -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", MOON_PORT)).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Clean up test keys.
async fn cleanup_keys(conn: &mut redis::aio::MultiplexedConnection, keys: &[&str]) {
    for key in keys {
        let _: Result<(), _> = redis::cmd("DEL").arg(*key).query_async(conn).await;
    }
}

// ---------------------------------------------------------------------------
// Test: BLPOP timeout returns nil (via redis-cli subprocess for true blocking)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore] // Requires running Moon server on port 16479
async fn blpop_timeout_returns_nil() {
    require_moon_server!();
    let mut conn = get_conn().await;
    cleanup_keys(&mut conn, &["empty_key_blpop"]).await;

    // Use redis-cli subprocess for true blocking semantics
    let start = std::time::Instant::now();
    let output = tokio::process::Command::new("redis-cli")
        .args([
            "-p",
            &MOON_PORT.to_string(),
            "BLPOP",
            "empty_key_blpop",
            "1",
        ])
        .output()
        .await
        .unwrap();
    let elapsed = start.elapsed();

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Redis returns empty string on nil/timeout
    assert!(
        stdout.trim().is_empty(),
        "BLPOP should return nil on timeout, got: '{}'",
        stdout.trim()
    );
    assert!(
        elapsed.as_millis() >= 900,
        "BLPOP should block for ~1s, elapsed={}ms",
        elapsed.as_millis()
    );
}

// ---------------------------------------------------------------------------
// Test: BRPOP wakes on RPUSH (via redis-cli subprocess)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore] // Requires running Moon server on port 16479
async fn brpop_wakes_on_rpush() {
    require_moon_server!();
    let mut conn = get_conn().await;
    cleanup_keys(&mut conn, &["wake_key"]).await;

    // Client A: start blocking BRPOP via redis-cli
    let mut child = tokio::process::Command::new("redis-cli")
        .args(["-p", &MOON_PORT.to_string(), "BRPOP", "wake_key", "5"])
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    // Give Client A time to register
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Client B: RPUSH to wake Client A
    let _: () = conn.rpush("wake_key", "hello").await.unwrap();

    // Wait for redis-cli to return
    let output = tokio::time::timeout(Duration::from_secs(3), child.wait_with_output())
        .await
        .expect("timed out waiting for BRPOP wake")
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("wake_key"),
        "BRPOP should return the key name, got: '{}'",
        stdout.trim()
    );
    assert!(
        stdout.contains("hello"),
        "BRPOP should return the value, got: '{}'",
        stdout.trim()
    );
}

// ---------------------------------------------------------------------------
// Test: BLMPOP count greater than one
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore] // Requires running Moon server on port 16479
async fn blmpop_count_greater_than_one() {
    require_moon_server!();
    let mut conn = get_conn().await;
    cleanup_keys(&mut conn, &["blmpop_key"]).await;

    // Pre-populate list: a b c d e (left to right)
    let _: () = redis::cmd("RPUSH")
        .arg("blmpop_key")
        .arg("a")
        .arg("b")
        .arg("c")
        .arg("d")
        .arg("e")
        .query_async(&mut conn)
        .await
        .unwrap();

    // BLMPOP timeout numkeys key LEFT COUNT 3 (immediate pop -- data available)
    let result: redis::Value = redis::cmd("BLMPOP")
        .arg(1) // timeout
        .arg(1) // numkeys
        .arg("blmpop_key")
        .arg("LEFT")
        .arg("COUNT")
        .arg(3)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Result: [key, [elem1, elem2, elem3]]
    match result {
        redis::Value::Array(outer) => {
            assert_eq!(outer.len(), 2, "BLMPOP should return [key, elements]");
            match &outer[0] {
                redis::Value::BulkString(k) => {
                    assert_eq!(k, b"blmpop_key", "key should be blmpop_key");
                }
                other => panic!("expected BulkString for key, got {:?}", other),
            }
            match &outer[1] {
                redis::Value::Array(elems) => {
                    assert_eq!(elems.len(), 3, "should pop 3 elements");
                }
                other => panic!("expected Array for elements, got {:?}", other),
            }
        }
        other => panic!("expected Array from BLMPOP, got {:?}", other),
    }

    // Verify only 2 elements remain
    let len: i64 = conn.llen("blmpop_key").await.unwrap();
    assert_eq!(len, 2, "2 elements should remain after popping 3 from 5");
}

// ---------------------------------------------------------------------------
// Test: BRPOPLPUSH legacy alias
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore] // Requires running Moon server on port 16479
async fn brpoplpush_legacy_alias() {
    require_moon_server!();
    let mut conn = get_conn().await;
    cleanup_keys(&mut conn, &["brpl_src", "brpl_dst"]).await;

    // Pre-populate source
    let _: () = conn.rpush("brpl_src", "alpha").await.unwrap();
    let _: () = conn.rpush("brpl_src", "beta").await.unwrap();

    // BRPOPLPUSH src dst 0 => immediate pop (data available)
    let result: String = redis::cmd("BRPOPLPUSH")
        .arg("brpl_src")
        .arg("brpl_dst")
        .arg(0)
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(result, "beta", "BRPOPLPUSH should return the moved element");

    let src_len: i64 = conn.llen("brpl_src").await.unwrap();
    let dst_len: i64 = conn.llen("brpl_dst").await.unwrap();
    assert_eq!(src_len, 1, "source should have 1 element left");
    assert_eq!(dst_len, 1, "destination should have 1 element");

    let dst_val: Vec<String> = conn.lrange("brpl_dst", 0, -1).await.unwrap();
    assert_eq!(dst_val, vec!["beta"]);
}

// ---------------------------------------------------------------------------
// Test: connection drop cleans registry (via redis-cli subprocess)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore] // Requires running Moon server on port 16479
async fn connection_drop_cleans_registry() {
    require_moon_server!();
    let mut conn = get_conn().await;
    cleanup_keys(&mut conn, &["drop_test_key"]).await;

    // Start a blocking client via redis-cli, then kill it
    let mut child = tokio::process::Command::new("redis-cli")
        .args(["-p", &MOON_PORT.to_string(), "BLPOP", "drop_test_key", "30"])
        .stdout(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // Wait for registration
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Kill the client (simulates connection drop)
    child.kill().await.unwrap();
    let _ = child.wait().await;

    // Wait for expire tick to clean up
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify a new blocking client works normally (short timeout via redis-cli)
    let output = tokio::process::Command::new("redis-cli")
        .args(["-p", &MOON_PORT.to_string(), "BLPOP", "drop_test_key", "1"])
        .output()
        .await
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.trim().is_empty(),
        "should timeout (no data in key), got: '{}'",
        stdout.trim()
    );
}
