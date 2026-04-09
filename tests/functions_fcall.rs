//! Integration tests for Redis 7.0+ Functions API (FUNCTION LOAD/LIST/DELETE/FLUSH,
//! FCALL, FCALL_RO).
//!
//! **Phase 101 limitation:** Functions are RAM-only. Not persisted across restarts.
//! The `function_not_persistent_across_restart` test documents this known limitation.
//!
//! Requires a running moon server on the port specified by MOON_PORT (default 16479):
//!   ./target/release/moon --port 16479 --shards 1
//!
//! Run with: cargo test --release --test functions_fcall

const MOON_PORT: u16 = 16479;

/// Get a multiplexed connection.
async fn get_conn() -> redis::aio::MultiplexedConnection {
    let client =
        redis::Client::open(format!("redis://127.0.0.1:{}/", MOON_PORT)).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Helper: send a raw redis command and get the result as a RedisValue.
async fn raw_cmd(
    con: &mut redis::aio::MultiplexedConnection,
    args: &[&str],
) -> redis::RedisResult<redis::Value> {
    let mut cmd = redis::cmd(args[0]);
    for arg in &args[1..] {
        cmd.arg(*arg);
    }
    cmd.query_async(con).await
}

/// Clean up any function state before each test.
async fn flush_functions(con: &mut redis::aio::MultiplexedConnection) {
    let _: redis::RedisResult<redis::Value> =
        raw_cmd(con, &["FUNCTION", "FLUSH"]).await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn function_load_and_fcall() {
    let mut con = get_conn().await;
    flush_functions(&mut con).await;

    let body = "#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)";

    // FUNCTION LOAD
    let result = raw_cmd(&mut con, &["FUNCTION", "LOAD", body])
        .await
        .unwrap();
    assert_eq!(result, redis::Value::BulkString(b"mylib".to_vec()));

    // FCALL hello 0
    let result = raw_cmd(&mut con, &["FCALL", "hello", "0"]).await.unwrap();
    assert_eq!(result, redis::Value::BulkString(b"world".to_vec()));
}

#[tokio::test]
async fn function_load_missing_header_errors() {
    let mut con = get_conn().await;
    flush_functions(&mut con).await;

    // Body without shebang
    let result = raw_cmd(&mut con, &["FUNCTION", "LOAD", "return 1"]).await;
    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("Missing library metadata")
            || err_str.contains("Missing library"),
        "Unexpected error: {err_str}"
    );
}

#[tokio::test]
async fn function_load_duplicate_without_replace_errors() {
    let mut con = get_conn().await;
    flush_functions(&mut con).await;

    let body = "#!lua name=duplib\nredis.register_function('dup_hello', function() return 'world' end)";

    // First load succeeds
    let _ = raw_cmd(&mut con, &["FUNCTION", "LOAD", body]).await.unwrap();

    // Second load without REPLACE fails
    let result = raw_cmd(&mut con, &["FUNCTION", "LOAD", body]).await;
    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("already exists"),
        "Unexpected error: {err_str}"
    );
}

#[tokio::test]
async fn function_load_replace_succeeds() {
    let mut con = get_conn().await;
    flush_functions(&mut con).await;

    let body1 = "#!lua name=replib\nredis.register_function('rep_hello', function() return 'world' end)";
    let body2 = "#!lua name=replib\nredis.register_function('rep_hello', function() return 'replaced' end)";

    let _ = raw_cmd(&mut con, &["FUNCTION", "LOAD", body1])
        .await
        .unwrap();

    // REPLACE should succeed
    let result = raw_cmd(&mut con, &["FUNCTION", "LOAD", "REPLACE", body2])
        .await
        .unwrap();
    assert_eq!(result, redis::Value::BulkString(b"replib".to_vec()));

    // Verify the function was replaced
    let result = raw_cmd(&mut con, &["FCALL", "rep_hello", "0"])
        .await
        .unwrap();
    assert_eq!(result, redis::Value::BulkString(b"replaced".to_vec()));
}

#[tokio::test]
async fn function_list_returns_libraries() {
    let mut con = get_conn().await;
    flush_functions(&mut con).await;

    let body = "#!lua name=listlib\nredis.register_function('list_hello', function() return 'world' end)";
    let _ = raw_cmd(&mut con, &["FUNCTION", "LOAD", body])
        .await
        .unwrap();

    let result = raw_cmd(&mut con, &["FUNCTION", "LIST"]).await.unwrap();
    // Should be an array containing at least one library descriptor
    let list_str = format!("{:?}", result);
    assert!(
        list_str.contains("listlib"),
        "FUNCTION LIST should contain listlib, got: {list_str}"
    );
}

#[tokio::test]
async fn function_delete_removes() {
    let mut con = get_conn().await;
    flush_functions(&mut con).await;

    let body = "#!lua name=dellib\nredis.register_function('del_hello', function() return 'world' end)";
    let _ = raw_cmd(&mut con, &["FUNCTION", "LOAD", body])
        .await
        .unwrap();

    // DELETE
    let result = raw_cmd(&mut con, &["FUNCTION", "DELETE", "dellib"])
        .await
        .unwrap();
    assert_eq!(result, redis::Value::Okay);

    // FCALL should fail
    let result = raw_cmd(&mut con, &["FCALL", "del_hello", "0"]).await;
    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("Function not found"),
        "Expected function not found error, got: {err_str}"
    );
}

#[tokio::test]
async fn fcall_ro_rejects_writes() {
    let mut con = get_conn().await;
    flush_functions(&mut con).await;

    let body = "#!lua name=rolib\nredis.register_function('writer', function() return redis.call('SET', 'k', 'v') end)";
    let _ = raw_cmd(&mut con, &["FUNCTION", "LOAD", body])
        .await
        .unwrap();

    // FCALL_RO should reject writes
    let result = raw_cmd(&mut con, &["FCALL_RO", "writer", "0"]).await;
    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("Write commands are not allowed")
            || err_str.contains("read-only"),
        "Expected write rejection error, got: {err_str}"
    );
}

#[tokio::test]
async fn function_dump_restore_stats_deferred() {
    let mut con = get_conn().await;

    // FUNCTION DUMP
    let result = raw_cmd(&mut con, &["FUNCTION", "DUMP"]).await;
    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("DUMP not supported") && err_str.contains("Phase 101"),
        "Expected DUMP deferred error, got: {err_str}"
    );

    // FUNCTION RESTORE
    let result =
        raw_cmd(&mut con, &["FUNCTION", "RESTORE", "payload"]).await;
    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("RESTORE not supported") && err_str.contains("Phase 101"),
        "Expected RESTORE deferred error, got: {err_str}"
    );

    // FUNCTION STATS
    let result = raw_cmd(&mut con, &["FUNCTION", "STATS"]).await;
    assert!(result.is_err());
    let err_str = format!("{}", result.unwrap_err());
    assert!(
        err_str.contains("STATS not supported") && err_str.contains("Phase 101"),
        "Expected STATS deferred error, got: {err_str}"
    );
}

#[tokio::test]
#[ignore = "Phase 101: FUNCTION is RAM-only; persistence deferred"]
async fn function_not_persistent_across_restart() {
    // This test documents the known limitation that functions are RAM-only
    // and will not survive a server restart. When persistence is added
    // (future phase), this test should be un-ignored and verify that
    // functions loaded before restart are available after restart.
    //
    // Cannot be tested in this harness as it requires server restart.
}
