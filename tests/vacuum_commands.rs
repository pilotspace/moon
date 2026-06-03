//! P8 RED+GREEN integration tests — VACUUM command family + DEBUG RECLAMATION.
//!
//! All tests use the tokio single-shard path (`run_with_shutdown`) which
//! routes through `handler_single`, matching the MA2 test pattern.
//!
//! ## Test inventory
//!
//! | ID | Command | What it verifies |
//! |---|---|---|
//! | P8-NET-1 | `VACUUM` | Returns 12-element array, command is dispatched |
//! | P8-NET-2 | `VACUUM FILES` | Returns 2-element array with manifest_pruned key |
//! | P8-NET-3 | `VACUUM (VERBOSE)` | Returns array with >= 18 elements |
//! | P8-NET-4 | `VACUUM (FREEZE)` | Returns 12-element array; valid shape |
//! | P8-NET-5 | `VACUUM VECTOR <idx>` | Returns `+OK pending` SimpleString |
//! | P8-NET-6 | `VACUUM GRAPH <name>` | Returns `+OK pending` SimpleString |
//! | P8-NET-7 | `VACUUM BOGUS` | Returns ERR for unknown subcommand |
//! | P8-NET-8 | `DEBUG RECLAMATION` | Returns bulk-string with section headers |
#![cfg(feature = "runtime-tokio")]

use std::time::Duration;

use moon::config::{CrossShardFastPath, ServerConfig};
use moon::runtime::cancel::CancellationToken;
use moon::server::listener;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn base_config(port: u16) -> ServerConfig {
    ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 1,
        requirepass: None,
        appendonly: "no".to_string(),
        unsafe_multishard_aof: false,
        appendfsync: "no".to_string(),
        save: None,
        dir: ".".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendfilename: "appendonly.aof".to_string(),
        maxmemory: Some(0),
        maxmemory_policy: "noeviction".to_string(),
        maxmemory_samples: 5,
        shards: 1,
        cluster_enabled: false,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "no".to_string(),
        acllog_max_len: 128,
        tls_port: 0,
        tls_cert_file: None,
        tls_key_file: None,
        tls_ca_cert_file: None,
        tls_ciphersuites: None,
        disk_offload: "disable".to_string(),
        disk_offload_dir: None,
        disk_offload_threshold: 0.85,
        segment_warm_after: 3600,
        pagecache_size: None,
        checkpoint_timeout: 300,
        checkpoint_completion: 0.9,
        max_wal_size: "256mb".to_string(),
        wal_fpi: "disable".to_string(),
        wal_compression: "none".to_string(),
        wal_segment_size: "16mb".to_string(),
        vec_codes_mlock: "disable".to_string(),
        segment_cold_after: 86400,
        segment_cold_min_qps: 0.1,
        vec_diskann_beam_width: 8,
        vec_diskann_cache_levels: 3,
        uring_sqpoll_ms: None,
        admin_port: 0,
        slowlog_log_slower_than: 10000,
        slowlog_max_len: 128,
        check_config: false,
        initial_keyspace_hint: 0,
        memory_arenas_cap: 8,
        maxclients: 10000,
        timeout: 0,
        tcp_keepalive: 300,
        console_auth_required: false,
        console_auth_secret: String::new(),
        console_cors_origin: vec![],
        console_rate_limit: 1000.0,
        console_rate_burst: 2000.0,
        wal_max_checkpoint_lag_ms: 10_000,
        recovery_target_lsn: None,
        recovery_target_time: None,
        manifest_tombstone_retain_epochs: 2,
        manifest_tombstone_retain_secs: 300,
        cross_shard_fast_path: CrossShardFastPath::Auto,
        disk_free_min_pct: 5,
        mvcc_committed_prune_margin: 1000,
        max_unflushed_immutable_segments: 20,
        mvcc_old_snapshot_threshold_secs: 0,
        autovacuum: "enable".to_string(),
        autovacuum_budget_ms_min: 5,
        autovacuum_budget_ms_max: 200,
        autovacuum_target_p95_ms: 10,
        autovacuum_interval_secs: 30,
        graph_merge_max_segments: 8,
        graph_dead_edge_trigger: 0.20,
        autovacuum_starvation_cap_secs: 300,
        cold_orphan_sweep_interval_secs: 300,
        migrate_aof_from: None,
        migrate_aof_to: None,
        migrate_aof_shards: 0,
        vec_warm_mmap_budget: "2gb".to_string(),
        ..Default::default()
    }
}

/// Start a single-shard server via `run_with_shutdown`.
async fn start_server() -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let token = CancellationToken::new();
    let server_token = token.clone();
    let config = base_config(port);

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (port, token)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// P8-NET-1: plain VACUUM returns a 12-element array.
#[tokio::test]
async fn test_vacuum_plain_returns_array() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<Vec<redis::Value>> =
        redis::cmd("VACUUM").query_async(&mut con).await;

    assert!(
        result.is_ok(),
        "VACUUM must succeed, got: {:?}",
        result.unwrap_err()
    );
    let arr = result.unwrap();
    assert_eq!(
        arr.len(),
        12,
        "VACUUM must return 12-element array (6 key/value pairs), got {}",
        arr.len()
    );
    // First element must be the bulk-string key "manifest_pruned"
    match &arr[0] {
        redis::Value::BulkString(b) => {
            assert_eq!(
                b.as_slice(),
                b"manifest_pruned",
                "first key must be manifest_pruned"
            );
        }
        other => panic!("expected BulkString at index 0, got {other:?}"),
    }
}

/// P8-NET-2: VACUUM FILES returns 2-element array with manifest_pruned key.
#[tokio::test]
async fn test_vacuum_files_returns_manifest_count() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("VACUUM")
        .arg("FILES")
        .query_async(&mut con)
        .await;

    assert!(result.is_ok(), "VACUUM FILES must succeed: {:?}", result);
    let arr = result.unwrap();
    assert_eq!(
        arr.len(),
        2,
        "VACUUM FILES must return 2-element array, got {}",
        arr.len()
    );
    match &arr[0] {
        redis::Value::BulkString(b) => {
            assert_eq!(b.as_slice(), b"manifest_pruned");
        }
        other => panic!("expected BulkString key, got {other:?}"),
    }
}

/// P8-NET-3: VACUUM (VERBOSE) returns array with >= 18 elements.
#[tokio::test]
async fn test_vacuum_verbose_returns_extended_array() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("VACUUM")
        .arg("(VERBOSE)")
        .query_async(&mut con)
        .await;

    assert!(
        result.is_ok(),
        "VACUUM (VERBOSE) must succeed: {:?}",
        result
    );
    let arr = result.unwrap();
    assert!(
        arr.len() >= 18,
        "VACUUM (VERBOSE) must return >= 18 elements (6 diagnostic + 12 kv), got {}",
        arr.len()
    );
    // First element must be a diagnostic line starting with "# "
    match &arr[0] {
        redis::Value::BulkString(b) => {
            assert!(
                b.starts_with(b"# "),
                "first verbose element must start with '# ', got: {:?}",
                std::str::from_utf8(b)
            );
        }
        other => panic!("expected BulkString diagnostic at index 0, got {other:?}"),
    }
}

/// P8-NET-4: VACUUM (FREEZE) returns 12-element array (same shape as plain VACUUM).
#[tokio::test]
async fn test_vacuum_freeze_returns_kv_array() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<Vec<redis::Value>> = redis::cmd("VACUUM")
        .arg("(FREEZE)")
        .query_async(&mut con)
        .await;

    assert!(result.is_ok(), "VACUUM (FREEZE) must succeed: {:?}", result);
    let arr = result.unwrap();
    assert_eq!(
        arr.len(),
        12,
        "VACUUM (FREEZE) must return 12-element array, got {}",
        arr.len()
    );
}

/// P8-NET-5: VACUUM VECTOR is routed to the dedicated `vacuum_vector` entry
/// point (B1 fix). With an unknown index name the call must return an
/// `ERR unknown vector index` instead of the legacy stub.
#[tokio::test]
async fn test_vacuum_vector_routes_to_dedicated_handler() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<String> = redis::cmd("VACUUM")
        .arg("VECTOR")
        .arg("myidx")
        .query_async(&mut con)
        .await;

    let err = result.expect_err("VACUUM VECTOR on unknown index must error");
    let msg = err.to_string();
    assert!(
        msg.contains("unknown vector index"),
        "expected 'unknown vector index', got: {msg}"
    );
}

/// P8-NET-6: VACUUM GRAPH is routed to the dedicated `vacuum_graph` entry
/// point (B1 fix). With an unknown graph name the call must error.
#[tokio::test]
#[cfg(feature = "graph")]
async fn test_vacuum_graph_routes_to_dedicated_handler() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<String> = redis::cmd("VACUUM")
        .arg("GRAPH")
        .arg("nonexistent_graph")
        .query_async(&mut con)
        .await;

    // Real handler may return OK with zero stats or an ERR — both prove
    // routing reached the dedicated path. The old stub returned a static
    // SimpleString "OK pending implementation in v0.1.14"; ensure we no
    // longer get that exact string.
    let observed = match result {
        Ok(s) => s,
        Err(e) => e.to_string(),
    };
    assert!(
        !observed.contains("pending implementation"),
        "VACUUM GRAPH should no longer hit the v0.1.14 stub; got: {observed}"
    );
}

/// P8-NET-7: VACUUM with unknown subcommand returns ERR.
#[tokio::test]
async fn test_vacuum_unknown_subcommand_returns_error() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<String> = redis::cmd("VACUUM")
        .arg("BOGUS")
        .query_async(&mut con)
        .await;

    assert!(
        result.is_err(),
        "VACUUM BOGUS must return an error, got: {:?}",
        result
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("ERR") || err.contains("unknown"),
        "error must mention ERR or unknown: {err}"
    );
}

/// P8-NET-8: DEBUG RECLAMATION returns bulk-string with all four section headers.
#[tokio::test]
async fn test_debug_reclamation_returns_diagnostic_bulk_string() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<String> = redis::cmd("DEBUG")
        .arg("RECLAMATION")
        .query_async(&mut con)
        .await;

    assert!(
        result.is_ok(),
        "DEBUG RECLAMATION must succeed, got: {:?}",
        result.unwrap_err()
    );
    let output = result.unwrap();
    assert!(
        output.contains("# Manifest"),
        "must contain '# Manifest' section"
    );
    assert!(output.contains("# WAL"), "must contain '# WAL' section");
    assert!(output.contains("# MVCC"), "must contain '# MVCC' section");
    assert!(
        output.contains("# Atomics"),
        "must contain '# Atomics' section"
    );
    assert!(
        output.contains("manifest_active_entries:"),
        "manifest field missing"
    );
    assert!(
        output.contains("mvcc_committed_count:"),
        "mvcc field missing"
    );
    assert!(
        output.contains("recl_wal_bytes:"),
        "atomic wal field missing"
    );
}

/// P8-NET-9: DEBUG SLEEP still works after VACUUM intercept (other DEBUG
/// subcommands must not be swallowed).
#[tokio::test]
async fn test_debug_sleep_still_works_after_vacuum_intercept() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // DEBUG SLEEP 0 — should return +OK immediately (0s sleep).
    let result: redis::RedisResult<String> = redis::cmd("DEBUG")
        .arg("SLEEP")
        .arg(0)
        .query_async(&mut con)
        .await;

    assert!(
        result.is_ok(),
        "DEBUG SLEEP 0 must still work after P8 intercept, got: {:?}",
        result
    );
}
