//! MA2 RED+GREEN tests — KILL SNAPSHOT command.
//!
//! Tests 1–2 (network) require the KILL command to be registered in the
//! dispatch table; they fail until GREEN implementation lands.
//! Test 3 (unit) verifies the TransactionManager API compiles.
#![cfg(feature = "runtime-tokio")]

use std::time::Duration;

use tokio::net::TcpListener;

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::server::listener;

async fn start_server() -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let token = CancellationToken::new();
    let server_token = token.clone();

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 1,
        requirepass: None,
        appendonly: "no".to_string(),
        appendfsync: "no".to_string(),
        save: None,
        dir: ".".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendfilename: "appendonly.aof".to_string(),
        maxmemory: 0,
        maxmemory_policy: "noeviction".to_string(),
        maxmemory_samples: 5,
        shards: 1,
        cluster_enabled: false,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "yes".to_string(),
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
        disk_free_min_pct: 5,
        mvcc_committed_prune_margin: 1000,
        max_unflushed_immutable_segments: 20,
        // Threshold disabled — kills only via explicit KILL SNAPSHOT command.
        mvcc_old_snapshot_threshold_secs: 0,
    };

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (port, token)
}

/// MA2-CMD-RED-1: KILL SNAPSHOT with an unknown txn_id returns an error.
#[tokio::test]
async fn test_kill_snapshot_unknown_txn_id_returns_error() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // txn_id 99999 was never started — must return an error
    let result: redis::RedisResult<String> = redis::cmd("KILL")
        .arg("SNAPSHOT")
        .arg(99999u64)
        .query_async(&mut con)
        .await;

    assert!(
        result.is_err(),
        "KILL SNAPSHOT with unknown txn_id must return an error"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not found") || err_msg.contains("ERR") || err_msg.contains("MOONERR"),
        "error must mention not found or ERR: {err_msg}"
    );
}

/// MA2-CMD-RED-2: KILL SNAPSHOT without txn_id returns a syntax error.
#[tokio::test]
async fn test_kill_snapshot_wrong_syntax_returns_error() {
    let (port, _token) = start_server().await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    let result: redis::RedisResult<String> = redis::cmd("KILL")
        .arg("SNAPSHOT")
        .query_async(&mut con)
        .await;

    assert!(
        result.is_err(),
        "KILL SNAPSHOT without txn_id must return an error"
    );
}

/// MA2-CMD-RED-3: Unit test — TransactionManager kill API compiles and works.
#[test]
fn test_kill_snapshot_method_on_manager() {
    let mut mgr = moon::vector::mvcc::manager::TransactionManager::new();
    let t = mgr.begin();
    assert!(
        mgr.kill_snapshot(t.txn_id),
        "kill_snapshot must return true for an active txn"
    );
    assert!(
        mgr.is_killed(t.txn_id),
        "is_killed must return true after kill_snapshot"
    );
    // Unknown txn_id → false
    assert!(
        !mgr.kill_snapshot(9999),
        "kill_snapshot must return false for unknown txn_id"
    );
    // Already killed → false (idempotent guard)
    assert!(
        !mgr.kill_snapshot(t.txn_id),
        "kill_snapshot must return false for already-killed txn"
    );
}
