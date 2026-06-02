//! MA2 RED+GREEN tests — KILL SNAPSHOT command + killed-snapshot enforcement.
//!
//! Tests 1–2 (network, single-shard): KILL SNAPSHOT dispatch routing via
//! handler_single (shards=1). Tests fail until GREEN implementation lands.
//! Test 3 (unit): TransactionManager kill API compiles.
//! Test 4 (network, multi-shard): TXN.BEGIN → KILL SNAPSHOT → TXN.COMMIT
//! must return "snapshot too old"; requires handler_sharded (shards=2).
#![cfg(feature = "runtime-tokio")]

use std::time::Duration;

use moon::config::{CrossShardFastPath, ServerConfig};
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn base_config(port: u16, num_shards: usize) -> ServerConfig {
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
        maxmemory: 0,
        maxmemory_policy: "noeviction".to_string(),
        maxmemory_samples: 5,
        shards: num_shards,
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
        // Threshold disabled — kills only via explicit KILL SNAPSHOT command.
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
    }
}

/// Start a single-shard server via `run_with_shutdown` (handler_single path).
/// Used for tests 1–2 which only need KILL to be routed but do NOT need TXN.
async fn start_server() -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let token = CancellationToken::new();
    let server_token = token.clone();
    let config = base_config(port, 1);

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    (port, token)
}

/// Start a multi-shard server via the ChannelMesh harness (handler_sharded path).
/// Required for test 4 which exercises TXN.BEGIN / TXN.COMMIT.
async fn start_sharded_server(num_shards: usize) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();
    let config = base_config(port, num_shards);
    let cancel = token.clone();

    std::thread::spawn(move || {
        let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);
        let conn_txs: Vec<channel::MpscSender<(tokio::net::TcpStream, bool)>> =
            (0..num_shards).map(|i| mesh.conn_tx(i)).collect();
        let all_notifiers = mesh.all_notifiers();
        let all_pubsub_registries: Vec<
            std::sync::Arc<parking_lot::RwLock<moon::pubsub::PubSubRegistry>>,
        > = (0..num_shards)
            .map(|_| {
                std::sync::Arc::new(parking_lot::RwLock::new(moon::pubsub::PubSubRegistry::new()))
            })
            .collect();
        let all_remote_sub_maps: Vec<
            std::sync::Arc<
                parking_lot::RwLock<moon::shard::remote_subscriber_map::RemoteSubscriberMap>,
            >,
        > = (0..num_shards)
            .map(|_| {
                std::sync::Arc::new(parking_lot::RwLock::new(
                    moon::shard::remote_subscriber_map::RemoteSubscriberMap::new(),
                ))
            })
            .collect();
        let affinity_tracker = std::sync::Arc::new(parking_lot::RwLock::new(
            moon::shard::affinity::AffinityTracker::new(),
        ));
        let mut shards: Vec<Shard> = (0..num_shards)
            .map(|id| Shard::new(id, num_shards, config.databases, config.to_runtime_config()))
            .collect();
        let all_dbs: Vec<Vec<moon::storage::Database>> = shards
            .iter_mut()
            .map(|s| std::mem::take(&mut s.databases))
            .collect();
        let shard_databases = moon::shard::shared_databases::ShardDatabases::new(all_dbs);

        let mut shard_handles = Vec::with_capacity(num_shards);
        for (id, mut shard) in shards.into_iter().enumerate() {
            let producers = mesh.take_producers(id);
            let consumers = mesh.take_consumers(id);
            let conn_rx = mesh.take_conn_rx(id);
            let shard_config = config.clone();
            let shard_cancel = cancel.clone();
            let shard_spsc_notify = mesh.take_notify(id);
            let shard_all_notifiers = all_notifiers.clone();
            let shard_dbs = shard_databases.clone();
            let shard_pubsub_regs = all_pubsub_registries.clone();
            let shard_remote_sub_maps = all_remote_sub_maps.clone();
            let shard_affinity = affinity_tracker.clone();

            let handle = std::thread::Builder::new()
                .name(format!("kill-snap-shard-{}", id))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build shard runtime");
                    let local = tokio::task::LocalSet::new();
                    let (_snap_tx, snap_rx) = channel::watch(0u64);
                    let snap_tx = _snap_tx;
                    let acl_t = std::sync::Arc::new(std::sync::RwLock::new(
                        moon::acl::AclTable::load_or_default(&shard_config),
                    ));
                    let rt_cfg = std::sync::Arc::new(parking_lot::RwLock::new(
                        shard_config.to_runtime_config(),
                    ));
                    rt.block_on(local.run_until(shard.run(
                        conn_rx,
                        None,
                        consumers,
                        producers,
                        shard_cancel,
                        None,
                        None,
                        None,
                        snap_rx,
                        snap_tx,
                        None,
                        None,
                        0,
                        acl_t,
                        rt_cfg,
                        std::sync::Arc::new(shard_config),
                        shard_spsc_notify,
                        shard_all_notifiers,
                        shard_dbs,
                        shard_pubsub_regs,
                        shard_remote_sub_maps,
                        shard_affinity,
                    )));
                })
                .expect("failed to spawn shard thread");
            shard_handles.push(handle);
        }

        let listener_rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build listener runtime");

        let listener_cancel = cancel.clone();
        listener_rt.block_on(async {
            if let Err(e) =
                listener::run_sharded(config, conn_txs, listener_cancel, false, affinity_tracker)
                    .await
            {
                eprintln!("Listener error: {e}");
            }
        });

        cancel.cancel();
        for handle in shard_handles {
            let _ = handle.join();
        }
    });

    tokio::time::sleep(Duration::from_millis(250)).await;
    (port, token)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

/// MA2-CMD-RED-4: TXN.BEGIN → KILL SNAPSHOT → TXN.COMMIT must return error.
///
/// A killed snapshot is stale — the GC may have advanced past it. Allowing
/// commit would silently produce a read set with undefined visibility. The
/// server must reject TXN.COMMIT and force the client to restart.
///
/// This test uses the multi-shard harness (handler_sharded) because
/// TXN.BEGIN / TXN.COMMIT are cross-store commands not handled by
/// handler_single.
#[tokio::test]
async fn test_commit_after_kill_snapshot_returns_error() {
    // 2-shard server exercises the handler_sharded / TXN path.
    let (port, _token) = start_sharded_server(2).await;
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // Start a cross-store transaction — acquires txn_id 1 on a fresh server.
    // TXN.BEGIN is sent as TXN + BEGIN (two RESP tokens), not as one "TXN.BEGIN".
    let begin_result: redis::RedisResult<String> =
        redis::cmd("TXN").arg("BEGIN").query_async(&mut con).await;
    assert!(
        begin_result.is_ok(),
        "TXN.BEGIN must succeed: {:?}",
        begin_result
    );

    // Kill the snapshot by its txn_id (1 on a fresh per-shard manager).
    // The KILL SNAPSHOT command is dispatched on the same connection so it
    // routes to the same shard as TXN.BEGIN.
    let kill_result: redis::RedisResult<String> = redis::cmd("KILL")
        .arg("SNAPSHOT")
        .arg(1u64)
        .query_async(&mut con)
        .await;
    assert!(
        kill_result.is_ok(),
        "KILL SNAPSHOT 1 must succeed: {:?}",
        kill_result
    );

    // TXN.COMMIT on a killed snapshot must be rejected with a clear error.
    // TXN.COMMIT is sent as TXN + COMMIT (two RESP tokens).
    let commit_result: redis::RedisResult<String> =
        redis::cmd("TXN").arg("COMMIT").query_async(&mut con).await;
    assert!(
        commit_result.is_err(),
        "TXN.COMMIT after KILL SNAPSHOT must return an error, got: {:?}",
        commit_result
    );
    let err_msg = commit_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("snapshot too old") || err_msg.contains("MOONERR"),
        "error must mention 'snapshot too old': {err_msg}"
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
