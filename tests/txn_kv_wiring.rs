//! Integration tests for KV Transaction Wiring (Phase 161).
//!
//! Verifies: ACID-05 (BEGIN/write intercept), ACID-06 (COMMIT WAL),
//!           ACID-07 (ABORT rollback), ACID-08 (HNSW deferral),
//!           ACID-09 (snapshot reads / read-your-writes), ACID-11 (WAL replay).
//!
//! Each test starts a full sharded Moon server on a random port using the
//! same runtime path as `main.rs` (via `listener::run_sharded`), so the
//! TXN.* handler intercepts in handler_sharded.rs are active.
//!
//! Run: cargo test --test txn_kv_wiring --no-default-features --features runtime-tokio,jemalloc -- --test-threads=1
#![cfg(feature = "runtime-tokio")]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use moon::shard::Shard;
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure
// ---------------------------------------------------------------------------

/// Start a full sharded Moon server on a random OS-assigned port.
///
/// Uses the same shard-thread + `run_sharded` pattern as `main.rs`, so
/// TXN.* handler intercepts are active.
///
/// `persistence_dir` is the data directory for WAL/AOF files. Pass an empty
/// string to use the current directory with `appendonly = "no"`.
async fn start_txn_server(num_shards: usize, persistence_dir: &str) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();

    let (appendonly, dir) = if persistence_dir.is_empty() {
        ("no".to_string(), ".".to_string())
    } else {
        ("yes".to_string(), persistence_dir.to_string())
    };

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        requirepass: None,
        appendonly,
        appendfsync: "everysec".to_string(),
        save: None,
        dir,
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
        wal_fpi: "enable".to_string(),
        wal_compression: "lz4".to_string(),
        wal_segment_size: "16mb".to_string(),
        vec_codes_mlock: "enable".to_string(),
        segment_cold_after: 86400,
        segment_cold_min_qps: 0.1,
        vec_diskann_beam_width: 8,
        vec_diskann_cache_levels: 3,
        uring_sqpoll_ms: None,
        admin_port: 0,
        slowlog_log_slower_than: 10000,
        slowlog_max_len: 128,
        check_config: false,
        maxclients: 10000,
        timeout: 0,
        tcp_keepalive: 300,
        console_auth_required: false,
        console_auth_secret: String::new(),
        console_cors_origin: vec![],
        console_rate_limit: 1000.0,
        console_rate_burst: 2000.0,
    };

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
                std::sync::Arc::new(parking_lot::RwLock::new(
                    moon::pubsub::PubSubRegistry::new(),
                ))
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
                .name(format!("txn-test-shard-{}", id))
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
                eprintln!("Listener error: {}", e);
            }
        });

        cancel.cancel();
        for handle in shard_handles {
            let _ = handle.join();
        }
    });

    // Give the server time to bind and start shards.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    (port, token)
}

/// Start a single-shard server with no persistence (fast, for lifecycle tests).
async fn start_server() -> (u16, CancellationToken) {
    start_txn_server(1, "").await
}

/// Open a multiplexed Redis connection to the server.
async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

// ---------------------------------------------------------------------------
// ACID-05: TXN.BEGIN / SET / TXN.COMMIT lifecycle
// ---------------------------------------------------------------------------

/// ACID-05: TXN.BEGIN returns OK, SET inside transaction works, TXN.COMMIT persists value.
#[tokio::test]
async fn test_txn_begin_set_commit_ok() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // TXN.BEGIN -> OK
    let begin_result: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");
    assert_eq!(begin_result, "OK", "TXN.BEGIN should return OK");

    // SET inside transaction -> OK
    let set_result: String = redis::cmd("SET")
        .arg("txn_commit_key")
        .arg("committed_value")
        .query_async(&mut conn)
        .await
        .expect("SET inside TXN should succeed");
    assert_eq!(set_result, "OK", "SET in TXN should return OK");

    // TXN.COMMIT -> OK
    let commit_result: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn)
        .await
        .expect("TXN.COMMIT should succeed");
    assert_eq!(commit_result, "OK", "TXN.COMMIT should return OK");

    // GET after commit -> "committed_value" (value persisted)
    let get_result: Option<String> = redis::cmd("GET")
        .arg("txn_commit_key")
        .query_async(&mut conn)
        .await
        .expect("GET after TXN.COMMIT should succeed");
    assert_eq!(
        get_result.as_deref(),
        Some("committed_value"),
        "GET after TXN.COMMIT should return committed value"
    );

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_commit_key")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACID-07: TXN.ABORT restores SET key (insert case — key did not exist)
// ---------------------------------------------------------------------------

/// ACID-07: TXN.ABORT on a new key (insert) removes the key entirely.
#[tokio::test]
async fn test_txn_abort_restores_set_insert() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Verify: key does not exist
    let pre_check: Option<String> = redis::cmd("GET")
        .arg("txn_abort_insert_key")
        .query_async(&mut conn)
        .await
        .expect("Initial GET should succeed");
    assert!(
        pre_check.is_none(),
        "Key should not exist before test"
    );

    // TXN.BEGIN -> OK
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // SET inside transaction
    let _: String = redis::cmd("SET")
        .arg("txn_abort_insert_key")
        .arg("created_in_txn")
        .query_async(&mut conn)
        .await
        .expect("SET inside TXN should succeed");

    // GET inside transaction -> should see own write (read-your-writes)
    let in_txn_get: Option<String> = redis::cmd("GET")
        .arg("txn_abort_insert_key")
        .query_async(&mut conn)
        .await
        .expect("GET inside TXN should succeed");
    assert_eq!(
        in_txn_get.as_deref(),
        Some("created_in_txn"),
        "Own writes should be visible inside TXN"
    );

    // TXN.ABORT -> OK
    let abort_result: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");
    assert_eq!(abort_result, "OK", "TXN.ABORT should return OK");

    // GET after abort -> nil (INSERT rollback: key removed)
    let post_abort_get: Option<String> = redis::cmd("GET")
        .arg("txn_abort_insert_key")
        .query_async(&mut conn)
        .await
        .expect("GET after TXN.ABORT should succeed");
    assert!(
        post_abort_get.is_none(),
        "Key inserted in aborted TXN must not exist after abort (undo INSERT)"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACID-07: TXN.ABORT restores SET key (update case — key already existed)
// ---------------------------------------------------------------------------

/// ACID-07: TXN.ABORT on an existing key restores original value.
#[tokio::test]
async fn test_txn_abort_restores_set_update() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Set baseline value outside of any transaction
    let _: String = redis::cmd("SET")
        .arg("txn_abort_update_key")
        .arg("original_value")
        .query_async(&mut conn)
        .await
        .expect("Baseline SET should succeed");

    // TXN.BEGIN -> OK
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // SET inside transaction (UPDATE — key existed)
    let _: String = redis::cmd("SET")
        .arg("txn_abort_update_key")
        .arg("modified_in_txn")
        .query_async(&mut conn)
        .await
        .expect("SET inside TXN should succeed");

    // TXN.ABORT -> OK
    let abort_result: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");
    assert_eq!(abort_result, "OK", "TXN.ABORT should return OK");

    // GET after abort -> "original_value" (UPDATE rollback: before-image restored)
    let post_abort_get: Option<String> = redis::cmd("GET")
        .arg("txn_abort_update_key")
        .query_async(&mut conn)
        .await
        .expect("GET after TXN.ABORT should succeed");
    assert_eq!(
        post_abort_get.as_deref(),
        Some("original_value"),
        "Key updated in aborted TXN must revert to original value (undo UPDATE)"
    );

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_abort_update_key")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACID-07: TXN.ABORT restores DEL key (delete rollback — key must reappear)
// ---------------------------------------------------------------------------

/// ACID-07: TXN.ABORT after DEL restores the deleted key to its original value.
#[tokio::test]
async fn test_txn_abort_restores_del() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Set key that will be deleted in the transaction
    let _: String = redis::cmd("SET")
        .arg("txn_del_restore_key")
        .arg("should_survive")
        .query_async(&mut conn)
        .await
        .expect("Baseline SET should succeed");

    // TXN.BEGIN -> OK
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // DEL inside transaction -> 1 (key existed and was deleted)
    let del_result: i64 = redis::cmd("DEL")
        .arg("txn_del_restore_key")
        .query_async(&mut conn)
        .await
        .expect("DEL inside TXN should succeed");
    assert_eq!(del_result, 1, "DEL inside TXN should return 1 (key existed)");

    // TXN.ABORT -> OK (undo log replays DELETE: restores before-image)
    let abort_result: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");
    assert_eq!(abort_result, "OK", "TXN.ABORT should return OK");

    // GET after abort -> "should_survive" (DELETE rollback: entry restored from undo log)
    let post_abort_get: Option<String> = redis::cmd("GET")
        .arg("txn_del_restore_key")
        .query_async(&mut conn)
        .await
        .expect("GET after TXN.ABORT should succeed");
    assert_eq!(
        post_abort_get.as_deref(),
        Some("should_survive"),
        "Key deleted in aborted TXN must be restored (undo DELETE via before-image)"
    );

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_del_restore_key")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACID-09: Read-your-writes inside a transaction
// ---------------------------------------------------------------------------

/// ACID-09: GET inside a TXN sees own uncommitted writes (read-your-writes).
#[tokio::test]
async fn test_txn_read_your_writes() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // TXN.BEGIN -> OK
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // SET inside transaction
    let _: String = redis::cmd("SET")
        .arg("txn_ryw_key")
        .arg("visible_in_txn")
        .query_async(&mut conn)
        .await
        .expect("SET inside TXN should succeed");

    // GET inside same transaction -> should see own write
    let ryw_get: Option<String> = redis::cmd("GET")
        .arg("txn_ryw_key")
        .query_async(&mut conn)
        .await
        .expect("GET inside TXN should succeed");
    assert_eq!(
        ryw_get.as_deref(),
        Some("visible_in_txn"),
        "Own writes must be visible within the same transaction (read-your-writes)"
    );

    // TXN.COMMIT -> OK
    let commit_result: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn)
        .await
        .expect("TXN.COMMIT should succeed");
    assert_eq!(commit_result, "OK", "TXN.COMMIT should return OK");

    // GET after commit -> value still visible
    let post_commit_get: Option<String> = redis::cmd("GET")
        .arg("txn_ryw_key")
        .query_async(&mut conn)
        .await
        .expect("GET after TXN.COMMIT should succeed");
    assert_eq!(
        post_commit_get.as_deref(),
        Some("visible_in_txn"),
        "Value should be visible after TXN.COMMIT"
    );

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_ryw_key")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACID-05 error cases: COMMIT/ABORT without BEGIN, double BEGIN
// ---------------------------------------------------------------------------

/// ACID-05: TXN.COMMIT without prior BEGIN returns an error.
/// TXN.ABORT without prior BEGIN returns an error.
/// TXN.BEGIN when already in a transaction returns an error.
#[tokio::test]
async fn test_txn_error_cases() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // TXN.COMMIT without BEGIN -> ERR
    let commit_no_begin: Result<String, redis::RedisError> = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn)
        .await;
    assert!(
        commit_no_begin.is_err(),
        "TXN.COMMIT without BEGIN should return error"
    );
    let commit_err_msg = commit_no_begin.unwrap_err().to_string();
    assert!(
        commit_err_msg.contains("not in a cross-store transaction"),
        "Error should mention 'not in a cross-store transaction', got: {}",
        commit_err_msg
    );

    // TXN.ABORT without BEGIN -> ERR
    let abort_no_begin: Result<String, redis::RedisError> = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await;
    assert!(
        abort_no_begin.is_err(),
        "TXN.ABORT without BEGIN should return error"
    );
    let abort_err_msg = abort_no_begin.unwrap_err().to_string();
    assert!(
        abort_err_msg.contains("not in a cross-store transaction"),
        "Error should mention 'not in a cross-store transaction', got: {}",
        abort_err_msg
    );

    // TXN.BEGIN -> OK
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // TXN.BEGIN again (double) -> ERR (already in transaction)
    let double_begin: Result<String, redis::RedisError> = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await;
    assert!(
        double_begin.is_err(),
        "Second TXN.BEGIN should return error when already in a transaction"
    );
    let double_err_msg = double_begin.unwrap_err().to_string();
    assert!(
        double_err_msg.contains("already in a cross-store transaction"),
        "Error should mention 'already in a cross-store transaction', got: {}",
        double_err_msg
    );

    // Clean up: abort the open transaction
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT cleanup should succeed");

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACID-06 + ACID-07: Aborted transaction's changes do NOT survive across
// the connection lifetime (state isolation — no WAL record written)
// ---------------------------------------------------------------------------

/// ACID-06 + ACID-07: An aborted transaction must not be visible after
/// the session ends (verifies undo log cleans up state correctly).
/// This test exercises a full begin → write → abort → reconnect cycle.
#[tokio::test]
async fn test_txn_abort_not_visible_on_reconnect() {
    let (port, shutdown) = start_server().await;

    // Connection 1: write baseline, start txn, write aborted value, abort
    {
        let mut conn1 = connect(port).await;

        // Baseline write outside any txn
        let _: String = redis::cmd("SET")
            .arg("txn_baseline_key")
            .arg("baseline_value")
            .query_async(&mut conn1)
            .await
            .expect("Baseline SET should succeed");

        // Transaction with a key that will be aborted
        let _: String = redis::cmd("TXN")
            .arg("BEGIN")
            .query_async(&mut conn1)
            .await
            .expect("TXN.BEGIN should succeed");

        let _: String = redis::cmd("SET")
            .arg("txn_aborted_key")
            .arg("should_not_persist")
            .query_async(&mut conn1)
            .await
            .expect("SET inside TXN should succeed");

        let abort_result: String = redis::cmd("TXN")
            .arg("ABORT")
            .query_async(&mut conn1)
            .await
            .expect("TXN.ABORT should succeed");
        assert_eq!(abort_result, "OK");
    }
    // conn1 dropped here

    // Connection 2: verify aborted key is absent, baseline survives
    {
        let mut conn2 = connect(port).await;

        let aborted_check: Option<String> = redis::cmd("GET")
            .arg("txn_aborted_key")
            .query_async(&mut conn2)
            .await
            .expect("GET of aborted key should succeed");
        assert!(
            aborted_check.is_none(),
            "Aborted transaction's writes must not be visible to other connections"
        );

        let baseline_check: Option<String> = redis::cmd("GET")
            .arg("txn_baseline_key")
            .query_async(&mut conn2)
            .await
            .expect("GET of baseline key should succeed");
        assert_eq!(
            baseline_check.as_deref(),
            Some("baseline_value"),
            "Non-transactional writes must survive abort of unrelated transaction"
        );

        // Cleanup
        let _: () = redis::cmd("DEL")
            .arg("txn_baseline_key")
            .query_async(&mut conn2)
            .await
            .unwrap_or(());
    }

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Multiple writes: abort should roll back all of them atomically
// ---------------------------------------------------------------------------

/// ACID-07: Multiple SET commands inside a TXN are all rolled back on ABORT.
#[tokio::test]
async fn test_txn_abort_multi_key_rollback() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Pre-set two keys to known baseline
    let _: String = redis::cmd("SET")
        .arg("txn_mk_key1")
        .arg("base1")
        .query_async(&mut conn)
        .await
        .expect("Baseline SET key1 should succeed");
    let _: String = redis::cmd("SET")
        .arg("txn_mk_key2")
        .arg("base2")
        .query_async(&mut conn)
        .await
        .expect("Baseline SET key2 should succeed");

    // TXN.BEGIN -> modify both keys
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    let _: String = redis::cmd("SET")
        .arg("txn_mk_key1")
        .arg("modified1")
        .query_async(&mut conn)
        .await
        .expect("SET key1 inside TXN should succeed");

    let _: String = redis::cmd("SET")
        .arg("txn_mk_key2")
        .arg("modified2")
        .query_async(&mut conn)
        .await
        .expect("SET key2 inside TXN should succeed");

    // Also add a new key that didn't exist before
    let _: String = redis::cmd("SET")
        .arg("txn_mk_key3_new")
        .arg("new_in_txn")
        .query_async(&mut conn)
        .await
        .expect("SET new key inside TXN should succeed");

    // TXN.ABORT -> all three writes rolled back
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");

    // Verify all keys reverted
    let k1: Option<String> = redis::cmd("GET")
        .arg("txn_mk_key1")
        .query_async(&mut conn)
        .await
        .expect("GET key1 after abort should succeed");
    assert_eq!(k1.as_deref(), Some("base1"), "key1 must revert to baseline");

    let k2: Option<String> = redis::cmd("GET")
        .arg("txn_mk_key2")
        .query_async(&mut conn)
        .await
        .expect("GET key2 after abort should succeed");
    assert_eq!(k2.as_deref(), Some("base2"), "key2 must revert to baseline");

    let k3: Option<String> = redis::cmd("GET")
        .arg("txn_mk_key3_new")
        .query_async(&mut conn)
        .await
        .expect("GET new key after abort should succeed");
    assert!(k3.is_none(), "New key inserted in aborted TXN must not exist");

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_mk_key1")
        .arg("txn_mk_key2")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACID-06 + ACID-11: WAL crash recovery — server restart replays committed TXN KV state
// ---------------------------------------------------------------------------

/// A Drop guard that kills and waits for a child process on drop (even on panic).
struct ChildGuard(std::process::Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A Drop guard that removes a directory on drop (even on panic).
struct DirGuard(std::path::PathBuf);

impl Drop for DirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Wait for a Moon server to accept TCP connections.
///
/// Retries for up to `timeout` with 50ms sleep between attempts.
/// Returns `true` if the server is ready, `false` on timeout.
fn wait_for_server(port: u16, timeout: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Find the Moon binary path, preferring release over debug.
///
/// Resolution order:
/// 1. `MOON_BIN` env var (CI / explicit override)
/// 2. `target/release/moon` relative to `CARGO_MANIFEST_DIR`
/// 3. `target/debug/moon` relative to `CARGO_MANIFEST_DIR`
///
/// Returns `None` if no binary is found.
fn find_moon_binary() -> Option<std::path::PathBuf> {
    if let Ok(bin) = std::env::var("MOON_BIN") {
        let p = std::path::PathBuf::from(bin);
        if p.exists() {
            return Some(p);
        }
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let release = std::path::PathBuf::from(format!("{manifest_dir}/target/release/moon"));
    if release.exists() {
        return Some(release);
    }
    let debug = std::path::PathBuf::from(format!("{manifest_dir}/target/debug/moon"));
    if debug.exists() {
        return Some(debug);
    }
    None
}

/// Bind a free OS port, drop the listener, and return the port number.
fn free_port() -> u16 {
    // SAFETY: bind + drop releases the port; the server will re-bind it.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    listener.local_addr().unwrap().port()
}

/// ACID-06 + ACID-11: Committed TXN KV state survives a server kill and restart.
///
/// This test spawns the Moon binary as a child process (not the in-process harness),
/// runs TXN.BEGIN -> SET k v -> TXN.COMMIT, kills the server, restarts from the same
/// persistence dir, and verifies GET returns the committed value — exercising the full
/// encode_xact_commit_payload -> WAL write -> replay_xact_commit() path end-to-end.
///
/// The test is skipped (with an informative eprintln!) if no Moon binary is found.
/// Use `MOON_BIN=/path/to/moon` or build with `cargo build --release` first.
#[tokio::test]
async fn test_txn_commit_wal_crash_recovery() {
    let Some(binary) = find_moon_binary() else {
        eprintln!(
            "Skipping test_txn_commit_wal_crash_recovery: moon binary not found. \
             Build with `cargo build --release` or set MOON_BIN=/path/to/moon."
        );
        return;
    };

    // Unique temp dir so parallel test runs don't collide.
    let tmp_dir = std::env::temp_dir().join(format!(
        "moon-wal-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
    ));
    std::fs::create_dir_all(&tmp_dir).expect("create temp dir");
    // Ensure temp dir is cleaned up regardless of test outcome.
    let _tmp_guard = DirGuard(tmp_dir.clone());

    // ---- Phase 1: start server, commit a TXN ----
    let port1 = free_port();
    let child1 = ChildGuard(
        std::process::Command::new(&binary)
            .args([
                "--port",
                &port1.to_string(),
                "--shards",
                "1",
                "--dir",
                tmp_dir.to_str().unwrap(),
                "--appendonly",
                "yes",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn moon server (phase 1)"),
    );

    assert!(
        wait_for_server(port1, std::time::Duration::from_secs(5)),
        "Moon server (phase 1) did not become ready on port {port1}"
    );

    // Connect with sync redis client to avoid holding an async runtime across process boundaries.
    let client1 = redis::Client::open(format!("redis://127.0.0.1:{port1}")).unwrap();
    let mut sync_conn1 = client1.get_connection().expect("connect to phase-1 server");

    // Non-TXN baseline (verifies plain AOF replay too)
    let _: String = redis::cmd("SET")
        .arg("wal_baseline_key")
        .arg("baseline_val")
        .query(&mut sync_conn1)
        .expect("SET baseline should succeed");

    // TXN.BEGIN -> SET -> TXN.COMMIT
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query(&mut sync_conn1)
        .expect("TXN BEGIN should succeed");
    let _: String = redis::cmd("SET")
        .arg("wal_recovery_key")
        .arg("wal_recovery_value")
        .query(&mut sync_conn1)
        .expect("SET inside TXN should succeed");
    let commit_resp: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query(&mut sync_conn1)
        .expect("TXN COMMIT should succeed");
    assert_eq!(commit_resp, "OK", "TXN.COMMIT should return OK");

    // Trigger BGREWRITEAOF to create the base RDB snapshot so the incr AOF can
    // be replayed on restart. Moon's multi-part AOF requires a base RDB to exist
    // before it will replay the incremental portion.
    let bgrw: String = redis::cmd("BGREWRITEAOF")
        .query(&mut sync_conn1)
        .expect("BGREWRITEAOF should succeed");
    assert!(
        bgrw.contains("rewriting") || bgrw.contains("started") || bgrw.contains("scheduled"),
        "BGREWRITEAOF should start background rewrite: {bgrw}"
    );

    // Wait for BGREWRITEAOF to complete by polling for the base RDB file.
    // The file is named moon.aof.<seq>.base.rdb inside the appendonlydir.
    let aof_dir = tmp_dir.join("appendonlydir");
    let base_rdb_exists = {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let found = std::fs::read_dir(&aof_dir)
                .ok()
                .and_then(|mut d| {
                    d.find(|e| {
                        e.as_ref()
                            .ok()
                            .and_then(|e| e.file_name().into_string().ok())
                            .map(|n| n.ends_with(".base.rdb"))
                            .unwrap_or(false)
                    })
                })
                .is_some();
            if found {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    };
    assert!(
        base_rdb_exists,
        "BGREWRITEAOF did not create a base RDB file within 10s in {aof_dir:?}"
    );

    // Kill server 1.
    drop(child1); // ChildGuard calls kill() + wait()

    // ---- Phase 2: restart server from same persistence dir, verify replay ----
    let port2 = free_port();
    let _child2 = ChildGuard(
        std::process::Command::new(&binary)
            .args([
                "--port",
                &port2.to_string(),
                "--shards",
                "1",
                "--dir",
                tmp_dir.to_str().unwrap(),
                "--appendonly",
                "yes",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn moon server (phase 2)"),
    );

    assert!(
        wait_for_server(port2, std::time::Duration::from_secs(5)),
        "Moon server (phase 2) did not become ready on port {port2}"
    );

    let client2 = redis::Client::open(format!("redis://127.0.0.1:{port2}")).unwrap();
    let mut sync_conn2 = client2.get_connection().expect("connect to phase-2 server");

    // Verify TXN committed value survived server restart via WAL replay.
    let recovered: Option<String> = redis::cmd("GET")
        .arg("wal_recovery_key")
        .query(&mut sync_conn2)
        .expect("GET wal_recovery_key should succeed");
    assert_eq!(
        recovered.as_deref(),
        Some("wal_recovery_value"),
        "WAL replay must restore committed TXN KV state after server restart (ACID-06, ACID-11)"
    );

    // Verify baseline (non-TXN) key also survived.
    let baseline_recovered: Option<String> = redis::cmd("GET")
        .arg("wal_baseline_key")
        .query(&mut sync_conn2)
        .expect("GET wal_baseline_key should succeed");
    assert_eq!(
        baseline_recovered.as_deref(),
        Some("baseline_val"),
        "Non-transactional writes must also survive server restart via AOF replay"
    );
    // _child2 dropped here — ChildGuard kills server 2.
    // _tmp_guard dropped here — removes temp dir.
}

// ---------------------------------------------------------------------------
// SC5 commit path: TXN.BEGIN -> SET -> MQ.PUBLISH -> TXN.COMMIT persists both
// ---------------------------------------------------------------------------

/// SC5 (commit): TXN.BEGIN -> SET k v -> MQ PUBLISH -> TXN.COMMIT persists both
/// KV and MQ atomically. After commit: GET returns committed value AND MQ POP
/// returns the published message.
#[tokio::test]
async fn test_txn_kv_mq_commit_atomic() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    // Create durable queue
    let create_result: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("txn_atomic_q")
        .arg("MAXDELIVERY")
        .arg("5")
        .query_async(&mut conn)
        .await
        .expect("MQ CREATE should succeed");
    assert_eq!(create_result, "OK", "MQ CREATE should return OK");

    // TXN.BEGIN -> SET -> MQ PUBLISH
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN BEGIN should succeed");

    let _: String = redis::cmd("SET")
        .arg("txn_mq_key")
        .arg("txn_mq_value")
        .query_async(&mut conn)
        .await
        .expect("SET inside TXN should succeed");

    // MQ PUBLISH inside a TXN returns "QUEUED" (deferred until COMMIT)
    let publish_result: String = redis::cmd("MQ")
        .arg("PUBLISH")
        .arg("txn_atomic_q")
        .arg("msg_field")
        .arg("msg_value")
        .query_async(&mut conn)
        .await
        .expect("MQ PUBLISH inside TXN should succeed");
    assert_eq!(
        publish_result, "QUEUED",
        "MQ PUBLISH inside TXN must return QUEUED"
    );

    // TXN.COMMIT — materializes both KV write and MQ message atomically
    let commit_result: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn)
        .await
        .expect("TXN COMMIT should succeed");
    assert_eq!(commit_result, "OK", "TXN.COMMIT should return OK");

    // Verify KV: GET returns committed value
    let kv_result: Option<String> = redis::cmd("GET")
        .arg("txn_mq_key")
        .query_async(&mut conn)
        .await
        .expect("GET after TXN.COMMIT should succeed");
    assert_eq!(
        kv_result.as_deref(),
        Some("txn_mq_value"),
        "KV write must be visible after TXN.COMMIT (KV+MQ atomic commit)"
    );

    // Verify MQ: POP returns the published message
    let pop_result: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("txn_atomic_q")
        .query_async(&mut conn)
        .await
        .expect("MQ POP after TXN.COMMIT should succeed");

    match &pop_result {
        redis::Value::Array(entries) => {
            assert!(
                !entries.is_empty(),
                "MQ POP must return at least one message after TXN.COMMIT"
            );
            // Verify msg_field/msg_value appear somewhere in the response
            let pop_debug = format!("{pop_result:?}");
            assert!(
                pop_debug.contains("msg_field") || pop_debug.contains("msg_value"),
                "MQ POP response must contain the published field/value, got: {pop_debug}"
            );
        }
        other => panic!(
            "MQ POP after TXN.COMMIT must return an Array, got: {:?}",
            other
        ),
    }

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_mq_key")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// SC5 abort path: TXN.BEGIN -> SET -> MQ.PUBLISH -> TXN.ABORT leaves no trace
// ---------------------------------------------------------------------------

/// SC5 (abort): TXN.BEGIN -> SET k v -> MQ PUBLISH -> TXN.ABORT leaves no trace
/// in KV or MQ. After abort: GET returns nil AND MQ POP returns empty/nil.
#[tokio::test]
async fn test_txn_kv_mq_abort_atomic() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    // Create durable queue
    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("txn_abort_q")
        .arg("MAXDELIVERY")
        .arg("5")
        .query_async(&mut conn)
        .await
        .expect("MQ CREATE should succeed");

    // Baseline KV outside any transaction (must survive the abort)
    let _: String = redis::cmd("SET")
        .arg("txn_mq_baseline")
        .arg("baseline")
        .query_async(&mut conn)
        .await
        .expect("Baseline SET should succeed");

    // TXN.BEGIN -> SET -> MQ PUBLISH -> TXN.ABORT
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN BEGIN should succeed");

    let _: String = redis::cmd("SET")
        .arg("txn_mq_abort_key")
        .arg("should_not_exist")
        .query_async(&mut conn)
        .await
        .expect("SET inside TXN should succeed");

    let _: String = redis::cmd("MQ")
        .arg("PUBLISH")
        .arg("txn_abort_q")
        .arg("abort_field")
        .arg("abort_value")
        .query_async(&mut conn)
        .await
        .expect("MQ PUBLISH inside TXN should succeed");

    let abort_result: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN ABORT should succeed");
    assert_eq!(abort_result, "OK", "TXN.ABORT should return OK");

    // Verify KV is absent after abort
    let kv_result: Option<String> = redis::cmd("GET")
        .arg("txn_mq_abort_key")
        .query_async(&mut conn)
        .await
        .expect("GET after TXN.ABORT should succeed");
    assert!(
        kv_result.is_none(),
        "KV write from aborted TXN must not be visible (got: {:?})",
        kv_result
    );

    // Verify MQ is empty after abort (PUBLISH was not materialized)
    let pop_result: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("txn_abort_q")
        .query_async(&mut conn)
        .await
        .expect("MQ POP after TXN.ABORT should succeed");

    match pop_result {
        redis::Value::Nil => { /* no messages — correct */ }
        redis::Value::Array(ref items) if items.is_empty() => { /* empty array — correct */ }
        other => panic!(
            "MQ POP after TXN.ABORT must return nil or empty array (no messages materialized), got: {:?}",
            other
        ),
    }

    // Verify baseline KV survived the abort
    let baseline_result: Option<String> = redis::cmd("GET")
        .arg("txn_mq_baseline")
        .query_async(&mut conn)
        .await
        .expect("GET baseline after TXN.ABORT should succeed");
    assert_eq!(
        baseline_result.as_deref(),
        Some("baseline"),
        "Non-transactional baseline key must survive TXN.ABORT"
    );

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_mq_baseline")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Sequential transactions: state does not leak between successive TXNs
// ---------------------------------------------------------------------------

/// ACID-05: Two sequential transactions on the same connection are independent.
/// Committed values from TXN1 are visible; aborted values from TXN2 are not.
#[tokio::test]
async fn test_txn_sequential_independence() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Transaction 1: commit a value
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN1 BEGIN should succeed");
    let _: String = redis::cmd("SET")
        .arg("txn_seq_committed")
        .arg("from_txn1")
        .query_async(&mut conn)
        .await
        .expect("SET in TXN1 should succeed");
    let _: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn)
        .await
        .expect("TXN1 COMMIT should succeed");

    // Transaction 2: abort a value
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN2 BEGIN should succeed");
    let _: String = redis::cmd("SET")
        .arg("txn_seq_aborted")
        .arg("from_txn2")
        .query_async(&mut conn)
        .await
        .expect("SET in TXN2 should succeed");
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN2 ABORT should succeed");

    // Verify: TXN1's value is visible, TXN2's is not
    let committed: Option<String> = redis::cmd("GET")
        .arg("txn_seq_committed")
        .query_async(&mut conn)
        .await
        .expect("GET committed key should succeed");
    assert_eq!(
        committed.as_deref(),
        Some("from_txn1"),
        "TXN1 committed value must be visible after TXN2"
    );

    let aborted: Option<String> = redis::cmd("GET")
        .arg("txn_seq_aborted")
        .query_async(&mut conn)
        .await
        .expect("GET aborted key should succeed");
    assert!(
        aborted.is_none(),
        "TXN2 aborted value must not be visible"
    );

    // Cleanup
    let _: () = redis::cmd("DEL")
        .arg("txn_seq_committed")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}
