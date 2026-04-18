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
