//! Phase 174 FIX-02 -- Adversarial RED/GREEN coverage for partial-Err intent
//! surfacing so TXN.ABORT can roll back partial mutations.
//!
//! Source: merged_bug_015 residual (ultrareview of v0.1.9 / commit 2e397c7).
//! Before this fix, `graph_read.rs:598` returned `Vec::new()` intents on the
//! Err branch of `execute_mut`, discarding any partial mutations that the
//! executor accumulated before hitting the error. This means `TXN.ABORT`
//! cannot undo partial CREATEs that happened before the error — ghost nodes
//! persist silently.
//!
//! **Err trigger chosen:** `CALL db.labels() YIELD label` — procedure calls are
//! unsupported in `execute_mut` (returns `ExecError::Unsupported`). By placing
//! a `CREATE` BEFORE the `CALL` in the same statement, the CREATE runs
//! successfully (pushing to `mutations`), then the CALL triggers the Err. On
//! buggy code, the Err branch discards the intent list → ABORT cannot find the
//! partial node → ghost node "Eve" survives.
//!
//! Test: `test_partial_err_surfaces_intents_for_rollback`
//!   1. BEGIN txn.
//!   2. Issue `CREATE (n:Person {name:'Eve'}) WITH n CALL db.labels() YIELD label RETURN label`
//!      which creates Eve then hits Err.
//!   3. Assert server replies Frame::Error.
//!   4. ABORT txn.
//!   5. MATCH (n {name:'Eve'}) RETURN n → must be 0 rows (Eve was rolled back).
//!
//! Runtime: `cargo test --no-default-features --features runtime-tokio,jemalloc,graph
//! --test adversarial_v0110_fix02_err_path_intent -- --test-threads=1`

#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure — mirrors adversarial_v0110_fix01_set_delete_rollback.rs
// ---------------------------------------------------------------------------

async fn start_txn_server(num_shards: usize) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        requirepass: None,
        appendonly: "no".to_string(),
        appendfsync: "everysec".to_string(),
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
        recovery_target_lsn: None,
        recovery_target_time: None,
        manifest_tombstone_retain_epochs: 2,
        manifest_tombstone_retain_secs: 300,
        disk_free_min_pct: 5,
        mvcc_committed_prune_margin: 1000,
        max_unflushed_immutable_segments: 20,
        mvcc_old_snapshot_threshold_secs: 600,
        autovacuum: "enable".to_string(),
        autovacuum_budget_ms_min: 5,
        autovacuum_budget_ms_max: 200,
        autovacuum_target_p95_ms: 10,
        autovacuum_interval_secs: 30,
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
                .name(format!("fix02-shard-{}", id))
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

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    (port, token)
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Number of rows in a GRAPH.QUERY response (Array[headers, rows, stats]).
fn row_count(v: &redis::Value) -> usize {
    let redis::Value::Array(items) = v else {
        return 0;
    };
    if items.len() < 2 {
        return 0;
    }
    match &items[1] {
        redis::Value::Array(rows) => rows.len(),
        _ => 0,
    }
}

// ===========================================================================
// TEST: Partial-Err intents are surfaced for TXN.ABORT rollback.
//
// The Cypher statement creates node Eve (partial mutation), then hits an
// unsupported ProcedureCall which returns ExecError::Unsupported. On the
// current buggy code, the Err branch in graph_read.rs discards intents via
// Vec::new(), so TXN.ABORT cannot find Eve to roll her back. Eve persists as
// a ghost node.
//
// After FIX-02, the Err branch surfaces partial mutations as intents, and
// ABORT removes Eve.
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_partial_err_surfaces_intents_for_rollback() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    // Create graph.
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Open transaction.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN");

    // Issue a Cypher statement that FIRST creates Eve, THEN hits an error.
    // CREATE runs as PhysicalOp::CreatePattern (pushes to `mutations` vec),
    // then WITH passes through, then CALL db.labels() hits
    // ExecError::Unsupported("procedure calls not yet implemented in executor").
    //
    // The server MUST return Frame::Error for this statement.
    let err_result: redis::RedisResult<redis::Value> = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("CREATE (n:Person {name:'Eve'}) WITH n CALL db.labels() YIELD label RETURN label")
        .query_async(&mut conn)
        .await;

    // The command must fail (Frame::Error from the unsupported CALL).
    assert!(
        err_result.is_err(),
        "Expected Frame::Error from unsupported procedure call, got Ok: {err_result:?}"
    );

    // ABORT the transaction — this should undo the partial CREATE of Eve.
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT");

    // Post-abort: Eve MUST NOT exist. On buggy code, Eve persists as a ghost
    // because the Err branch returned Vec::new() intents — ABORT had nothing
    // to roll back.
    let post: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (n:Person {name:'Eve'}) RETURN n.name")
        .query_async(&mut conn)
        .await
        .expect("post-abort MATCH");

    assert_eq!(
        row_count(&post),
        0,
        "TXN.ABORT must roll back partial CREATE of Eve from the Err path. \
         Found {} row(s) — FIX-02: Err branch discards intents via Vec::new(), \
         so ABORT cannot undo the partial CREATE. Ghost node persists. Resp={post:?}",
        row_count(&post),
    );

    shutdown.cancel();
}
