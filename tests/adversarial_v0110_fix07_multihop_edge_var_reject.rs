//! Phase 174 FIX-07 — Adversarial RED/GREEN coverage for multi-hop edge variable
//! reject at plan time.
//!
//! Source: bug_023 (ultrareview of v0.1.9 / commit 2e397c7). The planner at
//! `src/graph/cypher/planner.rs` unconditionally emits `PhysicalOp::Expand` with
//! `edge_variable` set, even for variable-length patterns. The BFS branch in
//! `executor/read.rs:122-131` never inserts the edge variable into the output
//! row — so `WHERE r.prop >= x` silently evaluates against Null and returns
//! empty results instead of a typed error.
//!
//! Fix: At plan time, when `edge.var_length.is_some() && edge.variable.is_some()`,
//! raise typed CYP-06 error: "Multi-hop edge variable binding not yet supported."
//!
//! Tests:
//!   1. `test_multihop_edge_var_rejected_at_plan_time` — asserts typed error
//!   2. `test_fixed_length_edge_var_still_works` — no regression for single-hop
//!   3. `test_varlength_without_edge_var_still_works` — no regression for anonymous
//!
//! Runtime: `cargo test --no-default-features --features runtime-tokio,jemalloc,graph
//! --test adversarial_v0110_fix07_multihop_edge_var_reject -- --test-threads=1`

#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure — mirrors existing adversarial test harnesses.
// ---------------------------------------------------------------------------

async fn start_server(num_shards: usize) -> (u16, CancellationToken) {
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
                .name(format!("fix07-shard-{}", id))
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a redis result is an error whose message contains `needle`.
fn is_error_containing(result: &Result<redis::Value, redis::RedisError>, needle: &str) -> bool {
    match result {
        Err(e) => {
            let msg = format!("{e}");
            msg.to_lowercase().contains(&needle.to_lowercase())
        }
        Ok(redis::Value::Array(items)) => {
            // Some graph responses wrap errors in the array
            for item in items {
                if let redis::Value::BulkString(b) = item {
                    if let Ok(s) = std::str::from_utf8(b) {
                        if s.to_lowercase().contains(&needle.to_lowercase()) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        _ => false,
    }
}

/// Extract row count from GRAPH.QUERY response (Array[headers, rows, stats]).
fn rows_count(v: &redis::Value) -> usize {
    match v {
        redis::Value::Array(items) if items.len() >= 2 => match &items[1] {
            redis::Value::Array(rows) => rows.len(),
            _ => 0,
        },
        _ => 0,
    }
}

/// Extract first row, first column as string.
fn first_string(v: &redis::Value) -> Option<String> {
    let redis::Value::Array(items) = v else {
        return None;
    };
    let redis::Value::Array(rows) = items.get(1)? else {
        return None;
    };
    let redis::Value::Array(cells) = rows.first()? else {
        return None;
    };
    match cells.first()? {
        redis::Value::BulkString(b) => std::str::from_utf8(b).ok().map(|s| s.to_string()),
        redis::Value::SimpleString(s) => Some(s.clone()),
        redis::Value::Int(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Seed a trivial graph: A -[:T]-> C -[:T]-> B
async fn seed_chain(conn: &mut redis::aio::MultiplexedConnection) {
    let _: redis::Value = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(conn)
        .await
        .expect("GRAPH.CREATE");

    let a_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Node")
        .arg("id")
        .arg("A")
        .query_async(conn)
        .await
        .expect("GRAPH.ADDNODE A");

    let c_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Node")
        .arg("id")
        .arg("C")
        .query_async(conn)
        .await
        .expect("GRAPH.ADDNODE C");

    let b_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Node")
        .arg("id")
        .arg("B")
        .query_async(conn)
        .await
        .expect("GRAPH.ADDNODE B");

    // A -> C
    let _: redis::Value = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(a_id)
        .arg(c_id)
        .arg("T")
        .arg("valid_to")
        .arg(9999_i64)
        .query_async(conn)
        .await
        .expect("GRAPH.ADDEDGE A->C");

    // C -> B
    let _: redis::Value = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(c_id)
        .arg(b_id)
        .arg("T")
        .arg("valid_to")
        .arg(9999_i64)
        .query_async(conn)
        .await
        .expect("GRAPH.ADDEDGE C->B");
}

// ---------------------------------------------------------------------------
// Test 1: Multi-hop edge variable MUST be rejected at plan time (CYP-06).
//
// Before fix: `MATCH (a)-[r:T*2..5]->(b)` silently returns empty results
// because the BFS branch never binds `r` into the output row.
//
// After fix: planner returns typed error with "Multi-hop edge variable" and
// references MVCC-02 / Phase 179.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multihop_edge_var_rejected_at_plan_time() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    seed_chain(&mut conn).await;

    // This query has both var_length (*2..5) AND an edge variable (r).
    // It must return a typed error, NOT empty results.
    let result: Result<redis::Value, redis::RedisError> = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Node {id:'A'})-[r:T*2..5]->(b:Node {id:'B'}) WHERE r.valid_to >= 0 RETURN b.id")
        .query_async(&mut conn)
        .await;

    // Assert we got an error (not a successful empty result).
    let got_error = is_error_containing(&result, "multi-hop edge variable")
        || is_error_containing(&result, "not yet supported");

    // If the result is Ok with zero rows, that's the old buggy behaviour.
    if let Ok(ref v) = result {
        let rc = rows_count(v);
        assert!(
            got_error,
            "BUG: MATCH (a)-[r:T*2..5]->(b) returned {rc} rows instead of typed CYP-06 error. \
             Response: {v:?}"
        );
    }

    assert!(
        got_error,
        "Expected CYP-06 error containing 'multi-hop edge variable' or 'not yet supported', \
         got: {result:?}"
    );

    // Verify error references Phase 179 / MVCC-02 for tracking.
    let references_tracking =
        is_error_containing(&result, "MVCC-02") || is_error_containing(&result, "Phase 179");
    assert!(
        references_tracking,
        "Error must reference MVCC-02 or Phase 179 tracking ticket. Got: {result:?}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2: Fixed-length with edge variable still works (no regression).
//
// `MATCH (a)-[r:T]->(b)` is single-hop — edge variable IS bound correctly
// by the executor. This must NOT be rejected.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_fixed_length_edge_var_still_works() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    seed_chain(&mut conn).await;

    // Single-hop with edge variable — must succeed and return edge property.
    // We access r.valid_to which we set to 9999 on the A->C edge.
    let result: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Node {id:'A'})-[r:T]->(c) RETURN r.valid_to")
        .query_async(&mut conn)
        .await
        .expect("Single-hop MATCH with edge variable must succeed");

    let rc = rows_count(&result);
    assert!(
        rc >= 1,
        "Expected at least 1 row from single-hop MATCH (a)-[r:T]->(c), got {rc}. \
         Response: {result:?}"
    );

    // r.valid_to should be 9999 (set in seed_chain for A->C edge).
    let val = first_string(&result);
    assert_eq!(
        val.as_deref(),
        Some("9999"),
        "Expected r.valid_to = '9999' for single-hop edge variable binding, got {val:?}. \
         Response: {result:?}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 3: Variable-length WITHOUT edge variable still works (no regression).
//
// `MATCH (a)-[:T*2..5]->(b)` has var_length but NO edge variable — this must
// continue working and return the 2-hop match A->C->B.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_varlength_without_edge_var_still_works() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    seed_chain(&mut conn).await;

    // Variable-length without edge variable — must succeed and find B via 2-hop.
    let result: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Node {id:'A'})-[:T*2..5]->(b) RETURN b.id")
        .query_async(&mut conn)
        .await
        .expect("Variable-length MATCH without edge variable must succeed");

    let rc = rows_count(&result);
    assert!(
        rc >= 1,
        "Expected at least 1 row from MATCH (a)-[:T*2..5]->(b), got {rc}. \
         Response: {result:?}"
    );

    let b_id = first_string(&result);
    assert_eq!(
        b_id.as_deref(),
        Some("B"),
        "Expected b.id = 'B' (2-hop A->C->B), got {b_id:?}"
    );

    shutdown.cancel();
}
