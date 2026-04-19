//! Integration tests — TXN completeness edge cases (TDD-165-166, G6–G13).
//!
//! Seven tests covering boundary conditions that Phase 166 must handle:
//!
//!   G6:  Empty TXN abort (no intents) → returns OK, no crash.
//!   G7:  Double abort → second TXN.ABORT returns ERR "not in a cross-store
//!        transaction"; state remains sane.
//!   G8:  TXN.COMMIT regression — GRAPH.ADDNODE + HSET (vector) inside one TXN,
//!        COMMIT → graph count==1 AND FT.SEARCH returns 1 hit. Verify the abort
//!        helper does NOT fire on the COMMIT path.
//!   G9:  Multi-graph TXN abort — writes to two different graphs in one TXN,
//!        ABORT reverts both. graph1 and graph2 node_count==0.
//!   G11: Connection drop with only graph intents → graph rolled back (node_count==0).
//!   G12: TXN.BEGIN → TXN.BEGIN (nested) → second BEGIN returns ERR; first TXN
//!        remains intact (can still write + commit).
//!   G13: GRAPH.DELETE inside TXN then ABORT → rollback iterates graph_intents,
//!        finds graph gone, emits tracing::warn! and continues. No panic. ABORT
//!        returns OK.
//!
//! File gate: both `runtime-tokio` AND `graph` — CI-parity gate from
//! Plan 165-04 lesson. Graph intents require the `graph` feature; the TXN
//! interception lives on the `run_sharded` path.
//!
//! Run:
//!   cargo test --test txn_completeness_edge_cases \
//!       --no-default-features --features runtime-tokio,jemalloc,graph,text-index \
//!       -- --test-threads=1
#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Harness — mirror of `tests/txn_graph_wiring.rs::start_txn_server`.
// Duplicated per plan directive (no shared non-lib test helpers in Rust).
// ---------------------------------------------------------------------------

fn build_config(port: u16, num_shards: usize) -> ServerConfig {
    ServerConfig {
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
        maxclients: 10000,
        timeout: 0,
        tcp_keepalive: 300,
        console_auth_required: false,
        console_auth_secret: String::new(),
        console_cors_origin: vec![],
        console_rate_limit: 1000.0,
        console_rate_burst: 2000.0,
    }
}

async fn start_txn_server(num_shards: usize) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();
    let config = build_config(port, num_shards);
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
                .name(format!("edge-shard-{}", id))
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

async fn connect_dedicated(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

fn f32_blob(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Parse GRAPH.INFO response — supports both RESP3 Map and legacy flat-Array
/// forms — and return the node_count + edge_count fields.
fn parse_graph_info_counts(v: &redis::Value) -> (i64, i64) {
    let mut nodes: i64 = 0;
    let mut edges: i64 = 0;
    let lookup = |key: &[u8], val: &redis::Value, nodes: &mut i64, edges: &mut i64| match val {
        redis::Value::Int(n) => {
            if key == b"node_count" {
                *nodes = *n;
            }
            if key == b"edge_count" {
                *edges = *n;
            }
        }
        redis::Value::BulkString(b) => {
            if let Ok(s) = std::str::from_utf8(b)
                && let Ok(n) = s.parse::<i64>()
            {
                if key == b"node_count" {
                    *nodes = n;
                }
                if key == b"edge_count" {
                    *edges = n;
                }
            }
        }
        _ => {}
    };
    match v {
        redis::Value::Map(pairs) => {
            for (k, val) in pairs {
                let key_bytes: Option<&[u8]> = match k {
                    redis::Value::BulkString(b) => Some(b.as_slice()),
                    redis::Value::SimpleString(s) => Some(s.as_bytes()),
                    _ => None,
                };
                if let Some(kb) = key_bytes {
                    lookup(kb, val, &mut nodes, &mut edges);
                }
            }
        }
        redis::Value::Array(items) => {
            let mut i = 0;
            while i + 1 < items.len() {
                let key_bytes: Option<&[u8]> = match &items[i] {
                    redis::Value::BulkString(b) => Some(b.as_slice()),
                    redis::Value::SimpleString(s) => Some(s.as_bytes()),
                    _ => None,
                };
                if let Some(kb) = key_bytes {
                    lookup(kb, &items[i + 1], &mut nodes, &mut edges);
                }
                i += 2;
            }
        }
        _ => {}
    }
    (nodes, edges)
}

fn ft_search_count(v: &redis::Value) -> i64 {
    match v {
        redis::Value::Array(items) => match items.first() {
            Some(redis::Value::Int(n)) => *n,
            Some(redis::Value::BulkString(b)) => std::str::from_utf8(b)
                .expect("count is valid utf-8")
                .parse::<i64>()
                .expect("count parses as i64"),
            other => panic!("unexpected first item in FT.SEARCH response: {other:?}"),
        },
        other => panic!("expected Array from FT.SEARCH, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// G6: Empty TXN abort (no intents) → returns OK, no crash.
//
// Contract: `kv_intents.release_txn` handles the case of zero intents
// gracefully. `abort_cross_store_txn` iterates over empty vectors without
// panicking. TXN.ABORT returns "OK" even when the transaction body is empty.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_empty_txn_abort_returns_ok() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK", "TXN.BEGIN must return OK");

    // No writes — zero intents in kv_undo, graph_intents, vector_intents.

    let abort_ok: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT on empty TXN should succeed");
    assert_eq!(
        abort_ok, "OK",
        "TXN.ABORT on empty TXN must return OK (no panic, no crash)"
    );

    // Server must be alive after the no-op abort — verify with a simple command.
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .expect("PING after empty abort must succeed");
    assert_eq!(
        pong, "PONG",
        "Server must remain responsive after empty abort"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G7: Double abort idempotence.
//
// Contract: The first TXN.ABORT returns OK and clears `active_cross_txn`.
// The second TXN.ABORT must return ERR "not in a cross-store transaction".
// The shard state must remain sane (a subsequent TXN.BEGIN still works).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_double_abort_second_returns_err() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    let first_abort: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("First TXN.ABORT should succeed");
    assert_eq!(first_abort, "OK", "First TXN.ABORT must return OK");

    // Second ABORT — no active TXN → must return ERR.
    let second_abort: Result<String, redis::RedisError> =
        redis::cmd("TXN").arg("ABORT").query_async(&mut conn).await;
    let err = second_abort.expect_err("Second TXN.ABORT must return ERR (not in TXN)");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("not in a cross-store transaction"),
        "Second TXN.ABORT must report 'not in a cross-store transaction'; got: {err_msg}"
    );

    // State sanity: a fresh TXN.BEGIN must still succeed after double abort.
    let begin_again: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN after double abort must succeed");
    assert_eq!(
        begin_again, "OK",
        "TXN.BEGIN after double abort must return OK"
    );
    // Clean up: abort the new TXN.
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("cleanup ABORT should succeed");

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G8: TXN.COMMIT regression — graph + HNSW intents are consumed, not
// rolled back.
//
// Contract: Committing a TXN that contains both GRAPH.ADDNODE and HSET
// (with auto-indexing into HNSW) must:
//   - Leave graph node_count==1 (graph write committed).
//   - Leave FT.SEARCH returning count==1 (HNSW write committed, not tombstoned).
//
// This test guards against the abort helper accidentally running on the
// COMMIT path (regression from Plan 166-03 abort refactor).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_commit_graph_and_hnsw_both_persist() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    // Create graph and FT index outside TXN.
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g_commit")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE should succeed");

    let _: String = redis::cmd("FT.CREATE")
        .arg("commit_idx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("cv:{t}:")
        .arg("SCHEMA")
        .arg("vec")
        .arg("VECTOR")
        .arg("HNSW")
        .arg("6")
        .arg("DIM")
        .arg("4")
        .arg("TYPE")
        .arg("FLOAT32")
        .arg("DISTANCE_METRIC")
        .arg("L2")
        .query_async(&mut conn)
        .await
        .expect("FT.CREATE should succeed");

    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // GRAPH.ADDNODE inside TXN.
    let node_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g_commit")
        .arg("Entity")
        .arg("name")
        .arg("committed_node")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.ADDNODE inside TXN should succeed");
    assert!(node_id > 0, "GRAPH.ADDNODE must return positive id");

    // HSET (vector) inside TXN — auto-indexed into HNSW mutable segment.
    let vec_bytes = f32_blob(&[1.0, 0.0, 0.0, 0.0]);
    let _: i64 = redis::cmd("HSET")
        .arg("cv:{t}:1")
        .arg("vec")
        .arg(vec_bytes)
        .query_async(&mut conn)
        .await
        .expect("HSET inside TXN should succeed");

    // COMMIT — must NOT trigger abort path.
    let commit_ok: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn)
        .await
        .expect("TXN.COMMIT should succeed");
    assert_eq!(commit_ok, "OK", "TXN.COMMIT must return OK");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Verify graph: node_count must be 1 (not rolled back).
    let info: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("g_commit")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO should succeed");
    let (nodes, _) = parse_graph_info_counts(&info);
    assert_eq!(
        nodes, 1,
        "After COMMIT, graph must have 1 node (abort path must NOT run on COMMIT). Got node_count={nodes}"
    );

    // Verify vector: FT.SEARCH must return 1 hit (HNSW row not tombstoned).
    let q_bytes = f32_blob(&[1.0, 0.0, 0.0, 0.0]);
    let search: redis::Value = redis::cmd("FT.SEARCH")
        .arg("commit_idx")
        .arg("*=>[KNN 5 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH after COMMIT should succeed");
    let count = ft_search_count(&search);
    assert_eq!(
        count, 1,
        "After COMMIT, FT.SEARCH must return 1 hit (HNSW row committed, not tombstoned). Got count={count}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G9: Multi-graph TXN abort — writes to two different graphs in one TXN,
// ABORT reverts both via `graph_name` dispatch.
//
// Contract: `graph_intents` records the `graph_name` for each intent.
// The rollback iterator uses that name to look up the correct graph and
// call `remove_node`. Both graphs must end up with node_count==0.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_reverts_multi_graph() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    // Create two distinct graphs.
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("mg1")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE mg1 should succeed");
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("mg2")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE mg2 should succeed");

    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // ADDNODE to graph 1.
    let nid1: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("mg1")
        .arg("TypeA")
        .arg("name")
        .arg("alpha")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.ADDNODE mg1 should succeed inside TXN");
    assert!(nid1 > 0, "ADDNODE mg1 must return positive id");

    // ADDNODE to graph 2.
    let nid2: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("mg2")
        .arg("TypeB")
        .arg("name")
        .arg("beta")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.ADDNODE mg2 should succeed inside TXN");
    assert!(nid2 > 0, "ADDNODE mg2 must return positive id");

    // ABORT — must revert both graphs.
    let abort_ok: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");
    assert_eq!(abort_ok, "OK");

    // Verify mg1: node_count == 0.
    let info1: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("mg1")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO mg1 should succeed");
    let (nodes1, _) = parse_graph_info_counts(&info1);
    assert_eq!(
        nodes1, 0,
        "TXN.ABORT must revert mg1 node. Found node_count={nodes1} — graph_name dispatch missing."
    );

    // Verify mg2: node_count == 0.
    let info2: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("mg2")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO mg2 should succeed");
    let (nodes2, _) = parse_graph_info_counts(&info2);
    assert_eq!(
        nodes2, 0,
        "TXN.ABORT must revert mg2 node. Found node_count={nodes2} — graph_name dispatch missing."
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G11: Connection drop with only graph intents → graph rolled back.
//
// Contract: When a connection with `active_cross_txn` drops (TCP close),
// the disconnect handler calls `abort_cross_store_txn`. If the transaction
// contained only graph intents (no KV, no vector), the graph rollback phase
// MUST still run and revert the node.
//
// Test: Conn A: BEGIN → GRAPH.ADDNODE → drop TCP. New conn: GRAPH.INFO
// must show node_count==0.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_connection_drop_with_graph_intents_reverts_graph() {
    let (port, shutdown) = start_txn_server(1).await;

    // Create graph outside of any TXN.
    {
        let mut setup = connect(port).await;
        let _: String = redis::cmd("GRAPH.CREATE")
            .arg("drop_g")
            .query_async(&mut setup)
            .await
            .expect("GRAPH.CREATE should succeed");
    }

    // Conn A: BEGIN → GRAPH.ADDNODE → drop (no explicit ABORT).
    {
        let mut conn_a = connect_dedicated(port).await;
        let _: String = redis::cmd("TXN")
            .arg("BEGIN")
            .query_async(&mut conn_a)
            .await
            .expect("Conn A TXN.BEGIN should succeed");
        let nid: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("drop_g")
            .arg("Entity")
            .arg("name")
            .arg("ghost")
            .query_async(&mut conn_a)
            .await
            .expect("Conn A GRAPH.ADDNODE should succeed");
        assert!(nid > 0, "ADDNODE must return positive id");
        drop(conn_a); // TCP disconnect — triggers server-side disconnect cleanup.
    }

    // Give the server time to process the disconnect and run abort_cross_store_txn.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // New connection: verify graph was rolled back.
    let mut conn_b = connect(port).await;
    let info: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("drop_g")
        .query_async(&mut conn_b)
        .await
        .expect("GRAPH.INFO should succeed after disconnect");
    let (nodes, _) = parse_graph_info_counts(&info);
    assert_eq!(
        nodes, 0,
        "Graph must be rolled back after connection drop. Found node_count={nodes} — \
         disconnect path does not call abort_cross_store_txn for graph-only intents."
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G12: TXN.BEGIN → TXN.BEGIN (nested) → second BEGIN returns ERR;
// first TXN stays intact.
//
// Contract: The handler rejects a second TXN.BEGIN when `active_cross_txn`
// is already set. The existing TXN is NOT modified. The client can still
// write to and commit the first TXN.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_nested_begin_rejected_first_txn_intact() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("First TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK");

    // Write inside the first TXN.
    let _: String = redis::cmd("SET")
        .arg("g12_key")
        .arg("first_txn_value")
        .query_async(&mut conn)
        .await
        .expect("SET inside first TXN should succeed");

    // Second TXN.BEGIN → must return ERR.
    let nested: Result<String, redis::RedisError> =
        redis::cmd("TXN").arg("BEGIN").query_async(&mut conn).await;
    let err = nested.expect_err("Second TXN.BEGIN must return ERR (already in TXN)");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("already in a cross-store transaction"),
        "Second BEGIN must report 'already in a cross-store transaction'; got: {err_msg}"
    );

    // First TXN must still be intact: COMMIT must succeed and persist the write.
    let commit_ok: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn)
        .await
        .expect("COMMIT of first TXN after nested BEGIN rejection must succeed");
    assert_eq!(
        commit_ok, "OK",
        "COMMIT after nested BEGIN rejection must return OK"
    );

    // Verify the write from the first TXN is visible.
    let got: Option<String> = redis::cmd("GET")
        .arg("g12_key")
        .query_async(&mut conn)
        .await
        .expect("GET after COMMIT should succeed");
    assert_eq!(
        got.as_deref(),
        Some("first_txn_value"),
        "First TXN write must be visible after COMMIT; got: {got:?}"
    );

    // Cleanup.
    let _: () = redis::cmd("DEL")
        .arg("g12_key")
        .query_async(&mut conn)
        .await
        .unwrap_or(());

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G13: GRAPH.DELETE inside TXN then ABORT.
//
// Contract (Q3 RESOLVED from plan): `abort_cross_store_txn` iterates
// `graph_intents` in reverse. If a GRAPH.ADDNODE intent refers to a graph
// that was deleted mid-TXN (via GRAPH.DELETE), `get_graph_mut` returns None.
// The abort helper MUST log a `tracing::warn!` and continue — no panic.
// TXN.ABORT must return OK.
//
// Scenario:
//   1. GRAPH.CREATE g13 (outside TXN).
//   2. TXN.BEGIN.
//   3. GRAPH.ADDNODE g13 "phantom" → records intent with graph_name="g13".
//   4. GRAPH.DELETE g13 → graph removed from the store.
//   5. TXN.ABORT → rollback iterates intent for g13, finds graph gone,
//      emits warn!, continues. Returns OK.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_with_deleted_graph_no_panic() {
    let (port, shutdown) = start_txn_server(1).await;
    let mut conn = connect(port).await;

    // Create graph outside TXN.
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g13")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE g13 should succeed");

    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    // ADDNODE inside TXN — records graph_name="g13" intent.
    let nid: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g13")
        .arg("Phantom")
        .arg("name")
        .arg("ghost")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.ADDNODE inside TXN should succeed");
    assert!(nid > 0, "ADDNODE must return positive id");

    // GRAPH.DELETE inside TXN — removes the graph from the store.
    // This is a non-transactional delete that happens mid-TXN.
    let _del: redis::Value = redis::cmd("GRAPH.DELETE")
        .arg("g13")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.DELETE inside TXN should not error");

    // ABORT — rollback must not panic when g13 is gone.
    let abort_result: Result<String, redis::RedisError> =
        redis::cmd("TXN").arg("ABORT").query_async(&mut conn).await;

    // G13 contract: no panic, ABORT returns OK.
    let abort_ok = abort_result.expect("TXN.ABORT with deleted graph must NOT panic or return ERR");
    assert_eq!(
        abort_ok, "OK",
        "TXN.ABORT with missing graph must return OK (warn! + continue, not panic)"
    );

    // Server must still be alive after the edge-case abort.
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .expect("PING after G13 abort must succeed");
    assert_eq!(pong, "PONG", "Server must be alive after G13 edge case");

    shutdown.cancel();
}
