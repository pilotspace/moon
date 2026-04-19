//! Phase 166 red-green TDD: these tests MUST fail at the end of Plan 01 and
//! MUST pass at the end of Plan 04.
//!
//! Integration tests for cross-store Transaction completeness (ACID-08).
//! Mirrors the `start_txn_server` harness from `tests/txn_kv_wiring.rs`.
//!
//! Five scenarios:
//!   1. `test_txn_abort_reverts_graph_addnode` — TXN.ABORT must unwind
//!      GRAPH.ADDNODE writes (currently they leak past abort).
//!   2. `test_txn_abort_reverts_graph_addedge` — TXN.ABORT must unwind
//!      GRAPH.ADDNODE + GRAPH.ADDEDGE writes.
//!   3. `test_txn_abort_hides_ft_search_results` — TXN.ABORT must tombstone
//!      vector rows appended by HSET via auto-indexing (ACID-08 core).
//!   4. `test_connection_drop_releases_kv_intents` — Disconnect must release
//!      KV write intents; otherwise subsequent reads are pinned invisible.
//!   5. `test_lunaris_temporal_two_snapshot_diff` — Lunaris V4: two-snapshot
//!      temporal diff with aborted middle transaction. Requires Phase 165
//!      `resolve_ft_search_as_of_lsn` helper (verified present at plan time).
//!
//! Plan 165 pre-flight gate (recorded in /tmp/txn_graph_wiring_red.log):
//!   PHASE165_HELPER_COUNT = count of `fn resolve_ft_search_as_of_lsn` in
//!   src/server/conn/shared.rs. Since the count is ≥1 at plan time, test 5
//!   is NOT `#[ignore]`'d — it runs as the 5th RED assertion.
//!
//! Run:
//!   cargo test --test txn_graph_wiring \
//!        --no-default-features --features runtime-tokio,jemalloc,graph \
//!        -- --test-threads=1
//!
//! This file is gated on BOTH `runtime-tokio` AND `graph` because the TXN
//! interception lives on the `run_sharded` + handler_sharded path, and the
//! graph rollback paths reference feature-gated graph types in Plan 166-03.
#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure (copy of txn_kv_wiring::start_txn_server;
// txn_kv_wiring keeps it file-local so we duplicate per test file rather
// than depend on a shared test-support module which does not exist yet).
// ---------------------------------------------------------------------------

/// Start a full sharded Moon server on a random OS-assigned port via
/// `listener::run_sharded`. This path routes through `handler_sharded.rs`
/// which is where Phase 166 plans land their TXN.*, GRAPH.*, and disconnect
/// wiring.
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
                .name(format!("txn-graph-shard-{}", id))
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

/// Encode an `&[f32]` as little-endian bytes for HSET vector payload.
fn f32_blob(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Parse GRAPH.INFO response — supports both RESP3 Map and legacy flat-Array
/// forms — and return the node_count + edge_count fields. Returns `(0, 0)` if
/// either field is absent (which would itself be a regression).
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
            // Flat pair-array form: [k1, v1, k2, v2, ...].
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

/// Parse a `FT.SEARCH` response and return the numeric result count.
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
// Test 1 (RED): TXN.ABORT reverts GRAPH.ADDNODE (Plan 166-02 wires
// record_graph; Plan 166-03 wires the rollback).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_reverts_graph_addnode() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g1")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE should succeed");

    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    let node_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g1")
        .arg("Entity")
        .arg("name")
        .arg("E1")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.ADDNODE should succeed inside TXN");
    assert!(node_id > 0, "GRAPH.ADDNODE should return a positive id");

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");

    let info: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("g1")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO should succeed");
    let (nodes, _edges) = parse_graph_info_counts(&info);
    assert_eq!(
        nodes, 0,
        "TXN.ABORT must remove the ADDNODE. Found node_count={nodes} — rollback wiring missing."
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2 (RED): TXN.ABORT reverts GRAPH.ADDEDGE + its endpoints.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_reverts_graph_addedge() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g2")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE should succeed");

    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    let a: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g2")
        .arg("Person")
        .arg("name")
        .arg("A")
        .query_async(&mut conn)
        .await
        .expect("ADDNODE A");
    let b: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g2")
        .arg("Person")
        .arg("name")
        .arg("B")
        .query_async(&mut conn)
        .await
        .expect("ADDNODE B");
    let edge: i64 = redis::cmd("GRAPH.ADDEDGE")
        .arg("g2")
        .arg(a)
        .arg(b)
        .arg("KNOWS")
        .query_async(&mut conn)
        .await
        .expect("ADDEDGE");
    assert!(edge > 0, "GRAPH.ADDEDGE should return a positive id");

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");

    let info: redis::Value = redis::cmd("GRAPH.INFO")
        .arg("g2")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.INFO should succeed");
    let (nodes, edges) = parse_graph_info_counts(&info);
    assert_eq!(
        edges, 0,
        "TXN.ABORT must remove the ADDEDGE. Found edge_count={edges} — rollback wiring missing."
    );
    assert_eq!(
        nodes, 0,
        "TXN.ABORT must remove both ADDNODE endpoints. Found node_count={nodes}."
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 3 (RED): TXN.ABORT tombstones the HNSW mutable-segment row so
// FT.SEARCH can no longer see it (ACID-08 core).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_txn_abort_hides_ft_search_results() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    // 16-dim HNSW L2 index on prefix `v:{t}:`. Hash-tag `{t}` routes every
    // key to one shard — required because FT.SEARCH is shard-local and the
    // TXN abort path runs on the owning shard.
    let _: String = redis::cmd("FT.CREATE")
        .arg("vidx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("v:{t}:")
        .arg("SCHEMA")
        .arg("vec")
        .arg("VECTOR")
        .arg("HNSW")
        .arg("6")
        .arg("DIM")
        .arg("16")
        .arg("TYPE")
        .arg("FLOAT32")
        .arg("DISTANCE_METRIC")
        .arg("L2")
        .query_async(&mut conn)
        .await
        .expect("FT.CREATE should succeed");

    let vec: Vec<f32> = (0..16).map(|i| i as f32 * 0.1).collect();
    let q_blob = f32_blob(&vec);

    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    let _: i64 = redis::cmd("HSET")
        .arg("v:{t}:1")
        .arg("vec")
        .arg(q_blob.clone())
        .arg("label")
        .arg("x")
        .query_async(&mut conn)
        .await
        .expect("HSET inside TXN should succeed");

    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");

    // Settle any post-abort async work.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let search: redis::Value = redis::cmd("FT.SEARCH")
        .arg("vidx")
        .arg("*=>[KNN 5 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_blob)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH should succeed");

    let count = ft_search_count(&search);
    assert_eq!(
        count, 0,
        "TXN.ABORT must tombstone the HSET'd vector row. Found count={count} — ACID-08 (mark_deleted_by_key_hash) not wired."
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 4 (RED): Connection drop without explicit TXN.ABORT must release
// KV write intents; otherwise subsequent reads on any connection are
// pinned invisible (T-161-05 leak).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_connection_drop_releases_kv_intents() {
    let (port, shutdown) = start_txn_server(1, "").await;

    // Establish a baseline outside any transaction.
    {
        let mut setup = connect(port).await;
        let _: String = redis::cmd("SET")
            .arg("{t}:leak_key")
            .arg("v_old")
            .query_async(&mut setup)
            .await
            .expect("baseline SET should succeed");
    }

    // Connection A: BEGIN, SET new value, then drop WITHOUT ABORT.
    {
        let mut conn_a = connect(port).await;
        let _: String = redis::cmd("TXN")
            .arg("BEGIN")
            .query_async(&mut conn_a)
            .await
            .expect("conn_a TXN.BEGIN should succeed");
        let _: String = redis::cmd("SET")
            .arg("{t}:leak_key")
            .arg("v_new")
            .query_async(&mut conn_a)
            .await
            .expect("conn_a SET should succeed");
        drop(conn_a);
        // Give the server time to process the disconnect.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Connection B: GET must see the ORIGINAL value; the leaked kv_intents
    // from conn_a must not pin the key invisible to outside readers.
    {
        let mut conn_b = connect(port).await;
        let got: Option<String> = redis::cmd("GET")
            .arg("{t}:leak_key")
            .query_async(&mut conn_b)
            .await
            .expect("conn_b GET should succeed");
        assert_eq!(
            got.as_deref(),
            Some("v_old"),
            "conn_b GET must return the original value 'v_old' — dropped conn_a's uncommitted SET must not be visible. Got: {got:?}"
        );
    }

    // Connection C: starting a new TXN must still succeed — shard state
    // must not be poisoned by the leaked intent.
    {
        let mut conn_c = connect(port).await;
        let begin: String = redis::cmd("TXN")
            .arg("BEGIN")
            .query_async(&mut conn_c)
            .await
            .expect("conn_c TXN.BEGIN should succeed");
        assert_eq!(begin, "OK", "conn_c TXN.BEGIN after drop must succeed");
        let _: String = redis::cmd("TXN")
            .arg("ABORT")
            .query_async(&mut conn_c)
            .await
            .expect("conn_c TXN.ABORT cleanup");
    }

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 5 (RED, Lunaris V4): Two temporal snapshots across an aborted
// transaction. Depends on Phase 165 `resolve_ft_search_as_of_lsn` helper
// (confirmed present at plan start — this test is NOT `#[ignore]`'d).
//
// Scenario:
//   - T1: commit entity with property "p1". Register TEMPORAL.SNAPSHOT_AT T1.
//   - Open txn2: HSET the same entity with mutated property "p2" and a new
//     vector; register TEMPORAL.SNAPSHOT_AT T2 (captures LSN > T1).
//   - FT.SEARCH AS_OF T1 must show the original (pre-T1) indexed entity.
//   - TXN.ABORT.
//   - FT.SEARCH AS_OF T2 after abort MUST match T1's view (the T2 update
//     was rolled back).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn test_lunaris_temporal_two_snapshot_diff() {
    let (port, shutdown) = start_txn_server(1, "").await;
    let mut conn = connect(port).await;

    // Vector-only schema (TAG requires feature text-index which is not in
    // this test's feature set — graph only). Two-snapshot diff is still
    // sound via the vector itself: v1 at T1, v2 at T2 inside aborted txn.
    let _: String = redis::cmd("FT.CREATE")
        .arg("lidx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("e:{t}:")
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

    // Initial commit at T1 — outside any transaction.
    let v1 = f32_blob(&[1.0, 0.0, 0.0, 0.0]);
    let _: i64 = redis::cmd("HSET")
        .arg("e:{t}:1")
        .arg("vec")
        .arg(v1.clone())
        .query_async(&mut conn)
        .await
        .expect("HSET T1 should succeed");

    // Let the auto-index settle before the T1 snapshot.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let _: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut conn)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT T1 should succeed");
    let wall_t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Ensure T2 wall strictly after T1.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Open txn2; mutate the entity.
    let _: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn)
        .await
        .expect("TXN.BEGIN should succeed");

    let v2 = f32_blob(&[0.0, 1.0, 0.0, 0.0]);
    let _: i64 = redis::cmd("HSET")
        .arg("e:{t}:1")
        .arg("vec")
        .arg(v2.clone())
        .query_async(&mut conn)
        .await
        .expect("HSET T2 inside TXN should succeed");

    // Register T2 snapshot while the txn is still open.
    let _: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut conn)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT T2 should succeed");
    let wall_t2 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Abort — rolls back the T2 HSET mutation.
    let _: String = redis::cmd("TXN")
        .arg("ABORT")
        .query_async(&mut conn)
        .await
        .expect("TXN.ABORT should succeed");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // FT.SEARCH AS_OF T1 — should see the pre-T1 entity (v1 vector).
    let r_t1: redis::Value = redis::cmd("FT.SEARCH")
        .arg("lidx")
        .arg("*=>[KNN 5 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(v1.clone())
        .arg("AS_OF")
        .arg(wall_t1.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH AS_OF T1 should succeed");
    let count_t1 = ft_search_count(&r_t1);
    assert_eq!(
        count_t1, 1,
        "FT.SEARCH AS_OF T1 must return the pre-T1 entity; got count={count_t1}"
    );

    // FT.SEARCH AS_OF T2 AFTER abort — must match the T1 view because the
    // txn's T2 mutation was rolled back. Since the rollback tombstones the
    // T2-appended vector (Plan 166-03), only the pre-T1 vector remains.
    let r_t2: redis::Value = redis::cmd("FT.SEARCH")
        .arg("lidx")
        .arg("*=>[KNN 5 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(v1)
        .arg("AS_OF")
        .arg(wall_t2.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH AS_OF T2 post-abort should succeed");
    let count_t2 = ft_search_count(&r_t2);
    assert_eq!(
        count_t2, 1,
        "FT.SEARCH AS_OF T2 after TXN.ABORT must match the T1 view (one entity, v1 only). Got count={count_t2} — the aborted T2 HSET leaked past rollback."
    );

    shutdown.cancel();
}
