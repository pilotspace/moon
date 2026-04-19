//! Integration tests — FT.SEARCH AS_OF boundary conditions (TDD-165-166, G1 G2 G5).
//!
//! G1: AS_OF=0 — explicit zero wall_ms. No TEMPORAL.SNAPSHOT_AT has been
//!     called at or before Unix epoch 0, so `lsn_at(0)` returns None → ERR.
//! G2: FT.SEARCH AS_OF inside a COMMITted TXN — after TXN.COMMIT the
//!     connection is no longer in a TXN; AS_OF works like a regular non-TXN
//!     temporal query and resolves to the registered snapshot.
//! G5: AS_OF with a negative timestamp ("AS_OF -1") — `lsn_at(-1)` returns
//!     None (range `..=-1` is empty for any positive-epoch registry) → ERR.
//!     Must return ERR cleanly (no panic, no crash).
//!
//! File gate: both `runtime-tokio` AND `graph` — CI-parity gate from
//! Plan 165-04 lesson. The graph feature enables the full TXN stack.
//!
//! Run:
//!   cargo test --test ft_search_as_of_boundary \
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

async fn start_moon_sharded(num_shards: usize) -> (u16, CancellationToken) {
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
                .name(format!("boundary-shard-{}", id))
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

fn vec4_bytes(v: [f32; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for x in &v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

async fn ft_create_idx(conn: &mut redis::aio::MultiplexedConnection) {
    let r: String = redis::cmd("FT.CREATE")
        .arg("idx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("doc:")
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
        .query_async(conn)
        .await
        .expect("FT.CREATE should succeed");
    assert_eq!(r, "OK");
}

async fn hset_vec(conn: &mut redis::aio::MultiplexedConnection, key: &str, v: [f32; 4]) {
    let bytes = vec4_bytes(v);
    let _: i64 = redis::cmd("HSET")
        .arg(key)
        .arg("vec")
        .arg(bytes)
        .query_async(conn)
        .await
        .expect("HSET should succeed");
}

fn parse_search_count(v: &redis::Value) -> i64 {
    match v {
        redis::Value::Array(items) => match items.first() {
            Some(redis::Value::Int(n)) => *n,
            Some(redis::Value::BulkString(b)) => std::str::from_utf8(b)
                .expect("count utf-8")
                .parse::<i64>()
                .expect("count parses"),
            other => panic!("unexpected first item in FT.SEARCH response: {other:?}"),
        },
        other => panic!("expected Array from FT.SEARCH, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// G1: AS_OF=0 (explicit zero wall_ms).
//
// Contract: TEMPORAL.SNAPSHOT_AT is never recorded at Unix epoch 0 (all real
// snapshots are at positive wall_ms). Therefore `lsn_at(0)` returns `None`
// and the helper MUST return ERR — not an empty result, not latest.
//
// If AS_OF=0 were silently treated as "latest", this test would fail with
// an unexpected success (count=1). The correct behaviour is ERR.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_as_of_zero_returns_err() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    ft_create_idx(&mut conn).await;
    hset_vec(&mut conn, "doc:1", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // No TEMPORAL.SNAPSHOT_AT has been called — registry is empty for wall_ms=0.
    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let result: Result<redis::Value, redis::RedisError> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("AS_OF")
        .arg("0")
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await;

    // G1 contract: AS_OF=0 with no registered binding at/before epoch 0 → ERR.
    let err = result.expect_err(
        "FT.SEARCH AS_OF 0 must return ERR (no temporal snapshot at Unix epoch 0)",
    );
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("no temporal snapshot registered for the given AS_OF timestamp"),
        "AS_OF 0 must surface the helper ERR; got: {err_msg}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G2: FT.SEARCH AS_OF after a COMMITTED TXN.
//
// Contract: After TXN.COMMIT, `active_cross_txn` is None. The AS_OF clause
// is handled by the temporal registry path (not the TXN snapshot path).
// With a registered TEMPORAL.SNAPSHOT_AT, AS_OF resolves normally.
//
// Scenario:
//   1. HSET doc:pre BEFORE snapshot registration.
//   2. TEMPORAL.SNAPSHOT_AT to register a binding at wall_ms T.
//   3. Begin TXN, HSET doc:in-txn, COMMIT.
//   4. FT.SEARCH AS_OF T → must see only doc:pre (pre-snapshot only).
//      The AS_OF clause must not see doc:in-txn (post-snapshot), confirming
//      the temporal registry path works on a post-COMMIT connection state.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_as_of_after_committed_txn() {
    let (port, shutdown) = start_moon_sharded(1).await;

    let mut setup = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL")
        .query_async(&mut setup)
        .await
        .unwrap();
    ft_create_idx(&mut setup).await;

    // doc:pre inserted BEFORE snapshot registration.
    hset_vec(&mut setup, "doc:pre", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Register snapshot at wall_ms T.
    let snap_ok: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut setup)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should succeed");
    assert_eq!(snap_ok, "OK");
    let wall_ms_t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Dedicated connection: BEGIN → HSET doc:in-txn → COMMIT.
    let mut txn_conn = connect_dedicated(port).await;
    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut txn_conn)
        .await
        .expect("TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK");
    hset_vec(&mut txn_conn, "doc:in-txn", [0.0, 1.0, 0.0, 0.0]).await;
    let commit_ok: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut txn_conn)
        .await
        .expect("TXN.COMMIT should succeed");
    assert_eq!(commit_ok, "OK");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // FT.SEARCH AS_OF T on the now-post-COMMIT connection.
    // Must resolve using the temporal registry (not TXN snapshot — txn is gone).
    // Must return only doc:pre (pre-snapshot).
    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let result: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("AS_OF")
        .arg(wall_ms_t.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut txn_conn)
        .await
        .expect("FT.SEARCH AS_OF after COMMIT should succeed");

    let count = parse_search_count(&result);
    assert_eq!(
        count, 1,
        "FT.SEARCH AS_OF after COMMIT must see only pre-snapshot doc; got count={count}"
    );

    // Verify latest (no AS_OF) sees both docs.
    let latest: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut txn_conn)
        .await
        .expect("FT.SEARCH latest should succeed");
    let count_latest = parse_search_count(&latest);
    assert_eq!(
        count_latest, 2,
        "FT.SEARCH without AS_OF after COMMIT must see both docs; got {count_latest}"
    );

    drop(txn_conn);
    drop(setup);
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G5: AS_OF with negative timestamp ("AS_OF -1").
//
// Contract: `parse_as_of_clause` returns `Some(-1)` (i64 is signed).
// `lsn_at(-1)` uses range `..=-1`, which is empty for any registry whose
// bindings are at positive Unix-epoch wall_ms values. Returns `None` → ERR.
// Must NOT panic. Must return ERR cleanly.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_as_of_negative_returns_err() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    ft_create_idx(&mut conn).await;
    hset_vec(&mut conn, "doc:1", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Register a snapshot at current time (positive epoch) — confirms that the
    // registry is non-empty, so the ERR is not due to an empty registry but
    // because -1 precedes all bindings.
    let _: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut conn)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should succeed");
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let result: Result<redis::Value, redis::RedisError> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("AS_OF")
        .arg("-1")
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await;

    // G5 contract: negative AS_OF must return ERR (not panic, not empty result).
    let err = result.expect_err(
        "FT.SEARCH AS_OF -1 must return ERR (negative timestamp precedes all registry entries)",
    );
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("no temporal snapshot registered for the given AS_OF timestamp"),
        "negative AS_OF must surface the helper ERR; got: {err_msg}"
    );

    shutdown.cancel();
}
