//! Integration tests — FT.SEARCH concurrent readers (TDD-165-166, G3 G10).
//!
//! G3: Two back-to-back FT.SEARCH calls on the same TXN connection see the
//!     same snapshot (identical count). Verifies that `snapshot_lsn` is
//!     stable across multiple reads inside a single TXN.
//! G10: Concurrent reader isolation — Conn A inside TXN inserts a vector
//!      (HSET), Conn B (outside TXN) FT.SEARCH must return 0 hits. After
//!      Conn A commits, Conn B FT.SEARCH must return 1 hit.
//!
//! File gate: both `runtime-tokio` AND `graph` — CI-parity gate from
//! Plan 165-04 lesson.
//!
//! Run:
//!   cargo test --test ft_search_concurrent_readers \
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
                .name(format!("concurrent-shard-{}", id))
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
// G3: Two back-to-back FT.SEARCH calls inside the same TXN see identical
// snapshot (same count).
//
// Contract: `snapshot_lsn` is captured at TXN.BEGIN and stays fixed for the
// lifetime of the transaction. Two sequential FT.SEARCH calls on the same
// connection MUST return the same count — the snapshot does not drift.
//
// Scenario:
//   1. HSET doc:a (pre-TXN baseline).
//   2. Conn B (no TXN): HSET doc:b between conn A's two reads (timing hack).
//      Since we cannot easily interleave two sequential commands on the same
//      connection, the test instead confirms that within a single TXN both
//      reads yield the same count even if doc:b is written in between:
//      Conn A: TXN.BEGIN → FT.SEARCH (count=C1) → Conn B: HSET doc:b
//              → Conn A: FT.SEARCH (count=C2). C1 MUST equal C2.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ft_search_txn_same_snapshot_both_reads() {
    let (port, shutdown) = start_moon_sharded(1).await;

    let mut setup = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL")
        .query_async(&mut setup)
        .await
        .unwrap();
    ft_create_idx(&mut setup).await;
    hset_vec(&mut setup, "doc:a", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut client_a = connect_dedicated(port).await;
    let mut client_b = connect_dedicated(port).await;

    // Client A enters TXN — snapshot_lsn frozen here.
    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut client_a)
        .await
        .expect("TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK");

    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);

    // First read inside TXN.
    let r1: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut client_a)
        .await
        .expect("First FT.SEARCH inside TXN should succeed");
    let count_1 = parse_search_count(&r1);

    // Client B (no TXN) inserts doc:b — this happens AFTER A's snapshot_lsn.
    hset_vec(&mut client_b, "doc:b", [0.0, 1.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Second read inside TXN — must see the same count as the first read.
    let r2: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut client_a)
        .await
        .expect("Second FT.SEARCH inside TXN should succeed");
    let count_2 = parse_search_count(&r2);

    // G3 contract: snapshot_lsn is stable; both reads must agree.
    assert_eq!(
        count_1, count_2,
        "Both FT.SEARCH reads inside TXN must return identical count (stable snapshot_lsn). \
         First={count_1}, Second={count_2}. Doc:b written between reads must not shift the snapshot."
    );
    // Exactly one doc was visible at snapshot (doc:a only; doc:b is post-snapshot).
    assert_eq!(
        count_1, 1,
        "TXN snapshot must capture exactly 1 doc (doc:a, pre-TXN); got count_1={count_1}"
    );

    // Clean up: commit the empty TXN.
    let _: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut client_a)
        .await
        .expect("TXN.COMMIT should succeed");

    drop(client_a);
    drop(client_b);
    drop(setup);
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// G10: Concurrent reader visibility isolation.
//
// Contract: Conn A inside TXN inserts a vector (HSET via auto-indexing).
// Conn B (no TXN) must NOT see it before COMMIT. After COMMIT, Conn B MUST
// see it.
//
// This test uses hash-tagged keys `{t}:*` so both key and doc route to the
// same shard — required for FT.SEARCH single-shard-local behaviour.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ft_search_uncommitted_invisible_to_outside_reader() {
    let (port, shutdown) = start_moon_sharded(1).await;

    let mut setup = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL")
        .query_async(&mut setup)
        .await
        .unwrap();

    // Vector-only index on prefix `doc:{t}:` with hash tag to ensure single-shard routing.
    let _: String = redis::cmd("FT.CREATE")
        .arg("cidx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("doc:{t}:")
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
        .query_async(&mut setup)
        .await
        .expect("FT.CREATE should succeed");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let mut conn_a = connect_dedicated(port).await;
    let mut conn_b = connect_dedicated(port).await;

    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);

    // Conn A: BEGIN TXN.
    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut conn_a)
        .await
        .expect("Conn A TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK");

    // Conn A: HSET inside TXN (auto-indexes into mutable HNSW segment).
    let vec_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let _: i64 = redis::cmd("HSET")
        .arg("doc:{t}:1")
        .arg("vec")
        .arg(vec_bytes)
        .query_async(&mut conn_a)
        .await
        .expect("Conn A HSET inside TXN should succeed");

    // Allow auto-indexing to settle.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Conn B (no TXN): FT.SEARCH must return 0 hits — uncommitted write invisible.
    let before_commit: redis::Value = redis::cmd("FT.SEARCH")
        .arg("cidx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn_b)
        .await
        .expect("Conn B FT.SEARCH before commit should succeed");
    let count_before = parse_search_count(&before_commit);
    assert_eq!(
        count_before, 0,
        "Conn B must NOT see Conn A's uncommitted HSET; got count={count_before} (ACID-09 isolation violated)"
    );

    // Conn A: COMMIT — materialises the vector write.
    let commit_ok: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut conn_a)
        .await
        .expect("Conn A TXN.COMMIT should succeed");
    assert_eq!(commit_ok, "OK");

    // Allow commit to propagate through the MVCC index.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Conn B: FT.SEARCH after COMMIT must return 1 hit.
    let after_commit: redis::Value = redis::cmd("FT.SEARCH")
        .arg("cidx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn_b)
        .await
        .expect("Conn B FT.SEARCH after commit should succeed");
    let count_after = parse_search_count(&after_commit);
    assert_eq!(
        count_after, 1,
        "Conn B MUST see the committed vector after TXN.COMMIT; got count={count_after}"
    );

    drop(conn_a);
    drop(conn_b);
    drop(setup);
    shutdown.cancel();
}
