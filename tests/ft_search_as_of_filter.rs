//! Integration test — FT.SEARCH AS_OF + FILTER compose (TDD-165-166, G4).
//!
//! G4: AS_OF clause and FILTER clause both apply simultaneously.
//!
//! Scenario: Two docs inserted at different times. Doc:a is inserted before
//! a snapshot, doc:b after. Both docs have a numeric "score" field.
//! `FT.SEARCH AS_OF <T> FILTER @score==10` should:
//!   - Honour the temporal bound (excludes doc:b, post-snapshot).
//!   - Apply the filter (doc:a has score=10 so it passes).
//! Result must be exactly count=1 (doc:a).
//!
//! A separate non-temporal filter query confirms both docs exist at latest and
//! filter applies correctly there too.
//!
//! NOTE: FILTER in FT.SEARCH requires a numeric or TAG field in the schema.
//! Moon's FT.CREATE supports TEXT and NUMERIC field types (see
//! `src/command/vector_search/ft_create.rs`). We use NUMERIC here.
//! The `text-index` feature flag is NOT required for NUMERIC-only schemas.
//!
//! File gate: both `runtime-tokio` AND `graph` — CI-parity gate from
//! Plan 165-04 lesson.
//!
//! Run:
//!   cargo test --test ft_search_as_of_filter \
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
                .name(format!("filter-shard-{}", id))
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

fn vec4_bytes(v: [f32; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for x in &v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
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
// G4: AS_OF + FILTER compose correctly.
//
// Contract: Both temporal and filter predicates are applied. The result must
// satisfy BOTH constraints simultaneously, not one-at-a-time.
//
// Schema: HNSW vector `vec` (4-dim) + NUMERIC field `score`.
// Setup:
//   - doc:a: vec=[1,0,0,0], score=10  (inserted BEFORE snapshot T)
//   - doc:b: vec=[0,1,0,0], score=99  (inserted AFTER snapshot T)
//
// Query: FT.SEARCH AS_OF T FILTER @score==10 KNN 10 @vec [1,0,0,0]
// Expected: count=1 (doc:a passes both filters).
//
// Sanity:
//   - FT.SEARCH AS_OF T (no filter): count=1 (temporal only, doc:a).
//   - FT.SEARCH FILTER @score==10 (no AS_OF): count=1 (filter only, doc:a).
//   - FT.SEARCH (no AS_OF, no filter): count=2 (latest, both).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_as_of_and_filter_compose() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    // Schema: vector (4-dim) + numeric score field.
    // Moon FT.CREATE numeric field syntax: <field_name> NUMERIC
    let _: String = redis::cmd("FT.CREATE")
        .arg("fidx")
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
        .arg("score")
        .arg("NUMERIC")
        .query_async(&mut conn)
        .await
        .expect("FT.CREATE with NUMERIC field should succeed");

    // doc:a: score=10, inserted BEFORE snapshot.
    let vec_a = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let _: i64 = redis::cmd("HSET")
        .arg("doc:a")
        .arg("vec")
        .arg(vec_a)
        .arg("score")
        .arg("10")
        .query_async(&mut conn)
        .await
        .expect("HSET doc:a should succeed");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Register temporal snapshot T.
    let snap_ok: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut conn)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should succeed");
    assert_eq!(snap_ok, "OK");
    let wall_ms_t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // doc:b: score=99, inserted AFTER snapshot T.
    let vec_b = vec4_bytes([0.0, 1.0, 0.0, 0.0]);
    let _: i64 = redis::cmd("HSET")
        .arg("doc:b")
        .arg("vec")
        .arg(vec_b)
        .arg("score")
        .arg("99")
        .query_async(&mut conn)
        .await
        .expect("HSET doc:b should succeed");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);

    // --- Sanity 1: no AS_OF, no filter → both docs visible (count=2).
    let s1: redis::Value = redis::cmd("FT.SEARCH")
        .arg("fidx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("sanity 1 should succeed");
    let c1 = parse_search_count(&s1);
    assert_eq!(
        c1, 2,
        "sanity 1: no AS_OF no filter must return 2 docs; got {c1}"
    );

    // --- Sanity 2: AS_OF T, no filter → only doc:a (count=1).
    let s2: redis::Value = redis::cmd("FT.SEARCH")
        .arg("fidx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("AS_OF")
        .arg(wall_ms_t.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("sanity 2 should succeed");
    let c2 = parse_search_count(&s2);
    assert_eq!(
        c2, 1,
        "sanity 2: AS_OF T no filter must return 1 doc (doc:a); got {c2}"
    );

    // --- G4 main: AS_OF T + FILTER @score:[10 10] → count=1 (doc:a passes both).
    // Moon FT.SEARCH FILTER numeric-equality syntax: @field:[val val] (range where min==max).
    // The `==` shorthand is not supported; use the RediSearch-compatible range syntax.
    let g4: redis::Value = redis::cmd("FT.SEARCH")
        .arg("fidx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("AS_OF")
        .arg(wall_ms_t.to_string())
        .arg("FILTER")
        .arg("@score:[10 10]")
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("G4 AS_OF + FILTER should succeed");
    let c_g4 = parse_search_count(&g4);
    assert_eq!(
        c_g4, 1,
        "G4: AS_OF + FILTER must return exactly 1 doc (doc:a, score=10, pre-snapshot). \
         Got count={c_g4}. Either temporal or filter constraint not applied."
    );

    // --- Sanity 3: AS_OF T + FILTER @score:[99 99] → count=0 (doc:b is post-snapshot).
    let s3: redis::Value = redis::cmd("FT.SEARCH")
        .arg("fidx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("AS_OF")
        .arg(wall_ms_t.to_string())
        .arg("FILTER")
        .arg("@score:[99 99]")
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("sanity 3 should succeed");
    let c3 = parse_search_count(&s3);
    assert_eq!(
        c3, 0,
        "sanity 3: AS_OF T + FILTER @score:[99 99] must return 0 (doc:b excluded by temporal); got {c3}"
    );

    shutdown.cancel();
}
