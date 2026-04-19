//! Integration tests — FT.SEARCH AS_OF multi-shard scatter (Phase 171, SCAT-01 / SCAT-02).
//!
//! Phase 171 closes the v0.1.8 audit gap:
//!
//!   "Multi-shard FT.SEARCH AS_OF scatter — ShardMessage::VectorSearch does
//!    not carry as_of_lsn (scoped out of 165)"
//!
//! These tests assert the end-to-end behavior AFTER the fix: with 4 shards,
//! an `AS_OF <t_ms>` clause on FT.SEARCH returns only entries inserted at or
//! before the snapshot-time binding registered on the coordinator's
//! TemporalRegistry. Before this phase, the coordinator resolved the LSN but
//! silently dropped it when building `ShardMessage::VectorSearch`, so every
//! responder executed with `as_of_lsn = 0` (no temporal filter).
//!
//! Two tests:
//! 1. `ft_search_multi_shard_as_of_honours_snapshot` — 4 shards, no hash
//!    tags, keys distribute naturally. HSET pre-snapshot,
//!    `TEMPORAL.SNAPSHOT_AT`, HSET post-snapshot. `AS_OF <wall_ms>` must
//!    return only pre-snapshot keys.
//! 2. `ft_search_single_shard_as_of_parity_guard` — 1-shard regression:
//!    same seeding + assertions still produce the single-shard counts
//!    that Phase 165 already delivered. Guards against SCAT-01
//!    accidentally breaking the single-shard AS_OF path.
//!
//! Run:
//!   cargo test --no-default-features --features runtime-tokio,jemalloc,text-index,graph \
//!        --test ft_search_multi_shard_as_of -- --test-threads=1
#![cfg(feature = "runtime-tokio")]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server harness (mirrors tests/ft_search_temporal_parity.rs)
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

/// Start a full sharded Moon server on a random OS-assigned port via
/// `listener::run_sharded`. Connections are handled by `handler_sharded.rs`.
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
                .name(format!("ft-mshard-asof-shard-{}", id))
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

    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    (port, token)
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Encode four f32 values as 16 little-endian bytes.
fn vec4_bytes(v: [f32; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for x in &v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Create an HNSW/L2 vector index on PREFIX `doc:` over field `vec` (4 dim).
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
    assert_eq!(r, "OK", "FT.CREATE should return OK, got {r}");
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
                .expect("count is valid utf-8")
                .parse::<i64>()
                .expect("count parses as i64"),
            other => panic!("unexpected first item in FT.SEARCH response: {other:?}"),
        },
        other => panic!("expected Array from FT.SEARCH, got {other:?}"),
    }
}

/// Run the seed + search flow that asserts AS_OF correctness against
/// `expected_asof_count` / `expected_latest_count`.
///
/// Seed plan:
///   1. FT.CREATE index.
///   2. HSET 4 pre-snapshot docs with distinct keys (no hash tags).
///   3. `TEMPORAL.SNAPSHOT_AT` + capture client wall_ms.
///   4. HSET 4 post-snapshot docs.
///   5. Assert FT.SEARCH AS_OF wall_ms returns `expected_asof_count`.
///   6. Assert FT.SEARCH without AS_OF returns `expected_latest_count`.
async fn run_as_of_seed_and_assert(
    port: u16,
    expected_asof_count: i64,
    expected_latest_count: i64,
) {
    let mut conn = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    ft_create_idx(&mut conn).await;

    // ── Pre-snapshot seed (4 distinct keys — no hash tags so they distribute
    //    naturally across shards on multi-shard runs).
    hset_vec(&mut conn, "doc:alpha", [1.0, 0.0, 0.0, 0.0]).await;
    hset_vec(&mut conn, "doc:beta", [0.0, 1.0, 0.0, 0.0]).await;
    hset_vec(&mut conn, "doc:gamma", [0.0, 0.0, 1.0, 0.0]).await;
    hset_vec(&mut conn, "doc:delta", [0.0, 0.0, 0.0, 1.0]).await;

    // Let auto-indexing settle before the snapshot.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // Register snapshot — server captures own wall_ms + current LSN on the
    // receiving shard. TEMPORAL.SNAPSHOT_AT takes NO args (see
    // src/command/temporal.rs validate_snapshot_at).
    let snap_ok: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut conn)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should return OK");
    assert_eq!(snap_ok, "OK");

    // Capture client wall_ms immediately after the snapshot call. Registry
    // lookup is "last binding <= wall_ms" — a client wall_ms >= server wall_ms
    // always hits the binding we just recorded.
    let wall_ms_t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Guarantee strictly later LSNs for the post-snapshot batch.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // ── Post-snapshot seed (4 more keys).
    hset_vec(&mut conn, "doc:epsilon", [1.0, 1.0, 0.0, 0.0]).await;
    hset_vec(&mut conn, "doc:zeta", [1.0, 0.0, 1.0, 0.0]).await;
    hset_vec(&mut conn, "doc:eta", [1.0, 0.0, 0.0, 1.0]).await;
    hset_vec(&mut conn, "doc:theta", [0.0, 1.0, 1.0, 0.0]).await;

    tokio::time::sleep(std::time::Duration::from_millis(80)).await;

    // ── FT.SEARCH AS_OF wall_ms_t1 → expect only pre-snapshot docs.
    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let as_of_result: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("AS_OF")
        .arg(wall_ms_t1.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH AS_OF should succeed");

    let count_asof = parse_search_count(&as_of_result);
    assert_eq!(
        count_asof, expected_asof_count,
        "AS_OF must filter out post-snapshot docs across all shards; got count={count_asof}, expected={expected_asof_count}"
    );

    // ── FT.SEARCH without AS_OF → expect latest (all 8 docs).
    let latest_result: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH without AS_OF should succeed");
    let count_latest = parse_search_count(&latest_result);
    assert_eq!(
        count_latest, expected_latest_count,
        "FT.SEARCH without AS_OF must return all seeded docs; got {count_latest}, expected={expected_latest_count}"
    );
}

// ---------------------------------------------------------------------------
// Test 1 (SCAT-01 / SCAT-02): 4-shard AS_OF correctness.
//
// With Phase 171's LSN threading, the coordinator resolves the TemporalRegistry
// binding ONCE and forwards it to every responder via
// `ShardMessage::VectorSearch { as_of_lsn, .. }`. Pre-snapshot docs must
// survive; post-snapshot docs must be filtered everywhere.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_multi_shard_as_of_honours_snapshot() {
    let (port, shutdown) = start_moon_sharded(4).await;

    // 4 pre-snapshot docs visible at AS_OF; 8 total without AS_OF.
    run_as_of_seed_and_assert(port, 4, 8).await;

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2 (single-shard regression guard): same seed + search flow on
// `--shards 1`. Phase 165 already wired AS_OF through handler_sharded.rs;
// this test guards against SCAT-01 regressing the single-shard path during
// the multi-shard rollout.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_single_shard_as_of_parity_guard() {
    let (port, shutdown) = start_moon_sharded(1).await;

    run_as_of_seed_and_assert(port, 4, 8).await;

    shutdown.cancel();
}
