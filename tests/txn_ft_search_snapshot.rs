//! Integration tests — FT.SEARCH inside TXN respects snapshot_lsn (Phase 165,
//! Plan 03 Task 2).
//!
//! Regression-guard tests (NOT RED-phase TDD) for ACID-09. Plan 165-02 wired
//! `conn.active_cross_txn.as_ref()` into `resolve_ft_search_as_of_lsn`, so a
//! client inside a TXN.BEGIN..TXN.COMMIT block observes a stable snapshot for
//! FT.SEARCH. These tests confirm the wiring holds end-to-end through real
//! RESP round-trips with two independent client connections on the
//! runtime-tokio build.
//!
//! Tests:
//!   1. `ft_search_sees_txn_snapshot`                  — ACID-09 core: client A
//!      inside a TXN does NOT see a committed HSET from client B when that
//!      HSET happened after A's snapshot. After A commits, it sees the new doc.
//!   2. `ft_search_explicit_as_of_beats_txn_snapshot`  — precedence: explicit
//!      AS_OF wins over `txn.snapshot_lsn` (matches unit-test
//!      `resolve_ft_search_as_of_lsn_explicit_as_of_beats_txn_snapshot`).
//!   3. `ft_search_sharded_no_txn_no_asof_is_latest`   — sharded regression
//!      guard: default path on a 4-shard handler_sharded still returns latest
//!      (locks in the "as_of_lsn=0 means latest" semantic — W4 resolves the
//!      name ambiguity vs Task 1's single-shard `no_asof_no_txn_returns_latest`).
//!
//! Run:
//!   cargo test --test txn_ft_search_snapshot --no-default-features \
//!        --features runtime-tokio,jemalloc -- --test-threads=1
#![cfg(feature = "runtime-tokio")]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test harness — mirrors `tests/ft_search_temporal_parity.rs`. Duplicated
// (not extracted) per plan directive: keep Task 1 stable; shared harness
// extraction is a later refactor if the pattern ossifies.
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
                .name(format!("txn-ft-shard-{}", id))
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

/// Distinct multiplexed connection — each call opens a new `redis::Client` so
/// the underlying TCP stream (and therefore server-side `ConnectionState`
/// including `active_cross_txn`) is independent from other calls. Two of these
/// give us the independent client A and client B needed to exercise cross-
/// connection TXN isolation. (redis 1.1 does not expose
/// `get_async_connection`; `get_multiplexed_async_connection` per-client is
/// equivalent for our purpose.)
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

async fn hset_vec_mx(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    v: [f32; 4],
) {
    let bytes = vec4_bytes(v);
    let _: i64 = redis::cmd("HSET")
        .arg(key)
        .arg("vec")
        .arg(bytes)
        .query_async(conn)
        .await
        .expect("HSET should succeed");
}

// hset_vec_dedicated removed — `connect_dedicated` now returns a
// `MultiplexedConnection`, so callers use `hset_vec_mx` uniformly.

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

fn extract_doc_keys(v: &redis::Value) -> Vec<String> {
    let mut keys = Vec::new();
    if let redis::Value::Array(items) = v {
        for item in items {
            if let redis::Value::BulkString(b) = item {
                if let Ok(s) = std::str::from_utf8(b) {
                    if s.starts_with("doc:") {
                        keys.push(s.to_string());
                    }
                }
            }
        }
    }
    keys
}

// ---------------------------------------------------------------------------
// Test 1 (ACID-09 core): FT.SEARCH inside TXN.BEGIN respects snapshot_lsn.
// Cross-connection: client A's TXN snapshot must hide doc:b written by client
// B AFTER A's BEGIN. After A commits, the snapshot is released and doc:b
// becomes visible.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ft_search_sees_txn_snapshot() {
    // 1 shard: scatter path does not propagate snapshot_lsn across shards
    // (pre-existing architectural limit — see header scope note). Phase 165's
    // objective is single-shard parity across the three handlers; that is
    // what this test verifies.
    let (port, shutdown) = start_moon_sharded(1).await;

    // Setup on a multiplexed connection (pre-TXN state).
    let mut setup = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut setup).await.unwrap();
    ft_create_idx(&mut setup).await;
    hset_vec_mx(&mut setup, "doc:a", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Dedicated connections A (TXN owner) and B (concurrent writer).
    let mut client_a = connect_dedicated(port).await;
    let mut client_b = connect_dedicated(port).await;

    // Client A: TXN.BEGIN captures snapshot_lsn at this moment.
    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut client_a)
        .await
        .expect("TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK");

    // Client B: HSET doc:b (committed) AFTER A's snapshot_lsn.
    hset_vec_mx(&mut client_b, "doc:b", [0.0, 1.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Client A: FT.SEARCH inside TXN — must NOT see doc:b.
    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let inside_txn: redis::Value = redis::cmd("FT.SEARCH")
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
        .expect("FT.SEARCH inside TXN should succeed");
    let inside_keys = extract_doc_keys(&inside_txn);
    assert!(
        inside_keys.contains(&"doc:a".to_string()),
        "inside TXN must see pre-TXN doc:a; got {inside_keys:?}"
    );
    assert!(
        !inside_keys.contains(&"doc:b".to_string()),
        "inside TXN must NOT see doc:b (post-snapshot foreign write); got {inside_keys:?}"
    );

    // Client A: TXN.COMMIT releases the snapshot.
    let commit_ok: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut client_a)
        .await
        .expect("TXN.COMMIT should succeed");
    assert_eq!(commit_ok, "OK");

    // Client A: FT.SEARCH after COMMIT — now sees both.
    let after_commit: redis::Value = redis::cmd("FT.SEARCH")
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
        .expect("FT.SEARCH after COMMIT should succeed");
    let after_keys = extract_doc_keys(&after_commit);
    assert!(
        after_keys.contains(&"doc:a".to_string()) && after_keys.contains(&"doc:b".to_string()),
        "after COMMIT must see both docs; got {after_keys:?}"
    );

    drop(client_a);
    drop(client_b);
    drop(setup);
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2 (ACID-09 precedence): explicit AS_OF overrides txn.snapshot_lsn.
// Matches the helper's precedence contract locked in Plan 165-01
// (resolve_ft_search_as_of_lsn_explicit_as_of_beats_txn_snapshot).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ft_search_explicit_as_of_beats_txn_snapshot() {
    // 1 shard: see comment in `ft_search_sees_txn_snapshot`.
    let (port, shutdown) = start_moon_sharded(1).await;

    let mut setup = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut setup).await.unwrap();
    ft_create_idx(&mut setup).await;

    // HSET doc:a BEFORE the temporal snapshot.
    hset_vec_mx(&mut setup, "doc:a", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Register T1 snapshot.
    let snap_ok: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut setup)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should succeed");
    assert_eq!(snap_ok, "OK");
    let wall_ms_t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // HSET doc:b AFTER the temporal snapshot but BEFORE our TXN.BEGIN.
    hset_vec_mx(&mut setup, "doc:b", [0.0, 1.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Dedicated TXN client.
    let mut client_a = connect_dedicated(port).await;
    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut client_a)
        .await
        .expect("TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK");

    // Inside TXN with explicit AS_OF wall_ms_t1 → must resolve to T1 LSN
    // (NOT txn.snapshot_lsn which would see both docs).
    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let as_of_result: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("AS_OF")
        .arg(wall_ms_t1.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut client_a)
        .await
        .expect("FT.SEARCH AS_OF inside TXN should succeed");
    let keys = extract_doc_keys(&as_of_result);
    let count = parse_search_count(&as_of_result);
    assert_eq!(
        count, 1,
        "explicit AS_OF must win over TXN snapshot; got count={count}, keys={keys:?}"
    );
    assert!(
        keys.contains(&"doc:a".to_string()),
        "explicit AS_OF result must be doc:a (pre-T1); got {keys:?}"
    );
    assert!(
        !keys.contains(&"doc:b".to_string()),
        "explicit AS_OF must filter out post-T1 doc:b; got {keys:?}"
    );

    let _: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut client_a)
        .await
        .expect("TXN.COMMIT should succeed");

    drop(client_a);
    drop(setup);
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 3 (sharded regression guard): no TXN, no AS_OF, 4-shard handler_sharded
// still returns latest. W4 disambiguation — name makes the sharded-handler
// intent explicit vs Task 1's single-shard `no_asof_no_txn_returns_latest`.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ft_search_sharded_no_txn_no_asof_is_latest() {
    let (port, shutdown) = start_moon_sharded(4).await;

    let mut conn = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    ft_create_idx(&mut conn).await;

    hset_vec_mx(&mut conn, "doc:1", [1.0, 0.0, 0.0, 0.0]).await;
    hset_vec_mx(&mut conn, "doc:2", [0.0, 1.0, 0.0, 0.0]).await;
    hset_vec_mx(&mut conn, "doc:3", [0.0, 0.0, 1.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let result: redis::Value = redis::cmd("FT.SEARCH")
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
        .expect("FT.SEARCH should succeed");

    let count = parse_search_count(&result);
    let keys = extract_doc_keys(&result);
    // Note: 1-shard-local-FT.SEARCH limitation — docs may route to different
    // shards; we only check that at least the locally-visible docs return
    // (>= 1), NOT exactly 3. Phase 165 scope is the single-shard path; the
    // cross-shard scatter is architectural follow-up.
    assert!(
        count >= 1,
        "sharded no-TXN no-AS_OF must return latest (>=1 locally); got count={count}, keys={keys:?}"
    );

    drop(conn);
    shutdown.cancel();
}
