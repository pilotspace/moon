//! Integration tests — FT.SEARCH AS_OF handler parity (Phase 165, Plan 03).
//!
//! Regression-guard tests (NOT RED-phase TDD) for TEMP-04. Plan 165-01 landed
//! the `resolve_ft_search_as_of_lsn` helper and Plan 165-02 wired all three
//! connection handlers (`handler_sharded.rs`, `handler_monoio.rs`,
//! `handler_single.rs`). These tests confirm the wiring holds end-to-end
//! through real RESP round-trips on the runtime-tokio build.
//!
//! If a test here fails, that is a real regression in Plan 165-02's wiring —
//! do NOT weaken the assertion to force-pass.
//!
//! The four tests cover:
//!   1. `ft_search_as_of_honours_snapshot`            — sharded path via
//!      `listener::run_sharded` (1 shard, so AS_OF stays local — multi-shard
//!      AS_OF propagation is outside Phase 165's scope). AS_OF filters out
//!      docs inserted post-snapshot.
//!   2. `ft_search_as_of_unknown_snapshot_returns_err` — error path: AS_OF
//!      with no prior snapshot binding surfaces the exact helper ERR bytes.
//!   3. `ft_search_as_of_handler_single_parity`       — single-shard tokio
//!      path via `listener::run_with_shutdown` (handler_single.rs): AS_OF
//!      returns the same ERR because the handler passes `shard_databases=None`
//!      to the helper (W1 unified-signature contract).
//!   4. `ft_search_no_asof_no_txn_returns_latest`     — regression guard:
//!      default path (no AS_OF, no TXN) still returns latest.
//!
//! Scope note on multi-shard AS_OF: `TEMPORAL.SNAPSHOT_AT` registers its
//! (wall_ms -> LSN) binding on the receiving shard ONLY; the cross-shard
//! `ShardMessage::VectorSearch` message (see `src/shard/coordinator.rs`) does
//! NOT carry `as_of_lsn`. Both gaps are pre-existing architectural limits,
//! not Plan 165-02 regressions — Plan 165's objective is to make the SINGLE
//! receiving-shard path honour AS_OF / TXN.snapshot_lsn identically across
//! the three handler implementations, which these tests verify.
//!
//! Run:
//!   cargo test --test ft_search_temporal_parity --no-default-features \
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
// Test server harnesses
// ---------------------------------------------------------------------------

/// Build a minimal `ServerConfig` bound to a specific port with `num_shards`
/// and no persistence. All other knobs mirror the test defaults used by
/// `tests/txn_kv_wiring.rs::start_txn_server`.
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
/// Suitable for Tests 1, 2, and 4 where we want the sharded FT.SEARCH path.
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
                .name(format!("ft-parity-shard-{}", id))
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

/// Start a non-sharded Moon server via `listener::run_with_shutdown`.
/// Connections are handled by `handler_single.rs` — the single-shard tokio
/// path that Plan 165-02 wired to pass `shard_databases=None` to the helper.
/// Use this harness for Test 3 which locks that W1 contract end-to-end.
async fn start_moon_handler_single() -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();
    // Shards=1 here is cosmetic — `run_with_shutdown` ignores the `shards`
    // field and always uses a single in-process `SharedDatabases` list.
    let config = build_config(port, 1);
    let listener_cancel = token.clone();

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build single-handler runtime");
        rt.block_on(async {
            if let Err(e) = listener::run_with_shutdown(config, listener_cancel).await {
                eprintln!("run_with_shutdown error: {}", e);
            }
        });
    });

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    (port, token)
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Encode four f32 values as 16 little-endian bytes (no bytemuck dep).
fn vec4_bytes(v: [f32; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for x in &v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Create an HNSW/L2 vector index on PREFIX `doc:` over field `vec` (4 dim).
///
/// Moon's FT.CREATE only accepts `HNSW` as the vector algorithm tag
/// (see `src/command/vector_search/ft_create.rs`). Parameter order is
/// `VECTOR HNSW <n_params> DIM <d> TYPE FLOAT32 DISTANCE_METRIC <metric>` —
/// mirrors the format used by `scripts/test-consistency.sh`.
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

/// Parse a FT.SEARCH KNN response and return the numeric result count
/// (the first element in the RESP Array reply).
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

/// Collect all `doc:*` keys from a FT.SEARCH response in the order they
/// appear.
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
// Test 1 (TEMP-04, sharded path via run_sharded): AS_OF honours snapshot —
// doc inserted AFTER snapshot is NOT visible; doc inserted BEFORE snapshot
// IS visible. Uses 1 shard to exercise handler_sharded's local FT.SEARCH
// path (cross-shard AS_OF scatter propagation is architectural follow-up).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_as_of_honours_snapshot() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    ft_create_idx(&mut conn).await;

    // HSET doc:1 BEFORE snapshot: visible at AS_OF == snapshot_time.
    hset_vec(&mut conn, "doc:1", [1.0, 0.0, 0.0, 0.0]).await;

    // Let the auto-index settle before capturing the snapshot LSN.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Register the snapshot (server captures its own wall_ms + current LSN).
    let snap_ok: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut conn)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should return OK");
    assert_eq!(snap_ok, "OK");

    // Capture client-side wall_ms (approximates server wall_ms within clock
    // drift). Registry lookup is "last binding <= wall_ms" — a client wall_ms
    // >= server wall_ms will hit the binding we just recorded.
    let wall_ms_t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Ensure the POST-snapshot HSET lands at a strictly larger LSN than the
    // snapshot binding.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    hset_vec(&mut conn, "doc:2", [0.0, 1.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // AS_OF wall_ms_t1 → helper resolves to the snapshot LSN → only doc:1.
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
    let keys_asof = extract_doc_keys(&as_of_result);
    assert_eq!(
        count_asof, 1,
        "AS_OF snapshot must filter out doc:2 (inserted post-snapshot); got count={count_asof}, keys={keys_asof:?}"
    );
    assert_eq!(
        keys_asof.first().map(String::as_str),
        Some("doc:1"),
        "AS_OF result must be doc:1 (pre-snapshot); keys={keys_asof:?}"
    );

    // Without AS_OF → latest → both docs visible.
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
        count_latest, 2,
        "FT.SEARCH without AS_OF must return both docs (latest); got {count_latest}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2 (TEMP-04, error path): AS_OF with no registered snapshot binding
// returns the exact ERR payload from the helper.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_as_of_unknown_snapshot_returns_err() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    ft_create_idx(&mut conn).await;
    hset_vec(&mut conn, "doc:1", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // AS_OF 1 (Unix millis = 1970-01-01T00:00:00.001Z): no registered binding
    // at or before this time → helper MUST return the ERR frame.
    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let result: Result<redis::Value, redis::RedisError> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("AS_OF")
        .arg("1")
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await;

    let err = result.expect_err("FT.SEARCH AS_OF 1 must return an ERR frame");
    let err_msg = err.to_string();
    // redis-rs strips the leading "ERR " category prefix and exposes the
    // remainder — assert the helper's payload text.
    assert!(
        err_msg.contains("no temporal snapshot registered for the given AS_OF timestamp"),
        "error must carry the helper's ERR payload; got: {err_msg}"
    );
    assert!(
        err_msg.contains("TEMPORAL.SNAPSHOT_AT"),
        "error must reference TEMPORAL.SNAPSHOT_AT; got: {err_msg}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 3 (TEMP-04, single-shard tokio path via run_with_shutdown):
// handler_single.rs passes `shard_databases=None` and `active_cross_txn=None`
// to the helper. On explicit AS_OF this ALWAYS returns the unified ERR —
// end-to-end lock of Plan 165-01 Test 5. The no-AS_OF path still returns
// latest (regression guard).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_as_of_handler_single_parity() {
    let (port, shutdown) = start_moon_handler_single().await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    ft_create_idx(&mut conn).await;
    hset_vec(&mut conn, "doc:1", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Any AS_OF value — even a plausible wall_ms — must ERR on handler_single
    // because the helper is called with `shard_databases=None` (no registry
    // available). Use a current wall_ms to show it's not a "stale timestamp"
    // artifact.
    let wall_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let as_of_result: Result<redis::Value, redis::RedisError> = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("AS_OF")
        .arg(wall_ms.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await;

    let err = as_of_result
        .expect_err("handler_single FT.SEARCH AS_OF must ERR (shard_databases=None contract)");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("no temporal snapshot registered for the given AS_OF timestamp"),
        "single-shard AS_OF must surface the unified helper ERR; got: {err_msg}"
    );

    // Regression guard: FT.SEARCH WITHOUT AS_OF on handler_single returns
    // latest (doc:1).
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
        .expect("FT.SEARCH without AS_OF on handler_single must succeed");
    let count = parse_search_count(&latest_result);
    assert_eq!(
        count, 1,
        "handler_single FT.SEARCH (no AS_OF) must return 1 doc; got {count}"
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 4 (regression guard): no AS_OF + no TXN still returns latest via
// handler_sharded. Locks the `as_of_lsn = 0 ⇒ latest` semantic for the
// default path.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ft_search_no_asof_no_txn_returns_latest() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    ft_create_idx(&mut conn).await;
    hset_vec(&mut conn, "doc:1", [1.0, 0.0, 0.0, 0.0]).await;
    hset_vec(&mut conn, "doc:2", [0.0, 1.0, 0.0, 0.0]).await;
    hset_vec(&mut conn, "doc:3", [0.0, 0.0, 1.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

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
        .expect("FT.SEARCH (no AS_OF, no TXN) should succeed");
    let count = parse_search_count(&result);
    assert_eq!(
        count, 3,
        "no-AS_OF-no-TXN must return latest (3 docs); got {count}"
    );

    shutdown.cancel();
}
