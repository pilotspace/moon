//! Moon-side RED→GREEN test — single-shard HYBRID FILTER on a TAG field
//! (TASK.md §3 CHANGE A/B/E; conformance list "vendor/moon/tests/hybrid_filter_tag.rs").
//!
//! Asserts the new `FILTER TAG @source <v>` HYBRID modifier constrains BOTH the
//! BM25 branch and the dense-KNN branch before RRF fusion.
//!
//! RED before CHANGE A/B/E: the FILTER clause was not parsed, so the foreign-
//! source doc leaked via the dense-KNN branch.
//! GREEN after CHANGE A/B/E: both doc:2 and doc:3 (source=planning) are absent.
#![cfg(all(feature = "runtime-tokio", feature = "text-index"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server harness — duplicated per Plan 165-03 "duplicate-not-extract" convention.
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
        maxmemory: Some(0),
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
        cross_shard_fast_path: moon::config::CrossShardFastPath::Auto,
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
        graph_merge_max_segments: 8,
        graph_dead_edge_trigger: 0.20,
        autovacuum_starvation_cap_secs: 300,
        vec_warm_mmap_budget: "2gb".to_string(),
        cold_orphan_sweep_interval_secs: 300,
        migrate_aof_from: None,
        migrate_aof_to: None,
        migrate_aof_shards: 0,
        ..Default::default()
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
                .name(format!("filter-tag-shard-{}", id))
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

fn parse_search_keys(v: &redis::Value) -> (i64, Vec<String>) {
    let items = match v {
        redis::Value::Array(items) => items,
        other => panic!("expected Array from FT.SEARCH, got {other:?}"),
    };
    let count = match items.first() {
        Some(redis::Value::Int(n)) => *n,
        Some(redis::Value::BulkString(b)) => std::str::from_utf8(b)
            .expect("count is utf-8")
            .parse::<i64>()
            .expect("count parses"),
        other => panic!("unexpected first item: {other:?}"),
    };
    let mut keys = Vec::new();
    for item in items.iter().skip(1) {
        if let redis::Value::BulkString(b) = item
            && let Ok(s) = std::str::from_utf8(b)
            && s.starts_with("doc:")
        {
            keys.push(s.to_string());
        }
    }
    (count, keys)
}

// ---------------------------------------------------------------------------
// Index + data helpers
// ---------------------------------------------------------------------------

async fn ft_create_tag_idx(conn: &mut redis::aio::MultiplexedConnection) {
    let r: String = redis::cmd("FT.CREATE")
        .arg("idx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("doc:")
        .arg("SCHEMA")
        .arg("title")
        .arg("TEXT")
        .arg("source")
        .arg("TAG")
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
        .expect("FT.CREATE tag index should succeed");
    assert_eq!(r, "OK");
}

async fn hset_doc(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    title: &str,
    source: &str,
    v: [f32; 4],
) {
    let _: i64 = redis::cmd("HSET")
        .arg(key)
        .arg("title")
        .arg(title)
        .arg("source")
        .arg(source)
        .arg("vec")
        .arg(vec4_bytes(v))
        .query_async(conn)
        .await
        .expect("HSET should succeed");
}

// ---------------------------------------------------------------------------
// Test: foreign source must be absent from BOTH BM25 and KNN branches
// ---------------------------------------------------------------------------

/// Foreign source must be absent from BOTH the BM25 and the dense-KNN branch of
/// a filtered HYBRID query.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hybrid_filter_tag_excludes_foreign_from_both_branches() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    ft_create_tag_idx(&mut conn).await;

    // doc:1 — KEEP: source=scratchpad, text-matches "alice", vector near query.
    hset_doc(
        &mut conn,
        "doc:1",
        "alice knows bob",
        "scratchpad",
        [1.0, 0.0, 0.0, 0.0],
    )
    .await;
    // doc:2 — FOREIGN, BM25-leak candidate: source=planning, text-matches "alice".
    hset_doc(
        &mut conn,
        "doc:2",
        "alice met carol",
        "planning",
        [0.0, 1.0, 0.0, 0.0],
    )
    .await;
    // doc:3 — FOREIGN, KNN-leak candidate: source=planning, vector near query,
    //          text does NOT match "alice".
    hset_doc(
        &mut conn,
        "doc:3",
        "bob met carol",
        "planning",
        [0.9, 0.1, 0.0, 0.0],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let q = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let resp: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("alice")
        .arg("HYBRID")
        .arg("VECTOR")
        .arg("@vec")
        .arg("$q")
        .arg("FUSION")
        .arg("RRF")
        .arg("FILTER")
        .arg("TAG")
        .arg("@source")
        .arg("scratchpad") // CHANGE E wire modifier
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q)
        .arg("LIMIT")
        .arg("0")
        .arg("10")
        .query_async(&mut conn)
        .await
        .expect("filtered HYBRID must succeed");

    let (_count, keys) = parse_search_keys(&resp);
    println!("[hybrid_filter_tag] keys={keys:?}");
    assert!(
        !keys.iter().any(|k| k == "doc:2"),
        "BM25-branch foreign doc:2 (planning) leaked past FILTER TAG @source scratchpad; keys={keys:?}",
    );
    assert!(
        !keys.iter().any(|k| k == "doc:3"),
        "KNN-branch foreign doc:3 (planning) leaked past FILTER TAG @source scratchpad; keys={keys:?}",
    );
    assert!(
        keys.contains(&"doc:1".to_string()),
        "kept doc:1 must remain; keys={keys:?}"
    );

    shutdown.cancel();
}
