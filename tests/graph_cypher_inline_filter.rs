//! v3-2 graph-cypher-inline-filter — effect-level row-count tests (GRAPH.QUERY).
//!
//! The server harness (build_config / start_moon_sharded / connect / rows_count) is
//! copied from tests/lunaris_cypher_shortest_path.rs per the duplicated-per-test
//! convention (Plan 165-03). Only the seed + assertions are task-specific.
#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

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
        let (shard_databases, mut slice_inits) =
            moon::shard::shared_databases::ShardDatabases::new(all_dbs);

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
            let shard_slice_init = slice_inits.remove(0);

            let handle = std::thread::Builder::new()
                .name(format!("lunaris-v3-shard-{}", id))
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
                        shard_slice_init,
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

fn rows_count(v: &redis::Value) -> usize {
    match v {
        redis::Value::Array(items) if items.len() >= 2 => match &items[1] {
            redis::Value::Array(rows) => rows.len(),
            other => panic!("expected rows Array in position [1], got {other:?}"),
        },
        other => panic!("expected outer Array with >=2 elements from GRAPH.QUERY, got {other:?}"),
    }
}

// ─── graph-cypher-inline-filter (v3-2) · effect-level row-count tests ──────────
// Corpus C: Person nodes id=1,2,3; directed edges (1)->(2),(1)->(3),(2)->(3).
// The (2)->(3) edge is load-bearing for the RED: it gives the label THREE out-edges
// while node 1 has only TWO, so a dropped inline predicate (the bug) returns 3 rows
// where the narrowed query must return 2 — bug and fix are distinguishable.
// Seeded via GRAPH.ADDNODE/GRAPH.ADDEDGE (the proven write path; a wire GRAPH.QUERY
// "CREATE" does not persist here — single-shard SPSC discards its write intent).
// `parse_property_value("1")` -> Int(1), so the `id` property is integer-typed and
// matches the `{id:N}` literal end-to-end (exercises the A2 type concern).
// RED until compile_match applies inline node-property predicates: with the predicate
// dropped, every query below returns all 3 label edges instead of the narrowed set.

async fn seed_corpus_c(conn: &mut redis::aio::MultiplexedConnection) {
    let _: redis::Value = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(conn)
        .await
        .expect("GRAPH.CREATE g");

    async fn add_person(conn: &mut redis::aio::MultiplexedConnection, id: i64) -> i64 {
        redis::cmd("GRAPH.ADDNODE")
            .arg("g")
            .arg("Person")
            .arg("id")
            .arg(id)
            .query_async(conn)
            .await
            .unwrap_or_else(|e| panic!("GRAPH.ADDNODE id={id}: {e}"))
    }
    let n1 = add_person(conn, 1).await;
    let n2 = add_person(conn, 2).await;
    let n3 = add_person(conn, 3).await;

    async fn add_edge(conn: &mut redis::aio::MultiplexedConnection, src: i64, dst: i64) {
        let _: redis::Value = redis::cmd("GRAPH.ADDEDGE")
            .arg("g")
            .arg(src)
            .arg(dst)
            .arg("R")
            .query_async(conn)
            .await
            .unwrap_or_else(|e| panic!("GRAPH.ADDEDGE {src}->{dst}: {e}"));
    }
    // Edges: (1)->(2), (1)->(3), (2)->(3).  Node 1 has 2 out-edges; label has 3.
    add_edge(conn, n1, n2).await;
    add_edge(conn, n1, n3).await;
    add_edge(conn, n2, n3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_point_query_returns_only_matching_edges() {
    // M3 (headline) — MATCH (a:Person {id:1})-[]->(b) returns ONLY node 1's 2 out-edges.
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    seed_corpus_c(&mut conn).await;

    let res: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Person {id:1})-[]->(b) RETURN b.id")
        .query_async(&mut conn)
        .await
        .expect("point query");
    assert_eq!(
        rows_count(&res),
        2,
        "inline {{id:1}} must narrow to node 1's 2 out-edges, not the whole label; got {res:?}"
    );
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_param_matches_like_literal() {
    // M5 (param) — {id:$pid} with --params {"pid":1} equals the literal {id:1} result.
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    seed_corpus_c(&mut conn).await;

    let lit: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Person {id:1})-[]->(b) RETURN b.id")
        .query_async(&mut conn)
        .await
        .expect("literal query");
    let par: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Person {id:$pid})-[]->(b) RETURN b.id")
        .arg("--params")
        .arg("{\"pid\":1}")
        .query_async(&mut conn)
        .await
        .expect("param query");
    assert_eq!(
        rows_count(&par),
        rows_count(&lit),
        "param {{id:$pid}} must match like literal {{id:1}}; lit={lit:?} par={par:?}"
    );
    assert_eq!(
        rows_count(&par),
        2,
        "param query narrows to node 1's 2 edges"
    );
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_missing_property_excluded_not_errored() {
    // M5 (missing prop) — {email:'x@y.z'} matches no node ⇒ 0 rows, NOT an error.
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    seed_corpus_c(&mut conn).await;

    let res: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Person {email:'x@y.z'})-[]->(b) RETURN b")
        .query_async(&mut conn)
        .await
        .expect("missing-prop query must NOT be a wire error");
    assert_eq!(
        rows_count(&res),
        0,
        "a node missing the inline property is excluded (empty result), got {res:?}"
    );
    shutdown.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_noneval_inline_value_no_panic() {
    // M7 / Reject (robustness green-pin) — an unbound identifier in the value position
    // must never panic/hang the server; the query may error or return empty, but a
    // subsequent PING must succeed.
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    seed_corpus_c(&mut conn).await;

    let res: Result<redis::Value, redis::RedisError> = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg("MATCH (a:Person {id: x})-[]->(b) RETURN b")
        .query_async(&mut conn)
        .await;
    // The query may be Err (error frame) or Ok (empty) — both acceptable; never a crash.
    let _ = res;
    let pong: redis::Value = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .expect("server must stay up after a bad inline value (no panic)");
    assert!(
        matches!(pong, redis::Value::SimpleString(ref s) if s == "PONG")
            || matches!(pong, redis::Value::Okay),
        "PING after the bad query should PONG; got {pong:?}"
    );
    shutdown.cancel();
}
