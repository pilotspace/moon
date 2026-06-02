//! Phase 174 FIX-06 — Adversarial RED/GREEN coverage for shortestPath() parser
//! honouring min_hops and correct error offset.
//!
//! Source: merged_bug_011 (ultrareview of v0.1.9 / commit 2e397c7). Two defects
//! at `src/graph/cypher/parser/pattern.rs:230-238`:
//!
//! 1. `Some((_, max)) => max` silently drops min_hops. A query
//!    `shortestPath((a)-[*2..5]-(b))` parses but Dijkstra returns a 1-hop path
//!    even though user specified min >= 2 — silent wrong result.
//!
//! 2. Fixed-length fallback error uses hardcoded `offset: 0` instead of the
//!    actual token span start — useless for debugging.
//!
//! FIX-06 RED. Accepts either min_hops plumb-through (Option A) OR typed reject
//! (Option B). Never a 1-hop result.
//!
//! Runtime: `cargo test --no-default-features --features runtime-tokio,jemalloc,graph
//! --test adversarial_v0110_fix06_shortest_path_min_hops -- --test-threads=1`

#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server infrastructure — mirrors existing adversarial test harnesses.
// ---------------------------------------------------------------------------

async fn start_server(num_shards: usize) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();

    let config = ServerConfig {
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
                .name(format!("fix06-shard-{}", id))
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

// ---------------------------------------------------------------------------
// Value helpers — extract data from GRAPH.QUERY response.
// ---------------------------------------------------------------------------

/// Extract the first row's first column as i64.
fn first_i64(v: &redis::Value) -> Option<i64> {
    let redis::Value::Array(items) = v else {
        return None;
    };
    if items.len() < 2 {
        return None;
    }
    let redis::Value::Array(rows) = &items[1] else {
        return None;
    };
    let first_row = rows.first()?;
    let redis::Value::Array(cells) = first_row else {
        return None;
    };
    let first_cell = cells.first()?;
    match first_cell {
        redis::Value::Int(n) => Some(*n),
        redis::Value::BulkString(b) => std::str::from_utf8(b).ok()?.parse().ok(),
        redis::Value::SimpleString(s) => s.parse().ok(),
        _ => None,
    }
}

/// Check if the response is a Redis error whose message contains a substring.
fn is_error_containing(result: &Result<redis::Value, redis::RedisError>, needle: &str) -> bool {
    match result {
        Err(e) => {
            let msg = format!("{}", e);
            msg.to_lowercase().contains(&needle.to_lowercase())
        }
        Ok(redis::Value::Array(items)) => {
            // Some graph responses wrap errors in the array
            for item in items {
                if let redis::Value::BulkString(b) = item {
                    if let Ok(s) = std::str::from_utf8(b) {
                        if s.to_lowercase().contains(&needle.to_lowercase()) {
                            return true;
                        }
                    }
                }
            }
            false
        }
        _ => false,
    }
}

// ===========================================================================
// TEST 1: shortestPath with min_hops > 1 must NOT return a 1-hop result.
//
// Graph: A -[:KNOWS]-> B (direct, 1 hop)
//        A -[:KNOWS]-> C -[:KNOWS]-> D -[:KNOWS]-> B (3 hops)
//
// Query: shortestPath((a)-[*2..5]-(b)) — min_hops = 2.
//
// Acceptable outcomes:
//   - Option A (plumb-through): returns L == 3 (the 3-hop chain)
//   - Option B (reject): returns an error mentioning "min_hops" or "unsupported"
//
// UNACCEPTABLE: L == 1 (current behaviour — silent wrong result)
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_shortest_path_min_hops_honoured() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    // Create graph
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Seed nodes and edges:
    // Direct path: A -> B (1 hop)
    // Indirect path: A -> C -> D -> B (3 hops)
    let _: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "CREATE (a:Node {id:'A'}), (b:Node {id:'B'}), (c:Node {id:'C'}), (d:Node {id:'D'}), \
             (a)-[:KNOWS]->(b), \
             (a)-[:KNOWS]->(c), \
             (c)-[:KNOWS]->(d), \
             (d)-[:KNOWS]->(b)",
        )
        .query_async(&mut conn)
        .await
        .expect("seed graph");

    // Query with min_hops = 2 — must NOT return L == 1.
    let result: Result<redis::Value, redis::RedisError> = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH p = shortestPath((a:Node {id:'A'})-[:KNOWS*2..5]-(b:Node {id:'B'})) \
             RETURN length(p) AS L",
        )
        .query_async(&mut conn)
        .await;

    // Either an error (Option B: reject non-default min_hops) or a correct
    // path length >= 2 (Option A: plumb-through).
    let option_b_error = is_error_containing(&result, "min_hops")
        || is_error_containing(&result, "unsupported")
        || is_error_containing(&result, "UnsupportedFeature");

    if option_b_error {
        // Option B chosen — parser rejects the query. This is acceptable.
    } else {
        // Option A — we expect L >= 2 (the 3-hop chain).
        let val = result.expect(
            "FIX-06: shortestPath with min_hops=2 should either error or return a valid path, \
             not fail with an unrelated error",
        );
        let length = first_i64(&val);
        assert!(
            length.is_some() && length.unwrap() >= 2,
            "FIX-06: shortestPath((a)-[*2..5]-(b)) returned L={:?} — expected L >= 2 \
             (3-hop chain) or a typed error rejecting min_hops > 1. Got silent wrong result \
             (1-hop path). Response: {:?}",
            length,
            val
        );
    }

    shutdown.cancel();
}

// ===========================================================================
// TEST 2: Fixed-length edge error offset must be non-zero.
//
// Query: `MATCH p = shortestPath((a)-[:KNOWS]-(b)) RETURN p`
// (no variable-length `*` on the edge — this is a fixed-length edge)
//
// Expected: error mentioning "variable-length" with offset > 0 (pointing to
// the actual edge token position, not hardcoded 0).
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_shortest_path_fixed_length_error_offset_nonzero() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    // Create graph (we need it to exist to reach the parser)
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Query with fixed-length edge (no `*`) — parser must reject.
    let query_str = "MATCH p = shortestPath((a)-[:KNOWS]-(b)) RETURN p";
    let result: Result<redis::Value, redis::RedisError> = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(query_str)
        .query_async(&mut conn)
        .await;

    // Must be an error about variable-length requirement.
    let err =
        result.expect_err("FIX-06: shortestPath with fixed-length edge must return a parse error");
    let err_msg = format!("{}", err);
    assert!(
        err_msg.to_lowercase().contains("variable-length")
            || err_msg.to_lowercase().contains("fixed-length"),
        "FIX-06: error message should mention variable-length requirement. Got: {}",
        err_msg
    );

    // The error message must contain a non-zero offset. The format from Display impl is:
    // "unexpected token at {offset}: ..." — extract the offset and confirm > 0.
    // OR for UnsupportedFeature: "unsupported feature at {offset}: ..."
    //
    // We check that the error does NOT contain "at 0:" which would indicate
    // the hardcoded offset: 0 bug.
    assert!(
        !err_msg.contains("at 0:"),
        "FIX-06: error offset must not be 0 (hardcoded). The error should point to the \
         actual token position. Got: {}",
        err_msg
    );

    // Additionally verify the offset IS present and non-zero by finding a
    // digit after "at ".
    let at_pos = err_msg.find("at ");
    assert!(
        at_pos.is_some(),
        "FIX-06: error message should contain 'at <offset>'. Got: {}",
        err_msg
    );
    let after_at = &err_msg[at_pos.unwrap() + 3..];
    let offset_str: String = after_at
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    let offset_val: usize = offset_str.parse().unwrap_or(0);
    assert!(
        offset_val > 0,
        "FIX-06: error offset must be > 0 (actual token position), got 0. Error: {}",
        err_msg
    );

    shutdown.cancel();
}
