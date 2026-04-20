// FIX-04 + FIX-05 shared file. Tests 1-3 = FIX-04. Test 4 body delivered in plan 174-05.
//!
//! Phase 174 FIX-04 — Adversarial RED/GREEN coverage for three defects in
//! `Expr::ShortestPathCall` at `src/graph/cypher/executor/eval.rs:250-282`:
//!
//! 1. `empty_segs: [Arc<CsrStorage>; 0] = []` hides committed immutable-segment
//!    edges after compaction — tested via max_hops parity (defect exposed by
//!    the shared helper receiving real segments).
//! 2. `edge_types: _` destructured then hardcoded to `None` — ignores filters.
//! 3. `direction: _` hardcoded to `Direction::Both` — ignores direction.
//!
//! Runtime: `cargo test --no-default-features --features runtime-tokio,jemalloc,graph
//! --test adversarial_v0110_fix04_shortest_path_call_parity -- --test-threads=1`

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
                .name(format!("fix04-shard-{}", id))
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

/// Extract the first row's first column as a list of integers (a Path).
/// Returns None if absent / wrong shape / nil.
fn first_path(v: &redis::Value) -> Option<Vec<i64>> {
    let redis::Value::Array(items) = v else { return None };
    if items.len() < 2 {
        return None;
    }
    let redis::Value::Array(rows) = &items[1] else { return None };
    let first_row = rows.first()?;
    let redis::Value::Array(cells) = first_row else { return None };
    let first_cell = cells.first()?;
    match first_cell {
        redis::Value::Array(arr) => {
            let mut path = Vec::new();
            for item in arr {
                match item {
                    redis::Value::Int(n) => path.push(*n),
                    redis::Value::BulkString(b) => {
                        if let Ok(s) = std::str::from_utf8(b) {
                            if let Ok(n) = s.parse::<i64>() {
                                path.push(n);
                            }
                        }
                    }
                    _ => {}
                }
            }
            if path.is_empty() { None } else { Some(path) }
        }
        _ => None,
    }
}

/// Check if the first row's first cell is Null (nil).
fn first_is_null(v: &redis::Value) -> bool {
    let redis::Value::Array(items) = v else { return true };
    if items.len() < 2 {
        return true;
    }
    let redis::Value::Array(rows) = &items[1] else { return true };
    let Some(first_row) = rows.first() else { return true };
    let redis::Value::Array(cells) = first_row else { return true };
    let Some(first_cell) = cells.first() else { return true };
    matches!(first_cell, redis::Value::Nil)
}

// ===========================================================================
// TEST 1: Expression-form shortestPath caps max_hops at 32 (parity with
// MATCH-form).
//
// Graph: linear chain of 35 nodes N0->N1->...->N34 (34 edges, type LINK).
//
// Query: RETURN shortestPath((n0)-[:LINK*..100]->(n34))
//   max_hops = 100 in the expression.
//   MATCH-form caps at .min(32) → path limited to depth 32.
//   Expression-form passes raw max_hops → path of all 34 hops.
//
// Expected (correct): path capped at 32 hops (33 nodes), same as MATCH-form.
// BUG: expression-form passes *max_hops raw without .min(32), so Dijkstra
// explores the full 34-hop chain. Path has 35 nodes, not 33.
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_shortest_path_expr_caps_max_hops() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    // Create graph
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Create 35 nodes in a linear chain (use "n0", "n1", ... as string IDs
    // since GRAPH.ADDNODE parses numeric strings as Int, breaking string WHERE).
    let chain_len = 35;
    let mut node_ids: Vec<i64> = Vec::with_capacity(chain_len);
    for i in 0..chain_len {
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("g")
            .arg("N")
            .arg("idx")
            .arg(format!("n{}", i))
            .query_async(&mut conn)
            .await
            .unwrap_or_else(|_| panic!("add node {}", i));
        node_ids.push(id);
    }

    // Create 34 edges: N0->N1->...->N34
    for i in 0..chain_len - 1 {
        let _: i64 = redis::cmd("GRAPH.ADDEDGE")
            .arg("g")
            .arg(node_ids[i])
            .arg(node_ids[i + 1])
            .arg("LINK")
            .query_async(&mut conn)
            .await
            .unwrap_or_else(|_| panic!("edge {}->{}", i, i + 1));
    }

    // Pre-check: verify a short chain works (4 nodes, 3 hops, max_hops=100)
    let short_result: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (s:N), (t:N) WHERE s.idx = 'n0' AND t.idx = 'n3' \
             RETURN shortestPath((s)-[:LINK*..100]->(t)) AS p",
        )
        .query_async(&mut conn)
        .await
        .expect("short chain");
    let short_path = first_path(&short_result);
    assert_eq!(
        short_path.as_ref().map(|p| p.len()),
        Some(4),
        "FIX-04 Test 1 pre-check: 3-hop chain with max_hops=100 should return 4 nodes. \
         Got: {:?}",
        short_result
    );

    // Now test a 34-hop chain with max_hops=100. After fix (capped at 32),
    // the Dijkstra can only explore 32 hops, which doesn't reach N34.
    // So the correct result is Null.
    //
    // BUG: expression-form passes *max_hops raw (100) without .min(32),
    // so Dijkstra explores the full 34-hop chain and returns all 35 nodes.
    let result: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (s:N), (t:N) WHERE s.idx = 'n0' AND t.idx = 'n34' \
             RETURN shortestPath((s)-[:LINK*..100]->(t)) AS p",
        )
        .query_async(&mut conn)
        .await
        .expect("expr shortestPath max_hops");

    let path = first_path(&result);
    let node_count = path.as_ref().map(|p| p.len());

    // RED assertion: after fix, .min(32) caps depth at 32, so N34 (at depth 34)
    // is unreachable → result is Null.
    // On buggy code: uncapped max_hops=100 finds the full path (35 nodes).
    assert!(
        first_is_null(&result),
        "FIX-04 Test 1: Expression-form shortestPath((s)-[:LINK*..100]->(t)) over a 34-hop \
         chain should return Null when max_hops is capped at 32 (parity with MATCH-form). \
         But max_hops is passed raw without .min(32), so the uncapped Dijkstra finds the \
         full 34-hop path. Got {} nodes in path: {:?}. Response: {:?}",
        node_count.unwrap_or(0),
        path,
        result
    );

    shutdown.cancel();
}

// ===========================================================================
// TEST 2: Expression-form shortestPath honours edge_types filter.
//
// Graph (via ADDNODE/ADDEDGE):
// - A -[:KNOWS]-> B (direct 1-hop via KNOWS)
// - A -[:HATES]-> C -[:HATES]-> B (2-hop via HATES)
//
// Query: RETURN shortestPath((a)-[:HATES*..3]->(b))
//
// Expected: path with 3 nodes [A, C, B] (2 hops via HATES edges only)
// BUG: edge_types is destructured to `_` and ignored, so Dijkstra uses ALL
// edges and returns path [A, B] (2 nodes, 1 hop via KNOWS).
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_shortest_path_expr_honours_edge_types() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    // Create graph
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Create nodes
    let node_a: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("P")
        .arg("id")
        .arg("A")
        .query_async(&mut conn)
        .await
        .expect("add A");
    let node_b: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("P")
        .arg("id")
        .arg("B")
        .query_async(&mut conn)
        .await
        .expect("add B");
    let node_c: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("P")
        .arg("id")
        .arg("C")
        .query_async(&mut conn)
        .await
        .expect("add C");

    // Create edges: A-[:KNOWS]->B, A-[:HATES]->C, C-[:HATES]->B
    let _: i64 = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(node_a)
        .arg(node_b)
        .arg("KNOWS")
        .query_async(&mut conn)
        .await
        .expect("A-KNOWS->B");
    let _: i64 = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(node_a)
        .arg(node_c)
        .arg("HATES")
        .query_async(&mut conn)
        .await
        .expect("A-HATES->C");
    let _: i64 = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(node_c)
        .arg(node_b)
        .arg("HATES")
        .query_async(&mut conn)
        .await
        .expect("C-HATES->B");

    // Expression-form with HATES edge type filter
    let result: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:P), (b:P) WHERE a.id = 'A' AND b.id = 'B' \
             RETURN shortestPath((a)-[:HATES*..3]->(b)) AS p",
        )
        .query_async(&mut conn)
        .await
        .expect("expr shortestPath with edge type");

    // Extract path. With HATES filter, the shortest HATES-only path is
    // A->C->B (3 nodes, 2 hops). Without filter (the bug), A->B via KNOWS
    // is returned (2 nodes, 1 hop).
    let path = first_path(&result);
    let path_len = path.as_ref().map(|p| p.len());

    assert_eq!(
        path_len,
        Some(3),
        "FIX-04 Test 2: Expression-form shortestPath((a)-[:HATES*..3]->(b)) should return \
         path with 3 nodes [A, C, B] (HATES-only). Got path {:?} (node count {:?}). \
         If 2 nodes, edge_types filter is being ignored (the bug). Response: {:?}",
        path,
        path_len,
        result
    );

    shutdown.cancel();
}

// ===========================================================================
// TEST 3: Expression-form shortestPath honours direction filter.
//
// Graph (via ADDNODE/ADDEDGE): A -[:KNOWS]-> B (one direction only)
//
// Query: RETURN shortestPath((b)-[:KNOWS*..2]->(a))
//   Syntax `->(a)` means the path MUST follow outgoing direction from b.
//
// Expected (correct): Null — no outgoing KNOWS edges from B to A.
// BUG: direction is destructured to `_` and hardcoded to Direction::Both,
// so the traversal finds B<-A (following A->B backwards) and returns [B, A].
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_shortest_path_expr_honours_direction() {
    let (port, shutdown) = start_server(1).await;
    let mut conn = connect(port).await;

    // Create graph
    let _: String = redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async(&mut conn)
        .await
        .expect("GRAPH.CREATE");

    // Create nodes and edge: A -[:KNOWS]-> B (one direction only)
    let node_a: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("P")
        .arg("id")
        .arg("A")
        .query_async(&mut conn)
        .await
        .expect("add A");
    let node_b: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("P")
        .arg("id")
        .arg("B")
        .query_async(&mut conn)
        .await
        .expect("add B");
    let _: i64 = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(node_a)
        .arg(node_b)
        .arg("KNOWS")
        .query_async(&mut conn)
        .await
        .expect("A-KNOWS->B");

    // Verify the forward path works (A->B should find path)
    let forward: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:P), (b:P) WHERE a.id = 'A' AND b.id = 'B' \
             RETURN shortestPath((a)-[:KNOWS*..2]->(b)) AS p",
        )
        .query_async(&mut conn)
        .await
        .expect("forward path");
    let fwd_path = first_path(&forward);
    assert!(
        fwd_path.is_some(),
        "FIX-04 Test 3 pre-check: forward A->B path should exist. Got: {:?}",
        forward
    );

    // Now test the reverse direction: B -> A (outgoing from B)
    // BUG: direction is ignored (hardcoded Both), so it finds B<-A path.
    // After fix: Direction::Outgoing means B has no outgoing KNOWS to A => Null.
    let result: redis::Value = redis::cmd("GRAPH.QUERY")
        .arg("g")
        .arg(
            "MATCH (a:P), (b:P) WHERE a.id = 'A' AND b.id = 'B' \
             RETURN shortestPath((b)-[:KNOWS*..2]->(a)) AS p",
        )
        .query_async(&mut conn)
        .await
        .expect("expr shortestPath direction");

    let is_null = first_is_null(&result);
    assert!(
        is_null,
        "FIX-04 Test 3: Expression-form shortestPath((b)-[:KNOWS*..2]->(a)) should return \
         Null because there is no outgoing path from B to A. But direction filter is ignored \
         (hardcoded Direction::Both) so it finds the reverse path [B, A]. Response: {:?}",
        result
    );

    shutdown.cancel();
}

// ===========================================================================
// TEST 4: PROFILE shortestPath loads CSR segments (FIX-05 — stub).
// ===========================================================================

#[tokio::test(flavor = "current_thread")]
#[ignore = "implemented in plan 174-05"]
async fn test_profile_shortestpath_loads_csr_segments() {
    // Stub — body delivered in plan 174-05 (FIX-05).
    // This test will verify that GRAPH.PROFILE's ShortestPath operator
    // loads immutable CSR segments correctly.
}
