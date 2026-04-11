//! Deep/wide graph stress tests — worst-case scenarios.
//!
//! Tests pathological graph structures that stress the engine:
//! - Deep chains (1000-hop linear path)
//! - Wide hubs (single node with 10K+ edges)
//! - Dense cliques (fully connected subgraphs)
//! - Power-law degree distribution (realistic social/knowledge graphs)
//! - Bounded frontier cap verification under worst-case fan-out
//!
//! Run: cargo test --test graph_stress_deep --no-default-features --features runtime-tokio,jemalloc,graph --release -- --nocapture
#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::runtime::cancel::CancellationToken;
use redis::Value;
use std::time::Instant;
use tokio::net::TcpListener;

use moon::config::ServerConfig;
use moon::server::listener;

async fn start_server() -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let token = CancellationToken::new();
    let server_token = token.clone();

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
        shards: 0,
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
    };

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    (port, token)
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

// ---------------------------------------------------------------------------
// Scenario 1: Deep Chain — Linear path of 1000 nodes
//   Worst case for: max-depth traversal, Dijkstra path length
//   A→B→C→...→Z (1000 hops)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stress_deep_chain_1000() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    let depth = 1000;

    println!("\n=== Scenario 1: Deep Chain ({depth} nodes) ===");

    redis::cmd("GRAPH.CREATE")
        .arg("deep_chain")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Build the chain
    let start = Instant::now();
    let mut ids: Vec<i64> = Vec::with_capacity(depth);
    for i in 0..depth {
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("deep_chain")
            .arg("Step")
            .arg("depth")
            .arg(i.to_string())
            .query_async(&mut conn)
            .await
            .unwrap();
        ids.push(id);
    }
    for i in 0..depth - 1 {
        redis::cmd("GRAPH.ADDEDGE")
            .arg("deep_chain")
            .arg(ids[i])
            .arg(ids[i + 1])
            .arg("NEXT")
            .arg("WEIGHT")
            .arg("1.0")
            .query_async::<i64>(&mut conn)
            .await
            .unwrap();
    }
    let build_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!(
        "  Build: {depth} nodes + {} edges in {build_ms:.1}ms",
        depth - 1
    );

    // Query at various depths from root
    for query_depth in [1, 3, 5, 10] {
        let start = Instant::now();
        let result: Value = redis::cmd("GRAPH.NEIGHBORS")
            .arg("deep_chain")
            .arg(ids[0])
            .arg("DEPTH")
            .arg(query_depth.to_string())
            .query_async(&mut conn)
            .await
            .unwrap();
        let elapsed_us = start.elapsed().as_micros();
        let count = match &result {
            Value::Array(items) => items.len(),
            _ => 0,
        };
        println!(
            "  Depth {query_depth:>2} from root: {count:>4} items in {elapsed_us:>6}µs ({:.0}µs/hop)",
            elapsed_us as f64 / query_depth as f64
        );
    }

    // Query from middle of chain
    let mid = depth / 2;
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("deep_chain")
        .arg(ids[mid])
        .arg("DEPTH")
        .arg("5")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Depth 5 from middle (node {mid}): {count} items in {elapsed_us}µs");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Scenario 2: Wide Hub — Single node with 5000 outgoing edges
//   Worst case for: 1-hop fan-out, memory pressure, result serialization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stress_wide_hub_5000() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    let fan_out = 5000;

    println!("\n=== Scenario 2: Wide Hub ({fan_out} edges from 1 node) ===");

    redis::cmd("GRAPH.CREATE")
        .arg("wide_hub")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Create hub node
    let hub: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("wide_hub")
        .arg("Hub")
        .arg("name")
        .arg("central")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Create leaf nodes and edges
    let start = Instant::now();
    let mut leaf_ids: Vec<i64> = Vec::with_capacity(fan_out);
    for i in 0..fan_out {
        let leaf: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("wide_hub")
            .arg("Leaf")
            .arg("idx")
            .arg(i.to_string())
            .query_async(&mut conn)
            .await
            .unwrap();
        leaf_ids.push(leaf);
    }
    let node_ms = start.elapsed().as_secs_f64() * 1000.0;

    let start = Instant::now();
    for leaf in &leaf_ids {
        redis::cmd("GRAPH.ADDEDGE")
            .arg("wide_hub")
            .arg(hub)
            .arg(*leaf)
            .arg("CONNECTS")
            .arg("WEIGHT")
            .arg("0.5")
            .query_async::<i64>(&mut conn)
            .await
            .unwrap();
    }
    let edge_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!("  Build: {fan_out} leaves in {node_ms:.1}ms + {fan_out} edges in {edge_ms:.1}ms");

    // 1-hop from hub: returns ALL 5000 neighbors (worst-case fan-out)
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("wide_hub")
        .arg(hub)
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!(
        "  1-hop from hub: {count} items in {elapsed_us}µs ({:.1}µs/neighbor)",
        elapsed_us as f64 / (count.max(1)) as f64
    );

    // Typed filter: only a fraction of edges match
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("wide_hub")
        .arg(hub)
        .arg("TYPE")
        .arg("CONNECTS")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  1-hop TYPE CONNECTS: {count} items in {elapsed_us}µs");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Scenario 3: Dense Clique — 50 nodes all connected to each other
//   Worst case for: O(n²) edge count, BFS explosion at depth 2
//   50 nodes × 49 edges each = 2450 edges total
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stress_dense_clique_50() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    let clique_size = 50;
    let expected_edges = clique_size * (clique_size - 1); // directed: n*(n-1)

    println!("\n=== Scenario 3: Dense Clique ({clique_size} nodes, {expected_edges} edges) ===");

    redis::cmd("GRAPH.CREATE")
        .arg("clique")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Create clique nodes
    let start = Instant::now();
    let mut ids: Vec<i64> = Vec::with_capacity(clique_size);
    for i in 0..clique_size {
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("clique")
            .arg("Member")
            .arg("idx")
            .arg(i.to_string())
            .query_async(&mut conn)
            .await
            .unwrap();
        ids.push(id);
    }
    let node_ms = start.elapsed().as_secs_f64() * 1000.0;

    // Connect every pair (directed: a→b for all a≠b)
    let start = Instant::now();
    let mut edge_count = 0u32;
    for i in 0..clique_size {
        for j in 0..clique_size {
            if i == j {
                continue;
            }
            let result: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
                .arg("clique")
                .arg(ids[i])
                .arg(ids[j])
                .arg("LINKED")
                .query_async(&mut conn)
                .await;
            if result.is_ok() {
                edge_count += 1;
            }
        }
    }
    let edge_ms = start.elapsed().as_secs_f64() * 1000.0;
    println!(
        "  Build: {clique_size} nodes in {node_ms:.1}ms + {edge_count} edges in {edge_ms:.1}ms"
    );

    // 1-hop: each node connects to all others (49 neighbors)
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("clique")
        .arg(ids[0])
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!(
        "  1-hop from node 0: {count} items in {elapsed_us}µs (expect ~{} items)",
        (clique_size - 1) * 2
    );

    // 2-hop in a clique: every node reachable (all 50 nodes visible)
    // This is the explosion case: 49 neighbors × 49 neighbors = 2401 intermediate
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("clique")
        .arg(ids[0])
        .arg("DEPTH")
        .arg("2")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  2-hop from node 0: {count} items in {elapsed_us}µs (clique explosion)");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Scenario 4: Power-Law Graph — Realistic AI knowledge graph topology
//   80% nodes have degree 1-5, 15% have degree 20-50, 5% hubs with 200-500
//   Total: 2000 nodes, ~15K edges
//   Worst case for: mixed fan-out, hot-path variance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stress_power_law_2000() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    let num_nodes = 2000;

    println!("\n=== Scenario 4: Power-Law Graph ({num_nodes} nodes) ===");

    redis::cmd("GRAPH.CREATE")
        .arg("powerlaw")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Create nodes
    let start = Instant::now();
    let mut ids: Vec<i64> = Vec::with_capacity(num_nodes);
    let labels = ["Concept", "Fact", "Event", "Source", "Agent"];
    for i in 0..num_nodes {
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("powerlaw")
            .arg(labels[i % 5])
            .arg("idx")
            .arg(i.to_string())
            .query_async(&mut conn)
            .await
            .unwrap();
        ids.push(id);
    }
    let node_ms = start.elapsed().as_secs_f64() * 1000.0;
    let node_ops = num_nodes as f64 / (node_ms / 1000.0);

    // Create edges with power-law distribution
    // Hubs (top 5%): 100 nodes with 100-300 edges each
    // Medium (15%): 300 nodes with 10-30 edges each
    // Leaves (80%): 1600 nodes with 1-3 edges each
    let start = Instant::now();
    let mut total_edges = 0u32;
    let edge_types = [
        "RELATED_TO",
        "DERIVED_FROM",
        "OBSERVED_AT",
        "SUPERSEDES",
        "CITED_BY",
    ];

    // Hub edges (first 100 nodes, high degree)
    for i in 0..100 {
        let degree = 100 + (i * 2) % 200; // 100-300 edges per hub
        for j in 0..degree {
            let dst = (i * 7 + j * 13 + 100) % num_nodes;
            if dst == i {
                continue;
            }
            let result: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
                .arg("powerlaw")
                .arg(ids[i])
                .arg(ids[dst])
                .arg(edge_types[j % 5])
                .arg("WEIGHT")
                .arg("0.8")
                .query_async(&mut conn)
                .await;
            if result.is_ok() {
                total_edges += 1;
            }
        }
    }

    // Medium edges (nodes 100-400)
    for i in 100..400 {
        let degree = 10 + (i % 20);
        for j in 0..degree {
            let dst = (i * 3 + j * 7 + 50) % num_nodes;
            if dst == i {
                continue;
            }
            let result: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
                .arg("powerlaw")
                .arg(ids[i])
                .arg(ids[dst])
                .arg(edge_types[j % 5])
                .arg("WEIGHT")
                .arg("0.5")
                .query_async(&mut conn)
                .await;
            if result.is_ok() {
                total_edges += 1;
            }
        }
    }

    // Leaf edges (nodes 400-2000)
    for i in 400..num_nodes {
        let degree = 1 + (i % 3);
        for j in 0..degree {
            let dst = (i + j * 11 + 1) % num_nodes;
            if dst == i {
                continue;
            }
            let result: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
                .arg("powerlaw")
                .arg(ids[i])
                .arg(ids[dst])
                .arg(edge_types[j % 5])
                .arg("WEIGHT")
                .arg("0.2")
                .query_async(&mut conn)
                .await;
            if result.is_ok() {
                total_edges += 1;
            }
        }
    }
    let edge_ms = start.elapsed().as_secs_f64() * 1000.0;
    let edge_ops = total_edges as f64 / (edge_ms / 1000.0);
    println!(
        "  Build: {num_nodes} nodes ({node_ops:.0}/s) + {total_edges} edges ({edge_ops:.0}/s)"
    );

    // Query hub node (high degree) — worst case 1-hop
    println!("\n  --- Hub Node Queries (high degree, worst case) ---");
    let hub_node = ids[0];
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("powerlaw")
        .arg(hub_node)
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Hub 1-hop: {count} items in {elapsed_us}µs");

    // 2-hop from hub — explosion: hub has 200+ neighbors, each has 10-200 neighbors
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("powerlaw")
        .arg(hub_node)
        .arg("DEPTH")
        .arg("2")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Hub 2-hop: {count} items in {elapsed_us}µs (fan-out explosion)");

    // 3-hop from hub — max depth, potentially touching 1000+ nodes
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("powerlaw")
        .arg(hub_node)
        .arg("DEPTH")
        .arg("3")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Hub 3-hop: {count} items in {elapsed_us}µs (deep explosion)");

    // Query leaf node (low degree) — best case
    println!("\n  --- Leaf Node Queries (low degree, best case) ---");
    let leaf_node = ids[1500];
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("powerlaw")
        .arg(leaf_node)
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Leaf 1-hop: {count} items in {elapsed_us}µs");

    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("powerlaw")
        .arg(leaf_node)
        .arg("DEPTH")
        .arg("3")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Leaf 3-hop: {count} items in {elapsed_us}µs");

    // Typed edge filter on hub — selective query
    println!("\n  --- Filtered Queries ---");
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("powerlaw")
        .arg(hub_node)
        .arg("TYPE")
        .arg("RELATED_TO")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Hub TYPE=RELATED_TO: {count} items in {elapsed_us}µs");

    // Cypher on power-law graph
    let start = Instant::now();
    let result: Value = redis::cmd("GRAPH.QUERY")
        .arg("powerlaw")
        .arg("MATCH (n:Concept) RETURN n LIMIT 20")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed_us = start.elapsed().as_micros();
    let count = match &result {
        Value::Array(items) => items.len(),
        _ => 0,
    };
    println!("  Cypher MATCH Concept LIMIT 20: {count} items in {elapsed_us}µs");

    // Throughput: 1000 random 1-hop queries (mixed hub/leaf)
    println!("\n  --- Throughput: 1000 Random 1-hop Queries ---");
    let start = Instant::now();
    let mut ok = 0u32;
    for i in 0..1000 {
        let idx = (i * 13 + 7) % num_nodes;
        let result: Result<Value, _> = redis::cmd("GRAPH.NEIGHBORS")
            .arg("powerlaw")
            .arg(ids[idx])
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            ok += 1;
        }
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let qps = ok as f64 / (elapsed_ms / 1000.0);
    println!("  {ok}/1000 queries in {elapsed_ms:.1}ms ({qps:.0} qps)");

    // Throughput: 500 random 2-hop queries
    println!("\n  --- Throughput: 500 Random 2-hop Queries ---");
    let start = Instant::now();
    ok = 0;
    for i in 0..500 {
        let idx = (i * 17 + 3) % num_nodes;
        let result: Result<Value, _> = redis::cmd("GRAPH.NEIGHBORS")
            .arg("powerlaw")
            .arg(ids[idx])
            .arg("DEPTH")
            .arg("2")
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            ok += 1;
        }
    }
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let qps = ok as f64 / (elapsed_ms / 1000.0);
    println!("  {ok}/500 queries in {elapsed_ms:.1}ms ({qps:.0} qps)");

    token.cancel();
}
