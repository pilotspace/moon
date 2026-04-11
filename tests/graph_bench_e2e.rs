//! End-to-end graph engine throughput benchmark over real TCP.
//!
//! Measures Moon graph operations at realistic scale (1000 nodes, 3000 edges)
//! and reports ops/sec for node insert, edge insert, 1-hop query, 2-hop query,
//! and Cypher queries.
//!
//! Run: cargo test --test graph_bench_e2e --no-default-features --features runtime-tokio,jemalloc,graph --release -- --nocapture
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

#[tokio::test]
async fn bench_graph_operations() {
    let (port, token) = start_server().await;
    let mut conn = connect(port).await;

    let num_nodes = 1000;
    let num_edges = 3000;

    println!("\n============================================================");
    println!("  Moon Graph Engine — E2E Throughput Benchmark");
    println!("  Scale: {num_nodes} nodes, {num_edges} edges");
    println!("  Transport: redis crate over TCP (multiplexed)");
    println!("============================================================\n");

    // --- Create Graph ---
    redis::cmd("GRAPH.CREATE")
        .arg("{agent}:bench")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // --- Node Insertion ---
    let labels = ["Concept", "Fact", "Event", "Source", "Agent"];
    let mut node_ids: Vec<i64> = Vec::with_capacity(num_nodes);

    let start = Instant::now();
    for i in 0..num_nodes {
        let label = labels[i % 5];
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("{agent}:bench")
            .arg(label)
            .arg("name")
            .arg(format!("k_{i}"))
            .arg("confidence")
            .arg("0.42")
            .query_async(&mut conn)
            .await
            .unwrap();
        node_ids.push(id);
    }
    let node_elapsed = start.elapsed();
    let node_ops = num_nodes as f64 / node_elapsed.as_secs_f64();
    println!(
        "  Node Insert:    {:>8} nodes in {:>6.1}ms  ({:>8.0} ops/s)",
        num_nodes,
        node_elapsed.as_secs_f64() * 1000.0,
        node_ops
    );

    // --- Edge Insertion ---
    let edge_types = [
        "RELATED_TO",
        "DERIVED_FROM",
        "OBSERVED_AT",
        "SUPERSEDES",
        "CITED_BY",
    ];
    let mut edge_ok = 0u32;

    let start = Instant::now();
    for i in 0..num_edges {
        let src_idx = i % num_nodes;
        let dst_idx = (i * 7 + 3) % num_nodes;
        if src_idx == dst_idx {
            continue;
        }
        let etype = edge_types[i % 5];
        let result: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
            .arg("{agent}:bench")
            .arg(node_ids[src_idx])
            .arg(node_ids[dst_idx])
            .arg(etype)
            .arg("WEIGHT")
            .arg("0.42")
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            edge_ok += 1;
        }
    }
    let edge_elapsed = start.elapsed();
    let edge_ops = edge_ok as f64 / edge_elapsed.as_secs_f64();
    println!(
        "  Edge Insert:    {:>8} edges in {:>6.1}ms  ({:>8.0} ops/s)",
        edge_ok,
        edge_elapsed.as_secs_f64() * 1000.0,
        edge_ops
    );

    // --- 1-Hop Neighbor Queries ---
    let num_queries = 500;
    let start = Instant::now();
    let mut query_ok = 0u32;
    for i in 0..num_queries {
        let idx = (i * 13) % num_nodes;
        let result: Result<Value, _> = redis::cmd("GRAPH.NEIGHBORS")
            .arg("{agent}:bench")
            .arg(node_ids[idx])
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            query_ok += 1;
        }
    }
    let query_elapsed = start.elapsed();
    let query_ops = query_ok as f64 / query_elapsed.as_secs_f64();
    let avg_us = query_elapsed.as_micros() as f64 / num_queries as f64;
    println!(
        "  1-Hop Query:    {:>8} queries in {:>6.1}ms  ({:>8.0} qps, avg {:>6.0}µs)",
        query_ok,
        query_elapsed.as_secs_f64() * 1000.0,
        query_ops,
        avg_us
    );

    // --- 2-Hop Neighbor Queries ---
    let num_2hop = 200;
    let start = Instant::now();
    query_ok = 0;
    for i in 0..num_2hop {
        let idx = (i * 17) % num_nodes;
        let result: Result<Value, _> = redis::cmd("GRAPH.NEIGHBORS")
            .arg("{agent}:bench")
            .arg(node_ids[idx])
            .arg("DEPTH")
            .arg("2")
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            query_ok += 1;
        }
    }
    let query_2hop_elapsed = start.elapsed();
    let query_2hop_ops = query_ok as f64 / query_2hop_elapsed.as_secs_f64();
    let avg_2hop_us = query_2hop_elapsed.as_micros() as f64 / num_2hop as f64;
    println!(
        "  2-Hop Query:    {:>8} queries in {:>6.1}ms  ({:>8.0} qps, avg {:>6.0}µs)",
        query_ok,
        query_2hop_elapsed.as_secs_f64() * 1000.0,
        query_2hop_ops,
        avg_2hop_us
    );

    // --- Cypher Queries ---
    let num_cypher = 200;
    let start = Instant::now();
    query_ok = 0;
    for _ in 0..num_cypher {
        let result: Result<Value, _> = redis::cmd("GRAPH.QUERY")
            .arg("{agent}:bench")
            .arg("MATCH (n:Concept) RETURN n LIMIT 10")
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            query_ok += 1;
        }
    }
    let cypher_elapsed = start.elapsed();
    let cypher_ops = query_ok as f64 / cypher_elapsed.as_secs_f64();
    let avg_cypher_us = cypher_elapsed.as_micros() as f64 / num_cypher as f64;
    println!(
        "  Cypher Query:   {:>8} queries in {:>6.1}ms  ({:>8.0} qps, avg {:>6.0}µs)",
        query_ok,
        cypher_elapsed.as_secs_f64() * 1000.0,
        cypher_ops,
        avg_cypher_us
    );

    // --- Typed Edge Query ---
    let num_typed = 200;
    let start = Instant::now();
    query_ok = 0;
    for i in 0..num_typed {
        let idx = (i * 11) % num_nodes;
        let result: Result<Value, _> = redis::cmd("GRAPH.NEIGHBORS")
            .arg("{agent}:bench")
            .arg(node_ids[idx])
            .arg("TYPE")
            .arg("RELATED_TO")
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            query_ok += 1;
        }
    }
    let typed_elapsed = start.elapsed();
    let typed_ops = query_ok as f64 / typed_elapsed.as_secs_f64();
    let avg_typed_us = typed_elapsed.as_micros() as f64 / num_typed as f64;
    println!(
        "  Typed Query:    {:>8} queries in {:>6.1}ms  ({:>8.0} qps, avg {:>6.0}µs)",
        query_ok,
        typed_elapsed.as_secs_f64() * 1000.0,
        typed_ops,
        avg_typed_us
    );

    // --- KV Baseline (SET + GET) ---
    let num_kv = 1000;
    let start = Instant::now();
    for i in 0..num_kv {
        let _: () = redis::cmd("SET")
            .arg(format!("bench_key_{i}"))
            .arg(format!("value_{i}"))
            .query_async(&mut conn)
            .await
            .unwrap();
    }
    let set_elapsed = start.elapsed();
    let set_ops = num_kv as f64 / set_elapsed.as_secs_f64();

    let start = Instant::now();
    for i in 0..num_kv {
        let _: String = redis::cmd("GET")
            .arg(format!("bench_key_{i}"))
            .query_async(&mut conn)
            .await
            .unwrap();
    }
    let get_elapsed = start.elapsed();
    let get_ops = num_kv as f64 / get_elapsed.as_secs_f64();

    println!(
        "  KV SET:         {:>8} ops   in {:>6.1}ms  ({:>8.0} ops/s)",
        num_kv,
        set_elapsed.as_secs_f64() * 1000.0,
        set_ops
    );
    println!(
        "  KV GET:         {:>8} ops   in {:>6.1}ms  ({:>8.0} ops/s)",
        num_kv,
        get_elapsed.as_secs_f64() * 1000.0,
        get_ops
    );

    // --- Graph Info ---
    let info: Value = redis::cmd("GRAPH.INFO")
        .arg("{agent}:bench")
        .query_async(&mut conn)
        .await
        .unwrap();
    println!(
        "\n  GRAPH.INFO response type: {:?}",
        std::mem::discriminant(&info)
    );

    // --- Summary ---
    println!("\n============================================================");
    println!("  SUMMARY");
    println!("============================================================");
    println!("  Node Insert:  {:>8.0} ops/s", node_ops);
    println!("  Edge Insert:  {:>8.0} ops/s", edge_ops);
    println!(
        "  1-Hop Query:  {:>8.0} qps  (avg {:>6.0}µs)",
        query_ops, avg_us
    );
    println!(
        "  2-Hop Query:  {:>8.0} qps  (avg {:>6.0}µs)",
        query_2hop_ops, avg_2hop_us
    );
    println!(
        "  Cypher Query: {:>8.0} qps  (avg {:>6.0}µs)",
        cypher_ops, avg_cypher_us
    );
    println!(
        "  Typed Query:  {:>8.0} qps  (avg {:>6.0}µs)",
        typed_ops, avg_typed_us
    );
    println!("  KV SET:       {:>8.0} ops/s (baseline)", set_ops);
    println!("  KV GET:       {:>8.0} ops/s (baseline)", get_ops);
    println!("============================================================");
    println!("  Note: Single multiplexed connection, sequential.");
    println!("  Criterion micro-benchmarks (no TCP):");
    println!("    CSR 1-hop: 923ps | Edge insert: 44ns | Freeze 64K: 5.2ms");
    println!("============================================================\n");

    token.cancel();
}
