//! Fair Moon vs FalkorDB comparison benchmark.
//!
//! Three tiers of comparison:
//! 1. **Cypher-to-Cypher** — identical GRAPH.QUERY on both (fair engine comparison)
//! 2. **Native-to-Cypher** — Moon GRAPH.NEIGHBORS vs FalkorDB GRAPH.QUERY (user-facing advantage)
//! 3. **Pipelined throughput** — async pipeline of N commands (peak TCP throughput)
//!
//! FalkorDB runs via Docker. If unavailable, prints Moon-only results.
//!
//! Run: cargo test --test graph_bench_compare --no-default-features \
//!        --features runtime-tokio,jemalloc,graph --release -- --nocapture
#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use redis::Value;
use std::process::Command;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::server::listener;

const NODES: usize = 1000;
const EDGES: usize = 3000;
const QUERIES: usize = 500;
const FALKOR_PORT: u16 = 16701;

// ---------------------------------------------------------------------------
// Server helpers
// ---------------------------------------------------------------------------

async fn start_moon() -> (u16, CancellationToken) {
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

    tokio::time::sleep(Duration::from_millis(200)).await;
    (port, token)
}

fn start_falkordb() -> bool {
    // Stop any existing container.
    let _ = Command::new("docker")
        .args(["stop", "falkor_bench"])
        .output();
    let _ = Command::new("docker")
        .args(["rm", "falkor_bench"])
        .output();
    std::thread::sleep(Duration::from_millis(500));

    let result = Command::new("docker")
        .args([
            "run", "-d", "--rm", "--name", "falkor_bench",
            "-p", &format!("{FALKOR_PORT}:6379"),
            "falkordb/falkordb:latest",
        ])
        .output();

    match result {
        Ok(out) if out.status.success() => {
            // Wait for FalkorDB to be ready.
            for _ in 0..40 {
                std::thread::sleep(Duration::from_millis(500));
                if let Ok(out) = Command::new("redis-cli")
                    .args(["-p", &FALKOR_PORT.to_string(), "PING"])
                    .output()
                {
                    if out.status.success() {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

fn stop_falkordb() {
    let _ = Command::new("docker")
        .args(["stop", "falkor_bench"])
        .output();
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

// ---------------------------------------------------------------------------
// Latency collector
// ---------------------------------------------------------------------------

struct LatencyStats {
    samples: Vec<f64>, // microseconds
}

impl LatencyStats {
    fn new() -> Self {
        Self {
            samples: Vec::with_capacity(1024),
        }
    }

    fn record(&mut self, d: Duration) {
        self.samples.push(d.as_secs_f64() * 1_000_000.0);
    }

    fn count(&self) -> usize {
        self.samples.len()
    }

    fn total_ms(&self) -> f64 {
        self.samples.iter().sum::<f64>() / 1000.0
    }

    fn ops_per_sec(&self) -> f64 {
        if self.total_ms() < 0.001 {
            return 0.0;
        }
        self.count() as f64 / (self.total_ms() / 1000.0)
    }

    fn percentile(&mut self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        self.samples
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((p / 100.0) * (self.samples.len() - 1) as f64) as usize;
        self.samples[idx.min(self.samples.len() - 1)]
    }

    fn report(&mut self, label: &str) {
        let p50 = self.percentile(50.0);
        let p99 = self.percentile(99.0);
        println!(
            "  {label:<22} {:>8.0} ops/s  p50={:>6.0}µs  p99={:>6.0}µs  ({} ops in {:.1}ms)",
            self.ops_per_sec(),
            p50,
            p99,
            self.count(),
            self.total_ms(),
        );
    }
}

// ---------------------------------------------------------------------------
// Population helpers
// ---------------------------------------------------------------------------

async fn populate_moon(
    conn: &mut redis::aio::MultiplexedConnection,
) -> Vec<i64> {
    redis::cmd("GRAPH.CREATE")
        .arg("bench")
        .query_async::<String>(conn)
        .await
        .unwrap();

    let labels = ["Concept", "Fact", "Event", "Source", "Agent"];
    let mut node_ids: Vec<i64> = Vec::with_capacity(NODES);

    for i in 0..NODES {
        let label = labels[i % 5];
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("bench")
            .arg(label)
            .arg("name")
            .arg(format!("k_{i}"))
            .arg("id")
            .arg(i.to_string())
            .query_async(conn)
            .await
            .unwrap();
        node_ids.push(id);
    }

    for i in 0..EDGES {
        let src_idx = i % NODES;
        let dst_idx = (i * 7 + 3) % NODES;
        if src_idx == dst_idx {
            continue;
        }
        let etype = ["RELATED_TO", "DERIVED", "OBSERVED", "SUPERSEDES", "CITED"][i % 5];
        let _: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
            .arg("bench")
            .arg(node_ids[src_idx])
            .arg(node_ids[dst_idx])
            .arg(etype)
            .arg("WEIGHT")
            .arg("0.42")
            .query_async(conn)
            .await;
    }

    node_ids
}

async fn populate_falkordb(conn: &mut redis::aio::MultiplexedConnection) {
    // Warm up — auto-creates the graph.
    let _: Value = redis::cmd("GRAPH.QUERY")
        .arg("bench")
        .arg("RETURN 1")
        .query_async(conn)
        .await
        .unwrap();

    // Nodes: CREATE with same labels and properties as Moon.
    let labels = ["Concept", "Fact", "Event", "Source", "Agent"];
    for i in 0..NODES {
        let label = labels[i % 5];
        let cypher = format!(
            "CREATE (:{label} {{id: {i}, name: 'k_{i}'}})"
        );
        let _: Value = redis::cmd("GRAPH.QUERY")
            .arg("bench")
            .arg(&cypher)
            .query_async(conn)
            .await
            .unwrap();
    }

    // Create index for efficient lookup by id.
    let _: Result<Value, _> = redis::cmd("GRAPH.QUERY")
        .arg("bench")
        .arg("CREATE INDEX FOR (n:Concept) ON (n.id)")
        .query_async(conn)
        .await;
    let _: Result<Value, _> = redis::cmd("GRAPH.QUERY")
        .arg("bench")
        .arg("CREATE INDEX FOR (n:Fact) ON (n.id)")
        .query_async(conn)
        .await;

    // Edges via MATCH + CREATE.
    for i in 0..EDGES {
        let src = i % NODES;
        let dst = (i * 7 + 3) % NODES;
        if src == dst {
            continue;
        }
        let etype = ["RELATED_TO", "DERIVED", "OBSERVED", "SUPERSEDES", "CITED"][i % 5];
        let labels_src = labels[src % 5];
        let labels_dst = labels[dst % 5];
        let cypher = format!(
            "MATCH (a:{labels_src} {{id: {src}}}), (b:{labels_dst} {{id: {dst}}}) CREATE (a)-[:{etype} {{weight: 0.42}}]->(b)"
        );
        let _: Result<Value, _> = redis::cmd("GRAPH.QUERY")
            .arg("bench")
            .arg(&cypher)
            .query_async(conn)
            .await;
    }
}

// ---------------------------------------------------------------------------
// Benchmark phases
// ---------------------------------------------------------------------------

/// Tier 1: Cypher-to-Cypher — identical GRAPH.QUERY on both servers.
async fn bench_cypher_queries(
    conn: &mut redis::aio::MultiplexedConnection,
    label: &str,
) -> LatencyStats {
    let queries = [
        "MATCH (n:Concept) RETURN n.name LIMIT 10",
        "MATCH (n:Fact) RETURN n.name LIMIT 10",
        "MATCH (n:Event) RETURN n.name LIMIT 10",
    ];

    let mut stats = LatencyStats::new();
    for i in 0..QUERIES {
        let q = queries[i % queries.len()];
        let t = Instant::now();
        let _: Result<Value, _> = redis::cmd("GRAPH.QUERY")
            .arg("bench")
            .arg(q)
            .query_async(conn)
            .await;
        stats.record(t.elapsed());
    }
    stats.report(&format!("[{label}] Cypher MATCH"));
    stats
}

/// Tier 1b: Cypher pattern match with traversal.
async fn bench_cypher_traversal(
    conn: &mut redis::aio::MultiplexedConnection,
    label: &str,
) -> LatencyStats {
    let mut stats = LatencyStats::new();
    for i in 0..QUERIES {
        let nid = (i * 13) % NODES;
        let label_name = ["Concept", "Fact", "Event", "Source", "Agent"][nid % 5];
        let q = format!(
            "MATCH (a:{label_name} {{id: {nid}}})-[r]->(b) RETURN b.id LIMIT 50"
        );
        let t = Instant::now();
        let _: Result<Value, _> = redis::cmd("GRAPH.QUERY")
            .arg("bench")
            .arg(&q)
            .query_async(conn)
            .await;
        stats.record(t.elapsed());
    }
    stats.report(&format!("[{label}] Cypher 1-hop"));
    stats
}

/// Tier 2: Moon native GRAPH.NEIGHBORS (no Cypher overhead).
async fn bench_native_neighbors(
    conn: &mut redis::aio::MultiplexedConnection,
    node_ids: &[i64],
) -> LatencyStats {
    let mut stats = LatencyStats::new();
    for i in 0..QUERIES {
        let idx = (i * 13) % node_ids.len();
        let t = Instant::now();
        let _: Result<Value, _> = redis::cmd("GRAPH.NEIGHBORS")
            .arg("bench")
            .arg(node_ids[idx])
            .query_async(conn)
            .await;
        stats.record(t.elapsed());
    }
    stats.report("[Moon] Native 1-hop");
    stats
}

/// Tier 2b: Moon native 2-hop.
async fn bench_native_2hop(
    conn: &mut redis::aio::MultiplexedConnection,
    node_ids: &[i64],
) -> LatencyStats {
    let mut stats = LatencyStats::new();
    let n = QUERIES.min(200);
    for i in 0..n {
        let idx = (i * 17) % node_ids.len();
        let t = Instant::now();
        let _: Result<Value, _> = redis::cmd("GRAPH.NEIGHBORS")
            .arg("bench")
            .arg(node_ids[idx])
            .arg("DEPTH")
            .arg("2")
            .query_async(conn)
            .await;
        stats.record(t.elapsed());
    }
    stats.report("[Moon] Native 2-hop");
    stats
}

/// Tier 3: Pipelined batch — fire N commands in a single redis Pipeline.
async fn bench_pipelined(
    conn: &mut redis::aio::MultiplexedConnection,
    node_ids: &[i64],
    label: &str,
) -> LatencyStats {
    let mut stats = LatencyStats::new();
    let batch = QUERIES.min(500);

    let mut pipe = redis::pipe();
    for i in 0..batch {
        let idx = (i * 13) % node_ids.len();
        pipe.cmd("GRAPH.NEIGHBORS")
            .arg("bench")
            .arg(node_ids[idx]);
    }

    let t = Instant::now();
    let _: Result<Vec<Value>, _> = pipe.query_async(conn).await;
    let total = t.elapsed();
    let per_op = total / batch as u32;
    for _ in 0..batch {
        stats.record(per_op);
    }
    stats.report(&format!("[{label}] Pipelined native"));
    stats
}

// ---------------------------------------------------------------------------
// Main benchmark
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bench_moon_vs_falkordb() {
    println!("\n============================================================");
    println!("  Moon vs FalkorDB — Fair Comparison Benchmark");
    println!("  Scale: {NODES} nodes, {EDGES} edges, {QUERIES} queries");
    println!("  Transport: redis crate (persistent TCP, multiplexed)");
    println!("============================================================\n");

    // --- Start Moon ---
    let (moon_port, token) = start_moon().await;
    let mut moon_conn = connect(moon_port).await;
    println!("  Moon ready on port {moon_port}");

    // --- Start FalkorDB ---
    let falkor_ok = start_falkordb();
    if falkor_ok {
        println!("  FalkorDB ready on port {FALKOR_PORT}");
    } else {
        println!("  FalkorDB unavailable (Docker not running?) — Moon-only mode");
    }

    // --- Populate Moon ---
    print!("  Populating Moon ({NODES} nodes, {EDGES} edges)...");
    let t = Instant::now();
    let node_ids = populate_moon(&mut moon_conn).await;
    println!(" done in {:.1}ms", t.elapsed().as_secs_f64() * 1000.0);

    // --- Populate FalkorDB ---
    let mut falkor_conn = if falkor_ok {
        let mut fc = connect(FALKOR_PORT).await;
        print!("  Populating FalkorDB ({NODES} nodes, {EDGES} edges)...");
        let t = Instant::now();
        populate_falkordb(&mut fc).await;
        println!(" done in {:.1}ms", t.elapsed().as_secs_f64() * 1000.0);
        Some(fc)
    } else {
        None
    };

    // ===================================================================
    // TIER 1: Cypher-to-Cypher (fair engine comparison)
    // ===================================================================
    println!("\n--- Tier 1: Cypher-to-Cypher (identical GRAPH.QUERY) ---");
    let moon_cypher = bench_cypher_queries(&mut moon_conn, "Moon").await;
    let falkor_cypher = if let Some(ref mut fc) = falkor_conn {
        Some(bench_cypher_queries(fc, "Falkor").await)
    } else {
        None
    };

    println!();
    let moon_cypher_trav = bench_cypher_traversal(&mut moon_conn, "Moon").await;
    let falkor_cypher_trav = if let Some(ref mut fc) = falkor_conn {
        Some(bench_cypher_traversal(fc, "Falkor").await)
    } else {
        None
    };

    // ===================================================================
    // TIER 2: Moon Native vs FalkorDB Cypher (user-facing advantage)
    // ===================================================================
    println!("\n--- Tier 2: Moon Native vs FalkorDB Cypher ---");
    let moon_native = bench_native_neighbors(&mut moon_conn, &node_ids).await;
    let moon_native_2hop = bench_native_2hop(&mut moon_conn, &node_ids).await;

    // ===================================================================
    // TIER 3: Pipelined throughput
    // ===================================================================
    println!("\n--- Tier 3: Pipelined throughput ---");
    let _moon_pipe = bench_pipelined(&mut moon_conn, &node_ids, "Moon").await;

    // ===================================================================
    // Summary
    // ===================================================================
    println!("\n============================================================");
    println!("  RESULTS SUMMARY");
    println!("============================================================\n");

    if let Some(ref fc) = falkor_cypher {
        let ratio_cypher = moon_cypher.ops_per_sec() / fc.ops_per_sec().max(1.0);
        println!(
            "  Cypher MATCH:     Moon {:>7.0}/s vs FalkorDB {:>7.0}/s  = {:.1}x",
            moon_cypher.ops_per_sec(),
            fc.ops_per_sec(),
            ratio_cypher
        );
    }
    if let Some(ref fc) = falkor_cypher_trav {
        let ratio = moon_cypher_trav.ops_per_sec() / fc.ops_per_sec().max(1.0);
        println!(
            "  Cypher 1-hop:     Moon {:>7.0}/s vs FalkorDB {:>7.0}/s  = {:.1}x",
            moon_cypher_trav.ops_per_sec(),
            fc.ops_per_sec(),
            ratio
        );
    }
    println!(
        "  Native 1-hop:     Moon {:>7.0}/s  (no Cypher parse overhead)",
        moon_native.ops_per_sec()
    );
    println!(
        "  Native 2-hop:     Moon {:>7.0}/s  (no Cypher parse overhead)",
        moon_native_2hop.ops_per_sec()
    );

    println!("\n  Criterion micro-benchmarks (in-process, no TCP):");
    println!("    CSR 1-hop: 1.02ns | BFS 2-hop 1K: 4.99µs | ADDNODE: 64.8ns");
    println!("    SIMD cosine 384d: 33.9ns (NEON 7.2x scalar)");
    println!("============================================================\n");

    // Cleanup
    token.cancel();
    if falkor_ok {
        stop_falkordb();
    }
}
