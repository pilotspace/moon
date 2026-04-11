//! Integration tests for the graph engine.
//!
//! Exercises all GRAPH.* commands over real TCP connections.
//! Each test spawns a Moon server, connects via the redis crate, and validates
//! the full command flow: create → insert nodes → insert edges → query → delete.
//!
//! Run: cargo test --test graph_integration --no-default-features --features runtime-tokio,jemalloc,graph
#![cfg(all(feature = "runtime-tokio", feature = "graph"))]

use moon::runtime::cancel::CancellationToken;
use redis::Value;
use tokio::net::TcpListener;

use moon::config::ServerConfig;
use moon::server::listener;

/// Start a Moon server with graph feature on a random port.
async fn start_graph_server() -> (u16, CancellationToken) {
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
// Use Case 1: AI Agent Knowledge Graph — Full CRUD Lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_create_and_list() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    // Create a graph
    let result: String = redis::cmd("GRAPH.CREATE")
        .arg("knowledge")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // List graphs
    let graphs: Vec<String> = redis::cmd("GRAPH.LIST")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(graphs, vec!["knowledge"]);

    // Duplicate create fails
    let result: Result<String, _> = redis::cmd("GRAPH.CREATE")
        .arg("knowledge")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err());

    token.cancel();
}

#[tokio::test]
async fn test_graph_addnode_returns_id() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Add a node with label and properties
    let node_id: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Concept")
        .arg("name")
        .arg("Quantum Computing")
        .arg("confidence")
        .arg("0.95")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Node ID should be a positive integer (SlotMap key)
    assert!(node_id > 0, "expected positive node ID, got {node_id}");

    token.cancel();
}

#[tokio::test]
async fn test_graph_addedge_and_neighbors() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Create two nodes
    let alice: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Person")
        .arg("name")
        .arg("Alice")
        .query_async(&mut conn)
        .await
        .unwrap();

    let bob: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Person")
        .arg("name")
        .arg("Bob")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Create edge with weight
    let edge_id: i64 = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(alice)
        .arg(bob)
        .arg("KNOWS")
        .arg("WEIGHT")
        .arg("0.95")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(edge_id > 0);

    // Query neighbors of Alice
    let neighbors: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("g")
        .arg(alice)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Should return an array with edge + node entries
    match &neighbors {
        Value::Array(items) => {
            assert!(!items.is_empty(), "expected neighbors, got empty array");
        }
        other => panic!("expected Array, got {other:?}"),
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 2: Knowledge Provenance Chain — Multi-Hop Traversal
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_multi_hop_traversal() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("provenance")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Build chain: Source → Fact → Concept → Insight
    let source: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("provenance")
        .arg("Source")
        .arg("name")
        .arg("Wikipedia")
        .query_async(&mut conn)
        .await
        .unwrap();

    let fact: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("provenance")
        .arg("Fact")
        .arg("name")
        .arg("Electrons have spin")
        .query_async(&mut conn)
        .await
        .unwrap();

    let concept: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("provenance")
        .arg("Concept")
        .arg("name")
        .arg("Quantum Spin")
        .query_async(&mut conn)
        .await
        .unwrap();

    let insight: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("provenance")
        .arg("Concept")
        .arg("name")
        .arg("Spintronics")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Wire the provenance chain
    for (src, dst) in [(source, fact), (fact, concept), (concept, insight)] {
        redis::cmd("GRAPH.ADDEDGE")
            .arg("provenance")
            .arg(src)
            .arg(dst)
            .arg("DERIVED_FROM")
            .arg("WEIGHT")
            .arg("0.1")
            .query_async::<i64>(&mut conn)
            .await
            .unwrap();
    }

    // 1-hop from source: should find fact only
    let depth1: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("provenance")
        .arg(source)
        .arg("DEPTH")
        .arg("1")
        .query_async(&mut conn)
        .await
        .unwrap();

    if let Value::Array(items) = &depth1 {
        assert_eq!(items.len(), 2, "1-hop should return edge + node (2 items)");
    }

    // 3-hop from source: should find fact + concept + insight
    let depth3: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("provenance")
        .arg(source)
        .arg("DEPTH")
        .arg("3")
        .query_async(&mut conn)
        .await
        .unwrap();

    if let Value::Array(items) = &depth3 {
        assert_eq!(
            items.len(),
            6,
            "3-hop chain should return 3 edges + 3 nodes (6 items), got {}",
            items.len()
        );
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 3: Typed Edge Filtering — "Show only RELATED_TO relationships"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_typed_edge_filter() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    let a: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Concept")
        .query_async(&mut conn)
        .await
        .unwrap();
    let b: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Concept")
        .query_async(&mut conn)
        .await
        .unwrap();
    let c: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("Source")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Edge a→b as RELATED_TO
    redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(a)
        .arg(b)
        .arg("RELATED_TO")
        .query_async::<i64>(&mut conn)
        .await
        .unwrap();

    // Edge a→c as DERIVED_FROM (different type)
    redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(a)
        .arg(c)
        .arg("DERIVED_FROM")
        .query_async::<i64>(&mut conn)
        .await
        .unwrap();

    // Without filter: should get both neighbors
    let all: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("g")
        .arg(a)
        .query_async(&mut conn)
        .await
        .unwrap();

    if let Value::Array(items) = &all {
        assert_eq!(items.len(), 4, "unfiltered should return 2 edges + 2 nodes");
    }

    // With TYPE filter: should only get RELATED_TO
    let filtered: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("g")
        .arg(a)
        .arg("TYPE")
        .arg("RELATED_TO")
        .query_async(&mut conn)
        .await
        .unwrap();

    if let Value::Array(items) = &filtered {
        assert_eq!(items.len(), 2, "TYPE filter should return 1 edge + 1 node");
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 4: Graph Info — Statistics and Monitoring
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_info_returns_stats() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("stats_test")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Add 5 nodes and 4 edges
    let mut ids = Vec::new();
    for i in 0..5 {
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("stats_test")
            .arg("Node")
            .arg("idx")
            .arg(i.to_string())
            .query_async(&mut conn)
            .await
            .unwrap();
        ids.push(id);
    }

    for i in 0..4 {
        redis::cmd("GRAPH.ADDEDGE")
            .arg("stats_test")
            .arg(ids[i])
            .arg(ids[i + 1])
            .arg("NEXT")
            .query_async::<i64>(&mut conn)
            .await
            .unwrap();
    }

    // GRAPH.INFO should return a map with node_count=5, edge_count=4
    let info: Value = redis::cmd("GRAPH.INFO")
        .arg("stats_test")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Verify it's a structured response (Map or Array of pairs)
    match &info {
        Value::Map(pairs) => {
            assert!(!pairs.is_empty(), "INFO should return non-empty map");
        }
        Value::Array(items) => {
            assert!(!items.is_empty(), "INFO should return non-empty response");
        }
        other => panic!("expected Map or Array from GRAPH.INFO, got {other:?}"),
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 5: Cypher Query — "MATCH (n) RETURN n"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_cypher_query() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("cypher_test")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    redis::cmd("GRAPH.ADDNODE")
        .arg("cypher_test")
        .arg("Person")
        .arg("name")
        .arg("Alice")
        .query_async::<i64>(&mut conn)
        .await
        .unwrap();

    redis::cmd("GRAPH.ADDNODE")
        .arg("cypher_test")
        .arg("Person")
        .arg("name")
        .arg("Bob")
        .query_async::<i64>(&mut conn)
        .await
        .unwrap();

    // Execute Cypher query
    let result: Value = redis::cmd("GRAPH.QUERY")
        .arg("cypher_test")
        .arg("MATCH (n:Person) RETURN n LIMIT 10")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Should return an array (query results)
    match &result {
        Value::Array(items) => {
            assert!(
                !items.is_empty(),
                "Cypher query should return results for 2 Person nodes"
            );
        }
        other => panic!("expected Array from GRAPH.QUERY, got {other:?}"),
    }

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 6: Read-Only Query Rejects Writes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_ro_query_rejects_writes() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("ro_test")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // RO_QUERY with CREATE clause should fail
    let result: Result<Value, _> = redis::cmd("GRAPH.RO_QUERY")
        .arg("ro_test")
        .arg("CREATE (n:Person)")
        .query_async(&mut conn)
        .await;

    assert!(result.is_err(), "RO_QUERY should reject write clauses");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 7: Graph Explain — Query Plan Inspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_explain() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    // EXPLAIN doesn't require the graph to exist (parses query only)
    let plan: String = redis::cmd("GRAPH.EXPLAIN")
        .arg("any_graph")
        .arg("MATCH (n:Person)-[:KNOWS]->(m) RETURN n, m LIMIT 5")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        !plan.is_empty(),
        "EXPLAIN should return a non-empty plan string"
    );

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 8: Graph Delete — Cleanup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_delete() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("temp")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    redis::cmd("GRAPH.ADDNODE")
        .arg("temp")
        .arg("Data")
        .query_async::<i64>(&mut conn)
        .await
        .unwrap();

    // Delete
    let result: String = redis::cmd("GRAPH.DELETE")
        .arg("temp")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // List should be empty
    let graphs: Vec<String> = redis::cmd("GRAPH.LIST")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(graphs.is_empty(), "after delete, GRAPH.LIST should be empty");

    // INFO on deleted graph should error
    let result: Result<Value, _> = redis::cmd("GRAPH.INFO")
        .arg("temp")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err(), "INFO on deleted graph should error");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 9: Self-Loop Rejection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_rejects_self_loop() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    let n: i64 = redis::cmd("GRAPH.ADDNODE")
        .arg("g")
        .arg("X")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Self-loop should be rejected
    let result: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
        .arg("g")
        .arg(n)
        .arg(n)
        .arg("LOOPS")
        .query_async(&mut conn)
        .await;

    assert!(result.is_err(), "self-loop edges should be rejected");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 10: KV Commands Still Work With Graph Feature
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_kv_commands_unaffected() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    // Standard KV operations must still work
    let _: () = redis::cmd("SET")
        .arg("key1")
        .arg("value1")
        .query_async(&mut conn)
        .await
        .unwrap();

    let val: String = redis::cmd("GET")
        .arg("key1")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(val, "value1");

    // Graph and KV coexist
    redis::cmd("GRAPH.CREATE")
        .arg("g")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    let val2: String = redis::cmd("GET")
        .arg("key1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val2, "value1", "KV must work after graph create");

    token.cancel();
}

// ---------------------------------------------------------------------------
// Use Case 11: AI Agent Knowledge Graph — Bulk Insert + Query (Realistic Scale)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graph_bulk_knowledge_graph() {
    let (port, token) = start_graph_server().await;
    let mut conn = connect(port).await;

    redis::cmd("GRAPH.CREATE")
        .arg("{agent-1}:knowledge")
        .query_async::<String>(&mut conn)
        .await
        .unwrap();

    // Insert 100 knowledge nodes
    let mut node_ids = Vec::with_capacity(100);
    let labels = ["Concept", "Fact", "Event", "Source", "Agent"];
    for i in 0..100 {
        let label = labels[i % 5];
        let id: i64 = redis::cmd("GRAPH.ADDNODE")
            .arg("{agent-1}:knowledge")
            .arg(label)
            .arg("name")
            .arg(format!("knowledge_{i}"))
            .query_async(&mut conn)
            .await
            .unwrap();
        node_ids.push(id);
    }

    // Insert 200 edges (random connections, avoid self-loops)
    let edge_types = ["RELATED_TO", "DERIVED_FROM", "OBSERVED_AT", "SUPERSEDES", "CITED_BY"];
    let mut edge_count = 0;
    for i in 0..200 {
        let src_idx = i % 100;
        let dst_idx = (i * 7 + 3) % 100; // deterministic pseudo-random
        if src_idx == dst_idx {
            continue;
        }
        let etype = edge_types[i % 5];
        let result: Result<i64, _> = redis::cmd("GRAPH.ADDEDGE")
            .arg("{agent-1}:knowledge")
            .arg(node_ids[src_idx])
            .arg(node_ids[dst_idx])
            .arg(etype)
            .arg("WEIGHT")
            .arg("0.5")
            .query_async(&mut conn)
            .await;
        if result.is_ok() {
            edge_count += 1;
        }
    }

    assert!(
        edge_count > 100,
        "should insert at least 100 edges, got {edge_count}"
    );

    // Query neighbors of first node (2-hop)
    let neighbors: Value = redis::cmd("GRAPH.NEIGHBORS")
        .arg("{agent-1}:knowledge")
        .arg(node_ids[0])
        .arg("DEPTH")
        .arg("2")
        .query_async(&mut conn)
        .await
        .unwrap();

    match &neighbors {
        Value::Array(items) => {
            assert!(
                !items.is_empty(),
                "2-hop from node 0 in 100-node graph should find neighbors"
            );
        }
        _ => panic!("expected Array"),
    }

    // Cypher query
    let result: Value = redis::cmd("GRAPH.QUERY")
        .arg("{agent-1}:knowledge")
        .arg("MATCH (n:Concept) RETURN n LIMIT 5")
        .query_async(&mut conn)
        .await
        .unwrap();

    match &result {
        Value::Array(items) => {
            assert!(
                !items.is_empty(),
                "Cypher should find Concept nodes"
            );
        }
        _ => panic!("expected Array from Cypher"),
    }

    token.cancel();
}
