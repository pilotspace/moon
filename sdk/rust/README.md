# moondb

<p>
  <a href="https://crates.io/crates/moondb"><img src="https://img.shields.io/crates/v/moondb" alt="crates.io"></a>
  <a href="https://docs.rs/moondb"><img src="https://img.shields.io/docsrs/moondb" alt="docs.rs"></a>
  <a href="https://crates.io/crates/moondb"><img src="https://img.shields.io/crates/d/moondb" alt="downloads"></a>
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT">
  <img src="https://img.shields.io/badge/rust-1.85%2B-orange" alt="MSRV">
</p>

Async Rust client for [Moon](https://github.com/pilotspace/moon) — a high-performance Redis-compatible server with built-in vector search, graph engine, full-text search, semantic caching, workspaces, and message queues.

## Install

```toml
[dependencies]
moondb = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Quick Start

```rust
use moondb::{MoonClient, Result, VectorIndexOptions, DistanceMetric};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = MoonClient::connect("redis://127.0.0.1:6399").await?;

    // All standard Redis commands work
    client.set("hello", "world").await?;
    let val: String = client.get("hello").await?;
    println!("{val}"); // world

    // Vector search
    let mut vector = client.vector();
    vector.create_index(
        "products",
        VectorIndexOptions::new(384, DistanceMetric::Cosine)
            .prefix("product:")
            .m(16)
            .ef_construction(200),
    ).await?;

    // Graph engine
    let mut graph = client.graph();
    graph.create("social").await?;
    let alice = graph.add_node("social", "Person", &[("name", "Alice")]).await?;
    let bob   = graph.add_node("social", "Person", &[("name", "Bob")]).await?;
    graph.add_edge("social", alice, bob, "KNOWS", 1.0, &[]).await?;

    // Moon cross-store ACID transactions
    client.txn_begin().await?;
    client.set("counter", 42).await?;
    client.txn_commit().await?;

    Ok(())
}
```

## Features

### Vector Search (FT.*)

```rust
use moondb::{VectorIndexOptions, DistanceMetric};
use moondb::types::encode_vector;

let mut v = client.vector();

// Create HNSW index
v.create_index(
    "docs",
    VectorIndexOptions::new(768, DistanceMetric::Cosine)
        .prefix("doc:")
        .m(32)
        .ef_construction(400)
        .ef_runtime(100)
        .compact_threshold(10_000),
).await?;

// Ingest — store vector blob alongside payload fields
let blob = encode_vector(&query_vec);
client.hset_multiple("doc:1", &[
    ("vec", blob.as_slice()),
    ("title", b"Rust async patterns"),
]).await?;

// KNN search
let results = v.search("docs", &query_vec, 10).await?;
for r in &results {
    println!("{}: {:.4}", r.key, r.score);
}

// Index metadata
let info = v.index_info("docs").await?;
println!("docs: {} docs, {}-dim {}", info.num_docs, info.dim, info.metric);

// List all indexes
let names = v.list_indexes().await?;

// Compact (flush mutable → immutable HNSW segment)
v.compact("docs").await?;

// Drop with data
v.drop_index("docs", true).await?;
```

### Graph Engine (GRAPH.*)

```rust
use moondb::NeighborDirection;

let mut g = client.graph();
g.create("knowledge").await?;

let alice = g.add_node("knowledge", "Person", &[("name", "Alice"), ("age", "30")]).await?;
let topic = g.add_node("knowledge", "Topic",  &[("name", "Rust")]).await?;
g.add_edge("knowledge", alice, topic, "INTERESTED_IN", 0.9, &[]).await?;

// Cypher queries
let result = g.query(
    "knowledge",
    "MATCH (p:Person)-[:INTERESTED_IN]->(t:Topic) RETURN p.name, t.name",
).await?;
for row in &result.rows {
    println!("{:?}", row);
}

// Traverse neighbors
let neighbors = g.neighbors("knowledge", alice, NeighborDirection::Both).await?;

// Query plan (EXPLAIN)
let plan = g.explain("knowledge", "MATCH (n) RETURN n LIMIT 1").await?;

// Clean up
g.delete("knowledge").await?;
```

### Workspaces (WS.*)

```rust
let mut ws = client.workspace();

let id = ws.create("my-workspace").await?;
let list = ws.list().await?;
let info = ws.info(&id).await?;
ws.auth(&id).await?;    // bind connection to workspace
ws.drop(&id).await?;
```

### Message Queue (MQ.*)

```rust
let mut mq = client.mq();

mq.create("tasks", Some(100)).await?;   // capacity 100

let entry_id = mq.push("tasks", b"payload bytes").await?;

let msgs = mq.pop("tasks", 5).await?;   // pop up to 5
for msg in &msgs {
    println!("id={} body={:?}", msg.id, msg.data);
    mq.ack("tasks", &msg.id).await?;
}

let dlq_len = mq.dlq_len("tasks").await?;
```

### ACID Transactions (TXN.*)

```rust
client.txn_begin().await?;
client.set("a", "1").await?;
client.hset("user:1", "name", "Alice").await?;
// vector / graph commands also participate
client.txn_commit().await?;

// or abort
client.txn_rollback().await?;
```

### Full-Text & Hybrid Search (FT.* TEXT)

> Requires Moon built with `text-index` (default since v0.1.10).

```rust
use moondb::types::SchemaField;

let mut text = client.text();

// Create BM25 index (auto-indexes HSET writes)
text.create_text_index(
    "articles",
    "article:",
    &[SchemaField::text("title"), SchemaField::text("body")],
    None,
).await?;

// BM25 full-text search
let hits = text.text_search("articles", "rust async", Some(10)).await?;
for h in &hits {
    println!("[{:.3}] {} {:?}", h.score, h.key, h.fields);
}

// Facet aggregation
use moondb::types::{Reducer, AggregateRow};
let rows: Vec<AggregateRow> = text.aggregate(
    "articles",
    "*",
    &["category"],
    &[Reducer::Count { alias: "n".into() }],
    Some(("n", false)),  // sort by n DESC
    Some(10),
).await?;

// Hybrid BM25 + dense RRF
let blob = encode_vector(&query_vec);
let results = text.hybrid_search(
    "articles", "rust async",
    &blob, 10,
    (1.0, 1.5, 0.0),  // (bm25_weight, vec_weight, rrf_k)
).await?;
```

### Semantic Caching (FT.CACHESEARCH)

```rust
let mut cache = client.cache();

// Store a cache entry
cache.store(
    "cache:qa:abc123",
    &embedding,
    &[("response", "The answer is 42"), ("model", "gpt-4")],
    Some(3600),
).await?;

// Lookup
let result = cache.lookup("qa", "cache:qa:", &embedding, 0.1).await?;
if result.cache_hit {
    println!("Hit: {:?}", result.results[0].fields);
}

// Invalidate
cache.invalidate("cache:qa:abc123").await?;
cache.invalidate_prefix("cache:qa:").await?;
```

### Session-Aware Search

```rust
let mut session = client.session();

// Deduplicates results across calls
let r1 = session.search("products", "session:user1", &query_vec, 5).await?;
let r2 = session.search("products", "session:user1", &query_vec, 5).await?;
// r2 contains only results not seen in r1

let history = session.history("session:user1").await?;
session.reset("session:user1").await?;
```

## Sub-Client Reference

| Sub-client | Access | Commands |
|---|---|---|
| `VectorClient` | `client.vector()` | `FT.CREATE`, `FT._LIST`, `FT.INFO`, `FT.SEARCH`, `FT.COMPACT`, `FT.DROPINDEX` |
| `GraphClient` | `client.graph()` | `GRAPH.CREATE`, `ADDNODE`, `ADDEDGE`, `QUERY`, `RO_QUERY`, `NEIGHBORS`, `EXPLAIN`, `DELETE` |
| `WorkspaceClient` | `client.workspace()` | `WS.CREATE`, `WS.LIST`, `WS.INFO`, `WS.AUTH`, `WS.DROP` |
| `MqClient` | `client.mq()` | `MQ.CREATE`, `MQ.PUSH`, `MQ.POP`, `MQ.ACK`, `MQ.DLQLEN` |
| `TextClient` | `client.text()` | `FT.CREATE TEXT`, BM25 search, `FT.AGGREGATE`, hybrid RRF |
| `CacheClient` | `client.cache()` | `FT.CACHESEARCH`, store, invalidate, invalidate-prefix |
| `SessionClient` | `client.session()` | `FT.SEARCH SESSION`, history, reset |
| `TemporalClient` | `client.temporal()` | `TEMPORAL.*` entity timeline commands |

## Feature Flags

| Feature | Description |
|---|---|
| `tls-rustls` | TLS via rustls (pure Rust) |
| `tls-native-tls` | TLS via the platform native TLS stack |

```toml
# TLS with rustls
moondb = { version = "0.1", features = ["tls-rustls"] }
```

## Live Validation

Run the end-to-end validator against a live Moon server:

```bash
# Start Moon (text-index is default since v0.1.10)
cargo build --release && ./target/release/moon --port 6399 --shards 1

# From sdk/rust/
MOON_TEST_URL=redis://127.0.0.1:6399 cargo run --example validate
```

Expected output:

```
─── PING ───
  PASS  PING
  PASS  PING response

─── String commands ───
  PASS  SET
  PASS  GET
  ...

─── Server info ───
  PASS  INFO returned … bytes
  PASS  DBSIZE=…

All validation sections completed.
```

The validator covers 13 sections: PING, strings, counter, hash, list, set, sorted set, TXN, vector, graph, workspace, MQ, and server info — **85 PASS, 0 FAIL**.

## MSRV

Rust **1.85**, edition 2024.

## Development

```bash
# Build
cargo build

# Run examples
cargo run --example basic
cargo run --example vector_search
MOON_TEST_URL=redis://127.0.0.1:6399 cargo run --example validate

# Tests
cargo test

# Lint
cargo clippy -- -D warnings

# Type check
cargo check
```
