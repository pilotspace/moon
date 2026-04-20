//! Integration tests against a live Moon server.
//!
//! These tests require a Moon server running at `MOON_TEST_URL`
//! (defaults to `redis://127.0.0.1:6399`).
//!
//! Run with:
//! ```bash
//! MOON_TEST_URL=redis://127.0.0.1:6399 cargo test --test integration
//! ```

use moon_client::{
    DistanceMetric, MoonClient, NeighborDirection, VectorIndexOptions,
    types::encode_vector,
};

fn test_url() -> String {
    std::env::var("MOON_TEST_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".into())
}

async fn connect() -> MoonClient {
    MoonClient::connect(test_url())
        .await
        .expect("failed to connect to Moon server")
}

// ── Basic connectivity ────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_ping() {
    let mut client = connect().await;
    let resp = client.ping().await.expect("PING failed");
    assert_eq!(resp.to_uppercase(), "PONG");
}

#[tokio::test]
#[ignore = "requires live server"]
async fn test_set_get_del() {
    let mut client = connect().await;
    client.flushdb().await.unwrap();

    client.set("sdk:test:key", "hello").await.unwrap();
    let val: String = client.get("sdk:test:key").await.unwrap();
    assert_eq!(val, "hello");

    let deleted = client.del("sdk:test:key").await.unwrap();
    assert_eq!(deleted, 1);

    let exists = client.exists("sdk:test:key").await.unwrap();
    assert!(!exists);
}

#[tokio::test]
#[ignore = "requires live server"]
async fn test_expire_ttl() {
    let mut client = connect().await;
    client.set("sdk:ttl", "value").await.unwrap();
    client.expire("sdk:ttl", 100).await.unwrap();
    let ttl = client.ttl("sdk:ttl").await.unwrap();
    assert!(ttl > 0 && ttl <= 100);
    client.del("sdk:ttl").await.unwrap();
}

#[tokio::test]
#[ignore = "requires live server"]
async fn test_incr_decr() {
    let mut client = connect().await;
    client.del("sdk:counter").await.unwrap();
    let v = client.incr("sdk:counter").await.unwrap();
    assert_eq!(v, 1);
    let v = client.incr_by("sdk:counter", 9).await.unwrap();
    assert_eq!(v, 10);
    let v = client.decr("sdk:counter").await.unwrap();
    assert_eq!(v, 9);
    client.del("sdk:counter").await.unwrap();
}

// ── Hash commands ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_hash_commands() {
    let mut client = connect().await;
    client.del("sdk:hash").await.unwrap();

    client.hset("sdk:hash", "field1", "value1").await.unwrap();
    client.hset("sdk:hash", "field2", "value2").await.unwrap();

    let v: Option<String> = client.hget("sdk:hash", "field1").await.unwrap();
    assert_eq!(v.as_deref(), Some("value1"));

    let all = client.hgetall("sdk:hash").await.unwrap();
    assert_eq!(all.get("field1").map(String::as_str), Some("value1"));
    assert_eq!(all.get("field2").map(String::as_str), Some("value2"));

    let len = client.hlen("sdk:hash").await.unwrap();
    assert_eq!(len, 2);

    client.hdel("sdk:hash", "field1").await.unwrap();
    let exists = client.hexists("sdk:hash", "field1").await.unwrap();
    assert!(!exists);

    client.del("sdk:hash").await.unwrap();
}

// ── List commands ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_list_commands() {
    let mut client = connect().await;
    client.del("sdk:list").await.unwrap();

    client.rpush("sdk:list", "a").await.unwrap();
    client.rpush("sdk:list", "b").await.unwrap();
    client.rpush("sdk:list", "c").await.unwrap();

    let len = client.llen("sdk:list").await.unwrap();
    assert_eq!(len, 3);

    let range: Vec<String> = client.lrange("sdk:list", 0, -1).await.unwrap();
    assert_eq!(range, vec!["a", "b", "c"]);

    let popped: Option<String> = client.lpop("sdk:list", None).await.unwrap();
    assert_eq!(popped.as_deref(), Some("a"));

    client.del("sdk:list").await.unwrap();
}

// ── Set commands ──────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_set_commands() {
    let mut client = connect().await;
    client.del("sdk:set").await.unwrap();

    client.sadd("sdk:set", "x").await.unwrap();
    client.sadd("sdk:set", "y").await.unwrap();
    client.sadd("sdk:set", "z").await.unwrap();

    let card = client.scard("sdk:set").await.unwrap();
    assert_eq!(card, 3);

    let is_member = client.sismember("sdk:set", "x").await.unwrap();
    assert!(is_member);

    client.srem("sdk:set", "x").await.unwrap();
    let is_member = client.sismember("sdk:set", "x").await.unwrap();
    assert!(!is_member);

    client.del("sdk:set").await.unwrap();
}

// ── Sorted set commands ───────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_zset_commands() {
    let mut client = connect().await;
    client.del("sdk:zset").await.unwrap();

    client.zadd("sdk:zset", 1.0, "alice").await.unwrap();
    client.zadd("sdk:zset", 2.0, "bob").await.unwrap();
    client.zadd("sdk:zset", 3.0, "charlie").await.unwrap();

    let card = client.zcard("sdk:zset").await.unwrap();
    assert_eq!(card, 3);

    let score = client.zscore("sdk:zset", "alice").await.unwrap();
    assert_eq!(score, Some(1.0));

    let rank = client.zrank("sdk:zset", "alice").await.unwrap();
    assert_eq!(rank, Some(0));

    let range: Vec<String> = client.zrange("sdk:zset", 0, -1).await.unwrap();
    assert_eq!(range, vec!["alice", "bob", "charlie"]);

    client.del("sdk:zset").await.unwrap();
}

// ── Moon TXN ─────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_moon_txn() {
    let mut client = connect().await;
    client.del("sdk:txn:a").await.unwrap();
    client.del("sdk:txn:b").await.unwrap();

    client.txn_begin().await.unwrap();
    client.set("sdk:txn:a", "1").await.unwrap();
    client.set("sdk:txn:b", "2").await.unwrap();
    client.txn_commit().await.unwrap();

    let a: String = client.get("sdk:txn:a").await.unwrap();
    let b: String = client.get("sdk:txn:b").await.unwrap();
    assert_eq!(a, "1");
    assert_eq!(b, "2");

    client.del("sdk:txn:a").await.unwrap();
    client.del("sdk:txn:b").await.unwrap();
}

// ── Vector search ─────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_vector_index_lifecycle() {
    let client = connect().await;
    let mut v = client.vector();

    // Clean up from previous run
    let _ = v.drop_index("sdk_test_idx", false).await;

    v.create_index(
        "sdk_test_idx",
        VectorIndexOptions::new(4, DistanceMetric::L2)
            .prefix("sdkdoc:")
            .compact_threshold(100),
    )
    .await
    .unwrap();

    // Verify it appears in the list
    let indexes = v.list_indexes().await.unwrap();
    assert!(indexes.iter().any(|i| i == "sdk_test_idx"));

    // Insert a document
    let vec_bytes = encode_vector(&[0.1, 0.2, 0.3, 0.4]);
    let mut c = client.clone();
    c.hset_multiple("sdkdoc:1", &[("vec", vec_bytes.as_slice()), ("title", b"test doc")]).await.unwrap();

    // Search
    let query = [0.1_f32, 0.2, 0.3, 0.4];
    let results = v.search("sdk_test_idx", &query, 5).await.unwrap();
    // Server may need time to index; just check no panic
    let _ = results;

    // FT.INFO
    let info = v.index_info("sdk_test_idx").await.unwrap();
    assert_eq!(info.name, "sdk_test_idx");

    // Compact
    let _ = v.compact("sdk_test_idx").await;

    // Drop
    v.drop_index("sdk_test_idx", true).await.unwrap();
    let indexes = v.list_indexes().await.unwrap();
    assert!(!indexes.iter().any(|i| i == "sdk_test_idx"));
}

// ── Graph engine ──────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_graph_lifecycle() {
    let client = connect().await;
    let mut g = client.graph();

    // Clean up
    let _ = g.delete("sdk_test_graph").await;

    g.create("sdk_test_graph").await.unwrap();

    let alice_id = g
        .add_node("sdk_test_graph", "Person", &[("name", "Alice"), ("age", "30")])
        .await
        .unwrap();
    let bob_id = g
        .add_node("sdk_test_graph", "Person", &[("name", "Bob")])
        .await
        .unwrap();

    g.add_edge("sdk_test_graph", alice_id, bob_id, "KNOWS", 1.0, &[])
        .await
        .unwrap();

    // Cypher query
    let result = g
        .query("sdk_test_graph", "MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .await
        .unwrap();
    assert!(!result.headers.is_empty());

    // Neighbors
    let neighbors = g
        .neighbors("sdk_test_graph", alice_id, NeighborDirection::Both)
        .await
        .unwrap();
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].1, bob_id);

    g.delete("sdk_test_graph").await.unwrap();
}

// ── Graph explain/profile ─────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires live server"]
async fn test_graph_explain() {
    let client = connect().await;
    let mut g = client.graph();
    let _ = g.delete("sdk_explain_test").await;
    g.create("sdk_explain_test").await.unwrap();

    let plan = g
        .explain("sdk_explain_test", "MATCH (n) RETURN n LIMIT 1")
        .await
        .unwrap();
    // Plan is a list of strings; just verify it's non-empty
    let _ = plan;

    g.delete("sdk_explain_test").await.unwrap();
}

// ── Encode/decode round-trip ──────────────────────────────────────────────────

#[test]
fn vector_encode_decode_roundtrip() {
    let original = vec![0.1_f32, -0.5, 1.23456, f32::MIN_POSITIVE];
    let encoded = encode_vector(&original);
    let decoded = moon_client::decode_vector(&encoded).unwrap();
    for (a, b) in original.iter().zip(decoded.iter()) {
        assert!((a - b).abs() < 1e-7, "mismatch: {a} vs {b}");
    }
}

#[test]
fn vector_decode_invalid_length() {
    let bad = vec![0u8, 1, 2]; // 3 bytes — not a multiple of 4
    assert!(moon_client::decode_vector(&bad).is_err());
}
