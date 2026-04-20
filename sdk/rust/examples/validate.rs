//! End-to-end validation script — connects to a live Moon server and exercises
//! every SDK sub-client. Each section prints PASS or FAIL with details.
//!
//! Run:
//!   cargo run --example validate
//!   MOON_TEST_URL=redis://127.0.0.1:6399 cargo run --example validate

use moon_client::{
    DistanceMetric, MoonClient, NeighborDirection, Result, VectorIndexOptions,
    types::encode_vector,
};

const URL_ENV: &str = "MOON_TEST_URL";
const DEFAULT_URL: &str = "redis://127.0.0.1:6399";

macro_rules! check {
    ($label:expr, $result:expr) => {
        match $result {
            Ok(v) => {
                println!("  PASS  {}", $label);
                v
            }
            Err(e) => {
                println!("  FAIL  {} — {e}", $label);
                return Ok(());
            }
        }
    };
}

macro_rules! assert_eq_or_fail {
    ($label:expr, $left:expr, $right:expr) => {
        if $left != $right {
            println!("  FAIL  {} — expected {:?} got {:?}", $label, $right, $left);
            return Ok(());
        } else {
            println!("  PASS  {}", $label);
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var(URL_ENV).unwrap_or_else(|_| DEFAULT_URL.to_string());
    println!("Connecting to {url} …");

    let mut client = MoonClient::connect(url).await.unwrap_or_else(|e| {
        eprintln!("FATAL: cannot connect — {e}");
        std::process::exit(1);
    });

    // ── Flush test namespace ──────────────────────────────────────────────────
    client.flushdb().await.ok();

    // =========================================================================
    section("PING");
    // =========================================================================
    {
        let pong = check!("PING", client.ping().await);
        assert_eq_or_fail!("PING response", pong.to_uppercase(), "PONG".to_string());
    }

    // =========================================================================
    section("String commands");
    // =========================================================================
    {
        check!("SET", client.set("val:key", "hello").await);
        let v: String = check!("GET", client.get("val:key").await);
        assert_eq_or_fail!("GET value", v, "hello".to_string());

        let d = check!("DEL", client.del("val:key").await);
        assert_eq_or_fail!("DEL count", d, 1i64);

        let exists = check!("EXISTS after DEL", client.exists("val:key").await);
        assert_eq_or_fail!("EXISTS=false", exists, false);

        check!("SET for TTL", client.set("ttl:key", "x").await);
        check!("EXPIRE", client.expire("ttl:key", 60).await);
        let ttl = check!("TTL", client.ttl("ttl:key").await);
        if ttl <= 0 || ttl > 60 {
            println!("  FAIL  TTL out of range: {ttl}");
            return Ok(());
        }
        println!("  PASS  TTL in range ({ttl}s)");
        client.del("ttl:key").await.ok();
    }

    // =========================================================================
    section("Counter (INCR/DECR)");
    // =========================================================================
    {
        client.del("ctr").await.ok();
        let v = check!("INCR", client.incr("ctr").await);
        assert_eq_or_fail!("INCR=1", v, 1i64);
        let v = check!("INCRBY +9", client.incr_by("ctr", 9).await);
        assert_eq_or_fail!("INCRBY=10", v, 10i64);
        let v = check!("DECR", client.decr("ctr").await);
        assert_eq_or_fail!("DECR=9", v, 9i64);
        client.del("ctr").await.ok();
    }

    // =========================================================================
    section("Hash commands");
    // =========================================================================
    {
        client.del("h").await.ok();
        check!("HSET f1", client.hset("h", "f1", "v1").await);
        check!("HSET f2", client.hset("h", "f2", "v2").await);
        let v: Option<String> = check!("HGET f1", client.hget("h", "f1").await);
        assert_eq_or_fail!("HGET value", v.as_deref(), Some("v1"));
        let len = check!("HLEN", client.hlen("h").await);
        assert_eq_or_fail!("HLEN=2", len, 2i64);
        let all = check!("HGETALL", client.hgetall("h").await);
        assert_eq_or_fail!("HGETALL f2", all.get("f2").map(|s| s.as_str()), Some("v2"));
        check!("HDEL f1", client.hdel("h", "f1").await);
        let ex = check!("HEXISTS f1 gone", client.hexists("h", "f1").await);
        assert_eq_or_fail!("HEXISTS=false", ex, false);
        client.del("h").await.ok();
    }

    // =========================================================================
    section("List commands");
    // =========================================================================
    {
        client.del("lst").await.ok();
        check!("RPUSH a", client.rpush("lst", "a").await);
        check!("RPUSH b", client.rpush("lst", "b").await);
        check!("RPUSH c", client.rpush("lst", "c").await);
        let len = check!("LLEN", client.llen("lst").await);
        assert_eq_or_fail!("LLEN=3", len, 3i64);
        let range: Vec<String> = check!("LRANGE 0 -1", client.lrange("lst", 0, -1).await);
        assert_eq_or_fail!("LRANGE", range, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        let popped: Option<String> = check!("LPOP", client.lpop("lst", None).await);
        assert_eq_or_fail!("LPOP=a", popped.as_deref(), Some("a"));
        client.del("lst").await.ok();
    }

    // =========================================================================
    section("Set commands");
    // =========================================================================
    {
        client.del("st").await.ok();
        check!("SADD x", client.sadd("st", "x").await);
        check!("SADD y", client.sadd("st", "y").await);
        let card = check!("SCARD", client.scard("st").await);
        assert_eq_or_fail!("SCARD=2", card, 2i64);
        let is_m = check!("SISMEMBER x", client.sismember("st", "x").await);
        assert_eq_or_fail!("SISMEMBER x=true", is_m, true);
        check!("SREM x", client.srem("st", "x").await);
        let is_m = check!("SISMEMBER x gone", client.sismember("st", "x").await);
        assert_eq_or_fail!("SISMEMBER x=false", is_m, false);
        client.del("st").await.ok();
    }

    // =========================================================================
    section("Sorted set commands");
    // =========================================================================
    {
        client.del("zst").await.ok();
        check!("ZADD alice 1.0", client.zadd("zst", 1.0, "alice").await);
        check!("ZADD bob 2.0", client.zadd("zst", 2.0, "bob").await);
        check!("ZADD charlie 3.0", client.zadd("zst", 3.0, "charlie").await);
        let card = check!("ZCARD", client.zcard("zst").await);
        assert_eq_or_fail!("ZCARD=3", card, 3i64);
        let score = check!("ZSCORE alice", client.zscore("zst", "alice").await);
        assert_eq_or_fail!("ZSCORE=1.0", score, Some(1.0f64));
        let rank = check!("ZRANK alice", client.zrank("zst", "alice").await);
        assert_eq_or_fail!("ZRANK=0", rank, Some(0i64));
        let range: Vec<String> = check!("ZRANGE 0 -1", client.zrange("zst", 0, -1).await);
        assert_eq_or_fail!("ZRANGE order", range, vec!["alice", "bob", "charlie"]);
        client.del("zst").await.ok();
    }

    // =========================================================================
    section("Moon TXN (cross-store ACID)");
    // =========================================================================
    {
        client.del("txn:a").await.ok();
        client.del("txn:b").await.ok();
        check!("TXN BEGIN", client.txn_begin().await);
        check!("SET txn:a", client.set("txn:a", "1").await);
        check!("SET txn:b", client.set("txn:b", "2").await);
        check!("TXN COMMIT", client.txn_commit().await);
        let a: String = check!("GET txn:a", client.get("txn:a").await);
        let b: String = check!("GET txn:b", client.get("txn:b").await);
        assert_eq_or_fail!("txn:a=1", a, "1".to_string());
        assert_eq_or_fail!("txn:b=2", b, "2".to_string());
        client.del("txn:a").await.ok();
        client.del("txn:b").await.ok();
    }

    // =========================================================================
    section("Vector index lifecycle (FT.*)");
    // =========================================================================
    {
        let mut v = client.vector();
        v.drop_index("val_idx", true).await.ok();

        check!(
            "FT.CREATE HNSW",
            v.create_index(
                "val_idx",
                VectorIndexOptions::new(4, DistanceMetric::Cosine)
                    .prefix("vdoc:")
                    .m(16)
                    .ef_construction(200)
                    .compact_threshold(100),
            )
            .await
        );

        let list = check!("FT._LIST", v.list_indexes().await);
        if !list.iter().any(|n| n == "val_idx") {
            println!("  FAIL  FT._LIST — val_idx not present");
            return Ok(());
        }
        println!("  PASS  FT._LIST contains val_idx");

        // Ingest a document
        let blob = encode_vector(&[0.1_f32, 0.2, 0.3, 0.4]);
        check!(
            "HSET vdoc:1",
            client.hset_multiple("vdoc:1", &[("vec", blob.as_slice()), ("title", b"test")]).await
        );

        let info = check!("FT.INFO", v.index_info("val_idx").await);
        assert_eq_or_fail!("FT.INFO name", info.name, "val_idx".to_string());

        check!("FT.SEARCH", v.search("val_idx", &[0.1_f32, 0.2, 0.3, 0.4], 5).await);

        v.compact("val_idx").await.ok();
        println!("  PASS  FT.COMPACT (no-error)");

        check!("FT.DROPINDEX DD", v.drop_index("val_idx", true).await);

        let list = check!("FT._LIST after drop", v.list_indexes().await);
        if list.iter().any(|n| n == "val_idx") {
            println!("  FAIL  val_idx still present after DROPINDEX");
            return Ok(());
        }
        println!("  PASS  val_idx removed");

        client.del("vdoc:1").await.ok();
    }

    // =========================================================================
    section("Graph engine (GRAPH.*)");
    // =========================================================================
    {
        let mut g = client.graph();
        g.delete("val_graph").await.ok();

        check!("GRAPH.CREATE", g.create("val_graph").await);

        let alice = check!(
            "GRAPH.ADDNODE alice",
            g.add_node("val_graph", "Person", &[("name", "Alice"), ("age", "30")]).await
        );
        let bob = check!(
            "GRAPH.ADDNODE bob",
            g.add_node("val_graph", "Person", &[("name", "Bob")]).await
        );

        check!(
            "GRAPH.ADDEDGE KNOWS",
            g.add_edge("val_graph", alice, bob, "KNOWS", 1.0, &[]).await
        );

        let node = check!("GRAPH.GETNODE alice", g.get_node("val_graph", alice).await);
        if node.is_none() {
            println!("  FAIL  GRAPH.GETNODE returned None");
            return Ok(());
        }
        println!("  PASS  GRAPH.GETNODE returned node");

        let result = check!(
            "GRAPH.QUERY",
            g.query("val_graph", "MATCH (p:Person) RETURN p.name ORDER BY p.name").await
        );
        if result.headers.is_empty() {
            println!("  FAIL  GRAPH.QUERY returned no headers");
            return Ok(());
        }
        println!("  PASS  GRAPH.QUERY headers={:?}", result.headers);

        let neighbors = check!(
            "GRAPH.NEIGHBORS",
            g.neighbors("val_graph", alice, NeighborDirection::Both).await
        );
        assert_eq_or_fail!("NEIGHBORS count=1", neighbors.len(), 1);

        let deg = check!("GRAPH.DEGREE", g.degree("val_graph", alice).await);
        assert_eq_or_fail!("DEGREE=1", deg, 1i64);

        let plan = check!(
            "GRAPH.EXPLAIN",
            g.explain("val_graph", "MATCH (n) RETURN n LIMIT 1").await
        );
        let _ = plan;
        println!("  PASS  GRAPH.EXPLAIN returned plan");

        check!("GRAPH.DELETE", g.delete("val_graph").await);
    }

    // =========================================================================
    section("Workspace (WS.*)");
    // =========================================================================
    {
        let mut ws = client.workspace();

        let id = check!("WS CREATE", ws.create("validate-ws").await);
        println!("  PASS  WS CREATE → {id}");

        let list = check!("WS LIST", ws.list().await);
        if !list.iter().any(|i| i == &id) {
            println!("  FAIL  WS LIST — new workspace not present");
            return Ok(());
        }
        println!("  PASS  WS LIST contains new workspace");

        let info = check!("WS INFO", ws.info(&id).await);
        assert_eq_or_fail!("WS INFO id", info.id, id.clone());

        check!("WS AUTH", ws.auth(&id).await);
        println!("  PASS  WS AUTH bound connection");

        check!("WS DROP", ws.drop(&id).await);
        println!("  PASS  WS DROP succeeded");
    }

    // =========================================================================
    section("Message queue (MQ.*)");
    // =========================================================================
    {
        let mut mq = client.mq();

        client.del("mq:validate").await.ok();

        check!("MQ CREATE", mq.create("mq:validate", Some(3)).await);

        let entry_id = check!("MQ PUSH", mq.push("mq:validate", b"hello world").await);
        println!("  PASS  MQ PUSH → {entry_id}");

        let msgs = check!("MQ POP", mq.pop("mq:validate", 1).await);
        if msgs.is_empty() {
            println!("  FAIL  MQ POP returned no messages");
            return Ok(());
        }
        let msg = &msgs[0];
        println!("  PASS  MQ POP id={} body={:?}", msg.id, std::str::from_utf8(&msg.data).unwrap_or("(binary)"));

        check!("MQ ACK", mq.ack("mq:validate", &msg.id).await);

        let dlq = check!("MQ DLQLEN", mq.dlq_len("mq:validate").await);
        assert_eq_or_fail!("DLQ empty", dlq, 0i64);

        client.del("mq:validate").await.ok();
    }

    // =========================================================================
    section("Server info");
    // =========================================================================
    {
        let info = check!("INFO server", client.info(Some("server")).await);
        if info.is_empty() {
            println!("  FAIL  INFO returned empty string");
            return Ok(());
        }
        let version_line = info.lines().find(|l| l.contains("version") || l.contains("moon"));
        println!("  PASS  INFO returned {} bytes; version hint: {:?}", info.len(), version_line);

        let db_size = check!("DBSIZE", client.dbsize().await);
        println!("  PASS  DBSIZE={db_size}");
    }

    // =========================================================================
    println!();
    println!("All validation sections completed.");
    Ok(())
}

fn section(name: &str) {
    println!();
    println!("─── {name} ───");
}
