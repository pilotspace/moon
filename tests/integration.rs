//! Integration tests for the rust-redis server.
//!
//! Each test spawns a real TCP server on an OS-assigned port, connects with the
//! `redis` crate client, exercises commands over real TCP, and shuts down cleanly.

use redis::AsyncCommands;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use rust_redis::config::ServerConfig;
use rust_redis::server::listener;

/// Start a server on a random port and return the port + shutdown token.
async fn start_server() -> (u16, CancellationToken) {
    // Bind to port 0 to get an OS-assigned port, then drop the listener
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
    };

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    // Give the server a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (port, token)
}

/// Start a server with requirepass on a random port.
async fn start_server_with_pass(password: &str) -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let token = CancellationToken::new();
    let server_token = token.clone();

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        requirepass: Some(password.to_string()),
        appendonly: "no".to_string(),
        appendfsync: "everysec".to_string(),
        save: None,
        dir: ".".to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendfilename: "appendonly.aof".to_string(),
    };

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (port, token)
}

/// Create a multiplexed async connection to the server on the given port.
async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    client
        .get_multiplexed_tokio_connection()
        .await
        .unwrap()
}

/// Create a non-multiplexed async connection (needed for SELECT).
async fn connect_single(port: u16) -> redis::aio::Connection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    client.get_tokio_connection().await.unwrap()
}

#[tokio::test]
async fn test_ping_pong() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;
    let result: String = redis::cmd("PING").query_async(&mut conn).await.unwrap();
    assert_eq!(result, "PONG");
    shutdown.cancel();
}

#[tokio::test]
async fn test_set_get_roundtrip() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: () = conn.set("mykey", "myvalue").await.unwrap();
    let val: String = conn.get("mykey").await.unwrap();
    assert_eq!(val, "myvalue");

    shutdown.cancel();
}

#[tokio::test]
async fn test_set_with_ex() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // SET key value EX 1
    let _: () = redis::cmd("SET")
        .arg("exkey")
        .arg("exvalue")
        .arg("EX")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Should exist immediately
    let val: String = conn.get("exkey").await.unwrap();
    assert_eq!(val, "exvalue");

    // Wait for expiry
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

    // Should be gone
    let val: Option<String> = conn.get("exkey").await.unwrap();
    assert_eq!(val, None);

    shutdown.cancel();
}

#[tokio::test]
async fn test_set_nx_xx() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // SET key value
    let _: () = conn.set("nxkey", "original").await.unwrap();

    // SET key value NX when key exists should return nil
    let result: Option<String> = redis::cmd("SET")
        .arg("nxkey")
        .arg("new")
        .arg("NX")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, None);

    // Value should be unchanged
    let val: String = conn.get("nxkey").await.unwrap();
    assert_eq!(val, "original");

    // SET key value XX when key missing should return nil
    let result: Option<String> = redis::cmd("SET")
        .arg("xxkey")
        .arg("val")
        .arg("XX")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, None);

    shutdown.cancel();
}

#[tokio::test]
async fn test_mget_mset() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // MSET k1 v1 k2 v2
    let _: () = redis::cmd("MSET")
        .arg("mk1")
        .arg("mv1")
        .arg("mk2")
        .arg("mv2")
        .query_async(&mut conn)
        .await
        .unwrap();

    // MGET mk1 mk2 mk3
    let result: Vec<Option<String>> = redis::cmd("MGET")
        .arg("mk1")
        .arg("mk2")
        .arg("mk3")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, vec![Some("mv1".to_string()), Some("mv2".to_string()), None]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_incr_decr() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let v: i64 = conn.incr("counter", 1).await.unwrap();
    assert_eq!(v, 1);
    let v: i64 = conn.incr("counter", 1).await.unwrap();
    assert_eq!(v, 2);
    let v: i64 = conn.decr("counter", 1).await.unwrap();
    assert_eq!(v, 1);

    shutdown.cancel();
}

#[tokio::test]
async fn test_del_exists() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: () = conn.set("delkey", "val").await.unwrap();
    let exists: i64 = conn.exists("delkey").await.unwrap();
    assert_eq!(exists, 1);

    let deleted: i64 = conn.del("delkey").await.unwrap();
    assert_eq!(deleted, 1);

    let exists: i64 = conn.exists("delkey").await.unwrap();
    assert_eq!(exists, 0);

    shutdown.cancel();
}

#[tokio::test]
async fn test_expire_ttl() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: () = conn.set("ttlkey", "val").await.unwrap();
    let set: bool = conn.expire("ttlkey", 100).await.unwrap();
    assert!(set);

    let ttl: i64 = conn.ttl("ttlkey").await.unwrap();
    assert!(ttl > 0 && ttl <= 100, "TTL was {}", ttl);

    let persisted: bool = conn.persist("ttlkey").await.unwrap();
    assert!(persisted);

    let ttl: i64 = conn.ttl("ttlkey").await.unwrap();
    assert_eq!(ttl, -1);

    shutdown.cancel();
}

#[tokio::test]
async fn test_keys_pattern() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: () = conn.set("hello", "1").await.unwrap();
    let _: () = conn.set("hallo", "2").await.unwrap();
    let _: () = conn.set("hxllo", "3").await.unwrap();

    // KEYS h?llo should match all 3
    let mut keys: Vec<String> = redis::cmd("KEYS")
        .arg("h?llo")
        .query_async(&mut conn)
        .await
        .unwrap();
    keys.sort();
    assert_eq!(keys.len(), 3);

    // KEYS h[ae]llo should match 2
    let mut keys: Vec<String> = redis::cmd("KEYS")
        .arg("h[ae]llo")
        .query_async(&mut conn)
        .await
        .unwrap();
    keys.sort();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"hallo".to_string()));
    assert!(keys.contains(&"hello".to_string()));

    shutdown.cancel();
}

#[tokio::test]
async fn test_concurrent_clients() {
    let (port, shutdown) = start_server().await;

    let mut handles = Vec::new();
    for i in 0..5u32 {
        let handle = tokio::spawn(async move {
            let mut conn = connect(port).await;
            let key = format!("client_{}", i);
            let val = format!("value_{}", i);
            let _: () = conn.set(&key, &val).await.unwrap();
            let result: String = conn.get(&key).await.unwrap();
            assert_eq!(result, val);
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    shutdown.cancel();
}

#[tokio::test]
async fn test_pipeline() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let (set_result, get_result): (String, String) = redis::pipe()
        .cmd("SET")
        .arg("pipekey")
        .arg("pipeval")
        .cmd("GET")
        .arg("pipekey")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(set_result, "OK");
    assert_eq!(get_result, "pipeval");

    shutdown.cancel();
}

#[tokio::test]
async fn test_rename() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: () = conn.set("src", "val").await.unwrap();
    let _: () = conn.rename("src", "dst").await.unwrap();

    let val: String = conn.get("dst").await.unwrap();
    assert_eq!(val, "val");

    let val: Option<String> = conn.get("src").await.unwrap();
    assert_eq!(val, None);

    shutdown.cancel();
}

#[tokio::test]
async fn test_type_command() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: () = conn.set("typed", "val").await.unwrap();

    let t: String = redis::cmd("TYPE")
        .arg("typed")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "string");

    let t: String = redis::cmd("TYPE")
        .arg("missing")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "none");

    shutdown.cancel();
}

#[tokio::test]
async fn test_select_database() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect_single(port).await;

    // SELECT 1
    let _: () = redis::cmd("SELECT")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();

    let _: () = redis::cmd("SET")
        .arg("dbkey")
        .arg("dbval")
        .query_async(&mut conn)
        .await
        .unwrap();

    // SELECT 0
    let _: () = redis::cmd("SELECT")
        .arg(0)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Key should not exist in db 0
    let val: Option<String> = redis::cmd("GET")
        .arg("dbkey")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, None);

    // SELECT 1 again
    let _: () = redis::cmd("SELECT")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();

    // Key should exist in db 1
    let val: String = redis::cmd("GET")
        .arg("dbkey")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "dbval");

    shutdown.cancel();
}

// ===== Phase 3: Collection Data Types Integration Tests =====

#[tokio::test]
async fn test_hash_commands() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // HSET key field1 val1 field2 val2 -> 2 (new fields)
    let added: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("name")
        .arg("Redis")
        .arg("version")
        .arg("7")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(added, 2);

    // HGET
    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("name")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "Redis");

    let val: String = redis::cmd("HGET")
        .arg("myhash")
        .arg("version")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "7");

    // HGETALL returns flat array [field, val, field, val, ...]
    let all: Vec<String> = redis::cmd("HGETALL")
        .arg("myhash")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(all.len(), 4);
    // Convert to pairs and sort for deterministic check
    let mut pairs: Vec<(String, String)> = all
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("name".to_string(), "Redis".to_string()),
            ("version".to_string(), "7".to_string()),
        ]
    );

    // HLEN
    let len: i64 = redis::cmd("HLEN")
        .arg("myhash")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 2);

    // HDEL
    let removed: i64 = redis::cmd("HDEL")
        .arg("myhash")
        .arg("version")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // HLEN after HDEL
    let len: i64 = redis::cmd("HLEN")
        .arg("myhash")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 1);

    // HEXISTS
    let exists: i64 = redis::cmd("HEXISTS")
        .arg("myhash")
        .arg("name")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(exists, 1);

    let exists: i64 = redis::cmd("HEXISTS")
        .arg("myhash")
        .arg("version")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(exists, 0);

    shutdown.cancel();
}

#[tokio::test]
async fn test_list_commands() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // LPUSH mylist a b c -> 3 (Pitfall 5: order is c, b, a)
    let len: i64 = redis::cmd("LPUSH")
        .arg("mylist")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 3);

    // LRANGE 0 -1 should return [c, b, a]
    let items: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(items, vec!["c", "b", "a"]);

    // RPUSH adds to end
    let len: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("d")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 4);

    // LLEN
    let len: i64 = redis::cmd("LLEN")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 4);

    // LPOP
    let val: String = redis::cmd("LPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "c");

    // RPOP
    let val: String = redis::cmd("RPOP")
        .arg("mylist")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "d");

    // Remaining: [b, a]
    let items: Vec<String> = redis::cmd("LRANGE")
        .arg("mylist")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(items, vec!["b", "a"]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_set_commands() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // SADD myset a b c
    let added: i64 = redis::cmd("SADD")
        .arg("myset")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(added, 3);

    // SCARD
    let card: i64 = redis::cmd("SCARD")
        .arg("myset")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(card, 3);

    // SMEMBERS
    let mut members: Vec<String> = redis::cmd("SMEMBERS")
        .arg("myset")
        .query_async(&mut conn)
        .await
        .unwrap();
    members.sort();
    assert_eq!(members, vec!["a", "b", "c"]);

    // SISMEMBER
    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("myset")
        .arg("a")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(is_member, 1);

    let is_member: i64 = redis::cmd("SISMEMBER")
        .arg("myset")
        .arg("z")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(is_member, 0);

    // SREM
    let removed: i64 = redis::cmd("SREM")
        .arg("myset")
        .arg("b")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // SCARD after remove
    let card: i64 = redis::cmd("SCARD")
        .arg("myset")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(card, 2);

    // SINTER of two sets
    let _: i64 = redis::cmd("SADD")
        .arg("set2")
        .arg("a")
        .arg("d")
        .query_async(&mut conn)
        .await
        .unwrap();

    let mut inter: Vec<String> = redis::cmd("SINTER")
        .arg("myset")
        .arg("set2")
        .query_async(&mut conn)
        .await
        .unwrap();
    inter.sort();
    assert_eq!(inter, vec!["a"]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sorted_set_commands() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // ZADD zs 1 a 2 b 3 c
    let added: i64 = redis::cmd("ZADD")
        .arg("zs")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(added, 3);

    // ZRANGE zs 0 -1 returns [a, b, c] ordered by score
    let members: Vec<String> = redis::cmd("ZRANGE")
        .arg("zs")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(members, vec!["a", "b", "c"]);

    // ZSCORE
    let score: String = redis::cmd("ZSCORE")
        .arg("zs")
        .arg("b")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(score, "2");

    // ZRANK
    let rank: i64 = redis::cmd("ZRANK")
        .arg("zs")
        .arg("a")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(rank, 0);

    let rank: i64 = redis::cmd("ZRANK")
        .arg("zs")
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(rank, 2);

    // ZRANGEBYSCORE with -inf +inf
    let all: Vec<String> = redis::cmd("ZRANGEBYSCORE")
        .arg("zs")
        .arg("-inf")
        .arg("+inf")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(all, vec!["a", "b", "c"]);

    // ZCARD
    let card: i64 = redis::cmd("ZCARD")
        .arg("zs")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(card, 3);

    shutdown.cancel();
}

#[tokio::test]
async fn test_type_command_all_types() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // String
    let _: () = redis::cmd("SET")
        .arg("s")
        .arg("val")
        .query_async(&mut conn)
        .await
        .unwrap();
    let t: String = redis::cmd("TYPE")
        .arg("s")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "string");

    // Hash
    let _: i64 = redis::cmd("HSET")
        .arg("h")
        .arg("f")
        .arg("v")
        .query_async(&mut conn)
        .await
        .unwrap();
    let t: String = redis::cmd("TYPE")
        .arg("h")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "hash");

    // List
    let _: i64 = redis::cmd("LPUSH")
        .arg("l")
        .arg("v")
        .query_async(&mut conn)
        .await
        .unwrap();
    let t: String = redis::cmd("TYPE")
        .arg("l")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "list");

    // Set
    let _: i64 = redis::cmd("SADD")
        .arg("st")
        .arg("v")
        .query_async(&mut conn)
        .await
        .unwrap();
    let t: String = redis::cmd("TYPE")
        .arg("st")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "set");

    // Sorted Set
    let _: i64 = redis::cmd("ZADD")
        .arg("z")
        .arg(1)
        .arg("v")
        .query_async(&mut conn)
        .await
        .unwrap();
    let t: String = redis::cmd("TYPE")
        .arg("z")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(t, "zset");

    shutdown.cancel();
}

#[tokio::test]
async fn test_wrongtype_error() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create a string key
    let _: () = redis::cmd("SET")
        .arg("strkey")
        .arg("val")
        .query_async(&mut conn)
        .await
        .unwrap();

    // HSET on string key -> WRONGTYPE
    let result: redis::RedisResult<i64> = redis::cmd("HSET")
        .arg("strkey")
        .arg("field")
        .arg("value")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{}", err).contains("WRONGTYPE"),
        "Expected WRONGTYPE error, got: {}",
        err
    );

    // LPUSH on string key -> WRONGTYPE
    let result: redis::RedisResult<i64> = redis::cmd("LPUSH")
        .arg("strkey")
        .arg("val")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{}", err).contains("WRONGTYPE"),
        "Expected WRONGTYPE error, got: {}",
        err
    );

    // Create a hash key and try GET on it -> WRONGTYPE
    let _: i64 = redis::cmd("HSET")
        .arg("hashkey")
        .arg("f")
        .arg("v")
        .query_async(&mut conn)
        .await
        .unwrap();

    let result: redis::RedisResult<Option<String>> = redis::cmd("GET")
        .arg("hashkey")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{}", err).contains("WRONGTYPE"),
        "Expected WRONGTYPE error, got: {}",
        err
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_scan_basic() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create several keys
    for i in 0..10 {
        let _: () = redis::cmd("SET")
            .arg(format!("scankey:{}", i))
            .arg(format!("val{}", i))
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    // SCAN until cursor returns 0, collect all keys
    let mut all_keys: Vec<String> = Vec::new();
    let mut cursor: i64 = 0;
    loop {
        let (next_cursor, keys): (i64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .query_async(&mut conn)
            .await
            .unwrap();
        all_keys.extend(keys);
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    // All 10 keys should be found
    all_keys.sort();
    assert_eq!(all_keys.len(), 10);
    for i in 0..10 {
        assert!(
            all_keys.contains(&format!("scankey:{}", i)),
            "Missing scankey:{}",
            i
        );
    }

    shutdown.cancel();
}

#[tokio::test]
async fn test_unlink() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // SET and UNLINK single key
    let _: () = redis::cmd("SET")
        .arg("unlinkme")
        .arg("val")
        .query_async(&mut conn)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("UNLINK")
        .arg("unlinkme")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // Verify gone
    let val: Option<String> = redis::cmd("GET")
        .arg("unlinkme")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, None);

    // UNLINK multiple keys
    let _: () = redis::cmd("SET")
        .arg("u1")
        .arg("v1")
        .query_async(&mut conn)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("u2")
        .arg("v2")
        .query_async(&mut conn)
        .await
        .unwrap();

    let removed: i64 = redis::cmd("UNLINK")
        .arg("u1")
        .arg("u2")
        .arg("u3") // non-existent
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(removed, 2);

    shutdown.cancel();
}

#[tokio::test]
async fn test_auth_required() {
    let (port, shutdown) = start_server_with_pass("testpass").await;

    // Connect without auth -- use a single (non-multiplexed) connection
    // so we control the auth flow precisely
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_tokio_connection().await.unwrap();

    // GET before AUTH -> NOAUTH error
    let result: redis::RedisResult<Option<String>> = redis::cmd("GET")
        .arg("anykey")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{}", err).contains("NOAUTH"),
        "Expected NOAUTH error, got: {}",
        err
    );

    // AUTH with wrong password -> error
    let result: redis::RedisResult<String> = redis::cmd("AUTH")
        .arg("wrongpass")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err());

    // AUTH with correct password -> OK
    let result: String = redis::cmd("AUTH")
        .arg("testpass")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // Now GET should work
    let _: () = redis::cmd("SET")
        .arg("authkey")
        .arg("authval")
        .query_async(&mut conn)
        .await
        .unwrap();
    let val: String = redis::cmd("GET")
        .arg("authkey")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "authval");

    shutdown.cancel();
}

#[tokio::test]
async fn test_hscan() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // HSET key with several fields
    let _: i64 = redis::cmd("HSET")
        .arg("hscankey")
        .arg("f1")
        .arg("v1")
        .arg("f2")
        .arg("v2")
        .arg("f3")
        .arg("v3")
        .arg("f4")
        .arg("v4")
        .query_async(&mut conn)
        .await
        .unwrap();

    // HSCAN 0, collect all results
    let mut all_items: Vec<String> = Vec::new();
    let mut cursor: i64 = 0;
    loop {
        let (next_cursor, items): (i64, Vec<String>) = redis::cmd("HSCAN")
            .arg("hscankey")
            .arg(cursor)
            .query_async(&mut conn)
            .await
            .unwrap();
        all_items.extend(items);
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    // Should have 8 items (4 field-value pairs as flat array)
    assert_eq!(all_items.len(), 8);

    // Convert to sorted pairs
    let mut pairs: Vec<(String, String)> = all_items
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("f1".to_string(), "v1".to_string()),
            ("f2".to_string(), "v2".to_string()),
            ("f3".to_string(), "v3".to_string()),
            ("f4".to_string(), "v4".to_string()),
        ]
    );

    shutdown.cancel();
}

// ===== Phase 4: Persistence Integration Tests =====

/// Issue BGSAVE, retrying if another test's save is still in progress (global AtomicBool).
async fn bgsave_with_retry(conn: &mut redis::aio::MultiplexedConnection) {
    for attempt in 0..20 {
        let result: Result<String, _> = redis::cmd("BGSAVE")
            .query_async(conn)
            .await;
        match result {
            Ok(msg) => {
                assert_eq!(msg, "Background saving started");
                // Wait for save to complete
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                return;
            }
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                if attempt == 19 {
                    panic!("BGSAVE failed after 20 retries");
                }
            }
        }
    }
}

/// Start a server with custom persistence config on a random port.
async fn start_server_with_persistence(
    appendonly: &str,
    appendfsync: &str,
    dir: &std::path::Path,
) -> (u16, CancellationToken) {
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
        appendonly: appendonly.to_string(),
        appendfsync: appendfsync.to_string(),
        save: None,
        dir: dir.to_string_lossy().to_string(),
        dbfilename: "dump.rdb".to_string(),
        appendfilename: "appendonly.aof".to_string(),
    };

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (port, token)
}

#[tokio::test]
async fn test_bgsave_creates_rdb_file() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let dir = tmp_dir.path().to_path_buf();
    let (port, shutdown) = start_server_with_persistence("no", "everysec", &dir).await;
    let mut conn = connect(port).await;

    // SET several keys
    let _: () = conn.set("key1", "value1").await.unwrap();
    let _: () = conn.set("key2", "value2").await.unwrap();
    let _: i64 = redis::cmd("HSET")
        .arg("myhash")
        .arg("f1")
        .arg("v1")
        .query_async(&mut conn)
        .await
        .unwrap();

    // BGSAVE (retry if another test's save is still in progress)
    bgsave_with_retry(&mut conn).await;

    // Verify dump.rdb exists
    let rdb_path = dir.join("dump.rdb");
    assert!(rdb_path.exists(), "dump.rdb should exist after BGSAVE");

    // Verify file starts with RUSTREDIS magic bytes
    let data = std::fs::read(&rdb_path).unwrap();
    assert!(
        data.starts_with(b"RUSTREDIS"),
        "RDB file should start with RUSTREDIS magic bytes"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_rdb_restore_on_startup() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let dir = tmp_dir.path().to_path_buf();

    // --- Server 1: write data and BGSAVE ---
    {
        let (port, shutdown) = start_server_with_persistence("no", "everysec", &dir).await;
        let mut conn = connect(port).await;

        // String
        let _: () = conn.set("str_key", "str_val").await.unwrap();
        // Hash
        let _: i64 = redis::cmd("HSET")
            .arg("hash_key")
            .arg("field1")
            .arg("val1")
            .query_async(&mut conn)
            .await
            .unwrap();
        // List
        let _: i64 = redis::cmd("RPUSH")
            .arg("list_key")
            .arg("a")
            .arg("b")
            .arg("c")
            .query_async(&mut conn)
            .await
            .unwrap();
        // Set
        let _: i64 = redis::cmd("SADD")
            .arg("set_key")
            .arg("x")
            .arg("y")
            .query_async(&mut conn)
            .await
            .unwrap();
        // Sorted Set
        let _: i64 = redis::cmd("ZADD")
            .arg("zset_key")
            .arg(1.5)
            .arg("alice")
            .arg(2.5)
            .arg("bob")
            .query_async(&mut conn)
            .await
            .unwrap();

        // BGSAVE
        bgsave_with_retry(&mut conn).await;

        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // --- Server 2: restart and verify restore ---
    {
        let (port, shutdown) = start_server_with_persistence("no", "everysec", &dir).await;
        let mut conn = connect(port).await;

        // String
        let val: String = conn.get("str_key").await.unwrap();
        assert_eq!(val, "str_val");

        // Hash
        let val: String = redis::cmd("HGET")
            .arg("hash_key")
            .arg("field1")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(val, "val1");

        // List
        let items: Vec<String> = redis::cmd("LRANGE")
            .arg("list_key")
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(items, vec!["a", "b", "c"]);

        // Set
        let card: i64 = redis::cmd("SCARD")
            .arg("set_key")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(card, 2);

        // Sorted Set
        let members: Vec<String> = redis::cmd("ZRANGE")
            .arg("zset_key")
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(members, vec!["alice", "bob"]);

        shutdown.cancel();
    }
}

#[tokio::test]
async fn test_aof_logging() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let dir = tmp_dir.path().to_path_buf();

    {
        let (port, shutdown) =
            start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        let _: () = conn.set("foo", "bar").await.unwrap();
        let _: i64 = redis::cmd("HSET")
            .arg("hash")
            .arg("f")
            .arg("v")
            .query_async(&mut conn)
            .await
            .unwrap();
        let _: i64 = redis::cmd("LPUSH")
            .arg("list")
            .arg("a")
            .query_async(&mut conn)
            .await
            .unwrap();

        // Give AOF writer time to flush
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Read AOF file and verify it contains RESP-formatted commands
    let aof_path = dir.join("appendonly.aof");
    assert!(aof_path.exists(), "appendonly.aof should exist");

    let content = std::fs::read_to_string(&aof_path).unwrap();
    assert!(
        content.contains("SET") || content.contains("set"),
        "AOF should contain SET command"
    );
    assert!(
        content.contains("foo"),
        "AOF should contain key 'foo'"
    );
    assert!(
        content.contains("HSET") || content.contains("hset"),
        "AOF should contain HSET command"
    );
    assert!(
        content.contains("LPUSH") || content.contains("lpush"),
        "AOF should contain LPUSH command"
    );
}

#[tokio::test]
async fn test_aof_restore_on_startup() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let dir = tmp_dir.path().to_path_buf();

    // --- Server 1: write data with AOF ---
    {
        let (port, shutdown) =
            start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        let _: () = conn.set("aof_key1", "aof_val1").await.unwrap();
        let _: () = conn.set("aof_key2", "aof_val2").await.unwrap();
        let _: i64 = redis::cmd("HSET")
            .arg("aof_hash")
            .arg("field")
            .arg("value")
            .query_async(&mut conn)
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // --- Server 2: restart with AOF and verify restore ---
    {
        let (port, shutdown) =
            start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        let val: String = conn.get("aof_key1").await.unwrap();
        assert_eq!(val, "aof_val1");
        let val: String = conn.get("aof_key2").await.unwrap();
        assert_eq!(val, "aof_val2");
        let val: String = redis::cmd("HGET")
            .arg("aof_hash")
            .arg("field")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(val, "value");

        shutdown.cancel();
    }
}

#[tokio::test]
async fn test_aof_priority_over_rdb() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let dir = tmp_dir.path().to_path_buf();

    // --- Server 1: create both RDB and AOF with different values ---
    {
        let (port, shutdown) =
            start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        // Set initial value and BGSAVE (RDB snapshot)
        let _: () = conn.set("key1", "rdb_value").await.unwrap();
        bgsave_with_retry(&mut conn).await;

        // Overwrite in AOF (but RDB already has "rdb_value")
        let _: () = conn.set("key1", "aof_value").await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // --- Server 2: restart with AOF enabled -> should use AOF (priority) ---
    {
        let (port, shutdown) =
            start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        let val: String = conn.get("key1").await.unwrap();
        assert_eq!(
            val, "aof_value",
            "AOF should take priority over RDB on startup"
        );

        shutdown.cancel();
    }
}

#[tokio::test]
async fn test_bgrewriteaof() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let dir = tmp_dir.path().to_path_buf();

    {
        let (port, shutdown) =
            start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        // Write key1 many times to bloat the AOF
        for i in 0..20 {
            let _: () = conn.set("key1", format!("value_{}", i)).await.unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Record AOF size before rewrite
        let aof_path = dir.join("appendonly.aof");
        let size_before = std::fs::metadata(&aof_path).unwrap().len();

        // BGREWRITEAOF
        let result: String = redis::cmd("BGREWRITEAOF")
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(result, "Background append only file rewriting started");

        // Wait for rewrite to complete
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let size_after = std::fs::metadata(&aof_path).unwrap().len();
        assert!(
            size_after <= size_before,
            "AOF should be same size or smaller after rewrite: before={}, after={}",
            size_before,
            size_after
        );

        shutdown.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Verify data survives restart after rewrite
    {
        let (port, shutdown) =
            start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        let val: String = conn.get("key1").await.unwrap();
        assert_eq!(val, "value_19", "key1 should have final value after rewrite + restart");

        shutdown.cancel();
    }
}
