//! Integration tests for the moon server.
//!
//! Each test spawns a real TCP server on an OS-assigned port, connects with the
//! `redis` crate client, exercises commands over real TCP, and shuts down cleanly.

use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use redis::AsyncCommands;
use tokio::net::TcpListener;

use moon::config::ServerConfig;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};

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
        maxmemory: 0,
        maxmemory_policy: "noeviction".to_string(),
        maxmemory_samples: 5,
        shards: 0,
        cluster_enabled: false,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "yes".to_string(),
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
        maxmemory: 0,
        maxmemory_policy: "noeviction".to_string(),
        maxmemory_samples: 5,
        shards: 0,
        cluster_enabled: false,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "yes".to_string(),
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
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Create a non-multiplexed async connection (needed for SELECT).
async fn connect_single(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
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
    assert_eq!(
        result,
        vec![Some("mv1".to_string()), Some("mv2".to_string()), None]
    );

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
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    // GET before AUTH -> NOAUTH error
    let result: redis::RedisResult<Option<String>> =
        redis::cmd("GET").arg("anykey").query_async(&mut conn).await;
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
        let result: Result<String, _> = redis::cmd("BGSAVE").query_async(conn).await;
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
        maxmemory: 0,
        maxmemory_policy: "noeviction".to_string(),
        maxmemory_samples: 5,
        shards: 0,
        cluster_enabled: false,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "yes".to_string(),
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

    // Verify file starts with MOON magic bytes
    let data = std::fs::read(&rdb_path).unwrap();
    assert!(
        data.starts_with(b"MOON"),
        "RDB file should start with MOON magic bytes"
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
        let (port, shutdown) = start_server_with_persistence("yes", "always", &dir).await;
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
    assert!(content.contains("foo"), "AOF should contain key 'foo'");
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
        let (port, shutdown) = start_server_with_persistence("yes", "always", &dir).await;
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
        let (port, shutdown) = start_server_with_persistence("yes", "always", &dir).await;
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
        let (port, shutdown) = start_server_with_persistence("yes", "always", &dir).await;
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
        let (port, shutdown) = start_server_with_persistence("yes", "always", &dir).await;
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
        let (port, shutdown) = start_server_with_persistence("yes", "always", &dir).await;
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
        let (port, shutdown) = start_server_with_persistence("yes", "always", &dir).await;
        let mut conn = connect(port).await;

        let val: String = conn.get("key1").await.unwrap();
        assert_eq!(
            val, "value_19",
            "key1 should have final value after rewrite + restart"
        );

        shutdown.cancel();
    }
}

// ===== Phase 5: Pub/Sub Integration Tests =====

#[tokio::test]
async fn test_pubsub_subscribe_and_publish() {
    use futures::StreamExt;

    let (port, shutdown) = start_server().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Subscriber connection (PubSub mode)
    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("test-channel").await.unwrap();

    // Publisher connection (multiplexed)
    let mut pub_conn = connect(port).await;

    // Publish a message
    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("test-channel")
        .arg("hello-world")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(receivers, 1, "PUBLISH should return 1 (one subscriber)");

    // Subscriber receives the message
    let msg: redis::Msg = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pubsub.on_message().next(),
    )
    .await
    .expect("timed out waiting for pubsub message")
    .expect("stream ended unexpectedly");

    assert_eq!(msg.get_channel_name(), "test-channel");
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "hello-world");

    shutdown.cancel();
}

#[tokio::test]
async fn test_pubsub_psubscribe() {
    use futures::StreamExt;

    let (port, shutdown) = start_server().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Subscriber with pattern
    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.psubscribe("news.*").await.unwrap();

    // Publisher
    let mut pub_conn = connect(port).await;

    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("news.sports")
        .arg("goal!")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(receivers, 1);

    // Subscriber receives pmessage
    let msg: redis::Msg = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pubsub.on_message().next(),
    )
    .await
    .expect("timed out waiting for pubsub message")
    .expect("stream ended");

    // For pattern subscriptions, get_channel_name returns the actual channel
    assert_eq!(msg.get_channel_name(), "news.sports");
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "goal!");
    // Verify it was a pattern match
    assert!(msg.from_pattern());

    shutdown.cancel();
}

#[tokio::test]
async fn test_pubsub_unsubscribe() {
    let (port, shutdown) = start_server().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Subscribe then unsubscribe
    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("ch1").await.unwrap();
    pubsub.unsubscribe("ch1").await.unwrap();

    // Publisher
    let mut pub_conn = connect(port).await;

    // PUBLISH should return 0 (no subscribers)
    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("ch1")
        .arg("msg")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(receivers, 0, "no subscribers after unsubscribe");

    shutdown.cancel();
}

#[tokio::test]
async fn test_pubsub_multiple_channels() {
    use futures::StreamExt;

    let (port, shutdown) = start_server().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("ch1").await.unwrap();
    pubsub.subscribe("ch2").await.unwrap();

    let mut pub_conn = connect(port).await;

    // Publish to ch1
    let r: i64 = redis::cmd("PUBLISH")
        .arg("ch1")
        .arg("msg1")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(r, 1);

    let msg: redis::Msg = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pubsub.on_message().next(),
    )
    .await
    .expect("timeout")
    .expect("stream ended");
    assert_eq!(msg.get_channel_name(), "ch1");
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "msg1");

    // Publish to ch2
    let r: i64 = redis::cmd("PUBLISH")
        .arg("ch2")
        .arg("msg2")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(r, 1);

    let msg: redis::Msg = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pubsub.on_message().next(),
    )
    .await
    .expect("timeout")
    .expect("stream ended");
    assert_eq!(msg.get_channel_name(), "ch2");
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "msg2");

    shutdown.cancel();
}

#[tokio::test]
async fn test_pubsub_publish_returns_subscriber_count() {
    let (port, shutdown) = start_server().await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Two subscribers on the same channel
    let mut pubsub1 = client.get_async_pubsub().await.unwrap();
    pubsub1.subscribe("shared-ch").await.unwrap();

    let mut pubsub2 = client.get_async_pubsub().await.unwrap();
    pubsub2.subscribe("shared-ch").await.unwrap();

    let mut pub_conn = connect(port).await;

    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("shared-ch")
        .arg("hello")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(receivers, 2, "PUBLISH should return 2 (two subscribers)");

    shutdown.cancel();
}

// ===== Phase 5: Transaction (MULTI/EXEC/DISCARD/WATCH) Integration Tests =====

#[tokio::test]
async fn test_multi_exec_basic() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect_single(port).await;

    // MULTI
    let ok: String = redis::cmd("MULTI").query_async(&mut conn).await.unwrap();
    assert_eq!(ok, "OK");

    // SET key1 val1 -> QUEUED
    let queued: String = redis::cmd("SET")
        .arg("txkey1")
        .arg("txval1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(queued, "QUEUED");

    // SET key2 val2 -> QUEUED
    let queued: String = redis::cmd("SET")
        .arg("txkey2")
        .arg("txval2")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(queued, "QUEUED");

    // GET key1 -> QUEUED
    let queued: String = redis::cmd("GET")
        .arg("txkey1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(queued, "QUEUED");

    // EXEC -> returns array of results
    let results: redis::Value = redis::cmd("EXEC").query_async(&mut conn).await.unwrap();

    // Verify we got an array with 3 results
    match results {
        redis::Value::Array(ref items) => {
            assert_eq!(items.len(), 3, "EXEC should return 3 results");
            // First two are OK from SET commands
            let set1: String = redis::FromRedisValue::from_redis_value(items[0].clone()).unwrap();
            assert_eq!(set1, "OK");
            let set2: String = redis::FromRedisValue::from_redis_value(items[1].clone()).unwrap();
            assert_eq!(set2, "OK");
            // Third is the GET result
            let get_val: String =
                redis::FromRedisValue::from_redis_value(items[2].clone()).unwrap();
            assert_eq!(get_val, "txval1");
        }
        _ => panic!("EXEC should return an array, got: {:?}", results),
    }

    shutdown.cancel();
}

#[tokio::test]
async fn test_multi_discard() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect_single(port).await;

    // MULTI
    let _: String = redis::cmd("MULTI").query_async(&mut conn).await.unwrap();

    // SET key1 val1 -> QUEUED
    let _: String = redis::cmd("SET")
        .arg("discardkey")
        .arg("discardval")
        .query_async(&mut conn)
        .await
        .unwrap();

    // DISCARD
    let ok: String = redis::cmd("DISCARD").query_async(&mut conn).await.unwrap();
    assert_eq!(ok, "OK");

    // GET key1 -> should be nil (key was never set)
    let val: Option<String> = redis::cmd("GET")
        .arg("discardkey")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, None);

    shutdown.cancel();
}

#[tokio::test]
async fn test_exec_without_multi() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect_single(port).await;

    // EXEC without MULTI -> should return error
    let result: Result<redis::Value, redis::RedisError> =
        redis::cmd("EXEC").query_async(&mut conn).await;
    assert!(result.is_err(), "EXEC without MULTI should fail");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("EXEC without MULTI"),
        "Error should mention EXEC without MULTI, got: {}",
        err
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_discard_without_multi() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect_single(port).await;

    // DISCARD without MULTI -> should return error
    let result: Result<redis::Value, redis::RedisError> =
        redis::cmd("DISCARD").query_async(&mut conn).await;
    assert!(result.is_err(), "DISCARD without MULTI should fail");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("DISCARD without MULTI"),
        "Error should mention DISCARD without MULTI, got: {}",
        err
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_nested_multi() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect_single(port).await;

    // First MULTI
    let _: String = redis::cmd("MULTI").query_async(&mut conn).await.unwrap();

    // Second MULTI -> should return error
    let result: Result<redis::Value, redis::RedisError> =
        redis::cmd("MULTI").query_async(&mut conn).await;
    assert!(result.is_err(), "nested MULTI should fail");

    // Clean up: DISCARD the first MULTI
    let _: String = redis::cmd("DISCARD").query_async(&mut conn).await.unwrap();

    shutdown.cancel();
}

#[tokio::test]
async fn test_watch_success() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect_single(port).await;

    // WATCH key1 (key doesn't exist yet, version 0)
    let ok: String = redis::cmd("WATCH")
        .arg("watchkey")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ok, "OK");

    // MULTI
    let _: String = redis::cmd("MULTI").query_async(&mut conn).await.unwrap();

    // SET watchkey "new" -> QUEUED
    let _: String = redis::cmd("SET")
        .arg("watchkey")
        .arg("new")
        .query_async(&mut conn)
        .await
        .unwrap();

    // EXEC -> should succeed (no interference)
    let result: redis::Value = redis::cmd("EXEC").query_async(&mut conn).await.unwrap();
    match result {
        redis::Value::Array(ref items) => {
            assert_eq!(items.len(), 1, "EXEC should return 1 result");
            let set_result: String =
                redis::FromRedisValue::from_redis_value(items[0].clone()).unwrap();
            assert_eq!(set_result, "OK");
        }
        _ => panic!("EXEC should return an array, got: {:?}", result),
    }

    // Verify the key was set
    let val: String = redis::cmd("GET")
        .arg("watchkey")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "new");

    shutdown.cancel();
}

#[tokio::test]
async fn test_watch_abort() {
    let (port, shutdown) = start_server().await;

    // Two separate non-multiplexed connections
    let mut conn_a = connect_single(port).await;
    let mut conn_b = connect_single(port).await;

    // conn_a: SET key1 "initial"
    let _: () = redis::cmd("SET")
        .arg("wkey")
        .arg("initial")
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // conn_a: WATCH wkey
    let _: String = redis::cmd("WATCH")
        .arg("wkey")
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // conn_a: MULTI
    let _: String = redis::cmd("MULTI").query_async(&mut conn_a).await.unwrap();

    // conn_b: interfere by modifying the watched key
    let _: () = redis::cmd("SET")
        .arg("wkey")
        .arg("modified")
        .query_async(&mut conn_b)
        .await
        .unwrap();

    // conn_a: queue a SET (will be discarded)
    let _: String = redis::cmd("SET")
        .arg("wkey")
        .arg("from_a")
        .query_async(&mut conn_a)
        .await
        .unwrap();

    // conn_a: EXEC -> should return Null (transaction aborted)
    let result: redis::Value = redis::cmd("EXEC").query_async(&mut conn_a).await.unwrap();
    assert_eq!(
        result,
        redis::Value::Nil,
        "EXEC should return Nil when WATCH detects modification"
    );

    // Verify: the key has conn_b's value
    let val: String = redis::cmd("GET")
        .arg("wkey")
        .query_async(&mut conn_b)
        .await
        .unwrap();
    assert_eq!(
        val, "modified",
        "conn_b's SET should persist after conn_a's transaction was aborted"
    );

    shutdown.cancel();
}

// ===== Phase 5: CONFIG and Eviction Integration Tests =====

/// Start a server with specific maxmemory and eviction policy settings.
async fn start_server_with_maxmemory(maxmemory: usize, policy: &str) -> (u16, CancellationToken) {
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
        maxmemory,
        maxmemory_policy: policy.to_string(),
        maxmemory_samples: 5,
        shards: 0,
        cluster_enabled: false,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "yes".to_string(),
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
async fn test_config_get_maxmemory() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // CONFIG GET maxmemory -- default is 0
    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, vec!["maxmemory".to_string(), "0".to_string()]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_config_set_maxmemory() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // CONFIG SET maxmemory 1048576 (1MB)
    let ok: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("1048576")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ok, "OK");

    // CONFIG GET maxmemory -- should now be 1048576
    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, vec!["maxmemory".to_string(), "1048576".to_string()]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_config_get_glob_pattern() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // CONFIG GET "maxmemory*" -- should match maxmemory, maxmemory-policy, maxmemory-samples
    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory*")
        .query_async(&mut conn)
        .await
        .unwrap();
    // Should have 3 param-value pairs = 6 elements
    assert_eq!(
        result.len(),
        6,
        "Expected 3 maxmemory params, got: {:?}",
        result
    );
    // Verify all expected param names are present
    assert!(result.contains(&"maxmemory".to_string()));
    assert!(result.contains(&"maxmemory-policy".to_string()));
    assert!(result.contains(&"maxmemory-samples".to_string()));

    shutdown.cancel();
}

#[tokio::test]
async fn test_config_set_maxmemory_policy() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // CONFIG SET maxmemory-policy allkeys-lru
    let ok: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("allkeys-lru")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ok, "OK");

    // CONFIG GET maxmemory-policy
    let result: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory-policy")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        result,
        vec!["maxmemory-policy".to_string(), "allkeys-lru".to_string()]
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_eviction_noeviction_rejects_writes() {
    // Start server with very small maxmemory and noeviction policy.
    // Eviction check happens BEFORE each write, so we need the memory to already
    // exceed maxmemory before the rejected write is attempted.
    // Entry overhead = key.len() + value.len() + 128
    // Use maxmemory=400, write two entries to push over, then the third should fail.
    let (port, shutdown) = start_server_with_maxmemory(400, "noeviction").await;
    let mut conn = connect(port).await;

    // First SET: overhead = 4 + 100 + 128 = 232 bytes (under 400)
    let val = "x".repeat(100);
    let result: Result<(), redis::RedisError> = redis::cmd("SET")
        .arg("key1")
        .arg(&val)
        .query_async(&mut conn)
        .await;
    assert!(result.is_ok(), "First SET should succeed (232 < 400)");

    // Second SET: overhead = 4 + 100 + 128 = 232 bytes, total = 464 (over 400)
    // But eviction check runs BEFORE this write, when memory is 232 (under 400),
    // so this SET will still succeed, pushing memory to 464
    let val2 = "x".repeat(100);
    let result: Result<(), redis::RedisError> = redis::cmd("SET")
        .arg("key2")
        .arg(&val2)
        .query_async(&mut conn)
        .await;
    assert!(
        result.is_ok(),
        "Second SET should succeed (pre-check sees 232 < 400)"
    );

    // Third SET: eviction check runs BEFORE this write, memory is 464 > 400,
    // noeviction policy means OOM error
    let result: Result<(), redis::RedisError> = redis::cmd("SET")
        .arg("key3")
        .arg("val")
        .query_async(&mut conn)
        .await;
    assert!(
        result.is_err(),
        "Third SET should fail with OOM when memory (464) > maxmemory (400)"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("OOM"),
        "Error should contain OOM, got: {}",
        err
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_eviction_allkeys_lru() {
    // Use a generous initial maxmemory so we can insert keys, then lower it via CONFIG SET
    // to trigger eviction on the next write. This avoids the pre-check timing issue.
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Set allkeys-lru policy
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("allkeys-lru")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Set several keys with moderate values
    // Each entry ~= key_len + 200 + 128 overhead
    let val = "x".repeat(200);
    for i in 0..5 {
        let key = format!("lrukey{}", i);
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg(&val)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    // Verify all 5 keys exist
    let mut before_count = 0i64;
    for i in 0..5 {
        let key = format!("lrukey{}", i);
        let exists: i64 = redis::cmd("EXISTS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        before_count += exists;
    }
    assert_eq!(before_count, 5, "All 5 keys should exist before eviction");

    // Now lower maxmemory to a small value to force eviction on next write
    // 5 entries * ~335 bytes each = ~1675 bytes total. Set limit to 500.
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("500")
        .query_async(&mut conn)
        .await
        .unwrap();

    // This write triggers eviction check: memory (~1675) > maxmemory (500)
    let result: Result<(), redis::RedisError> = redis::cmd("SET")
        .arg("lru_trigger")
        .arg("val")
        .query_async(&mut conn)
        .await;
    assert!(
        result.is_ok(),
        "SET should succeed with allkeys-lru (eviction frees space)"
    );

    // Count how many of the original keys survived
    let mut after_count = 0i64;
    for i in 0..5 {
        let key = format!("lrukey{}", i);
        let exists: i64 = redis::cmd("EXISTS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        after_count += exists;
    }

    // At least one key should have been evicted
    assert!(
        after_count < before_count,
        "Some keys should have been evicted by LRU: before={}, after={}",
        before_count,
        after_count
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_eviction_config_set_triggers_eviction() {
    // Start server with no maxmemory limit
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Fill up some data
    let val = "x".repeat(200);
    for i in 0..10 {
        let key = format!("evictkey{}", i);
        let _: () = redis::cmd("SET")
            .arg(&key)
            .arg(&val)
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    // CONFIG SET maxmemory-policy to allkeys-lru
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("allkeys-lru")
        .query_async(&mut conn)
        .await
        .unwrap();

    // CONFIG SET maxmemory to a small value (triggers eviction on next write)
    let _: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory")
        .arg("500")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Do one more write to trigger eviction check
    let _: () = redis::cmd("SET")
        .arg("trigger")
        .arg("val")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Count surviving keys
    let mut surviving = 0i64;
    for i in 0..10 {
        let key = format!("evictkey{}", i);
        let exists: i64 = redis::cmd("EXISTS")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .unwrap();
        surviving += exists;
    }

    assert!(
        surviving < 10,
        "Some keys should have been evicted after CONFIG SET maxmemory, surviving: {}",
        surviving
    );

    shutdown.cancel();
}

// ===== Phase 11: Sharded Architecture Integration Tests =====

/// Start a sharded server with N shard threads on a random port.
///
/// Mirrors the main.rs bootstrap: creates ChannelMesh, spawns shard threads
/// (each with its own current_thread runtime), and runs the listener on a
/// dedicated thread. Returns the port and a CancellationToken for shutdown.
async fn start_sharded_server(num_shards: usize) -> (u16, CancellationToken) {
    // Bind to port 0 to get an OS-assigned port, then drop the listener
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
        protected_mode: "yes".to_string(),
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
    };

    let cancel = token.clone();

    // Build the channel mesh and spawn shards on std threads (like main.rs)
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

        // Create shards on main thread, extract databases for SharedDatabases
        let mut shards: Vec<Shard> = (0..num_shards)
            .map(|id| Shard::new(id, num_shards, config.databases, config.to_runtime_config()))
            .collect();
        let all_dbs: Vec<Vec<moon::storage::Database>> = shards
            .iter_mut()
            .map(|s| std::mem::take(&mut s.databases))
            .collect();
        let shard_databases = moon::shard::shared_databases::ShardDatabases::new(all_dbs);

        // Spawn shard threads
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
                .name(format!("test-shard-{}", id))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build shard runtime");

                    let local = tokio::task::LocalSet::new();

                    let (snap_tx, snap_rx) = channel::watch(0u64);
                    let acl_t = std::sync::Arc::new(std::sync::RwLock::new(
                        moon::acl::AclTable::load_or_default(&shard_config),
                    ));
                    let rt_cfg = std::sync::Arc::new(std::sync::RwLock::new(
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

        // Run listener on this thread
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

    // Give the server a moment to bind and start shards
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (port, token)
}

#[tokio::test]
async fn test_sharded_ping_pong() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let result: String = redis::cmd("PING").query_async(&mut conn).await.unwrap();
    assert_eq!(result, "PONG");

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_set_get_across_shards() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // SET 100 random keys -- they will naturally distribute across 2 shards
    for i in 0..100 {
        let key = format!("shard_key_{}", i);
        let val = format!("shard_val_{}", i);
        let _: () = conn.set(&key, &val).await.unwrap();
    }

    // GET all of them back and verify correctness
    for i in 0..100 {
        let key = format!("shard_key_{}", i);
        let expected = format!("shard_val_{}", i);
        let val: String = conn.get(&key).await.unwrap();
        assert_eq!(val, expected, "Mismatch for key {}", key);
    }

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_mget_cross_shard() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // SET keys that will hash to different shards (no hash tags)
    let _: () = conn.set("alpha", "1").await.unwrap();
    let _: () = conn.set("beta", "2").await.unwrap();
    let _: () = conn.set("gamma", "3").await.unwrap();
    let _: () = conn.set("delta", "4").await.unwrap();

    // MGET all of them in one command -- exercises cross-shard multi-key coordinator
    let result: Vec<Option<String>> = redis::cmd("MGET")
        .arg("alpha")
        .arg("beta")
        .arg("gamma")
        .arg("delta")
        .arg("missing")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(
        result,
        vec![
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3".to_string()),
            Some("4".to_string()),
            None,
        ]
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_mset_cross_shard() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // MSET keys that will hash to different shards
    let _: () = redis::cmd("MSET")
        .arg("ms_a")
        .arg("v1")
        .arg("ms_b")
        .arg("v2")
        .arg("ms_c")
        .arg("v3")
        .arg("ms_d")
        .arg("v4")
        .query_async(&mut conn)
        .await
        .unwrap();

    // GET each key individually
    let v1: String = conn.get("ms_a").await.unwrap();
    let v2: String = conn.get("ms_b").await.unwrap();
    let v3: String = conn.get("ms_c").await.unwrap();
    let v4: String = conn.get("ms_d").await.unwrap();
    assert_eq!(v1, "v1");
    assert_eq!(v2, "v2");
    assert_eq!(v3, "v3");
    assert_eq!(v4, "v4");

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_hash_tag_co_location() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // Hash tags: {user:1}.name and {user:1}.email should be on the same shard
    let _: () = conn.set("{user:1}.name", "Alice").await.unwrap();
    let _: () = conn
        .set("{user:1}.email", "alice@example.com")
        .await
        .unwrap();

    // MGET both -- should work efficiently (same shard)
    let result: Vec<String> = redis::cmd("MGET")
        .arg("{user:1}.name")
        .arg("{user:1}.email")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, vec!["Alice", "alice@example.com"]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_del_multi_cross_shard() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // SET 10 keys distributed across shards
    for i in 0..10 {
        let key = format!("deltest_{}", i);
        let _: () = conn.set(&key, "val").await.unwrap();
    }

    // Verify they exist
    for i in 0..10 {
        let key = format!("deltest_{}", i);
        let exists: i64 = conn.exists(&key).await.unwrap();
        assert_eq!(exists, 1, "Key {} should exist before DEL", key);
    }

    // DEL all 10 keys in one command -- exercises multi-key cross-shard DEL
    let mut cmd = redis::cmd("DEL");
    for i in 0..10 {
        cmd.arg(format!("deltest_{}", i));
    }
    let deleted: i64 = cmd.query_async(&mut conn).await.unwrap();
    assert_eq!(deleted, 10);

    // Verify all removed
    for i in 0..10 {
        let key = format!("deltest_{}", i);
        let exists: i64 = conn.exists(&key).await.unwrap();
        assert_eq!(exists, 0, "Key {} should be gone after DEL", key);
    }

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_keys_pattern_all_shards() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // Insert keys with prefix "kptest:" distributed across shards
    for i in 0..20 {
        let key = format!("kptest:{}", i);
        let _: () = conn.set(&key, "val").await.unwrap();
    }

    // Also insert some keys with a different prefix
    let _: () = conn.set("other:1", "val").await.unwrap();
    let _: () = conn.set("other:2", "val").await.unwrap();

    // KEYS "kptest:*" should return all 20 keys across all shards
    let mut keys: Vec<String> = redis::cmd("KEYS")
        .arg("kptest:*")
        .query_async(&mut conn)
        .await
        .unwrap();
    keys.sort();
    assert_eq!(
        keys.len(),
        20,
        "KEYS should find all 20 kptest: keys across shards"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_scan_all_shards() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // Insert 50 keys distributed across shards
    for i in 0..50 {
        let key = format!("scantest_{}", i);
        let _: () = conn.set(&key, "val").await.unwrap();
    }

    // SCAN until cursor returns 0, collect all keys
    let mut all_keys: Vec<String> = Vec::new();
    let mut cursor: i64 = 0;
    let mut iterations = 0;
    loop {
        let (next_cursor, keys): (i64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .query_async(&mut conn)
            .await
            .unwrap();
        all_keys.extend(keys);
        cursor = next_cursor;
        iterations += 1;
        if cursor == 0 || iterations > 200 {
            break;
        }
    }

    // All 50 keys should be found
    all_keys.sort();
    all_keys.dedup();
    assert_eq!(
        all_keys.len(),
        50,
        "SCAN should find all 50 keys across shards, found: {}",
        all_keys.len()
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_concurrent_clients() {
    let (port, shutdown) = start_sharded_server(2).await;

    // Spawn 10 concurrent clients, each doing SET/GET on unique keys
    let mut handles = Vec::new();
    for i in 0..10u32 {
        let handle = tokio::spawn(async move {
            let mut conn = connect(port).await;
            for j in 0..10u32 {
                let key = format!("client_{}_{}", i, j);
                let val = format!("value_{}_{}", i, j);
                let _: () = conn.set(&key, &val).await.unwrap();
                let result: String = conn.get(&key).await.unwrap();
                assert_eq!(result, val);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify total key count
    let mut conn = connect(port).await;
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("client_*")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        keys.len(),
        100,
        "All 100 keys from concurrent clients should exist"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_pipeline() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let (set_result, get_result): (String, String) = redis::pipe()
        .cmd("SET")
        .arg("shpipe_key")
        .arg("shpipe_val")
        .cmd("GET")
        .arg("shpipe_key")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert_eq!(set_result, "OK");
    assert_eq!(get_result, "shpipe_val");

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_incr_decr() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let v: i64 = conn.incr("sh_counter", 1).await.unwrap();
    assert_eq!(v, 1);
    let v: i64 = conn.incr("sh_counter", 5).await.unwrap();
    assert_eq!(v, 6);
    let v: i64 = conn.decr("sh_counter", 2).await.unwrap();
    assert_eq!(v, 4);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_hash_commands() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let added: i64 = redis::cmd("HSET")
        .arg("sh_hash")
        .arg("name")
        .arg("Redis")
        .arg("version")
        .arg("7")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(added, 2);

    let val: String = redis::cmd("HGET")
        .arg("sh_hash")
        .arg("name")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(val, "Redis");

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_list_commands() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let len: i64 = redis::cmd("LPUSH")
        .arg("sh_list")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 3);

    let items: Vec<String> = redis::cmd("LRANGE")
        .arg("sh_list")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(items, vec!["c", "b", "a"]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_set_commands() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let added: i64 = redis::cmd("SADD")
        .arg("sh_set")
        .arg("x")
        .arg("y")
        .arg("z")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(added, 3);

    let card: i64 = redis::cmd("SCARD")
        .arg("sh_set")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(card, 3);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_sorted_set_commands() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let added: i64 = redis::cmd("ZADD")
        .arg("sh_zs")
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

    let members: Vec<String> = redis::cmd("ZRANGE")
        .arg("sh_zs")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(members, vec!["a", "b", "c"]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_expire_ttl() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    let _: () = conn.set("sh_ttl", "val").await.unwrap();
    let set: bool = conn.expire("sh_ttl", 100).await.unwrap();
    assert!(set);

    let ttl: i64 = conn.ttl("sh_ttl").await.unwrap();
    assert!(ttl > 0 && ttl <= 100, "TTL was {}", ttl);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_transaction_same_shard() {
    let (port, shutdown) = start_sharded_server(2).await;
    let mut conn = connect(port).await;

    // Use hash tags to co-locate keys on the same shard.
    // Verify that SET + GET of co-located keys works consistently.
    let _: () = conn.set("{txn}.a", "1").await.unwrap();
    let _: () = conn.set("{txn}.b", "2").await.unwrap();

    // MGET both co-located keys -- both on same shard
    let result: Vec<String> = redis::cmd("MGET")
        .arg("{txn}.a")
        .arg("{txn}.b")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, vec!["1", "2"]);

    // Test MULTI/EXEC via pipe().atomic() with hash-tagged keys
    let result: (String, String) = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("{txn}.c")
        .arg("3")
        .cmd("SET")
        .arg("{txn}.d")
        .arg("4")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result.0, "OK");
    assert_eq!(result.1, "OK");

    // Verify both keys set
    let c: String = conn.get("{txn}.c").await.unwrap();
    let d: String = conn.get("{txn}.d").await.unwrap();
    assert_eq!(c, "3");
    assert_eq!(d, "4");

    shutdown.cancel();
}

// =============================================================================
// Blocking commands (Phase 17)
// =============================================================================

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn blpop_immediate() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut conn = connect(port).await;

    // Push data first
    let _: i64 = redis::cmd("LPUSH")
        .arg("mylist")
        .arg("hello")
        .query_async(&mut conn)
        .await
        .unwrap();

    // BLPOP should return immediately since data exists
    let result: (String, String) = redis::cmd("BLPOP")
        .arg("mylist")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result.0, "mylist");
    assert_eq!(result.1, "hello");

    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn brpop_immediate() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut conn = connect(port).await;

    // Push multiple values
    let _: i64 = redis::cmd("RPUSH")
        .arg("mylist")
        .arg("first")
        .arg("second")
        .query_async(&mut conn)
        .await
        .unwrap();

    // BRPOP pops from tail
    let result: (String, String) = redis::cmd("BRPOP")
        .arg("mylist")
        .arg(1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result.0, "mylist");
    assert_eq!(result.1, "second");

    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (io_uring blocking command path)
async fn blpop_blocks_then_wakes() {
    let (port, shutdown) = start_sharded_server(1).await;

    // Use non-multiplexed connection for blocking
    let client1 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut con1 = client1.get_multiplexed_async_connection().await.unwrap();

    let mut con2 = connect(port).await;

    let start = std::time::Instant::now();

    // Task A: BLPOP (blocks)
    let handle = tokio::spawn(async move {
        let result: (String, String) = redis::cmd("BLPOP")
            .arg("blocklist")
            .arg(5)
            .query_async(&mut con1)
            .await
            .unwrap();
        result
    });

    // Task B: wait then push
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let _: i64 = redis::cmd("LPUSH")
        .arg("blocklist")
        .arg("wakeup")
        .query_async(&mut con2)
        .await
        .unwrap();

    let result = handle.await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(result.0, "blocklist");
    assert_eq!(result.1, "wakeup");
    // Should have taken ~200ms (not 5s)
    assert!(elapsed.as_millis() < 2000, "took too long: {:?}", elapsed);

    shutdown.cancel();
}

#[tokio::test]
async fn blpop_timeout() {
    let (port, shutdown) = start_sharded_server(1).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let start = std::time::Instant::now();
    // BLPOP on empty key with short timeout
    let result: Option<(String, String)> = redis::cmd("BLPOP")
        .arg("emptylist")
        .arg("0.2")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert!(
        result.is_none(),
        "expected nil on timeout, got {:?}",
        result
    );
    // Should have taken ~200ms
    assert!(
        elapsed.as_millis() >= 150,
        "returned too quickly: {:?}",
        elapsed
    );
    assert!(elapsed.as_millis() < 1000, "took too long: {:?}", elapsed);

    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn blpop_multi_key_first_nonempty() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut conn = connect(port).await;

    // Only push to key2, not key1
    let _: i64 = redis::cmd("LPUSH")
        .arg("key2")
        .arg("found")
        .query_async(&mut conn)
        .await
        .unwrap();

    // BLPOP checks keys in order -- key1 is empty, key2 has data
    let result: (String, String) = redis::cmd("BLPOP")
        .arg("key1")
        .arg("key2")
        .arg("0.5")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result.0, "key2");
    assert_eq!(result.1, "found");

    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn blpop_cross_shard_wakeup() {
    // 4 shards: high probability keys land on different shards
    let (port, shutdown) = start_sharded_server(4).await;

    let client1 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut con1 = client1.get_multiplexed_async_connection().await.unwrap();
    let mut con2 = connect(port).await;

    // BLPOP blocks on two keys; push to the second key from another connection
    let blpop_handle = tokio::spawn(async move {
        let result: (String, String) = redis::cmd("BLPOP")
            .arg("xshard_a1")
            .arg("xshard_b2")
            .arg(5)
            .query_async(&mut con1)
            .await
            .unwrap();
        result
    });

    // Wait for registration to propagate
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Push to second key (may be on a different shard)
    let _: i64 = redis::cmd("LPUSH")
        .arg("xshard_b2")
        .arg("woke")
        .query_async(&mut con2)
        .await
        .unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_secs(4), blpop_handle)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.0, "xshard_b2");
    assert_eq!(result.1, "woke");
    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (io_uring blocking command path)
async fn blpop_cross_shard_timeout() {
    let (port, shutdown) = start_sharded_server(4).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();

    let start = std::time::Instant::now();
    let result: Option<(String, String)> = redis::cmd("BLPOP")
        .arg("timeout_a")
        .arg("timeout_b")
        .arg("0.3")
        .query_async(&mut conn)
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert!(result.is_none(), "Expected nil on timeout");
    assert!(
        elapsed.as_millis() >= 250,
        "Timed out too early: {}ms",
        elapsed.as_millis()
    );
    assert!(
        elapsed.as_millis() < 1500,
        "Timed out too late: {}ms",
        elapsed.as_millis()
    );
    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn blpop_multi_key_all_local_regression() {
    // 1 shard = all keys local (regression guard for multi-key path)
    let (port, shutdown) = start_sharded_server(1).await;

    let client1 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut con1 = client1.get_multiplexed_async_connection().await.unwrap();
    let mut con2 = connect(port).await;

    let blpop_handle = tokio::spawn(async move {
        let result: (String, String) = redis::cmd("BLPOP")
            .arg("localA")
            .arg("localB")
            .arg("localC")
            .arg(5)
            .query_async(&mut con1)
            .await
            .unwrap();
        result
    });

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Push to middle key
    let _: i64 = redis::cmd("LPUSH")
        .arg("localB")
        .arg("found_it")
        .query_async(&mut con2)
        .await
        .unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_secs(4), blpop_handle)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.0, "localB");
    assert_eq!(result.1, "found_it");
    shutdown.cancel();
}

#[tokio::test]
async fn lmove_basic() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut conn = connect(port).await;

    // RPUSH source a b c
    let _: i64 = redis::cmd("RPUSH")
        .arg("source")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();

    // LMOVE source dest LEFT RIGHT -> moves "a" to dest
    let result: String = redis::cmd("LMOVE")
        .arg("source")
        .arg("dest")
        .arg("LEFT")
        .arg("RIGHT")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "a");

    // dest should have [a]
    let dest_vals: Vec<String> = redis::cmd("LRANGE")
        .arg("dest")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(dest_vals, vec!["a"]);

    // LMOVE source dest RIGHT LEFT -> moves "c" to front of dest
    let result2: String = redis::cmd("LMOVE")
        .arg("source")
        .arg("dest")
        .arg("RIGHT")
        .arg("LEFT")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result2, "c");

    let dest_vals2: Vec<String> = redis::cmd("LRANGE")
        .arg("dest")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(dest_vals2, vec!["c", "a"]);

    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn bzpopmin_immediate() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut conn = connect(port).await;

    // ZADD key 1.0 "a" 2.0 "b"
    let _: i64 = redis::cmd("ZADD")
        .arg("myzset")
        .arg(1.0f64)
        .arg("a")
        .arg(2.0f64)
        .arg("b")
        .query_async(&mut conn)
        .await
        .unwrap();

    // BZPOPMIN returns the minimum
    let result: (String, String, String) = redis::cmd("BZPOPMIN")
        .arg("myzset")
        .arg("0.1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result.0, "myzset");
    assert_eq!(result.1, "a");
    assert_eq!(result.2, "1");

    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn bzpopmin_blocks_then_wakes() {
    let (port, shutdown) = start_sharded_server(1).await;

    let client1 = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    let mut con1 = client1.get_multiplexed_async_connection().await.unwrap();

    let mut con2 = connect(port).await;

    let start = std::time::Instant::now();

    // Task A: BZPOPMIN (blocks)
    let handle = tokio::spawn(async move {
        let result: (String, String, String) = redis::cmd("BZPOPMIN")
            .arg("blockzset")
            .arg(5)
            .query_async(&mut con1)
            .await
            .unwrap();
        result
    });

    // Task B: wait then ZADD
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let _: i64 = redis::cmd("ZADD")
        .arg("blockzset")
        .arg(3.5f64)
        .arg("member1")
        .query_async(&mut con2)
        .await
        .unwrap();

    let result = handle.await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(result.0, "blockzset");
    assert_eq!(result.1, "member1");
    // Should have taken ~200ms, not 5s
    assert!(elapsed.as_millis() < 2000, "took too long: {:?}", elapsed);

    shutdown.cancel();
}

#[tokio::test]
#[ignore] // hangs on CI (blocking command + io_uring)
async fn bzpopmax_immediate() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut conn = connect(port).await;

    let _: i64 = redis::cmd("ZADD")
        .arg("myzset2")
        .arg(1.0f64)
        .arg("a")
        .arg(2.0f64)
        .arg("b")
        .query_async(&mut conn)
        .await
        .unwrap();

    let result: (String, String, String) = redis::cmd("BZPOPMAX")
        .arg("myzset2")
        .arg("0.1")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result.0, "myzset2");
    assert_eq!(result.1, "b");
    assert_eq!(result.2, "2");

    shutdown.cancel();
}

#[tokio::test]
async fn blmove_basic() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut conn = connect(port).await;

    // RPUSH source a b c
    let _: i64 = redis::cmd("RPUSH")
        .arg("blmsrc")
        .arg("a")
        .arg("b")
        .arg("c")
        .query_async(&mut conn)
        .await
        .unwrap();

    // BLMOVE source dest LEFT RIGHT 1.0 -> moves "a"
    let result: String = redis::cmd("BLMOVE")
        .arg("blmsrc")
        .arg("blmdest")
        .arg("LEFT")
        .arg("RIGHT")
        .arg("1.0")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "a");

    // dest should have [a]
    let dest_vals: Vec<String> = redis::cmd("LRANGE")
        .arg("blmdest")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(dest_vals, vec!["a"]);

    shutdown.cancel();
}

// --- Cluster Integration Tests ---

/// Start a cluster-enabled sharded server on a random port.
async fn start_cluster_server() -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();
    let cancel = token.clone();

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
        shards: 1,
        cluster_enabled: true,
        cluster_node_timeout: 15000,
        aclfile: None,
        protected_mode: "yes".to_string(),
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
    };

    std::thread::spawn(move || {
        let num_shards = 1;
        let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);
        let conn_txs: Vec<channel::MpscSender<(tokio::net::TcpStream, bool)>> =
            (0..num_shards).map(|i| mesh.conn_tx(i)).collect();

        // Initialize cluster state
        moon::cluster::CLUSTER_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
        let self_addr: std::net::SocketAddr = format!("127.0.0.1:{}", config.port).parse().unwrap();
        let node_id = moon::replication::state::generate_repl_id();
        let state = moon::cluster::ClusterState::new(node_id, self_addr);
        let cluster_state = Some(std::sync::Arc::new(std::sync::RwLock::new(state)));

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

        // Create shards on main thread, extract databases for SharedDatabases
        let mut shards: Vec<Shard> = (0..num_shards)
            .map(|id| Shard::new(id, num_shards, config.databases, config.to_runtime_config()))
            .collect();
        let all_dbs: Vec<Vec<moon::storage::Database>> = shards
            .iter_mut()
            .map(|s| std::mem::take(&mut s.databases))
            .collect();
        let shard_databases = moon::shard::shared_databases::ShardDatabases::new(all_dbs);

        // Spawn shard threads
        let mut shard_handles = Vec::with_capacity(num_shards);
        for (id, mut shard) in shards.into_iter().enumerate() {
            let producers = mesh.take_producers(id);
            let consumers = mesh.take_consumers(id);
            let conn_rx = mesh.take_conn_rx(id);
            let shard_config = config.clone();
            let shard_cancel = cancel.clone();
            let shard_cs = cluster_state.clone();
            let shard_spsc_notify = mesh.take_notify(id);
            let shard_all_notifiers = all_notifiers.clone();
            let shard_dbs = shard_databases.clone();
            let shard_pubsub_regs = all_pubsub_registries.clone();
            let shard_remote_sub_maps = all_remote_sub_maps.clone();
            let shard_affinity = affinity_tracker.clone();

            let handle = std::thread::Builder::new()
                .name(format!("test-cluster-shard-{}", id))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build shard runtime");

                    let local = tokio::task::LocalSet::new();

                    let (snap_tx, snap_rx) = channel::watch(0u64);
                    let acl_t = std::sync::Arc::new(std::sync::RwLock::new(
                        moon::acl::AclTable::load_or_default(&shard_config),
                    ));
                    let rt_cfg = std::sync::Arc::new(std::sync::RwLock::new(
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
                        shard_cs,
                        shard_config.port,
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

        // Run listener on this thread
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

    // Give the server time to bind and start shards
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (port, token)
}

/// CLUSTER-06: CLUSTER INFO returns cluster_enabled:1 when started with --cluster-enabled.
#[tokio::test]
async fn cluster_info() {
    let (port, shutdown) = start_cluster_server().await;
    let mut con = connect(port).await;

    let info: String = redis::cmd("CLUSTER")
        .arg("INFO")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        info.contains("cluster_enabled:1"),
        "expected cluster_enabled:1 in: {}",
        info
    );
    assert!(
        info.contains("cluster_state:ok"),
        "expected cluster_state:ok in: {}",
        info
    );

    shutdown.cancel();
}

/// CLUSTER-07: CLUSTER MYID returns a 40-char hex string.
#[tokio::test]
async fn cluster_myid() {
    let (port, shutdown) = start_cluster_server().await;
    let mut con = connect(port).await;

    let myid: String = redis::cmd("CLUSTER")
        .arg("MYID")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(myid.len(), 40, "node ID should be 40 chars, got: {}", myid);
    assert!(
        myid.chars().all(|c| c.is_ascii_hexdigit()),
        "node ID not hex: {}",
        myid
    );

    shutdown.cancel();
}

/// CLUSTER-08: ADDSLOTS + SET/GET routes locally.
#[tokio::test]
async fn cluster_addslots() {
    let (port, shutdown) = start_cluster_server().await;
    let mut con = connect(port).await;

    // Assign all 16384 slots to ourselves for full local routing
    for chunk_start in (0u16..16384).step_by(100) {
        let chunk_end = (chunk_start + 100).min(16384);
        let mut cmd = redis::cmd("CLUSTER");
        cmd.arg("ADDSLOTS");
        for s in chunk_start..chunk_end {
            cmd.arg(s);
        }
        let _: () = cmd.query_async(&mut con).await.unwrap();
    }

    // SET "foo" should succeed (slot 12182 is local)
    let _: () = redis::cmd("SET")
        .arg("foo")
        .arg("bar")
        .query_async(&mut con)
        .await
        .unwrap();
    let val: String = redis::cmd("GET")
        .arg("foo")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(val, "bar");

    shutdown.cancel();
}

/// CLUSTER-09: CLUSTER KEYSLOT returns correct slot.
#[tokio::test]
async fn cluster_keyslot() {
    let (port, shutdown) = start_cluster_server().await;
    let mut con = connect(port).await;

    let slot: i64 = redis::cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg("foo")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(slot, 12182, "CLUSTER KEYSLOT foo should return 12182");

    shutdown.cancel();
}

/// CLUSTER-13: CLUSTER NODES output has correct fields.
#[tokio::test]
async fn cluster_nodes() {
    let (port, shutdown) = start_cluster_server().await;
    let mut con = connect(port).await;

    let nodes_output: String = redis::cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut con)
        .await
        .unwrap();

    assert!(
        !nodes_output.trim().is_empty(),
        "CLUSTER NODES should not be empty"
    );
    for line in nodes_output.trim().lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert!(
            fields.len() >= 9,
            "CLUSTER NODES line should have >= 9 fields, got {}: {}",
            fields.len(),
            line
        );
        // First field is 40-char hex node ID
        assert_eq!(fields[0].len(), 40, "node ID not 40 chars: {}", fields[0]);
    }

    shutdown.cancel();
}

// ============================================================
// Phase 21: Lua Scripting Engine
// ============================================================

#[tokio::test]
async fn test_eval_basic() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let result: String = redis::cmd("EVAL")
        .arg("return KEYS[1]")
        .arg(1)
        .arg("mykey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "mykey");

    shutdown.cancel();
}

#[tokio::test]
async fn test_eval_argv() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let result: String = redis::cmd("EVAL")
        .arg("return ARGV[1]")
        .arg(0)
        .arg("hello")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "hello");

    shutdown.cancel();
}

#[tokio::test]
async fn test_evalsha() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    // SCRIPT LOAD returns SHA1
    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return KEYS[1]")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(sha.len(), 40);

    // EVALSHA with that SHA1 should return the same result
    let result: String = redis::cmd("EVALSHA")
        .arg(&sha)
        .arg(1)
        .arg("testkey")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "testkey");

    shutdown.cancel();
}

#[tokio::test]
async fn test_evalsha_noscript() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let result = redis::cmd("EVALSHA")
        .arg("0000000000000000000000000000000000000000")
        .arg(0)
        .query_async::<String>(&mut con)
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("NOSCRIPT") || err_msg.contains("NoScript"),
        "expected NOSCRIPT, got: {err_msg}"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_script_load() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 42")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(sha.len(), 40);
    assert!(sha.chars().all(|c| c.is_ascii_hexdigit()));

    shutdown.cancel();
}

#[tokio::test]
async fn test_script_exists() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 1")
        .query_async(&mut con)
        .await
        .unwrap();

    let results: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&sha)
        .arg("0000000000000000000000000000000000000000")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results, vec![1, 0]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_script_flush() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let sha: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg("return 1")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: String = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut con)
        .await
        .unwrap();

    let results: Vec<i64> = redis::cmd("SCRIPT")
        .arg("EXISTS")
        .arg(&sha)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(results, vec![0]);

    shutdown.cancel();
}

#[tokio::test]
async fn test_sandbox_restrictions() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    // load() should be sandboxed
    let result = redis::cmd("EVAL")
        .arg("return load('return 1')()")
        .arg(0)
        .query_async::<redis::Value>(&mut con)
        .await;
    assert!(result.is_err(), "load() should be sandboxed");

    // dofile should be sandboxed
    let result2 = redis::cmd("EVAL")
        .arg("return dofile('/tmp/x')")
        .arg(0)
        .query_async::<redis::Value>(&mut con)
        .await;
    assert!(result2.is_err(), "dofile() should be sandboxed");

    shutdown.cancel();
}

#[tokio::test]
async fn test_sandbox_allowed() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let result: String = redis::cmd("EVAL")
        .arg("return string.upper('hello')")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "HELLO");

    shutdown.cancel();
}

#[tokio::test]
async fn test_redis_call() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let result: String = redis::cmd("EVAL")
        .arg("redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])")
        .arg(1)
        .arg("lua_test_key")
        .arg("lua_test_value")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "lua_test_value");

    // Verify the key was actually set in the database
    let direct: String = redis::cmd("GET")
        .arg("lua_test_key")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(direct, "lua_test_value");

    shutdown.cancel();
}

#[tokio::test]
async fn test_redis_pcall() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    // Call an invalid command -- pcall catches it as {err=...} table
    let result: String = redis::cmd("EVAL")
        .arg("local r = redis.pcall('NOTACOMMAND'); if r.err then return 'caught' else return 'uncaught' end")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "caught");

    shutdown.cancel();
}

#[tokio::test]
async fn test_script_timeout() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    let result = redis::cmd("EVAL")
        .arg("local i = 0; while true do i = i + 1 end")
        .arg(0)
        .query_async::<redis::Value>(&mut con)
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("BUSY")
            || err_msg.contains("timeout")
            || err_msg.contains("timed out")
            || err_msg.contains("script"),
        "expected timeout error, got: {err_msg}"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_eval_return_types() {
    let (port, shutdown) = start_sharded_server(1).await;
    let mut con = connect(port).await;

    // Integer return
    let n: i64 = redis::cmd("EVAL")
        .arg("return 42")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(n, 42);

    // Float truncated to integer (Redis behavior)
    let f: i64 = redis::cmd("EVAL")
        .arg("return 3.99")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(f, 3);

    // Nil returns as null/nil
    let nil_result: redis::Value = redis::cmd("EVAL")
        .arg("return nil")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(matches!(nil_result, redis::Value::Nil));

    // Table returns as array
    let arr: Vec<i64> = redis::cmd("EVAL")
        .arg("return {1, 2, 3}")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(arr, vec![1, 2, 3]);

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// ACL integration tests (Phase 22-03)
// ---------------------------------------------------------------------------

/// Start a server with an aclfile configured (for ACL SAVE/LOAD tests).
async fn start_server_with_aclfile(acl_path: &str) -> (u16, CancellationToken) {
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
        aclfile: Some(acl_path.to_string()),
        protected_mode: "yes".to_string(),
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
    };

    tokio::spawn(async move {
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (port, token)
}

/// ACL-01 + ACL-02: SETUSER creates user, GETUSER returns user info
#[tokio::test]
async fn test_acl_setuser_and_getuser() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // ACL SETUSER alice on >secret ~* +@all
    let r: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("alice")
        .arg("on")
        .arg(">secret")
        .arg("~*")
        .arg("+@all")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // ACL GETUSER alice -- returns array with username, flags, etc.
    let r: redis::Value = redis::cmd("ACL")
        .arg("GETUSER")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();

    // Flatten to string for inspection
    let s = format!("{:?}", r);
    assert!(
        s.contains("alice"),
        "GETUSER should contain username alice, got: {}",
        s
    );
    assert!(
        s.contains("on"),
        "GETUSER flags should contain 'on', got: {}",
        s
    );

    shutdown.cancel();
}

/// ACL-02: LIST shows users, DELUSER removes them
#[tokio::test]
async fn test_acl_list_and_deluser() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Create alice
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("alice")
        .arg("on")
        .arg(">secret")
        .arg("~*")
        .arg("+@all")
        .query_async(&mut con)
        .await
        .unwrap();

    // ACL LIST should include alice
    let list: Vec<String> = redis::cmd("ACL")
        .arg("LIST")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        list.iter().any(|l| l.contains("alice")),
        "ACL LIST should contain alice, got: {:?}",
        list
    );

    // ACL DELUSER alice -> 1
    let count: i64 = redis::cmd("ACL")
        .arg("DELUSER")
        .arg("alice")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(count, 1);

    // After delete, AUTH alice secret should fail with WRONGPASS
    let r: redis::RedisResult<String> = redis::cmd("AUTH")
        .arg("alice")
        .arg("secret")
        .query_async(&mut con)
        .await;
    assert!(r.is_err());
    let err = format!("{}", r.unwrap_err());
    assert!(
        err.contains("WRONGPASS"),
        "Expected WRONGPASS after delete, got: {}",
        err
    );

    shutdown.cancel();
}

/// ACL-02: WHOAMI returns correct username after AUTH
#[tokio::test]
async fn test_acl_whoami() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Default user: WHOAMI -> "default"
    let who: String = redis::cmd("ACL")
        .arg("WHOAMI")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(who, "default");

    // Create alice, AUTH as alice, WHOAMI -> "alice"
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("alice")
        .arg("on")
        .arg(">secret")
        .arg("~*")
        .arg("+@all")
        .query_async(&mut con)
        .await
        .unwrap();

    let _: String = redis::cmd("AUTH")
        .arg("alice")
        .arg("secret")
        .query_async(&mut con)
        .await
        .unwrap();

    let who: String = redis::cmd("ACL")
        .arg("WHOAMI")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(who, "alice");

    shutdown.cancel();
}

/// ACL-03: Two-argument AUTH with correct and incorrect passwords
#[tokio::test]
async fn test_acl_auth_two_arg() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Create alice
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("alice")
        .arg("on")
        .arg(">secret")
        .arg("~*")
        .arg("+@all")
        .query_async(&mut con)
        .await
        .unwrap();

    // AUTH alice secret -> OK
    let r: String = redis::cmd("AUTH")
        .arg("alice")
        .arg("secret")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // AUTH alice wrongpass -> WRONGPASS
    let r: redis::RedisResult<String> = redis::cmd("AUTH")
        .arg("alice")
        .arg("wrongpass")
        .query_async(&mut con)
        .await;
    assert!(r.is_err());
    let err = format!("{}", r.unwrap_err());
    assert!(
        err.contains("WRONGPASS"),
        "Expected WRONGPASS, got: {}",
        err
    );

    // AUTH nonexistent pass -> WRONGPASS
    let r: redis::RedisResult<String> = redis::cmd("AUTH")
        .arg("nonexistent")
        .arg("pass")
        .query_async(&mut con)
        .await;
    assert!(r.is_err());
    let err = format!("{}", r.unwrap_err());
    assert!(
        err.contains("WRONGPASS"),
        "Expected WRONGPASS for nonexistent user, got: {}",
        err
    );

    shutdown.cancel();
}

/// ACL-04: Default user backward compat with requirepass
#[tokio::test]
async fn test_acl_default_user_compat() {
    let (port, shutdown) = start_server_with_pass("mysecret").await;
    let mut con = connect_single(port).await;

    // 1-arg AUTH mysecret -> OK
    let r: String = redis::cmd("AUTH")
        .arg("mysecret")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // 2-arg AUTH default mysecret -> OK
    let r: String = redis::cmd("AUTH")
        .arg("default")
        .arg("mysecret")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // AUTH wrongpass -> WRONGPASS
    let r: redis::RedisResult<String> = redis::cmd("AUTH")
        .arg("wrongpass")
        .query_async(&mut con)
        .await;
    assert!(r.is_err());
    let err = format!("{}", r.unwrap_err());
    assert!(
        err.contains("WRONGPASS"),
        "Expected WRONGPASS, got: {}",
        err
    );

    shutdown.cancel();
}

/// ACL-05: NOPERM for denied commands (user with restricted command permissions)
#[tokio::test]
async fn test_acl_noperm_denied_write() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Create bob with +@all -@write (can read, cannot write)
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("bob")
        .arg("on")
        .arg(">bobpass")
        .arg("~*")
        .arg("+@all")
        .arg("-@write")
        .query_async(&mut con)
        .await
        .unwrap();

    // AUTH as bob
    let _: String = redis::cmd("AUTH")
        .arg("bob")
        .arg("bobpass")
        .query_async(&mut con)
        .await
        .unwrap();

    // GET should work (returns nil since key doesn't exist)
    let r: redis::RedisResult<Option<String>> =
        redis::cmd("GET").arg("somekey").query_async(&mut con).await;
    assert!(r.is_ok(), "GET should be allowed for bob, got: {:?}", r);

    // SET should fail with NOPERM
    let r: redis::RedisResult<String> = redis::cmd("SET")
        .arg("somekey")
        .arg("value")
        .query_async(&mut con)
        .await;
    assert!(r.is_err());
    let err = format!("{}", r.unwrap_err());
    assert!(
        err.contains("NOPERM") || err.contains("NoPerm"),
        "Expected NOPERM error for SET, got: {}",
        err
    );

    shutdown.cancel();
}

/// ACL-06: Key pattern restriction - single key
#[tokio::test]
async fn test_acl_key_pattern_single() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Create user restricted with ~cache:* key pattern
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("restricted")
        .arg("on")
        .arg(">pass")
        .arg("~cache:*")
        .arg("+@all")
        .query_async(&mut con)
        .await
        .unwrap();

    // AUTH as restricted
    let _: String = redis::cmd("AUTH")
        .arg("restricted")
        .arg("pass")
        .query_async(&mut con)
        .await
        .unwrap();

    // SET cache:foo bar -> OK
    let r: String = redis::cmd("SET")
        .arg("cache:foo")
        .arg("bar")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // GET cache:foo -> OK
    let r: String = redis::cmd("GET")
        .arg("cache:foo")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "bar");

    // SET other:key bar -> NOPERM
    let r: redis::RedisResult<String> = redis::cmd("SET")
        .arg("other:key")
        .arg("bar")
        .query_async(&mut con)
        .await;
    assert!(r.is_err());
    let err = format!("{}", r.unwrap_err());
    assert!(
        err.contains("NOPERM") || err.contains("NoPerm"),
        "Expected NOPERM for key outside pattern, got: {}",
        err
    );

    shutdown.cancel();
}

/// ACL-06: Key pattern restriction - MSET with mixed keys
#[tokio::test]
async fn test_acl_key_pattern_mset() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Create user restricted with ~cache:* key pattern
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("restricted")
        .arg("on")
        .arg(">pass")
        .arg("~cache:*")
        .arg("+@all")
        .query_async(&mut con)
        .await
        .unwrap();

    // AUTH as restricted
    let _: String = redis::cmd("AUTH")
        .arg("restricted")
        .arg("pass")
        .query_async(&mut con)
        .await
        .unwrap();

    // MSET cache:a 1 other:b 2 -> NOPERM (other:b fails)
    let r: redis::RedisResult<String> = redis::cmd("MSET")
        .arg("cache:a")
        .arg("1")
        .arg("other:b")
        .arg("2")
        .query_async(&mut con)
        .await;
    assert!(r.is_err());
    let err = format!("{}", r.unwrap_err());
    assert!(
        err.contains("NOPERM") || err.contains("NoPerm"),
        "Expected NOPERM for MSET with mixed keys, got: {}",
        err
    );

    shutdown.cancel();
}

/// ACL-07: ACL LOG records denied commands
#[tokio::test]
async fn test_acl_log_records_denial() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Create bob with -@write
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("bob")
        .arg("on")
        .arg(">bobpass")
        .arg("~*")
        .arg("+@all")
        .arg("-@write")
        .query_async(&mut con)
        .await
        .unwrap();

    // AUTH as bob
    let _: String = redis::cmd("AUTH")
        .arg("bob")
        .arg("bobpass")
        .query_async(&mut con)
        .await
        .unwrap();

    // Attempt denied SET
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("bob_key")
        .arg("val")
        .query_async(&mut con)
        .await;

    // ACL LOG (on same connection -- log is per-connection)
    let log: Vec<redis::Value> = redis::cmd("ACL")
        .arg("LOG")
        .query_async(&mut con)
        .await
        .unwrap();

    assert!(
        !log.is_empty(),
        "ACL LOG should have at least one entry after denial"
    );

    // Check the entry contains reason, username, object
    let entry = format!("{:?}", log[0]);
    assert!(
        entry.contains("command"),
        "ACL LOG entry should contain reason='command', got: {}",
        entry
    );
    assert!(
        entry.contains("bob"),
        "ACL LOG entry should contain username='bob', got: {}",
        entry
    );

    shutdown.cancel();
}

/// ACL-07: ACL LOG RESET clears log; ACL LOG count limits entries
#[tokio::test]
async fn test_acl_log_reset_and_count() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // Create bob with -@write
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("bob")
        .arg("on")
        .arg(">bobpass")
        .arg("~*")
        .arg("+@all")
        .arg("-@write")
        .query_async(&mut con)
        .await
        .unwrap();

    // AUTH as bob
    let _: String = redis::cmd("AUTH")
        .arg("bob")
        .arg("bobpass")
        .query_async(&mut con)
        .await
        .unwrap();

    // Trigger two denials
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("k1")
        .arg("v1")
        .query_async(&mut con)
        .await;
    let _: redis::RedisResult<String> = redis::cmd("SET")
        .arg("k2")
        .arg("v2")
        .query_async(&mut con)
        .await;

    // ACL LOG 1 -> at most 1 entry
    let log: Vec<redis::Value> = redis::cmd("ACL")
        .arg("LOG")
        .arg("1")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(log.len(), 1, "ACL LOG 1 should return exactly 1 entry");

    // ACL LOG RESET -> OK
    let r: String = redis::cmd("ACL")
        .arg("LOG")
        .arg("RESET")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // ACL LOG should now be empty
    let log: Vec<redis::Value> = redis::cmd("ACL")
        .arg("LOG")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        log.is_empty(),
        "ACL LOG should be empty after RESET, got {} entries",
        log.len()
    );

    shutdown.cancel();
}

/// ACL-08: SAVE writes ACL file, LOAD reloads it, users survive round-trip
#[tokio::test]
async fn test_acl_save_load() {
    let acl_path = std::env::temp_dir()
        .join(format!("test_acl_{}.acl", std::process::id()))
        .to_string_lossy()
        .to_string();

    // Clean up any previous file
    let _ = std::fs::remove_file(&acl_path);

    let (port, shutdown) = start_server_with_aclfile(&acl_path).await;
    let mut con = connect_single(port).await;

    // Create persisted_user
    let _: String = redis::cmd("ACL")
        .arg("SETUSER")
        .arg("persisted_user")
        .arg("on")
        .arg(">pass")
        .arg("~*")
        .arg("+@all")
        .query_async(&mut con)
        .await
        .unwrap();

    // ACL SAVE -> OK
    let r: String = redis::cmd("ACL")
        .arg("SAVE")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // Verify file exists and contains the user
    let content = std::fs::read_to_string(&acl_path).unwrap();
    assert!(
        content.contains("persisted_user"),
        "ACL file should contain persisted_user, got: {}",
        content
    );
    assert!(
        content.contains("user "),
        "ACL file should start lines with 'user ', got: {}",
        content
    );

    // ACL LOAD -> OK (reload from file)
    let r: String = redis::cmd("ACL")
        .arg("LOAD")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(r, "OK");

    // After LOAD, persisted_user should still exist
    let user: redis::Value = redis::cmd("ACL")
        .arg("GETUSER")
        .arg("persisted_user")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        !matches!(user, redis::Value::Nil),
        "persisted_user should exist after ACL LOAD"
    );

    // Clean up
    let _ = std::fs::remove_file(&acl_path);
    shutdown.cancel();
}

/// ACL CAT smoke test: categories list and per-category commands
#[tokio::test]
async fn test_acl_cat() {
    let (port, shutdown) = start_server().await;
    let mut con = connect_single(port).await;

    // ACL CAT -> list of category names
    let cats: Vec<String> = redis::cmd("ACL")
        .arg("CAT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        !cats.is_empty(),
        "ACL CAT should return non-empty list of categories"
    );
    assert!(
        cats.iter().any(|c| c == "string"),
        "ACL CAT should include 'string' category, got: {:?}",
        cats
    );

    // ACL CAT @string -> list containing "get", "set", etc.
    let cmds: Vec<String> = redis::cmd("ACL")
        .arg("CAT")
        .arg("@string")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        !cmds.is_empty(),
        "ACL CAT @string should return non-empty list"
    );
    assert!(
        cmds.iter().any(|c| c == "get"),
        "ACL CAT @string should contain 'get', got: {:?}",
        cmds
    );
    assert!(
        cmds.iter().any(|c| c == "set"),
        "ACL CAT @string should contain 'set', got: {:?}",
        cmds
    );

    shutdown.cancel();
}

// ===== Phase 56: Sharded Pub/Sub Integration Tests =====

#[tokio::test]
async fn test_sharded_pubsub_subscribe() {
    use futures::StreamExt;

    let (port, shutdown) = start_sharded_server(4).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Subscriber
    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("test-channel").await.unwrap();

    // Publisher (separate connection)
    let mut pub_conn = connect(port).await;

    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("test-channel")
        .arg("hello-sharded")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert!(
        receivers >= 1,
        "PUBLISH should return at least 1 subscriber, got {}",
        receivers
    );

    let msg: redis::Msg = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        pubsub.on_message().next(),
    )
    .await
    .expect("timed out waiting for pubsub message")
    .expect("stream ended unexpectedly");

    assert_eq!(msg.get_channel_name(), "test-channel");
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "hello-sharded");

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_pubsub_psubscribe() {
    use futures::StreamExt;

    let (port, shutdown) = start_sharded_server(4).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.psubscribe("news.*").await.unwrap();

    let mut pub_conn = connect(port).await;

    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("news.sports")
        .arg("goal!")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert!(receivers >= 1, "PUBLISH should return at least 1");

    let msg: redis::Msg = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        pubsub.on_message().next(),
    )
    .await
    .expect("timed out")
    .expect("stream ended");

    assert_eq!(msg.get_channel_name(), "news.sports");
    let payload: String = msg.get_payload().unwrap();
    assert_eq!(payload, "goal!");

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_pubsub_publish_count() {
    let (port, shutdown) = start_sharded_server(4).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Two subscribers on same channel (may land on different shards)
    let mut pubsub1 = client.get_async_pubsub().await.unwrap();
    pubsub1.subscribe("shared-ch").await.unwrap();

    let mut pubsub2 = client.get_async_pubsub().await.unwrap();
    pubsub2.subscribe("shared-ch").await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut pub_conn = connect(port).await;
    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("shared-ch")
        .arg("hello")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(
        receivers, 2,
        "PUBLISH should return 2 (two subscribers across shards)"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_pubsub_channels() {
    let (port, shutdown) = start_sharded_server(4).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Subscribe to a few channels on different connections (may hit different shards)
    let mut ps1 = client.get_async_pubsub().await.unwrap();
    ps1.subscribe("alpha").await.unwrap();

    let mut ps2 = client.get_async_pubsub().await.unwrap();
    ps2.subscribe("beta").await.unwrap();

    let mut ps3 = client.get_async_pubsub().await.unwrap();
    ps3.subscribe("gamma").await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut conn = connect(port).await;
    let channels: Vec<String> = redis::cmd("PUBSUB")
        .arg("CHANNELS")
        .query_async(&mut conn)
        .await
        .unwrap();

    assert!(
        channels.contains(&"alpha".to_string()),
        "Missing alpha in {:?}",
        channels
    );
    assert!(
        channels.contains(&"beta".to_string()),
        "Missing beta in {:?}",
        channels
    );
    assert!(
        channels.contains(&"gamma".to_string()),
        "Missing gamma in {:?}",
        channels
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_pubsub_numsub() {
    let (port, shutdown) = start_sharded_server(4).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    let mut ps1 = client.get_async_pubsub().await.unwrap();
    ps1.subscribe("ch1").await.unwrap();

    let mut ps2 = client.get_async_pubsub().await.unwrap();
    ps2.subscribe("ch1").await.unwrap();

    let mut ps3 = client.get_async_pubsub().await.unwrap();
    ps3.subscribe("ch2").await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut conn = connect(port).await;
    // PUBSUB NUMSUB returns [channel, count, channel, count, ...]
    let result: Vec<redis::Value> = redis::cmd("PUBSUB")
        .arg("NUMSUB")
        .arg("ch1")
        .arg("ch2")
        .arg("ch3")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Parse pairs: result is [BulkString("ch1"), Int(2), BulkString("ch2"), Int(1), BulkString("ch3"), Int(0)]
    assert_eq!(
        result.len(),
        6,
        "Expected 6 elements (3 channels * 2), got {:?}",
        result
    );
    // ch1 should have 2 subscribers
    if let redis::Value::Int(count) = &result[1] {
        assert_eq!(*count, 2, "ch1 should have 2 subscribers");
    }
    // ch2 should have 1 subscriber
    if let redis::Value::Int(count) = &result[3] {
        assert_eq!(*count, 1, "ch2 should have 1 subscriber");
    }
    // ch3 should have 0 subscribers
    if let redis::Value::Int(count) = &result[5] {
        assert_eq!(*count, 0, "ch3 should have 0 subscribers");
    }

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_pubsub_numpat() {
    let (port, shutdown) = start_sharded_server(4).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    let mut ps1 = client.get_async_pubsub().await.unwrap();
    ps1.psubscribe("news.*").await.unwrap();

    let mut ps2 = client.get_async_pubsub().await.unwrap();
    ps2.psubscribe("sports.*").await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut conn = connect(port).await;
    let numpat: i64 = redis::cmd("PUBSUB")
        .arg("NUMPAT")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        numpat, 2,
        "Should have 2 pattern subscriptions, got {}",
        numpat
    );

    shutdown.cancel();
}

#[tokio::test]
async fn test_sharded_pubsub_unsubscribe_cleanup() {
    let (port, shutdown) = start_sharded_server(4).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();

    // Subscribe then unsubscribe
    let mut pubsub = client.get_async_pubsub().await.unwrap();
    pubsub.subscribe("temp-ch").await.unwrap();
    pubsub.unsubscribe("temp-ch").await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut pub_conn = connect(port).await;
    let receivers: i64 = redis::cmd("PUBLISH")
        .arg("temp-ch")
        .arg("msg")
        .query_async(&mut pub_conn)
        .await
        .unwrap();
    assert_eq!(receivers, 0, "no subscribers after unsubscribe");

    shutdown.cancel();
}
