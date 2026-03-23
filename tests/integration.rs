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
