//! Integration tests for PSYNC2 replication commands.
//!
//! Tests REPLICAOF, REPLCONF, INFO replication, READONLY enforcement,
//! and REPLICAOF NO ONE promotion -- using real TCP connections.

use rust_redis::runtime::cancel::CancellationToken;
use tokio::net::TcpListener;

use rust_redis::config::ServerConfig;
use rust_redis::server::listener;

/// Start a server on a random port with replication support and return port + shutdown token.
async fn start_server() -> (u16, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let token = CancellationToken::new();
    let server_token = token.clone();

    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_string_lossy().to_string();

    let config = ServerConfig {
        bind: "127.0.0.1".to_string(),
        port,
        databases: 16,
        requirepass: None,
        appendonly: "no".to_string(),
        appendfsync: "everysec".to_string(),
        save: None,
        dir: dir_path,
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
    };

    tokio::spawn(async move {
        let _dir = dir; // keep tempdir alive
        listener::run_with_shutdown(config, server_token)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (port, token)
}

/// Create an async connection.
async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{}/", port)).unwrap();
    client.get_multiplexed_tokio_connection().await.unwrap()
}

#[tokio::test]
async fn test_info_replication_master() {
    let (port, token) = start_server().await;
    let mut con = connect(port).await;

    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut con)
        .await
        .unwrap();

    assert!(
        info.contains("role:master"),
        "INFO should contain role:master, got: {}",
        info
    );
    assert!(
        info.contains("master_replid:"),
        "INFO should contain master_replid:, got: {}",
        info
    );
    assert!(
        info.contains("master_repl_offset:"),
        "INFO should contain master_repl_offset:",
    );
    assert!(
        info.contains("connected_slaves:0"),
        "INFO should contain connected_slaves:0",
    );

    token.cancel();
}

#[tokio::test]
async fn test_readonly_replica() {
    let (port, token) = start_server().await;
    let mut con = connect(port).await;

    // Set server as a replica of a non-existent master
    let result: String = redis::cmd("REPLICAOF")
        .arg("127.0.0.1")
        .arg("9999")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // Now a write should be rejected
    let write_result: redis::RedisResult<String> = redis::cmd("SET")
        .arg("test_key")
        .arg("test_val")
        .query_async(&mut con)
        .await;
    assert!(
        write_result.is_err(),
        "Write on replica should return READONLY error"
    );
    let err_str = write_result.unwrap_err().to_string();
    // redis crate formats the error as "ReadOnly:" (camelCase)
    assert!(
        err_str.to_uppercase().contains("READONLY"),
        "Error should say READONLY, got: {}",
        err_str
    );

    token.cancel();
}

#[tokio::test]
async fn test_replicaof_no_one() {
    let (port, token) = start_server().await;
    let mut con = connect(port).await;

    // Become a replica
    let _: String = redis::cmd("REPLICAOF")
        .arg("127.0.0.1")
        .arg("9999")
        .query_async(&mut con)
        .await
        .unwrap();

    // Promote back to master
    let result: String = redis::cmd("REPLICAOF")
        .arg("NO")
        .arg("ONE")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // INFO should now show master
    let info: String = redis::cmd("INFO")
        .arg("replication")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        info.contains("role:master"),
        "After REPLICAOF NO ONE, should be master, got: {}",
        info
    );

    token.cancel();
}

#[tokio::test]
async fn test_replconf_ok() {
    let (port, token) = start_server().await;
    let mut con = connect(port).await;

    // REPLCONF responds OK to any subcommand
    let result: String = redis::cmd("REPLCONF")
        .arg("listening-port")
        .arg("6380")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    let result: String = redis::cmd("REPLCONF")
        .arg("capa")
        .arg("psync2")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    token.cancel();
}

#[tokio::test]
async fn test_wait_no_replicas() {
    let (port, token) = start_server().await;
    let mut con = connect(port).await;

    // WAIT with no connected replicas should return 0 quickly
    let result: i64 = redis::cmd("WAIT")
        .arg("1")
        .arg("100")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(result, 0, "WAIT with no replicas should return 0");

    token.cancel();
}
