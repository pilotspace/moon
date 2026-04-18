//! Integration tests for the MQ (Durable Message Queue) subsystem.
//!
//! MQ.* commands are intercepted in the sharded handlers (handler_monoio.rs /
//! handler_sharded.rs), NOT in the single-thread handler used by
//! `listener::run_with_shutdown`. These tests therefore start a full sharded
//! server via `listener::run_sharded` (same path as `main.rs`).
//!
//! MQ commands tested:
//! - MQ CREATE: Create a durable queue with optional MAXDELIVERY
//! - MQ PUSH: Enqueue messages with field/value pairs
//! - MQ POP: Claim messages with optional COUNT
//! - MQ ACK: Acknowledge message delivery
//! - MQ DLQLEN: Query dead-letter queue depth
//! - MQ TRIGGER: Register debounced callback trigger
//!
//! Cursor-rollback after crash is validated by unit tests in shared_databases.rs
//! and by the test scripts (test-commands.sh) which can restart the server.
//!
//! Run: cargo test --test mq_integration --no-default-features --features runtime-tokio,jemalloc -- --test-threads=1
#![cfg(feature = "runtime-tokio")]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use moon::shard::Shard;
use tokio::net::TcpListener;

/// Start a full sharded Moon server on a random port.
///
/// Uses the same shard-thread + `run_sharded` pattern as `main.rs`, so
/// MQ.* handler intercepts are active.
async fn start_mq_server(num_shards: usize) -> (u16, CancellationToken) {
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
        maxclients: 10000,
        timeout: 0,
        tcp_keepalive: 300,
        console_auth_required: false,
        console_auth_secret: String::new(),
        console_cors_origin: vec![],
        console_rate_limit: 1000.0,
        console_rate_burst: 2000.0,
    };

    let cancel = token.clone();

    std::thread::spawn(move || {
        let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);
        let conn_txs: Vec<channel::MpscSender<(tokio::net::TcpStream, bool)>> =
            (0..num_shards).map(|i| mesh.conn_tx(i)).collect();
        let all_notifiers = mesh.all_notifiers();
        let all_pubsub_registries: Vec<
            std::sync::Arc<parking_lot::RwLock<moon::pubsub::PubSubRegistry>>,
        > = (0..num_shards)
            .map(|_| {
                std::sync::Arc::new(parking_lot::RwLock::new(
                    moon::pubsub::PubSubRegistry::new(),
                ))
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

        let mut shards: Vec<Shard> = (0..num_shards)
            .map(|id| Shard::new(id, num_shards, config.databases, config.to_runtime_config()))
            .collect();
        let all_dbs: Vec<Vec<moon::storage::Database>> = shards
            .iter_mut()
            .map(|s| std::mem::take(&mut s.databases))
            .collect();
        let shard_databases = moon::shard::shared_databases::ShardDatabases::new(all_dbs);

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
                .name(format!("mq-test-shard-{}", id))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build shard runtime");

                    let local = tokio::task::LocalSet::new();

                    let (_snap_tx, snap_rx) = channel::watch(0u64);
                    let snap_tx = _snap_tx;
                    let acl_t = std::sync::Arc::new(std::sync::RwLock::new(
                        moon::acl::AclTable::load_or_default(&shard_config),
                    ));
                    let rt_cfg = std::sync::Arc::new(parking_lot::RwLock::new(
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
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    (port, token)
}

/// Start a sharded server with 1 shard for simple tests.
async fn start_server() -> (u16, CancellationToken) {
    start_mq_server(1).await
}

/// Open a new multiplexed connection to the server.
async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Extract message IDs from MQ POP response.
///
/// The response is an array of arrays, each inner array being:
/// `[bulk_string(id), array([field, value, ...])]`.
fn extract_message_ids(value: &redis::Value) -> Vec<String> {
    let mut ids = Vec::new();
    if let redis::Value::Array(entries) = value {
        for entry in entries {
            if let redis::Value::Array(parts) = entry {
                if let Some(redis::Value::BulkString(id_bytes)) = parts.first() {
                    if let Ok(id_str) = std::str::from_utf8(id_bytes) {
                        ids.push(id_str.to_string());
                    }
                }
            }
        }
    }
    ids
}

// ---------------------------------------------------------------------------
// Test 1: MQ CREATE basic
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_create_basic() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // MQ CREATE myqueue MAXDELIVERY 5 -> OK
    let result: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("myqueue")
        .arg("MAXDELIVERY")
        .arg("5")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // Verify the queue is functional: push a message
    let push_result: String = redis::cmd("MQ")
        .arg("PUSH")
        .arg("myqueue")
        .arg("field1")
        .arg("value1")
        .query_async(&mut conn)
        .await
        .unwrap();
    // Push returns a stream ID like "1234567890-0"
    assert!(
        push_result.contains('-'),
        "MQ PUSH should return a stream ID, got: {}",
        push_result
    );

    // Verify POP works on the created queue
    let pop_result: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("myqueue")
        .query_async(&mut conn)
        .await
        .unwrap();
    // Should return a non-empty array
    match &pop_result {
        redis::Value::Array(items) => {
            assert!(!items.is_empty(), "MQ POP should return at least one entry");
        }
        other => panic!("Expected Array from MQ POP, got: {:?}", other),
    }

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2: MQ CREATE with default MAXDELIVERY
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_create_default_maxdelivery() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // MQ CREATE with no MAXDELIVERY -> defaults to 3
    let result: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("defqueue")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "OK");

    // Verify the queue works
    let push_result: String = redis::cmd("MQ")
        .arg("PUSH")
        .arg("defqueue")
        .arg("key")
        .arg("val")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(push_result.contains('-'));

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 3: MQ CREATE idempotent
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_create_idempotent() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // First CREATE
    let r1: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("idempotent_q")
        .arg("MAXDELIVERY")
        .arg("5")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(r1, "OK");

    // Second CREATE with same config -> OK
    let r2: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("idempotent_q")
        .arg("MAXDELIVERY")
        .arg("5")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(r2, "OK");

    // Third CREATE with different config -> still OK (overwrite)
    let r3: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("idempotent_q")
        .arg("MAXDELIVERY")
        .arg("10")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(r3, "OK");

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 4: MQ PUSH/POP/ACK round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_push_pop_ack_roundtrip() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create queue
    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("roundtrip_q")
        .arg("MAXDELIVERY")
        .arg("5")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Push 3 messages with distinct field/value pairs
    let mut msg_ids = Vec::new();
    for i in 1..=3 {
        let id: String = redis::cmd("MQ")
            .arg("PUSH")
            .arg("roundtrip_q")
            .arg(format!("field{}", i))
            .arg(format!("value{}", i))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(id.contains('-'), "MQ PUSH should return stream ID: {}", id);
        msg_ids.push(id);
    }
    assert_eq!(msg_ids.len(), 3);

    // Pop all 3 messages
    let pop_result: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("roundtrip_q")
        .arg("COUNT")
        .arg("3")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Extract message IDs from the POP result
    let popped_ids = extract_message_ids(&pop_result);
    assert_eq!(popped_ids.len(), 3, "Should pop 3 messages");

    // Verify the fields are present in the response
    let pop_str = format!("{:?}", pop_result);
    assert!(
        pop_str.contains("field1") || pop_str.contains("field2") || pop_str.contains("field3"),
        "POP response should contain the pushed fields"
    );

    // ACK 2 messages -> returns 2
    let ack_count: i64 = redis::cmd("MQ")
        .arg("ACK")
        .arg("roundtrip_q")
        .arg(&popped_ids[0])
        .arg(&popped_ids[1])
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ack_count, 2, "ACK of 2 messages should return 2");

    // ACK the third -> returns 1
    let ack_count: i64 = redis::cmd("MQ")
        .arg("ACK")
        .arg("roundtrip_q")
        .arg(&popped_ids[2])
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ack_count, 1, "ACK of 1 message should return 1");

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 5: MQ PUSH to non-durable stream -> error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_push_to_non_durable() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create a regular stream with XADD (not MQ CREATE)
    let _: String = redis::cmd("XADD")
        .arg("regular_stream")
        .arg("*")
        .arg("field1")
        .arg("value1")
        .query_async(&mut conn)
        .await
        .unwrap();

    // MQ PUSH to regular stream -> error
    let result: Result<String, redis::RedisError> = redis::cmd("MQ")
        .arg("PUSH")
        .arg("regular_stream")
        .arg("f1")
        .arg("v1")
        .query_async(&mut conn)
        .await;
    assert!(
        result.is_err(),
        "MQ PUSH to non-durable stream should error"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("not a durable queue"),
        "Error should mention 'not a durable queue', got: {}",
        err_msg
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 6: MQ POP on empty queue
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_pop_empty_queue() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create empty queue
    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("empty_q")
        .query_async(&mut conn)
        .await
        .unwrap();

    // POP immediately -> empty array
    let result: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("empty_q")
        .query_async(&mut conn)
        .await
        .unwrap();
    match &result {
        redis::Value::Array(items) => {
            assert!(
                items.is_empty(),
                "POP on empty queue should return empty array"
            );
        }
        redis::Value::Nil => {
            // Also acceptable
        }
        other => panic!(
            "Expected empty Array or Nil from MQ POP on empty queue, got: {:?}",
            other
        ),
    }

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 7: MQ POP with COUNT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_pop_count() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("count_q")
        .arg("MAXDELIVERY")
        .arg("10")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Push 3 messages (first batch)
    for i in 1..=3 {
        let _: String = redis::cmd("MQ")
            .arg("PUSH")
            .arg("count_q")
            .arg(format!("f{}", i))
            .arg(format!("v{}", i))
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    // POP COUNT 2 -> returns 2 out of 3 available
    let pop2: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("count_q")
        .arg("COUNT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .unwrap();
    let ids_2 = extract_message_ids(&pop2);
    assert_eq!(ids_2.len(), 2, "POP COUNT 2 should return 2 messages");

    // Push 2 more messages AFTER first POP (these are new messages
    // that read_group_new will see on next POP)
    for i in 4..=5 {
        let _: String = redis::cmd("MQ")
            .arg("PUSH")
            .arg("count_q")
            .arg(format!("f{}", i))
            .arg(format!("v{}", i))
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    // POP COUNT 10 -> returns remaining new messages
    // Note: due to DLQ over-fetch (request_count = count + mdc = 10 + 10 = 20),
    // this may consume more messages from the stream than the user requested COUNT.
    let pop_rest: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("count_q")
        .arg("COUNT")
        .arg("10")
        .query_async(&mut conn)
        .await
        .unwrap();
    let ids_rest = extract_message_ids(&pop_rest);
    // Should return at least 2 new messages (the ones pushed after first POP)
    assert!(
        ids_rest.len() >= 2,
        "POP COUNT 10 should return at least 2 new messages, got {}",
        ids_rest.len()
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 8: MQ ACK returns correct count
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_ack_returns_count() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("ack_q")
        .arg("MAXDELIVERY")
        .arg("10")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Push 3 messages
    for i in 1..=3 {
        let _: String = redis::cmd("MQ")
            .arg("PUSH")
            .arg("ack_q")
            .arg(format!("f{}", i))
            .arg(format!("v{}", i))
            .query_async(&mut conn)
            .await
            .unwrap();
    }

    // Pop 3
    let pop_result: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("ack_q")
        .arg("COUNT")
        .arg("3")
        .query_async(&mut conn)
        .await
        .unwrap();
    let ids = extract_message_ids(&pop_result);
    assert_eq!(ids.len(), 3);

    // ACK 2 -> returns 2
    let ack2: i64 = redis::cmd("MQ")
        .arg("ACK")
        .arg("ack_q")
        .arg(&ids[0])
        .arg(&ids[1])
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ack2, 2);

    // ACK the third -> returns 1
    let ack1: i64 = redis::cmd("MQ")
        .arg("ACK")
        .arg("ack_q")
        .arg(&ids[2])
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ack1, 1);

    // ACK non-existent ID -> returns 0
    let ack0: i64 = redis::cmd("MQ")
        .arg("ACK")
        .arg("ack_q")
        .arg("999999999-999")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(ack0, 0);

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 9: MQ DLQ routing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_dlq_routing() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create queue with MAXDELIVERY 1.
    //
    // DLQ routing triggers during POP when delivery_count >= max_delivery_count.
    // With MAXDELIVERY 1, the very first POP creates a PEL entry with
    // delivery_count=1, which equals max_delivery_count. The handler's DLQ
    // filter immediately routes it to the dead-letter stream instead of
    // returning it to the consumer.
    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("dlq_q")
        .arg("MAXDELIVERY")
        .arg("1")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Push 1 message
    let msg_id: String = redis::cmd("MQ")
        .arg("PUSH")
        .arg("dlq_q")
        .arg("payload")
        .arg("test_data")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(msg_id.contains('-'));

    // Pop -- delivery_count=1 hits max_delivery_count=1, message
    // should be routed to DLQ and NOT returned to the consumer.
    let pop1: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("dlq_q")
        .query_async(&mut conn)
        .await
        .unwrap();
    let ids1 = extract_message_ids(&pop1);
    // The message was DLQ-routed, so consumer gets empty array
    assert!(
        ids1.is_empty(),
        "POP with MAXDELIVERY 1 should return empty (message routed to DLQ), got {} messages",
        ids1.len()
    );

    // DLQLEN should be 1
    let dlq_len: i64 = redis::cmd("MQ")
        .arg("DLQLEN")
        .arg("dlq_q")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        dlq_len, 1,
        "DLQLEN should be 1 after DLQ routing, got: {}",
        dlq_len
    );

    // Next POP should also return empty (no more messages)
    let pop2: redis::Value = redis::cmd("MQ")
        .arg("POP")
        .arg("dlq_q")
        .query_async(&mut conn)
        .await
        .unwrap();
    let ids2 = extract_message_ids(&pop2);
    assert!(
        ids2.is_empty(),
        "POP after DLQ routing should return empty, got {} messages",
        ids2.len()
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 10: MQ DLQLEN on empty queue
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_dlqlen_empty() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create queue
    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("dlqlen_q")
        .query_async(&mut conn)
        .await
        .unwrap();

    // DLQLEN on queue with no dead letters -> 0
    let len: i64 = redis::cmd("MQ")
        .arg("DLQLEN")
        .arg("dlqlen_q")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(len, 0, "DLQLEN on fresh queue should be 0");

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 11: MQ TRIGGER register
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_trigger_register() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create queue first
    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("trigger_q")
        .query_async(&mut conn)
        .await
        .unwrap();

    // Register a trigger with DEBOUNCE
    let result: String = redis::cmd("MQ")
        .arg("TRIGGER")
        .arg("trigger_q")
        .arg("PUBLISH events new_data")
        .arg("DEBOUNCE")
        .arg("500")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result, "OK", "MQ TRIGGER should return OK");

    // Register another trigger (overwrite) -> still OK
    let result2: String = redis::cmd("MQ")
        .arg("TRIGGER")
        .arg("trigger_q")
        .arg("PUBLISH events updated")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(result2, "OK", "MQ TRIGGER overwrite should return OK");

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 12: MQ unknown subcommand
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_unknown_subcommand() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    let result: Result<String, redis::RedisError> = redis::cmd("MQ")
        .arg("FOOBAR")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err(), "MQ FOOBAR should return error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("unknown MQ subcommand"),
        "Error should mention 'unknown MQ subcommand', got: {}",
        err_msg
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 13: MQ PUSH missing args
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_push_missing_args() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // MQ PUSH with no key and no fields
    let result: Result<String, redis::RedisError> = redis::cmd("MQ")
        .arg("PUSH")
        .query_async(&mut conn)
        .await;
    assert!(result.is_err(), "MQ PUSH without args should return error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("wrong number of arguments"),
        "Error should mention 'wrong number of arguments', got: {}",
        err_msg
    );

    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 14: MQ ACK invalid ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mq_ack_invalid_id() {
    let (port, shutdown) = start_server().await;
    let mut conn = connect(port).await;

    // Create queue for ACK test
    let _: String = redis::cmd("MQ")
        .arg("CREATE")
        .arg("ack_invalid_q")
        .query_async(&mut conn)
        .await
        .unwrap();

    // MQ ACK with an invalid ID format
    let result: Result<String, redis::RedisError> = redis::cmd("MQ")
        .arg("ACK")
        .arg("ack_invalid_q")
        .arg("not-a-valid-id")
        .query_async(&mut conn)
        .await;
    assert!(
        result.is_err(),
        "MQ ACK with invalid ID should return error"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("invalid message ID format"),
        "Error should mention 'invalid message ID format', got: {}",
        err_msg
    );

    shutdown.cancel();
}
