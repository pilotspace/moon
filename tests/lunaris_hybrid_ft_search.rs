//! Lunaris V2 verification — hybrid FT.SEARCH AS_OF threading.
//!
//! Phase 165 Plan 04 — Task 2. VERIFICATION ONLY. No `src/` files are modified
//! by this test (enforced by plan `test -z "$(git diff --name-only HEAD -- src/)"`).
//!
//! # Lunaris dependency
//!
//! Blueprint §5.2 / `lunaris_integration_guideline.md §C.6` maps
//! `HybridRRFRetriever` onto a single Moon-native hybrid FT.SEARCH that combines
//! BM25 text + dense vector + RRF fusion, with `AS_OF` threaded through to
//! BOTH sub-streams so snapshot semantics apply uniformly.
//!
//! Blueprint claim: "Moon supports hybrid FT.SEARCH (vector + BM25) natively —
//! RRF can be client-side. Single FT.SEARCH with hybrid config outperforms
//! three parallel searches. Use the single-call path."
//!
//! # Expected default (per 165-04 plan priors): BRANCH_B_AS_OF_UNTHREADED
//!
//! Grep of `src/command/vector_search/hybrid.rs` returns ZERO references to
//! `as_of` or `as_of_lsn`. The function `execute_hybrid_search_local` has the
//! signature `(vector_store, text_store, query)` — no LSN parameter at all.
//! The FT.SEARCH dispatcher at
//! `src/command/vector_search/ft_search/dispatch.rs:77-108` routes to hybrid
//! BEFORE the AS_OF resolution happens (AS_OF-threading lives on the
//! KNN+SPARSE fallthrough path only, line 203).
//!
//! Prediction: a hybrid FT.SEARCH with AS_OF will silently return
//! post-snapshot documents because the hybrid path never receives the
//! resolved `as_of_lsn` and treats the world as "latest".
//!
//! # Observed at run time: BRANCH_B_BOTH_BRANCHES_LEAK
//!
//! The test seeds three docs — one before snapshot, two after — and issues a
//! hybrid FT.SEARCH with AS_OF = pre-seed snapshot timestamp. Moon returns ALL
//! three docs (both the BM25 text branch and the dense vector branch ignore
//! AS_OF). The test asserts this leak explicitly so any future fix will trip
//! the assertion.
//!
//! Reference: the non-hybrid KNN path DOES honour AS_OF (tests/ft_search_temporal_parity.rs
//! locks this in), so the gap is specifically in the hybrid branch.
//!
//! # Why this test PASSES despite the underlying feature gap
//!
//! The test's contract is "Moon's hybrid FT.SEARCH with AS_OF is DETERMINISTIC
//! and DOCUMENTED." We lock the observed leak count so a future Moon change
//! will trip the assertion and force a gap-doc review. The gap doc tells
//! Lunaris to route hybrid RRF fusion client-side (two separate FT.SEARCH
//! calls — one vector with AS_OF, one text with AS_OF — then RRF fuse in
//! Rust).
//!
//! Run:
//!   cargo test --test lunaris_hybrid_ft_search --no-default-features \
//!        --features runtime-tokio,jemalloc,graph,text-index \
//!        -- --test-threads=1 --nocapture
#![cfg(all(feature = "runtime-tokio", feature = "text-index"))]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test server harness — duplicated (not extracted) per Plan 165-03 convention.
// ---------------------------------------------------------------------------

fn build_config(port: u16, num_shards: usize) -> ServerConfig {
    ServerConfig {
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
    }
}

async fn start_moon_sharded(num_shards: usize) -> (u16, CancellationToken) {
    let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let token = CancellationToken::new();
    let config = build_config(port, num_shards);
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
                .name(format!("lunaris-v2-shard-{}", id))
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

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    (port, token)
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Encode four f32 values as 16 little-endian bytes.
fn vec4_bytes(v: [f32; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for x in &v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Create an HNSW index on HASH prefix `doc:` with a TEXT field + 4-dim vector
/// field. This is the minimum schema needed for hybrid FT.SEARCH (BM25 + dense).
async fn ft_create_hybrid_idx(conn: &mut redis::aio::MultiplexedConnection) {
    let r: String = redis::cmd("FT.CREATE")
        .arg("idx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("doc:")
        .arg("SCHEMA")
        .arg("title")
        .arg("TEXT")
        .arg("vec")
        .arg("VECTOR")
        .arg("HNSW")
        .arg("6")
        .arg("DIM")
        .arg("4")
        .arg("TYPE")
        .arg("FLOAT32")
        .arg("DISTANCE_METRIC")
        .arg("L2")
        .query_async(conn)
        .await
        .expect("FT.CREATE hybrid index should succeed");
    assert_eq!(r, "OK", "FT.CREATE should return OK, got {r}");
}

async fn hset_doc(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    title: &str,
    v: [f32; 4],
) {
    let bytes = vec4_bytes(v);
    let _: i64 = redis::cmd("HSET")
        .arg(key)
        .arg("title")
        .arg(title)
        .arg("vec")
        .arg(bytes)
        .query_async(conn)
        .await
        .expect("HSET should succeed");
}

/// Parse an FT.SEARCH reply: Array[ count_int, key_bulk, fields_array, ... ].
/// Returns (count, matched_doc_keys_in_order).
fn parse_search_keys(v: &redis::Value) -> (i64, Vec<String>) {
    let items = match v {
        redis::Value::Array(items) => items,
        other => panic!("expected Array from FT.SEARCH, got {other:?}"),
    };
    let count = match items.first() {
        Some(redis::Value::Int(n)) => *n,
        Some(redis::Value::BulkString(b)) => std::str::from_utf8(b)
            .expect("count is utf-8")
            .parse::<i64>()
            .expect("count parses"),
        other => panic!("unexpected first item: {other:?}"),
    };
    let mut keys = Vec::new();
    for item in items.iter().skip(1) {
        if let redis::Value::BulkString(b) = item {
            if let Ok(s) = std::str::from_utf8(b) {
                if s.starts_with("doc:") {
                    keys.push(s.to_string());
                }
            }
        }
    }
    (count, keys)
}

// ---------------------------------------------------------------------------
// Primary V2 assertion — hybrid_ft_search_as_of_threads_to_both_branches.
//
// Seeds three docs (one pre-snapshot, two post-snapshot), captures wall_ms_t1,
// then issues a hybrid FT.SEARCH `"alice" HYBRID VECTOR @vec $q FUSION RRF`
// with AS_OF wall_ms_t1. The dispatcher routes hybrid BEFORE the AS_OF
// resolution takes effect (src/command/vector_search/ft_search/dispatch.rs:77-108).
//
// Expected under Branch B (research default): BOTH branches leak — all three
// docs appear in the response because the hybrid path never receives the
// resolved as_of_lsn.
//
// Control: a non-hybrid KNN query with the SAME AS_OF returns only doc:1
// (reference behavior locked by Plan 165-03's ft_search_as_of_honours_snapshot).
//
// The test asserts the leak explicitly. When Moon v0.1.9 threads as_of_lsn
// through hybrid.rs, this test will trip and force the fix to be rolled into
// a new Lunaris code path (unthread the client-side RRF fallback).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hybrid_ft_search_as_of_threads_to_both_branches() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();

    ft_create_hybrid_idx(&mut conn).await;

    // doc:1 — pre-snapshot. Text contains "alice"; vector is [1,0,0,0].
    hset_doc(&mut conn, "doc:1", "alice knows bob", [1.0, 0.0, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Register snapshot.
    let snap_ok: String = redis::cmd("TEMPORAL.SNAPSHOT_AT")
        .query_async(&mut conn)
        .await
        .expect("TEMPORAL.SNAPSHOT_AT should return OK");
    assert_eq!(snap_ok, "OK");

    let wall_ms_t1 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // doc:2 — post-snapshot, text matches "alice" (text-branch leak candidate).
    hset_doc(&mut conn, "doc:2", "alice met carol", [0.0, 1.0, 0.0, 0.0]).await;
    // doc:3 — post-snapshot, text does NOT match "alice", but vector is close
    //          to the query vector [1,0,0,0] (vector-branch leak candidate).
    hset_doc(&mut conn, "doc:3", "bob met carol", [0.9, 0.1, 0.0, 0.0]).await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let q_bytes = vec4_bytes([1.0, 0.0, 0.0, 0.0]);

    // ── Control: non-hybrid KNN with AS_OF (should return only doc:1) ───────
    //
    // This is the reference path from tests/ft_search_temporal_parity.rs
    // already proven green. We re-run it here to rule out test-environment
    // issues before asserting the hybrid leak.
    let control: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes.clone())
        .arg("AS_OF")
        .arg(wall_ms_t1.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("control KNN+AS_OF must succeed");
    let (control_count, control_keys) = parse_search_keys(&control);
    println!("[V2 control KNN+AS_OF] count={control_count} keys={control_keys:?}");
    assert_eq!(
        control_count, 1,
        "control: non-hybrid KNN path honours AS_OF (Plan 165-03 locked this in); \
         got count={control_count} keys={control_keys:?}"
    );
    assert_eq!(
        control_keys.first().map(String::as_str),
        Some("doc:1"),
        "control: KNN+AS_OF must return exactly doc:1 (pre-snapshot); got {control_keys:?}"
    );

    // ── Primary probe: hybrid FT.SEARCH with AS_OF ──────────────────────────
    //
    // Per `src/command/vector_search/ft_search/dispatch.rs:77-108`, the
    // dispatcher parses HYBRID and calls `execute_hybrid_search_local` — which
    // does NOT accept as_of_lsn. The AS_OF resolution on the handler side
    // does happen, but its result is never propagated into the hybrid
    // executor.
    //
    // Order note: AS_OF is a FT.SEARCH-level modifier, not a HYBRID-modifier.
    // Placing AS_OF after PARAMS works; no alternative orderings are needed
    // because the FT.SEARCH arg parser scans the entire args array for AS_OF
    // regardless of position (see `parse_as_of_clause`).
    let hybrid: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("alice")
        .arg("HYBRID")
        .arg("VECTOR")
        .arg("@vec")
        .arg("$q")
        .arg("FUSION")
        .arg("RRF")
        .arg("LIMIT")
        .arg("0")
        .arg("10")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q_bytes)
        .arg("AS_OF")
        .arg(wall_ms_t1.to_string())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("hybrid FT.SEARCH + AS_OF should not ERR (parses as valid FT.SEARCH)");
    let (hybrid_count, hybrid_keys) = parse_search_keys(&hybrid);
    println!("[V2 hybrid+AS_OF] count={hybrid_count} keys={hybrid_keys:?}");

    // Branch B lock-in: the hybrid path ignores AS_OF, so it leaks at least
    // one post-snapshot document that the control correctly filtered out.
    // We assert `hybrid_count > control_count` (leak >= 1) rather than a
    // precise value because RRF fusion may dedup differently.
    assert!(
        hybrid_count > control_count,
        "Branch B lock-in: hybrid FT.SEARCH MUST leak post-snapshot docs (AS_OF unthreaded). \
         Got hybrid_count={hybrid_count} == control_count={control_count}. \
         If these match, as_of_lsn is now threaded through hybrid.rs — \
         remove this assertion, update LUNARIS-CYPHER-GAPS.md V2 to PASS, \
         and Lunaris can switch to Moon-native hybrid."
    );

    // Additionally: the hybrid response must contain at least one of doc:2 /
    // doc:3 (the post-snapshot docs). This pinpoints WHICH branch leaks.
    let has_doc2 = hybrid_keys.iter().any(|k| k == "doc:2");
    let has_doc3 = hybrid_keys.iter().any(|k| k == "doc:3");
    assert!(
        has_doc2 || has_doc3,
        "Branch B lock-in: hybrid leak MUST include at least one post-snapshot doc \
         (doc:2 text-match or doc:3 vector-match). Got keys={hybrid_keys:?}"
    );

    // Record WHICH branch leaked for the SUMMARY / gap doc.
    let leak_label = match (has_doc2, has_doc3) {
        (true, true) => "BOTH (text + vector)",
        (true, false) => "TEXT only (doc:2)",
        (false, true) => "VECTOR only (doc:3)",
        (false, false) => unreachable!("asserted above"),
    };
    println!("[V2 leak-branch-label] {leak_label}");

    shutdown.cancel();
}
