//! Integration tests — pipelined HSET auto-indexing (Phase 172, PIPE-01/02/03).
//!
//! Today single HSET triggers auto-index on every dispatch path. RESP-pipelined
//! HSETs historically skipped the callback in MULTI/EXEC and non-atomic pipeline
//! paths; Phase 166 closed the vector gap, Phase 172 extends coverage to
//! text-index auto-indexing AND asserts end-to-end parity via real TCP round-
//! trips so Lunaris bulk ingestion (100+ HSETs per pipeline) stops under-
//! indexing.
//!
//! Tests (all run against `--shards 1` to pin local-write path):
//!   1. `pipeline_hset_fires_vector_and_text_auto_index` — 100 HSETs in one
//!      non-atomic RESP pipeline; FT.INFO num_docs == 100, KNN returns 10,
//!      text token "test-1" returns doc:1.
//!   2. `individual_hset_baseline_unchanged` — 100 individual HSETs (no
//!      pipeline); same assertions. Regression guard for single-command path.
//!   3. `pipeline_hset_inside_txn_respects_snapshot` — pipeline HSET inside
//!      TXN.BEGIN / TXN.COMMIT fires auto-index AND the pre-commit snapshot
//!      excludes pipeline inserts from a non-TXN reader (ACID-09 semantics).
//!
//! Run:
//!   cargo test --no-default-features \
//!     --features runtime-tokio,jemalloc,text-index,graph \
//!     --test pipeline_auto_index
#![cfg(feature = "runtime-tokio")]

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Test harness (mirrors tests/txn_ft_search_snapshot.rs). Duplicated per the
// same "keep harness local until the pattern ossifies" directive used there.
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
                .name(format!("pipe-auto-shard-{}", id))
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn vec4_bytes(v: [f32; 4]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    for x in &v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Deterministic vector per doc index so KNN results are reproducible.
/// All vectors share component 0 = 1.0 so every doc is similar to the query.
fn doc_vec(i: usize) -> [f32; 4] {
    let f = (i as f32) * 0.001;
    [1.0, f, f * 0.5, f * 0.25]
}

/// Produce a unique alphabetic token per doc index, avoiding hyphens/digits
/// that unicode_words splitting (see src/text/analyzer.rs) would fragment
/// below the `min_token_len` filter. Format: "xxx<alpha-ordinal>doc" — e.g.
/// 1 → "alphadoc", 2 → "bravodoc", 27 → "aadoc", 28 → "abdoc". Deterministic
/// and bijective over 1..=usize::MAX.
fn unique_token(i: usize) -> String {
    // NATO-style prefixes for 1..=26 keep readability of individual asserts;
    // fallback to base-26 letter sequence for larger N (still alphabetic only).
    const NATO: [&str; 26] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra",
        "tango", "uniform", "victor", "whiskey", "xray", "yankee", "zulu",
    ];
    if (1..=26).contains(&i) {
        format!("{}doc", NATO[i - 1])
    } else {
        // Base-26 letter string (a..z, aa..zz, ...) for N > 26.
        let mut n = i;
        let mut letters = Vec::new();
        while n > 0 {
            n -= 1;
            letters.push((b'a' + (n % 26) as u8) as char);
            n /= 26;
        }
        letters.reverse();
        let s: String = letters.into_iter().collect();
        format!("{s}doc")
    }
}

async fn ft_create_idx(conn: &mut redis::aio::MultiplexedConnection) {
    let r: String = redis::cmd("FT.CREATE")
        .arg("idx")
        .arg("ON")
        .arg("HASH")
        .arg("PREFIX")
        .arg("1")
        .arg("doc:")
        .arg("SCHEMA")
        .arg("name")
        .arg("TEXT")
        .arg("vec")
        .arg("VECTOR")
        .arg("HNSW")
        .arg("6")
        .arg("TYPE")
        .arg("FLOAT32")
        .arg("DIM")
        .arg("4")
        .arg("DISTANCE_METRIC")
        .arg("L2")
        .query_async(conn)
        .await
        .expect("FT.CREATE should succeed");
    assert_eq!(r, "OK");
}

/// Extract FT.INFO's num_docs field (response is a flat key/value array).
fn info_num_docs(v: &redis::Value) -> i64 {
    let items = match v {
        redis::Value::Array(a) => a,
        other => panic!("FT.INFO returned non-array: {other:?}"),
    };
    let mut i = 0;
    while i + 1 < items.len() {
        let key = match &items[i] {
            redis::Value::BulkString(b) => std::str::from_utf8(b).unwrap_or(""),
            redis::Value::SimpleString(s) => s.as_str(),
            _ => "",
        };
        if key == "num_docs" {
            return match &items[i + 1] {
                redis::Value::Int(n) => *n,
                redis::Value::BulkString(b) => std::str::from_utf8(b)
                    .expect("utf-8")
                    .parse::<i64>()
                    .expect("num_docs parses"),
                other => panic!("unexpected num_docs value: {other:?}"),
            };
        }
        i += 2;
    }
    panic!("FT.INFO response missing num_docs: {items:?}");
}

/// Count results in an FT.SEARCH response. First element is the total count.
fn search_count(v: &redis::Value) -> i64 {
    match v {
        redis::Value::Array(items) => match items.first() {
            Some(redis::Value::Int(n)) => *n,
            Some(redis::Value::BulkString(b)) => std::str::from_utf8(b)
                .expect("utf-8")
                .parse::<i64>()
                .expect("count parses"),
            other => panic!("unexpected first item: {other:?}"),
        },
        other => panic!("expected array, got {other:?}"),
    }
}

fn extract_doc_keys(v: &redis::Value) -> Vec<String> {
    let mut keys = Vec::new();
    if let redis::Value::Array(items) = v {
        for item in items {
            if let redis::Value::BulkString(b) = item
                && let Ok(s) = std::str::from_utf8(b)
                && s.starts_with("doc:")
            {
                keys.push(s.to_string());
            }
        }
    }
    keys
}

// ---------------------------------------------------------------------------
// Test 1 (PIPE-01 / PIPE-02): non-atomic RESP pipeline fires BOTH vector and
// text auto-indexing for every command, not just the first/last.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_hset_fires_vector_and_text_auto_index() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    ft_create_idx(&mut conn).await;

    // Build a 100-HSET non-atomic pipeline (redis::pipe() is non-atomic by
    // default — explicit `.atomic()` would wrap in MULTI/EXEC). This exercises
    // the PipelineBatch/PipelineBatchSlotted auto-index path.
    // Use a unique-per-doc token ("alphadoc", "bravodoc", ...) plus a shared
    // "corpus" token so we can validate both per-doc and all-doc TEXT matches.
    // Unicode word segmentation splits on hyphens/digits (see
    // src/text/analyzer.rs::tokenize_with_positions) so we avoid those.
    let mut pipe = redis::pipe();
    for i in 1..=100_usize {
        let key = format!("doc:{i}");
        let name = format!("corpus {}", unique_token(i));
        let vec_bytes = vec4_bytes(doc_vec(i));
        pipe.cmd("HSET")
            .arg(&key)
            .arg("name")
            .arg(&name)
            .arg("vec")
            .arg(vec_bytes);
    }
    let _: redis::Value = pipe.query_async(&mut conn).await.expect("pipeline HSETs");

    // Allow any deferred indexing to settle. Auto-index is synchronous in the
    // shard loop so this is only a safety margin for async scheduling.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // 1) FT.INFO reports num_docs=100 (vector index).
    let info: redis::Value = redis::cmd("FT.INFO")
        .arg("idx")
        .query_async(&mut conn)
        .await
        .expect("FT.INFO should succeed");
    let n = info_num_docs(&info);
    assert_eq!(
        n, 100,
        "pipelined HSET must auto-index all 100 docs into vector segment; got num_docs={n}"
    );

    // 2) Vector KNN returns 10 results.
    let q = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let knn: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH KNN should succeed");
    let knn_count = search_count(&knn);
    assert_eq!(
        knn_count, 10,
        "KNN top-10 over 100 pipelined docs must return exactly 10 hits; got {knn_count}"
    );

    // 3) Text search for the shared "corpus" token must find >=100 docs —
    // proves text auto-index fired on every pipeline HSET, not just a subset.
    let text_all: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("@name:corpus")
        .arg("LIMIT")
        .arg("0")
        .arg("200")
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH TEXT (shared token) should succeed");
    let text_all_count = search_count(&text_all);
    assert_eq!(
        text_all_count, 100,
        "TEXT search for 'corpus' must surface all 100 pipeline-indexed docs; \
         got count={text_all_count} (this is the exact regression documented in \
         feedback_pipeline_text_autoindex.md — ~24% previously indexed)"
    );

    // 4) Per-doc unique token (alphadoc) must pinpoint doc:1 — confirms that
    // the text analyzer indexed each doc's unique field value, not just the
    // shared prefix.
    let needle = unique_token(1);
    let text_one: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg(format!("@name:{needle}"))
        .arg("LIMIT")
        .arg("0")
        .arg("5")
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH TEXT (unique token) should succeed");
    let text_one_count = search_count(&text_one);
    assert!(
        text_one_count >= 1,
        "TEXT search for unique token '{needle}' must find doc:1; got count={text_one_count}"
    );
    let text_keys = extract_doc_keys(&text_one);
    assert!(
        text_keys.iter().any(|k| k == "doc:1"),
        "TEXT search must surface doc:1 specifically; got keys={text_keys:?}"
    );

    drop(conn);
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 2 (PIPE-03 regression guard): 100 individual (non-pipelined) HSETs hit
// the single-command dispatch path. Must produce identical outcome so the
// pipeline fix has no behavioural drift.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn individual_hset_baseline_unchanged() {
    let (port, shutdown) = start_moon_sharded(1).await;
    let mut conn = connect(port).await;

    let _: redis::Value = redis::cmd("FLUSHALL").query_async(&mut conn).await.unwrap();
    ft_create_idx(&mut conn).await;

    for i in 1..=100_usize {
        let key = format!("doc:{i}");
        let name = format!("test-{i}");
        let vec_bytes = vec4_bytes(doc_vec(i));
        let _: i64 = redis::cmd("HSET")
            .arg(&key)
            .arg("name")
            .arg(&name)
            .arg("vec")
            .arg(vec_bytes)
            .query_async(&mut conn)
            .await
            .expect("individual HSET should succeed");
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let info: redis::Value = redis::cmd("FT.INFO")
        .arg("idx")
        .query_async(&mut conn)
        .await
        .expect("FT.INFO should succeed");
    assert_eq!(
        info_num_docs(&info),
        100,
        "individual HSET baseline must continue auto-indexing all 100 docs"
    );

    let q = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let knn: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 10 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut conn)
        .await
        .expect("FT.SEARCH KNN should succeed");
    assert_eq!(search_count(&knn), 10);

    drop(conn);
    shutdown.cancel();
}

// ---------------------------------------------------------------------------
// Test 3 (PIPE-03 + TXN wiring): pipelined HSET inside TXN.BEGIN/TXN.COMMIT
// honors snapshot_lsn semantics — pre-commit reads from a sibling connection
// must NOT see the pipeline's inserts; post-commit they become visible. Also
// asserts that auto-index actually fired (num_docs advances after COMMIT).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_hset_inside_txn_respects_snapshot() {
    let (port, shutdown) = start_moon_sharded(1).await;

    let mut setup = connect(port).await;
    let _: redis::Value = redis::cmd("FLUSHALL")
        .query_async(&mut setup)
        .await
        .unwrap();
    ft_create_idx(&mut setup).await;

    // Seed one doc pre-TXN so the reader can anchor its baseline.
    let _: i64 = redis::cmd("HSET")
        .arg("doc:seed")
        .arg("name")
        .arg("seed")
        .arg("vec")
        .arg(vec4_bytes([1.0, 0.0, 0.0, 0.0]))
        .query_async(&mut setup)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Client A opens TXN then pushes a 50-HSET pipeline inside it. The
    // pipeline is non-atomic (no MULTI/EXEC wrapping) — the TXN wrapper is
    // Moon's TXN.BEGIN / TXN.COMMIT, not the redis-py MULTI transaction.
    let mut client_a = redis::Client::open(format!("redis://127.0.0.1:{port}"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let mut client_b = redis::Client::open(format!("redis://127.0.0.1:{port}"))
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let begin_ok: String = redis::cmd("TXN")
        .arg("BEGIN")
        .query_async(&mut client_a)
        .await
        .expect("TXN.BEGIN should succeed");
    assert_eq!(begin_ok, "OK");

    let mut pipe = redis::pipe();
    for i in 1..=50_usize {
        let key = format!("doc:{i}");
        let name = format!("test-{i}");
        let vec_bytes = vec4_bytes(doc_vec(i));
        pipe.cmd("HSET")
            .arg(&key)
            .arg("name")
            .arg(&name)
            .arg("vec")
            .arg(vec_bytes);
    }
    let _: redis::Value = pipe
        .query_async(&mut client_a)
        .await
        .expect("TXN pipeline HSETs");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Client B (non-TXN reader) takes a snapshot BEFORE A commits. It must
    // NOT see A's uncommitted pipeline inserts. It should still see doc:seed.
    let q = vec4_bytes([1.0, 0.0, 0.0, 0.0]);
    let pre_commit: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 100 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q.clone())
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut client_b)
        .await
        .expect("pre-commit FT.SEARCH should succeed");
    let pre_keys = extract_doc_keys(&pre_commit);
    assert!(
        pre_keys.contains(&"doc:seed".to_string()),
        "non-TXN reader must still see pre-TXN doc:seed; got {pre_keys:?}"
    );
    // Uncommitted pipeline inserts must be hidden.
    let pre_pipeline_visible = pre_keys
        .iter()
        .filter(|k| k.starts_with("doc:") && *k != "doc:seed")
        .count();
    assert_eq!(
        pre_pipeline_visible, 0,
        "pre-commit reader must NOT see TXN-pending pipeline docs; got {pre_keys:?}"
    );

    // Client A commits — releases snapshot.
    let commit_ok: String = redis::cmd("TXN")
        .arg("COMMIT")
        .query_async(&mut client_a)
        .await
        .expect("TXN.COMMIT should succeed");
    assert_eq!(commit_ok, "OK");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Post-commit: FT.INFO and a non-TXN reader must now see all 51 docs
    // (doc:seed + 50 pipelined). If auto-index didn't fire during the
    // pipeline, num_docs would be 1, not 51.
    let info: redis::Value = redis::cmd("FT.INFO")
        .arg("idx")
        .query_async(&mut setup)
        .await
        .expect("FT.INFO should succeed");
    let n = info_num_docs(&info);
    assert_eq!(
        n, 51,
        "post-commit num_docs must include seed + 50 pipelined HSETs; got {n}"
    );

    let post_commit: redis::Value = redis::cmd("FT.SEARCH")
        .arg("idx")
        .arg("*=>[KNN 100 @vec $q]")
        .arg("PARAMS")
        .arg("2")
        .arg("q")
        .arg(q)
        .arg("DIALECT")
        .arg("2")
        .query_async(&mut client_b)
        .await
        .expect("post-commit FT.SEARCH should succeed");
    let post_keys = extract_doc_keys(&post_commit);
    assert!(
        post_keys.len() >= 51,
        "post-commit reader must see all 51 docs; got count={}, keys={post_keys:?}",
        post_keys.len()
    );

    drop(client_a);
    drop(client_b);
    drop(setup);
    shutdown.cancel();
}
