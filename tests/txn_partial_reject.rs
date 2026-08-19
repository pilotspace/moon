//! Integration tests — #499: `TXN.COMMIT` must never report `+OK` when an
//! operation in the transaction body was rejected by a TXN guard.
//!
//! Repro (Lunaris RFC 0008 §2.4, reproduced on v0.8.5 at `--shards 4`): a TXN
//! body that writes keys owned by other shards has those writes rejected by
//! the cross-shard guard (`ERR TXN does not support cross-shard writes ...`),
//! yet `TXN.COMMIT` still answered `+OK` and the accepted subset was applied.
//! A caller that only inspects the COMMIT reply — the normal driver shape —
//! sees success for a transaction that was applied in part: silent data loss.
//!
//! Chosen semantics (Redis `MULTI`/`EXEC` parity): a rejected op poisons the
//! transaction. `TXN.COMMIT` rolls the whole transaction back through the
//! `TXN.ABORT` path and answers `EXECABORT ...` — nothing is applied.
//!
//! Run:
//!   cargo test --test txn_partial_reject \
//!       --no-default-features --features runtime-tokio,jemalloc -- --test-threads=1
#![cfg(feature = "runtime-tokio")]

mod common;

use moon::config::ServerConfig;
use moon::runtime::cancel::CancellationToken;
use moon::runtime::channel;
use moon::server::listener;
use moon::shard::Shard;
use moon::shard::dispatch::key_to_shard;
use moon::shard::mesh::{CHANNEL_BUFFER_SIZE, ChannelMesh};

// ---------------------------------------------------------------------------
// Test server infrastructure — mirrors `tests/txn_kv_wiring.rs`, but builds the
// `ServerConfig` through the real clap parser instead of a field-by-field
// literal so new config fields cannot rot this harness.
// ---------------------------------------------------------------------------

async fn start_txn_server(num_shards: usize) -> (u16, CancellationToken) {
    const MAX_ATTEMPTS: usize = 8;
    // `--disk-free-min-pct 0`: dev volumes routinely sit under the 5% default
    // and the diskfull guard would turn every write in this suite into
    // `MOONERR diskfull`, masking the behaviour under test.
    let tmp = std::env::temp_dir();
    let dir = tmp.to_string_lossy().into_owned();
    for attempt in 1..=MAX_ATTEMPTS {
        let port = common::reserve_port();
        let token = CancellationToken::new();
        let (config, _matches) = ServerConfig::parse_from_with_matches([
            "moon",
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--shards",
            &num_shards.to_string(),
            "--appendonly",
            "no",
            "--dir",
            &dir,
            "--maxmemory",
            "0",
            "--disk-free-min-pct",
            "0",
        ]);

        spawn_txn_server_thread(config, num_shards, token.clone());

        if await_server_ready(port, std::time::Duration::from_secs(5)).await {
            return (port, token);
        }

        token.cancel();
        eprintln!(
            "start_txn_server: server on port {port} not ready \
             (attempt {attempt}/{MAX_ATTEMPTS}); retrying on a new port"
        );
    }

    panic!("start_txn_server: could not bring up a server after {MAX_ATTEMPTS} attempts");
}

fn spawn_txn_server_thread(config: ServerConfig, num_shards: usize, cancel: CancellationToken) {
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
        let (shard_databases, mut slice_inits) =
            moon::shard::shared_databases::ShardDatabases::new(all_dbs);

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
            let shard_slice_init = slice_inits.remove(0);

            let handle = std::thread::Builder::new()
                .name(format!("txn499-shard-{id}"))
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
                        shard_slice_init,
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
                eprintln!("Listener error: {e}");
            }
        });

        cancel.cancel();
        for handle in shard_handles {
            let _ = handle.join();
        }
    });
}

async fn await_server_ready(port: u16, timeout: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if let Ok(client) = redis::Client::open(format!("redis://127.0.0.1:{port}"))
            && let Ok(mut conn) = client.get_multiplexed_async_connection().await
        {
            let pong: redis::RedisResult<String> = redis::cmd("PING").query_async(&mut conn).await;
            if pong.is_ok() {
                return true;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    false
}

async fn connect(port: u16) -> redis::aio::MultiplexedConnection {
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

const SHARDS: usize = 4;

/// One key per shard, so that whichever shard the connection landed on,
/// exactly one of them is shard-local (accepted) and the rest trip the
/// cross-shard TXN guard.
fn one_key_per_shard(prefix: &str) -> Vec<String> {
    let mut by_shard: Vec<Option<String>> = vec![None; SHARDS];
    for i in 0..10_000 {
        let key = format!("{prefix}:{i}");
        let shard = key_to_shard(key.as_bytes(), SHARDS);
        if by_shard[shard].is_none() {
            by_shard[shard] = Some(key);
        }
        if by_shard.iter().all(Option::is_some) {
            break;
        }
    }
    by_shard
        .into_iter()
        .map(|k| k.expect("every shard must be reachable by some key"))
        .collect()
}

/// Two extra keys that both hash to `shard`.
fn keys_on_shard(prefix: &str, shard: usize, want: usize) -> Vec<String> {
    let mut out = Vec::with_capacity(want);
    for i in 0..10_000 {
        let key = format!("{prefix}:{i}");
        if key_to_shard(key.as_bytes(), SHARDS) == shard {
            out.push(key);
            if out.len() == want {
                return out;
            }
        }
    }
    panic!("could not find {want} keys on shard {shard}");
}

async fn txn(conn: &mut redis::aio::MultiplexedConnection, sub: &str) -> Result<String, String> {
    redis::cmd("TXN")
        .arg(sub)
        .query_async::<String>(conn)
        .await
        .map_err(|e| e.to_string())
}

async fn set(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    val: &str,
) -> Result<String, String> {
    redis::cmd("SET")
        .arg(key)
        .arg(val)
        .query_async::<String>(conn)
        .await
        .map_err(|e| e.to_string())
}

async fn get(conn: &mut redis::aio::MultiplexedConnection, key: &str) -> Option<String> {
    redis::cmd("GET")
        .arg(key)
        .query_async::<Option<String>>(conn)
        .await
        .expect("GET must not fail")
}

// ---------------------------------------------------------------------------
// #499 — the bug
// ---------------------------------------------------------------------------

/// RED before the fix: `TXN.COMMIT` replied `+OK` after the cross-shard guard
/// rejected 3 of the 4 writes, and the one accepted write was applied.
#[tokio::test]
async fn test_txn_commit_aborts_when_ops_were_rejected() {
    let (port, shutdown) = start_txn_server(SHARDS).await;
    let mut conn = connect(port).await;

    let keys = one_key_per_shard("txn499:mixed");

    assert_eq!(txn(&mut conn, "BEGIN").await.as_deref(), Ok("OK"));

    let mut accepted = Vec::new();
    let mut rejected = Vec::new();
    for key in &keys {
        match set(&mut conn, key, "v").await {
            Ok(_) => accepted.push(key.clone()),
            Err(e) => {
                assert!(
                    e.contains("cross-shard"),
                    "unexpected rejection for {key}: {e}"
                );
                rejected.push(key.clone());
            }
        }
    }
    assert_eq!(
        accepted.len(),
        1,
        "exactly one of the four keys is shard-local (accepted={accepted:?})"
    );
    assert_eq!(rejected.len(), SHARDS - 1, "the rest must be rejected");

    // The core assertion: COMMIT must NOT report success.
    let commit = txn(&mut conn, "COMMIT").await;
    let err = commit.expect_err("TXN.COMMIT must fail when queued ops were rejected");
    // redis-rs recognises the `EXECABORT` code and renders it `ExecAbort:` —
    // driver parity with Redis's own dirty-EXEC abort, which is the point of
    // reusing that code. Match case-insensitively so either form passes.
    assert!(
        err.to_ascii_lowercase().contains("execabort"),
        "COMMIT error should carry the EXECABORT transaction-discarded code, got: {err}"
    );
    assert!(
        err.contains("rolled back and NOT committed"),
        "COMMIT error should state the transaction was rolled back, got: {err}"
    );
    assert!(
        err.contains('3'),
        "COMMIT error should name the rejected-op count, got: {err}"
    );

    // Abort-all semantics: the accepted sibling write is rolled back too.
    for key in &keys {
        assert_eq!(
            get(&mut conn, key).await,
            None,
            "{key} must not exist — a rejected op aborts the whole transaction"
        );
    }

    // The transaction is discarded, exactly like Redis EXECABORT leaves MULTI.
    let second = txn(&mut conn, "COMMIT").await;
    let err2 = second.expect_err("connection must no longer be in a transaction");
    assert!(
        err2.contains("not in a cross-store transaction"),
        "expected not-in-txn error, got: {err2}"
    );

    shutdown.cancel();
}

/// Regression: a transaction whose every op was accepted still commits `+OK`
/// and applies, at `--shards 4`. Guards the fix against over-reach.
#[tokio::test]
async fn test_txn_commit_ok_when_no_op_was_rejected() {
    let (port, shutdown) = start_txn_server(SHARDS).await;
    let mut conn = connect(port).await;

    let probe_keys = one_key_per_shard("txn499:probe");

    // Discover which shard this connection landed on: inside a throwaway
    // transaction exactly one of the four probe keys is accepted. The
    // transaction is aborted, so it leaves no state behind.
    //
    // Bounded retry: connection migration can move the connection between the
    // probe and the real transaction (it is inhibited *during* a txn, not
    // between two).
    let mut committed = false;
    for _ in 0..4 {
        assert_eq!(txn(&mut conn, "BEGIN").await.as_deref(), Ok("OK"));
        let mut local: Option<usize> = None;
        for key in &probe_keys {
            if set(&mut conn, key, "probe").await.is_ok() {
                local = Some(key_to_shard(key.as_bytes(), SHARDS));
            }
        }
        assert_eq!(txn(&mut conn, "ABORT").await.as_deref(), Ok("OK"));
        let local = local.expect("one probe key must be shard-local");

        let keys = keys_on_shard("txn499:ok", local, 2);
        assert_eq!(txn(&mut conn, "BEGIN").await.as_deref(), Ok("OK"));
        let all_ok = set(&mut conn, &keys[0], "a").await.is_ok()
            && set(&mut conn, &keys[1], "b").await.is_ok();
        if !all_ok {
            // Migrated mid-test — discard and re-probe.
            let _ = txn(&mut conn, "ABORT").await;
            continue;
        }
        assert_eq!(
            txn(&mut conn, "COMMIT").await.as_deref(),
            Ok("OK"),
            "a fully-accepted TXN must still commit OK"
        );
        assert_eq!(get(&mut conn, &keys[0]).await.as_deref(), Some("a"));
        assert_eq!(get(&mut conn, &keys[1]).await.as_deref(), Some("b"));
        committed = true;
        break;
    }
    assert!(committed, "clean multi-shard TXN never got to commit");

    // The aborted probe left nothing behind.
    for key in &probe_keys {
        assert_eq!(get(&mut conn, key).await, None, "{key} must not exist");
    }

    shutdown.cancel();
}
