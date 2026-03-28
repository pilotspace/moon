#![allow(unused_imports)]
#[cfg(feature = "runtime-tokio")]
use crate::runtime::TcpListener;
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use tracing::{debug, error, info};

#[cfg(feature = "runtime-tokio")]
use crate::command::connection as conn_cmd;
use crate::config::ServerConfig;
#[cfg(feature = "runtime-tokio")]
use crate::persistence::aof::{self, AofMessage, FsyncPolicy};
#[cfg(feature = "runtime-tokio")]
use crate::persistence::rdb;
#[cfg(feature = "runtime-tokio")]
use crate::persistence::replay::DispatchReplayEngine;
#[cfg(feature = "runtime-tokio")]
use crate::pubsub::PubSubRegistry;
#[cfg(feature = "runtime-tokio")]
use crate::storage::Database;
#[cfg(feature = "runtime-tokio")]
use crate::tracking::TrackingTable;

/// Type alias for the per-database RwLock container.
#[cfg(feature = "runtime-tokio")]
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

#[cfg(feature = "runtime-tokio")]
use super::connection;
#[cfg(feature = "runtime-tokio")]
use super::expiration;

/// Run the TCP server accept loop.
///
/// Binds to the configured address, spawns a task per connection, and shuts down
/// gracefully on Ctrl+C (SIGINT).
#[cfg(feature = "runtime-tokio")]
pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    let token = CancellationToken::new();
    let shutdown_token = token.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received");
        shutdown_token.cancel();
    });

    run_with_shutdown(config, token).await
}

/// Run the TCP server accept loop with an externally-provided shutdown token.
///
/// This is the core server loop, factored out from `run()` for testability.
/// Tests can create their own `CancellationToken` and cancel it to shut down
/// the server cleanly.
#[cfg(feature = "runtime-tokio")]
pub async fn run_with_shutdown(
    config: ServerConfig,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", config.bind, config.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on {}", addr);

    let databases: Vec<parking_lot::RwLock<Database>> = (0..config.databases)
        .map(|_| parking_lot::RwLock::new(Database::new()))
        .collect();
    let db: SharedDatabases = Arc::new(databases);

    // Startup restore: AOF takes priority over RDB
    let aof_path = PathBuf::from(&config.dir).join(&config.appendfilename);
    let rdb_path = PathBuf::from(&config.dir).join(&config.dbfilename);

    if config.appendonly == "yes" && aof_path.exists() {
        // Lock each db individually for AOF replay
        let mut dbs_vec: Vec<Database> = db
            .iter()
            .map(|lock| {
                let mut guard = lock.write();
                std::mem::replace(&mut *guard, Database::new())
            })
            .collect();
        match aof::replay_aof(&mut dbs_vec, &aof_path, &DispatchReplayEngine) {
            Ok(n) => info!(
                "AOF loaded: {} commands replayed from {}",
                n,
                aof_path.display()
            ),
            Err(e) => error!("AOF load failed: {}. Starting with empty database.", e),
        }
        // Put databases back
        for (lock, restored_db) in db.iter().zip(dbs_vec.into_iter()) {
            *lock.write() = restored_db;
        }
    } else if rdb_path.exists() {
        let mut dbs_vec: Vec<Database> = db
            .iter()
            .map(|lock| {
                let mut guard = lock.write();
                std::mem::replace(&mut *guard, Database::new())
            })
            .collect();
        match rdb::load(&mut dbs_vec, &rdb_path) {
            Ok(count) => info!("RDB loaded: {} keys from {}", count, rdb_path.display()),
            Err(e) => error!("Failed to load RDB: {}. Starting with empty database.", e),
        }
        for (lock, restored_db) in db.iter().zip(dbs_vec.into_iter()) {
            *lock.write() = restored_db;
        }
    }

    let config = Arc::new(config);

    // Set up AOF writer task if appendonly is enabled
    let aof_tx: Option<channel::MpscSender<AofMessage>> = if config.appendonly == "yes" {
        let (tx, rx) = channel::mpsc_bounded::<AofMessage>(10_000);
        let aof_token = token.child_token();
        let fsync = FsyncPolicy::from_str(&config.appendfsync);
        let aof_file_path = PathBuf::from(&config.dir).join(&config.appendfilename);
        tokio::spawn(aof::aof_writer_task(rx, aof_file_path, fsync, aof_token));
        info!("AOF enabled with fsync policy: {:?}", fsync);
        Some(tx)
    } else {
        None
    };

    // Set up change counter for auto-save
    let change_counter = Arc::new(AtomicU64::new(0));

    // Set up auto-save timer if save rules are configured
    if config.save.is_some() {
        let rules = crate::persistence::auto_save::parse_save_rules(&config.save);
        if !rules.is_empty() {
            let auto_save_db = db.clone();
            let auto_save_token = token.child_token();
            let auto_save_counter = change_counter.clone();
            let dir = config.dir.clone();
            let dbfilename = config.dbfilename.clone();
            tokio::spawn(crate::persistence::auto_save::run_auto_save(
                auto_save_db,
                rules,
                dir,
                dbfilename,
                auto_save_counter,
                auto_save_token,
            ));
            info!("Auto-save timer started");
        }
    }

    // Spawn active expiration background task
    let exp_db = db.clone();
    let exp_token = token.child_token();
    tokio::spawn(expiration::run_active_expiration(exp_db, exp_token));

    // Create shared Pub/Sub registry
    let pubsub_registry = Arc::new(Mutex::new(PubSubRegistry::new()));

    // Create shared runtime config (mutable via CONFIG SET)
    let runtime_config = Arc::new(RwLock::new(config.to_runtime_config()));

    // Create shared tracking table for client-side caching invalidation
    let tracking_table = Arc::new(Mutex::new(TrackingTable::new()));

    // Create replication state -- load persisted repl_id or generate new one.
    let (repl_id, repl_id2) =
        crate::replication::state::load_replication_state(std::path::Path::new(&config.dir));
    let repl_state = Arc::new(RwLock::new(
        crate::replication::state::ReplicationState::new(
            1, // non-sharded mode uses 1 shard
            repl_id, repl_id2,
        ),
    ));

    // Build ACL table from config (load aclfile if configured, else bootstrap from requirepass)
    let acl_table: Arc<RwLock<crate::acl::AclTable>> = {
        let table = crate::acl::AclTable::load_or_default(&config);
        Arc::new(RwLock::new(table))
    };

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((mut stream, addr)) => {
                        // Protected mode: reject non-loopback connections when no auth configured
                        if config.protected_mode == "yes"
                            && config.requirepass.is_none()
                            && config.aclfile.is_none()
                        {
                            let ip = addr.ip();
                            if !ip.is_loopback() {
                                tracing::warn!(
                                    "Protected mode: rejected connection from {} (no password set, use --protected-mode no to disable)",
                                    addr
                                );
                                let err_msg = b"-DENIED Redis is running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface. If you want to connect from external computers to Redis you may adopt one of the following solutions: 1) Just disable protected mode sending the command 'CONFIG SET protected-mode no' from the loopback interface. 2) Set a password for the default user.\r\n";
                                let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, err_msg).await;
                                continue;
                            }
                        }

                        debug!("New connection from {}", addr);
                        let db = db.clone();
                        let conn_token = token.child_token();
                        let requirepass = config.requirepass.clone();
                        let config = config.clone();
                        let aof_tx = aof_tx.clone();
                        let change_counter = Some(change_counter.clone());
                        let pubsub = pubsub_registry.clone();
                        let rt_config = runtime_config.clone();
                        let tracking = tracking_table.clone();
                        let cid = conn_cmd::next_client_id();
                        let rs = repl_state.clone();
                        let acl = acl_table.clone();
                        tokio::spawn(connection::handle_connection(
                            stream, db, conn_token, requirepass, config,
                            aof_tx, change_counter, pubsub, rt_config,
                            tracking, cid, Some(rs), acl,
                        ));
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }
            _ = token.cancelled() => {
                info!("Server shutting down");
                // Send shutdown to AOF writer
                if let Some(ref tx) = aof_tx {
                    let _ = tx.send_async(AofMessage::Shutdown).await;
                }
                break;
            }
        }
    }

    Ok(())
}

/// Run the sharded accept loop. Listener distributes connections to shard threads.
///
/// The listener runs on its own current_thread runtime (the main thread).
/// It does NOT own any database state -- all data lives in shard threads.
#[cfg(feature = "runtime-tokio")]
pub async fn run_sharded(
    config: ServerConfig,
    conn_txs: Vec<channel::MpscSender<(crate::runtime::TcpStream, bool)>>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", config.bind, config.port);
    let listener = TcpListener::bind(&addr).await?;
    let num_shards = conn_txs.len();
    info!("Listening on {} ({} shards)", addr, num_shards);

    // Note: protected_mode_active is evaluated once at startup. CONFIG SET protected-mode
    // changes RuntimeConfig on the shard side but does not propagate to the listener.
    // This matches Redis behavior where listener-level checks use startup config.
    // To disable protected mode at runtime, clients must connect via loopback first.
    let protected_mode_active =
        config.protected_mode == "yes" && config.requirepass.is_none() && config.aclfile.is_none();

    let mut next_shard: usize = 0;

    // Ctrl+C handler
    let shutdown_signal = shutdown.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        info!("Shutdown signal received");
        shutdown_signal.cancel();
    });

    // Spawn TLS listener if tls_port > 0
    if config.tls_port > 0 {
        let tls_addr = format!("{}:{}", config.bind, config.tls_port);
        let tls_listener = TcpListener::bind(&tls_addr).await?;
        info!("TLS listening on {} ({} shards)", tls_addr, num_shards);
        let tls_txs = conn_txs.clone();
        let tls_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut tls_next_shard: usize = 0;
            let tls_num_shards = tls_txs.len();
            loop {
                tokio::select! {
                    result = tls_listener.accept() => {
                        match result {
                            Ok((mut stream, addr)) => {
                                // Protected mode: reject non-loopback connections when no auth configured
                                if protected_mode_active && !addr.ip().is_loopback() {
                                    tracing::warn!(
                                        "Protected mode: rejected TLS connection from {} (no password set)",
                                        addr
                                    );
                                    let err_msg = b"-DENIED Redis is running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface.\r\n";
                                    let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, err_msg).await;
                                    continue;
                                }
                                debug!("New TLS connection from {} -> shard {}", addr, tls_next_shard);
                                let tx = &tls_txs[tls_next_shard];
                                if tx.send_async((stream, true)).await.is_err() {
                                    error!("Failed to send TLS connection to shard {}", tls_next_shard);
                                }
                                tls_next_shard = (tls_next_shard + 1) % tls_num_shards;
                            }
                            Err(e) => {
                                error!("TLS accept error: {}", e);
                            }
                        }
                    }
                    _ = tls_shutdown.cancelled() => {
                        info!("TLS listener shutting down");
                        break;
                    }
                }
            }
        });
    }

    // On Linux, per-shard SO_REUSEPORT listeners handle plain TCP accept directly.
    // The central listener only handles TLS connections (spawned above).
    // On non-Linux, the central listener handles all connections via round-robin MPSC.
    #[cfg(target_os = "linux")]
    let linux_reuseport_active = true;
    #[cfg(not(target_os = "linux"))]
    let linux_reuseport_active = false;

    loop {
        tokio::select! {
            // Plain TCP accept -- only active on non-Linux (macOS fallback).
            // On Linux, per-shard SO_REUSEPORT listeners handle plain TCP directly.
            result = async {
                if linux_reuseport_active {
                    std::future::pending::<std::io::Result<(tokio::net::TcpStream, std::net::SocketAddr)>>().await
                } else {
                    listener.accept().await
                }
            } => {
                match result {
                    Ok((mut stream, addr)) => {
                        // Protected mode: reject non-loopback connections when no auth configured
                        if protected_mode_active && !addr.ip().is_loopback() {
                            tracing::warn!(
                                "Protected mode: rejected connection from {} (no password set, use --protected-mode no to disable)",
                                addr
                            );
                            let err_msg = b"-DENIED Redis is running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface. If you want to connect from external computers to Redis you may adopt one of the following solutions: 1) Just disable protected mode sending the command 'CONFIG SET protected-mode no' from the loopback interface. 2) Set a password for the default user.\r\n";
                            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, err_msg).await;
                            continue;
                        }

                        debug!("New connection from {} -> shard {}", addr, next_shard);
                        let tx = &conn_txs[next_shard];
                        if tx.send_async((stream, false)).await.is_err() {
                            error!("Failed to send connection to shard {}", next_shard);
                        }
                        next_shard = (next_shard + 1) % num_shards;
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }
            _ = shutdown.cancelled() => {
                info!("Listener shutting down");
                break;
            }
        }
    }

    Ok(())
}

/// Non-sharded mode is not supported under monoio -- use `--shards 1`.
#[cfg(feature = "runtime-monoio")]
pub async fn run(_config: ServerConfig) -> anyhow::Result<()> {
    anyhow::bail!("Non-sharded mode not supported under monoio -- use --shards 1")
}

/// Run the sharded accept loop under Monoio.
///
/// Uses `monoio::net::TcpListener` for async accept and converts accepted
/// `monoio::net::TcpStream` to `std::net::TcpStream` for cross-thread handoff
/// to shard threads via flume channels.
#[cfg(feature = "runtime-monoio")]
pub async fn run_sharded(
    config: ServerConfig,
    conn_txs: Vec<channel::MpscSender<(crate::runtime::TcpStream, bool)>>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", config.bind, config.port);
    let listener = monoio::net::TcpListener::bind(&addr)?;
    let num_shards = conn_txs.len();
    info!("Listening on {} ({} shards, monoio)", addr, num_shards);

    // Note: protected_mode_active is evaluated once at startup (see tokio listener comment).
    let protected_mode_active =
        config.protected_mode == "yes" && config.requirepass.is_none() && config.aclfile.is_none();

    let mut next_shard: usize = 0;

    // Ctrl+C handler -- ctrlc crate sets handler on OS thread, signals our token
    let shutdown_signal = shutdown.clone();
    ctrlc::set_handler(move || {
        info!("Shutdown signal received");
        shutdown_signal.cancel();
    })
    .expect("failed to set Ctrl+C handler");

    // Spawn TLS listener if tls_port > 0
    let tls_listener = if config.tls_port > 0 {
        let tls_addr = format!("{}:{}", config.bind, config.tls_port);
        let tl = monoio::net::TcpListener::bind(&tls_addr)?;
        info!(
            "TLS listening on {} ({} shards, monoio)",
            tls_addr, num_shards
        );
        Some(tl)
    } else {
        None
    };
    let mut tls_next_shard: usize = 0;

    loop {
        // If TLS listener is configured, select on both plain and TLS accepts
        if let Some(ref tls_listener) = tls_listener {
            monoio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((mut stream, addr)) => {
                            if protected_mode_active && !addr.ip().is_loopback() {
                                tracing::warn!(
                                    "Protected mode: rejected connection from {} (no password set, use --protected-mode no to disable)",
                                    addr
                                );
                                let err_msg: Vec<u8> = b"-DENIED Redis is running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface.\r\n".to_vec();
                                let _ = monoio::io::AsyncWriteRentExt::write_all(&mut stream, err_msg).await;
                                continue;
                            }
                            debug!("New connection from {} -> shard {}", addr, next_shard);
                            let std_stream = {
                                use std::os::unix::io::{IntoRawFd, FromRawFd};
                                let fd = stream.into_raw_fd();
                                unsafe { std::net::TcpStream::from_raw_fd(fd) }
                            };
                            let tx = &conn_txs[next_shard];
                            if tx.send((std_stream, false)).is_err() {
                                error!("Failed to send connection to shard {}", next_shard);
                            }
                            next_shard = (next_shard + 1) % num_shards;
                        }
                        Err(e) => { error!("Accept error: {}", e); }
                    }
                }
                result = tls_listener.accept() => {
                    match result {
                        Ok((mut stream, addr)) => {
                            // Protected mode: reject non-loopback connections when no auth configured
                            if protected_mode_active && !addr.ip().is_loopback() {
                                tracing::warn!(
                                    "Protected mode: rejected TLS connection from {} (no password set)",
                                    addr
                                );
                                let err_msg: Vec<u8> = b"-DENIED Redis is running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface.\r\n".to_vec();
                                let _ = monoio::io::AsyncWriteRentExt::write_all(&mut stream, err_msg).await;
                                continue;
                            }
                            debug!("New TLS connection from {} -> shard {}", addr, tls_next_shard);
                            let std_stream = {
                                use std::os::unix::io::{IntoRawFd, FromRawFd};
                                let fd = stream.into_raw_fd();
                                unsafe { std::net::TcpStream::from_raw_fd(fd) }
                            };
                            let tx = &conn_txs[tls_next_shard];
                            if tx.send((std_stream, true)).is_err() {
                                error!("Failed to send TLS connection to shard {}", tls_next_shard);
                            }
                            tls_next_shard = (tls_next_shard + 1) % num_shards;
                        }
                        Err(e) => { error!("TLS accept error: {}", e); }
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("Listener shutting down");
                    break;
                }
            }
        } else {
            monoio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((mut stream, addr)) => {
                            if protected_mode_active && !addr.ip().is_loopback() {
                                tracing::warn!(
                                    "Protected mode: rejected connection from {} (no password set, use --protected-mode no to disable)",
                                    addr
                                );
                                let err_msg: Vec<u8> = b"-DENIED Redis is running in protected mode because protected mode is enabled and no password is set for the default user. In this mode connections are only accepted from the loopback interface. If you want to connect from external computers to Redis you may adopt one of the following solutions: 1) Just disable protected mode sending the command 'CONFIG SET protected-mode no' from the loopback interface. 2) Set a password for the default user.\r\n".to_vec();
                                let _ = monoio::io::AsyncWriteRentExt::write_all(&mut stream, err_msg).await;
                                continue;
                            }

                            debug!("New connection from {} -> shard {}", addr, next_shard);
                            let std_stream = {
                                use std::os::unix::io::{IntoRawFd, FromRawFd};
                                let fd = stream.into_raw_fd();
                                unsafe { std::net::TcpStream::from_raw_fd(fd) }
                            };
                            let tx = &conn_txs[next_shard];
                            if tx.send((std_stream, false)).is_err() {
                                error!("Failed to send connection to shard {}", next_shard);
                            }
                            next_shard = (next_shard + 1) % num_shards;
                        }
                        Err(e) => {
                            error!("Accept error: {}", e);
                        }
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("Listener shutting down");
                    break;
                }
            }
        }
    }

    Ok(())
}
