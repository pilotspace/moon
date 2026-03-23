use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::{RuntimeConfig, ServerConfig};
use crate::persistence::aof::{self, AofMessage, FsyncPolicy};
use crate::persistence::rdb;
use crate::pubsub::PubSubRegistry;
use crate::storage::Database;

use super::connection;
use super::expiration;

/// Run the TCP server accept loop.
///
/// Binds to the configured address, spawns a task per connection, and shuts down
/// gracefully on Ctrl+C (SIGINT).
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
pub async fn run_with_shutdown(
    config: ServerConfig,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let addr = format!("{}:{}", config.bind, config.port);
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on {}", addr);

    let databases: Vec<Database> = (0..config.databases).map(|_| Database::new()).collect();
    let db = Arc::new(Mutex::new(databases));

    // Startup restore: AOF takes priority over RDB
    let aof_path = PathBuf::from(&config.dir).join(&config.appendfilename);
    let rdb_path = PathBuf::from(&config.dir).join(&config.dbfilename);

    if config.appendonly == "yes" && aof_path.exists() {
        let mut dbs = db.lock();
        match aof::replay_aof(&mut dbs, &aof_path) {
            Ok(n) => info!("AOF loaded: {} commands replayed from {}", n, aof_path.display()),
            Err(e) => error!("AOF load failed: {}. Starting with empty database.", e),
        }
    } else if rdb_path.exists() {
        let mut dbs = db.lock();
        match rdb::load(&mut dbs, &rdb_path) {
            Ok(count) => info!("RDB loaded: {} keys from {}", count, rdb_path.display()),
            Err(e) => error!("Failed to load RDB: {}. Starting with empty database.", e),
        }
    }

    let config = Arc::new(config);

    // Set up AOF writer task if appendonly is enabled
    let aof_tx: Option<mpsc::Sender<AofMessage>> = if config.appendonly == "yes" {
        let (tx, rx) = mpsc::channel::<AofMessage>(10_000);
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

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        info!("New connection from {}", addr);
                        let db = db.clone();
                        let conn_token = token.child_token();
                        let requirepass = config.requirepass.clone();
                        let config = config.clone();
                        let aof_tx = aof_tx.clone();
                        let change_counter = Some(change_counter.clone());
                        let pubsub = pubsub_registry.clone();
                        let rt_config = runtime_config.clone();
                        tokio::spawn(connection::handle_connection(
                            stream, db, conn_token, requirepass, config,
                            aof_tx, change_counter, pubsub, rt_config,
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
                    let _ = tx.send(AofMessage::Shutdown).await;
                }
                break;
            }
        }
    }

    Ok(())
}
