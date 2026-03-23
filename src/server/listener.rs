use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::ServerConfig;
use crate::storage::Database;

use super::connection;

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

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        info!("New connection from {}", addr);
                        let db = db.clone();
                        let conn_token = token.child_token();
                        tokio::spawn(connection::handle_connection(stream, db, conn_token));
                    }
                    Err(e) => {
                        error!("Accept error: {}", e);
                    }
                }
            }
            _ = token.cancelled() => {
                info!("Server shutting down");
                break;
            }
        }
    }

    Ok(())
}
