#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use rust_redis::config::ServerConfig;
use rust_redis::server;
use rust_redis::shard::mesh::{ChannelMesh, CHANNEL_BUFFER_SIZE};
use rust_redis::shard::Shard;
use tokio_util::sync::CancellationToken;
use tracing::info;

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rust_redis=info".into()),
        )
        .init();

    let config = ServerConfig::parse();

    // Determine number of shards
    let num_shards = if config.shards == 0 {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    } else {
        config.shards
    };

    info!("Starting with {} shards", num_shards);

    // Create channel mesh for inter-shard communication
    let mut mesh = ChannelMesh::new(num_shards, CHANNEL_BUFFER_SIZE);

    // Shared cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();

    // Collect connection senders for the listener before spawning shard threads
    let conn_txs: Vec<_> = (0..num_shards).map(|i| mesh.conn_tx(i)).collect();

    // Spawn shard threads
    let mut shard_handles = Vec::with_capacity(num_shards);
    for id in 0..num_shards {
        let producers = mesh.take_producers(id);
        let consumers = mesh.take_consumers(id);
        let conn_rx = mesh.take_conn_rx(id);
        let shard_config = config.clone();
        let shard_cancel = cancel_token.clone();

        let handle = std::thread::Builder::new()
            .name(format!("shard-{}", id))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build shard runtime");

                let mut shard = Shard::new(
                    id,
                    num_shards,
                    shard_config.databases,
                    shard_config.to_runtime_config(),
                );

                rt.block_on(shard.run(conn_rx, consumers, producers, shard_cancel));
            })
            .expect("failed to spawn shard thread");

        shard_handles.push(handle);
    }

    // Run the sharded listener on the main thread with its own current_thread runtime
    let listener_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build listener runtime");

    let listener_cancel = cancel_token.clone();
    listener_rt.block_on(async {
        if let Err(e) = server::listener::run_sharded(config, conn_txs, listener_cancel).await {
            tracing::error!("Listener error: {}", e);
        }
    });

    // After listener exits, cancel all shards and join threads
    cancel_token.cancel();
    for handle in shard_handles {
        let _ = handle.join();
    }

    info!("Server shut down");
    Ok(())
}
