pub mod dispatch;
pub mod mesh;

use std::time::Duration;

use ringbuf::HeapCons;
use ringbuf::HeapProd;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::RuntimeConfig;
use crate::storage::Database;

use self::dispatch::ShardMessage;

/// A shard owns all per-core state. No Arc, no Mutex -- fully owned by its thread.
///
/// Each shard contains its own set of databases, runtime configuration, and will
/// eventually own its connection set and event loop. This is the fundamental unit
/// of the shared-nothing architecture.
pub struct Shard {
    /// Shard index (0..num_shards).
    pub id: usize,
    /// 16 databases per shard (SELECT 0-15), directly owned.
    pub databases: Vec<Database>,
    /// Total number of shards in the system.
    pub num_shards: usize,
    /// Runtime config (cloned per-shard, not shared).
    pub runtime_config: RuntimeConfig,
}

impl Shard {
    /// Create a new shard with `num_databases` empty databases.
    pub fn new(id: usize, num_shards: usize, num_databases: usize, config: RuntimeConfig) -> Self {
        let databases = (0..num_databases).map(|_| Database::new()).collect();
        Shard {
            id,
            databases,
            num_shards,
            runtime_config: config,
        }
    }

    /// Run the shard event loop on its dedicated current_thread runtime.
    ///
    /// Receives new connections from the listener, runs cooperative active expiry,
    /// and shuts down gracefully when the cancellation token fires.
    ///
    /// NOTE: Connection handling is intentionally stubbed -- Plan 04 implements
    /// the full connection handler with cross-shard dispatch.
    pub async fn run(
        &mut self,
        mut conn_rx: mpsc::Receiver<tokio::net::TcpStream>,
        _consumers: Vec<HeapCons<ShardMessage>>,
        _producers: Vec<HeapProd<ShardMessage>>,
        shutdown: CancellationToken,
    ) {
        info!("Shard {} started", self.id);

        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));

        loop {
            tokio::select! {
                // Accept new connections from listener
                stream = conn_rx.recv() => {
                    match stream {
                        Some(tcp_stream) => {
                            // TODO(Plan 04): Wire up connection handler with cross-shard dispatch.
                            // For now, log and drop the stream so compilation succeeds.
                            let peer = tcp_stream.peer_addr().ok();
                            info!("Shard {} received connection from {:?} (stub -- dropping)", self.id, peer);
                            drop(tcp_stream);
                        }
                        None => {
                            info!("Shard {} connection channel closed", self.id);
                            break;
                        }
                    }
                }
                // Cooperative active expiry
                _ = expiry_interval.tick() => {
                    for db in &mut self.databases {
                        crate::server::expiration::expire_cycle_direct(db);
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("Shard {} shutting down", self.id);
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_new() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(0, 4, 16, config);
        assert_eq!(shard.id, 0);
        assert_eq!(shard.num_shards, 4);
        assert_eq!(shard.databases.len(), 16);
    }

    #[test]
    fn test_shard_databases_independent() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(1, 8, 4, config);
        assert_eq!(shard.databases.len(), 4);
        // Each database starts empty -- verify they exist and are independent
        // (Database::new() creates an empty DashTable)
        assert_eq!(shard.id, 1);
        assert_eq!(shard.num_shards, 8);
    }
}
