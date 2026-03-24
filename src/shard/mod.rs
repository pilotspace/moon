pub mod coordinator;
pub mod dispatch;
pub mod mesh;

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use ringbuf::traits::Consumer;
use ringbuf::HeapCons;
use ringbuf::HeapProd;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::command::{dispatch as cmd_dispatch, DispatchResult};
use crate::config::RuntimeConfig;
use crate::pubsub::PubSubRegistry;
use crate::server::connection::handle_connection_sharded;
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
    /// Per-shard Pub/Sub registry -- no global Mutex, fully owned by shard thread.
    pub pubsub_registry: PubSubRegistry,
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
            pubsub_registry: PubSubRegistry::new(),
        }
    }

    /// Run the shard event loop on its dedicated current_thread runtime.
    ///
    /// Wraps shard databases, pubsub registry, and SPSC producers in `Rc<RefCell<...>>`
    /// (safe because the runtime is single-threaded -- cooperative scheduling prevents
    /// concurrent borrows).
    ///
    /// Receives new connections from the listener and spawns them as local tasks.
    /// Drains SPSC consumers for cross-shard dispatch requests and PubSubFanOut.
    /// Runs cooperative active expiry. Shuts down gracefully on cancellation.
    pub async fn run(
        &mut self,
        mut conn_rx: mpsc::Receiver<tokio::net::TcpStream>,
        mut consumers: Vec<HeapCons<ShardMessage>>,
        producers: Vec<HeapProd<ShardMessage>>,
        shutdown: CancellationToken,
        aof_tx: Option<mpsc::Sender<crate::persistence::aof::AofMessage>>,
    ) {
        info!("Shard {} started", self.id);

        // Wrap databases and pubsub_registry in Rc<RefCell> for sharing with spawned
        // connection tasks. Safe: single-threaded runtime guarantees no concurrent access.
        let databases = Rc::new(RefCell::new(std::mem::take(&mut self.databases)));
        let dispatch_tx = Rc::new(RefCell::new(producers));
        let pubsub_rc = Rc::new(RefCell::new(std::mem::take(&mut self.pubsub_registry)));

        let shard_id = self.id;
        let num_shards = self.num_shards;

        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));
        // SPSC drain interval -- check for cross-shard messages every 1ms
        let mut spsc_interval = tokio::time::interval(Duration::from_millis(1));

        loop {
            tokio::select! {
                // Accept new connections from listener
                stream = conn_rx.recv() => {
                    match stream {
                        Some(tcp_stream) => {
                            let dbs = databases.clone();
                            let dtx = dispatch_tx.clone();
                            let psr = pubsub_rc.clone();
                            let sd = shutdown.clone();
                            let aof = aof_tx.clone();
                            tokio::task::spawn_local(async move {
                                handle_connection_sharded(
                                    tcp_stream,
                                    dbs,
                                    shard_id,
                                    num_shards,
                                    dtx,
                                    psr,
                                    sd,
                                    None, // requirepass: TODO wire from shard config
                                    aof,
                                ).await;
                            });
                        }
                        None => {
                            info!("Shard {} connection channel closed", self.id);
                            break;
                        }
                    }
                }
                // Drain SPSC consumers for cross-shard messages
                _ = spsc_interval.tick() => {
                    Self::drain_spsc_shared(&databases, &mut consumers, &mut *pubsub_rc.borrow_mut());
                }
                // Cooperative active expiry
                _ = expiry_interval.tick() => {
                    let mut dbs = databases.borrow_mut();
                    for db in dbs.iter_mut() {
                        crate::server::expiration::expire_cycle_direct(db);
                    }
                }
                _ = shutdown.cancelled() => {
                    info!("Shard {} shutting down", self.id);
                    break;
                }
            }
        }

        // Restore databases and pubsub_registry back to self for cleanup.
        self.databases = match Rc::try_unwrap(databases) {
            Ok(refcell) => refcell.into_inner(),
            Err(_) => {
                info!("Shard {}: could not reclaim databases (outstanding Rc references)", self.id);
                Vec::new()
            }
        };
        self.pubsub_registry = match Rc::try_unwrap(pubsub_rc) {
            Ok(refcell) => refcell.into_inner(),
            Err(_) => {
                info!("Shard {}: could not reclaim pubsub_registry (outstanding Rc references)", self.id);
                PubSubRegistry::new()
            }
        };
    }

    /// Drain all SPSC consumer channels, processing cross-shard messages.
    fn drain_spsc_shared(
        databases: &Rc<RefCell<Vec<Database>>>,
        consumers: &mut [HeapCons<ShardMessage>],
        pubsub_registry: &mut PubSubRegistry,
    ) {
        const MAX_DRAIN_PER_CYCLE: usize = 256;
        let mut drained = 0;

        for consumer in consumers.iter_mut() {
            while drained < MAX_DRAIN_PER_CYCLE {
                match consumer.try_pop() {
                    Some(msg) => {
                        drained += 1;
                        Self::handle_shard_message_shared(databases, pubsub_registry, msg);
                    }
                    None => break,
                }
            }
            if drained >= MAX_DRAIN_PER_CYCLE {
                break;
            }
        }
    }

    /// Process a single cross-shard message using shared database access.
    fn handle_shard_message_shared(
        databases: &Rc<RefCell<Vec<Database>>>,
        pubsub_registry: &mut PubSubRegistry,
        msg: ShardMessage,
    ) {
        match msg {
            ShardMessage::Execute {
                db_index,
                command,
                reply_tx,
            } => {
                let response = {
                    let mut dbs = databases.borrow_mut();
                    let db_count = dbs.len();
                    let db_idx = db_index.min(db_count.saturating_sub(1));
                    dbs[db_idx].refresh_now();
                    let (cmd, args) = match Self::extract_command_static(&command) {
                        Some(pair) => pair,
                        None => {
                            let _ = reply_tx.send(crate::protocol::Frame::Error(
                                bytes::Bytes::from_static(b"ERR invalid command format"),
                            ));
                            return;
                        }
                    };
                    let mut selected = db_idx;
                    let result = cmd_dispatch(&mut dbs[db_idx], cmd, args, &mut selected, db_count);
                    match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    }
                };
                let _ = reply_tx.send(response);
            }
            ShardMessage::MultiExecute {
                db_index,
                commands,
                reply_tx,
            } => {
                let mut results = Vec::with_capacity(commands.len());
                let mut dbs = databases.borrow_mut();
                let db_count = dbs.len();
                let db_idx = db_index.min(db_count.saturating_sub(1));
                dbs[db_idx].refresh_now();
                for (_key, cmd_frame) in &commands {
                    let (cmd, args) = match Self::extract_command_static(cmd_frame) {
                        Some(pair) => pair,
                        None => {
                            results.push(crate::protocol::Frame::Error(
                                bytes::Bytes::from_static(b"ERR invalid command format"),
                            ));
                            continue;
                        }
                    };
                    let mut selected = db_idx;
                    let result =
                        cmd_dispatch(&mut dbs[db_idx], cmd, args, &mut selected, db_count);
                    let frame = match result {
                        DispatchResult::Response(f) => f,
                        DispatchResult::Quit(f) => f,
                    };
                    results.push(frame);
                }
                let _ = reply_tx.send(results);
            }
            ShardMessage::PubSubFanOut { channel, message } => {
                pubsub_registry.publish(&channel, &message);
            }
            ShardMessage::SnapshotRequest { reply_tx } => {
                // Clone all databases in this shard for RDB snapshot.
                // NOTE: Phase 11 snapshots are NOT point-in-time across shards.
                // Keys modified between per-shard snapshot requests may be inconsistent.
                // Phase 14 implements true consistent snapshots via compartmentalized persistence.
                let snapshot: Vec<(Vec<(bytes::Bytes, crate::storage::entry::Entry)>, u32)> = {
                    let dbs = databases.borrow();
                    dbs.iter().map(|db| {
                        let base_ts = db.base_timestamp();
                        let entries: Vec<(bytes::Bytes, crate::storage::entry::Entry)> = db.data().iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        (entries, base_ts)
                    }).collect()
                };
                let _ = reply_tx.send(snapshot);
            }
            ShardMessage::Shutdown => {
                info!("Received shutdown via SPSC");
            }
            ShardMessage::NewConnection(_) => {
                // NewConnection is handled via conn_rx, not SPSC
            }
        }
    }

    /// Extract command name and args from a Frame (static helper for SPSC dispatch).
    fn extract_command_static(frame: &crate::protocol::Frame) -> Option<(&[u8], &[crate::protocol::Frame])> {
        match frame {
            crate::protocol::Frame::Array(args) if !args.is_empty() => {
                let name = match &args[0] {
                    crate::protocol::Frame::BulkString(s) => s.as_ref(),
                    crate::protocol::Frame::SimpleString(s) => s.as_ref(),
                    _ => return None,
                };
                Some((name, &args[1..]))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ringbuf::HeapRb;
    use ringbuf::traits::{Producer, Split};
    use tokio::sync::mpsc as tokio_mpsc;

    use crate::protocol::Frame;
    use crate::pubsub::subscriber::Subscriber;

    #[test]
    fn test_shard_new() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(0, 4, 16, config);
        assert_eq!(shard.id, 0);
        assert_eq!(shard.num_shards, 4);
        assert_eq!(shard.databases.len(), 16);
    }

    #[test]
    fn test_shard_has_pubsub_registry() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(0, 4, 16, config);
        assert_eq!(shard.pubsub_registry.channel_subscription_count(1), 0);
        assert_eq!(shard.pubsub_registry.pattern_subscription_count(1), 0);
    }

    #[test]
    fn test_shard_databases_independent() {
        let config = RuntimeConfig::default();
        let shard = Shard::new(1, 8, 4, config);
        assert_eq!(shard.databases.len(), 4);
        assert_eq!(shard.id, 1);
        assert_eq!(shard.num_shards, 8);
    }

    #[test]
    fn test_pubsub_fanout_via_spsc() {
        let mut pubsub = PubSubRegistry::new();
        let databases = Rc::new(RefCell::new(vec![Database::new()]));

        let (tx, mut rx) = tokio_mpsc::channel::<Frame>(16);
        let sub = Subscriber::new(tx, 42);
        pubsub.subscribe(Bytes::from_static(b"news"), sub);

        let rb = HeapRb::new(64);
        let (mut prod, mut cons) = rb.split();
        prod.try_push(ShardMessage::PubSubFanOut {
            channel: Bytes::from_static(b"news"),
            message: Bytes::from_static(b"hello from shard 1"),
        })
        .ok()
        .expect("push should succeed");

        Shard::drain_spsc_shared(&databases, &mut [cons], &mut pubsub);

        let msg = rx.try_recv().expect("subscriber should receive message");
        match msg {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Bytes::from_static(b"message")));
                assert_eq!(parts[1], Frame::BulkString(Bytes::from_static(b"news")));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Bytes::from_static(b"hello from shard 1"))
                );
            }
            _ => panic!("expected Array frame"),
        }
    }

    #[test]
    fn test_drain_spsc_respects_limit() {
        let mut pubsub = PubSubRegistry::new();
        let databases = Rc::new(RefCell::new(vec![Database::new()]));

        let rb = HeapRb::new(512);
        let (mut prod, mut cons) = rb.split();

        for _ in 0..300 {
            prod.try_push(ShardMessage::PubSubFanOut {
                channel: Bytes::from_static(b"ch"),
                message: Bytes::from_static(b"msg"),
            })
            .ok()
            .unwrap();
        }

        Shard::drain_spsc_shared(&databases, &mut [cons], &mut pubsub);
    }
}
