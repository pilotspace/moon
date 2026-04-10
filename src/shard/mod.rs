pub mod affinity;
pub mod conn_accept;
pub mod coordinator;
pub mod dispatch;
pub mod event_loop;
pub mod mesh;
pub mod numa;
pub mod persistence_tick;
pub mod remote_subscriber_map;
pub mod shared_databases;
pub mod spsc_handler;
pub mod timers;
pub mod uring_handler;

use tracing::info;

use crate::config::RuntimeConfig;
use crate::persistence::replay::DispatchReplayEngine;
use crate::pubsub::PubSubRegistry;
use crate::storage::Database;
use crate::vector::store::VectorStore;
#[cfg(feature = "graph")]
use crate::graph::store::GraphStore;

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
    /// Per-shard vector store -- no Arc, no Mutex, fully owned by shard thread.
    pub vector_store: VectorStore,
    /// Per-shard graph store -- lazy init, zero memory when unused.
    #[cfg(feature = "graph")]
    pub graph_store: GraphStore,
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
            vector_store: VectorStore::new(),
            #[cfg(feature = "graph")]
            graph_store: GraphStore::new(),
        }
    }

    /// Restore shard state from per-shard snapshot and WAL files at startup.
    ///
    /// When `disk_offload_dir` is `Some`, uses the v3 recovery protocol
    /// (6-phase: control file -> manifest -> data load -> WAL v3 replay ->
    /// consistency -> ready). Falls back to v2 path on v3 failure.
    ///
    /// When `disk_offload_dir` is `None`, uses the existing v2 path:
    /// load per-shard RRDSHARD snapshot, replay per-shard WAL v2.
    ///
    /// Returns total keys loaded (snapshot + WAL replay).
    pub fn restore_from_persistence(
        &mut self,
        persistence_dir: &str,
        disk_offload_dir: Option<&std::path::Path>,
    ) -> usize {
        // If disk-offload was enabled, use v3 recovery protocol
        if let Some(offload_dir) = disk_offload_dir {
            let shard_dir = offload_dir.join(format!("shard-{}", self.id));
            if shard_dir.exists() {
                match crate::persistence::recovery::recover_shard_v3_with_fallback(
                    &mut self.databases,
                    self.id,
                    &shard_dir,
                    &DispatchReplayEngine,
                    Some(std::path::Path::new(persistence_dir)),
                ) {
                    Ok(result) => {
                        info!(
                            "Shard {}: v3 recovery complete (cmds={}, fpi={}, last_lsn={}, warm={}, cold={}, kv_heap={}, txn_rollback={})",
                            self.id,
                            result.commands_replayed,
                            result.fpi_applied,
                            result.last_lsn,
                            result.warm_segments_loaded,
                            result.cold_segments_loaded,
                            result.kv_heap_entries_loaded,
                            result.txns_rolled_back,
                        );
                        // Initialize cold_index + cold_shard_dir on all databases
                        // so cold_read_through can find keys spilled to NVMe.
                        {
                            let cold_dir = shard_dir.clone();
                            for db in &mut self.databases {
                                db.cold_shard_dir = Some(cold_dir.clone());
                                if db.cold_index.is_none() {
                                    db.cold_index =
                                        Some(crate::storage::tiered::cold_index::ColdIndex::new());
                                }
                            }
                            if let Some(recovered_ci) = result.cold_index {
                                if let Some(ref mut ci) = self.databases[0].cold_index {
                                    ci.merge(recovered_ci);
                                }
                            }
                        }

                        // Vector recovery still uses the v2 path for now
                        self.recover_vectors(persistence_dir);

                        // Register warm segments into VectorStore so they're searchable
                        if !result.warm_segments.is_empty() {
                            info!(
                                "Shard {}: registering {} warm segment(s)",
                                self.id,
                                result.warm_segments.len()
                            );
                            self.vector_store
                                .register_warm_segments(result.warm_segments);
                        }

                        // Register cold DiskANN segments for discovery
                        if !result.cold_segments.is_empty() {
                            info!(
                                "Shard {}: registering {} cold segment(s)",
                                self.id,
                                result.cold_segments.len()
                            );
                            self.vector_store
                                .register_cold_segments(result.cold_segments);
                        }
                        return result.commands_replayed;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Shard {}: v3 recovery failed, falling back to v2: {}",
                            self.id,
                            e
                        );
                        // Fall through to v2 path
                    }
                }
            }
        }

        // Existing v2 path (unchanged)
        self.restore_from_persistence_v2(persistence_dir)
    }

    /// V2 recovery path: snapshot load + WAL v2 replay + vector recovery.
    fn restore_from_persistence_v2(&mut self, persistence_dir: &str) -> usize {
        use crate::persistence::snapshot::shard_snapshot_load;
        use crate::persistence::wal;

        let dir = std::path::Path::new(persistence_dir);
        let mut total_keys = 0;

        // Load per-shard snapshot
        let snap_path = dir.join(format!("shard-{}.rrdshard", self.id));
        if snap_path.exists() {
            match shard_snapshot_load(&mut self.databases, &snap_path) {
                Ok(n) => {
                    info!("Shard {}: loaded {} keys from snapshot", self.id, n);
                    total_keys += n;
                }
                Err(e) => {
                    tracing::error!("Shard {}: snapshot load failed: {}", self.id, e);
                }
            }
        }

        // Replay per-shard WAL, then fall back to appendonly.aof if WAL has 0 commands.
        // The per-shard WalWriter writes to shard-N.wal but the global AOF writer
        // (aof_writer_task) writes to appendonly.aof. Both may exist; try both.
        let wal_file = wal::wal_path(dir, self.id);
        let mut wal_replayed = 0usize;
        if wal_file.exists() {
            match wal::replay_wal(&mut self.databases, &wal_file, &DispatchReplayEngine) {
                Ok(n) => {
                    info!("Shard {}: replayed {} WAL commands", self.id, n);
                    wal_replayed = n;
                    total_keys += n;
                }
                Err(e) => {
                    tracing::error!("Shard {}: WAL replay failed: {}", self.id, e);
                }
            }
        }
        // Fall back to appendonly.aof when per-shard WAL has 0 commands
        if wal_replayed == 0 {
            let aof_path = dir.join("appendonly.aof");
            if aof_path.exists() {
                info!(
                    "Shard {}: WAL empty, falling back to appendonly.aof",
                    self.id
                );
                match crate::persistence::aof::replay_aof(
                    &mut self.databases,
                    &aof_path,
                    &DispatchReplayEngine,
                ) {
                    Ok(n) => {
                        info!("Shard {}: replayed {} AOF commands", self.id, n);
                        total_keys += n;
                    }
                    Err(e) => {
                        tracing::error!("Shard {}: AOF replay failed: {}", self.id, e);
                    }
                }
            }
        }

        // Recover vector store
        self.recover_vectors(persistence_dir);

        total_keys
    }

    /// Recover vector store from WAL + on-disk segments.
    fn recover_vectors(&mut self, persistence_dir: &str) {
        let dir = std::path::Path::new(persistence_dir);
        let wal_file = crate::persistence::wal::wal_path(dir, self.id);
        let vector_persist_dir = dir.join(format!("shard-{}-vectors", self.id));
        if vector_persist_dir.exists() || wal_file.exists() {
            match crate::vector::persistence::recovery::recover_vector_store(
                &wal_file,
                &vector_persist_dir,
            ) {
                Ok(recovered) => {
                    let seg_count: usize = recovered
                        .collections
                        .values()
                        .map(|c| c.immutable.len())
                        .sum();
                    if !recovered.collections.is_empty() {
                        info!(
                            "Shard {}: recovered {} vector collections ({} immutable segments)",
                            self.id,
                            recovered.collections.len(),
                            seg_count
                        );
                    }
                    self.vector_store.attach_recovered(recovered);
                }
                Err(e) => {
                    tracing::error!("Shard {}: vector recovery failed: {:?}", self.id, e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::BlockingRegistry;
    use crate::framevec;
    use crate::persistence::wal::WalWriter;
    use crate::protocol::Frame;
    use crate::pubsub::subscriber::Subscriber;
    use crate::runtime::channel as rt_channel;
    use crate::storage::entry::CachedClock;
    use bytes::Bytes;
    use ringbuf::HeapRb;
    use ringbuf::traits::{Producer, Split};
    use std::cell::RefCell;
    use std::rc::Rc;

    use self::dispatch::ShardMessage;
    use self::shared_databases::ShardDatabases;

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
        let shard_databases = ShardDatabases::new(vec![vec![Database::new()]]);

        let (tx, rx) = rt_channel::mpsc_bounded::<Bytes>(16);
        let sub = Subscriber::new(tx, 42);
        pubsub.subscribe(Bytes::from_static(b"news"), sub);

        let rb = HeapRb::new(64);
        let (mut prod, cons) = rb.split();
        let slot = std::sync::Arc::new(crate::shard::dispatch::PubSubResponseSlot::new(0));
        prod.try_push(ShardMessage::PubSubPublish {
            channel: Bytes::from_static(b"news"),
            message: Bytes::from_static(b"hello from shard 1"),
            slot,
        })
        .ok()
        .expect("push should succeed");

        let mut pending_snap = None;
        let mut snap_state = None;
        let mut wal_w: Option<WalWriter> = None;
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        let mut vs = crate::vector::store::VectorStore::new();
        spsc_handler::drain_spsc_shared(
            &shard_databases,
            &mut [cons],
            &mut pubsub,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut wal_w,
            &mut None, // wal_v3_writer
            &mut None,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
            &mut Vec::new(),
            &mut vs,
        );

        // Subscriber now receives pre-serialized RESP bytes
        let msg = rx.try_recv().expect("subscriber should receive message");
        // Verify it's valid RESP: *3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$18\r\nhello from shard 1\r\n
        let expected = b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$18\r\nhello from shard 1\r\n";
        assert_eq!(&msg[..], &expected[..]);
    }

    #[test]
    fn test_drain_spsc_respects_limit() {
        let mut pubsub = PubSubRegistry::new();
        let shard_databases = ShardDatabases::new(vec![vec![Database::new()]]);

        let rb = HeapRb::new(512);
        let (mut prod, cons) = rb.split();

        for _ in 0..300 {
            let slot = std::sync::Arc::new(crate::shard::dispatch::PubSubResponseSlot::new(0));
            prod.try_push(ShardMessage::PubSubPublish {
                channel: Bytes::from_static(b"ch"),
                message: Bytes::from_static(b"msg"),
                slot,
            })
            .ok()
            .unwrap();
        }

        let mut pending_snap = None;
        let mut snap_state = None;
        let mut wal_w: Option<WalWriter> = None;
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        let mut vs = crate::vector::store::VectorStore::new();
        spsc_handler::drain_spsc_shared(
            &shard_databases,
            &mut [cons],
            &mut pubsub,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut wal_w,
            &mut None, // wal_v3_writer
            &mut None,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
            &mut Vec::new(),
            &mut vs,
        );
    }

    #[test]
    fn test_extract_command_static_ping() {
        let frame = Frame::Array(framevec![Frame::BulkString(Bytes::from_static(b"PING")),]);
        let (cmd, args) = spsc_handler::extract_command_static(&frame).unwrap();
        assert_eq!(cmd, b"PING");
        assert!(args.is_empty());
    }

    #[test]
    fn test_extract_command_static_with_args() {
        let frame = Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"key")),
            Frame::BulkString(Bytes::from_static(b"value")),
        ]);
        let (cmd, args) = spsc_handler::extract_command_static(&frame).unwrap();
        assert_eq!(cmd, b"SET");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn test_extract_command_static_invalid() {
        // Non-array frame
        let frame = Frame::SimpleString(Bytes::from_static(b"PING"));
        assert!(spsc_handler::extract_command_static(&frame).is_none());

        // Empty array
        let frame = Frame::Array(framevec![]);
        assert!(spsc_handler::extract_command_static(&frame).is_none());

        // Array with non-string first element
        let frame = Frame::Array(framevec![Frame::Integer(42)]);
        assert!(spsc_handler::extract_command_static(&frame).is_none());
    }

    /// Linux-only: verify handle_uring_event processes Disconnect correctly.
    #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
    #[test]
    fn test_handle_uring_event_disconnect() {
        if std::env::var("MOON_NO_URING").is_ok() {
            return; // io_uring unavailable in this environment
        }
        use crate::io::{IoEvent, UringConfig, UringDriver};

        let config = RuntimeConfig::default();
        let shard = Shard::new(0, 1, 1, config);
        let shard_databases = ShardDatabases::new(vec![shard.databases]);
        let mut parse_bufs = std::collections::HashMap::new();
        parse_bufs.insert(42u32, bytes::BytesMut::from(&b"partial"[..]));
        let mut inflight_sends = std::collections::HashMap::new();

        let mut driver = UringDriver::new(UringConfig::default()).unwrap();
        driver.init().unwrap();

        let clock = CachedClock::new();

        uring_handler::handle_uring_event(
            IoEvent::Disconnect { conn_id: 42 },
            &mut driver,
            &shard_databases,
            0,
            &mut parse_bufs,
            &mut inflight_sends,
            None,
            &clock,
        );

        assert!(
            !parse_bufs.contains_key(&42),
            "parse buffer should be removed on disconnect"
        );
    }

    /// Linux-only: verify handle_uring_event processes SendComplete as no-op.
    #[cfg(all(target_os = "linux", feature = "runtime-tokio"))]
    #[test]
    fn test_handle_uring_event_send_complete() {
        if std::env::var("MOON_NO_URING").is_ok() {
            return; // io_uring unavailable in this environment
        }
        use crate::io::{IoEvent, UringConfig, UringDriver};

        let config = RuntimeConfig::default();
        let shard = Shard::new(0, 1, 1, config);
        let shard_databases = ShardDatabases::new(vec![shard.databases]);
        let mut parse_bufs = std::collections::HashMap::new();
        let mut inflight_sends = std::collections::HashMap::new();

        let mut driver = UringDriver::new(UringConfig::default()).unwrap();
        driver.init().unwrap();

        let clock = CachedClock::new();

        uring_handler::handle_uring_event(
            IoEvent::SendComplete { conn_id: 1 },
            &mut driver,
            &shard_databases,
            0,
            &mut parse_bufs,
            &mut inflight_sends,
            None,
            &clock,
        );
    }
}
