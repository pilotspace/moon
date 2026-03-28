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

    /// Restore shard state from per-shard snapshot and WAL files at startup.
    ///
    /// Loads the per-shard RRDSHARD snapshot file first (if it exists), then replays
    /// the per-shard WAL for any commands written after the last snapshot.
    /// Returns total keys loaded (snapshot + WAL replay).
    pub fn restore_from_persistence(&mut self, persistence_dir: &str) -> usize {
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

        // Replay per-shard WAL
        let wal_file = wal::wal_path(dir, self.id);
        if wal_file.exists() {
            match wal::replay_wal(&mut self.databases, &wal_file, &DispatchReplayEngine) {
                Ok(n) => {
                    info!("Shard {}: replayed {} WAL commands", self.id, n);
                    total_keys += n;
                }
                Err(e) => {
                    tracing::error!("Shard {}: WAL replay failed: {}", self.id, e);
                }
            }
        }

        total_keys
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

        let (tx, rx) = rt_channel::mpsc_bounded::<Frame>(16);
        let sub = Subscriber::new(tx, 42);
        pubsub.subscribe(Bytes::from_static(b"news"), sub);

        let rb = HeapRb::new(64);
        let (mut prod, cons) = rb.split();
        prod.try_push(ShardMessage::PubSubFanOut {
            channel: Bytes::from_static(b"news"),
            message: Bytes::from_static(b"hello from shard 1"),
        })
        .ok()
        .expect("push should succeed");

        let mut pending_snap = None;
        let mut snap_state = None;
        let mut wal_w: Option<WalWriter> = None;
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        let mut remote_sub_map = self::remote_subscriber_map::RemoteSubscriberMap::new();
        spsc_handler::drain_spsc_shared(
            &shard_databases,
            &mut [cons],
            &mut pubsub,
            &mut remote_sub_map,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut wal_w,
            &mut None,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
            &mut Vec::new(),
        );

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
        let shard_databases = ShardDatabases::new(vec![vec![Database::new()]]);

        let rb = HeapRb::new(512);
        let (mut prod, cons) = rb.split();

        for _ in 0..300 {
            prod.try_push(ShardMessage::PubSubFanOut {
                channel: Bytes::from_static(b"ch"),
                message: Bytes::from_static(b"msg"),
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
        let mut remote_sub_map = self::remote_subscriber_map::RemoteSubscriberMap::new();
        spsc_handler::drain_spsc_shared(
            &shard_databases,
            &mut [cons],
            &mut pubsub,
            &mut remote_sub_map,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut wal_w,
            &mut None,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
            &mut Vec::new(),
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
