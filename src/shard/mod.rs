pub mod affinity;
/// P4: per-shard autovacuum daemon with cost-based throttle.
pub mod autovacuum;
pub mod conn_accept;
pub mod coordinator;
pub mod disk_monitor;
pub mod dispatch;
pub mod event_loop;
/// MA5: maintenance-window scheduler (cron-style budget multipliers).
pub mod maintenance_schedule;
/// Wave 3: proactive RSS memory watchdog ("mem-full guard") — analogue of
/// `disk_monitor` (MA12) for process RSS vs the detected system/cgroup limit.
pub mod mem_monitor;
pub mod mesh;
/// C2 (shardslice-migration Wave A1): owner-side MQ.* execution on the shard thread.
pub(crate) mod mq_exec;
pub mod numa;
pub mod persistence_tick;
pub mod remote_subscriber_map;
#[cfg(feature = "text-index")]
pub mod scatter_aggregate;
#[cfg(feature = "text-index")]
pub mod scatter_hybrid;
/// MA1: write-stall on immutable segment backlog.
pub mod segment_stall;
pub mod shared_databases;
pub mod slice;
pub mod spsc_handler;
/// Shared MOVE/COPY-DB two-database intercept for every `ShardMessage` SPSC
/// arm (Gap A). Split out of `spsc_handler.rs` per the repo's file-size
/// convention rather than growing that file further.
pub(crate) mod spsc_two_db;
pub mod timers;
pub mod uring_handler;

pub use disk_monitor::DiskMonitor;
pub use slice::{
    ShardSlice, ShardSliceInit, init_shard, is_initialized, try_with_shard, with_shard,
    with_shard_db,
};

#[cfg(feature = "text-index")]
pub use scatter_aggregate::scatter_text_aggregate;
#[cfg(feature = "text-index")]
pub use scatter_hybrid::scatter_hybrid_search;

use tracing::info;

use crate::config::RuntimeConfig;
use crate::persistence::replay::DispatchReplayEngine;
use crate::pubsub::PubSubRegistry;
use crate::storage::Database;
use crate::vector::store::VectorStore;

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
}

impl Shard {
    /// Create a new shard with `num_databases` empty databases.
    pub fn new(id: usize, num_shards: usize, num_databases: usize, config: RuntimeConfig) -> Self {
        Self::with_initial_keyspace_hint(id, num_shards, num_databases, 0, config)
    }

    /// Create a new shard with `num_databases` empty databases, pre-sizing DB 0
    /// (the default SELECTed database) to hold approximately
    /// `initial_keyspace_hint` entries without segment splits.
    ///
    /// Only DB 0 is pre-sized because it is the only database most workloads
    /// touch; pre-sizing all 16 per shard would multiply the startup RSS by
    /// 16× for zero benefit on the default deployment path.
    ///
    /// `initial_keyspace_hint == 0` is equivalent to `Shard::new` (no pre-sizing).
    pub fn with_initial_keyspace_hint(
        id: usize,
        num_shards: usize,
        num_databases: usize,
        initial_keyspace_hint: usize,
        config: RuntimeConfig,
    ) -> Self {
        // Split the hint across shards: each shard holds ~1/num_shards of keys.
        let per_shard_hint = if initial_keyspace_hint == 0 || num_shards == 0 {
            0
        } else {
            initial_keyspace_hint / num_shards.max(1)
        };
        let databases: Vec<Database> = (0..num_databases)
            .map(|i| {
                if i == 0 {
                    Database::with_capacity(per_shard_hint)
                } else {
                    Database::new()
                }
            })
            .collect();
        Shard {
            id,
            databases,
            num_shards,
            runtime_config: config,
            pubsub_registry: PubSubRegistry::new(),
            vector_store: VectorStore::new(),
        }
    }

    /// Restore shard state from per-shard snapshot and WAL files at startup.
    ///
    /// When `disk_offload_dir` is `Some`, uses the v3 recovery protocol
    /// (6-phase: control file -> manifest -> data load -> WAL replay ->
    /// consistency -> ready). Falls back to the legacy path on v3 failure.
    ///
    /// When `disk_offload_dir` is `None`, uses the legacy path: load the
    /// per-shard RRDSHARD snapshot, then replay WAL v3 (written even without
    /// disk-offload, see `event_loop::run`) preferentially, falling back to
    /// appendonly.aof only when WAL v3 has nothing (the WAL v2 rung was
    /// removed in the pre-1.0 WAL-v3-only format freeze; this restores the
    /// "prefer WAL, fall back to AOF" contract WAL v2 had, using WAL v3 as
    /// its replacement — see `restore_from_persistence_v2`).
    ///
    /// Returns total keys loaded (snapshot + WAL v3/AOF replay).
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
                    &DispatchReplayEngine::new(),
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

                        // Vector recovery: the `Shard`-owned `vector_store`
                        // populated here is discarded wholesale at
                        // `event_loop.rs` (`_discarded_vector_store`) in
                        // favor of `ShardSlice.vector_store` — see that
                        // file's comment for the real recovery contract
                        // (sidecar definitions + manifest/segments/keymap +
                        // dedup rescan, B3). Nothing to do on this struct.
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

    /// Legacy recovery path: snapshot load + WAL v3 (preferred) / appendonly.aof
    /// (fallback) replay.
    ///
    /// Pre-1.0 WAL-v3-only format freeze: the per-shard WAL v2 rung
    /// (`shard-N.wal`) was removed and is no longer replayed by this build —
    /// see the loud `tracing::error!` below if one is still on disk. WAL v3
    /// is now ALSO written in this (non-disk-offload) mode, rooted at
    /// `shard-N/wal-v3/` (see `event_loop::run`'s writer-creation block),
    /// fsynced on the same 1s cadence the retired WAL v2 writer used
    /// regardless of `--appendfsync`. To preserve the property WAL v2 gave
    /// ("prefer the WAL, fall back to `appendonly.aof` only if the WAL
    /// replayed zero commands" — the AOF's own fsync obeys `--appendfsync`,
    /// so under `--appendfsync no` the WAL can be durably ahead of the AOF),
    /// this replays WAL v3 first and only touches the AOF when WAL v3 has
    /// nothing. See `wal_v3::replay::replay_wal_v3_dir_commands` for the
    /// full rationale.
    fn restore_from_persistence_v2(&mut self, persistence_dir: &str) -> usize {
        use crate::persistence::snapshot::shard_snapshot_load;

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

        // Loud failure (not silent skip) for a leftover pre-freeze WAL v2
        // file. WAL v2 support was removed in this build; the file below is
        // NEVER consulted by any recovery path in this binary, so an
        // operator who upgraded over a v2-only deployment must find out
        // immediately rather than discover a silent recovery gap later.
        let legacy_v2_wal = dir.join(format!("shard-{}.wal", self.id));
        if legacy_v2_wal.exists() {
            tracing::error!(
                "Shard {}: found legacy WAL v2 file {:?} — WAL v2 support was \
                 removed (pre-1.0 WAL-v3-only format freeze) and this build \
                 does NOT replay it. If its contents are not already reflected \
                 in appendonly.aof, replay it on a pre-freeze Moon build first \
                 (which re-persists through the AOF) before removing this file.",
                self.id,
                legacy_v2_wal
            );
        }

        // Prefer WAL v3 over the AOF (mirrors the retired WAL v2 contract).
        let wal_v3_dir = dir.join(format!("shard-{}", self.id)).join("wal-v3");
        let wal_replayed = match crate::persistence::wal_v3::replay::replay_wal_v3_dir_commands(
            &wal_v3_dir,
            &mut self.databases,
            &DispatchReplayEngine::new(),
        ) {
            Ok(n) => n,
            Err(e) => {
                tracing::error!("Shard {}: WAL v3 replay failed: {}", self.id, e);
                0
            }
        };

        if wal_replayed > 0 {
            info!(
                "Shard {}: replayed {} WAL v3 commands (preferred over AOF)",
                self.id, wal_replayed
            );
            total_keys += wal_replayed;
        } else {
            let aof_path = dir.join("appendonly.aof");
            if aof_path.exists() {
                match crate::persistence::aof::replay_aof(
                    &mut self.databases,
                    &aof_path,
                    &DispatchReplayEngine::new(),
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

        // Vector store recovery does NOT happen here — see the comment in
        // `restore_from_persistence`'s v3 branch above. The `Shard`-owned
        // `vector_store` this method populates is discarded wholesale at
        // `event_loop.rs`; real recovery runs later against
        // `ShardSlice.vector_store` (sidecar definitions + B3
        // manifest/segments/keymap load + dedup rescan).

        total_keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::BlockingRegistry;
    use crate::framevec;
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
        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);

        let (tx, rx) = rt_channel::mpsc_bounded::<Bytes>(16);
        let sub = Subscriber::new(tx, 42);
        pubsub.subscribe(Bytes::from_static(b"news"), sub);

        let rb = HeapRb::new(64);
        let (mut prod, cons) = rb.split();
        let slot = std::sync::Arc::new(crate::shard::dispatch::PubSubResponseSlot::new(0));
        prod.try_push(ShardMessage::PubSubPublish(Box::new(
            crate::shard::dispatch::PubSubPublishPayload {
                channel: Bytes::from_static(b"news"),
                message: Bytes::from_static(b"hello from shard 1"),
                slot,
            },
        )))
        .ok()
        .expect("push should succeed");

        let mut pending_snap = None;
        let mut snap_state = None;
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        let backlog = std::sync::Arc::new(parking_lot::Mutex::new(None));
        spsc_handler::drain_spsc_shared(
            &shard_databases,
            &mut [cons],
            &mut pubsub,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut None, // wal_writer
            &backlog,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
            &mut Vec::new(),
            &mut Vec::new(),
            &mut None, // shard_manifest — None in tests (no persistence_dir)
            1000,      // mvcc_prune_margin default
            8,         // graph_merge_max_segments default
            0.20,      // graph_dead_edge_trigger default
            &mut crate::shard::autovacuum::AutovacuumDaemon::new(
                crate::shard::autovacuum::AutovacuumConfig::default(),
            ),
            None, // aof_pool — None in tests
            true, // wal_kv_log — legacy behavior in tests
            &std::sync::Arc::new(parking_lot::RwLock::new(RuntimeConfig::default())), // M2 fix: no shard context needed (maxmemory unset)
            None,
            &Rc::new(std::cell::Cell::new(1u64)),
            None,
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
        let (shard_databases, _inits) = ShardDatabases::new(vec![vec![Database::new()]]);

        let rb = HeapRb::new(512);
        let (mut prod, cons) = rb.split();

        for _ in 0..300 {
            let slot = std::sync::Arc::new(crate::shard::dispatch::PubSubResponseSlot::new(0));
            prod.try_push(ShardMessage::PubSubPublish(Box::new(
                crate::shard::dispatch::PubSubPublishPayload {
                    channel: Bytes::from_static(b"ch"),
                    message: Bytes::from_static(b"msg"),
                    slot,
                },
            )))
            .ok()
            .unwrap();
        }

        let mut pending_snap = None;
        let mut snap_state = None;
        let blocking = Rc::new(RefCell::new(BlockingRegistry::new(0)));
        let script_cache = Rc::new(RefCell::new(crate::scripting::ScriptCache::new()));
        let clock = CachedClock::new();
        let backlog = std::sync::Arc::new(parking_lot::Mutex::new(None));
        spsc_handler::drain_spsc_shared(
            &shard_databases,
            &mut [cons],
            &mut pubsub,
            &blocking,
            &mut pending_snap,
            &mut snap_state,
            &mut None, // wal_writer
            &backlog,
            &mut Vec::new(),
            &None,
            0,
            &script_cache,
            &clock,
            &mut Vec::new(),
            &mut Vec::new(),
            &mut None, // shard_manifest — None in tests (no persistence_dir)
            1000,      // mvcc_prune_margin default
            8,         // graph_merge_max_segments default
            0.20,      // graph_dead_edge_trigger default
            &mut crate::shard::autovacuum::AutovacuumDaemon::new(
                crate::shard::autovacuum::AutovacuumConfig::default(),
            ),
            None, // aof_pool — None in tests
            true, // wal_kv_log — legacy behavior in tests
            &std::sync::Arc::new(parking_lot::RwLock::new(RuntimeConfig::default())), // M2 fix: no shard context needed (maxmemory unset)
            None,
            &Rc::new(std::cell::Cell::new(1u64)),
            None,
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
        let (shard_databases, _inits) = ShardDatabases::new(vec![shard.databases]);
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
        let (shard_databases, _inits) = ShardDatabases::new(vec![shard.databases]);
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

    // ── restore_from_persistence_v2: WAL v3 preferred over AOF (fix for
    // review finding P0#1) ─────────────────────────────────────────────────
    //
    // WAL v3 is written even without --disk-offload (see event_loop::run's
    // writer-creation block), so the legacy (non-disk-offload) recovery path
    // must replay it -- preferring it over `appendonly.aof`, mirroring the
    // retired WAL v2 contract -- instead of only ever checking the AOF. See
    // `src/persistence/recovery.rs`'s equivalent tests for the disk-offload
    // path's analogous fallback fix.

    #[test]
    fn test_restore_from_persistence_v2_prefers_wal_v3_over_aof() {
        use crate::persistence::wal_v3::record::{WalRecordType, write_wal_v3_record};

        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // Populated `shard-0/wal-v3/` (3 records) -- written even without
        // disk-offload.
        let wal_dir = dir.join("shard-0").join("wal-v3");
        std::fs::create_dir_all(&wal_dir).unwrap();
        let mut header = vec![0u8; 64];
        header[0..6].copy_from_slice(b"RRDWAL");
        header[6] = 3; // version = 3
        let mut wal_data = header;
        for i in 1..=3u64 {
            write_wal_v3_record(
                &mut wal_data,
                i,
                WalRecordType::Command,
                b"*1\r\n$4\r\nPING\r\n",
            );
        }
        std::fs::write(wal_dir.join("000000000001.wal"), &wal_data).unwrap();

        // A shorter `appendonly.aof` (1 record) -- must NOT be preferred.
        std::fs::write(dir.join("appendonly.aof"), b"*1\r\n$4\r\nPING\r\n").unwrap();

        let config = RuntimeConfig::default();
        let mut shard = Shard::new(0, 1, 1, config);
        let total = shard.restore_from_persistence_v2(dir.to_str().unwrap());

        assert_eq!(
            total, 3,
            "restore_from_persistence_v2 must prefer the 3-record WAL v3 \
             directory over the 1-record appendonly.aof -- got {} (AOF was \
             used instead of the WAL if this is 1)",
            total
        );
    }

    #[test]
    fn test_restore_from_persistence_v2_falls_back_to_aof_when_wal_v3_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // No `shard-0/wal-v3/` at all -- only the AOF.
        std::fs::write(
            dir.join("appendonly.aof"),
            b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n",
        )
        .unwrap();

        let config = RuntimeConfig::default();
        let mut shard = Shard::new(0, 1, 1, config);
        let total = shard.restore_from_persistence_v2(dir.to_str().unwrap());

        assert_eq!(
            total, 2,
            "with no WAL v3 data at all, the AOF fallback must still work \
             exactly as before this fix"
        );
    }

    #[test]
    fn test_restore_from_persistence_v2_stray_legacy_v2_wal_does_not_break_aof() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();

        // Stray pre-freeze WAL v2 file (`shard-0.wal`, RRDWAL version=2) must
        // be loudly logged and ignored, not crash recovery or block the AOF
        // fallback.
        let mut legacy_v2_wal = vec![0u8; 32];
        legacy_v2_wal[0..6].copy_from_slice(b"RRDWAL");
        legacy_v2_wal[6] = 2;
        std::fs::write(dir.join("shard-0.wal"), &legacy_v2_wal).unwrap();
        std::fs::write(dir.join("appendonly.aof"), b"*1\r\n$4\r\nPING\r\n").unwrap();

        let config = RuntimeConfig::default();
        let mut shard = Shard::new(0, 1, 1, config);
        let total = shard.restore_from_persistence_v2(dir.to_str().unwrap());

        assert_eq!(
            total, 1,
            "a stray legacy v2 WAL file must not prevent the AOF fallback \
             from working"
        );
    }
}
