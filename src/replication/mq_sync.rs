//! Wave B stage 2b: FULLRESYNC snapshot for a shard's MQ durable-queue +
//! trigger registries — the `MOON_AUX_MQ_REGISTRY` aux blob, one per shard
//! (mirrors [`crate::replication::graph_sync`]'s per-shard
//! `MOON_AUX_GRAPH_STORE` blob).
//!
//! # What this blob does NOT carry
//!
//! Stream CONTENT (entries, `last_id`, consumer-group PEL, per-consumer
//! pending sets) rides the ordinary keyspace RDB body — every durable
//! queue's underlying `Stream` is an regular key, loaded by
//! `redis_rdb::load_rdb` before this blob is installed (see
//! `replication::apply::load_snapshot`). That codec (`RDB_TYPE_STREAM_MOON`)
//! ALSO carries the `durable` flag and `max_delivery_count` on the stream
//! value itself, so a freshly-loaded Stream is already fully functional for
//! MQ.PUSH/POP/ACK without this blob.
//!
//! What lives OUTSIDE the keyspace — and is therefore missing without this
//! blob — is the shard-level bookkeeping on `ShardSlice`:
//! `durable_queue_registry` (used by MQ.CREATE idempotency / introspection)
//! and `trigger_registry` (MQ.TRIGGER debounce state, never fired on a
//! replica — see [`crate::replication::apply::apply_mq`]'s docs). Losing
//! `trigger_registry` specifically means a replica-turned-master after
//! failover would need every MQ.TRIGGER re-registered; this blob prevents
//! that data loss.
//!
//! Blob format (version 1, little-endian):
//! ```text
//! [u8  version = 1]
//! [u32 queue_count]
//! per queue:
//!   [u32 key_len][key bytes]
//!   [u32 max_delivery_count]
//! [u32 trigger_count]
//! per trigger:
//!   [u32 trig_key_len][trig_key bytes]
//!   [u32 queue_key_len][queue_key bytes]
//!   [u32 callback_len][callback bytes]
//!   [u64 debounce_ms]
//! ```
//! `dlq_key` / `consumer_group` (on `DurableStreamConfig`) and
//! `last_fire_ms` / `pending_fire_ms` (on `TriggerEntry`) are NOT
//! serialized — both are deterministically derived / reset-on-install (see
//! `DurableStreamConfig::new` and `apply_mq_trigger`'s doc comment), exactly
//! like boot-time WAL replay already does.

use bytes::Bytes;

use crate::mq::{DurableQueueRegistry, DurableStreamConfig, TriggerEntry, TriggerRegistry};
use crate::shard::slice::ShardSlice;

const FORMAT_VERSION: u8 = 1;

/// Export one shard's MQ registries as a snapshot blob. Always returns a
/// blob — an empty shard (no durable queues, no triggers) encodes as
/// `queue_count = 0, trigger_count = 0`, letting the replica distinguish
/// "master shard has no MQ state" (authoritative: drop local registries)
/// from "pre-MQ-replication master" (aux absent entirely, handled by the
/// caller checking `blobs.is_empty()` exactly like graph does).
pub fn export_mq_registry(
    durable_queue_registry: Option<&DurableQueueRegistry>,
    trigger_registry: Option<&TriggerRegistry>,
) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    buf.push(FORMAT_VERSION);

    let queue_count = durable_queue_registry.map_or(0, DurableQueueRegistry::len);
    buf.extend_from_slice(&(queue_count as u32).to_le_bytes());
    if let Some(reg) = durable_queue_registry {
        for (key, config) in reg.iter() {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&config.max_delivery_count.to_le_bytes());
        }
    }

    let trigger_count = trigger_registry.map_or(0, TriggerRegistry::len);
    buf.extend_from_slice(&(trigger_count as u32).to_le_bytes());
    if let Some(reg) = trigger_registry {
        for (trig_key, entry) in reg.iter() {
            buf.extend_from_slice(&(trig_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(trig_key);
            buf.extend_from_slice(&(entry.queue_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&entry.queue_key);
            buf.extend_from_slice(&(entry.callback_cmd.len() as u32).to_le_bytes());
            buf.extend_from_slice(&entry.callback_cmd);
            buf.extend_from_slice(&entry.debounce_ms.to_le_bytes());
        }
    }
    buf
}

/// Install a MULTI-SHARD snapshot's MQ registry blobs — one per master
/// shard, `read_moon_aux_all` order — into this replica shard's registries.
/// Authoritative replace happens ONCE (drop all local queue/trigger state),
/// then every blob installs additively. Queue keys and trigger keys are
/// disjoint across blobs (each queue/trigger lives on exactly one master
/// shard, hashed by key — same ownership contract graph's per-graph-name
/// disjointness relies on); a duplicate key across blobs simply gets
/// overwritten (last-wins), matching `DurableQueueRegistry::insert` /
/// `TriggerRegistry::register`'s own semantics for a single blob.
///
/// Returns `Some((queues_installed, triggers_installed))`, or `None` on a
/// malformed blob (registries are left in whatever partial state was
/// reached — the caller aborts the sync and the replica retries with a
/// fresh full resync, same contract as `graph_sync::install_graph_store_many`).
pub fn install_mq_registry_many(s: &mut ShardSlice, blobs: &[Vec<u8>]) -> Option<(usize, usize)> {
    s.durable_queue_registry = None;
    s.trigger_registry = None;
    let mut total_queues = 0usize;
    let mut total_triggers = 0usize;
    for blob in blobs {
        let (q, t) = install_one(s, blob)?;
        total_queues += q;
        total_triggers += t;
    }
    Some((total_queues, total_triggers))
}

fn install_one(s: &mut ShardSlice, blob: &[u8]) -> Option<(usize, usize)> {
    let mut cur = Cursor { data: blob, pos: 0 };
    if cur.u8()? != FORMAT_VERSION {
        return None;
    }

    let queue_count = cur.u32()? as usize;
    for _ in 0..queue_count {
        let key_len = cur.len_checked()? as usize;
        let key = Bytes::copy_from_slice(cur.take(key_len)?);
        let max_delivery_count = cur.u32()?;
        let reg = s
            .durable_queue_registry
            .get_or_insert_with(|| Box::new(DurableQueueRegistry::new()));
        reg.insert(
            key.clone(),
            DurableStreamConfig::new(key, max_delivery_count),
        );
    }

    let trigger_count = cur.u32()? as usize;
    for _ in 0..trigger_count {
        let trig_key_len = cur.len_checked()? as usize;
        let trig_key = Bytes::copy_from_slice(cur.take(trig_key_len)?);
        let queue_key_len = cur.len_checked()? as usize;
        let queue_key = Bytes::copy_from_slice(cur.take(queue_key_len)?);
        let callback_len = cur.len_checked()? as usize;
        let callback_cmd = Bytes::copy_from_slice(cur.take(callback_len)?);
        let debounce_ms = cur.u64()?;
        let reg = s
            .trigger_registry
            .get_or_insert_with(|| Box::new(TriggerRegistry::new()));
        reg.register(
            trig_key,
            TriggerEntry {
                queue_key,
                callback_cmd,
                debounce_ms,
                last_fire_ms: 0,
                pending_fire_ms: 0,
            },
        );
    }

    Some((queue_count, trigger_count))
}

/// Minimal bounds-checked reader over the blob (mirrors
/// `graph_sync::Cursor` — kept as a private per-module copy rather than
/// shared, matching that module's own precedent).
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        if end > self.data.len() {
            return None;
        }
        let s = &self.data[self.pos..end];
        self.pos = end;
        Some(s)
    }
    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }
    fn u32(&mut self) -> Option<u32> {
        #[allow(clippy::unwrap_used)] // take(4) guarantees the length
        Some(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        #[allow(clippy::unwrap_used)] // take(8) guarantees the length
        Some(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    /// Read a `u32` length prefix, bounds-checked against the remaining
    /// blob so a corrupt/adversarial length can't drive an oversized
    /// `Bytes::copy_from_slice` allocation attempt before `take()` would
    /// have rejected it anyway — same DoS-avoidance discipline as
    /// `redis_rdb::check_alloc_bound`.
    fn len_checked(&mut self) -> Option<u32> {
        let len = self.u32()?;
        if len as usize > self.data.len().saturating_sub(self.pos) {
            return None;
        }
        Some(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::shared_databases::ShardStoreMemory;
    use crate::shard::slice::{ShardSlice, ShardSliceInit, init_shard};
    use crate::storage::Database;
    use crate::text::store::TextStore;
    use crate::transaction::{DeferredHnswInserts, KvWriteIntents};
    use crate::vector::store::VectorStore;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    fn make_test_slice() -> ShardSlice {
        let databases: Box<[Database]> = (0..1).map(|_| Database::new()).collect();
        ShardSlice::new(ShardSliceInit {
            shard_id: 0,
            databases,
            vector_store: VectorStore::new(),
            text_store: TextStore::new(),
            #[cfg(feature = "graph")]
            graph_store: crate::graph::store::GraphStore::new(),
            kv_write_intents: KvWriteIntents::new(),
            deferred_hnsw_inserts: DeferredHnswInserts::new(),
            temporal_registry: None,
            temporal_kv_index: None,
            durable_queue_registry: None,
            trigger_registry: None,
            wal_append_tx: None,
            estimated_memory: Arc::new(AtomicUsize::new(0)),
            store_memory: Arc::new(ShardStoreMemory {
                vector: AtomicUsize::new(0),
                text: AtomicUsize::new(0),
                graph: AtomicUsize::new(0),
                lua: AtomicUsize::new(0),
                pagecache: AtomicUsize::new(0),
            }),
        })
    }

    #[test]
    fn export_empty_registries_round_trips_to_zero_counts() {
        let blob = export_mq_registry(None, None);
        assert_eq!(blob[0], FORMAT_VERSION);
        std::thread::spawn(|| {
            init_shard(make_test_slice());
            crate::shard::slice::with_shard(|s| {
                let installed = install_mq_registry_many(s, &[blob]).expect("valid blob");
                assert_eq!(installed, (0, 0));
                assert!(
                    s.durable_queue_registry.is_none() || {
                        s.durable_queue_registry.as_ref().unwrap().is_empty()
                    }
                );
            });
        })
        .join()
        .unwrap();
    }

    #[test]
    fn export_install_round_trip_preserves_queues_and_triggers() {
        let mut queues = DurableQueueRegistry::new();
        queues.insert(
            Bytes::from_static(b"orders"),
            DurableStreamConfig::new(Bytes::from_static(b"orders"), 5),
        );
        queues.insert(
            Bytes::from_static(b"tasks"),
            DurableStreamConfig::new(Bytes::from_static(b"tasks"), 3),
        );
        let mut triggers = TriggerRegistry::new();
        triggers.register(
            Bytes::from_static(b"wshex:orders"),
            TriggerEntry {
                queue_key: Bytes::from_static(b"orders"),
                callback_cmd: Bytes::from_static(b"PUBLISH ch msg"),
                debounce_ms: 1500,
                last_fire_ms: 999, // transient — must NOT survive round-trip
                pending_fire_ms: 888,
            },
        );

        let blob = export_mq_registry(Some(&queues), Some(&triggers));

        std::thread::spawn(move || {
            init_shard(make_test_slice());
            crate::shard::slice::with_shard(|s| {
                // Pre-existing local state must be dropped (authoritative replace).
                s.durable_queue_registry = Some(Box::new({
                    let mut stale = DurableQueueRegistry::new();
                    stale.insert(
                        Bytes::from_static(b"stale"),
                        DurableStreamConfig::new(Bytes::from_static(b"stale"), 1),
                    );
                    stale
                }));

                let (q, t) = install_mq_registry_many(s, &[blob]).expect("valid blob");
                assert_eq!(q, 2);
                assert_eq!(t, 1);

                let qreg = s.durable_queue_registry.as_ref().unwrap();
                assert!(qreg.get(b"stale").is_none(), "stale entry must be dropped");
                assert_eq!(qreg.get(b"orders").unwrap().max_delivery_count, 5);
                assert_eq!(qreg.get(b"tasks").unwrap().max_delivery_count, 3);

                let treg = s.trigger_registry.as_ref().unwrap();
                let entry = treg.get(b"wshex:orders").unwrap();
                assert_eq!(entry.queue_key.as_ref(), b"orders");
                assert_eq!(entry.callback_cmd.as_ref(), b"PUBLISH ch msg");
                assert_eq!(entry.debounce_ms, 1500);
                assert_eq!(entry.last_fire_ms, 0, "transient fire state must reset");
                assert_eq!(entry.pending_fire_ms, 0, "transient fire state must reset");
            });
        })
        .join()
        .unwrap();
    }

    #[test]
    fn install_many_merges_disjoint_shard_blobs() {
        let mut q0 = DurableQueueRegistry::new();
        q0.insert(
            Bytes::from_static(b"q0"),
            DurableStreamConfig::new(Bytes::from_static(b"q0"), 1),
        );
        let mut q1 = DurableQueueRegistry::new();
        q1.insert(
            Bytes::from_static(b"q1"),
            DurableStreamConfig::new(Bytes::from_static(b"q1"), 2),
        );
        let blob0 = export_mq_registry(Some(&q0), None);
        let blob1 = export_mq_registry(Some(&q1), None);

        std::thread::spawn(move || {
            init_shard(make_test_slice());
            crate::shard::slice::with_shard(|s| {
                let (q, _) = install_mq_registry_many(s, &[blob0, blob1]).expect("valid blobs");
                assert_eq!(q, 2);
                let reg = s.durable_queue_registry.as_ref().unwrap();
                assert!(reg.get(b"q0").is_some());
                assert!(reg.get(b"q1").is_some());
            });
        })
        .join()
        .unwrap();
    }

    #[test]
    fn install_rejects_unknown_version() {
        let mut blob = export_mq_registry(None, None);
        blob[0] = 99;
        std::thread::spawn(move || {
            init_shard(make_test_slice());
            crate::shard::slice::with_shard(|s| {
                assert_eq!(install_mq_registry_many(s, &[blob]), None);
            });
        })
        .join()
        .unwrap();
    }

    #[test]
    fn install_rejects_truncated_blob_not_panicking() {
        let mut queues = DurableQueueRegistry::new();
        queues.insert(
            Bytes::from_static(b"orders"),
            DurableStreamConfig::new(Bytes::from_static(b"orders"), 5),
        );
        let full = export_mq_registry(Some(&queues), None);
        for cut in 0..full.len() {
            let truncated = full[..cut].to_vec();
            std::thread::spawn(move || {
                init_shard(make_test_slice());
                crate::shard::slice::with_shard(|s| {
                    // Must not panic; either Some (if the cut happens to
                    // still be a valid smaller blob) or None.
                    let _ = install_mq_registry_many(s, &[truncated]);
                });
            })
            .join()
            .unwrap();
        }
    }
}
