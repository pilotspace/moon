#![no_main]
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use libfuzzer_sys::fuzz_target;
use moon::replication::mq_sync::install_mq_registry_many;
use moon::shard::shared_databases::ShardStoreMemory;
use moon::shard::slice::{ShardSlice, ShardSliceInit, init_shard, with_shard};
use moon::storage::db::Database;
use moon::text::store::TextStore;
use moon::transaction::{DeferredHnswInserts, KvWriteIntents};
use moon::vector::store::VectorStore;

/// Fuzz the Wave B stage 2b MQ-registry FULLRESYNC blob decoder
/// (`replication::mq_sync::install_mq_registry_many`), mirroring
/// `mq_wal_record.rs`'s coverage of the sibling WAL op-blob decoders.
///
/// `data` arrives on the replica exactly like `apply::load_snapshot` feeds
/// it: bytes read straight off a `MOON_AUX_MQ_REGISTRY` RDB aux field sent
/// by a master over the wire — attacker/corruption-controlled (a malicious
/// or buggy master, a torn transfer, a version skew). The decoder
/// (`mq_sync::Cursor` + `install_one`) is documented to return `None` on
/// ANY malformed input, truncated blob, or unknown version byte — never
/// panic, never read out of bounds, never allocate unboundedly off an
/// attacker length prefix (`len_checked` bounds every prefix against the
/// remaining blob before `Bytes::copy_from_slice`).
///
/// `ShardSlice` state lives in a `thread_local!`, so — mirroring
/// `mq_sync.rs`'s own unit tests — each fuzz iteration installs into a
/// fresh `ShardSlice` on a throwaway OS thread.
fuzz_target!(|data: &[u8]| {
    let data = data.to_vec();
    let _ = std::thread::spawn(move || {
        init_shard(make_fuzz_slice());
        with_shard(|s: &mut ShardSlice| {
            // Should not panic regardless of input.
            let _ = install_mq_registry_many(s, &[data]);
        });
    })
    .join();
});

fn make_fuzz_slice() -> ShardSlice {
    let databases: Box<[Database]> = (0..1).map(|_| Database::new()).collect();
    ShardSlice::new(ShardSliceInit {
        shard_id: 0,
        databases,
        vector_store: VectorStore::new(),
        text_store: TextStore::new(),
        // moon-fuzz always builds `moon` with the "graph" feature (see
        // fuzz/Cargo.toml), so this field is unconditional here even
        // though it's `#[cfg(feature = "graph")]` inside moon itself.
        graph_store: moon::graph::store::GraphStore::new(),
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
        }),
    })
}
