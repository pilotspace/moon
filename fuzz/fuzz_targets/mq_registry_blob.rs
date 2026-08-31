#![no_main]
use libfuzzer_sys::fuzz_target;
use moon::replication::mq_sync::install_mq_registry_many;
use moon::shard::slice::test_support::make_init;
use moon::shard::slice::{ShardSlice, init_shard, with_shard};

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
    // `test_support::make_init` rather than a hand-written `ShardSliceInit`
    // literal. The literal that used to live here rotted twice without any
    // gate noticing -- `ShardStoreMemory` gained two fields, then `databases`
    // became `Arc<ShardDbSet>` -- and this target failed to BUILD on every
    // nightly since at least 2026-07-17, running zero executions the whole
    // time. Sharing the fixture with moon's own tests means a new field
    // breaks the build in-tree, where a gate can see it.
    //
    // make_init deliberately builds a STANDALONE db set: the L4 registry is a
    // process-wide OnceLock, and a fuzz process runs many iterations.
    ShardSlice::new(make_init(0, 1))
}
