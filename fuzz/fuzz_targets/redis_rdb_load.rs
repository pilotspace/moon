#![no_main]
use libfuzzer_sys::fuzz_target;

/// Fuzz the Redis-compatible RDB loader used by PSYNC FULLRESYNC
/// (`persistence::redis_rdb::load_rdb`), distinct from the SAVE/BGSAVE
/// codec covered by `rdb_load.rs` (`persistence::rdb::load`).
///
/// This is the codec `replication::master` streams to a replica and
/// `replication::apply::load_snapshot` decodes on the replica side; a
/// malicious or corrupted master (or a bit-flipped wire transfer) must
/// never panic the replica. Exercises magic/version validation, type-tag
/// dispatch (including the private `RDB_TYPE_STREAM_MOON` extension added
/// for Wave B stage 2b MQ replication), length-prefixed collection bounds
/// checks (`check_alloc_bound`), and CRC/EOF handling — operates directly
/// on the in-memory byte slice (no temp file needed, unlike `rdb_load.rs`).
fuzz_target!(|data: &[u8]| {
    let mut databases: Vec<moon::storage::db::Database> =
        (0..1).map(|_| moon::storage::db::Database::new()).collect();

    // Should not panic regardless of input.
    let _ = moon::persistence::redis_rdb::load_rdb(&mut databases, data);
});
