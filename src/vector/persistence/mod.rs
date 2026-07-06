pub mod manifest;
pub mod mmap_budget;
/// B3: startup recovery — load segments/keymap from the B1/B2 durability
/// layout into the live `ShardSlice.vector_store`, dedup-rescan the rest.
/// Supersedes the old `recovery.rs` WAL-replay module (deleted: it only ever
/// populated the doomed `Shard.vector_store`, see event_loop.rs's
/// `_discarded_vector_store`).
pub mod recover_v2;
pub mod sealed_mmap;
pub mod segment_io;
pub mod wal_record;
pub mod warm_search;
pub mod warm_segment;
