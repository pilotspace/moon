//! WAL v3 — per-record LSN, CRC32C, FPI compression, segmented files.

pub mod record;
pub mod replay;
pub mod segment;

pub use record::{WalRecord, WalRecordType, read_wal_v3_record, write_wal_v3_record};
pub use replay::{WalV3ReplayResult, replay_wal_auto, replay_wal_v3_dir, replay_wal_v3_file};
pub use segment::{WalSegment, WalWriterV3};
