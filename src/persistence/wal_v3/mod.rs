//! WAL v3 — per-record LSN, CRC32C, FPI compression, segmented files.

pub mod record;
pub mod replay;
pub mod segment;
pub mod tail;

pub use record::{WalRecord, WalRecordType, read_wal_v3_record, write_wal_v3_record};
pub use replay::{
    WalV3ReplayResult, replay_wal_auto, replay_wal_v3_dir, replay_wal_v3_dir_until,
    replay_wal_v3_file, replay_wal_v3_file_until, resolve_target_time_to_lsn,
};
pub use segment::{WalSegment, WalWriterV3};
pub use tail::{TailCursor, WalTailReader};
