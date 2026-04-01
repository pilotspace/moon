//! WAL v3 — per-record LSN, CRC32C, FPI compression, segmented files.

pub mod record;
pub mod segment;

pub use record::{WalRecord, WalRecordType, read_wal_v3_record, write_wal_v3_record};
pub use segment::{WalSegment, WalWriterV3};
