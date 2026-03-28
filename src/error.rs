//! Structured error types for the Moon server.
//!
//! Provides a top-level [`MoonError`] enum with domain-specific sub-errors
//! for each persistence subsystem (WAL, AOF, RDB, snapshots). These types
//! will replace `anyhow::Result` in persistence code paths, enabling
//! pattern-matched recovery and richer diagnostics.

use std::path::PathBuf;
use thiserror::Error;

/// Top-level error type for the Moon server.
/// Domain-specific sub-errors provide detailed context for each subsystem.
#[derive(Debug, Error)]
pub enum MoonError {
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("AOF error: {0}")]
    Aof(#[from] AofError),

    #[error("RDB error: {0}")]
    Rdb(#[from] RdbError),

    #[error("snapshot error: {0}")]
    Snapshot(#[from] SnapshotError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("{0}")]
    Other(String),
}

/// Errors originating from the Write-Ahead Log subsystem.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("WAL I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("WAL corrupted at offset {offset}: {detail}")]
    Corrupted { offset: u64, detail: String },

    #[error(
        "WAL entry checksum mismatch at offset {offset}: expected {expected:#010x}, got {actual:#010x}"
    )]
    ChecksumMismatch {
        offset: u64,
        expected: u32,
        actual: u32,
    },

    #[error("WAL version unsupported: {version}")]
    UnsupportedVersion { version: u32 },
}

/// Errors originating from the Append-Only File subsystem.
#[derive(Debug, Error)]
pub enum AofError {
    #[error("AOF I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("AOF corrupted at byte {offset}: {detail}")]
    Corrupted { offset: u64, detail: String },

    #[error("AOF rewrite failed: {detail}")]
    RewriteFailed { detail: String },
}

/// Errors originating from the RDB persistence subsystem.
#[derive(Debug, Error)]
pub enum RdbError {
    #[error("RDB I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("RDB corrupted: {detail}")]
    Corrupted { detail: String },

    #[error("RDB unsupported type tag: {type_tag}")]
    UnsupportedType { type_tag: u8 },

    #[error("RDB version unsupported: {version}")]
    UnsupportedVersion { version: u32 },

    #[error("RDB checksum mismatch")]
    ChecksumMismatch,
}

/// Errors originating from the snapshot subsystem.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("snapshot I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("snapshot corrupted: {detail}")]
    Corrupted { detail: String },

    #[error("snapshot version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u32, actual: u32 },
}

/// Convenience type alias for Results using MoonError.
pub type MoonResult<T> = std::result::Result<T, MoonError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn moon_error_from_wal_error() {
        let wal_err = WalError::Corrupted {
            offset: 42,
            detail: "bad magic".to_string(),
        };
        let moon_err: MoonError = wal_err.into();
        assert!(matches!(
            moon_err,
            MoonError::Wal(WalError::Corrupted { offset: 42, .. })
        ));
        assert!(moon_err.to_string().contains("bad magic"));
    }

    #[test]
    fn moon_error_from_aof_error() {
        let aof_err = AofError::Io {
            path: PathBuf::from("/tmp/test.aof"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        };
        let moon_err: MoonError = aof_err.into();
        assert!(matches!(moon_err, MoonError::Aof(AofError::Io { .. })));
    }

    #[test]
    fn moon_error_from_rdb_error() {
        let rdb_err = RdbError::UnsupportedType { type_tag: 0xFF };
        let moon_err: MoonError = rdb_err.into();
        assert!(matches!(
            moon_err,
            MoonError::Rdb(RdbError::UnsupportedType { type_tag: 0xFF })
        ));
    }

    #[test]
    fn moon_error_from_snapshot_error() {
        let snap_err = SnapshotError::VersionMismatch {
            expected: 2,
            actual: 1,
        };
        let moon_err: MoonError = snap_err.into();
        assert!(matches!(
            moon_err,
            MoonError::Snapshot(SnapshotError::VersionMismatch {
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn moon_error_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let moon_err: MoonError = io_err.into();
        assert!(matches!(moon_err, MoonError::Io(_)));
    }

    #[test]
    fn moon_result_alias_works() {
        fn example() -> MoonResult<u32> {
            Ok(42)
        }
        assert_eq!(example().unwrap(), 42);
    }
}
