//! WAL v3 segment file management — placeholder for Task 2.

use std::path::PathBuf;

/// Represents a single WAL v3 segment file.
pub struct WalSegment {
    /// Path to the segment file.
    pub path: PathBuf,
    /// Monotonic segment sequence number.
    pub sequence: u64,
}

/// WAL v3 writer with segmented files — placeholder.
pub struct WalWriterV3;
