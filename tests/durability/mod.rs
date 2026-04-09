//! Durability test infrastructure for Moon.
//!
//! Tests crash recovery, torn writes, and backup/restore workflows.
//! These tests spawn a real Moon server process, write data, kill it
//! with SIGKILL, restart, and verify data integrity via DEBUG DIGEST.

pub mod crash_matrix;
pub mod torn_write;
pub mod backup_restore;
