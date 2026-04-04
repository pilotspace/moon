//! Cold read-through helper for tiered KV storage.
//!
//! Extracted from Database::get() to keep db.rs under 1500 lines.
//! Reads a spilled KV entry from disk via ColdIndex lookup + pread.

use std::path::Path;

use super::cold_index::{ColdIndex, ColdLocation};

/// Attempt to read a cold KV entry from disk.
///
/// Returns `Some((value_bytes, ttl_ms))` on hit, `None` on miss/expired/error.
/// The caller is responsible for promoting the entry back to the DashTable
/// and removing it from the cold index.
pub fn cold_read_through(
    cold_index: &ColdIndex,
    shard_dir: &Path,
    key: &[u8],
    now_ms: u64,
) -> Option<(Vec<u8>, Option<u64>)> {
    let location = cold_index.lookup(key)?;
    read_cold_entry(shard_dir, location, now_ms)
}

/// Read a cold entry from disk given its location.
///
/// Returns the raw value bytes and optional TTL (absolute ms).
/// Returns None if the entry is expired, file is missing, or data is corrupt.
fn read_cold_entry(
    shard_dir: &Path,
    location: ColdLocation,
    now_ms: u64,
) -> Option<(Vec<u8>, Option<u64>)> {
    let file_path = shard_dir
        .join("data")
        .join(format!("heap-{:06}.mpf", location.file_id));
    let pages = crate::persistence::kv_page::read_datafile(&file_path).ok()?;
    // Currently single-page files; page index = 0
    let page = pages.first()?;
    let entry = page.get(location.slot_idx)?;
    // Check TTL expiry
    if let Some(ttl_ms) = entry.ttl_ms {
        if now_ms > ttl_ms {
            return None; // Expired
        }
    }
    Some((entry.value, entry.ttl_ms))
}
