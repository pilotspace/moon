//! Cold read-through helper for tiered KV storage.
//!
//! Extracted from Database::get() to keep db.rs under 1500 lines.
//! Reads a spilled KV entry from disk via ColdIndex lookup + pread.

use std::path::Path;

use bytes::Bytes;

use super::cold_index::{ColdIndex, ColdLocation};
use super::kv_serde;
use crate::persistence::kv_page::{ValueType, entry_flags, read_overflow_chain};
use crate::persistence::page::PAGE_4K;
use crate::storage::entry::RedisValue;

/// Attempt to read a cold KV entry from disk.
///
/// Returns `Some((RedisValue, ttl_ms))` on hit, `None` on miss/expired/error.
/// The caller is responsible for promoting the entry back to the DashTable
/// and removing it from the cold index.
pub fn cold_read_through(
    cold_index: &ColdIndex,
    shard_dir: &Path,
    key: &[u8],
    now_ms: u64,
) -> Option<(RedisValue, Option<u64>)> {
    let location = cold_index.lookup(key)?;
    read_cold_entry(shard_dir, location, now_ms)
}

/// Read a cold entry from disk given its location.
///
/// Returns the deserialized RedisValue and optional TTL (absolute ms).
/// Returns None if the entry is expired, file is missing, or data is corrupt.
fn read_cold_entry(
    shard_dir: &Path,
    location: ColdLocation,
    now_ms: u64,
) -> Option<(RedisValue, Option<u64>)> {
    let file_path = shard_dir
        .join("data")
        .join(format!("heap-{:06}.mpf", location.file_id));

    // Read the full file (needed for potential overflow chain reads)
    let file_data = std::fs::read(&file_path).ok()?;
    if file_data.len() < PAGE_4K {
        return None;
    }

    // Parse the KvLeaf page (page 0)
    let mut leaf_buf = [0u8; PAGE_4K];
    leaf_buf.copy_from_slice(&file_data[..PAGE_4K]);
    let page = crate::persistence::kv_page::KvLeafPage::from_bytes(leaf_buf)?;
    let entry = page.get(location.slot_idx)?;

    // Check TTL expiry
    if let Some(ttl_ms) = entry.ttl_ms {
        if now_ms > ttl_ms {
            return None; // Expired
        }
    }

    // Resolve value bytes: handle overflow chain if flagged
    let value_bytes = if entry.flags & entry_flags::OVERFLOW != 0 {
        // Overflow pointer: start_page_idx as u32 LE
        if entry.value.len() < 4 {
            return None;
        }
        let start_page_idx = u32::from_le_bytes(entry.value[..4].try_into().ok()?) as usize;
        read_overflow_chain(&file_data, start_page_idx)?
    } else {
        entry.value
    };

    // Convert to RedisValue based on value_type
    let redis_value = match entry.value_type {
        ValueType::String => RedisValue::String(Bytes::from(value_bytes)),
        _ => kv_serde::deserialize_collection(&value_bytes, entry.value_type)?,
    };

    Some((redis_value, entry.ttl_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::compact_value::CompactValue;
    use crate::storage::entry::Entry;
    use crate::storage::tiered::cold_index::ColdIndex;
    use crate::storage::tiered::kv_spill::spill_to_datafile;
    use bytes::Bytes;
    use std::collections::HashMap;

    #[test]
    fn test_cold_read_hash_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();

        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"color"), Bytes::from_static(b"red"));
        map.insert(Bytes::from_static(b"size"), Bytes::from_static(b"large"));

        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(RedisValue::Hash(map));

        spill_to_datafile(
            shard_dir,
            20,
            b"myhash",
            &entry,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        // Read back via cold_read_through
        let result = cold_read_through(&cold_index, shard_dir, b"myhash", 0);
        assert!(result.is_some(), "should find cold hash entry");

        let (value, ttl) = result.unwrap();
        assert!(ttl.is_none());
        match value {
            RedisValue::Hash(result_map) => {
                assert_eq!(result_map.len(), 2);
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"color")).unwrap(),
                    &Bytes::from_static(b"red")
                );
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"size")).unwrap(),
                    &Bytes::from_static(b"large")
                );
            }
            _ => panic!("expected Hash, got {:?}", value.type_name()),
        }
    }

    #[test]
    fn test_cold_read_overflow_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();

        // Create a large incompressible string that exceeds a single 4KB page
        let mut big_value = vec![0u8; 6000];
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for b in big_value.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = state as u8;
        }
        let entry = Entry::new_string(Bytes::from(big_value.clone()));

        spill_to_datafile(
            shard_dir,
            30,
            b"big_key",
            &entry,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        // Verify the file has multiple pages
        let file_path = shard_dir.join("data/heap-000030.mpf");
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        assert!(
            file_size > PAGE_4K as u64,
            "should have overflow pages: file size = {file_size}"
        );

        // Read back via cold_read_through
        let result = cold_read_through(&cold_index, shard_dir, b"big_key", 0);
        assert!(result.is_some(), "should find cold overflow entry");

        let (value, ttl) = result.unwrap();
        assert!(ttl.is_none());
        match value {
            RedisValue::String(data) => {
                assert_eq!(
                    data.as_ref(),
                    big_value.as_slice(),
                    "overflow data must match original"
                );
            }
            _ => panic!("expected String, got {:?}", value.type_name()),
        }
    }
}
