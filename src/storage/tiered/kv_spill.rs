//! KV spill-to-disk: serialize evicted entries to KvLeafPage DataFiles.
//!
//! When `disk_offload_enabled`, eviction writes entries to `.mpf` files
//! instead of permanently deleting them.

use std::io;
use std::path::Path;

use bytes::Bytes;
use tracing::warn;

use crate::persistence::kv_page::{
    KvLeafPage, PageFull, ValueType, entry_flags, write_datafile,
    build_overflow_chain, write_datafile_mixed,
};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::{PageType, PAGE_4K};
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;
use super::kv_serde;

/// Spill a single evicted KV entry to a DataFile on disk.
///
/// Creates a single-page `.mpf` file at `{shard_dir}/data/heap-{file_id:06}.mpf`,
/// writes a `KvLeafPage` containing the entry, and registers the file in the
/// shard manifest.
///
/// String entries are fully supported. Non-string types (hash, list, set, zset,
/// stream) are skipped with a warning -- overflow serialization is future work.
///
/// If the entry does not fit in a single 4KB page, it is skipped (oversized
/// entries require overflow pages, also future work).
///
/// Returns `Ok(())` on success, skip, or best-effort failure logging.
pub fn spill_to_datafile(
    shard_dir: &Path,
    file_id: u64,
    key: &[u8],
    entry: &Entry,
    manifest: &mut ShardManifest,
    cold_index: Option<&mut super::cold_index::ColdIndex>,
) -> io::Result<()> {
    // Determine value type and extract bytes.
    // For collections, serialize via kv_serde; for strings, borrow directly.
    let collection_buf: Vec<u8>;
    let val_ref = entry.as_redis_value();
    let (value_type, value_bytes): (ValueType, &[u8]) = match val_ref {
        RedisValueRef::String(s) => (ValueType::String, s),
        ref other => {
            let vt = match other {
                RedisValueRef::Hash(_) | RedisValueRef::HashListpack(_) => ValueType::Hash,
                RedisValueRef::List(_) | RedisValueRef::ListListpack(_) => ValueType::List,
                RedisValueRef::Set(_) | RedisValueRef::SetListpack(_) | RedisValueRef::SetIntset(_) => ValueType::Set,
                RedisValueRef::SortedSet { .. } | RedisValueRef::SortedSetBPTree { .. } | RedisValueRef::SortedSetListpack(_) => ValueType::ZSet,
                RedisValueRef::Stream(_) => ValueType::Stream,
                RedisValueRef::String(_) => unreachable!(),
            };
            collection_buf = kv_serde::serialize_collection(other).unwrap_or_default();
            (vt, collection_buf.as_slice())
        }
    };

    // Determine flags and TTL
    let mut flags: u8 = 0;
    let ttl_ms = if entry.has_expiry() {
        flags |= entry_flags::HAS_TTL;
        Some(entry.expires_at_ms(0))
    } else {
        None
    };

    // Create page and insert entry
    let mut page = KvLeafPage::new(0, file_id);
    let overflow_pages: Vec<crate::persistence::kv_page::KvOverflowPage>;
    let total_pages: u32;

    match page.insert(key, value_bytes, value_type, flags, ttl_ms) {
        Ok(_) => {
            overflow_pages = Vec::new();
            total_pages = 1;
        }
        Err(PageFull) => {
            // Build overflow chain for the full value
            let chain = build_overflow_chain(value_bytes, file_id, 1);
            let chain_len = chain.len() as u32;

            // Build overflow pointer: start_page_idx u32 LE (= 1, first page after leaf)
            let overflow_ptr = 1u32.to_le_bytes();
            // Insert the pointer into the leaf with OVERFLOW flag
            let overflow_flags = flags | entry_flags::OVERFLOW;
            match page.insert(key, &overflow_ptr, value_type, overflow_flags, ttl_ms) {
                Ok(_) => {}
                Err(PageFull) => {
                    // Key itself is too large even for the overflow pointer
                    warn!(
                        key = %String::from_utf8_lossy(key),
                        key_len = key.len(),
                        "kv_spill: key too large for leaf page even with overflow pointer"
                    );
                    return Ok(());
                }
            }
            overflow_pages = chain;
            total_pages = 1 + chain_len;
        }
    }
    page.finalize();

    // Ensure data directory exists
    let data_dir = shard_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;

    // Write DataFile
    let file_path = data_dir.join(format!("heap-{file_id:06}.mpf"));
    if overflow_pages.is_empty() {
        write_datafile(&file_path, &[&page])?;
    } else {
        write_datafile_mixed(&file_path, &page, &overflow_pages)?;
    }

    // Register in manifest
    manifest.add_file(FileEntry {
        file_id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12, // 4KB = 2^12
        page_count: total_pages,
        byte_size: (total_pages as u64) * (PAGE_4K as u64),
        created_lsn: 0,
        min_key_hash: 0,
        max_key_hash: 0,
    });
    manifest.commit()?;

    // Update cold index with the spilled key's disk location
    if let Some(ci) = cold_index {
        ci.insert(
            Bytes::copy_from_slice(key),
            super::cold_index::ColdLocation {
                file_id,
                slot_idx: 0,
            },
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use crate::persistence::kv_page::read_datafile;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::compact_value::CompactValue;
    use crate::storage::entry::{Entry, RedisValue, current_time_ms};

    #[test]
    fn test_spill_string_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let entry = Entry::new_string(Bytes::from_static(b"hello world"));
        spill_to_datafile(shard_dir, 1, b"mykey", &entry, &mut manifest, None).unwrap();

        // Verify file was created
        let file_path = shard_dir.join("data/heap-000001.mpf");
        assert!(file_path.exists());

        // Read back and verify
        let pages = read_datafile(&file_path).unwrap();
        assert_eq!(pages.len(), 1);

        let kv_entry = pages[0].get(0).unwrap();
        assert_eq!(kv_entry.key, b"mykey");
        assert_eq!(kv_entry.value, b"hello world");
        assert_eq!(kv_entry.value_type, ValueType::String);
        assert_eq!(kv_entry.ttl_ms, None);

        // Verify manifest was updated
        assert_eq!(manifest.files().len(), 1);
        assert_eq!(manifest.files()[0].file_id, 1);
    }

    #[test]
    fn test_spill_with_ttl() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let mut entry = Entry::new_string(Bytes::from_static(b"expiring"));
        let future_ms = current_time_ms() + 60_000;
        entry.set_expires_at_ms(0, future_ms);

        spill_to_datafile(shard_dir, 2, b"ttl_key", &entry, &mut manifest, None).unwrap();

        let file_path = shard_dir.join("data/heap-000002.mpf");
        let pages = read_datafile(&file_path).unwrap();
        let kv_entry = pages[0].get(0).unwrap();

        assert_eq!(kv_entry.key, b"ttl_key");
        assert_eq!(kv_entry.value, b"expiring");
        // TTL should be present (stored as absolute ms, derived from seconds)
        assert!(kv_entry.ttl_ms.is_some());
        let stored_ttl = kv_entry.ttl_ms.unwrap();
        assert!(stored_ttl > 0);
    }

    #[test]
    fn test_spill_oversized_uses_overflow() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        // Create an entry that won't fit in a 4KB page even after LZ4.
        // Use a simple hash-like sequence that LZ4 cannot compress.
        let mut big_value = vec![0u8; 4000];
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for b in big_value.iter_mut() {
            // xorshift64
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = state as u8;
        }
        let entry = Entry::new_string(Bytes::from(big_value));

        spill_to_datafile(shard_dir, 3, b"big_key", &entry, &mut manifest, None).unwrap();

        // File SHOULD now exist with overflow pages
        let file_path = shard_dir.join("data/heap-000003.mpf");
        assert!(file_path.exists(), "oversized entry should use overflow pages");

        // Manifest should have an entry with page_count > 1
        assert_eq!(manifest.files().len(), 1);
        assert!(manifest.files()[0].page_count > 1, "should have overflow pages");

        // Verify the leaf page has OVERFLOW flag
        let file_data = std::fs::read(&file_path).unwrap();
        let mut leaf_buf = [0u8; PAGE_4K];
        leaf_buf.copy_from_slice(&file_data[..PAGE_4K]);
        let leaf = crate::persistence::kv_page::KvLeafPage::from_bytes(leaf_buf).unwrap();
        let kv_entry = leaf.get(0).unwrap();
        assert_ne!(kv_entry.flags & entry_flags::OVERFLOW, 0, "OVERFLOW flag should be set");
    }

    #[test]
    fn test_spill_hash_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let mut map = HashMap::new();
        map.insert(Bytes::from_static(b"f1"), Bytes::from_static(b"v1"));
        map.insert(Bytes::from_static(b"f2"), Bytes::from_static(b"v2"));

        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(RedisValue::Hash(map));

        spill_to_datafile(shard_dir, 10, b"hash_key", &entry, &mut manifest, None).unwrap();

        let file_path = shard_dir.join("data/heap-000010.mpf");
        assert!(file_path.exists(), "DataFile should exist for hash entry");

        let pages = read_datafile(&file_path).unwrap();
        assert_eq!(pages.len(), 1);

        let kv_entry = pages[0].get(0).unwrap();
        assert_eq!(kv_entry.key, b"hash_key");
        assert_eq!(kv_entry.value_type, ValueType::Hash);

        // Verify deserialization
        let deserialized = kv_serde::deserialize_collection(&kv_entry.value, ValueType::Hash)
            .expect("should deserialize hash");
        match deserialized {
            RedisValue::Hash(result_map) => {
                assert_eq!(result_map.len(), 2);
                assert_eq!(result_map.get(&Bytes::from_static(b"f1")).unwrap(), &Bytes::from_static(b"v1"));
                assert_eq!(result_map.get(&Bytes::from_static(b"f2")).unwrap(), &Bytes::from_static(b"v2"));
            }
            _ => panic!("expected Hash"),
        }
    }

    #[test]
    fn test_spill_list_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let mut list = VecDeque::new();
        list.push_back(Bytes::from_static(b"elem1"));
        list.push_back(Bytes::from_static(b"elem2"));
        list.push_back(Bytes::from_static(b"elem3"));

        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(RedisValue::List(list));

        spill_to_datafile(shard_dir, 11, b"list_key", &entry, &mut manifest, None).unwrap();

        let file_path = shard_dir.join("data/heap-000011.mpf");
        assert!(file_path.exists(), "DataFile should exist for list entry");

        let pages = read_datafile(&file_path).unwrap();
        let kv_entry = pages[0].get(0).unwrap();
        assert_eq!(kv_entry.key, b"list_key");
        assert_eq!(kv_entry.value_type, ValueType::List);

        let deserialized = kv_serde::deserialize_collection(&kv_entry.value, ValueType::List)
            .expect("should deserialize list");
        match deserialized {
            RedisValue::List(result_list) => {
                assert_eq!(result_list.len(), 3);
                assert_eq!(result_list[0], Bytes::from_static(b"elem1"));
                assert_eq!(result_list[1], Bytes::from_static(b"elem2"));
                assert_eq!(result_list[2], Bytes::from_static(b"elem3"));
            }
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn test_spill_overflow_string_roundtrip() {
        use crate::storage::tiered::cold_read::cold_read_through;
        use crate::storage::tiered::cold_index::ColdIndex;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();

        // 6KB of incompressible data (xorshift PRNG)
        let mut big_value = vec![0u8; 6000];
        let mut state: u64 = 0xDEAD_BEEF_CAFE_BABE;
        for b in big_value.iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = state as u8;
        }
        let entry = Entry::new_string(Bytes::from(big_value.clone()));

        spill_to_datafile(shard_dir, 50, b"overflow_key", &entry, &mut manifest, Some(&mut cold_index)).unwrap();

        // Verify file is multi-page
        let file_path = shard_dir.join("data/heap-000050.mpf");
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        assert!(file_size > PAGE_4K as u64, "file should have overflow pages");

        // Read back via cold_read_through
        let result = cold_read_through(&cold_index, shard_dir, b"overflow_key", 0);
        assert!(result.is_some(), "should read overflow entry");
        let (value, _ttl) = result.unwrap();
        match value {
            RedisValue::String(data) => {
                assert_eq!(data.as_ref(), big_value.as_slice());
            }
            _ => panic!("expected String"),
        }
    }
}
