//! KV spill-to-disk: serialize evicted entries to KvLeafPage DataFiles.
//!
//! When `disk_offload_enabled`, eviction writes entries to `.mpf` files
//! instead of permanently deleting them.

use std::io;
use std::path::Path;

use tracing::warn;

use crate::persistence::kv_page::{
    KvLeafPage, PageFull, ValueType, entry_flags, write_datafile,
};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::{PageType, PAGE_4K};
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;

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
) -> io::Result<()> {
    // Determine value type and extract bytes
    let (value_type, value_bytes): (ValueType, &[u8]) = match entry.as_redis_value() {
        RedisValueRef::String(s) => (ValueType::String, s),
        RedisValueRef::Hash(_) | RedisValueRef::HashListpack(_) => {
            warn!(
                key = %String::from_utf8_lossy(key),
                "kv_spill: skipping Hash entry (collection serialization not yet supported)"
            );
            return Ok(());
        }
        RedisValueRef::List(_) | RedisValueRef::ListListpack(_) => {
            warn!(
                key = %String::from_utf8_lossy(key),
                "kv_spill: skipping List entry (collection serialization not yet supported)"
            );
            return Ok(());
        }
        RedisValueRef::Set(_) | RedisValueRef::SetListpack(_) | RedisValueRef::SetIntset(_) => {
            warn!(
                key = %String::from_utf8_lossy(key),
                "kv_spill: skipping Set entry (collection serialization not yet supported)"
            );
            return Ok(());
        }
        RedisValueRef::SortedSet { .. }
        | RedisValueRef::SortedSetBPTree { .. }
        | RedisValueRef::SortedSetListpack(_) => {
            warn!(
                key = %String::from_utf8_lossy(key),
                "kv_spill: skipping ZSet entry (collection serialization not yet supported)"
            );
            return Ok(());
        }
        RedisValueRef::Stream(_) => {
            warn!(
                key = %String::from_utf8_lossy(key),
                "kv_spill: skipping Stream entry (collection serialization not yet supported)"
            );
            return Ok(());
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
    match page.insert(key, value_bytes, value_type, flags, ttl_ms) {
        Ok(_) => {}
        Err(PageFull) => {
            warn!(
                key = %String::from_utf8_lossy(key),
                key_len = key.len(),
                value_len = value_bytes.len(),
                "kv_spill: entry too large for single 4KB page, skipping (overflow pages TODO)"
            );
            return Ok(());
        }
    }
    page.finalize();

    // Ensure data directory exists
    let data_dir = shard_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;

    // Write DataFile
    let file_path = data_dir.join(format!("heap-{file_id:06}.mpf"));
    write_datafile(&file_path, &[&page])?;

    // Register in manifest
    manifest.add_file(FileEntry {
        file_id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12, // 4KB = 2^12
        page_count: 1,
        byte_size: PAGE_4K as u64,
        created_lsn: 0,
        min_key_hash: 0,
        max_key_hash: 0,
    });
    manifest.commit()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::persistence::kv_page::read_datafile;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::entry::{Entry, current_time_ms};

    #[test]
    fn test_spill_string_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();

        let entry = Entry::new_string(Bytes::from_static(b"hello world"));
        spill_to_datafile(shard_dir, 1, b"mykey", &entry, &mut manifest).unwrap();

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

        spill_to_datafile(shard_dir, 2, b"ttl_key", &entry, &mut manifest).unwrap();

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
    fn test_spill_oversized_entry_skips() {
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

        spill_to_datafile(shard_dir, 3, b"big_key", &entry, &mut manifest).unwrap();

        // No file should have been written
        let file_path = shard_dir.join("data/heap-000003.mpf");
        assert!(!file_path.exists());

        // Manifest should not have a new entry
        assert!(manifest.files().is_empty());
    }
}
