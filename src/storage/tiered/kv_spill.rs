//! KV spill-to-disk: serialize evicted entries to KvLeafPage DataFiles.
//!
//! When `disk_offload_enabled`, eviction writes entries to `.mpf` files
//! instead of permanently deleting them.

use std::io;
use std::path::Path;

use bytes::Bytes;
use tracing::warn;

use super::kv_serde;
use crate::persistence::kv_page::{
    KvLeafPage, PageFull, ValueType, build_overflow_chain, entry_flags, write_datafile,
    write_datafile_mixed,
};
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::{PAGE_4K, PageType};
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;

/// Outcome of building a spill page set: a finalized leaf page, the overflow
/// chain (empty unless the value didn't fit), and the total page count.
///
/// Both the synchronous (`spill_to_datafile`) and asynchronous
/// (`SpillThread::write_spill_file`) paths construct identical leaf/overflow
/// layouts; this helper is the single source of truth for that layout.
pub struct KvSpillPages {
    pub leaf: KvLeafPage,
    pub overflow: Vec<crate::persistence::kv_page::KvOverflowPage>,
    pub total_pages: u32,
}

/// Build the leaf + overflow page set for a spilled KV entry.
///
/// Returns `Ok(KvSpillPages)` on success. Returns `Err(io::ErrorKind::InvalidData)`
/// if the key itself is too large to fit in a leaf page even alongside an
/// overflow pointer (an irrecoverable layout failure for that key).
pub fn build_kv_spill_pages(
    key: &[u8],
    value_bytes: &[u8],
    value_type: ValueType,
    flags: u8,
    ttl_ms: Option<u64>,
    file_id: u64,
) -> io::Result<KvSpillPages> {
    let mut leaf = KvLeafPage::new(0, file_id);

    let (overflow, total_pages) = match leaf.insert(key, value_bytes, value_type, flags, ttl_ms) {
        Ok(_) => (Vec::new(), 1u32),
        Err(PageFull) => {
            // Build the overflow chain and reinsert the key with an overflow pointer.
            let chain = build_overflow_chain(value_bytes, file_id, 1);
            let chain_len = chain.len() as u32;
            let overflow_ptr = 1u32.to_le_bytes();
            let overflow_flags = flags | entry_flags::OVERFLOW;
            match leaf.insert(key, &overflow_ptr, value_type, overflow_flags, ttl_ms) {
                Ok(_) => {}
                Err(PageFull) => {
                    warn!(
                        key_len = key.len(),
                        "kv_spill: key too large for leaf page even with overflow pointer"
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "key too large for leaf page",
                    ));
                }
            }
            (chain, 1 + chain_len)
        }
    };

    leaf.finalize();

    Ok(KvSpillPages {
        leaf,
        overflow,
        total_pages,
    })
}

/// Write a previously-built `KvSpillPages` to `{shard_dir}/data/heap-{file_id:06}.mpf`.
///
/// Returns the byte size of the written file. The caller is responsible for
/// updating the manifest / cold index after this returns.
pub fn write_kv_spill_pages(
    shard_dir: &Path,
    file_id: u64,
    pages: &KvSpillPages,
) -> io::Result<u64> {
    let data_dir = shard_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;
    let file_path = data_dir.join(format!("heap-{file_id:06}.mpf"));

    if pages.overflow.is_empty() {
        write_datafile(&file_path, &[&pages.leaf])?;
    } else {
        write_datafile_mixed(&file_path, &pages.leaf, &pages.overflow)?;
    }

    Ok((pages.total_pages as u64) * (PAGE_4K as u64))
}

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
    // Determine value type and extract bytes. For collections, serialize via
    // kv_serde; for strings, borrow directly.
    let collection_buf: Vec<u8>;
    let val_ref = entry.as_redis_value();
    let (value_type, value_bytes): (ValueType, &[u8]) = match val_ref {
        RedisValueRef::String(s) => (ValueType::String, s),
        ref other => {
            let vt = match other {
                RedisValueRef::Hash(_)
                | RedisValueRef::HashListpack(_)
                | RedisValueRef::HashWithTtl { .. } => ValueType::Hash,
                RedisValueRef::List(_) | RedisValueRef::ListListpack(_) => ValueType::List,
                RedisValueRef::Set(_)
                | RedisValueRef::SetListpack(_)
                | RedisValueRef::SetIntset(_) => ValueType::Set,
                RedisValueRef::SortedSet { .. }
                | RedisValueRef::SortedSetBPTree { .. }
                | RedisValueRef::SortedSetListpack(_) => ValueType::ZSet,
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

    // Build leaf + overflow via the shared helper. A "key too large" failure
    // is non-fatal here (legacy behavior) — log and skip the spill.
    let pages = match build_kv_spill_pages(key, value_bytes, value_type, flags, ttl_ms, file_id) {
        Ok(p) => p,
        Err(e) if e.kind() == io::ErrorKind::InvalidData => {
            warn!(key = %String::from_utf8_lossy(key), "kv_spill: skipping oversized key");
            return Ok(());
        }
        Err(e) => return Err(e),
    };

    let byte_size = write_kv_spill_pages(shard_dir, file_id, &pages)?;

    // Register in manifest
    manifest.add_file(FileEntry {
        file_id,
        file_type: PageType::KvLeaf as u8,
        status: FileStatus::Active,
        tier: StorageTier::Hot,
        page_size_log2: 12, // 4KB = 2^12
        page_count: pages.total_pages,
        byte_size,
        created_lsn: 0,
        min_key_hash: 0,
        max_key_hash: 0,
        last_modified_lsn: 0,
    });
    manifest.commit()?;

    // Update cold index with the spilled key's disk location
    if let Some(ci) = cold_index {
        ci.insert(
            Bytes::copy_from_slice(key),
            super::cold_index::ColdLocation {
                file_id,
                page_idx: 0,
                slot_idx: 0,
            },
        );
    }

    Ok(())
}

// ── Multi-page batch spill ───────────────────────────────────────────────────

/// Maximum raw value size (in bytes) for an entry to be eligible for inline
/// batching.  Entries whose serialized value exceeds this threshold are spilled
/// via the existing single-file path (`build_kv_spill_pages`) which handles
/// overflow chains correctly.
///
/// The 4KB leaf page has ~3916B of usable payload after all headers + one slot
/// (PAGE_4K=4096 − MoonPage header 64B − KV header 16B − slot 4B).  After LZ4
/// compression (minimum 256B value → may not shrink) plus key overhead, the
/// safe inline threshold is 3500B.  Using the raw value length is conservative
/// but correct: the caller pre-screens before building the entry, so the batch
/// builder never sees truly oversized values.
pub const INLINE_MAX_VALUE_BYTES: usize = 3500;

/// One entry to include in a spill batch.
pub struct SpillEntry {
    pub key: bytes::Bytes,
    pub value_bytes: bytes::Bytes,
    pub value_type: ValueType,
    pub flags: u8,
    pub ttl_ms: Option<u64>,
}

/// Result of building a multi-page inline spill batch.
///
/// `leaves` contains all KvLeafPages (in file order).  `locations[i]` is the
/// `(page_idx, slot_idx)` for `entries[i]` — page_idx is the FILE-ABSOLUTE
/// 4KB chunk index.
///
/// Inline-only MVP: the caller MUST pre-screen entries so that no entry's
/// `value_bytes.len()` exceeds `INLINE_MAX_VALUE_BYTES`.  Entries larger than
/// that threshold are routed to `build_kv_spill_pages` (single-file path) by
/// the caller (`flush_buffer` in `spill_thread.rs`).  Mixing overflow and
/// inline entries in a single file is deferred to a future phase.
pub struct BatchPages {
    pub leaves: Vec<KvLeafPage>,
    pub overflow: Vec<crate::persistence::kv_page::KvOverflowPage>,
    /// Parallel to the *accepted* entries slice: (file-absolute page_idx, slot_idx).
    pub locations: Vec<(u32, u16)>,
}

/// Build a multi-page inline spill batch from a slice of entries.
///
/// Greedily packs entries into KvLeafPages using `KvLeafPage::insert`.  When
/// a page fills up (`Err(PageFull)`) the current leaf is finalized and a fresh
/// leaf is started.
///
/// **Inline-only** — the caller (`flush_buffer`) MUST pre-screen so that no
/// entry exceeds `INLINE_MAX_VALUE_BYTES`.  If an entry still does not fit on a
/// fresh leaf (e.g. because post-LZ4 compression it is still too large), the
/// function returns `Err(io::ErrorKind::InvalidData)`.  The caller must catch
/// that and fall back to the single-file path for that entry.
///
/// The `overflow` field of `BatchPages` is always empty from this function.
/// It exists on the struct for forward-compatibility.
pub fn build_kv_spill_batch(entries: &[SpillEntry], file_id: u64) -> io::Result<BatchPages> {
    let mut leaves: Vec<KvLeafPage> = Vec::new();
    let mut locations: Vec<(u32, u16)> = Vec::with_capacity(entries.len());

    // Start with page 0.
    let mut current_leaf = KvLeafPage::new(0, file_id);
    let mut current_page_idx: u32 = 0;

    for entry in entries {
        // Try inserting directly into the current leaf.
        match current_leaf.insert(
            &entry.key,
            &entry.value_bytes,
            entry.value_type,
            entry.flags,
            entry.ttl_ms,
        ) {
            Ok(slot_idx) => {
                locations.push((current_page_idx, slot_idx));
            }
            Err(PageFull) => {
                // Current leaf is full.  Finalize it and start a new one,
                // then retry the insert on the fresh leaf.
                current_leaf.finalize();
                leaves.push(current_leaf);
                current_page_idx += 1;
                current_leaf = KvLeafPage::new(current_page_idx as u64, file_id);

                match current_leaf.insert(
                    &entry.key,
                    &entry.value_bytes,
                    entry.value_type,
                    entry.flags,
                    entry.ttl_ms,
                ) {
                    Ok(slot_idx) => {
                        locations.push((current_page_idx, slot_idx));
                    }
                    Err(PageFull) => {
                        // Value is too large for a fresh leaf even after LZ4.
                        // The caller should have pre-screened using
                        // INLINE_MAX_VALUE_BYTES; this is a defensive fallback.
                        warn!(
                            key_len = entry.key.len(),
                            value_len = entry.value_bytes.len(),
                            "kv_spill batch: entry too large for inline leaf, skipping"
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "entry too large for inline leaf page",
                        ));
                    }
                }
            }
        }
    }

    // Finalize and push the last (possibly only) leaf.
    current_leaf.finalize();
    leaves.push(current_leaf);

    Ok(BatchPages {
        leaves,
        overflow: Vec::new(),
        locations,
    })
}

/// Write a `BatchPages` to `{shard_dir}/data/heap-{file_id:06}.mpf` atomically.
///
/// Layout: all leaf pages first (file offsets 0..L*4KB), then overflow pages
/// (offsets L*4KB..).  Writes to a `.tmp` file, fsyncs, then renames — so a
/// crash during write leaves no partial file visible.
///
/// Returns the total byte size written.
pub fn write_kv_spill_batch(shard_dir: &Path, file_id: u64, batch: &BatchPages) -> io::Result<u64> {
    use std::io::Write as _;

    let data_dir = shard_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;

    let final_path = data_dir.join(format!("heap-{file_id:06}.mpf"));
    let tmp_path = data_dir.join(format!("heap-{file_id:06}.tmp"));

    {
        let mut file = std::fs::File::create(&tmp_path)?;
        for leaf in &batch.leaves {
            file.write_all(leaf.as_bytes())?;
        }
        for ov in &batch.overflow {
            file.write_all(ov.as_bytes())?;
        }
        file.sync_all()?;
    }

    std::fs::rename(&tmp_path, &final_path)?;

    let total_pages = (batch.leaves.len() + batch.overflow.len()) as u64;
    Ok(total_pages * PAGE_4K as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kv_page::read_datafile;
    use crate::persistence::manifest::ShardManifest;
    use crate::storage::compact_value::CompactValue;
    use crate::storage::entry::{Entry, RedisValue, current_time_ms};
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::collections::VecDeque;

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
        assert!(
            file_path.exists(),
            "oversized entry should use overflow pages"
        );

        // Manifest should have an entry with page_count > 1
        assert_eq!(manifest.files().len(), 1);
        assert!(
            manifest.files()[0].page_count > 1,
            "should have overflow pages"
        );

        // Verify the leaf page has OVERFLOW flag
        let file_data = std::fs::read(&file_path).unwrap();
        let mut leaf_buf = [0u8; PAGE_4K];
        leaf_buf.copy_from_slice(&file_data[..PAGE_4K]);
        let leaf = crate::persistence::kv_page::KvLeafPage::from_bytes(leaf_buf).unwrap();
        let kv_entry = leaf.get(0).unwrap();
        assert_ne!(
            kv_entry.flags & entry_flags::OVERFLOW,
            0,
            "OVERFLOW flag should be set"
        );
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
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"f1")).unwrap(),
                    &Bytes::from_static(b"v1")
                );
                assert_eq!(
                    result_map.get(&Bytes::from_static(b"f2")).unwrap(),
                    &Bytes::from_static(b"v2")
                );
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
        use crate::storage::tiered::cold_index::ColdIndex;
        use crate::storage::tiered::cold_read::cold_read_through;

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

        spill_to_datafile(
            shard_dir,
            50,
            b"overflow_key",
            &entry,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        // Verify file is multi-page
        let file_path = shard_dir.join("data/heap-000050.mpf");
        let file_size = std::fs::metadata(&file_path).unwrap().len();
        assert!(
            file_size > PAGE_4K as u64,
            "file should have overflow pages"
        );

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

    // ── Multi-page batch tests (TDD: written before implementation) ──────────

    /// Helper: build N entries with distinct keys/values small enough to fit
    /// inline (≤200 bytes each), forcing page overflow by sheer count.
    fn make_inline_entries(n: usize) -> Vec<SpillEntry> {
        (0..n)
            .map(|i| SpillEntry {
                key: bytes::Bytes::from(format!("batch_key_{i:04}")),
                value_bytes: bytes::Bytes::from(format!(
                    "batch_value_{i:04}_padding_to_200_bytes_{:0>150}",
                    i
                )),
                value_type: ValueType::String,
                flags: 0,
                ttl_ms: None,
            })
            .collect()
    }

    /// A KvLeafPage holds roughly 7-10 entries of ~200 B each.  Generating 50
    /// entries guarantees ≥ 2 leaf pages.
    #[test]
    fn test_build_kv_spill_batch_multi_page() {
        const N: usize = 50;
        let entries = make_inline_entries(N);
        let file_id = 42u64;

        let batch =
            build_kv_spill_batch(&entries, file_id).expect("build_kv_spill_batch should succeed");

        // Must have spanned at least 2 leaf pages.
        assert!(
            batch.leaves.len() >= 2,
            "expected ≥2 leaf pages, got {}",
            batch.leaves.len()
        );
        // One location per entry.
        assert_eq!(batch.locations.len(), N);

        // page_idx values must be monotonically non-decreasing and within range.
        let max_page = batch.leaves.len() as u32 - 1;
        for (i, &(page_idx, slot_idx)) in batch.locations.iter().enumerate() {
            assert!(
                page_idx <= max_page,
                "entry {i}: page_idx {page_idx} out of range (max {max_page})"
            );
            let _ = slot_idx; // just assert no panic
        }
    }

    /// write_kv_spill_batch must produce an atomic file and
    /// read_cold_entry_at must recover every entry by (page_idx, slot_idx).
    #[test]
    fn test_write_and_read_batch_multi_page() {
        use crate::storage::tiered::cold_index::ColdLocation;
        use crate::storage::tiered::cold_read::read_cold_entry_at;

        const N: usize = 50;
        let entries = make_inline_entries(N);
        let file_id = 77u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch = build_kv_spill_batch(&entries, file_id).unwrap();
        assert!(batch.leaves.len() >= 2, "test requires ≥2 leaf pages");

        let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();
        assert!(byte_size > 0);

        // The file must exist at the canonical path (not the .tmp).
        let file_path = shard_dir
            .join("data")
            .join(format!("heap-{file_id:06}.mpf"));
        assert!(
            file_path.exists(),
            "batch file should exist at canonical path"
        );
        let tmp_path = shard_dir
            .join("data")
            .join(format!("heap-{file_id:06}.tmp"));
        assert!(!tmp_path.exists(), ".tmp file should be renamed away");

        // Round-trip: every entry must be readable by its location.
        for (i, (&(page_idx, slot_idx), entry)) in
            batch.locations.iter().zip(entries.iter()).enumerate()
        {
            let loc = ColdLocation {
                file_id,
                page_idx,
                slot_idx,
            };
            let result = read_cold_entry_at(shard_dir, loc, 0);
            assert!(
                result.is_some(),
                "entry {i} (key={}) not readable at page_idx={page_idx} slot_idx={slot_idx}",
                String::from_utf8_lossy(&entry.key)
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => {
                    assert_eq!(
                        data.as_ref(),
                        entry.value_bytes.as_ref(),
                        "entry {i}: value mismatch"
                    );
                }
                _ => panic!("entry {i}: expected String"),
            }
        }
    }

    /// An entry at page_idx=3 slot=2 (deep in the batch) resolves correctly.
    #[test]
    fn test_batch_deep_page_slot_resolves() {
        use crate::storage::tiered::cold_index::ColdLocation;
        use crate::storage::tiered::cold_read::read_cold_entry_at;

        // Generate enough entries to reach page_idx ≥ 3.
        const N: usize = 100;
        let entries = make_inline_entries(N);
        let file_id = 88u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        let batch = build_kv_spill_batch(&entries, file_id).unwrap();
        write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        // Find the first entry with page_idx >= 3 and slot_idx >= 1.
        let target = batch
            .locations
            .iter()
            .zip(entries.iter())
            .enumerate()
            .find(|&(_, (&(page_idx, slot_idx), _))| page_idx >= 3 && slot_idx >= 1);

        if let Some((i, (&(page_idx, slot_idx), entry))) = target {
            let loc = ColdLocation {
                file_id,
                page_idx,
                slot_idx,
            };
            let result = read_cold_entry_at(shard_dir, loc, 0);
            assert!(
                result.is_some(),
                "deep entry {i} (page={page_idx} slot={slot_idx}) not readable"
            );
            let (value, _) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => {
                    assert_eq!(data.as_ref(), entry.value_bytes.as_ref());
                }
                _ => panic!("expected String"),
            }
        } else {
            // Fewer than 100 entries didn't reach page 3 — bump N if this fires.
            panic!(
                "test needs more entries to reach page_idx≥3 slot≥1; got {} pages",
                batch.leaves.len()
            );
        }
    }

    /// RECOVERY-PATH test: prove `ColdIndex::rebuild_from_manifest` reconstructs
    /// the SAME (page_idx, slot_idx) mapping the builder produced.
    ///
    /// The other batch tests read back via the *builder's* returned `locations`.
    /// On crash recovery the cold_index is thrown away and rebuilt by SCANNING
    /// the heap file (`chunks_exact(PAGE_4K).enumerate()` + `slot_count()`) — a
    /// completely independent mapping. If that scan disagrees with the builder
    /// (off-by-one page index, slot ordering, overflow-page miscount), every
    /// cold key returns nil after restart even though the live path is green.
    /// This test exercises the recovery mapping end-to-end, fully in-process.
    #[test]
    fn test_rebuild_from_manifest_roundtrip() {
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;
        use crate::storage::tiered::cold_index::ColdIndex;
        use crate::storage::tiered::cold_read::cold_read_through;

        // Enough entries to span several leaf pages (the multi-page case is the
        // whole point — single-page would never exercise the page_idx scan).
        const N: usize = 100;
        let entries = make_inline_entries(N);
        let file_id = 123u64;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();

        // 1. Build + write the batch file (the live spill path).
        let batch = build_kv_spill_batch(&entries, file_id).unwrap();
        assert!(
            batch.leaves.len() >= 3,
            "test requires ≥3 leaf pages to exercise page_idx>0 in the rebuild scan"
        );
        let byte_size = write_kv_spill_batch(shard_dir, file_id, &batch).unwrap();

        // 2. Register the file in a manifest exactly as apply_spill_completions does.
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        manifest.add_file(FileEntry {
            file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: batch.leaves.len() as u32,
            byte_size,
            created_lsn: 0,
            min_key_hash: 0,
            max_key_hash: 0,
            last_modified_lsn: 0,
        });
        manifest.commit().unwrap();

        // 3. Rebuild the cold index FROM THE MANIFEST (the recovery path under test).
        //    Note: this throws away `batch.locations` and recomputes everything.
        let rebuilt = ColdIndex::rebuild_from_manifest(shard_dir, &manifest);
        assert_eq!(
            rebuilt.len(),
            N,
            "rebuild must recover every entry (got {} of {N})",
            rebuilt.len()
        );

        // 4. Every key must read back its exact value VIA THE REBUILT INDEX —
        //    not the builder's locations. This is what a real restart does.
        for entry in &entries {
            let result = cold_read_through(&rebuilt, shard_dir, &entry.key, 0);
            assert!(
                result.is_some(),
                "rebuilt index: key {} returned nil (page/slot mapping mismatch)",
                String::from_utf8_lossy(&entry.key)
            );
            let (value, _ttl) = result.unwrap();
            match value {
                crate::storage::entry::RedisValue::String(data) => assert_eq!(
                    data.as_ref(),
                    entry.value_bytes.as_ref(),
                    "rebuilt index: key {} resolved to the WRONG value (slot/page swap)",
                    String::from_utf8_lossy(&entry.key)
                ),
                other => panic!("expected String for {:?}, got {other:?}", entry.key),
            }
        }
    }
}
