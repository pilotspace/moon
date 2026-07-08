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
use crate::persistence::page_cache::PageCache;
use crate::storage::entry::RedisValue;

/// Outcome of a cold read, distinguishing EXPIRED from plain miss so the
/// caller can reclaim the index entry (R1: expired cold entries used to leak
/// their index entry + file refcount forever — nothing else ever reclaims
/// them; the orphan sweep only checks hot-shadowing).
pub enum ColdReadOutcome {
    /// Entry found and alive.
    Hit(RedisValue, Option<u64>),
    /// Entry found but its TTL has passed — caller must remove the index entry.
    Expired,
    /// Not found / file unreadable / corrupt. The index entry is left alone:
    /// a transient I/O error must not permanently drop the key.
    Miss,
}

/// Attempt to read a cold KV entry from disk.
///
/// Returns `Some((RedisValue, ttl_ms))` on hit, `None` on miss/expired/error.
/// The caller is responsible for promoting the entry back to the DashTable
/// and removing it from the cold index. Callers that can reclaim expired
/// entries should prefer [`cold_read_through_outcome`].
pub fn cold_read_through(
    cold_index: &ColdIndex,
    shard_dir: &Path,
    key: &[u8],
    now_ms: u64,
) -> Option<(RedisValue, Option<u64>)> {
    match cold_read_through_outcome(cold_index, shard_dir, key, now_ms) {
        ColdReadOutcome::Hit(v, ttl) => Some((v, ttl)),
        ColdReadOutcome::Expired | ColdReadOutcome::Miss => None,
    }
}

/// Outcome-aware variant of [`cold_read_through`] (R1 reclaim path).
pub fn cold_read_through_outcome(
    cold_index: &ColdIndex,
    shard_dir: &Path,
    key: &[u8],
    now_ms: u64,
) -> ColdReadOutcome {
    cold_read_through_outcome_cached(cold_index, shard_dir, key, now_ms, None)
}

/// Same as [`cold_read_through_outcome`], but reads the 4KB leaf page through
/// `page_cache` when given (WS3 KV polish: repeated cold reads that land on
/// the same on-disk page -- e.g. distinct keys packed into one `KvLeafPage`,
/// or the same key re-evicted after promotion -- hit the PageCache instead of
/// re-issuing a `pread` every time). `None` preserves the exact pre-WS3
/// behavior (always pread).
pub fn cold_read_through_outcome_cached(
    cold_index: &ColdIndex,
    shard_dir: &Path,
    key: &[u8],
    now_ms: u64,
    page_cache: Option<&PageCache>,
) -> ColdReadOutcome {
    let Some(location) = cold_index.lookup(key) else {
        return ColdReadOutcome::Miss;
    };
    read_cold_entry(shard_dir, location, now_ms, page_cache)
}

/// Read a cold entry from disk given its location.
///
/// Returns the deserialized RedisValue and optional TTL (absolute ms).
/// Returns None if the entry is expired, file is missing, or data is corrupt.
pub fn read_cold_entry_at(
    shard_dir: &Path,
    location: ColdLocation,
    now_ms: u64,
) -> Option<(RedisValue, Option<u64>)> {
    match read_cold_entry(shard_dir, location, now_ms, None) {
        ColdReadOutcome::Hit(v, ttl) => Some((v, ttl)),
        ColdReadOutcome::Expired | ColdReadOutcome::Miss => None,
    }
}

/// Cached variant of [`read_cold_entry_at`] (see
/// [`cold_read_through_outcome_cached`]).
pub fn read_cold_entry_at_cached(
    shard_dir: &Path,
    location: ColdLocation,
    now_ms: u64,
    page_cache: Option<&PageCache>,
) -> Option<(RedisValue, Option<u64>)> {
    match read_cold_entry(shard_dir, location, now_ms, page_cache) {
        ColdReadOutcome::Hit(v, ttl) => Some((v, ttl)),
        ColdReadOutcome::Expired | ColdReadOutcome::Miss => None,
    }
}

fn read_cold_entry(
    shard_dir: &Path,
    location: ColdLocation,
    now_ms: u64,
    page_cache: Option<&PageCache>,
) -> ColdReadOutcome {
    let file_path = shard_dir
        .join("data")
        .join(format!("heap-{:06}.mpf", location.file_id));

    let page_offset = (location.page_idx as u64) * (PAGE_4K as u64);

    let leaf_buf: [u8; PAGE_4K] = match page_cache {
        Some(pc) => {
            // file_id namespaces the PageCache key by the on-disk DataFile's
            // own id (unique per shard, per `ColdLocation::file_id`) -- no
            // collision risk with other files sharing this PageCache pool.
            let Ok(handle) = pc.fetch_page(location.file_id, page_offset, false, |buf| {
                let Ok(file) = std::fs::File::open(&file_path) else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "cold data file missing",
                    ));
                };
                crate::util::file_ext::read_exact_at(&file, buf, page_offset)
            }) else {
                return ColdReadOutcome::Miss;
            };
            let data = pc.page_data(&handle);
            let mut buf = [0u8; PAGE_4K];
            buf.copy_from_slice(&data);
            drop(data);
            pc.unpin_page(handle);
            buf
        }
        None => {
            let Ok(file) = std::fs::File::open(&file_path) else {
                return ColdReadOutcome::Miss;
            };
            // Read only the specific 4KB page identified by page_idx (pread,
            // no whole-file read).
            let mut buf = [0u8; PAGE_4K];
            if crate::util::file_ext::read_exact_at(&file, &mut buf, page_offset).is_err() {
                return ColdReadOutcome::Miss;
            }
            buf
        }
    };

    let Some(page) = crate::persistence::kv_page::KvLeafPage::from_bytes(leaf_buf) else {
        return ColdReadOutcome::Miss;
    };
    let Some(entry) = page.get(location.slot_idx) else {
        return ColdReadOutcome::Miss;
    };

    // Check TTL expiry
    if let Some(ttl_ms) = entry.ttl_ms {
        if now_ms > ttl_ms {
            return ColdReadOutcome::Expired;
        }
    }

    // Resolve value bytes: handle overflow chain if flagged.
    // For overflow we need the full file to traverse the chain.
    let value_bytes = if entry.flags & entry_flags::OVERFLOW != 0 {
        // Overflow pointer: start_page_idx as u32 LE
        if entry.value.len() < 4 {
            return ColdReadOutcome::Miss;
        }
        let Ok(ptr_bytes) = <[u8; 4]>::try_from(&entry.value[..4]) else {
            return ColdReadOutcome::Miss;
        };
        let start_page_idx = u32::from_le_bytes(ptr_bytes) as usize;
        // Only read the full file when following an overflow chain.
        let Ok(file_data) = std::fs::read(&file_path) else {
            return ColdReadOutcome::Miss;
        };
        match read_overflow_chain(&file_data, start_page_idx) {
            Some(v) => v,
            None => return ColdReadOutcome::Miss,
        }
    } else {
        entry.value
    };

    // Convert to RedisValue based on value_type
    let redis_value = match entry.value_type {
        ValueType::String => RedisValue::String(Bytes::from(value_bytes)),
        _ => match kv_serde::deserialize_collection(&value_bytes, entry.value_type) {
            Some(v) => v,
            None => return ColdReadOutcome::Miss,
        },
    };

    ColdReadOutcome::Hit(redis_value, entry.ttl_ms)
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

    /// WS3: a `PageCache`-backed read must serve a second lookup against the
    /// same on-disk page from the cache instead of re-opening the file --
    /// proven by renaming the underlying data file away between the two
    /// reads. The plain (uncached) path would fail on the second read (file
    /// gone); the cached path must still succeed (page pinned in RAM from the
    /// first read).
    #[test]
    fn test_cold_read_through_page_cache_serves_second_read_without_disk() {
        use crate::persistence::page_cache::PageCache;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();

        let entry = Entry::new_string(Bytes::from_static(b"cached-value"));
        spill_to_datafile(
            shard_dir,
            21,
            b"cachekey",
            &entry,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        let page_cache = PageCache::new(8, 0);

        // First read: real cache miss, must pread from disk and populate the
        // cache.
        let r1 = cold_read_through_outcome_cached(
            &cold_index,
            shard_dir,
            b"cachekey",
            0,
            Some(&page_cache),
        );
        assert!(
            matches!(r1, ColdReadOutcome::Hit(..)),
            "first read should hit"
        );

        // Sabotage the on-disk file: if the second read falls through to a
        // real pread, it MUST miss.
        let location = cold_index.lookup(b"cachekey").expect("indexed");
        let file_path = shard_dir
            .join("data")
            .join(format!("heap-{:06}.mpf", location.file_id));
        std::fs::rename(&file_path, shard_dir.join("heap-moved-away.mpf")).unwrap();

        // Second read: must be served entirely from the PageCache -- the
        // page is still pinned-and-touched from the first fetch, so
        // `fetch_page` takes the cache-hit path and never calls `read_fn`
        // (which is the only place that would touch the now-missing file).
        let r2 = cold_read_through_outcome_cached(
            &cold_index,
            shard_dir,
            b"cachekey",
            0,
            Some(&page_cache),
        );
        assert!(
            matches!(r2, ColdReadOutcome::Hit(..)),
            "second read must be served from PageCache even though the file was moved away, got {:?}",
            match r2 {
                ColdReadOutcome::Hit(..) => "Hit",
                ColdReadOutcome::Expired => "Expired",
                ColdReadOutcome::Miss => "Miss",
            }
        );

        // Sanity: the uncached path against the same (now-missing) file
        // really does miss -- proves the test's premise (a real second pread
        // would fail) rather than the file having survived by luck.
        let r3 = cold_read_through_outcome(&cold_index, shard_dir, b"cachekey", 0);
        assert!(
            matches!(r3, ColdReadOutcome::Miss),
            "uncached read against the moved-away file must miss (test premise check)"
        );
    }

    /// Build a Database with an active cold tier holding one spilled key.
    fn db_with_spilled_key(
        shard_dir: &std::path::Path,
        key: &[u8],
        value: &[u8],
        ttl_ms: Option<u64>,
    ) -> crate::storage::db::Database {
        let manifest_path = shard_dir.join("shard.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        let mut cold_index = ColdIndex::new();

        let mut entry = Entry::new_string(Bytes::copy_from_slice(value));
        if let Some(ttl) = ttl_ms {
            entry.set_expires_at_ms(0, ttl);
        }
        spill_to_datafile(
            shard_dir,
            40,
            key,
            &entry,
            &mut manifest,
            Some(&mut cold_index),
        )
        .unwrap();

        let mut db = crate::storage::db::Database::new();
        db.cold_shard_dir = Some(shard_dir.to_path_buf());
        db.cold_index = Some(cold_index);
        db
    }

    /// D1 (PR review of tmp/OFFLOAD-COMPRESSION-REVIEW.md): DEL of a spilled
    /// key must actually delete it — count it, drop the index entry, and make
    /// subsequent GETs return nil instead of resurrecting the cold value.
    #[test]
    fn test_del_removes_cold_entry_no_resurrection() {
        let tmp = tempfile::tempdir().unwrap();
        let mut db = db_with_spilled_key(tmp.path(), b"doomed", b"value-on-disk", None);

        // Sanity: the key is reachable via cold read-through before DEL.
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"doomed").is_some(),
            "precondition: key is cold-indexed"
        );

        let frame = crate::command::key::del(
            &mut db,
            &[crate::protocol::Frame::BulkString(Bytes::from_static(
                b"doomed",
            ))],
        );
        assert_eq!(
            frame,
            crate::protocol::Frame::Integer(1),
            "DEL of a cold-only key must count it as removed"
        );
        assert!(
            db.get(b"doomed").is_none(),
            "GET after DEL must NOT resurrect the cold value"
        );
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"doomed").is_none(),
            "cold index entry must be gone after DEL"
        );
        assert!(
            db.cold_index.as_ref().unwrap().has_pending_unlink(),
            "last referrer removed: file must be queued for unlink"
        );
    }

    /// D1: FLUSHDB/FLUSHALL (`Database::clear`) must clear the cold tier too —
    /// flushed keys must not remain readable from disk.
    #[test]
    fn test_clear_flushes_cold_tier() {
        let tmp = tempfile::tempdir().unwrap();
        let mut db = db_with_spilled_key(tmp.path(), b"flushed", b"value-on-disk", None);

        db.clear();

        assert!(
            db.get(b"flushed").is_none(),
            "GET after FLUSH must NOT read the cold value back from disk"
        );
        assert!(
            db.cold_index.as_ref().unwrap().has_pending_unlink(),
            "cold files must be queued for unlink after clear"
        );
    }

    /// R1: a cold read that finds the entry EXPIRED must reclaim the index
    /// entry (and thereby the file refcount) instead of leaking it forever.
    #[test]
    fn test_expired_cold_read_reclaims_index_entry() {
        let tmp = tempfile::tempdir().unwrap();
        // TTL 1ms in the past relative to the read below.
        let mut db = db_with_spilled_key(tmp.path(), b"stale", b"old", Some(1));

        assert!(
            db.get(b"stale").is_none(),
            "expired cold entry reads as nil"
        );
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"stale").is_none(),
            "expired cold entry must be reclaimed from the index on read"
        );
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
