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
    /// The cold index has NO entry for the key. The only outcome that means
    /// "absent".
    Miss,
    /// The cold index HAS an entry, but the bytes it points at could not be
    /// produced: file missing or unreadable, page corrupt, slot undecodable,
    /// overflow chain broken (moon#875). This is NOT absence — the key is
    /// indexed, `EXISTS` says 1, `DBSIZE` counts it — and it must never be
    /// folded into [`Self::Miss`] by a caller that has a way to say so.
    /// The index entry is deliberately left alone: a transient I/O error
    /// must not permanently drop the key, and a later read retries.
    ///
    /// At the wire the command answers `-IOERR` (see
    /// `Database::cold_fault_error`): the fault is noted on the `Database`
    /// by the two read-through funnels (`promote_cold_outcome`,
    /// `get_cold_value`), the fabricating accessors refuse BEFORE mutating,
    /// and the dispatch boundary turns any remaining reply into the error.
    /// It is also counted in `INFO` (`reclamation_cold_read_unreadable_total`)
    /// and logged with its location by [`read_cold_entry`].
    Unreadable(ColdReadFault),
}

/// Why an indexed cold entry could not be read. Carried on
/// [`ColdReadOutcome::Unreadable`] so the log line names the exact page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColdReadFault {
    pub location: ColdLocation,
    pub reason: ColdReadFaultReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColdReadFaultReason {
    /// The heap file is not on disk.
    FileMissing,
    /// The heap file exists but could not be opened or read.
    FileUnreadable,
    /// The page at `page_idx` failed its magic/type/CRC check.
    PageRejected,
    /// The slot at `slot_idx` is out of range or does not decode.
    SlotUndecodable,
    /// The value's overflow chain could not be followed.
    OverflowBroken,
    /// The collection body did not deserialize.
    ValueUndecodable,
}

impl ColdReadFaultReason {
    /// Inverse of `as u8`, for the `Database::cold_fault` flag.
    #[must_use]
    pub fn from_code(v: u8) -> Option<Self> {
        Some(match v {
            0 => Self::FileMissing,
            1 => Self::FileUnreadable,
            2 => Self::PageRejected,
            3 => Self::SlotUndecodable,
            4 => Self::OverflowBroken,
            5 => Self::ValueUndecodable,
            _ => return None,
        })
    }
}

/// Every unreadable-but-indexed cold read funnels through here: count it in
/// `INFO`, and log the first few (then every power of two) with the exact
/// location so an operator can find the file. Rate-limited because a client
/// hammering one broken key must not turn the log into the disk it is
/// reporting on.
fn note_unreadable(fault: ColdReadFault) -> ColdReadOutcome {
    let n = crate::command::info_reclamation::record_cold_read_unreadable();
    if n <= 16 || n.is_power_of_two() {
        tracing::error!(
            file_id = fault.location.file_id,
            page_idx = fault.location.page_idx,
            slot_idx = fault.location.slot_idx,
            reason = ?fault.reason,
            total = n,
            "cold read: key is INDEXED but its data could not be read; answered as a miss \
             to the client. The index entry is kept so a later read retries"
        );
    }
    ColdReadOutcome::Unreadable(fault)
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
        ColdReadOutcome::Expired | ColdReadOutcome::Miss | ColdReadOutcome::Unreadable(_) => None,
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
        ColdReadOutcome::Expired | ColdReadOutcome::Miss | ColdReadOutcome::Unreadable(_) => None,
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
        ColdReadOutcome::Expired | ColdReadOutcome::Miss | ColdReadOutcome::Unreadable(_) => None,
    }
}

/// Test-only injected latency (milliseconds), checked at the top of
/// [`read_cold_entry`] before any real I/O. Lets tests deterministically
/// simulate a slow/backlogged disk (task #59) without real disk contention.
/// `0` (the default) is a no-op.
#[cfg(test)]
pub(crate) static TEST_INJECT_DELAY_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Process-wide lock serializing any test (in this module or
/// `cold_read_pool`) that mutates [`TEST_INJECT_DELAY_MS`] or the pool's
/// timeout knob -- `cargo test`'s default parallelism otherwise lets one
/// test's injected delay leak into an unrelated concurrently-running test.
#[cfg(test)]
pub(crate) static TEST_DELAY_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// `pub(crate)` (rather than private) so [`super::cold_read_pool`] can call
/// it from its off-shard-thread worker pool (task #59). Prefer the
/// pooled/bounded entry points (`read_cold_entry_at_bounded`,
/// `cold_read_through_outcome_bounded`) on the shard event-loop path; this
/// raw synchronous form remains for tests and for the pool worker itself.
pub(crate) fn read_cold_entry(
    shard_dir: &Path,
    location: ColdLocation,
    now_ms: u64,
    page_cache: Option<&PageCache>,
) -> ColdReadOutcome {
    // Task #59 lever 2: any cold read — including the synchronous MGET /
    // MULTI / Lua paths that never go through the async pool — signals the
    // spill writer to briefly yield the device. Double-counting with the
    // async path's own guard is harmless (the signal is "readers > 0").
    let _inflight = super::cold_read_pool::ColdReadInflightGuard::new();
    #[cfg(test)]
    {
        let delay_ms = TEST_INJECT_DELAY_MS.load(std::sync::atomic::Ordering::Relaxed);
        if delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        }
    }
    let file_path = shard_dir
        .join("data")
        .join(format!("heap-{:06}.mpf", location.file_id));
    let fault = |reason: ColdReadFaultReason| note_unreadable(ColdReadFault { location, reason });
    let io_reason = |e: &std::io::Error| {
        if e.kind() == std::io::ErrorKind::NotFound {
            ColdReadFaultReason::FileMissing
        } else {
            ColdReadFaultReason::FileUnreadable
        }
    };

    let page_offset = (location.page_idx as u64) * (PAGE_4K as u64);

    let leaf_buf: [u8; PAGE_4K] = match page_cache {
        Some(pc) => {
            // file_id namespaces the PageCache key by the on-disk DataFile's
            // own id (unique per shard, per `ColdLocation::file_id`) -- no
            // collision risk with other files sharing this PageCache pool.
            let handle = match pc.fetch_page(location.file_id, page_offset, false, |buf| {
                let file = std::fs::File::open(&file_path)?;
                crate::util::file_ext::read_exact_at(&file, buf, page_offset)
            }) {
                Ok(h) => h,
                Err(e) => return fault(io_reason(&e)),
            };
            let data = pc.page_data(&handle);
            let mut buf = [0u8; PAGE_4K];
            buf.copy_from_slice(&data);
            drop(data);
            pc.unpin_page(handle);
            buf
        }
        None => {
            let file = match std::fs::File::open(&file_path) {
                Ok(f) => f,
                Err(e) => return fault(io_reason(&e)),
            };
            // Read only the specific 4KB page identified by page_idx (pread,
            // no whole-file read).
            let mut buf = [0u8; PAGE_4K];
            if crate::util::file_ext::read_exact_at(&file, &mut buf, page_offset).is_err() {
                return fault(ColdReadFaultReason::FileUnreadable);
            }
            buf
        }
    };

    let Some(page) = crate::persistence::kv_page::KvLeafPage::from_bytes(leaf_buf) else {
        return fault(ColdReadFaultReason::PageRejected);
    };
    let Some(entry) = page.get(location.slot_idx) else {
        return fault(ColdReadFaultReason::SlotUndecodable);
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
            return fault(ColdReadFaultReason::OverflowBroken);
        }
        let Ok(ptr_bytes) = <[u8; 4]>::try_from(&entry.value[..4]) else {
            return fault(ColdReadFaultReason::OverflowBroken);
        };
        let start_page_idx = u32::from_le_bytes(ptr_bytes) as usize;
        // Only read the full file when following an overflow chain.
        let file_data = match std::fs::read(&file_path) {
            Ok(d) => d,
            Err(e) => return fault(io_reason(&e)),
        };
        match read_overflow_chain(&file_data, start_page_idx) {
            Some(v) => v,
            None => return fault(ColdReadFaultReason::OverflowBroken),
        }
    } else {
        entry.value
    };

    // Convert to RedisValue based on value_type
    let redis_value = match entry.value_type {
        ValueType::String => RedisValue::String(Bytes::from(value_bytes)),
        _ => match kv_serde::deserialize_collection(&value_bytes, entry.value_type) {
            Some(v) => v,
            None => return fault(ColdReadFaultReason::ValueUndecodable),
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
        entry.value = CompactValue::from_redis_value(RedisValue::Hash(Box::new(map)));

        spill_to_datafile(
            shard_dir,
            20,
            b"myhash",
            &entry,
            0,
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
            0,
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
                ColdReadOutcome::Unreadable(_) => "Unreadable",
            }
        );

        // Sanity: the uncached path against the same (now-missing) file
        // really does fail -- proves the test's premise (a real second pread
        // would fail) rather than the file having survived by luck. moon#875:
        // and it fails as UNREADABLE, not as a miss — the key is indexed.
        let r3 = cold_read_through_outcome(&cold_index, shard_dir, b"cachekey", 0);
        assert!(
            matches!(
                r3,
                ColdReadOutcome::Unreadable(ColdReadFault {
                    reason: ColdReadFaultReason::FileMissing,
                    ..
                })
            ),
            "uncached read against the moved-away file must be Unreadable(FileMissing) \
             (test premise check)"
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
            entry.set_expires_at_ms(ttl);
        }
        spill_to_datafile(
            shard_dir,
            40,
            key,
            &entry,
            0,
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
        // task #59: `db.get()` now routes through the bounded off-thread
        // pool; a concurrently-running injected-delay test elsewhere could
        // otherwise starve this read past the pool's timeout and turn the
        // expiry-reclaim path into a plain (non-reclaiming) timeout Miss.
        let _guard = TEST_DELAY_LOCK.lock().unwrap();
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

    /// moon#1013: both cold-tier expiry reclaims — lazy (on read) and the
    /// periodic `sweep_expired` — invalidate CLIENT TRACKING caches, like a
    /// hot-plane expiry. A tracked key can be spilled and then expire on disk.
    #[test]
    fn cold_expiry_invalidates_tracking_clients() {
        use crate::tracking::invalidation::test_support::GlobalTracker;
        let _guard = TEST_DELAY_LOCK.lock().unwrap();

        let on_read = GlobalTracker::tracking(b"cold1013:read");
        let tmp = tempfile::tempdir().unwrap();
        let mut db = db_with_spilled_key(tmp.path(), b"cold1013:read", b"old", Some(1));
        assert!(db.get(b"cold1013:read").is_none(), "expired cold reads nil");
        assert_eq!(
            on_read.invalidated_keys(),
            vec![Bytes::from_static(b"cold1013:read")]
        );

        let by_sweep = GlobalTracker::tracking(b"cold1013:sweep");
        let tmp2 = tempfile::tempdir().unwrap();
        let mut db2 = db_with_spilled_key(tmp2.path(), b"cold1013:sweep", b"old", Some(1));
        let stats = db2
            .cold_index
            .as_mut()
            .unwrap()
            .sweep_expired(
                1_001,
                tmp2.path(),
                None,
                crate::storage::tiered::cold_index::MAX_EXPIRED_SWEEP_BATCH,
            )
            .unwrap();
        assert_eq!(stats.entries_reclaimed, 1, "precondition: swept");
        assert_eq!(
            by_sweep.invalidated_keys(),
            vec![Bytes::from_static(b"cold1013:sweep")]
        );
    }

    /// R1 (H-2, proactive reclaim): a cold entry that EXPIRES and is NEVER
    /// re-read must still be reclaimed. The on-read reclaim proven above only
    /// fires when a caller actually issues a `GET` — a TTL'd key that expires
    /// and is never touched again (the exact shape of the flagship offload
    /// use case: sessions, caches) previously leaked its index entry (RAM,
    /// full key bytes) and its backing file (disk) forever, because nothing
    /// else in the system ever inspected a cold entry's TTL except a read
    /// that never comes.
    ///
    /// Exercises the REAL production insert path (`spill_to_datafile` via
    /// `db_with_spilled_key`, same helper the on-read test above uses) rather
    /// than hand-rolling a `ColdLocation`, so it proves the full plumbing:
    /// `Entry::has_expiry`/`expires_at_ms` -> spill flags -> `ColdLocation
    /// ::ttl_ms` -> `ColdIndex::sweep_expired`.
    #[test]
    fn test_never_read_expired_cold_entry_reclaimed_by_sweep() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        // TTL 1ms in the past relative to the sweep below. Crucially: this
        // key is NEVER read (no `db.get` call anywhere in this test) — the
        // on-read reclaim path above never has a chance to fire.
        let mut db = db_with_spilled_key(shard_dir, b"never-read", b"leaked-on-r1", Some(1));

        // Precondition: the key is cold-indexed and its file exists on disk
        // (file_id=40 is `db_with_spilled_key`'s fixed spill file id).
        assert!(
            db.cold_index
                .as_ref()
                .unwrap()
                .lookup(b"never-read")
                .is_some(),
            "precondition: key must be cold-indexed before the sweep"
        );
        let file_path = shard_dir.join("data").join("heap-000040.mpf");
        assert!(file_path.exists(), "precondition: spill file must exist");

        // Sweep at a time strictly after expiry, WITHOUT ever reading the key.
        // The `Some(1)` passed to `db_with_spilled_key` above round-trips
        // through the on-disk `KvEntry` as an absolute `ttl_ms` of 1 (exact
        // since W3 ms fidelity) — sweeping at 1_001 is strictly after it.
        let stats = db
            .cold_index
            .as_mut()
            .unwrap()
            .sweep_expired(
                1_001,
                shard_dir,
                None,
                crate::storage::tiered::cold_index::MAX_EXPIRED_SWEEP_BATCH,
            )
            .unwrap();

        assert_eq!(
            stats.entries_reclaimed, 1,
            "sweep must reclaim the never-read expired entry"
        );
        assert!(
            stats.bytes_reclaimed > 0,
            "sweep must reclaim the backing file's bytes (last live ref)"
        );
        assert!(
            db.cold_index
                .as_ref()
                .unwrap()
                .lookup(b"never-read")
                .is_none(),
            "index entry must be gone after sweep — this is the R1 leak fix"
        );
        assert!(
            !file_path.exists(),
            "backing DataFile must be unlinked after the sweep — this is the R1 disk leak fix"
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
            0,
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

    // =======================================================================
    // moon#875: indexed-but-unreadable is NOT a miss.
    // =======================================================================

    fn heap_path_of(shard_dir: &std::path::Path, file_id: u64) -> std::path::PathBuf {
        shard_dir
            .join("data")
            .join(format!("heap-{file_id:06}.mpf"))
    }

    #[test]
    fn unindexed_key_is_a_miss_but_a_missing_file_is_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let db = db_with_spilled_key(shard_dir, b"k875", b"value", None);
        let ci = db.cold_index.as_ref().unwrap();

        assert!(
            matches!(
                cold_read_through_outcome(ci, shard_dir, b"never-written", 0),
                ColdReadOutcome::Miss
            ),
            "no index entry is the ONLY thing that means absent"
        );
        assert!(matches!(
            cold_read_through_outcome(ci, shard_dir, b"k875", 0),
            ColdReadOutcome::Hit(..)
        ));

        std::fs::remove_file(heap_path_of(shard_dir, 40)).unwrap();
        let before = crate::command::info_reclamation::RECL_COLD_READ_UNREADABLE_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed);
        let r = cold_read_through_outcome(ci, shard_dir, b"k875", 0);
        let after = crate::command::info_reclamation::RECL_COLD_READ_UNREADABLE_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            matches!(
                r,
                ColdReadOutcome::Unreadable(ColdReadFault {
                    reason: ColdReadFaultReason::FileMissing,
                    location,
                }) if location.file_id == 40
            ),
            "indexed key whose file is gone must be Unreadable(FileMissing) with its location"
        );
        assert!(after > before, "the fault must be counted for INFO");
    }

    #[test]
    fn corrupt_page_is_unreadable_with_page_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let db = db_with_spilled_key(shard_dir, b"crc", b"value", None);
        let ci = db.cold_index.as_ref().unwrap();
        let p = heap_path_of(shard_dir, 40);
        let mut bytes = std::fs::read(&p).unwrap();
        bytes[64 + 1] ^= 0xFF;
        std::fs::write(&p, &bytes).unwrap();
        assert!(matches!(
            cold_read_through_outcome(ci, shard_dir, b"crc", 0),
            ColdReadOutcome::Unreadable(ColdReadFault {
                reason: ColdReadFaultReason::PageRejected,
                ..
            })
        ));
    }

    /// The promoting path: an unreadable entry promotes nothing, fabricates
    /// nothing, and — the load-bearing part — leaves the index entry in place
    /// so a later read retries and the orphan sweep cannot reclaim the file.
    #[test]
    fn promote_keeps_the_index_entry_when_the_bytes_are_unreadable() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut db = db_with_spilled_key(shard_dir, b"stuck", b"value", None);
        let p = heap_path_of(shard_dir, 40);
        std::fs::remove_file(&p).unwrap();

        assert!(!db.promote_cold_if_present(b"stuck", 0));
        assert!(!db.is_hot(b"stuck"), "nothing may be fabricated in hot RAM");
        assert!(
            db.cold_index.as_ref().unwrap().lookup(b"stuck").is_some(),
            "the index entry must survive an unreadable read"
        );
        assert!(
            db.exists_if_alive(b"stuck", 0),
            "EXISTS still sees the indexed key — it is not absent"
        );
    }

    // =======================================================================
    // moon#875: at the wire, indexed-but-unreadable answers -IOERR, not nil.
    // =======================================================================

    fn args(parts: &[&str]) -> Vec<crate::protocol::Frame> {
        parts
            .iter()
            .map(|p| crate::protocol::Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect()
    }

    fn run(
        db: &mut crate::storage::db::Database,
        cmd: &str,
        parts: &[&str],
    ) -> crate::protocol::Frame {
        let mut selected = 0usize;
        match crate::command::dispatch(db, cmd.as_bytes(), &args(parts), &mut selected, 16) {
            crate::command::DispatchResult::Response(f)
            | crate::command::DispatchResult::Quit(f) => f,
        }
    }

    fn run_read(
        db: &crate::storage::db::Database,
        cmd: &str,
        parts: &[&str],
    ) -> crate::protocol::Frame {
        let mut selected = 0usize;
        match crate::command::dispatch_read(db, cmd.as_bytes(), &args(parts), 0, &mut selected, 16)
        {
            crate::command::DispatchResult::Response(f)
            | crate::command::DispatchResult::Quit(f) => f,
        }
    }

    fn is_ioerr(f: &crate::protocol::Frame) -> bool {
        matches!(f, crate::protocol::Frame::Error(e) if e.starts_with(b"IOERR cold tier"))
    }

    #[test]
    fn indexed_but_unreadable_key_answers_ioerr_and_writes_refuse_before_mutating() {
        use crate::protocol::Frame;
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut db = db_with_spilled_key(shard_dir, b"k875", b"the-cold-value", None);
        let heap = heap_path_of(shard_dir, 40);
        let parked = shard_dir.join("parked.mpf");
        std::fs::rename(&heap, &parked).unwrap();

        // Reads: an ERROR, never nil — on both dispatch paths.
        assert!(is_ioerr(&run(&mut db, "GET", &["k875"])), "exclusive GET");
        assert!(
            is_ioerr(&run_read(&db, "GET", &["k875"])),
            "shared-read GET"
        );
        assert!(is_ioerr(&run(&mut db, "STRLEN", &["k875"])), "STRLEN");
        // Still indexed: the key is not absent.
        assert_eq!(run(&mut db, "EXISTS", &["k875"]), Frame::Integer(1));
        // The flag never outlives the command that raised it.
        assert_eq!(
            run(&mut db, "PING", &[]),
            Frame::SimpleString(Bytes::from_static(b"PONG"))
        );
        // A genuinely absent key is still a plain miss.
        assert_eq!(run(&mut db, "GET", &["never-written"]), Frame::Null);

        // Writes that depend on or would shadow the old value: refused, and
        // nothing is fabricated in hot RAM.
        for (cmd, parts) in [
            ("INCR", vec!["k875"]),
            ("INCRBYFLOAT", vec!["k875", "1.5"]),
            ("APPEND", vec!["k875", "x"]),
            ("SETRANGE", vec!["k875", "0", "x"]),
            ("GETSET", vec!["k875", "x"]),
            ("HSET", vec!["k875", "f", "v"]),
            ("LPUSH", vec!["k875", "x"]),
            ("SADD", vec!["k875", "x"]),
            ("ZADD", vec!["k875", "1", "x"]),
            // Streams have no compact sibling: XADD is the one command that
            // reaches the generic `get_or_create::<K>` site directly (the
            // four above take the listpack/intset siblings first).
            ("XADD", vec!["k875", "*", "f", "v"]),
            ("SET", vec!["k875", "x", "GET"]),
            ("SET", vec!["k875", "x", "KEEPTTL"]),
        ] {
            let f = run(&mut db, cmd, &parts);
            assert!(is_ioerr(&f), "{cmd} must answer IOERR, got {f:?}");
            assert!(!db.is_hot(b"k875"), "{cmd} must not fabricate a hot value");
            assert!(
                db.cold_index.as_ref().unwrap().lookup(b"k875").is_some(),
                "{cmd} must leave the index entry in place"
            );
        }

        // The bytes come back: the retained index entry heals the read.
        std::fs::rename(&parked, &heap).unwrap();
        assert_eq!(
            run(&mut db, "GET", &["k875"]),
            Frame::BulkString(Bytes::from_static(b"the-cold-value")),
            "once readable again the value is served — nothing was tombstoned or shadowed"
        );
    }

    #[test]
    fn plain_set_and_del_remain_the_escape_hatches() {
        use crate::protocol::Frame;
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path();
        let mut db = db_with_spilled_key(shard_dir, b"k875", b"the-cold-value", None);
        std::fs::remove_file(heap_path_of(shard_dir, 40)).unwrap();
        assert!(is_ioerr(&run(&mut db, "GET", &["k875"])));

        // A plain SET does not read the old value: it overwrites, as in Redis.
        assert_eq!(
            run(&mut db, "SET", &["k875", "fresh"]),
            Frame::SimpleString(Bytes::from_static(b"OK"))
        );
        assert_eq!(
            run(&mut db, "GET", &["k875"]),
            Frame::BulkString(Bytes::from_static(b"fresh"))
        );

        // DEL discards the unreadable entry; afterwards the key is absent.
        let mut db2 = db_with_spilled_key(tmp.path(), b"k876", b"v", None);
        std::fs::remove_file(heap_path_of(tmp.path(), 40)).unwrap();
        assert!(is_ioerr(&run(&mut db2, "GET", &["k876"])));
        assert_eq!(run(&mut db2, "DEL", &["k876"]), Frame::Integer(1));
        assert_eq!(run(&mut db2, "GET", &["k876"]), Frame::Null);
        assert_eq!(run(&mut db2, "EXISTS", &["k876"]), Frame::Integer(0));
    }
}
