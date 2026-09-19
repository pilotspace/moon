//! moon#875: nothing the cold-index rebuild cannot read may vanish silently.
//!
//! Every loss class of [`ColdRebuildReport`] is produced here from a real
//! spill corpus (`build_kv_spill_batch` + `write_kv_spill_batch`, the exact
//! writers the spill thread uses) damaged the way a disk damages it, and
//! each is asserted to be counted, logged-for, and to leave the readable
//! remainder intact. Lives beside `cold_index.rs` because that file is
//! already past the 1500-line cap.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::{MoonPageHeader, PAGE_4K, PageType};
use crate::storage::tiered::cold_index::ColdIndex;
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};

fn heap_path(shard_dir: &Path, file_id: u64) -> std::path::PathBuf {
    shard_dir
        .join("data")
        .join(format!("heap-{file_id:06}.mpf"))
}

fn entry(k: &str, v: &str) -> SpillEntry {
    SpillEntry {
        key: Bytes::copy_from_slice(k.as_bytes()),
        value_bytes: Bytes::copy_from_slice(v.as_bytes()),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
    }
}

/// Write `files` as real spill batches and register them Active.
/// Returns the manifest, still open, so a test can add more.
fn corpus(shard_dir: &Path, files: &[(u64, usize, Vec<SpillEntry>)]) -> ShardManifest {
    let manifest_path = shard_dir.join("shard.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();
    for (file_id, db, entries) in files {
        let batch = build_kv_spill_batch(entries, *file_id).unwrap();
        let byte_size = write_kv_spill_batch(shard_dir, *file_id, &batch).unwrap();
        manifest.add_file(FileEntry {
            file_id: *file_id,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: batch.pages.len() as u32,
            byte_size,
            created_lsn: 0,
            db_index: *db as u64,
            max_key_hash: 0,
            last_modified_lsn: 0,
        });
    }
    manifest.commit().unwrap();
    manifest
}

/// Enough ~200 B entries to need more than one leaf page.
fn two_page_entries(prefix: &str) -> Vec<SpillEntry> {
    (0..40)
        .map(|i| entry(&format!("{prefix}:{i:03}"), &"v".repeat(200)))
        .collect()
}

fn page_of(bytes: &[u8], key: &str) -> usize {
    bytes
        .windows(key.len())
        .position(|w| w == key.as_bytes())
        .expect("key is in the file")
        / PAGE_4K
}

#[test]
fn clean_corpus_reports_everything_read_and_nothing_lost() {
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(
        tmp.path(),
        &[
            (1, 0, vec![entry("a", "1"), entry("b", "2")]),
            (2, 0, vec![entry("c", "3")]),
        ],
    );
    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert_eq!(r.report.files_attempted, 2);
    assert_eq!(r.report.files_read, 2);
    assert_eq!(r.report.pages_scanned, 2);
    assert_eq!(r.report.entries_recovered, 3);
    assert_eq!(r.report.pages_overflow, 0);
    assert!(!r.report.is_degraded(), "{:?}", r.report);
    assert_eq!(r.per_db.len(), 1);
    assert_eq!(r.per_db[0].1.len(), 3);
}

#[test]
fn missing_file_is_counted_and_queued_for_manifest_retirement() {
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(
        tmp.path(),
        &[
            (1, 0, vec![entry("kept", "1")]),
            (2, 0, vec![entry("gone", "2")]),
        ],
    );
    std::fs::remove_file(heap_path(tmp.path(), 2)).unwrap();
    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert_eq!(r.report.files_attempted, 2);
    assert_eq!(r.report.files_read, 1);
    assert_eq!(r.report.files_missing, 1);
    assert_eq!(r.report.files_unreadable, 0);
    assert!(r.report.is_degraded());
    let index = &r.per_db[0].1;
    assert!(index.lookup(b"kept").is_some());
    assert!(index.lookup(b"gone").is_none(), "its bytes are gone");
    assert_eq!(
        index.pending_unlink_len(),
        1,
        "the missing file must be queued so the sweep retires its manifest entry \
         instead of re-warning on every boot"
    );
}

#[test]
fn a_db_whose_only_file_is_missing_still_gets_an_index_to_carry_the_queue() {
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(
        tmp.path(),
        &[
            (1, 0, vec![entry("db0", "1")]),
            (2, 3, vec![entry("db3", "2")]),
        ],
    );
    std::fs::remove_file(heap_path(tmp.path(), 2)).unwrap();
    let mut r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    r.per_db.sort_by_key(|(db, _)| *db);
    let dbs: Vec<usize> = r.per_db.iter().map(|(db, _)| *db).collect();
    assert_eq!(dbs, vec![0, 3]);
    assert_eq!(r.per_db[1].1.len(), 0);
    assert_eq!(r.per_db[1].1.pending_unlink_len(), 1);
    assert_eq!(r.per_db[0].1.pending_unlink_len(), 0, "the queue is per db");
}

#[cfg(unix)]
#[test]
fn unreadable_file_is_counted_and_skipped_never_queued_for_unlink() {
    use std::os::unix::fs::PermissionsExt;
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(
        tmp.path(),
        &[
            (1, 0, vec![entry("kept", "1")]),
            (2, 0, vec![entry("locked", "2")]),
        ],
    );
    let p = heap_path(tmp.path(), 2);
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o000)).unwrap();
    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(r.report.files_unreadable, 1, "{:?}", r.report);
    assert_eq!(r.report.files_missing, 0);
    assert!(r.report.is_degraded());
    let index = &r.per_db[0].1;
    assert!(index.lookup(b"kept").is_some());
    assert!(index.lookup(b"locked").is_none());
    assert_eq!(
        index.pending_unlink_len(),
        0,
        "an unreadable file still holds data — it must never be queued for unlink"
    );
    // Readable again: the same manifest recovers the key. Nothing was
    // tombstoned.
    let r2 = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert!(!r2.report.is_degraded());
    assert!(r2.per_db[0].1.lookup(b"locked").is_some());
}

#[test]
fn rejected_page_is_counted_and_the_other_pages_survive() {
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(tmp.path(), &[(7, 0, two_page_entries("k"))]);
    let p = heap_path(tmp.path(), 7);
    let mut bytes = std::fs::read(&p).unwrap();
    assert!(bytes.len() >= 2 * PAGE_4K, "precondition: two leaf pages");
    let victim_page = page_of(&bytes, "k:000");
    bytes[victim_page * PAGE_4K + 64 + 1] ^= 0xFF; // payload byte: CRC now wrong
    std::fs::write(&p, &bytes).unwrap();

    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert_eq!(r.report.pages_rejected, 1, "{:?}", r.report);
    assert_eq!(r.report.files_read, 1);
    assert!(r.report.is_degraded());
    let index = &r.per_db[0].1;
    assert!(index.lookup(b"k:000").is_none(), "its page was rejected");
    assert!(
        index.lookup(b"k:039").is_some(),
        "the other page's keys must still be recovered"
    );
    assert_eq!(
        r.report.entries_recovered as usize,
        index.len(),
        "no duplicates in this corpus"
    );
}

#[test]
fn trailing_partial_page_is_counted_in_bytes() {
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(tmp.path(), &[(3, 0, vec![entry("a", "1")])]);
    let p = heap_path(tmp.path(), 3);
    let mut bytes = std::fs::read(&p).unwrap();
    bytes.extend_from_slice(&[0xEEu8; 100]);
    std::fs::write(&p, &bytes).unwrap();
    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert_eq!(r.report.partial_page_bytes, 100, "{:?}", r.report);
    assert_eq!(r.report.files_short, 0, "longer, not shorter");
    assert!(r.report.is_degraded());
    assert!(
        r.per_db[0].1.lookup(b"a").is_some(),
        "the whole page still reads"
    );
}

#[test]
fn file_truncated_at_a_page_boundary_is_caught_by_the_manifest_size() {
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(tmp.path(), &[(4, 0, two_page_entries("t"))]);
    let p = heap_path(tmp.path(), 4);
    let bytes = std::fs::read(&p).unwrap();
    let pages = bytes.len() / PAGE_4K;
    assert!(pages >= 2);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&p)
        .unwrap()
        .set_len(((pages - 1) * PAGE_4K) as u64)
        .unwrap();
    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert_eq!(r.report.files_short, 1, "{:?}", r.report);
    assert_eq!(r.report.short_file_bytes, PAGE_4K as u64);
    assert_eq!(r.report.partial_page_bytes, 0, "cut on a page boundary");
    assert_eq!(r.report.pages_rejected, 0);
    assert!(r.report.is_degraded());
    assert!(
        (r.per_db[0].1.len() as u64) < 40,
        "the lost page's keys are gone from the index"
    );
}

#[test]
fn overflow_pages_are_expected_not_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let big = "B".repeat(10 * 1024);
    let m = corpus(
        tmp.path(),
        &[(5, 0, vec![entry("small", "s"), entry("big", &big)])],
    );
    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert!(r.report.pages_overflow >= 3, "{:?}", r.report);
    assert_eq!(r.report.pages_rejected, 0);
    assert!(!r.report.is_degraded(), "{:?}", r.report);
    assert_eq!(r.per_db[0].1.len(), 2);
}

#[test]
fn undecodable_slot_inside_a_valid_page_is_counted() {
    let tmp = tempfile::tempdir().unwrap();
    let m = corpus(
        tmp.path(),
        &[(6, 0, vec![entry("fine", "1"), entry("broken-type", "2")])],
    );
    let p = heap_path(tmp.path(), 6);
    let mut bytes = std::fs::read(&p).unwrap();
    // Entry layout: key_len u16 | value_type u8 | flags u8 | key | value
    // (no TTL). The value_type byte sits two before the key.
    let key_off = bytes.windows(11).position(|w| w == b"broken-type").unwrap();
    bytes[key_off - 2] = 0xEE; // no such ValueType
    let page_start = (key_off / PAGE_4K) * PAGE_4K;
    MoonPageHeader::compute_checksum(&mut bytes[page_start..page_start + PAGE_4K]);
    std::fs::write(&p, &bytes).unwrap();

    let r = ColdIndex::rebuild_from_manifest_per_db(tmp.path(), &m);
    assert_eq!(r.report.pages_rejected, 0, "the page itself is valid");
    assert_eq!(r.report.entries_rejected, 1, "{:?}", r.report);
    assert_eq!(r.report.entries_recovered, 1);
    assert!(r.report.is_degraded());
    let index = &r.per_db[0].1;
    assert!(index.lookup(b"fine").is_some());
    assert!(index.lookup(b"broken-type").is_none());
}
