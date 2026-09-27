//! Adversarial review of moon#1281 (WS22 review 1). Runtime-independent.
//!
//! - `review_graves_of_an_unreadable_listed_file_survive_the_rebuild`: RED on
//!   5b5593d. The boot rebuild re-records only the grave slots it actually
//!   scanned. A listed file that could not be READ at this boot (EIO, EACCES,
//!   a remount — the rebuild says "until the file is readable and the server
//!   restarts") keeps its manifest entry, but its graves are dropped from the
//!   rebuilt index, so the next snapshot's trailer no longer names them. The
//!   boot after the file is readable again resurrects every deleted key in it.
//! - `review_trailer_build_cost_at_1m_graves` (ignored, prints timings): the
//!   synchronous cost `note_snapshot_started` pays on the shard thread.

use std::path::Path;

use bytes::Bytes;

use crate::persistence::kv_page::ValueType;
use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::persistence::snapshot::cold_graves::{self, ColdGraves};
use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};
use crate::storage::tiered::kv_spill::{SpillEntry, build_kv_spill_batch, write_kv_spill_batch};
use crate::storage::tiered::slot_graves;

fn entry(k: &str, v: &str) -> SpillEntry {
    SpillEntry {
        key: Bytes::copy_from_slice(k.as_bytes()),
        value_bytes: Bytes::copy_from_slice(v.as_bytes()),
        value_type: ValueType::String,
        flags: 0,
        ttl_ms: None,
    }
}

fn corpus(shard_dir: &Path, files: &[(u64, Vec<SpillEntry>)]) -> ShardManifest {
    let mut manifest = ShardManifest::create(&shard_dir.join("shard.manifest")).unwrap();
    for (file_id, entries) in files {
        let batch = build_kv_spill_batch(entries, *file_id).unwrap();
        let byte_size = write_kv_spill_batch(shard_dir, *file_id, &batch).unwrap();
        manifest
            .add_file(FileEntry {
                file_id: *file_id,
                file_type: PageType::KvLeaf as u8,
                status: FileStatus::Active,
                tier: StorageTier::Hot,
                page_size_log2: 12,
                page_count: batch.pages.len() as u32,
                byte_size,
                created_lsn: 0,
                db_index: 0,
                max_key_hash: 0,
                last_modified_lsn: 0,
            })
            .unwrap();
    }
    manifest.commit().unwrap();
    manifest
}

/// What the next snapshot started by this boot would carry: every rebuilt
/// index's graves, encoded then decoded exactly as the trailer round trip.
fn next_trailer(rebuilt: &crate::storage::tiered::cold_index::ColdRebuild) -> Option<ColdGraves> {
    let mut files = Vec::new();
    for (_, ci) in &rebuilt.per_db {
        ci.collect_graves_into(&mut files);
    }
    let bytes = cold_graves::encode(&files);
    if bytes.is_empty() {
        None
    } else {
        Some(cold_graves::decode(&bytes).unwrap())
    }
}

fn lookup(rebuilt: crate::storage::tiered::cold_index::ColdRebuild, key: &[u8]) -> bool {
    rebuilt
        .per_db
        .into_iter()
        .any(|(_, ci)| ci.lookup(key).is_some())
}

#[test]
fn review_graves_of_an_unreadable_listed_file_survive_the_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let manifest = corpus(dir, &[(1, vec![entry("live", "a"), entry("dead", "b")])]);

    // Boot 0: the key "dead" was DELeted and a snapshot recorded its slot.
    let plain = ColdIndex::rebuild_from_manifest_per_db(dir, &manifest);
    let dead_at: ColdLocation = plain
        .per_db
        .iter()
        .find_map(|(_, ci)| ci.lookup(b"dead"))
        .unwrap();
    let mut boot1_trailer = ColdGraves::default();
    boot1_trailer.insert(dead_at.file_id, dead_at.page_idx, dead_at.slot_idx);

    // Boot 1: the listed file exists but cannot be read (a directory in its
    // place gives EISDIR — the test runs as root, where chmod 000 is no bar).
    let heap = dir.join("data").join("heap-000001.mpf");
    let aside = dir.join("aside.mpf");
    std::fs::rename(&heap, &aside).unwrap();
    std::fs::create_dir(&heap).unwrap();
    let boot1 =
        ColdIndex::rebuild_from_manifest_per_db_with_graves(dir, &manifest, Some(&boot1_trailer));
    assert_eq!(
        boot1.report.files_unreadable, 1,
        "precondition: the file is unreadable, not missing"
    );
    // Boot 1 then takes a snapshot (BGSAVE / save point) — its trailer:
    let boot2_trailer = next_trailer(&boot1);

    // The I/O error is fixed; boot 2 loads that snapshot.
    std::fs::remove_dir(&heap).unwrap();
    std::fs::rename(&aside, &heap).unwrap();
    let boot2 =
        ColdIndex::rebuild_from_manifest_per_db_with_graves(dir, &manifest, boot2_trailer.as_ref());
    assert!(
        !lookup(boot2, b"dead"),
        "moon#1281 review: a key DELeted before a successful snapshot came back — its grave \
         was dropped by a boot that could not read its (still listed) file, so the next \
         snapshot's trailer {} it",
        if boot2_trailer.is_some() {
            "did not name"
        } else {
            "was empty and did not carry"
        }
    );
}

/// Not an assertion: the shard-thread stall `note_snapshot_started` pays to
/// build the trailer (collect: a `Vec<u64>` clone per file; encode) at 1M
/// dead slots spread 255 per file (one survivor per 256-entry spill batch —
/// the no-AOF steady state under delete churn: nothing compacts without an
/// AOF). Run with `--ignored --nocapture`.
#[test]
#[ignore]
fn review_trailer_build_cost_at_1m_graves() {
    slot_graves::force_enabled(Some(true));
    for total in [100_000u64, 1_000_000, 4_000_000] {
        let mut ci = ColdIndex::new();
        let files = total / 255;
        for f in 0..files {
            for s in 0..255u16 {
                let loc = ColdLocation {
                    file_id: f + 1,
                    page_idx: u32::from(s / 16),
                    slot_idx: s % 16,
                    ttl_ms: None,
                    value_type: ValueType::String,
                };
                ci.note_dead_slot(Bytes::new(), loc);
            }
        }
        let n = ci.slot_graves().len();
        let mut best = std::time::Duration::MAX;
        let mut bytes = 0;
        for _ in 0..5 {
            let t = std::time::Instant::now();
            let mut out = Vec::new();
            ci.collect_graves_into(&mut out);
            let enc = cold_graves::encode(&out);
            bytes = enc.len();
            std::hint::black_box(&enc);
            best = best.min(t.elapsed());
        }
        let t = std::time::Instant::now();
        let mut out = Vec::new();
        ci.collect_graves_into(&mut out);
        let dec = cold_graves::decode(&cold_graves::encode(&out)).unwrap();
        let dec_t = t.elapsed();
        eprintln!(
            "graves={n} files={files}: collect+encode best {:?} ({} B trailer, {} B RAM \
             by resident_bytes()); encode+decode(boot) {:?}, decoded {}",
            best,
            bytes,
            ci.slot_graves().resident_bytes(),
            dec_t,
            dec.len()
        );
    }
    slot_graves::force_enabled(None);
}

/// The `snapshot_cold_graves` fuzz target's body, verbatim: decode, and if it
/// accepts, `decode(encode(to_files()))` must succeed and be equal. A valid
/// trailer that names no slot (file_count 0, or every file with slot_count 0)
/// decodes to EMPTY graves; `encode` of nothing is zero bytes (by design: no
/// trailer), and `decode(&[])` is `Err(Truncated)` — so the target's
/// `.expect("an encoded trailer decodes")` panics: a guaranteed false crash
/// once libFuzzer solves the 4-byte CRC compare (CMP tracing does).
#[test]
fn review_fuzz_target_round_trip_panics_on_an_empty_valid_trailer() {
    let fuzz_body = |data: &[u8]| {
        if let Ok(graves) = cold_graves::decode(data) {
            let files = graves.to_files();
            let again = cold_graves::decode(&cold_graves::encode(&files))
                .expect("an encoded trailer decodes");
            assert_eq!(again, graves, "round trip changed the graves");
        }
    };
    let mut body = Vec::new();
    body.extend_from_slice(b"MCGV");
    body.push(1);
    body.extend_from_slice(&0u32.to_le_bytes());
    let crc = crc32fast::hash(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    assert!(
        cold_graves::decode(&body).is_ok_and(|g| g.is_empty()),
        "precondition: an empty trailer is accepted"
    );
    let crashed = std::panic::catch_unwind(|| fuzz_body(&body)).is_err();
    assert!(
        !crashed,
        "the snapshot_cold_graves fuzz target panics on a 13-byte valid empty trailer"
    );
}
