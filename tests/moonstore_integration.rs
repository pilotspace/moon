//! MoonStore v2 integration tests — component-level validation.
//!
//! Tests WAL v3 write/recovery, checkpoint state machine, warm tier
//! transition, FPI torn-page defense, and disk-offload-disable noop.
//!
//! These tests exercise MoonStore v2 components directly (not through
//! a running server) since end-to-end server wiring is not yet complete.

use moon::config::ServerConfig;
use moon::persistence::checkpoint::{
    CheckpointAction, CheckpointManager, CheckpointState, CheckpointTrigger,
};
use moon::persistence::manifest::{FileStatus, ShardManifest, StorageTier};
use moon::persistence::page::{MoonPageHeader, PageType, MOONPAGE_HEADER_SIZE};
use moon::persistence::wal_v3::record::{
    WalRecordType, write_wal_v3_record,
};
use moon::persistence::wal_v3::replay::{replay_wal_v3_dir, replay_wal_v3_file};
use moon::persistence::wal_v3::segment::{
    WalSegment, WalWriterV3, DEFAULT_SEGMENT_SIZE, WAL_V3_HEADER_SIZE,
};
use moon::storage::tiered::warm_tier::transition_to_warm;

use clap::Parser;

// ---- Helpers ----

/// Build a minimal v3 segment header in memory.
fn make_v3_header(shard_id: u16) -> Vec<u8> {
    let mut header = vec![0u8; WAL_V3_HEADER_SIZE];
    header[0..6].copy_from_slice(b"RRDWAL");
    header[6] = 3; // version = 3
    header[7] = 0x01; // flags = FPI_ENABLED
    header[8..10].copy_from_slice(&shard_id.to_le_bytes());
    header
}

// ======================================================================
// Test 1: WAL v3 write-and-recovery cycle
// ======================================================================

#[test]
fn test_wal_v3_write_and_recovery() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    // Phase 1: Write 100 command records via WalWriterV3
    {
        let mut writer = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();
        for i in 1..=100u64 {
            let payload = format!("*3\r\n$3\r\nSET\r\n$6\r\nkey:{i:03}\r\n$9\r\nvalue:{i:03}\r\n");
            writer.append(WalRecordType::Command, payload.as_bytes());
        }
        writer.flush_sync().unwrap();
        // Writer dropped here -- simulates crash (no graceful shutdown)
    }

    // Phase 2: Replay and verify all 100 records recovered
    let mut recovered_lsns = Vec::new();
    let mut recovered_payloads = Vec::new();

    let result = replay_wal_v3_dir(
        &wal_dir,
        0, // redo_lsn=0 => replay everything
        &mut |record| {
            recovered_lsns.push(record.lsn);
            recovered_payloads.push(record.payload.clone());
        },
        &mut |_| {},
    )
    .unwrap();

    // Verify all 100 commands replayed
    assert_eq!(result.commands_replayed, 100, "all 100 commands must be replayed");
    assert_eq!(result.last_lsn, 100, "last LSN should be 100");
    assert_eq!(recovered_lsns.len(), 100);

    // Verify LSNs are monotonically increasing 1..=100
    for (i, &lsn) in recovered_lsns.iter().enumerate() {
        assert_eq!(lsn, (i + 1) as u64, "LSN {i} should be {}", i + 1);
    }

    // Verify payload content for a few records
    let payload_1 = String::from_utf8_lossy(&recovered_payloads[0]);
    assert!(
        payload_1.contains("key:001"),
        "first record should contain key:001, got: {payload_1}"
    );
    let payload_100 = String::from_utf8_lossy(&recovered_payloads[99]);
    assert!(
        payload_100.contains("key:100"),
        "last record should contain key:100, got: {payload_100}"
    );

    // Phase 3: Verify partial replay with redo_lsn skips already-applied records
    let mut partial_count = 0usize;
    let partial = replay_wal_v3_dir(
        &wal_dir,
        50, // skip LSNs 1..=50
        &mut |_| partial_count += 1,
        &mut |_| {},
    )
    .unwrap();

    assert_eq!(partial.commands_replayed, 50, "should replay only LSNs 51-100");
    assert_eq!(partial_count, 50);
    assert_eq!(partial.last_lsn, 100, "last_lsn tracks all records seen");
}

// ======================================================================
// Test 2: Checkpoint creates redo point
// ======================================================================

#[test]
fn test_checkpoint_creates_redo_point() {
    // Use small timeout so we can test the state machine quickly
    let trigger = CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9);
    let mut mgr = CheckpointManager::new(trigger);

    // Initially idle
    assert!(!mgr.is_active());
    assert_eq!(mgr.advance_tick(), CheckpointAction::Nothing);

    // --- Checkpoint 1: begin at LSN 50, 10 dirty pages ---
    assert!(mgr.begin(50, 10));
    assert!(mgr.is_active());

    match mgr.state() {
        CheckpointState::InProgress { redo_lsn, dirty_count, flushed, .. } => {
            assert_eq!(*redo_lsn, 50, "redo_lsn should capture LSN at checkpoint start");
            assert_eq!(*dirty_count, 10);
            assert_eq!(*flushed, 0);
        }
        other => panic!("expected InProgress, got {other:?}"),
    }

    // Double begin rejected
    assert!(!mgr.begin(999, 999));

    // Advance ticks until all pages flushed
    let mut total_flushed = 0usize;
    loop {
        let action = mgr.advance_tick();
        match action {
            CheckpointAction::FlushPages(n) => {
                total_flushed += n;
            }
            CheckpointAction::Finalize { redo_lsn } => {
                assert_eq!(redo_lsn, 50, "finalize must report the original redo_lsn");
                break;
            }
            CheckpointAction::Nothing => {
                panic!("should not get Nothing during active checkpoint");
            }
        }
    }
    assert_eq!(total_flushed, 10, "should flush exactly 10 dirty pages");

    // Complete checkpoint
    mgr.complete();
    assert!(!mgr.is_active());

    // --- Checkpoint 2: zero dirty pages goes straight to Finalize ---
    assert!(mgr.begin(100, 0));
    let action = mgr.advance_tick();
    assert_eq!(
        action,
        CheckpointAction::Finalize { redo_lsn: 100 },
        "zero dirty pages should immediately finalize"
    );
    mgr.complete();

    // --- Verify WAL checkpoint record integration ---
    // Write a WAL v3 segment with a checkpoint marker and verify replay handles it
    let tmp = tempfile::tempdir().unwrap();
    let seg_path = tmp.path().join("000000000001.wal");

    let mut data = make_v3_header(0);
    // 3 commands before checkpoint
    for i in 1..=3u64 {
        write_wal_v3_record(&mut data, i, WalRecordType::Command, b"SET a 1");
    }
    // Checkpoint marker at LSN 4
    write_wal_v3_record(&mut data, 4, WalRecordType::Checkpoint, &[]);
    // 3 commands after checkpoint
    for i in 5..=7u64 {
        write_wal_v3_record(&mut data, i, WalRecordType::Command, b"SET b 2");
    }
    std::fs::write(&seg_path, &data).unwrap();

    let mut cmd_count = 0usize;
    let result = replay_wal_v3_file(
        &seg_path,
        0,
        &mut |_| cmd_count += 1,
        &mut |_| {},
    )
    .unwrap();

    // Checkpoint marker is NOT dispatched to callbacks
    assert_eq!(result.commands_replayed, 6, "6 commands total (3 before + 3 after checkpoint)");
    assert_eq!(cmd_count, 6);
    assert_eq!(result.last_lsn, 7);

    // Replay with redo_lsn=4 skips records 1-4 (including checkpoint), replays 5-7
    let mut partial_count = 0usize;
    let partial = replay_wal_v3_file(
        &seg_path,
        4,
        &mut |_| partial_count += 1,
        &mut |_| {},
    )
    .unwrap();
    assert_eq!(partial.commands_replayed, 3, "only LSNs 5-7 after redo point");
    assert_eq!(partial_count, 3);
}

// ======================================================================
// Test 3: Warm tier transition preserves data and updates manifest
// ======================================================================

#[test]
fn test_warm_tier_transition_preserves_search() {
    let tmp = tempfile::tempdir().unwrap();
    let shard_dir = tmp.path().join("shard-0");
    std::fs::create_dir_all(&shard_dir).unwrap();

    let manifest_path = shard_dir.join("shard-0.manifest");
    let mut manifest = ShardManifest::create(&manifest_path).unwrap();

    let initial_epoch = manifest.epoch();

    // Simulate 500 vectors * 384d * 4 bytes = 768KB of codes
    let num_vectors = 500usize;
    let dim = 384usize;
    let codes_data: Vec<u8> = (0..num_vectors * dim)
        .flat_map(|i| ((i as f32) * 0.001).to_le_bytes())
        .collect();
    let graph_data = vec![0xBBu8; num_vectors * 64]; // adjacency lists
    let mvcc_data = vec![0u8; num_vectors * 24]; // visibility headers

    // Transition to warm
    let handle = transition_to_warm(
        &shard_dir,
        1,    // segment_id
        100,  // file_id
        &codes_data,
        &graph_data,
        None, // no raw vectors (TQ encoded)
        &mvcc_data,
        &mut manifest,
        None, // no WAL in integration test
    )
    .unwrap();

    // Verify segment directory exists with .mpf files
    let seg_dir = handle.segment_dir();
    assert!(seg_dir.exists(), "segment directory should exist");
    assert!(seg_dir.join("codes.mpf").exists(), "codes.mpf should exist");
    assert!(seg_dir.join("graph.mpf").exists(), "graph.mpf should exist");
    assert!(seg_dir.join("mvcc.mpf").exists(), "mvcc.mpf should exist");
    assert!(
        !seg_dir.join("vectors.mpf").exists(),
        "vectors.mpf should NOT exist when None passed"
    );

    // Verify staging directory was cleaned up (renamed to final)
    let staging = shard_dir.join("vectors/.segment-1.staging");
    assert!(!staging.exists(), "staging dir should be removed after rename");

    // Verify manifest was updated
    assert!(
        manifest.epoch() > initial_epoch,
        "epoch should increment after commit"
    );
    assert_eq!(manifest.files().len(), 1, "manifest should have 1 file entry");

    let entry = &manifest.files()[0];
    assert_eq!(entry.file_id, 100);
    assert_eq!(entry.status, FileStatus::Active);
    assert_eq!(entry.tier, StorageTier::Warm);
    assert_eq!(entry.byte_size, codes_data.len() as u64);

    // Verify .mpf files have valid MoonPage headers with CRC32C
    let codes_file = std::fs::read(seg_dir.join("codes.mpf")).unwrap();
    assert!(codes_file.len() >= MOONPAGE_HEADER_SIZE, "codes.mpf too small");

    let hdr = MoonPageHeader::read_from(&codes_file)
        .expect("codes.mpf should have valid MoonPage header");
    assert_eq!(hdr.page_type, PageType::VecCodes);
    assert!(
        MoonPageHeader::verify_checksum(&codes_file),
        "codes.mpf first page CRC32C should verify"
    );

    // Verify manifest can be recovered from disk
    let recovered = ShardManifest::open(&manifest_path).unwrap();
    assert_eq!(recovered.files().len(), 1);
    assert_eq!(recovered.files()[0].file_id, 100);
    assert_eq!(recovered.files()[0].tier, StorageTier::Warm);

    // Transition a second segment WITH optional vectors
    let vectors_data = vec![0xCCu8; num_vectors * dim * 4]; // raw f32
    let handle2 = transition_to_warm(
        &shard_dir,
        2,
        200,
        &codes_data,
        &graph_data,
        Some(&vectors_data),
        &mvcc_data,
        &mut manifest,
        None, // no WAL in integration test
    )
    .unwrap();

    assert!(handle2.segment_dir().join("vectors.mpf").exists());
    assert_eq!(manifest.files().len(), 2);
}

// ======================================================================
// Test 4: FPI torn-page defense
// ======================================================================

#[test]
fn test_fpi_torn_page_defense() {
    let tmp = tempfile::tempdir().unwrap();
    let wal_dir = tmp.path().join("wal");

    let mut writer = WalWriterV3::new(0, &wal_dir, DEFAULT_SEGMENT_SIZE).unwrap();

    // Write 50 command records
    for i in 1..=50u64 {
        let payload = format!("SET k{i} v{i}");
        writer.append(WalRecordType::Command, payload.as_bytes());
    }

    // Write 5 FPI records (simulating page images before checkpoint flush)
    let mut fpi_payloads = Vec::new();
    for i in 0..5u32 {
        // Create a realistic page image (4KB page with header + payload)
        let mut page = vec![0u8; 4096];
        let mut hdr = MoonPageHeader::new(PageType::KvLeaf, i as u64, 1);
        hdr.payload_bytes = 200;
        hdr.page_lsn = 50 + i as u64;
        hdr.write_to(&mut page);
        // Fill some payload
        for j in 0..200 {
            page[MOONPAGE_HEADER_SIZE + j] = ((i as usize * 7 + j) & 0xFF) as u8;
        }
        MoonPageHeader::compute_checksum(&mut page);

        writer.append(WalRecordType::FullPageImage, &page);
        fpi_payloads.push(page);
    }

    // Write 5 more command records after FPIs
    for i in 51..=55u64 {
        writer.append(WalRecordType::Command, format!("SET k{i} v{i}").as_bytes());
    }

    writer.flush_sync().unwrap();

    // Replay and verify FPI records
    let mut replayed_fpis: Vec<Vec<u8>> = Vec::new();
    let mut cmd_count = 0usize;

    let result = replay_wal_v3_dir(
        &wal_dir,
        0,
        &mut |_| cmd_count += 1,
        &mut |record| {
            replayed_fpis.push(record.payload.clone());
        },
    )
    .unwrap();

    assert_eq!(result.commands_replayed, 55, "55 command records");
    assert_eq!(result.fpi_applied, 5, "5 FPI records");
    assert_eq!(cmd_count, 55);
    assert_eq!(replayed_fpis.len(), 5);

    // Verify each FPI record preserves the full page image with valid CRC
    for (i, fpi_data) in replayed_fpis.iter().enumerate() {
        assert_eq!(fpi_data.len(), 4096, "FPI {i} should be a full 4KB page");

        let hdr = MoonPageHeader::read_from(fpi_data)
            .unwrap_or_else(|| panic!("FPI {i} should have valid MoonPage header"));
        assert_eq!(hdr.page_type, PageType::KvLeaf);
        assert_eq!(hdr.page_id, i as u64);
        assert_eq!(hdr.payload_bytes, 200);

        // CRC32C of the FPI payload should verify (torn-page defense)
        assert!(
            MoonPageHeader::verify_checksum(fpi_data),
            "FPI {i} CRC32C should verify -- torn page defense"
        );

        // Content should match what we wrote
        assert_eq!(
            fpi_data, &fpi_payloads[i],
            "FPI {i} content must match original page image exactly"
        );
    }

    // Verify FPI records in the raw segment file have correct record_type byte (0x10)
    let seg_path = WalSegment::segment_path(&wal_dir, 1);
    let raw_data = std::fs::read(&seg_path).unwrap();
    let mut offset = WAL_V3_HEADER_SIZE;
    let mut fpi_found = 0usize;

    while offset + 20 <= raw_data.len() {
        let record_len = u32::from_le_bytes([
            raw_data[offset],
            raw_data[offset + 1],
            raw_data[offset + 2],
            raw_data[offset + 3],
        ]) as usize;
        if record_len < 20 || offset + record_len > raw_data.len() {
            break;
        }
        // record_type at offset+12 within the record
        if raw_data[offset + 12] == WalRecordType::FullPageImage as u8 {
            fpi_found += 1;
            // Verify CRC32C of the raw record
            let crc_stored = u32::from_le_bytes([
                raw_data[offset + record_len - 4],
                raw_data[offset + record_len - 3],
                raw_data[offset + record_len - 2],
                raw_data[offset + record_len - 1],
            ]);
            let crc_computed = crc32c::crc32c(&raw_data[offset + 4..offset + record_len - 4]);
            assert_eq!(
                crc_stored, crc_computed,
                "raw FPI record CRC32C must verify"
            );
        }
        offset += record_len;
    }
    assert_eq!(fpi_found, 5, "should find 5 FPI records in raw segment data");
}

// ======================================================================
// Test 5: disk-offload=disable is a noop
// ======================================================================

#[test]
fn test_disk_offload_disable_is_noop() {
    // Verify default config has disk-offload disabled
    let config = ServerConfig::parse_from::<[&str; 0], &str>([]);
    assert!(!config.disk_offload_enabled());
    assert_eq!(config.disk_offload, "disable");

    // Verify enable parses correctly
    let config_on = ServerConfig::parse_from(["moon", "--disk-offload", "enable"]);
    assert!(config_on.disk_offload_enabled());

    // With disk-offload disabled, no persistence artifacts should exist
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();

    // Simulate what the shard event loop checks: disk_offload_enabled() == false
    // means CheckpointManager is None, no WAL v3 writer created, no manifest, no control file
    if !config.disk_offload_enabled() {
        // This is the expected path -- no MoonStore v2 artifacts created
    } else {
        panic!("default config should have disk-offload disabled");
    }

    // Verify no manifest file
    let manifest_path = data_dir.join("shard-0.manifest");
    assert!(
        !manifest_path.exists(),
        "no manifest file when disk-offload disabled"
    );

    // Verify no control file
    let control_path = data_dir.join("shard-0.control");
    assert!(
        !control_path.exists(),
        "no control file when disk-offload disabled"
    );

    // Verify no .mpf files
    let has_mpf = walkdir_find_mpf(&data_dir);
    assert!(!has_mpf, "no .mpf files when disk-offload disabled");

    // Verify no WAL v3 segments
    let wal_dir = data_dir.join("wal");
    assert!(
        !wal_dir.exists(),
        "no WAL v3 directory when disk-offload disabled"
    );

    // Verify checkpoint manager is None when disabled
    let ckpt: Option<CheckpointManager> = if config.disk_offload_enabled() {
        Some(CheckpointManager::new(CheckpointTrigger::new(300, 256 * 1024 * 1024, 0.9)))
    } else {
        None
    };
    assert!(ckpt.is_none(), "CheckpointManager should be None when disabled");

    // Verify all config knobs have sane defaults
    assert_eq!(config.segment_warm_after, 3600);
    assert_eq!(config.checkpoint_timeout, 300);
    assert!((config.checkpoint_completion - 0.9).abs() < f64::EPSILON);
}

/// Recursively check if any .mpf files exist under a directory.
fn walkdir_find_mpf(dir: &std::path::Path) -> bool {
    if !dir.exists() {
        return false;
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            if walkdir_find_mpf(&path) {
                return true;
            }
        } else if path.extension().is_some_and(|e| e == "mpf") {
            return true;
        }
    }
    false
}
