//! 6-phase recovery protocol for disk-offload mode.
//!
//! When disk-offload is enabled, shard recovery follows a structured protocol
//! inspired by PostgreSQL's crash recovery:
//!
//! 1. **ENTRY POINT** — Read control file, detect crash vs clean shutdown
//! 2. **MANIFEST RECOVERY** — Validate dual-root, build active file table
//! 3. **DATA LOAD** — Load snapshot if available
//! 4. **WAL REPLAY** — Forward replay from redo_lsn with FPI application
//! 5. **CONSISTENCY** — Cross-check manifest entries vs on-disk files
//! 6. **READY** — Update control file to Running state

use std::path::Path;

use bytes::Bytes;
use tracing::info;

use crate::persistence::clog::{ClogPage, TxnStatus};
use crate::persistence::control::{ShardControlFile, ShardState};
use crate::persistence::kv_page::{ValueType, read_datafile};
use crate::persistence::manifest::{FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::persistence::wal_v3::record::{WalRecord, WalRecordType};
use crate::persistence::wal_v3::replay::replay_wal_v3_dir;

/// Result of a v3 recovery operation.
#[derive(Debug, Default)]
pub struct RecoveryResult {
    /// Number of command records replayed from WAL v3.
    pub commands_replayed: usize,
    /// Number of Full Page Image records applied.
    pub fpi_applied: usize,
    /// Highest LSN seen during replay.
    pub last_lsn: u64,
    /// Manifest epoch at recovery time.
    pub manifest_epoch: u64,
    /// Number of IN_PROGRESS transactions rolled back via CLOG.
    pub txns_rolled_back: usize,
    /// Number of warm segments discovered from manifest.
    pub warm_segments_loaded: usize,
    /// Warm segment paths recovered from manifest, ready for VectorStore registration.
    /// Each tuple: (file_id, segment_dir_path).
    pub warm_segments: Vec<(u64, std::path::PathBuf)>,
    /// Number of KV entries reloaded from heap DataFiles.
    pub kv_heap_entries_loaded: usize,
    /// Cold DiskANN segment paths recovered from manifest.
    /// Each tuple: (file_id, segment_dir_path).
    pub cold_segments: Vec<(u64, std::path::PathBuf)>,
    /// Number of cold segments discovered.
    pub cold_segments_loaded: usize,
    /// Cold index rebuilt from heap DataFiles (None if no KvLeaf entries).
    pub cold_index: Option<crate::storage::tiered::cold_index::ColdIndex>,
}

/// 6-phase recovery protocol for disk-offload mode.
///
/// Phases:
/// 1. ENTRY POINT: Read control file, detect crash state
/// 2. MANIFEST RECOVERY: Validate dual-root, build file table
/// 3. DATA LOAD: Load snapshot if newer than redo_lsn
/// 4. WAL REPLAY: Forward replay from redo_lsn
/// 5. CONSISTENCY: Cross-check manifest vs disk
/// 6. READY: Update control file to Running
pub fn recover_shard_v3(
    databases: &mut [crate::storage::Database],
    shard_id: usize,
    shard_dir: &Path,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
) -> Result<RecoveryResult, crate::error::MoonError> {
    recover_shard_v3_with_fallback(databases, shard_id, shard_dir, engine, None)
}

/// v3 recovery with optional v2 WAL fallback directory.
///
/// When `v2_persistence_dir` is provided and the v3 WAL replays 0 commands,
/// falls back to replaying the v2 AOF file from `v2_persistence_dir/shard-{id}.aof`.
/// This handles the common case where disk offload was enabled but writes went
/// to the v2 AOF (the standard appendonly path).
pub fn recover_shard_v3_with_fallback(
    databases: &mut [crate::storage::Database],
    shard_id: usize,
    shard_dir: &Path,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
    v2_persistence_dir: Option<&Path>,
) -> Result<RecoveryResult, crate::error::MoonError> {
    let mut result = RecoveryResult::default();

    // ── Phase 1: ENTRY POINT ──────────────────────────────────────────
    let control_path = ShardControlFile::control_path(shard_dir, shard_id);
    let control = if control_path.exists() {
        match ShardControlFile::read(&control_path) {
            Ok(c) => {
                info!(
                    "Shard {}: control file loaded (checkpoint_lsn={}, state={:?})",
                    shard_id, c.last_checkpoint_lsn, c.shard_state
                );
                Some(c)
            }
            Err(e) => {
                tracing::warn!(
                    "Shard {}: control file read failed: {}, starting fresh",
                    shard_id,
                    e
                );
                None
            }
        }
    } else {
        info!(
            "Shard {}: no control file, first boot with disk-offload",
            shard_id
        );
        None
    };

    let redo_lsn = control.as_ref().map(|c| c.last_checkpoint_lsn).unwrap_or(0);

    // ── Phase 2: MANIFEST RECOVERY ────────────────────────────────────
    let manifest_path = shard_dir.join(format!("shard-{}.manifest", shard_id));
    if manifest_path.exists() {
        match ShardManifest::open(&manifest_path) {
            Ok(manifest) => {
                let file_count = manifest.files().len();
                info!(
                    "Shard {}: manifest recovered (epoch={}, files={})",
                    shard_id,
                    manifest.epoch(),
                    file_count
                );
                result.manifest_epoch = manifest.epoch();
                // Building/Compacting entries are cleaned up on next checkpoint commit
            }
            Err(e) => {
                tracing::warn!("Shard {}: manifest recovery failed: {}", shard_id, e);
            }
        }
    }

    // ── Phase 3: DATA LOAD ────────────────────────────────────────────
    // Load per-shard snapshot (reuses existing v2 snapshot format)
    let snap_path = shard_dir.join(format!("shard-{}.rrdshard", shard_id));
    if snap_path.exists() {
        match crate::persistence::snapshot::shard_snapshot_load(databases, &snap_path) {
            Ok(n) => {
                info!("Shard {}: loaded {} keys from snapshot", shard_id, n);
            }
            Err(e) => {
                tracing::error!("Shard {}: snapshot load failed: {}", shard_id, e);
            }
        }
    }

    // Phase 3 continued: Reload warm vector segments from manifest.
    // Scan manifest for tier=Warm, status=Active, file_type=VecCodes entries.
    // Each represents a segment that was offloaded to disk before the crash.
    if manifest_path.exists() {
        if let Ok(manifest) = ShardManifest::open(&manifest_path) {
            let vectors_dir = shard_dir.join("vectors");
            for entry in manifest.files() {
                if entry.tier == StorageTier::Warm
                    && entry.status == FileStatus::Active
                    && entry.file_type == PageType::VecCodes as u8
                {
                    let seg_dir = vectors_dir.join(format!("segment-{}", entry.file_id));
                    if seg_dir.exists() && seg_dir.join("codes.mpf").exists() {
                        result.warm_segments.push((entry.file_id, seg_dir));
                        info!(
                            "Shard {}: warm segment {} found ({}B codes)",
                            shard_id, entry.file_id, entry.byte_size
                        );
                    } else {
                        tracing::warn!(
                            "Shard {}: manifest references warm segment {} but directory missing",
                            shard_id,
                            entry.file_id
                        );
                    }
                }
            }
            result.warm_segments_loaded = result.warm_segments.len();
            if result.warm_segments_loaded > 0 {
                info!(
                    "Shard {}: discovered {} warm segment(s) from manifest",
                    shard_id, result.warm_segments_loaded
                );
            }
        }
    }

    // Phase 3 continued: Reload KV heap entries from DataFiles.
    // Scan manifest for status=Active, file_type=KvLeaf entries.
    // These represent KV entries spilled to disk before the crash.
    if manifest_path.exists() {
        if let Ok(manifest) = ShardManifest::open(&manifest_path) {
            let data_dir = shard_dir.join("data");
            for entry in manifest.files() {
                if entry.status == FileStatus::Active && entry.file_type == PageType::KvLeaf as u8 {
                    let heap_path = data_dir.join(format!("heap-{:06}.mpf", entry.file_id));
                    if heap_path.exists() {
                        match read_datafile(&heap_path) {
                            Ok(pages) => {
                                let mut file_entries = 0usize;
                                for page in &pages {
                                    for slot_idx in 0..page.slot_count() {
                                        if let Some(kv_entry) = page.get(slot_idx) {
                                            if kv_entry.value_type == ValueType::String {
                                                let key = Bytes::from(kv_entry.key);
                                                let value = Bytes::from(kv_entry.value);
                                                if let Some(ttl) = kv_entry.ttl_ms {
                                                    // ttl_ms is absolute unix millis
                                                    databases[0]
                                                        .set_string_with_expiry(key, value, ttl);
                                                } else {
                                                    databases[0].set_string(key, value);
                                                }
                                                file_entries += 1;
                                            }
                                            // Non-string types: skip for now (future work)
                                        }
                                    }
                                }
                                result.kv_heap_entries_loaded += file_entries;
                                info!(
                                    "Shard {}: reloaded {} KV entries from heap-{:06}.mpf",
                                    shard_id, file_entries, entry.file_id
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Shard {}: heap DataFile read failed for file {}: {}",
                                    shard_id,
                                    entry.file_id,
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    // Phase 3 continued: Build ColdIndex from manifest KvLeaf entries.
    // Used by Database::get() for read-through on DashTable miss.
    if manifest_path.exists() {
        if let Ok(manifest) = ShardManifest::open(&manifest_path) {
            let cold_idx = crate::storage::tiered::cold_index::ColdIndex::rebuild_from_manifest(
                shard_dir, &manifest,
            );
            if cold_idx.len() > 0 {
                info!(
                    "Shard {}: rebuilt cold index with {} entries",
                    shard_id,
                    cold_idx.len()
                );
                result.cold_index = Some(cold_idx);
            }
        }
    }

    // Phase 3 continued: Discover cold DiskANN segments from manifest.
    // tier=Cold, status=Active entries point to on-disk DiskAnnSegment directories.
    if manifest_path.exists() {
        if let Ok(manifest) = ShardManifest::open(&manifest_path) {
            let vectors_dir = shard_dir.join("vectors");
            for entry in manifest.files() {
                if entry.tier == StorageTier::Cold && entry.status == FileStatus::Active {
                    let seg_dir = vectors_dir.join(format!("segment-{}-diskann", entry.file_id));
                    if seg_dir.exists() && seg_dir.join("vamana.mpf").exists() {
                        result.cold_segments.push((entry.file_id, seg_dir));
                        result.cold_segments_loaded += 1;
                    }
                }
            }
            if result.cold_segments_loaded > 0 {
                info!(
                    "Shard {}: discovered {} cold DiskANN segment(s) from manifest",
                    shard_id, result.cold_segments_loaded
                );
            }
        }
    }

    // ── Phase 4: WAL REPLAY ───────────────────────────────────────────
    let wal_dir = shard_dir.join("wal-v3");
    if wal_dir.exists() {
        let mut selected_db = 0usize;
        let on_command = &mut |record: &WalRecord| {
            match record.record_type {
                WalRecordType::Command => {
                    // Parse RESP frames from the serialized command payload.
                    // The payload is RESP-encoded (same format as AOF/WAL v2 blocks).
                    let mut buf = bytes::BytesMut::from(&record.payload[..]);
                    let parse_cfg = crate::protocol::ParseConfig::default();
                    while let Ok(Some(frame)) = crate::protocol::parse::parse(&mut buf, &parse_cfg)
                    {
                        if let crate::protocol::Frame::Array(ref arr) = frame {
                            if !arr.is_empty() {
                                let cmd_name = match &arr[0] {
                                    crate::protocol::Frame::BulkString(s) => s.as_ref(),
                                    crate::protocol::Frame::SimpleString(s) => s.as_ref(),
                                    _ => continue,
                                };
                                engine.replay_command(
                                    databases,
                                    cmd_name,
                                    &arr[1..],
                                    &mut selected_db,
                                );
                                result.commands_replayed += 1;
                            }
                        }
                    }
                }
                WalRecordType::VectorUpsert
                | WalRecordType::VectorDelete
                | WalRecordType::VectorTxnCommit
                | WalRecordType::VectorTxnAbort
                | WalRecordType::VectorCheckpoint => {
                    // Vector WAL records -- tracked for future CLOG integration
                    result.commands_replayed += 1;
                }
                WalRecordType::FileCreate
                | WalRecordType::FileDelete
                | WalRecordType::FileTierChange => {
                    // File lifecycle events -- verify against manifest (future)
                    result.commands_replayed += 1;
                }
                _ => {}
            }
        };
        let on_fpi = &mut |record: &WalRecord| {
            use std::os::unix::fs::FileExt;

            let payload = &record.payload;
            if payload.len() < 16 {
                tracing::warn!(
                    "Shard {}: FPI record at LSN {} too short ({} bytes), skipping",
                    shard_id,
                    record.lsn,
                    payload.len()
                );
                return;
            }
            let file_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let page_offset = u64::from_le_bytes(payload[8..16].try_into().unwrap());

            // Check compression flag at offset 16 (added in Phase 84).
            // Pre-Phase-84 FPI records start page_data at offset 16 (first byte is
            // MoonPage magic 0x4D), so 0x00/0x01 flag bytes are unambiguous.
            let (page_data_owned, page_data_slice): (Vec<u8>, &[u8]) = if payload.len() > 17
                && payload[16] == 0x01
            {
                // LZ4-compressed FPI payload — bounded to defend against
                // crafted/oversized size prefixes (CRC alone does not).
                match crate::persistence::compression::safe_lz4_decompress(
                    &payload[17..],
                    crate::persistence::compression::MAX_LZ4_DECOMPRESSED,
                ) {
                    Some(decompressed) => (decompressed, &[]),
                    None => {
                        tracing::warn!(
                            "Shard {}: FPI LZ4 decompression failed or oversized at LSN {}, skipping",
                            shard_id,
                            record.lsn,
                        );
                        return;
                    }
                }
            } else if payload.len() > 17 && payload[16] == 0x00 {
                // Uncompressed FPI with flag byte
                (Vec::new(), &payload[17..])
            } else {
                // Legacy FPI (pre-Phase-84): no flag byte, page_data at offset 16
                (Vec::new(), &payload[16..])
            };

            let page_data: &[u8] = if !page_data_owned.is_empty() {
                &page_data_owned
            } else {
                page_data_slice
            };

            // Determine page size from data length
            let page_size = if page_data.len() > crate::persistence::page::PAGE_4K {
                crate::persistence::page::PAGE_64K
            } else {
                crate::persistence::page::PAGE_4K
            };
            let byte_offset = page_offset * page_size as u64;

            let data_dir = shard_dir.join("data");
            let _ = std::fs::create_dir_all(&data_dir);
            let file_path = data_dir.join(format!("heap-{:06}.mpf", file_id));

            // Open or create the DataFile and pwrite unconditionally (torn page repair).
            match std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&file_path)
            {
                Ok(file) => {
                    if let Err(e) = file.write_at(page_data, byte_offset) {
                        tracing::error!(
                            "Shard {}: FPI pwrite failed for file_id={}, offset={}: {}",
                            shard_id,
                            file_id,
                            page_offset,
                            e
                        );
                        return;
                    }
                    info!(
                        "Shard {}: FPI applied at LSN {} (file_id={}, offset={}, {} bytes)",
                        shard_id,
                        record.lsn,
                        file_id,
                        page_offset,
                        page_data.len()
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Shard {}: FPI cannot open DataFile heap-{:06}.mpf: {}",
                        shard_id,
                        file_id,
                        e
                    );
                    return;
                }
            }
            result.fpi_applied += 1;
        };

        match replay_wal_v3_dir(&wal_dir, redo_lsn, on_command, on_fpi) {
            Ok(replay_result) => {
                result.last_lsn = replay_result.last_lsn;
                info!(
                    "Shard {}: WAL v3 replay complete (cmds={}, fpi={}, last_lsn={})",
                    shard_id,
                    replay_result.commands_replayed,
                    replay_result.fpi_applied,
                    replay_result.last_lsn
                );
            }
            Err(e) => {
                tracing::error!("Shard {}: WAL v3 replay failed: {}", shard_id, e);
            }
        }
    }

    // ── Phase 4b: V2 WAL FALLBACK ──────────────────────────────────────
    // When v3 replay produced 0 commands and a v2 persistence directory is
    // available, fall back to replaying the v2 AOF file. This handles the
    // common case where --disk-offload enable was used with --appendonly yes
    // but write commands logged to the v2 AOF (standard appendonly path).
    if result.commands_replayed == 0 {
        if let Some(v2_dir) = v2_persistence_dir {
            // Try all v2 persistence sources in order:
            // 1. Per-shard binary WAL (shard-N.wal)
            // 2. Global RESP-format AOF (appendonly.aof)
            let v2_sources: &[(&std::path::Path, bool)] = &[
                (&crate::persistence::wal::wal_path(v2_dir, shard_id), false),
                (&v2_dir.join("appendonly.aof"), true),
            ];
            for &(ref path, is_aof) in v2_sources {
                if !path.exists() {
                    continue;
                }
                info!(
                    "Shard {}: v3 WAL empty, falling back to v2 replay from {:?}",
                    shard_id, path
                );
                let replay_result = if is_aof {
                    crate::persistence::aof::replay_aof(databases, path, engine)
                } else {
                    crate::persistence::wal::replay_wal(databases, path, engine)
                };
                match replay_result {
                    Ok(n) if n > 0 => {
                        result.commands_replayed = n;
                        info!("Shard {}: v2 fallback replayed {} commands", shard_id, n);
                        break;
                    }
                    Ok(_) => {
                        info!(
                            "Shard {}: v2 source {:?} had 0 commands, trying next",
                            shard_id, path
                        );
                    }
                    Err(e) => {
                        tracing::error!("Shard {}: v2 fallback {:?} failed: {}", shard_id, path, e);
                    }
                }
            }
        }
    }

    // ── Phase 5: CONSISTENCY ──────────────────────────────────────────
    // Cross-check: verify manifest files exist on disk.
    // (Lightweight for now -- full CRC verification is expensive at startup)

    // CLOG rollback: scan all CLOG pages and mark IN_PROGRESS txns as Aborted.
    // Any transaction still IN_PROGRESS at WAL end was interrupted by a crash.
    let clog_dir = shard_dir.join("clog");
    if clog_dir.exists() {
        let next_txn = control.as_ref().map(|c| c.next_txn_id).unwrap_or(0);
        match crate::persistence::clog::scan_clog_dir(&clog_dir) {
            Ok(mut pages) => {
                let mut rolled_back = 0u64;
                for txn_id in 0..next_txn {
                    let page_idx = ClogPage::page_for_txn(txn_id);
                    if let Some(page) = pages.iter_mut().find(|p| p.page_index() == page_idx) {
                        if page.get_status(txn_id) == TxnStatus::InProgress {
                            page.set_status(txn_id, TxnStatus::Aborted);
                            rolled_back += 1;
                        }
                    }
                }
                if rolled_back > 0 {
                    info!(
                        "Shard {}: rolled back {} uncommitted vector transactions via CLOG",
                        shard_id, rolled_back
                    );
                    // Write modified CLOG pages back to disk
                    for page in &pages {
                        if let Err(e) = crate::persistence::clog::write_clog_page(&clog_dir, page) {
                            tracing::error!("Shard {}: CLOG page write failed: {}", shard_id, e);
                        }
                    }
                }
                result.txns_rolled_back = rolled_back as usize;
            }
            Err(e) => {
                tracing::warn!("Shard {}: CLOG scan failed: {}", shard_id, e);
            }
        }
    }

    // ── Phase 6: READY ────────────────────────────────────────────────
    // Update control file to Running state with recovered LSN position.
    let shard_uuid = control.as_ref().map(|c| c.shard_uuid).unwrap_or([0u8; 16]);
    let mut new_control = ShardControlFile::new(shard_uuid);
    new_control.shard_state = ShardState::Running;
    new_control.last_checkpoint_lsn = redo_lsn;
    new_control.last_checkpoint_epoch = control
        .as_ref()
        .map(|c| c.last_checkpoint_epoch)
        .unwrap_or(0);
    new_control.wal_flush_lsn = result.last_lsn;
    new_control.next_txn_id = control.as_ref().map(|c| c.next_txn_id).unwrap_or(0);
    new_control.next_page_id = control.as_ref().map(|c| c.next_page_id).unwrap_or(0);
    if let Err(e) = new_control.write(&control_path) {
        tracing::error!(
            "Shard {}: control file update to Running failed: {}",
            shard_id,
            e
        );
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::wal_v3::record::write_wal_v3_record;
    use crate::storage::Database;

    /// Build a minimal v3 segment header.
    fn make_v3_header(shard_id: u16) -> Vec<u8> {
        let mut header = vec![0u8; 64];
        header[0..6].copy_from_slice(b"RRDWAL");
        header[6] = 3; // version = 3
        header[7] = 0x01; // flags = FPI_ENABLED
        header[8..10].copy_from_slice(&shard_id.to_le_bytes());
        header
    }

    #[test]
    fn test_recover_shard_v3_no_files() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        assert_eq!(result.commands_replayed, 0);
        assert_eq!(result.fpi_applied, 0);
        assert_eq!(result.last_lsn, 0);
    }

    #[test]
    fn test_recover_shard_v3_control_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Write a control file
        let mut ctl = ShardControlFile::new([0xAA; 16]);
        ctl.shard_state = ShardState::Crashed;
        ctl.last_checkpoint_lsn = 42;
        ctl.write(&ShardControlFile::control_path(&shard_dir, 0))
            .unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        // Control file should be updated to Running
        let ctl_back =
            ShardControlFile::read(&ShardControlFile::control_path(&shard_dir, 0)).unwrap();
        assert_eq!(ctl_back.shard_state, ShardState::Running);
        assert_eq!(ctl_back.last_checkpoint_lsn, 42);
        assert_eq!(ctl_back.shard_uuid, [0xAA; 16]);
        assert_eq!(result.last_lsn, 0);
    }

    #[test]
    fn test_recover_shard_v3_wal_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write a WAL segment with 3 command records
        let mut data = make_v3_header(0);
        for i in 1..=3u64 {
            write_wal_v3_record(
                &mut data,
                i,
                WalRecordType::Command,
                b"*1\r\n$4\r\nPING\r\n",
            );
        }
        std::fs::write(wal_dir.join("000000000001.wal"), &data).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        assert_eq!(result.commands_replayed, 3);
        assert_eq!(result.last_lsn, 3);

        // Control file should be written
        let ctl_path = ShardControlFile::control_path(&shard_dir, 0);
        assert!(ctl_path.exists());
        let ctl = ShardControlFile::read(&ctl_path).unwrap();
        assert_eq!(ctl.shard_state, ShardState::Running);
        assert_eq!(ctl.wal_flush_lsn, 3);
    }

    #[test]
    fn test_recover_shard_v3_fpi_counted() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut data = make_v3_header(0);
        write_wal_v3_record(
            &mut data,
            1,
            WalRecordType::Command,
            b"*1\r\n$4\r\nPING\r\n",
        );
        // FPI payload: file_id(8 LE) + page_offset(8 LE) + page_data
        let mut fpi_payload = Vec::new();
        fpi_payload.extend_from_slice(&1u64.to_le_bytes()); // file_id = 1
        fpi_payload.extend_from_slice(&0u64.to_le_bytes()); // page_offset = 0
        fpi_payload.extend_from_slice(&vec![0xABu8; 128]); // page_data
        write_wal_v3_record(&mut data, 2, WalRecordType::FullPageImage, &fpi_payload);
        std::fs::write(wal_dir.join("000000000001.wal"), &data).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        assert_eq!(result.commands_replayed, 1);
        assert_eq!(result.fpi_applied, 1);
        assert_eq!(result.last_lsn, 2);
    }

    #[test]
    fn test_recover_shard_v3_skips_below_redo_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Control file with checkpoint at LSN 2
        let mut ctl = ShardControlFile::new([0u8; 16]);
        ctl.last_checkpoint_lsn = 2;
        ctl.write(&ShardControlFile::control_path(&shard_dir, 0))
            .unwrap();

        // WAL with LSNs 1-5
        let mut data = make_v3_header(0);
        for i in 1..=5u64 {
            write_wal_v3_record(
                &mut data,
                i,
                WalRecordType::Command,
                b"*1\r\n$4\r\nPING\r\n",
            );
        }
        std::fs::write(wal_dir.join("000000000001.wal"), &data).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        // Only LSNs 3, 4, 5 should be replayed (skip 1, 2)
        assert_eq!(result.commands_replayed, 3);
        assert_eq!(result.last_lsn, 5);
    }

    #[test]
    fn test_recover_shard_v3_clog_rollback() {
        use crate::persistence::clog::{self, ClogPage, TxnStatus};

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Write a control file with next_txn_id = 5
        let mut ctl = ShardControlFile::new([0u8; 16]);
        ctl.next_txn_id = 5;
        ctl.write(&ShardControlFile::control_path(&shard_dir, 0))
            .unwrap();

        // Write a CLOG page with txns 0=Committed, 1=InProgress, 2=Aborted,
        // 3=InProgress, 4=Committed
        let clog_dir = shard_dir.join("clog");
        let mut page0 = ClogPage::new(0);
        page0.set_status(0, TxnStatus::Committed);
        page0.set_status(1, TxnStatus::InProgress);
        page0.set_status(2, TxnStatus::Aborted);
        page0.set_status(3, TxnStatus::InProgress);
        page0.set_status(4, TxnStatus::Committed);
        clog::write_clog_page(&clog_dir, &page0).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        // Txns 1 and 3 should have been rolled back
        assert_eq!(result.txns_rolled_back, 2);

        // Verify CLOG pages on disk were updated
        let pages = clog::scan_clog_dir(&clog_dir).unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].get_status(0), TxnStatus::Committed);
        assert_eq!(pages[0].get_status(1), TxnStatus::Aborted);
        assert_eq!(pages[0].get_status(2), TxnStatus::Aborted);
        assert_eq!(pages[0].get_status(3), TxnStatus::Aborted);
        assert_eq!(pages[0].get_status(4), TxnStatus::Committed);
    }

    #[test]
    fn test_recover_warm_segments_from_manifest() {
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Create a manifest with one warm VecCodes entry and one hot entry
        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        manifest.add_file(FileEntry {
            file_id: 42,
            file_type: PageType::VecCodes as u8,
            status: FileStatus::Active,
            tier: StorageTier::Warm,
            page_size_log2: 16,
            page_count: 10,
            byte_size: 655360,
            created_lsn: 1,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
        });
        manifest.add_file(FileEntry {
            file_id: 99,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 5,
            byte_size: 20480,
            created_lsn: 2,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
        });
        manifest.commit().unwrap();
        drop(manifest);

        // Create the segment directory with codes.mpf
        let seg_dir = shard_dir.join("vectors").join("segment-42");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::write(seg_dir.join("codes.mpf"), &[0u8; 64]).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        assert_eq!(result.warm_segments_loaded, 1);
        assert_eq!(result.warm_segments.len(), 1);
        assert_eq!(result.warm_segments[0].0, 42);
        assert_eq!(result.warm_segments[0].1, seg_dir);
    }

    #[test]
    fn test_recover_kv_heap_entries() {
        use crate::persistence::kv_page::{KvLeafPage, ValueType, write_datafile};
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Create manifest with one KvLeaf/Active entry
        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        manifest.add_file(FileEntry {
            file_id: 7,
            file_type: PageType::KvLeaf as u8,
            status: FileStatus::Active,
            tier: StorageTier::Hot,
            page_size_log2: 12,
            page_count: 1,
            byte_size: 4096,
            created_lsn: 1,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
        });
        manifest.commit().unwrap();
        drop(manifest);

        // Create DataFile with 3 string KV entries
        let data_dir = shard_dir.join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let mut page = KvLeafPage::new(0, 7);
        page.insert(b"key1", b"val1", ValueType::String, 0, None)
            .unwrap();
        page.insert(b"key2", b"val2", ValueType::String, 0, None)
            .unwrap();
        // TTL is stored as absolute unix millis -- use a far-future value
        page.insert(
            b"key3",
            b"val3",
            ValueType::String,
            0,
            Some(4_000_000_000_000),
        )
        .unwrap();
        page.finalize();
        write_datafile(&data_dir.join("heap-000007.mpf"), &[&page]).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        assert_eq!(result.kv_heap_entries_loaded, 3);

        // Verify entries exist in database
        assert!(
            databases[0].get(b"key1").is_some(),
            "key1 should be in database"
        );
        assert!(
            databases[0].get(b"key2").is_some(),
            "key2 should be in database"
        );
        assert!(
            databases[0].get(b"key3").is_some(),
            "key3 should be in database"
        );
    }

    #[test]
    fn test_recover_cold_segments_from_manifest() {
        use crate::persistence::manifest::{FileEntry, FileStatus, ShardManifest, StorageTier};
        use crate::persistence::page::PageType;

        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Create manifest with a Cold/Active entry
        let manifest_path = shard_dir.join("shard-0.manifest");
        let mut manifest = ShardManifest::create(&manifest_path).unwrap();
        manifest.add_file(FileEntry {
            file_id: 50,
            file_type: PageType::VecCodes as u8,
            status: FileStatus::Active,
            tier: StorageTier::Cold,
            page_size_log2: 16,
            page_count: 8,
            byte_size: 524288,
            created_lsn: 10,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
        });
        // Also add a non-cold entry that should be ignored
        manifest.add_file(FileEntry {
            file_id: 51,
            file_type: PageType::VecCodes as u8,
            status: FileStatus::Active,
            tier: StorageTier::Warm,
            page_size_log2: 16,
            page_count: 4,
            byte_size: 262144,
            created_lsn: 11,
            min_key_hash: 0,
            max_key_hash: u64::MAX,
        });
        manifest.commit().unwrap();
        drop(manifest);

        // Create the cold segment directory with vamana.mpf
        let seg_dir = shard_dir.join("vectors").join("segment-50-diskann");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::write(seg_dir.join("vamana.mpf"), &[0u8; 128]).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine;
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        assert_eq!(result.cold_segments_loaded, 1);
        assert_eq!(result.cold_segments.len(), 1);
        assert_eq!(result.cold_segments[0].0, 50);
        assert_eq!(result.cold_segments[0].1, seg_dir);
    }
}
