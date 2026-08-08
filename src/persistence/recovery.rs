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

use tracing::info;

use crate::persistence::clog::{ClogPage, TxnStatus};
use crate::persistence::control::{ShardControlFile, ShardState};
use crate::persistence::manifest::{FileStatus, ShardManifest, StorageTier};
use crate::persistence::page::PageType;
use crate::persistence::wal_v3::record::{WalRecord, WalRecordType};
use crate::persistence::wal_v3::replay::{replay_wal_v3_dir, replay_wal_v3_dir_until};

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
    /// Number of KV entries recovered from heap DataFiles into the COLD
    /// index (read-through on next access), NOT hot-loaded into RAM.
    ///
    /// Task #56 (used_memory truthfulness): this used to ALSO re-insert
    /// every `ValueType::String` heap entry directly into `databases[0]`'s
    /// hot DashTable (added by #79-04, predating ColdIndex read-through),
    /// while the very next Phase-3 step (#80-02) independently rebuilds a
    /// `ColdIndex` entry for the SAME key pointing at the SAME on-disk
    /// location. That double-booked every previously-spilled String key:
    /// once as a fully-charged hot RAM entry (defeating the entire point of
    /// disk-offload -- the data it evicted specifically to stay under
    /// `maxmemory` came right back) and again as a redundant cold-index
    /// stub. This is the mechanism behind the observed "used_memory got
    /// WORSE after a kill-9 restart" regression: a restart un-evicted
    /// everything in one shot, with no re-application of eviction/maxmemory
    /// gating during the reload. Non-string types were never affected by
    /// the old bug (the pre-existing code explicitly skipped them), which is
    /// how the asymmetry went unnoticed — Hash/List/Set/ZSet/Stream cold
    /// entries were already recovered as cold-only stubs, exactly like every
    /// other evicted key.
    ///
    /// Now every value type funnels through the same `ColdIndex` +
    /// `cold_read_through` path recovery already relies on for non-string
    /// types: a key surfaces to callers as a normal cold-index-backed miss,
    /// and `Database::get`'s existing `promote_cold_if_present` fallback
    /// lazily rehydrates it into hot RAM (and charges `used_memory`) only
    /// when something actually reads it. Restart no longer moves the
    /// maxmemory needle by itself.
    pub kv_heap_entries_loaded: usize,
    /// Paths of crash-orphaned `heap-*.mpf`/`.tmp` files classified during
    /// recovery (task #55) but NOT yet deleted. Classification is cheap
    /// (metadata-only) and safe to run synchronously here — it observes the
    /// manifest state recovery just rebuilt, before any command has been
    /// served on this shard. The actual `remove_file` I/O (the part that
    /// measured ~40s/shard at ~59K files in production) is deferred to a
    /// background sweep that starts only after the shard is already serving
    /// traffic, so restart-to-ready stays seconds-scale regardless of
    /// cold-plane size. See `storage::tiered::kv_spill::classify_orphan_heap_files`.
    pub pending_heap_orphans: Vec<std::path::PathBuf>,
    // NOTE: the ColdIndex rebuilt in Phase 3 is attached directly to
    // `databases[0]` BEFORE Phase 4 replay (never returned here) so that
    // replayed deletes tombstone the cold plane — see the Phase 3 comment.
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
    recover_shard_v3_pitr(
        databases,
        shard_id,
        shard_dir,
        engine,
        v2_persistence_dir,
        None,
    )
}

/// PITR-aware recovery entry point (P3b).
///
/// When `recovery_target_lsn = Some(target)` is provided:
/// 1. The on-disk snapshot is loaded only if its `last_lsn <= target`.
///    Snapshots taken after the target are skipped -- they contain state
///    from after the desired restore point.
/// 2. WAL replay stops at the first record with `lsn > target`. The
///    recovery's `last_lsn` reflects only records actually applied.
/// 3. Snapshots with `last_lsn == 0` (legacy v1 or unstamped v2) are
///    treated conservatively: skipped when a target is set, because we
///    cannot prove they're safe to load relative to the target.
///
/// `recovery_target_lsn = None` reproduces the classic crash-recovery
/// behavior (load snapshot, replay all WAL past redo_lsn).
pub fn recover_shard_v3_pitr(
    databases: &mut [crate::storage::Database],
    shard_id: usize,
    shard_dir: &Path,
    engine: &dyn crate::persistence::replay::CommandReplayEngine,
    v2_persistence_dir: Option<&Path>,
    recovery_target_lsn: Option<u64>,
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
    // Load per-shard snapshot (reuses existing v2 snapshot format).
    // PITR: when recovery_target_lsn is set we must skip snapshots that
    // were taken AFTER the target (their state includes records we want
    // to undo), and also skip snapshots whose last_lsn is unknown (== 0
    // for legacy v1 or unstamped v2 files). Skipping forces full WAL
    // replay up to the target -- slower but correct.
    let snap_path = shard_dir.join(format!("shard-{}.rrdshard", shard_id));
    if snap_path.exists() {
        let snapshot_ok = if let Some(target) = recovery_target_lsn {
            match crate::persistence::snapshot::read_snapshot_metadata(&snap_path) {
                Ok(meta) => {
                    if meta.last_lsn == 0 {
                        tracing::warn!(
                            "Shard {}: PITR target_lsn={} set but snapshot has no \
                             stamped LSN; skipping snapshot to force full WAL replay",
                            shard_id,
                            target
                        );
                        false
                    } else if meta.last_lsn > target {
                        tracing::warn!(
                            "Shard {}: PITR target_lsn={} is before snapshot \
                             last_lsn={}; skipping snapshot",
                            shard_id,
                            target,
                            meta.last_lsn
                        );
                        false
                    } else {
                        true
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Shard {}: PITR could not peek snapshot metadata ({}); skipping",
                        shard_id,
                        e
                    );
                    false
                }
            }
        } else {
            true
        };

        if snapshot_ok {
            match crate::persistence::snapshot::shard_snapshot_load(databases, &snap_path) {
                Ok(n) => {
                    info!("Shard {}: loaded {} keys from snapshot", shard_id, n);
                }
                Err(e) => {
                    tracing::error!("Shard {}: snapshot load failed: {}", shard_id, e);
                }
            }
        }
    }

    // Phase 3 continued: Reload warm vector segments from manifest.
    // Scan manifest for tier=Warm, status=Active, file_type=VecCodes entries.
    // Each represents a segment that was offloaded to disk before the crash.
    if manifest_path.exists() {
        if let Ok(manifest) = ShardManifest::open(&manifest_path) {
            let vectors_dir = shard_dir.join("vectors");
            // #435: reclaim `.segment-*.staging` leftovers before scanning.
            // A warm transition that died between its manifest commit and its
            // rename leaves a fully-written staging dir that nothing reads and
            // nothing removed — a live instance accumulated 14,499 of them
            // holding 20 GB against 174 MB of real segments, invisible to `ls`
            // and to any `vectors/*` glob because of the dot prefix. The
            // in-process guard covers new failures; this covers orphans from a
            // kill -9 or an older build.
            crate::storage::tiered::warm_tier::sweep_orphan_staging(&vectors_dir);
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

    // Phase 3 continued: Build ColdIndex from manifest KvLeaf entries.
    // Used by Database::get() for read-through on DashTable miss.
    //
    // Task #56 (used_memory truthfulness): this used to run AFTER a separate
    // loop that read every KvLeaf DataFile and re-inserted its
    // `ValueType::String` entries directly into `databases[0]`'s hot
    // DashTable (see `RecoveryResult::kv_heap_entries_loaded`'s doc comment
    // for the full history). That loop is gone -- every spilled key,
    // regardless of value type, now recovers as a cold-index-only stub and
    // is promoted into hot RAM (and charged to `used_memory`) lazily, on
    // first access, via the exact same `Database::promote_cold_if_present`
    // path normal (non-restart) cold read-through already uses. This is the
    // ONLY place a KvLeaf DataFile is read during recovery now.
    if manifest_path.exists() {
        if let Ok(manifest) = ShardManifest::open(&manifest_path) {
            let per_db =
                crate::storage::tiered::cold_index::ColdIndex::rebuild_from_manifest_per_db(
                    shard_dir, &manifest,
                );
            // Observability parity with the removed hot-reload loop's
            // counter: report how many keys the cold tier recovered, NOT
            // how many were loaded into RAM (zero, by design).
            result.kv_heap_entries_loaded = per_db.iter().map(|(_, ci)| ci.len()).sum();
            // Attach each database's index to ITS database (#139 — spilled
            // SELECT >0 keys used to recover into db0's index, unreachable
            // from their own db) BEFORE Phase 4 replay. Replayed
            // DEL/UNLINK/FLUSH*/EXPIRE-past must tombstone the cold plane:
            // with `cold_index == None`, `remove_counting_cold()`/`clear()`
            // are silent no-ops (`as_mut()` on None), so a key deleted in
            // the WAL tail resurrects via cold read-through after restart
            // whenever the crash lands inside the pre-orphan-sweep window
            // (the manifest entry is still Active until the sweep).
            for (db_index, cold_idx) in per_db {
                info!(
                    "Shard {}: rebuilt cold index for db {} with {} entries",
                    shard_id,
                    db_index,
                    cold_idx.len()
                );
                match databases.get_mut(db_index) {
                    Some(db) => {
                        db.cold_shard_dir = Some(shard_dir.to_path_buf());
                        match db.cold_index.as_mut() {
                            Some(existing) => existing.merge(cold_idx),
                            None => db.cold_index = Some(cold_idx),
                        }
                    }
                    None => {
                        // --databases was reduced below what this manifest
                        // was written under. The keys are unreachable either
                        // way (their db no longer exists); refuse to attach
                        // them to a WRONG db and say so loudly instead of
                        // silently resurrecting them elsewhere.
                        tracing::warn!(
                            shard_id,
                            db_index,
                            entries = cold_idx.len(),
                            "cold recovery: manifest references a logical db beyond the \
                             configured --databases count; its spilled keys are NOT attached \
                             (unreachable until the server is restarted with enough databases)"
                        );
                    }
                }
            }
            // Crash-orphan sweep (task #55): heap files written but never
            // registered in the manifest (crash between spill write and
            // manifest commit) leak disk forever otherwise. Manifest opened
            // OK — safe to classify. Only CLASSIFY here (cheap: read_dir +
            // HashSet membership, no `remove_file` syscalls) so recovery
            // stays fast at any cold-plane size; the actual deletes are
            // deferred to a background sweep the caller starts once the
            // shard is serving traffic (see `Shard::restore_from_persistence`
            // / `event_loop.rs`'s `pending_heap_orphans` handoff).
            let pending =
                crate::storage::tiered::kv_spill::classify_orphan_heap_files(shard_dir, &manifest);
            if !pending.is_empty() {
                info!(
                    "Shard {}: classified {} crash-orphaned heap file(s), deferred for background reclaim",
                    shard_id,
                    pending.len()
                );
            }
            result.pending_heap_orphans = pending;
        }
    }

    // ── Phase 4: WAL REPLAY ───────────────────────────────────────────
    // KV `Command` records counted separately from vector/file-lifecycle
    // records: the Phase 4b "WAL gave us no KV history → fall back to the
    // AOF" gate must key on THIS count. Gating on the aggregate
    // `commands_replayed` silently skipped the AOF (the only complete KV
    // history — `wal_kv_log` is auto-off when the AOF is the authority)
    // whenever disk-offload spills had written FileCreate records into the
    // WAL: every KV write since the last snapshot was lost on restart.
    let mut kv_commands_replayed = 0usize;
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
                                kv_commands_replayed += 1;
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
                WalRecordType::XactCommit => {
                    // K1a decision (storage-audit-2026-07-12-wal.md §5.1):
                    // intentional documented no-op, NOT a gap. Before K1a this
                    // record arrived here nested inside an outer `Command`
                    // frame, so `read_wal_v3_record`-parsing the payload as
                    // RESP silently produced nothing (the audit's "XactCommit
                    // replay is functionally dead"); now that the typed
                    // channel preserves the real outer type, this arm is
                    // reachable for the first time and needs an explicit
                    // decision rather than silently falling into `_ => {}`.
                    //
                    // The forward-image KV payload
                    // (`encode_xact_commit_payload`) is REDUNDANT here in
                    // every reachable config: the transaction's individual
                    // SET/DEL ops already ride EITHER this same Phase-4 WAL
                    // replay as ordinary `Command` records (when
                    // `--wal-kv-log` is on — ordinary KV write dispatch does
                    // not distinguish "inside a cross-store txn" from any
                    // other write) OR the AOF, which is the KV recovery
                    // authority in every reachable config (Phase 4b below
                    // falls back to it whenever `kv_commands_replayed == 0`).
                    // Decoding + re-applying the forward image would at best
                    // be a no-op (same final value) and at worst double-apply
                    // a non-idempotent op if that invariant ever drifts — the
                    // exact risk the Phase 4b comment above already flags for
                    // WAL+AOF overlap. Counted (like Vector*/File* above) so
                    // `commands_replayed` reflects what was actually on disk;
                    // never dispatched.
                    //
                    // ASYMMETRY (intentional): the last-resort legacy fallback
                    // `wal_v3::replay::replay_wal_v3_dir_commands` DOES apply
                    // XactCommit via `replay_xact_commit`. That path runs only
                    // when this Phase 4 replayed ZERO KV commands AND no
                    // `appendonly.aof` exists — i.e. exactly when neither of
                    // the redundant coverage sources argued above is present,
                    // so applying the forward image there is the only way the
                    // txn's writes survive at all. Same reasoning, opposite
                    // conclusion, because the preconditions are complementary.
                    result.commands_replayed += 1;
                }
                _ => {}
            }
        };
        let on_fpi = &mut |record: &WalRecord| {
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
            // Bounds already checked by `payload.len() < 16` guard above; use
            // explicit byte arrays to avoid `.unwrap()` per coding guidelines.
            let file_id = u64::from_le_bytes([
                payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
                payload[7],
            ]);
            let page_offset = u64::from_le_bytes([
                payload[8],
                payload[9],
                payload[10],
                payload[11],
                payload[12],
                payload[13],
                payload[14],
                payload[15],
            ]);

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
                    if let Err(e) = crate::util::file_ext::write_at(&file, page_data, byte_offset) {
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

        // PITR: when recovery_target_lsn is set, stop WAL replay at the
        // target. Otherwise call the classic replay_wal_v3_dir for
        // bit-identical behavior with crash recovery.
        let replay_outcome = if let Some(target) = recovery_target_lsn {
            info!(
                "Shard {}: PITR replay with stop_at_lsn={}",
                shard_id, target
            );
            replay_wal_v3_dir_until(&wal_dir, redo_lsn, Some(target), on_command, on_fpi)
        } else {
            replay_wal_v3_dir(&wal_dir, redo_lsn, on_command, on_fpi)
        };

        match replay_outcome {
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
                // #452.2: a mid-chain tear must ABORT boot, not degrade to a
                // silently-truncated dataset (pre-tear records are already
                // applied; post-tear segments were dropped).
                if crate::persistence::wal_v3::replay::is_mid_chain_tear(&e) {
                    crate::persistence::wal_v3::replay::abort_boot_on_mid_chain_tear(shard_id, &e);
                }
                tracing::error!("Shard {}: WAL v3 replay failed: {}", shard_id, e);
            }
        }
    }

    // Loud failure (not silent skip) for a leftover pre-freeze WAL v2 file
    // in the legacy persistence dir. WAL v2 support was removed in this
    // build; the file is never consulted by any recovery path here, so
    // surface it unconditionally (not only when the branch below happens to
    // run) so an operator who upgraded over a v2-only deployment finds out
    // immediately rather than discovering a silent recovery gap later.
    if let Some(v2_dir) = v2_persistence_dir {
        let legacy_v2_wal = v2_dir.join(format!("shard-{}.wal", shard_id));
        if legacy_v2_wal.exists() {
            tracing::error!(
                "Shard {}: found legacy WAL v2 file {:?} — WAL v2 support was \
                 removed (pre-1.0 WAL-v3-only format freeze) and this build \
                 does NOT replay it. If its contents are not already reflected \
                 in appendonly.aof, replay it on a pre-freeze Moon build first \
                 (which re-persists through the AOF) before removing this file.",
                shard_id,
                legacy_v2_wal
            );
        }
    }

    // ── Phase 4b: legacy-dir AOF / WAL v3 FALLBACK ─────────────────────
    // When the disk-offload WAL v3 replay (Phase 4, above) produced 0 KV
    // `Command` records and a legacy persistence directory is available, fall
    // back to it. The gate is `kv_commands_replayed` — NOT the aggregate
    // `commands_replayed`, which also counts vector + file-lifecycle records:
    // disk-offload spills write FileCreate records into the same WAL, and
    // gating on the aggregate skipped the AOF (the only complete KV history)
    // on every post-spill restart. `appendonly.aof` is the AUTHORITY there
    // (post-#211, WAL v3's KV coverage is intentionally partial:
    // `--wal-kv-log` default-off, and connection-local writes bypass it even
    // when on — a non-empty WAL v3 must never shadow the AOF). Only when NO
    // AOF exists at all does the legacy-mode WAL v3 directory
    // (`v2_dir/shard-N/wal-v3/`, distinct from `shard_dir`'s Phase-4 WAL
    // despite the shared name) serve as a last-resort partial recovery
    // source. The WAL v2 rung (per-shard `shard-N.wal`) was removed in the
    // pre-1.0 WAL-v3-only format freeze and is never replayed by this build.
    //
    // Known follow-up: when the Phase-4 WAL DID carry some KV records
    // (`--wal-kv-log on` / CDC active) alongside an existing AOF, this gate
    // keeps the WAL's partial KV view and still skips the AOF (replaying
    // both would double-apply non-idempotent commands). Resolving that needs
    // an AOF-first redesign of Phase 4, tracked in the roadmap.
    if kv_commands_replayed == 0 {
        if let Some(v2_dir) = v2_persistence_dir {
            let aof_path = v2_dir.join("appendonly.aof");
            if aof_path.exists() {
                info!(
                    "Shard {}: WAL carried no KV commands, falling back to AOF replay from {:?}",
                    shard_id, aof_path
                );
                match crate::persistence::aof::replay_aof(databases, &aof_path, engine) {
                    Ok(n) => {
                        result.commands_replayed += n;
                        info!("Shard {}: AOF fallback replayed {} commands", shard_id, n);
                    }
                    Err(e) => {
                        tracing::error!(
                            "Shard {}: AOF fallback {:?} failed: {}",
                            shard_id,
                            aof_path,
                            e
                        );
                    }
                }
            } else {
                let legacy_wal_v3_dir = v2_dir.join(format!("shard-{}", shard_id)).join("wal-v3");
                match crate::persistence::wal_v3::replay::replay_wal_v3_dir_commands(
                    &legacy_wal_v3_dir,
                    databases,
                    engine,
                ) {
                    Ok(0) => {}
                    Ok(n) => {
                        result.commands_replayed += n;
                        tracing::warn!(
                            "Shard {}: no appendonly.aof found — replayed {} records \
                             from legacy-mode WAL v3 at {:?} as a LAST-RESORT \
                             fallback. WAL v3 KV coverage is partial (gated by \
                             --wal-kv-log; connection-local writes bypass it), so \
                             this recovery may be incomplete.",
                            shard_id,
                            n,
                            legacy_wal_v3_dir
                        );
                    }
                    Err(e) => {
                        // #452.2: mid-chain tear ⇒ refuse to boot.
                        if crate::persistence::wal_v3::replay::is_mid_chain_tear(&e) {
                            crate::persistence::wal_v3::replay::abort_boot_on_mid_chain_tear(
                                shard_id, &e,
                            );
                        }
                        tracing::error!(
                            "Shard {}: legacy-mode WAL v3 fallback replay failed: {}",
                            shard_id,
                            e
                        );
                    }
                }
            }
        }
    }

    // Task #56 finding 2 (adversarial review): the main.rs boot sequence
    // only invokes `Database::demote_replayed_cold_shadows` after the LATER
    // multi-part/per-shard manifest-based AOF replay branches -- but under
    // `runtime-tokio` + `--shards 1`, no such manifest is ever created
    // (`AofManifest::initialize` is monoio-only there), so the Phase 4b
    // fallback immediately above is the ONLY place `appendonly.aof` (or the
    // last-resort legacy WAL v3 dir) ever gets replayed for that config.
    // Without this call, that config's disk-offload restarts were exactly
    // as exposed to the AOF-replay-rehydrates-cold-shadows bug as the
    // manifest-based paths were before task #56 fixed those -- silently
    // inert for this one runtime/shard combination. Reconciling here, right
    // after Phase 4b and before Phase 5, covers it the same way regardless
    // of which fallback source (AOF or legacy WAL v3) supplied the replay.
    if let Some(db0) = databases.first_mut() {
        let demoted = db0.demote_replayed_cold_shadows();
        if demoted > 0 {
            info!(
                "Shard {}: demoted {} AOF-replay hot shadow(s) back to cold-only \
                 stubs (Phase 4b fallback path, used_memory truthful after restart, task #56)",
                shard_id, demoted
            );
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
    // Kernel M3 K2: carry the per-plane floor register forward across
    // restart, same pattern as every other field above. `graph_floor_lsn`
    // is only a recycle-decision MIRROR of `graph_metadata.json`'s own
    // `snapshot_lsn` (graph's real replay-skip authority, loaded
    // independently by `recover_graph_store`); both were written together
    // in the same Finalize tick, so the last successfully-written control
    // file's mirror is still consistent with it. Losing this on restart
    // (resetting to the `0` sentinel) would not be UNSAFE — `0` is
    // maximally conservative — but it would silently disable WAL recycling
    // for graph-touched shards until the next checkpoint completes, which
    // is a real, avoidable regression. ws/mq stay at whatever sentinel they
    // already were (always `0` this milestone — no writer exists yet).
    new_control.graph_floor_lsn = control.as_ref().map(|c| c.graph_floor_lsn).unwrap_or(0);
    new_control.ws_floor_lsn = control.as_ref().map(|c| c.ws_floor_lsn).unwrap_or(0);
    new_control.mq_floor_lsn = control.as_ref().map(|c| c.mq_floor_lsn).unwrap_or(0);
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
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
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
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
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
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
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

    /// P3b — PITR end-to-end: write 10 WAL commands, recover with
    /// target_lsn = 5, assert only 5 records were applied and the
    /// recovery's last_lsn reflects exactly the cutoff.
    #[test]
    fn test_recover_shard_v3_pitr_stops_at_target_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut data = make_v3_header(0);
        for i in 1..=10u64 {
            write_wal_v3_record(
                &mut data,
                i,
                WalRecordType::Command,
                b"*1\r\n$4\r\nPING\r\n",
            );
        }
        std::fs::write(wal_dir.join("000000000001.wal"), &data).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
        let result =
            recover_shard_v3_pitr(&mut databases, 0, &shard_dir, &engine, None, Some(5)).unwrap();

        assert_eq!(
            result.commands_replayed, 5,
            "PITR target=5 should apply LSN 1..=5 only"
        );
        assert_eq!(
            result.last_lsn, 5,
            "control file's wal_flush_lsn must reflect the PITR cutoff"
        );

        // Control file must be written with the truncated last_lsn so a
        // subsequent restart (without target) doesn't re-apply records 6..10.
        let ctl = ShardControlFile::read(&ShardControlFile::control_path(&shard_dir, 0)).unwrap();
        assert_eq!(ctl.wal_flush_lsn, 5);
    }

    /// P3b — PITR with target_lsn = None must reproduce the classic
    /// recovery path bit-for-bit.
    #[test]
    fn test_recover_shard_v3_pitr_none_matches_classic() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let wal_dir = shard_dir.join("wal-v3");
        std::fs::create_dir_all(&wal_dir).unwrap();

        let mut data = make_v3_header(0);
        for i in 1..=4u64 {
            write_wal_v3_record(
                &mut data,
                i,
                WalRecordType::Command,
                b"*1\r\n$4\r\nPING\r\n",
            );
        }
        std::fs::write(wal_dir.join("000000000001.wal"), &data).unwrap();

        let mut dbs_pitr = vec![Database::new()];
        let mut dbs_classic = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();

        let pitr =
            recover_shard_v3_pitr(&mut dbs_pitr, 0, &shard_dir, &engine, None, None).unwrap();
        // Use a fresh shard dir for the classic side so its control file
        // doesn't reflect the PITR side's state.
        let shard_dir2 = tmp.path().join("shard-0-classic");
        let wal_dir2 = shard_dir2.join("wal-v3");
        std::fs::create_dir_all(&wal_dir2).unwrap();
        std::fs::write(wal_dir2.join("000000000001.wal"), &data).unwrap();
        let classic = recover_shard_v3(&mut dbs_classic, 0, &shard_dir2, &engine).unwrap();

        assert_eq!(pitr.commands_replayed, classic.commands_replayed);
        assert_eq!(pitr.last_lsn, classic.last_lsn);
        assert_eq!(pitr.fpi_applied, classic.fpi_applied);
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
        fpi_payload.extend_from_slice(&[0xABu8; 128]); // page_data
        write_wal_v3_record(&mut data, 2, WalRecordType::FullPageImage, &fpi_payload);
        std::fs::write(wal_dir.join("000000000001.wal"), &data).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
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
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
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
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
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
            db_index: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: 1,
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
            db_index: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: 2,
        });
        manifest.commit().unwrap();
        drop(manifest);

        // Create the segment directory with codes.mpf
        let seg_dir = shard_dir.join("vectors").join("segment-42");
        std::fs::create_dir_all(&seg_dir).unwrap();
        std::fs::write(seg_dir.join("codes.mpf"), [0u8; 64]).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
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
            db_index: 0,
            max_key_hash: u64::MAX,
            last_modified_lsn: 1,
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
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
        let result = recover_shard_v3(&mut databases, 0, &shard_dir, &engine).unwrap();

        // Cold index recovers all 3 keys -- same count as before the fix,
        // just no longer synonymous with "loaded into hot RAM".
        assert_eq!(result.kv_heap_entries_loaded, 3);

        // Task #56 (used_memory truthfulness): recovery must NOT hot-load
        // spilled String entries into the DashTable. Before the fix, this is
        // exactly where a restart double-booked memory -- every spilled
        // key came back fully charged to `used_memory` in one shot, with no
        // eviction pass in between. `resident_bytes()` staying at the tiny
        // ColdIndex-only overhead (not the full value payloads) is the
        // regression guard.
        assert!(
            !databases[0].is_hot(b"key1"),
            "key1 must recover as a cold-only stub, not a hot RAM entry"
        );
        assert!(!databases[0].is_hot(b"key2"), "key2 must recover cold-only");
        assert!(!databases[0].is_hot(b"key3"), "key3 must recover cold-only");
        let cold_only_bytes = databases[0].resident_bytes();
        assert!(
            cold_only_bytes < 200,
            "resident_bytes() should reflect only the tiny ColdIndex overhead \
             right after recovery (no hot entries yet), got {cold_only_bytes}"
        );

        // Cold read-through still serves every key transparently -- it's
        // just lazy now: the first touch promotes it into hot RAM (and only
        // then charges `used_memory`), exactly like ordinary (non-restart)
        // eviction + read-through already works for every other value type.
        assert!(
            databases[0].get(b"key1").is_some(),
            "key1 should be readable via cold read-through"
        );
        assert!(
            databases[0].get(b"key2").is_some(),
            "key2 should be readable via cold read-through"
        );
        assert!(
            databases[0].get(b"key3").is_some(),
            "key3 should be readable via cold read-through"
        );
        assert!(
            databases[0].is_hot(b"key1"),
            "key1 must be promoted to hot RAM after a GET"
        );
    }

    // ── Legacy-dir AOF authority + WAL v3 last-resort fallback (review
    // finding P0#1, corrected) ──────────────────────────────────────────────
    //
    // The AOF is the recovery authority for the legacy dir: post-#211,
    // WAL v3's KV coverage is intentionally partial (`--wal-kv-log`
    // default-off; connection-local writes bypass it even when on), so a
    // non-empty legacy-mode WAL v3 directory (`v2_dir/shard-N/wal-v3/`) must
    // never shadow `appendonly.aof`. WAL v3 is only a last-resort source
    // when NO AOF exists. A raw SIGKILL integration test cannot exercise
    // this deterministically (see the NOTE in
    // `tests/crash_matrix_per_shard_aof.rs`), so these test the exact
    // function directly instead -- the same style every other test in this
    // module already uses (construct WAL v3 segments by hand via
    // `write_wal_v3_record`, assert on `commands_replayed`).

    #[test]
    fn test_legacy_aof_is_authority_over_wal_v3() {
        let tmp = tempfile::tempdir().unwrap();
        // Disk-offload shard_dir: empty (no wal-v3 data) so Phase 4 replays 0
        // commands and Phase 4b's fallback engages.
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Legacy (non-disk-offload) persistence dir: has BOTH a populated
        // `shard-0/wal-v3/` (5 records) and a shorter `appendonly.aof`
        // (2 records). Post-#211 the WAL routinely holds MORE records than
        // the AOF while missing KV writes entirely — the AOF must win.
        let v2_dir = tmp.path().join("legacy");
        let legacy_wal_dir = v2_dir.join("shard-0").join("wal-v3");
        std::fs::create_dir_all(&legacy_wal_dir).unwrap();
        let mut wal_data = make_v3_header(0);
        for i in 1..=5u64 {
            write_wal_v3_record(
                &mut wal_data,
                i,
                WalRecordType::Command,
                b"*1\r\n$4\r\nPING\r\n",
            );
        }
        std::fs::write(legacy_wal_dir.join("000000000001.wal"), &wal_data).unwrap();

        std::fs::create_dir_all(&v2_dir).unwrap();
        let aof_payload = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"; // 2 records
        std::fs::write(v2_dir.join("appendonly.aof"), aof_payload).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
        let result =
            recover_shard_v3_with_fallback(&mut databases, 0, &shard_dir, &engine, Some(&v2_dir))
                .unwrap();

        assert_eq!(
            result.commands_replayed, 2,
            "appendonly.aof (2 records, the authority) must be replayed and \
             the KV-incomplete legacy WAL v3 (5 records) ignored -- got {} \
             (the WAL shadowed the AOF if this is 5)",
            result.commands_replayed
        );
    }

    #[test]
    fn test_spill_lifecycle_records_do_not_suppress_aof_fallback() {
        // Disk-offload spills write FileCreate/FileDelete/FileTierChange
        // records into the shard's own wal-v3 — but NO KV Command records
        // (`--wal-kv-log` is auto-off when the AOF is the authority). The
        // Phase 4b gate must key on KV Command records only: gating on the
        // aggregate count made any post-spill restart skip the AOF entirely,
        // losing every KV write since the last snapshot (and resurrecting
        // deleted cold keys, since the AOF'd DELs never replayed).
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        let offload_wal_dir = shard_dir.join("wal-v3");
        std::fs::create_dir_all(&offload_wal_dir).unwrap();
        let mut wal_data = make_v3_header(0);
        // One spill lifecycle record: 16-byte payload (file_id + page_offset).
        write_wal_v3_record(&mut wal_data, 1, WalRecordType::FileCreate, &[0u8; 16]);
        std::fs::write(offload_wal_dir.join("000000000001.wal"), &wal_data).unwrap();

        let v2_dir = tmp.path().join("legacy");
        std::fs::create_dir_all(&v2_dir).unwrap();
        let aof_payload = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"; // 2 KV records
        std::fs::write(v2_dir.join("appendonly.aof"), aof_payload).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
        let result =
            recover_shard_v3_with_fallback(&mut databases, 0, &shard_dir, &engine, Some(&v2_dir))
                .unwrap();

        assert_eq!(
            result.commands_replayed, 3,
            "1 FileCreate lifecycle record + 2 AOF commands must all be \
             replayed -- got {} (1 means the lifecycle record suppressed the \
             AOF fallback and every KV write was dropped)",
            result.commands_replayed
        );
    }

    #[test]
    fn test_legacy_wal_v3_last_resort_when_no_aof() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Legacy dir has ONLY a populated WAL v3 — no appendonly.aof at all
        // (lost/rebuilt). Partial recovery beats none.
        let v2_dir = tmp.path().join("legacy");
        let legacy_wal_dir = v2_dir.join("shard-0").join("wal-v3");
        std::fs::create_dir_all(&legacy_wal_dir).unwrap();
        let mut wal_data = make_v3_header(0);
        for i in 1..=5u64 {
            write_wal_v3_record(
                &mut wal_data,
                i,
                WalRecordType::Command,
                b"*1\r\n$4\r\nPING\r\n",
            );
        }
        std::fs::write(legacy_wal_dir.join("000000000001.wal"), &wal_data).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
        let result =
            recover_shard_v3_with_fallback(&mut databases, 0, &shard_dir, &engine, Some(&v2_dir))
                .unwrap();

        assert_eq!(
            result.commands_replayed, 5,
            "with no appendonly.aof, the legacy WAL v3 last-resort fallback \
             must replay what it can"
        );
    }

    #[test]
    fn test_legacy_wal_v3_absent_falls_back_to_aof() {
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        // Legacy dir has ONLY an AOF -- no `shard-0/wal-v3/` at all (the
        // common case: disk-offload was never used and `--wal-kv-log` never
        // populated WAL v3, so the AOF is genuinely the only source).
        let v2_dir = tmp.path().join("legacy");
        std::fs::create_dir_all(&v2_dir).unwrap();
        let aof_payload = b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
        std::fs::write(v2_dir.join("appendonly.aof"), aof_payload).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
        let result =
            recover_shard_v3_with_fallback(&mut databases, 0, &shard_dir, &engine, Some(&v2_dir))
                .unwrap();

        assert_eq!(
            result.commands_replayed, 3,
            "with no legacy WAL v3 data at all, the AOF fallback must still \
             work exactly as before this fix"
        );
    }

    #[test]
    fn test_stray_legacy_v2_wal_file_does_not_break_recovery() {
        // A leftover pre-freeze WAL v2 file (`shard-0.wal`, the old flat
        // `RRDWAL` version=2 format) must not be replayed (WAL v2 support was
        // removed) and must not crash or otherwise break recovery from the
        // AOF -- it should just be loudly logged (see the `tracing::error!`
        // in the Phase 4b block above) and ignored.
        let tmp = tempfile::tempdir().unwrap();
        let shard_dir = tmp.path().join("shard-0");
        std::fs::create_dir_all(&shard_dir).unwrap();

        let v2_dir = tmp.path().join("legacy");
        std::fs::create_dir_all(&v2_dir).unwrap();
        // Stray pre-freeze v2 WAL file: RRDWAL magic + version=2 header.
        let mut legacy_v2_wal = vec![0u8; 32];
        legacy_v2_wal[0..6].copy_from_slice(b"RRDWAL");
        legacy_v2_wal[6] = 2;
        std::fs::write(v2_dir.join("shard-0.wal"), &legacy_v2_wal).unwrap();
        // A real AOF is still present and must still be used.
        let aof_payload = b"*1\r\n$4\r\nPING\r\n";
        std::fs::write(v2_dir.join("appendonly.aof"), aof_payload).unwrap();

        let mut databases = vec![Database::new()];
        let engine = crate::persistence::replay::DispatchReplayEngine::new();
        let result =
            recover_shard_v3_with_fallback(&mut databases, 0, &shard_dir, &engine, Some(&v2_dir))
                .unwrap();

        assert_eq!(
            result.commands_replayed, 1,
            "a stray legacy v2 WAL file must not prevent the AOF fallback \
             from working"
        );
    }
}
