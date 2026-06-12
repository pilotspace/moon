//! AOF writer tasks: single-file and per-shard background append loops.
#![allow(unused_imports, unused_variables, unreachable_code, clippy::empty_loop)]

use super::rewrite::{
    do_rewrite_per_shard, drain_pending_appends_framed, rewrite_aof_sharded_sync,
    sync_and_fulfill_drain,
};
use super::*;
// `do_rewrite_single` / `do_rewrite_sharded` exist only under the monoio runtime.
#[cfg(feature = "runtime-monoio")]
use super::rewrite::{do_rewrite_sharded, do_rewrite_single};

/// Background AOF writer task. Receives commands via mpsc channel and appends them
/// to the AOF file. Handles fsync according to the configured policy.
///
/// `fold_channels` — for the TopLevel `--shards 1` path: an optional
/// `(fold_producer, fold_notifier)` pair for shard 0, wired by `main.rs` via
/// `set_fold_channels` + extracted here.  When `Some`, `do_rewrite_sharded`
/// uses the C4 cooperative-snapshot fold; when `None` (legacy, no AOF fold
/// wired), `do_rewrite_sharded` falls back to an error-abort so the old
/// generation stays authoritative rather than deadlocking.
pub async fn aof_writer_task(
    rx: channel::MpscReceiver<AofMessage>,
    aof_path: PathBuf,
    fsync: FsyncPolicy,
    cancel: CancellationToken,
    fold_channels: Option<(
        Arc<parking_lot::Mutex<ringbuf::HeapProd<crate::shard::dispatch::ShardMessage>>>,
        Arc<crate::runtime::channel::Notify>,
    )>,
) {
    #[cfg(feature = "runtime-tokio")]
    use tokio::io::AsyncWriteExt;

    // Open file in append mode (create if not exists)
    #[cfg(feature = "runtime-tokio")]
    let file: tokio::fs::File = match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&aof_path)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open AOF file {}: {}", aof_path.display(), e);
            return;
        }
    };

    #[cfg(feature = "runtime-tokio")]
    let mut writer = tokio::io::BufWriter::new(file);
    #[cfg(feature = "runtime-tokio")]
    let mut last_fsync = Instant::now();
    #[cfg(feature = "runtime-tokio")]
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
    #[cfg(feature = "runtime-tokio")]
    interval.tick().await; // consume first tick

    // Monoio path: multi-part AOF (base RDB + incremental RESP) with sync I/O.
    //
    // On startup, if appendonlydir/ exists with a manifest, open the current
    // incr file for appending. Otherwise start fresh with seq 1.
    // On BGREWRITEAOF: snapshot → write new base RDB → create new incr → advance manifest.
    #[cfg(feature = "runtime-monoio")]
    {
        use crate::persistence::aof_manifest::AofManifest;
        use std::io::Write;

        // Resolve the persistence base directory from aof_path's parent.
        let base_dir = aof_path.parent().unwrap_or(Path::new(".")).to_path_buf();

        // Load manifest — do NOT create one here if it doesn't exist.
        // main.rs recovery runs concurrently and must finish before a manifest
        // is created, to avoid racing against legacy single-file AOF detection.
        // main.rs will create the manifest after recovery completes.
        //
        // A corrupt manifest is fatal — exit the writer so the server startup
        // notices and fails loud rather than silently overwriting.
        //
        // Bounded wait: check the cancellation token each iteration and enforce
        // a hard timeout so the writer doesn't spin forever if main.rs fails to
        // create the manifest (e.g. disk full, permission error).
        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let mut manifest = loop {
            if cancel.is_cancelled() {
                info!("AOF writer: cancelled while waiting for manifest");
                return;
            }
            if manifest_wait_start.elapsed() > MANIFEST_TIMEOUT {
                error!(
                    "AOF writer: manifest not found at {} after {:?}. Writer exiting; check recovery logs.",
                    base_dir.display(),
                    MANIFEST_TIMEOUT,
                );
                return;
            }
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
                    // main.rs recovery hasn't created the manifest yet — wait.
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => {
                    error!(
                        "AOF manifest corrupt at {}: {}. Writer exiting; persistence disabled.",
                        base_dir.display(),
                        e
                    );
                    return;
                }
            }
        };

        // Open the current incremental file for appending
        let incr_path = manifest.incr_path();
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr_path)
        {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "Failed to open AOF incr file {}: {}",
                    incr_path.display(),
                    e
                );
                return;
            }
        };
        info!(
            "AOF writer: seq {}, incr={}",
            manifest.seq,
            incr_path.display()
        );

        let mut last_fsync = Instant::now();

        let mut write_error = false;

        // Test-only fault injection: same env var as the PerShard writer.
        // Read once at task startup; zero cost in production (var absent).
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        loop {
            match rx.recv() {
                // TopLevel writer: legacy v1 disk format is plain RESP. The
                // LSN is ignored — TopLevel is single-shard so per-shard merge
                // by LSN is moot.
                Ok(AofMessage::Append {
                    bytes: data,
                    lsn: _,
                }) => {
                    if write_error {
                        continue; // Drop appends after persistent I/O failure
                    }
                    if let Err(e) = file.write_all(&data) {
                        error!(
                            "AOF write failed (seq {}): {}. Persistence degraded.",
                            manifest.seq, e
                        );
                        write_error = true;
                        continue;
                    }
                    match fsync {
                        FsyncPolicy::Always => {
                            let t = Instant::now();
                            if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                error!("AOF sync failed (seq {}, always): {}", manifest.seq, e);
                                write_error = true;
                            } else {
                                crate::admin::metrics_setup::record_aof_fsync(
                                    t.elapsed().as_micros() as u64,
                                );
                            }
                        }
                        FsyncPolicy::EverySec => {
                            if last_fsync.elapsed() >= std::time::Duration::from_secs(1) {
                                let t = Instant::now();
                                if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                                    error!(
                                        "AOF sync failed (seq {}, everysec): {}",
                                        manifest.seq, e
                                    );
                                    // Non-fatal for everysec: retry next interval
                                } else {
                                    crate::admin::metrics_setup::record_aof_fsync(
                                        t.elapsed().as_micros() as u64,
                                    );
                                    last_fsync = Instant::now();
                                }
                            }
                        }
                        FsyncPolicy::No => {}
                    }
                }
                // TopLevel writer (monoio): legacy v1 plain RESP, lsn ignored.
                // AppendSync ALWAYS fsyncs and acks before returning, regardless
                // of the configured policy — that's the durability contract the
                // caller signed up for by choosing AppendSync.
                Ok(AofMessage::AppendSync {
                    bytes: data,
                    lsn: _,
                    ack,
                }) => {
                    if write_error {
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    // Test-only: return FsyncFailed immediately without touching disk.
                    if fail_fsync_for_test {
                        let _ = ack.send(AofAck::FsyncFailed);
                        continue;
                    }
                    if let Err(e) = file.write_all(&data) {
                        error!(
                            "AOF AppendSync write failed (seq {}): {}. Persistence degraded.",
                            manifest.seq, e
                        );
                        write_error = true;
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    let t = Instant::now();
                    if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                        error!("AOF AppendSync sync failed (seq {}): {}", manifest.seq, e);
                        write_error = true;
                        let _ = ack.send(AofAck::FsyncFailed);
                    } else {
                        crate::admin::metrics_setup::record_aof_fsync(
                            t.elapsed().as_micros() as u64
                        );
                        let _ = ack.send(AofAck::Synced);
                    }
                }
                Ok(AofMessage::Shutdown) | Err(_) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!("AOF final sync failed (seq {}): {}", manifest.seq, e);
                        }
                    }
                    info!("AOF writer shutting down (monoio, seq {})", manifest.seq);
                    break;
                }
                Ok(AofMessage::Rewrite(db)) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!("AOF pre-rewrite sync failed (seq {}): {}", manifest.seq, e);
                        }
                    }
                    match do_rewrite_single(&db, &mut manifest, &mut file, &rx) {
                        Ok(()) => {
                            write_error = false; // Reset on successful rewrite
                        }
                        Err(e) => error!("AOF rewrite failed (seq {}): {}", manifest.seq, e),
                    }
                    crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                }
                Ok(AofMessage::RewriteSharded(shard_dbs)) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!("AOF pre-rewrite sync failed (seq {}): {}", manifest.seq, e);
                        }
                    }
                    // C4 TopLevel cooperative fold: pass the wired fold channels
                    // (producer + notifier for shard 0) so do_rewrite_sharded can
                    // use the AofFold SPSC protocol instead of the deleted RwLock
                    // path.  `fold_channels` is `None` only if main.rs failed to
                    // wire them at startup (Arc::get_mut race — logged at boot).
                    match do_rewrite_sharded(
                        &shard_dbs,
                        &mut manifest,
                        &mut file,
                        &rx,
                        fold_channels.as_ref(),
                    ) {
                        Ok(()) => {
                            write_error = false;
                        }
                        Err(e) => error!("AOF rewrite failed (seq {}): {}", manifest.seq, e),
                    }
                    crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                        .store(false, std::sync::atomic::Ordering::SeqCst);
                }
                // [F6] A TopLevel writer never owns per-shard files; receiving
                // RewritePerShard means a routing bug. Self-abort so the
                // coordinator's countdown completes and the flag clears.
                Ok(AofMessage::RewritePerShard { coord, .. }) => {
                    warn!("AOF TopLevel writer received RewritePerShard — routing bug; aborting");
                    coord.mark_failed();
                    coord.shard_done();
                }
            }
        }
        return;
    }

    loop {
        #[cfg(feature = "runtime-tokio")]
        tokio::select! {
            msg = rx.recv_async() => {
                match msg {
                    // TopLevel writer (tokio): legacy v1 plain RESP, lsn ignored.
                    Ok(AofMessage::Append { bytes: data, lsn: _ }) => {
                        if let Err(e) = writer.write_all(&data).await {
                            error!("AOF write error: {}", e);
                            continue;
                        }
                        match fsync {
                            FsyncPolicy::Always => {
                                let _ = writer.flush().await;
                                let _ = writer.get_ref().sync_data().await;
                            }
                            FsyncPolicy::EverySec | FsyncPolicy::No => {
                                // EverySec handled by interval tick below; No does nothing
                            }
                        }
                    }
                    // AppendSync: write + fsync + ack, regardless of policy.
                    Ok(AofMessage::AppendSync { bytes: data, lsn: _, ack }) => {
                        if let Err(e) = writer.write_all(&data).await {
                            error!("AOF AppendSync write error: {}", e);
                            let _ = ack.send(AofAck::WriteFailed);
                            continue;
                        }
                        if let Err(e) = writer.flush().await {
                            error!("AOF AppendSync flush error: {}", e);
                            let _ = ack.send(AofAck::FsyncFailed);
                            continue;
                        }
                        if let Err(e) = writer.get_ref().sync_data().await {
                            error!("AOF AppendSync sync_data error: {}", e);
                            let _ = ack.send(AofAck::FsyncFailed);
                            continue;
                        }
                        let _ = ack.send(AofAck::Synced);
                    }
                    Ok(AofMessage::Rewrite(db)) => {
                        // Flush current writer before rewrite
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;

                        if let Err(e) = rewrite_aof(db, &aof_path).await {
                            error!("AOF rewrite failed: {}", e);
                        }
                        crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                            .store(false, std::sync::atomic::Ordering::SeqCst);

                        // Reopen file after rewrite (it was replaced)
                        let reopen_result: Result<tokio::fs::File, _> = tokio::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&aof_path)
                            .await;
                        match reopen_result {
                            Ok(f) => {
                                writer = tokio::io::BufWriter::new(f);
                            }
                            Err(e) => {
                                error!("Failed to reopen AOF file after rewrite: {}", e);
                                return;
                            }
                        }
                    }
                    Ok(AofMessage::RewriteSharded(shard_dbs)) => {
                        // C4 TopLevel cooperative fold (tokio path):
                        // flush + sync the BufWriter, convert to std::fs::File
                        // for the sync fold (same pattern as tokio per-shard),
                        // then reopen for appending.
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;
                        let mut sf = writer.into_inner().into_std().await;
                        if let Err(e) = rewrite_aof_sharded_sync(
                            &shard_dbs,
                            &aof_path,
                            &rx,
                            &mut sf,
                            fold_channels.as_ref(),
                        ) {
                            error!("AOF rewrite (sharded) failed: {}", e);
                        }
                        // Drop sf — caller will reopen aof_path below.
                        drop(sf);
                        crate::command::persistence::AOF_REWRITE_IN_PROGRESS
                            .store(false, std::sync::atomic::Ordering::SeqCst);
                        let reopen_result: Result<tokio::fs::File, _> = tokio::fs::OpenOptions::new()
                            .create(true).append(true).open(&aof_path).await;
                        match reopen_result {
                            Ok(f) => writer = tokio::io::BufWriter::new(f),
                            Err(e) => { error!("Failed to reopen AOF after rewrite: {}", e); return; }
                        }
                    }
                    // [F6] TopLevel writer never owns per-shard files — routing
                    // bug. Self-abort so the countdown completes + flag clears.
                    Ok(AofMessage::RewritePerShard { coord, .. }) => {
                        warn!("AOF TopLevel writer received RewritePerShard — routing bug; aborting");
                        coord.mark_failed();
                        coord.shard_done();
                    }
                    Ok(AofMessage::Shutdown) | Err(_) => {
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;
                        info!("AOF writer shutting down");
                        break;
                    }
                }
            }
            _ = interval.tick(), if fsync == FsyncPolicy::EverySec => {
                if last_fsync.elapsed() >= std::time::Duration::from_secs(1) {
                    let _ = writer.flush().await;
                    let _ = writer.get_ref().sync_data().await;
                    last_fsync = Instant::now();
                }
            }
            _ = cancel.cancelled() => {
                let _ = writer.flush().await;
                let _ = writer.get_ref().sync_data().await;
                info!("AOF writer cancelled");
                break;
            }
        }
    }
}

/// Background per-shard AOF writer task (Option B step 2b).
///
/// One instance is spawned per shard in `PerShard` layout. Each instance owns
/// `appendonlydir/shard-{shard_id}/moon.aof.{seq}.incr.aof` exclusively — no
/// other writer touches that file, so there is no per-file locking.
///
/// Differences from [`aof_writer_task`] (TopLevel):
/// - Opens `manifest.shard_incr_path(shard_id)` instead of `manifest.incr_path()`.
/// - `Rewrite`/`RewriteSharded` variants are rejected (logged + dropped).
///   The legacy single-writer rewrite enum has no meaning when each shard
///   owns its own files; per-shard BGREWRITEAOF lands in RFC step 6.
/// - Refuses to start if the loaded manifest's layout is `TopLevel` — the
///   spawn site (step 2f) must only invoke this task body for `PerShard`
///   layouts. Mismatch is a programmer error.
///
/// Wait/timeout/corruption semantics for manifest loading match the existing
/// `aof_writer_task` (60s bounded wait, hard fail on corrupt manifest).
/// Test-only torn-write injection for `per_shard_aof_writer_task`: when set to a
/// nonzero `N`, the `N`-th `Append` received by a tokio per-shard writer writes
/// its header and then simulates a payload write failure, exercising the
/// `write_error` latch. `0` disables. Atomic (not an env var) because
/// `std::env::set_var` is `unsafe` under edition 2024. Compiled out of release.
#[cfg(all(test, feature = "runtime-tokio"))]
pub(crate) static TEST_FAIL_WRITE_AT: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub async fn per_shard_aof_writer_task(
    rx: channel::MpscReceiver<AofMessage>,
    base_dir: PathBuf,
    shard_id: u16,
    fsync: FsyncPolicy,
    cancel: CancellationToken,
) {
    #[cfg(feature = "runtime-tokio")]
    {
        use crate::persistence::aof_manifest::{AofLayout, AofManifest};
        use tokio::io::AsyncWriteExt;

        // Wait for main.rs recovery to create/load the manifest.
        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let manifest = loop {
            if cancel.is_cancelled() {
                info!(
                    "AOF writer shard {}: cancelled while waiting for manifest",
                    shard_id
                );
                return;
            }
            if manifest_wait_start.elapsed() > MANIFEST_TIMEOUT {
                error!(
                    "AOF writer shard {}: manifest not found at {} after {:?}. Writer exiting.",
                    shard_id,
                    base_dir.display(),
                    MANIFEST_TIMEOUT,
                );
                return;
            }
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(e) => {
                    error!(
                        "AOF writer shard {}: manifest corrupt at {}: {}. Persistence disabled.",
                        shard_id,
                        base_dir.display(),
                        e
                    );
                    return;
                }
            }
        };

        if manifest.layout != AofLayout::PerShard {
            error!(
                "AOF writer shard {}: layout is {:?}, expected PerShard. \
                 per_shard_aof_writer_task should only be spawned for PerShard layouts. \
                 Writer exiting.",
                shard_id, manifest.layout
            );
            return;
        }
        if (shard_id as usize) >= manifest.shards.len() {
            error!(
                "AOF writer shard {}: out of range for manifest with {} shards. Writer exiting.",
                shard_id,
                manifest.shards.len()
            );
            return;
        }

        let incr_path = manifest.shard_incr_path(shard_id);
        // Ensure shard-{N}/ exists. The manifest constructor for PerShard
        // already creates these, but be defensive — a manual deletion or
        // a manifest written by an older binary could leave them missing.
        if let Some(parent) = incr_path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!(
                    "AOF writer shard {}: failed to create dir {}: {}",
                    shard_id,
                    parent.display(),
                    e
                );
                return;
            }
        }
        let file: tokio::fs::File = match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr_path)
            .await
        {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "AOF writer shard {}: failed to open incr {}: {}",
                    shard_id,
                    incr_path.display(),
                    e
                );
                return;
            }
        };
        info!(
            "AOF writer shard {}: seq {}, incr={}",
            shard_id,
            manifest.seq,
            incr_path.display()
        );

        let mut writer = tokio::io::BufWriter::new(file);
        let mut last_fsync = Instant::now();
        // (No `interval` here: the EverySec flush deadline is enforced by the
        // timeout-bounded recv in the loop below, which wakes at least every
        // 200ms regardless of message traffic. A long-lived `interval.tick()`
        // select arm is fairness-starvable under sustained writes and proved
        // unreliable when idle on this dedicated current-thread writer runtime.)

        // Test-only fault injection: if MOON_TEST_AOF_FSYNC_FAIL=1 is set in
        // the environment at writer task startup, every AppendSync ack resolves
        // as FsyncFailed instead of Synced. This lets integration tests exercise
        // the AOF_FSYNC_ERR response path without requiring a real disk error.
        // The env var is read once here (not per-message) so it costs zero on the
        // hot path in production deployments where the var is absent.
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        // Torn-write latch: once any write to this incr file fails partway
        // (e.g. the header landed but the payload did not), we must NEVER write
        // another record — a lone orphaned header makes the framed reader
        // misinterpret the next record's bytes as the orphan's payload,
        // corrupting every record after it on replay. Stay latched for the
        // writer's lifetime; recovery replays the clean prefix and a rewrite
        // starts a fresh file. This mirrors the single-file (line ~1467) and
        // monoio per-shard (line ~2125) writers, which already carry the latch.
        let mut write_error = false;
        // Test-only fault injection (no env var: edition-2024 set_var is unsafe).
        // When `TEST_FAIL_WRITE_AT` is the ordinal of an incoming Append, that
        // append writes its header then simulates a payload failure, exercising
        // the latch. Compiled out of production builds.
        #[cfg(test)]
        let mut test_append_ordinal: usize = 0;

        loop {
            tokio::select! {
                // Bounded recv (EverySec durability): wake at least every 200ms
                // even when idle so the flush deadline after this select! is
                // honored within its 1s bound. flume's recv future is drop-safe
                // on the Elapsed branch (no message consumed on timeout); the
                // Ok(Ok(msg)) path below captures the message with no loss.
                r = tokio::time::timeout(
                    std::time::Duration::from_millis(200),
                    rx.recv_async(),
                ) => {
                    // On Elapsed (timeout) `r` is Err: skip the match and fall
                    // through to the EverySec deadline check after this select!.
                    if let Ok(msg) = r {
                    match msg {
                        // PerShard writer (tokio): per RFC § 2 Rule 1 the on-disk
                        // format is `[u64 lsn LE][u32 len LE][RESP bytes]`. Header
                        // is written sequentially with the body — both calls land
                        // in the same BufWriter so this is one syscall under load.
                        Ok(AofMessage::Append { lsn, bytes: data }) => {
                            // Latch: stream already torn — drop silently (Append
                            // is fire-and-forget; no ack channel to notify).
                            if write_error {
                                continue;
                            }
                            #[cfg(test)]
                            {
                                test_append_ordinal += 1;
                                let fail_at = TEST_FAIL_WRITE_AT
                                    .load(std::sync::atomic::Ordering::Relaxed);
                                if fail_at != 0 && fail_at == test_append_ordinal {
                                    // Reproduce a torn write: header lands, payload
                                    // "fails". The orphaned header is flushed so the
                                    // on-disk effect matches the real I/O-error case.
                                    let mut header = [0u8; 12];
                                    header[..8].copy_from_slice(&lsn.to_le_bytes());
                                    header[8..]
                                        .copy_from_slice(&(data.len() as u32).to_le_bytes());
                                    let _ = writer.write_all(&header).await;
                                    let _ = writer.flush().await;
                                    error!(
                                        "AOF shard {}: injected torn write after header (test)",
                                        shard_id
                                    );
                                    write_error = true;
                                    continue;
                                }
                            }
                            let mut header = [0u8; 12];
                            header[..8].copy_from_slice(&lsn.to_le_bytes());
                            header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                            if let Err(e) = writer.write_all(&header).await {
                                error!("AOF header write error shard {}: {}", shard_id, e);
                                write_error = true;
                                continue;
                            }
                            if let Err(e) = writer.write_all(&data).await {
                                error!("AOF write error shard {}: {}", shard_id, e);
                                write_error = true;
                                continue;
                            }
                            if matches!(fsync, FsyncPolicy::Always) {
                                let _ = writer.flush().await;
                                let _ = writer.get_ref().sync_data().await;
                            }
                        }
                        // AppendSync (tokio + PerShard): framed write + fsync + ack.
                        Ok(AofMessage::AppendSync { lsn, bytes: data, ack }) => {
                            // Latch: stream already torn — refuse to write more and
                            // report failure so the caller does not hang to the F2
                            // timeout and does not ack a write into a corrupt stream.
                            if write_error {
                                let _ = ack.send(AofAck::WriteFailed);
                                continue;
                            }
                            // H1-BARRIER: a zero-length AppendSync is an fsync
                            // barrier (pool::fsync_barrier) — fsync + ack only,
                            // NO on-disk record. A len=0 framed header would make
                            // replay_incr_framed reject the file as corrupt.
                            if !data.is_empty() {
                                let mut header = [0u8; 12];
                                header[..8].copy_from_slice(&lsn.to_le_bytes());
                                header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                                if let Err(e) = writer.write_all(&header).await {
                                    error!(
                                        "AOF AppendSync header write error shard {}: {}",
                                        shard_id, e
                                    );
                                    write_error = true;
                                    let _ = ack.send(AofAck::WriteFailed);
                                    continue;
                                }
                                if let Err(e) = writer.write_all(&data).await {
                                    error!(
                                        "AOF AppendSync write error shard {}: {}",
                                        shard_id, e
                                    );
                                    write_error = true;
                                    let _ = ack.send(AofAck::WriteFailed);
                                    continue;
                                }
                            }
                            // Test-only: skip real fsync and return FsyncFailed
                            // immediately when the fault-injection env var is set.
                            if fail_fsync_for_test {
                                let _ = ack.send(AofAck::FsyncFailed);
                                continue;
                            }
                            if let Err(e) = writer.flush().await {
                                error!(
                                    "AOF AppendSync flush error shard {}: {}",
                                    shard_id, e
                                );
                                let _ = ack.send(AofAck::FsyncFailed);
                                continue;
                            }
                            if let Err(e) = writer.get_ref().sync_data().await {
                                error!(
                                    "AOF AppendSync sync_data error shard {}: {}",
                                    shard_id, e
                                );
                                let _ = ack.send(AofAck::FsyncFailed);
                                continue;
                            }
                            let _ = ack.send(AofAck::Synced);
                        }
                        Ok(AofMessage::Rewrite(_)) | Ok(AofMessage::RewriteSharded(_)) => {
                            warn!(
                                "AOF writer shard {}: received Rewrite/RewriteSharded — \
                                 not applicable in PerShard layout, dropped.",
                                shard_id
                            );
                        }
                        // [F6] Per-shard rewrite (tokio): reuse the proven
                        // synchronous fold (`do_rewrite_per_shard`) verbatim, so
                        // the exactly-once invariant carries over unchanged. This
                        // writer runs on a DEDICATED std::thread (block_on_local,
                        // main.rs) — not a shared tokio worker — so executing the
                        // blocking fold here cannot starve the runtime. We flush
                        // the BufWriter (its `into_inner` does NOT flush) so any
                        // buffered appends are durable in the OLD incr, convert
                        // `tokio::fs::File` -> `std::fs::File` for the sync fold,
                        // then wrap the (reopened) file back into the BufWriter.
                        Ok(AofMessage::RewritePerShard { shard_dbs, coord, fold_producer, fold_notifier }) => {
                            if let Err(e) = writer.flush().await {
                                error!(
                                    "F6 tokio per-shard rewrite: shard {} pre-fold flush \
                                     failed: {}. Aborting; old generation stays authoritative.",
                                    shard_id, e
                                );
                                coord.mark_failed();
                                coord.shard_done();
                            } else {
                                // `into_std().await` waits for in-flight ops and is
                                // infallible; the buffer is already flushed above.
                                let mut sf = writer.into_inner().into_std().await;
                                let res = do_rewrite_per_shard(
                                    shard_id, &shard_dbs, &mut sf, &rx, &coord,
                                    &fold_producer, &fold_notifier,
                                );
                                // `sf` is left pointing at the committed generation
                                // by the fold's internal barrier: NEW incr on
                                // success, OLD incr on abort (phase-8 rollback) or
                                // on a pre-reopen error. The fold's ShardDoneGuard
                                // already performed `shard_done` for every exit, so
                                // we MUST NOT decrement again here. Wrap `sf` back so
                                // the writer stays valid either way.
                                writer =
                                    tokio::io::BufWriter::new(tokio::fs::File::from_std(sf));
                                if let Err(e) = res {
                                    error!(
                                        "F6 tokio per-shard rewrite: shard {} fold failed: {}. \
                                         Rewrite aborted by the fold guard; old generation \
                                         stays authoritative.",
                                        shard_id, e
                                    );
                                }
                            }
                        }
                        Ok(AofMessage::Shutdown) | Err(_) => {
                            let _ = writer.flush().await;
                            let _ = writer.get_ref().sync_data().await;
                            info!("AOF writer shard {} shutting down", shard_id);
                            break;
                        }
                    }
                    }
                }
                _ = cancel.cancelled() => {
                    let _ = writer.flush().await;
                    let _ = writer.get_ref().sync_data().await;
                    info!("AOF writer shard {} cancelled", shard_id);
                    break;
                }
            }
            // EverySec deadline — checked after EVERY wake (message OR timeout),
            // so it is NOT subject to select! fairness and holds the 1s bound
            // under sustained writes as well as when idle. (The old long-lived
            // `interval.tick()` arm could be starved by the always-ready recv
            // arm under load, leaving >1s of writes buffered in the BufWriter
            // and lost on SIGKILL — the COMPOSE crash-matrix failure.)
            if fsync == FsyncPolicy::EverySec
                && last_fsync.elapsed() >= std::time::Duration::from_secs(1)
            {
                let _ = writer.flush().await;
                let _ = writer.get_ref().sync_data().await;
                last_fsync = Instant::now();
            }
        }
    }

    #[cfg(feature = "runtime-monoio")]
    {
        use crate::persistence::aof_manifest::{AofLayout, AofManifest};
        use std::io::Write;

        let manifest_wait_start = Instant::now();
        const MANIFEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
        let manifest = loop {
            if cancel.is_cancelled() {
                info!(
                    "AOF writer shard {}: cancelled while waiting for manifest",
                    shard_id
                );
                return;
            }
            if manifest_wait_start.elapsed() > MANIFEST_TIMEOUT {
                error!(
                    "AOF writer shard {}: manifest not found at {} after {:?}. Writer exiting.",
                    shard_id,
                    base_dir.display(),
                    MANIFEST_TIMEOUT,
                );
                return;
            }
            match AofManifest::load(&base_dir) {
                Ok(Some(m)) => break m,
                Ok(None) => {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => {
                    error!(
                        "AOF writer shard {}: manifest corrupt at {}: {}. Persistence disabled.",
                        shard_id,
                        base_dir.display(),
                        e
                    );
                    return;
                }
            }
        };

        if manifest.layout != AofLayout::PerShard {
            error!(
                "AOF writer shard {}: layout is {:?}, expected PerShard. Writer exiting.",
                shard_id, manifest.layout
            );
            return;
        }
        if (shard_id as usize) >= manifest.shards.len() {
            error!(
                "AOF writer shard {}: out of range for manifest with {} shards. Writer exiting.",
                shard_id,
                manifest.shards.len()
            );
            return;
        }

        let incr_path = manifest.shard_incr_path(shard_id);
        if let Some(parent) = incr_path.parent() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                error!(
                    "AOF writer shard {}: failed to create dir {}: {}",
                    shard_id,
                    parent.display(),
                    e
                );
                return;
            }
        }
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&incr_path)
        {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "AOF writer shard {}: failed to open incr {}: {}",
                    shard_id,
                    incr_path.display(),
                    e
                );
                return;
            }
        };
        info!(
            "AOF writer shard {}: seq {}, incr={}",
            shard_id,
            manifest.seq,
            incr_path.display()
        );

        let mut last_fsync = Instant::now();
        let mut write_error = false;
        let mut _dbg_processed: u64 = 0;
        let _dbg_start = Instant::now();
        // Test-only fault injection: if MOON_TEST_AOF_FSYNC_FAIL=1 is set in
        // the environment at writer task startup, every AppendSync ack resolves
        // as FsyncFailed instead of Synced. Read once before the loop so there
        // is zero cost in production deployments where the var is absent.
        let fail_fsync_for_test = std::env::var("MOON_TEST_AOF_FSYNC_FAIL").as_deref() == Ok("1");

        loop {
            // Use recv_timeout so the EverySec fsync fires even when no new
            // Appends arrive after a fold (or when the client stops writing).
            // Without a timeout, the writer blocks forever in rx.recv() and
            // the 1s fsync window never fires → data loss on kill.
            match rx.recv_timeout(std::time::Duration::from_millis(50)) {
                // AppendSync (monoio + PerShard): framed write + fsync + ack.
                Ok(AofMessage::AppendSync {
                    lsn,
                    bytes: data,
                    ack,
                }) => {
                    if write_error {
                        let _ = ack.send(AofAck::WriteFailed);
                        continue;
                    }
                    // H1-BARRIER: a zero-length AppendSync is an fsync barrier
                    // (pool::fsync_barrier) — fsync + ack only, NO on-disk
                    // record. A len=0 framed header would make
                    // replay_incr_framed reject the file as corrupt.
                    if !data.is_empty() {
                        let mut header = [0u8; 12];
                        header[..8].copy_from_slice(&lsn.to_le_bytes());
                        header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                        if let Err(e) = file.write_all(&header) {
                            error!(
                                "AOF AppendSync header write failed shard {} (seq {}): {}",
                                shard_id, manifest.seq, e
                            );
                            write_error = true;
                            let _ = ack.send(AofAck::WriteFailed);
                            continue;
                        }
                        if let Err(e) = file.write_all(&data) {
                            error!(
                                "AOF AppendSync write failed shard {} (seq {}): {}",
                                shard_id, manifest.seq, e
                            );
                            write_error = true;
                            let _ = ack.send(AofAck::WriteFailed);
                            continue;
                        }
                    }
                    // Test-only: skip real fsync and return FsyncFailed
                    // immediately when the fault-injection env var is set.
                    if fail_fsync_for_test {
                        let _ = ack.send(AofAck::FsyncFailed);
                        continue;
                    }
                    let t = Instant::now();
                    if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                        error!(
                            "AOF AppendSync sync failed shard {} (seq {}): {}",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        let _ = ack.send(AofAck::FsyncFailed);
                    } else {
                        crate::admin::metrics_setup::record_aof_fsync(
                            t.elapsed().as_micros() as u64
                        );
                        let _ = ack.send(AofAck::Synced);
                    }
                }
                // PerShard writer (monoio): framed `[u64 lsn LE][u32 len LE][RESP]`.
                // See the tokio twin above for format rationale.
                Ok(AofMessage::Append { lsn, bytes: data }) => {
                    if write_error {
                        continue;
                    }
                    _dbg_processed += 1;
                    let mut header = [0u8; 12];
                    header[..8].copy_from_slice(&lsn.to_le_bytes());
                    header[8..].copy_from_slice(&(data.len() as u32).to_le_bytes());
                    if let Err(e) = file.write_all(&header) {
                        error!(
                            "AOF header write failed shard {} (seq {}): {}. Persistence degraded.",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        continue;
                    }
                    if let Err(e) = file.write_all(&data) {
                        error!(
                            "AOF write failed shard {} (seq {}): {}. Persistence degraded.",
                            shard_id, manifest.seq, e
                        );
                        write_error = true;
                        continue;
                    }
                    // Always policy fsyncs inline. EverySec and No policies
                    // rely on the proactive fsync check AFTER the match block,
                    // which runs after every message OR timeout. Keeping EverySec
                    // out of the Append arm prevents it from advancing last_fsync
                    // after a fold completes, which would delay the next proactive
                    // fsync by a full second and open the EverySec durability window.
                    if fsync == FsyncPolicy::Always {
                        let t = Instant::now();
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!(
                                "AOF sync failed shard {} (seq {}, always): {}",
                                shard_id, manifest.seq, e
                            );
                            write_error = true;
                        } else {
                            crate::admin::metrics_setup::record_aof_fsync(
                                t.elapsed().as_micros() as u64
                            );
                        }
                    }
                }
                Ok(AofMessage::Rewrite(_)) | Ok(AofMessage::RewriteSharded(_)) => {
                    warn!(
                        "AOF writer shard {}: received Rewrite/RewriteSharded — \
                         not applicable in PerShard layout (use per-shard \
                         BGREWRITEAOF), dropped.",
                        shard_id
                    );
                }
                // [F6] Per-shard rewrite fan-out (monoio). Fold THIS shard,
                // then signal the coordinator; the last shard commits the
                // manifest. On error the old generation stays authoritative
                // (advance_shard did not commit the seq).
                Ok(AofMessage::RewritePerShard {
                    shard_dbs,
                    coord,
                    fold_producer,
                    fold_notifier,
                }) => {
                    if let Err(e) = do_rewrite_per_shard(
                        shard_id,
                        &shard_dbs,
                        &mut file,
                        &rx,
                        &coord,
                        &fold_producer,
                        &fold_notifier,
                    ) {
                        // The fold's ShardDoneGuard already marked the rewrite
                        // failed and decremented on this error exit (committing
                        // new_seq with a shard missing its new base would break
                        // recovery), so do NOT decrement again here. `file` is left
                        // on the OLD incr (error exits are pre-reopen).
                        error!(
                            "F6 per-shard rewrite: shard {} fold failed: {}. \
                             Rewrite aborted by the fold guard; old generation \
                             stays authoritative.",
                            shard_id, e
                        );
                    }
                    // EverySec post-fold drain+fsync: the fold runs synchronously
                    // and does NOT update `last_fsync`. Appends that arrived during
                    // the fold queue in the bounded AOF channel; they land on the NEW
                    // incr but are NOT fsynced until the EverySec timer fires.
                    // Strategy: drain what's currently in the channel and fsync now
                    // (covers appends that landed before this drain), then set
                    // `last_fsync` 900ms in the past so the proactive check below
                    // fires within the NEXT 100ms window (≤150ms total, since the
                    // recv_timeout is 50ms). That second fsync covers any appends
                    // that arrived between the drain and that window close.
                    // Combined, the two fsyncs bound the post-fold EverySec window
                    // to ≤150ms — well within the test's 1500ms kill margin.
                    if !write_error {
                        if let Ok(mut post_drain) =
                            drain_pending_appends_framed(&rx, &mut file, usize::MAX)
                        {
                            if let Err(e) = sync_and_fulfill_drain(
                                &mut post_drain,
                                &mut file,
                                std::path::PathBuf::from("<aof per-shard new-incr post-fold>"),
                            ) {
                                error!(
                                    "F6 per-shard rewrite: shard {} post-fold fsync \
                                     failed: {}. EverySec window open until next Append.",
                                    shard_id, e
                                );
                            } else {
                                // Back-date last_fsync by 900ms: the proactive check
                                // (threshold=1s) fires within the next 100ms, covering
                                // any appends that arrived after the drain above.
                                last_fsync = Instant::now() - std::time::Duration::from_millis(900);
                            }
                        }
                    }
                }
                Ok(AofMessage::Shutdown) | Err(flume::RecvTimeoutError::Disconnected) => {
                    if !write_error {
                        if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                            error!(
                                "AOF final sync failed shard {} (seq {}): {}",
                                shard_id, manifest.seq, e
                            );
                        }
                    }
                    info!(
                        "AOF writer shard {} shutting down (monoio, seq {}, processed {} appends in {:.3}s)",
                        shard_id,
                        manifest.seq,
                        _dbg_processed,
                        _dbg_start.elapsed().as_secs_f64()
                    );
                    break;
                }
                // Timeout: no message in the 50ms window. Fall through to
                // the EverySec proactive fsync below so queued-but-unfsynced
                // appends (e.g. after a fold with no new writes) are durable
                // within the everysec contract even if the client goes quiet.
                Err(flume::RecvTimeoutError::Timeout) => {}
            }
            // EverySec proactive fsync — runs after every loop iteration
            // (message processed OR timeout). This is the only path that
            // guarantees the 1s fsync bound when no new Appends arrive
            // (e.g. after BGREWRITEAOF completes and the client stops writing).
            if fsync == FsyncPolicy::EverySec
                && !write_error
                && last_fsync.elapsed() >= std::time::Duration::from_secs(1)
            {
                tracing::debug!(
                    "AOF EverySec proactive fsync firing shard {} (elapsed={:.3}s)",
                    shard_id,
                    last_fsync.elapsed().as_secs_f64()
                );
                let t = Instant::now();
                if let Err(e) = file.flush().and_then(|_| file.sync_data()) {
                    error!(
                        "AOF EverySec proactive sync failed shard {} (seq {}): {}",
                        shard_id, manifest.seq, e
                    );
                } else {
                    crate::admin::metrics_setup::record_aof_fsync(t.elapsed().as_micros() as u64);
                    last_fsync = Instant::now();
                }
            }
        }
    }
}
