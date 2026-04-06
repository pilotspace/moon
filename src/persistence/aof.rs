//! Append-Only File (AOF) persistence: logs every write command in RESP format
//! for crash recovery. Supports three fsync policies and AOF rewriting for compaction.
//!
//! ## Unwrap Classification
//!
//! | Context | Classification | Rationale |
//! |---------|---------------|-----------|
//! | `AofWriter::append` (hot path) | **fire-and-forget** | Channel send; no Result needed |
//! | `aof_writer_task` | **must-panic** | Writer task; errors logged inline |
//! | `replay_aof` | **should-recover** (`Result<_, MoonError>`) | Startup replay; log+skip on corruption |
//! | `rewrite_aof` | **should-recover** (`Result<_, MoonError>`) | Background rewrite; caller logs error |
//! | `#[cfg(test)]` code (55 unwraps) | **test-only** | Panics are appropriate in tests |
// Suppressions narrowed: only keep what's needed for conditional compilation
#![allow(unused_imports, unused_variables, unreachable_code, clippy::empty_loop)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use bytes::{Bytes, BytesMut};
use tracing::{error, info, warn};

use crate::error::{AofError, MoonError};
use crate::framevec;
use crate::persistence::replay::CommandReplayEngine;
use crate::protocol::{Frame, ParseConfig, parse, serialize};
use crate::storage::compact_key::CompactKey;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::db::Database;
use crate::storage::entry::{Entry, current_time_ms};
/// Type alias for the per-database RwLock container.
type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// AOF fsync policy controlling when data is flushed to disk.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FsyncPolicy {
    /// Fsync after every write command (safest, slowest).
    Always,
    /// Fsync once per second in the background (good balance).
    EverySec,
    /// Let the OS decide when to flush (fastest, least safe).
    No,
}

impl FsyncPolicy {
    /// Parse a policy string (as from config). Defaults to EverySec for unknown values.
    pub fn from_str(s: &str) -> Self {
        match s {
            "always" => FsyncPolicy::Always,
            "no" => FsyncPolicy::No,
            _ => FsyncPolicy::EverySec,
        }
    }
}

/// Messages sent to the AOF writer task via mpsc channel.
pub enum AofMessage {
    /// Append serialized RESP command bytes to the AOF file.
    Append(Bytes),
    /// Trigger a full AOF rewrite (compaction) using current database state.
    Rewrite(SharedDatabases),
    /// Trigger AOF rewrite in sharded mode (all shards' databases).
    RewriteSharded(Arc<crate::shard::shared_databases::ShardDatabases>),
    /// Shut down the AOF writer task gracefully.
    Shutdown,
}

/// Serialize a Frame into RESP wire format bytes.
pub fn serialize_command(frame: &Frame) -> Bytes {
    let mut buf = BytesMut::with_capacity(64);
    serialize::serialize(frame, &mut buf);
    buf.freeze()
}

/// Background AOF writer task. Receives commands via mpsc channel and appends them
/// to the AOF file. Handles fsync according to the configured policy.
pub async fn aof_writer_task(
    rx: channel::MpscReceiver<AofMessage>,
    aof_path: PathBuf,
    fsync: FsyncPolicy,
    cancel: CancellationToken,
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
        let mut manifest = loop {
            if let Some(m) = AofManifest::load(&base_dir) {
                break m;
            }
            // main.rs recovery hasn't created the manifest yet — wait.
            std::thread::sleep(std::time::Duration::from_millis(50));
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

        loop {
            match rx.recv() {
                Ok(AofMessage::Append(data)) => {
                    let _ = file.write_all(&data);
                    match fsync {
                        FsyncPolicy::Always => {
                            let _ = file.flush();
                            let _ = file.sync_data();
                        }
                        FsyncPolicy::EverySec => {
                            if last_fsync.elapsed() >= std::time::Duration::from_secs(1) {
                                let _ = file.flush();
                                let _ = file.sync_data();
                                last_fsync = Instant::now();
                            }
                        }
                        FsyncPolicy::No => {}
                    }
                }
                Ok(AofMessage::Shutdown) | Err(_) => {
                    let _ = file.flush();
                    let _ = file.sync_data();
                    info!("AOF writer shutting down (monoio, seq {})", manifest.seq);
                    break;
                }
                Ok(AofMessage::Rewrite(db)) => {
                    let _ = file.flush();
                    let _ = file.sync_data();
                    match do_rewrite_single(&db, &mut manifest, &mut file) {
                        Ok(()) => {}
                        Err(e) => error!("AOF rewrite failed: {}", e),
                    }
                }
                Ok(AofMessage::RewriteSharded(shard_dbs)) => {
                    let _ = file.flush();
                    let _ = file.sync_data();
                    match do_rewrite_sharded(&shard_dbs, &mut manifest, &mut file) {
                        Ok(()) => {}
                        Err(e) => error!("AOF rewrite failed: {}", e),
                    }
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
                    Ok(AofMessage::Append(data)) => {
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
                    Ok(AofMessage::Rewrite(db)) => {
                        // Flush current writer before rewrite
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;

                        if let Err(e) = rewrite_aof(db, &aof_path).await {
                            error!("AOF rewrite failed: {}", e);
                        }

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
                        let _ = writer.flush().await;
                        let _ = writer.get_ref().sync_data().await;
                        if let Err(e) = rewrite_aof_sharded_sync(&shard_dbs, &aof_path) {
                            error!("AOF rewrite (sharded) failed: {}", e);
                        }
                        let reopen_result: Result<tokio::fs::File, _> = tokio::fs::OpenOptions::new()
                            .create(true).append(true).open(&aof_path).await;
                        match reopen_result {
                            Ok(f) => writer = tokio::io::BufWriter::new(f),
                            Err(e) => { error!("Failed to reopen AOF after rewrite: {}", e); return; }
                        }
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

/// Replay an AOF file by parsing RESP commands and dispatching them.
///
/// Returns the number of commands successfully replayed.
///
/// **Corruption recovery:** On mid-stream parse errors, logs a warning with the
/// byte offset, skips to the next RESP array marker (`*`), and continues replay.
/// At EOF, reports total corrupted entries skipped. Truncated tails are handled
/// gracefully (warn + stop).
pub fn replay_aof(
    databases: &mut [Database],
    path: &Path,
    engine: &dyn CommandReplayEngine,
) -> Result<usize, MoonError> {
    let data = std::fs::read(path)?;
    if data.is_empty() {
        return Ok(0);
    }

    // Detect RDB preamble: if the file starts with "MOON" magic, load the binary
    // RDB section first, then replay any RESP commands appended after it.
    let (rdb_keys, resp_start) = if data.starts_with(b"MOON") {
        match crate::persistence::rdb::load_from_bytes(databases, &data) {
            Ok((keys, consumed)) => {
                info!(
                    "AOF RDB preamble loaded: {} keys ({} bytes)",
                    keys, consumed
                );
                (keys, consumed)
            }
            Err(e) => {
                // Data starts with MOON magic — it IS RDB format.
                // Falling back to RESP would parse garbage. Propagate the error.
                return Err(e);
            }
        }
    } else {
        (0, 0)
    };

    // If the entire file was RDB (no RESP tail), we're done
    if resp_start >= data.len() {
        return Ok(rdb_keys);
    }

    let resp_data = &data[resp_start..];
    let total_len = resp_data.len();
    let mut buf = BytesMut::from(resp_data);
    let config = ParseConfig::default();
    let mut selected_db: usize = 0;
    let mut count: usize = 0;
    let mut corruption_count: usize = 0;

    loop {
        if buf.is_empty() {
            break;
        }

        match parse::parse(&mut buf, &config) {
            Ok(Some(frame)) => {
                // Extract command name and args, then dispatch
                let (cmd, cmd_args) = match &frame {
                    Frame::Array(arr) if !arr.is_empty() => {
                        let name = match &arr[0] {
                            Frame::BulkString(s) => s.as_ref(),
                            Frame::SimpleString(s) => s.as_ref(),
                            _ => {
                                count += 1;
                                continue;
                            }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    _ => {
                        count += 1;
                        continue;
                    }
                };
                engine.replay_command(databases, cmd, cmd_args, &mut selected_db);
                count += 1;
            }
            Ok(None) => {
                // Incomplete frame at end of file - truncated AOF
                if !buf.is_empty() {
                    let offset = total_len - buf.len();
                    warn!(
                        "AOF truncated: {} unparseable bytes at offset {} (end of file)",
                        buf.len(),
                        offset
                    );
                }
                break;
            }
            Err(e) => {
                let error_offset = total_len - buf.len();
                warn!(
                    "AOF parse error at byte offset {} after {} commands: {}. Attempting skip.",
                    error_offset, count, e
                );
                corruption_count += 1;

                // Skip past the corrupt byte(s) to the next RESP array marker ('*')
                // Always discard at least 1 byte to guarantee forward progress.
                let _ = buf.split_to(1);
                if let Some(pos) = buf.iter().position(|&b| b == b'*') {
                    let _ = buf.split_to(pos);
                } else if buf.is_empty() {
                    break;
                } else {
                    // No more RESP array markers found; stop replay
                    warn!(
                        "AOF: no recoverable RESP frame found after offset {}; stopping",
                        error_offset
                    );
                    break;
                }
            }
        }
    }

    if corruption_count > 0 {
        warn!(
            "AOF replay completed with {} corrupted entries skipped, {} commands replayed",
            corruption_count, count
        );
    }

    Ok(rdb_keys + count)
}

/// Generate synthetic RESP commands from the current database state for AOF rewriting.
///
/// Produces commands for all 5 data types plus PEXPIRE for keys with TTL.
#[allow(dead_code)] // Retained for RESP-only AOF rewrite fallback and testing
pub fn generate_rewrite_commands(databases: &[Database]) -> BytesMut {
    let mut buf = BytesMut::new();
    let now_ms = current_time_ms();

    for (db_idx, db) in databases.iter().enumerate() {
        let base_ts = db.base_timestamp();
        let data = db.data();
        if data.is_empty() {
            continue;
        }

        // Generate SELECT if not db 0
        if db_idx > 0 {
            let select_frame = Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"SELECT")),
                Frame::BulkString(Bytes::from(db_idx.to_string())),
            ]);
            serialize::serialize(&select_frame, &mut buf);
        }

        for (key, entry) in data {
            // Skip expired entries
            if entry.is_expired_at(base_ts, now_ms) {
                continue;
            }

            match entry.value.as_redis_value() {
                RedisValueRef::String(val) => {
                    let frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"SET")),
                        Frame::BulkString(key.to_bytes()),
                        Frame::BulkString(Bytes::copy_from_slice(val)),
                    ]);
                    serialize::serialize(&frame, &mut buf);
                }
                RedisValueRef::Hash(map) => {
                    if map.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in map.iter() {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::HashListpack(lp) => {
                    let map = lp.to_hash_map();
                    if map.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"HSET")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (field, val) in &map {
                        args.push(Frame::BulkString(field.clone()));
                        args.push(Frame::BulkString(val.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::List(list) => {
                    if list.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"RPUSH")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for elem in list.iter() {
                        args.push(Frame::BulkString(elem.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::ListListpack(lp) => {
                    let list = lp.to_vec_deque();
                    if list.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"RPUSH")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for elem in &list {
                        args.push(Frame::BulkString(elem.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::Set(set) => {
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in set.iter() {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SetListpack(lp) => {
                    let set = lp.to_hash_set();
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in &set {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SetIntset(is) => {
                    let set = is.to_hash_set();
                    if set.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"SADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for member in &set {
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SortedSet { members, .. }
                | RedisValueRef::SortedSetBPTree { members, .. } => {
                    if members.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"ZADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (member, score) in members.iter() {
                        args.push(Frame::BulkString(Bytes::from(score.to_string())));
                        args.push(Frame::BulkString(member.clone()));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::SortedSetListpack(lp) => {
                    let pairs: Vec<_> = lp.iter_pairs().collect();
                    if pairs.is_empty() {
                        continue;
                    }
                    let mut args = vec![
                        Frame::BulkString(Bytes::from_static(b"ZADD")),
                        Frame::BulkString(key.to_bytes()),
                    ];
                    for (member_entry, score_entry) in &pairs {
                        let score_bytes = score_entry.as_bytes();
                        args.push(Frame::BulkString(Bytes::from(score_bytes)));
                        args.push(Frame::BulkString(Bytes::from(member_entry.as_bytes())));
                    }
                    serialize::serialize(&Frame::Array(args.into()), &mut buf);
                }
                RedisValueRef::Stream(stream) => {
                    for (id, fields) in &stream.entries {
                        let mut args = vec![
                            Frame::BulkString(Bytes::from_static(b"XADD")),
                            Frame::BulkString(key.to_bytes()),
                            Frame::BulkString(id.to_bytes()),
                        ];
                        for (field, value) in fields {
                            args.push(Frame::BulkString(field.clone()));
                            args.push(Frame::BulkString(value.clone()));
                        }
                        serialize::serialize(&Frame::Array(args.into()), &mut buf);
                    }
                }
            }

            // Generate PEXPIRE for keys with TTL
            if entry.has_expiry() {
                let exp_ms = entry.expires_at_ms(base_ts);
                if exp_ms > now_ms {
                    let remaining_ms = exp_ms - now_ms;
                    let pexpire_frame = Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"PEXPIRE")),
                        Frame::BulkString(key.to_bytes()),
                        Frame::BulkString(Bytes::from(remaining_ms.to_string())),
                    ]);
                    serialize::serialize(&pexpire_frame, &mut buf);
                }
            }
        }
    }

    buf
}

/// Snapshot databases and generate compacted AOF commands.
///
/// Shared by both the async (tokio) and sync (monoio) rewrite paths.
#[allow(dead_code)]
fn snapshot_and_generate(db: &SharedDatabases) -> BytesMut {
    let snapshot: Vec<(Vec<(CompactKey, Entry)>, u32)> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let base_ts = guard.base_timestamp();
            let entries = guard
                .data()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (entries, base_ts)
        })
        .collect();

    let mut temp_dbs: Vec<Database> = Vec::with_capacity(snapshot.len());
    for (entries, _base_ts) in &snapshot {
        let mut db = Database::new();
        for (key, entry) in entries {
            db.set(key.to_bytes(), entry.clone());
        }
        temp_dbs.push(db);
    }

    generate_rewrite_commands(&temp_dbs)
}

/// Multi-part rewrite: snapshot single-shard databases → RDB base → advance manifest.
#[cfg(feature = "runtime-monoio")]
fn do_rewrite_single(
    db: &SharedDatabases,
    manifest: &mut crate::persistence::aof_manifest::AofManifest,
    file: &mut std::fs::File,
) -> Result<(), MoonError> {
    let snapshot: Vec<Database> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let now_ms = current_time_ms();
            let mut temp = Database::new();
            for (k, v) in guard.data().iter() {
                if !v.is_expired_at(guard.base_timestamp(), now_ms) {
                    temp.set(k.to_bytes(), v.clone());
                }
            }
            temp
        })
        .collect();

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&snapshot)?;
    let new_incr = manifest.advance(&rdb_bytes)?;

    // Switch writer to new incr file
    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    Ok(())
}

/// Multi-part rewrite: snapshot all shards → merged RDB base → advance manifest.
#[cfg(feature = "runtime-monoio")]
fn do_rewrite_sharded(
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    manifest: &mut crate::persistence::aof_manifest::AofManifest,
    file: &mut std::fs::File,
) -> Result<(), MoonError> {
    let db_count = shard_dbs.db_count();
    let now_ms = current_time_ms();
    let mut merged_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();

    for shard_locks in shard_dbs.all_shard_dbs() {
        for (db_idx, lock) in shard_locks.iter().enumerate() {
            let guard = lock.read();
            for (key, entry) in guard.data().iter() {
                if !entry.is_expired_at(guard.base_timestamp(), now_ms) {
                    merged_dbs[db_idx].set(key.to_bytes(), entry.clone());
                }
            }
        }
    }

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&merged_dbs)?;
    let new_incr = manifest.advance(&rdb_bytes)?;

    *file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&new_incr)
        .map_err(|e| AofError::Io {
            path: new_incr,
            source: e,
        })?;

    Ok(())
}

/// Rewrite the AOF file with RDB preamble (binary base + empty RESP incremental).
///
/// Uses the same strategy as Redis 7+ `aof-use-rdb-preamble yes`:
/// the rewritten AOF starts with a full RDB snapshot (compact binary),
/// and new writes are appended as RESP after it. On startup, the loader
/// detects the RDB magic and reads the binary preamble, then switches
/// to RESP parsing for any incremental commands appended after.
#[allow(dead_code)] // Retained for legacy single-file and tokio path
fn rewrite_aof_sync(db: &SharedDatabases, aof_path: &Path) -> Result<(), MoonError> {
    // Snapshot under read locks, build temp Database objects for RDB serialization
    let snapshot: Vec<Database> = db
        .iter()
        .map(|lock| {
            let guard = lock.read();
            let mut temp = Database::new();
            let now_ms = current_time_ms();
            for (k, v) in guard.data().iter() {
                if !v.is_expired_at(guard.base_timestamp(), now_ms) {
                    temp.set(k.to_bytes(), v.clone());
                }
            }
            temp
        })
        .collect();

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&snapshot)?;

    let tmp_path = aof_path.with_extension("aof.tmp");
    std::fs::write(&tmp_path, &rdb_bytes).map_err(|e| AofError::Io {
        path: tmp_path.clone(),
        source: e,
    })?;
    std::fs::rename(&tmp_path, aof_path).map_err(|e| AofError::RewriteFailed {
        detail: format!(
            "rename {} -> {}: {}",
            tmp_path.display(),
            aof_path.display(),
            e
        ),
    })?;

    info!(
        "AOF rewrite complete (RDB preamble): {} bytes",
        rdb_bytes.len()
    );
    Ok(())
}

/// Rewrite the AOF in sharded mode with RDB preamble.
///
/// Merges all shards' databases into a single RDB snapshot, writes it as
/// the AOF base file. New incremental writes are appended as RESP after.
#[allow(dead_code)]
fn rewrite_aof_sharded_sync(
    shard_dbs: &crate::shard::shared_databases::ShardDatabases,
    aof_path: &Path,
) -> Result<(), MoonError> {
    let db_count = shard_dbs.db_count();
    let now_ms = current_time_ms();
    let mut merged_dbs: Vec<Database> = (0..db_count).map(|_| Database::new()).collect();

    for shard_locks in shard_dbs.all_shard_dbs() {
        for (db_idx, lock) in shard_locks.iter().enumerate() {
            let guard = lock.read();
            for (key, entry) in guard.data().iter() {
                if !entry.is_expired_at(guard.base_timestamp(), now_ms) {
                    merged_dbs[db_idx].set(key.to_bytes(), entry.clone());
                }
            }
        }
    }

    let rdb_bytes = crate::persistence::rdb::save_to_bytes(&merged_dbs)?;

    let tmp_path = aof_path.with_extension("aof.tmp");
    std::fs::write(&tmp_path, &rdb_bytes).map_err(|e| AofError::Io {
        path: tmp_path.clone(),
        source: e,
    })?;
    std::fs::rename(&tmp_path, aof_path).map_err(|e| AofError::RewriteFailed {
        detail: format!(
            "rename {} -> {}: {}",
            tmp_path.display(),
            aof_path.display(),
            e
        ),
    })?;

    info!(
        "AOF rewrite (sharded, RDB preamble) complete: {} bytes",
        rdb_bytes.len()
    );
    Ok(())
}

/// Reopen AOF file in append mode after atomic rewrite replaced it.
#[allow(dead_code)]
fn reopen_aof_sync(aof_path: &Path) -> Result<std::fs::File, std::io::Error> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(aof_path)
}

/// Rewrite the AOF file (tokio async wrapper).
///
/// Delegates to `rewrite_aof_sync` — the actual I/O is synchronous (temp write + rename).
#[cfg(feature = "runtime-tokio")]
pub async fn rewrite_aof(db: SharedDatabases, aof_path: &Path) -> Result<(), MoonError> {
    rewrite_aof_sync(&db, aof_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::replay::DispatchReplayEngine;
    use ordered_float::OrderedFloat;
    use tempfile::tempdir;

    fn make_command(parts: &[&[u8]]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
                .collect(),
        )
    }

    // --- serialize_command / generate_aof_command round-trip tests ---

    #[test]
    fn test_generate_aof_command_produces_valid_resp_that_round_trips() {
        let frame = make_command(&[b"SET", b"key", b"value"]);
        let serialized = serialize_command(&frame);

        let mut buf = BytesMut::from(&serialized[..]);
        let config = ParseConfig::default();
        let parsed = parse::parse(&mut buf, &config).unwrap().unwrap();
        assert_eq!(parsed, frame);
    }

    #[test]
    fn test_serialize_command_round_trip_hset() {
        let frame = make_command(&[b"HSET", b"myhash", b"f1", b"v1"]);
        let serialized = serialize_command(&frame);
        let mut buf = BytesMut::from(&serialized[..]);
        let parsed = parse::parse(&mut buf, &ParseConfig::default())
            .unwrap()
            .unwrap();
        assert_eq!(parsed, frame);
    }

    // --- AOF replay tests ---

    #[test]
    fn test_aof_replay_set_commands_restores_string_keys() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        // Write SET commands in RESP format
        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"k1", b"v1"]), &mut aof_data);
        serialize::serialize(&make_command(&[b"SET", b"k2", b"v2"]), &mut aof_data);
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine).unwrap();
        assert_eq!(count, 2);

        let entry = dbs[0].get(b"k1").unwrap();
        assert_eq!(entry.value.as_bytes().unwrap(), b"v1");
        let entry = dbs[0].get(b"k2").unwrap();
        assert_eq!(entry.value.as_bytes().unwrap(), b"v2");
    }

    #[test]
    fn test_aof_replay_collection_types() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        // HSET
        serialize::serialize(
            &make_command(&[b"HSET", b"myhash", b"f1", b"v1"]),
            &mut aof_data,
        );
        // LPUSH
        serialize::serialize(
            &make_command(&[b"LPUSH", b"mylist", b"a", b"b"]),
            &mut aof_data,
        );
        // SADD
        serialize::serialize(
            &make_command(&[b"SADD", b"myset", b"x", b"y"]),
            &mut aof_data,
        );
        // ZADD
        serialize::serialize(
            &make_command(&[b"ZADD", b"myzset", b"1.5", b"alice"]),
            &mut aof_data,
        );
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine).unwrap();
        assert_eq!(count, 4);

        // Check hash
        let hash = dbs[0].get_hash(b"myhash").unwrap().unwrap();
        assert_eq!(
            hash.get(&Bytes::from_static(b"f1")).unwrap().as_ref(),
            b"v1"
        );

        // Check list
        let list = dbs[0].get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list.len(), 2);

        // Check set
        let set = dbs[0].get_set(b"myset").unwrap().unwrap();
        assert_eq!(set.len(), 2);

        // Check sorted set
        let (members, _) = dbs[0].get_sorted_set(b"myzset").unwrap().unwrap();
        assert_eq!(*members.get(&Bytes::from_static(b"alice")).unwrap(), 1.5);
    }

    #[test]
    fn test_aof_replay_with_expire_preserves_ttls() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"mykey", b"myval"]), &mut aof_data);
        serialize::serialize(
            &make_command(&[b"PEXPIRE", b"mykey", b"60000"]),
            &mut aof_data,
        );
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine).unwrap();
        assert_eq!(count, 2);

        let base_ts = dbs[0].base_timestamp();
        let entry = dbs[0].get(b"mykey").unwrap();
        assert!(entry.has_expiry());
        let remaining_secs = (entry.expires_at_ms(base_ts) - current_time_ms()) / 1000;
        assert!(remaining_secs >= 50); // Allow some tolerance
    }

    #[test]
    fn test_aof_replay_with_select_switches_databases() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"k0", b"v0"]), &mut aof_data);
        serialize::serialize(&make_command(&[b"SELECT", b"1"]), &mut aof_data);
        serialize::serialize(&make_command(&[b"SET", b"k1", b"v1"]), &mut aof_data);
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new(), Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine).unwrap();
        assert_eq!(count, 3);

        assert!(dbs[0].get(b"k0").is_some());
        assert!(dbs[1].get(b"k1").is_some());
    }

    #[test]
    fn test_aof_replay_empty_file_produces_zero_keys() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");
        std::fs::write(&aof_path, b"").unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine).unwrap();
        assert_eq!(count, 0);
        assert_eq!(dbs[0].len(), 0);
    }

    #[test]
    fn test_aof_replay_corrupt_truncated_logs_error_loads_what_it_can() {
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("test.aof");

        let mut aof_data = BytesMut::new();
        serialize::serialize(&make_command(&[b"SET", b"k1", b"v1"]), &mut aof_data);
        // Append corrupt data
        aof_data.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$2\r\nk2");
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path, &DispatchReplayEngine).unwrap();
        // Should have loaded the first command
        assert_eq!(count, 1);
        assert!(dbs[0].get(b"k1").is_some());
    }

    // --- FsyncPolicy tests ---

    #[test]
    fn test_fsync_policy_from_str() {
        assert_eq!(FsyncPolicy::from_str("always"), FsyncPolicy::Always);
        assert_eq!(FsyncPolicy::from_str("everysec"), FsyncPolicy::EverySec);
        assert_eq!(FsyncPolicy::from_str("no"), FsyncPolicy::No);
        assert_eq!(FsyncPolicy::from_str("unknown"), FsyncPolicy::EverySec);
    }

    // --- generate_rewrite_commands tests ---

    #[test]
    fn test_generate_rewrite_commands_all_5_types() {
        let mut dbs = vec![Database::new()];

        // String
        dbs[0].set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
        // Hash
        {
            let map = dbs[0].get_or_create_hash(b"h").unwrap();
            map.insert(Bytes::from_static(b"f"), Bytes::from_static(b"v"));
        }
        // List
        {
            let list = dbs[0].get_or_create_list(b"l").unwrap();
            list.push_back(Bytes::from_static(b"item"));
        }
        // Set
        {
            let set = dbs[0].get_or_create_set(b"s").unwrap();
            set.insert(Bytes::from_static(b"m"));
        }
        // Sorted set
        {
            let (members, tree) = dbs[0].get_or_create_sorted_set(b"z").unwrap();
            members.insert(Bytes::from_static(b"a"), 1.0);
            tree.insert(OrderedFloat(1.0), Bytes::from_static(b"a"));
        }

        let commands = generate_rewrite_commands(&dbs);
        assert!(!commands.is_empty());

        // Replay and verify round-trip
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("rewrite.aof");
        std::fs::write(&aof_path, &commands).unwrap();

        let mut loaded_dbs = vec![Database::new()];
        let count = replay_aof(&mut loaded_dbs, &aof_path, &DispatchReplayEngine).unwrap();
        assert!(count >= 5, "Expected at least 5 commands, got {}", count);

        // Verify each type restored
        assert_eq!(
            loaded_dbs[0].get(b"str").unwrap().value.type_name(),
            "string"
        );
        assert!(loaded_dbs[0].get_hash(b"h").unwrap().is_some());
        assert!(loaded_dbs[0].get_list(b"l").unwrap().is_some());
        assert!(loaded_dbs[0].get_set(b"s").unwrap().is_some());
        assert!(loaded_dbs[0].get_sorted_set(b"z").unwrap().is_some());
    }

    #[test]
    fn test_generate_rewrite_commands_with_ttl() {
        let mut dbs = vec![Database::new()];
        let future_ms = current_time_ms() + 3_600_000;
        dbs[0].set_string_with_expiry(
            Bytes::from_static(b"key"),
            Bytes::from_static(b"val"),
            future_ms,
        );

        let commands = generate_rewrite_commands(&dbs);

        // Replay and check TTL is preserved
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("rewrite.aof");
        std::fs::write(&aof_path, &commands).unwrap();

        let mut loaded_dbs = vec![Database::new()];
        let count = replay_aof(&mut loaded_dbs, &aof_path, &DispatchReplayEngine).unwrap();
        assert_eq!(count, 2); // SET + PEXPIRE

        let base_ts = loaded_dbs[0].base_timestamp();
        let entry = loaded_dbs[0].get(b"key").unwrap();
        assert!(entry.has_expiry());
        let remaining_secs = (entry.expires_at_ms(base_ts) - current_time_ms()) / 1000;
        assert!(remaining_secs > 3500);
    }

    #[test]
    fn test_generate_rewrite_round_trip_preserves_state() {
        let mut dbs = vec![Database::new()];
        dbs[0].set_string(Bytes::from_static(b"a"), Bytes::from_static(b"1"));
        dbs[0].set_string(Bytes::from_static(b"b"), Bytes::from_static(b"2"));
        {
            let list = dbs[0].get_or_create_list(b"mylist").unwrap();
            list.push_back(Bytes::from_static(b"x"));
            list.push_back(Bytes::from_static(b"y"));
            list.push_back(Bytes::from_static(b"z"));
        }

        let commands = generate_rewrite_commands(&dbs);
        let dir = tempdir().unwrap();
        let aof_path = dir.path().join("rewrite.aof");
        std::fs::write(&aof_path, &commands).unwrap();

        let mut loaded = vec![Database::new()];
        replay_aof(&mut loaded, &aof_path, &DispatchReplayEngine).unwrap();

        // Check strings
        assert_eq!(loaded[0].get(b"a").unwrap().value.as_bytes().unwrap(), b"1");
        assert_eq!(loaded[0].get(b"b").unwrap().value.as_bytes().unwrap(), b"2");

        // Check list order preserved
        let list = loaded[0].get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_ref(), b"x");
        assert_eq!(list[1].as_ref(), b"y");
        assert_eq!(list[2].as_ref(), b"z");
    }
}
