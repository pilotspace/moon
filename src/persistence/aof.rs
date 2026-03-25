//! Append-Only File (AOF) persistence: logs every write command in RESP format
//! for crash recovery. Supports three fsync policies and AOF rewriting for compaction.
#![allow(unused_imports, unused_variables, unreachable_code, clippy::empty_loop)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use crate::runtime::cancel::CancellationToken;
use crate::runtime::channel;
use tracing::{error, info, warn};

use crate::command::{dispatch, DispatchResult};
use crate::protocol::{Frame, ParseConfig};
use crate::protocol::{parse, serialize};
use crate::storage::compact_key::CompactKey;
use crate::storage::db::Database;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::{current_time_ms, Entry};

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
    /// Shut down the AOF writer task gracefully.
    Shutdown,
}

/// Canonical list of all write commands for AOF logging (reference/documentation).
/// SELECT is included so replay can track database switching.
/// The actual check is done via (length, first_byte) match in `is_write_command`.
#[allow(dead_code)]
const WRITE_COMMANDS: &[&[u8]] = &[
    b"SET", b"MSET", b"SETNX", b"SETEX", b"PSETEX",
    b"GETSET", b"GETDEL", b"GETEX",
    b"INCR", b"DECR", b"INCRBY", b"DECRBY", b"INCRBYFLOAT",
    b"APPEND", b"DEL", b"UNLINK",
    b"EXPIRE", b"PEXPIRE", b"PERSIST",
    b"RENAME", b"RENAMENX",
    b"HSET", b"HMSET", b"HDEL", b"HSETNX", b"HINCRBY", b"HINCRBYFLOAT",
    b"LPUSH", b"RPUSH", b"LPOP", b"RPOP", b"LSET", b"LINSERT", b"LREM", b"LTRIM", b"LMOVE",
    b"SADD", b"SREM", b"SPOP",
    b"SINTERSTORE", b"SUNIONSTORE", b"SDIFFSTORE",
    b"ZADD", b"ZREM", b"ZINCRBY", b"ZPOPMIN", b"ZPOPMAX",
    b"ZUNIONSTORE", b"ZINTERSTORE",
    b"SELECT",
];

/// Check if a command name is a write command that should be logged to AOF.
///
/// Uses (length, first_byte) dispatch for O(1) lookup instead of linear scan.
#[inline]
pub fn is_write_command(name: &[u8]) -> bool {
    let len = name.len();
    if len == 0 { return false; }
    let b0 = name[0] | 0x20;
    match (len, b0) {
        // 3-letter
        (3, b's') => name.eq_ignore_ascii_case(b"SET"),
        (3, b'd') => name.eq_ignore_ascii_case(b"DEL"),
        // 4-letter
        (4, b'i') => name.eq_ignore_ascii_case(b"INCR"),
        (4, b'd') => name.eq_ignore_ascii_case(b"DECR"),
        (4, b'm') => name.eq_ignore_ascii_case(b"MSET"),
        (4, b'h') => name.eq_ignore_ascii_case(b"HSET") || name.eq_ignore_ascii_case(b"HDEL"),
        (4, b'l') => name.eq_ignore_ascii_case(b"LSET") || name.eq_ignore_ascii_case(b"LREM") || name.eq_ignore_ascii_case(b"LPOP"),
        (4, b'r') => name.eq_ignore_ascii_case(b"RPOP"),
        (4, b's') => name.eq_ignore_ascii_case(b"SADD") || name.eq_ignore_ascii_case(b"SREM") || name.eq_ignore_ascii_case(b"SPOP"),
        (4, b'z') => name.eq_ignore_ascii_case(b"ZADD") || name.eq_ignore_ascii_case(b"ZREM"),
        // 5-letter
        (5, b'l') => name.eq_ignore_ascii_case(b"LPUSH") || name.eq_ignore_ascii_case(b"LTRIM") || name.eq_ignore_ascii_case(b"LMOVE"),
        (5, b'r') => name.eq_ignore_ascii_case(b"RPUSH"),
        (5, b'h') => name.eq_ignore_ascii_case(b"HMSET"),
        (5, b's') => name.eq_ignore_ascii_case(b"SETNX") || name.eq_ignore_ascii_case(b"SETEX"),
        // 6-letter
        (6, b'a') => name.eq_ignore_ascii_case(b"APPEND"),
        (6, b'd') => name.eq_ignore_ascii_case(b"DECRBY"),
        (6, b'e') => name.eq_ignore_ascii_case(b"EXPIRE"),
        (6, b'g') => name.eq_ignore_ascii_case(b"GETSET") || name.eq_ignore_ascii_case(b"GETDEL"),
        (6, b'h') => name.eq_ignore_ascii_case(b"HSETNX"),
        (6, b'i') => name.eq_ignore_ascii_case(b"INCRBY"),
        (6, b'p') => name.eq_ignore_ascii_case(b"PSETEX"),
        (6, b'r') => name.eq_ignore_ascii_case(b"RENAME"),
        (6, b's') => name.eq_ignore_ascii_case(b"SELECT"),
        (6, b'u') => name.eq_ignore_ascii_case(b"UNLINK"),
        // 7-letter
        (7, b'h') => name.eq_ignore_ascii_case(b"HINCRBY"),
        (7, b'l') => name.eq_ignore_ascii_case(b"LINSERT"),
        (7, b'p') => name.eq_ignore_ascii_case(b"PEXPIRE") || name.eq_ignore_ascii_case(b"PERSIST"),
        (7, b'z') => name.eq_ignore_ascii_case(b"ZINCRBY") || name.eq_ignore_ascii_case(b"ZPOPMIN") || name.eq_ignore_ascii_case(b"ZPOPMAX"),
        // 5-letter GETEX
        (5, b'g') => name.eq_ignore_ascii_case(b"GETEX"),
        // 8-letter
        (8, b'r') => name.eq_ignore_ascii_case(b"RENAMENX"),
        // 11-letter
        (11, b'i') => name.eq_ignore_ascii_case(b"INCRBYFLOAT"),
        (11, b's') => name.eq_ignore_ascii_case(b"SINTERSTORE") || name.eq_ignore_ascii_case(b"SUNIONSTORE"),
        (11, b'z') => name.eq_ignore_ascii_case(b"ZUNIONSTORE") || name.eq_ignore_ascii_case(b"ZINTERSTORE"),
        // 10-letter
        (10, b's') => name.eq_ignore_ascii_case(b"SDIFFSTORE"),
        // 12-letter
        (12, b'h') => name.eq_ignore_ascii_case(b"HINCRBYFLOAT"),
        _ => false,
    }
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

    // Monoio fallback: AOF writer uses sync I/O in a simple recv loop.
    #[cfg(feature = "runtime-monoio")]
    {
        use std::io::Write;
        let mut file = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&aof_path)
        {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to open AOF file {}: {}", aof_path.display(), e);
                return;
            }
        };
        loop {
            // Use blocking recv since monoio doesn't support tokio::select!
            match rx.recv() {
                Ok(AofMessage::Append(data)) => {
                    let _ = file.write_all(&data);
                    if fsync == FsyncPolicy::Always {
                        let _ = file.flush();
                        let _ = std::io::Write::flush(&mut file);
                    }
                }
                Ok(AofMessage::Shutdown) | Err(_) => {
                    let _ = file.flush();
                    info!("AOF writer shutting down (monoio)");
                    break;
                }
                Ok(AofMessage::Rewrite(_db)) => {
                    // AOF rewrite under monoio: not yet implemented
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
/// On parse error, logs a warning and returns what was loaded so far (partial replay).
pub fn replay_aof(databases: &mut [Database], path: &Path) -> anyhow::Result<usize> {
    let data = std::fs::read(path)?;
    if data.is_empty() {
        return Ok(0);
    }

    let mut buf = BytesMut::from(&data[..]);
    let config = ParseConfig::default();
    let mut selected_db: usize = 0;
    let mut count: usize = 0;
    let db_count = databases.len();

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
                            _ => { count += 1; continue; }
                        };
                        (name as &[u8], &arr[1..])
                    }
                    _ => { count += 1; continue; }
                };
                let result = dispatch(
                    &mut databases[selected_db],
                    cmd,
                    cmd_args,
                    &mut selected_db,
                    db_count,
                );
                match result {
                    DispatchResult::Response(_) => {
                        count += 1;
                    }
                    DispatchResult::Quit(_) => {
                        // QUIT in AOF is unexpected, skip
                        count += 1;
                    }
                }
            }
            Ok(None) => {
                // Incomplete frame at end of file - truncated AOF
                if !buf.is_empty() {
                    warn!("AOF truncated: {} unparseable bytes at end", buf.len());
                }
                break;
            }
            Err(e) => {
                warn!("AOF parse error after {} commands: {}. Partial replay.", count, e);
                break;
            }
        }
    }

    Ok(count)
}

/// Generate synthetic RESP commands from the current database state for AOF rewriting.
///
/// Produces commands for all 5 data types plus PEXPIRE for keys with TTL.
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
            let select_frame = Frame::Array(vec![
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
                    let frame = Frame::Array(vec![
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                    serialize::serialize(&Frame::Array(args), &mut buf);
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
                        serialize::serialize(&Frame::Array(args), &mut buf);
                    }
                }
            }

            // Generate PEXPIRE for keys with TTL
            if entry.has_expiry() {
                let exp_ms = entry.expires_at_ms(base_ts);
                if exp_ms > now_ms {
                    let remaining_ms = exp_ms - now_ms;
                    let pexpire_frame = Frame::Array(vec![
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

/// Rewrite the AOF file with synthetic commands from current database state.
///
/// Writes to a temporary file first, then atomically renames for crash safety.
#[cfg(feature = "runtime-tokio")]
pub async fn rewrite_aof(db: SharedDatabases, aof_path: &Path) -> anyhow::Result<()> {
    // Clone database state: lock each db individually with read lock
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

    // Reconstruct temporary Database objects for generate_rewrite_commands
    let mut temp_dbs: Vec<Database> = Vec::with_capacity(snapshot.len());
    for (entries, _base_ts) in &snapshot {
        let mut db = Database::new();
        for (key, entry) in entries {
            db.set(key.to_bytes(), entry.clone());
        }
        temp_dbs.push(db);
    }

    let commands = generate_rewrite_commands(&temp_dbs);

    // Write to temp file, then atomic rename
    let tmp_path = aof_path.with_extension("aof.tmp");
    std::fs::write(&tmp_path, &commands)?;
    std::fs::rename(&tmp_path, aof_path)?;

    info!("AOF rewrite complete: {} bytes", commands.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    // --- is_write_command tests ---

    #[test]
    fn test_write_commands_list_includes_all_known_write_commands() {
        let known_writes = [
            b"SET" as &[u8], b"MSET", b"SETNX", b"SETEX", b"PSETEX",
            b"GETSET", b"GETDEL", b"GETEX",
            b"INCR", b"DECR", b"INCRBY", b"DECRBY", b"INCRBYFLOAT",
            b"APPEND", b"DEL", b"UNLINK",
            b"EXPIRE", b"PEXPIRE", b"PERSIST",
            b"RENAME", b"RENAMENX",
            b"HSET", b"HMSET", b"HDEL", b"HSETNX", b"HINCRBY", b"HINCRBYFLOAT",
            b"LPUSH", b"RPUSH", b"LPOP", b"RPOP", b"LSET", b"LINSERT", b"LREM", b"LTRIM", b"LMOVE",
            b"SADD", b"SREM", b"SPOP",
            b"SINTERSTORE", b"SUNIONSTORE", b"SDIFFSTORE",
            b"ZADD", b"ZREM", b"ZINCRBY", b"ZPOPMIN", b"ZPOPMAX",
            b"ZUNIONSTORE", b"ZINTERSTORE",
            b"SELECT",
        ];
        for cmd in &known_writes {
            assert!(is_write_command(cmd), "Expected {} to be a write command", String::from_utf8_lossy(cmd));
        }
    }

    #[test]
    fn test_is_write_command_returns_false_for_read_commands() {
        assert!(!is_write_command(b"GET"));
        assert!(!is_write_command(b"PING"));
        assert!(!is_write_command(b"ECHO"));
        assert!(!is_write_command(b"KEYS"));
        assert!(!is_write_command(b"HGET"));
        assert!(!is_write_command(b"LRANGE"));
        assert!(!is_write_command(b"SMEMBERS"));
        assert!(!is_write_command(b"ZSCORE"));
        assert!(!is_write_command(b"INFO"));
    }

    #[test]
    fn test_is_write_command_case_insensitive() {
        assert!(is_write_command(b"set"));
        assert!(is_write_command(b"Set"));
        assert!(is_write_command(b"hset"));
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
        let parsed = parse::parse(&mut buf, &ParseConfig::default()).unwrap().unwrap();
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
        let count = replay_aof(&mut dbs, &aof_path).unwrap();
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
        serialize::serialize(&make_command(&[b"HSET", b"myhash", b"f1", b"v1"]), &mut aof_data);
        // LPUSH
        serialize::serialize(&make_command(&[b"LPUSH", b"mylist", b"a", b"b"]), &mut aof_data);
        // SADD
        serialize::serialize(&make_command(&[b"SADD", b"myset", b"x", b"y"]), &mut aof_data);
        // ZADD
        serialize::serialize(&make_command(&[b"ZADD", b"myzset", b"1.5", b"alice"]), &mut aof_data);
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path).unwrap();
        assert_eq!(count, 4);

        // Check hash
        let hash = dbs[0].get_hash(b"myhash").unwrap().unwrap();
        assert_eq!(hash.get(&Bytes::from_static(b"f1")).unwrap().as_ref(), b"v1");

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
        serialize::serialize(&make_command(&[b"PEXPIRE", b"mykey", b"60000"]), &mut aof_data);
        std::fs::write(&aof_path, &aof_data).unwrap();

        let mut dbs = vec![Database::new()];
        let count = replay_aof(&mut dbs, &aof_path).unwrap();
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
        let count = replay_aof(&mut dbs, &aof_path).unwrap();
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
        let count = replay_aof(&mut dbs, &aof_path).unwrap();
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
        let count = replay_aof(&mut dbs, &aof_path).unwrap();
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
        let count = replay_aof(&mut loaded_dbs, &aof_path).unwrap();
        assert!(count >= 5, "Expected at least 5 commands, got {}", count);

        // Verify each type restored
        assert_eq!(loaded_dbs[0].get(b"str").unwrap().value.type_name(), "string");
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
        let count = replay_aof(&mut loaded_dbs, &aof_path).unwrap();
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
        replay_aof(&mut loaded, &aof_path).unwrap();

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
