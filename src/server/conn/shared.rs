#[cfg(feature = "runtime-tokio")]
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use bytes::Bytes;
#[cfg(feature = "runtime-tokio")]
use bytes::BytesMut;

use crate::command::config as config_cmd;
#[cfg(feature = "runtime-tokio")]
use crate::command::metadata;
use crate::command::{DispatchResult, dispatch};
use crate::config::{RuntimeConfig, ServerConfig};
use crate::protocol::Frame;
#[cfg(feature = "runtime-tokio")]
use crate::storage::Database;
use crate::storage::entry::CachedClock;

use super::util::extract_command;

/// Type alias for the per-database RwLock container (tokio single-thread mode only).
#[cfg(feature = "runtime-tokio")]
pub(crate) type SharedDatabases = Arc<Vec<parking_lot::RwLock<Database>>>;

/// Handle CONFIG GET/SET subcommands.
pub(crate) fn handle_config(
    args: &[Frame],
    runtime_config: &Arc<RwLock<RuntimeConfig>>,
    server_config: &Arc<ServerConfig>,
) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'config' command",
        ));
    }

    let subcmd = match &args[0] {
        Frame::BulkString(s) => s.as_ref(),
        Frame::SimpleString(s) => s.as_ref(),
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR unknown subcommand for CONFIG"));
        }
    };

    let sub_args = &args[1..];

    if subcmd.eq_ignore_ascii_case(b"GET") {
        let rt = runtime_config.read();
        config_cmd::config_get(&rt, server_config, sub_args)
    } else if subcmd.eq_ignore_ascii_case(b"SET") {
        let mut rt = runtime_config.write();
        config_cmd::config_set(&mut rt, sub_args)
    } else {
        Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}'. Try CONFIG GET, CONFIG SET.",
            String::from_utf8_lossy(subcmd)
        )))
    }
}

/// Execute a queued transaction atomically under a single database lock.
///
/// Checks WATCH versions first -- if any watched key's version has changed since
/// the snapshot was taken, the transaction is aborted and Frame::Null is returned.
///
/// Returns the result Frame (Array of responses, or Null on abort) and a Vec of
/// AOF byte entries for write commands that succeeded (caller sends them async).
#[cfg(feature = "runtime-tokio")]
pub(crate) fn execute_transaction(
    db: &SharedDatabases,
    command_queue: &[Frame],
    watched_keys: &HashMap<Bytes, u32>,
    selected_db: &mut usize,
) -> (Frame, Vec<Bytes>) {
    let mut guard = db[*selected_db].write();
    let db_count = db.len();
    guard.refresh_now();

    // Check WATCH versions -- if any key's version changed, abort
    for (key, watched_version) in watched_keys {
        let current_version = guard.get_version(key);
        if current_version != *watched_version {
            return (Frame::Null, Vec::new()); // Transaction aborted
        }
    }

    // Execute all queued commands atomically (under the same lock)
    let mut results = Vec::with_capacity(command_queue.len());
    let mut aof_entries: Vec<Bytes> = Vec::new();

    for cmd_frame in command_queue {
        // Extract command name and args (zero-alloc)
        let (cmd, cmd_args) = match extract_command(cmd_frame) {
            Some(pair) => pair,
            None => {
                results.push(Frame::Error(Bytes::from_static(
                    b"ERR invalid command format",
                )));
                continue;
            }
        };

        // Check if this is a write command for AOF logging
        let is_write = metadata::is_write(cmd);

        // Serialize for AOF before dispatch
        let aof_bytes = if is_write {
            let mut buf = BytesMut::new();
            crate::protocol::serialize::serialize(cmd_frame, &mut buf);
            Some(buf.freeze())
        } else {
            None
        };

        let result = dispatch(&mut *guard, cmd, cmd_args, selected_db, db_count);
        let response = match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f, // QUIT inside MULTI just returns OK
        };

        // Collect AOF entry for successful writes (not error responses)
        if let Some(bytes) = aof_bytes {
            if !matches!(&response, Frame::Error(_)) {
                aof_entries.push(bytes);
            }
        }

        results.push(response);
    }

    (Frame::Array(results.into()), aof_entries)
}

/// Execute a queued transaction on the local shard (sharded path).
///
/// Transactions in the shared-nothing architecture are restricted to local-shard
/// keys only. Cross-shard transactions require distributed coordination (future work).
pub(crate) fn execute_transaction_sharded(
    shard_databases: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
    shard_id: usize,
    command_queue: &[Frame],
    selected_db: usize,
    cached_clock: &CachedClock,
) -> Frame {
    let mut guard = shard_databases.write_db(shard_id, selected_db);
    let db_count = shard_databases.db_count();
    guard.refresh_now_from_cache(cached_clock);

    let mut results = Vec::with_capacity(command_queue.len());
    let mut selected = selected_db;

    for cmd_frame in command_queue {
        let (cmd, cmd_args) = match extract_command(cmd_frame) {
            Some(pair) => pair,
            None => {
                results.push(Frame::Error(Bytes::from_static(
                    b"ERR invalid command format",
                )));
                continue;
            }
        };

        let result = dispatch(&mut guard, cmd, cmd_args, &mut selected, db_count);
        let response = match result {
            DispatchResult::Response(f) => f,
            DispatchResult::Quit(f) => f,
        };
        results.push(response);
    }

    Frame::Array(results.into())
}

/// Extract the primary key from a parsed command for shard routing.
///
/// Returns `None` for keyless commands (PING, DBSIZE, SELECT, etc.)
/// which should execute locally on the connection's shard.
pub(crate) fn extract_primary_key<'a>(cmd: &[u8], args: &'a [Frame]) -> Option<&'a Bytes> {
    // Fast check: is this command keyless? Uses (length, first_byte) dispatch.
    let len = cmd.len();
    if len == 0 {
        return None;
    }
    let b0 = cmd[0] | 0x20;

    let is_keyless = match (len, b0) {
        (4, b'a') => cmd.eq_ignore_ascii_case(b"AUTH"),
        (4, b'e') => cmd.eq_ignore_ascii_case(b"ECHO") || cmd.eq_ignore_ascii_case(b"EXEC"),
        (4, b'i') => cmd.eq_ignore_ascii_case(b"INFO"),
        (4, b'k') => cmd.eq_ignore_ascii_case(b"KEYS"),
        (4, b'p') => cmd.eq_ignore_ascii_case(b"PING"),
        (4, b'q') => cmd.eq_ignore_ascii_case(b"QUIT"),
        (4, b's') => cmd.eq_ignore_ascii_case(b"SCAN") || cmd.eq_ignore_ascii_case(b"SAVE"),
        (4, b'w') => cmd.eq_ignore_ascii_case(b"WAIT"),
        (5, b'd') => cmd.eq_ignore_ascii_case(b"DEBUG"),
        (5, b'h') => cmd.eq_ignore_ascii_case(b"HELLO"),
        (5, b'm') => cmd.eq_ignore_ascii_case(b"MULTI"),
        (5, b'p') => cmd.eq_ignore_ascii_case(b"PSYNC"),
        (6, b'a') => cmd.eq_ignore_ascii_case(b"ASKING"),
        (6, b'b') => cmd.eq_ignore_ascii_case(b"BGSAVE"),
        (6, b'c') => cmd.eq_ignore_ascii_case(b"CLIENT") || cmd.eq_ignore_ascii_case(b"CONFIG"),
        (6, b'd') => cmd.eq_ignore_ascii_case(b"DBSIZE"),
        (6, b's') => cmd.eq_ignore_ascii_case(b"SELECT"),
        (7, b'c') => cmd.eq_ignore_ascii_case(b"COMMAND") || cmd.eq_ignore_ascii_case(b"CLUSTER"),
        (7, b'd') => cmd.eq_ignore_ascii_case(b"DISCARD"),
        (7, b'p') => cmd.eq_ignore_ascii_case(b"PUBLISH"),
        (7, b's') => cmd.eq_ignore_ascii_case(b"SLAVEOF"),
        (8, b'l') => cmd.eq_ignore_ascii_case(b"LASTSAVE"),
        (8, b'r') => cmd.eq_ignore_ascii_case(b"REPLCONF"),
        (9, b'r') => cmd.eq_ignore_ascii_case(b"REPLICAOF"),
        (9, b's') => cmd.eq_ignore_ascii_case(b"SUBSCRIBE"),
        (10, b'p') => cmd.eq_ignore_ascii_case(b"PSUBSCRIBE"),
        (11, b'u') => cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE"),
        (12, b'p') => cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE"),
        (13, b'b') => cmd.eq_ignore_ascii_case(b"BGREWRITEAOF"),
        _ => false,
    };

    if is_keyless || args.is_empty() {
        return None;
    }
    match &args[0] {
        Frame::BulkString(key) => Some(key),
        _ => None,
    }
}

/// Check if a command is a multi-key command requiring VLL coordination.
///
/// These commands operate on multiple keys that may live on different shards.
/// Single-arg DEL/UNLINK/EXISTS are NOT multi-key (handled as single-key fast path).
pub(crate) fn is_multi_key_command(cmd: &[u8], args: &[Frame]) -> bool {
    let len = cmd.len();
    if len == 0 {
        return false;
    }
    let b0 = cmd[0] | 0x20;
    match (len, b0) {
        (4, b'm') => cmd.eq_ignore_ascii_case(b"MGET") || cmd.eq_ignore_ascii_case(b"MSET"),
        // DEL, UNLINK, EXISTS with multiple keys
        (3, b'd') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"DEL"),
        (6, b'u') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"UNLINK"),
        (6, b'e') => args.len() > 1 && cmd.eq_ignore_ascii_case(b"EXISTS"),
        _ => false,
    }
}
