//! Replication stream apply (R0): parse the master's RESP command stream and
//! apply each write to the local shard.
//!
//! The master fans out every write as `aof::serialize_command(cmd)` — a bare
//! RESP Array frame, identical to the AOF wire format (see
//! [`crate::shard::spsc_handler::wal_append_and_fanout`]). The replica
//! connection task runs ON its shard's thread, so for a single-shard deployment
//! every command targets the local shard and is applied directly through the
//! thread-local [`ShardSlice`](crate::shard::slice) via `with_shard` — there is
//! no SPSC self-hop (the ChannelMesh has no self-slot; local legs never go
//! through `spsc_send`). Multi-shard replica routing (hash each key to its
//! owning shard, broadcast keyless commands) is deferred to R2.
//!
//! **Read-only bypass:** the read-only-replica guard lives in the connection
//! layer (`try_enforce_readonly`), which the replica task never invokes. Applying
//! here therefore correctly bypasses it — a replica MUST apply whatever the
//! master streams regardless of its own read-only role.
//!
//! **Durability (R0 scope):** apply is in-memory only. The replica's own AOF is
//! NOT appended per replicated write; a replica recovers by re-syncing from its
//! master on restart (standard, safe). BGREWRITEAOF / RDB snapshots on the
//! replica still fold the applied in-memory state. Independent replica-side
//! incremental persistence is a documented follow-up.

use std::sync::Arc;

use bytes::BytesMut;

use crate::protocol::{Frame, ParseConfig, parse};

/// One replicated data command, already resolved to its logical db.
#[derive(Debug)]
pub(crate) struct ReplCommand {
    pub db_index: usize,
    pub command: Arc<Frame>,
}

/// Outcome of draining complete frames out of the replication read buffer.
pub(crate) struct DrainResult {
    /// Data commands to apply, in stream order.
    pub commands: Vec<ReplCommand>,
    /// Bytes consumed from `buf` — advance the replication offset by exactly
    /// this (NOT by the raw socket read count, which may split a frame).
    pub consumed: usize,
    /// A frame failed to parse. The RESP replication stream is unframed and
    /// cannot be safely resynced mid-stream, so the caller must drop the
    /// connection; the reconnect path then negotiates a fresh resync.
    pub fatal: bool,
}

/// Drain every COMPLETE RESP command frame from `buf`, tracking `SELECT` into
/// `selected_db`. Any partial trailing frame is left in `buf` for the next read
/// (the parser does not consume incomplete frames), so `consumed` counts only
/// whole frames.
///
/// `SELECT n` updates `selected_db` and is NOT emitted (carries no data).
/// Replication chatter (`PING`, `REPLCONF`) is skipped. Every other command is
/// emitted bound to the `selected_db` in effect when it was parsed.
pub(crate) fn drain_replicated_commands(
    buf: &mut BytesMut,
    selected_db: &mut usize,
) -> DrainResult {
    let config = ParseConfig::default();
    let mut commands = Vec::new();
    let mut consumed = 0usize;
    let mut fatal = false;

    loop {
        if buf.is_empty() {
            break;
        }
        let before = buf.len();
        match parse::parse(buf, &config) {
            Ok(Some(frame)) => {
                consumed += before - buf.len();
                classify(frame, selected_db, &mut commands);
            }
            // Incomplete trailing frame: parser left `buf` untouched — wait for
            // the next socket read to complete it.
            Ok(None) => break,
            Err(_) => {
                fatal = true;
                break;
            }
        }
    }

    DrainResult {
        commands,
        consumed,
        fatal,
    }
}

/// Route a single parsed frame: absorb `SELECT`, drop chatter, or record a data
/// command bound to the current `selected_db`.
fn classify(frame: Frame, selected_db: &mut usize, out: &mut Vec<ReplCommand>) {
    let Some((cmd, args)) = command_parts(&frame) else {
        return; // non-array / empty — ignore (e.g. inline-newline keepalive)
    };
    if cmd.eq_ignore_ascii_case(b"SELECT") {
        if let Some(db) = args.first().and_then(frame_to_usize) {
            *selected_db = db;
        }
        return;
    }
    // Keepalive / ack-negotiation frames the master may interleave — never data.
    if cmd.eq_ignore_ascii_case(b"PING") || cmd.eq_ignore_ascii_case(b"REPLCONF") {
        return;
    }
    out.push(ReplCommand {
        db_index: *selected_db,
        command: Arc::new(frame),
    });
}

/// Borrow `(command_name, args)` out of a RESP Array frame.
fn command_parts(frame: &Frame) -> Option<(&[u8], &[Frame])> {
    match frame {
        Frame::Array(arr) if !arr.is_empty() => {
            let name = match &arr[0] {
                Frame::BulkString(s) => s.as_ref(),
                Frame::SimpleString(s) => s.as_ref(),
                _ => return None,
            };
            Some((name, &arr[1..]))
        }
        _ => None,
    }
}

fn frame_to_usize(f: &Frame) -> Option<usize> {
    match f {
        Frame::BulkString(s) | Frame::SimpleString(s) => {
            std::str::from_utf8(s).ok()?.trim().parse().ok()
        }
        Frame::Integer(n) if *n >= 0 => Some(*n as usize),
        _ => None,
    }
}

/// Apply one replicated command to the local shard's database.
///
/// Runs synchronously on the shard thread through the thread-local `ShardSlice`
/// (no `.await`), so it cannot interleave with the shard event loop's own
/// `with_shard` access on the same cooperative thread. Bypasses the read-only
/// guard by construction (see module docs).
///
/// Returns `false` only if this thread has no initialized `ShardSlice` — which
/// would indicate the replica task was spawned off a shard thread (a wiring
/// bug); the caller logs and drops the stream.
pub(crate) fn apply_local(rc: &ReplCommand) -> bool {
    use crate::command::{DispatchResult, dispatch as cmd_dispatch};
    use crate::shard::spsc_handler::extract_command_static;

    let Some((cmd, args)) = extract_command_static(&rc.command) else {
        return true; // not an array command — nothing to apply (defensive)
    };
    crate::shard::slice::try_with_shard(|s| {
        let db_count = s.databases.len();
        if db_count == 0 {
            return;
        }
        let db_idx = rc.db_index.min(db_count - 1);
        if db_idx != rc.db_index {
            // Replica configured with fewer logical dbs than the master: a
            // high-index write is clamped rather than lost, but that is a
            // divergence — surface it.
            tracing::debug!(
                "replica apply: db {} clamped to {} ({} dbs on this shard)",
                rc.db_index,
                db_idx,
                db_count
            );
        }

        // MOVE / cross-db COPY touch two databases at once and are intercepted
        // BEFORE generic dispatch on the master (see `spsc_two_db`). Generic
        // `dispatch()` cannot apply them — it returns an error for MOVE and
        // silently mis-targets COPY..DB — so mirror the master's two-db
        // intercept here. A replica never evicts on apply (it follows the
        // master), so the destination-db eviction gate is skipped.
        if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
            if let Some(resp) = apply_two_db(cmd, args, &mut s.databases, db_idx, db_count) {
                warn_on_error(cmd, &resp);
                return;
            }
            // COPY with no DB clause / same-db COPY: fall through to dispatch.
        }

        let db = &mut s.databases[db_idx];
        // Replica applies off the shard's periodic clock tick; refresh directly
        // so command-relative expiries (EXPIRE/SETEX) compute against real time.
        db.refresh_now();
        let mut selected = db_idx;
        let resp = match cmd_dispatch(db, cmd, args, &mut selected, db_count) {
            DispatchResult::Response(f) | DispatchResult::Quit(f) => f,
        };
        warn_on_error(cmd, &resp);
    })
    .is_some()
}

/// A replicated write that fails to apply is a silent-divergence risk — log it
/// loudly instead of dropping it on the floor. (Read-only errors cannot occur
/// here: apply bypasses the connection-layer read-only guard by construction.)
fn warn_on_error(cmd: &[u8], resp: &Frame) {
    if let Frame::Error(e) = resp {
        tracing::warn!(
            "replica apply: {} returned error, replica may diverge from master: {}",
            String::from_utf8_lossy(cmd),
            String::from_utf8_lossy(e)
        );
    }
}

/// Apply `MOVE` / cross-db `COPY ... DB n` on the replica using the same core
/// helpers as the master's two-db intercept. Returns `None` for a same-db /
/// no-`DB`-clause COPY (caller falls through to generic dispatch), `Some(resp)`
/// otherwise.
fn apply_two_db(
    cmd: &[u8],
    args: &[Frame],
    databases: &mut [crate::storage::Database],
    db_idx: usize,
    db_count: usize,
) -> Option<Frame> {
    use crate::command::keyspace::move_cmd as ksmv;

    if cmd.eq_ignore_ascii_case(b"MOVE") {
        let resp = match ksmv::parse_move_args(args, db_count) {
            Err(e) => e,
            Ok((_key, dst)) if dst == db_idx => Frame::Integer(0),
            Ok((key, dst)) => ksmv::with_two_slice_dbs(databases, db_idx, dst, |src, dstdb| {
                src.refresh_now();
                dstdb.refresh_now();
                ksmv::move_core(src, dstdb, &key)
            }),
        };
        return Some(resp);
    }

    // COPY: `?` returns None (fall through to dispatch) for no-DB / same-db.
    let copy_result = ksmv::parse_copy_db_args(args, db_idx, db_count)?;
    let resp = match copy_result {
        Err(e) => e,
        Ok(ca) => ksmv::with_two_slice_dbs(databases, db_idx, ca.dst_db, |src, dst| {
            src.refresh_now();
            dst.refresh_now();
            ksmv::copy_core(src, dst, &ca.src_key, &ca.dst_key, ca.replace)
        }),
    };
    Some(resp)
}

/// Load a full-resync RDB snapshot into the local shard's databases, replacing
/// existing contents (full resync = authoritative master state).
///
/// Returns the number of keys loaded, or an error if this thread has no
/// `ShardSlice` or the RDB is malformed.
pub(crate) fn load_snapshot(rdb: &[u8]) -> anyhow::Result<usize> {
    match crate::shard::slice::try_with_shard(|s| {
        for db in s.databases.iter_mut() {
            db.clear();
        }
        crate::persistence::redis_rdb::load_rdb(&mut s.databases, rdb)
    }) {
        Some(r) => r,
        None => Err(anyhow::anyhow!(
            "replica snapshot load: no ShardSlice on this thread"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// Build the RESP bytes for `SET key val` etc. (bare Array, AOF wire form).
    fn resp_cmd(parts: &[&[u8]]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
        for p in parts {
            v.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
            v.extend_from_slice(p);
            v.extend_from_slice(b"\r\n");
        }
        v
    }

    fn cmd_name(rc: &ReplCommand) -> Vec<u8> {
        match rc.command.as_ref() {
            Frame::Array(a) => match &a[0] {
                Frame::BulkString(s) => s.to_vec(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        }
    }

    #[test]
    fn single_complete_command_consumes_all() {
        let bytes = resp_cmd(&[b"SET", b"foo", b"bar"]);
        let total = bytes.len();
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 1);
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert_eq!(r.commands[0].db_index, 0);
        assert_eq!(r.consumed, total);
        assert!(!r.fatal);
        assert!(buf.is_empty(), "whole frame must be consumed");
    }

    #[test]
    fn two_back_to_back_commands() {
        let mut bytes = resp_cmd(&[b"SET", b"a", b"1"]);
        bytes.extend_from_slice(&resp_cmd(&[b"DEL", b"a"]));
        let total = bytes.len();
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 2);
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert_eq!(cmd_name(&r.commands[1]), b"DEL");
        assert_eq!(r.consumed, total);
        assert!(buf.is_empty());
    }

    #[test]
    fn partial_trailing_frame_is_retained() {
        let full = resp_cmd(&[b"SET", b"a", b"1"]);
        let complete_len = full.len();
        let mut bytes = full.clone();
        // Append a truncated second frame (header only, body missing).
        bytes.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$3\r\nfo");
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        // Only the first (complete) command is emitted; consumed excludes the
        // partial tail, which stays buffered for the next read.
        assert_eq!(r.commands.len(), 1);
        assert_eq!(r.consumed, complete_len);
        assert!(!r.fatal);
        assert_eq!(&buf[..], b"*3\r\n$3\r\nSET\r\n$3\r\nfo");
    }

    #[test]
    fn select_updates_db_and_is_not_emitted() {
        let mut bytes = resp_cmd(&[b"SELECT", b"2"]);
        bytes.extend_from_slice(&resp_cmd(&[b"SET", b"k", b"v"]));
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 1, "SELECT must not be emitted as data");
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert_eq!(r.commands[0].db_index, 2, "SET must bind to selected db 2");
        assert_eq!(db, 2, "selected_db persists across drains");
    }

    #[test]
    fn ping_and_replconf_are_skipped() {
        let mut bytes = resp_cmd(&[b"PING"]);
        bytes.extend_from_slice(&resp_cmd(&[b"REPLCONF", b"GETACK", b"*"]));
        bytes.extend_from_slice(&resp_cmd(&[b"SET", b"k", b"v"]));
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert_eq!(r.commands.len(), 1);
        assert_eq!(cmd_name(&r.commands[0]), b"SET");
        assert!(buf.is_empty());
    }

    #[test]
    fn malformed_frame_is_fatal() {
        // A bulk-string length that cannot parse → hard parse error.
        let bytes = b"*1\r\n$-5\r\nX\r\n".to_vec();
        let mut buf = BytesMut::from(&bytes[..]);
        let mut db = 0usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert!(r.fatal, "unparseable frame must flag fatal for reconnect");
    }

    #[test]
    fn empty_buffer_is_noop() {
        let mut buf = BytesMut::new();
        let mut db = 3usize;
        let r = drain_replicated_commands(&mut buf, &mut db);
        assert!(r.commands.is_empty());
        assert_eq!(r.consumed, 0);
        assert!(!r.fatal);
        assert_eq!(db, 3);
    }

    #[test]
    fn integer_select_arg_parses() {
        // SELECT sent with an integer arg instead of bulk string.
        let frame = Frame::Array(
            vec![
                Frame::BulkString(Bytes::from_static(b"SELECT")),
                Frame::Integer(4),
            ]
            .into(),
        );
        let mut db = 0usize;
        let mut out = Vec::new();
        classify(frame, &mut db, &mut out);
        assert_eq!(db, 4);
        assert!(out.is_empty());
    }
}
