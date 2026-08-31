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
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::BytesMut;

use crate::protocol::{Frame, ParseConfig, parse};

// ── Unified poison-record policy (task #48) ────────────────────────────
//
// A replicated record that fails to DECODE (malformed graph/MQ/WS/temporal
// payload, wrong arg shape, etc.) is PROTOCOL-level evidence that this
// replica's view of the stream has desynced from the master — the same
// class of event as `DrainResult::fatal` (an unparseable RESP frame) and
// "no ShardSlice on this thread". All three now share ONE outcome:
//
//   1. Never silently skip-and-continue applying. A skip means every
//      subsequent record in the stream keeps applying against state that
//      has already silently diverged — the divergence is permanent and
//      invisible until an operator notices data loss.
//   2. Never panic. A malformed record is untrusted network input (or a
//      benign version-skew record from an older/newer master), not a
//      local invariant violation.
//   3. Log loud (rate-limited — a poisoned link can produce many records
//      per second before the caller tears the connection down) and count
//      it in an INFO-visible counter (`replication_poison_records_total`,
//      `# Replication` section, precedent: `info_reclamation.rs`).
//   4. Signal the caller to KICK the link: drop the connection and let the
//      existing reconnect/resync loop (`run_replica_task`'s backoff loop)
//      renegotiate PSYNC. That loop is the ONLY recovery path — it either
//      resumes with `+CONTINUE` from a clean offset or falls back to a
//      full resync, both of which re-establish a known-good state instead
//      of compounding a guess.
//
// PSYNC snapshot install (`load_snapshot`) already behaves correctly for
// this class of failure: a malformed aux blob fails the WHOLE install via
// `anyhow::Result` (fail-closed), propagated up through `run_replica_task`
// as a hard error that drops the connection — no change needed there, only
// documented here for completeness.
//
// This is distinct from a SEMANTIC apply error (e.g. `WRONGTYPE`, "entity
// not found") on an otherwise well-formed, successfully decoded record —
// that is data-level divergence from a command that legitimately executed
// differently than on the master (should not happen, but is not stream
// corruption); those keep the existing warn-and-continue behavior
// (`warn_on_error`), matching the generic KV dispatch path's long-standing
// posture. Decode failure vs. semantic failure is the line every plane
// below now draws consistently.

/// Process-wide count of poison (malformed/undecodable) replicated records
/// observed on this replica, across every plane (graph/MQ/WS/temporal/RESP
/// framing). Exposed via `INFO replication` as
/// `replication_poison_records_total`.
pub static REPL_POISON_RECORDS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Rate-limit gate for the loud poison log line: epoch millis of the last
/// emitted log, so a burst of poison records (the common case — once the
/// stream desyncs, every subsequent record is usually poison too, until the
/// caller tears the connection down) logs once per window instead of
/// flooding.
static REPL_POISON_LAST_LOG_MS: AtomicU64 = AtomicU64::new(0);

/// Minimum spacing between poison log lines, in milliseconds.
const REPL_POISON_LOG_WINDOW_MS: u64 = 1000;

/// Record one poison event: increments the INFO counter unconditionally,
/// emits a rate-limited loud `warn!`, and returns `false` — the unified
/// signal every apply helper below uses to tell its caller "this record did
/// not apply; the stream desynced; drop the connection and resync."
fn poison(cmd: &[u8], reason: &str) -> bool {
    REPL_POISON_RECORDS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let last = REPL_POISON_LAST_LOG_MS.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) >= REPL_POISON_LOG_WINDOW_MS
        && REPL_POISON_LAST_LOG_MS
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        tracing::warn!(
            "replica apply: POISON RECORD {} — {} — dropping replication link to force resync \
             (replica desynced; silent skip would diverge permanently)",
            String::from_utf8_lossy(cmd),
            reason
        );
    }
    false
}

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
                // Unframed RESP-level corruption is the same poison-record
                // class as a malformed graph/MQ/WS/temporal payload (task
                // #48 unified policy) — count it in the same INFO counter,
                // even though the caller (not this function) owns the
                // actual "drop the connection" step via `DrainResult::fatal`.
                poison(
                    b"<resp-frame>",
                    "unparseable RESP frame in replication stream",
                );
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

/// Outcome of [`apply_local`] — the unified poison-record contract (task
/// #48). `Poisoned` and `NoShardSlice` both mean "the caller must drop the
/// connection to force a resync"; they are kept distinct only so the
/// caller's error message names the right cause. See the module-level
/// "Unified poison-record policy" docs above.
pub(crate) enum ApplyOutcome {
    /// Applied cleanly, or a well-formed record hit a semantic dispatch
    /// error (already logged loud by `warn_on_error`) — not stream
    /// corruption, no action required beyond the existing log.
    Applied,
    /// A malformed/undecodable record — desync evidence. Already logged +
    /// counted by [`poison`]; caller must drop the connection.
    Poisoned,
    /// This thread has no initialized `ShardSlice` — the replica task was
    /// spawned off a shard thread (a wiring bug); caller must drop the
    /// stream.
    NoShardSlice,
}

impl ApplyOutcome {
    fn from_poison_bool(ok: bool) -> Self {
        if ok { Self::Applied } else { Self::Poisoned }
    }
}

/// Apply one replicated command to the local shard's database.
///
/// Runs synchronously on the shard thread through the thread-local `ShardSlice`
/// (no `.await`), so it cannot interleave with the shard event loop's own
/// `with_shard` access on the same cooperative thread. Bypasses the read-only
/// guard by construction (see module docs).
///
/// Returns [`ApplyOutcome::NoShardSlice`] only if this thread has no
/// initialized `ShardSlice`; returns [`ApplyOutcome::Poisoned`] for any
/// malformed/undecodable record (task #48 unified policy) — both signal the
/// caller to drop the connection and let the reconnect loop resync.
///
/// `shard_databases` gives the WS.CREATE.APPLY / WS.DROP.APPLY arms
/// (Wave B ws-plane) access to the process-global `WorkspaceRegistry`, which
/// lives on `ShardDatabases` rather than the thread-local `ShardSlice` — R0
/// replication is single-shard-only, so applying directly here (no shard-0
/// hop) is correct: there is exactly one shard.
pub(crate) fn apply_local(
    rc: &ReplCommand,
    shard_databases: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
) -> ApplyOutcome {
    use crate::command::{DispatchResult, dispatch as cmd_dispatch};
    use crate::shard::spsc_handler::extract_command_static;

    let Some((cmd, args)) = extract_command_static(&rc.command) else {
        return ApplyOutcome::Applied; // not an array command — nothing to apply (defensive)
    };
    // Redis parity (and #373 idle-park visibility): applied master-stream
    // commands count toward total_commands_processed. The apply task runs
    // on the target shard's OS thread, so this lands in the same counter
    // slot the shard's idle gate reads.
    crate::admin::metrics_setup::record_replica_apply();
    if cmd.eq_ignore_ascii_case(crate::workspace::repl::WS_CREATE_APPLY_CMD) {
        // Doesn't touch `ShardSlice`, but routes through `try_with_shard`
        // anyway so a replica task wired to a non-shard thread is caught
        // uniformly (same "no ShardSlice here" contract every other arm has).
        return match crate::shard::slice::try_with_shard(|_s| {
            apply_ws_create(shard_databases, cmd, args)
        }) {
            Some(ok) => ApplyOutcome::from_poison_bool(ok),
            None => ApplyOutcome::NoShardSlice,
        };
    }
    if cmd.eq_ignore_ascii_case(crate::workspace::repl::WS_DROP_APPLY_CMD) {
        return match crate::shard::slice::try_with_shard(|s| {
            apply_ws_drop(s, shard_databases, cmd, args)
        }) {
            Some(ok) => ApplyOutcome::from_poison_bool(ok),
            None => ApplyOutcome::NoShardSlice,
        };
    }
    let result = crate::shard::slice::try_with_shard(|s| -> bool {
        let db_count = s.databases.db_count();
        if db_count == 0 {
            return true;
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

        // FT.* index-definition commands (v0.7 R0.5) are streamed verbatim by
        // the master's connection layer (`ShardMessage::ReplicateVerbatim`) —
        // generic `dispatch()` does not know them. Route through the same
        // handlers the master's connection layer uses.
        if cmd.len() > 3 && cmd[..3].eq_ignore_ascii_case(b"FT.") {
            let resp = apply_ft(s, cmd, args, db_idx);
            warn_on_error(cmd, &resp);
            return true;
        }

        // GRAPH.* mutations (v0.7 graph replication) arrive as the master's
        // DETERMINISTIC WAL-record form: id-pinned (GRAPH.ADDNODE <g>
        // <node_id> …) with FNV-hashed u16 label/prop-key ids — `label_to_id`
        // is a stateless hash, so both sides resolve the same strings to the
        // same ids. Generic `dispatch()` parses the USER syntax and would
        // re-allocate ids; route through the WAL replay collector instead,
        // exactly like restart recovery does.
        #[cfg(feature = "graph")]
        if crate::graph::replay::GraphReplayCollector::is_graph_command(cmd) {
            return apply_graph(s, cmd, args);
        }

        // MQ.* effect records (Wave B stage 2b) arrive as one of the
        // `MQ._REPL.*` synthetic pseudo-commands `shard::mq_exec` emits —
        // never a real client command. Route through the SAME apply engine
        // (`apply_mq_*` in `shard::shared_databases`) boot-time WAL replay
        // uses, generalized over `MqApplyTarget` so both share one codec.
        if crate::mq::wal::is_mq_replay_command(cmd) {
            return apply_mq(s, cmd, args);
        }

        // TEMPORAL.INVALIDATE arrives as the master's deterministic
        // wall-clock-pinned form (`TEMPORAL.INVALIDATE-AT <graph> <N|E>
        // <entity_id> <wall_ms>`, round-2 finding B) — apply with the SAME
        // wall_ms the master used so `valid_to` matches exactly. Generic
        // `dispatch()` does not know this internal record.
        #[cfg(feature = "graph")]
        if cmd.eq_ignore_ascii_case(b"TEMPORAL.INVALIDATE-AT") {
            return apply_temporal_invalidate(s, cmd, args);
        }

        // SWAPDB (#386): the wire carries exactly ONE record per client
        // SWAPDB (emitted by the master's coordinator after its durability
        // gate; remote SPSC legs write AOF/WAL only). Generic `dispatch()`
        // hard-errors on SWAPDB ("must be issued at the connection handler
        // level") and `warn_on_error` only logs — so without this intercept
        // every streamed SWAPDB silently no-ops on the replica.
        if cmd.eq_ignore_ascii_case(b"SWAPDB") {
            apply_swapdb(cmd, args, &s.databases);
            return true;
        }

        // MOVE / cross-db COPY touch two databases at once and are intercepted
        // BEFORE generic dispatch on the master (see `spsc_two_db`). Generic
        // `dispatch()` cannot apply them — it returns an error for MOVE and
        // silently mis-targets COPY..DB — so mirror the master's two-db
        // intercept here. A replica never evicts on apply (it follows the
        // master), so the destination-db eviction gate is skipped.
        if cmd.eq_ignore_ascii_case(b"MOVE") || cmd.eq_ignore_ascii_case(b"COPY") {
            if let Some(resp) = apply_two_db(cmd, args, &s.databases, db_idx, db_count) {
                warn_on_error(cmd, &resp);
                return true;
            }
            // COPY with no DB clause / same-db COPY: fall through to dispatch.
        }

        let resp = {
            // Guard scoped to the dispatch alone — dropped before the
            // index-parity hooks below, which take their own guards (a second
            // guard on this db from inside this block would panic).
            let mut db = s.databases.write(db_idx);
            // Replica applies off the shard's periodic clock tick; refresh
            // directly so command-relative expiries (EXPIRE/SETEX) compute
            // against real time.
            db.refresh_now();
            let mut selected = db_idx;
            match cmd_dispatch(&mut db, cmd, args, &mut selected, db_count) {
                DispatchResult::Response(f) | DispatchResult::Quit(f) => f,
            }
        };
        // Index-plane parity (v0.7 R0.5): mirror of the master's
        // connection-layer hook block — every dispatch path that applies KV
        // writes MUST run these, or the replica's FT indexes silently diverge
        // from its own keyspace (wire-parity requirement, see
        // `auto_delete_vectors` docs).
        if !matches!(resp, Frame::Error(_)) {
            apply_index_parity_hooks(s, cmd, args, db_idx as u8);
        }
        warn_on_error(cmd, &resp);
        true
    });
    match result {
        Some(ok) => ApplyOutcome::from_poison_bool(ok),
        None => ApplyOutcome::NoShardSlice,
    }
}

/// Apply a replicated `WS.CREATE.APPLY` record (Wave B ws-plane): install the
/// MASTER's `ws_id` + `name` + `created_at` VERBATIM into the process-global
/// workspace registry. UUIDv7 is nondeterministic — a replica must never mint
/// its own; it applies exactly what the master already decided (round-2
/// finding A / task #34). In-memory only, matching `apply_graph`'s
/// no-local-persistence model — a restarted replica resyncs from its master
/// rather than replaying a local WAL copy of this record.
fn apply_ws_create(
    shard_databases: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
    cmd: &[u8],
    args: &[Frame],
) -> bool {
    let Some(payload) = crate::workspace::repl::extract_payload(args) else {
        return poison(cmd, "WS.CREATE.APPLY missing payload arg");
    };
    let Some((ws_id_bytes, name, created_at)) =
        crate::workspace::wal::decode_workspace_create(&payload)
    else {
        return poison(cmd, "WS.CREATE.APPLY payload malformed");
    };
    let id = crate::workspace::WorkspaceId::from_bytes(ws_id_bytes);
    let mut guard = shard_databases.workspace_registry();
    let reg = guard.get_or_insert_with(|| Box::new(crate::workspace::WorkspaceRegistry::new()));
    reg.insert(
        id,
        crate::workspace::registry::WorkspaceMetadata {
            id,
            name: bytes::Bytes::from(name),
            created_at,
        },
    );
    true
}

/// Apply a replicated `WS.DROP.APPLY` record (Wave B ws-plane): remove the
/// workspace from the registry, then run the SAME best-effort key-prefix
/// sweep the master's local-owner branch of `WsDropCleanup` runs (R0
/// replication is single-shard-only, so there is no cross-shard hop to
/// mirror — sweeping every db on this one shard IS the whole sweep).
fn apply_ws_drop(
    s: &mut crate::shard::slice::ShardSlice,
    shard_databases: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
    cmd: &[u8],
    args: &[Frame],
) -> bool {
    let Some(payload) = crate::workspace::repl::extract_payload(args) else {
        return poison(cmd, "WS.DROP.APPLY missing payload arg");
    };
    let Some(ws_id_bytes) = crate::workspace::wal::decode_workspace_drop(&payload) else {
        return poison(cmd, "WS.DROP.APPLY payload malformed");
    };
    let id = crate::workspace::WorkspaceId::from_bytes(ws_id_bytes);
    let removed = {
        let mut guard = shard_databases.workspace_registry();
        match guard.as_mut() {
            Some(reg) => reg.remove(&id).is_some(),
            None => false,
        }
    };
    if !removed {
        // Already absent (e.g. a re-delivered record after a resync) —
        // nothing to sweep.
        return true;
    }
    let prefix = format!("{{{}}}:", id.as_hex());
    let prefix_bytes = prefix.into_bytes();
    // Every db exclusively guarded for the whole sweep: dropping a workspace
    // must not be observable half-done (db 0 swept, db 7 not), which is the
    // atomicity this loop had for free while the slice was single-threaded.
    s.databases.with_all(|dbs| {
        for db in dbs.iter_mut() {
            let keys_to_delete: Vec<Vec<u8>> = db
                .keys()
                .filter(|k| k.as_bytes().starts_with(&prefix_bytes[..]))
                .map(|k| k.as_bytes().to_vec())
                .collect();
            for key in &keys_to_delete {
                db.remove(key);
            }
        }
    });
    true
}

/// Apply a replicated FT.* index-definition command (FT.CREATE / FT.DROPINDEX
/// / FT.CONFIG SET) through the same handlers the master's connection layer
/// uses. Deliberately NO keyspace backfill on live FT.CREATE — master parity:
/// FT.CREATE never indexes pre-existing keys outside restart recovery (and,
/// on a replica, the snapshot-load path below).
fn apply_ft(
    s: &mut crate::shard::slice::ShardSlice,
    cmd: &[u8],
    args: &[Frame],
    db_idx: usize,
) -> Frame {
    use crate::command::vector_search::{ft_admin, ft_config, ft_create};
    let db_index = db_idx as u8;
    if cmd.eq_ignore_ascii_case(b"FT.CREATE") {
        ft_create::ft_create(&mut s.vector_store, &mut s.text_store, args, db_index)
    } else if cmd.eq_ignore_ascii_case(b"FT.DROPINDEX") {
        // `try_write` keeps `get_mut`'s out-of-range-is-None contract: a
        // replica with fewer dbs than the master drops the index definition
        // without a keyspace sweep, exactly as before.
        let mut db_guard = s.databases.try_write(db_idx);
        ft_admin::ft_dropindex(
            &mut s.vector_store,
            &mut s.text_store,
            db_guard.as_deref_mut(),
            args,
            db_index,
        )
    } else if cmd.eq_ignore_ascii_case(b"FT.CONFIG") {
        ft_config::ft_config(&mut s.vector_store, &mut s.text_store, args, db_index)
    } else {
        // Only def mutations are streamed; any other FT.* here is unexpected —
        // skip quietly rather than fabricate a divergence warning.
        tracing::debug!(
            "replica apply: ignoring non-replicable FT command {}",
            String::from_utf8_lossy(cmd)
        );
        Frame::Null
    }
}

/// Apply one replicated graph WAL record (v0.7 graph replication) into this
/// shard's `GraphStore` through the same `GraphReplayCollector` restart
/// recovery uses. Records arrive one at a time in stream order; the collector
/// resolves edge/SET targets against nodes applied by EARLIER records via the
/// write-buffer seeding in `replay_epoch_aware`.
#[cfg(feature = "graph")]
fn apply_graph(s: &mut crate::shard::slice::ShardSlice, cmd: &[u8], args: &[Frame]) -> bool {
    use crate::graph::replay::GraphReplayCollector;
    let mut arg_bytes: Vec<&[u8]> = Vec::with_capacity(args.len());
    for a in args {
        match a {
            Frame::BulkString(b) | Frame::SimpleString(b) => arg_bytes.push(b.as_ref()),
            _ => {
                return poison(cmd, "non-bulk arg in graph record");
            }
        }
    }
    let mut collector = GraphReplayCollector::new();
    if !collector.collect_command(cmd, &arg_bytes) {
        return poison(cmd, "unparseable graph record");
    }
    if collector.replay_into(&mut s.graph_store) == 0 {
        // Not always divergence (e.g. GRAPH.CREATE of an existing graph
        // counts 0), but worth surfacing at debug for stream forensics.
        tracing::debug!(
            "replica apply: graph record {} replayed 0 mutations",
            String::from_utf8_lossy(cmd)
        );
    }
    true
}

/// Apply one replicated MQ effect record (Wave B stage 2b): decode the
/// SINGLE bulk-string payload arg (the verbatim `encode_mq_*` bytes
/// `shard::mq_exec::replicate_mq_record` shipped) and dispatch to the same
/// `apply_mq_*` functions boot-time WAL replay uses
/// (`shard::shared_databases`, generic over `MqApplyTarget` so this
/// `ShardSlice` target and that `ShardSliceInit` target share one engine).
///
/// A replica NEVER fires `MQ.TRIGGER` callbacks on apply (registrations are
/// stored as opaque data, same as boot-time replay — firing only happens
/// live via `MQ.PUSH`'s debounce arming on a master).
fn apply_mq(s: &mut crate::shard::slice::ShardSlice, cmd: &[u8], args: &[Frame]) -> bool {
    use crate::mq::wal::{
        MQ_REPL_ACK, MQ_REPL_CREATE, MQ_REPL_DROP, MQ_REPL_POP, MQ_REPL_PUSH, MQ_REPL_TRIGGER,
        decode_mq_ack, decode_mq_create, decode_mq_drop, decode_mq_pop, decode_mq_push,
        decode_mq_trigger,
    };
    use crate::shard::shared_databases::{
        apply_mq_ack, apply_mq_create, apply_mq_drop, apply_mq_pop, apply_mq_push, apply_mq_trigger,
    };
    use crate::storage::stream::StreamId;

    let Some(Frame::BulkString(payload)) = args.first() else {
        return poison(cmd, "missing payload arg");
    };

    // Replicas configured with fewer logical dbs than the master clamp the
    // payload's db index — same posture as the generic KV clamp in
    // `apply_local` above. Without this, `MqApplyTarget::mq_databases_mut()
    // .get_mut(out_of_range)` silently no-ops: CREATE would register the
    // queue but never create the stream, and PUSH/POP/ACK would be dropped.
    // (Boot-time WAL replay never needs this — a shard replays its OWN
    // records, whose db indices are valid by construction.)
    let db_count = s.databases.db_count();
    fn clamp_mq_db(db_count: usize, db_index: u32) -> usize {
        let idx = (db_index as usize).min(db_count.saturating_sub(1));
        if idx != db_index as usize {
            tracing::debug!(
                "replica apply: MQ db {} clamped to {} ({} dbs on this shard)",
                db_index,
                idx,
                db_count
            );
        }
        idx
    }

    let applied = if cmd.eq_ignore_ascii_case(MQ_REPL_CREATE) {
        decode_mq_create(payload).map(|(db_index, key, mdc)| {
            apply_mq_create(s, clamp_mq_db(db_count, db_index), &key, mdc);
        })
    } else if cmd.eq_ignore_ascii_case(MQ_REPL_PUSH) {
        decode_mq_push(payload).map(|(db_index, key, ms, seq, fields)| {
            apply_mq_push(
                s,
                clamp_mq_db(db_count, db_index),
                &key,
                StreamId { ms, seq },
                fields,
            );
        })
    } else if cmd.eq_ignore_ascii_case(MQ_REPL_POP) {
        decode_mq_pop(payload).map(
            |(db_index, key, last_delivered, claimed, dlq, delivery_time_ms, seen_time_ms)| {
                let claimed: Vec<(StreamId, u64)> = claimed
                    .into_iter()
                    .map(|(ms, seq, dc)| (StreamId { ms, seq }, dc))
                    .collect();
                let dlq: Vec<(StreamId, StreamId)> = dlq
                    .into_iter()
                    .map(|(src_ms, src_seq, dlq_ms, dlq_seq)| {
                        (
                            StreamId {
                                ms: src_ms,
                                seq: src_seq,
                            },
                            StreamId {
                                ms: dlq_ms,
                                seq: dlq_seq,
                            },
                        )
                    })
                    .collect();
                apply_mq_pop(
                    s,
                    clamp_mq_db(db_count, db_index),
                    &key,
                    StreamId {
                        ms: last_delivered.0,
                        seq: last_delivered.1,
                    },
                    claimed,
                    dlq,
                    delivery_time_ms,
                    seen_time_ms,
                );
            },
        )
    } else if cmd.eq_ignore_ascii_case(MQ_REPL_ACK) {
        decode_mq_ack(payload).map(|(db_index, key, ms, seq)| {
            apply_mq_ack(
                s,
                clamp_mq_db(db_count, db_index),
                &key,
                StreamId { ms, seq },
            );
        })
    } else if cmd.eq_ignore_ascii_case(MQ_REPL_TRIGGER) {
        decode_mq_trigger(payload).map(|(trig_key, queue_key, callback_cmd, debounce_ms)| {
            apply_mq_trigger(s, trig_key, queue_key, callback_cmd, debounce_ms);
        })
    } else if cmd.eq_ignore_ascii_case(MQ_REPL_DROP) {
        decode_mq_drop(payload).map(|(db_index, key)| {
            apply_mq_drop(s, clamp_mq_db(db_count, db_index), &key);
        })
    } else {
        // Unreachable: `is_mq_replay_command` gated this call. Defensive.
        tracing::debug!(
            "replica apply: unrecognized MQ replay command {}",
            String::from_utf8_lossy(cmd)
        );
        Some(())
    };

    match applied {
        Some(()) => true,
        None => poison(cmd, "malformed record payload"),
    }
}

/// Apply a replicated `TEMPORAL.INVALIDATE-AT` record: same mutation the
/// master ran (`apply_invalidate`) with the master's pinned `wall_ms`. The
/// returned `GraphTemporal` WAL payload is dropped (`Ok(_)` is ignored),
/// matching `apply_graph`'s no-local-persistence model (a restarted replica
/// resyncs from the master).
///
/// K1a: `apply_invalidate` now returns the payload directly instead of
/// pushing it into `gs.wal_pending` — there is nothing left to drain here.
#[cfg(feature = "graph")]
fn apply_temporal_invalidate(
    s: &mut crate::shard::slice::ShardSlice,
    cmd: &[u8],
    args: &[Frame],
) -> bool {
    let Some((graph_name, is_node, entity_id, wall_ms)) =
        crate::command::temporal::parse_invalidate_at(args)
    else {
        return poison(cmd, "malformed TEMPORAL.INVALIDATE-AT record");
    };
    // A decode-failure (above) is stream corruption — poison. An `Err` here
    // is a SEMANTIC failure on a well-formed record (e.g. the entity id no
    // longer exists) — logged loud but not treated as protocol desync,
    // matching `warn_on_error`'s posture for the generic KV path.
    if let Err(e) = crate::command::temporal::apply_invalidate(
        &mut s.graph_store,
        entity_id,
        is_node,
        &graph_name,
        wall_ms,
    ) {
        tracing::warn!(
            "replica apply: TEMPORAL.INVALIDATE-AT entity_id={} failed: {} \
             (replica may diverge from master)",
            entity_id,
            String::from_utf8_lossy(e)
        );
    }
    true
}

/// Mirror of the master's connection-layer index-parity block
/// (`handler_monoio/mod.rs`, "HSET auto-index" onwards): HSET feeds the
/// auto-indexer, DEL/UNLINK tombstone, HDEL of a vector field tombstones,
/// FLUSHDB/FLUSHALL clear index contents (definitions survive).
fn apply_index_parity_hooks(
    s: &mut crate::shard::slice::ShardSlice,
    cmd: &[u8],
    args: &[Frame],
    db_index: u8,
) {
    use crate::shard::spsc_handler as hooks;
    if cmd.eq_ignore_ascii_case(b"HSET") {
        if let Some(key) = args
            .first()
            .and_then(crate::server::connection::extract_bytes)
        {
            // Return value (txn vector-intents) is only meaningful inside an
            // active CrossStoreTxn; replica apply has none.
            let _ = hooks::auto_index_hset_public(
                &mut s.vector_store,
                &mut s.text_store,
                key.as_ref(),
                args,
                db_index,
            );
        }
    } else if cmd.eq_ignore_ascii_case(b"DEL") || cmd.eq_ignore_ascii_case(b"UNLINK") {
        hooks::auto_delete_vectors(&mut s.vector_store, args, db_index);
        // task #46: a replica must also tombstone its OWN WAL for any
        // durable MQ stream a replicated generic DEL/UNLINK removed —
        // otherwise the replica resurrects it on its own restart even
        // though the master's copy stayed correctly deleted.
        crate::shard::mq_exec::auto_drop_mq_streams(s, args, db_index as usize);
    } else if cmd.eq_ignore_ascii_case(b"HDEL") {
        hooks::auto_hdel_vectors(&mut s.vector_store, args, db_index);
    } else if cmd.eq_ignore_ascii_case(b"FLUSHDB") || cmd.eq_ignore_ascii_case(b"FLUSHALL") {
        // moon#677: a streamed FLUSHALL clears every database on the replica,
        // the same as on the master. Without this the master empties db0..15
        // and the replica empties only the db the record was attributed to --
        // divergence in fifteen databases, invisible until someone SELECTs
        // one of them.
        if cmd.eq_ignore_ascii_case(b"FLUSHALL") {
            // moon#677 atomicity: all 16 dbs guarded together, so no reader
            // can observe a partially-flushed keyspace.
            s.databases.with_all(|dbs| {
                crate::command::server_admin::flush_every_database(dbs, db_index as usize);
            });
        }
        hooks::auto_flush_indexes(
            &mut s.vector_store,
            &mut s.text_store,
            cmd.eq_ignore_ascii_case(b"FLUSHDB"),
            db_index,
        );
        // task #46: same replica-side tombstone requirement as DEL/UNLINK
        // above.
        crate::shard::mq_exec::auto_drop_mq_streams_on_flush(s, db_index as usize);
    }
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

/// Apply a streamed `SWAPDB a b` (#386) against the replica's full database
/// set — `ShardDbSet::swap` exchanges the two databases' CONTENTS under an
/// ascending-ordered pair of write guards, which is the same atomic
/// two-database exchange the slice-split swap gave while the slice was
/// single-threaded (and what the WAL replay intercept in
/// `persistence/replay.rs` does). Out-of-range / same-index / malformed args
/// skip with a warn: the replica must never poison its stream over an index
/// the master accepted (e.g. a replica configured with fewer `--databases`),
/// it just can't honor it.
fn apply_swapdb(cmd: &[u8], args: &[Frame], databases: &crate::shard::db_plane::ShardDbSet) {
    let parse_idx = |f: &Frame| match f {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<usize>().ok(),
        Frame::Integer(n) => usize::try_from(*n).ok(),
        _ => None,
    };
    let db_count = databases.db_count();
    match (
        args.first().and_then(parse_idx),
        args.get(1).and_then(parse_idx),
    ) {
        (Some(a), Some(b)) if a != b && a < db_count && b < db_count => {
            databases.swap(a, b);
        }
        (Some(a), Some(b)) if a == b => {} // same-index: no-op, matches Redis
        _ => {
            tracing::warn!(
                "replication apply: skipping {} with unusable args (out of range for {} local dbs)",
                String::from_utf8_lossy(cmd),
                db_count
            );
        }
    }
}

/// Apply `MOVE` / cross-db `COPY ... DB n` on the replica using the same core
/// helpers as the master's two-db intercept. Returns `None` for a same-db /
/// no-`DB`-clause COPY (caller falls through to generic dispatch), `Some(resp)`
/// otherwise.
fn apply_two_db(
    cmd: &[u8],
    args: &[Frame],
    databases: &crate::shard::db_plane::ShardDbSet,
    db_idx: usize,
    db_count: usize,
) -> Option<Frame> {
    use crate::command::keyspace::move_cmd as ksmv;

    // `with_pair` replaces `ksmv::with_two_slice_dbs`: same distinct-index and
    // in-range preconditions (both assert/panic otherwise), same closure
    // argument order, and it holds BOTH write guards for the whole move/copy
    // so the two-database mutation stays atomic to any observer.
    if cmd.eq_ignore_ascii_case(b"MOVE") {
        let resp = match ksmv::parse_move_args(args, db_count) {
            Err(e) => e,
            Ok((_key, dst)) if dst == db_idx => Frame::Integer(0),
            Ok((key, dst)) => databases.with_pair(db_idx, dst, |src, dstdb| {
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
        Ok(ca) => databases.with_pair(db_idx, ca.dst_db, |src, dst| {
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
///
/// `shard_databases` carries the process-global `WorkspaceRegistry`
/// (Wave B ws-plane) — installed AFTER the keyspace load, same authoritative-
/// replace semantics as the graph store below, but outside `try_with_shard`
/// since the registry lives on `ShardDatabases`, not `ShardSlice`.
pub(crate) fn load_snapshot(
    rdb: &[u8],
    shard_databases: &std::sync::Arc<crate::shard::shared_databases::ShardDatabases>,
) -> anyhow::Result<usize> {
    use crate::persistence::redis_rdb;
    // Moon-private aux fields (written by the master right after the RDB
    // header) carry the FT index DEFINITIONS; standard RDB loaders skip them.
    let vec_defs = redis_rdb::read_moon_aux(rdb, redis_rdb::MOON_AUX_VECTOR_DEFS);
    let text_defs = redis_rdb::read_moon_aux(rdb, redis_rdb::MOON_AUX_TEXT_DEFS);
    // R2 (task #20): a multi-shard master's merged snapshot carries one
    // graph-store aux entry PER shard (graph content is sharded) — collect
    // them all; a single-shard snapshot yields exactly one.
    #[cfg(feature = "graph")]
    let graph_blobs = redis_rdb::read_moon_aux_all(rdb, redis_rdb::MOON_AUX_GRAPH_STORE);
    // Wave B ws-plane: exactly one blob regardless of the master's shard
    // count (shard-0-authoritative — see `ws_sync` module docs).
    let ws_registry_blob = redis_rdb::read_moon_aux(rdb, redis_rdb::MOON_AUX_WORKSPACE_REGISTRY);
    // Wave B stage 2b: same per-shard-blob collection as graph — a
    // multi-shard master's merged snapshot carries one MQ registry aux
    // entry PER shard.
    let mq_blobs = redis_rdb::read_moon_aux_all(rdb, redis_rdb::MOON_AUX_MQ_REGISTRY);
    let result: anyhow::Result<usize> = match crate::shard::slice::try_with_shard(|s| {
        // Clear-then-load under ONE batch of write guards on every database:
        // a full resync replaces the whole keyspace atomically, so no reader
        // may observe the window where the old contents are gone and the new
        // ones are not yet in. That window did not exist while the slice was
        // single-threaded; splitting this into two `with_all` calls would
        // create it.
        let loaded = s.databases.with_all(|dbs| {
            for db in dbs.iter_mut() {
                db.clear();
            }
            redis_rdb::load_rdb(dbs, rdb)
        })?;
        install_snapshot_index_defs(s, vec_defs.as_deref(), text_defs.as_deref());
        // v0.7 graph replication: install the master's whole graph store
        // (authoritative replace — an EMPTY blob drops replica-local graphs;
        // an ABSENT aux means a pre-graph-sync master, warn-and-keep).
        #[cfg(feature = "graph")]
        match &graph_blobs[..] {
            blobs if !blobs.is_empty() => {
                match crate::replication::graph_sync::install_graph_store_many(
                    &mut s.graph_store,
                    blobs,
                ) {
                    Some(n) => {
                        if n > 0 {
                            tracing::info!(
                                "replica snapshot: installed {} graph(s) from {} shard blob(s)",
                                n,
                                blobs.len()
                            );
                        }
                    }
                    None => {
                        poison(b"<snapshot-graph-aux>", "malformed graph-store aux blob");
                        return Err(anyhow::anyhow!(
                            "replica snapshot: malformed graph-store aux blob"
                        ));
                    }
                }
            }
            _ => {
                if s.graph_store.graph_count() > 0 {
                    tracing::warn!(
                        "replica snapshot carried no graph-store aux but {} local graph(s) \
                         exist — master predates graph replication; keeping local graphs \
                         (they may diverge)",
                        s.graph_store.graph_count()
                    );
                }
            }
        }
        // Wave B stage 2b: install the master's MQ durable-queue + trigger
        // registries (authoritative replace — an EMPTY blob list means the
        // master sent no aux at all, a pre-MQ-replication master, so we
        // warn-and-keep local state exactly like the graph fallback above;
        // a non-empty list, even of all-zero-count blobs, is authoritative
        // and clears local registries).
        match &mq_blobs[..] {
            blobs if !blobs.is_empty() => {
                match crate::replication::mq_sync::install_mq_registry_many(s, blobs) {
                    Some((queues, triggers)) => {
                        if queues > 0 || triggers > 0 {
                            tracing::info!(
                                "replica snapshot: installed {} durable queue(s), {} \
                                 trigger(s) from {} shard blob(s)",
                                queues,
                                triggers,
                                blobs.len()
                            );
                        }
                    }
                    None => {
                        poison(b"<snapshot-mq-aux>", "malformed MQ registry aux blob");
                        return Err(anyhow::anyhow!(
                            "replica snapshot: malformed MQ registry aux blob"
                        ));
                    }
                }
            }
            _ => {
                let has_local = s
                    .durable_queue_registry
                    .as_ref()
                    .is_some_and(|r| !r.is_empty())
                    || s.trigger_registry.as_ref().is_some_and(|r| !r.is_empty());
                if has_local {
                    tracing::warn!(
                        "replica snapshot carried no MQ-registry aux but local durable \
                         queue/trigger state exists — master predates MQ replication; \
                         keeping local state (it may diverge)"
                    );
                }
            }
        }
        Ok(loaded)
    }) {
        Some(r) => r,
        None => Err(anyhow::anyhow!(
            "replica snapshot load: no ShardSlice on this thread"
        )),
    };
    let loaded = result?;

    // Wave B ws-plane: install the master's workspace registry AFTER a
    // successful keyspace load — authoritative replace (an EMPTY blob drops
    // replica-local workspaces; an ABSENT aux means a pre-WS-sync master,
    // warn-and-keep), same convention as the graph store above.
    match ws_registry_blob {
        Some(blob) => match crate::replication::ws_sync::install_workspace_registry(&blob) {
            Some(reg) => {
                let count = reg.len();
                *shard_databases.workspace_registry() = Some(Box::new(reg));
                if count > 0 {
                    tracing::info!("replica snapshot: installed {} workspace(s)", count);
                }
            }
            None => {
                poison(
                    b"<snapshot-ws-aux>",
                    "malformed workspace-registry aux blob",
                );
                return Err(anyhow::anyhow!(
                    "replica snapshot: malformed workspace-registry aux blob"
                ));
            }
        },
        None => {
            let existing = shard_databases
                .workspace_registry()
                .as_ref()
                .map_or(0, |r| r.len());
            if existing > 0 {
                tracing::warn!(
                    "replica snapshot carried no workspace-registry aux but {} local \
                     workspace(s) exist — master predates WS replication; keeping local \
                     registry (it may diverge)",
                    existing
                );
            }
        }
    }

    Ok(loaded)
}

/// Full resync = authoritative replace: drop every replica-local FT index,
/// install the master's definitions, then backfill them from the just-loaded
/// keyspace. Backfill here is "restart semantics" — the same matching-hash
/// rescan restart recovery performs (`event_loop.rs`, "Auto-reindex existing
/// HASH keys"); live FT.CREATE apply deliberately does NOT backfill.
fn install_snapshot_index_defs(
    s: &mut crate::shard::slice::ShardSlice,
    vec_defs: Option<&[u8]>,
    text_defs: Option<&[u8]>,
) {
    let names: Vec<bytes::Bytes> = s.vector_store.index_names().into_iter().cloned().collect();
    let local_text_names = s.text_store.index_names();
    let (local_vec, local_text) = (names.len(), local_text_names.len());
    for n in &names {
        s.vector_store.drop_index(n);
    }
    for n in local_text_names {
        s.text_store.drop_index(&n);
    }
    // The authoritative drop above is silent when the master streams no defs
    // at all (pre-R0.5 master, or def serialization failed) — but wiping a
    // replica's local indexes with zero replacement is exactly the scenario an
    // operator needs to hear about (mixed-version rolling upgrade, REPLICAOF
    // pointed at the wrong host).
    if vec_defs.is_none() && text_defs.is_none() {
        if local_vec + local_text > 0 {
            tracing::warn!(
                "replica full resync: dropped {} local vector and {} local text index(es); \
                 the master's snapshot carried NO index definitions (pre-R0.5 master or \
                 def-serialization failure) — FT indexes are gone on this replica",
                local_vec,
                local_text
            );
        }
        return;
    }

    // (db_index, key_prefix) pairs feeding the backfill scan below.
    let mut prefixes: Vec<(u8, bytes::Bytes)> = Vec::new();

    if let Some(blob) = vec_defs {
        match crate::vector::index_persist::deserialize_index_metas_with_weights(blob) {
            Ok(pairs) => {
                for (meta, weight) in pairs {
                    for p in &meta.key_prefixes {
                        prefixes.push((meta.db_index, p.clone()));
                    }
                    let name = meta.name.clone();
                    if let Err(e) = s.vector_store.create_index(meta) {
                        tracing::warn!(
                            "replica snapshot: failed to create vector index '{}': {}",
                            String::from_utf8_lossy(&name),
                            e
                        );
                        continue;
                    }
                    if weight != 1.0
                        && let Some(idx) = s.vector_store.get_index_mut(&name)
                    {
                        idx.set_compaction_weight(weight);
                    }
                }
            }
            Err(e) => tracing::warn!(
                "replica snapshot: vector index defs unreadable ({}); vector indexes NOT replicated",
                e
            ),
        }
    }

    #[cfg(not(feature = "text-index"))]
    if text_defs.is_some() {
        tracing::warn!(
            "replica snapshot: master streamed text index defs but this build lacks the \
             text-index feature; text indexes NOT replicated"
        );
    }
    #[cfg(feature = "text-index")]
    if let Some(blob) = text_defs {
        match crate::text::index_persist::deserialize_text_index_metas(blob) {
            Ok(metas) => {
                for meta in metas {
                    for p in &meta.key_prefixes {
                        prefixes.push((meta.db_index, p.clone()));
                    }
                    let mut text_index = crate::text::store::TextIndex::new(
                        meta.name.clone(),
                        meta.key_prefixes.clone(),
                        meta.text_fields.clone(),
                        meta.bm25_config,
                    );
                    // Carry the master's db binding forward (WS5a) — same as
                    // the restart-recovery restore path.
                    text_index.db_index = meta.db_index;
                    if let Err(e) = s.text_store.create_index(meta.name.clone(), text_index) {
                        tracing::warn!(
                            "replica snapshot: failed to create text index '{}': {}",
                            String::from_utf8_lossy(&meta.name),
                            e
                        );
                    }
                }
            }
            Err(e) => tracing::warn!(
                "replica snapshot: text index defs unreadable ({}); text indexes NOT replicated",
                e
            ),
        }
    }

    if prefixes.is_empty() {
        return;
    }
    let db_count = s.databases.db_count();
    let mut backfilled = 0usize;
    for db_idx in 0..db_count {
        let wanted: Vec<&bytes::Bytes> = prefixes
            .iter()
            .filter(|(d, _)| *d as usize == db_idx)
            .map(|(_, p)| p)
            .collect();
        if wanted.is_empty() {
            continue;
        }
        // Shared guard, scoped to the scan only: `collect_matching_hash_args`
        // returns owned data, so the guard is released before the
        // `auto_index_hset_public` loop below (which must not hold one).
        let matching = {
            let db = s.databases.read(db_idx);
            collect_matching_hash_args(&db, |key| wanted.iter().any(|p| key.starts_with(&p[..])))
        };
        for (key, args) in &matching {
            let _ = crate::shard::spsc_handler::auto_index_hset_public(
                &mut s.vector_store,
                &mut s.text_store,
                key,
                args,
                db_idx as u8,
            );
        }
        backfilled += matching.len();
    }
    if backfilled > 0 {
        tracing::info!(
            "replica snapshot: backfilled {} hash key(s) into replicated FT indexes",
            backfilled
        );
    }
}

/// Collect `(key, HSET-shaped args)` for every HASH key in `db` matching
/// `key_matches` — args are `[key, field, value, ...]`, ready for
/// `auto_index_hset_public`. Same extraction as restart recovery's rescan
/// closure in `event_loop.rs`.
fn collect_matching_hash_args(
    db: &crate::storage::Database,
    key_matches: impl Fn(&[u8]) -> bool,
) -> Vec<(Vec<u8>, Vec<Frame>)> {
    use crate::storage::compact_value::RedisValueRef;
    let mut matching = Vec::new();
    for (key, entry) in db.data().iter() {
        let key_bytes = key.as_bytes();
        if !key_matches(key_bytes) {
            continue;
        }
        let mut args = Vec::new();
        args.push(Frame::BulkString(bytes::Bytes::copy_from_slice(key_bytes)));
        match entry.as_redis_value() {
            RedisValueRef::Hash(map) => {
                for (field, value) in map.iter() {
                    args.push(Frame::BulkString(bytes::Bytes::copy_from_slice(field)));
                    args.push(Frame::BulkString(bytes::Bytes::copy_from_slice(value)));
                }
            }
            RedisValueRef::HashListpack(lp) => {
                let entries: Vec<_> = lp.iter().collect();
                let mut j = 0;
                while j + 1 < entries.len() {
                    args.push(Frame::BulkString(bytes::Bytes::from(entries[j].as_bytes())));
                    args.push(Frame::BulkString(bytes::Bytes::from(
                        entries[j + 1].as_bytes(),
                    )));
                    j += 2;
                }
            }
            _ => continue,
        }
        if args.len() > 1 {
            matching.push((key_bytes.to_vec(), args));
        }
    }
    matching
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

    // ── Poison-record policy tests (task #48) ──────────────────────────
    //
    // Each test feeds ONE malformed record for a different plane through
    // `apply_local` on a freshly-initialized `ShardSlice` and asserts the
    // unified outcome: `ApplyOutcome::Poisoned`, the INFO counter advanced
    // by exactly one, and — where the plane has externally observable state
    // (the workspace registry) — that state is untouched.

    /// Build a `ReplCommand` bound to db 0 from `cmd` + raw byte args (no
    /// RESP round-trip needed; `apply_local` only reads the already-parsed
    /// `Frame::Array`).
    fn repl_cmd(cmd: &[u8], args: &[&[u8]]) -> ReplCommand {
        let mut arr = vec![Frame::BulkString(Bytes::copy_from_slice(cmd))];
        arr.extend(
            args.iter()
                .map(|a| Frame::BulkString(Bytes::copy_from_slice(a))),
        );
        ReplCommand {
            db_index: 0,
            command: Arc::new(Frame::Array(arr.into())),
        }
    }

    /// Run `body` on a freshly-initialized `ShardSlice` (one db, shard 0) on
    /// a dedicated OS thread, with a matching `ShardDatabases` Arc — same
    /// recipe `shard::shared_databases`'s own tests use.
    fn on_fresh_shard<F, R>(body: F) -> R
    where
        F: FnOnce(&std::sync::Arc<crate::shard::shared_databases::ShardDatabases>) -> R
            + Send
            + 'static,
        R: Send + 'static,
    {
        std::thread::spawn(move || {
            let dbs = vec![vec![crate::storage::Database::new()]];
            let (shared, mut inits) = crate::shard::shared_databases::ShardDatabases::new(dbs);
            crate::shard::slice::init_shard(crate::shard::slice::ShardSlice::new(inits.remove(0)));
            body(&shared)
        })
        .join()
        .expect("test shard thread panicked")
    }

    fn poison_count() -> u64 {
        REPL_POISON_RECORDS_TOTAL.load(Ordering::Relaxed)
    }

    /// `REPL_POISON_RECORDS_TOTAL` is a single process-wide atomic (by
    /// design — it backs one INFO counter), so the `before`/`after` delta
    /// assertions below are only meaningful if no other poison test runs
    /// concurrently. `cargo test` runs tests in the same binary in parallel
    /// by default; serialize just this file's poison tests against each
    /// other with a dedicated lock (cheap — these tests are not
    /// performance-sensitive) rather than weakening the assertions.
    static POISON_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // Without the graph feature `apply_graph` is compiled out, so a
    // GRAPH.* record falls through to generic dispatch and is warn-and-
    // continue (unknown command), not Poisoned — the poison contract for
    // graph records only exists when the graph plane does.
    #[cfg(feature = "graph")]
    #[test]
    fn poison_graph_record_kicks_and_counts() {
        let _guard = POISON_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = poison_count();
        let outcome = on_fresh_shard(|shard_databases| {
            // GRAPH.ADDNODE with a missing required arg — `collect_command`
            // rejects it (wrong shape), never a valid mutation.
            let rc = repl_cmd(b"GRAPH.ADDNODE", &[b"mygraph"]);
            apply_local(&rc, shard_databases)
        });
        assert!(
            matches!(outcome, ApplyOutcome::Poisoned),
            "malformed graph record must be Poisoned, not silently applied/skipped"
        );
        assert_eq!(
            poison_count(),
            before + 1,
            "poison counter must advance by exactly one"
        );
    }

    #[test]
    fn poison_mq_record_kicks_and_counts() {
        let _guard = POISON_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        use crate::mq::wal::MQ_REPL_PUSH;
        let before = poison_count();
        let outcome = on_fresh_shard(|shard_databases| {
            // Missing the single payload bulk-string arg every MQ._REPL.*
            // record requires.
            let rc = repl_cmd(MQ_REPL_PUSH, &[]);
            apply_local(&rc, shard_databases)
        });
        assert!(
            matches!(outcome, ApplyOutcome::Poisoned),
            "malformed MQ record must be Poisoned, not silently applied/skipped"
        );
        assert_eq!(poison_count(), before + 1);
    }

    // TEMPORAL.INVALIDATE-AT apply lives behind the graph feature too.
    #[cfg(feature = "graph")]
    #[test]
    fn poison_temporal_invalidate_record_kicks_and_counts() {
        let _guard = POISON_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = poison_count();
        let outcome = on_fresh_shard(|shard_databases| {
            // Wrong arg count (`parse_invalidate_at` requires exactly 4).
            let rc = repl_cmd(b"TEMPORAL.INVALIDATE-AT", &[b"g", b"N"]);
            apply_local(&rc, shard_databases)
        });
        assert!(
            matches!(outcome, ApplyOutcome::Poisoned),
            "malformed TEMPORAL.INVALIDATE-AT record must be Poisoned"
        );
        assert_eq!(poison_count(), before + 1);
    }

    #[test]
    fn poison_ws_create_record_kicks_counts_and_leaves_registry_untouched() {
        let _guard = POISON_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = poison_count();
        let (outcome, registry_after) = on_fresh_shard(|shard_databases| {
            let rc = repl_cmd(crate::workspace::repl::WS_CREATE_APPLY_CMD, &[]); // missing payload
            let outcome = apply_local(&rc, shard_databases);
            let count = shard_databases
                .workspace_registry()
                .as_ref()
                .map_or(0, |r| r.len());
            (outcome, count)
        });
        assert!(
            matches!(outcome, ApplyOutcome::Poisoned),
            "malformed WS.CREATE.APPLY record must be Poisoned, not silently skipped"
        );
        assert_eq!(poison_count(), before + 1);
        assert_eq!(
            registry_after, 0,
            "no workspace must be installed from a poison record"
        );
    }

    #[test]
    fn poison_ws_drop_record_kicks_and_counts() {
        let _guard = POISON_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = poison_count();
        let outcome = on_fresh_shard(|shard_databases| {
            let rc = repl_cmd(crate::workspace::repl::WS_DROP_APPLY_CMD, &[]); // missing payload
            apply_local(&rc, shard_databases)
        });
        assert!(
            matches!(outcome, ApplyOutcome::Poisoned),
            "malformed WS.DROP.APPLY record must be Poisoned, not silently skipped"
        );
        assert_eq!(poison_count(), before + 1);
    }

    #[test]
    fn well_formed_record_does_not_poison() {
        let _guard = POISON_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let before = poison_count();
        let outcome = on_fresh_shard(|shard_databases| {
            let rc = repl_cmd(b"SET", &[b"k", b"v"]);
            apply_local(&rc, shard_databases)
        });
        assert!(matches!(outcome, ApplyOutcome::Applied));
        assert_eq!(
            poison_count(),
            before,
            "a well-formed record must never advance the poison counter"
        );
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
        // Also increments the shared poison counter (see `poison()`) — take
        // the same lock the dedicated poison tests use so it can't land its
        // increment inside another test's before/after window.
        let _guard = POISON_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
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

    // ── #386: streamed SWAPDB apply ─────────────────────────────────────

    /// Marker key in db `i` so a swap is observable.
    fn dbs_with_markers(n: usize) -> Vec<crate::storage::Database> {
        (0..n)
            .map(|i| {
                let mut db = crate::storage::Database::new();
                db.set(
                    Bytes::copy_from_slice(format!("marker:{i}").as_bytes()),
                    crate::storage::Entry::new_string(Bytes::copy_from_slice(
                        format!("from-db-{i}").as_bytes(),
                    )),
                );
                db
            })
            .collect()
    }

    /// The same markers, behind a `ShardDbSet` — `build_sets` constructs the
    /// per-shard set WITHOUT touching the process-wide registry `OnceLock`, so
    /// these tests stay independent of every other test in the binary.
    fn db_set_with_markers(n: usize) -> std::sync::Arc<crate::shard::db_plane::ShardDbSet> {
        let sets = crate::shard::db_plane::build_sets(vec![dbs_with_markers(n)]);
        std::sync::Arc::clone(&sets[0])
    }

    fn has_marker(db: &mut crate::storage::Database, origin: usize) -> bool {
        db.get(format!("marker:{origin}").as_bytes()).is_some()
    }

    /// One guard at a time — the re-entrancy mask forbids holding two guards
    /// on the same db, and each temporary here is dropped at the end of its
    /// statement.
    fn set_has_marker(
        set: &crate::shard::db_plane::ShardDbSet,
        db_idx: usize,
        origin: usize,
    ) -> bool {
        has_marker(&mut set.write(db_idx), origin)
    }

    #[test]
    fn apply_swapdb_swaps_databases() {
        let dbs = db_set_with_markers(4);
        let args = [
            Frame::BulkString(Bytes::from_static(b"0")),
            Frame::BulkString(Bytes::from_static(b"2")),
        ];
        apply_swapdb(b"SWAPDB", &args, &dbs);
        assert!(set_has_marker(&dbs, 0, 2), "db0 must now hold db2's data");
        assert!(set_has_marker(&dbs, 2, 0), "db2 must now hold db0's data");
        assert!(set_has_marker(&dbs, 1, 1), "db1 untouched");
        assert!(set_has_marker(&dbs, 3, 3), "db3 untouched");
    }

    #[test]
    fn apply_swapdb_integer_args_and_reversed_order() {
        let dbs = db_set_with_markers(3);
        // Integer frames + b > a ordering must both work.
        let args = [Frame::Integer(2), Frame::Integer(1)];
        apply_swapdb(b"SWAPDB", &args, &dbs);
        assert!(set_has_marker(&dbs, 1, 2));
        assert!(set_has_marker(&dbs, 2, 1));
    }

    #[test]
    fn apply_swapdb_out_of_range_and_same_index_are_noops() {
        let dbs = db_set_with_markers(2);
        // Out of range for this replica's db_count — skip, don't panic.
        let oor = [Frame::Integer(0), Frame::Integer(9)];
        apply_swapdb(b"SWAPDB", &oor, &dbs);
        assert!(set_has_marker(&dbs, 0, 0));
        // Same index — no-op.
        let same = [Frame::Integer(1), Frame::Integer(1)];
        apply_swapdb(b"SWAPDB", &same, &dbs);
        assert!(set_has_marker(&dbs, 1, 1));
        // Malformed (missing / non-numeric args) — skip, don't panic.
        apply_swapdb(b"SWAPDB", &[], &dbs);
        let junk = [
            Frame::BulkString(Bytes::from_static(b"x")),
            Frame::Integer(1),
        ];
        apply_swapdb(b"SWAPDB", &junk, &dbs);
        assert!(set_has_marker(&dbs, 0, 0));
        assert!(set_has_marker(&dbs, 1, 1));
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
