//! Owner-side MQ.* command execution for the ShardSlice (shardslice-migration C2).
//!
//! `execute_mq_on_owner` runs **on the shard thread** that owns the queue key,
//! after a `ShardMessage::MqCommand` hop. It faithfully mirrors ALL SIX subcommand
//! arms from `handler_sharded/write.rs` and `handler_monoio` (which are byte-
//! identical mirrors of each other).
//!
//! # Key derivation
//!
//! `MqCommandPayload.key_prefix` carries the pre-computed workspace prefix
//! `"{<ws_hex>}:"` (35 bytes) when the connection is WS-bound, or an empty
//! `Bytes` otherwise. The owner derives the effective key as:
//!
//! ```text
//! effective_key = concat(key_prefix, raw_queue_key)
//! ```
//!
//! The `trigger_key` used by PUSH/TRIGGER also needs the prefix but WITHOUT the
//! hash-tag braces: `ws_hex:raw_queue_key`. This is extracted from `key_prefix`
//! by stripping the leading `{` and the trailing `}:` characters (see
//! `derive_trig_key`).
//!
//! # WAL appends
//!
//! WAL records are written via the slice's `wal_append_tx` channel sender
//! (fire-and-forget, same as the existing `ShardDatabases::wal_append` path).
//! Only MQ.CREATE and MQ.ACK emit WAL records — matching the lock-path arms.
//!
//! # Mirror divergences found
//!
//! NONE: the two runtime mirrors (`handler_sharded/write.rs` and
//! `handler_monoio/write.rs`) are byte-identical for MQ logic. The cached-clock
//! read in the PUSH trigger-debounce arm is resolved by passing `now_ms`
//! explicitly (the owner shard's thread-local clock, same as the cached clock).

use bytes::Bytes;

use crate::command::mq::{
    ERR_MQ_NOT_DURABLE, ERR_MQ_UNKNOWN_SUB, parse_mq_subcommand, validate_mq_ack,
    validate_mq_create, validate_mq_dlqlen, validate_mq_pop, validate_mq_push, validate_mq_trigger,
};
use crate::mq::is_mq_command;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::current_time_ms;
use crate::storage::stream::StreamId;

/// Execute one MQ.* subcommand on the owning shard thread against its ShardSlice.
///
/// Called from `spsc_handler::handle_shard_message_shared` when
/// `ShardMessage::MqCommand` is received. Takes the three data fields directly
/// (the caller destructures `MqCommandPayload` and sends the `reply_tx`
/// separately) so no dummy channel allocation is needed.
///
/// # Contract
///
/// - Runs synchronously on the shard's OS thread.
/// - `with_shard` / `with_shard_db` calls inside this function are NOT
///   re-entrant: each subcommand handler uses exactly one `with_shard*` call
///   per logical step, and no call is nested.
/// - Allocations: only at result-building boundaries (`Vec::with_capacity`).
///   No `format!`, `to_string`, or needless `clone` on hot fields.
/// - WAL appends use fire-and-forget `try_send` via `wal_append_on_slice`.
///
/// `gate` is the caller's maxmemory / per-db-quota write gate (moon#1250),
/// run before a subcommand that grows the keyspace (CREATE, PUSH) — the
/// same gate its runtime runs before any other write, which MQ bypassed
/// entirely. It is only called while a limit is configured
/// (`eviction::write_gate_active`).
pub(crate) fn execute_mq_on_owner(
    db_index: usize,
    key_prefix: Bytes,
    command: std::sync::Arc<Frame>,
    gate: MqWriteGate<'_>,
) -> Frame {
    // full_args[0] = command name (b"MQ"), full_args[1..] = subcommand + params.
    // Mirrors try_handle_mq_command which receives cmd and cmd_args separately:
    // cmd = b"MQ", cmd_args = full_args[1..].
    let full_args = match &*command {
        Frame::Array(a) if a.len() >= 2 => a.as_slice(),
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid MQ command format"));
        }
    };

    // Verify the command name is MQ.
    let cmd_name = match &full_args[0] {
        Frame::BulkString(b) => b.as_ref(),
        _ => return Frame::Error(Bytes::from_static(b"ERR invalid command format")),
    };
    if !is_mq_command(cmd_name) {
        return Frame::Error(Bytes::from_static(ERR_MQ_UNKNOWN_SUB));
    }

    // cmd_args mirrors what try_handle_mq_command receives: subcommand at [0].
    let cmd_args = &full_args[1..];
    let sub = match parse_mq_subcommand(cmd_args) {
        Ok(s) => s,
        Err(e) => return e,
    };

    if sub.eq_ignore_ascii_case(b"CREATE") {
        return handle_create(cmd_args, &key_prefix, db_index, gate);
    }
    if sub.eq_ignore_ascii_case(b"PUSH") {
        return handle_push(cmd_args, &key_prefix, db_index, gate);
    }
    if sub.eq_ignore_ascii_case(b"POP") {
        return handle_pop(cmd_args, &key_prefix, db_index);
    }
    if sub.eq_ignore_ascii_case(b"ACK") {
        return handle_ack(cmd_args, &key_prefix, db_index);
    }
    if sub.eq_ignore_ascii_case(b"DLQLEN") {
        return handle_dlqlen(cmd_args, &key_prefix, db_index);
    }
    if sub.eq_ignore_ascii_case(b"TRIGGER") {
        return handle_trigger(cmd_args, &key_prefix, db_index);
    }

    Frame::Error(Bytes::from_static(ERR_MQ_UNKNOWN_SUB))
}

/// The write gate a caller hands [`execute_mq_on_owner`] (moon#1250): run
/// against the owner database (`databases[db_index]`) before a write that
/// grows it; `Err` is the refusal (`-OOM ...`) the subcommand answers with.
pub(crate) type MqWriteGate<'a> = &'a mut dyn FnMut(&mut Database, usize) -> Result<(), Frame>;

/// Run `gate` only while a memory limit is configured — the one relaxed
/// atomic load every other write path pays when none is.
#[inline]
fn run_gate(gate: &mut MqWriteGate<'_>, db: &mut Database, db_index: usize) -> Result<(), Frame> {
    if crate::storage::eviction::write_gate_active() {
        gate(db, db_index)
    } else {
        Ok(())
    }
}

/// Apply a stream's drained byte delta (`Stream::take_unbilled`) to
/// `used_memory` (moon#1250) — what the X* commands do after every mutation
/// (moon#1163). The MQ subcommands, the TXN MQ.PUBLISH materialization, the
/// replica MQ apply and the stream waker write streams outside those
/// commands and never drained it, so a queue grew without `used_memory`
/// seeing it and `maxmemory` could not bind.
#[inline]
pub(crate) fn bill_stream_delta(db: &mut Database, delta: isize) {
    if delta >= 0 {
        db.charge_memory(delta.unsigned_abs());
    } else {
        db.credit_memory(delta.unsigned_abs());
    }
}

// ── Effective-key derivation ──────────────────────────────────────────────────

/// Build the effective queue key: `concat(key_prefix, raw_queue_key)`.
///
/// When `key_prefix` is empty (no workspace binding), returns a copy of
/// `raw_queue_key`. Otherwise prepends the workspace prefix (`{ws_hex}:`).
///
/// Allocation: one `Vec` per call; acceptable at command granularity.
#[inline]
fn effective_key(key_prefix: &Bytes, raw_key: &Bytes) -> Bytes {
    if key_prefix.is_empty() {
        raw_key.clone()
    } else {
        let mut buf = Vec::with_capacity(key_prefix.len() + raw_key.len());
        buf.extend_from_slice(key_prefix);
        buf.extend_from_slice(raw_key);
        Bytes::from(buf)
    }
}

/// Build the trigger registry key from `key_prefix` and `raw_queue_key`.
///
/// The trigger registry indexes triggers by `ws_hex:queue_key` (without the
/// hash-tag braces), while `key_prefix = "{ws_hex}:"`. Strip the `{` at [0]
/// and the `}` at [key_prefix.len()-2] (keeping the trailing `:`):
///
/// ```text
/// key_prefix = "{" + ws_hex(32) + "}:"  → 35 bytes
/// trig_prefix = ws_hex(32) + ":"        → 33 bytes = key_prefix[1..33] + ":"
/// ```
///
/// When `key_prefix` is empty (no workspace) the trigger key equals `raw_queue_key`.
#[inline]
fn derive_trig_key(key_prefix: &Bytes, raw_queue_key: &Bytes) -> Bytes {
    if key_prefix.is_empty() {
        raw_queue_key.clone()
    } else {
        // key_prefix = "{" + ws_hex(32) + "}:"
        // We want: ws_hex(32) + ":" + raw_queue_key
        // i.e. key_prefix[1..key_prefix.len()-1] + raw_queue_key
        // key_prefix.len()-1 strips the trailing ":" — wait, we keep ":"
        // key_prefix[1..] = ws_hex + "}:"  → we want ws_hex + ":"
        // → key_prefix[1..key_prefix.len()-1] gives "ws_hex}" — no.
        // Let's be explicit:
        //   key_prefix bytes: b'{', hex×32, b'}', b':'
        //   We want: hex×32, b':', raw_queue_key...
        //   That is: &key_prefix[1..key_prefix.len()-1] + raw_queue_key
        //   key_prefix[1..len-1] = hex(32) + "}" — still has "}"
        //
        // Correct: we want key_prefix[1..len-2] + ":" + raw_queue_key
        //   key_prefix[1..len-2] = hex(32)
        //   then append ":" (1 byte) and raw_queue_key
        let prefix_len = key_prefix.len();
        if prefix_len < 4 {
            // Malformed prefix; fall back to raw key
            return raw_queue_key.clone();
        }
        // ws_hex bytes = key_prefix[1..prefix_len-2]
        let ws_hex = &key_prefix[1..prefix_len - 2];
        let mut buf = Vec::with_capacity(ws_hex.len() + 1 + raw_queue_key.len());
        buf.extend_from_slice(ws_hex);
        buf.push(b':');
        buf.extend_from_slice(raw_queue_key);
        Bytes::from(buf)
    }
}

// ── WAL append helper ─────────────────────────────────────────────────────────

/// Append a WAL record on the current shard thread via the slice's
/// `wal_append_tx` channel (fire-and-forget), tagged with its REAL
/// `record_type` (K1a) — the payload is UNFRAMED (no nested `write_wal_v3_record`
/// pre-framing); the event-loop drain calls `wal.append(record_type, &payload)`
/// directly.
///
/// Failure policy (CodeRabbit PR #291): `try_send` is structurally required
/// here — every caller runs ON the shard thread, which is also the channel's
/// sole consumer (the 1ms-tick drain in `event_loop.rs`), so blocking on a
/// full channel would deadlock the drain that empties it. Overflow therefore
/// needs >4096 records enqueued within a single tick. When it DOES happen
/// the mutation has already been applied and cannot be unwound mid-commit,
/// so the record loss is made LOUD instead of silent: `tracing::error!` +
/// the `RECL_WAL_APPEND_CHANNEL_DROPPED_TOTAL` INFO counter, giving
/// operators a durability-gap signal to alert on.
///
/// `pub(crate)`: also called from `src/server/conn/handler_monoio/txn.rs` and
/// `src/server/conn/handler_sharded/txn.rs` (MQ.PUBLISH self-fold) and
/// `src/shard/spsc_handler.rs` (`MqTxnMaterialize` foreign-leg fold) to emit
/// `MqPush` records at TXN materialization time (Wave B stage 2a).
#[inline]
pub(crate) fn wal_append_on_slice(
    record_type: crate::persistence::wal_v3::record::WalRecordType,
    payload: bytes::Bytes,
) {
    crate::shard::slice::with_shard(|s| {
        if let Some(ref tx) = s.wal_append_tx {
            if tx.try_send((record_type, payload)).is_err() {
                crate::command::info_reclamation::RECL_WAL_APPEND_CHANNEL_DROPPED_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    ?record_type,
                    "WAL append channel rejected a plane record AFTER the \
                     in-memory mutation was applied — this record will be \
                     MISSING from crash recovery. The channel is drained by \
                     this same shard thread every 1ms (capacity 4096), so \
                     this indicates a pathological single-tick burst."
                );
            }
        }
    });
}

/// TXN.COMMIT's MQ.PUBLISH materialization on the queue's owner shard: add
/// each intent's fields to its durable stream in `db` (`databases[db_index]`)
/// and return one `MqPush` WAL payload per applied intent, each carrying the
/// ASSIGNED id so replay is outcome-deterministic (Wave B stage 2a). A
/// missing or non-durable queue is skipped.
///
/// The one body behind the self-shard legs of both runtimes' TXN.COMMIT
/// (`handler_monoio/txn.rs`, `handler_sharded/txn.rs`) and the foreign-shard
/// `MqTxnMaterialize` arm (`spsc_handler.rs`). Each queue key's epoch-start
/// state is captured before the push (moon#1228): like the MQ subcommands,
/// this writes the stream outside `command::dispatch`.
pub(crate) fn materialize_mq_intents(
    db: &mut crate::storage::Database,
    db_index: usize,
    intents: &[crate::transaction::MqIntent],
) -> Vec<Vec<u8>> {
    let mut payloads = Vec::with_capacity(intents.len());
    for intent in intents {
        crate::persistence::snapshot_cow::capture_write_pre_image(db, db_index, &intent.queue_key);
        if let Ok(Some(stream)) = db.get_stream_mut(&intent.queue_key)
            && stream.durable
        {
            let msg_id = stream.next_auto_id();
            // Review 5: a push `add` refuses (moon#1249: the queue's last
            // possible ID is taken) added nothing, so it logs nothing.
            if stream.add(msg_id, intent.fields.clone()).is_some() {
                payloads.push(crate::mq::wal::encode_mq_push(
                    db_index as u32,
                    &intent.queue_key,
                    msg_id.ms,
                    msg_id.seq,
                    &intent.fields,
                ));
            }
            // moon#1250: charged like an MQ PUSH.
            let delta = stream.take_unbilled();
            bill_stream_delta(db, delta);
        }
    }
    payloads
}

// ── Replication emission (Wave B stage 2b) ────────────────────────────────

/// The current shard's index, read off the thread-local `ShardSlice`.
/// `execute_mq_on_owner` always runs on a shard's own OS thread (either
/// invoked locally by `write.rs::mq_hop_or_local` or via the `MqCommand`
/// SPSC hop), so this is always available.
#[inline]
pub(crate) fn current_shard_id() -> usize {
    crate::shard::slice::with_shard(|s| s.shard_id)
}

/// Replicate one MQ effect record to the live stream, mirroring the graph
/// plane's single-shard gate
/// (`server::conn::handler_monoio::write.rs`'s `record_local_write_db` call
/// site: `num_shards == 1 && replication_fanout_active`). Ctx-free —
/// `execute_mq_on_owner` runs on the shard's own OS thread without a
/// `ConnectionContext` in scope, so this resolves the SAME per-process
/// `ReplicationState` handle the connection layer uses via
/// `replication::state::record_local_write_db_global` (see that function's
/// docs for why sharing the global Arc is safe).
///
/// `cmd_name` is one of the `MQ._REPL.*` synthetic pseudo-commands
/// (`crate::mq::wal::MQ_REPL_*`) — never dispatched to real clients, matched
/// only by `replication::apply::apply_local`'s command-name routing (mirrors
/// `GraphReplayCollector::is_graph_command`). `payload` is the SAME encoded
/// WAL record bytes already built for `wal_append_on_slice`, so the
/// replica's apply arm can call the identical `decode_mq_*` function WAL
/// replay uses — one wire form, two consumers.
///
/// Multi-shard masters simply don't stream (the `num_shards == 1` gate),
/// same durability-without-live-replication posture graph accepts for its
/// own multi-shard gap — a durable queue is still WAL-durable and covered by
/// FULLRESYNC, it just doesn't get incremental live updates yet.
///
/// monoio-only (`ShardSlice` / `shard::self_msg` are thread-per-core
/// primitives — see `shard::self_msg` module docs); a no-op under
/// `runtime-tokio`, where `execute_mq_on_owner` is unreachable in practice
/// (`ShardSlice` is not the tokio runtime's storage model).
#[cfg(feature = "runtime-monoio")]
#[inline]
fn replicate_mq_record(shard_id: usize, db_index: usize, cmd_name: &'static [u8], payload: &[u8]) {
    if !crate::replication::state::fanout_hint_active() {
        return;
    }
    let Some(repl_state) = crate::admin::metrics_setup::get_global_repl_state_arc() else {
        return;
    };
    let single_shard = repl_state.read().num_shards() == 1;
    if !single_shard {
        return;
    }
    let frame = Frame::Array(
        vec![
            Frame::BulkString(Bytes::from_static(cmd_name)),
            Frame::BulkString(Bytes::copy_from_slice(payload)),
        ]
        .into(),
    );
    let record_bytes = crate::persistence::aof::serialize_command(&frame);
    crate::replication::state::record_local_write_db_global(shard_id, db_index, record_bytes);
}

#[cfg(not(feature = "runtime-monoio"))]
#[inline]
fn replicate_mq_record(
    _shard_id: usize,
    _db_index: usize,
    _cmd_name: &'static [u8],
    _payload: &[u8],
) {
}

// ── Generic-keyspace tombstone hooks (kernel M3 stage 3 / task #46) ───────────
//
// MQ durable streams live as ordinary keys in the shard's keyspace, so a
// GENERIC `DEL`/`UNLINK`/`FLUSHDB`/`FLUSHALL` (not an `MQ.*` command) can
// remove one without going through `handle_create`/etc. Before this fix
// `replay_mq_wal` had no way to represent "this queue was deleted after
// these pushes": a kill-9 after such a delete resurrected the full
// pre-delete content on restart. These hooks mirror the vector/text
// index-parity hooks (`auto_delete_vectors` / `auto_flush_indexes` in
// `spsc_handler.rs`) and the WS.DROP `WorkspaceDrop` precedent, and are
// called from every connection-layer write path AFTER the generic command
// already succeeded (same call shape as those hooks) — using DIRECT field
// access on an already-owned `&mut ShardSlice` (never re-enters
// `with_shard`, matching the "Direct field access" convention documented at
// the KV undo-log call site in `handler_monoio/mod.rs`).
//
// Scope decision: `DurableQueueRegistry` keys are NOT db-indexed (the same
// pre-existing limitation `MqCreate`'s registry already has — see its doc
// comment); a key match tombstones regardless of which db the command ran
// in, and `FLUSHDB` drops every registered durable queue exactly like
// `FLUSHALL` does (the registry has no way to scope to one db). Two
// different dbs sharing an MQ queue NAME is not a supported configuration.

/// Tombstone any durable MQ stream(s) removed by a generic-keyspace
/// `DEL`/`UNLINK`. For each key argument found in
/// `s.durable_queue_registry`, removes the registry entry, deletes the
/// stream from `s.databases[db_index]` (idempotent if the generic command
/// already did so), and appends + replicates an `MqDrop` WAL tombstone.
pub(crate) fn auto_drop_mq_streams(
    s: &mut crate::shard::slice::ShardSlice,
    cmd_args: &[Frame],
    db_index: usize,
) {
    let Some(reg) = s.durable_queue_registry.as_mut() else {
        return;
    };
    if reg.is_empty() {
        return;
    }
    let mut dropped: smallvec::SmallVec<[Bytes; 4]> = smallvec::SmallVec::new();
    for arg in cmd_args {
        if let Frame::BulkString(key) = arg {
            if reg.get(key).is_some() {
                reg.remove(key);
                dropped.push(key.clone());
            }
        }
    }
    if dropped.is_empty() {
        return;
    }
    emit_mq_drops(s, db_index, &dropped);
}

/// Tombstone every durable MQ stream cleared by `FLUSHDB`/`FLUSHALL`. Both
/// clear the WHOLE registry (see the module-level scope note above) — the
/// only difference from `FLUSHALL` would be db-scoping, which the registry
/// cannot represent.
pub(crate) fn auto_drop_mq_streams_on_flush(
    s: &mut crate::shard::slice::ShardSlice,
    db_index: usize,
) {
    let Some(reg) = s.durable_queue_registry.as_mut() else {
        return;
    };
    if reg.is_empty() {
        return;
    }
    let dropped: smallvec::SmallVec<[Bytes; 4]> = reg.iter().map(|(k, _)| k.clone()).collect();
    for key in &dropped {
        reg.remove(key);
    }
    emit_mq_drops(s, db_index, &dropped);
}

/// Shared tail: append + replicate one `MqDrop` WAL record per already-
/// removed key. `db_index` is the record's db attribution — matches the
/// `MqCreate` convention of tagging the record with the acting command's
/// db, not a per-queue stored value (the registry has none).
fn emit_mq_drops(s: &mut crate::shard::slice::ShardSlice, db_index: usize, dropped: &[Bytes]) {
    let shard_id = s.shard_id;
    for key in dropped {
        let payload = crate::mq::wal::encode_mq_drop(db_index as u32, key);
        replicate_mq_record(shard_id, db_index, crate::mq::wal::MQ_REPL_DROP, &payload);
        if let Some(ref tx) = s.wal_append_tx {
            if tx
                .try_send((
                    crate::persistence::wal_v3::record::WalRecordType::MqDrop,
                    Bytes::from(payload),
                ))
                .is_err()
            {
                crate::command::info_reclamation::RECL_WAL_APPEND_CHANNEL_DROPPED_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    "MqDrop WAL append channel rejected a tombstone record for a deleted \
                     durable MQ stream AFTER the in-memory removal was applied — this \
                     queue may resurrect on crash recovery. The channel is drained by \
                     this same shard thread every 1ms (capacity 4096), so this indicates \
                     a pathological single-tick burst."
                );
            }
        }
    }
}

// ── Subcommand handlers ───────────────────────────────────────────────────────

/// MQ.CREATE — owner creates the durable stream + registry entry.
///
/// Mirrors handler_sharded/write.rs MQ CREATE arm (lock-path `else` branch).
fn handle_create(
    args: &[Frame],
    key_prefix: &Bytes,
    db_index: usize,
    mut gate: MqWriteGate<'_>,
) -> Frame {
    let (raw_key, max_delivery_count, _debounce_ms) = match validate_mq_create(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);

    // Create / configure the durable stream.
    let create_result: Result<(), Frame> = crate::shard::slice::with_shard_db(db_index, |db| {
        // moon#1250: a new queue grows the keyspace — the write gate first
        // (it may evict, so before the pre-image capture).
        run_gate(&mut gate, db, db_index)?;
        // moon#1228: MQ runs outside `command::dispatch`, so an armed BGSAVE
        // epoch gets the queue key's epoch-start state (or absence) here.
        crate::persistence::snapshot_cow::capture_write_pre_image(db, db_index, &eff_key);
        let delta = match db.get_or_create_stream(&eff_key) {
            Ok(stream) => {
                stream.durable = true;
                stream.max_delivery_count = max_delivery_count;
                let group_name = Bytes::from_static(b"__mq_consumers");
                let _ = stream.create_group(group_name, StreamId::ZERO);
                stream.take_unbilled()
            }
            Err(e) => return Err(e),
        };
        bill_stream_delta(db, delta);
        Ok(())
    });
    if let Err(e) = create_result {
        return e;
    }

    // Register in the durable-queue registry (lazy-init on first CREATE).
    let config = crate::mq::DurableStreamConfig::new(eff_key.clone(), max_delivery_count);
    crate::shard::slice::with_shard(|s| {
        let reg = s
            .durable_queue_registry
            .get_or_insert_with(|| Box::new(crate::mq::DurableQueueRegistry::new()));
        reg.insert(eff_key.clone(), config);
    });

    // WAL: MqCreate record on the owner shard. K1a: send the unframed payload
    // tagged with its real type — the event-loop drain does the single framing.
    // Wave B stage 2a: payload now carries `db_index` so replay recreates the
    // stream in the SAME db it was created in (fixes the db-0 hardcode).
    {
        let payload =
            crate::mq::wal::encode_mq_create(db_index as u32, &eff_key, max_delivery_count);
        replicate_mq_record(
            current_shard_id(),
            db_index,
            crate::mq::wal::MQ_REPL_CREATE,
            &payload,
        );
        wal_append_on_slice(
            crate::persistence::wal_v3::record::WalRecordType::MqCreate,
            Bytes::from(payload),
        );
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// MQ.PUSH — owner enqueues a message into the durable stream.
///
/// Mirrors handler_sharded/write.rs MQ PUSH arm (lock-path `else` branch).
/// Fires any registered trigger's debounce timer (sets `pending_fire_ms` if
/// not already pending).
fn handle_push(
    args: &[Frame],
    key_prefix: &Bytes,
    db_index: usize,
    mut gate: MqWriteGate<'_>,
) -> Frame {
    let (raw_key, fields) = match validate_mq_push(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);
    let trig_key = derive_trig_key(key_prefix, &raw_key);

    // Push into the stream. The WAL payload is built INSIDE the closure
    // (before `fields` is moved into `stream.add`) — it captures the
    // ASSIGNED id, not the request, so replay is outcome-deterministic.
    // Encoding here is a pure function call, not a nested `with_shard*`
    // call, so it doesn't violate the non-reentrancy contract.
    type PushResult = Result<Option<(StreamId, Vec<u8>)>, Frame>;
    let push_result: PushResult = crate::shard::slice::with_shard_db(db_index, |db| {
        // moon#1250: see `handle_create`.
        run_gate(&mut gate, db, db_index)?;
        // moon#1228: see `handle_create`.
        crate::persistence::snapshot_cow::capture_write_pre_image(db, db_index, &eff_key);
        let (pushed, delta) = match db.get_stream_mut(&eff_key) {
            Ok(Some(stream)) => {
                if !stream.durable {
                    (None, 0)
                } else {
                    let msg_id = stream.next_auto_id();
                    let payload = crate::mq::wal::encode_mq_push(
                        db_index as u32,
                        &eff_key,
                        msg_id.ms,
                        msg_id.seq,
                        &fields,
                    );
                    // moon#1249: `add` refuses an ID at or below `last_id`.
                    // `next_auto_id` is above it unless the sequence wrapped
                    // at the last possible ID — redis's wording for that.
                    let Some(msg_id) = stream.add(msg_id, fields) else {
                        return Err(Frame::Error(Bytes::from_static(
                            b"ERR The stream has exhausted the last possible ID, unable to add more items",
                        )));
                    };
                    (Some((msg_id, payload)), stream.take_unbilled())
                }
            }
            Ok(None) => (None, 0),
            Err(e) => return Err(e),
        };
        // moon#1250: the pushed entry is charged to `used_memory`.
        bill_stream_delta(db, delta);
        Ok(pushed)
    });

    match push_result {
        Ok(Some((msg_id, payload))) => {
            // WAL: MqPush record on the owner shard (Wave B stage 2a), plus
            // live replication fan-out of the SAME payload (Wave B stage 2b).
            replicate_mq_record(
                current_shard_id(),
                db_index,
                crate::mq::wal::MQ_REPL_PUSH,
                &payload,
            );
            wal_append_on_slice(
                crate::persistence::wal_v3::record::WalRecordType::MqPush,
                Bytes::from(payload),
            );

            // Debounce trigger: set pending_fire_ms if not already armed.
            let now_ms = current_time_ms();
            crate::shard::slice::with_shard(|s| {
                if let Some(ref mut reg) = s.trigger_registry {
                    if let Some(trig_entry) = reg.get_mut(&trig_key) {
                        if trig_entry.pending_fire_ms == 0 {
                            trig_entry.pending_fire_ms = now_ms + trig_entry.debounce_ms;
                        }
                    }
                }
            });

            let mut buf = itoa::Buffer::new();
            let ms_str = buf.format(msg_id.ms);
            let mut buf2 = itoa::Buffer::new();
            let seq_str = buf2.format(msg_id.seq);
            let mut id_bytes = Vec::with_capacity(ms_str.len() + 1 + seq_str.len());
            id_bytes.extend_from_slice(ms_str.as_bytes());
            id_bytes.push(b'-');
            id_bytes.extend_from_slice(seq_str.as_bytes());
            Frame::BulkString(Bytes::from(id_bytes))
        }
        Ok(None) => Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)),
        Err(e) => e,
    }
}

/// MQ.POP — owner claims messages, routing max-delivery entries to the DLQ.
///
/// Should an entry claimed at `delivery_count` be routed to the dead-letter
/// queue instead of returned to the caller? `mdc == 0` disables dead-lettering.
///
/// This keeps the shipped `>=` comparison, which this function exists to NAME
/// rather than quietly change. `delivery_count` counts the delivery in
/// progress, so `MAXDELIVERY 1` dead-letters every FIRST delivery and such a
/// queue never delivers anything — see moon#663.
///
/// That is not fixable here alone. `Stream::read_group_new` reads only `>`
/// entries and writes each PEL entry with a hardcoded `delivery_count: 1`, and
/// MQ exposes no XCLAIM/XAUTOCLAIM/visibility-timeout path, so the value
/// reaching this predicate is ALWAYS exactly 1. Switching to `>` would make the
/// branch unreachable for every `mdc` and turn dead-lettering into dead code;
/// MAXDELIVERY only becomes meaningful once MQ can redeliver. moon#663 tracks
/// both halves together.
#[inline]
fn should_dead_letter(delivery_count: u64, mdc: u32) -> bool {
    mdc > 0 && delivery_count >= mdc as u64
}

/// Mirrors handler_sharded/write.rs MQ POP arm (lock-path `else` branch).
/// Emits an `MqPop` WAL record capturing the full outcome (claimed ids +
/// resulting delivery_count, the group's resulting `last_delivered_id`, and
/// any DLQ routing decisions) so replay can reproduce it deterministically
/// without recomputing "which entries would this claim" against
/// possibly-different post-crash state.
fn handle_pop(args: &[Frame], key_prefix: &Bytes, db_index: usize) -> Frame {
    let (raw_key, count) = match validate_mq_pop(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);
    let group_name = Bytes::from_static(b"__mq_consumers");
    let consumer_name = Bytes::from_static(b"__mq_default");

    // All POP logic runs in a single with_shard_db closure to avoid
    // re-entrancy. Returns `(reply, wal_payload)`; `wal_payload` is `None`
    // on any error/early-exit path (nothing claimed, nothing to record).
    let (reply, wal_payload): (Frame, Option<Vec<u8>>) =
        crate::shard::slice::with_shard_db(db_index, |db| -> (Frame, Option<Vec<u8>>) {
            // moon#1228: the claim below writes the queue's PEL and group
            // cursor (and may release, ack and dead-letter): an armed BGSAVE
            // epoch gets the queue key's epoch-start state first.
            crate::persistence::snapshot_cow::capture_write_pre_image(db, db_index, &eff_key);
            // Step 1: read max_delivery_count.
            let mdc = match db.get_stream_mut(&eff_key) {
                Ok(Some(stream)) => {
                    if !stream.durable {
                        return (Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)), None);
                    }
                    stream.max_delivery_count
                }
                Ok(None) => return (Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)), None),
                Err(e) => return (e, None),
            };

            // Saturating: COUNT may be as large as `usize::MAX` (review 5 —
            // the plain sum panicked a debug build and wrapped a release one
            // to a claim smaller than COUNT).
            let request_count = count.saturating_add(mdc as usize);

            // Step 2: read_group_new to claim entries.
            let stream = match db.get_stream_mut(&eff_key) {
                Ok(Some(s)) => s,
                _ => return (Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)), None),
            };
            // Review 6 (N3): nothing to claim — answer without reading.
            // `read_group_new` would create (and bill) the `__mq_default`
            // consumer first, and an empty POP logs no MqPop, so the replica
            // and a WAL replay never had it (MEMORY USAGE 497 vs 321).
            if stream.group_has_new(&group_name) == Some(false) {
                return (Frame::Array(vec![].into()), None);
            }
            // The group's cursor BEFORE this call, so the release below can
            // rewind to it if this POP ends up keeping nothing.
            let prev_last_delivered = stream
                .groups
                .get(group_name.as_ref())
                .map(|g| g.last_delivered_id)
                .unwrap_or(StreamId::ZERO);
            let claimed = match stream.read_group_new(
                &group_name,
                &consumer_name,
                Some(request_count),
                false,
            ) {
                Ok(entries) => entries,
                Err(_) => return (Frame::Array(vec![].into()), None),
            };
            if claimed.is_empty() {
                // moon#1250: the read may still have created the consumer.
                let delta = stream.take_unbilled();
                bill_stream_delta(db, delta);
                return (Frame::Array(vec![].into()), None);
            }

            // Step 3: partition claimed into good entries and DLQ entries.
            let mut results: Vec<(StreamId, Vec<(Bytes, Bytes)>)> =
                Vec::with_capacity(count.min(claimed.len()));
            let mut dlq_entries: Vec<(StreamId, Vec<(Bytes, Bytes)>)> = Vec::new();
            // Every id this POP KEEPS (delivered or dead-lettered), with its
            // delivery_count at claim time. Entries released below are
            // deliberately absent: replay rebuilds the PEL from this list, so
            // recording a released id would resurrect the stranding on
            // recovery.
            let mut claimed_for_wal: Vec<crate::mq::wal::ClaimedEntry> =
                Vec::with_capacity(claimed.len());
            // task #47: every entry `read_group_new` just claimed shares one
            // `delivery_time` (the call's single `now`) — capture it once
            // from the first claimed PEL entry so a promoted replica / a
            // kill-9 replay reports the SAME idle time XPENDING would have
            // reported on the original node, instead of resetting to 0.
            let mut delivery_time_for_wal: u64 = 0;

            // `read_group_new` over-claims by `mdc` so dead letters do not
            // eat the caller's COUNT. Walk the claim in id order: each entry
            // either dead-letters, fills the caller's budget, or ends the
            // batch. The first entry that can do neither marks `cut` — it and
            // every entry after it are RELEASED below, because MQ has no
            // XCLAIM/XAUTOCLAIM path and `read_group_new` only ever reads `>`,
            // so an entry left claimed-but-undelivered is unreachable forever
            // (moon#652).
            let mut cut = claimed.len();
            for (i, (id, fields)) in claimed.iter().enumerate() {
                let pe = stream
                    .groups
                    .get(group_name.as_ref())
                    .and_then(|g| g.pel.get(id));
                let delivery_count = pe.map(|pe| pe.delivery_count).unwrap_or(1);
                if should_dead_letter(delivery_count, mdc) {
                    dlq_entries.push((*id, fields.clone()));
                } else if results.len() < count {
                    results.push((*id, fields.clone()));
                } else {
                    cut = i;
                    break;
                }
                if delivery_time_for_wal == 0 {
                    delivery_time_for_wal = pe.map(|pe| pe.delivery_time).unwrap_or(0);
                }
                claimed_for_wal.push((id.ms, id.seq, delivery_count));
            }

            // Release the surplus: un-claim it from the PEL and the consumer's
            // pending set, and rewind the group cursor to the last entry this
            // POP actually kept, so the next POP re-reads from there.
            //
            // moon#1250: the un-claim goes through `Stream::xack` (exactly the
            // PEL + consumer-pending removal this did by hand) because xack
            // also credits the stream's unbilled delta. `read_group_new`
            // charged every over-claimed entry; removing them behind the
            // stream's back left those bytes charged to `used_memory` after
            // they were gone — again on every POP, since the next POP
            // re-claims the same entries.
            if cut < claimed.len() {
                let released: smallvec::SmallVec<[StreamId; 8]> =
                    claimed[cut..].iter().map(|(id, _)| *id).collect();
                let _ = stream.xack(&group_name, &released);
                if let Some(group) = stream.groups.get_mut(group_name.as_ref()) {
                    group.last_delivered_id = if cut == 0 {
                        // Defensive, unreachable: COUNT >= 1 (`validate_mq_pop`,
                        // `test_validate_mq_pop_count_zero`) keeps entry 0, so
                        // `cut >= 1`; this arm keeps `cut - 1` from underflowing.
                        prev_last_delivered
                    } else {
                        claimed[cut - 1].0
                    };
                }
            }

            if results.is_empty() && dlq_entries.is_empty() {
                // Defensive, unreachable: entry 0 is always delivered or
                // dead-lettered while COUNT >= 1 (see the `cut == 0` arm).
                // moon#1250: the claim and its release both moved bytes.
                let delta = stream.take_unbilled();
                bill_stream_delta(db, delta);
                return (Frame::Array(vec![].into()), None);
            }

            // Consumer-level `seen_time` for the fixed `__mq_default`
            // consumer this POP claimed under — same "single value for the
            // whole batch" reasoning as `delivery_time_for_wal` above.
            let seen_time_for_wal: u64 = stream
                .groups
                .get(group_name.as_ref())
                .and_then(|g| g.consumers.get(consumer_name.as_ref()))
                .map(|c| c.seen_time)
                .unwrap_or(0);

            let last_delivered_id = stream
                .groups
                .get(group_name.as_ref())
                .map(|g| g.last_delivered_id)
                .unwrap_or(StreamId::ZERO);
            // moon#1250: the claim (PEL, consumer) and release are charged /
            // credited to `used_memory`.
            let delta = stream.take_unbilled();
            bill_stream_delta(db, delta);

            // Step 4: append DLQ entries to the sibling DLQ stream, capturing
            // the ASSIGNED dlq-stream id for each (outcome-deterministic —
            // replay must reproduce the exact id, not regenerate one from
            // its own wall clock).
            let mut dlq_for_wal: Vec<crate::mq::wal::DlqRouting> =
                Vec::with_capacity(dlq_entries.len());
            let mut routed: smallvec::SmallVec<[StreamId; 8]> = smallvec::SmallVec::new();
            if !dlq_entries.is_empty() {
                let dlq_key = {
                    let mut buf = Vec::with_capacity(eff_key.len() + 8);
                    buf.extend_from_slice(&eff_key);
                    buf.extend_from_slice(b"::mq:dlq");
                    Bytes::from(buf)
                };
                // moon#1228: the DLQ stream is a second written key.
                crate::persistence::snapshot_cow::capture_write_pre_image(db, db_index, &dlq_key);
                if let Ok(dlq_stream) = db.get_or_create_stream(&dlq_key) {
                    for (src_id, fields) in dlq_entries {
                        let next = dlq_stream.next_auto_id();
                        // Review 5: `add` refuses an ID at or below the DLQ's
                        // `last_id` (moon#1249: its last possible ID is
                        // taken). Such a dead letter is neither routed nor
                        // logged as routed, and step 5 leaves it pending in
                        // the queue — where the MqPop record (it is claimed,
                        // with no routing) replays it too — instead of acking
                        // away a message the DLQ never received.
                        let Some(dlq_id) = dlq_stream.add(next, fields) else {
                            tracing::warn!(
                                "MQ POP: the dead-letter stream of a queue has exhausted its \
                                 last possible ID; the dead letter stays pending in the queue"
                            );
                            continue;
                        };
                        dlq_for_wal.push((src_id.ms, src_id.seq, dlq_id.ms, dlq_id.seq));
                        routed.push(src_id);
                    }
                    // moon#1250: the dead letters are charged too.
                    let delta = dlq_stream.take_unbilled();
                    bill_stream_delta(db, delta);
                }
            }

            // Step 5: ACK the dead letters the DLQ received from the queue's
            // PEL, credited like any ACK.
            if !routed.is_empty()
                && let Ok(Some(stream)) = db.get_stream_mut(&eff_key)
            {
                let _ = stream.xack(&group_name, &routed);
                let delta = stream.take_unbilled();
                bill_stream_delta(db, delta);
            }

            let wal_payload = crate::mq::wal::encode_mq_pop(
                db_index as u32,
                &eff_key,
                (last_delivered_id.ms, last_delivered_id.seq),
                &claimed_for_wal,
                &dlq_for_wal,
                delivery_time_for_wal,
                seen_time_for_wal,
            );

            // Step 6: build response frames.
            let result_frames: Vec<Frame> = results
                .iter()
                .map(|(id, fields)| {
                    let mut entry_frames = Vec::with_capacity(2);
                    let mut ms_buf = itoa::Buffer::new();
                    let mut seq_buf = itoa::Buffer::new();
                    let ms_str = ms_buf.format(id.ms);
                    let seq_str = seq_buf.format(id.seq);
                    let mut id_bytes = Vec::with_capacity(ms_str.len() + 1 + seq_str.len());
                    id_bytes.extend_from_slice(ms_str.as_bytes());
                    id_bytes.push(b'-');
                    id_bytes.extend_from_slice(seq_str.as_bytes());
                    entry_frames.push(Frame::BulkString(Bytes::from(id_bytes)));
                    let field_frames: Vec<Frame> = fields
                        .iter()
                        .flat_map(|(f, v)| {
                            [Frame::BulkString(f.clone()), Frame::BulkString(v.clone())]
                        })
                        .collect();
                    entry_frames.push(Frame::Array(field_frames.into()));
                    Frame::Array(entry_frames.into())
                })
                .collect();
            (Frame::Array(result_frames.into()), Some(wal_payload))
        });

    if let Some(payload) = wal_payload {
        replicate_mq_record(
            current_shard_id(),
            db_index,
            crate::mq::wal::MQ_REPL_POP,
            &payload,
        );
        wal_append_on_slice(
            crate::persistence::wal_v3::record::WalRecordType::MqPop,
            Bytes::from(payload),
        );
    }
    reply
}

/// MQ.ACK — owner acknowledges one or more message IDs.
///
/// Mirrors handler_sharded/write.rs MQ ACK arm. Emits a WAL record per acked
/// message, same as the lock-path.
fn handle_ack(args: &[Frame], key_prefix: &Bytes, db_index: usize) -> Frame {
    let (raw_key, msg_ids) = match validate_mq_ack(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);
    let ids: Vec<StreamId> = msg_ids
        .iter()
        .map(|(ms, seq)| StreamId { ms: *ms, seq: *seq })
        .collect();

    let ack_result = crate::shard::slice::with_shard_db(db_index, |db| {
        // moon#1228: see `handle_create`.
        crate::persistence::snapshot_cow::capture_write_pre_image(db, db_index, &eff_key);
        let (acked, delta) = match db.get_stream_mut(&eff_key) {
            Ok(Some(stream)) => {
                let group_name = Bytes::from_static(b"__mq_consumers");
                let acked = stream.xack(&group_name, &ids).ok();
                (acked, stream.take_unbilled())
            }
            _ => (None, 0),
        };
        // moon#1250: the PEL entries an ACK removes are credited back.
        bill_stream_delta(db, delta);
        acked
    });

    match ack_result {
        Some(acked_count) => {
            // Emit one WAL record per acked message id (unframed, real type).
            // Replay applies MqAck BY ID via `Stream::xack` — idempotent even
            // if a request id was never actually in the PEL (matches this
            // aggregate-count live path, which can't distinguish per-id
            // success without changing `Stream::xack`'s return type).
            let shard_id = current_shard_id();
            for (ms, seq) in &msg_ids {
                let payload = crate::mq::wal::encode_mq_ack(db_index as u32, &eff_key, *ms, *seq);
                replicate_mq_record(shard_id, db_index, crate::mq::wal::MQ_REPL_ACK, &payload);
                wal_append_on_slice(
                    crate::persistence::wal_v3::record::WalRecordType::MqAck,
                    Bytes::from(payload),
                );
            }
            Frame::Integer(acked_count as i64)
        }
        None => Frame::Integer(0),
    }
}

/// MQ.DLQLEN — owner returns the depth of the dead-letter queue stream.
///
/// Mirrors handler_sharded/write.rs MQ DLQLEN arm.
fn handle_dlqlen(args: &[Frame], key_prefix: &Bytes, db_index: usize) -> Frame {
    let raw_key = match validate_mq_dlqlen(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);
    let dlq_key = {
        let mut buf = Vec::with_capacity(eff_key.len() + 8);
        buf.extend_from_slice(&eff_key);
        buf.extend_from_slice(b"::mq:dlq");
        Bytes::from(buf)
    };

    let len =
        crate::shard::slice::with_shard_db(db_index, |db| match db.get_stream_mut(&dlq_key) {
            Ok(Some(stream)) => stream.length as i64,
            _ => 0i64,
        });
    Frame::Integer(len)
}

/// MQ.TRIGGER — owner registers a debounced trigger in the trigger registry.
///
/// Mirrors handler_sharded/write.rs MQ TRIGGER arm.
fn handle_trigger(args: &[Frame], key_prefix: &Bytes, db_index: usize) -> Frame {
    let (raw_key, callback_cmd, debounce_ms) = match validate_mq_trigger(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);
    let trig_key = derive_trig_key(key_prefix, &raw_key);

    // WAL: MqTrigger record (Wave B stage 2a) — registration is durable/
    // replayed as opaque data, never fired during replay. Built before the
    // registry insert consumes `eff_key`/`callback_cmd`/`trig_key`.
    let payload =
        crate::mq::wal::encode_mq_trigger(&trig_key, &eff_key, &callback_cmd, debounce_ms);

    let entry = crate::mq::TriggerEntry {
        queue_key: eff_key,
        callback_cmd,
        debounce_ms,
        last_fire_ms: 0,
        pending_fire_ms: 0,
    };

    crate::shard::slice::with_shard(|s| {
        let reg = s
            .trigger_registry
            .get_or_insert_with(|| Box::new(crate::mq::TriggerRegistry::new()));
        reg.register(trig_key, entry);
    });

    // `db_index` is unused by the TRIGGER *mutation* (registry is
    // shard-level, not per-db) but is still passed through to
    // `replicate_mq_record` for the SELECT-context invariant
    // `record_local_write_db` documents: even a db-agnostic record must keep
    // the replication stream's db context aligned with the writing
    // connection's selected db, so the NEXT db-scoped record on this shard
    // doesn't inherit a stale `SELECT`.
    replicate_mq_record(
        current_shard_id(),
        db_index,
        crate::mq::wal::MQ_REPL_TRIGGER,
        &payload,
    );
    wal_append_on_slice(
        crate::persistence::wal_v3::record::WalRecordType::MqTrigger,
        Bytes::from(payload),
    );

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests;
