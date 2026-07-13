//! WAL encode/decode for MQ lifecycle + effect operations.
//!
//! Wave B stage 2a (task #34) gives MQ its own durability sub-plane: every
//! owner-shard mutation (`MQ.CREATE`/`PUSH`/`POP`/`ACK`/`TRIGGER`) emits a
//! versioned effect record through the typed `wal_append` channel. Replay
//! (`crate::shard::shared_databases::replay_mq_wal`) applies these records
//! IN ORDER after the AOF-authority wipe (`main.rs`'s `db.clear()`), which is
//! what lets a durable queue's content, consumer-group PEL, DLQ routing, and
//! trigger registrations all survive a kill-9 even when `--appendonly yes`
//! would otherwise discard the whole keyspace.
//!
//! Kernel M3 stage 3 (task #46) adds `MqDrop` (0x75): a tombstone emitted
//! when a durable stream is removed via generic keyspace `DEL`/`UNLINK`/
//! `FLUSHDB`/`FLUSHALL` (as opposed to any `MQ.*` command), closing the gap
//! where such a delete had no WAL representation and replay resurrected the
//! full pre-delete content after a kill-9.
//!
//! All payloads share a leading `version: u8` byte. Current version is `1`
//! for every record kind below. Decoders return `None` for ANY structurally
//! invalid payload (including an unrecognized/future version byte) -- NEVER
//! panic or unwrap -- so a single corrupt or newer-than-supported record
//! degrades to "skip and warn" at the replay call site instead of aborting
//! the whole WAL scan.
//!
//! `MqCreate` (0x70) and `MqAck` (0x71) keep their pre-existing WAL
//! discriminants but their payload layout is bumped to this versioned,
//! db-index-carrying form (fixing the "replay always assumes db 0" bug).
//! This is a deliberate breaking change to those two payloads' bytes,
//! precedented by the 0x51->0x53 XactCommit format freeze documented in
//! `WalRecordType`: WAL v3 segments are short-lived/rotated and carry no
//! cross-version compatibility contract. A pre-K2 unversioned payload simply
//! fails to decode under the new layout and is skipped like any other
//! malformed record.

use bytes::Bytes;

/// Only supported payload version for every MQ WAL record kind.
pub const MQ_WAL_VERSION: u8 = 1;

/// Peek the leading version byte of an MQ WAL payload without fully
/// decoding it. Used by replay call sites to distinguish "malformed" from
/// "well-formed but a version newer than this build understands" for
/// logging purposes (`decode_*` returns `None` for both cases; skip-and-warn
/// applies either way, but the log message differs).
#[inline]
pub fn peek_version(payload: &[u8]) -> Option<u8> {
    payload.first().copied()
}

// ── MqCreate (0x70) ───────────────────────────────────────────────────────

/// Encode an MqCreate WAL payload.
///
/// Layout: `[version:u8=1][db_index:u32 LE][key_len:u32 LE][key:N][max_delivery_count:u32 LE]`
pub fn encode_mq_create(db_index: u32, queue_key: &[u8], max_delivery_count: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + 4 + queue_key.len() + 4);
    payload.push(MQ_WAL_VERSION);
    payload.extend_from_slice(&db_index.to_le_bytes());
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload.extend_from_slice(&max_delivery_count.to_le_bytes());
    payload
}

/// Decode an MqCreate WAL payload.
///
/// Returns `(db_index, queue_key, max_delivery_count)` or `None` if
/// malformed or an unsupported version.
pub fn decode_mq_create(payload: &[u8]) -> Option<(u32, Vec<u8>, u32)> {
    if payload.is_empty() || payload[0] != MQ_WAL_VERSION {
        return None;
    }
    let p = &payload[1..];
    if p.len() < 8 {
        return None;
    }
    let db_index = u32::from_le_bytes(p[0..4].try_into().ok()?);
    let key_len = u32::from_le_bytes(p[4..8].try_into().ok()?) as usize;
    if p.len() < 8 + key_len + 4 {
        return None;
    }
    let key = p[8..8 + key_len].to_vec();
    let mdc_offset = 8 + key_len;
    let max_delivery_count = u32::from_le_bytes(p[mdc_offset..mdc_offset + 4].try_into().ok()?);
    Some((db_index, key, max_delivery_count))
}

// ── MqAck (0x71) ──────────────────────────────────────────────────────────

/// Encode an MqAck WAL payload.
///
/// Layout: `[version:u8=1][db_index:u32 LE][key_len:u32 LE][key:N][msg_id_ms:u64 LE][msg_id_seq:u64 LE]`
pub fn encode_mq_ack(db_index: u32, queue_key: &[u8], msg_id_ms: u64, msg_id_seq: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + 4 + queue_key.len() + 16);
    payload.push(MQ_WAL_VERSION);
    payload.extend_from_slice(&db_index.to_le_bytes());
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload.extend_from_slice(&msg_id_ms.to_le_bytes());
    payload.extend_from_slice(&msg_id_seq.to_le_bytes());
    payload
}

/// Decode an MqAck WAL payload.
///
/// Returns `(db_index, queue_key, ms, seq)` or `None` if malformed or an
/// unsupported version.
pub fn decode_mq_ack(payload: &[u8]) -> Option<(u32, Vec<u8>, u64, u64)> {
    if payload.is_empty() || payload[0] != MQ_WAL_VERSION {
        return None;
    }
    let p = &payload[1..];
    if p.len() < 8 {
        return None;
    }
    let db_index = u32::from_le_bytes(p[0..4].try_into().ok()?);
    let key_len = u32::from_le_bytes(p[4..8].try_into().ok()?) as usize;
    if p.len() < 8 + key_len + 16 {
        return None;
    }
    let key = p[8..8 + key_len].to_vec();
    let ms_offset = 8 + key_len;
    let ms = u64::from_le_bytes(p[ms_offset..ms_offset + 8].try_into().ok()?);
    let seq_offset = ms_offset + 8;
    let seq = u64::from_le_bytes(p[seq_offset..seq_offset + 8].try_into().ok()?);
    Some((db_index, key, ms, seq))
}

// ── MqPush (0x72) ─────────────────────────────────────────────────────────

/// Encode an MqPush WAL payload. Captures the ASSIGNED message id (not the
/// request), so replay is outcome-deterministic regardless of wall-clock
/// skew between the original write and the replay pass.
///
/// Layout:
/// `[version:u8=1][db_index:u32][key_len:u32][key:N][id_ms:u64][id_seq:u64]`
/// `[field_count:u32]` then `field_count` times `[flen:u32][f:N][vlen:u32][v:N]`
pub fn encode_mq_push(
    db_index: u32,
    queue_key: &[u8],
    id_ms: u64,
    id_seq: u64,
    fields: &[(Bytes, Bytes)],
) -> Vec<u8> {
    let fields_size: usize = fields.iter().map(|(f, v)| 8 + f.len() + v.len()).sum();
    let mut payload = Vec::with_capacity(1 + 4 + 4 + queue_key.len() + 16 + 4 + fields_size);
    payload.push(MQ_WAL_VERSION);
    payload.extend_from_slice(&db_index.to_le_bytes());
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload.extend_from_slice(&id_ms.to_le_bytes());
    payload.extend_from_slice(&id_seq.to_le_bytes());
    payload.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    for (f, v) in fields {
        payload.extend_from_slice(&(f.len() as u32).to_le_bytes());
        payload.extend_from_slice(f);
        payload.extend_from_slice(&(v.len() as u32).to_le_bytes());
        payload.extend_from_slice(v);
    }
    payload
}

/// Decode an MqPush WAL payload.
///
/// Returns `(db_index, queue_key, id_ms, id_seq, fields)` or `None` if
/// malformed or an unsupported version.
#[allow(clippy::type_complexity)]
pub fn decode_mq_push(payload: &[u8]) -> Option<(u32, Vec<u8>, u64, u64, Vec<(Bytes, Bytes)>)> {
    if payload.is_empty() || payload[0] != MQ_WAL_VERSION {
        return None;
    }
    let p = &payload[1..];
    if p.len() < 8 {
        return None;
    }
    let db_index = u32::from_le_bytes(p[0..4].try_into().ok()?);
    let key_len = u32::from_le_bytes(p[4..8].try_into().ok()?) as usize;
    let mut off = 8usize;
    if p.len() < off + key_len {
        return None;
    }
    let key = p[off..off + key_len].to_vec();
    off += key_len;
    if p.len() < off + 20 {
        return None;
    }
    let id_ms = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
    off += 8;
    let id_seq = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
    off += 8;
    let field_count = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
    off += 4;

    let mut fields = Vec::with_capacity(field_count.min(4096));
    for _ in 0..field_count {
        if p.len() < off + 4 {
            return None;
        }
        let flen = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
        off += 4;
        if p.len() < off + flen + 4 {
            return None;
        }
        let f = Bytes::copy_from_slice(&p[off..off + flen]);
        off += flen;
        let vlen = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
        off += 4;
        if p.len() < off + vlen {
            return None;
        }
        let v = Bytes::copy_from_slice(&p[off..off + vlen]);
        off += vlen;
        fields.push((f, v));
    }

    Some((db_index, key, id_ms, id_seq, fields))
}

// ── MqPop (0x73) ──────────────────────────────────────────────────────────

/// One claimed entry: id + resulting delivery_count.
pub type ClaimedEntry = (u64, u64, u64);
/// One DLQ routing decision: (source_ms, source_seq, dlq_ms, dlq_seq).
pub type DlqRouting = (u64, u64, u64, u64);

/// `MqPop` diverges from the shared `MQ_WAL_VERSION` scheme: it carries its
/// OWN version byte because task #47 needed to add fields to this one
/// record kind without bumping every other MQ record's format in lockstep.
/// v1 is the pre-task-#47 layout (no PEL timing metadata); v2 (current)
/// adds `delivery_time_ms`/`seen_time_ms` so a promoted replica or a
/// kill-9 restart can reconstruct real `XPENDING` idle times instead of
/// resetting the PEL entry's clock to 0 (which reported "maximally idle"
/// until the entry was next touched).
pub const MQ_POP_WAL_V1: u8 = 1;
/// Current `MqPop` payload version. See [`MQ_POP_WAL_V1`].
pub const MQ_POP_WAL_V2: u8 = 2;

/// Encode an MqPop WAL payload. Captures the full outcome of a POP: which
/// ids were claimed (and at what delivery_count), the consumer group's
/// resulting `last_delivered_id`, which claimed ids were immediately
/// routed to the DLQ (source id -> assigned DLQ-stream id), and (v2+) the
/// PEL timing metadata (`delivery_time_ms`, `seen_time_ms`) captured at
/// claim time. The fixed group/consumer names (`__mq_consumers` /
/// `__mq_default`) used by every MQ.POP are NOT carried -- baked in at
/// replay, matching the single-group-per-queue design
/// `src/shard/mq_exec.rs` implements today. Every entry claimed by one
/// MQ.POP shares a single `delivery_time_ms`/`seen_time_ms` (the call's
/// `now`, per `Stream::read_group_new`), so one pair of `u64`s covers the
/// whole batch losslessly -- no need for a per-entry timestamp.
///
/// Layout (v2):
/// `[version:u8=2][db_index:u32][key_len:u32][key:N]`
/// `[last_delivered_ms:u64][last_delivered_seq:u64]`
/// `[claimed_count:u32]` then per entry `[ms:u64][seq:u64][delivery_count:u64]`
/// `[dlq_count:u32]` then per entry `[src_ms:u64][src_seq:u64][dlq_ms:u64][dlq_seq:u64]`
/// `[delivery_time_ms:u64][seen_time_ms:u64]`
pub fn encode_mq_pop(
    db_index: u32,
    queue_key: &[u8],
    last_delivered: (u64, u64),
    claimed: &[ClaimedEntry],
    dlq: &[DlqRouting],
    delivery_time_ms: u64,
    seen_time_ms: u64,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        1 + 4 + 4 + queue_key.len() + 16 + 4 + claimed.len() * 24 + 4 + dlq.len() * 32 + 16,
    );
    payload.push(MQ_POP_WAL_V2);
    payload.extend_from_slice(&db_index.to_le_bytes());
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload.extend_from_slice(&last_delivered.0.to_le_bytes());
    payload.extend_from_slice(&last_delivered.1.to_le_bytes());
    payload.extend_from_slice(&(claimed.len() as u32).to_le_bytes());
    for (ms, seq, dc) in claimed {
        payload.extend_from_slice(&ms.to_le_bytes());
        payload.extend_from_slice(&seq.to_le_bytes());
        payload.extend_from_slice(&dc.to_le_bytes());
    }
    payload.extend_from_slice(&(dlq.len() as u32).to_le_bytes());
    for (src_ms, src_seq, dlq_ms, dlq_seq) in dlq {
        payload.extend_from_slice(&src_ms.to_le_bytes());
        payload.extend_from_slice(&src_seq.to_le_bytes());
        payload.extend_from_slice(&dlq_ms.to_le_bytes());
        payload.extend_from_slice(&dlq_seq.to_le_bytes());
    }
    payload.extend_from_slice(&delivery_time_ms.to_le_bytes());
    payload.extend_from_slice(&seen_time_ms.to_le_bytes());
    payload
}

/// Decode an MqPop WAL payload.
///
/// Returns `(db_index, queue_key, last_delivered, claimed, dlq,
/// delivery_time_ms, seen_time_ms)` or `None` if malformed or an
/// unsupported version.
///
/// Version handling (own scheme, see [`MQ_POP_WAL_V2`]):
/// - v1: legacy layout with no trailing timing fields. Decodes fine;
///   `delivery_time_ms`/`seen_time_ms` are returned as sentinel `0`,
///   EXPLICITLY meaning "not recorded -- the apply-time caller must
///   substitute its own current-time reading" (this is a STRICT
///   IMPROVEMENT over the pre-task-#47 apply behavior, which hardcoded the
///   PEL entry's clock to 0 forever; callers now resolve the sentinel to
///   "now" instead).
/// - v2: full layout: real captured timestamps round-trip exactly.
/// - anything else: rejected (`None`), same fail-closed posture as every
///   other MQ WAL decoder in this module.
#[allow(clippy::type_complexity)]
pub fn decode_mq_pop(
    payload: &[u8],
) -> Option<(
    u32,
    Vec<u8>,
    (u64, u64),
    Vec<ClaimedEntry>,
    Vec<DlqRouting>,
    u64,
    u64,
)> {
    let version = *payload.first()?;
    if version != MQ_POP_WAL_V1 && version != MQ_POP_WAL_V2 {
        return None;
    }
    let p = &payload[1..];
    if p.len() < 8 {
        return None;
    }
    let db_index = u32::from_le_bytes(p[0..4].try_into().ok()?);
    let key_len = u32::from_le_bytes(p[4..8].try_into().ok()?) as usize;
    let mut off = 8usize;
    if p.len() < off + key_len {
        return None;
    }
    let key = p[off..off + key_len].to_vec();
    off += key_len;
    if p.len() < off + 16 + 4 {
        return None;
    }
    let last_ms = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
    off += 8;
    let last_seq = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
    off += 8;

    let claimed_count = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
    off += 4;
    let mut claimed = Vec::with_capacity(claimed_count.min(65536));
    for _ in 0..claimed_count {
        if p.len() < off + 24 {
            return None;
        }
        let ms = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
        let seq = u64::from_le_bytes(p[off + 8..off + 16].try_into().ok()?);
        let dc = u64::from_le_bytes(p[off + 16..off + 24].try_into().ok()?);
        off += 24;
        claimed.push((ms, seq, dc));
    }

    if p.len() < off + 4 {
        return None;
    }
    let dlq_count = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
    off += 4;
    let mut dlq = Vec::with_capacity(dlq_count.min(65536));
    for _ in 0..dlq_count {
        if p.len() < off + 32 {
            return None;
        }
        let src_ms = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
        let src_seq = u64::from_le_bytes(p[off + 8..off + 16].try_into().ok()?);
        let dlq_ms = u64::from_le_bytes(p[off + 16..off + 24].try_into().ok()?);
        let dlq_seq = u64::from_le_bytes(p[off + 24..off + 32].try_into().ok()?);
        off += 32;
        dlq.push((src_ms, src_seq, dlq_ms, dlq_seq));
    }

    let (delivery_time_ms, seen_time_ms) = if version >= MQ_POP_WAL_V2 {
        if p.len() < off + 16 {
            return None;
        }
        let delivery_time_ms = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
        off += 8;
        let seen_time_ms = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);
        (delivery_time_ms, seen_time_ms)
    } else {
        (0u64, 0u64)
    };

    Some((
        db_index,
        key,
        (last_ms, last_seq),
        claimed,
        dlq,
        delivery_time_ms,
        seen_time_ms,
    ))
}

// ── MqTrigger (0x74) ──────────────────────────────────────────────────────

/// Encode an MqTrigger WAL payload. The trigger registry is shard-level
/// (not per-db), so unlike the other MQ records this carries no db_index --
/// mirrors `src/shard/mq_exec.rs::handle_trigger`, which explicitly ignores
/// `db_index` for the same reason. `last_fire_ms`/`pending_fire_ms` are
/// transient runtime state (not persisted); replay always restores a fresh
/// entry with both at 0, matching a never-yet-armed registration.
///
/// Layout:
/// `[version:u8=1][trig_key_len:u32][trig_key:N][queue_key_len:u32][queue_key:N]`
/// `[callback_len:u32][callback:N][debounce_ms:u64]`
pub fn encode_mq_trigger(
    trig_key: &[u8],
    queue_key: &[u8],
    callback_cmd: &[u8],
    debounce_ms: u64,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        1 + 4 + trig_key.len() + 4 + queue_key.len() + 4 + callback_cmd.len() + 8,
    );
    payload.push(MQ_WAL_VERSION);
    payload.extend_from_slice(&(trig_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(trig_key);
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload.extend_from_slice(&(callback_cmd.len() as u32).to_le_bytes());
    payload.extend_from_slice(callback_cmd);
    payload.extend_from_slice(&debounce_ms.to_le_bytes());
    payload
}

/// Decode an MqTrigger WAL payload.
///
/// Returns `(trig_key, queue_key, callback_cmd, debounce_ms)` or `None` if
/// malformed or an unsupported version.
#[allow(clippy::type_complexity)]
pub fn decode_mq_trigger(payload: &[u8]) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>, u64)> {
    if payload.is_empty() || payload[0] != MQ_WAL_VERSION {
        return None;
    }
    let p = &payload[1..];
    if p.len() < 4 {
        return None;
    }
    let mut off = 0usize;
    let trig_key_len = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
    off += 4;
    if p.len() < off + trig_key_len + 4 {
        return None;
    }
    let trig_key = p[off..off + trig_key_len].to_vec();
    off += trig_key_len;
    let queue_key_len = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
    off += 4;
    if p.len() < off + queue_key_len + 4 {
        return None;
    }
    let queue_key = p[off..off + queue_key_len].to_vec();
    off += queue_key_len;
    let callback_len = u32::from_le_bytes(p[off..off + 4].try_into().ok()?) as usize;
    off += 4;
    if p.len() < off + callback_len + 8 {
        return None;
    }
    let callback_cmd = p[off..off + callback_len].to_vec();
    off += callback_len;
    let debounce_ms = u64::from_le_bytes(p[off..off + 8].try_into().ok()?);

    Some((trig_key, queue_key, callback_cmd, debounce_ms))
}

// ── MqDrop (0x75) ─────────────────────────────────────────────────────────

/// Encode an MqDrop WAL payload — a tombstone emitted when a durable MQ
/// stream is removed via generic keyspace `DEL`/`UNLINK`/`FLUSHDB`/
/// `FLUSHALL` (kernel M3 stage 3 / task #46). Replay applies these strictly
/// in WAL order, so a `Drop` only kills the `MqCreate`/`MqPush`/... records
/// that precede it for this `(db_index, key)` pair — a later `MqCreate` for
/// the same key re-materializes normally (create -> drop -> create survives
/// intact).
///
/// Layout: `[version:u8=1][db_index:u32 LE][key_len:u32 LE][key:N]`
pub fn encode_mq_drop(db_index: u32, queue_key: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + 4 + 4 + queue_key.len());
    payload.push(MQ_WAL_VERSION);
    payload.extend_from_slice(&db_index.to_le_bytes());
    payload.extend_from_slice(&(queue_key.len() as u32).to_le_bytes());
    payload.extend_from_slice(queue_key);
    payload
}

/// Decode an MqDrop WAL payload.
///
/// Returns `(db_index, queue_key)` or `None` if malformed or an unsupported
/// version.
pub fn decode_mq_drop(payload: &[u8]) -> Option<(u32, Vec<u8>)> {
    if payload.is_empty() || payload[0] != MQ_WAL_VERSION {
        return None;
    }
    let p = &payload[1..];
    if p.len() < 8 {
        return None;
    }
    let db_index = u32::from_le_bytes(p[0..4].try_into().ok()?);
    let key_len = u32::from_le_bytes(p[4..8].try_into().ok()?) as usize;
    if p.len() < 8 + key_len {
        return None;
    }
    let key = p[8..8 + key_len].to_vec();
    Some((db_index, key))
}

// ── Replication wire framing (Wave B stage 2b) ────────────────────────────
//
// MQ effect records are versioned BINARY payloads, not RESP commands (unlike
// GRAPH.*/TEMPORAL.INVALIDATE-AT, which stream deterministic user-syntax
// commands). Replication wraps each one in a synthetic pseudo-command that
// carries the SAME `encode_mq_*` payload verbatim as a single bulk-string
// arg: `MQ._REPL.PUSH <payload bytes>`. These names are NEVER dispatched to
// a real client (no `CommandMeta`/ACL entry, no overlap with
// `is_mq_command`'s `"MQ"` check) — they are matched only by
// `replication::apply::apply_local`'s command-name routing, exactly like
// `GraphReplayCollector::is_graph_command`, and decoded with the SAME
// `decode_mq_*` functions above so the replica's apply arm and boot-time WAL
// replay share one codec.

/// Synthetic replication pseudo-command for [`WalRecordType::MqCreate`].
pub const MQ_REPL_CREATE: &[u8] = b"MQ._REPL.CREATE";
/// Synthetic replication pseudo-command for [`WalRecordType::MqPush`].
pub const MQ_REPL_PUSH: &[u8] = b"MQ._REPL.PUSH";
/// Synthetic replication pseudo-command for [`WalRecordType::MqPop`].
pub const MQ_REPL_POP: &[u8] = b"MQ._REPL.POP";
/// Synthetic replication pseudo-command for [`WalRecordType::MqAck`].
pub const MQ_REPL_ACK: &[u8] = b"MQ._REPL.ACK";
/// Synthetic replication pseudo-command for [`WalRecordType::MqTrigger`].
pub const MQ_REPL_TRIGGER: &[u8] = b"MQ._REPL.TRIGGER";
/// Synthetic replication pseudo-command for [`WalRecordType::MqDrop`].
pub const MQ_REPL_DROP: &[u8] = b"MQ._REPL.DROP";

/// True for any `MQ._REPL.*` synthetic replication pseudo-command emitted by
/// [`crate::shard::mq_exec`]'s live fan-out. Used by
/// `replication::apply::apply_local` to route a replicated command to the
/// MQ apply engine instead of generic dispatch.
///
/// [`WalRecordType`]: crate::persistence::wal_v3::record::WalRecordType
#[inline]
pub fn is_mq_replay_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(MQ_REPL_CREATE)
        || cmd.eq_ignore_ascii_case(MQ_REPL_PUSH)
        || cmd.eq_ignore_ascii_case(MQ_REPL_POP)
        || cmd.eq_ignore_ascii_case(MQ_REPL_ACK)
        || cmd.eq_ignore_ascii_case(MQ_REPL_TRIGGER)
        || cmd.eq_ignore_ascii_case(MQ_REPL_DROP)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_mq_replay_command() {
        assert!(is_mq_replay_command(b"MQ._REPL.CREATE"));
        assert!(is_mq_replay_command(b"mq._repl.push"));
        assert!(is_mq_replay_command(b"MQ._REPL.POP"));
        assert!(is_mq_replay_command(b"MQ._REPL.ACK"));
        assert!(is_mq_replay_command(b"MQ._REPL.TRIGGER"));
        assert!(is_mq_replay_command(b"MQ._REPL.DROP"));
        assert!(is_mq_replay_command(b"mq._repl.drop"));
        assert!(!is_mq_replay_command(b"MQ"));
        assert!(!is_mq_replay_command(b"MQ.PUSH"));
        assert!(!is_mq_replay_command(b"GRAPH.ADDNODE"));
        assert!(!is_mq_replay_command(b""));
    }

    // --- MqCreate roundtrip ---

    #[test]
    fn test_mq_create_roundtrip() {
        let payload = encode_mq_create(2, b"orders", 5);
        let (db, key, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(db, 2);
        assert_eq!(key, b"orders");
        assert_eq!(mdc, 5);
    }

    #[test]
    fn test_mq_create_roundtrip_zero_delivery() {
        let payload = encode_mq_create(0, b"q", 0);
        let (db, key, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(db, 0);
        assert_eq!(key, b"q");
        assert_eq!(mdc, 0);
    }

    #[test]
    fn test_mq_create_roundtrip_max_delivery() {
        let payload = encode_mq_create(15, b"q", u32::MAX);
        let (db, _, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(db, 15);
        assert_eq!(mdc, u32::MAX);
    }

    #[test]
    fn test_mq_create_roundtrip_empty_key() {
        let payload = encode_mq_create(1, b"", 3);
        let (_, key, mdc) = decode_mq_create(&payload).unwrap();
        assert!(key.is_empty());
        assert_eq!(mdc, 3);
    }

    #[test]
    fn test_mq_create_roundtrip_long_key() {
        let long_key = vec![b'x'; 1024];
        let payload = encode_mq_create(3, &long_key, 10);
        let (db, key, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(db, 3);
        assert_eq!(key, long_key);
        assert_eq!(mdc, 10);
    }

    #[test]
    fn test_mq_create_malformed_empty() {
        assert!(decode_mq_create(b"").is_none());
    }

    #[test]
    fn test_mq_create_malformed_too_short() {
        assert!(decode_mq_create(&[1, 0, 0]).is_none());
    }

    #[test]
    fn test_mq_create_malformed_truncated_key() {
        let mut bad = vec![MQ_WAL_VERSION];
        bad.extend_from_slice(&0u32.to_le_bytes()); // db_index
        bad.extend_from_slice(&100u32.to_le_bytes()); // key_len = 100
        bad.extend_from_slice(&[0u8; 10]); // only 10 bytes of key
        assert!(decode_mq_create(&bad).is_none());
    }

    #[test]
    fn test_mq_create_unknown_version_rejected() {
        let mut payload = encode_mq_create(0, b"q", 5);
        payload[0] = 99; // future version
        assert!(decode_mq_create(&payload).is_none());
        assert_eq!(peek_version(&payload), Some(99));
    }

    #[test]
    fn test_mq_create_extra_bytes_ignored() {
        let mut payload = encode_mq_create(1, b"q", 5);
        payload.extend_from_slice(b"trailing");
        let (db, key, mdc) = decode_mq_create(&payload).unwrap();
        assert_eq!(db, 1);
        assert_eq!(key, b"q");
        assert_eq!(mdc, 5);
    }

    // --- MqAck roundtrip ---

    #[test]
    fn test_mq_ack_roundtrip() {
        let payload = encode_mq_ack(4, b"orders", 1_713_394_800_000u64, 42);
        let (db, key, ms, seq) = decode_mq_ack(&payload).unwrap();
        assert_eq!(db, 4);
        assert_eq!(key, b"orders");
        assert_eq!(ms, 1_713_394_800_000u64);
        assert_eq!(seq, 42);
    }

    #[test]
    fn test_mq_ack_roundtrip_zero_id() {
        let payload = encode_mq_ack(0, b"q", 0, 0);
        let (db, key, ms, seq) = decode_mq_ack(&payload).unwrap();
        assert_eq!(db, 0);
        assert_eq!(key, b"q");
        assert_eq!(ms, 0);
        assert_eq!(seq, 0);
    }

    #[test]
    fn test_mq_ack_roundtrip_max_id() {
        let payload = encode_mq_ack(0, b"q", u64::MAX, u64::MAX);
        let (_, _, ms, seq) = decode_mq_ack(&payload).unwrap();
        assert_eq!(ms, u64::MAX);
        assert_eq!(seq, u64::MAX);
    }

    #[test]
    fn test_mq_ack_malformed_empty() {
        assert!(decode_mq_ack(b"").is_none());
    }

    #[test]
    fn test_mq_ack_malformed_missing_seq() {
        let mut bad = vec![MQ_WAL_VERSION];
        bad.extend_from_slice(&0u32.to_le_bytes());
        bad.extend_from_slice(&2u32.to_le_bytes());
        bad.extend_from_slice(b"ok");
        bad.extend_from_slice(&1000u64.to_le_bytes());
        // Missing seq bytes
        assert!(decode_mq_ack(&bad).is_none());
    }

    #[test]
    fn test_mq_ack_unknown_version_rejected() {
        let mut payload = encode_mq_ack(0, b"q", 100, 200);
        payload[0] = 2;
        assert!(decode_mq_ack(&payload).is_none());
    }

    // --- MqPush roundtrip ---

    #[test]
    fn test_mq_push_roundtrip() {
        let fields = vec![
            (Bytes::from_static(b"seq"), Bytes::from_static(b"1")),
            (Bytes::from_static(b"name"), Bytes::from_static(b"alice")),
        ];
        let payload = encode_mq_push(3, b"orders", 1000, 5, &fields);
        let (db, key, ms, seq, decoded_fields) = decode_mq_push(&payload).unwrap();
        assert_eq!(db, 3);
        assert_eq!(key, b"orders");
        assert_eq!(ms, 1000);
        assert_eq!(seq, 5);
        assert_eq!(decoded_fields, fields);
    }

    #[test]
    fn test_mq_push_roundtrip_no_fields() {
        let payload = encode_mq_push(0, b"q", 1, 0, &[]);
        let (_, _, _, _, fields) = decode_mq_push(&payload).unwrap();
        assert!(fields.is_empty());
    }

    #[test]
    fn test_mq_push_roundtrip_empty_field_value() {
        let fields = vec![(Bytes::from_static(b""), Bytes::from_static(b""))];
        let payload = encode_mq_push(0, b"q", 1, 0, &fields);
        let (_, _, _, _, decoded) = decode_mq_push(&payload).unwrap();
        assert_eq!(decoded, fields);
    }

    #[test]
    fn test_mq_push_malformed_truncated_fields() {
        let mut bad = vec![MQ_WAL_VERSION];
        bad.extend_from_slice(&0u32.to_le_bytes()); // db
        bad.extend_from_slice(&1u32.to_le_bytes()); // key_len
        bad.push(b'q');
        bad.extend_from_slice(&1u64.to_le_bytes()); // id_ms
        bad.extend_from_slice(&0u64.to_le_bytes()); // id_seq
        bad.extend_from_slice(&1u32.to_le_bytes()); // field_count = 1
        bad.extend_from_slice(&100u32.to_le_bytes()); // flen = 100 but nothing follows
        assert!(decode_mq_push(&bad).is_none());
    }

    #[test]
    fn test_mq_push_unknown_version_rejected() {
        let mut payload = encode_mq_push(0, b"q", 1, 0, &[]);
        payload[0] = 7;
        assert!(decode_mq_push(&payload).is_none());
    }

    // --- MqPop roundtrip ---

    #[test]
    fn test_mq_pop_roundtrip() {
        let claimed: Vec<ClaimedEntry> = vec![(1, 0, 1), (1, 1, 1), (1, 2, 2)];
        let dlq: Vec<DlqRouting> = vec![(1, 2, 2, 0)];
        let payload = encode_mq_pop(1, b"orders", (1, 2), &claimed, &dlq, 12345, 12300);
        let (db, key, last, decoded_claimed, decoded_dlq, delivery_time_ms, seen_time_ms) =
            decode_mq_pop(&payload).unwrap();
        assert_eq!(db, 1);
        assert_eq!(key, b"orders");
        assert_eq!(last, (1, 2));
        assert_eq!(decoded_claimed, claimed);
        assert_eq!(decoded_dlq, dlq);
        assert_eq!(delivery_time_ms, 12345);
        assert_eq!(seen_time_ms, 12300);
    }

    #[test]
    fn test_mq_pop_roundtrip_empty() {
        let payload = encode_mq_pop(0, b"q", (0, 0), &[], &[], 0, 0);
        let (_, _, last, claimed, dlq, delivery_time_ms, seen_time_ms) =
            decode_mq_pop(&payload).unwrap();
        assert_eq!(last, (0, 0));
        assert!(claimed.is_empty());
        assert!(dlq.is_empty());
        assert_eq!(delivery_time_ms, 0);
        assert_eq!(seen_time_ms, 0);
    }

    #[test]
    fn test_mq_pop_malformed_truncated_claimed() {
        let mut bad = vec![MQ_POP_WAL_V2];
        bad.extend_from_slice(&0u32.to_le_bytes());
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.push(b'q');
        bad.extend_from_slice(&0u64.to_le_bytes());
        bad.extend_from_slice(&0u64.to_le_bytes());
        bad.extend_from_slice(&5u32.to_le_bytes()); // claims 5 but none follow
        assert!(decode_mq_pop(&bad).is_none());
    }

    #[test]
    fn test_mq_pop_unknown_version_rejected() {
        let mut payload = encode_mq_pop(0, b"q", (0, 0), &[], &[], 0, 0);
        payload[0] = 42;
        assert!(decode_mq_pop(&payload).is_none());
    }

    /// v1 payloads (pre-task-#47, no trailing timing fields) must still
    /// decode -- and must fill the sentinel `0` for both new fields,
    /// documented in `decode_mq_pop` as "not recorded; apply-time caller
    /// substitutes current time" rather than silently misreporting a real
    /// timestamp.
    #[test]
    fn test_mq_pop_v1_decodes_with_sentinel_timing() {
        let claimed: Vec<ClaimedEntry> = vec![(1, 0, 1)];
        let dlq: Vec<DlqRouting> = vec![];
        let mut v1 = vec![MQ_POP_WAL_V1];
        v1.extend_from_slice(&0u32.to_le_bytes()); // db_index
        v1.extend_from_slice(&(b"q".len() as u32).to_le_bytes());
        v1.extend_from_slice(b"q");
        v1.extend_from_slice(&1u64.to_le_bytes()); // last_delivered ms
        v1.extend_from_slice(&0u64.to_le_bytes()); // last_delivered seq
        v1.extend_from_slice(&(claimed.len() as u32).to_le_bytes());
        for (ms, seq, dc) in &claimed {
            v1.extend_from_slice(&ms.to_le_bytes());
            v1.extend_from_slice(&seq.to_le_bytes());
            v1.extend_from_slice(&dc.to_le_bytes());
        }
        v1.extend_from_slice(&(dlq.len() as u32).to_le_bytes());
        // NOTE: no trailing delivery_time_ms/seen_time_ms -- that's the v1/v2 delta.

        let (db, key, last, decoded_claimed, decoded_dlq, delivery_time_ms, seen_time_ms) =
            decode_mq_pop(&v1).unwrap();
        assert_eq!(db, 0);
        assert_eq!(key, b"q");
        assert_eq!(last, (1, 0));
        assert_eq!(decoded_claimed, claimed);
        assert!(decoded_dlq.is_empty());
        assert_eq!(delivery_time_ms, 0);
        assert_eq!(seen_time_ms, 0);
    }

    #[test]
    fn test_mq_pop_v2_truncated_timing_rejected() {
        // A v2-tagged payload that's missing the trailing 16 timing bytes
        // must fail closed, not silently degrade to sentinel-0 (that would
        // hide real truncation/corruption behind the v1 fallback path).
        let payload = encode_mq_pop(0, b"q", (0, 0), &[], &[], 999, 888);
        let truncated = &payload[..payload.len() - 16];
        assert!(decode_mq_pop(truncated).is_none());
    }

    // --- MqDrop roundtrip ---

    #[test]
    fn test_mq_drop_roundtrip() {
        let payload = encode_mq_drop(3, b"orders");
        let (db, key) = decode_mq_drop(&payload).unwrap();
        assert_eq!(db, 3);
        assert_eq!(key, b"orders");
    }

    #[test]
    fn test_mq_drop_roundtrip_empty_key() {
        let payload = encode_mq_drop(0, b"");
        let (db, key) = decode_mq_drop(&payload).unwrap();
        assert_eq!(db, 0);
        assert!(key.is_empty());
    }

    #[test]
    fn test_mq_drop_malformed_empty() {
        assert!(decode_mq_drop(b"").is_none());
    }

    #[test]
    fn test_mq_drop_malformed_truncated_key() {
        let mut bad = vec![MQ_WAL_VERSION];
        bad.extend_from_slice(&0u32.to_le_bytes());
        bad.extend_from_slice(&100u32.to_le_bytes()); // key_len = 100
        bad.extend_from_slice(&[0u8; 10]); // only 10 bytes of key
        assert!(decode_mq_drop(&bad).is_none());
    }

    #[test]
    fn test_mq_drop_unknown_version_rejected() {
        let mut payload = encode_mq_drop(0, b"q");
        payload[0] = 42;
        assert!(decode_mq_drop(&payload).is_none());
    }

    #[test]
    fn test_mq_drop_extra_bytes_ignored() {
        let mut payload = encode_mq_drop(1, b"q");
        payload.extend_from_slice(b"trailing");
        let (db, key) = decode_mq_drop(&payload).unwrap();
        assert_eq!(db, 1);
        assert_eq!(key, b"q");
    }

    // --- MqTrigger roundtrip ---

    #[test]
    fn test_mq_trigger_roundtrip() {
        let payload = encode_mq_trigger(b"wshex:orders", b"orders", b"PUBLISH ch msg", 1500);
        let (trig_key, queue_key, callback, debounce) = decode_mq_trigger(&payload).unwrap();
        assert_eq!(trig_key, b"wshex:orders");
        assert_eq!(queue_key, b"orders");
        assert_eq!(callback, b"PUBLISH ch msg");
        assert_eq!(debounce, 1500);
    }

    #[test]
    fn test_mq_trigger_roundtrip_empty_callback() {
        let payload = encode_mq_trigger(b"q", b"q", b"", 0);
        let (_, _, callback, debounce) = decode_mq_trigger(&payload).unwrap();
        assert!(callback.is_empty());
        assert_eq!(debounce, 0);
    }

    #[test]
    fn test_mq_trigger_malformed_truncated_callback() {
        let mut bad = vec![MQ_WAL_VERSION];
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.push(b'q');
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.push(b'q');
        bad.extend_from_slice(&50u32.to_le_bytes()); // callback_len = 50, nothing follows
        assert!(decode_mq_trigger(&bad).is_none());
    }

    #[test]
    fn test_mq_trigger_unknown_version_rejected() {
        let mut payload = encode_mq_trigger(b"q", b"q", b"CMD", 100);
        payload[0] = 9;
        assert!(decode_mq_trigger(&payload).is_none());
    }

    #[test]
    fn test_peek_version_empty() {
        assert_eq!(peek_version(&[]), None);
    }

    /// Adversarial-review P2: pre-versioning (unversioned) MqCreate/MqAck
    /// payloads written by dev builds between PR #286 and the stage-2a
    /// version bump must FAIL CLOSED (`None` → skip-and-warn at replay),
    /// never decode as garbage. The dangerous shape is a legacy record
    /// whose `key_len` low byte equals `MQ_WAL_VERSION` (1): its first
    /// byte passes the version check and the remaining bytes get
    /// reinterpreted at shifted offsets.
    #[test]
    fn test_legacy_unversioned_payloads_fail_closed() {
        // Legacy MqCreate: [key_len:u32][key][mdc:u32] with key_len = 1.
        // First byte = 0x01 == MQ_WAL_VERSION → version check passes; the
        // reinterpreted db_index/key_len fields must then fail the
        // structural length checks.
        let mut legacy_create = Vec::new();
        legacy_create.extend_from_slice(&1u32.to_le_bytes());
        legacy_create.push(b'q');
        legacy_create.extend_from_slice(&3u32.to_le_bytes());
        assert_eq!(
            decode_mq_create(&legacy_create),
            None,
            "legacy 1-byte-key MqCreate must fail closed, not decode as garbage"
        );

        // Same shape with key_len = 257 (low byte still 0x01).
        let key = vec![b'k'; 257];
        let mut legacy_create_257 = Vec::new();
        legacy_create_257.extend_from_slice(&257u32.to_le_bytes());
        legacy_create_257.extend_from_slice(&key);
        legacy_create_257.extend_from_slice(&3u32.to_le_bytes());
        assert_eq!(
            decode_mq_create(&legacy_create_257),
            None,
            "legacy 257-byte-key MqCreate must fail closed"
        );

        // Legacy MqAck: [key_len:u32][key][ms:u64][seq:u64], key_len = 1.
        let mut legacy_ack = Vec::new();
        legacy_ack.extend_from_slice(&1u32.to_le_bytes());
        legacy_ack.push(b'q');
        legacy_ack.extend_from_slice(&123u64.to_le_bytes());
        legacy_ack.extend_from_slice(&7u64.to_le_bytes());
        assert_eq!(
            decode_mq_ack(&legacy_ack),
            None,
            "legacy 1-byte-key MqAck must fail closed"
        );

        // Legacy layouts whose first byte is NOT the version (key_len 2)
        // fail at the version gate directly.
        let mut legacy_create_2 = Vec::new();
        legacy_create_2.extend_from_slice(&2u32.to_le_bytes());
        legacy_create_2.extend_from_slice(b"qq");
        legacy_create_2.extend_from_slice(&3u32.to_le_bytes());
        assert_eq!(decode_mq_create(&legacy_create_2), None);
    }
}
