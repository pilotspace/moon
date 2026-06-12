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
pub(crate) fn execute_mq_on_owner(
    db_index: usize,
    key_prefix: Bytes,
    command: std::sync::Arc<Frame>,
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
        return handle_create(cmd_args, &key_prefix, db_index);
    }
    if sub.eq_ignore_ascii_case(b"PUSH") {
        return handle_push(cmd_args, &key_prefix, db_index);
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

/// Append a WAL record byte sequence on the current shard thread via the
/// slice's `wal_append_tx` channel (fire-and-forget).
///
/// Mirrors `ShardDatabases::wal_append` semantics — `try_send` failures are
/// ignored (the channel is bounded; under extreme backpressure the record is
/// dropped, same as the existing lock-path).
#[inline]
fn wal_append_on_slice(record_bytes: bytes::Bytes) {
    crate::shard::slice::with_shard(|s| {
        if let Some(ref tx) = s.wal_append_tx {
            let _ = tx.try_send(record_bytes);
        }
    });
}

// ── Subcommand handlers ───────────────────────────────────────────────────────

/// MQ.CREATE — owner creates the durable stream + registry entry.
///
/// Mirrors handler_sharded/write.rs MQ CREATE arm (lock-path `else` branch).
fn handle_create(args: &[Frame], key_prefix: &Bytes, db_index: usize) -> Frame {
    let (raw_key, max_delivery_count, _debounce_ms) = match validate_mq_create(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);

    // Create / configure the durable stream.
    let create_result: Result<(), Frame> = crate::shard::slice::with_shard_db(db_index, |db| {
        match db.get_or_create_stream(&eff_key) {
            Ok(stream) => {
                stream.durable = true;
                stream.max_delivery_count = max_delivery_count;
                let group_name = Bytes::from_static(b"__mq_consumers");
                let _ = stream.create_group(group_name, StreamId::ZERO);
                Ok(())
            }
            Err(e) => Err(e),
        }
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

    // WAL: MqCreate record on the owner shard.
    {
        let payload = crate::mq::wal::encode_mq_create(&eff_key, max_delivery_count);
        let mut wal_buf = Vec::new();
        crate::persistence::wal_v3::record::write_wal_v3_record(
            &mut wal_buf,
            0,
            crate::persistence::wal_v3::record::WalRecordType::MqCreate,
            &payload,
        );
        wal_append_on_slice(Bytes::from(wal_buf));
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// MQ.PUSH — owner enqueues a message into the durable stream.
///
/// Mirrors handler_sharded/write.rs MQ PUSH arm (lock-path `else` branch).
/// Fires any registered trigger's debounce timer (sets `pending_fire_ms` if
/// not already pending).
fn handle_push(args: &[Frame], key_prefix: &Bytes, db_index: usize) -> Frame {
    let (raw_key, fields) = match validate_mq_push(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);
    let trig_key = derive_trig_key(key_prefix, &raw_key);

    // Push into the stream.
    type PushResult = Result<Option<StreamId>, Frame>;
    let push_result: PushResult =
        crate::shard::slice::with_shard_db(db_index, |db| match db.get_stream_mut(&eff_key) {
            Ok(Some(stream)) => {
                if !stream.durable {
                    Ok(None)
                } else {
                    let msg_id = stream.next_auto_id();
                    let msg_id = stream.add(msg_id, fields);
                    Ok(Some(msg_id))
                }
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        });

    match push_result {
        Ok(Some(msg_id)) => {
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
/// Mirrors handler_sharded/write.rs MQ POP arm (lock-path `else` branch).
fn handle_pop(args: &[Frame], key_prefix: &Bytes, db_index: usize) -> Frame {
    let (raw_key, count) = match validate_mq_pop(args) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let eff_key = effective_key(key_prefix, &raw_key);
    let group_name = Bytes::from_static(b"__mq_consumers");
    let consumer_name = Bytes::from_static(b"__mq_default");

    // All POP logic runs in a single with_shard_db closure to avoid re-entrancy.
    crate::shard::slice::with_shard_db(db_index, |db| {
        // Step 1: read max_delivery_count.
        let mdc = match db.get_stream_mut(&eff_key) {
            Ok(Some(stream)) => {
                if !stream.durable {
                    return Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE));
                }
                stream.max_delivery_count
            }
            Ok(None) => return Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)),
            Err(e) => return e,
        };

        let request_count = count + (mdc as usize);

        // Step 2: read_group_new to claim entries.
        let stream = match db.get_stream_mut(&eff_key) {
            Ok(Some(s)) => s,
            _ => return Frame::Error(Bytes::from_static(ERR_MQ_NOT_DURABLE)),
        };
        let claimed =
            match stream.read_group_new(&group_name, &consumer_name, Some(request_count), false) {
                Ok(entries) => entries,
                Err(_) => return Frame::Array(vec![].into()),
            };

        // Step 3: partition claimed into good entries and DLQ entries.
        let mut results: Vec<(StreamId, Vec<(Bytes, Bytes)>)> =
            Vec::with_capacity(count.min(claimed.len()));
        let mut dlq_entries: Vec<(StreamId, Vec<(Bytes, Bytes)>)> = Vec::new();
        let mut dlq_ack_ids: Vec<StreamId> = Vec::new();

        for (id, fields) in &claimed {
            let delivery_count = stream
                .groups
                .get(group_name.as_ref())
                .and_then(|g| g.pel.get(id))
                .map(|pe| pe.delivery_count)
                .unwrap_or(1);
            if mdc > 0 && delivery_count >= mdc as u64 {
                dlq_entries.push((*id, fields.clone()));
                dlq_ack_ids.push(*id);
            } else if results.len() < count {
                results.push((*id, fields.clone()));
            }
        }

        // Step 4: ACK DLQ entries from the main stream PEL.
        if !dlq_ack_ids.is_empty() {
            let _ = stream.xack(&group_name, &dlq_ack_ids);
        }

        // Step 5: append DLQ entries to the sibling DLQ stream.
        if !dlq_entries.is_empty() {
            let dlq_key = {
                let mut buf = Vec::with_capacity(eff_key.len() + 8);
                buf.extend_from_slice(&eff_key);
                buf.extend_from_slice(b"::mq:dlq");
                Bytes::from(buf)
            };
            if let Ok(dlq_stream) = db.get_or_create_stream(&dlq_key) {
                for (_id, fields) in dlq_entries {
                    let dlq_id = dlq_stream.next_auto_id();
                    dlq_stream.add(dlq_id, fields);
                }
            }
        }

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
                    .flat_map(|(f, v)| [Frame::BulkString(f.clone()), Frame::BulkString(v.clone())])
                    .collect();
                entry_frames.push(Frame::Array(field_frames.into()));
                Frame::Array(entry_frames.into())
            })
            .collect();
        Frame::Array(result_frames.into())
    })
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

    let ack_result =
        crate::shard::slice::with_shard_db(db_index, |db| match db.get_stream_mut(&eff_key) {
            Ok(Some(stream)) => {
                let group_name = Bytes::from_static(b"__mq_consumers");
                match stream.xack(&group_name, &ids) {
                    Ok(count) => Some(count),
                    Err(_) => None,
                }
            }
            _ => None,
        });

    match ack_result {
        Some(acked_count) => {
            // Emit one WAL record per acked message id.
            for (ms, seq) in &msg_ids {
                let payload = crate::mq::wal::encode_mq_ack(&eff_key, *ms, *seq);
                let mut wal_buf = Vec::new();
                crate::persistence::wal_v3::record::write_wal_v3_record(
                    &mut wal_buf,
                    0,
                    crate::persistence::wal_v3::record::WalRecordType::MqAck,
                    &payload,
                );
                wal_append_on_slice(Bytes::from(wal_buf));
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

    // `db_index` is unused for TRIGGER (registry only) but kept in the
    // signature for API uniformity. Suppress the unused-variable warning.
    let _ = db_index;

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::shard::shared_databases::ShardStoreMemory;
    use crate::shard::slice::{ShardSlice, ShardSliceInit, init_shard};
    use crate::storage::Database;
    use crate::text::store::TextStore;
    use crate::transaction::{DeferredHnswInserts, KvWriteIntents};
    use crate::vector::store::VectorStore;

    fn make_test_slice(db_count: usize) -> ShardSlice {
        let databases: Box<[Database]> = (0..db_count).map(|_| Database::new()).collect();
        ShardSlice::new(ShardSliceInit {
            shard_id: 0,
            databases,
            vector_store: VectorStore::new(),
            text_store: TextStore::new(),
            #[cfg(feature = "graph")]
            graph_store: crate::graph::store::GraphStore::new(),
            kv_write_intents: KvWriteIntents::new(),
            deferred_hnsw_inserts: DeferredHnswInserts::new(),
            temporal_registry: None,
            temporal_kv_index: None,
            durable_queue_registry: None,
            trigger_registry: None,
            wal_append_tx: None,
            estimated_memory: Arc::new(AtomicUsize::new(0)),
            store_memory: Arc::new(ShardStoreMemory {
                vector: AtomicUsize::new(0),
                text: AtomicUsize::new(0),
                graph: AtomicUsize::new(0),
            }),
        })
    }

    // ── effective_key derivation ──────────────────────────────────────────────

    #[test]
    fn effective_key_without_prefix() {
        let prefix = Bytes::new();
        let raw = Bytes::from_static(b"myqueue");
        let eff = effective_key(&prefix, &raw);
        assert_eq!(eff.as_ref(), b"myqueue");
    }

    #[test]
    fn effective_key_with_prefix() {
        // key_prefix = "{" + 32 hex chars + "}:"
        let ws_hex = "0102030405060708090a0b0c0d0e0f10";
        let prefix_str = format!("{{{ws_hex}}}:");
        let prefix = Bytes::from(prefix_str.clone());
        let raw = Bytes::from_static(b"tasks");
        let eff = effective_key(&prefix, &raw);
        let expected = format!("{prefix_str}tasks");
        assert_eq!(eff.as_ref(), expected.as_bytes());
    }

    // ── derive_trig_key ───────────────────────────────────────────────────────

    #[test]
    fn trig_key_without_prefix() {
        let prefix = Bytes::new();
        let raw = Bytes::from_static(b"alerts");
        let trig = derive_trig_key(&prefix, &raw);
        assert_eq!(trig.as_ref(), b"alerts");
    }

    #[test]
    fn trig_key_with_prefix() {
        // prefix = "{0102...10}:" (35 bytes)
        let ws_hex = "0102030405060708090a0b0c0d0e0f10";
        let prefix = Bytes::from(format!("{{{ws_hex}}}:"));
        let raw = Bytes::from_static(b"alerts");
        let trig = derive_trig_key(&prefix, &raw);
        // expected: ws_hex + ":" + "alerts"
        let expected = format!("{ws_hex}:alerts");
        assert_eq!(trig.as_ref(), expected.as_bytes());
    }

    // ── DLQLEN on empty queue ─────────────────────────────────────────────────

    #[test]
    fn dlqlen_empty_queue_returns_zero() {
        // Use a fresh OS thread so init_shard doesn't conflict with the test thread.
        let result = std::thread::spawn(|| {
            init_shard(make_test_slice(1));

            // Build a fake MQ DLQLEN command frame.
            let cmd = Arc::new(Frame::Array(
                vec![
                    Frame::BulkString(Bytes::from_static(b"MQ")),
                    Frame::BulkString(Bytes::from_static(b"DLQLEN")),
                    Frame::BulkString(Bytes::from_static(b"nosuchqueue")),
                ]
                .into(),
            ));
            execute_mq_on_owner(0, Bytes::new(), cmd)
        })
        .join()
        .expect("test thread panicked");

        assert_eq!(result, Frame::Integer(0));
    }

    // ── MQ.CREATE + DLQLEN round-trip ─────────────────────────────────────────

    #[test]
    fn create_then_dlqlen_zero() {
        let result = std::thread::spawn(|| {
            init_shard(make_test_slice(1));

            // CREATE
            let create_cmd = Arc::new(Frame::Array(
                vec![
                    Frame::BulkString(Bytes::from_static(b"MQ")),
                    Frame::BulkString(Bytes::from_static(b"CREATE")),
                    Frame::BulkString(Bytes::from_static(b"testq")),
                ]
                .into(),
            ));
            let create_result = execute_mq_on_owner(0, Bytes::new(), create_cmd);
            assert_eq!(
                create_result,
                Frame::SimpleString(Bytes::from_static(b"OK")),
                "CREATE must return +OK"
            );

            // DLQLEN on newly created queue — DLQ stream doesn't exist yet.
            let dlqlen_cmd = Arc::new(Frame::Array(
                vec![
                    Frame::BulkString(Bytes::from_static(b"MQ")),
                    Frame::BulkString(Bytes::from_static(b"DLQLEN")),
                    Frame::BulkString(Bytes::from_static(b"testq")),
                ]
                .into(),
            ));
            execute_mq_on_owner(0, Bytes::new(), dlqlen_cmd)
        })
        .join()
        .expect("test thread panicked");

        assert_eq!(result, Frame::Integer(0));
    }
}
