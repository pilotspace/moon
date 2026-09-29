//! The KV half of `TXN.ABORT`, made durable (moon#1285, moon#1185 option b).
//!
//! # The defect
//!
//! A cross-store transaction applies its KV writes to the live keyspace as
//! they run, and the generic write path logs each one to the AOF and the
//! replication stream at that moment — a `TXN` body is not held back the way
//! a `MULTI` body is. `TXN.ABORT` then restored the before-images through
//! `Database::set` / `Database::remove` and logged NOTHING. The keyspace was
//! right until the next restart: AOF replay re-applied the forward writes, so
//! an aborted `SET k aborted` read back as `aborted` (both runtimes), and every
//! replica kept the aborted values forever.
//!
//! # The fix: compensating records
//!
//! Each key the transaction wrote is restored to its pre-transaction state and
//! the restore itself is logged through the normal append path, as ordinary
//! commands every replay and replica already understands:
//!
//! | undo record | restore | compensating record(s) |
//! |---|---|---|
//! | `Insert` (key was absent) | remove | `DEL k` |
//! | `Update` / `Delete` (key held `v`) | `v` back in place | `RESTORE k <abs-ms> <dump(v)> REPLACE ABSTTL`, then one `HPEXPIREAT k <ms> FIELDS n f..` per distinct field deadline of a hash with per-field TTLs (the DUMP payload does not carry them) |
//!
//! Both records are absolute: replayed on top of any prefix of the forward
//! writes they land the same value, so the result does not depend on how the
//! forward records and the abort interleave with a fold or a crash.
//!
//! Only the FIRST undo record of each `(db, key)` is applied (see
//! [`UndoLog::into_records_with_db`]): its before-image is the key's
//! pre-transaction state, which is where the old reverse replay of every
//! record ended anyway. A key written ten times costs one restore, one
//! pre-image and one record.
//!
//! # Snapshot pre-images, by move
//!
//! A BGSAVE epoch armed between a transaction's write and its abort must keep
//! the value that was live at the epoch instant F — the UNCOMMITTED value —
//! or the image is not point-in-time and the fold's exactly-once contract
//! (records at or above F replay on top of the base) breaks. The value the
//! undo replaces leaves the keyspace anyway, so it is taken out with
//! `remove_counting_cold_costed` and handed to the epoch by MOVE
//! ([`snapshot_cow::capture_removed_sized`], the moon#1269 removal pattern) —
//! never deep-cloned. When the key is absent at abort time its epoch-start
//! state is "absent" and a tombstone is recorded instead. If the forward write
//! came after F, dispatch already captured the pre-transaction value (first
//! capture wins) and the capture here disposes of the entry as usual.
//!
//! [`UndoLog::into_records_with_db`]: crate::transaction::UndoLog::into_records_with_db
//! [`snapshot_cow::capture_removed_sized`]: crate::persistence::snapshot_cow::capture_removed_sized

use std::collections::{BTreeMap, HashSet};

use bytes::{BufMut, Bytes, BytesMut};

use crate::persistence::{dump_payload, snapshot_cow};
use crate::storage::Database;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;
use crate::transaction::{UndoLog, UndoRecord};

/// A compensating record: the database it belongs to and the RESP command.
pub type CompensatingRecord = (usize, Bytes);

/// The undo records `TXN.ABORT` must apply: the first record of each
/// `(db, key)`, in capture order.
///
/// Allocation is proportional to the undo log: one dedupe set entry per
/// record (a `Bytes` refcount bump, not a copy) when the log has more than
/// one record.
pub(crate) fn first_per_key(log: UndoLog) -> Vec<(usize, UndoRecord)> {
    let n = log.len();
    let mut out = Vec::with_capacity(n);
    if n <= 1 {
        out.extend(log.into_records_with_db());
        return out;
    }
    let mut seen: HashSet<(usize, Bytes)> = HashSet::with_capacity(n);
    for (db, record) in log.into_records_with_db() {
        if seen.insert((db, record_key(&record).clone())) {
            out.push((db, record));
        }
    }
    out
}

/// The key an undo record restores.
#[inline]
pub(crate) fn record_key(record: &UndoRecord) -> &Bytes {
    match record {
        UndoRecord::Insert { key }
        | UndoRecord::Update { key, .. }
        | UndoRecord::Delete { key, .. } => key,
    }
}

/// Restore one key of an aborted transaction in `db` (which MUST be the
/// shard's `databases[slot]`) and append the records that make the restore
/// durable and replicable to `out`.
///
/// The value the restore replaces becomes the armed snapshot's pre-image by
/// move (module docs). Runs on the shard thread under the caller's guard.
pub(crate) fn undo_one(
    db: &mut Database,
    slot: usize,
    record: UndoRecord,
    out: &mut Vec<CompensatingRecord>,
) {
    match record {
        UndoRecord::Insert { key } => {
            take_out_current(db, slot, &key);
            out.push((slot, encode_command(&[b"DEL", &key])));
        }
        UndoRecord::Update { key, old_entry } | UndoRecord::Delete { key, old_entry } => {
            take_out_current(db, slot, &key);
            push_restore_records(slot, &key, &old_entry, out);
            db.set(&key, old_entry);
        }
    }
}

/// Remove whatever `key` holds now (hot and cold copies), handing a hot
/// entry to an armed snapshot as the key's pre-image by MOVE, or recording
/// "absent" as its pre-image when there is none.
fn take_out_current(db: &mut Database, slot: usize, key: &[u8]) {
    let (_live, hot) = db.remove_counting_cold_costed(key);
    match hot {
        // Dispose: no save wants it — dropped here, as `DEL` drops it.
        Some((entry, cost)) => drop(snapshot_cow::capture_removed_sized(slot, key, entry, cost)),
        // No hot entry: the epoch-start state is "absent" (the image holds hot
        // keys only). One thread-local load when no save is armed.
        None => snapshot_cow::capture_write_pre_image(db, slot, key),
    }
}

/// `RESTORE key <abs-ms> <payload> REPLACE ABSTTL` for `entry`, plus the
/// per-field deadlines the payload cannot carry.
fn push_restore_records(
    slot: usize,
    key: &Bytes,
    entry: &Entry,
    out: &mut Vec<CompensatingRecord>,
) {
    // The HPEXPIREAT records below restore what the payload cannot carry,
    // so the encoder's "drops per-field TTLs" warning would be false here.
    let payload = dump_payload::encode_field_ttls_restored_separately(entry);
    let mut ms = itoa::Buffer::new();
    // `0` = no expiry: RESTORE only applies a TTL above zero. A deadline
    // already in the past is kept as is — RESTORE ABSTTL of a past deadline
    // answers OK and the key reads as absent, exactly like the restored
    // entry does here.
    let abs_ms = ms.format(if entry.has_expiry() {
        entry.expires_at_ms()
    } else {
        0
    });
    out.push((
        slot,
        encode_command(&[
            b"RESTORE",
            key,
            abs_ms.as_bytes(),
            &payload,
            b"REPLACE",
            b"ABSTTL",
        ]),
    ));
    if let RedisValueRef::HashWithTtl { fields, ttls, .. } = entry.value.as_redis_value() {
        // Group fields by deadline: one record per distinct deadline, in
        // deadline order (deterministic output for a deterministic input).
        let mut by_deadline: BTreeMap<u64, Vec<&Bytes>> = BTreeMap::new();
        for (field, deadline) in ttls.iter() {
            if fields.contains_key(field) {
                by_deadline.entry(*deadline).or_default().push(field);
            }
        }
        for (deadline, mut group) in by_deadline {
            group.sort();
            let mut when = itoa::Buffer::new();
            let mut count = itoa::Buffer::new();
            let when = when.format(deadline);
            let count = count.format(group.len());
            let mut parts: Vec<&[u8]> = Vec::with_capacity(5 + group.len());
            parts.extend_from_slice(&[
                b"HPEXPIREAT",
                key.as_ref(),
                when.as_bytes(),
                b"FIELDS",
                count.as_bytes(),
            ]);
            parts.extend(group.iter().map(|f| f.as_ref()));
            out.push((slot, encode_command(&parts)));
        }
    }
}

/// One RESP array of bulk strings, built straight into its buffer.
pub(crate) fn encode_command(parts: &[&[u8]]) -> Bytes {
    let body: usize = parts.iter().map(|p| p.len() + 16).sum();
    let mut buf = BytesMut::with_capacity(16 + body);
    let mut n = itoa::Buffer::new();
    buf.put_u8(b'*');
    buf.put_slice(n.format(parts.len()).as_bytes());
    buf.put_slice(b"\r\n");
    for part in parts {
        buf.put_u8(b'$');
        buf.put_slice(n.format(part.len()).as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(part);
        buf.put_slice(b"\r\n");
    }
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Frame;
    use crate::storage::entry::current_time_ms;

    fn dispatch(db: &mut Database, parts: &[&[u8]]) -> Frame {
        let (cmd, args) = parts.split_first().expect("command");
        let frames: Vec<Frame> = args
            .iter()
            .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
            .collect();
        let mut selected = 0usize;
        match crate::command::dispatch(db, cmd, &frames, &mut selected, 16) {
            crate::command::DispatchResult::Response(f)
            | crate::command::DispatchResult::Quit(f) => f,
        }
    }

    /// Replay a compensating record the way AOF replay and replica apply do:
    /// parse the RESP, dispatch it.
    fn replay(db: &mut Database, record: &Bytes) -> Frame {
        let mut buf = BytesMut::from(record.as_ref());
        let frame =
            crate::protocol::parse::parse(&mut buf, &crate::protocol::ParseConfig::default())
                .expect("well-formed record")
                .expect("one complete frame");
        let Frame::Array(items) = frame else {
            panic!("not an array: {frame:?}")
        };
        let parts: Vec<Vec<u8>> = items
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.to_vec(),
                other => panic!("not a bulk string: {other:?}"),
            })
            .collect();
        let refs: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
        dispatch(db, &refs)
    }

    fn get(db: &mut Database, key: &[u8]) -> Frame {
        dispatch(db, &[b"GET", key])
    }

    #[test]
    fn encode_command_is_resp_bulk_array() {
        assert_eq!(
            encode_command(&[b"DEL", b"k"]).as_ref(),
            b"*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"
        );
    }

    #[test]
    fn first_per_key_keeps_the_oldest_before_image_per_db_and_key() {
        let mut log = UndoLog::new();
        log.record_update(
            0,
            Bytes::from_static(b"k"),
            Entry::new_string(Bytes::from_static(b"v0")),
        );
        log.record_update(
            0,
            Bytes::from_static(b"k"),
            Entry::new_string(Bytes::from_static(b"v1")),
        );
        log.record_insert(1, Bytes::from_static(b"k"));
        log.record_insert(1, Bytes::from_static(b"k"));
        let plan = first_per_key(log);
        assert_eq!(plan.len(), 2, "one record per (db, key)");
        match &plan[0] {
            (0, UndoRecord::Update { old_entry, .. }) => {
                assert_eq!(old_entry.value.as_bytes().expect("string"), b"v0")
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(matches!(plan[1], (1, UndoRecord::Insert { .. })));
    }

    /// The live undo and the replayed compensation land the same keyspace:
    /// forward writes replayed, then the compensating records, equals the
    /// aborted-to state for insert, update (with TTL) and delete.
    #[test]
    fn compensation_replayed_after_the_forward_writes_equals_the_live_abort() {
        let deadline = current_time_ms() + 600_000;
        let deadline_s = deadline.to_string();
        // Pre-transaction state, twice: `live` undergoes the abort, `replica`
        // only ever sees the log (forward writes + compensation).
        let seed = |db: &mut Database| {
            dispatch(db, &[b"SET", b"upd", b"original"]);
            dispatch(
                db,
                &[b"SET", b"ttl", b"keep", b"PXAT", deadline_s.as_bytes()],
            );
            dispatch(db, &[b"RPUSH", b"del", b"a", b"b", b"c"]);
        };
        let mut live = Database::new();
        let mut replica = Database::new();
        seed(&mut live);
        seed(&mut replica);

        // The transaction: capture undo as the handler does, then write.
        let mut log = UndoLog::new();
        let forward: [&[&[u8]]; 5] = [
            &[b"SET", b"upd", b"aborted"],
            &[b"SET", b"ttl", b"changed"],
            &[b"DEL", b"del"],
            &[b"SET", b"new", b"inserted"],
            &[b"SET", b"upd", b"aborted-again"],
        ];
        for cmd in forward {
            let key = cmd[1];
            match live.get(key).cloned() {
                None => log.record_insert(0, Bytes::copy_from_slice(key)),
                Some(e) if cmd[0] == b"DEL" => log.record_delete(0, Bytes::copy_from_slice(key), e),
                Some(e) => log.record_update(0, Bytes::copy_from_slice(key), e),
            }
            dispatch(&mut live, cmd);
            dispatch(&mut replica, cmd);
        }

        let mut records = Vec::new();
        for (db, record) in first_per_key(log) {
            undo_one(&mut live, db, record, &mut records);
        }
        assert_eq!(records.len(), 4, "one record per key: {records:?}");
        for (db, record) in &records {
            assert_eq!(*db, 0);
            assert!(
                !matches!(replay(&mut replica, record), Frame::Error(_)),
                "{record:?}"
            );
        }

        for db in [&mut live, &mut replica] {
            assert_eq!(
                get(db, b"upd"),
                Frame::BulkString(Bytes::from_static(b"original"))
            );
            assert_eq!(
                get(db, b"ttl"),
                Frame::BulkString(Bytes::from_static(b"keep"))
            );
            assert_eq!(db.get(b"ttl").map(Entry::expires_at_ms), Some(deadline));
            assert_eq!(get(db, b"new"), Frame::Null);
            assert_eq!(
                dispatch(db, &[b"LRANGE", b"del", b"0", b"-1"]),
                Frame::Array(
                    vec![
                        Frame::BulkString(Bytes::from_static(b"a")),
                        Frame::BulkString(Bytes::from_static(b"b")),
                        Frame::BulkString(Bytes::from_static(b"c")),
                    ]
                    .into()
                )
            );
        }
    }

    /// A hash with per-field TTLs: the DUMP payload drops them, the
    /// `HPEXPIREAT` records that follow the `RESTORE` put them back.
    #[test]
    fn hash_field_ttls_survive_the_compensation() {
        let deadline = current_time_ms() + 900_000;
        let deadline_s = deadline.to_string();
        let seed = |db: &mut Database| {
            dispatch(
                db,
                &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"],
            );
            dispatch(
                db,
                &[
                    b"HPEXPIREAT",
                    b"h",
                    deadline_s.as_bytes(),
                    b"FIELDS",
                    b"2",
                    b"f1",
                    b"f3",
                ],
            );
        };
        let mut live = Database::new();
        let mut replica = Database::new();
        seed(&mut live);
        seed(&mut replica);
        let mut log = UndoLog::new();
        log.record_update(
            0,
            Bytes::from_static(b"h"),
            live.get(b"h").cloned().expect("hash"),
        );
        for db in [&mut live, &mut replica] {
            dispatch(db, &[b"HSET", b"h", b"f1", b"X", b"f4", b"Y"]);
        }
        let mut records = Vec::new();
        for (db, record) in first_per_key(log) {
            undo_one(&mut live, db, record, &mut records);
        }
        assert_eq!(records.len(), 2, "RESTORE + one HPEXPIREAT: {records:?}");
        for (_, record) in &records {
            assert!(!matches!(replay(&mut replica, record), Frame::Error(_)));
        }
        for db in [&mut live, &mut replica] {
            let ttls = dispatch(
                db,
                &[b"HPEXPIRETIME", b"h", b"FIELDS", b"3", b"f1", b"f2", b"f3"],
            );
            assert_eq!(
                ttls,
                Frame::Array(
                    vec![
                        Frame::Integer(deadline as i64),
                        Frame::Integer(-1),
                        Frame::Integer(deadline as i64),
                    ]
                    .into()
                )
            );
            assert_eq!(dispatch(db, &[b"HLEN", b"h"]), Frame::Integer(3));
            assert_eq!(
                dispatch(db, &[b"HGET", b"h", b"f1"]),
                Frame::BulkString(Bytes::from_static(b"v1"))
            );
        }
    }

    /// WARN lines logged on this thread while `f` runs.
    fn warnings_during(f: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Capture(std::sync::Arc<parking_lot::Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
            type Writer = Capture;
            fn make_writer(&'a self) -> Capture {
                self.clone()
            }
        }
        let cap = Capture::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(cap.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let out = String::from_utf8_lossy(&cap.0.lock()).into_owned();
        out
    }

    /// Wave-1 review NIT: every abort restoring a hash with field TTLs logged
    /// "Redis-compat RDB drops per-field TTLs" although its `HPEXPIREAT`
    /// records restore them. The compensation is quiet; a plain `DUMP` of the
    /// same entry still warns (the capture can see the warning), and both
    /// payloads are byte-identical.
    #[test]
    fn restoring_a_hash_with_field_ttls_does_not_warn_that_they_are_dropped() {
        let mut db = Database::new();
        dispatch(&mut db, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]);
        let deadline = (current_time_ms() + 900_000).to_string();
        dispatch(
            &mut db,
            &[
                b"HPEXPIREAT",
                b"h",
                deadline.as_bytes(),
                b"FIELDS",
                b"1",
                b"f1",
            ],
        );
        let entry = db.get(b"h").cloned().expect("hash");
        let key = Bytes::from_static(b"h");
        let mut records = Vec::new();
        let quiet = warnings_during(|| push_restore_records(0, &key, &entry, &mut records));
        assert_eq!(records.len(), 2, "RESTORE + HPEXPIREAT: {records:?}");
        assert!(
            !quiet.contains("drops per-field TTLs"),
            "the compensation warned: {quiet}"
        );
        let mut dumped = Vec::new();
        let loud = warnings_during(|| dumped = dump_payload::encode(&entry));
        assert!(loud.contains("drops per-field TTLs"), "control: {loud:?}");
        assert_eq!(
            dumped,
            dump_payload::encode_field_ttls_restored_separately(&entry)
        );
    }
}
