//! Translate a `WalRecord` into a typed `CdcEvent`.
//!
//! The decoder is intentionally permissive: a record we don't fully
//! understand becomes `CdcEvent::Other` rather than an error. The CDC
//! stream must keep flowing even when new record types are introduced —
//! consumers can always re-parse the raw payload.

use bytes::Bytes;

use crate::persistence::wal_v3::record::{
    WalRecord, WalRecordType, decode_graph_temporal, decode_temporal_upsert,
};

use super::event::{CdcEvent, CdcOp};

/// Decode one WAL record into a CDC event.
///
/// `shard` is supplied by the caller — WAL records don't carry shard ID
/// (it lives in the segment header), so the caller plumbs it through from
/// the segment context.
pub fn decode_wal_record(record: &WalRecord, shard: u16) -> CdcEvent {
    match record.record_type {
        WalRecordType::Command => decode_kv_command(record, shard),
        WalRecordType::TemporalUpsert => match decode_temporal_upsert(&record.payload) {
            Some((key, valid_from, system_from, value)) => CdcEvent::TemporalUpsert {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
                valid_from,
                system_from,
                lsn: record.lsn,
                shard,
            },
            None => CdcEvent::Other {
                record_type: record.record_type as u8,
                payload: Bytes::copy_from_slice(&record.payload),
                lsn: record.lsn,
                shard,
            },
        },
        WalRecordType::GraphTemporal => match decode_graph_temporal(&record.payload) {
            Some((entity_id, is_node, valid_to, system_from)) => CdcEvent::GraphTemporal {
                entity_id,
                is_node,
                valid_to,
                system_from,
                lsn: record.lsn,
                shard,
            },
            None => CdcEvent::Other {
                record_type: record.record_type as u8,
                payload: Bytes::copy_from_slice(&record.payload),
                lsn: record.lsn,
                shard,
            },
        },
        WalRecordType::Checkpoint => {
            let redo_lsn = if record.payload.len() >= 8 {
                u64::from_le_bytes(record.payload[..8].try_into().unwrap_or([0; 8]))
            } else {
                0
            };
            CdcEvent::Checkpoint {
                redo_lsn,
                lsn: record.lsn,
                shard,
            }
        }
        _ => CdcEvent::Other {
            record_type: record.record_type as u8,
            payload: Bytes::copy_from_slice(&record.payload),
            lsn: record.lsn,
            shard,
        },
    }
}

/// Best-effort extraction of (command, key) from a RESP-encoded Command
/// record. Returns a fully-populated `KvCommand` event either way.
///
/// We classify a tiny set of well-known commands to give downstream the
/// right Debezium `op` code. Anything we don't recognize falls back to
/// `CdcOp::Other` and the consumer can inspect `raw_resp`.
fn decode_kv_command(record: &WalRecord, shard: u16) -> CdcEvent {
    let raw_resp = Bytes::copy_from_slice(&record.payload);
    let (command, key) = extract_command_and_key(&raw_resp);
    let op = classify_command(command);
    CdcEvent::KvCommand {
        op,
        command: command.map(Bytes::copy_from_slice),
        key: key.map(Bytes::copy_from_slice),
        raw_resp,
        lsn: record.lsn,
        shard,
    }
}

/// Pull the first two bulk strings out of a RESP array payload.
///
/// Returns `(verb, first_arg)`. Either may be `None` if parsing fails or
/// the payload isn't shaped like an array. We do NOT use the full RESP
/// parser here — that would pull in command-dispatch concerns. The mini
/// scanner below handles `*N\r\n$M\r\n<bytes>\r\n` arrays which is what
/// every Command record actually contains.
fn extract_command_and_key(payload: &[u8]) -> (Option<&[u8]>, Option<&[u8]>) {
    let mut pos = 0;
    // Expect leading '*'
    if payload.first().copied() != Some(b'*') {
        return (None, None);
    }
    pos += 1;
    // Skip array count to CRLF
    pos = match skip_to_crlf(payload, pos) {
        Some(p) => p,
        None => return (None, None),
    };
    let verb = match read_bulk_string(payload, &mut pos) {
        Some(s) => s,
        None => return (None, None),
    };
    let key = read_bulk_string(payload, &mut pos);
    (Some(verb), key)
}

/// Read one `$N\r\n<bytes>\r\n` bulk string and advance `pos` past it.
fn read_bulk_string<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    if buf.get(*pos).copied() != Some(b'$') {
        return None;
    }
    *pos += 1;
    let len_end = find_crlf(buf, *pos)?;
    let len_str = std::str::from_utf8(&buf[*pos..len_end]).ok()?;
    let len: i64 = len_str.parse().ok()?;
    *pos = len_end + 2; // past CRLF
    if len < 0 {
        return None;
    }
    let len = len as usize;
    if *pos + len + 2 > buf.len() {
        return None;
    }
    let out = &buf[*pos..*pos + len];
    *pos += len + 2; // past content + CRLF
    Some(out)
}

fn skip_to_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let end = find_crlf(buf, from)?;
    Some(end + 2)
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Map a command verb to a Debezium-style op code.
fn classify_command(verb: Option<&[u8]>) -> CdcOp {
    let v = match verb {
        Some(v) => v,
        None => return CdcOp::Other,
    };
    // ASCII upper-casing in place — verbs are short, this is fine.
    let mut upper: [u8; 16] = [0; 16];
    let n = v.len().min(upper.len());
    for i in 0..n {
        upper[i] = v[i].to_ascii_uppercase();
    }
    let upper = &upper[..n];
    match upper {
        // Deletes
        b"DEL" | b"UNLINK" | b"HDEL" | b"SREM" | b"ZREM" | b"LREM" | b"XDEL" => CdcOp::Delete,
        // Upserts (the common KV / collection mutations)
        b"SET" | b"SETNX" | b"SETEX" | b"PSETEX" | b"MSET" | b"MSETNX" | b"APPEND" | b"GETSET"
        | b"INCR" | b"INCRBY" | b"INCRBYFLOAT" | b"DECR" | b"DECRBY" | b"HSET" | b"HMSET"
        | b"HSETNX" | b"HINCRBY" | b"HINCRBYFLOAT" | b"LPUSH" | b"LPUSHX" | b"RPUSH"
        | b"RPUSHX" | b"LSET" | b"SADD" | b"ZADD" | b"ZINCRBY" | b"XADD" | b"COPY" | b"RENAME"
        | b"RENAMENX" | b"PEXPIRE" | b"EXPIRE" | b"EXPIREAT" | b"PEXPIREAT" | b"PERSIST" => {
            CdcOp::Upsert
        }
        _ => CdcOp::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::wal_v3::record::{encode_graph_temporal, encode_temporal_upsert};

    fn mk_command(payload: &[u8], lsn: u64) -> WalRecord {
        WalRecord {
            lsn,
            record_type: WalRecordType::Command,
            flags: 0,
            payload: payload.to_vec(),
        }
    }

    /// C2 — SET command must decode into KvCommand{Upsert, "SET", "k"}.
    #[test]
    fn test_decode_set_command() {
        let resp = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        let rec = mk_command(resp, 7);
        let event = decode_wal_record(&rec, 4);
        match event {
            CdcEvent::KvCommand {
                op,
                command,
                key,
                lsn,
                shard,
                ..
            } => {
                assert_eq!(op, CdcOp::Upsert);
                assert_eq!(command.as_deref(), Some(b"SET" as &[u8]));
                assert_eq!(key.as_deref(), Some(b"k" as &[u8]));
                assert_eq!(lsn, 7);
                assert_eq!(shard, 4);
            }
            other => panic!("expected KvCommand, got {:?}", other),
        }
    }

    /// C2 — DEL command must classify as CdcOp::Delete.
    #[test]
    fn test_decode_del_command() {
        let resp = b"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n";
        let rec = mk_command(resp, 9);
        let event = decode_wal_record(&rec, 0);
        match event {
            CdcEvent::KvCommand {
                op, command, key, ..
            } => {
                assert_eq!(op, CdcOp::Delete);
                assert_eq!(command.as_deref(), Some(b"DEL" as &[u8]));
                assert_eq!(key.as_deref(), Some(b"foo" as &[u8]));
            }
            _ => panic!("expected KvCommand"),
        }
    }

    /// C2 — unknown / unmodeled command falls back to CdcOp::Other but the
    /// raw_resp must always be preserved for consumer-side re-parsing.
    #[test]
    fn test_decode_unknown_command_preserves_raw() {
        let resp = b"*1\r\n$4\r\nPING\r\n";
        let rec = mk_command(resp, 1);
        let event = decode_wal_record(&rec, 0);
        match event {
            CdcEvent::KvCommand {
                op,
                command,
                raw_resp,
                ..
            } => {
                assert_eq!(op, CdcOp::Other);
                assert_eq!(command.as_deref(), Some(b"PING" as &[u8]));
                assert_eq!(raw_resp.as_ref(), resp);
            }
            _ => panic!("expected KvCommand"),
        }
    }

    /// C2 — TemporalUpsert payload decodes via the existing helper.
    #[test]
    fn test_decode_temporal_upsert() {
        let payload = encode_temporal_upsert(b"k", 100, 200, b"v");
        let rec = WalRecord {
            lsn: 5,
            record_type: WalRecordType::TemporalUpsert,
            flags: 0,
            payload,
        };
        let event = decode_wal_record(&rec, 1);
        match event {
            CdcEvent::TemporalUpsert {
                key,
                value,
                valid_from,
                system_from,
                lsn,
                shard,
            } => {
                assert_eq!(key.as_ref(), b"k");
                assert_eq!(value.as_ref(), b"v");
                assert_eq!(valid_from, 100);
                assert_eq!(system_from, 200);
                assert_eq!(lsn, 5);
                assert_eq!(shard, 1);
            }
            _ => panic!("expected TemporalUpsert"),
        }
    }

    /// C2 — Checkpoint record exposes the embedded redo_lsn.
    #[test]
    fn test_decode_checkpoint() {
        let mut payload = [0u8; 8];
        payload.copy_from_slice(&123_456u64.to_le_bytes());
        let rec = WalRecord {
            lsn: 99,
            record_type: WalRecordType::Checkpoint,
            flags: 0,
            payload: payload.to_vec(),
        };
        let event = decode_wal_record(&rec, 2);
        match event {
            CdcEvent::Checkpoint {
                redo_lsn,
                lsn,
                shard,
            } => {
                assert_eq!(redo_lsn, 123_456);
                assert_eq!(lsn, 99);
                assert_eq!(shard, 2);
            }
            _ => panic!("expected Checkpoint"),
        }
    }

    /// C2 — GraphTemporal decoding round-trips entity + system_from.
    #[test]
    fn test_decode_graph_temporal() {
        let payload = encode_graph_temporal(42, true, i64::MAX, 1234);
        let rec = WalRecord {
            lsn: 3,
            record_type: WalRecordType::GraphTemporal,
            flags: 0,
            payload,
        };
        let event = decode_wal_record(&rec, 0);
        match event {
            CdcEvent::GraphTemporal {
                entity_id,
                is_node,
                valid_to,
                system_from,
                ..
            } => {
                assert_eq!(entity_id, 42);
                assert!(is_node);
                assert_eq!(valid_to, i64::MAX);
                assert_eq!(system_from, 1234);
            }
            _ => panic!("expected GraphTemporal"),
        }
    }
}
