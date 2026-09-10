//! Replay-only AOF records that cut the cold plane against the command log
//! (moon#902). See `storage::db::cold_replay_gate` for the model.
//!
//! Two records, both intercepted by the replay engine before dispatch and
//! never served to a client (a live `MOON.COLDCUT` is an unknown command):
//!
//! * `MOON.COLDCUT <watermark>` — first record of every AOF generation. Cold
//!   files with `file_id < watermark` were sealed before this generation's
//!   base was cut. Written by `AofManifest::seed_cold_cut` on
//!   `initialize*`, and by every rewrite as the head of the new incr.
//! * `MOON.SPILLED <file_id> key [key …]` — appended (AOF only, never to
//!   the replication stream: file ids are shard-local) when a spill
//!   completion publishes those keys into the cold index.

use bytes::Bytes;

use crate::persistence::aof::serialize_command;
use crate::protocol::Frame;
use crate::storage::Database;

/// `MOON.COLDCUT <watermark>` — opens a replay generation.
pub const COLD_CUT: &[u8] = b"MOON.COLDCUT";
/// `MOON.SPILLED <file_id> key…` — keys now served by a cold file.
pub const SPILLED: &[u8] = b"MOON.SPILLED";

/// RESP bytes of `MOON.COLDCUT <watermark>`.
pub fn serialize_cold_cut(watermark: u64) -> Bytes {
    let mut n = itoa::Buffer::new();
    serialize_command(&Frame::Array(crate::framevec![
        Frame::BulkString(Bytes::from_static(COLD_CUT)),
        Frame::BulkString(Bytes::copy_from_slice(n.format(watermark).as_bytes())),
    ]))
}

/// RESP bytes of `MOON.SPILLED <file_id> key…`.
pub fn serialize_spilled(file_id: u64, keys: &[Bytes]) -> Bytes {
    let mut n = itoa::Buffer::new();
    let mut parts = crate::protocol::FrameVec::with_capacity(keys.len() + 2);
    parts.push(Frame::BulkString(Bytes::from_static(SPILLED)));
    parts.push(Frame::BulkString(Bytes::copy_from_slice(
        n.format(file_id).as_bytes(),
    )));
    for key in keys {
        parts.push(Frame::BulkString(key.clone()));
    }
    serialize_command(&Frame::Array(parts))
}

/// Per-shard framed incr encoding (`[u64 lsn LE][u32 len LE][RESP]`) of a
/// record that carries no replication offset — the same `lsn = 0` the
/// writer's own `SELECT` injection uses.
pub fn frame_unoffset(resp: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + resp.len());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&(resp.len() as u32).to_le_bytes());
    out.extend_from_slice(resp);
    out
}

#[inline]
fn frame_u64(f: &Frame) -> Option<u64> {
    match f {
        Frame::BulkString(b) => std::str::from_utf8(b).ok()?.parse::<u64>().ok(),
        Frame::Integer(n) => u64::try_from(*n).ok(),
        _ => None,
    }
}

/// Apply a cold-plane record during replay. Returns `true` when `cmd` was
/// one (whether or not it was well-formed — a malformed record is logged and
/// skipped, never handed to dispatch), `false` for every other command.
pub fn replay_cold_plane_record(
    databases: &mut [Database],
    cmd: &[u8],
    args: &[Frame],
    selected_db: usize,
) -> bool {
    if cmd.eq_ignore_ascii_case(COLD_CUT) {
        match args.first().and_then(frame_u64) {
            Some(watermark) => {
                for db in databases.iter_mut() {
                    db.install_replay_cold_gate(watermark);
                }
            }
            None => tracing::warn!(
                "AOF replay: malformed MOON.COLDCUT ({} args) — skipped; this generation \
                 replays ungated",
                args.len()
            ),
        }
        return true;
    }
    if cmd.eq_ignore_ascii_case(SPILLED) {
        let Some(file_id) = args.first().and_then(frame_u64) else {
            tracing::warn!("AOF replay: malformed MOON.SPILLED (no file id) — skipped");
            return true;
        };
        let Some(db) = databases.get_mut(selected_db) else {
            tracing::warn!(
                "AOF replay: MOON.SPILLED for db {} beyond the configured database count — skipped",
                selected_db
            );
            return true;
        };
        let keys = args[1..].iter().filter_map(|f| match f {
            Frame::BulkString(b) => Some(b.as_ref()),
            _ => None,
        });
        let dropped = db.replay_cold_spilled(file_id, keys);
        if dropped > 0 {
            tracing::debug!(
                file_id,
                dropped,
                "AOF replay: MOON.SPILLED demoted replay-built hot copies to their cold entries"
            );
        }
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::kv_page::ValueType;
    use crate::protocol::{ParseConfig, parse};
    use crate::storage::entry::Entry;
    use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};

    fn parse_one(bytes: &Bytes) -> (Vec<u8>, Vec<Frame>) {
        let mut buf = bytes::BytesMut::from(bytes.as_ref());
        let frame = parse::parse(&mut buf, &ParseConfig::default())
            .unwrap()
            .unwrap();
        match frame {
            Frame::Array(arr) => {
                let name = match &arr[0] {
                    Frame::BulkString(b) => b.to_vec(),
                    _ => panic!("name"),
                };
                (name, arr[1..].to_vec())
            }
            _ => panic!("array"),
        }
    }

    fn loc(file_id: u64) -> ColdLocation {
        ColdLocation {
            file_id,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: ValueType::String,
        }
    }

    #[test]
    fn cold_cut_roundtrip_installs_the_gate_on_every_db() {
        let (name, args) = parse_one(&serialize_cold_cut(42));
        assert_eq!(name, COLD_CUT);
        let mut dbs = vec![Database::new(), Database::new()];
        assert!(replay_cold_plane_record(&mut dbs, &name, &args, 1));
        for db in &dbs {
            assert_eq!(db.replay_cold_gate().unwrap().pre_generation_below(), 42);
        }
    }

    #[test]
    fn spilled_roundtrip_drops_the_hot_copy_and_authorizes_the_file() {
        let (name, args) = parse_one(&serialize_spilled(
            7,
            &[Bytes::from_static(b"k1"), Bytes::from_static(b"k2")],
        ));
        assert_eq!(name, SPILLED);
        let mut db = Database::new();
        let mut ci = ColdIndex::new();
        ci.insert(Bytes::from_static(b"k1"), loc(7));
        ci.insert(Bytes::from_static(b"k2"), loc(8));
        db.cold_index = Some(ci);
        db.install_replay_cold_gate(1);
        db.set(b"k1", Entry::new_string(Bytes::from_static(b"v")));
        db.set(b"k2", Entry::new_string(Bytes::from_static(b"v")));
        let mut dbs = vec![Database::new(), db];
        assert!(replay_cold_plane_record(&mut dbs, &name, &args, 1));
        assert!(!dbs[1].is_hot(b"k1"), "k1 is served by file 7 from here on");
        assert!(
            dbs[1].is_hot(b"k2"),
            "k2's cold entry is a later file; untouched"
        );
        assert!(dbs[1].replay_cold_gate().unwrap().is_authorized(7));
        assert!(!dbs[1].replay_cold_gate().unwrap().is_authorized(8));
        assert!(!dbs[0].is_hot(b"k1") && !dbs[0].replay_cold_gate_active());
    }

    #[test]
    fn other_commands_are_not_intercepted_and_malformed_records_are_swallowed() {
        let mut dbs = vec![Database::new()];
        assert!(!replay_cold_plane_record(&mut dbs, b"SET", &[], 0));
        assert!(replay_cold_plane_record(&mut dbs, b"moon.coldcut", &[], 0));
        assert!(!dbs[0].replay_cold_gate_active());
        assert!(replay_cold_plane_record(&mut dbs, b"MOON.SPILLED", &[], 0));
        assert!(replay_cold_plane_record(
            &mut dbs,
            b"MOON.SPILLED",
            &[Frame::BulkString(Bytes::from_static(b"3"))],
            9
        ));
    }

    #[test]
    fn framed_form_carries_lsn_zero() {
        let resp = serialize_cold_cut(5);
        let framed = frame_unoffset(&resp);
        assert_eq!(&framed[..8], &0u64.to_le_bytes());
        assert_eq!(&framed[8..12], &(resp.len() as u32).to_le_bytes());
        assert_eq!(&framed[12..], resp.as_ref());
    }
}
