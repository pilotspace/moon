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

/// Keys per `DEL` record in a generation head, and the size of one
/// [`ColdDeleteChunk`]: bounds one record's size (one RESP array per 512
/// keys) without a record per key.
pub const HEAD_DEL_BATCH: usize = 512;

/// One batch of keys a new AOF generation must delete right after its
/// `MOON.COLDCUT` (moon#1215), all in database `db`: at most
/// [`HEAD_DEL_BATCH`] keys, written as ONE `DEL` record. The fold ships its
/// deletes as a stream of these (`FoldChunk::ColdDeletes`), never as one
/// list, and the writer writes each as it goes (PR #1233 review).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdDeleteChunk {
    pub db: usize,
    pub keys: Vec<Bytes>,
}

/// The cold deletes of one fold, as computed at the fold instant by
/// `aof::fold_stream`: each key has a slot on disk in a listed spill file
/// (so a rebuild would index it and the cut would authorize it) and is not
/// alive at the fold instant (not in the base, not cold, not in flight).
///
/// Chunks come in database order. A key may be listed more than once when it
/// is dead in several files; duplicates inside one chunk are written once,
/// across chunks they are harmless (`DEL` of an absent key is a no-op).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ColdDeletes {
    pub chunks: Vec<ColdDeleteChunk>,
}

impl ColdDeletes {
    /// Total listed keys across every chunk (duplicates included).
    pub fn len(&self) -> usize {
        self.chunks.iter().map(|c| c.keys.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.iter().all(|c| c.keys.is_empty())
    }

    /// Every listed key with its database, in chunk order.
    pub fn keys(&self) -> impl Iterator<Item = (usize, &Bytes)> {
        self.chunks
            .iter()
            .flat_map(|c| c.keys.iter().map(move |k| (c.db, k)))
    }
}

/// Stream the head of a new AOF generation into `out`: `MOON.COLDCUT
/// <watermark>`, then for every chunk of `deletes` a `SELECT <db>` when the
/// database changes and ONE `DEL key…` record, ending selected on db 0 — the
/// writer and every replay reader start an incr's records at db 0, so a head
/// that left another db selected would replay the generation's first db-0
/// records into it. Returns how many `DEL` arguments it wrote.
///
/// Each chunk is serialized on its own and dropped once written, so the
/// head never exists in memory as one buffer (PR #1233 review: a large
/// ledger used to build a multi-megabyte `Vec` here).
///
/// Only records every moon binary already replays: `SELECT` and `DEL` (which
/// tombstones the cold plane through the replay gate, moon#257), so an older
/// binary reading this generation keeps the deletes too. `framed` selects
/// the per-shard `[lsn=0][len][RESP]` encoding for every record.
pub fn write_generation_head_to(
    out: &mut impl std::io::Write,
    watermark: u64,
    deletes: ColdDeletes,
    framed: bool,
) -> std::io::Result<usize> {
    let mut put = |resp: &[u8]| -> std::io::Result<()> {
        if framed {
            out.write_all(&frame_unoffset(resp))
        } else {
            out.write_all(resp)
        }
    };
    put(&serialize_cold_cut(watermark))?;
    let mut selected = 0usize;
    let mut written = 0usize;
    for chunk in deletes.chunks {
        let ColdDeleteChunk { db, mut keys } = chunk;
        if keys.is_empty() {
            continue;
        }
        if db != selected {
            put(&serialize_select(db))?;
            selected = db;
        }
        keys.sort_unstable();
        keys.dedup();
        for batch in keys.chunks(HEAD_DEL_BATCH) {
            let mut parts = crate::protocol::FrameVec::with_capacity(batch.len() + 1);
            parts.push(Frame::BulkString(Bytes::from_static(b"DEL")));
            for key in batch {
                parts.push(Frame::BulkString(key.clone()));
            }
            put(&serialize_command(&Frame::Array(parts)))?;
            written += batch.len();
        }
    }
    if selected != 0 {
        put(&serialize_select(0))?;
    }
    Ok(written)
}

/// [`write_generation_head_to`] into a buffer — tests only; production
/// streams straight into the incr.
#[cfg(test)]
pub fn generation_head(watermark: u64, deletes: &ColdDeletes, framed: bool) -> Vec<u8> {
    let mut out = Vec::new();
    #[allow(clippy::unwrap_used)] // writing into a Vec cannot fail
    write_generation_head_to(&mut out, watermark, deletes.clone(), framed).unwrap();
    out
}

fn serialize_select(db: usize) -> Bytes {
    let mut n = itoa::Buffer::new();
    serialize_command(&Frame::Array(crate::framevec![
        Frame::BulkString(Bytes::from_static(b"SELECT")),
        Frame::BulkString(Bytes::copy_from_slice(n.format(db).as_bytes())),
    ]))
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

/// moon#914: open a legacy single-file AOF generation with its
/// `MOON.COLDCUT <watermark>` head — the layout `runtime-tokio` with
/// `--shards 1` uses, which has no `AofManifest` and therefore never runs
/// `AofManifest::seed_cold_cut`.
///
/// Writes the head only when the file is absent or EMPTY, i.e. when no
/// record of this generation exists yet, so it is always the first record.
/// A non-empty file already has its head (written here or by the rewrite
/// that produced it) or is a legacy generation that predates the cut; a head
/// appended to its tail would gate only the records after it, so it is left
/// untouched. The write is fsynced before returning — like `seed_cold_cut` —
/// so the head is durable before any client write can be acknowledged.
///
/// Returns whether the head was written.
pub fn seed_cold_cut_if_fresh(path: &std::path::Path, watermark: u64) -> std::io::Result<bool> {
    use std::io::Write;
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() > 0 => return Ok(false),
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(&serialize_cold_cut(watermark))?;
    file.sync_data()?;
    Ok(true)
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
    fn cold_plane_records_are_recognised_case_insensitively() {
        let mut dbs = vec![Database::new()];
        assert!(replay_cold_plane_record(&mut dbs, b"moon.spilled", &[], 0));
        assert!(replay_cold_plane_record(&mut dbs, b"MOON.COLDCUT", &[], 0));
        assert!(!replay_cold_plane_record(&mut dbs, b"SET", &[], 0));
        assert!(!replay_cold_plane_record(&mut dbs, b"MOON.SPILL", &[], 0));
    }

    /// moon#914: the head goes into an absent or empty legacy AOF as its
    /// FIRST record, and never into a file that already holds records.
    #[test]
    fn seed_cold_cut_if_fresh_writes_only_the_first_record() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("appendonly.aof");

        assert!(seed_cold_cut_if_fresh(&path, 9).unwrap(), "absent → seeded");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            serialize_cold_cut(9).as_ref()
        );
        assert!(
            !seed_cold_cut_if_fresh(&path, 12).unwrap(),
            "a second boot must not append another head"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            serialize_cold_cut(9).as_ref()
        );

        let empty = dir.path().join("empty.aof");
        std::fs::write(&empty, b"").unwrap();
        assert!(seed_cold_cut_if_fresh(&empty, 3).unwrap(), "empty → seeded");

        // A legacy generation: records but no head. Appending one at the
        // tail would gate only what follows it — leave the file alone.
        let legacy = dir.path().join("legacy.aof");
        let set = serialize_command(&Frame::Array(crate::framevec![
            Frame::BulkString(Bytes::from_static(b"SET")),
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::BulkString(Bytes::from_static(b"v")),
        ]));
        std::fs::write(&legacy, &set).unwrap();
        assert!(!seed_cold_cut_if_fresh(&legacy, 3).unwrap());
        assert_eq!(std::fs::read(&legacy).unwrap(), set.as_ref());
    }

    /// moon#1215: the head writes each chunk as one DEL (duplicates inside a
    /// chunk once), SELECTs a database only when it changes, and hands the
    /// generation back on db 0; the framed form carries the same records.
    #[test]
    fn generation_head_dedupes_batches_and_ends_on_db_0() {
        let key = |i: usize| Bytes::from(format!("k{i:04}"));
        let mut first: Vec<Bytes> = (0..HEAD_DEL_BATCH).map(key).collect();
        first.push(key(0));
        let deletes = ColdDeletes {
            chunks: vec![
                ColdDeleteChunk {
                    db: 0,
                    keys: vec![Bytes::from_static(b"z"), Bytes::from_static(b"z")],
                },
                ColdDeleteChunk { db: 2, keys: first },
                ColdDeleteChunk {
                    db: 2,
                    keys: (HEAD_DEL_BATCH..HEAD_DEL_BATCH + 3).map(key).collect(),
                },
            ],
        };
        assert_eq!(deletes.len(), HEAD_DEL_BATCH + 6, "duplicates are counted");
        let head = generation_head(9, &deletes, false);
        let mut want = serialize_cold_cut(9).to_vec();
        let del = |keys: &[Bytes]| {
            let mut parts = crate::protocol::FrameVec::new();
            parts.push(Frame::BulkString(Bytes::from_static(b"DEL")));
            for k in keys {
                parts.push(Frame::BulkString(k.clone()));
            }
            serialize_command(&Frame::Array(parts)).to_vec()
        };
        want.extend(del(&[Bytes::from_static(b"z")]));
        want.extend_from_slice(&serialize_select(2));
        let all: Vec<Bytes> = (0..HEAD_DEL_BATCH + 3).map(key).collect();
        want.extend(del(&all[..HEAD_DEL_BATCH]));
        want.extend(del(&all[HEAD_DEL_BATCH..]));
        want.extend_from_slice(&serialize_select(0));
        assert_eq!(head, want);

        let framed = generation_head(9, &deletes, true);
        let mut records = 0usize;
        let mut at = 0usize;
        while at < framed.len() {
            assert_eq!(&framed[at..at + 8], &0u64.to_le_bytes(), "lsn 0");
            let len = u32::from_le_bytes(framed[at + 8..at + 12].try_into().unwrap()) as usize;
            at += 12 + len;
            records += 1;
        }
        assert_eq!(at, framed.len());
        assert_eq!(records, 6, "COLDCUT, DEL z, SELECT 2, DEL x2, SELECT 0");
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
