//! Test harness for snapshot-epoch correctness (moon#1216, moon#1217).
//!
//! Drives a [`SnapshotState`] exactly the way the shard event loop does
//! (`shard::persistence_tick::advance_snapshot_segment`): writes reach the
//! keyspace through `command::dispatch` — which captures pre-images into the
//! per-shard `snapshot_cow` queue — and every tick first drains that queue
//! into the state, then serializes one segment, then publishes the new
//! cursor. [`Epoch::finish`] reads the published file back as RAW records
//! (every record, in file order, duplicates included), because the normal
//! loader resolves duplicates by letting the last one win and would hide
//! exactly the class of bug these tests exist for.

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use bytes::Bytes;

use super::*;
use crate::persistence::snapshot_cow;
use crate::protocol::Frame;

/// One record of a snapshot file: `(db, key, entry)`.
pub(super) type Record = (usize, Bytes, Entry);

/// A snapshot epoch in flight, advanced like the event loop advances it.
pub(super) struct Epoch {
    pub(super) state: Option<SnapshotState>,
    path: PathBuf,
    _dir: tempfile::TempDir,
}

impl Epoch {
    /// Begin an epoch over `dbs`: what `handle_pending_snapshot` does
    /// (construct the state, arm the off-loop capture).
    pub(super) fn begin(dbs: &[Database]) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("shard-0.rrdshard");
        let state = SnapshotState::new(0, 1, dbs, path.clone());
        snapshot_cow::disarm();
        snapshot_cow::arm_with_layout(state.segment_counts().to_vec());
        Epoch {
            state: Some(state),
            path,
            _dir: dir,
        }
    }

    /// One persistence tick exactly as `advance_snapshot_segment` runs it:
    /// drain the off-loop captures, serialize one tick's budget of the
    /// current database, publish the cursor. True once every db is written.
    pub(super) fn tick(&mut self, dbs: &[Database]) -> bool {
        self.step(dbs, true)
    }

    /// [`Self::tick`] that serializes exactly ONE segment, for tests that
    /// need to stop the walk at a precise point.
    pub(super) fn tick_one(&mut self, dbs: &[Database]) -> bool {
        self.step(dbs, false)
    }

    fn step(&mut self, dbs: &[Database], budgeted: bool) -> bool {
        let Some(state) = self.state.as_mut() else {
            return true;
        };
        snapshot_cow::drain_pending_for_test(state);
        if state.is_complete() || state.aborted().is_some() {
            return true;
        }
        let done = if budgeted {
            let db = state.current_db_index();
            state.advance_budgeted_db(&dbs[db])
        } else {
            state.advance_one_segment(dbs)
        };
        snapshot_cow::note_progress(state.current_db_index(), state.cursor());
        done
    }

    /// Run the epoch to completion, publish the file, disarm, and return its
    /// raw records.
    pub(super) fn finish(mut self, dbs: &[Database]) -> Vec<Record> {
        self.try_finish(dbs).expect("finalize")
    }

    /// [`Self::finish`], reporting a failed (e.g. aborted) snapshot instead
    /// of panicking. The file is read back only if it was published.
    /// The epoch's directory lives as long as `self`.
    pub(super) fn try_finish(&mut self, dbs: &[Database]) -> Result<Vec<Record>, String> {
        while !self.tick(dbs) {}
        let mut state = self.state.take().expect("epoch already finished");
        let outcome = state.finalize().map_err(|e| e.to_string());
        snapshot_cow::disarm();
        outcome.map(|()| read_records(&self.path))
    }

    /// Where this epoch publishes its file.
    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

/// Run `cmd args..` against `dbs[db]` through the generic dispatch path — the
/// path every local write (and, after `cow_intercept`, every routed one)
/// takes, including its snapshot pre-image capture.
pub(super) fn run(dbs: &mut [Database], db: usize, parts: &[&[u8]]) -> Frame {
    let args: Vec<Frame> = parts[1..]
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
        .collect();
    let mut selected = db;
    let db_count = dbs.len();
    match crate::command::dispatch(&mut dbs[db], parts[0], &args, &mut selected, db_count) {
        crate::command::DispatchResult::Response(f) => f,
        crate::command::DispatchResult::Quit(f) => f,
    }
}

/// Every record of a published snapshot file, in file order.
pub(super) fn read_records(path: &Path) -> Vec<Record> {
    let data = std::fs::read(path).expect("read snapshot");
    let (payload, _crc) = data.split_at(data.len() - 4);
    let mut cur = Cursor::new(payload);
    let mut magic = [0u8; 8];
    cur.read_exact(&mut magic).expect("magic");
    assert_eq!(&magic, SHARD_RDB_MAGIC);
    let mut version = [0u8; 1];
    cur.read_exact(&mut version).expect("version");
    assert_eq!(
        version[0], SHARD_RDB_VERSION,
        "harness reads the current format"
    );
    // shard_id(2) + epoch(8) + last_lsn(8) + created_at(8)
    let mut rest = [0u8; 26];
    cur.read_exact(&mut rest).expect("preamble");
    let mut out = Vec::new();
    let mut db = 0usize;
    loop {
        let mut tag = [0u8; 1];
        cur.read_exact(&mut tag).expect("tag");
        match tag[0] {
            EOF_MARKER => break,
            DB_SELECTOR => {
                let mut b = [0u8; 1];
                cur.read_exact(&mut b).expect("db index");
                db = b[0] as usize;
            }
            SEGMENT_BLOCK_MARKER => {
                let _seg = rdb::read_u32(&mut cur).expect("segment index");
                let count = rdb::read_u32(&mut cur).expect("entry count");
                for _ in 0..count {
                    let mut t = [0u8; 1];
                    cur.read_exact(&mut t).expect("type tag");
                    let (key, entry) = rdb::read_entry(&mut cur, t[0], true).expect("entry");
                    out.push((db, key, entry));
                }
                let _ = rdb::read_u32(&mut cur).expect("segment crc");
            }
            other => panic!("unexpected tag {other:#04x}"),
        }
    }
    out
}

/// A string value as bytes, for comparing records against a model.
pub(super) fn string_of(entry: &Entry) -> Vec<u8> {
    match entry.value.as_redis_value() {
        crate::storage::compact_value::RedisValueRef::String(s) => s.to_vec(),
        _ => panic!("expected a string value"),
    }
}

/// The string keyspace of `dbs` right now: `(db, key) -> value`. Taken at
/// epoch start, it is exactly what the snapshot must contain.
pub(super) fn string_keyspace(dbs: &[Database]) -> BTreeMap<(usize, Vec<u8>), Vec<u8>> {
    let mut out = BTreeMap::new();
    for (i, db) in dbs.iter().enumerate() {
        for (k, e) in db.data().iter() {
            out.insert((i, k.as_bytes().to_vec()), string_of(e));
        }
    }
    out
}

/// How a snapshot's records differ from the epoch-start keyspace.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Divergence {
    /// Epoch-start keys with no record.
    pub(super) missing: usize,
    /// Records for keys that did not exist at epoch start.
    pub(super) extra: usize,
    /// Keys with more than one record.
    pub(super) duplicated: usize,
    /// Keys whose (single) record holds a value other than the epoch-start one.
    pub(super) wrong_value: usize,
}

/// Compare a string-valued snapshot with the epoch-start keyspace.
pub(super) fn diverge(
    expected: &BTreeMap<(usize, Vec<u8>), Vec<u8>>,
    records: &[Record],
) -> Divergence {
    let mut seen: BTreeMap<(usize, Vec<u8>), Vec<Vec<u8>>> = BTreeMap::new();
    for (db, key, entry) in records {
        seen.entry((*db, key.to_vec()))
            .or_default()
            .push(string_of(entry));
    }
    let mut d = Divergence::default();
    for (k, v) in expected {
        match seen.get(k) {
            None => d.missing += 1,
            Some(vals) if vals.len() > 1 => d.duplicated += 1,
            Some(vals) if vals[0] != *v => d.wrong_value += 1,
            Some(_) => {}
        }
    }
    for (k, vals) in &seen {
        if !expected.contains_key(k) {
            d.extra += 1;
            if vals.len() > 1 {
                d.duplicated += 1;
            }
        }
    }
    d
}

/// splitmix64 — a deterministic, dependency-free generator for the
/// randomized interleavings (seeds are printed on failure).
pub(super) struct Rng(pub(super) u64);

impl Rng {
    pub(super) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub(super) fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}
