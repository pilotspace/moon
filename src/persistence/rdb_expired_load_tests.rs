//! moon#1236: `rdb::load` / `rdb::load_from_bytes` LOAD an entry whose TTL
//! passed after the file was written; they do not skip it.
//!
//! # The rule
//!
//! These loaders read an AOF's base (`aof_manifest::shard_replay` for the
//! multi-part / per-shard layouts, `aof::replay_aof` for the tokio
//! `--shards 1` preamble, `migrate_aof`). An AOF base is the keyspace at the
//! fold instant, and the records after it are replayed onto exactly that
//! state — redis loads an AOF preamble the same way (`rdbLoad` never expires
//! keys under `RDBFLAGS_AOF_PREAMBLE`). A loaded expired entry is hidden by
//! every read and reaped by the active expiry, which logs its `DEL`.
//!
//! # Why skipping lost data
//!
//! A cold key overwritten hot with a TTL keeps its old slot on disk as a
//! shadow (a blind write's `Inserted` arm leaves the cold entry). A rewrite
//! with the TTL still ahead puts the new value in the base. At a restart
//! after the TTL, recovery rebuilds the cold index from the manifest FIRST,
//! then loaded the base with the expired entry skipped — so the key was
//! absent from the hot plane and the shadow read as its value: the OLD,
//! pre-overwrite value came back. Loaded instead, the key is hot and cold
//! when the replay generation closes, and the gated hot-wins resolution
//! (`finish_replay_cold_reconcile`) drops the shadow. The recovery-level
//! proofs are in `storage::tiered::cold_del_rewrite_tests` (moon#1236
//! section); these pin the loader rule itself.

use bytes::Bytes;

use super::{load, load_from_bytes, save, save_to_bytes};
use crate::storage::Database;
use crate::storage::entry::{Entry, current_time_ms};

/// `dying`'s TTL: long enough that the save (which drops an entry already
/// expired when it is written) never outruns it — REVIEW-WS20 F8, the 30 ms
/// it replaced failed whenever the save took longer.
const TTL_MS: u64 = 1_000;

/// Save `live` (alive now) and `dying` (TTL [`TTL_MS`] ahead), then wait
/// until `dying`'s TTL has passed: the image holds an entry that expired
/// after the save, exactly like an AOF base read after a downtime.
fn db_with_a_dying_key() -> (Vec<Database>, u64) {
    let deadline = current_time_ms() + TTL_MS;
    let mut dbs = vec![Database::new()];
    dbs[0].set(b"live", Entry::new_string(Bytes::from_static(b"yes")));
    dbs[0].set(
        b"dying",
        Entry::new_string_with_expiry(Bytes::from_static(b"soon"), deadline),
    );
    (dbs, deadline)
}

fn wait_past(deadline: u64) {
    while current_time_ms() <= deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

fn assert_loaded_and_hidden(db: &mut Database) {
    assert!(
        db.is_hot(b"dying"),
        "an entry that expired after the save must be LOADED (moon#1236), not skipped"
    );
    assert!(
        db.get(b"dying").is_none(),
        "an expired entry reads as absent"
    );
    assert!(db.get(b"live").is_some());
}

#[test]
fn load_keeps_an_entry_that_expired_after_the_save() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("base.rdb");
    let (dbs, deadline) = db_with_a_dying_key();
    save(&dbs, &path).expect("save");
    wait_past(deadline);
    let mut loaded = vec![Database::new()];
    assert_eq!(load(&mut loaded, &path).expect("load"), 2);
    assert_loaded_and_hidden(&mut loaded[0]);
}

#[test]
fn load_from_bytes_keeps_an_entry_that_expired_after_the_save() {
    let (dbs, deadline) = db_with_a_dying_key();
    let bytes = save_to_bytes(&dbs).expect("save");
    wait_past(deadline);
    let mut loaded = vec![Database::new()];
    let (keys, consumed) = load_from_bytes(&mut loaded, &bytes).expect("load");
    assert_eq!((keys, consumed), (2, bytes.len()));
    assert_loaded_and_hidden(&mut loaded[0]);
}

/// The save side is unchanged: an entry already expired when the file is
/// written is not in it (the fold's base filter, moon#1215's `DEL` head).
#[test]
fn save_still_drops_an_entry_already_expired_when_it_is_written() {
    let mut dbs = vec![Database::new()];
    dbs[0].set(
        b"dead",
        Entry::new_string_with_expiry(Bytes::from_static(b"no"), current_time_ms() - 1_000),
    );
    let bytes = save_to_bytes(&dbs).expect("save");
    let mut loaded = vec![Database::new()];
    assert_eq!(load_from_bytes(&mut loaded, &bytes).expect("load").0, 0);
    assert!(!loaded[0].is_hot(b"dead"));
}
