//! moon#1215: every path that takes a slot out of the cold index records it
//! in the dead-slot ledger, and only unlinking the file forgets it.

use bytes::Bytes;

use super::cold_index::{ColdIndex, ColdLocation};
use crate::persistence::kv_page::ValueType;

fn loc(file_id: u64, slot: u16) -> ColdLocation {
    ColdLocation {
        file_id,
        page_idx: 0,
        slot_idx: slot,
        ttl_ms: None,
        value_type: ValueType::String,
    }
}

fn dead_keys(ci: &ColdIndex) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = ci.dead_slots().keys().map(|k| k.to_vec()).collect();
    keys.sort();
    keys
}

#[test]
fn remove_records_the_slot() {
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"a"), loc(1, 0));
    ci.insert(Bytes::from_static(b"b"), loc(1, 1));
    assert!(ci.dead_slots().is_empty(), "a live entry is not dead");
    assert!(ci.remove(b"a"));
    assert!(!ci.remove(b"a"), "second remove finds nothing");
    assert_eq!(dead_keys(&ci), vec![b"a".to_vec()]);
    assert!(ci.dead_slots().file_has_dead_slots(1));
}

#[test]
fn a_respill_to_another_file_records_the_old_slot_but_not_a_same_file_move() {
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"k"), loc(1, 0));
    ci.insert(Bytes::from_static(b"k"), loc(1, 5));
    assert!(
        ci.dead_slots().is_empty(),
        "same file: the key is still alive in it, one DEL covers any later death"
    );
    ci.insert(Bytes::from_static(b"k"), loc(2, 0));
    assert!(ci.dead_slots().file_has_dead_slots(1));
    assert!(!ci.dead_slots().file_has_dead_slots(2));
}

#[test]
fn clear_all_records_every_slot_and_older_copy() {
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"a"), loc(1, 0));
    ci.insert(Bytes::from_static(b"b"), loc(2, 0));
    ci.clear_all();
    assert_eq!(ci.len(), 0);
    assert_eq!(dead_keys(&ci), vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn the_sweeps_record_what_they_take() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"shadowed"), loc(1, 0));
    ci.insert(
        Bytes::from_static(b"expired"),
        ColdLocation {
            ttl_ms: Some(10),
            ..loc(1, 1)
        },
    );
    ci.insert(Bytes::from_static(b"live"), loc(1, 2));
    ci.sweep_known_orphans(vec![Bytes::from_static(b"shadowed")], dir.path(), None)
        .unwrap();
    ci.sweep_expired(1_000, dir.path(), None, 16).unwrap();
    assert_eq!(
        dead_keys(&ci),
        vec![b"expired".to_vec(), b"shadowed".to_vec()]
    );
    assert_eq!(ci.len(), 1);
}

#[test]
fn only_unlinking_the_file_forgets_its_slots() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::write(data.join("heap-000001.mpf"), b"x").unwrap();
    std::fs::write(data.join("heap-000002.mpf"), b"x").unwrap();
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"gone"), loc(1, 0));
    ci.insert(Bytes::from_static(b"dead"), loc(2, 0));
    ci.insert(Bytes::from_static(b"live"), loc(2, 1));
    ci.remove(b"gone");
    ci.remove(b"dead");
    // File 1 has no referrer left and is unlinked; file 2 still backs `live`.
    ci.sweep_known_orphans(Vec::new(), dir.path(), None)
        .unwrap();
    assert!(!data.join("heap-000001.mpf").exists());
    assert!(data.join("heap-000002.mpf").exists());
    assert!(!ci.dead_slots().file_has_dead_slots(1), "file 1 is gone");
    assert_eq!(
        dead_keys(&ci),
        vec![b"dead".to_vec()],
        "file 2 still holds `dead`"
    );
}

#[test]
fn a_file_that_cannot_be_unlinked_keeps_its_slots() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    // `heap-000001.mpf` is a non-empty DIRECTORY: `remove_file` fails with
    // something other than NotFound, so the file is re-queued, not gone.
    std::fs::create_dir_all(data.join("heap-000001.mpf").join("x")).unwrap();
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"k"), loc(1, 0));
    ci.remove(b"k");
    ci.sweep_known_orphans(Vec::new(), dir.path(), None)
        .unwrap();
    assert!(ci.has_pending_unlink(), "re-queued for a later sweep");
    assert!(ci.dead_slots().file_has_dead_slots(1));
}

#[test]
fn a_file_already_gone_forgets_its_slots() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("data")).unwrap();
    let mut ci = ColdIndex::new();
    ci.insert(Bytes::from_static(b"k"), loc(1, 0));
    ci.remove(b"k");
    ci.sweep_known_orphans(Vec::new(), dir.path(), None)
        .unwrap();
    assert!(ci.dead_slots().is_empty());
}

#[test]
fn a_ghost_slot_is_recorded_on_request() {
    let mut ci = ColdIndex::new();
    ci.note_dead_slot(9, Bytes::from_static(b"ghost"));
    assert!(ci.dead_slots().file_has_dead_slots(9));
    assert_eq!(ci.len(), 0, "a ghost is never an entry");
}

#[test]
fn merge_carries_the_ledger() {
    let mut a = ColdIndex::new();
    let mut b = ColdIndex::new();
    b.insert(Bytes::from_static(b"k"), loc(3, 0));
    b.remove(b"k");
    a.merge(b);
    assert!(a.dead_slots().file_has_dead_slots(3));
}
