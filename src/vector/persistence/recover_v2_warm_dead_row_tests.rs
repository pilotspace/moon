use super::*;

/// A warm row killed by an install-time tombstone is no evidence that its
/// segment serves the key, even when the keymap names that very row's
/// `(key_hash, global_id)`.
///
/// How it arises: at boot, recovery kills a duplicate of a key's current copy
/// (same key_hash, same global_id, in a second segment) with an install-time
/// tombstone, while the keymap keeps naming that copy. When both segments go
/// WARM, the duplicate's dead flag is written into its `mvcc.mpf`. On the next
/// boot, if the segment holding the dead duplicate was registered first and
/// its row were counted as live, it would claim the key, and the segment
/// holding the real live row would then tombstone it as "already served":
/// the key is served by neither.
#[test]
fn a_dead_duplicate_registered_first_does_not_take_the_key_from_its_live_copy() {
    const TARGET: usize = 3;
    let fx = build_warm_fixture(12, None, false);
    let (live_id, live_dir) = fx.warm[0].clone();

    // The duplicate: the same segment, with TARGET's row install-dead.
    let dup_id = live_id + 100;
    let dup_dir = live_dir.with_file_name(format!("segment-{dup_id}"));
    copy_dir_all(&live_dir, &dup_dir);
    let mut mvcc = crate::vector::persistence::warm_search::read_mvcc_payload(&live_dir).unwrap();
    let target_hash = xxhash_rust::xxh64::xxh64(format!("doc:{TARGET}").as_bytes(), 0);
    let mut killed = 0;
    for row in mvcc.chunks_exact_mut(32) {
        if row[8..16] == target_hash.to_le_bytes() {
            row[24..32].copy_from_slice(&1u64.to_le_bytes());
            killed += 1;
        }
    }
    assert_eq!(
        killed, 1,
        "sanity: the duplicate holds exactly one row for the key"
    );
    crate::vector::persistence::warm_segment::write_mvcc_mpf(
        &dup_dir.join("mvcc.mpf"),
        dup_id,
        &mvcc,
    )
    .unwrap();
    assert!(
        crate::vector::persistence::warm_search::peek_mvcc_rows(&dup_dir)
            .unwrap()
            .iter()
            .any(|r| r.key_hash == target_hash && r.dead),
        "sanity: the duplicate's row reads back as install-dead"
    );

    // The duplicate is registered FIRST.
    let mut fresh = reboot(&fx, vec![(dup_id, dup_dir), (live_id, live_dir)]);

    for i in 0..fx.n {
        let want = gid_of(&fresh, &format!("doc:{i}"));
        assert_eq!(
            top1(&mut fresh, &fx, i),
            Some(want),
            "doc:{i} is not served after the reboot"
        );
    }
}
