//! FT.SEARCH … SESSION filtering against the live session set (moon#1196).

use bytes::Bytes;
use smallvec::SmallVec;

use super::session;
use crate::storage::db::Database;
use crate::vector::keymap::BucketedKeyMap;
use crate::vector::types::{SearchResult, VectorId};

fn results(n: u64) -> SmallVec<[SearchResult; 32]> {
    (0..n)
        .map(|i| SearchResult {
            id: VectorId(i as u32),
            distance: i as f32 * 0.1,
            key_hash: 1_000 + i,
        })
        .collect()
}

fn keymap(n: u64) -> BucketedKeyMap<Bytes> {
    let mut m = BucketedKeyMap::new();
    for i in 0..n {
        m.insert(1_000 + i, Bytes::from(format!("doc:{i}")));
    }
    m
}

/// A session set holding `doc:{seen...}` plus `filler` unrelated members.
fn db_with_session(key: &[u8], seen: &[u64], filler: usize) -> Database {
    let mut db = Database::new();
    let (members, tree) = db.get_or_create_sorted_set(key).unwrap();
    let mut add = |m: Bytes, score: f64| {
        members.insert(m.clone(), score);
        tree.insert(ordered_float::OrderedFloat(score), m);
    };
    for &s in seen {
        add(Bytes::from(format!("doc:{s}")), 1.0);
    }
    for f in 0..filler {
        add(Bytes::from(format!("old:{f}")), 2.0 + f as f64);
    }
    db
}

#[test]
fn in_db_filter_matches_snapshot_filter() {
    let db = db_with_session(b"sess", &[1, 3, 4], 50);
    let res = results(8);
    let k2k = keymap(8);
    let got = session::filter_session_results_in_db(&res, &db, b"sess", &k2k);
    // Reference: the pre-moon#1196 snapshot path.
    let mut snapshot = std::collections::HashMap::new();
    for s in [1u64, 3, 4] {
        snapshot.insert(Bytes::from(format!("doc:{s}")), 1.0);
    }
    let want = session::filter_session_results(&res, &snapshot, &k2k);
    assert_eq!(
        got.iter().map(|r| r.key_hash).collect::<Vec<_>>(),
        want.iter().map(|r| r.key_hash).collect::<Vec<_>>()
    );
    assert_eq!(got.len(), 5);
}

#[test]
fn missing_or_wrong_type_session_filters_nothing() {
    let mut db = Database::new();
    let res = results(4);
    let k2k = keymap(4);
    assert_eq!(
        session::filter_session_results_in_db(&res, &db, b"nope", &k2k).len(),
        4
    );
    // A string under the session key: WRONGTYPE → no filtering (as before).
    db.set_string(b"str", Bytes::from_static(b"v"));
    assert_eq!(
        session::filter_session_results_in_db(&res, &db, b"str", &k2k).len(),
        4
    );
}

#[test]
fn session_filter_cost_does_not_grow_with_session_size() {
    // moon#1196 wall-time red test: HEAD cloned the session's whole member
    // map on every FT.SEARCH … SESSION. With 200K recorded members, 50
    // queries cloned 10M map entries (seconds in this debug build); probing
    // the live set costs k lookups per query regardless of its size.
    let db = db_with_session(b"big", &[2], 200_000);
    let res = results(10);
    let k2k = keymap(10);
    let t = std::time::Instant::now();
    let mut kept = 0usize;
    for _ in 0..50 {
        kept += session::filter_session_results_in_db(&res, &db, b"big", &k2k).len();
    }
    let el = t.elapsed();
    assert_eq!(kept, 50 * 9);
    assert!(
        el < std::time::Duration::from_millis(250),
        "50 session filters over a 200K-member session took {el:?}"
    );
}

/// moon#1226: the in-place filter matches the borrowed one and never copies
/// — the survivors stay in the caller's buffer (spilled past 32 inline
/// results here, so a copy would show up as a different heap pointer), and
/// the no-session path leaves the buffer untouched.
#[test]
fn retain_unseen_filters_in_place_without_copying() {
    let db = db_with_session(b"sess", &[1, 3, 4, 40], 20);
    let k2k = keymap(48);
    let res = results(48);
    let want = session::filter_session_results_in_db(&res, &db, b"sess", &k2k);

    let mut inplace = results(48);
    assert!(inplace.spilled());
    let buf = inplace.as_ptr();
    session::retain_unseen_in_db(&mut inplace, &db, b"sess", &k2k);
    assert_eq!(inplace.as_ptr(), buf, "filtered in the caller's buffer");
    assert_eq!(
        inplace.iter().map(|r| r.key_hash).collect::<Vec<_>>(),
        want.iter().map(|r| r.key_hash).collect::<Vec<_>>()
    );
    assert_eq!(inplace.len(), 44);

    // No session key: nothing filtered, nothing moved.
    let mut untouched = results(48);
    let buf = untouched.as_ptr();
    session::retain_unseen_in_db(&mut untouched, &db, b"no-such-session", &k2k);
    assert_eq!(untouched.as_ptr(), buf);
    assert_eq!(untouched.len(), 48);
}
