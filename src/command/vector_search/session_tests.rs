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

/// moon#1226 / moon#1242: the in-place filter drops exactly the results the
/// session has seen and never copies — for the dense path's `SmallVec`
/// (spilled past 32 inline results, so a copy would show up as a different
/// heap pointer) and the hybrid / sparse paths' `Vec` alike — and the
/// no-session path leaves the buffer untouched. The expected survivors come
/// from a plain filter written here, not from the session module.
#[test]
fn retain_unseen_filters_in_place_without_copying() {
    let seen = [1u64, 3, 4, 40];
    let db = db_with_session(b"sess", &seen, 20);
    let k2k = keymap(48);
    // `results(48)` holds `doc:{i}` at key_hash 1000 + i.
    let want: Vec<u64> = (0..48u64)
        .filter(|i| !seen.contains(i))
        .map(|i| 1_000 + i)
        .collect();
    let hashes = |r: &[SearchResult]| r.iter().map(|r| r.key_hash).collect::<Vec<_>>();

    let mut inplace = results(48);
    assert!(inplace.spilled());
    let buf = inplace.as_ptr();
    session::retain_unseen_in_db(&mut inplace, &db, b"sess", &k2k);
    assert_eq!(
        inplace.as_ptr(),
        buf,
        "SmallVec: filtered in the caller's buffer"
    );
    assert_eq!(hashes(&inplace), want, "SmallVec");

    let mut fused: Vec<SearchResult> = results(48).into_vec();
    let buf = fused.as_ptr();
    session::retain_unseen_in_db(&mut fused, &db, b"sess", &k2k);
    assert_eq!(fused.as_ptr(), buf, "Vec: filtered in the caller's buffer");
    assert_eq!(hashes(&fused), want, "Vec");

    // The borrowed form answers the same survivors.
    let borrowed = session::filter_session_results_in_db(&results(48), &db, b"sess", &k2k);
    assert_eq!(hashes(&borrowed), want, "borrowed");

    // No session key: nothing filtered, nothing moved.
    let mut untouched = results(48);
    let buf = untouched.as_ptr();
    session::retain_unseen_in_db(&mut untouched, &db, b"no-such-session", &k2k);
    assert_eq!(untouched.as_ptr(), buf);
    assert_eq!(untouched.len(), 48);
}

/// moon#1242: FT.SEARCH … SESSION on the sparse-only and hybrid (KNN +
/// SPARSE) paths, which filter and record the fused `Vec` in place. The
/// first search returns both documents and records them; the same search
/// again in the same session returns none, and another session is
/// unaffected.
#[test]
fn sparse_and_hybrid_session_searches_skip_seen_documents() {
    use super::tests::{
        METRICS_LOCK, encode_sparse_blob, ft_create_hybrid_args, insert_hybrid_doc,
    };
    use crate::command::vector_search::{ft_create, ft_search};
    use crate::protocol::Frame;

    let _lock = METRICS_LOCK.write();
    crate::vector::distance::init();
    let mut store = crate::vector::store::VectorStore::new();
    let created = ft_create(
        &mut store,
        &mut crate::text::store::TextStore::new(),
        &ft_create_hybrid_args(),
        0,
    );
    assert!(matches!(created, Frame::SimpleString(_)), "{created:?}");
    insert_hybrid_doc(&mut store, b"doc:1", &[1.0, 0.0, 0.0, 0.0], &[(0, 1.0)]);
    insert_hybrid_doc(&mut store, b"doc:2", &[0.0, 1.0, 0.0, 0.0], &[(0, 0.5)]);

    let bulk = |b: &[u8]| Frame::BulkString(Bytes::copy_from_slice(b));
    let dense: Vec<u8> = [0.9f32, 0.1, 0.0, 0.0]
        .iter()
        .flat_map(|f| f.to_le_bytes())
        .collect();
    let sparse = encode_sparse_blob(&[(0, 1.0)]);
    let args = |query: &[u8], session: &[u8]| -> Vec<Frame> {
        vec![
            bulk(b"hybridx"),
            bulk(query),
            bulk(b"SPARSE"),
            bulk(b"@sparse_vec"),
            bulk(b"$sq"),
            bulk(b"PARAMS"),
            bulk(b"4"),
            bulk(b"q"),
            bulk(&dense),
            bulk(b"sq"),
            bulk(&sparse),
            bulk(b"SESSION"),
            bulk(session),
        ]
    };
    let docs = |reply: &Frame| -> Vec<Bytes> {
        let Frame::Array(items) = reply else {
            panic!("expected an array, got {reply:?}");
        };
        let mut keys: Vec<Bytes> = items
            .iter()
            .filter_map(|f| match f {
                Frame::BulkString(b) if b.starts_with(b"doc:") => Some(b.clone()),
                _ => None,
            })
            .collect();
        keys.sort();
        keys
    };
    let both = vec![Bytes::from_static(b"doc:1"), Bytes::from_static(b"doc:2")];

    let mut db = Database::new();
    for (path, query) in [
        ("sparse", &b"*"[..]),
        ("hybrid", &b"*=>[KNN 10 @vec $q]"[..]),
    ] {
        let session = format!("sess:{path}");
        let first = ft_search(
            &mut store,
            &args(query, session.as_bytes()),
            Some(&mut db),
            None,
            0,
            0,
        );
        assert_eq!(docs(&first), both, "{path}: first search");
        let again = ft_search(
            &mut store,
            &args(query, session.as_bytes()),
            Some(&mut db),
            None,
            0,
            0,
        );
        assert_eq!(
            docs(&again),
            Vec::<Bytes>::new(),
            "{path}: both already seen"
        );
        let other = ft_search(
            &mut store,
            &args(query, format!("{session}:other").as_bytes()),
            Some(&mut db),
            None,
            0,
            0,
        );
        assert_eq!(docs(&other), both, "{path}: another session");
    }
}
