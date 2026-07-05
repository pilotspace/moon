//! VEC-1 (vector deep review 2026-07-05): updating an existing key via HSET
//! must tombstone the previously indexed vector — both while it still lives in
//! the mutable segment AND after it has been compacted into an immutable
//! segment. Without the tombstone the index accumulates stale duplicates:
//! FT.SEARCH returns the same doc twice (once with the old embedding) and
//! num_docs inflates monotonically under update churn.

use bytes::Bytes;

use moon::command::vector_search::{ft_create, ft_search};
use moon::protocol::Frame;
use moon::shard::spsc_handler::auto_index_hset_public;
use moon::text::store::TextStore;
use moon::vector::distance;
use moon::vector::store::VectorStore;

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::from(s.to_vec()))
}

fn f32_blob(v: &[f32]) -> Frame {
    let mut b = Vec::with_capacity(v.len() * 4);
    for x in v {
        b.extend_from_slice(&x.to_le_bytes());
    }
    Frame::BulkString(Bytes::from(b))
}

fn ft_create_args(name: &str, dim: u32) -> Vec<Frame> {
    vec![
        bulk(name.as_bytes()),
        bulk(b"ON"),
        bulk(b"HASH"),
        bulk(b"PREFIX"),
        bulk(b"1"),
        bulk(b"doc:"),
        bulk(b"SCHEMA"),
        bulk(b"vec"),
        bulk(b"VECTOR"),
        bulk(b"HNSW"),
        bulk(b"6"),
        bulk(b"TYPE"),
        bulk(b"FLOAT32"),
        bulk(b"DIM"),
        bulk(dim.to_string().as_bytes()),
        bulk(b"DISTANCE_METRIC"),
        bulk(b"L2"),
    ]
}

fn hset(vs: &mut VectorStore, ts: &mut TextStore, key: &[u8], vec: &[f32]) {
    let args = vec![bulk(key), bulk(b"vec"), f32_blob(vec)];
    auto_index_hset_public(vs, ts, key, &args);
}

/// KNN k=10 search; returns the total-result count (first array element).
fn search_total(vs: &mut VectorStore, index: &str, query: &[f32]) -> i64 {
    let mut qb = Vec::with_capacity(query.len() * 4);
    for x in query {
        qb.extend_from_slice(&x.to_le_bytes());
    }
    let args = vec![
        bulk(index.as_bytes()),
        bulk(b"*=>[KNN 10 @vec $query]"),
        bulk(b"PARAMS"),
        bulk(b"2"),
        bulk(b"query"),
        Frame::BulkString(Bytes::from(qb)),
    ];
    match ft_search(vs, &args, None, None, 0) {
        Frame::Array(items) => match items.first() {
            Some(Frame::Integer(n)) => *n,
            other => panic!("expected Integer total, got {other:?}"),
        },
        other => panic!("expected Array response, got {other:?}"),
    }
}

const DIM: usize = 8;

#[test]
fn hset_update_in_mutable_segment_does_not_duplicate() {
    distance::init();
    let mut vs = VectorStore::new();
    let mut ts = TextStore::new();
    let out = ft_create(&mut vs, &mut ts, &ft_create_args("upd_mut", DIM as u32));
    assert!(!matches!(out, Frame::Error(_)), "ft_create failed: {out:?}");

    let v1: Vec<f32> = (0..DIM).map(|i| if i == 0 { 1.0 } else { 0.0 }).collect();
    let v2: Vec<f32> = (0..DIM).map(|i| if i == 1 { 1.0 } else { 0.0 }).collect();

    hset(&mut vs, &mut ts, b"doc:1", &v1);
    hset(&mut vs, &mut ts, b"doc:1", &v2); // update, still in mutable

    let total = search_total(&mut vs, "upd_mut", &v2);
    assert_eq!(
        total, 1,
        "updated key must appear exactly once (stale mutable duplicate returned)"
    );
}

#[test]
fn hset_update_after_compaction_tombstones_immutable_copy() {
    distance::init();
    let mut vs = VectorStore::new();
    let mut ts = TextStore::new();
    let out = ft_create(&mut vs, &mut ts, &ft_create_args("upd_imm", DIM as u32));
    assert!(!matches!(out, Frame::Error(_)), "ft_create failed: {out:?}");

    let v1: Vec<f32> = (0..DIM).map(|i| if i == 0 { 1.0 } else { 0.0 }).collect();
    let v2: Vec<f32> = (0..DIM).map(|i| if i == 1 { 1.0 } else { 0.0 }).collect();

    hset(&mut vs, &mut ts, b"doc:1", &v1);
    // Push a few fillers so the compacted segment is non-trivial.
    for i in 2..6 {
        let vf: Vec<f32> = (0..DIM).map(|j| (i * j) as f32 * 0.1).collect();
        hset(&mut vs, &mut ts, format!("doc:{i}").as_bytes(), &vf);
    }
    vs.force_compact_index(b"upd_imm")
        .expect("force_compact_index");

    // Update doc:1 AFTER its old copy was compacted into the immutable segment.
    hset(&mut vs, &mut ts, b"doc:1", &v2);

    let total = search_total(&mut vs, "upd_imm", &v2);
    assert_eq!(
        total, 5,
        "5 live docs expected; a stale immutable copy of doc:1 was resurrected"
    );
}
