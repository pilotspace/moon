//! The encoding matrix: what `OBJECT ENCODING` answers for every container
//! type at every size around the compact-encoding thresholds, built three
//! ways. Pinned cell by cell in `encoding_matrix.golden`.
//!
//! The three builds are the three consultation sites of the encoding policy
//! (moon#896):
//!
//! * `bulk`   — ONE command carrying all `n` items. Exercises the ENTRY gate
//!   (may this batch go through the listpack path?) and then the upgrade
//!   check.
//! * `incr`   — `n` commands of one item each. Exercises the UPGRADE check
//!   alone: no batch is ever large enough for the entry gate to refuse.
//! * `decode` — the full form holding `n` items, through the RESTART path
//!   (`encode_value_body` -> `decode_value_body_compacting`, which is what
//!   `compact_after_decode` sits behind).
//!
//! Two uses:
//!
//! 1. **Behaviour-neutrality proof.** The golden was captured on `f7c83769`,
//!    BEFORE the `EncodingLimits` authority existed. The refactor that
//!    introduced the authority had to reproduce every cell — including the
//!    cells that were WRONG (`bulk` != `incr` for hash and zset at 65..128
//!    items, which is moon#896 itself). A refactor that quietly fixed a bug
//!    would fail here, which is the point: a fix is a separate, visible
//!    commit whose diff to the golden IS its before/after table.
//! 2. **Regression net.** Any later change to a threshold, a gate or the
//!    decode path moves cells, and the golden diff says exactly which.
//!
//! To regenerate after an INTENDED change:
//!
//! ```text
//! MOON_ENCODING_MATRIX_PRINT=1 cargo test --lib encoding_matrix -- --nocapture
//! ```
//!
//! and paste the table between the `BEGIN`/`END` markers into the golden.
//! Element sizes: 8 B (well inside), 64 B (exactly the value threshold), 65 B
//! (one past it). Sizes bracket every boundary at ±1 and include the argv
//! 128 -> 130 flip (64 vs 65 pairs) moon#896 was measured at.

use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;

use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::bptree::BPTree;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::SetValue;
use crate::storage::value_codec::{
    HashTtlTrailer, decode_value_body_compacting, encode_value_body,
};

const SIZES: &[usize] = &[
    1, 2, 63, 64, 65, 66, 100, 127, 128, 129, 130, 200, 256, 257, 300,
];
const ELEM_LENS: &[usize] = &[8, 64, 65];

const GOLDEN: &str = include_str!("encoding_matrix.golden");

fn bs(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

/// What `OBJECT ENCODING` replies, through the real handler.
fn encoding_of(db: &mut Database, key: &[u8]) -> String {
    match crate::command::key::object(db, &[bs(b"ENCODING"), bs(key)]) {
        Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        Frame::Null => "<missing>".to_string(),
        other => panic!("OBJECT ENCODING did not reply a bulk string: {other:?}"),
    }
}

/// A distinct, non-numeric element of exactly `len` bytes (`len >= 8`).
fn elem(i: usize, len: usize) -> Vec<u8> {
    let mut v = format!("m{i:06}").into_bytes();
    v.resize(len, b'x');
    v
}

fn field(i: usize) -> Vec<u8> {
    format!("f{i:06}").into_bytes()
}

/// The restart path: encode the full form, decode with re-derivation.
fn decoded_encoding(v: &RedisValueRef) -> String {
    let mut buf = Vec::new();
    encode_value_body(v, &mut buf).expect("encode");
    let vt = crate::storage::value_codec::value_type_of(v);
    let mut cursor = Cursor::new(buf.as_slice());
    decode_value_body_compacting(&mut cursor, vt, HashTtlTrailer::Absent)
        .expect("decode")
        .encoding_name()
        .to_string()
}

fn hash_row(n: usize, len: usize) -> [String; 3] {
    let vals: Vec<Vec<u8>> = (0..n).map(|i| elem(i, len)).collect();
    let fields: Vec<Vec<u8>> = (0..n).map(field).collect();

    let mut db = Database::new();
    let mut args = vec![bs(b"h")];
    for i in 0..n {
        args.push(bs(&fields[i]));
        args.push(bs(&vals[i]));
    }
    crate::command::hash::hset(&mut db, &args);
    let bulk = encoding_of(&mut db, b"h");

    let mut db = Database::new();
    for i in 0..n {
        crate::command::hash::hset(&mut db, &[bs(b"h"), bs(&fields[i]), bs(&vals[i])]);
    }
    let incr = encoding_of(&mut db, b"h");

    let map: HashMap<Bytes, Bytes> = (0..n)
        .map(|i| (Bytes::from(fields[i].clone()), Bytes::from(vals[i].clone())))
        .collect();
    let decode = decoded_encoding(&RedisValueRef::Hash(&map));
    [bulk, incr, decode]
}

fn set_row(n: usize, len: usize) -> [String; 3] {
    let members: Vec<Vec<u8>> = (0..n).map(|i| elem(i, len)).collect();

    let mut db = Database::new();
    let mut args = vec![bs(b"s")];
    args.extend(members.iter().map(|m| bs(m)));
    crate::command::set::sadd(&mut db, &args);
    let bulk = encoding_of(&mut db, b"s");

    let mut db = Database::new();
    for m in &members {
        crate::command::set::sadd(&mut db, &[bs(b"s"), bs(m)]);
    }
    let incr = encoding_of(&mut db, b"s");

    let set: SetValue = members.iter().map(|m| Bytes::from(m.clone())).collect();
    let decode = decoded_encoding(&RedisValueRef::Set(&set));
    [bulk, incr, decode]
}

fn zset_row(n: usize, len: usize) -> [String; 3] {
    let members: Vec<Vec<u8>> = (0..n).map(|i| elem(i, len)).collect();
    let scores: Vec<Vec<u8>> = (0..n).map(|i| i.to_string().into_bytes()).collect();

    let mut db = Database::new();
    let mut args = vec![bs(b"z")];
    for i in 0..n {
        args.push(bs(&scores[i]));
        args.push(bs(&members[i]));
    }
    crate::command::sorted_set::zadd(&mut db, &args);
    let bulk = encoding_of(&mut db, b"z");

    let mut db = Database::new();
    for i in 0..n {
        crate::command::sorted_set::zadd(&mut db, &[bs(b"z"), bs(&scores[i]), bs(&members[i])]);
    }
    let incr = encoding_of(&mut db, b"z");

    let mut map = HashMap::new();
    let mut tree = BPTree::new();
    for (i, m) in members.iter().enumerate() {
        let m = Bytes::from(m.clone());
        map.insert(m.clone(), i as f64);
        tree.insert(OrderedFloat(i as f64), m);
    }
    let decode = decoded_encoding(&RedisValueRef::SortedSetBPTree {
        members: &map,
        tree: &tree,
    });
    [bulk, incr, decode]
}

fn list_row(n: usize, len: usize) -> [String; 3] {
    let elems: Vec<Vec<u8>> = (0..n).map(|i| elem(i, len)).collect();

    let mut db = Database::new();
    let mut args = vec![bs(b"l")];
    args.extend(elems.iter().map(|e| bs(e)));
    crate::command::list::rpush(&mut db, &args);
    let bulk = encoding_of(&mut db, b"l");

    let mut db = Database::new();
    for e in &elems {
        crate::command::list::rpush(&mut db, &[bs(b"l"), bs(e)]);
    }
    let incr = encoding_of(&mut db, b"l");

    let list: VecDeque<Bytes> = elems.iter().map(|e| Bytes::from(e.clone())).collect();
    let decode = decoded_encoding(&RedisValueRef::List(&list));
    [bulk, incr, decode]
}

/// Every row of the matrix, in golden order: `type n elem bulk incr decode`.
pub(crate) fn render_matrix() -> String {
    let mut out = String::new();
    out.push_str("# type n elem bulk incr decode\n");
    for (ty, f) in [
        ("hash", hash_row as fn(usize, usize) -> [String; 3]),
        ("set", set_row),
        ("zset", zset_row),
        ("list", list_row),
    ] {
        for &n in SIZES {
            for &len in ELEM_LENS {
                let [bulk, incr, decode] = f(n, len);
                out.push_str(&format!("{ty} {n} {len} {bulk} {incr} {decode}\n"));
            }
        }
    }
    out
}

fn golden_table() -> String {
    let begin = GOLDEN.find("# BEGIN\n").expect("golden has a BEGIN marker") + "# BEGIN\n".len();
    let end = GOLDEN.find("# END\n").expect("golden has an END marker");
    GOLDEN[begin..end].to_string()
}

#[test]
fn encoding_matrix_matches_the_golden() {
    let actual = render_matrix();
    if std::env::var_os("MOON_ENCODING_MATRIX_PRINT").is_some() {
        println!("# BEGIN\n{actual}# END");
    }
    let expected = golden_table();
    if actual != expected {
        let mut diff = String::new();
        for (a, e) in actual.lines().zip(expected.lines()) {
            if a != e {
                diff.push_str(&format!("  golden: {e}\n  actual: {a}\n"));
            }
        }
        let (al, el) = (actual.lines().count(), expected.lines().count());
        if al != el {
            diff.push_str(&format!("  row count: golden {el}, actual {al}\n"));
        }
        panic!(
            "the encoding matrix moved. If that is intended, regenerate the golden \
             (see the module docs) and explain the moved cells in the commit.\n{diff}"
        );
    }
}
