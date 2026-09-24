//! moon#1174 §3: an owned read of a listpack entry copies it ONCE.
//!
//! `decode_entry_at` materializes a string entry into a `Vec`, and
//! `ListpackEntry::to_bytes` then CLONED that `Vec` -- two mallocs, one free
//! and two memcpys per element on every LRANGE, LINDEX, HGETALL, HKEYS, HVALS,
//! SMEMBERS and whole-container promotion. HKEYS/HVALS also materialized the
//! other half of every pair, and HEXISTS/HSTRLEN the value they only test or
//! measure.
//!
//! The instrument is `OWNED_DECODES` (test-only, thread-local): the read paths
//! below must borrow until their one terminal copy, so they leave it unmoved.
//! `into_bytes` is pinned by pointer identity: the `Vec` must BECOME the
//! `Bytes`, which no amount of allocator noise can fake.

use bytes::Bytes;

use super::{Listpack, ListpackEntry, ListpackRef, owned_decodes};
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{HashRef, ListRef};

fn bs(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

/// Every encoding width, integers included, so `to_bytes` on a ref and on the
/// owned entry are compared on all nine arms.
fn widths() -> Vec<Vec<u8>> {
    vec![
        b"0".to_vec(),
        b"127".to_vec(),
        b"-1".to_vec(),
        b"4095".to_vec(),
        b"32767".to_vec(),
        b"8388607".to_vec(),
        b"2147483647".to_vec(),
        b"9223372036854775807".to_vec(),
        b"-9223372036854775808".to_vec(),
        b"".to_vec(),
        b"007".to_vec(),
        b"+5".to_vec(),
        vec![b'x'; 63],
        vec![b'y'; 64],
        vec![b'z'; 4096],
    ]
}

fn build(values: &[Vec<u8>]) -> Listpack {
    let mut lp = Listpack::new();
    for v in values {
        lp.push_back(v);
    }
    lp
}

#[test]
fn into_bytes_takes_the_vec_over_without_copying() {
    let v = b"a string entry long enough to be unmistakable".to_vec();
    let ptr = v.as_ptr();
    let b = ListpackEntry::String(v).into_bytes();
    assert_eq!(b.as_ptr(), ptr, "into_bytes copied the string entry");
    assert_eq!(
        ListpackEntry::Integer(-42).into_bytes(),
        Bytes::from_static(b"-42")
    );
}

/// The borrowed renderings must equal the owned ones on every width -- the
/// guard on moving readers from `ListpackEntry` to `ListpackRef`.
#[test]
fn ref_to_bytes_and_byte_len_agree_with_the_owned_entry() {
    let values = widths();
    let lp = build(&values);
    for ((r, owned), want) in lp.iter_refs().zip(lp.iter()).zip(&values) {
        assert_eq!(r.to_bytes().as_ref(), want.as_slice());
        assert_eq!(owned.to_bytes().as_ref(), want.as_slice());
        assert_eq!(owned.clone().into_bytes().as_ref(), want.as_slice());
        assert_eq!(r.byte_len(), want.len());
    }
    assert_eq!(ListpackRef::Integer(i64::MIN).byte_len(), 20);
}

/// The whole-container conversions (promotion, AOF rewrite, RDB) and the
/// list/hash read helpers copy each entry once: no owned decode at all.
#[test]
fn owned_read_paths_do_not_decode_into_a_vec_first() {
    let values = widths();
    let lp = build(&values);
    let pairs = build(&values[..values.len() - 1]); // even count: 7 pairs

    let mark = owned_decodes();
    let deque = lp.to_vec_deque();
    let vec = lp.to_vec();
    let set = lp.to_set_value();
    let map = pairs.to_hash_map();
    let list = ListRef::Listpack(&lp);
    let all = list.iter_bytes();
    let one = list.get(values.len() - 1);
    let range = list.range(2, 9);
    let href = HashRef::Listpack(&pairs);
    let entries = href.entries();
    let got = href.get_field(&values[0]);
    let mut keys = Vec::new();
    href.for_each_field(|k| keys.push(k));
    let mut vals = Vec::new();
    href.for_each_value(|v| vals.push(v));
    let exists = href.contains_field(&values[2]);
    let len = href.field_len(&values[12]);
    assert_eq!(
        owned_decodes() - mark,
        0,
        "an owned read decoded a string entry into a Vec before copying it"
    );

    // ...and they answer exactly what they answered before.
    let want: Vec<Bytes> = values.iter().map(|v| Bytes::copy_from_slice(v)).collect();
    assert_eq!(deque.iter().cloned().collect::<Vec<_>>(), want);
    assert_eq!(vec, want);
    assert_eq!(all, want);
    assert_eq!(set.len(), want.len());
    assert_eq!(one.as_ref(), want.last());
    assert_eq!(range, want[2..=9].to_vec());
    assert_eq!(map.len(), 7);
    assert_eq!(entries.len(), 7);
    assert_eq!(got.as_deref(), Some(values[1].as_slice()));
    assert_eq!(
        keys,
        (0..7).map(|i| want[2 * i].clone()).collect::<Vec<_>>()
    );
    assert_eq!(
        vals,
        (0..7).map(|i| want[2 * i + 1].clone()).collect::<Vec<_>>()
    );
    assert!(exists);
    assert_eq!(len, Some(values[13].len()));
}

/// The field-only / length-only helpers agree with `get_field` / `entries`
/// on every encoding, TTL-filtered ones included.
#[test]
fn hash_helpers_agree_with_get_field_on_every_encoding() {
    use std::collections::HashMap;
    let fields: Vec<(Bytes, Bytes)> = (0..6)
        .map(|i| {
            (
                Bytes::from(format!("f{i}")),
                Bytes::from(if i % 2 == 0 {
                    format!("{}", i * 1000)
                } else {
                    format!("value-{i}")
                }),
            )
        })
        .collect();
    let map: HashMap<Bytes, Bytes> = fields.iter().cloned().collect();
    let mut lp = Listpack::new();
    for (f, v) in &fields {
        lp.push_back(f);
        lp.push_back(v);
    }
    // f1 expired at 50, f3 lives until 500; now = 100.
    let ttls: HashMap<Bytes, u64> = [(Bytes::from("f1"), 50u64), (Bytes::from("f3"), 500)]
        .into_iter()
        .collect();
    let refs = vec![
        ("Map", HashRef::Map(&map)),
        ("Listpack", HashRef::Listpack(&lp)),
        ("Owned", HashRef::Owned(map.clone())),
        (
            "WithTtl",
            HashRef::WithTtl {
                fields: &map,
                ttls: &ttls,
                now_ms: 100,
                min_expiry_ms: 50,
            },
        ),
        (
            "WithTtl fast path",
            HashRef::WithTtl {
                fields: &map,
                ttls: &ttls,
                now_ms: 10,
                min_expiry_ms: 50,
            },
        ),
        (
            "OwnedWithTtl",
            HashRef::OwnedWithTtl {
                fields: map.clone(),
                ttls: ttls.clone(),
                now_ms: 100,
                min_expiry_ms: 50,
            },
        ),
    ];
    for (name, href) in &refs {
        for probe in ["f0", "f1", "f3", "f5", "nope"] {
            let want = href.get_field(probe.as_bytes());
            assert_eq!(
                href.contains_field(probe.as_bytes()),
                want.is_some(),
                "{name}: contains_field({probe})"
            );
            assert_eq!(
                href.field_len(probe.as_bytes()),
                want.as_ref().map(Bytes::len),
                "{name}: field_len({probe})"
            );
        }
        let mut entries = href.entries();
        entries.sort();
        let mut keys = Vec::new();
        href.for_each_field(|k| keys.push(k));
        keys.sort();
        let mut vals = Vec::new();
        href.for_each_value(|v| vals.push(v));
        vals.sort();
        let mut want_keys: Vec<Bytes> = entries.iter().map(|(k, _)| k.clone()).collect();
        want_keys.sort();
        let mut want_vals: Vec<Bytes> = entries.iter().map(|(_, v)| v.clone()).collect();
        want_vals.sort();
        assert_eq!(keys, want_keys, "{name}: for_each_field");
        assert_eq!(vals, want_vals, "{name}: for_each_value");
        assert!(
            href.len_hint() >= href.len(),
            "{name}: len_hint is an upper bound"
        );
    }
}

/// End to end through the command handlers, on a listpack hash: same
/// replies, no owned decode, and the encoding survives.
#[test]
fn hkeys_hvals_hexists_hstrlen_read_in_place() {
    use crate::command::hash;
    let mut db = Database::new();
    hash::hset(
        &mut db,
        &[
            bs(b"h"),
            bs(b"name"),
            bs(b"moon"),
            bs(b"n"),
            bs(b"12345"),
            bs(b"z"),
            bs(b"007"),
        ],
    );
    let mark = owned_decodes();
    let keys = hash::hkeys_readonly(&db, &[bs(b"h")], 0);
    let vals = hash::hvals_readonly(&db, &[bs(b"h")], 0);
    let ex = hash::hexists_readonly(&db, &[bs(b"h"), bs(b"n")], 0);
    let nx = hash::hexists_readonly(&db, &[bs(b"h"), bs(b"nope")], 0);
    let sl = hash::hstrlen_readonly(&db, &[bs(b"h"), bs(b"n")], 0);
    let sl_str = hash::hstrlen_readonly(&db, &[bs(b"h"), bs(b"z")], 0);
    let sl_none = hash::hstrlen_readonly(&db, &[bs(b"h"), bs(b"nope")], 0);
    assert_eq!(owned_decodes() - mark, 0);
    assert_eq!(
        keys,
        Frame::Array(crate::framevec![bs(b"name"), bs(b"n"), bs(b"z")])
    );
    assert_eq!(
        vals,
        Frame::Array(crate::framevec![bs(b"moon"), bs(b"12345"), bs(b"007")])
    );
    assert_eq!(ex, Frame::Integer(1));
    assert_eq!(nx, Frame::Integer(0));
    assert_eq!(
        sl,
        Frame::Integer(5),
        "an integer-encoded value measures its spelling"
    );
    assert_eq!(sl_str, Frame::Integer(3));
    assert_eq!(sl_none, Frame::Integer(0));
    // The mutable-path twins answer the same.
    assert_eq!(hash::hkeys(&mut db, &[bs(b"h")]), keys);
    assert_eq!(hash::hvals(&mut db, &[bs(b"h")]), vals);
    assert_eq!(hash::hexists(&mut db, &[bs(b"h"), bs(b"n")]), ex);
    assert_eq!(hash::hstrlen(&mut db, &[bs(b"h"), bs(b"n")]), sl);
    let enc = crate::command::key::object(&mut db, &[bs(b"ENCODING"), bs(b"h")]);
    assert_eq!(enc, Frame::BulkString(Bytes::from_static(b"listpack")));
}
