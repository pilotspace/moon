//! moon#1206: the backlen is written in redis's byte order, so backward walks
//! over wide entries land on entry boundaries and the bytes match redis.
//!
//! The encoder used to write the 7-bit groups low-first (`129` as `81 01`)
//! while [`decode_backlen`] reads them the way redis's `lpDecodeBacklen`
//! does (`01 81`): every backward step over an entry of 128 B or more read a
//! wrong length and landed inside the payload.

use super::*;

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Listpacks of a two-entry hash (`f0` + value) captured from redis-server
/// 7.0.15 `DUMP` (`rdbcompression no`, `hash-max-listpack-value 65536`; the
/// payload is `RDB_TYPE_HASH_LISTPACK` and these are its listpack bytes).
const REDIS_P127: &str = "8e000000020082663003e07f707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070707070700181ff";

/// A three-pair hash `{f0: "x"*200, f1: "12345", f2: "y"*130}` from the
/// same redis: a wide entry at both ends and an integer between them.
const REDIS_MIXED: &str = "69010000060082663003e0c8787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787878787801ca82663103f139300382663203e082797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979797979790184ff";

fn built(values: &[&[u8]]) -> Listpack {
    let mut lp = Listpack::new();
    for v in values {
        lp.push_back(v);
    }
    lp
}

#[test]
fn listpacks_are_byte_identical_to_redis() {
    let p = [b'p'; 127];
    assert_eq!(hex(&built(&[b"f0", &p]).data), REDIS_P127);
    let (x, y) = ([b'x'; 200], [b'y'; 130]);
    assert_eq!(
        hex(&built(&[b"f0", &x, b"f1", b"12345", b"f2", &y]).data),
        REDIS_MIXED
    );
}

/// The large seams, against `(length, first 12 bytes, last 8 bytes)` of the
/// same redis listpacks — the middle is the value itself. 16378 bytes makes an
/// entry of exactly 16383, which redis gives a THREE-byte backlen (`00 ff ff`):
/// its two-byte form holds `< 16383`, not `<= 16383`.
#[test]
fn wide_seams_match_redis() {
    let all: Vec<u8> = (0u8..=255).collect();
    let cases: [(&[u8], usize, &str, &str); 5] = [
        (&all, 271, "0f010000020082663003e100", "fbfcfdfeff0282ff"),
        (
            &[b'q'; 4095],
            4110,
            "0e100000020082663003efff",
            "71717171712081ff",
        ),
        (
            &[b'r'; 4096],
            4114,
            "12100000020082663003f000",
            "72727272722085ff",
        ),
        (
            &[b's'; 16378],
            16397,
            "0d400000020082663003f0fa",
            "7373737300ffffff",
        ),
        (
            &[b's'; 16377],
            16395,
            "0b400000020082663003f0f9",
            "73737373737ffeff",
        ),
    ];
    for (value, len, head, tail) in cases {
        let lp = built(&[b"f0", value]);
        let got = hex(&lp.data);
        assert_eq!(
            lp.data.len(),
            len,
            "{}-byte value: total length",
            value.len()
        );
        assert_eq!(&got[..24], head, "{}-byte value: head", value.len());
        assert_eq!(
            &got[got.len() - 16..],
            tail,
            "{}-byte value: tail",
            value.len()
        );
    }
}

/// Every width, decoded back from the tail exactly as a backward walk reads
/// it — including the redis seams (16383, 2097151, 268435455) and the widest
/// `usize` the encoder can be handed.
#[test]
fn every_backlen_decodes_back_to_its_length_and_width() {
    let mut probes = vec![
        0usize,
        1,
        126,
        127,
        128,
        129,
        255,
        256,
        4097,
        16382,
        16383,
        16384,
        2097150,
        2097151,
        2097152,
        268435454,
        268435455,
        268435456,
        u32::MAX as usize,
        usize::MAX >> 1,
        usize::MAX,
    ];
    let mut x = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..2000 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        probes.push((x >> (x % 60)) as usize);
    }
    for l in probes {
        let mut buf = [0u8; LP_MAX_BACKLEN];
        let n = encode_backlen_into(l, &mut buf);
        assert_eq!(n, backlen_size(l), "width at {l}");
        assert_eq!(decode_backlen(&buf[..n], n), (l, n), "decode(encode({l}))");
        if n > 1 {
            assert_eq!(
                buf[0] & 0x80,
                0,
                "{l}: the top group carries no continuation bit"
            );
            assert!(
                buf[1..n].iter().all(|b| b & 0x80 != 0),
                "{l}: lower groups do"
            );
        }
    }
}

/// The backward walk the bug broke: `iter_rev` over entries of every width,
/// against the forward walk reversed.
#[test]
fn iter_rev_is_correct_on_wide_entries() {
    let values: Vec<Vec<u8>> = vec![
        b"a".to_vec(),
        vec![b'b'; 126], // entry 127: the last one-byte backlen
        vec![b'c'; 127], // entry 129
        b"12345".to_vec(),
        vec![b'd'; 200],
        vec![b'e'; 4095],
        b"-7".to_vec(),
        vec![b'f'; 4096],
        vec![b'g'; 16377], // entry 16382
        vec![b'h'; 16378], // entry 16383: redis's three-byte seam
        vec![b'i'; 16379],
        b"z".to_vec(),
    ];
    let refs: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
    let lp = built(&refs);
    let forward: Vec<ListpackEntry> = lp.iter().collect();
    let mut backward: Vec<ListpackEntry> = lp.iter_rev().collect();
    backward.reverse();
    assert_eq!(
        backward.len(),
        values.len(),
        "the backward walk lost entries"
    );
    assert_eq!(
        backward, forward,
        "a backward step landed off an entry boundary"
    );
    for (entry, want) in forward.iter().zip(&values) {
        assert_eq!(&entry.as_bytes(), want);
    }
}

/// Why no normalize pass exists: no persisted form carries listpack BYTES.
/// A listpack-encoded value is DUMPed (and written to the RDB by the same
/// `write_typed_value`) as the plain element type, and rebuilt through
/// `push_*` on load, so the old byte order can never be read back.
#[test]
fn persisted_forms_carry_elements_not_listpack_bytes() {
    use crate::storage::compact_value::CompactValue;
    use crate::storage::entry::{Entry, RedisValue};
    let lp = built(&[b"f", &[b'w'; 200]]);
    for (want_type, value) in [
        (4u8, RedisValue::HashListpack(lp.clone())), // RDB_TYPE_HASH
        (1u8, RedisValue::ListListpack(lp.clone())), // RDB_TYPE_LIST
        (2u8, RedisValue::SetListpack(lp)),          // RDB_TYPE_SET
    ] {
        let mut entry = Entry::new_string(Bytes::new());
        entry.value = CompactValue::from_redis_value(value);
        let payload = crate::persistence::dump_payload::encode(&entry);
        assert_eq!(
            payload[0], want_type,
            "a listpack value is persisted as its elements"
        );
        let back = crate::persistence::dump_payload::decode(&payload).expect("round trip");
        let elems: Vec<Vec<u8>> = match back.value.as_redis_value() {
            crate::storage::compact_value::RedisValueRef::HashListpack(l)
            | crate::storage::compact_value::RedisValueRef::ListListpack(l)
            | crate::storage::compact_value::RedisValueRef::SetListpack(l) => {
                let mut rev: Vec<Vec<u8>> = l.iter_rev().map(|e| e.as_bytes()).collect();
                rev.reverse();
                rev
            }
            crate::storage::compact_value::RedisValueRef::Hash(h) => h
                .iter()
                .flat_map(|(k, v)| [k.to_vec(), v.to_vec()])
                .collect(),
            crate::storage::compact_value::RedisValueRef::List(l) => {
                l.iter().map(|e| e.to_vec()).collect()
            }
            crate::storage::compact_value::RedisValueRef::Set(s) => {
                let mut m: Vec<Vec<u8>> = s.iter().map(|e| e.to_vec()).collect();
                m.sort();
                m
            }
            other => panic!("unexpected {}", other.encoding_name()),
        };
        assert!(
            elems.iter().any(|e| e.len() == 200),
            "the wide element survives the round trip"
        );
    }
}
