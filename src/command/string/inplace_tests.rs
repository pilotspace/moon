//! moon#1168 — SETBIT / SETRANGE / BITFIELD / APPEND mutate in place.
//!
//! A randomized differential of the four commands against a byte-vector
//! model (replies AND final value), with TTL and LFU preservation and the
//! WATCH version checked on every write; the redis BITFIELD growth rule; and
//! the size sweeps that are RED on HEAD `935c555`.

use bytes::Bytes;

use super::*;
use crate::storage::Database;
use crate::storage::entry::Entry;

fn frames(args: &[&[u8]]) -> Vec<Frame> {
    args.iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
        .collect()
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn stored(db: &mut Database, key: &[u8]) -> Option<Vec<u8>> {
    db.get(key)
        .and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec))
}

fn model_get_bits(v: &[u8], off: usize, bits: usize) -> u64 {
    let mut x = 0u64;
    for b in 0..bits {
        let pos = off + b;
        let bit = v.get(pos / 8).map_or(0, |byte| (byte >> (7 - pos % 8)) & 1);
        x = (x << 1) | bit as u64;
    }
    x
}

fn model_set_bits(v: &mut Vec<u8>, off: usize, bits: usize, val: u64) {
    let need = (off + bits).div_ceil(8);
    if v.len() < need {
        v.resize(need, 0);
    }
    for b in 0..bits {
        let pos = off + b;
        let bit = (val >> (bits - 1 - b)) & 1;
        if bit == 1 {
            v[pos / 8] |= 1 << (7 - pos % 8);
        } else {
            v[pos / 8] &= !(1 << (7 - pos % 8));
        }
    }
}

/// Random SETBIT / SETRANGE / APPEND / BITFIELD(u8 SET/GET/INCRBY WRAP)
/// sequences against a model. Keys start absent, as a short (inline) value,
/// as a long value with a TTL, and every write must keep that TTL and the
/// LFU counter, and move the WATCH version.
#[test]
fn string_mutators_match_the_model_and_keep_metadata() {
    let mut rng = Rng(0x1168);
    let mut db = Database::new();
    let far = crate::storage::entry::current_time_ms() + 3_600_000;
    let mut models: Vec<(Vec<u8>, Option<Vec<u8>>, u64)> = vec![
        (b"absent".to_vec(), None, 0),
        (b"short".to_vec(), Some(b"abc".to_vec()), 0),
        (b"withttl".to_vec(), Some(vec![0x5a; 300]), far),
    ];
    db.set(b"short", Entry::new_string(Bytes::from_static(b"abc")));
    let mut e = Entry::new_string_with_expiry(Bytes::from(vec![0x5a; 300]), far);
    e.set_access_counter(99);
    db.set(b"withttl", e);

    for step in 0..3000 {
        let k = rng.below(models.len() as u64) as usize;
        let key = models[k].0.clone();
        let v0 = db.get_version(&key);
        let model = models[k].1.get_or_insert_with(Vec::new);
        let got = match rng.below(4) {
            0 => {
                let off = rng.below(4000) as usize;
                let bit = rng.below(2) as u8;
                let want = Frame::Integer(model_get_bits(model, off, 1) as i64);
                model_set_bits(model, off, 1, bit as u64);
                let (o, b) = (off.to_string(), bit.to_string());
                let got = setbit(&mut db, &frames(&[&key, o.as_bytes(), b.as_bytes()]));
                assert_eq!(got, want, "step {step}: SETBIT");
                got
            }
            1 => {
                let off = rng.below(600) as usize;
                let val: Vec<u8> = (0..1 + rng.below(20))
                    .map(|_| rng.below(256) as u8)
                    .collect();
                if model.len() < off + val.len() {
                    model.resize(off + val.len(), 0);
                }
                model[off..off + val.len()].copy_from_slice(&val);
                let o = off.to_string();
                let got = setrange(&mut db, &frames(&[&key, o.as_bytes(), &val]));
                assert_eq!(
                    got,
                    Frame::Integer(model.len() as i64),
                    "step {step}: SETRANGE"
                );
                got
            }
            2 => {
                let val: Vec<u8> = (0..rng.below(40))
                    .map(|_| b'a' + rng.below(26) as u8)
                    .collect();
                model.extend_from_slice(&val);
                let got = append(&mut db, &frames(&[&key, &val]));
                assert_eq!(
                    got,
                    Frame::Integer(model.len() as i64),
                    "step {step}: APPEND"
                );
                got
            }
            _ => {
                let off = rng.below(3000) as usize;
                let val = rng.below(256);
                let incr = rng.below(600) as i64 - 300;
                let old = model_get_bits(model, off, 8);
                model_set_bits(model, off, 8, val);
                let after = (val as i64 + incr).rem_euclid(256) as u64;
                model_set_bits(model, off, 8, after);
                let (o, v, i) = (off.to_string(), val.to_string(), incr.to_string());
                let got = bitfield(
                    &mut db,
                    &frames(&[
                        &key,
                        b"GET",
                        b"u8",
                        o.as_bytes(),
                        b"SET",
                        b"u8",
                        o.as_bytes(),
                        v.as_bytes(),
                        b"INCRBY",
                        b"u8",
                        o.as_bytes(),
                        i.as_bytes(),
                        b"GET",
                        b"u8",
                        o.as_bytes(),
                    ]),
                );
                let want = Frame::Array(
                    vec![
                        Frame::Integer(old as i64),
                        // SET replies with the value it replaced.
                        Frame::Integer(old as i64),
                        Frame::Integer(after as i64),
                        Frame::Integer(after as i64),
                    ]
                    .into(),
                );
                assert_eq!(got, want, "step {step}: BITFIELD");
                got
            }
        };
        assert!(!matches!(got, Frame::Error(_)));
        let want = models[k].1.clone();
        assert_eq!(
            stored(&mut db, &key),
            want,
            "step {step}: value of {:?}",
            String::from_utf8_lossy(&key)
        );
        assert_ne!(
            db.get_version(&key),
            v0,
            "step {step}: WATCH version did not move"
        );
        let ttl = models[k].2;
        let entry = db.get(&key).unwrap();
        assert_eq!(entry.expires_at_ms(), ttl, "step {step}: TTL changed");
        if key == b"withttl" {
            assert_eq!(entry.access_counter(), 99, "step {step}: LFU counter reset");
        }
    }
}

/// Redis's `lookupStringForBitCommand`: a BITFIELD with any SET/INCRBY grows
/// the string (creating the key) to cover the highest written bit BEFORE the
/// ops run, so an INCRBY that `OVERFLOW FAIL` refuses still leaves the key
/// grown — verified against redis-server 7.0.15: `BITFIELD nk OVERFLOW FAIL
/// INCRBY u2 0 5` -> `[nil]`, `EXISTS nk` -> 1, `STRLEN nk` -> 1; on a
/// 3-byte `s`, `... INCRBY u2 100 5` -> `[nil]`, `STRLEN s` -> 13. A
/// GET-only BITFIELD never creates the key.
#[test]
fn bitfield_grows_before_running_like_redis() {
    let mut db = Database::new();
    let got = bitfield(
        &mut db,
        &frames(&[b"nk", b"OVERFLOW", b"FAIL", b"INCRBY", b"u2", b"0", b"5"]),
    );
    assert_eq!(got, Frame::Array(vec![Frame::Null].into()));
    assert_eq!(stored(&mut db, b"nk"), Some(vec![0]));
    db.set(b"s", Entry::new_string(Bytes::from_static(b"abc")));
    bitfield(
        &mut db,
        &frames(&[b"s", b"OVERFLOW", b"FAIL", b"INCRBY", b"u2", b"100", b"5"]),
    );
    assert_eq!(stored(&mut db, b"s").map(|v| v.len()), Some(13));
    let got = bitfield(&mut db, &frames(&[b"none", b"GET", b"u8", b"0"]));
    assert_eq!(got, Frame::Array(vec![Frame::Integer(0)].into()));
    assert!(stored(&mut db, b"none").is_none());
    // A syntax error anywhere writes nothing, even after a valid SET.
    let got = bitfield(
        &mut db,
        &frames(&[b"t", b"SET", b"u8", b"0", b"7", b"BOGUS"]),
    );
    assert!(matches!(got, Frame::Error(_)));
    assert!(stored(&mut db, b"t").is_none());
}

fn best_ns(reps: usize, mut f: impl FnMut()) -> u128 {
    (0..reps)
        .map(|_| {
            let t = std::time::Instant::now();
            f();
            t.elapsed().as_nanos()
        })
        .min()
        .unwrap_or(0)
}

/// SETBIT cost must not depend on the bitmap's size: 1 KiB vs 12.5 MB, 200
/// random in-bounds bits each, best of 5, bound 8x. HEAD `935c555` copied
/// the whole bitmap per SETBIT (a ~10^4 ratio here).
#[test]
fn setbit_cost_is_independent_of_bitmap_size() {
    let mut rng = Rng(3);
    let mut t = Vec::new();
    for size in [1024usize, 12_500_000] {
        let mut db = Database::new();
        let last = (size * 8 - 1).to_string();
        setbit(&mut db, &frames(&[b"bm", last.as_bytes(), b"1"]));
        let offs: Vec<String> = (0..200)
            .map(|_| rng.below(size as u64 * 8).to_string())
            .collect();
        t.push(best_ns(5, || {
            for o in &offs {
                std::hint::black_box(setbit(&mut db, &frames(&[b"bm", o.as_bytes(), b"1"])));
            }
        }));
    }
    let ratio = t[1] as f64 / t[0].max(1) as f64;
    assert!(
        ratio < 8.0,
        "SETBIT on 12.5 MB cost {ratio:.1}x the 1 KiB cost ({} vs {} ns)",
        t[1],
        t[0]
    );
}

/// APPEND is amortized O(1): 2000 x 100 B appended to a 10 KB string cost
/// about what they cost appended to a 4 MB one (bound 8x). HEAD copied the
/// whole value twice per APPEND — quadratic (a ~400x ratio here).
#[test]
fn append_growth_is_amortized_linear() {
    let chunk = [b'x'; 100];
    let mut t = Vec::new();
    for start in [10_000usize, 4_000_000] {
        let mut db = Database::new();
        db.set(b"ap", Entry::new_string(Bytes::from(vec![b'y'; start])));
        t.push(best_ns(3, || {
            for _ in 0..2000 {
                std::hint::black_box(append(&mut db, &frames(&[b"ap", &chunk])));
            }
        }));
    }
    let ratio = t[1] as f64 / t[0].max(1) as f64;
    assert!(
        ratio < 8.0,
        "APPEND onto 4 MB cost {ratio:.1}x onto 10 KB ({} vs {} ns)",
        t[1],
        t[0]
    );
}
