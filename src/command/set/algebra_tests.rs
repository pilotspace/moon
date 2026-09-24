//! moon#1169 — set algebra over borrowed `SetRef`s.
//!
//! A randomized differential of SINTER / SUNION / SDIFF / SINTERCARD and the
//! three `*STORE` forms against a `BTreeSet` model, over every source
//! encoding (intset, listpack, hashtable), missing keys and WRONGTYPE keys,
//! on both dispatch paths; the source-encoding guarantee for `*STORE`; and
//! the size sweep that is RED on HEAD `935c555`.

use std::collections::BTreeSet;

use bytes::Bytes;

use super::*;
use crate::command::string::set as string_set;
use crate::storage::Database;
use crate::storage::db::SetRef;

type Model = BTreeSet<Vec<u8>>;

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
}

fn add(db: &mut Database, key: &[u8], members: &[Vec<u8>]) {
    for chunk in members.chunks(100) {
        let mut args: Vec<&[u8]> = vec![key];
        args.extend(chunk.iter().map(|m| m.as_slice()));
        sadd(db, &frames(&args));
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Enc {
    Intset,
    Listpack,
    Hash,
}

fn encoding(db: &Database, key: &[u8]) -> Option<Enc> {
    match db.get_set_ref_if_alive(key, 0).ok().flatten()? {
        SetRef::Intset(_) => Some(Enc::Intset),
        SetRef::Listpack(_) => Some(Enc::Listpack),
        SetRef::Hash(_) | SetRef::Owned(_) => Some(Enc::Hash),
    }
}

/// Fixture: two sets of each encoding with overlapping members (integers
/// shared between the intsets and the other encodings, so the Int-vs-bytes
/// probe paths meet), a string key, and no key called `missing`.
fn fixture(rng: &mut Rng) -> (Database, Vec<(&'static [u8], Model)>) {
    let mut db = Database::new();
    let mut models = Vec::new();
    let ints = |rng: &mut Rng, n: usize| -> Vec<Vec<u8>> {
        (0..n)
            .map(|_| ((rng.next() % 300) as i64 - 50).to_string().into_bytes())
            .collect()
    };
    let mixed = |rng: &mut Rng, n: usize| -> Vec<Vec<u8>> {
        (0..n)
            .map(|_| {
                let v = rng.next() % 300;
                if v % 3 == 0 {
                    (v as i64 - 50).to_string().into_bytes()
                } else {
                    format!("m{v}").into_bytes()
                }
            })
            .collect()
    };
    let specs: [(&'static [u8], Enc, usize, bool); 6] = [
        (b"i1", Enc::Intset, 60, true),
        (b"i2", Enc::Intset, 200, true),
        (b"l1", Enc::Listpack, 40, false),
        (b"l2", Enc::Listpack, 90, false),
        (b"h1", Enc::Hash, 400, false),
        (b"h2", Enc::Hash, 700, false),
    ];
    for (key, enc, n, integer) in specs {
        let members = if integer { ints(rng, n) } else { mixed(rng, n) };
        add(&mut db, key, &members);
        assert_eq!(
            encoding(&db, key),
            Some(enc),
            "fixture {:?}",
            std::str::from_utf8(key)
        );
        models.push((key, members.into_iter().collect::<Model>()));
    }
    string_set(&mut db, &frames(&[b"str", b"x"]));
    (db, models)
}

fn sorted_members(f: &Frame) -> Model {
    match f {
        Frame::Array(items) => items
            .iter()
            .map(|x| match x {
                Frame::BulkString(b) => b.to_vec(),
                other => panic!("not a bulk: {other:?}"),
            })
            .collect(),
        other => panic!("expected array, got {other:?}"),
    }
}

fn read_both(
    db: &mut Database,
    args: &[&[u8]],
    mutable: fn(&mut Database, &[Frame]) -> Frame,
    readonly: fn(&Database, &[Frame], u64) -> Frame,
) -> Frame {
    let f = frames(args);
    let ro = readonly(db, &f, 0);
    let rw = mutable(db, &f);
    match (&ro, &rw) {
        (Frame::Array(_), Frame::Array(_)) => assert_eq!(sorted_members(&ro), sorted_members(&rw)),
        _ => assert_eq!(ro, rw),
    }
    ro
}

#[test]
fn set_algebra_matches_the_model_on_every_encoding() {
    let mut rng = Rng(0x1169);
    let (mut db, models) = fixture(&mut rng);
    let names: Vec<&[u8]> = models
        .iter()
        .map(|(k, _)| *k)
        .chain([&b"missing"[..]])
        .collect();
    let model_of = |k: &[u8]| -> Model {
        models
            .iter()
            .find(|(n, _)| *n == k)
            .map(|(_, m)| m.clone())
            .unwrap_or_default()
    };
    let before: Vec<_> = models.iter().map(|(k, _)| encoding(&db, k)).collect();

    for round in 0..400 {
        let n = 1 + (rng.next() % 4) as usize;
        let keys: Vec<&[u8]> = (0..n)
            .map(|_| names[(rng.next() % names.len() as u64) as usize])
            .collect();
        let sets: Vec<Model> = keys.iter().map(|k| model_of(k)).collect();
        let present = |k: &[u8]| k != b"missing";

        let inter: Model = if keys.iter().all(|k| present(k)) {
            sets[0]
                .iter()
                .filter(|m| sets[1..].iter().all(|s| s.contains(*m)))
                .cloned()
                .collect()
        } else {
            Model::new()
        };
        let uni: Model = sets.iter().flatten().cloned().collect();
        let diff: Model = sets[0]
            .iter()
            .filter(|m| sets[1..].iter().all(|s| !s.contains(*m)))
            .cloned()
            .collect();

        assert_eq!(
            sorted_members(&read_both(&mut db, &keys, sinter, sinter_readonly)),
            inter,
            "SINTER {keys:?}"
        );
        assert_eq!(
            sorted_members(&read_both(&mut db, &keys, sunion, sunion_readonly)),
            uni,
            "SUNION {keys:?}"
        );
        assert_eq!(
            sorted_members(&read_both(&mut db, &keys, sdiff, sdiff_readonly)),
            diff,
            "SDIFF {keys:?}"
        );

        let nk = n.to_string();
        for limit in ["0", "1", "3", "100000"] {
            let mut args: Vec<&[u8]> = vec![nk.as_bytes()];
            args.extend(keys.iter());
            if limit != "0" || round % 2 == 0 {
                args.push(b"LIMIT");
                args.push(limit.as_bytes());
            }
            let l: usize = limit.parse().unwrap();
            let want = if l == 0 {
                inter.len()
            } else {
                inter.len().min(l)
            };
            let got = read_both(&mut db, &args, sintercard, sintercard_readonly);
            assert_eq!(
                got,
                Frame::Integer(want as i64),
                "SINTERCARD {keys:?} LIMIT {limit}"
            );
        }

        for (store, want) in [
            (sinterstore as fn(&mut Database, &[Frame]) -> Frame, &inter),
            (sunionstore, &uni),
            (sdiffstore, &diff),
        ] {
            let mut args: Vec<&[u8]> = vec![b"dest"];
            args.extend(keys.iter());
            assert_eq!(
                store(&mut db, &frames(&args)),
                Frame::Integer(want.len() as i64)
            );
            let stored = smembers_readonly(&db, &frames(&[b"dest"]), 0);
            assert_eq!(&sorted_members(&stored), want, "*STORE {keys:?}");
        }
    }
    // No source was re-encoded by any of it (moon#1169's *STORE defect).
    let after: Vec<_> = models.iter().map(|(k, _)| encoding(&db, k)).collect();
    assert_eq!(before, after, "a set-algebra command re-encoded a source");
}

/// WRONGTYPE on ANY key wins over a missing key, whatever the order — redis
/// type-checks every key before it computes. HEAD's SINTERCARD answered `0`
/// for `SINTERCARD 2 missing str` because it returned at the first missing
/// key.
#[test]
fn wrongtype_anywhere_wins_over_missing() {
    let mut rng = Rng(7);
    let (mut db, _) = fixture(&mut rng);
    let is_wrongtype = |f: &Frame| matches!(f, Frame::Error(e) if e.starts_with(b"WRONGTYPE"));
    for keys in [
        &[&b"missing"[..], b"str"][..],
        &[b"str", b"missing"],
        &[b"i1", b"missing", b"str"],
    ] {
        assert!(
            is_wrongtype(&sinter_readonly(&db, &frames(keys), 0)),
            "SINTER {keys:?}"
        );
        assert!(
            is_wrongtype(&sunion_readonly(&db, &frames(keys), 0)),
            "SUNION {keys:?}"
        );
        assert!(
            is_wrongtype(&sdiff_readonly(&db, &frames(keys), 0)),
            "SDIFF {keys:?}"
        );
        let nk = keys.len().to_string();
        let mut args: Vec<&[u8]> = vec![nk.as_bytes()];
        args.extend(keys.iter());
        assert!(
            is_wrongtype(&sintercard_readonly(&db, &frames(&args), 0)),
            "SINTERCARD {keys:?}"
        );
        let mut args: Vec<&[u8]> = vec![b"dest"];
        args.extend(keys.iter());
        assert!(
            is_wrongtype(&sinterstore(&mut db, &frames(&args))),
            "SINTERSTORE {keys:?}"
        );
    }
}

/// `SINTERSTORE d small small` (and the union/diff forms) must leave `small`
/// in its compact encoding. RED on HEAD `935c555`, which read *STORE sources
/// through `get_set` and flattened them to a hashtable for good.
#[test]
fn store_sources_keep_their_compact_encoding() {
    let mut db = Database::new();
    add(
        &mut db,
        b"ints",
        &(0..20)
            .map(|i: i32| i.to_string().into_bytes())
            .collect::<Vec<_>>(),
    );
    add(
        &mut db,
        b"lp",
        &(0..20)
            .map(|i| format!("m{i}").into_bytes())
            .collect::<Vec<_>>(),
    );
    assert_eq!(encoding(&db, b"ints"), Some(Enc::Intset));
    assert_eq!(encoding(&db, b"lp"), Some(Enc::Listpack));
    for store in [sinterstore, sunionstore, sdiffstore] {
        store(&mut db, &frames(&[b"d1", b"ints", b"ints"]));
        store(&mut db, &frames(&[b"d2", b"lp", b"ints"]));
        assert_eq!(encoding(&db, b"ints"), Some(Enc::Intset));
        assert_eq!(encoding(&db, b"lp"), Some(Enc::Listpack));
    }
}

fn best_ns(reps: usize, iters: usize, mut f: impl FnMut()) -> u128 {
    (0..reps)
        .map(|_| {
            let t = std::time::Instant::now();
            for _ in 0..iters {
                f();
            }
            t.elapsed().as_nanos()
        })
        .min()
        .unwrap_or(0)
}

/// `SINTER small big` / `SDIFF small big` / `SINTERCARD 2 big big LIMIT 5`
/// must not scale with |big|: 2K vs 200K members, best-of-7, bound 8x.
/// HEAD `935c555` copied `big` into a HashSet on every call (~100x here).
#[test]
fn small_against_big_does_not_scale_with_big() {
    let mut small_db = Database::new();
    let mut big_db = Database::new();
    for (db, n) in [(&mut small_db, 2_000usize), (&mut big_db, 200_000)] {
        let big: Vec<Vec<u8>> = (0..n).map(|i| format!("m{i}").into_bytes()).collect();
        add(db, b"big", &big);
        add(
            db,
            b"small",
            &(0..10)
                .map(|i| format!("m{}", i * 7))
                .map(String::into_bytes)
                .collect::<Vec<_>>(),
        );
    }
    let cases: [(&str, fn(&Database, &[Frame], u64) -> Frame, Vec<Frame>); 4] = [
        (
            "SINTER small big",
            sinter_readonly,
            frames(&[b"small", b"big"]),
        ),
        (
            "SINTER big small",
            sinter_readonly,
            frames(&[b"big", b"small"]),
        ),
        (
            "SDIFF small big",
            sdiff_readonly,
            frames(&[b"small", b"big"]),
        ),
        (
            "SINTERCARD 2 big big LIMIT 5",
            sintercard_readonly,
            frames(&[b"2", b"big", b"big", b"LIMIT", b"5"]),
        ),
    ];
    for (name, f, args) in cases.iter() {
        let t_small = best_ns(7, 10, || {
            std::hint::black_box(f(&small_db, std::hint::black_box(args), 0));
        });
        let t_big = best_ns(7, 10, || {
            std::hint::black_box(f(&big_db, std::hint::black_box(args), 0));
        });
        let ratio = t_big as f64 / t_small.max(1) as f64;
        assert!(
            ratio < 8.0,
            "{name}: {ratio:.1}x ({t_big} ns vs {t_small} ns)"
        );
    }
}
