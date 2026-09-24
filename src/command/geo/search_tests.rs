//! moon#1172 — GEOSEARCH by neighbour cells, and the mutable geo reads
//! through the shared-borrow twins.

use bytes::Bytes;

use super::geo_search::{fmt_distance, geo_distance, geo_lat_distance};
use super::*;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::SortedSetRef;

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
    fn uniform(&mut self, lo: f64, hi: f64) -> f64 {
        lo + (self.next() >> 11) as f64 / (1u64 << 53) as f64 * (hi - lo)
    }
}

/// GEOADD `n` points inside [lon_lo, lon_hi] x [lat_lo, lat_hi]; returns the
/// stored (member, lon, lat) — the cell centres GEOPOS reports.
fn load(
    db: &mut Database,
    rng: &mut Rng,
    n: usize,
    box_: (f64, f64, f64, f64),
) -> Vec<(Bytes, f64, f64)> {
    let mut out = Vec::with_capacity(n);
    for chunk in (0..n).collect::<Vec<_>>().chunks(500) {
        let mut args: Vec<Vec<u8>> = vec![b"g".to_vec()];
        for &i in chunk {
            let lon = rng.uniform(box_.0, box_.1);
            let lat = rng.uniform(box_.2, box_.3);
            args.push(format!("{lon:.6}").into_bytes());
            args.push(format!("{lat:.6}").into_bytes());
            args.push(format!("p{i}").into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        geoadd(db, &frames(&refs));
    }
    let zref = db.get_sorted_set_ref_if_alive(b"g", 0).unwrap().unwrap();
    for (m, score) in zref.entries_sorted() {
        let (lon, lat) = geohash_decode(score);
        out.push((m, lon, lat));
    }
    out
}

/// Brute force with redis's predicates: every member tested.
fn oracle(
    points: &[(Bytes, f64, f64)],
    lon: f64,
    lat: f64,
    shape: (f64, Option<f64>),
) -> Vec<(Bytes, f64)> {
    let mut v: Vec<(Bytes, f64)> = points
        .iter()
        .filter_map(|(m, x, y)| match shape {
            (r, None) => {
                let d = geo_distance(lon, lat, *x, *y);
                (d <= r).then(|| (m.clone(), d))
            }
            (w, Some(h)) => {
                if geo_lat_distance(*y, lat) > h / 2.0 || geo_distance(*x, *y, lon, *y) > w / 2.0 {
                    None
                } else {
                    Some((m.clone(), geo_distance(lon, lat, *x, *y)))
                }
            }
        })
        .collect();
    v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
    v
}

fn members(f: &Frame) -> Vec<Bytes> {
    match f {
        Frame::Array(a) => a
            .iter()
            .map(|x| match x {
                Frame::BulkString(b) => b.clone(),
                Frame::Array(inner) => match &inner[0] {
                    Frame::BulkString(b) => b.clone(),
                    o => panic!("{o:?}"),
                },
                o => panic!("{o:?}"),
            })
            .collect(),
        o => panic!("expected array, got {o:?}"),
    }
}

/// The cell search finds exactly what a scan of every member finds, for
/// radii from 1 km to 1000 km and boxes, in ASC and DESC order, with COUNT —
/// on both dispatch paths. (It is redis's candidate set; for these shapes
/// the 9 cells cover the whole shape, so it equals the brute-force set.)
#[test]
fn cell_search_matches_brute_force() {
    let mut rng = Rng(0x1172);
    let mut db = Database::new();
    let points = load(&mut db, &mut rng, 20_000, (-20.0, 40.0, 20.0, 60.0));
    for round in 0..300 {
        let lon = rng.uniform(-15.0, 35.0);
        let lat = rng.uniform(25.0, 55.0);
        let km = [1.0, 7.5, 40.0, 150.0, 600.0, 1000.0][round % 6];
        let by_box = round % 3 == 0;
        let (lon_s, lat_s, km_s, km2_s) = (
            format!("{lon}"),
            format!("{lat}"),
            format!("{km}"),
            format!("{}", km * 0.6),
        );
        let mut args: Vec<&[u8]> = vec![b"g", b"FROMLONLAT", lon_s.as_bytes(), lat_s.as_bytes()];
        let shape = if by_box {
            args.extend([&b"BYBOX"[..], km_s.as_bytes(), km2_s.as_bytes(), b"km"]);
            (km * 1000.0, Some(km * 0.6 * 1000.0))
        } else {
            args.extend([&b"BYRADIUS"[..], km_s.as_bytes(), b"km"]);
            (km * 1000.0, None)
        };
        let want = oracle(&points, lon, lat, shape);
        let desc = round % 2 == 1;
        args.push(if desc { b"DESC" } else { b"ASC" });
        let count = (round % 4 == 0).then(|| 3usize);
        if count.is_some() {
            args.extend([&b"COUNT"[..], b"3"]);
        }
        args.push(b"WITHDIST");
        let f = frames(&args);
        let got = geosearch_readonly(&db, &f, 0);
        assert_eq!(got, geosearch(&mut db, &f), "dispatch paths disagree");
        let mut want_members: Vec<(Bytes, f64)> = want.clone();
        if desc {
            want_members.reverse();
        }
        if let Some(c) = count {
            want_members.truncate(c);
        }
        let got_members = members(&got);
        assert_eq!(
            got_members.len(),
            want_members.len(),
            "round {round}: {} vs {}",
            got_members.len(),
            want_members.len()
        );
        // Compare as distance-ordered sequences: ties can legitimately swap.
        let got_d: Vec<Bytes> = match &got {
            Frame::Array(a) => a
                .iter()
                .map(|x| match x {
                    Frame::Array(i) => match &i[1] {
                        Frame::BulkString(b) => b.clone(),
                        o => panic!("{o:?}"),
                    },
                    o => panic!("{o:?}"),
                })
                .collect(),
            _ => unreachable!(),
        };
        let want_d: Vec<Bytes> = want_members
            .iter()
            .map(|(_, d)| fmt_distance(d / 1000.0))
            .collect();
        assert_eq!(got_d, want_d, "round {round}: distances");
        if count.is_none() {
            let mut a = got_members.clone();
            let mut b: Vec<Bytes> = want_members.iter().map(|(m, _)| m.clone()).collect();
            a.sort();
            b.sort();
            assert_eq!(a, b, "round {round}: member sets");
        }
    }
}

/// `COUNT n ANY` returns n members that are in the shape (the first n the
/// cell walk meets, as redis's `limit`), not necessarily the n closest; no
/// ANY without COUNT; COUNT errors are redis's.
#[test]
fn any_count_and_errors() {
    let mut rng = Rng(9);
    let mut db = Database::new();
    let points = load(&mut db, &mut rng, 5_000, (10.0, 20.0, 40.0, 50.0));
    let f = frames(&[
        b"g",
        b"FROMLONLAT",
        b"15",
        b"45",
        b"BYRADIUS",
        b"300",
        b"km",
        b"COUNT",
        b"5",
        b"ANY",
    ]);
    let got = members(&geosearch_readonly(&db, &f, 0));
    assert_eq!(got.len(), 5);
    let inside: std::collections::HashSet<Bytes> = oracle(&points, 15.0, 45.0, (300_000.0, None))
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    assert!(got.iter().all(|m| inside.contains(m)));
    for (args, msg) in [
        (
            &[
                &b"g"[..],
                b"FROMLONLAT",
                b"15",
                b"45",
                b"BYRADIUS",
                b"3",
                b"km",
                b"ANY",
            ][..],
            &b"ERR the ANY argument requires COUNT argument"[..],
        ),
        (
            &[
                b"g",
                b"FROMLONLAT",
                b"15",
                b"45",
                b"BYRADIUS",
                b"3",
                b"km",
                b"COUNT",
                b"0",
            ],
            b"ERR COUNT must be > 0",
        ),
        (
            &[
                b"g",
                b"FROMLONLAT",
                b"15",
                b"45",
                b"BYRADIUS",
                b"3",
                b"km",
                b"COUNT",
                b"1.5",
            ],
            b"ERR value is not an integer or out of range",
        ),
        (
            &[b"g", b"FROMLONLAT", b"200", b"45", b"BYRADIUS", b"3", b"km"],
            b"ERR invalid longitude,latitude pair 200.000000,45.000000",
        ),
    ] {
        assert_eq!(
            geosearch_readonly(&db, &frames(args), 0),
            Frame::Error(Bytes::copy_from_slice(msg))
        );
    }
}

/// `fixedpoint_d2string(d, 4)`: llrint(d * 1e4), round half to even.
#[test]
fn distance_formatting_is_redis_fixedpoint() {
    for (d, s) in [
        (0.0, "0.0000"),
        (166274.15156960033, "166274.1516"),
        (0.03125, "0.0312"), // 312.5 exactly -> 312 (ties to even)
        (0.09375, "0.0938"), // 937.5 exactly -> 938
        (1.23456, "1.2346"),
        (12.0, "12.0000"),
        (0.0123, "0.0123"),
    ] {
        assert_eq!(fmt_distance(d), Bytes::from(s), "{d}");
    }
}

fn is_listpack(db: &Database) -> bool {
    matches!(
        db.get_sorted_set_ref_if_alive(b"small", 0),
        Ok(Some(SortedSetRef::Listpack(_)))
    )
}

/// The mutable twins (cross-shard SPSC, MULTI, Lua) used `get_sorted_set`,
/// which flattened a listpack geo set for good. RED on HEAD `935c555`.
#[test]
fn mutable_geo_reads_keep_the_listpack() {
    // GEOADD creates the B+tree form directly; a listpack geo set is a
    // small zset written by ZADD (or loaded from an RDB) with geohash scores.
    let mut db = Database::new();
    let p = (geohash_encode(13.361389, 38.115556) as u64).to_string();
    let c = (geohash_encode(15.087269, 37.502669) as u64).to_string();
    crate::command::sorted_set::zadd(
        &mut db,
        &frames(&[b"small", p.as_bytes(), b"Palermo", c.as_bytes(), b"Catania"]),
    );
    assert!(is_listpack(&db));
    geopos(&mut db, &frames(&[b"small", b"Palermo"]));
    assert!(is_listpack(&db), "GEOPOS flattened");
    let d = geodist(&mut db, &frames(&[b"small", b"Palermo", b"Catania", b"km"]));
    assert_eq!(d, Frame::BulkString(Bytes::from_static(b"166.2742")));
    assert!(is_listpack(&db), "GEODIST flattened");
    geohash(&mut db, &frames(&[b"small", b"Palermo"]));
    assert!(is_listpack(&db), "GEOHASH flattened");
    let got = geosearch(
        &mut db,
        &frames(&[
            b"small",
            b"FROMLONLAT",
            b"15",
            b"37",
            b"BYRADIUS",
            b"200",
            b"km",
            b"ASC",
        ]),
    );
    assert_eq!(
        members(&got),
        vec![
            Bytes::from_static(b"Catania"),
            Bytes::from_static(b"Palermo")
        ]
    );
    assert!(is_listpack(&db), "GEOSEARCH flattened");
    georadius(
        &mut db,
        &frames(&[b"small", b"15", b"37", b"200", b"km", b"STORE", b"dst"]),
    );
    assert!(is_listpack(&db), "GEORADIUS STORE flattened its source");
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

/// The review's query — `BYRADIUS 50 km ASC COUNT 10` on uniformly random
/// world points — must not scale with N: 2K vs 200K, best-of-5, bound 8x.
/// HEAD `935c555` ran haversine on every member (~100x here).
#[test]
fn geosearch_does_not_scale_with_n() {
    let mut rng = Rng(1);
    let mut small = Database::new();
    let mut big = Database::new();
    load(&mut small, &mut rng, 2_000, (-180.0, 180.0, -85.0, 85.0));
    load(&mut big, &mut rng, 200_000, (-180.0, 180.0, -85.0, 85.0));
    let args = frames(&[
        b"g",
        b"FROMLONLAT",
        b"13.4",
        b"52.5",
        b"BYRADIUS",
        b"50",
        b"km",
        b"ASC",
        b"COUNT",
        b"10",
    ]);
    let t_small = best_ns(5, 20, || {
        std::hint::black_box(geosearch_readonly(&small, std::hint::black_box(&args), 0));
    });
    let t_big = best_ns(5, 20, || {
        std::hint::black_box(geosearch_readonly(&big, std::hint::black_box(&args), 0));
    });
    let ratio = t_big as f64 / t_small.max(1) as f64;
    assert!(
        ratio < 8.0,
        "GEOSEARCH 200K took {ratio:.1}x the 2K time ({t_big} vs {t_small} ns)"
    );
}
