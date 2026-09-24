//! moon#1170 — sorted-set range reads by order statistics.
//!
//! Two kinds of test, both against the full `SortedSetBPTree` encoding (the
//! listpack arm is untouched by moon#1170):
//!
//! * a randomized DIFFERENTIAL of every rewritten reply against a naive
//!   oracle that sorts all entries, filters, reverses and slices — the exact
//!   semantics the old collect-everything code had, including the moon#961
//!   directional infinities, the moon#966 exact comparisons and the moon#967
//!   negative-LIMIT rules. Both dispatch paths (`zrangebyscore` on
//!   `&mut Database`, `*_readonly` on `&Database`) are compared.
//! * a SIZE SWEEP: `LIMIT 0 10`, its REV twin and `ZCOUNT` must not scale
//!   with N. RED on HEAD `935c555`, where each walked every in-range entry.
//!
//! Kept as a sibling file (declared from `mod.rs`) because `mod.rs` is
//! already far past the file-size rule.

use bytes::Bytes;
use ordered_float::OrderedFloat;

use super::*;
use crate::storage::Database;

fn bulk(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

fn frames(args: &[&[u8]]) -> Vec<Frame> {
    args.iter().map(|a| bulk(a)).collect()
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
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[(self.next() % xs.len() as u64) as usize]
    }
}

/// Score spellings: many ties, both zeros, both infinities, tiny steps.
const SCORES: [&str; 10] = [
    "-inf", "-2.5", "-0", "0", "0.1", "1", "1.5", "7", "1e300", "+inf",
];

/// Bound spellings for BYSCORE, inclusive and exclusive, including the
/// directional infinities of moon#961 and values between stored scores.
const SCORE_BOUNDS: [&str; 20] = [
    "-inf", "+inf", "inf", "(-inf", "(+inf", "-2.5", "(-2.5", "-0", "(-0", "0", "(0", "0.05",
    "(0.1", "1", "(1", "1.25", "(1.5", "7", "(7", "1e300",
];

const LEX_BOUNDS: [&str; 12] = [
    "-", "+", "[a", "(a", "[m1", "(m1", "[m5", "(m50", "[m9", "(zz", "[", "(",
];

/// Build a B+tree-encoded zset of `n` members at `key`.
fn load(db: &mut Database, rng: &mut Rng, key: &[u8], n: usize, one_score: bool) {
    let mut added = 0;
    while added < n {
        let mut args: Vec<Vec<u8>> = vec![key.to_vec()];
        for _ in 0..50 {
            let s = if one_score { "3" } else { *rng.pick(&SCORES) };
            let m = format!("m{}", rng.next() % (n as u64 * 3));
            args.push(s.as_bytes().to_vec());
            args.push(m.into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        zadd(db, &frames(&refs));
        added = match zcard(db, &frames(&[key])) {
            Frame::Integer(c) => c as usize,
            other => panic!("ZCARD: {other:?}"),
        };
    }
    let enc = db
        .get_sorted_set_ref_if_alive(key, 0)
        .ok()
        .flatten()
        .map(|z| z.bptree().is_some());
    assert_eq!(enc, Some(true), "fixture must be B+tree encoded");
}

/// All `(score, member)` pairs, sorted the tree's way.
fn model(db: &Database, key: &[u8]) -> Vec<(OrderedFloat<f64>, Bytes)> {
    let zref = db.get_sorted_set_ref_if_alive(key, 0).unwrap().unwrap();
    let mut v: Vec<_> = zref
        .members_map()
        .unwrap()
        .iter()
        .map(|(m, s)| (OrderedFloat(*s), m.clone()))
        .collect();
    v.sort();
    v
}

fn oracle_score_ok(bound: &str, s: f64, lower: bool) -> bool {
    let b = parse_score_bound(bound.as_bytes()).unwrap();
    if lower {
        b.includes(s)
    } else {
        b.includes_upper(s)
    }
}

fn lex_ok(bound: &str, m: &[u8], lower: bool) -> bool {
    let b = parse_lex_bound(bound.as_bytes()).unwrap();
    if lower {
        lex_in_range(m, &b, &LexBound::PosInf)
    } else {
        lex_in_range(m, &LexBound::NegInf, &b)
    }
}

/// Slice a filtered, ordered sequence the way moon always has: negative
/// offset -> nothing, negative count -> no limit.
fn slice_limit<T: Clone>(v: Vec<T>, limit: Option<(i64, i64)>) -> Vec<T> {
    match limit {
        None => v,
        Some((o, _)) if o < 0 => Vec::new(),
        Some((o, c)) => {
            let it = v.into_iter().skip(o as usize);
            if c < 0 {
                it.collect()
            } else {
                it.take(c as usize).collect()
            }
        }
    }
}

fn reply(entries: &[(OrderedFloat<f64>, Bytes)], withscores: bool) -> Frame {
    let mut out = Vec::new();
    for (s, m) in entries {
        out.push(Frame::BulkString(m.clone()));
        if withscores {
            out.push(Frame::BulkString(format_score_bytes(s.0)));
        }
    }
    Frame::Array(out.into())
}

/// Run a read on BOTH dispatch paths and insist they agree.
fn both_paths(
    db: &mut Database,
    args: &[Vec<u8>],
    mutable: fn(&mut Database, &[Frame]) -> Frame,
    readonly: fn(&Database, &[Frame], u64) -> Frame,
) -> Frame {
    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
    let f = frames(&refs);
    let ro = readonly(db, &f, 0);
    let rw = mutable(db, &f);
    assert_eq!(
        ro,
        rw,
        "dispatch paths disagree for {:?}",
        String::from_utf8_lossy(&args.concat())
    );
    ro
}

fn limit_args(rng: &mut Rng) -> Option<(i64, i64)> {
    match rng.next() % 4 {
        0 => None,
        _ => {
            let o = *rng.pick(&[-1i64, 0, 0, 1, 3, 17, 150, 100_000]);
            let c = *rng.pick(&[-5i64, -1, 0, 1, 2, 10, 64, 100_000]);
            Some((o, c))
        }
    }
}

fn push_limit(args: &mut Vec<Vec<u8>>, limit: Option<(i64, i64)>) {
    if let Some((o, c)) = limit {
        args.push(b"LIMIT".to_vec());
        args.push(o.to_string().into_bytes());
        args.push(c.to_string().into_bytes());
    }
}

#[test]
fn score_ranges_match_the_naive_oracle() {
    let mut rng = Rng(0x1170_0001);
    for &n in &[200usize, 1500] {
        let mut db = Database::new();
        load(&mut db, &mut rng, b"z", n, false);
        let all = model(&db, b"z");
        for _ in 0..600 {
            let min = *rng.pick(&SCORE_BOUNDS);
            let max = *rng.pick(&SCORE_BOUNDS);
            let withscores = rng.next() % 2 == 0;
            let limit = limit_args(&mut rng);
            let asc: Vec<_> = all
                .iter()
                .filter(|(s, _)| {
                    oracle_score_ok(min, s.0, true) && oracle_score_ok(max, s.0, false)
                })
                .cloned()
                .collect();

            // ZRANGEBYSCORE key min max
            let mut args = vec![b"z".to_vec(), min.into(), max.into()];
            if withscores {
                args.push(b"WITHSCORES".to_vec());
            }
            push_limit(&mut args, limit);
            let got = both_paths(&mut db, &args, zrangebyscore, zrangebyscore_readonly);
            assert_eq!(
                got,
                reply(&slice_limit(asc.clone(), limit), withscores),
                "ZRANGEBYSCORE {min} {max} {limit:?}"
            );

            // ZREVRANGEBYSCORE key max min — same set, descending.
            let mut desc = asc.clone();
            desc.reverse();
            let mut args = vec![b"z".to_vec(), max.into(), min.into()];
            if withscores {
                args.push(b"WITHSCORES".to_vec());
            }
            push_limit(&mut args, limit);
            let got = both_paths(&mut db, &args, zrevrangebyscore, zrevrangebyscore_readonly);
            let want = reply(&slice_limit(desc.clone(), limit), withscores);
            assert_eq!(got, want, "ZREVRANGEBYSCORE {max} {min} {limit:?}");

            // ZRANGE key max min BYSCORE REV — the unified spelling.
            let mut args = vec![
                b"z".to_vec(),
                max.into(),
                min.into(),
                b"BYSCORE".to_vec(),
                b"REV".to_vec(),
            ];
            if withscores {
                args.push(b"WITHSCORES".to_vec());
            }
            push_limit(&mut args, limit);
            let got = both_paths(&mut db, &args, zrange, zrange_readonly);
            assert_eq!(got, want, "ZRANGE BYSCORE REV {max} {min} {limit:?}");

            // ZCOUNT
            let args = vec![b"z".to_vec(), min.into(), max.into()];
            let got = both_paths(&mut db, &args, zcount, zcount_readonly);
            assert_eq!(got, Frame::Integer(asc.len() as i64), "ZCOUNT {min} {max}");
        }
    }
}

#[test]
fn rank_ranges_match_the_naive_oracle() {
    let mut rng = Rng(0x1170_0002);
    let mut db = Database::new();
    load(&mut db, &mut rng, b"z", 700, false);
    let all = model(&db, b"z");
    let len = all.len() as i64;
    let idx = [
        0i64, 1, 5, 13, 14, 99, -1, -2, -15, -700, -701, 699, 700, 5000, -5000,
    ];
    for &start in &idx {
        for &stop in &idx {
            for rev in [false, true] {
                let withscores = (start + stop) % 2 == 0;
                let norm = |i: i64| if i < 0 { len + i } else { i };
                let (s, e) = (norm(start).max(0), norm(stop).min(len - 1));
                let window: Vec<_> = if s > e || s >= len {
                    Vec::new()
                } else if rev {
                    let r: Vec<_> = all.iter().rev().cloned().collect();
                    r[s as usize..=e as usize].to_vec()
                } else {
                    all[s as usize..=e as usize].to_vec()
                };
                let mut args = vec![
                    b"z".to_vec(),
                    start.to_string().into(),
                    stop.to_string().into(),
                ];
                if rev {
                    args.push(b"REV".to_vec());
                }
                if withscores {
                    args.push(b"WITHSCORES".to_vec());
                }
                let got = both_paths(&mut db, &args, zrange, zrange_readonly);
                assert_eq!(
                    got,
                    reply(&window, withscores),
                    "ZRANGE {start} {stop} rev={rev}"
                );
                if rev {
                    let mut args = vec![
                        b"z".to_vec(),
                        start.to_string().into(),
                        stop.to_string().into(),
                    ];
                    if withscores {
                        args.push(b"WITHSCORES".to_vec());
                    }
                    let got = both_paths(&mut db, &args, zrevrange, zrevrange_readonly);
                    assert_eq!(got, reply(&window, withscores), "ZREVRANGE {start} {stop}");
                }
            }
        }
    }
}

/// Lex ranges on BOTH shapes: an all-one-score zset (the documented
/// precondition — answered by rank seeks) and a mixed-score one (kept on
/// the scan, whose long-standing answer the oracle reproduces).
#[test]
fn lex_ranges_match_the_naive_oracle() {
    let mut rng = Rng(0x1170_0003);
    for one_score in [true, false] {
        let mut db = Database::new();
        load(&mut db, &mut rng, b"z", 400, one_score);
        let all = model(&db, b"z");
        for _ in 0..500 {
            let min = *rng.pick(&LEX_BOUNDS);
            let max = *rng.pick(&LEX_BOUNDS);
            if parse_lex_bound(min.as_bytes()).is_err() || parse_lex_bound(max.as_bytes()).is_err()
            {
                continue;
            }
            let limit = limit_args(&mut rng);
            let asc: Vec<_> = all
                .iter()
                .filter(|(_, m)| lex_ok(min, m, true) && lex_ok(max, m, false))
                .cloned()
                .collect();
            let mut desc = asc.clone();
            desc.reverse();

            let mut args = vec![b"z".to_vec(), min.into(), max.into()];
            push_limit(&mut args, limit);
            let got = both_paths(&mut db, &args, zrangebylex, zrangebylex_readonly);
            assert_eq!(
                got,
                reply(&slice_limit(asc.clone(), limit), false),
                "ZRANGEBYLEX {min} {max} {limit:?} one={one_score}"
            );

            let mut args = vec![b"z".to_vec(), max.into(), min.into()];
            push_limit(&mut args, limit);
            let got = both_paths(&mut db, &args, zrevrangebylex, zrevrangebylex_readonly);
            assert_eq!(
                got,
                reply(&slice_limit(desc, limit), false),
                "ZREVRANGEBYLEX {max} {min} {limit:?} one={one_score}"
            );

            let args = vec![b"z".to_vec(), min.into(), max.into()];
            let got = both_paths(&mut db, &args, zlexcount, zlexcount_readonly);
            assert_eq!(
                got,
                Frame::Integer(asc.len() as i64),
                "ZLEXCOUNT {min} {max} one={one_score}"
            );
        }
    }
}

/// Best-of-`reps` wall time of `iters` calls, in nanoseconds.
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

/// The complexity claim itself: `LIMIT 0 10`, its REV twin and `ZCOUNT`
/// over the WHOLE range must cost the same on 2K and 200K members (they
/// are O(log N + 10) / O(log N) now). HEAD `935c555` walked every in-range
/// entry, a ~100x ratio here; the bound is 8x so scheduler noise on a
/// shared box cannot flake it while any O(N) regression still trips it.
#[test]
fn limit_and_count_do_not_scale_with_n() {
    let mut rng = Rng(0x1170_0004);
    let mut small = Database::new();
    let mut big = Database::new();
    // Bulk-load directly: 200K ZADD round trips would dominate the test.
    for (db, n) in [(&mut small, 2_000usize), (&mut big, 200_000)] {
        let mut args: Vec<Vec<u8>> = vec![b"z".to_vec()];
        for i in 0..n {
            args.push(((rng.next() % 1_000_000) as f64).to_string().into_bytes());
            args.push(format!("m{i}").into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        zadd(db, &frames(&refs));
    }
    let cases: [(&str, fn(&Database, &[Frame], u64) -> Frame, Vec<Frame>); 3] = [
        (
            "ZRANGEBYSCORE -inf +inf LIMIT 0 10",
            zrangebyscore_readonly,
            frames(&[b"z", b"-inf", b"+inf", b"LIMIT", b"0", b"10"]),
        ),
        (
            "ZREVRANGEBYSCORE +inf -inf LIMIT 0 10",
            zrevrangebyscore_readonly,
            frames(&[b"z", b"+inf", b"-inf", b"LIMIT", b"0", b"10"]),
        ),
        (
            "ZCOUNT -inf +inf",
            zcount_readonly,
            frames(&[b"z", b"-inf", b"+inf"]),
        ),
    ];
    for (name, f, args) in cases.iter() {
        let t_small = best_ns(7, 20, || {
            std::hint::black_box(f(&small, std::hint::black_box(args), 0));
        });
        let t_big = best_ns(7, 20, || {
            std::hint::black_box(f(&big, std::hint::black_box(args), 0));
        });
        let ratio = t_big as f64 / t_small.max(1) as f64;
        assert!(
            ratio < 8.0,
            "{name}: 200K members took {ratio:.1}x the 2K time ({t_big} ns vs {t_small} ns) — O(N) again?"
        );
    }
}

/// The autocomplete shape — `ZRANGEBYLEX k [pre + LIMIT 0 10` on a zset whose
/// members all share one score — and `ZLEXCOUNT` must not scale with N
/// either. HEAD `935c555` scanned every member for both.
#[test]
fn lex_limit_and_count_do_not_scale_with_n() {
    let mut small = Database::new();
    let mut big = Database::new();
    for (db, n) in [(&mut small, 2_000usize), (&mut big, 200_000)] {
        let mut args: Vec<Vec<u8>> = vec![b"z".to_vec()];
        for i in 0..n {
            args.push(b"0".to_vec());
            args.push(format!("m{i:07}").into_bytes());
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        zadd(db, &frames(&refs));
    }
    let cases: [(&str, fn(&Database, &[Frame], u64) -> Frame, Vec<Frame>); 3] = [
        (
            "ZRANGEBYLEX [m0001 + LIMIT 0 10",
            zrangebylex_readonly,
            frames(&[b"z", b"[m0001", b"+", b"LIMIT", b"0", b"10"]),
        ),
        (
            "ZREVRANGEBYLEX + - LIMIT 0 10",
            zrevrangebylex_readonly,
            frames(&[b"z", b"+", b"-", b"LIMIT", b"0", b"10"]),
        ),
        (
            "ZLEXCOUNT - +",
            zlexcount_readonly,
            frames(&[b"z", b"-", b"+"]),
        ),
    ];
    for (name, f, args) in cases.iter() {
        let t_small = best_ns(7, 20, || {
            std::hint::black_box(f(&small, std::hint::black_box(args), 0));
        });
        let t_big = best_ns(7, 20, || {
            std::hint::black_box(f(&big, std::hint::black_box(args), 0));
        });
        let ratio = t_big as f64 / t_small.max(1) as f64;
        assert!(
            ratio < 8.0,
            "{name}: 200K members took {ratio:.1}x the 2K time ({t_big} ns vs {t_small} ns) — O(N) again?"
        );
    }
}
