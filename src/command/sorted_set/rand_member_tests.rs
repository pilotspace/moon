//! moon#1171 — ZRANDMEMBER by position, not by materializing the zset.
//!
//! Correctness on both encodings and both dispatch paths, plus the size
//! sweep that is RED on HEAD `935c555` (which collected all N pairs into a
//! Vec to pick one).

use bytes::Bytes;

use super::*;
use crate::storage::Database;

fn frames(args: &[&[u8]]) -> Vec<Frame> {
    args.iter()
        .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
        .collect()
}

fn load(db: &mut Database, key: &[u8], n: usize) -> Vec<(Bytes, f64)> {
    let mut args: Vec<Vec<u8>> = vec![key.to_vec()];
    let mut want = Vec::new();
    for i in 0..n {
        // Scores with ties and a fractional part, members of mixed length.
        let score = ((i * 7919) % 97) as f64 / 4.0;
        let member = format!("member:{i}:{}", "x".repeat(i % 5));
        args.push(score.to_string().into_bytes());
        args.push(member.clone().into_bytes());
        want.push((Bytes::from(member), score));
    }
    let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
    zadd(db, &frames(&refs));
    want.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    want
}

fn run(db: &mut Database, args: &[&[u8]]) -> Frame {
    let f = frames(args);
    let ro = zrandmember_readonly(db, &f, 0);
    // The mutable path must be the same code; for a random reply only the
    // SHAPE can be compared, so compare lengths.
    let rw = zrandmember(db, &f);
    match (&ro, &rw) {
        (Frame::Array(a), Frame::Array(b)) => assert_eq!(a.len(), b.len()),
        (Frame::BulkString(_), Frame::BulkString(_)) => {}
        (a, b) => assert_eq!(a, b),
    }
    ro
}

fn members_of(f: &Frame, withscores: bool) -> Vec<(Bytes, Option<Bytes>)> {
    let Frame::Array(items) = f else {
        panic!("expected array, got {f:?}");
    };
    let items: Vec<Bytes> = items
        .iter()
        .map(|x| match x {
            Frame::BulkString(b) => b.clone(),
            other => panic!("not a bulk: {other:?}"),
        })
        .collect();
    if withscores {
        items
            .chunks(2)
            .map(|c| (c[0].clone(), Some(c[1].clone())))
            .collect()
    } else {
        items.into_iter().map(|m| (m, None)).collect()
    }
}

#[test]
fn zrandmember_contract_on_both_encodings() {
    // 300 members: B+tree. 20 members: listpack (insertion-ordered storage).
    for n in [300usize, 20] {
        let mut db = Database::new();
        let want = load(&mut db, b"z", n);
        let score_of = |m: &Bytes| want.iter().find(|(w, _)| w == m).map(|(_, s)| *s);

        // Single draws: always a member; every member eventually drawn.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..(n * 40) {
            match run(&mut db, &[b"z"]) {
                Frame::BulkString(m) => {
                    assert!(score_of(&m).is_some(), "drew a non-member {m:?}");
                    seen.insert(m);
                }
                other => panic!("single draw: {other:?}"),
            }
        }
        assert_eq!(seen.len(), n, "n={n}: some member was never drawn");

        for &k in &[1usize, 2, 7, n / 2, n - 1] {
            for withscores in [false, true] {
                let k_s = k.to_string();
                let mut args: Vec<&[u8]> = vec![b"z", k_s.as_bytes()];
                if withscores {
                    args.push(b"WITHSCORES");
                }
                let got = members_of(&run(&mut db, &args), withscores);
                assert_eq!(got.len(), k, "n={n} count {k}");
                let distinct: std::collections::HashSet<_> =
                    got.iter().map(|(m, _)| m.clone()).collect();
                assert_eq!(
                    distinct.len(),
                    k,
                    "n={n} count {k}: duplicates in a positive count"
                );
                for (m, s) in &got {
                    let score = score_of(m).expect("member");
                    if let Some(s) = s {
                        assert_eq!(s, &format_score_bytes(score), "WITHSCORES score for {m:?}");
                    }
                }
            }
        }

        // count >= size: the whole zset, in score order (Redis CASE 2).
        for k in [n, n + 1, 10 * n] {
            let k_s = k.to_string();
            let got = members_of(&run(&mut db, &[b"z", k_s.as_bytes(), b"WITHSCORES"]), true);
            let expect: Vec<_> = want
                .iter()
                .map(|(m, s)| (m.clone(), Some(format_score_bytes(*s))))
                .collect();
            assert_eq!(got, expect, "n={n} count {k}: whole zset in score order");
        }

        // Negative count: exactly |count| draws, repeats allowed.
        for k in [1usize, 5, 3 * n] {
            let k_s = format!("-{k}");
            let got = members_of(&run(&mut db, &[b"z", k_s.as_bytes()]), false);
            assert_eq!(got.len(), k, "n={n} count -{k}");
            assert!(got.iter().all(|(m, _)| score_of(m).is_some()));
        }
        assert_eq!(
            run(&mut db, &[b"z", b"0"]),
            Frame::Array(crate::framevec![])
        );
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

/// One member out of 200K must cost about what one member out of 2K costs:
/// O(log N) per pick. HEAD `935c555` collected every `(member, score)` pair
/// first — a ~100x ratio here; the bound is 8x.
#[test]
fn zrandmember_does_not_scale_with_n() {
    let mut small = Database::new();
    let mut big = Database::new();
    load(&mut small, b"z", 2_000);
    load(&mut big, b"z", 200_000);
    for args in [
        frames(&[b"z"]),
        frames(&[b"z", b"5", b"WITHSCORES"]),
        frames(&[b"z", b"-5"]),
    ] {
        let t_small = best_ns(7, 30, || {
            std::hint::black_box(zrandmember_readonly(&small, std::hint::black_box(&args), 0));
        });
        let t_big = best_ns(7, 30, || {
            std::hint::black_box(zrandmember_readonly(&big, std::hint::black_box(&args), 0));
        });
        let ratio = t_big as f64 / t_small.max(1) as f64;
        assert!(
            ratio < 8.0,
            "ZRANDMEMBER {:?}: 200K took {ratio:.1}x the 2K time ({t_big} ns vs {t_small} ns)",
            args.len()
        );
    }
}
