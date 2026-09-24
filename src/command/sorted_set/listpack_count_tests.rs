//! moon#1174 §4 — ZRANK / ZREVRANK / ZCOUNT / ZLEXCOUNT on a LISTPACK zset
//! answer with one borrowed counting pass: no `entries_sorted` (decode every
//! pair, parse every score, sort, allocate), same answers.

use bytes::Bytes;
use ordered_float::OrderedFloat;

use super::*;
use crate::storage::Database;
use crate::storage::db::SortedSetRef;
use crate::storage::db_read::take_entries_sorted_calls;

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
    fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[(self.next() % xs.len() as u64) as usize]
    }
}

const SCORES: [&str; 9] = ["-inf", "-3", "-0", "0", "0.5", "1", "2.25", "1e10", "+inf"];
/// Integer-looking members too, which a listpack stores as integer entries.
const MEMBERS: [&str; 12] = [
    "a", "b", "ab", "zz", "7", "70", "-1", "10", "m1", "m2", "", "Z",
];

#[test]
fn listpack_rank_and_counts_match_the_oracle_without_sorting() {
    let mut rng = Rng(0x1174);
    for _ in 0..60 {
        let mut db = Database::new();
        let mut model: Vec<(OrderedFloat<f64>, Vec<u8>)> = Vec::new();
        for m in MEMBERS {
            if rng.next() % 4 == 0 {
                continue;
            }
            let s = *rng.pick(&SCORES);
            zadd(&mut db, &frames(&[b"z", s.as_bytes(), m.as_bytes()]));
            let v: f64 = match s {
                "-inf" => f64::NEG_INFINITY,
                "+inf" => f64::INFINITY,
                x => x.parse().unwrap(),
            };
            model.push((OrderedFloat(v), m.as_bytes().to_vec()));
        }
        if model.is_empty() {
            continue;
        }
        assert!(matches!(
            db.get_sorted_set_ref_if_alive(b"z", 0),
            Ok(Some(SortedSetRef::Listpack(_)))
        ));
        model.sort();
        let _ = take_entries_sorted_calls();

        for (rank, (score, m)) in model.iter().enumerate() {
            let want = Frame::Integer(rank as i64);
            assert_eq!(
                zrank_readonly(&db, &frames(&[b"z", m]), 0),
                want,
                "ZRANK {m:?}"
            );
            let rev = Frame::Integer((model.len() - 1 - rank) as i64);
            assert_eq!(
                zrevrank_readonly(&db, &frames(&[b"z", m]), 0),
                rev,
                "ZREVRANK {m:?}"
            );
            let with = zrank_readonly(&db, &frames(&[b"z", m, b"WITHSCORE"]), 0);
            assert_eq!(
                with,
                Frame::Array(crate::framevec![
                    Frame::Integer(rank as i64),
                    Frame::Double(score.0)
                ])
            );
        }
        assert_eq!(
            zrank_readonly(&db, &frames(&[b"z", b"absent"]), 0),
            Frame::Null
        );

        for min in ["-inf", "(0", "0", "-0", "(-3", "1", "+inf"] {
            for max in ["+inf", "(1", "2.25", "0", "(1e10", "-inf"] {
                let lo = parse_score_bound(min.as_bytes()).unwrap();
                let hi = parse_score_bound(max.as_bytes()).unwrap();
                let want = model
                    .iter()
                    .filter(|(s, _)| lo.includes(s.0) && hi.includes_upper(s.0))
                    .count();
                let got = zcount_readonly(&db, &frames(&[b"z", min.as_bytes(), max.as_bytes()]), 0);
                assert_eq!(got, Frame::Integer(want as i64), "ZCOUNT {min} {max}");
            }
        }
        for min in ["-", "[a", "(a", "[7", "(m1", "[Z"] {
            for max in ["+", "[b", "(zz", "[10", "(m2"] {
                let lo = parse_lex_bound(min.as_bytes()).unwrap();
                let hi = parse_lex_bound(max.as_bytes()).unwrap();
                let want = model
                    .iter()
                    .filter(|(_, m)| lex_in_range(m, &lo, &hi))
                    .count();
                let got =
                    zlexcount_readonly(&db, &frames(&[b"z", min.as_bytes(), max.as_bytes()]), 0);
                assert_eq!(got, Frame::Integer(want as i64), "ZLEXCOUNT {min} {max}");
            }
        }
        assert_eq!(
            take_entries_sorted_calls(),
            0,
            "a listpack rank/count materialized and sorted the zset"
        );
    }
}
