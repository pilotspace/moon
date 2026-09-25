//! moon#1173: LREM is one compaction pass and LPOS scans in place.
//!
//! Three kinds of guard, per CONVENTIONS "red-suite shape":
//! * randomized equivalence against a naive oracle, on BOTH encodings, for every
//!   count sign and every RANK/COUNT/MAXLEN combination -- the rewrite must be a
//!   cheaper route to the same answer, not a different answer;
//! * behavioural wall-time bounds that the O(N*K) LREM and the copy-everything
//!   LPOS cannot meet (13.1 s and 38.9 ms in release on HEAD 935c555);
//! * redis 7.0.15 parity for the LPOS option errors this rewrite touches.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use super::list_compact::lrem_deque;
use super::{lpos, lpos_readonly, lrange_readonly, lrem, rpush};

fn bs(s: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(s))
}

fn encoding_of(db: &mut Database, key: &[u8]) -> String {
    match crate::command::key::object(db, &[bs(b"ENCODING"), bs(key)]) {
        Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        Frame::Null => "<missing>".to_owned(),
        other => panic!("OBJECT ENCODING answered {other:?}"),
    }
}

fn assert_ledger_exact(db: &mut Database, step: &str) {
    let running = db.estimated_memory();
    db.recalculate_memory();
    assert_eq!(running, db.estimated_memory(), "{step}: ledger drifted");
}

/// The list's elements through the shared-read path (never flattens).
fn elements(db: &Database, key: &[u8]) -> Vec<Vec<u8>> {
    match lrange_readonly(db, &[bs(key), bs(b"0"), bs(b"-1")], 0) {
        Frame::Array(items) => items
            .iter()
            .map(|f| match f {
                Frame::BulkString(b) => b.to_vec(),
                other => panic!("expected bulk, got {other:?}"),
            })
            .collect(),
        other => panic!("LRANGE answered {other:?}"),
    }
}

/// Build `key` from `values`, one RPUSH per chunk of 100 so a long list takes
/// the linkedlist encoding and a short one the listpack.
fn load(db: &mut Database, key: &[u8], values: &[Vec<u8>]) {
    for chunk in values.chunks(100) {
        let mut args = Vec::with_capacity(chunk.len() + 1);
        args.push(bs(key));
        args.extend(chunk.iter().map(|v| bs(v)));
        rpush(db, &args);
    }
}

struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Values that exercise the canonical-integer rule (`7` is integer-encoded in
/// a listpack; `07`, `+7`, `-0` are strings that must not answer to it).
fn alphabet() -> Vec<Vec<u8>> {
    vec![
        b"a".to_vec(),
        b"b".to_vec(),
        b"7".to_vec(),
        b"07".to_vec(),
        b"+7".to_vec(),
        b"-0".to_vec(),
        b"".to_vec(),
    ]
}

fn lrem_oracle(list: &[Vec<u8>], value: &[u8], count: i64) -> (Vec<Vec<u8>>, usize) {
    let max = if count == 0 {
        usize::MAX
    } else {
        count.unsigned_abs() as usize
    };
    let mut hits: Vec<usize> = (0..list.len()).filter(|&i| list[i] == value).collect();
    if count < 0 {
        hits.reverse();
    }
    hits.truncate(max);
    let kept = (0..list.len())
        .filter(|i| !hits.contains(i))
        .map(|i| list[i].clone())
        .collect();
    (kept, hits.len())
}

/// Every index `LPOS` should report, from the redis documentation.
fn lpos_oracle(
    list: &[Vec<u8>],
    value: &[u8],
    rank: i64,
    count: Option<usize>,
    maxlen: usize,
) -> Frame {
    let len = list.len();
    let order: Vec<usize> = if rank > 0 {
        (0..len).collect()
    } else {
        (0..len).rev().collect()
    };
    let limit = if maxlen == 0 { len } else { maxlen.min(len) };
    let want = match count {
        Some(0) => usize::MAX,
        Some(c) => c,
        None => 1,
    };
    let hits: Vec<i64> = order[..limit]
        .iter()
        .filter(|&&i| list[i] == value)
        .skip(rank.unsigned_abs() as usize - 1)
        .take(want)
        .map(|&i| i as i64)
        .collect();
    match count {
        Some(_) => Frame::Array(hits.into_iter().map(Frame::Integer).collect()),
        None => hits.first().copied().map_or(Frame::Null, Frame::Integer),
    }
}

/// The deque compaction on its own, against the oracle, every count sign.
#[test]
fn lrem_deque_agrees_with_the_naive_oracle() {
    let alpha = alphabet();
    let mut rng = XorShift(0xD1B5_4A32_D192_ED03);
    for round in 0..2000 {
        let n = rng.below(30) as usize;
        let list: Vec<Vec<u8>> = (0..n)
            .map(|_| alpha[rng.below(alpha.len() as u64) as usize].clone())
            .collect();
        let probe = alpha[rng.below(alpha.len() as u64) as usize].clone();
        let count = rng.below(11) as i64 - 5;
        let (want, want_removed) = lrem_oracle(&list, &probe, count);
        let mut dq: std::collections::VecDeque<Bytes> =
            list.iter().map(|v| Bytes::copy_from_slice(v)).collect();
        let max = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let removed = lrem_deque(&mut dq, &probe, count < 0, max);
        let got: Vec<Vec<u8>> = dq.iter().map(|b| b.to_vec()).collect();
        assert_eq!(
            (got, removed),
            (want, want_removed),
            "round {round}: LREM {count} {:?} on {list:?}",
            String::from_utf8_lossy(&probe)
        );
    }
}

/// The LREM handler on BOTH encodings: same answer as the oracle, the
/// listpack stays a listpack (moon#1174 §1), the ledger stays exact, and a
/// list emptied by LREM loses its key.
#[test]
fn lrem_handler_agrees_with_the_oracle_on_both_encodings() {
    let alpha = alphabet();
    let mut rng = XorShift(0x2545_F491_4F6C_DD1D);
    for round in 0..300 {
        // Short lists are listpacks; 129..=180 elements are linkedlists.
        let big = round % 3 == 0;
        let n = if big {
            129 + rng.below(52) as usize
        } else {
            1 + rng.below(40) as usize
        };
        let list: Vec<Vec<u8>> = (0..n)
            .map(|_| alpha[rng.below(alpha.len() as u64) as usize].clone())
            .collect();
        let probe = alpha[rng.below(alpha.len() as u64) as usize].clone();
        let count = rng.below(9) as i64 - 4;
        let (want, want_removed) = lrem_oracle(&list, &probe, count);

        let mut db = Database::new();
        load(&mut db, b"l", &list);
        let enc = encoding_of(&mut db, b"l");
        assert_eq!(enc, if big { "linkedlist" } else { "listpack" }, "fixture");
        let got = lrem(
            &mut db,
            &[bs(b"l"), bs(count.to_string().as_bytes()), bs(&probe)],
        );
        assert_eq!(got, Frame::Integer(want_removed as i64), "round {round}");
        if want.is_empty() {
            assert!(db.data().get(b"l").is_none(), "round {round}: key must go");
        } else {
            assert_eq!(elements(&db, b"l"), want, "round {round}: survivors");
            assert_eq!(
                encoding_of(&mut db, b"l"),
                enc,
                "round {round}: LREM changed the encoding (moon#1174 §1)"
            );
        }
        assert_ledger_exact(&mut db, "after LREM");
    }
}

/// LPOS on both encodings, every RANK sign, COUNT and MAXLEN shape.
#[test]
fn lpos_agrees_with_the_oracle_on_both_encodings() {
    let alpha = alphabet();
    let mut rng = XorShift(0x0123_4567_89AB_CDEF);
    for round in 0..200 {
        let big = round % 4 == 0;
        let n = if big {
            129 + rng.below(40) as usize
        } else {
            rng.below(30) as usize + 1
        };
        let list: Vec<Vec<u8>> = (0..n)
            .map(|_| alpha[rng.below(alpha.len() as u64) as usize].clone())
            .collect();
        let mut db = Database::new();
        load(&mut db, b"l", &list);
        for probe in &alpha {
            for rank in [1i64, 2, 3, -1, -2, -4] {
                for count in [None, Some(0usize), Some(1), Some(2), Some(5)] {
                    for maxlen in [0usize, 1, 3, n, n + 7] {
                        let mut args = vec![bs(b"l"), bs(probe)];
                        args.push(bs(b"RANK"));
                        args.push(bs(rank.to_string().as_bytes()));
                        if let Some(c) = count {
                            args.push(bs(b"COUNT"));
                            args.push(bs(c.to_string().as_bytes()));
                        }
                        args.push(bs(b"MAXLEN"));
                        args.push(bs(maxlen.to_string().as_bytes()));
                        let want = lpos_oracle(&list, probe, rank, count, maxlen);
                        assert_eq!(
                            lpos_readonly(&db, &args, 0),
                            want,
                            "round {round}: LPOS {:?} RANK {rank} COUNT {count:?} MAXLEN {maxlen}",
                            String::from_utf8_lossy(probe)
                        );
                    }
                }
            }
        }
    }
    // A miss is Null bare and an empty array with COUNT.
    let db = Database::new();
    assert_eq!(lpos_readonly(&db, &[bs(b"nope"), bs(b"a")], 0), Frame::Null);
    assert_eq!(
        lpos_readonly(&db, &[bs(b"nope"), bs(b"a"), bs(b"COUNT"), bs(b"0")], 0),
        Frame::Array(framevec![])
    );
}

/// The LPOS option errors, byte-for-byte as redis 7.0.15 answers them.
#[test]
fn lpos_option_errors_match_redis() {
    let mut db = Database::new();
    rpush(&mut db, &[bs(b"l"), bs(b"a"), bs(b"b"), bs(b"a")]);
    let err = |args: &[&[u8]]| -> Vec<u8> {
        let frames: Vec<Frame> = args.iter().map(|a| bs(a)).collect();
        match lpos_readonly(&db, &frames, 0) {
            Frame::Error(e) => e.to_vec(),
            other => panic!("LPOS {args:?} answered {other:?}"),
        }
    };
    // The OPTION is decided before its value is parsed.
    assert_eq!(err(&[b"l", b"a", b"FOO", b"bar"]), b"ERR syntax error");
    assert_eq!(err(&[b"l", b"a", b"FOO", b"1"]), b"ERR syntax error");
    assert_eq!(err(&[b"l", b"a", b"RANK"]), b"ERR syntax error");
    assert_eq!(
        err(&[b"l", b"a", b"RANK", b"1", b"COUNT"]),
        b"ERR syntax error"
    );
    assert_eq!(
        err(&[b"l", b"a", b"RANK", b"x"]),
        b"ERR value is not an integer or out of range"
    );
    assert_eq!(
        err(&[b"l", b"a", b"RANK", b"0"]),
        &b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... \
           or use negative to start from the end of the list"[..]
    );
    for bad in [&b"-1"[..], b"abc"] {
        assert_eq!(
            err(&[b"l", b"a", b"COUNT", bad]),
            b"ERR COUNT can't be negative"
        );
        assert_eq!(
            err(&[b"l", b"a", b"MAXLEN", bad]),
            b"ERR MAXLEN can't be negative"
        );
    }
    // The extreme RANK must not overflow (a debug-build panic on HEAD).
    assert_eq!(
        lpos_readonly(
            &db,
            &[bs(b"l"), bs(b"a"), bs(b"RANK"), bs(b"-9223372036854775808")],
            0
        ),
        Frame::Null
    );
    // WRONGTYPE is unchanged.
    crate::command::string::set(&mut db, &[bs(b"s"), bs(b"v")]);
    assert!(matches!(
        lpos(&mut db, &[bs(b"s"), bs(b"a")]),
        Frame::Error(e) if e.starts_with(b"WRONGTYPE")
    ));
}

/// A 200K-element linkedlist, half of it matching.
fn big_ab_list(db: &mut Database) {
    let pair = [b"a".to_vec(), b"b".to_vec()];
    let chunk: Vec<Vec<u8>> = pair.iter().cycle().take(1000).cloned().collect();
    for _ in 0..200 {
        load(db, b"l", &chunk);
    }
}

/// moon#1173: `LREM l 0 a` on a 200K list with 100K matches.
///
/// HEAD 935c555 shifted up to half the list per match: 13.1 s in RELEASE.
/// One compaction pass is ~200K compares; the bound is two orders of
/// magnitude above that in a debug build, and two orders below the old cost.
#[test]
fn lrem_on_a_200k_list_is_one_pass() {
    let mut db = Database::new();
    big_ab_list(&mut db);
    let t = std::time::Instant::now();
    let got = lrem(&mut db, &[bs(b"l"), bs(b"0"), bs(b"a")]);
    let took = t.elapsed();
    assert_eq!(got, Frame::Integer(100_000));
    assert!(
        took < std::time::Duration::from_secs(2),
        "LREM of 100K matches from a 200K list took {took:?} -- quadratic again?"
    );
    assert_eq!(
        crate::command::list::llen_readonly(&db, &[bs(b"l")], 0),
        Frame::Integer(100_000)
    );
    assert_ledger_exact(&mut db, "after the big LREM");

    // A negative count stops at |count| matches from the tail.
    let t = std::time::Instant::now();
    assert_eq!(
        lrem(&mut db, &[bs(b"l"), bs(b"-3"), bs(b"b")]),
        Frame::Integer(3)
    );
    assert!(t.elapsed() < std::time::Duration::from_secs(1));
}

/// moon#1173: `LPOS l a MAXLEN 10` must cost O(MAXLEN), not O(len).
///
/// HEAD cloned all 200K elements per call before looking at MAXLEN.
#[test]
fn lpos_with_maxlen_does_not_copy_the_list() {
    let mut db = Database::new();
    big_ab_list(&mut db);
    let args = [bs(b"l"), bs(b"b"), bs(b"MAXLEN"), bs(b"10")];
    let t = std::time::Instant::now();
    for _ in 0..500 {
        assert_eq!(lpos_readonly(&db, &args, 0), Frame::Integer(1));
    }
    let took = t.elapsed();
    assert!(
        took < std::time::Duration::from_millis(250),
        "500 LPOS MAXLEN 10 on a 200K list took {took:?} -- copying the list again?"
    );
    // RANK from the tail with MAXLEN scans only the last MAXLEN elements.
    let args = [
        bs(b"l"),
        bs(b"a"),
        bs(b"RANK"),
        bs(b"-1"),
        bs(b"MAXLEN"),
        bs(b"10"),
    ];
    assert_eq!(lpos_readonly(&db, &args, 0), Frame::Integer(199_998));
}
