//! moon#1283: `MOON.TS` records and the pseudo-command intercept.

use std::time::{Duration, SystemTime};

use bytes::Bytes;

use super::{CLOSE_RECORD_MAX_LEN, Pseudo, TS_RECORD_MAX_LEN, TsRecord, classify, intercept};
use crate::persistence::replay::clock::{
    observe_log_ts, pin_replay_clock_ms, pinned_replay_clock_ms,
};
use crate::persistence::replay::{CommandReplayEngine, DispatchReplayEngine, ReplayRoute};
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::current_time_ms;

fn bulk(b: &[u8]) -> Frame {
    Frame::BulkString(Bytes::copy_from_slice(b))
}

fn resp(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

fn parse_one(bytes: &[u8]) -> Vec<Frame> {
    let mut buf = bytes::BytesMut::from(bytes);
    let frame = crate::protocol::parse::parse(&mut buf, &crate::protocol::ParseConfig::default())
        .expect("parse")
        .expect("a whole frame");
    assert!(buf.is_empty(), "exactly one frame");
    match frame {
        Frame::Array(arr) => arr.to_vec(),
        other => panic!("not an array: {other:?}"),
    }
}

#[test]
fn a_ts_record_is_a_plain_resp_array_every_reader_parses() {
    for ms in [1u64, 1_790_000_000_123, super::MAX_TS_MS, u64::MAX] {
        let rec = TsRecord::new(ms);
        assert_eq!(
            rec.as_bytes(),
            resp(&[b"MOON.TS", ms.to_string().as_bytes()])
        );
        let arr = parse_one(rec.as_bytes());
        let want = if ms <= super::MAX_TS_MS {
            Pseudo::Ts(ms)
        } else {
            Pseudo::MalformedTs
        };
        assert_eq!(classify(b"MOON.TS", &arr[1..]), Some(want));
    }
    assert_eq!(TsRecord::new(u64::MAX).as_bytes().len(), TS_RECORD_MAX_LEN);
}

/// R2 review of moon#1283: the clean-close marker is `MOON.TS <ms> CLOSE`,
/// a plain three-element array; a well-formed one classifies as
/// [`Pseudo::Close`], anything else with extra arguments stays malformed
/// (skipped, the clock unmoved).
#[test]
fn a_close_marker_is_a_three_element_ts_record() {
    for ms in [1u64, 1_790_000_000_123, super::MAX_TS_MS, u64::MAX] {
        let rec = TsRecord::close(ms);
        assert_eq!(
            rec.as_bytes(),
            resp(&[b"MOON.TS", ms.to_string().as_bytes(), b"CLOSE"])
        );
        let arr = parse_one(rec.as_bytes());
        let want = if ms <= super::MAX_TS_MS {
            Pseudo::Close(ms)
        } else {
            Pseudo::MalformedTs
        };
        assert_eq!(classify(b"MOON.TS", &arr[1..]), Some(want));
        assert_eq!(want.route(), ReplayRoute::Marker);
    }
    assert_eq!(
        TsRecord::close(u64::MAX).as_bytes().len(),
        CLOSE_RECORD_MAX_LEN
    );
    assert_eq!(
        classify(b"MOON.TS", &[bulk(b"5"), bulk(b"close")]),
        Some(Pseudo::Close(5))
    );
    for bad in [
        &[bulk(b"0"), bulk(b"CLOSE")][..],
        &[bulk(b"5"), bulk(b"OPEN")][..],
        &[bulk(b"5"), bulk(b"CLOSE"), bulk(b"x")][..],
        &[bulk(b"5"), Frame::Integer(1)][..],
    ] {
        assert_eq!(
            classify(b"MOON.TS", bad),
            Some(Pseudo::MalformedTs),
            "{bad:?}"
        );
    }
}

#[test]
fn classify_knows_the_moon_records_and_nothing_else() {
    let ms = [bulk(b"123")];
    assert_eq!(classify(b"MOON.TS", &ms), Some(Pseudo::Ts(123)));
    assert_eq!(classify(b"moon.ts", &ms), Some(Pseudo::Ts(123)));
    assert_eq!(
        classify(b"MOON.TS", &[Frame::Integer(7)]),
        Some(Pseudo::Ts(7))
    );
    for bad in [
        &[][..],
        &[bulk(b"0")][..],
        &[bulk(b"-5")][..],
        &[bulk(b"12x")][..],
        &[bulk(b"1"), bulk(b"2")][..],
        &[Frame::Null][..],
    ] {
        assert_eq!(
            classify(b"MOON.TS", bad),
            Some(Pseudo::MalformedTs),
            "{bad:?}"
        );
    }
    assert_eq!(
        classify(b"MOON.COLDCUT", &[bulk(b"1")]),
        Some(Pseudo::ColdPlane)
    );
    assert_eq!(classify(b"MOON.SPILLED", &[]), Some(Pseudo::ColdPlane));
    // Data, and a MOON.* name this build does not know: not intercepted.
    assert_eq!(classify(b"SET", &ms), None);
    assert_eq!(classify(b"MOON.", &ms), None);
    assert_eq!(classify(b"MOON", &ms), None);
    assert_eq!(classify(b"MOON.TSX", &ms), None);
    assert_eq!(classify(b"", &[]), None);
}

/// The clock is the LAST stamp read (never a running maximum), only inside a
/// replay's pin scope, and every scope — every replayed file — starts back
/// on its own pin (the file's mtime) until its first stamp.
#[test]
fn the_judgment_clock_is_the_last_stamp_of_the_current_file() {
    let mut dbs = vec![Database::new()];
    // No replay scope open: a stamp changes nothing.
    assert_eq!(
        intercept(&mut dbs, b"MOON.TS", &[bulk(b"500")], 0),
        Some(ReplayRoute::Marker)
    );
    assert_eq!(pinned_replay_clock_ms(), None);
    {
        let _file_a = pin_replay_clock_ms(1_000);
        assert_eq!(
            pinned_replay_clock_ms(),
            Some(1_000),
            "mtime until the first stamp"
        );
        observe_log_ts(9_000);
        assert_eq!(pinned_replay_clock_ms(), Some(9_000));
        observe_log_ts(4_000);
        assert_eq!(pinned_replay_clock_ms(), Some(4_000), "last, not max");
        // A malformed or zero stamp leaves it where it was.
        let _ = intercept(&mut dbs, b"MOON.TS", &[bulk(b"0")], 0);
        let _ = intercept(&mut dbs, b"MOON.TS", &[bulk(b"x")], 0);
        assert_eq!(pinned_replay_clock_ms(), Some(4_000));
        {
            let _file_b = pin_replay_clock_ms(2_000);
            assert_eq!(
                pinned_replay_clock_ms(),
                Some(2_000),
                "a new file: its own pin"
            );
            observe_log_ts(7_000);
            assert_eq!(pinned_replay_clock_ms(), Some(7_000));
        }
        assert_eq!(
            pinned_replay_clock_ms(),
            Some(4_000),
            "restored on scope exit"
        );
    }
    assert_eq!(pinned_replay_clock_ms(), None);
    // A scope with no readable mtime (pin 0) still honours its stamps.
    {
        let _file = pin_replay_clock_ms(0);
        assert_eq!(pinned_replay_clock_ms(), None);
        observe_log_ts(3_000);
        assert_eq!(pinned_replay_clock_ms(), Some(3_000));
    }
    assert_eq!(pinned_replay_clock_ms(), None);
}

/// Replay `records` from an `appendonly.aof` whose mtime is `mtime_ms`, via
/// the production flat-file replay.
fn replay_written_at(records: &[Vec<u8>], mtime_ms: u64) -> Database {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("appendonly.aof");
    std::fs::write(&path, records.concat()).expect("write aof");
    std::fs::File::options()
        .write(true)
        .open(&path)
        .expect("open aof")
        .set_modified(SystemTime::UNIX_EPOCH + Duration::from_millis(mtime_ms))
        .expect("set mtime");
    let mut dbs = vec![Database::new()];
    crate::persistence::aof::replay_aof(&mut dbs, &path, &DispatchReplayEngine::new())
        .expect("replay");
    assert_eq!(
        pinned_replay_clock_ms(),
        None,
        "the replay leaked its clock"
    );
    dbs.remove(0)
}

fn raw(db: &Database, key: &[u8]) -> Option<(Vec<u8>, u64)> {
    let e = db.data().get(key)?;
    Some((
        e.value
            .as_bytes_owned()
            .map(|b| b.to_vec())
            .unwrap_or_default(),
        e.expires_at_ms(),
    ))
}

/// The touchback shape in one file: `n` expired while the server ran and an
/// `INCR` rebuilt it (live value 1, no TTL, no `DEL` logged in between —
/// moon#542). The file's mtime is an hour BEFORE its writes. Without stamps
/// the replay judges `n` alive at the INCR (6, then gone at its deadline);
/// the `MOON.TS` before the INCR says it had expired.
#[test]
fn a_stamp_judges_a_record_by_its_own_write_time_not_the_mtime() {
    let now = current_time_ms();
    let deadline = now - 60_000;
    let d = deadline.to_string();
    let set = resp(&[b"SET", b"n", b"5", b"PXAT", d.as_bytes()]);
    let incr = resp(&[b"INCR", b"n"]);
    let before = TsRecord::new(deadline - 100).as_bytes().to_vec();
    let after = TsRecord::new(deadline + 5).as_bytes().to_vec();
    let mtime = now - 3_600_000;

    // No stamps (an older binary's log): the mtime rules, exactly as before.
    let db = replay_written_at(&[set.clone(), incr.clone()], mtime);
    assert_eq!(raw(&db, b"n"), Some((b"6".to_vec(), deadline)));

    // Stamped: the INCR ran after the deadline.
    let db = replay_written_at(
        &[before.clone(), set.clone(), after.clone(), incr.clone()],
        mtime,
    );
    assert_eq!(
        raw(&db, b"n"),
        Some((b"1".to_vec(), 0)),
        "the INCR must replay onto an expired key: a new persistent 1"
    );

    // Mixed: a stamp-less prefix (mtime judgment) then stamped records.
    let e = resp(&[b"SET", b"e", b"5", b"PXAT", d.as_bytes()]);
    let incr_e = resp(&[b"INCR", b"e"]);
    let db = replay_written_at(
        &[set.clone(), incr.clone(), e.clone(), after.clone(), incr_e],
        mtime,
    );
    assert_eq!(
        raw(&db, b"n"),
        Some((b"6".to_vec(), deadline)),
        "prefix: mtime"
    );
    assert_eq!(
        raw(&db, b"e"),
        Some((b"1".to_vec(), 0)),
        "suffix: its stamp"
    );

    // Last stamp, not max: a parked producer's older stamp puts the clock
    // back before the deadline for its record.
    let db = replay_written_at(&[before.clone(), set, after, before, incr], mtime);
    assert_eq!(raw(&db, b"n"), Some((b"6".to_vec(), deadline)));
}

/// Downgrade read: an older binary has no intercept, so `MOON.TS` reaches
/// `command::dispatch` — which answers "unknown command" and changes
/// nothing — and the replay goes on (`ReplayRoute::Unhandled`, which no
/// recovery decision counts as KV history).
#[test]
fn an_older_binary_sees_an_unknown_command_and_skips_it() {
    let mut db = Database::new();
    let mut selected = 0usize;
    let reply = crate::command::dispatch(
        &mut db,
        b"MOON.TS",
        &[bulk(b"1790000000000")],
        &mut selected,
        16,
    );
    assert!(reply.is_unknown_command());
    assert_eq!(db.data().len(), 0);
    assert!(!ReplayRoute::Unhandled.is_kv_history());
    assert!(!ReplayRoute::Marker.is_kv_history());
}

/// The engine routes a stamp through the intercept (a marker, never KV
/// history) and a data record through dispatch.
#[test]
fn the_engine_routes_stamps_as_markers() {
    let engine = DispatchReplayEngine::new();
    let mut dbs = vec![Database::new()];
    let mut sel = 0usize;
    let _file = pin_replay_clock_ms(1);
    assert_eq!(
        engine.replay_command(&mut dbs, b"MOON.TS", &[bulk(b"77")], &mut sel),
        ReplayRoute::Marker
    );
    assert_eq!(pinned_replay_clock_ms(), Some(77));
    assert_eq!(
        engine.replay_command(&mut dbs, b"SET", &[bulk(b"k"), bulk(b"v")], &mut sel),
        ReplayRoute::Keyspace
    );
    assert_eq!(dbs[0].data().len(), 1);
}
