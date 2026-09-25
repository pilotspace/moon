//! moon#1226 (lists / listpack residuals of the moon#1221 review).
//!
//! * WATCH parity: an `LREM` that removes nothing and an `LINSERT` whose pivot
//!   is missing are not writes in redis 7.0.15 (`signalModifiedKey` runs only
//!   when something was removed / inserted), so a `WATCH`ing `EXEC` still
//!   runs. Taking the mutable list handle IS moon's WATCH version bump
//!   (moon#926), and both commands took it before looking.

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

fn run(db: &mut Database, parts: &[&str]) -> Frame {
    let args: Vec<Frame> = parts[1..]
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
        .collect();
    let mut selected = 0usize;
    match crate::command::dispatch(db, parts[0].as_bytes(), &args, &mut selected, 16) {
        crate::command::DispatchResult::Response(f) | crate::command::DispatchResult::Quit(f) => f,
    }
}

fn encoding(db: &mut Database, key: &str) -> String {
    match run(db, &["OBJECT", "ENCODING", key]) {
        Frame::BulkString(b) => String::from_utf8_lossy(&b).into_owned(),
        other => panic!("OBJECT ENCODING {key} answered {other:?}"),
    }
}

/// The key's WATCH version (moon#926: one bump per mutable handle).
fn version(db: &Database, key: &str) -> u32 {
    db.data()
        .get(key.as_bytes())
        .map(|e| e.version())
        .unwrap_or_else(|| panic!("{key} should exist"))
}

/// A list on each encoding: a small listpack and a `linkedlist` (one element
/// past the 64-byte listpack policy).
fn fixtures(db: &mut Database) {
    run(db, &["RPUSH", "lp", "a", "b", "c"]);
    let wide = "w".repeat(80);
    run(db, &["RPUSH", "ll", "a", "b", "c", &wide]);
    assert_eq!(encoding(db, "lp"), "listpack", "fixture");
    assert_eq!(encoding(db, "ll"), "linkedlist", "fixture");
}

#[test]
fn a_no_op_lrem_or_linsert_does_not_bump_the_watch_version() {
    let mut db = Database::new();
    fixtures(&mut db);
    for key in ["lp", "ll"] {
        let enc = encoding(&mut db, key);
        let v0 = version(&db, key);
        assert_eq!(run(&mut db, &["LREM", key, "0", "zz"]), Frame::Integer(0));
        assert_eq!(run(&mut db, &["LREM", key, "-2", "zz"]), Frame::Integer(0));
        assert_eq!(
            run(&mut db, &["LINSERT", key, "BEFORE", "zz", "x"]),
            Frame::Integer(-1)
        );
        assert_eq!(
            version(&db, key),
            v0,
            "{key} ({enc}): an LREM that removed nothing / an LINSERT with no pivot \
             moved the WATCH version, so a WATCHing EXEC would abort where redis \
             7.0.15 runs it"
        );
        assert_eq!(
            encoding(&mut db, key),
            enc,
            "{key}: a no-op kept its encoding"
        );
        // Controls: the same commands that DO write still bump.
        assert_eq!(
            run(&mut db, &["LINSERT", key, "AFTER", "a", "x"]),
            Frame::Integer(if key == "lp" { 4 } else { 5 })
        );
        let v1 = version(&db, key);
        assert!(v1 > v0, "{key}: a real LINSERT bumps the version");
        assert_eq!(run(&mut db, &["LREM", key, "1", "x"]), Frame::Integer(1));
        assert!(
            version(&db, key) > v1,
            "{key}: a real LREM bumps the version"
        );
    }
    // A missing key and a wrong type answer as before.
    assert_eq!(
        run(&mut db, &["LREM", "nokey", "0", "a"]),
        Frame::Integer(0)
    );
    assert_eq!(
        run(&mut db, &["LINSERT", "nokey", "BEFORE", "a", "x"]),
        Frame::Integer(0)
    );
    run(&mut db, &["SET", "s", "v"]);
    assert!(matches!(
        run(&mut db, &["LREM", "s", "0", "a"]),
        Frame::Error(e) if e.starts_with(b"WRONGTYPE")
    ));
    assert!(matches!(
        run(&mut db, &["LINSERT", "s", "BEFORE", "a", "x"]),
        Frame::Error(e) if e.starts_with(b"WRONGTYPE")
    ));
}
