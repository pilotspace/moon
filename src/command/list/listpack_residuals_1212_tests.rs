//! moon#1212: the listpack residuals after moon#1174.
//!
//! * The blocking serve path (`Database::list_pop_*` / `list_push_*`, used by
//!   BLPOP/BRPOP/BLMOVE/BLMPOP served on the spot or on wake) flattened a small
//!   list to `linkedlist` for good.
//! * SORT read listpack elements through the OWNING iterator — a `Vec` per
//!   string, then copied again into the `Bytes` it kept.
//!
//! (`LPUSHX`/`RPUSHX` are in `mod.rs`'s `lpushx_and_rpushx_keep_a_listpack_moon1212`.)

use bytes::Bytes;

use crate::protocol::Frame;
use crate::server::conn::blocking::immediate_scan;
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

fn assert_ledger_exact(db: &mut Database, step: &str) {
    let running = db.estimated_memory();
    db.recalculate_memory();
    assert_eq!(running, db.estimated_memory(), "{step}: ledger drifted");
}

#[test]
fn the_blocking_pop_and_push_accessors_keep_a_listpack() {
    let mut db = Database::new();
    run(&mut db, &["RPUSH", "q", "a", "b", "c", "d"]);
    assert_eq!(encoding(&mut db, "q"), "listpack", "fixture");

    assert_eq!(db.list_pop_front(b"q"), Some(Bytes::from_static(b"a")));
    assert_eq!(db.list_pop_back(b"q"), Some(Bytes::from_static(b"d")));
    assert_eq!(
        encoding(&mut db, "q"),
        "listpack",
        "a served pop flattened the list"
    );
    assert_ledger_exact(&mut db, "after the pops");

    db.list_push_front(b"q", Bytes::from_static(b"z"));
    db.list_push_back(b"q", Bytes::from_static(b"y"));
    assert_eq!(
        encoding(&mut db, "q"),
        "listpack",
        "a served push flattened the list"
    );
    assert_eq!(
        run(&mut db, &["LRANGE", "q", "0", "-1"]),
        Frame::Array(
            ["z", "b", "c", "y"]
                .iter()
                .map(|s| Frame::BulkString(Bytes::copy_from_slice(s.as_bytes())))
                .collect::<Vec<_>>()
                .into()
        )
    );
    assert_ledger_exact(&mut db, "after the pushes");

    // A missing destination is born a listpack, as LPUSH makes it.
    db.list_push_back(b"dst", Bytes::from_static(b"v"));
    assert_eq!(encoding(&mut db, "dst"), "listpack");
    // Past the element policy the push promotes, exactly as RPUSH does.
    db.list_push_back(b"dst", Bytes::from(vec![b'w'; 65]));
    assert_eq!(encoding(&mut db, "dst"), "linkedlist");
    assert_ledger_exact(&mut db, "after the promoting push");

    // Draining a listpack removes the key; a missing key creates nothing.
    assert_eq!(db.list_pop_front(b"missing"), None);
    assert_eq!(run(&mut db, &["EXISTS", "missing"]), Frame::Integer(0));
    for _ in 0..4 {
        assert!(db.list_pop_back(b"q").is_some());
    }
    assert_eq!(db.list_pop_back(b"q"), None);
    assert_eq!(run(&mut db, &["EXISTS", "q"]), Frame::Integer(0));
    assert_ledger_exact(&mut db, "after draining");
}

/// The same property through the real blocking scan: BLPOP and BLMOVE
/// served on the spot keep both lists listpacks.
#[test]
fn blocking_commands_served_on_the_spot_keep_listpacks() {
    let keys = |k: &str| vec![Bytes::copy_from_slice(k.as_bytes())];
    let args = |parts: &[&str]| -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect()
    };
    let mut db = Database::new();
    run(&mut db, &["RPUSH", "src", "a", "b", "c"]);
    run(&mut db, &["RPUSH", "dst", "x"]);
    let r = immediate_scan(b"BLPOP", &args(&["src", "0"]), &keys("src"), &mut db, 0, 1);
    assert!(matches!(r, Some(Frame::Array(_))), "BLPOP served: {r:?}");
    let r = immediate_scan(
        b"BLMOVE",
        &args(&["src", "dst", "LEFT", "RIGHT", "0"]),
        &keys("src"),
        &mut db,
        0,
        1,
    );
    assert_eq!(r, Some(Frame::BulkString(Bytes::from_static(b"b"))));
    assert_eq!(
        encoding(&mut db, "src"),
        "listpack",
        "BLPOP/BLMOVE flattened the source"
    );
    assert_eq!(
        encoding(&mut db, "dst"),
        "listpack",
        "BLMOVE flattened the destination"
    );
    assert_ledger_exact(&mut db, "after the served blocking commands");
}

/// SORT over a listpack reads each element borrowed and copies it once.
#[test]
fn sort_reads_listpacks_without_owned_decodes() {
    let mut db = Database::new();
    run(&mut db, &["RPUSH", "l", "pear", "apple", "fig", "kiwi"]);
    run(&mut db, &["SADD", "s", "pear", "apple", "fig"]);
    run(
        &mut db,
        &["ZADD", "z", "3", "pear", "1", "apple", "2", "fig"],
    );
    for key in ["l", "s", "z"] {
        let mark = crate::storage::listpack::owned_decodes();
        let reply = run(&mut db, &["SORT", key, "ALPHA"]);
        assert!(matches!(reply, Frame::Array(_)), "SORT {key}: {reply:?}");
        assert_eq!(
            crate::storage::listpack::owned_decodes() - mark,
            0,
            "SORT {key} decoded listpack strings into owned Vecs (moon#1212)"
        );
    }
    let Frame::Array(items) = run(&mut db, &["SORT", "z", "ALPHA"]) else {
        panic!("SORT z");
    };
    let got: Vec<Bytes> = items
        .iter()
        .map(|f| match f {
            Frame::BulkString(b) => b.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        got,
        ["apple", "fig", "pear"].map(|s| Bytes::from_static(s.as_bytes()))
    );
}
