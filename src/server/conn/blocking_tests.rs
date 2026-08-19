//! Unit tests for the pre-registration scan a blocking pop runs before it
//! decides to block (`blocking::immediate_scan`) — moon#556.
//!
//! Deliberately NOT in `super::tests`: that module is
//! `#[cfg(all(test, feature = "runtime-monoio"))]`, so everything in it is
//! invisible to the tokio CI leg. The scan is runtime-agnostic (a plain
//! `Database`, no shard slice, no I/O), so it is tested under both runtimes.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::server::conn::blocking::immediate_scan;
use crate::storage::Database;
use crate::storage::entry::Entry;

/// Build a RESP argv (everything AFTER the command name).
fn args(parts: &[&str]) -> Vec<Frame> {
    parts
        .iter()
        .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
        .collect()
}

fn keys(parts: &[&str]) -> Vec<Bytes> {
    parts
        .iter()
        .map(|p| Bytes::copy_from_slice(p.as_bytes()))
        .collect()
}

/// Assert `reply` is a `-WRONGTYPE` error.
#[track_caller]
fn assert_wrongtype(reply: &Option<Frame>, what: &str) {
    match reply {
        Some(Frame::Error(e)) => assert!(
            e.starts_with(b"WRONGTYPE"),
            "{what}: expected WRONGTYPE, got {:?}",
            String::from_utf8_lossy(e)
        ),
        other => panic!(
            "{what}: expected a WRONGTYPE error, got {other:?} \
             (`None` means the client goes on to block)"
        ),
    }
}

/// Every blocking pop in the argv shape its parser expects, paired with the
/// key it pops from — so a newly added blocking command is one row here rather
/// than one more test.
fn blocking_pop_shapes() -> Vec<(&'static [u8], Vec<Frame>, &'static str)> {
    vec![
        (&b"BLPOP"[..], args(&["wt", "0"]), "wt"),
        (&b"BRPOP"[..], args(&["wt", "0"]), "wt"),
        (
            &b"BLMOVE"[..],
            args(&["wt", "dst", "LEFT", "LEFT", "0"]),
            "wt",
        ),
        (&b"BRPOPLPUSH"[..], args(&["wt", "dst", "0"]), "wt"),
        (&b"BLMPOP"[..], args(&["0", "1", "wt", "LEFT"]), "wt"),
        (&b"BZPOPMIN"[..], args(&["wt", "0"]), "wt"),
        (&b"BZPOPMAX"[..], args(&["wt", "0"]), "wt"),
        (&b"BZMPOP"[..], args(&["0", "1", "wt", "MIN"]), "wt"),
    ]
}

/// moon#556: a blocking pop on an EXISTING key of the wrong type answers
/// `-WRONGTYPE` immediately, as redis-server does — it must never register and
/// block.
///
/// Pre-fix the pop helpers swallowed their `Err(WRONGTYPE)` into `None`
/// (`get_mut_if_present(..).ok()??`), so the scan reported "nothing here" and
/// the client blocked on a key that can never serve it.
#[test]
fn immediate_scan_surfaces_wrongtype_instead_of_blocking() {
    for (cmd, argv, key) in blocking_pop_shapes() {
        let name = String::from_utf8_lossy(cmd).into_owned();
        let mut db = Database::new();
        // The wrong type for BOTH families: a plain string.
        db.set(
            Bytes::copy_from_slice(key.as_bytes()),
            Entry::new_string(Bytes::from_static(b"payload")),
        );
        let reply = immediate_scan(cmd, &argv, &keys(&[key]), &mut db);
        assert_wrongtype(&reply, &name);
        // moon#560's guarantee still holds: the value is untouched.
        assert_eq!(
            db.get(key.as_bytes())
                .and_then(|e| e.value.as_bytes().map(<[u8]>::to_vec)),
            Some(b"payload".to_vec()),
            "{name}: the existing value must survive a wrong-type blocking pop"
        );
        assert_eq!(
            db.logical_len(),
            1,
            "{name}: no key may be created or removed"
        );
    }
}

/// The type gate is per-family: a zset pop on a LIST key (and the reverse) is
/// the same WRONGTYPE, not a null.
#[test]
fn immediate_scan_wrongtype_across_collection_families() {
    let mut db = Database::new();
    db.list_push_back(b"l", Bytes::from_static(b"v"));
    let reply = immediate_scan(b"BZPOPMIN", &args(&["l", "0"]), &keys(&["l"]), &mut db);
    assert_wrongtype(&reply, "BZPOPMIN on a list");
    assert_eq!(
        db.list_pop_back(b"l"),
        Some(Bytes::from_static(b"v")),
        "the list must be intact"
    );

    let mut db = Database::new();
    db.zset_restore(b"z", Bytes::from_static(b"m"), 1.0);
    let reply = immediate_scan(b"BLPOP", &args(&["z", "0"]), &keys(&["z"]), &mut db);
    assert_wrongtype(&reply, "BLPOP on a zset");
    assert_eq!(
        db.zset_pop_min(b"z").map(|(m, _)| m),
        Some(Bytes::from_static(b"m")),
        "the sorted set must be intact"
    );
}

/// Redis scans keys left to right and stops at the FIRST existing key of the
/// wrong type, even when a later key could have served the pop.
#[test]
fn immediate_scan_errors_on_the_first_wrongtype_key_in_order() {
    let mut db = Database::new();
    db.set(
        Bytes::from_static(b"bad"),
        Entry::new_string(Bytes::from_static(b"v")),
    );
    db.list_push_back(b"good", Bytes::from_static(b"job"));
    let reply = immediate_scan(
        b"BLPOP",
        &args(&["bad", "good", "0"]),
        &keys(&["bad", "good"]),
        &mut db,
    );
    assert_wrongtype(&reply, "BLPOP bad good");
    assert_eq!(
        db.list_pop_front(b"good"),
        Some(Bytes::from_static(b"job")),
        "the later key must NOT have been served"
    );
}

/// A servable blocking pop still is served, and a genuine miss still answers
/// `None` (= go block) without conjuring the key (moon#560).
#[test]
fn immediate_scan_still_serves_hits_and_leaves_misses_absent() {
    let mut db = Database::new();
    db.list_push_back(b"q", Bytes::from_static(b"v1"));
    assert_eq!(
        immediate_scan(b"BLPOP", &args(&["q", "0"]), &keys(&["q"]), &mut db),
        Some(Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"q")),
            Frame::BulkString(Bytes::from_static(b"v1")),
        ]))
    );

    let mut db = Database::new();
    assert_eq!(
        immediate_scan(b"BLPOP", &args(&["ghost", "0"]), &keys(&["ghost"]), &mut db,),
        None,
        "an absent key must fall through to registration"
    );
    assert!(
        db.get(b"ghost").is_none(),
        "a miss must not conjure the key (moon#560)"
    );
}

/// BLMOVE/BRPOPLPUSH consult the DESTINATION's type only when the move is
/// actually about to happen — the order Redis's `lmoveGenericCommand` uses and
/// that moon's own non-blocking `LMOVE` already follows. The error must arrive
/// INSTEAD of the move: pre-fix the element was popped from the source and
/// then silently dropped by `list_push_*`'s `if let Ok(..)`.
#[test]
fn immediate_blmove_rejects_a_wrongtype_destination_without_losing_the_element() {
    for (cmd, argv) in [
        (&b"BLMOVE"[..], args(&["src", "dst", "LEFT", "LEFT", "0"])),
        (&b"BRPOPLPUSH"[..], args(&["src", "dst", "0"])),
    ] {
        let name = String::from_utf8_lossy(cmd).into_owned();
        let mut db = Database::new();
        db.list_push_back(b"src", Bytes::from_static(b"v1"));
        db.set(
            Bytes::from_static(b"dst"),
            Entry::new_string(Bytes::from_static(b"iam-a-string")),
        );
        let reply = immediate_scan(cmd, &argv, &keys(&["src"]), &mut db);
        assert_wrongtype(&reply, &name);
        assert_eq!(
            db.list_pop_front(b"src"),
            Some(Bytes::from_static(b"v1")),
            "{name}: the element must still be in the source"
        );
    }
}

/// An ABSENT source still blocks even when the destination holds the wrong
/// type: Redis never looks at the destination until it has something to move.
#[test]
fn immediate_blmove_with_absent_source_ignores_the_destination_type() {
    let mut db = Database::new();
    db.set(
        Bytes::from_static(b"dst"),
        Entry::new_string(Bytes::from_static(b"s")),
    );
    assert_eq!(
        immediate_scan(
            b"BLMOVE",
            &args(&["src", "dst", "LEFT", "LEFT", "0"]),
            &keys(&["src"]),
            &mut db,
        ),
        None,
        "nothing to move yet — the client blocks, destination untouched"
    );
}
