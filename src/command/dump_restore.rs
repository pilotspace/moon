//! `DUMP` and `RESTORE` (moon#636).
//!
//! Its own module rather than an addition to `key.rs`, which is already past
//! the 1500-line guideline.
//!
//! Every error string and edge case here was read off redis-server 8.6.1
//! rather than inferred; the surprises worth naming are:
//!
//! * `RESTORE` validates its TTL **before** its payload — `RESTORE k -1
//!   garbage` reports the TTL, not the garbage.
//! * An `ABSTTL` in the past is **not** an error. redis answers `+OK` and the
//!   key is simply already gone (`EXISTS` -> 0).
//! * `DUMP` with the wrong argument count says `dump`, lower-case, like every
//!   other redis arity error.
//! * `IDLETIME` and `FREQ` parse and range-check, then are **discarded**: they
//!   describe eviction bookkeeping moon does not model per key. Rejecting a
//!   bad value while ignoring a good one is exactly what redis does when the
//!   relevant `maxmemory-policy` is not active, so a client cannot tell the
//!   difference — and silently accepting `FREQ 300` would be the divergence.

use bytes::Bytes;

use crate::persistence::dump_payload::{self, DumpError};
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::current_time_ms;

use super::helpers::err_wrong_args;
use super::key::extract_key;

fn syntax_error() -> Frame {
    Frame::Error(Bytes::from_static(b"ERR syntax error"))
}

/// `DUMP key`
///
/// The serialized value, or a null bulk string when the key is absent —
/// redis returns nil rather than an error, so a client can use `DUMP` as an
/// existence probe.
pub fn dump(db: &mut Database, args: &[Frame]) -> Frame {
    // Exactly one argument: redis's arity for DUMP is 2, not -2.
    if args.len() != 1 {
        return err_wrong_args("DUMP");
    }
    let Some(key) = extract_key(&args[0]) else {
        return err_wrong_args("DUMP");
    };
    match db.get(key) {
        Some(entry) => Frame::BulkString(Bytes::from(dump_payload::encode(entry))),
        None => Frame::Null,
    }
}

/// `DUMP key`, on the shared-read path.
///
/// This arm is not optional. The monoio handler branches on
/// `metadata::is_write(cmd)`, and `DUMP` is `readonly` — so WITHOUT an arm in
/// `dispatch_read` a live server answers `unknown command 'DUMP'` while every
/// unit test calling `dump()` stays green, and `dispatch()` routes it happily.
/// That is exactly what happened while building this (moon#636): `RESTORE`
/// worked, `DUMP` did not, and at `--shards 4` it appeared to work for the
/// keys that happened to route cross-shard, because the SPSC path uses
/// `dispatch()`.
///
/// The cold-tier fallback is the second half of the same lesson: a read-path
/// arm that only consults the hot plane answers nil for a spilled key
/// (moon#610's class), so the miss branch asks the cold tier before giving up.
pub fn dump_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DUMP");
    }
    let Some(key) = extract_key(&args[0]) else {
        return err_wrong_args("DUMP");
    };
    if let Some(entry) = db.get_if_alive(key, now_ms) {
        return Frame::BulkString(Bytes::from(dump_payload::encode(entry)));
    }
    // Spilled to the cold tier: still a real key, and DUMP must serialize it.
    match db.get_cold_value(key, now_ms) {
        Some(value) => match entry_from_value(value) {
            Some(entry) => Frame::BulkString(Bytes::from(dump_payload::encode(&entry))),
            // A cold value of a kind with no redis-compatible tag. Nil is the
            // honest answer -- the alternative is emitting a payload no
            // RESTORE can read.
            None => Frame::Null,
        },
        None => Frame::Null,
    }
}

/// Rebuild an owned entry around a value read back from the cold tier, so it
/// can be handed to the same encoder the hot path uses.
fn entry_from_value(value: crate::storage::entry::RedisValue) -> Option<crate::storage::Entry> {
    use crate::storage::entry::RedisValue;
    let mut entry = match value {
        RedisValue::String(v) => return Some(crate::storage::Entry::new_string(v)),
        RedisValue::Hash(_) => crate::storage::Entry::new_hash(),
        RedisValue::List(_) => crate::storage::Entry::new_list(),
        RedisValue::Set(_) => crate::storage::Entry::new_set(),
        _ => return None,
    };
    let slot = entry.redis_value_mut()?;
    *slot = value;
    Some(entry)
}

/// `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]`
pub fn restore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("RESTORE");
    }
    let Some(key) = extract_key(&args[0]) else {
        return err_wrong_args("RESTORE");
    };
    let Some(ttl_raw) = extract_key(&args[1]) else {
        return Frame::Error(Bytes::from_static(
            b"ERR value is not an integer or out of range",
        ));
    };
    let Some(payload) = extract_key(&args[2]) else {
        return err_wrong_args("RESTORE");
    };

    let Some(ttl) = std::str::from_utf8(ttl_raw)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    else {
        return Frame::Error(Bytes::from_static(
            b"ERR value is not an integer or out of range",
        ));
    };
    // Before the payload: redis reports the TTL first, and a client that
    // sends both a bad TTL and a bad payload must see the same error moon
    // and redis agree on.
    if ttl < 0 {
        return Frame::Error(Bytes::from_static(b"ERR Invalid TTL value, must be >= 0"));
    }

    let mut replace = false;
    let mut absttl = false;
    let mut i = 3;
    while i < args.len() {
        let Some(opt) = extract_key(&args[i]) else {
            return syntax_error();
        };
        if opt.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
        } else if opt.eq_ignore_ascii_case(b"ABSTTL") {
            absttl = true;
        } else if opt.eq_ignore_ascii_case(b"IDLETIME") {
            i += 1;
            match numeric_option(args, i) {
                Ok(v) if v >= 0 => {}
                Ok(_) => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR Invalid IDLETIME value, must be >= 0",
                    ));
                }
                Err(f) => return f,
            }
        } else if opt.eq_ignore_ascii_case(b"FREQ") {
            i += 1;
            match numeric_option(args, i) {
                Ok(v) if (0..=255).contains(&v) => {}
                Ok(_) => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR Invalid FREQ value, must be >= 0 and <= 255",
                    ));
                }
                Err(f) => return f,
            }
        } else {
            return syntax_error();
        }
        i += 1;
    }

    if db.exists(key) && !replace {
        return Frame::Error(Bytes::from_static(
            b"BUSYKEY Target key name already exists.",
        ));
    }

    let mut entry = match dump_payload::decode(payload) {
        Ok(entry) => entry,
        Err(e @ (DumpError::BadPayload | DumpError::UnsupportedEncoding(_))) => {
            return Frame::Error(Bytes::from(e.message()));
        }
    };

    if ttl > 0 {
        // A relative TTL is measured from now; ABSTTL is already absolute.
        // Saturating, because `RESTORE k 9223372036854775807` must not wrap
        // into a timestamp in the past and delete the key it just wrote.
        let when = if absttl {
            ttl as u64
        } else {
            current_time_ms().saturating_add(ttl as u64)
        };
        entry.set_expires_at_ms(when);
    }

    db.set(Bytes::copy_from_slice(key), entry);
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// One numeric option value, or the frame to answer with.
fn numeric_option(args: &[Frame], idx: usize) -> Result<i64, Frame> {
    let Some(raw) = args.get(idx).and_then(extract_key) else {
        return Err(syntax_error());
    };
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| {
            Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Entry;

    fn db() -> Database {
        Database::new()
    }

    /// `matches!` cannot pattern-match a `Bytes` payload, so tests compare
    /// against the built frame instead.
    fn ok_frame() -> Frame {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    }

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn payload_of(db: &mut Database, key: &[u8]) -> Bytes {
        match dump(db, &[bulk(key)]) {
            Frame::BulkString(b) => b,
            other => panic!("DUMP did not return a payload: {other:?}"),
        }
    }

    #[test]
    fn dump_of_a_missing_key_is_null_not_an_error() {
        // redis answers nil here, so a client may use DUMP as a probe. An
        // error would break that.
        let mut db = db();
        assert!(matches!(dump(&mut db, &[bulk(b"nope")]), Frame::Null));
    }

    #[test]
    fn a_value_survives_dump_then_restore() {
        let mut db = db();
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"hello")),
        );
        let p = payload_of(&mut db, b"src");
        let r = restore(&mut db, &[bulk(b"dst"), bulk(b"0"), Frame::BulkString(p)]);
        assert_eq!(r, ok_frame(), "restore said {r:?}");
        assert!(db.exists(b"dst"));
    }

    #[test]
    fn restoring_onto_a_live_key_needs_replace() {
        let mut db = db();
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"hello")),
        );
        db.set(
            Bytes::from_static(b"taken"),
            Entry::new_string(Bytes::from_static(b"old")),
        );
        let p = payload_of(&mut db, b"src");
        match restore(
            &mut db,
            &[bulk(b"taken"), bulk(b"0"), Frame::BulkString(p.clone())],
        ) {
            Frame::Error(e) => assert_eq!(&e[..], b"BUSYKEY Target key name already exists."),
            other => panic!("expected BUSYKEY, got {other:?}"),
        }
        let r = restore(
            &mut db,
            &[
                bulk(b"taken"),
                bulk(b"0"),
                Frame::BulkString(p),
                bulk(b"REPLACE"),
            ],
        );
        assert_eq!(r, ok_frame());
    }

    #[test]
    fn the_ttl_is_checked_before_the_payload() {
        // Both arguments are bad. redis reports the TTL, and a client that
        // fixes the error it is shown must not then hit a different one.
        let mut db = db();
        match restore(&mut db, &[bulk(b"k"), bulk(b"-1"), bulk(b"garbage")]) {
            Frame::Error(e) => assert_eq!(&e[..], b"ERR Invalid TTL value, must be >= 0"),
            other => panic!("expected the TTL error, got {other:?}"),
        }
    }

    #[test]
    fn a_corrupt_payload_is_refused_in_rediss_words() {
        let mut db = db();
        match restore(&mut db, &[bulk(b"k"), bulk(b"0"), bulk(b"garbage")]) {
            Frame::Error(e) => {
                assert_eq!(&e[..], b"ERR DUMP payload version or checksum are wrong")
            }
            other => panic!("expected the payload error, got {other:?}"),
        }
        assert!(
            !db.exists(b"k"),
            "a refused RESTORE must not create the key"
        );
    }

    #[test]
    fn a_relative_ttl_becomes_an_expiry_in_the_future() {
        let mut db = db();
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        let p = payload_of(&mut db, b"src");
        assert_eq!(
            restore(
                &mut db,
                &[bulk(b"k"), bulk(b"100000"), Frame::BulkString(p)]
            ),
            ok_frame()
        );
        let entry = db.get(b"k").expect("restored key");
        assert!(entry.has_expiry());
        assert!(entry.expires_at_ms() > current_time_ms());
    }

    #[test]
    fn an_absttl_in_the_past_is_accepted_and_the_key_is_already_gone() {
        // Not an error in redis: +OK, and EXISTS answers 0. Treating it as an
        // error would break MIGRATE of a key whose deadline passed in flight.
        let mut db = db();
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        let p = payload_of(&mut db, b"src");
        let r = restore(
            &mut db,
            &[
                bulk(b"k"),
                bulk(b"1"),
                Frame::BulkString(p),
                bulk(b"ABSTTL"),
            ],
        );
        assert_eq!(r, ok_frame(), "got {r:?}");
        assert!(!db.exists(b"k"), "a past ABSTTL must leave no live key");
    }

    #[test]
    fn idletime_and_freq_are_range_checked_even_though_they_are_discarded() {
        // Accepting `FREQ 300` because the value is unused is the divergence
        // a client would notice; the range check is the whole contract here.
        let mut db = db();
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        let p = payload_of(&mut db, b"src");
        let mk = |opt: &[u8], v: &[u8]| {
            vec![
                bulk(b"k"),
                bulk(b"0"),
                Frame::BulkString(p.clone()),
                bulk(opt),
                bulk(v),
            ]
        };
        match restore(&mut db, &mk(b"FREQ", b"300")) {
            Frame::Error(e) => {
                assert_eq!(&e[..], b"ERR Invalid FREQ value, must be >= 0 and <= 255")
            }
            other => panic!("expected the FREQ error, got {other:?}"),
        }
        match restore(&mut db, &mk(b"IDLETIME", b"-1")) {
            Frame::Error(e) => assert_eq!(&e[..], b"ERR Invalid IDLETIME value, must be >= 0"),
            other => panic!("expected the IDLETIME error, got {other:?}"),
        }
        // ...and a value in range is accepted.
        assert_eq!(restore(&mut db, &mk(b"FREQ", b"5")), ok_frame());
    }

    #[test]
    fn an_unknown_option_is_a_syntax_error() {
        let mut db = db();
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        let p = payload_of(&mut db, b"src");
        match restore(
            &mut db,
            &[bulk(b"k"), bulk(b"0"), Frame::BulkString(p), bulk(b"BOGUS")],
        ) {
            Frame::Error(e) => assert_eq!(&e[..], b"ERR syntax error"),
            other => panic!("expected a syntax error, got {other:?}"),
        }
    }

    #[test]
    fn the_arity_errors_name_the_command_in_lower_case() {
        let mut db = db();
        match dump(&mut db, &[]) {
            Frame::Error(e) => {
                assert_eq!(&e[..], b"ERR wrong number of arguments for 'dump' command")
            }
            other => panic!("got {other:?}"),
        }
        // DUMP takes exactly one key: an extra argument is an arity error in
        // redis, not a silently ignored token.
        match dump(&mut db, &[bulk(b"a"), bulk(b"b")]) {
            Frame::Error(e) => {
                assert_eq!(&e[..], b"ERR wrong number of arguments for 'dump' command")
            }
            other => panic!("got {other:?}"),
        }
        match restore(&mut db, &[bulk(b"k")]) {
            Frame::Error(e) => {
                assert_eq!(
                    &e[..],
                    b"ERR wrong number of arguments for 'restore' command"
                )
            }
            other => panic!("got {other:?}"),
        }
    }
}

#[cfg(test)]
mod dispatch_wiring_tests {
    use super::*;
    use crate::command::{DispatchResult, dispatch};

    /// The unit tests above call `dump`/`restore` directly, which proves the
    /// implementation and NOT that anything routes to it. moon has three
    /// dispatch paths and a command wired into none of them answers "unknown
    /// command" while its unit tests stay green.
    #[test]
    fn the_read_path_serves_dump() {
        // The arm that was MISSING. A live monoio server branches on
        // `metadata::is_write`, and DUMP is readonly -- so this, not
        // `dispatch`, is the path a real client reaches. Without it the
        // server answered `unknown command 'DUMP'` while every other test
        // here passed, and at --shards 4 it looked intermittent, because keys
        // routed cross-shard went through `dispatch` instead.
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"src"),
            crate::storage::Entry::new_string(Bytes::from_static(b"v")),
        );
        let mut sel = 0usize;
        let key = Frame::BulkString(Bytes::from_static(b"src"));
        match crate::command::dispatch_read(&db, b"DUMP", &[key], 0, &mut sel, 16) {
            DispatchResult::Response(Frame::BulkString(_)) => {}
            DispatchResult::Response(Frame::Error(e)) => {
                panic!(
                    "DUMP not served on the read path: {}",
                    String::from_utf8_lossy(&e)
                )
            }
            _ => panic!("unexpected dispatch_read result for DUMP"),
        }
    }

    /// The prefilter gates whether the handler calls `dispatch_read` at all,
    /// so an arm added without a prefilter entry is still unreachable.
    #[test]
    fn the_prefilter_admits_dump() {
        assert!(crate::command::is_dispatch_read_supported(b"DUMP"));
    }

    #[test]
    fn dispatch_actually_routes_dump_and_restore() {
        let mut db = Database::new();
        let mut sel = 0usize;
        db.set(
            Bytes::from_static(b"src"),
            crate::storage::Entry::new_string(Bytes::from_static(b"v")),
        );
        let key = Frame::BulkString(Bytes::from_static(b"src"));
        match dispatch(&mut db, b"DUMP", &[key], &mut sel, 16) {
            DispatchResult::Response(Frame::BulkString(_)) => {}
            DispatchResult::Response(Frame::Error(e)) => {
                panic!("DUMP not routed: {}", String::from_utf8_lossy(&e))
            }
            _ => panic!("DUMP returned an unexpected dispatch result"),
        }
        match dispatch(&mut db, b"RESTORE", &[], &mut sel, 16) {
            // Arity, not "unknown command": proof the arm was reached.
            DispatchResult::Response(Frame::Error(e)) => assert_eq!(
                &e[..],
                b"ERR wrong number of arguments for 'restore' command"
            ),
            _ => panic!("RESTORE returned an unexpected dispatch result"),
        }
    }
}
