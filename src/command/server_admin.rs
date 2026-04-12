//! Server administration commands: FLUSHALL, FLUSHDB, DEBUG, MEMORY USAGE.
//!
//! These are routed through the main `dispatch()` function (keyless, broadcast
//! to all shards via the console gateway). The handlers operate on the
//! per-shard `Database` passed in by the event loop.
//!
//! ## Semantics
//!
//! * `FLUSHDB` clears the currently-selected database on this shard.
//! * `FLUSHALL` clears the currently-selected database on this shard as well;
//!   the console gateway broadcasts the command to every shard, and a future
//!   extension can iterate every DB index before flushing. For v0.1.5 we
//!   match Redis behaviour with a single active DB.
//! * `DEBUG OBJECT <key>` returns the redis-compatible one-line summary
//!   (`Value at:0x0 refcount:N encoding:... serializedlength:N lru:0 lru_seconds_idle:0`)
//!   so downstream tooling (redis-cli, RDBTools, Prometheus exporters) can
//!   parse the encoding + length fields without changes.
//! * `DEBUG SLEEP <seconds>` blocks the current shard thread for up to 30s,
//!   which is deliberate: it unlocks slowlog-testing and deterministic
//!   latency benchmarks. Capped to prevent accidental DoS on ACL-less builds.
//! * `MEMORY USAGE <key> [SAMPLES n]` returns a conservative estimate of
//!   bytes consumed by the entry. SAMPLES is accepted and ignored (Moon
//!   always walks the entire value — there is no probabilistic sampling).

use bytes::Bytes;

use crate::command::helpers::{err_wrong_args, extract_bytes};
use crate::framevec;
use crate::protocol::Frame;
use crate::storage::compact_value::RedisValueRef;
use crate::storage::entry::Entry;
use crate::storage::Database;

// ---------------------------------------------------------------------------
// FLUSHDB / FLUSHALL
// ---------------------------------------------------------------------------

/// `FLUSHDB [ASYNC|SYNC]`
///
/// Clears the currently-selected database. ASYNC/SYNC are accepted for
/// compatibility but treated identically (Moon has no background flush).
pub fn flushdb(db: &mut Database, args: &[Frame]) -> Frame {
    if !check_flush_args(args) {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    }
    db.clear();
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// `FLUSHALL [ASYNC|SYNC]`
///
/// Clears the currently-selected database on this shard. The console gateway
/// broadcasts the command to every shard, so a single client call fans out to
/// the full cluster.
pub fn flushall(db: &mut Database, args: &[Frame]) -> Frame {
    if !check_flush_args(args) {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    }
    db.clear();
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

fn check_flush_args(args: &[Frame]) -> bool {
    match args.len() {
        0 => true,
        1 => match extract_bytes(&args[0]) {
            Some(s) => s.eq_ignore_ascii_case(b"ASYNC") || s.eq_ignore_ascii_case(b"SYNC"),
            None => false,
        },
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// DEBUG OBJECT / SLEEP / HELP
// ---------------------------------------------------------------------------

/// `DEBUG <subcommand> [args...]`
///
/// Supported subcommands:
/// * `OBJECT <key>` — encoding/refcount/serializedlength summary.
/// * `SLEEP <seconds>` — blocking sleep on the current shard (0..=30s).
/// * `HELP` — list subcommands.
pub fn debug(db: &mut Database, args: &[Frame]) -> Frame {
    match classify_debug(args) {
        Ok(DebugCall::Object(rest)) => debug_object(db, rest),
        Ok(DebugCall::Sleep(rest)) => debug_sleep(rest),
        Ok(DebugCall::Help) => debug_help(),
        Err(e) => e,
    }
}

/// Read-only variant used by `dispatch_read()` on the shared-read path.
///
/// DEBUG is flagged as ADMIN (not WRITE and not READONLY) which steers the
/// connection handler into the read-dispatch branch. None of the supported
/// subcommands mutate `Database`, so exposing a `&Database` overload here
/// keeps the command working without forcing a WRITE reclassification
/// (which would incorrectly AOF-log DEBUG SLEEP).
pub fn debug_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    match classify_debug(args) {
        Ok(DebugCall::Object(rest)) => debug_object_readonly(db, rest, now_ms),
        Ok(DebugCall::Sleep(rest)) => debug_sleep(rest),
        Ok(DebugCall::Help) => debug_help(),
        Err(e) => e,
    }
}

enum DebugCall<'a> {
    Object(&'a [Frame]),
    Sleep(&'a [Frame]),
    Help,
}

fn classify_debug(args: &[Frame]) -> Result<DebugCall<'_>, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("DEBUG"));
    }
    let sub = match extract_bytes(&args[0]) {
        Some(s) => s,
        None => return Err(err_wrong_args("DEBUG")),
    };
    if sub.eq_ignore_ascii_case(b"OBJECT") {
        Ok(DebugCall::Object(&args[1..]))
    } else if sub.eq_ignore_ascii_case(b"SLEEP") {
        Ok(DebugCall::Sleep(&args[1..]))
    } else if sub.eq_ignore_ascii_case(b"HELP") {
        Ok(DebugCall::Help)
    } else {
        Err(Frame::Error(Bytes::from(format!(
            "ERR DEBUG subcommand '{}' not supported",
            String::from_utf8_lossy(sub),
        ))))
    }
}

fn debug_help() -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"DEBUG OBJECT <key>")),
        Frame::BulkString(Bytes::from_static(
            b"  Show low-level info about a key's object.",
        )),
        Frame::BulkString(Bytes::from_static(b"DEBUG SLEEP <seconds>")),
        Frame::BulkString(Bytes::from_static(
            b"  Stall this shard for <seconds> (float, capped at 30).",
        )),
        Frame::BulkString(Bytes::from_static(b"DEBUG HELP")),
        Frame::BulkString(Bytes::from_static(b"  Return subcommand help.")),
    ])
}

fn debug_object(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DEBUG OBJECT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("DEBUG OBJECT"),
    };
    match db.get(key.as_ref()) {
        Some(entry) => debug_object_reply(entry),
        None => Frame::Error(Bytes::from_static(b"ERR no such key")),
    }
}

fn debug_object_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DEBUG OBJECT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("DEBUG OBJECT"),
    };
    match db.get_if_alive(key.as_ref(), now_ms) {
        Some(entry) => debug_object_reply(entry),
        None => Frame::Error(Bytes::from_static(b"ERR no such key")),
    }
}

fn debug_object_reply(entry: &Entry) -> Frame {
    let encoding = entry.as_redis_value().encoding_name();
    let slen = estimate_serialized_length(entry);
    // Redis format: Value at:0x<addr> refcount:N encoding:X serializedlength:N lru:N lru_seconds_idle:N
    // Tools parse the key/value pairs; the exact address is not meaningful.
    let body = format!(
        "Value at:0x0000000000000000 refcount:1 encoding:{} serializedlength:{} lru:0 lru_seconds_idle:0",
        encoding, slen,
    );
    Frame::SimpleString(Bytes::from(body))
}

fn debug_sleep(args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("DEBUG SLEEP");
    }
    let secs_bytes = match extract_bytes(&args[0]) {
        Some(b) => b,
        None => return err_wrong_args("DEBUG SLEEP"),
    };
    let secs: f64 = match std::str::from_utf8(secs_bytes.as_ref())
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(v) if v.is_finite() && v >= 0.0 => v,
        _ => return Frame::Error(Bytes::from_static(b"ERR value is not a valid float")),
    };
    // Dispatch runs on the shard event loop; blocking sleep stalls that
    // shard only (by design — tests rely on this to populate slowlog). The
    // upper bound is 30s to avoid accidental DoS when ACLs are not used.
    let millis = (secs.min(30.0) * 1000.0) as u64;
    if millis > 0 {
        std::thread::sleep(std::time::Duration::from_millis(millis));
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Conservative byte estimate of an entry's payload, excluding per-key
/// accounting overhead (entry header, compact key bytes).
fn estimate_serialized_length(entry: &Entry) -> usize {
    match entry.as_redis_value() {
        RedisValueRef::String(s) => s.len(),
        RedisValueRef::Hash(h) => h.iter().map(|(k, v)| k.len() + v.len() + 2).sum(),
        RedisValueRef::HashListpack(lp) => lp.total_bytes(),
        RedisValueRef::List(lst) => lst.iter().map(|e| e.len() + 1).sum(),
        RedisValueRef::ListListpack(lp) => lp.total_bytes(),
        RedisValueRef::Set(s) => s.iter().map(|m| m.len() + 1).sum(),
        RedisValueRef::SetListpack(lp) => lp.total_bytes(),
        RedisValueRef::SetIntset(is) => is.len() * 8,
        RedisValueRef::SortedSet { members, .. } => members.iter().map(|(m, _)| m.len() + 9).sum(),
        RedisValueRef::SortedSetBPTree { members, .. } => {
            members.iter().map(|(m, _)| m.len() + 9).sum()
        }
        RedisValueRef::SortedSetListpack(lp) => lp.total_bytes(),
        // Streams track their own size; the header is an acceptable lower
        // bound for tooling — `XINFO STREAM` gives a richer picture.
        RedisValueRef::Stream(_) => 64,
    }
}

// ---------------------------------------------------------------------------
// MEMORY USAGE / STATS / DOCTOR / HELP
// ---------------------------------------------------------------------------

/// `MEMORY <subcommand> [args...]`
pub fn memory(db: &mut Database, args: &[Frame]) -> Frame {
    match classify_memory(args) {
        Ok(MemoryCall::Usage(rest)) => memory_usage(db, rest),
        Ok(MemoryCall::Stats) => memory_stats(db.estimated_memory()),
        Ok(MemoryCall::Doctor) => memory_doctor(),
        Ok(MemoryCall::Help) => memory_help(),
        Err(e) => e,
    }
}

/// Read-only variant routed from `dispatch_read()`.
pub fn memory_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    match classify_memory(args) {
        Ok(MemoryCall::Usage(rest)) => memory_usage_readonly(db, rest, now_ms),
        Ok(MemoryCall::Stats) => memory_stats(db.estimated_memory()),
        Ok(MemoryCall::Doctor) => memory_doctor(),
        Ok(MemoryCall::Help) => memory_help(),
        Err(e) => e,
    }
}

enum MemoryCall<'a> {
    Usage(&'a [Frame]),
    Stats,
    Doctor,
    Help,
}

fn classify_memory(args: &[Frame]) -> Result<MemoryCall<'_>, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("MEMORY"));
    }
    let sub = match extract_bytes(&args[0]) {
        Some(s) => s,
        None => return Err(err_wrong_args("MEMORY")),
    };
    if sub.eq_ignore_ascii_case(b"USAGE") {
        Ok(MemoryCall::Usage(&args[1..]))
    } else if sub.eq_ignore_ascii_case(b"STATS") {
        Ok(MemoryCall::Stats)
    } else if sub.eq_ignore_ascii_case(b"DOCTOR") {
        Ok(MemoryCall::Doctor)
    } else if sub.eq_ignore_ascii_case(b"HELP") {
        Ok(MemoryCall::Help)
    } else {
        Err(Frame::Error(Bytes::from(format!(
            "ERR MEMORY subcommand '{}' not supported",
            String::from_utf8_lossy(sub),
        ))))
    }
}

fn memory_stats(used: usize) -> Frame {
    Frame::Map(vec![(
        Frame::BulkString(Bytes::from_static(b"peak.allocated")),
        Frame::Integer(used as i64),
    )])
}

fn memory_doctor() -> Frame {
    Frame::BulkString(Bytes::from_static(
        b"Sam, I detected no issues in this Moon instance. Keep calm and carry on.\n",
    ))
}

fn memory_help() -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"MEMORY USAGE <key> [SAMPLES <count>]")),
        Frame::BulkString(Bytes::from_static(
            b"  Estimate memory usage of the key in bytes.",
        )),
        Frame::BulkString(Bytes::from_static(b"MEMORY STATS")),
        Frame::BulkString(Bytes::from_static(
            b"  Return a map of memory usage counters.",
        )),
        Frame::BulkString(Bytes::from_static(b"MEMORY DOCTOR")),
        Frame::BulkString(Bytes::from_static(b"  Memory health report.")),
        Frame::BulkString(Bytes::from_static(b"MEMORY HELP")),
        Frame::BulkString(Bytes::from_static(b"  Return subcommand help.")),
    ])
}

fn memory_usage(db: &mut Database, args: &[Frame]) -> Frame {
    let key = match parse_memory_usage_args(args) {
        Ok(k) => k,
        Err(e) => return e,
    };
    match db.get(key.as_ref()) {
        Some(entry) => memory_usage_reply(key.as_ref(), entry),
        None => Frame::Null,
    }
}

fn memory_usage_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    let key = match parse_memory_usage_args(args) {
        Ok(k) => k,
        Err(e) => return e,
    };
    match db.get_if_alive(key.as_ref(), now_ms) {
        Some(entry) => memory_usage_reply(key.as_ref(), entry),
        None => Frame::Null,
    }
}

/// Validate the `MEMORY USAGE key [SAMPLES n]` argument list and return the
/// key bytes on success, or an error frame on failure.
fn parse_memory_usage_args(args: &[Frame]) -> Result<Bytes, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("MEMORY USAGE"));
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return Err(err_wrong_args("MEMORY USAGE")),
    };
    // Accept (and ignore) SAMPLES <n> — Moon always visits every entry.
    if args.len() > 1 {
        if args.len() != 3 {
            return Err(err_wrong_args("MEMORY USAGE"));
        }
        match extract_bytes(&args[1]) {
            Some(flag) if flag.eq_ignore_ascii_case(b"SAMPLES") => {
                if extract_bytes(&args[2])
                    .and_then(|s| std::str::from_utf8(s.as_ref()).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .is_none()
                {
                    return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
                }
            }
            _ => return Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))),
        }
    }
    Ok(key)
}

fn memory_usage_reply(key: &[u8], entry: &Entry) -> Frame {
    // Entry header + compact key bytes + payload estimate.
    // `48` models the DashTable entry metadata + CompactKey inline
    // bytes for the common case (Moon's SSO caps at 23 bytes; the
    // constant is intentionally conservative — Redis's numbers
    // include jemalloc fragmentation that we do not).
    let payload = estimate_serialized_length(entry);
    let total = 48usize.saturating_add(key.len()).saturating_add(payload);
    Frame::Integer(total as i64)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn bulk(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn db_with_key() -> Database {
        let mut db = Database::new();
        // Use the public command entry point so the value goes through the
        // same insertion path the server uses at runtime.
        let _ = crate::command::string::set(&mut db, &[bulk(b"mykey"), bulk(b"helloworld")]);
        db
    }

    #[test]
    fn flushall_empties_db() {
        let mut db = db_with_key();
        assert_eq!(db.len(), 1);
        let f = flushall(&mut db, &[]);
        assert!(matches!(f, Frame::SimpleString(ref b) if b.as_ref() == b"OK"));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn flushall_accepts_async() {
        let mut db = db_with_key();
        let f = flushall(&mut db, &[bulk(b"ASYNC")]);
        assert!(matches!(f, Frame::SimpleString(_)));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn flushall_accepts_sync() {
        let mut db = db_with_key();
        let f = flushall(&mut db, &[bulk(b"sync")]);
        assert!(matches!(f, Frame::SimpleString(_)));
    }

    #[test]
    fn flushall_rejects_garbage() {
        let mut db = db_with_key();
        let f = flushall(&mut db, &[bulk(b"GARBAGE")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR syntax")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    #[test]
    fn flushdb_clears_current() {
        let mut db = db_with_key();
        let f = flushdb(&mut db, &[]);
        assert!(matches!(f, Frame::SimpleString(_)));
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn debug_object_returns_encoding_refcount_serlen() {
        let mut db = db_with_key();
        let f = debug(&mut db, &[bulk(b"OBJECT"), bulk(b"mykey")]);
        let s = match f {
            Frame::SimpleString(b) => String::from_utf8(b.to_vec()).unwrap(),
            other => panic!("expected SimpleString, got {other:?}"),
        };
        assert!(s.contains("encoding:"), "missing encoding: in {s}");
        assert!(s.contains("refcount:1"), "missing refcount:1 in {s}");
        assert!(s.contains("serializedlength:"), "missing serlen in {s}");
    }

    #[test]
    fn debug_object_missing_key() {
        let mut db = Database::new();
        let f = debug(&mut db, &[bulk(b"OBJECT"), bulk(b"missing")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR no such key")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    #[test]
    fn debug_sleep_zero_is_immediate() {
        let mut db = Database::new();
        let start = std::time::Instant::now();
        let f = debug(&mut db, &[bulk(b"SLEEP"), bulk(b"0")]);
        assert!(start.elapsed() < std::time::Duration::from_millis(50));
        assert!(matches!(f, Frame::SimpleString(_)));
    }

    #[test]
    fn debug_sleep_200ms() {
        let mut db = Database::new();
        let start = std::time::Instant::now();
        let f = debug(&mut db, &[bulk(b"SLEEP"), bulk(b"0.2")]);
        assert!(start.elapsed() >= std::time::Duration::from_millis(180));
        assert!(matches!(f, Frame::SimpleString(_)));
    }

    #[test]
    fn debug_sleep_rejects_negative() {
        let f = debug_sleep(&[bulk(b"-1")]);
        assert!(matches!(f, Frame::Error(_)));
    }

    #[test]
    fn debug_sleep_rejects_non_float() {
        let f = debug_sleep(&[bulk(b"abc")]);
        assert!(matches!(f, Frame::Error(_)));
    }

    #[test]
    fn debug_help_lists_subcommands() {
        let mut db = Database::new();
        let f = debug(&mut db, &[bulk(b"HELP")]);
        match f {
            Frame::Array(v) => {
                // At least OBJECT + SLEEP + HELP (3 pairs of label + description = 6 entries).
                assert!(v.len() >= 6, "expected >=6 help lines, got {}", v.len());
                let joined: Vec<String> = v
                    .iter()
                    .filter_map(|e| match e {
                        Frame::BulkString(b) => Some(String::from_utf8_lossy(b).to_string()),
                        _ => None,
                    })
                    .collect();
                let blob = joined.join("\n");
                assert!(blob.contains("OBJECT"), "help missing OBJECT");
                assert!(blob.contains("SLEEP"), "help missing SLEEP");
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn debug_unknown_subcommand() {
        let mut db = Database::new();
        let f = debug(&mut db, &[bulk(b"NUKE")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR DEBUG subcommand")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }

    #[test]
    fn memory_usage_existing_key() {
        let mut db = db_with_key();
        let f = memory_usage(&mut db, &[bulk(b"mykey")]);
        match f {
            Frame::Integer(n) => assert!(n >= 10, "expected >=10 bytes, got {n}"),
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    #[test]
    fn memory_usage_missing_key_returns_null() {
        let mut db = Database::new();
        let f = memory_usage(&mut db, &[bulk(b"missing")]);
        assert!(matches!(f, Frame::Null));
    }

    #[test]
    fn memory_usage_samples_flag_accepted() {
        let mut db = db_with_key();
        let f = memory_usage(&mut db, &[bulk(b"mykey"), bulk(b"SAMPLES"), bulk(b"5")]);
        assert!(matches!(f, Frame::Integer(_)));
    }

    #[test]
    fn memory_usage_samples_rejects_non_integer() {
        let mut db = db_with_key();
        let f = memory_usage(&mut db, &[bulk(b"mykey"), bulk(b"SAMPLES"), bulk(b"abc")]);
        assert!(matches!(f, Frame::Error(_)));
    }

    #[test]
    fn memory_stats_returns_map() {
        let mut db = db_with_key();
        let f = memory(&mut db, &[bulk(b"STATS")]);
        assert!(matches!(f, Frame::Map(_)));
    }

    #[test]
    fn memory_help_lists_usage() {
        let mut db = Database::new();
        let f = memory(&mut db, &[bulk(b"HELP")]);
        assert!(matches!(f, Frame::Array(_)));
    }

    #[test]
    fn memory_unknown_subcommand() {
        let mut db = Database::new();
        let f = memory(&mut db, &[bulk(b"NUKE")]);
        match f {
            Frame::Error(b) => assert!(b.starts_with(b"ERR MEMORY subcommand")),
            _ => panic!("expected ERR, got {f:?}"),
        }
    }
}
