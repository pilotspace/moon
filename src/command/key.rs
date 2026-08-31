use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_key::CompactKey;
use crate::storage::entry::current_time_ms;

use super::helpers::{err_wrong_args, expiry_ms_in_range};

/// Extract a key as &[u8] from a Frame argument.
pub(crate) fn extract_key(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(s) | Frame::SimpleString(s) => Some(s.as_ref()),
        _ => None,
    }
}

/// Parse an integer argument from a Frame.
pub(crate) fn parse_int(frame: &Frame) -> Option<i64> {
    match frame {
        Frame::BulkString(s) | Frame::SimpleString(s) => std::str::from_utf8(s).ok()?.parse().ok(),
        Frame::Integer(n) => Some(*n),
        _ => None,
    }
}

/// DEL key [key ...]
///
/// Removes the specified keys. Returns the number of keys that were removed.
pub fn del(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("DEL");
    }
    let mut count: i64 = 0;
    for arg in args {
        if let Some(key) = extract_key(arg) {
            // Counting variant: a spilled (cold-only) key logically exists
            // and must count as removed (D1).
            let (removed, _hot) = db.remove_counting_cold(key);
            if removed {
                count += 1;
            }
        }
    }
    Frame::Integer(count)
}

/// EXISTS key [key ...]
///
/// Returns the number of specified keys that exist. Duplicate keys are counted
/// multiple times (Redis behavior).
pub fn exists(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("EXISTS");
    }
    let mut count: i64 = 0;
    for arg in args {
        if let Some(key) = extract_key(arg) {
            if db.exists(key) {
                count += 1;
            }
        }
    }
    Frame::Integer(count)
}

/// The `NX | XX | GT | LT` condition flags of the EXPIRE family (Redis 7.0),
/// moon#544. Parsed by [`parse_expire_flags`], evaluated by
/// [`expire_cond_blocks`]. `XX` may combine with `GT`/`LT`; `NX` combines
/// with nothing; `GT` and `LT` exclude each other — Redis's exact rules and
/// error strings.
#[derive(Default, Clone, Copy)]
struct ExpireFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

/// Parse the option tokens after `key <time>`. Returns the flags or the
/// RESP error to reply verbatim.
fn parse_expire_flags(extra: &[Frame]) -> Result<ExpireFlags, Frame> {
    let mut f = ExpireFlags::default();
    for tok in extra {
        let Frame::BulkString(t) = tok else {
            return Err(Frame::Error(Bytes::from_static(b"ERR Unsupported option")));
        };
        if t.eq_ignore_ascii_case(b"NX") {
            f.nx = true;
        } else if t.eq_ignore_ascii_case(b"XX") {
            f.xx = true;
        } else if t.eq_ignore_ascii_case(b"GT") {
            f.gt = true;
        } else if t.eq_ignore_ascii_case(b"LT") {
            f.lt = true;
        } else {
            let mut msg = Vec::with_capacity(24 + t.len());
            msg.extend_from_slice(b"ERR Unsupported option ");
            msg.extend_from_slice(t);
            return Err(Frame::Error(Bytes::from(msg)));
        }
    }
    if f.nx && (f.xx || f.gt || f.lt) {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR NX and XX, GT or LT options at the same time are not compatible",
        )));
    }
    if f.gt && f.lt {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR GT and LT options at the same time are not compatible",
        )));
    }
    Ok(f)
}

/// Whether the condition flags block setting `when_ms` as the new expiry,
/// given the key's current expiry (`None` = no TTL). Redis's evaluation
/// order verbatim: a key with no TTL counts as an infinite expiry, so GT
/// never sets it and LT always does.
fn expire_cond_blocks(f: ExpireFlags, current: Option<u64>, when_ms: u64) -> bool {
    (f.nx && current.is_some())
        || (f.xx && current.is_none())
        || (f.gt && current.is_none_or(|cur| when_ms <= cur))
        || (f.lt && current.is_some_and(|cur| when_ms >= cur))
}

/// Shared condition gate for the four EXPIRE-family commands: parse flags,
/// probe the key (lazy-expiring it), and decide. `when_ms` is the absolute
/// candidate expiry, saturated to 0 for past times — only its ORDER versus
/// the current expiry matters to GT/LT.
///
/// `Ok(true)` = proceed with the set/delete; `Ok(false)` = reply `:0`
/// (condition blocked, or the key is missing while any flag is present —
/// Redis answers 0 for a missing key on every path). `Err` = reply the
/// syntax error verbatim.
fn expire_condition_allows(
    db: &mut Database,
    key: &[u8],
    extra: &[Frame],
    when_ms: u64,
) -> Result<bool, Frame> {
    let f = parse_expire_flags(extra)?;
    if !(f.nx || f.xx || f.gt || f.lt) {
        return Ok(true);
    }
    let Some(entry) = db.get(key) else {
        return Ok(false);
    };
    let current = entry.has_expiry().then(|| entry.expires_at_ms());
    Ok(!expire_cond_blocks(f, current, when_ms))
}

/// EXPIRE key seconds [NX | XX | GT | LT]
///
/// Set a timeout on key. Returns 1 if the timeout was set (or the key was
/// deleted because of a non-positive/past TTL), 0 if the key does not exist
/// or a condition flag blocked the set (moon#544).
/// A non-positive TTL deletes the key immediately (Redis past-time semantics).
pub fn expire(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("EXPIRE");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("EXPIRE"),
    };
    let seconds = match parse_int(&args[1]) {
        Some(n) => n,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    // Redis rejects an out-of-i64-range expiry (`seconds < LLONG_MIN/1000`) BEFORE
    // the past-time delete, so an extreme negative errors rather than deleting.
    if seconds < i64::MIN / 1000 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'EXPIRE' command",
        ));
    }
    // Redis parity: a non-positive TTL is a past-time expiry -> delete the key now
    // (return 1 if it existed, 0 otherwise) rather than erroring. Mirrors EXPIREAT.
    // moon#544: the condition gate runs first — GT must not delete via a past
    // time; LT must. The saturated when_ms only needs correct ORDER vs current.
    if seconds <= 0 {
        let when_ms = current_time_ms().saturating_add_signed(seconds.saturating_mul(1000));
        match expire_condition_allows(db, key, &args[2..], when_ms) {
            Err(e) => return e,
            Ok(false) => return Frame::Integer(0),
            Ok(true) => {}
        }
        return if db.remove(key).is_some() {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
    }
    // Guard the u64 arithmetic (seconds*1000 + now_ms can overflow) AND bound the
    // result to the i64 domain so PTTL — which casts the stored u64 back to i64 —
    // never wraps negative on a live key. Redis rejects an out-of-range expiry —
    // BEFORE evaluating condition flags, so the gate sees a valid when_ms.
    let expires_at_ms = match (seconds as u64)
        .checked_mul(1000)
        .and_then(|delta| current_time_ms().checked_add(delta))
        .filter(|ms| expiry_ms_in_range(*ms))
    {
        Some(ms) => ms,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR invalid expire time in 'EXPIRE' command",
            ));
        }
    };
    match expire_condition_allows(db, key, &args[2..], expires_at_ms) {
        Err(e) => return e,
        Ok(false) => return Frame::Integer(0),
        Ok(true) => {}
    }
    if db.set_expiry(key, expires_at_ms) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// PEXPIRE key milliseconds
///
/// Like EXPIRE but the timeout is specified in milliseconds. A non-positive TTL
/// deletes the key immediately (Redis past-time semantics).
pub fn pexpire(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("PEXPIRE");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PEXPIRE"),
    };
    let millis = match parse_int(&args[1]) {
        Some(n) => n,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    // Redis parity: a non-positive TTL is a past-time expiry -> delete the key
    // now. moon#544: condition gate first (see `expire`).
    if millis <= 0 {
        let when_ms = current_time_ms().saturating_add_signed(millis);
        match expire_condition_allows(db, key, &args[2..], when_ms) {
            Err(e) => return e,
            Ok(false) => return Frame::Integer(0),
            Ok(true) => {}
        }
        return if db.remove(key).is_some() {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
    }
    // Guard the u64 arithmetic against overflow AND bound the result to the i64
    // domain so PTTL never wraps negative on a live key (consistent with EXPIRE).
    let expires_at_ms = match current_time_ms()
        .checked_add(millis as u64)
        .filter(|ms| expiry_ms_in_range(*ms))
    {
        Some(ms) => ms,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR invalid expire time in 'PEXPIRE' command",
            ));
        }
    };
    match expire_condition_allows(db, key, &args[2..], expires_at_ms) {
        Err(e) => return e,
        Ok(false) => return Frame::Integer(0),
        Ok(true) => {}
    }
    if db.set_expiry(key, expires_at_ms) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// TTL key
///
/// Returns the remaining time to live of a key that has a timeout, in seconds.
/// Returns -2 if the key does not exist, -1 if the key has no associated timeout.
pub fn ttl(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("TTL");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("TTL"),
    };
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now_ms = current_time_ms();
                let exp_ms = entry.expires_at_ms();
                if now_ms >= exp_ms {
                    // Edge case: expired between get and now
                    Frame::Integer(-2)
                } else {
                    // Redis rounds to the NEAREST second ((ms+500)/1000): a
                    // fresh `EXPIRE k 100` answers TTL 100, not 99. Oracle-
                    // diffed vs redis-server (moon#544 probe run).
                    Frame::Integer(((exp_ms - now_ms + 500) / 1000) as i64)
                }
            }
        }
    }
}

/// PTTL key
///
/// Like TTL but returns the remaining time in milliseconds.
pub fn pttl(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("PTTL");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PTTL"),
    };
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now_ms = current_time_ms();
                let exp_ms = entry.expires_at_ms();
                if now_ms >= exp_ms {
                    Frame::Integer(-2)
                } else {
                    Frame::Integer((exp_ms - now_ms) as i64)
                }
            }
        }
    }
}

/// PERSIST key
///
/// Remove the existing timeout on key. Returns 1 if the timeout was removed,
/// 0 if the key does not exist or does not have an associated timeout.
pub fn persist(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("PERSIST");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PERSIST"),
    };
    // Check if key exists and has a TTL
    match db.get(key) {
        None => Frame::Integer(0),
        Some(entry) => {
            if !entry.has_expiry() {
                return Frame::Integer(0);
            }
            // Key exists and has TTL -- remove it
            // Release immutable borrow before mutable set_expiry call
            let _ = entry;
            db.set_expiry(key, 0);
            Frame::Integer(1)
        }
    }
}

/// EXPIREAT key unix-time-seconds
///
/// Set the expiration for a key as a UNIX timestamp (seconds).
pub fn expireat(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("EXPIREAT");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("EXPIREAT"),
    };
    let timestamp = match parse_int(&args[1]) {
        Some(n) => n,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR invalid expire time in 'EXPIREAT' command",
            ));
        }
    };
    // Redis rejects an out-of-i64-range timestamp (`< LLONG_MIN/1000`) before the
    // past-time delete, so an extreme negative errors rather than deleting.
    if timestamp < i64::MIN / 1000 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'EXPIREAT' command",
        ));
    }
    // Redis accepts 0 and negative timestamps as past-time expiry (deletes key
    // immediately). moon#544: condition gate first (see `expire`).
    if timestamp <= 0 {
        let when_ms = 0u64.saturating_add_signed(timestamp.saturating_mul(1000));
        match expire_condition_allows(db, key, &args[2..], when_ms) {
            Err(e) => return e,
            Ok(false) => return Frame::Integer(0),
            Ok(true) => {}
        }
        return if db.remove(key).is_some() {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
    }
    // Guard the *1000 conversion against u64 overflow AND bound the result to the
    // i64 domain so PEXPIRETIME never wraps negative on a live key.
    let expires_at_ms = match (timestamp as u64)
        .checked_mul(1000)
        .filter(|ms| expiry_ms_in_range(*ms))
    {
        Some(ms) => ms,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR invalid expire time in 'EXPIREAT' command",
            ));
        }
    };
    match expire_condition_allows(db, key, &args[2..], expires_at_ms) {
        Err(e) => return e,
        Ok(false) => return Frame::Integer(0),
        Ok(true) => {}
    }
    if db.set_expiry(key, expires_at_ms) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// PEXPIREAT key unix-time-milliseconds
///
/// Set the expiration for a key as a UNIX timestamp in milliseconds.
pub fn pexpireat(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("PEXPIREAT");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PEXPIREAT"),
    };
    let timestamp_ms = match parse_int(&args[1]) {
        Some(n) => n,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR invalid expire time in 'PEXPIREAT' command",
            ));
        }
    };
    // Redis accepts 0 and negative timestamps as past-time expiry (deletes key
    // immediately). moon#544: condition gate first (see `expire`).
    if timestamp_ms <= 0 {
        let when_ms = 0u64.saturating_add_signed(timestamp_ms);
        match expire_condition_allows(db, key, &args[2..], when_ms) {
            Err(e) => return e,
            Ok(false) => return Frame::Integer(0),
            Ok(true) => {}
        }
        return if db.remove(key).is_some() {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
    }
    match expire_condition_allows(db, key, &args[2..], timestamp_ms as u64) {
        Err(e) => return e,
        Ok(false) => return Frame::Integer(0),
        Ok(true) => {}
    }
    if db.set_expiry(key, timestamp_ms as u64) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// EXPIRETIME key
///
/// Returns the absolute Unix timestamp (in seconds) at which the key will expire.
/// Returns -2 if key doesn't exist, -1 if key has no expiry.
pub fn expiretime(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("EXPIRETIME");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("EXPIRETIME"),
    };
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer((entry.expires_at_ms() / 1000) as i64)
            }
        }
    }
}

/// PEXPIRETIME key
///
/// Returns the absolute Unix timestamp (in milliseconds) at which the key will expire.
pub fn pexpiretime(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("PEXPIRETIME");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PEXPIRETIME"),
    };
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer(entry.expires_at_ms() as i64)
            }
        }
    }
}

/// RANDOMKEY
///
/// Returns a random key from the currently selected database.
pub fn randomkey(db: &mut Database, _args: &[Frame]) -> Frame {
    match db.random_key() {
        Some(key) => Frame::BulkString(key),
        None => Frame::Null,
    }
}

/// TOUCH key [key ...]
///
/// Alters the last access time of a key(s). Returns the number of keys that exist.
pub fn touch(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("TOUCH");
    }
    let mut count = 0i64;
    for arg in args {
        let key = match extract_key(arg) {
            Some(k) => k,
            None => continue,
        };
        if db.exists(key) {
            // exists() already does lazy expiry + access tracking
            count += 1;
        }
    }
    Frame::Integer(count)
}

/// TIME
///
/// Returns the current server time as a two-element array:
/// [unix-seconds, microseconds-since-epoch-second].
pub fn time() -> Frame {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let mut secs_buf = itoa::Buffer::new();
    let mut micros_buf = itoa::Buffer::new();
    Frame::Array(
        vec![
            Frame::BulkString(Bytes::copy_from_slice(
                secs_buf.format(now.as_secs()).as_bytes(),
            )),
            Frame::BulkString(Bytes::copy_from_slice(
                micros_buf.format(now.subsec_micros()).as_bytes(),
            )),
        ]
        .into(),
    )
}

/// FLUSHDB [ASYNC|SYNC]
///
/// Delete all keys in the currently selected database.
pub fn flushdb(db: &mut Database, _args: &[Frame]) -> Frame {
    db.clear();
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// TYPE key
///
/// Returns the string representation of the type of the value stored at key.
/// Returns "none" if the key does not exist.
pub fn type_cmd(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("TYPE");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("TYPE"),
    };
    match db.get(key) {
        None => Frame::SimpleString(Bytes::from_static(b"none")),
        Some(entry) => {
            let type_name = entry.value.type_name();
            Frame::SimpleString(Bytes::from_static(type_name.as_bytes()))
        }
    }
}

/// OBJECT subcommand [arguments]
///
/// Inspect the internals of Redis objects. Currently supports:
/// - OBJECT ENCODING key: returns the internal encoding used for the value
/// - OBJECT HELP: returns help text
pub fn object(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("OBJECT");
    }
    let subcommand = match extract_key(&args[0]) {
        Some(s) => s,
        None => return err_wrong_args("OBJECT"),
    };
    // moon#670: an unknown subcommand is refused with Redis's shape BEFORE any
    // arity check, and from the SAME table the `MULTI` queue gate consults. An
    // arity error here reads to a client as "the subcommand exists, you called
    // it wrong", which is how `OBJECT BOGUS` used to answer.
    if !crate::command::metadata::is_known_subcommand(b"OBJECT", subcommand) {
        return crate::command::helpers::err_unknown_subcommand("OBJECT", subcommand);
    }
    if subcommand.eq_ignore_ascii_case(b"ENCODING") {
        if args.len() != 2 {
            return err_wrong_args("OBJECT");
        }
        let key = match extract_key(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("OBJECT"),
        };
        match db.get(key) {
            Some(entry) => {
                let encoding = entry.value.as_redis_value().encoding_name();
                Frame::BulkString(Bytes::from(encoding))
            }
            None => Frame::Null,
        }
    } else if subcommand.eq_ignore_ascii_case(b"FREQ") {
        if args.len() != 2 {
            return err_wrong_args("OBJECT");
        }
        let key = match extract_key(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("OBJECT"),
        };
        match db.get(key) {
            Some(entry) => Frame::Integer(entry.access_counter() as i64),
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else if subcommand.eq_ignore_ascii_case(b"IDLETIME") {
        if args.len() != 2 {
            return err_wrong_args("OBJECT");
        }
        let key = match extract_key(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("OBJECT"),
        };
        let now = db.now();
        match db.get(key) {
            Some(entry) => {
                let last = entry.last_access();
                // Full u32 epoch-seconds delta; saturate rather than wrap if
                // the cached clock lags a concurrent touch.
                let idle = now.saturating_sub(last);
                Frame::Integer(idle as i64)
            }
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else if subcommand.eq_ignore_ascii_case(b"REFCOUNT") {
        if args.len() != 2 {
            return err_wrong_args("OBJECT");
        }
        let key = match extract_key(&args[1]) {
            Some(k) => k,
            None => return err_wrong_args("OBJECT"),
        };
        match db.get(key) {
            // Moon doesn't use reference counting — always return 1
            Some(_) => Frame::Integer(1),
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else if subcommand.eq_ignore_ascii_case(b"HELP") {
        object_help()
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown OBJECT subcommand"))
    }
}

/// OBJECT (read-only) — served from the shared read path.
///
/// All OBJECT subcommands are pure reads; this variant uses
/// `get_if_alive` so it never mutates expiry state. Without it, OBJECT
/// over the wire dies in `dispatch_read`'s unknown-command fallback
/// (the monoio handler routes every non-write command through the read
/// dispatcher).
pub fn object_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("OBJECT");
    }
    let subcommand = match extract_key(&args[0]) {
        Some(s) => s,
        None => return err_wrong_args("OBJECT"),
    };
    // moon#670: an unknown subcommand is refused with Redis's shape BEFORE any
    // arity check, and from the SAME table the `MULTI` queue gate consults. An
    // arity error here reads to a client as "the subcommand exists, you called
    // it wrong", which is how `OBJECT BOGUS` used to answer.
    if !crate::command::metadata::is_known_subcommand(b"OBJECT", subcommand) {
        return crate::command::helpers::err_unknown_subcommand("OBJECT", subcommand);
    }
    if subcommand.eq_ignore_ascii_case(b"HELP") {
        return object_help();
    }
    if args.len() != 2 {
        return err_wrong_args("OBJECT");
    }
    let key = match extract_key(&args[1]) {
        Some(k) => k,
        None => return err_wrong_args("OBJECT"),
    };
    if subcommand.eq_ignore_ascii_case(b"ENCODING") {
        match db.get_if_alive_any_plane(key, now_ms) {
            Some(entry) => {
                let encoding = entry.value.as_redis_value().encoding_name();
                Frame::BulkString(Bytes::from(encoding))
            }
            None => Frame::Null,
        }
    } else if subcommand.eq_ignore_ascii_case(b"FREQ") {
        match db.get_if_alive_any_plane(key, now_ms) {
            Some(entry) => Frame::Integer(entry.access_counter() as i64),
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else if subcommand.eq_ignore_ascii_case(b"IDLETIME") {
        let now = db.now();
        match db.get_if_alive_any_plane(key, now_ms) {
            Some(entry) => {
                let last = entry.last_access();
                // Full u32 epoch-seconds delta; saturate rather than wrap if
                // the cached clock lags a concurrent touch.
                let idle = now.saturating_sub(last);
                Frame::Integer(idle as i64)
            }
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else if subcommand.eq_ignore_ascii_case(b"REFCOUNT") {
        match db.get_if_alive_any_plane(key, now_ms) {
            // Moon doesn't use reference counting — always return 1
            Some(_) => Frame::Integer(1),
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown OBJECT subcommand"))
    }
}

/// Shared OBJECT HELP response.
fn object_help() -> Frame {
    // Shape and body both live in the shared table (moon#698): the header line
    // and the `HELP` footer are emitted by `help_reply`, so OBJECT cannot drift
    // from the other twelve containers.
    crate::command::help_text::help_or_empty("OBJECT")
}

/// Redis-compatible glob pattern matcher.
///
/// Supports: `*` (any sequence), `?` (one byte), `[abc]` (character class),
/// `[^abc]`/`[!abc]` (negated class), `[a-z]` (range), `\x` (escape).
pub(crate) fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0; // pattern index
    let mut si = 0; // string index
    let mut star_pi = usize::MAX;
    let mut star_si = usize::MAX;

    while si < string.len() {
        if pi < pattern.len() && pattern[pi] == b'\\' {
            // Escaped character: match literally
            pi += 1;
            if pi < pattern.len() && pattern[pi] == string[si] {
                pi += 1;
                si += 1;
                continue;
            }
            // Backslash at end of pattern or mismatch -- try star backtrack
        } else if pi < pattern.len() && pattern[pi] == b'?' {
            pi += 1;
            si += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == b'[' {
            // Character class
            if let Some((matched, new_pi)) = match_char_class(&pattern[pi..], string[si]) {
                if matched {
                    pi += new_pi;
                    si += 1;
                    continue;
                }
            }
            // Class didn't match -- try star backtrack
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == string[si] {
            pi += 1;
            si += 1;
            continue;
        }

        // Mismatch: backtrack to last * if possible
        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
            continue;
        }

        return false;
    }

    // Consume trailing *s in pattern
    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

/// Match a character class `[...]` at the start of `pattern`.
/// Returns `Some((matched, bytes_consumed))` or `None` if malformed.
fn match_char_class(pattern: &[u8], ch: u8) -> Option<(bool, usize)> {
    if pattern.is_empty() || pattern[0] != b'[' {
        return None;
    }
    let mut i = 1;
    let negated = if i < pattern.len() && (pattern[i] == b'^' || pattern[i] == b'!') {
        i += 1;
        true
    } else {
        false
    };

    let mut matched = false;
    while i < pattern.len() && pattern[i] != b']' {
        let start = pattern[i];
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
            // Range: a-z
            let end = pattern[i + 2];
            let (lo, hi) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            if ch >= lo && ch <= hi {
                matched = true;
            }
            i += 3;
        } else {
            if ch == start {
                matched = true;
            }
            i += 1;
        }
    }

    if i >= pattern.len() {
        return None; // No closing bracket
    }

    // i is at ']'
    Some((matched ^ negated, i + 1))
}

/// DBSIZE
///
/// Returns the number of LOGICAL keys in the currently selected database —
/// hot entries plus disk-offloaded (cold) keys, overlap counted once
/// (issue #355; Redis parity: a spilled-but-readable key is still a key).
pub fn dbsize(db: &mut Database, _args: &[Frame]) -> Frame {
    Frame::Integer(db.logical_len() as i64)
}

/// DBSIZE — read-only variant for `dispatch_read()`.
///
/// DBSIZE is flagged as READONLY (`RF`) in the metadata registry, which
/// routes it through the shared-read dispatch path. This handler takes an
/// immutable `&Database` and returns the current key count, matching the
/// mutable `dbsize` semantics exactly (neither variant lazily expires).
pub fn dbsize_readonly(db: &Database, _args: &[Frame]) -> Frame {
    Frame::Integer(db.logical_len() as i64)
}

/// KEYS pattern
///
/// Returns all keys matching the given glob-style pattern.
/// Expired keys are excluded (lazy expiry check on each key).
pub fn keys(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("KEYS");
    }
    let pattern = match extract_key(&args[0]) {
        Some(p) => p,
        None => return err_wrong_args("KEYS"),
    };

    // Collect all keys first (need to release immutable borrow before calling db.get)
    let all_keys: Vec<CompactKey> = db.keys().cloned().collect();
    let now_ms = db.now_ms();

    let mut result = Vec::new();
    for key in all_keys {
        // Trigger lazy expiry by calling exists; membership below is strict
        // hot-aliveness so cold-only keys enter exactly once, via the cold
        // loop (#364 plane partition — see Database::cold_only_keys).
        let _ = db.exists(key.as_bytes());
        if db.get_if_alive(key.as_bytes(), now_ms).is_some() && glob_match(pattern, key.as_bytes())
        {
            result.push(Frame::BulkString(key.to_bytes()));
        }
    }
    for key in db.cold_only_keys(now_ms) {
        if glob_match(pattern, key.as_ref()) {
            result.push(Frame::BulkString(key.clone()));
        }
    }

    Frame::Array(result.into())
}

/// RENAME key newkey
///
/// Renames key to newkey. Returns an error when key does not exist.
/// If source and destination are the same, returns OK without deleting.
/// Overwrites destination if it exists. Preserves TTL.
#[allow(clippy::unwrap_used)] // remove() after exists() check — key guaranteed present
pub fn rename(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("RENAME");
    }
    let src = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("RENAME"),
    };
    let dst = match extract_key(&args[1]) {
        Some(k) => k,
        None => return err_wrong_args("RENAME"),
    };

    // Check if source exists (with lazy expiry)
    if !db.exists(src) {
        return Frame::Error(Bytes::from_static(b"ERR no such key"));
    }

    // Same key: no-op (Pitfall 5)
    if src == dst {
        return Frame::SimpleString(Bytes::from_static(b"OK"));
    }

    // Remove source, set as destination (preserves entire Entry including TTL)
    let entry = db.remove(src).unwrap();
    db.set(dst, entry);

    // TWO events, not one: a consumer tracking key lifetimes needs to see the
    // source disappear and the destination appear, and the halves carry
    // different keys.
    crate::notify::notify_keyspace_event(
        crate::notify::NotifyFlags::GENERIC,
        "rename_from",
        src,
        db.db_index,
    );
    crate::notify::notify_keyspace_event(
        crate::notify::NotifyFlags::GENERIC,
        "rename_to",
        dst,
        db.db_index,
    );

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// RENAMENX key newkey
///
/// Renames key to newkey only if newkey does not exist.
/// Returns 1 if renamed, 0 if newkey already exists.
#[allow(clippy::unwrap_used)] // remove() after exists() check — key guaranteed present
pub fn renamenx(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("RENAMENX");
    }
    let src = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("RENAMENX"),
    };
    let dst = match extract_key(&args[1]) {
        Some(k) => k,
        None => return err_wrong_args("RENAMENX"),
    };

    // Check if source exists
    if !db.exists(src) {
        return Frame::Error(Bytes::from_static(b"ERR no such key"));
    }

    // Same key: destination "exists", return 0
    if src == dst {
        return Frame::Integer(0);
    }

    // Check if destination exists
    if db.exists(dst) {
        return Frame::Integer(0);
    }

    let entry = db.remove(src).unwrap();
    db.set(dst, entry);

    Frame::Integer(1)
}

/// Check if a value is large enough to warrant async drop.
fn should_async_drop(entry: &crate::storage::entry::Entry) -> bool {
    use crate::storage::compact_value::RedisValueRef;
    match entry.value.as_redis_value() {
        RedisValueRef::Hash(m) => m.len() > 64,
        RedisValueRef::HashWithTtl { fields, .. } => fields.len() > 64,
        RedisValueRef::List(l) => l.len() > 64,
        RedisValueRef::Set(s) => s.len() > 64,
        RedisValueRef::SortedSet { members, .. } => members.len() > 64,
        RedisValueRef::SortedSetBPTree { members, .. } => members.len() > 64,
        RedisValueRef::String(_) => false,
        RedisValueRef::Stream(s) => s.entries.len() > 64,
        // Compact encodings are always small, no async drop needed
        RedisValueRef::HashListpack(_)
        | RedisValueRef::ListListpack(_)
        | RedisValueRef::SetListpack(_)
        | RedisValueRef::SetIntset(_)
        | RedisValueRef::SortedSetListpack(_) => false,
    }
}

/// UNLINK key [key ...]
///
/// Removes the specified keys. Like DEL but reclaims memory asynchronously
/// for large collections.
pub fn unlink(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("UNLINK");
    }
    let mut count: i64 = 0;
    for arg in args {
        if let Some(key) = extract_key(arg) {
            // Counting variant: cold-only keys count as removed (D1).
            let (removed, hot) = db.remove_counting_cold(key);
            if removed {
                count += 1;
            }
            if let Some(entry) = hot {
                if should_async_drop(&entry) {
                    // Async drop for large collections: spawn a blocking
                    // task to avoid holding the event loop.
                    #[cfg(feature = "runtime-tokio")]
                    tokio::task::spawn_blocking(move || drop(entry));
                    #[cfg(feature = "runtime-monoio")]
                    drop(entry);
                }
                // Small values drop normally (entry goes out of scope)
            }
        }
    }
    Frame::Integer(count)
}

/// SCAN's TYPE-filter judgment for a cold-only (spilled) key.
///
/// Judged from the in-RAM `ColdLocation::value_type` cache — must never
/// read the cold value from disk or promote it into RAM, or SCAN over a
/// large offloaded keyspace turns into a disk crawl / memory-pressure
/// storm on the shard thread (#364).
fn cold_type_matches(db: &Database, key: &[u8], type_filter: &[u8]) -> bool {
    db.cold_index
        .as_ref()
        .and_then(|ci| ci.lookup(key))
        .is_some_and(|loc| type_filter.eq_ignore_ascii_case(loc.value_type.type_name().as_bytes()))
}

/// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
///
/// Incrementally iterates the key space. Returns a cursor and a batch of keys.
pub fn scan(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SCAN");
    }

    // Parse cursor (a position in 48-bit hash space — see `scan_core`)
    let cursor_str = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SCAN"),
    };
    let cursor: u64 = match std::str::from_utf8(cursor_str)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
    };
    // Clamp to the 48-bit hash space. Legitimate resumed cursors always
    // fit (multi-shard composites are unpacked by `coordinate_scan` before
    // reaching here); an out-of-range client cursor would otherwise filter
    // out every key (h48 < 2^48) and falsely report "scan complete" on a
    // non-empty keyspace.
    let cursor = cursor & 0x0000_FFFF_FFFF_FFFF;

    // One parser for the whole family — see `command::scan_options`.
    let opts = match crate::command::scan_options::parse_scan_options(
        crate::command::scan_options::ScanKind::Keyspace,
        &args[1..],
    ) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let match_pattern = opts.pattern;
    let count = opts.count;
    let type_filter = opts.type_filter;

    let now_ms = db.now_ms();
    scan_core(db, cursor, count, match_pattern, type_filter, now_ms)
}

// The SCAN cursor is a POSITION IN 48-BIT HASH SPACE: the hot table's own
// fixed-seed key hash truncated to its TOP 48 bits
// (`storage::dashtable::hash_key(key) >> 16`). Neither plane computes it
// here anymore — `Database::scan_hot_page` walks only the DashTable
// segments covering `[cursor << 16, ..)` and the ordered cold index
// (`Database::cold_only_keys_from`) range-resumes from the cursor — but
// both derive the SAME value internally, keeping the two planes in one
// merged hash order. 48 bits because the multi-shard composite cursor
// reserves the upper 16 bits for the shard index (`coordinate_scan`).

/// Shared SCAN page walk (#368): hash-ordered iteration with a real
/// stable-key guarantee, replacing the old positional-index-over-a-
/// per-call-re-sort design.
///
/// Keys are visited in `(hash48(key), key)` order; the cursor is the next
/// unvisited hash. A key's hash never changes, so inserts/deletes/spills/
/// promotions between pages cannot displace another key's position —
/// Redis's contract ("a key present for the entire scan is returned at
/// least once"; here exactly once) holds under churn. Per page the hot
/// plane is a true O(COUNT) segment-range walk (`Database::scan_hot_page`
/// visits only DashTable segments covering hashes ≥ cursor, #368); the
/// cold plane is still a filtered in-RAM index walk. Both feed one bounded
/// `count`-min selection heap — no full-keyspace sort and no second
/// lookup pass (the old design paid `collect + sort + exists() +
/// get_if_alive()` over every key on every page).
///
/// Page-boundary rule: a full page never advances the cursor past a hash
/// value whose key group might be only partially selected, so hash
/// collisions (rare at 48 bits) can never skip a key. Trailing entries
/// sharing the last hash are deferred to the next page; if an entire page
/// shares one hash (pathological), the whole equal-hash group is emitted
/// and the cursor steps past it.
fn scan_core(
    db: &Database,
    cursor: u64,
    count: usize,
    match_pattern: Option<&[u8]>,
    type_filter: Option<&[u8]>,
    now_ms: u64,
) -> Frame {
    use std::collections::BinaryHeap;

    // Hot plane: O(COUNT) hash-range page — only the DashTable segments
    // covering hashes ≥ cursor are visited; entries arrive live-filtered
    // with their hash48 precomputed. When `more`, the page holds ≥ count
    // entries, all below anything in unvisited segments, so it always
    // contains the count smallest hot candidates.
    let (hot_page, _hot_more) = db.scan_hot_page(cursor, count, now_ms);

    // Bounded max-heap selection: the `count` smallest (hash, key) pairs
    // at or after the cursor, across both planes.
    let mut heap: BinaryHeap<(u64, CompactKey, bool)> = BinaryHeap::with_capacity(count + 1);
    {
        let mut consider = |h: u64, key_bytes: &[u8], is_cold: bool| {
            if h < cursor {
                return;
            }
            if heap.len() < count {
                heap.push((h, CompactKey::from(key_bytes), is_cold));
            } else if let Some(top) = heap.peek() {
                if h < top.0 || (h == top.0 && key_bytes < top.1.as_bytes()) {
                    heap.pop();
                    heap.push((h, CompactKey::from(key_bytes), is_cold));
                }
            }
        };
        for (h, key) in &hot_page {
            consider(*h, key.as_bytes(), false);
        }
        // Cold plane: spilled keys with no live hot shadow (pure in-RAM
        // index probe — no disk I/O). Partitioned from the hot walk, so no
        // dedup pass (#364). Hash-ordered range resume: the first `count`
        // live candidates at or after the cursor are exactly the smallest
        // cold candidates — no full cold-index filter per page.
        for (h, key) in db.cold_only_keys_from(cursor, now_ms).take(count) {
            consider(h, key.as_ref(), true);
        }
    }

    let full_page = heap.len() == count;
    let mut selected = heap.into_sorted_vec();
    let mut next_cursor: u64 = 0;
    if full_page {
        #[allow(clippy::unwrap_used)] // full_page ⇒ count ≥ 1 entries
        let h_last = selected.last().unwrap().0;
        let h_first = selected.first().map(|e| e.0).unwrap_or(h_last);
        if h_first == h_last {
            // Whole page one hash value: emit the ENTIRE equal-hash group
            // (unreachable in practice at 48 bits) so the cursor may step
            // past it. Hot members all live in the already-fetched page:
            // equal h48 ⇒ same DashTable segment, and pages are
            // whole-segment granular.
            let mut extra: Vec<(u64, CompactKey, bool)> = Vec::new();
            for (h, key) in &hot_page {
                if *h == h_last && !selected.iter().any(|(_, k, _)| k == key) {
                    extra.push((h_last, key.clone(), false));
                }
            }
            for (h, key) in db.cold_only_keys_from(h_last, now_ms) {
                if h > h_last {
                    break;
                }
                if !selected
                    .iter()
                    .any(|(_, k, _)| k.as_bytes() == key.as_ref())
                {
                    extra.push((h_last, CompactKey::from(key.as_ref()), true));
                }
            }
            selected.extend(extra);
            // If the group sits at the very top of the 48-bit hash space,
            // the scan is complete — nothing can hash above it, and
            // `h_last + 1` (2^48) would be masked back to 0 by the cursor
            // clamp on the next call, restarting the scan forever. Not
            // practically constructible (needs a full page of keys at the
            // exact max hash), hence no test — this is belt-and-braces
            // against an infinite scan loop.
            next_cursor = if h_last == 0x0000_FFFF_FFFF_FFFF {
                0
            } else {
                h_last + 1
            };
        } else {
            // Defer the (possibly incomplete) trailing hash group to the
            // next page; resume exactly at its hash.
            selected.retain(|(h, _, _)| *h < h_last);
            next_cursor = h_last;
        }
    }

    let mut results = Vec::new();
    for (_, key, is_cold) in &selected {
        // TYPE filter (`is_cold` avoids a promoting/disk-reading lookup)
        if let Some(tf) = type_filter {
            let matches = if *is_cold {
                cold_type_matches(db, key.as_bytes(), tf)
            } else {
                db.get_if_alive(key.as_bytes(), now_ms)
                    .is_some_and(|e| tf.eq_ignore_ascii_case(e.value.type_name().as_bytes()))
            };
            if !matches {
                continue;
            }
        }
        if let Some(pattern) = match_pattern {
            if !glob_match(pattern, key.as_bytes()) {
                continue;
            }
        }
        results.push(Frame::BulkString(key.to_bytes()));
    }

    let next_cursor_bytes = if next_cursor == 0 {
        Bytes::from_static(b"0")
    } else {
        Bytes::from(next_cursor.to_string())
    };

    Frame::Array(framevec![
        Frame::BulkString(next_cursor_bytes),
        Frame::Array(results.into()),
    ])
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// EXISTS (read-only).
pub fn exists_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("EXISTS");
    }
    let mut count: i64 = 0;
    for arg in args {
        if let Some(key) = extract_key(arg) {
            if db.exists_if_alive(key, now_ms) {
                count += 1;
            }
        }
    }
    Frame::Integer(count)
}

/// TTL (read-only).
pub fn ttl_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("TTL");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("TTL"),
    };
    match db.get_if_alive_any_plane(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now = current_time_ms();
                let exp_ms = entry.expires_at_ms();
                if now >= exp_ms {
                    Frame::Integer(-2)
                } else {
                    Frame::Integer(((exp_ms - now) / 1000) as i64)
                }
            }
        }
    }
}

/// PTTL (read-only).
pub fn pttl_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("PTTL");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PTTL"),
    };
    match db.get_if_alive_any_plane(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now = current_time_ms();
                let exp_ms = entry.expires_at_ms();
                if now >= exp_ms {
                    Frame::Integer(-2)
                } else {
                    Frame::Integer((exp_ms - now) as i64)
                }
            }
        }
    }
}

/// TYPE (read-only).
pub fn type_cmd_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("TYPE");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("TYPE"),
    };
    match db.get_if_alive_any_plane(key, now_ms) {
        None => Frame::SimpleString(Bytes::from_static(b"none")),
        Some(entry) => {
            let type_name = entry.value.type_name();
            Frame::SimpleString(Bytes::from_static(type_name.as_bytes()))
        }
    }
}

/// KEYS (read-only).
pub fn keys_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("KEYS");
    }
    let pattern = match extract_key(&args[0]) {
        Some(p) => p,
        None => return err_wrong_args("KEYS"),
    };

    let mut result = Vec::new();
    for key in db.keys() {
        // Strict hot-aliveness: cold-visible keys enter exactly once, via
        // the cold loop below (#364 plane partition).
        if db.get_if_alive(key.as_bytes(), now_ms).is_some() && glob_match(pattern, key.as_bytes())
        {
            result.push(Frame::BulkString(key.to_bytes()));
        }
    }
    for key in db.cold_only_keys(now_ms) {
        if glob_match(pattern, key.as_ref()) {
            result.push(Frame::BulkString(key.clone()));
        }
    }
    Frame::Array(result.into())
}

/// SCAN (read-only).
pub fn scan_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SCAN");
    }

    let cursor_str = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SCAN"),
    };
    let cursor: u64 = match std::str::from_utf8(cursor_str)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
    };
    // Clamp to the 48-bit hash space. Legitimate resumed cursors always
    // fit (multi-shard composites are unpacked by `coordinate_scan` before
    // reaching here); an out-of-range client cursor would otherwise filter
    // out every key (h48 < 2^48) and falsely report "scan complete" on a
    // non-empty keyspace.
    let cursor = cursor & 0x0000_FFFF_FFFF_FFFF;

    // One parser for the whole family — see `command::scan_options`.
    let opts = match crate::command::scan_options::parse_scan_options(
        crate::command::scan_options::ScanKind::Keyspace,
        &args[1..],
    ) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let match_pattern = opts.pattern;
    let count = opts.count;
    let type_filter = opts.type_filter;

    scan_core(db, cursor, count, match_pattern, type_filter, now_ms)
}

// ---------------------------------------------------------------------------
// New read-only twins (dispatch_read path)
// ---------------------------------------------------------------------------

/// EXPIRETIME key — read-only twin.
///
/// Returns -2 if missing/expired, -1 if no TTL, else absolute Unix seconds.
pub fn expiretime_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("EXPIRETIME");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("EXPIRETIME"),
    };
    match db.get_if_alive_any_plane(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer((entry.expires_at_ms() / 1000) as i64)
            }
        }
    }
}

/// PEXPIRETIME key — read-only twin.
///
/// Returns -2 if missing/expired, -1 if no TTL, else absolute Unix milliseconds.
pub fn pexpiretime_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("PEXPIRETIME");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("PEXPIRETIME"),
    };
    match db.get_if_alive_any_plane(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer(entry.expires_at_ms() as i64)
            }
        }
    }
}

/// RANDOMKEY — read-only twin.
///
/// Returns a random alive key, or Null if all keys are expired/absent.
/// Uses `random_key()` which already filters expired keys without deleting them.
pub fn randomkey_readonly(db: &Database, _args: &[Frame], _now_ms: u64) -> Frame {
    match db.random_key() {
        Some(key) => Frame::BulkString(key),
        None => Frame::Null,
    }
}

/// TOUCH key [key …] — read-only twin.
///
/// Counts alive keys. Does NOT update LRU/access metadata (contract M2).
pub fn touch_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("TOUCH");
    }
    let mut count = 0i64;
    for arg in args {
        let key = match extract_key(arg) {
            Some(k) => k,
            None => continue,
        };
        if db.exists_if_alive(key, now_ms) {
            count += 1;
        }
    }
    Frame::Integer(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::{Entry, current_time_ms};

    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn setup_db_with_key(key: &[u8], val: &[u8]) -> Database {
        let mut db = Database::new();
        db.set(key, Entry::new_string(Bytes::copy_from_slice(val)));
        db
    }

    fn setup_db_with_expiry(key: &[u8], val: &[u8], expires_at_ms: u64) -> Database {
        let mut db = Database::new();
        db.set(
            key,
            Entry::new_string_with_expiry(Bytes::copy_from_slice(val), expires_at_ms),
        );
        db
    }

    // --- SCAN cursor tests (issue #368: hash-ordered stable-key cursor) ---

    fn scan_page(db: &mut Database, cursor: u64, count: usize) -> (u64, Vec<Bytes>) {
        let reply = scan(
            db,
            &[
                bs(cursor.to_string().as_bytes()),
                bs(b"COUNT"),
                bs(count.to_string().as_bytes()),
            ],
        );
        let Frame::Array(parts) = reply else {
            panic!("SCAN must return an array");
        };
        let next: u64 = match &parts[0] {
            Frame::BulkString(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
            other => panic!("cursor frame: {other:?}"),
        };
        let keys = match &parts[1] {
            Frame::Array(items) => items
                .iter()
                .map(|f| match f {
                    Frame::BulkString(b) => b.clone(),
                    other => panic!("key frame: {other:?}"),
                })
                .collect(),
            other => panic!("keys frame: {other:?}"),
        };
        (next, keys)
    }

    /// Issue #368 red/green: the old positional cursor skipped stable keys
    /// when deletions shifted positions between pages. The hash-ordered
    /// cursor must return every key that exists for the entire scan.
    #[test]
    fn scan_returns_all_stable_keys_under_delete_and_insert_churn() {
        let mut db = Database::new();
        for i in 0..100 {
            db.set(
                &Bytes::from(format!("stable:{i:03}")),
                Entry::new_string(Bytes::from_static(b"v")),
            );
        }

        let mut returned: std::collections::HashSet<Bytes> = std::collections::HashSet::new();
        let mut cursor = 0u64;
        let mut churn = 0;
        loop {
            let (next, keys) = scan_page(&mut db, cursor, 10);
            for k in keys {
                returned.insert(k.clone());
                // Churn: delete a key we already got, insert a brand-new one.
                if churn < 40 {
                    db.remove(k.as_ref());
                    db.set(
                        &Bytes::from(format!("churn:{churn:03}")),
                        Entry::new_string(Bytes::from_static(b"v")),
                    );
                    churn += 1;
                }
            }
            if next == 0 {
                break;
            }
            assert!(next < 1 << 48, "cursor must fit the 48-bit composite slot");
            cursor = next;
        }

        for i in 0..100 {
            let key = format!("stable:{i:03}");
            assert!(
                returned.contains(key.as_bytes()),
                "stable key {key} was skipped under churn — the SCAN \
                 stable-key guarantee is broken"
            );
        }
    }

    /// Full no-churn drain: exact key-set equality and no duplicates.
    #[test]
    fn scan_full_drain_is_exact_and_duplicate_free() {
        let mut db = Database::new();
        for i in 0..57 {
            db.set(
                &Bytes::from(format!("k:{i}")),
                Entry::new_string(Bytes::from_static(b"v")),
            );
        }
        let mut seen: Vec<Bytes> = Vec::new();
        let mut cursor = 0u64;
        loop {
            let (next, keys) = scan_page(&mut db, cursor, 7);
            seen.extend(keys);
            if next == 0 {
                break;
            }
            cursor = next;
        }
        let unique: std::collections::HashSet<&Bytes> = seen.iter().collect();
        assert_eq!(unique.len(), seen.len(), "no key may be returned twice");
        assert_eq!(seen.len(), 57, "every live key exactly once");
    }

    /// An out-of-range client cursor (bits above the 48-bit hash space,
    /// e.g. a composite cursor replayed against a single-shard server)
    /// must be clamped, not silently filter out every key and report a
    /// false "scan complete" on a non-empty keyspace.
    #[test]
    fn scan_out_of_range_cursor_clamps_to_hash_space() {
        let mut db = Database::new();
        for i in 0..20 {
            db.set(
                &Bytes::from(format!("k:{i}")),
                Entry::new_string(Bytes::from_static(b"v")),
            );
        }
        // 2^63: garbage in the shard-index bits, zero in the low 48.
        let (_, keys) = scan_page(&mut db, 1u64 << 63, 50);
        assert_eq!(keys.len(), 20, "clamped cursor must scan from position 0");
    }

    /// MATCH + COUNT paging still terminates and honors the filter.
    #[test]
    fn scan_match_filter_pages_terminate() {
        let mut db = Database::new();
        for i in 0..30 {
            db.set(
                &Bytes::from(format!("user:{i}")),
                Entry::new_string(Bytes::from_static(b"v")),
            );
            db.set(
                &Bytes::from(format!("other:{i}")),
                Entry::new_string(Bytes::from_static(b"v")),
            );
        }
        let mut matched = 0usize;
        let mut cursor = 0u64;
        let mut pages = 0;
        loop {
            let reply = scan(
                &mut db,
                &[
                    bs(cursor.to_string().as_bytes()),
                    bs(b"MATCH"),
                    bs(b"user:*"),
                    bs(b"COUNT"),
                    bs(b"5"),
                ],
            );
            let Frame::Array(parts) = reply else {
                panic!("array")
            };
            let next: u64 = match &parts[0] {
                Frame::BulkString(b) => std::str::from_utf8(b).unwrap().parse().unwrap(),
                _ => panic!("cursor"),
            };
            if let Frame::Array(items) = &parts[1] {
                for f in items.iter() {
                    if let Frame::BulkString(b) = f {
                        assert!(b.starts_with(b"user:"), "MATCH must filter");
                        matched += 1;
                    }
                }
            }
            pages += 1;
            assert!(pages < 1000, "must terminate");
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert_eq!(matched, 30, "every user:* key exactly once");
    }

    // --- DBSIZE tests (issue #355: logical count under disk-offload) ---

    fn cold_loc(file_id: u64) -> crate::storage::tiered::cold_index::ColdLocation {
        crate::storage::tiered::cold_index::ColdLocation {
            file_id,
            page_idx: 0,
            slot_idx: 0,
            ttl_ms: None,
            value_type: crate::persistence::kv_page::ValueType::String,
        }
    }

    /// Issue #355 red/green: a spilled-but-readable key is still a key.
    /// DBSIZE must count hot + cold, not the resident set only (observed
    /// 24K reported vs ~164K logical in the 2026-07-16 G2 re-run).
    #[test]
    fn test_dbsize_counts_cold_entries() {
        use crate::storage::tiered::cold_index::ColdIndex;
        let mut db = setup_db_with_key(b"hot1", b"v");
        db.set(b"hot2", Entry::new_string(Bytes::from_static(b"v")));
        let mut ci = ColdIndex::new();
        ci.insert(Bytes::from_static(b"cold1"), cold_loc(1));
        ci.insert(Bytes::from_static(b"cold2"), cold_loc(2));
        ci.insert(Bytes::from_static(b"cold3"), cold_loc(3));
        db.cold_index = Some(ci);

        assert_eq!(dbsize(&mut db, &[]), Frame::Integer(5));
        assert_eq!(dbsize_readonly(&db, &[]), Frame::Integer(5));
    }

    /// Issue #355: a key that is BOTH hot and cold-shadowed (fresh SET over a
    /// cold-only key lands on the `Inserted` arm, which must leave the shadow
    /// for the AOF-replay ambiguity proof — see `Database::set`) counts ONCE.
    #[test]
    fn test_dbsize_does_not_double_count_cold_shadowed_hot_key() {
        use crate::storage::tiered::cold_index::ColdIndex;
        let mut ci = ColdIndex::new();
        // Cold entries exist FIRST (rebuilt from manifest / spilled earlier)…
        ci.insert(Bytes::from_static(b"shadowed"), cold_loc(1));
        ci.insert(Bytes::from_static(b"cold_only"), cold_loc(2));
        let mut db = Database::new();
        db.cold_index = Some(ci);
        // …then a fresh SET over the cold-only key: `Inserted` arm, shadow
        // intentionally survives.
        db.set(b"shadowed", Entry::new_string(Bytes::from_static(b"new")));
        assert!(db.is_hot(b"shadowed"));
        assert!(
            db.cold_index
                .as_ref()
                .unwrap()
                .lookup(b"shadowed")
                .is_some(),
            "precondition: the cold shadow must still exist for this test to bite"
        );

        // shadowed (1) + cold_only (1) = 2 logical keys, never 3.
        assert_eq!(dbsize(&mut db, &[]), Frame::Integer(2));
        assert_eq!(dbsize_readonly(&db, &[]), Frame::Integer(2));
    }

    /// Control: without a cold index (disk-offload disabled) DBSIZE is the
    /// plain hot count, and `clear()` zeroes both planes.
    #[test]
    fn test_dbsize_no_cold_index_and_clear() {
        use crate::storage::tiered::cold_index::ColdIndex;
        let mut db = setup_db_with_key(b"k", b"v");
        assert_eq!(dbsize(&mut db, &[]), Frame::Integer(1));

        let mut ci = ColdIndex::new();
        ci.insert(Bytes::from_static(b"cold"), cold_loc(1));
        db.cold_index = Some(ci);
        assert_eq!(dbsize(&mut db, &[]), Frame::Integer(2));

        db.clear();
        assert_eq!(dbsize(&mut db, &[]), Frame::Integer(0));
    }

    // --- DEL tests ---

    #[test]
    fn test_del_single() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = del(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(1));
        assert!(!db.exists(b"foo"));
    }

    #[test]
    fn test_del_multiple() {
        let mut db = Database::new();
        db.set(b"a", Entry::new_string(Bytes::from_static(b"1")));
        db.set(b"b", Entry::new_string(Bytes::from_static(b"2")));
        db.set(b"c", Entry::new_string(Bytes::from_static(b"3")));
        let result = del(&mut db, &[bs(b"a"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(2));
        assert!(db.exists(b"b"));
    }

    #[test]
    fn test_del_missing() {
        let mut db = Database::new();
        let result = del(&mut db, &[bs(b"nonexistent")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- EXISTS tests ---

    #[test]
    fn test_exists_single() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = exists(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_exists_duplicate_counted() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = exists(&mut db, &[bs(b"foo"), bs(b"foo")]);
        assert_eq!(result, Frame::Integer(2));
    }

    #[test]
    fn test_exists_missing() {
        let mut db = Database::new();
        let result = exists(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- EXPIRE tests ---

    #[test]
    fn test_expire_sets_ttl() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = expire(&mut db, &[bs(b"foo"), bs(b"100")]);
        assert_eq!(result, Frame::Integer(1));
        // TTL should be positive
        let ttl_result = ttl(&mut db, &[bs(b"foo")]);
        match ttl_result {
            Frame::Integer(n) => assert!(n > 0 && n <= 100, "TTL was {}", n),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_expire_missing_key() {
        let mut db = Database::new();
        let result = expire(&mut db, &[bs(b"foo"), bs(b"100")]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_expire_nonpositive_deletes() {
        // Redis parity: EXPIRE with a non-positive TTL deletes the key immediately
        // (past-time expiry) and returns 1 -- it must NOT error and leave the key.
        let mut db = setup_db_with_key(b"foo", b"bar");
        assert_eq!(expire(&mut db, &[bs(b"foo"), bs(b"-1")]), Frame::Integer(1));
        assert!(!db.exists(b"foo"), "EXPIRE foo -1 must delete the key");

        let mut db2 = setup_db_with_key(b"foo", b"bar");
        assert_eq!(expire(&mut db2, &[bs(b"foo"), bs(b"0")]), Frame::Integer(1));
        assert!(!db2.exists(b"foo"), "EXPIRE foo 0 must delete the key");
    }

    #[test]
    fn test_expire_nonpositive_missing_key() {
        let mut db = Database::new();
        assert_eq!(
            expire(&mut db, &[bs(b"nope"), bs(b"-1")]),
            Frame::Integer(0)
        );
    }

    #[test]
    fn test_expire_overflow_rejected() {
        // now_ms + seconds*1000 overflows u64 -> error, no silent wrap; key untouched.
        let mut db = setup_db_with_key(b"foo", b"bar");
        let huge = Frame::BulkString(Bytes::from(i64::MAX.to_string()));
        let result = expire(&mut db, &[bs(b"foo"), huge]);
        assert!(
            matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR invalid expire")),
            "overflowing EXPIRE must error, got {result:?}"
        );
        assert!(
            db.exists(b"foo"),
            "rejected EXPIRE must not disturb the key"
        );
    }

    // --- i64-domain expiry bound (Finding 2/3): an absolute expiry past i64::MAX
    // would surface as a NEGATIVE TTL on a live key, because PTTL/PEXPIRETIME cast
    // the stored u64 back to i64. Redis rejects such expiries outright
    // (`when > LLONG_MAX/1000` -> "invalid expire time"); Moon must too. ---

    #[test]
    fn test_expire_i64_bound_rejected() {
        // seconds*1000 fits u64 but now+that exceeds i64::MAX. Without the i64 bound
        // Moon accepts it and PTTL wraps negative. Must error and leave the key's TTL
        // untouched (no expiry set -> PTTL == -1, never a bogus large-negative).
        let mut db = setup_db_with_key(b"foo", b"bar");
        let over = Frame::BulkString(Bytes::from("15000000000000000")); // 1.5e16 s
        let result = expire(&mut db, &[bs(b"foo"), over]);
        assert!(
            matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR invalid expire")),
            "EXPIRE past i64::MAX must error, got {result:?}"
        );
        assert!(
            db.exists(b"foo"),
            "rejected EXPIRE must not disturb the key"
        );
        assert_eq!(
            pttl(&mut db, &[bs(b"foo")]),
            Frame::Integer(-1),
            "rejected EXPIRE must leave the key with no expiry (PTTL -1, never wrapped-negative)"
        );
    }

    #[test]
    fn test_expire_extreme_negative_errors_not_deletes() {
        // Redis rejects |seconds| > LLONG_MAX/1000 BEFORE the past-time delete
        // (when < LLONG_MIN/1000). i64::MIN must ERROR and PRESERVE the key, unlike a
        // normal small negative which deletes.
        let mut db = setup_db_with_key(b"foo", b"bar");
        let min = Frame::BulkString(Bytes::from(i64::MIN.to_string()));
        let result = expire(&mut db, &[bs(b"foo"), min]);
        assert!(
            matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR invalid expire")),
            "EXPIRE i64::MIN must error, got {result:?}"
        );
        assert!(
            db.exists(b"foo"),
            "rejected extreme-negative EXPIRE must not delete the key"
        );
    }

    #[test]
    fn test_pexpire_i64_bound_rejected() {
        // now_ms + i64::MAX ms exceeds i64::MAX -> reject (else PTTL wraps negative).
        let mut db = setup_db_with_key(b"foo", b"bar");
        let over = Frame::BulkString(Bytes::from(i64::MAX.to_string()));
        let result = pexpire(&mut db, &[bs(b"foo"), over]);
        assert!(
            matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR invalid expire")),
            "PEXPIRE past i64::MAX must error, got {result:?}"
        );
        assert!(db.exists(b"foo"));
        assert_eq!(pttl(&mut db, &[bs(b"foo")]), Frame::Integer(-1));
    }

    #[test]
    fn test_expireat_i64_bound_rejected() {
        // absolute seconds*1000 exceeds i64::MAX -> reject (else PEXPIRETIME wraps negative).
        let mut db = setup_db_with_key(b"foo", b"bar");
        let over = Frame::BulkString(Bytes::from("15000000000000000")); // 1.5e16 s
        let result = expireat(&mut db, &[bs(b"foo"), over]);
        assert!(
            matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR invalid expire")),
            "EXPIREAT past i64::MAX must error, got {result:?}"
        );
        assert!(db.exists(b"foo"));
        assert_eq!(pexpiretime(&mut db, &[bs(b"foo")]), Frame::Integer(-1));
    }

    #[test]
    fn test_expireat_extreme_negative_errors_not_deletes() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let min = Frame::BulkString(Bytes::from(i64::MIN.to_string()));
        let result = expireat(&mut db, &[bs(b"foo"), min]);
        assert!(
            matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR invalid expire")),
            "EXPIREAT i64::MIN must error, got {result:?}"
        );
        assert!(
            db.exists(b"foo"),
            "rejected extreme-negative EXPIREAT must not delete the key"
        );
    }

    // --- PEXPIRE tests ---

    #[test]
    fn test_pexpire_sets_ttl() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = pexpire(&mut db, &[bs(b"foo"), bs(b"100000")]);
        assert_eq!(result, Frame::Integer(1));
        let pttl_result = pttl(&mut db, &[bs(b"foo")]);
        match pttl_result {
            Frame::Integer(n) => assert!(n > 0 && n <= 100000, "PTTL was {}", n),
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_pexpire_nonpositive_deletes() {
        // Redis parity: PEXPIRE with a non-positive TTL deletes the key and returns 1.
        let mut db = setup_db_with_key(b"foo", b"bar");
        assert_eq!(
            pexpire(&mut db, &[bs(b"foo"), bs(b"-1")]),
            Frame::Integer(1)
        );
        assert!(!db.exists(b"foo"), "PEXPIRE foo -1 must delete the key");
    }

    // --- TTL tests ---

    #[test]
    fn test_ttl_no_expiry() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = ttl(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(-1));
    }

    #[test]
    fn test_ttl_missing_key() {
        let mut db = Database::new();
        let result = ttl(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(-2));
    }

    // --- PTTL tests ---

    #[test]
    fn test_pttl_no_expiry() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = pttl(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(-1));
    }

    // --- PERSIST tests ---

    #[test]
    fn test_persist_removes_ttl() {
        let mut db = setup_db_with_expiry(b"foo", b"bar", current_time_ms() + 3_600_000);
        // Verify TTL exists
        let t = ttl(&mut db, &[bs(b"foo")]);
        match t {
            Frame::Integer(n) => assert!(n > 0),
            _ => panic!("Expected positive TTL"),
        }
        // PERSIST
        let result = persist(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(1));
        // TTL should now be -1
        let t = ttl(&mut db, &[bs(b"foo")]);
        assert_eq!(t, Frame::Integer(-1));
    }

    #[test]
    fn test_persist_no_ttl() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = persist(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- TYPE tests ---

    #[test]
    fn test_type_string() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = type_cmd(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"string")));
    }

    #[test]
    fn test_type_none() {
        let mut db = Database::new();
        let result = type_cmd(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"none")));
    }

    // --- Glob matcher tests ---

    #[test]
    fn test_glob_star() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"hello world"));
    }

    #[test]
    fn test_glob_question() {
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(glob_match(b"h?llo", b"hxllo"));
        assert!(!glob_match(b"h?llo", b"hllo"));
    }

    #[test]
    fn test_glob_star_prefix() {
        assert!(glob_match(b"h*llo", b"hllo"));
        assert!(glob_match(b"h*llo", b"heeeello"));
        assert!(glob_match(b"h*llo", b"hello"));
        assert!(!glob_match(b"h*llo", b"hllox"));
    }

    #[test]
    fn test_glob_char_class() {
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
    }

    #[test]
    fn test_glob_negated_class() {
        assert!(glob_match(b"h[^e]llo", b"hallo"));
        assert!(glob_match(b"h[^e]llo", b"hbllo"));
        assert!(!glob_match(b"h[^e]llo", b"hello"));
        // Also test ! syntax
        assert!(glob_match(b"h[!e]llo", b"hallo"));
        assert!(!glob_match(b"h[!e]llo", b"hello"));
    }

    #[test]
    fn test_glob_range() {
        assert!(glob_match(b"h[a-b]llo", b"hallo"));
        assert!(glob_match(b"h[a-b]llo", b"hbllo"));
        assert!(!glob_match(b"h[a-b]llo", b"hcllo"));
    }

    #[test]
    fn test_glob_escaped() {
        assert!(glob_match(b"h\\*llo", b"h*llo"));
        assert!(!glob_match(b"h\\*llo", b"hello"));
        assert!(!glob_match(b"h\\*llo", b"heeeello"));
    }

    // --- KEYS tests ---

    #[test]
    fn test_keys_all() {
        let mut db = Database::new();
        db.set(b"foo", Entry::new_string(Bytes::from_static(b"1")));
        db.set(b"bar", Entry::new_string(Bytes::from_static(b"2")));
        db.set(b"baz", Entry::new_string(Bytes::from_static(b"3")));
        let result = keys(&mut db, &[bs(b"*")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_keys_pattern() {
        let mut db = Database::new();
        db.set(b"hello", Entry::new_string(Bytes::from_static(b"1")));
        db.set(b"hallo", Entry::new_string(Bytes::from_static(b"2")));
        db.set(b"world", Entry::new_string(Bytes::from_static(b"3")));
        let result = keys(&mut db, &[bs(b"h?llo")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_keys_expired_excluded() {
        let mut db = Database::new();
        db.set(b"alive", Entry::new_string(Bytes::from_static(b"1")));
        let past_ms = current_time_ms() - 1000;
        db.set(
            b"dead",
            Entry::new_string_with_expiry(Bytes::from_static(b"2"), past_ms),
        );
        let result = keys(&mut db, &[bs(b"*")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"alive")));
            }
            _ => panic!("Expected array"),
        }
    }

    // --- RENAME tests ---

    #[test]
    fn test_rename_basic() {
        let mut db = setup_db_with_key(b"old", b"value");
        let result = rename(&mut db, &[bs(b"old"), bs(b"new")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(!db.exists(b"old"));
        assert!(db.exists(b"new"));
    }

    #[test]
    fn test_rename_same_key() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = rename(&mut db, &[bs(b"foo"), bs(b"foo")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        // Key should still exist (no-op, not deleted)
        assert!(db.exists(b"foo"));
    }

    #[test]
    fn test_rename_missing_source() {
        let mut db = Database::new();
        let result = rename(&mut db, &[bs(b"missing"), bs(b"new")]);
        assert!(matches!(result, Frame::Error(ref s) if s.as_ref() == b"ERR no such key"));
    }

    #[test]
    fn test_rename_preserves_ttl() {
        let future_ms = current_time_ms() + 3_600_000;
        let mut db = setup_db_with_expiry(b"old", b"value", future_ms);
        rename(&mut db, &[bs(b"old"), bs(b"new")]);
        // TTL should be preserved on new key
        let t = ttl(&mut db, &[bs(b"new")]);
        match t {
            Frame::Integer(n) => assert!(n > 0, "TTL should be positive, got {}", n),
            _ => panic!("Expected positive TTL"),
        }
    }

    #[test]
    fn test_rename_overwrites_dest() {
        let mut db = Database::new();
        db.set(b"src", Entry::new_string(Bytes::from_static(b"srcval")));
        db.set(b"dst", Entry::new_string(Bytes::from_static(b"dstval")));
        rename(&mut db, &[bs(b"src"), bs(b"dst")]);
        assert!(!db.exists(b"src"));
        let entry = db.get(b"dst").unwrap();
        assert_eq!(entry.value.as_bytes().unwrap(), b"srcval");
    }

    // --- RENAMENX tests ---

    #[test]
    fn test_renamenx_success() {
        let mut db = setup_db_with_key(b"old", b"value");
        let result = renamenx(&mut db, &[bs(b"old"), bs(b"new")]);
        assert_eq!(result, Frame::Integer(1));
        assert!(!db.exists(b"old"));
        assert!(db.exists(b"new"));
    }

    #[test]
    fn test_renamenx_dest_exists() {
        let mut db = Database::new();
        db.set(b"src", Entry::new_string(Bytes::from_static(b"1")));
        db.set(b"dst", Entry::new_string(Bytes::from_static(b"2")));
        let result = renamenx(&mut db, &[bs(b"src"), bs(b"dst")]);
        assert_eq!(result, Frame::Integer(0));
        // Both keys should still exist
        assert!(db.exists(b"src"));
        assert!(db.exists(b"dst"));
    }

    // --- UNLINK tests ---

    #[test]
    fn test_unlink_single() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = unlink(&mut db, &[bs(b"foo")]);
        assert_eq!(result, Frame::Integer(1));
        assert!(!db.exists(b"foo"));
    }

    #[test]
    fn test_unlink_multiple() {
        let mut db = Database::new();
        db.set(b"a", Entry::new_string(Bytes::from_static(b"1")));
        db.set(b"b", Entry::new_string(Bytes::from_static(b"2")));
        db.set(b"c", Entry::new_string(Bytes::from_static(b"3")));
        let result = unlink(&mut db, &[bs(b"a"), bs(b"c"), bs(b"missing")]);
        assert_eq!(result, Frame::Integer(2));
        assert!(db.exists(b"b"));
    }

    #[test]
    fn test_unlink_no_args() {
        let mut db = Database::new();
        let result = unlink(&mut db, &[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- TYPE tests for collection types ---

    #[test]
    fn test_type_hash() {
        let mut db = Database::new();
        db.set(b"h", Entry::new_hash());
        let result = type_cmd(&mut db, &[bs(b"h")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"hash")));
    }

    #[test]
    fn test_type_list() {
        let mut db = Database::new();
        db.set(b"l", Entry::new_list());
        let result = type_cmd(&mut db, &[bs(b"l")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"list")));
    }

    #[test]
    fn test_type_set() {
        let mut db = Database::new();
        db.set(b"s", Entry::new_set());
        let result = type_cmd(&mut db, &[bs(b"s")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"set")));
    }

    #[test]
    fn test_type_zset() {
        let mut db = Database::new();
        db.set(b"z", Entry::new_sorted_set());
        let result = type_cmd(&mut db, &[bs(b"z")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"zset")));
    }

    // --- SCAN tests ---

    #[test]
    fn test_scan_basic() {
        let mut db = Database::new();
        db.set(b"key1", Entry::new_string(Bytes::from_static(b"v1")));
        db.set(b"key2", Entry::new_string(Bytes::from_static(b"v2")));
        db.set(b"key3", Entry::new_string(Bytes::from_static(b"v3")));

        let result = scan(&mut db, &[bs(b"0")]);
        match result {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 2);
                // First element is cursor, second is array of keys
                match &arr[0] {
                    Frame::BulkString(c) => assert_eq!(c.as_ref(), b"0"), // all returned
                    _ => panic!("Expected cursor"),
                }
                match &arr[1] {
                    Frame::Array(keys) => assert_eq!(keys.len(), 3),
                    _ => panic!("Expected keys array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_scan_with_count() {
        let mut db = Database::new();
        for i in 0..20 {
            db.set(
                &Bytes::from(format!("key{:02}", i)),
                Entry::new_string(Bytes::from_static(b"v")),
            );
        }

        let result = scan(&mut db, &[bs(b"0"), bs(b"COUNT"), bs(b"5")]);
        match result {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    Frame::BulkString(c) => assert_ne!(c.as_ref(), b"0"), // more to go
                    _ => panic!("Expected cursor"),
                }
                match &arr[1] {
                    // COUNT is a hint (Redis parity): the hash-ordered
                    // cursor (#368) may defer the trailing hash group to
                    // the next page, so a full page returns 1..=COUNT keys.
                    Frame::Array(keys) => {
                        assert!(
                            !keys.is_empty() && keys.len() <= 5,
                            "full page must return 1..=COUNT keys, got {}",
                            keys.len()
                        );
                    }
                    _ => panic!("Expected keys array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_scan_with_match() {
        let mut db = Database::new();
        db.set(b"user:1", Entry::new_string(Bytes::from_static(b"v")));
        db.set(b"user:2", Entry::new_string(Bytes::from_static(b"v")));
        db.set(b"post:1", Entry::new_string(Bytes::from_static(b"v")));

        let result = scan(&mut db, &[bs(b"0"), bs(b"MATCH"), bs(b"user:*")]);
        match result {
            Frame::Array(ref arr) => match &arr[1] {
                Frame::Array(keys) => assert_eq!(keys.len(), 2),
                _ => panic!("Expected keys array"),
            },
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_scan_with_type_filter() {
        let mut db = Database::new();
        db.set(b"str", Entry::new_string(Bytes::from_static(b"v")));
        db.set(b"hash", Entry::new_hash());
        db.set(b"list", Entry::new_list());

        let result = scan(&mut db, &[bs(b"0"), bs(b"TYPE"), bs(b"hash")]);
        match result {
            Frame::Array(ref arr) => match &arr[1] {
                Frame::Array(keys) => {
                    assert_eq!(keys.len(), 1);
                    assert_eq!(keys[0], Frame::BulkString(Bytes::from_static(b"hash")));
                }
                _ => panic!("Expected keys array"),
            },
            _ => panic!("Expected array"),
        }
    }

    // --- EXPIREAT / PEXPIREAT / EXPIRETIME / PEXPIRETIME tests ---

    #[test]
    fn test_expireat() {
        let mut db = setup_db_with_key(b"k", b"v");
        let future_ts = (current_time_ms() / 1000 + 3600) as i64;
        let result = expireat(
            &mut db,
            &[
                bs(b"k"),
                Frame::BulkString(Bytes::from(future_ts.to_string())),
            ],
        );
        assert_eq!(result, Frame::Integer(1));
        assert!(db.get(b"k").unwrap().has_expiry());
    }

    #[test]
    fn test_expireat_overflow_rejected() {
        // (timestamp * 1000) overflows u64 for huge timestamps -> error, key untouched.
        let mut db = setup_db_with_key(b"k", b"v");
        let huge = Frame::BulkString(Bytes::from(i64::MAX.to_string()));
        let result = expireat(&mut db, &[bs(b"k"), huge]);
        assert!(
            matches!(result, Frame::Error(ref e) if e.starts_with(b"ERR invalid expire")),
            "overflowing EXPIREAT must error, got {result:?}"
        );
        assert!(
            db.exists(b"k"),
            "rejected EXPIREAT must not disturb the key"
        );
    }

    #[test]
    fn test_expireat_missing() {
        let mut db = Database::new();
        let result = expireat(&mut db, &[bs(b"k"), bs(b"9999999999")]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_pexpireat() {
        let mut db = setup_db_with_key(b"k", b"v");
        let future_ms = (current_time_ms() + 3_600_000) as i64;
        let result = pexpireat(
            &mut db,
            &[
                bs(b"k"),
                Frame::BulkString(Bytes::from(future_ms.to_string())),
            ],
        );
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_expiretime() {
        let mut db = setup_db_with_key(b"k", b"v");
        // No expiry → -1
        let result = expiretime(&mut db, &[bs(b"k")]);
        assert_eq!(result, Frame::Integer(-1));
        // Missing → -2
        let result = expiretime(&mut db, &[bs(b"nope")]);
        assert_eq!(result, Frame::Integer(-2));
    }

    #[test]
    fn test_pexpiretime() {
        let mut db = setup_db_with_key(b"k", b"v");
        let result = pexpiretime(&mut db, &[bs(b"k")]);
        assert_eq!(result, Frame::Integer(-1));
    }

    // --- RANDOMKEY / TOUCH / TIME / FLUSHDB tests ---

    #[test]
    fn test_randomkey_empty() {
        let mut db = Database::new();
        assert_eq!(randomkey(&mut db, &[]), Frame::Null);
    }

    #[test]
    fn test_randomkey_nonempty() {
        let mut db = setup_db_with_key(b"only", b"val");
        match randomkey(&mut db, &[]) {
            Frame::BulkString(k) => assert_eq!(k.as_ref(), b"only"),
            _ => panic!("Expected BulkString"),
        }
    }

    /// moon#629: consecutive draws must not collapse onto one key.
    ///
    /// `random_key` used to index with `current_time_ms() % total`, so every
    /// call inside the same millisecond returned the SAME key — a client
    /// polling RANDOMKEY (the normal way to sample a keyspace) saw roughly one
    /// distinct name per millisecond of wall time however fast it asked. A
    /// live server measured 10 distinct keys of 64 across 300 draws in 17 ms;
    /// with the thread RNG it reaches 62.
    ///
    /// The bound is deliberately loose (a fair draw over 64 keys reaches ~63
    /// here, and even a poor RNG clears 20) so the test pins the DEFECT, not a
    /// particular RNG's quality — it cannot flake on an unlucky sample.
    #[test]
    fn randomkey_does_not_repeat_within_a_millisecond() {
        let mut db = Database::new();
        for i in 0..64u32 {
            db.set(
                &Bytes::from(format!("k{i}")),
                Entry::new_string(Bytes::from_static(b"v")),
            );
        }
        let mut seen = std::collections::HashSet::new();
        for _ in 0..300 {
            match randomkey(&mut db, &[]) {
                Frame::BulkString(k) => {
                    seen.insert(k);
                }
                other => panic!("RANDOMKEY on a 64-key db answered {other:?}"),
            }
        }
        assert!(
            seen.len() > 20,
            "300 draws over 64 keys reached only {} distinct keys — RANDOMKEY \
             is not drawing at random",
            seen.len()
        );
    }

    #[test]
    fn test_touch() {
        let mut db = setup_db_with_key(b"a", b"1");
        db.set(b"b", Entry::new_string(Bytes::from_static(b"2")));
        let result = touch(&mut db, &[bs(b"a"), bs(b"b"), bs(b"missing")]);
        assert_eq!(result, Frame::Integer(2));
    }

    #[test]
    fn test_time() {
        match time() {
            Frame::Array(ref arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_flushdb() {
        let mut db = setup_db_with_key(b"a", b"1");
        db.set(b"b", Entry::new_string(Bytes::from_static(b"2")));
        assert_eq!(db.len(), 2);
        let result = flushdb(&mut db, &[]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(db.len(), 0);
    }

    // --- OBJECT HELP regression tests (WS1 command-parity audit: found
    // already implemented on both the mutable and read-only tracks; these
    // tests lock in that coverage against regression). ---

    #[test]
    fn test_object_help_mutable_track() {
        let mut db = Database::new();
        let result = object(&mut db, &[bs(b"HELP")]);
        match result {
            Frame::Array(ref arr) => {
                assert!(!arr.is_empty());
                // moon#698: Redis's help header verbatim, as a SIMPLE string.
                // Pinned exactly rather than by prefix — the old assertion
                // passed on a bulk-string reply with no header line at all,
                // which is precisely the divergence #698 fixed.
                assert_eq!(
                    &arr[0],
                    &Frame::SimpleString(Bytes::from_static(
                        b"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"
                    ))
                );
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    #[test]
    fn test_object_help_readonly_track() {
        let db = Database::new();
        let result = object_readonly(&db, &[bs(b"HELP")], 0);
        assert!(matches!(result, Frame::Array(_)));
    }

    #[test]
    fn test_object_unknown_subcommand_errors() {
        let mut db = Database::new();
        let result = object(&mut db, &[bs(b"BOGUS")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- Cold-plane enumeration tests (issue #364) ---
    //
    // Under disk-offload, keys spilled by eviction live ONLY in
    // `Database::cold_index` (no in-RAM Entry). SCAN/KEYS/RANDOMKEY must
    // enumerate them from the in-RAM index alone — no disk I/O.

    mod cold_enumeration {
        use super::*;
        use crate::storage::tiered::cold_index::{ColdIndex, ColdLocation};

        /// Db with `hot` resident string keys and `cold` cold-only keys.
        fn db_with_planes(hot: &[&[u8]], cold: &[(&[u8], Option<u64>)]) -> Database {
            let mut db = Database::new();
            for k in hot {
                db.set(k, Entry::new_string(Bytes::from_static(b"v")));
            }
            let mut ci = ColdIndex::new();
            for (i, (k, ttl_ms)) in cold.iter().enumerate() {
                ci.insert(
                    Bytes::copy_from_slice(k),
                    ColdLocation {
                        file_id: 1,
                        page_idx: 0,
                        slot_idx: i as u16,
                        ttl_ms: *ttl_ms,
                        value_type: crate::persistence::kv_page::ValueType::String,
                    },
                );
            }
            db.cold_index = Some(ci);
            db
        }

        /// Drive SCAN (mutable track) to completion, collecting every key.
        fn full_scan(db: &mut Database, extra: &[&[u8]]) -> Vec<Bytes> {
            let mut cursor = Bytes::from_static(b"0");
            let mut keys = Vec::new();
            loop {
                let mut args = vec![Frame::BulkString(cursor.clone())];
                for e in extra {
                    args.push(bs(e));
                }
                let Frame::Array(parts) = scan(db, &args) else {
                    panic!("SCAN did not return an array");
                };
                let Frame::BulkString(next) = &parts[0] else {
                    panic!("SCAN cursor not a bulk string");
                };
                let Frame::Array(batch) = &parts[1] else {
                    panic!("SCAN batch not an array");
                };
                for f in batch.iter() {
                    if let Frame::BulkString(k) = f {
                        keys.push(k.clone());
                    }
                }
                if next.as_ref() == b"0" {
                    return keys;
                }
                cursor = next.clone();
            }
        }

        fn full_scan_readonly(db: &Database, now_ms: u64, extra: &[&[u8]]) -> Vec<Bytes> {
            let mut cursor = Bytes::from_static(b"0");
            let mut keys = Vec::new();
            loop {
                let mut args = vec![Frame::BulkString(cursor.clone())];
                for e in extra {
                    args.push(bs(e));
                }
                let Frame::Array(parts) = scan_readonly(db, &args, now_ms) else {
                    panic!("SCAN did not return an array");
                };
                let Frame::BulkString(next) = &parts[0] else {
                    panic!("SCAN cursor not a bulk string");
                };
                let Frame::Array(batch) = &parts[1] else {
                    panic!("SCAN batch not an array");
                };
                for f in batch.iter() {
                    if let Frame::BulkString(k) = f {
                        keys.push(k.clone());
                    }
                }
                if next.as_ref() == b"0" {
                    return keys;
                }
                cursor = next.clone();
            }
        }

        #[test]
        fn scan_paged_drain_across_planes_exact_once() {
            // 40 hot + 40 cold-only + 10 both-planes keys drained at
            // COUNT 7: the ranged cold walk (#368 cold-plane resume) must
            // merge with the hot page walk so every LOGICAL key appears
            // exactly once across pages.
            let hot_names: Vec<Vec<u8>> = (0..50).map(|i| format!("h:{i}").into_bytes()).collect();
            let cold_names: Vec<Vec<u8>> = (0..40).map(|i| format!("c:{i}").into_bytes()).collect();
            let hot_refs: Vec<&[u8]> = hot_names.iter().map(|v| v.as_slice()).collect();
            let mut cold_refs: Vec<(&[u8], Option<u64>)> =
                cold_names.iter().map(|v| (v.as_slice(), None)).collect();
            // 10 both-planes keys: hot shadow over a stale cold entry.
            for v in hot_names.iter().take(10) {
                cold_refs.push((v.as_slice(), None));
            }
            let mut db = db_with_planes(&hot_refs, &cold_refs);
            let keys = full_scan(&mut db, &[b"COUNT", b"7"]);
            let unique: std::collections::HashSet<&Bytes> = keys.iter().collect();
            assert_eq!(unique.len(), keys.len(), "no key may be returned twice");
            assert_eq!(keys.len(), 90, "every logical key exactly once");
        }

        #[test]
        fn scan_cold_churn_between_pages_keeps_stable_keys() {
            // New spills landing mid-scan must not displace pre-existing
            // cold keys (hash-space cursor: churn can't shift positions).
            let cold_names: Vec<Vec<u8>> =
                (0..30).map(|i| format!("st:{i}").into_bytes()).collect();
            let cold_refs: Vec<(&[u8], Option<u64>)> =
                cold_names.iter().map(|v| (v.as_slice(), None)).collect();
            let mut db = db_with_planes(&[], &cold_refs);
            let mut cursor = Bytes::from_static(b"0");
            let mut seen: Vec<Bytes> = Vec::new();
            let mut churn = 0u32;
            loop {
                let args = [Frame::BulkString(cursor.clone()), bs(b"COUNT"), bs(b"5")];
                let Frame::Array(parts) = scan(&mut db, &args) else {
                    panic!("SCAN did not return an array");
                };
                let (Frame::BulkString(next), Frame::Array(batch)) = (&parts[0], &parts[1]) else {
                    panic!("malformed SCAN reply");
                };
                for f in batch.iter() {
                    if let Frame::BulkString(k) = f {
                        seen.push(k.clone());
                    }
                }
                if next.as_ref() == b"0" {
                    break;
                }
                cursor = next.clone();
                // Churn: spill 5 NEW cold keys between every page.
                #[allow(clippy::unwrap_used)] // test-only: planes fabricated above
                let ci = db.cold_index.as_mut().unwrap();
                for _ in 0..5 {
                    ci.insert(
                        Bytes::from(format!("churn:{churn}")),
                        ColdLocation {
                            file_id: 2,
                            page_idx: 0,
                            slot_idx: 0,
                            ttl_ms: None,
                            value_type: crate::persistence::kv_page::ValueType::String,
                        },
                    );
                    churn += 1;
                }
            }
            for name in &cold_names {
                assert!(
                    seen.iter().any(|k| k.as_ref() == name.as_slice()),
                    "stable cold key {} lost under mid-scan spill churn",
                    String::from_utf8_lossy(name)
                );
            }
            let unique: std::collections::HashSet<&Bytes> = seen.iter().collect();
            assert_eq!(unique.len(), seen.len(), "no key may be returned twice");
        }

        #[test]
        fn scan_includes_cold_only_keys() {
            let mut db = db_with_planes(&[b"hot1", b"hot2"], &[(b"cold1", None), (b"cold2", None)]);
            let mut keys = full_scan(&mut db, &[b"COUNT", b"100"]);
            keys.sort();
            assert_eq!(keys, vec!["cold1", "cold2", "hot1", "hot2"]);
        }

        #[test]
        fn scan_readonly_includes_cold_only_keys() {
            let db = db_with_planes(&[b"hot1"], &[(b"cold1", None)]);
            let now_ms = current_time_ms();
            let mut keys = full_scan_readonly(&db, now_ms, &[b"COUNT", b"100"]);
            keys.sort();
            assert_eq!(keys, vec!["cold1", "hot1"]);
        }

        #[test]
        fn scan_returns_both_planes_key_exactly_once() {
            // A key present in BOTH planes (hot shadow over a stale cold
            // entry, e.g. after AOF-replay) must be returned exactly once.
            let mut db = db_with_planes(&[b"both"], &[(b"both", None), (b"coldonly", None)]);
            let mut keys = full_scan(&mut db, &[b"COUNT", b"100"]);
            keys.sort();
            assert_eq!(keys, vec!["both", "coldonly"]);
        }

        #[test]
        fn scan_skips_ttl_expired_cold_keys() {
            let now_ms = current_time_ms();
            let mut db = db_with_planes(
                &[],
                &[
                    (b"alive", Some(now_ms + 60_000)),
                    (b"dead", Some(now_ms - 60_000)),
                ],
            );
            let keys = full_scan(&mut db, &[b"COUNT", b"100"]);
            assert_eq!(keys, vec!["alive"]);
        }

        #[test]
        fn scan_match_filter_applies_to_cold_keys() {
            let mut db = db_with_planes(&[b"user:1"], &[(b"user:2", None), (b"other", None)]);
            let mut keys = full_scan(&mut db, &[b"MATCH", b"user:*", b"COUNT", b"100"]);
            keys.sort();
            assert_eq!(keys, vec!["user:1", "user:2"]);
        }

        #[test]
        fn scan_small_count_pages_through_cold_keys() {
            let mut db = db_with_planes(
                &[b"h1", b"h2", b"h3"],
                &[(b"c1", None), (b"c2", None), (b"c3", None)],
            );
            // COUNT 1 forces one key per page — exercises cursor continuity
            // across the hot/cold boundary.
            let mut keys = full_scan(&mut db, &[b"COUNT", b"1"]);
            keys.sort();
            assert_eq!(keys, vec!["c1", "c2", "c3", "h1", "h2", "h3"]);
        }

        #[test]
        fn keys_includes_cold_only_keys() {
            let mut db = db_with_planes(&[b"hot1"], &[(b"cold1", None), (b"both", None)]);
            db.set(b"both", Entry::new_string(Bytes::from_static(b"v")));
            let Frame::Array(arr) = keys(&mut db, &[bs(b"*")]) else {
                panic!("KEYS did not return an array");
            };
            let mut got: Vec<Bytes> = arr
                .iter()
                .filter_map(|f| match f {
                    Frame::BulkString(b) => Some(b.clone()),
                    _ => None,
                })
                .collect();
            got.sort();
            assert_eq!(got, vec!["both", "cold1", "hot1"]);
        }

        #[test]
        fn keys_readonly_includes_cold_only_keys() {
            let db = db_with_planes(&[b"hot1"], &[(b"cold1", None)]);
            let now_ms = current_time_ms();
            let Frame::Array(arr) = keys_readonly(&db, &[bs(b"*")], now_ms) else {
                panic!("KEYS did not return an array");
            };
            let mut got: Vec<Bytes> = arr
                .iter()
                .filter_map(|f| match f {
                    Frame::BulkString(b) => Some(b.clone()),
                    _ => None,
                })
                .collect();
            got.sort();
            assert_eq!(got, vec!["cold1", "hot1"]);
        }

        #[test]
        fn scan_type_filter_judges_cold_keys_from_index() {
            use crate::persistence::kv_page::ValueType;
            // One cold hash + one cold string + one hot string. TYPE hash
            // must surface ONLY the cold hash — judged from the in-RAM
            // ColdLocation::value_type cache, no disk read available here
            // (no spill file exists behind these locations).
            let mut db = db_with_planes(&[b"hotstr"], &[]);
            let mut ci = ColdIndex::new();
            ci.insert(
                Bytes::from_static(b"coldhash"),
                ColdLocation {
                    file_id: 1,
                    page_idx: 0,
                    slot_idx: 0,
                    ttl_ms: None,
                    value_type: ValueType::Hash,
                },
            );
            ci.insert(
                Bytes::from_static(b"coldstr"),
                ColdLocation {
                    file_id: 1,
                    page_idx: 0,
                    slot_idx: 1,
                    ttl_ms: None,
                    value_type: ValueType::String,
                },
            );
            db.cold_index = Some(ci);

            let hashes = full_scan(&mut db, &[b"TYPE", b"hash", b"COUNT", b"100"]);
            assert_eq!(hashes, vec!["coldhash"]);

            let mut strings = full_scan(&mut db, &[b"TYPE", b"string", b"COUNT", b"100"]);
            strings.sort();
            assert_eq!(strings, vec!["coldstr", "hotstr"]);

            // Read-only twin must agree.
            let now_ms = current_time_ms();
            let ro_hashes = full_scan_readonly(&db, now_ms, &[b"TYPE", b"hash", b"COUNT", b"100"]);
            assert_eq!(ro_hashes, vec!["coldhash"]);
        }

        #[test]
        fn randomkey_sees_all_cold_database() {
            // Every key spilled: RANDOMKEY must not report an empty db.
            let mut db = db_with_planes(&[], &[(b"cold1", None), (b"cold2", None)]);
            match randomkey(&mut db, &[]) {
                Frame::BulkString(k) => {
                    assert!(k.as_ref() == b"cold1" || k.as_ref() == b"cold2");
                }
                other => panic!("expected a key, got {other:?}"),
            }
        }

        #[test]
        fn randomkey_readonly_sees_all_cold_database() {
            let db = db_with_planes(&[], &[(b"cold1", None)]);
            match randomkey_readonly(&db, &[], current_time_ms()) {
                Frame::BulkString(k) => assert_eq!(k.as_ref(), b"cold1"),
                other => panic!("expected a key, got {other:?}"),
            }
        }

        #[test]
        fn del_of_ttl_expired_cold_key_answers_zero_but_reclaims() {
            // Redis parity: DEL of a logically-expired key deletes nothing
            // (returns 0) — but the stale cold-index entry must still be
            // reclaimed as a side effect, not left behind.
            let now_ms = current_time_ms();
            let mut db = db_with_planes(&[], &[(b"dead", Some(now_ms - 60_000))]);
            assert_eq!(del(&mut db, &[bs(b"dead")]), Frame::Integer(0));
            assert!(
                db.cold_index
                    .as_ref()
                    .is_some_and(|ci| ci.lookup(b"dead").is_none()),
                "stale cold entry must be reclaimed by the DEL attempt"
            );
        }

        #[test]
        fn unlink_of_alive_cold_key_still_counts() {
            let mut db = db_with_planes(&[], &[(b"alive", None)]);
            assert_eq!(unlink(&mut db, &[bs(b"alive")]), Frame::Integer(1));
        }
    }

    // --- moon#544: EXPIRE family NX | XX | GT | LT conditions (Redis 7.0) ---
    mod expire_conditions {
        use super::*;

        fn ttl_of(db: &mut Database, key: &[u8]) -> Frame {
            pttl(db, &[bs(key)])
        }

        fn assert_ttl_roughly(db: &mut Database, key: &[u8], expect_ms: u64) {
            let Frame::Integer(ms) = ttl_of(db, key) else {
                panic!("PTTL must return an integer");
            };
            let expect = expect_ms as i64;
            assert!(
                (expect - 2_000..=expect).contains(&ms),
                "PTTL {ms} not within 2s below {expect}"
            );
        }

        /// NX sets only when the key has no TTL.
        #[test]
        fn nx_sets_on_no_ttl_and_refuses_on_existing_ttl() {
            let mut db = setup_db_with_key(b"k", b"v");
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"NX")]),
                Frame::Integer(1)
            );
            assert_ttl_roughly(&mut db, b"k", 100_000);
            // Second NX must refuse and leave the TTL untouched.
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"999"), bs(b"nx")]),
                Frame::Integer(0)
            );
            assert_ttl_roughly(&mut db, b"k", 100_000);
        }

        /// XX sets only when the key already has a TTL.
        #[test]
        fn xx_refuses_on_no_ttl_and_sets_on_existing_ttl() {
            let mut db = setup_db_with_key(b"k", b"v");
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"XX")]),
                Frame::Integer(0)
            );
            assert_eq!(ttl_of(&mut db, b"k"), Frame::Integer(-1));
            db.set_expiry(b"k", current_time_ms() + 50_000);
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"XX")]),
                Frame::Integer(1)
            );
            assert_ttl_roughly(&mut db, b"k", 100_000);
        }

        /// GT sets only a LATER expiry; a key with no TTL counts as infinite,
        /// so GT never sets it.
        #[test]
        fn gt_only_extends_and_treats_no_ttl_as_infinite() {
            let mut db = setup_db_with_key(b"k", b"v");
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"GT")]),
                Frame::Integer(0)
            );
            assert_eq!(ttl_of(&mut db, b"k"), Frame::Integer(-1));
            db.set_expiry(b"k", current_time_ms() + 50_000);
            // Shorter → refused.
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"10"), bs(b"GT")]),
                Frame::Integer(0)
            );
            assert_ttl_roughly(&mut db, b"k", 50_000);
            // Longer → set.
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"GT")]),
                Frame::Integer(1)
            );
            assert_ttl_roughly(&mut db, b"k", 100_000);
        }

        /// LT sets only an EARLIER expiry; a key with no TTL counts as
        /// infinite, so LT always sets it.
        #[test]
        fn lt_only_shortens_and_always_sets_on_no_ttl() {
            let mut db = setup_db_with_key(b"k", b"v");
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"LT")]),
                Frame::Integer(1)
            );
            assert_ttl_roughly(&mut db, b"k", 100_000);
            // Longer → refused.
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"999"), bs(b"LT")]),
                Frame::Integer(0)
            );
            assert_ttl_roughly(&mut db, b"k", 100_000);
            // Shorter → set.
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"10"), bs(b"LT")]),
                Frame::Integer(1)
            );
            assert_ttl_roughly(&mut db, b"k", 10_000);
        }

        /// LT with a past-time expiry on a longer-TTL key deletes it (the
        /// past time IS earlier); GT with a past time never deletes.
        #[test]
        fn past_time_respects_the_condition() {
            let mut db = setup_db_with_key(b"k", b"v");
            db.set_expiry(b"k", current_time_ms() + 50_000);
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"-1"), bs(b"GT")]),
                Frame::Integer(0)
            );
            assert!(db.exists(b"k"), "GT must not delete via a past time");
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"-1"), bs(b"LT")]),
                Frame::Integer(1)
            );
            assert!(!db.exists(b"k"), "LT past-time must delete");
        }

        /// NX with XX/GT/LT is a syntax error; GT with LT is a syntax error.
        #[test]
        fn incompatible_flag_combinations_error() {
            let mut db = setup_db_with_key(b"k", b"v");
            for combo in [[b"NX" as &[u8], b"XX"], [b"NX", b"GT"], [b"NX", b"LT"]] {
                let r = expire(&mut db, &[bs(b"k"), bs(b"100"), bs(combo[0]), bs(combo[1])]);
                let Frame::Error(e) = r else {
                    panic!("NX+{:?} must error", String::from_utf8_lossy(combo[1]));
                };
                assert!(
                    e.starts_with(b"ERR NX and XX"),
                    "wrong error: {}",
                    String::from_utf8_lossy(&e)
                );
            }
            let r = expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"GT"), bs(b"LT")]);
            let Frame::Error(e) = r else {
                panic!("GT+LT must error");
            };
            assert!(
                e.starts_with(b"ERR GT and LT"),
                "wrong error: {}",
                String::from_utf8_lossy(&e)
            );
            // XX GT is legal (Redis accepts it).
            db.set_expiry(b"k", current_time_ms() + 50_000);
            assert_eq!(
                expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"XX"), bs(b"GT")]),
                Frame::Integer(1)
            );
        }

        /// TTL rounds to the NEAREST second like Redis: a fresh 100s expiry
        /// answers 100 (the old floor answered 99 for 99.999s remaining).
        #[test]
        fn ttl_rounds_to_nearest_second() {
            let mut db = setup_db_with_key(b"k", b"v");
            assert_eq!(expire(&mut db, &[bs(b"k"), bs(b"100")]), Frame::Integer(1));
            assert_eq!(ttl(&mut db, &[bs(b"k")]), Frame::Integer(100));
        }

        /// An unknown option errors with Redis's message shape.
        #[test]
        fn unknown_option_errors() {
            let mut db = setup_db_with_key(b"k", b"v");
            let r = expire(&mut db, &[bs(b"k"), bs(b"100"), bs(b"BOGUS")]);
            let Frame::Error(e) = r else {
                panic!("unknown option must error");
            };
            assert!(
                e.starts_with(b"ERR Unsupported option"),
                "wrong error: {}",
                String::from_utf8_lossy(&e)
            );
        }

        /// A missing key answers 0 under every condition, never an error.
        #[test]
        fn missing_key_answers_zero_under_conditions() {
            let mut db = Database::new();
            for flag in [b"NX" as &[u8], b"XX", b"GT", b"LT"] {
                assert_eq!(
                    expire(&mut db, &[bs(b"nokey"), bs(b"100"), bs(flag)]),
                    Frame::Integer(0),
                    "flag {}",
                    String::from_utf8_lossy(flag)
                );
            }
        }

        /// All four commands of the family accept the options.
        #[test]
        fn whole_family_accepts_conditions() {
            let mut db = setup_db_with_key(b"k", b"v");
            let far_s = (current_time_ms() / 1000 + 100).to_string();
            // Strictly EARLIER than the ~+100s TTL the expireat leg sets, so
            // the LT leg genuinely shortens (LT refuses >= — see lt test).
            let far_ms = (current_time_ms() + 50_000).to_string();
            assert_eq!(
                pexpire(&mut db, &[bs(b"k"), bs(b"100000"), bs(b"NX")]),
                Frame::Integer(1)
            );
            assert_eq!(
                expireat(&mut db, &[bs(b"k"), bs(far_s.as_bytes()), bs(b"XX")]),
                Frame::Integer(1)
            );
            assert_eq!(
                pexpireat(
                    &mut db,
                    &[bs(b"k"), bs(far_ms.as_bytes()), bs(b"XX"), bs(b"LT")]
                ),
                Frame::Integer(1)
            );
        }
    }
}
