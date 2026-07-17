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

/// EXPIRE key seconds
///
/// Set a timeout on key. Returns 1 if the timeout was set (or the key was
/// deleted because of a non-positive/past TTL), 0 if the key does not exist.
/// A non-positive TTL deletes the key immediately (Redis past-time semantics).
pub fn expire(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
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
    if seconds <= 0 {
        return if db.remove(key).is_some() {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
    }
    // Guard the u64 arithmetic (seconds*1000 + now_ms can overflow) AND bound the
    // result to the i64 domain so PTTL — which casts the stored u64 back to i64 —
    // never wraps negative on a live key. Redis rejects an out-of-range expiry.
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
    if args.len() != 2 {
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
    // Redis parity: a non-positive TTL is a past-time expiry -> delete the key now.
    if millis <= 0 {
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
    let base_ts = db.base_timestamp();
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now_ms = current_time_ms();
                let exp_ms = entry.expires_at_ms(base_ts);
                if now_ms >= exp_ms {
                    // Edge case: expired between get and now
                    Frame::Integer(-2)
                } else {
                    Frame::Integer(((exp_ms - now_ms) / 1000) as i64)
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
    let base_ts = db.base_timestamp();
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now_ms = current_time_ms();
                let exp_ms = entry.expires_at_ms(base_ts);
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
    if args.len() != 2 {
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
    // Redis accepts 0 and negative timestamps as past-time expiry (deletes key immediately)
    if timestamp <= 0 {
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
    if args.len() != 2 {
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
    // Redis accepts 0 and negative timestamps as past-time expiry (deletes key immediately)
    if timestamp_ms <= 0 {
        return if db.remove(key).is_some() {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
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
    let base_ts = db.base_timestamp();
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer((entry.expires_at_ms(base_ts) / 1000) as i64)
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
    let base_ts = db.base_timestamp();
    match db.get(key) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer(entry.expires_at_ms(base_ts) as i64)
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
                // Wraparound-safe delta in seconds (16-bit)
                let idle = (now.wrapping_sub(last)) & 0xFFFF;
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
        match db.get_if_alive(key, now_ms) {
            Some(entry) => {
                let encoding = entry.value.as_redis_value().encoding_name();
                Frame::BulkString(Bytes::from(encoding))
            }
            None => Frame::Null,
        }
    } else if subcommand.eq_ignore_ascii_case(b"FREQ") {
        match db.get_if_alive(key, now_ms) {
            Some(entry) => Frame::Integer(entry.access_counter() as i64),
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else if subcommand.eq_ignore_ascii_case(b"IDLETIME") {
        let now = db.now();
        match db.get_if_alive(key, now_ms) {
            Some(entry) => {
                let last = entry.last_access();
                // Wraparound-safe delta in seconds (16-bit)
                let idle = (now.wrapping_sub(last)) & 0xFFFF;
                Frame::Integer(idle as i64)
            }
            None => Frame::Error(Bytes::from_static(b"ERR no such key")),
        }
    } else if subcommand.eq_ignore_ascii_case(b"REFCOUNT") {
        match db.get_if_alive(key, now_ms) {
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
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"OBJECT ENCODING <key>")),
        Frame::BulkString(Bytes::from_static(
            b"  Return the encoding of the object stored at <key>."
        )),
        Frame::BulkString(Bytes::from_static(b"OBJECT FREQ <key>")),
        Frame::BulkString(Bytes::from_static(
            b"  Return the access frequency of the object at <key>."
        )),
        Frame::BulkString(Bytes::from_static(b"OBJECT IDLETIME <key>")),
        Frame::BulkString(Bytes::from_static(
            b"  Return the idle time in seconds of the object at <key>."
        )),
        Frame::BulkString(Bytes::from_static(b"OBJECT REFCOUNT <key>")),
        Frame::BulkString(Bytes::from_static(
            b"  Return the reference count of the object at <key>."
        )),
        Frame::BulkString(Bytes::from_static(b"OBJECT HELP")),
        Frame::BulkString(Bytes::from_static(b"  Return subcommand help.")),
    ])
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
    db.set(Bytes::copy_from_slice(dst), entry);

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
    db.set(Bytes::copy_from_slice(dst), entry);

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

    // Parse cursor
    let cursor_str = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SCAN"),
    };
    let cursor: usize = match std::str::from_utf8(cursor_str)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
    };

    // Parse optional arguments
    let mut match_pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut type_filter: Option<&[u8]> = None;

    let mut i = 1;
    while i < args.len() {
        let opt = match extract_key(&args[i]) {
            Some(o) => o,
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            i += 1;
            if i < args.len() {
                match_pattern = extract_key(&args[i]);
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i < args.len() {
                if let Some(c) = parse_int(&args[i]) {
                    if c > 0 {
                        count = c as usize;
                    }
                }
            }
        } else if opt.eq_ignore_ascii_case(b"TYPE") {
            i += 1;
            if i < args.len() {
                type_filter = extract_key(&args[i]);
            }
        }
        i += 1;
    }

    // Collect all non-expired keys sorted for deterministic iteration.
    // Two planes, partitioned with no overlap (#364): hot = live in-RAM
    // entries (exists() keeps its lazy-expiry reclamation side effect);
    // cold = spilled keys with no live hot shadow (`cold_only_keys`, pure
    // in-RAM index probe — no disk I/O). The `is_cold` tag lets the TYPE
    // filter below avoid a promoting/disk-reading lookup on cold keys.
    let now_ms = db.now_ms();
    let all_keys: Vec<CompactKey> = db.keys().cloned().collect();
    let mut sorted_keys: Vec<(CompactKey, bool)> = Vec::new();
    for key in all_keys {
        let _ = db.exists(key.as_bytes());
        if db.get_if_alive(key.as_bytes(), now_ms).is_some() {
            sorted_keys.push((key, false));
        }
    }
    let cold_keys: Vec<CompactKey> = db
        .cold_only_keys(now_ms)
        .map(|k| CompactKey::from(k.as_ref()))
        .collect();
    sorted_keys.extend(cold_keys.into_iter().map(|k| (k, true)));
    sorted_keys.sort();

    let total = sorted_keys.len();
    let mut results = Vec::new();
    let mut pos = cursor;

    // Iterate from cursor position, collect up to `count` matching keys
    let mut checked = 0;
    while pos < total && checked < count {
        let (key, is_cold) = &sorted_keys[pos];
        pos += 1;
        checked += 1;

        // TYPE filter
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

        // MATCH filter
        if let Some(pattern) = match_pattern {
            if !glob_match(pattern, key.as_bytes()) {
                continue;
            }
        }

        results.push(Frame::BulkString(key.to_bytes()));
    }

    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        Bytes::from(pos.to_string())
    };

    Frame::Array(framevec![
        Frame::BulkString(next_cursor),
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
    let base_ts = db.base_timestamp();
    match db.get_if_alive(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now = current_time_ms();
                let exp_ms = entry.expires_at_ms(base_ts);
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
    let base_ts = db.base_timestamp();
    match db.get_if_alive(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                let now = current_time_ms();
                let exp_ms = entry.expires_at_ms(base_ts);
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
    match db.get_if_alive(key, now_ms) {
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
    let cursor: usize = match std::str::from_utf8(cursor_str)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
    };

    let mut match_pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut type_filter: Option<&[u8]> = None;

    let mut i = 1;
    while i < args.len() {
        let opt = match extract_key(&args[i]) {
            Some(o) => o,
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            i += 1;
            if i < args.len() {
                match_pattern = extract_key(&args[i]);
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i < args.len() {
                if let Some(c) = parse_int(&args[i]) {
                    if c > 0 {
                        count = c as usize;
                    }
                }
            }
        } else if opt.eq_ignore_ascii_case(b"TYPE") {
            i += 1;
            if i < args.len() {
                type_filter = extract_key(&args[i]);
            }
        }
        i += 1;
    }

    // Collect all non-expired keys sorted for deterministic iteration.
    // Hot plane (live in-RAM entries) unioned with cold-only spilled keys —
    // the two are partitioned by `cold_only_keys`, so no dedup pass (#364).
    let mut sorted_keys: Vec<(CompactKey, bool)> = db
        .keys()
        .filter(|k| db.get_if_alive(k.as_bytes(), now_ms).is_some())
        .cloned()
        .map(|k| (k, false))
        .collect();
    sorted_keys.extend(
        db.cold_only_keys(now_ms)
            .map(|k| (CompactKey::from(k.as_ref()), true)),
    );
    sorted_keys.sort();

    let total = sorted_keys.len();
    let mut results = Vec::new();
    let mut pos = cursor;
    let mut checked = 0;

    while pos < total && checked < count {
        let (key, is_cold) = &sorted_keys[pos];
        pos += 1;
        checked += 1;

        // TYPE filter
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

        // MATCH filter
        if let Some(pattern) = match_pattern {
            if !glob_match(pattern, key.as_bytes()) {
                continue;
            }
        }

        results.push(Frame::BulkString(key.to_bytes()));
    }

    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        Bytes::from(pos.to_string())
    };

    Frame::Array(framevec![
        Frame::BulkString(next_cursor),
        Frame::Array(results.into()),
    ])
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
    let base_ts = db.base_timestamp();
    match db.get_if_alive(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer((entry.expires_at_ms(base_ts) / 1000) as i64)
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
    let base_ts = db.base_timestamp();
    match db.get_if_alive(key, now_ms) {
        None => Frame::Integer(-2),
        Some(entry) => {
            if !entry.has_expiry() {
                Frame::Integer(-1)
            } else {
                Frame::Integer(entry.expires_at_ms(base_ts) as i64)
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
        db.set(
            Bytes::copy_from_slice(key),
            Entry::new_string(Bytes::copy_from_slice(val)),
        );
        db
    }

    fn setup_db_with_expiry(key: &[u8], val: &[u8], expires_at_ms: u64) -> Database {
        let mut db = Database::new();
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::copy_from_slice(key),
            Entry::new_string_with_expiry(Bytes::copy_from_slice(val), expires_at_ms, base_ts),
        );
        db
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
        db.set(
            Bytes::from_static(b"hot2"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
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
        db.set(
            Bytes::from_static(b"shadowed"),
            Entry::new_string(Bytes::from_static(b"new")),
        );
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
        db.set(
            Bytes::from_static(b"a"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        db.set(
            Bytes::from_static(b"b"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
        db.set(
            Bytes::from_static(b"c"),
            Entry::new_string(Bytes::from_static(b"3")),
        );
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
        db.set(
            Bytes::from_static(b"foo"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        db.set(
            Bytes::from_static(b"bar"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
        db.set(
            Bytes::from_static(b"baz"),
            Entry::new_string(Bytes::from_static(b"3")),
        );
        let result = keys(&mut db, &[bs(b"*")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_keys_pattern() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"hello"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        db.set(
            Bytes::from_static(b"hallo"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
        db.set(
            Bytes::from_static(b"world"),
            Entry::new_string(Bytes::from_static(b"3")),
        );
        let result = keys(&mut db, &[bs(b"h?llo")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_keys_expired_excluded() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"alive"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        let past_ms = current_time_ms() - 1000;
        let base_ts = db.base_timestamp();
        db.set(
            Bytes::from_static(b"dead"),
            Entry::new_string_with_expiry(Bytes::from_static(b"2"), past_ms, base_ts),
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
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"srcval")),
        );
        db.set(
            Bytes::from_static(b"dst"),
            Entry::new_string(Bytes::from_static(b"dstval")),
        );
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
        db.set(
            Bytes::from_static(b"src"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        db.set(
            Bytes::from_static(b"dst"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
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
        db.set(
            Bytes::from_static(b"a"),
            Entry::new_string(Bytes::from_static(b"1")),
        );
        db.set(
            Bytes::from_static(b"b"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
        db.set(
            Bytes::from_static(b"c"),
            Entry::new_string(Bytes::from_static(b"3")),
        );
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
        db.set(Bytes::from_static(b"h"), Entry::new_hash());
        let result = type_cmd(&mut db, &[bs(b"h")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"hash")));
    }

    #[test]
    fn test_type_list() {
        let mut db = Database::new();
        db.set(Bytes::from_static(b"l"), Entry::new_list());
        let result = type_cmd(&mut db, &[bs(b"l")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"list")));
    }

    #[test]
    fn test_type_set() {
        let mut db = Database::new();
        db.set(Bytes::from_static(b"s"), Entry::new_set());
        let result = type_cmd(&mut db, &[bs(b"s")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"set")));
    }

    #[test]
    fn test_type_zset() {
        let mut db = Database::new();
        db.set(Bytes::from_static(b"z"), Entry::new_sorted_set());
        let result = type_cmd(&mut db, &[bs(b"z")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"zset")));
    }

    // --- SCAN tests ---

    #[test]
    fn test_scan_basic() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            Entry::new_string(Bytes::from_static(b"v1")),
        );
        db.set(
            Bytes::from_static(b"key2"),
            Entry::new_string(Bytes::from_static(b"v2")),
        );
        db.set(
            Bytes::from_static(b"key3"),
            Entry::new_string(Bytes::from_static(b"v3")),
        );

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
                Bytes::from(format!("key{:02}", i)),
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
                    Frame::Array(keys) => assert_eq!(keys.len(), 5),
                    _ => panic!("Expected keys array"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_scan_with_match() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"user:1"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        db.set(
            Bytes::from_static(b"user:2"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        db.set(
            Bytes::from_static(b"post:1"),
            Entry::new_string(Bytes::from_static(b"v")),
        );

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
        db.set(
            Bytes::from_static(b"str"),
            Entry::new_string(Bytes::from_static(b"v")),
        );
        db.set(Bytes::from_static(b"hash"), Entry::new_hash());
        db.set(Bytes::from_static(b"list"), Entry::new_list());

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

    #[test]
    fn test_touch() {
        let mut db = setup_db_with_key(b"a", b"1");
        db.set(
            Bytes::from_static(b"b"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
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
        db.set(
            Bytes::from_static(b"b"),
            Entry::new_string(Bytes::from_static(b"2")),
        );
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
                // First line names the command family, matching Redis's
                // "<CMD> <subcommand> ..." HELP convention.
                assert!(matches!(&arr[0], Frame::BulkString(b) if b.starts_with(b"OBJECT")));
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
                db.set(
                    Bytes::copy_from_slice(k),
                    Entry::new_string(Bytes::from_static(b"v")),
                );
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
            db.set(
                Bytes::from_static(b"both"),
                Entry::new_string(Bytes::from_static(b"v")),
            );
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
}
