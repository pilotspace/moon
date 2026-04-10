use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::compact_key::CompactKey;
use crate::storage::entry::current_time_ms;

use super::helpers::err_wrong_args;

/// Extract a key as &[u8] from a Frame argument.
fn extract_key(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(s) | Frame::SimpleString(s) => Some(s.as_ref()),
        _ => None,
    }
}

/// Parse an integer argument from a Frame.
fn parse_int(frame: &Frame) -> Option<i64> {
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
            if db.remove(key).is_some() {
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
/// Set a timeout on key. Returns 1 if timeout was set, 0 if key does not exist.
/// Negative or zero seconds returns an error (modern Redis 7+ behavior).
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
    if seconds <= 0 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'EXPIRE' command",
        ));
    }
    let expires_at_ms = current_time_ms() + (seconds as u64) * 1000;
    if db.set_expiry(key, expires_at_ms) {
        Frame::Integer(1)
    } else {
        Frame::Integer(0)
    }
}

/// PEXPIRE key milliseconds
///
/// Like EXPIRE but the timeout is specified in milliseconds.
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
    if millis <= 0 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'PEXPIRE' command",
        ));
    }
    let expires_at_ms = current_time_ms() + millis as u64;
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
    } else if subcommand.eq_ignore_ascii_case(b"HELP") {
        Frame::Array(framevec![
            Frame::BulkString(Bytes::from_static(b"OBJECT ENCODING <key>")),
            Frame::BulkString(Bytes::from_static(
                b"  Return the encoding of the object stored at <key>."
            )),
            Frame::BulkString(Bytes::from_static(b"OBJECT HELP")),
            Frame::BulkString(Bytes::from_static(b"  Return subcommand help.")),
        ])
    } else {
        Frame::Error(Bytes::from_static(b"ERR unknown OBJECT subcommand"))
    }
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
/// Returns the number of keys in the currently selected database.
pub fn dbsize(db: &mut Database, _args: &[Frame]) -> Frame {
    Frame::Integer(db.len() as i64)
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

    let mut result = Vec::new();
    for key in all_keys {
        // Trigger lazy expiry by calling exists
        if db.exists(key.as_bytes()) && glob_match(pattern, key.as_bytes()) {
            result.push(Frame::BulkString(key.to_bytes()));
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

/// COPY source destination [DB destination-db] [REPLACE]
///
/// Copies the value stored at the source key to the destination key.
/// Returns 1 if source was copied, 0 if destination already exists without REPLACE.
pub fn copy(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("COPY");
    }
    let src = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("COPY"),
    };
    let dst = match extract_key(&args[1]) {
        Some(k) => k,
        None => return err_wrong_args("COPY"),
    };

    // Parse optional arguments: DB destination-db, REPLACE
    let mut replace = false;
    let mut i = 2;
    while i < args.len() {
        let arg = match extract_key(&args[i]) {
            Some(k) => k,
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        if arg.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            i += 1;
        } else if arg.eq_ignore_ascii_case(b"DB") {
            // Cross-DB copy requires shard_databases context not available here
            return Frame::Error(Bytes::from_static(
                b"ERR COPY with DB option is not supported yet",
            ));
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    // Check if source exists (with lazy expiry)
    if !db.exists(src) {
        return Frame::Error(Bytes::from_static(b"ERR no such key"));
    }

    // Same key: no data to copy, but it's valid
    if src == dst {
        return Frame::Integer(1);
    }

    // Check if destination exists
    if db.exists(dst) && !replace {
        return Frame::Integer(0);
    }

    // Clone the source entry (CompactEntry derives Clone)
    let entry = db.get(src).cloned();
    if let Some(cloned) = entry {
        db.set(Bytes::copy_from_slice(dst), cloned);
        Frame::Integer(1)
    } else {
        // Source expired between exists() and get() — race with lazy expiry
        Frame::Error(Bytes::from_static(b"ERR no such key"))
    }
}

/// SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE dest]
///
/// Sort elements in a list, set, or sorted set.
pub fn sort(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SORT");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SORT"),
    };

    // Parse options
    let mut by_pattern: Option<&[u8]> = None;
    let mut get_patterns: Vec<&[u8]> = Vec::new();
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut descending = false;
    let mut alpha = false;
    let mut store_dest: Option<&[u8]> = None;

    let mut i = 1;
    while i < args.len() {
        let arg = match extract_key(&args[i]) {
            Some(a) => a,
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        if arg.eq_ignore_ascii_case(b"BY") {
            i += 1;
            by_pattern = Some(match extract_key(args.get(i).unwrap_or(&Frame::Null)) {
                Some(p) => p,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            });
        } else if arg.eq_ignore_ascii_case(b"GET") {
            i += 1;
            let pat = match extract_key(args.get(i).unwrap_or(&Frame::Null)) {
                Some(p) => p,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            get_patterns.push(pat);
        } else if arg.eq_ignore_ascii_case(b"LIMIT") {
            let off = match args.get(i + 1).and_then(|f| parse_int(f)) {
                Some(v) if v >= 0 => v as usize,
                _ => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            let cnt = match args.get(i + 2).and_then(|f| parse_int(f)) {
                Some(v) if v >= 0 => v as usize,
                _ => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            };
            limit_offset = off;
            limit_count = Some(cnt);
            i += 2;
        } else if arg.eq_ignore_ascii_case(b"ASC") {
            descending = false;
        } else if arg.eq_ignore_ascii_case(b"DESC") {
            descending = true;
        } else if arg.eq_ignore_ascii_case(b"ALPHA") {
            alpha = true;
        } else if arg.eq_ignore_ascii_case(b"STORE") {
            i += 1;
            store_dest = Some(match extract_key(args.get(i).unwrap_or(&Frame::Null)) {
                Some(d) => d,
                None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
            });
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        i += 1;
    }

    // Extract elements from the key
    use crate::storage::compact_value::RedisValueRef;
    let elements: Vec<Bytes> = match db.get(key) {
        None => {
            // Non-existent key: return empty result
            return if let Some(dest) = store_dest {
                // STORE with empty → create empty list
                let entry = crate::storage::entry::Entry::new_list();
                db.set(Bytes::copy_from_slice(dest), entry);
                Frame::Integer(0)
            } else {
                Frame::Array(crate::framevec![])
            };
        }
        Some(entry) => match entry.value.as_redis_value() {
            RedisValueRef::List(l) => l.iter().cloned().collect(),
            RedisValueRef::ListListpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
            RedisValueRef::Set(s) => s.iter().cloned().collect(),
            RedisValueRef::SetListpack(lp) => lp.iter().map(|e| e.to_bytes()).collect(),
            RedisValueRef::SetIntset(is) => is.iter().map(|v| Bytes::from(v.to_string())).collect(),
            RedisValueRef::SortedSet { members, .. } => members.keys().cloned().collect(),
            RedisValueRef::SortedSetBPTree { members, .. } => members.keys().cloned().collect(),
            RedisValueRef::SortedSetListpack(lp) => {
                // Listpack stores member, score pairs
                let entries: Vec<_> = lp.iter().collect();
                entries
                    .chunks(2)
                    .filter_map(|c| c.first().map(|e| e.to_bytes()))
                    .collect()
            }
            _ => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        },
    };

    // Resolve sort keys (BY pattern or element itself)
    let sort_keys: Vec<Option<Bytes>> = if let Some(pattern) = by_pattern {
        if pattern == b"nosort" {
            // BY nosort = skip sorting
            elements.iter().map(|_| None).collect()
        } else {
            elements
                .iter()
                .map(|elem| {
                    let lookup_key = apply_pattern(pattern, elem);
                    db.get(&lookup_key)
                        .and_then(|e| e.value.as_bytes().map(|b| Bytes::copy_from_slice(b)))
                })
                .collect()
        }
    } else {
        elements.iter().map(|e| Some(e.clone())).collect()
    };

    // Create indexed pairs for stable sort
    let mut indices: Vec<usize> = (0..elements.len()).collect();

    // Sort (skip if BY nosort)
    let no_sort = by_pattern.is_some_and(|p| p == b"nosort");
    if !no_sort {
        indices.sort_by(|&a, &b| {
            let ka = sort_keys[a].as_ref();
            let kb = sort_keys[b].as_ref();
            let cmp = match (ka, kb) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (Some(va), Some(vb)) => {
                    if alpha {
                        va.cmp(vb)
                    } else {
                        let fa = std::str::from_utf8(va)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let fb = std::str::from_utf8(vb)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
                    }
                }
            };
            if descending { cmp.reverse() } else { cmp }
        });
    }

    // Apply LIMIT
    let start = limit_offset.min(indices.len());
    let count = limit_count.unwrap_or(indices.len());
    let end = (start + count).min(indices.len());
    let selected = &indices[start..end];

    // Build results
    let results: Vec<Frame> = if get_patterns.is_empty() {
        selected
            .iter()
            .map(|&idx| Frame::BulkString(elements[idx].clone()))
            .collect()
    } else {
        let mut out = Vec::with_capacity(selected.len() * get_patterns.len());
        for &idx in selected {
            for pat in &get_patterns {
                if *pat == b"#" {
                    out.push(Frame::BulkString(elements[idx].clone()));
                } else {
                    let lookup_key = apply_pattern(pat, &elements[idx]);
                    match db.get(&lookup_key) {
                        Some(e) => match e.value.as_bytes() {
                            Some(v) => out.push(Frame::BulkString(Bytes::copy_from_slice(v))),
                            None => out.push(Frame::Null),
                        },
                        None => out.push(Frame::Null),
                    }
                }
            }
        }
        out
    };

    // STORE or return
    if let Some(dest) = store_dest {
        let count = results.len() as i64;
        let mut list = std::collections::VecDeque::with_capacity(results.len());
        for frame in results {
            if let Frame::BulkString(b) = frame {
                list.push_back(b);
            }
        }
        let mut entry = crate::storage::entry::Entry::new_list();
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            crate::storage::entry::RedisValue::List(list),
        );
        db.set(Bytes::copy_from_slice(dest), entry);
        Frame::Integer(count)
    } else {
        Frame::Array(results.into())
    }
}

/// Apply a SORT pattern by replacing the first `*` with the element value.
fn apply_pattern(pattern: &[u8], element: &[u8]) -> Bytes {
    if let Some(pos) = pattern.iter().position(|&b| b == b'*') {
        let mut result = Vec::with_capacity(pattern.len() + element.len());
        result.extend_from_slice(&pattern[..pos]);
        result.extend_from_slice(element);
        result.extend_from_slice(&pattern[pos + 1..]);
        Bytes::from(result)
    } else {
        Bytes::copy_from_slice(pattern)
    }
}

/// Check if a value is large enough to warrant async drop.
fn should_async_drop(entry: &crate::storage::entry::Entry) -> bool {
    use crate::storage::compact_value::RedisValueRef;
    match entry.value.as_redis_value() {
        RedisValueRef::Hash(m) => m.len() > 64,
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
            if let Some(entry) = db.remove(key) {
                count += 1;
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

    // Collect all non-expired keys sorted for deterministic iteration
    let all_keys: Vec<CompactKey> = db.keys().cloned().collect();
    let mut sorted_keys: Vec<CompactKey> = Vec::new();
    for key in all_keys {
        if db.exists(key.as_bytes()) {
            sorted_keys.push(key);
        }
    }
    sorted_keys.sort();

    let total = sorted_keys.len();
    let mut results = Vec::new();
    let mut pos = cursor;

    // Iterate from cursor position, collect up to `count` matching keys
    let mut checked = 0;
    while pos < total && checked < count {
        let key = &sorted_keys[pos];
        pos += 1;
        checked += 1;

        // TYPE filter
        if let Some(tf) = type_filter {
            if let Some(entry) = db.get(key.as_bytes()) {
                let tn = entry.value.type_name().as_bytes();
                if !tf.eq_ignore_ascii_case(tn) {
                    continue;
                }
            } else {
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
        if db.exists_if_alive(key.as_bytes(), now_ms) && glob_match(pattern, key.as_bytes()) {
            result.push(Frame::BulkString(key.to_bytes()));
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

    // Collect all non-expired keys sorted for deterministic iteration
    let mut sorted_keys: Vec<CompactKey> = db
        .keys()
        .filter(|k| db.exists_if_alive(k.as_bytes(), now_ms))
        .cloned()
        .collect();
    sorted_keys.sort();

    let total = sorted_keys.len();
    let mut results = Vec::new();
    let mut pos = cursor;
    let mut checked = 0;

    while pos < total && checked < count {
        let key = &sorted_keys[pos];
        pos += 1;
        checked += 1;

        // TYPE filter
        if let Some(tf) = type_filter {
            if let Some(entry) = db.get_if_alive(key.as_bytes(), now_ms) {
                let tn = entry.value.type_name().as_bytes();
                if !tf.eq_ignore_ascii_case(tn) {
                    continue;
                }
            } else {
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
    fn test_expire_negative() {
        let mut db = setup_db_with_key(b"foo", b"bar");
        let result = expire(&mut db, &[bs(b"foo"), bs(b"-1")]);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR invalid expire")));
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

    // --- COPY tests ---

    #[test]
    fn test_copy_basic() {
        let mut db = setup_db_with_key(b"src", b"hello");
        let result = copy(&mut db, &[bs(b"src"), bs(b"dst")]);
        assert_eq!(result, Frame::Integer(1));
        assert!(db.exists(b"src"));
        assert!(db.exists(b"dst"));
    }

    #[test]
    fn test_copy_dest_exists_no_replace() {
        let mut db = setup_db_with_key(b"src", b"hello");
        db.set(
            Bytes::from_static(b"dst"),
            Entry::new_string(Bytes::from_static(b"existing")),
        );
        let result = copy(&mut db, &[bs(b"src"), bs(b"dst")]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_copy_with_replace() {
        let mut db = setup_db_with_key(b"src", b"hello");
        db.set(
            Bytes::from_static(b"dst"),
            Entry::new_string(Bytes::from_static(b"existing")),
        );
        let result = copy(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"REPLACE")]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_copy_nonexistent_source() {
        let mut db = Database::new();
        let result = copy(&mut db, &[bs(b"nosuchkey"), bs(b"dst")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_copy_same_key() {
        let mut db = setup_db_with_key(b"src", b"hello");
        let result = copy(&mut db, &[bs(b"src"), bs(b"src")]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_copy_db_option_errors() {
        let mut db = setup_db_with_key(b"src", b"hello");
        let result = copy(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"DB")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- SORT tests ---

    fn setup_list(db: &mut Database, key: &[u8], vals: &[&[u8]]) {
        use std::collections::VecDeque;
        let list: VecDeque<Bytes> = vals.iter().map(|v| Bytes::copy_from_slice(v)).collect();
        let mut entry = Entry::new_list();
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            crate::storage::entry::RedisValue::List(list),
        );
        db.set(Bytes::copy_from_slice(key), entry);
    }

    #[test]
    fn test_sort_numeric() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"3", b"1", b"2"]);
        let result = sort(&mut db, &[bs(b"mylist")]);
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"1")),
                Frame::BulkString(Bytes::from_static(b"2")),
                Frame::BulkString(Bytes::from_static(b"3")),
            ])
        );
    }

    #[test]
    fn test_sort_alpha() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"c", b"a", b"b"]);
        let result = sort(&mut db, &[bs(b"mylist"), bs(b"ALPHA")]);
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"a")),
                Frame::BulkString(Bytes::from_static(b"b")),
                Frame::BulkString(Bytes::from_static(b"c")),
            ])
        );
    }

    #[test]
    fn test_sort_desc() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"1", b"3", b"2"]);
        let result = sort(&mut db, &[bs(b"mylist"), bs(b"DESC")]);
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"2")),
                Frame::BulkString(Bytes::from_static(b"1")),
            ])
        );
    }

    #[test]
    fn test_sort_limit() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"3", b"1", b"2", b"4"]);
        let result = sort(&mut db, &[bs(b"mylist"), bs(b"LIMIT"), bs(b"1"), bs(b"2")]);
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"2")),
                Frame::BulkString(Bytes::from_static(b"3")),
            ])
        );
    }

    #[test]
    fn test_sort_store() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"3", b"1", b"2"]);
        let result = sort(&mut db, &[bs(b"mylist"), bs(b"STORE"), bs(b"sorted")]);
        assert_eq!(result, Frame::Integer(3));
        assert!(db.exists(b"sorted"));
    }

    #[test]
    fn test_sort_nonexistent() {
        let mut db = Database::new();
        let result = sort(&mut db, &[bs(b"nokey")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    #[test]
    fn test_sort_set() {
        let mut db = Database::new();
        let mut s = std::collections::HashSet::new();
        s.insert(Bytes::from_static(b"3"));
        s.insert(Bytes::from_static(b"1"));
        s.insert(Bytes::from_static(b"2"));
        let mut entry = Entry::new_set();
        entry.value = crate::storage::compact_value::CompactValue::from_redis_value(
            crate::storage::entry::RedisValue::Set(s),
        );
        db.set(Bytes::from_static(b"myset"), entry);
        let result = sort(&mut db, &[bs(b"myset")]);
        // Sort result should be [1, 2, 3] regardless of HashSet order
        assert_eq!(
            result,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"1")),
                Frame::BulkString(Bytes::from_static(b"2")),
                Frame::BulkString(Bytes::from_static(b"3")),
            ])
        );
    }
}
