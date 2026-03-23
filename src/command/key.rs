use bytes::Bytes;
use std::time::{Duration, Instant};

use crate::protocol::Frame;
use crate::storage::entry::RedisValue;
use crate::storage::Database;

/// Helper: build an ERR frame for wrong number of arguments.
fn err_wrong_args(cmd: &str) -> Frame {
    Frame::Error(Bytes::from(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd
    )))
}

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
        Frame::BulkString(s) | Frame::SimpleString(s) => {
            std::str::from_utf8(s).ok()?.parse().ok()
        }
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
            ))
        }
    };
    if seconds <= 0 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'EXPIRE' command",
        ));
    }
    let expires_at = Instant::now() + Duration::from_secs(seconds as u64);
    if db.set_expiry(key, Some(expires_at)) {
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
            ))
        }
    };
    if millis <= 0 {
        return Frame::Error(Bytes::from_static(
            b"ERR invalid expire time in 'PEXPIRE' command",
        ));
    }
    let expires_at = Instant::now() + Duration::from_millis(millis as u64);
    if db.set_expiry(key, Some(expires_at)) {
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
        Some(entry) => match entry.expires_at {
            None => Frame::Integer(-1),
            Some(exp) => {
                let now = Instant::now();
                if now >= exp {
                    // Edge case: expired between get and now
                    Frame::Integer(-2)
                } else {
                    Frame::Integer((exp - now).as_secs() as i64)
                }
            }
        },
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
        Some(entry) => match entry.expires_at {
            None => Frame::Integer(-1),
            Some(exp) => {
                let now = Instant::now();
                if now >= exp {
                    Frame::Integer(-2)
                } else {
                    Frame::Integer((exp - now).as_millis() as i64)
                }
            }
        },
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
            if entry.expires_at.is_none() {
                return Frame::Integer(0);
            }
            // Key exists and has TTL -- remove it
            // We need to re-borrow mutably, so use set_expiry
            drop(entry);
            db.set_expiry(key, None);
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

/// Redis-compatible glob pattern matcher.
///
/// Supports: `*` (any sequence), `?` (one byte), `[abc]` (character class),
/// `[^abc]`/`[!abc]` (negated class), `[a-z]` (range), `\x` (escape).
fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
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
    let all_keys: Vec<Bytes> = db.keys().cloned().collect();

    let mut result = Vec::new();
    for key in all_keys {
        // Trigger lazy expiry by calling exists
        if db.exists(&key) && glob_match(pattern, &key) {
            result.push(Frame::BulkString(key));
        }
    }

    Frame::Array(result)
}

/// RENAME key newkey
///
/// Renames key to newkey. Returns an error when key does not exist.
/// If source and destination are the same, returns OK without deleting.
/// Overwrites destination if it exists. Preserves TTL.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::Entry;

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

    fn setup_db_with_expiry(key: &[u8], val: &[u8], expires_at: Instant) -> Database {
        let mut db = Database::new();
        db.set(
            Bytes::copy_from_slice(key),
            Entry::new_string_with_expiry(Bytes::copy_from_slice(val), expires_at),
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
        db.set(Bytes::from_static(b"a"), Entry::new_string(Bytes::from_static(b"1")));
        db.set(Bytes::from_static(b"b"), Entry::new_string(Bytes::from_static(b"2")));
        db.set(Bytes::from_static(b"c"), Entry::new_string(Bytes::from_static(b"3")));
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
        let mut db = setup_db_with_expiry(
            b"foo",
            b"bar",
            Instant::now() + Duration::from_secs(3600),
        );
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
        db.set(Bytes::from_static(b"foo"), Entry::new_string(Bytes::from_static(b"1")));
        db.set(Bytes::from_static(b"bar"), Entry::new_string(Bytes::from_static(b"2")));
        db.set(Bytes::from_static(b"baz"), Entry::new_string(Bytes::from_static(b"3")));
        let result = keys(&mut db, &[bs(b"*")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_keys_pattern() {
        let mut db = Database::new();
        db.set(Bytes::from_static(b"hello"), Entry::new_string(Bytes::from_static(b"1")));
        db.set(Bytes::from_static(b"hallo"), Entry::new_string(Bytes::from_static(b"2")));
        db.set(Bytes::from_static(b"world"), Entry::new_string(Bytes::from_static(b"3")));
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
        let past = Instant::now() - Duration::from_secs(1);
        db.set(
            Bytes::from_static(b"dead"),
            Entry::new_string_with_expiry(Bytes::from_static(b"2"), past),
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
        let future = Instant::now() + Duration::from_secs(3600);
        let mut db = setup_db_with_expiry(b"old", b"value", future);
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
        db.set(Bytes::from_static(b"src"), Entry::new_string(Bytes::from_static(b"srcval")));
        db.set(Bytes::from_static(b"dst"), Entry::new_string(Bytes::from_static(b"dstval")));
        rename(&mut db, &[bs(b"src"), bs(b"dst")]);
        assert!(!db.exists(b"src"));
        let entry = db.get(b"dst").unwrap();
        match &entry.value {
            RedisValue::String(v) => assert_eq!(v.as_ref(), b"srcval"),
            _ => panic!("unexpected type"),
        }
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
        db.set(Bytes::from_static(b"src"), Entry::new_string(Bytes::from_static(b"1")));
        db.set(Bytes::from_static(b"dst"), Entry::new_string(Bytes::from_static(b"2")));
        let result = renamenx(&mut db, &[bs(b"src"), bs(b"dst")]);
        assert_eq!(result, Frame::Integer(0));
        // Both keys should still exist
        assert!(db.exists(b"src"));
        assert!(db.exists(b"dst"));
    }
}
