//! Extra key commands: COPY, SORT, MEMORY.
//!
//! Split from key.rs to stay under the 1500-line limit.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use super::helpers::err_wrong_args;
use super::key::{extract_key, parse_int};

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
        } else if arg.eq_ignore_ascii_case(b"DB") {
            // Cross-DB copy requires shard_databases context not available here
            return Frame::Error(Bytes::from_static(
                b"ERR COPY with DB option is not supported yet",
            ));
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        i += 1;
    }

    // Redis returns 0 (not error) when source doesn't exist
    if !db.exists(src) {
        return Frame::Integer(0);
    }

    // Same key: source == dest — Redis returns 1 only with REPLACE, else 0
    if src == dst {
        return Frame::Integer(if replace { 1 } else { 0 });
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

/// MEMORY USAGE key [SAMPLES count]
///
/// Returns the number of bytes a key and its value require to be stored.
pub fn memory_usage(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("MEMORY");
    }
    let key = match extract_key(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("MEMORY"),
    };

    // Parse optional SAMPLES count; reject unknown trailing args
    let mut i = 1;
    let mut _samples: usize = 5; // default sample count (like Redis)
    while i < args.len() {
        let arg = match extract_key(&args[i]) {
            Some(a) => a,
            None => return err_wrong_args("MEMORY"),
        };
        if arg.eq_ignore_ascii_case(b"SAMPLES") {
            i += 1;
            let count_arg = match args.get(i).and_then(|f| extract_key(f)) {
                Some(c) => c,
                None => return err_wrong_args("MEMORY"),
            };
            match std::str::from_utf8(count_arg).ok().and_then(|s| s.parse::<usize>().ok()) {
                Some(c) if c > 0 => _samples = c,
                _ => return err_wrong_args("MEMORY"),
            }
        } else {
            return err_wrong_args("MEMORY");
        }
        i += 1;
    }

    match db.get(key) {
        Some(entry) => {
            let mem = key.len() + entry.value.estimate_memory() + 128; // same as entry_overhead
            Frame::Integer(mem as i64)
        }
        None => Frame::Null,
    }
}

/// MEMORY DOCTOR — report memory health issues.
pub fn memory_doctor() -> Frame {
    // Basic health report
    Frame::BulkString(Bytes::from_static(b"Sam, I have no memory problems"))
}

/// MEMORY HELP — list MEMORY subcommands.
pub fn memory_help() -> Frame {
    Frame::Array(
        vec![
            Frame::BulkString(Bytes::from_static(
                b"MEMORY DOCTOR - Return memory problems reports.",
            )),
            Frame::BulkString(Bytes::from_static(
                b"MEMORY HELP - Return this help message.",
            )),
            Frame::BulkString(Bytes::from_static(
                b"MEMORY USAGE <key> [SAMPLES <count>] - Return memory in bytes used by <key> and its value.",
            )),
        ]
        .into(),
    )
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
                Frame::Array(framevec![])
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

    // For numeric sort (no ALPHA), validate all sort keys are parseable as f64
    let no_sort = by_pattern.is_some_and(|p| p == b"nosort");
    if !alpha && !no_sort {
        for sk in &sort_keys {
            if let Some(v) = sk {
                if std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .is_none()
                {
                    return Frame::Error(Bytes::from_static(
                        b"ERR One or more scores can't be converted into double",
                    ));
                }
            }
        }
    }

    // Create indexed pairs for stable sort
    let mut indices: Vec<usize> = (0..elements.len()).collect();
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
            match frame {
                Frame::BulkString(b) => list.push_back(b),
                // Redis stores nil GET results as empty strings in SORT...STORE
                _ => list.push_back(Bytes::new()),
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
        // Redis returns 0 for missing source, not error
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_copy_same_key() {
        let mut db = setup_db_with_key(b"src", b"hello");
        let result = copy(&mut db, &[bs(b"src"), bs(b"src")]);
        // Redis returns 0 for same-key copy
        assert_eq!(result, Frame::Integer(0));
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
