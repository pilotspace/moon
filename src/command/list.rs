use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::db::{LISTPACK_MAX_ELEMENT_SIZE, LISTPACK_MAX_ENTRIES};
use crate::storage::Database;
use crate::framevec;
/// Helper: return ERR wrong number of arguments for a given command.
fn err_wrong_args(cmd: &str) -> Frame {
    Frame::Error(Bytes::from(format!(
        "ERR wrong number of arguments for '{}' command",
        cmd
    )))
}

/// Helper: extract &Bytes from a BulkString or SimpleString frame.
fn extract_bytes(frame: &Frame) -> Option<&Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b),
        _ => None,
    }
}

/// Helper: parse i64 from a frame.
fn parse_i64(frame: &Frame) -> Option<i64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

/// Helper: resolve a Redis index (possibly negative) to a usize position within a list of given length.
/// Returns None if the resolved index is out of bounds.
fn resolve_index(index: i64, len: usize) -> Option<usize> {
    let resolved = if index < 0 {
        len as i64 + index
    } else {
        index
    };
    if resolved < 0 || resolved >= len as i64 {
        None
    } else {
        Some(resolved as usize)
    }
}

/// LPUSH key element [element ...]
/// Each element is pushed to the front in order, so LPUSH mylist a b c -> [c, b, a].
pub fn lpush(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("LPUSH");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPUSH"),
    };

    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        match db.get_or_create_list_listpack(key) {
            Ok(Some(lp)) => {
                for arg in &args[1..] {
                    let val = match extract_bytes(arg) {
                        Some(v) => v,
                        None => return err_wrong_args("LPUSH"),
                    };
                    lp.push_front(val);
                }
                let len = lp.len();
                if len > LISTPACK_MAX_ENTRIES {
                    db.upgrade_list_listpack_to_list(key);
                }
                return Frame::Integer(len as i64);
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    for arg in &args[1..] {
        let val = match extract_bytes(arg) {
            Some(v) => v.clone(),
            None => return err_wrong_args("LPUSH"),
        };
        list.push_front(val);
    }
    Frame::Integer(list.len() as i64)
}

/// RPUSH key element [element ...]
pub fn rpush(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("RPUSH");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("RPUSH"),
    };

    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        match db.get_or_create_list_listpack(key) {
            Ok(Some(lp)) => {
                for arg in &args[1..] {
                    let val = match extract_bytes(arg) {
                        Some(v) => v,
                        None => return err_wrong_args("RPUSH"),
                    };
                    lp.push_back(val);
                }
                let len = lp.len();
                if len > LISTPACK_MAX_ENTRIES {
                    db.upgrade_list_listpack_to_list(key);
                }
                return Frame::Integer(len as i64);
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    for arg in &args[1..] {
        let val = match extract_bytes(arg) {
            Some(v) => v.clone(),
            None => return err_wrong_args("RPUSH"),
        };
        list.push_back(val);
    }
    Frame::Integer(list.len() as i64)
}

/// LPOP key [count]
pub fn lpop(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("LPOP");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("LPOP"),
    };

    // Check if the key exists first (for the no-list case)
    match db.get_list(&key) {
        Ok(None) => {
            return if args.len() == 2 {
                Frame::Array(framevec![])
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let count = if args.len() == 2 {
        match parse_i64(&args[1]) {
            Some(c) if c >= 0 => Some(c as usize),
            Some(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            }
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            }
        }
    } else {
        None
    };

    let list = match db.get_or_create_list(&key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let result = match count {
        None => {
            // Single pop
            match list.pop_front() {
                Some(v) => Frame::BulkString(v),
                None => Frame::Null,
            }
        }
        Some(c) => {
            let actual = c.min(list.len());
            let mut items = Vec::with_capacity(actual);
            for _ in 0..actual {
                if let Some(v) = list.pop_front() {
                    items.push(Frame::BulkString(v));
                }
            }
            Frame::Array(items.into())
        }
    };

    // If list is now empty, remove the key
    if db.get_list(&key).ok().flatten().map_or(false, |l| l.is_empty()) {
        db.remove(&key);
    }

    result
}

/// RPOP key [count]
pub fn rpop(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("RPOP");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("RPOP"),
    };

    match db.get_list(&key) {
        Ok(None) => {
            return if args.len() == 2 {
                Frame::Array(framevec![])
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let count = if args.len() == 2 {
        match parse_i64(&args[1]) {
            Some(c) if c >= 0 => Some(c as usize),
            Some(_) => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            }
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            }
        }
    } else {
        None
    };

    let list = match db.get_or_create_list(&key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let result = match count {
        None => match list.pop_back() {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        Some(c) => {
            let actual = c.min(list.len());
            let mut items = Vec::with_capacity(actual);
            for _ in 0..actual {
                if let Some(v) = list.pop_back() {
                    items.push(Frame::BulkString(v));
                }
            }
            Frame::Array(items.into())
        }
    };

    if db.get_list(&key).ok().flatten().map_or(false, |l| l.is_empty()) {
        db.remove(&key);
    }

    result
}

/// LLEN key
pub fn llen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("LLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LLEN"),
    };
    match db.get_list(key) {
        Ok(Some(list)) => Frame::Integer(list.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// LRANGE key start stop
pub fn lrange(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LRANGE"),
    };
    let start = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let stop = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };

    let list = match db.get_list(key) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Array(framevec![]),
        Err(e) => return e,
    };

    let len = list.len() as i64;

    // Resolve negative indices
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };

    // Clamp
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return Frame::Array(framevec![]);
    }

    let items: Vec<Frame> = (s as usize..=e as usize)
        .map(|i| Frame::BulkString(list[i].clone()))
        .collect();
    Frame::Array(items.into())
}

/// LINDEX key index
pub fn lindex(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("LINDEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LINDEX"),
    };
    let index = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };

    let list = match db.get_list(key) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };

    match resolve_index(index, list.len()) {
        Some(i) => Frame::BulkString(list[i].clone()),
        None => Frame::Null,
    }
}

/// LSET key index element
pub fn lset(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("LSET"),
    };
    let index = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let element = match extract_bytes(&args[2]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LSET"),
    };

    let list = match db.get_or_create_list(&key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    if list.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR no such key"));
    }

    match resolve_index(index, list.len()) {
        Some(i) => {
            list[i] = element;
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }
        None => Frame::Error(Bytes::from_static(b"ERR index out of range")),
    }
}

/// LINSERT key BEFORE|AFTER pivot element
pub fn linsert(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 4 {
        return err_wrong_args("LINSERT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("LINSERT"),
    };
    let position = match extract_bytes(&args[1]) {
        Some(p) => p.as_ref(),
        None => return err_wrong_args("LINSERT"),
    };
    let pivot = match extract_bytes(&args[2]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LINSERT"),
    };
    let element = match extract_bytes(&args[3]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LINSERT"),
    };

    let before = if position.eq_ignore_ascii_case(b"BEFORE") {
        true
    } else if position.eq_ignore_ascii_case(b"AFTER") {
        false
    } else {
        return Frame::Error(Bytes::from_static(
            b"ERR syntax error",
        ));
    };

    // If key doesn't exist, return 0
    match db.get_list(&key) {
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(&key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    // Find pivot
    let pos = list.iter().position(|v| v == &pivot);
    match pos {
        None => Frame::Integer(-1),
        Some(idx) => {
            let insert_at = if before { idx } else { idx + 1 };
            // VecDeque doesn't have insert, we need to use make_contiguous or manual approach
            // Actually VecDeque does NOT have insert in stable Rust... let's work around it.
            // Wait -- VecDeque::insert is stable since Rust 1.43
            list.insert(insert_at, element);
            Frame::Integer(list.len() as i64)
        }
    }
}

/// LREM key count element
pub fn lrem(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LREM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("LREM"),
    };
    let count = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let element = match extract_bytes(&args[2]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LREM"),
    };

    match db.get_list(&key) {
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(&key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let mut removed = 0i64;
    let max_remove = if count == 0 {
        usize::MAX
    } else {
        count.unsigned_abs() as usize
    };

    if count >= 0 {
        // Remove from head (or all if count == 0)
        let mut i = 0;
        while i < list.len() && (removed as usize) < max_remove {
            if list[i] == element {
                list.remove(i);
                removed += 1;
            } else {
                i += 1;
            }
        }
    } else {
        // Remove from tail
        let mut i = list.len();
        while i > 0 && (removed as usize) < max_remove {
            i -= 1;
            if list[i] == element {
                list.remove(i);
                removed += 1;
            }
        }
    }

    // If list is now empty, remove the key
    if list.is_empty() {
        db.remove(&key);
    }

    Frame::Integer(removed)
}

/// LTRIM key start stop
pub fn ltrim(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LTRIM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("LTRIM"),
    };
    let start = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let stop = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };

    match db.get_list(&key) {
        Ok(None) => return Frame::SimpleString(Bytes::from_static(b"OK")),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(&key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let len = list.len() as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };

    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }

    if s > e || s >= len {
        // Empty range -- clear the list
        list.clear();
    } else {
        // Keep only [s..=e]
        let s = s as usize;
        let e = e as usize;
        // Drain from the back first, then from the front
        if e + 1 < list.len() {
            list.drain(e + 1..);
        }
        if s > 0 {
            list.drain(..s);
        }
    }

    if list.is_empty() {
        db.remove(&key);
    }

    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
pub fn lpos(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("LPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPOS"),
    };
    let element = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LPOS"),
    };

    // Parse optional arguments
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0; // 0 means no limit

    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => return err_wrong_args("LPOS"),
        };
        i += 1;
        if i >= args.len() {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        let val = match parse_i64(&args[i]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            }
        };
        i += 1;
        if opt.eq_ignore_ascii_case(b"RANK") {
            if val == 0 {
                return Frame::Error(Bytes::from_static(
                    b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the end of the list",
                ));
            }
            rank = val;
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(
                    b"ERR COUNT can't be negative",
                ));
            }
            count = Some(val as usize);
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(
                    b"ERR MAXLEN can't be negative",
                ));
            }
            maxlen = val as usize;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }

    let list = match db.get_list(key) {
        Ok(Some(l)) => l,
        Ok(None) => {
            return if count.is_some() {
                Frame::Array(framevec![])
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
    };

    let len = list.len();
    let max_count = match count {
        Some(0) => usize::MAX, // COUNT 0 means all
        Some(c) => c,
        None => 1, // Without COUNT, return first match only
    };

    let mut matches: Vec<i64> = Vec::new();
    let mut match_count = 0usize;

    if rank > 0 {
        // Forward scan
        let mut skip = rank as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        for idx in 0..scan_limit {
            if list[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count {
                        break;
                    }
                }
            }
        }
    } else {
        // Reverse scan
        let mut skip = (-rank) as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        let mut scanned = 0;
        for idx in (0..len).rev() {
            if scanned >= scan_limit {
                break;
            }
            scanned += 1;
            if list[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count {
                        break;
                    }
                }
            }
        }
    }

    if count.is_some() {
        // Return array of positions
        Frame::Array(matches.into_iter().map(Frame::Integer).collect())
    } else {
        // Return single position or Null
        matches
            .first()
            .map(|&pos| Frame::Integer(pos))
            .unwrap_or(Frame::Null)
    }
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// LLEN (read-only).
pub fn llen_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("LLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LLEN"),
    };
    match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(lref)) => Frame::Integer(lref.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// LRANGE (read-only).
pub fn lrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LRANGE"),
    };
    let start = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let stop = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Array(framevec![]),
        Err(e) => return e,
    };
    let len = lref.len() as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 { s = 0; }
    if e >= len { e = len - 1; }
    if s > e || s >= len {
        return Frame::Array(framevec![]);
    }
    let items: Vec<Frame> = lref.range(s as usize, e as usize)
        .into_iter()
        .map(Frame::BulkString)
        .collect();
    Frame::Array(items.into())
}

/// LINDEX (read-only).
pub fn lindex_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("LINDEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LINDEX"),
    };
    let index = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        }
    };
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };
    match resolve_index(index, lref.len()) {
        Some(i) => match lref.get(i) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        None => Frame::Null,
    }
}

/// LPOS (read-only).
pub fn lpos_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("LPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPOS"),
    };
    let element = match extract_bytes(&args[1]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("LPOS"),
    };
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => return err_wrong_args("LPOS"),
        };
        i += 1;
        if i >= args.len() {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
        let val = match parse_i64(&args[i]) {
            Some(v) => v,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ))
            }
        };
        i += 1;
        if opt.eq_ignore_ascii_case(b"RANK") {
            if val == 0 {
                return Frame::Error(Bytes::from_static(
                    b"ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative values meaning from the end of the list",
                ));
            }
            rank = val;
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(b"ERR COUNT can't be negative"));
            }
            count = Some(val as usize);
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") {
            if val < 0 {
                return Frame::Error(Bytes::from_static(b"ERR MAXLEN can't be negative"));
            }
            maxlen = val as usize;
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => {
            return if count.is_some() {
                Frame::Array(framevec![])
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
    };
    let all_elements = lref.iter_bytes();
    let len = all_elements.len();
    let max_count = match count {
        Some(0) => usize::MAX,
        Some(c) => c,
        None => 1,
    };
    let mut matches: Vec<i64> = Vec::new();
    let mut match_count = 0usize;
    if rank > 0 {
        let mut skip = rank as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        for idx in 0..scan_limit {
            if all_elements[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count { break; }
                }
            }
        }
    } else {
        let mut skip = (-rank) as usize - 1;
        let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
        let mut scanned = 0;
        for idx in (0..len).rev() {
            if scanned >= scan_limit { break; }
            scanned += 1;
            if all_elements[idx] == element {
                if skip > 0 {
                    skip -= 1;
                } else {
                    matches.push(idx as i64);
                    match_count += 1;
                    if match_count >= max_count { break; }
                }
            }
        }
    }
    if count.is_some() {
        Frame::Array(matches.into_iter().map(Frame::Integer).collect())
    } else {
        matches.first().copied().map(Frame::Integer).unwrap_or(Frame::Null)
    }
}

/// Parse a direction argument (LEFT or RIGHT).
fn parse_direction(frame: &Frame) -> Result<crate::blocking::Direction, Frame> {
    let b = extract_bytes(frame).ok_or_else(|| err_wrong_args("LMOVE"))?;
    if b.eq_ignore_ascii_case(b"LEFT") {
        Ok(crate::blocking::Direction::Left)
    } else if b.eq_ignore_ascii_case(b"RIGHT") {
        Ok(crate::blocking::Direction::Right)
    } else {
        Err(Frame::Error(Bytes::from_static(b"ERR syntax error")))
    }
}

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
/// Atomically pops from source and pushes to destination.
/// Returns the moved element, or Null if source is empty.
pub fn lmove(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 4 {
        return err_wrong_args("LMOVE");
    }
    let source = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("LMOVE"),
    };
    let destination = match extract_bytes(&args[1]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("LMOVE"),
    };
    let wherefrom = match parse_direction(&args[2]) {
        Ok(d) => d,
        Err(e) => return e,
    };
    let whereto = match parse_direction(&args[3]) {
        Ok(d) => d,
        Err(e) => return e,
    };

    use crate::blocking::Direction;

    // Type check: source must be a list or not exist
    match db.get_list(&source) {
        Ok(None) => return Frame::Null, // source empty or missing
        Err(e) => return e,             // WRONGTYPE
        Ok(Some(_)) => {}
    }

    // If destination exists, type check it too (unless same as source)
    if source != destination {
        match db.get_list(&destination) {
            Ok(_) => {}     // exists as list or missing -- both OK
            Err(e) => return e, // WRONGTYPE
        }
    }

    // Pop from source
    let value = match wherefrom {
        Direction::Left => db.list_pop_front(&source),
        Direction::Right => db.list_pop_back(&source),
    };
    let value = match value {
        Some(v) => v,
        None => return Frame::Null,
    };

    // Push to destination
    match whereto {
        Direction::Left => db.list_push_front(&destination, value.clone()),
        Direction::Right => db.list_push_back(&destination, value.clone()),
    }

    Frame::BulkString(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn setup_list(db: &mut Database, key: &[u8], elements: &[&[u8]]) {
        for elem in elements {
            rpush(db, &[bs(key), bs(elem)]);
        }
    }

    // --- LPUSH tests ---

    #[test]
    fn test_lpush_basic() {
        let mut db = Database::new();
        let result = lpush(&mut db, &[bs(b"mylist"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(3));
        // LPUSH a b c -> [c, b, a] (each pushed to front in order)
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"c"), bs(b"b"), bs(b"a")])
        );
    }

    #[test]
    fn test_lpush_wrong_args() {
        let mut db = Database::new();
        let result = lpush(&mut db, &[bs(b"mylist")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    // --- RPUSH tests ---

    #[test]
    fn test_rpush_basic() {
        let mut db = Database::new();
        let result = rpush(&mut db, &[bs(b"mylist"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(3));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")])
        );
    }

    // --- LPOP tests ---

    #[test]
    fn test_lpop_single() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"a")));
    }

    #[test]
    fn test_lpop_with_count() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lpop(&mut db, &[bs(b"mylist"), bs(b"2")]);
        assert_eq!(
            result,
            Frame::Array(framevec![bs(b"a"), bs(b"b")])
        );
    }

    #[test]
    fn test_lpop_empty() {
        let mut db = Database::new();
        let result = lpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_lpop_removes_empty_list() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a"]);
        lpop(&mut db, &[bs(b"mylist")]);
        assert!(!db.exists(b"mylist"));
    }

    // --- RPOP tests ---

    #[test]
    fn test_rpop_single() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = rpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
    }

    #[test]
    fn test_rpop_with_count() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = rpop(&mut db, &[bs(b"mylist"), bs(b"2")]);
        assert_eq!(
            result,
            Frame::Array(framevec![bs(b"c"), bs(b"b")])
        );
    }

    #[test]
    fn test_rpop_empty() {
        let mut db = Database::new();
        let result = rpop(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Null);
    }

    // --- LLEN tests ---

    #[test]
    fn test_llen() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = llen(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_llen_missing() {
        let mut db = Database::new();
        let result = llen(&mut db, &[bs(b"mylist")]);
        assert_eq!(result, Frame::Integer(0));
    }

    // --- LRANGE tests ---

    #[test]
    fn test_lrange_positive_indices() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"d"]);
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"1"), bs(b"2")]);
        assert_eq!(
            result,
            Frame::Array(framevec![bs(b"b"), bs(b"c")])
        );
    }

    #[test]
    fn test_lrange_negative_indices() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"d"]);
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"-3"), bs(b"-1")]);
        assert_eq!(
            result,
            Frame::Array(framevec![bs(b"b"), bs(b"c"), bs(b"d")])
        );
    }

    #[test]
    fn test_lrange_out_of_range_clamping() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"100")]);
        assert_eq!(
            result,
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")])
        );
    }

    #[test]
    fn test_lrange_missing_key() {
        let mut db = Database::new();
        let result = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    // --- LINDEX tests ---

    #[test]
    fn test_lindex_positive() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lindex(&mut db, &[bs(b"mylist"), bs(b"1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"b")));
    }

    #[test]
    fn test_lindex_negative() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lindex(&mut db, &[bs(b"mylist"), bs(b"-1")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
    }

    #[test]
    fn test_lindex_out_of_range() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lindex(&mut db, &[bs(b"mylist"), bs(b"10")]);
        assert_eq!(result, Frame::Null);
    }

    // --- LSET tests ---

    #[test]
    fn test_lset_valid() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lset(&mut db, &[bs(b"mylist"), bs(b"1"), bs(b"x")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        let val = lindex(&mut db, &[bs(b"mylist"), bs(b"1")]);
        assert_eq!(val, Frame::BulkString(Bytes::from_static(b"x")));
    }

    #[test]
    fn test_lset_out_of_range() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lset(&mut db, &[bs(b"mylist"), bs(b"10"), bs(b"x")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"ERR index out of range")));
    }

    // --- LINSERT tests ---

    #[test]
    fn test_linsert_before() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = linsert(&mut db, &[bs(b"mylist"), bs(b"BEFORE"), bs(b"b"), bs(b"x")]);
        assert_eq!(result, Frame::Integer(4));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"a"), bs(b"x"), bs(b"b"), bs(b"c")])
        );
    }

    #[test]
    fn test_linsert_after() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = linsert(&mut db, &[bs(b"mylist"), bs(b"AFTER"), bs(b"b"), bs(b"x")]);
        assert_eq!(result, Frame::Integer(4));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"x"), bs(b"c")])
        );
    }

    #[test]
    fn test_linsert_pivot_not_found() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = linsert(
            &mut db,
            &[bs(b"mylist"), bs(b"BEFORE"), bs(b"z"), bs(b"x")],
        );
        assert_eq!(result, Frame::Integer(-1));
    }

    #[test]
    fn test_linsert_missing_key() {
        let mut db = Database::new();
        let result = linsert(
            &mut db,
            &[bs(b"mylist"), bs(b"BEFORE"), bs(b"a"), bs(b"x")],
        );
        assert_eq!(result, Frame::Integer(0));
    }

    // --- LREM tests ---

    #[test]
    fn test_lrem_from_head() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"a", b"c", b"a"]);
        let result = lrem(&mut db, &[bs(b"mylist"), bs(b"2"), bs(b"a")]);
        assert_eq!(result, Frame::Integer(2));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"b"), bs(b"c"), bs(b"a")])
        );
    }

    #[test]
    fn test_lrem_from_tail() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"a", b"c", b"a"]);
        let result = lrem(&mut db, &[bs(b"mylist"), bs(b"-2"), bs(b"a")]);
        assert_eq!(result, Frame::Integer(2));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"a"), bs(b"b"), bs(b"c")])
        );
    }

    #[test]
    fn test_lrem_all() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"a", b"c", b"a"]);
        let result = lrem(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"a")]);
        assert_eq!(result, Frame::Integer(3));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(range, Frame::Array(framevec![bs(b"b"), bs(b"c")]));
    }

    #[test]
    fn test_lrem_removes_empty_list() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"a"]);
        lrem(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"a")]);
        assert!(!db.exists(b"mylist"));
    }

    // --- LTRIM tests ---

    #[test]
    fn test_ltrim_subrange() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"d", b"e"]);
        let result = ltrim(&mut db, &[bs(b"mylist"), bs(b"1"), bs(b"3")]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        let range = lrange(&mut db, &[bs(b"mylist"), bs(b"0"), bs(b"-1")]);
        assert_eq!(
            range,
            Frame::Array(framevec![bs(b"b"), bs(b"c"), bs(b"d")])
        );
    }

    #[test]
    fn test_ltrim_to_empty() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        ltrim(&mut db, &[bs(b"mylist"), bs(b"5"), bs(b"10")]);
        assert!(!db.exists(b"mylist"));
    }

    // --- LPOS tests ---

    #[test]
    fn test_lpos_basic() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b")]);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_lpos_not_found() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"z")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_lpos_with_rank() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b"), bs(b"RANK"), bs(b"2")]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_lpos_with_count() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b"), bs(b"COUNT"), bs(b"0")]);
        assert_eq!(
            result,
            Frame::Array(framevec![Frame::Integer(1), Frame::Integer(3)])
        );
    }

    #[test]
    fn test_lpos_with_count_limited() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        let result = lpos(&mut db, &[bs(b"mylist"), bs(b"b"), bs(b"COUNT"), bs(b"1")]);
        assert_eq!(result, Frame::Array(framevec![Frame::Integer(1)]));
    }

    #[test]
    fn test_lpos_with_maxlen() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c", b"b", b"d"]);
        // MAXLEN 2 only scans first 2 elements
        let result = lpos(
            &mut db,
            &[bs(b"mylist"), bs(b"b"), bs(b"MAXLEN"), bs(b"2"), bs(b"COUNT"), bs(b"0")],
        );
        assert_eq!(result, Frame::Array(framevec![Frame::Integer(1)]));
    }

    // --- WRONGTYPE test ---

    #[test]
    fn test_wrongtype_on_string_key() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"mykey"), Bytes::from_static(b"val"));
        let result = lpush(&mut db, &[bs(b"mykey"), bs(b"a")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"WRONGTYPE")));
    }

    // --- LMOVE tests ---

    #[test]
    fn test_lmove_left_left() {
        let mut db = Database::new();
        setup_list(&mut db, b"src", &[b"a", b"b", b"c"]);
        let result = lmove(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"LEFT"), bs(b"LEFT")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"a")));
        // src should be [b, c], dst should be [a]
        let src_list = db.get_list(b"src").unwrap().unwrap();
        assert_eq!(src_list.len(), 2);
        let dst_list = db.get_list(b"dst").unwrap().unwrap();
        assert_eq!(dst_list.len(), 1);
        assert_eq!(dst_list[0], Bytes::from_static(b"a"));
    }

    #[test]
    fn test_lmove_right_right() {
        let mut db = Database::new();
        setup_list(&mut db, b"src", &[b"a", b"b", b"c"]);
        let result = lmove(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"RIGHT"), bs(b"RIGHT")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
    }

    #[test]
    fn test_lmove_empty_source() {
        let mut db = Database::new();
        let result = lmove(&mut db, &[bs(b"nosrc"), bs(b"dst"), bs(b"LEFT"), bs(b"RIGHT")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_lmove_same_key() {
        let mut db = Database::new();
        setup_list(&mut db, b"mylist", &[b"a", b"b", b"c"]);
        // Rotate: pop right, push left -> c moves to front
        let result = lmove(&mut db, &[bs(b"mylist"), bs(b"mylist"), bs(b"RIGHT"), bs(b"LEFT")]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"c")));
        let list = db.get_list(b"mylist").unwrap().unwrap();
        assert_eq!(list[0], Bytes::from_static(b"c"));
        assert_eq!(list[1], Bytes::from_static(b"a"));
        assert_eq!(list[2], Bytes::from_static(b"b"));
    }

    #[test]
    fn test_lmove_wrongtype_source() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
        let result = lmove(&mut db, &[bs(b"str"), bs(b"dst"), bs(b"LEFT"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"WRONGTYPE")));
    }

    #[test]
    fn test_lmove_wrongtype_destination() {
        let mut db = Database::new();
        setup_list(&mut db, b"src", &[b"a"]);
        db.set_string(Bytes::from_static(b"str"), Bytes::from_static(b"val"));
        let result = lmove(&mut db, &[bs(b"src"), bs(b"str"), bs(b"LEFT"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"WRONGTYPE")));
    }

    #[test]
    fn test_lmove_wrong_args() {
        let mut db = Database::new();
        let result = lmove(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_lmove_invalid_direction() {
        let mut db = Database::new();
        let result = lmove(&mut db, &[bs(b"src"), bs(b"dst"), bs(b"UP"), bs(b"LEFT")]);
        assert!(matches!(result, Frame::Error(ref e) if e.as_ref().starts_with(b"ERR syntax")));
    }
}
