use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{LISTPACK_MAX_ELEMENT_SIZE, LISTPACK_MAX_ENTRIES};

use super::{parse_i64, resolve_index};
use crate::command::helpers::{err_wrong_args, extract_bytes};

// ---------------------------------------------------------------------------
// LPUSH key element [element ...]
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// RPUSH key element [element ...]
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// LPOP key [count]
// ---------------------------------------------------------------------------

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
                ));
            }
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
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
    if db
        .get_list(&key)
        .ok()
        .flatten()
        .map_or(false, |l| l.is_empty())
    {
        db.remove(&key);
    }

    result
}

// ---------------------------------------------------------------------------
// RPOP key [count]
// ---------------------------------------------------------------------------

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
                ));
            }
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
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

    if db
        .get_list(&key)
        .ok()
        .flatten()
        .map_or(false, |l| l.is_empty())
    {
        db.remove(&key);
    }

    result
}

// ---------------------------------------------------------------------------
// LSET key index element
// ---------------------------------------------------------------------------

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
            ));
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

// ---------------------------------------------------------------------------
// LINSERT key BEFORE|AFTER pivot element
// ---------------------------------------------------------------------------

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
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
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
            list.insert(insert_at, element);
            Frame::Integer(list.len() as i64)
        }
    }
}

// ---------------------------------------------------------------------------
// LREM key count element
// ---------------------------------------------------------------------------

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
            ));
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

// ---------------------------------------------------------------------------
// LTRIM key start stop
// ---------------------------------------------------------------------------

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
            ));
        }
    };
    let stop = match parse_i64(&args[2]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
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

// ---------------------------------------------------------------------------
// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
// ---------------------------------------------------------------------------

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
            Ok(_) => {}         // exists as list or missing -- both OK
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
