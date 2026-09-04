use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{LISTPACK_MAX_ELEMENT_SIZE, LISTPACK_MAX_ENTRIES, list_elem_cost};

use super::{parse_i64, resolve_index};
use crate::command::helpers::{all_args_are_bytes, err_wrong_args, extract_bytes};

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

    // moon#823: refuse a non-argument-shaped frame BEFORE the mutation window
    // opens. The loop below bails on the first one `extract_bytes` rejects,
    // and by then it has already written part of the command — a partial write
    // that is applied on the master and, because propagation is gated on the
    // reply not being an error, never reaches the AOF or a replica.
    if !all_args_are_bytes(&args[1..]) {
        return err_wrong_args("LPUSH");
    }

    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        match db.get_or_create_list_listpack(key) {
            Ok(Some(lp)) => {
                // Listpack `estimate_memory()` is O(1) (capacity-based).
                let before = lp.estimate_memory();
                for arg in &args[1..] {
                    let val = match extract_bytes(arg) {
                        Some(v) => v,
                        None => return err_wrong_args("LPUSH"),
                    };
                    lp.push_front(val);
                }
                let len = lp.len();
                let after = lp.estimate_memory();
                let should_upgrade = len > LISTPACK_MAX_ENTRIES;
                // `lp`'s borrow of `db` ends here.
                if after >= before {
                    db.charge_memory(after - before);
                } else {
                    db.credit_memory(before - after);
                }
                if should_upgrade {
                    // One-time cost-model swing (listpack -> List) — see the
                    // matching comment in hash_write.rs's hset.
                    let list = db.upgrade_list_listpack_to_list(key);
                    let new_cost: usize = list.iter().map(|e| list_elem_cost(e)).sum();
                    db.credit_memory(after);
                    db.charge_memory(new_cost);
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
    let mut mem_delta: usize = 0;
    for arg in &args[1..] {
        let val = match extract_bytes(arg) {
            Some(v) => v.clone(),
            None => return err_wrong_args("LPUSH"),
        };
        mem_delta += list_elem_cost(&val);
        list.push_front(val);
    }
    let len = list.len() as i64;
    // `list`'s borrow of `db` ends above.
    db.charge_memory(mem_delta);
    Frame::Integer(len)
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

    // moon#823: refuse a non-argument-shaped frame BEFORE the mutation window
    // opens. The loop below bails on the first one `extract_bytes` rejects,
    // and by then it has already written part of the command — a partial write
    // that is applied on the master and, because propagation is gated on the
    // reply not being an error, never reaches the AOF or a replica.
    if !all_args_are_bytes(&args[1..]) {
        return err_wrong_args("RPUSH");
    }

    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        match db.get_or_create_list_listpack(key) {
            Ok(Some(lp)) => {
                let before = lp.estimate_memory();
                for arg in &args[1..] {
                    let val = match extract_bytes(arg) {
                        Some(v) => v,
                        None => return err_wrong_args("RPUSH"),
                    };
                    lp.push_back(val);
                }
                let len = lp.len();
                let after = lp.estimate_memory();
                let should_upgrade = len > LISTPACK_MAX_ENTRIES;
                // `lp`'s borrow of `db` ends here.
                if after >= before {
                    db.charge_memory(after - before);
                } else {
                    db.credit_memory(before - after);
                }
                if should_upgrade {
                    let list = db.upgrade_list_listpack_to_list(key);
                    let new_cost: usize = list.iter().map(|e| list_elem_cost(e)).sum();
                    db.credit_memory(after);
                    db.charge_memory(new_cost);
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
    let mut mem_delta: usize = 0;
    for arg in &args[1..] {
        let val = match extract_bytes(arg) {
            Some(v) => v.clone(),
            None => return err_wrong_args("RPUSH"),
        };
        mem_delta += list_elem_cost(&val);
        list.push_back(val);
    }
    let len = list.len() as i64;
    // `list`'s borrow of `db` ends above.
    db.charge_memory(mem_delta);
    Frame::Integer(len)
}

// ---------------------------------------------------------------------------
// LPOP key [count]
// ---------------------------------------------------------------------------

/// Parse the optional `count` of `LPOP`/`RPOP` (`args[1]`, when present).
///
/// Redis answers a NON-INTEGER and a NEGATIVE count with the SAME message —
/// `ERR value is out of range, must be positive` — because both fall out of
/// the one `getPositiveLongFromObject` call. Moon used to answer `ERR value is
/// not an integer or out of range` here, which clients that classify retries by
/// error string read as a different failure (moon#527).
#[inline]
fn parse_count_arg(args: &[Frame]) -> Result<Option<usize>, Frame> {
    let Some(arg) = args.get(1) else {
        return Ok(None);
    };
    match parse_i64(arg) {
        Some(c) if c >= 0 => Ok(Some(c as usize)),
        _ => Err(Frame::Error(Bytes::from_static(
            b"ERR value is out of range, must be positive",
        ))),
    }
}

/// LPOP key [count]
pub fn lpop(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("LPOP");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPOP"),
    };

    // The optional count is validated BEFORE the key lookup, exactly as Redis
    // does (`lpopGenericCommand` parses argv[2] and returns on error before it
    // reaches `lookupKeyWrite`). Ordering it the other way made an argument
    // error depend on whether the key happened to exist: `LPOP nokey abc`
    // answered a miss and `LPOP existingkey abc` an error, so the same
    // malformed command got opposite answers as keys came and went (moon#527).
    let count = match parse_count_arg(args) {
        Ok(c) => c,
        Err(e) => return e,
    };

    // Check if the key exists first (for the no-list case)
    match db.get_list(key) {
        Ok(None) => {
            // The count form's miss is a null ARRAY, not an EMPTY array: Redis
            // distinguishes "no such list" (`*-1`) from "a list that yielded
            // nothing" (`*0`). Measured, because this site was never a
            // `Frame::Null` at all and so is invisible to a null-site audit
            // (moon#482).
            return if args.len() == 2 {
                Frame::NullArray
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let mut credit: usize = 0;
    let result = match count {
        None => {
            // Single pop
            match list.pop_front() {
                Some(v) => {
                    credit = list_elem_cost(&v);
                    Frame::BulkString(v)
                }
                None => Frame::Null,
            }
        }
        Some(c) => {
            let actual = c.min(list.len());
            let mut items = Vec::with_capacity(actual);
            for _ in 0..actual {
                if let Some(v) = list.pop_front() {
                    credit += list_elem_cost(&v);
                    items.push(Frame::BulkString(v));
                }
            }
            Frame::Array(items.into())
        }
    };
    // `list`'s borrow of `db` ends above.
    db.credit_memory(credit);

    // If list is now empty, remove the key (this credits the container's
    // fixed key/struct overhead — the popped elements were already credited
    // above, so there is no double count).
    if db
        .get_list(key)
        .ok()
        .flatten()
        .map_or(false, |l| l.is_empty())
    {
        db.remove(key);
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
        Some(k) => k,
        None => return err_wrong_args("RPOP"),
    };

    // Count validated before the lookup — see `lpop` (moon#527).
    let count = match parse_count_arg(args) {
        Ok(c) => c,
        Err(e) => return e,
    };

    match db.get_list(key) {
        Ok(None) => {
            // The count form's miss is a null ARRAY, not an EMPTY array: Redis
            // distinguishes "no such list" (`*-1`) from "a list that yielded
            // nothing" (`*0`). Measured, because this site was never a
            // `Frame::Null` at all and so is invisible to a null-site audit
            // (moon#482).
            return if args.len() == 2 {
                Frame::NullArray
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let mut credit: usize = 0;
    let result = match count {
        None => match list.pop_back() {
            Some(v) => {
                credit = list_elem_cost(&v);
                Frame::BulkString(v)
            }
            None => Frame::Null,
        },
        Some(c) => {
            let actual = c.min(list.len());
            let mut items = Vec::with_capacity(actual);
            for _ in 0..actual {
                if let Some(v) = list.pop_back() {
                    credit += list_elem_cost(&v);
                    items.push(Frame::BulkString(v));
                }
            }
            Frame::Array(items.into())
        }
    };
    // `list`'s borrow of `db` ends above.
    db.credit_memory(credit);

    if db
        .get_list(key)
        .ok()
        .flatten()
        .map_or(false, |l| l.is_empty())
    {
        db.remove(key);
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
        Some(k) => k,
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

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    if list.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR no such key"));
    }

    match resolve_index(index, list.len()) {
        Some(i) => {
            let old_cost = list_elem_cost(&list[i]) as i64;
            let new_cost = list_elem_cost(&element) as i64;
            list[i] = element;
            // `list`'s borrow of `db` ends above.
            let delta = new_cost - old_cost;
            if delta >= 0 {
                db.charge_memory(delta as usize);
            } else {
                db.credit_memory((-delta) as usize);
            }
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
        Some(k) => k,
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
    match db.get_list(key) {
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    // Find pivot
    let pos = list.iter().position(|v| v == &pivot);
    match pos {
        None => Frame::Integer(-1),
        Some(idx) => {
            let insert_at = if before { idx } else { idx + 1 };
            let cost = list_elem_cost(&element);
            list.insert(insert_at, element);
            let len = list.len() as i64;
            // `list`'s borrow of `db` ends above.
            db.charge_memory(cost);
            Frame::Integer(len)
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
        Some(k) => k,
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

    match db.get_list(key) {
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(key) {
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

    let is_empty = list.is_empty();
    // `list`'s borrow of `db` ends above. Every removed element compared equal
    // to `element` (LREM semantics), so a single per-element cost applies to
    // all of them — O(1), no need to track each removed value individually.
    if removed > 0 {
        db.credit_memory(removed as usize * list_elem_cost(&element));
    }

    // If list is now empty, remove the key
    if is_empty {
        db.remove(key);
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
        Some(k) => k,
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

    match db.get_list(key) {
        Ok(None) => return Frame::SimpleString(Bytes::from_static(b"OK")),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(key) {
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

    let mut credit: usize = 0;
    if s > e || s >= len {
        // Empty range -- clear the list. Credit every dropped element's cost
        // (proportional to what's removed, same as the drain paths below).
        credit = list.iter().map(|v| list_elem_cost(v)).sum();
        list.clear();
    } else {
        // Keep only [s..=e]
        let s = s as usize;
        let e = e as usize;
        // Drain from the back first, then from the front
        if e + 1 < list.len() {
            credit += list
                .drain(e + 1..)
                .map(|v| list_elem_cost(&v))
                .sum::<usize>();
        }
        if s > 0 {
            credit += list.drain(..s).map(|v| list_elem_cost(&v)).sum::<usize>();
        }
    }
    let is_empty = list.is_empty();
    // `list`'s borrow of `db` ends above.
    db.credit_memory(credit);

    if is_empty {
        db.remove(key);
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
    lmove_inner(db, source, destination, wherefrom, whereto)
}

/// RPOPLPUSH source destination
///
/// Deprecated in Redis in favour of `LMOVE source destination RIGHT LEFT`, but
/// NOT removed: it is the form baked into a decade of client code and every
/// reliable-queue tutorial, and `redis-py`, `jedis`, `go-redis` and
/// `node-redis` all expose it as a first-class method. Moon had it in the
/// routing table, in the metrics labels, and as the internal rewrite target of
/// `BRPOPLPUSH` — everywhere except the two places a client reaches, so
/// `r.rpoplpush(...)` got "unknown command" (moon#520).
///
/// Implemented by DELEGATING to `lmove_inner`, not by re-deriving the list
/// logic: identical replies, identical memory accounting, identical
/// WRONGTYPE-before-pop ordering, and no second copy to drift.
pub fn rpoplpush(db: &mut Database, args: &[Frame]) -> Frame {
    // Lowercase in the arity message because that is what Redis emits
    // (`ERR wrong number of arguments for 'rpoplpush' command`) and what the
    // `BRPOPLPUSH` arity error in `server/conn/blocking.rs` already says. Much
    // of the rest of this codebase passes the UPPERCASE name here and so
    // diverges from Redis — a pre-existing, systemic difference that is not
    // this change's to fix, but is also not one a NEW command should inherit.
    if args.len() != 2 {
        return err_wrong_args("rpoplpush");
    }
    let source = match extract_bytes(&args[0]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("rpoplpush"),
    };
    let destination = match extract_bytes(&args[1]) {
        Some(k) => k.clone(),
        None => return err_wrong_args("rpoplpush"),
    };
    lmove_inner(
        db,
        source,
        destination,
        crate::blocking::Direction::Right,
        crate::blocking::Direction::Left,
    )
}

/// The shared body of `LMOVE` and `RPOPLPUSH`, after argument parsing.
fn lmove_inner(
    db: &mut Database,
    source: Bytes,
    destination: Bytes,
    wherefrom: crate::blocking::Direction,
    whereto: crate::blocking::Direction,
) -> Frame {
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

// ---------------------------------------------------------------------------
// LPUSHX key element [element ...]
// ---------------------------------------------------------------------------

/// LPUSHX key element [element ...]
/// Pushes elements to the front of the list ONLY if the key already exists as a list.
pub fn lpushx(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("LPUSHX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPUSHX"),
    };

    // moon#823: refuse a non-argument-shaped frame BEFORE the mutation window
    // opens. The loop below bails on the first one `extract_bytes` rejects,
    // and by then it has already written part of the command — a partial write
    // that is applied on the master and, because propagation is gated on the
    // reply not being an error, never reaches the AOF or a replica.
    if !all_args_are_bytes(&args[1..]) {
        return err_wrong_args("LPUSHX");
    }

    match db.get_list(key) {
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    let mut mem_delta: usize = 0;
    for arg in &args[1..] {
        let val = match extract_bytes(arg) {
            Some(v) => v.clone(),
            None => return err_wrong_args("LPUSHX"),
        };
        mem_delta += list_elem_cost(&val);
        list.push_front(val);
    }
    let len = list.len() as i64;
    // `list`'s borrow of `db` ends above.
    db.charge_memory(mem_delta);
    Frame::Integer(len)
}

// ---------------------------------------------------------------------------
// RPUSHX key element [element ...]
// ---------------------------------------------------------------------------

/// RPUSHX key element [element ...]
/// Pushes elements to the back of the list ONLY if the key already exists as a list.
pub fn rpushx(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("RPUSHX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("RPUSHX"),
    };

    // moon#823: refuse a non-argument-shaped frame BEFORE the mutation window
    // opens. The loop below bails on the first one `extract_bytes` rejects,
    // and by then it has already written part of the command — a partial write
    // that is applied on the master and, because propagation is gated on the
    // reply not being an error, never reaches the AOF or a replica.
    if !all_args_are_bytes(&args[1..]) {
        return err_wrong_args("RPUSHX");
    }

    match db.get_list(key) {
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }

    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    let mut mem_delta: usize = 0;
    for arg in &args[1..] {
        let val = match extract_bytes(arg) {
            Some(v) => v.clone(),
            None => return err_wrong_args("RPUSHX"),
        };
        mem_delta += list_elem_cost(&val);
        list.push_back(val);
    }
    let len = list.len() as i64;
    // `list`'s borrow of `db` ends above.
    db.charge_memory(mem_delta);
    Frame::Integer(len)
}

// ---------------------------------------------------------------------------
// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
// ---------------------------------------------------------------------------

/// LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
/// Pops elements from the first non-empty list among the specified keys.
pub fn lmpop(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 {
        return err_wrong_args("LMPOP");
    }
    let numkeys = match parse_i64(&args[0]) {
        Some(n) if n > 0 => n as usize,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR numkeys can't be non-positive value",
            ));
        }
    };
    if args.len() < 1 + numkeys + 1 {
        return err_wrong_args("LMPOP");
    }

    let dir_bytes = match extract_bytes(&args[1 + numkeys]) {
        Some(b) => b,
        None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
    };
    let left = if dir_bytes.eq_ignore_ascii_case(b"LEFT") {
        true
    } else if dir_bytes.eq_ignore_ascii_case(b"RIGHT") {
        false
    } else {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    };

    let mut count: usize = 1;
    let remaining = &args[2 + numkeys..];
    if remaining.len() >= 2 {
        if let Some(kw) = extract_bytes(&remaining[0]) {
            if kw.eq_ignore_ascii_case(b"COUNT") {
                match parse_i64(&remaining[1]) {
                    Some(c) if c > 0 => count = c as usize,
                    _ => {
                        return Frame::Error(Bytes::from_static(
                            b"ERR COUNT value of LMPOP command is not an integer or out of range",
                        ));
                    }
                }
            } else {
                return Frame::Error(Bytes::from_static(b"ERR syntax error"));
            }
        }
    } else if !remaining.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    }

    for i in 0..numkeys {
        let key = match extract_bytes(&args[1 + i]) {
            Some(k) => k.clone(),
            None => return err_wrong_args("LMPOP"),
        };

        let list_len = match db.get_list(&key) {
            Ok(Some(l)) => l.len(),
            Ok(None) => continue,
            Err(e) => return e,
        };
        if list_len == 0 {
            continue;
        }

        let n = count.min(list_len);
        let list = match db.get_or_create_list(&key) {
            Ok(l) => l,
            Err(e) => return e,
        };
        let mut elems = Vec::with_capacity(n);
        let mut credit: usize = 0;
        for _ in 0..n {
            let val = if left {
                list.pop_front()
            } else {
                list.pop_back()
            };
            match val {
                Some(v) => {
                    credit += list_elem_cost(&v);
                    elems.push(Frame::BulkString(v));
                }
                None => break,
            }
        }
        let is_empty = list.is_empty();
        // `list`'s borrow of `db` ends above.
        db.credit_memory(credit);

        if is_empty {
            db.remove(&key);
        }

        if elems.is_empty() {
            continue;
        }
        return Frame::Array(framevec![
            Frame::BulkString(key),
            Frame::Array(elems.into()),
        ]);
    }

    // No key held anything: LMPOP's miss is a null ARRAY (moon#482).
    Frame::NullArray
}
