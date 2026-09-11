use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{ListRef, Shape, list_elem_cost};

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

    // The entry gate, from the ONE authority (moon#896): the longest element
    // in the batch against the element threshold, the batch size against the
    // list entry threshold. The same predicate bounds a batch far below the
    // listpack header's u16 range (moon#865) — the upgrade check below runs
    // after the push loop, too late to stop a batch already too big.
    let limits = db.encoding_limits();
    let max_elem = args[1..]
        .iter()
        .map(|a| extract_bytes(a).map_or(0, |b| b.len()))
        .max()
        .unwrap_or(0);
    if limits.fits(Shape::List, args.len() - 1, max_elem) {
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
                // The upgrade check, from the same authority as the gate.
                let should_upgrade = !limits.listpack_fits(Shape::List, lp);
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

    // The entry gate, from the ONE authority (moon#896): the longest element
    // in the batch against the element threshold, the batch size against the
    // list entry threshold. The same predicate bounds a batch far below the
    // listpack header's u16 range (moon#865) — the upgrade check below runs
    // after the push loop, too late to stop a batch already too big.
    let limits = db.encoding_limits();
    let max_elem = args[1..]
        .iter()
        .map(|a| extract_bytes(a).map_or(0, |b| b.len()))
        .max()
        .unwrap_or(0);
    if limits.fits(Shape::List, args.len() - 1, max_elem) {
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
                // The upgrade check, from the same authority as the gate.
                let should_upgrade = !limits.listpack_fits(Shape::List, lp);
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

// ---------------------------------------------------------------------------
// Encoding-preserving routing for the list secondary writes (moon#897)
// ---------------------------------------------------------------------------

/// Which encoding the key holds RIGHT NOW, as answered by a `&self` probe that
/// cannot rewrite it.
///
/// `Copy` and field-free on purpose: it is the whole of what a `ListRef`
/// borrow tells the router, so the borrow of `db` ends at the `match` that
/// produces one and the mutation below is free to take `&mut db`.
#[derive(Clone, Copy)]
enum ListRoute {
    /// A live `ListListpack` — mutate it in place.
    Listpack,
    /// The full `VecDeque`, or a cold-spilled value: the pre-existing eager
    /// path, byte for byte.
    Full,
}

/// Ask what `key` holds without rewriting it. `Ok(None)` = no such (live) key.
///
/// `get_list_ref_if_alive` takes `&self`, so — unlike `get_or_create_list` and
/// `get_list` (= `get_promoted`) — asking this question cannot itself flatten
/// the compact encoding (moon#832). This matters twice over here: the old
/// shape asked `db.get_list(key)` purely to decide "does the key exist?", so
/// the MISS CHECK was a flattener before the pop had even started.
fn list_route(db: &Database, key: &[u8]) -> Result<Option<ListRoute>, Frame> {
    match db.get_list_ref_if_alive(key, db.now_ms()) {
        Ok(None) => Ok(None),
        Ok(Some(ListRef::Listpack(_))) => Ok(Some(ListRoute::Listpack)),
        Ok(Some(ListRef::Deque(_) | ListRef::Owned(_))) => Ok(Some(ListRoute::Full)),
        Err(e) => Err(e),
    }
}

/// Promote a `ListListpack` to the full `VecDeque` and settle the one-time
/// cost-model swing, `after` being the listpack's last billed size.
///
/// Exactly the block `LPUSH`/`RPUSH` run when their push loop overflows the
/// policy; factored out so the four sites that can now cross a threshold
/// cannot drift apart the way the three consultation sites did in moon#896.
fn promote_list_listpack(db: &mut Database, key: &[u8], after: usize) {
    let list = db.upgrade_list_listpack_to_list(key);
    let new_cost: usize = list.iter().map(|e| list_elem_cost(e)).sum();
    db.credit_memory(after);
    db.charge_memory(new_cost);
}

/// Remove and return one end of a listpack, without decoding anything else.
///
/// `get_at` walks borrowed and materialises only the entry it returns;
/// `remove_at` discards its own without decoding it. A stored `Integer` renders
/// through the canonical spelling it was admitted under, so the bytes handed
/// back are the bytes the client wrote (moon#795/#903).
#[inline]
fn listpack_pop_end(lp: &mut crate::storage::listpack::Listpack, front: bool) -> Option<Bytes> {
    let idx = if front { 0 } else { lp.len().checked_sub(1)? };
    let value = lp.get_at(idx)?.to_bytes();
    lp.remove_at(idx);
    Some(value)
}

/// The shared body of `LPOP` and `RPOP`.
///
/// # Encoding (moon#897)
///
/// One `LPOP` of a three-element list used to report `linkedlist` where redis
/// reports `listpack`, permanently — nothing demotes (moon#832). `LPOP` is the
/// queue primitive, so a small work queue flattened on its first pop and never
/// came back. Two separate sites did it: the `db.get_list(key)` existence
/// probe, and `get_or_create_list` for the pop itself. Both are gone from the
/// compact path.
fn pop_generic(db: &mut Database, args: &[Frame], name: &'static str, front: bool) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args(name);
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args(name),
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

    match list_route(db, key) {
        Err(e) => e,
        Ok(None) => {
            // The count form's miss is a null ARRAY, not an EMPTY array: Redis
            // distinguishes "no such list" (`*-1`) from "a list that yielded
            // nothing" (`*0`). Measured, because this site was never a
            // `Frame::Null` at all and so is invisible to a null-site audit
            // (moon#482).
            if args.len() == 2 {
                Frame::NullArray
            } else {
                Frame::Null
            }
        }
        Ok(Some(ListRoute::Listpack)) => pop_listpack(db, key, count, front),
        Ok(Some(ListRoute::Full)) => pop_eager(db, key, count, front),
    }
}

/// Pop from a `ListListpack` in place, keeping the listpack.
fn pop_listpack(db: &mut Database, key: &Bytes, count: Option<usize>, front: bool) -> Frame {
    let limits = db.encoding_limits();
    let lp = match db.get_or_create_list_listpack(key) {
        Ok(Some(lp)) => lp,
        // The probe said listpack; anything else means the value changed
        // between the probe and here, which one shard thread cannot do. Fall
        // back to the eager path rather than assume.
        Ok(None) => return pop_eager(db, key, count, front),
        Err(e) => return e,
    };
    // Listpack `estimate_memory()` is O(1) (capacity-based).
    let before = lp.estimate_memory();
    let result = match count {
        None => match listpack_pop_end(lp, front) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        Some(c) => {
            let actual = c.min(lp.len());
            let mut items = Vec::with_capacity(actual);
            for _ in 0..actual {
                if let Some(v) = listpack_pop_end(lp, front) {
                    items.push(Frame::BulkString(v));
                }
            }
            Frame::Array(items.into())
        }
    };
    let after = lp.estimate_memory();
    let empty = lp.is_empty();
    // The upgrade check, from the ONE authority (moon#896) and the same
    // predicate the push loops use. A pop only shrinks, so this is false for
    // every listpack a push could have produced; it fires only for one already
    // past the policy, which is the container that belongs in the full form.
    let should_upgrade = !limits.listpack_fits(Shape::List, lp);
    // `lp`'s borrow of `db` ends here.
    db.adjust_memory(before, after);
    if empty {
        // Whole-key removal recomputes the entry cost, so the popped elements
        // need no separate credit.
        db.remove(key);
    } else if should_upgrade {
        promote_list_listpack(db, key, after);
    }
    result
}

/// The pre-moon#897 path: materialise the full `VecDeque` and pop from it.
///
/// Reached for a list that is ALREADY a `linkedlist` and for a cold-spilled
/// value (which `get_or_create_list` promotes back) — i.e. where there is no
/// compact encoding to preserve, so the routing changes nothing.
fn pop_eager(db: &mut Database, key: &Bytes, count: Option<usize>, front: bool) -> Frame {
    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let mut credit: usize = 0;
    let result = match count {
        None => {
            let popped = if front {
                list.pop_front()
            } else {
                list.pop_back()
            };
            match popped {
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
                let popped = if front {
                    list.pop_front()
                } else {
                    list.pop_back()
                };
                if let Some(v) = popped {
                    credit += list_elem_cost(&v);
                    items.push(Frame::BulkString(v));
                }
            }
            Frame::Array(items.into())
        }
    };
    // `list`'s borrow of `db` ends above.
    db.credit_memory(credit);

    // If the list is now empty, remove the key (this credits the container's
    // fixed key/struct overhead — the popped elements were already credited
    // above, so there is no double count). The emptiness probe goes through
    // the `&self` accessor: `get_list` is `get_promoted`, and using it here
    // would re-flatten whatever the pop had just preserved (moon#832).
    let now_ms = db.now_ms();
    let empty = matches!(db.get_list_ref_if_alive(key, now_ms), Ok(Some(l)) if l.is_empty());
    if empty {
        db.remove(key);
    }

    result
}

// ---------------------------------------------------------------------------
// LPOP key [count]
// ---------------------------------------------------------------------------

/// LPOP key [count]
pub fn lpop(db: &mut Database, args: &[Frame]) -> Frame {
    pop_generic(db, args, "LPOP", true)
}

// ---------------------------------------------------------------------------
// RPOP key [count]
// ---------------------------------------------------------------------------

/// RPOP key [count]
///
/// The same state writer as `LPOP` from the other end — it took the same two
/// flattening accessors and gets the same routing. Fixing only the end named
/// in moon#897 would have left `RPOP mylist` turning a listpack into a
/// linkedlist.
pub fn rpop(db: &mut Database, args: &[Frame]) -> Frame {
    pop_generic(db, args, "RPOP", false)
}
// ---------------------------------------------------------------------------
// LSET key index element
// ---------------------------------------------------------------------------

const ERR_NO_SUCH_KEY: &[u8] = b"ERR no such key";
const ERR_INDEX_OUT_OF_RANGE: &[u8] = b"ERR index out of range";

/// LSET key index element
///
/// # Encoding (moon#897)
///
/// One `LSET` of a three-element list used to report `linkedlist` where redis
/// reports `listpack`, permanently — nothing demotes (moon#832). The mechanism
/// was `get_or_create_list`, whose `ListKind::upgrade` materialises the full
/// `VecDeque` unconditionally.
///
/// `LSET` is the one list secondary write that can legitimately cross a
/// threshold: the count is unchanged, but the REPLACEMENT element may be
/// longer than the value limit. That decision goes to the ONE authority
/// (moon#896) rather than to a constant here. A 65-byte element therefore
/// still promotes — matching what moon's own `RPUSH` gate would have done with
/// it, and diverging from redis only because moon's list policy is a 64-byte
/// element limit where `list-max-listpack-size -2` is an 8 KB node budget.
/// That policy gap is pre-existing and out of scope for this change; keeping
/// `LSET` consistent with `RPUSH` is what stops a listpack from holding an
/// element the restart-side re-derivation would refuse.
///
/// # moon#830
///
/// This function used to call `get_or_create_list` BEFORE it could answer
/// "no such key", so `LSET missing 0 v` replied `ERR no such key` and left a
/// charged, `DBSIZE`-visible, never-propagated empty list behind. The
/// non-creating `&self` router the encoding fix needs answers that question
/// without the create half, so the key is no longer fabricated. Verified
/// against redis 8.6.1: `EXISTS`, `TYPE` and `DBSIZE` now all match.
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

    match list_route(db, key) {
        Err(e) => e,
        Ok(None) => Frame::Error(Bytes::from_static(ERR_NO_SUCH_KEY)),
        Ok(Some(ListRoute::Listpack)) => lset_listpack(db, key, index, element),
        Ok(Some(ListRoute::Full)) => lset_eager(db, key, index, element),
    }
}

/// `LSET` against a `ListListpack`: replace in place when the result still
/// fits the policy, promote and fall through when it does not.
fn lset_listpack(db: &mut Database, key: &Bytes, index: i64, element: Bytes) -> Frame {
    let limits = db.encoding_limits();
    let lp = match db.get_or_create_list_listpack(key) {
        Ok(Some(lp)) => lp,
        Ok(None) => return lset_eager(db, key, index, element),
        Err(e) => return e,
    };
    if lp.is_empty() {
        // An empty container is not reachable through the router (an emptied
        // list is deleted with its key), but the answer redis gives for one is
        // "no such key", not "index out of range".
        return Frame::Error(Bytes::from_static(ERR_NO_SUCH_KEY));
    }
    let Some(i) = resolve_index(index, lp.len()) else {
        return Frame::Error(Bytes::from_static(ERR_INDEX_OUT_OF_RANGE));
    };
    // THE consultation: does the container still fit with this element in it?
    // The item count is unchanged by a replacement, so only `element.len()`
    // can move the verdict — but passing the real count keeps the call honest
    // for a container that was already over.
    let fits = limits.fits(Shape::List, lp.len(), element.len());
    if !fits {
        // `lp`'s borrow of `db` ends here; promote, then take the eager path,
        // which does the replacement and its own accounting.
        let after = lp.estimate_memory();
        promote_list_listpack(db, key, after);
        return lset_eager(db, key, index, element);
    }
    // Listpack `estimate_memory()` is O(1) (capacity-based). `replace_at`
    // discards the old entry without decoding it, and encodes the new one
    // under the canonical-integer rule, so a numeric-looking element goes in
    // and comes back out with its exact bytes (moon#795/#903).
    let before = lp.estimate_memory();
    lp.replace_at(i, &element);
    let after = lp.estimate_memory();
    // `lp`'s borrow of `db` ends here.
    db.adjust_memory(before, after);
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// The pre-moon#897 path: materialise the full `VecDeque` and index into it.
fn lset_eager(db: &mut Database, key: &Bytes, index: i64, element: Bytes) -> Frame {
    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    if list.is_empty() {
        return Frame::Error(Bytes::from_static(ERR_NO_SUCH_KEY));
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
        None => Frame::Error(Bytes::from_static(ERR_INDEX_OUT_OF_RANGE)),
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
