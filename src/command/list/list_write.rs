use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{ListRef, Shape, list_elem_cost};
use crate::storage::owned_bytes::detach;

use super::list_compact::lrem_deque;
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
        // moon#1160: an exact-size copy, never a slice of the request buffer.
        let val = match extract_bytes(arg) {
            Some(v) => detach(v),
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
        // moon#1160: an exact-size copy, never a slice of the request buffer.
        let val = match extract_bytes(arg) {
            Some(v) => detach(v),
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
///
/// A probe, not the command's access (moon#1221 review INTEG-5): every route
/// it answers ends in a write accessor that records the one access redis's
/// `lookupKeyWrite` would, so the probe is `LOOKUP_NOTOUCH`.
/// moon#1225: an indexed-but-unreadable cold key is `Err(-IOERR)`, never absent.
fn list_route(db: &Database, key: &[u8]) -> Result<Option<ListRoute>, Frame> {
    match db.peek_list_ref_if_alive(key, db.now_ms()) {
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

/// Remove and return one end of a listpack, materialising the popped element
/// exactly ONCE and decoding nothing else.
///
/// The walk to the element is borrowed either way — `iter_refs` and `seek_to`
/// share `decode_entry_ref_at`, so nothing stepped over is ever materialised —
/// and `remove_at` discards its own entry without decoding it. What changed
/// (moon#942) is the element the pop actually keeps: `Listpack::get_at`
/// returns the OWNING `ListpackEntry`, whose string arm is a fresh `Vec`, and
/// `ListpackEntry::to_bytes` then goes through `as_bytes`, which CLONES that
/// `Vec` — two heap allocations, the first dropped having been copied and
/// never read. `ListpackRef` borrows instead, so the only copy is the one the
/// reply genuinely needs: it owns its bytes and `remove_at` mutates the buffer
/// out from under them on the next line.
///
/// One allocation is the floor, not zero, and
/// `tests/list_pop_alloc_942.rs` pins it there against a counting
/// `GlobalAlloc` — with a full-encoding pop (a `Bytes` move, zero) as the
/// control that says the surplus belonged to the decode and not to the reply.
///
/// A stored `Integer` renders through the canonical decimal spelling it was
/// admitted under, so the bytes handed back are the bytes the client wrote
/// (moon#795/#903). `itoa` here and `i64::to_string` in `ListpackEntry`
/// produce the same bytes for every `i64`; `a_listpack_pop_returns_the_exact_bytes_that_were_pushed`
/// asks both ends for `007`, `+7`, `-0` and both limits.
///
/// moon#1174 §2: the BACK used to be reached twice from the head
/// (`iter_refs().nth(len - 1)` to read it, `remove_at(len - 1)` to seek to it
/// again). `Listpack::pop_end` steps back over one backlen instead.
#[inline]
fn listpack_pop_end(lp: &mut crate::storage::listpack::Listpack, front: bool) -> Option<Bytes> {
    lp.pop_end(front)
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
    // Whether the pop emptied the list, read from the handle that did the
    // popping. moon#942: this used to be a FOURTH DashTable probe — the
    // borrow was dropped and `get_list_ref_if_alive` was asked to look the
    // key up again to answer a question the `&mut VecDeque` had in hand. The
    // `&self` accessor was the right choice for the question (`get_list` is
    // `get_promoted` and would re-flatten whatever the pop preserved,
    // moon#832); not asking it at all is better still, and identical — one
    // shard thread owns this keyspace and nothing between here and there can
    // change the list's length.
    let empty = list.is_empty();
    // `list`'s borrow of `db` ends above.
    db.credit_memory(credit);

    // If the list is now empty, remove the key (this credits the container's
    // fixed key/struct overhead — the popped elements were already credited
    // above, so there is no double count).
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
            // moon#1160: stored as an exact-size copy of the request's bytes.
            list[i] = detach(&element);
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
///
/// # moon#1174 §1
///
/// Gated with `db.get_list` then `get_or_create_list` -- four probes and an
/// unconditional listpack flatten, so one `LINSERT` permanently turned a small
/// list into a `linkedlist`. The gate is now the one-probe `&self`
/// [`list_route`], and a listpack takes the insert in place.
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
        Some(v) => v.as_ref(),
        None => return err_wrong_args("LINSERT"),
    };
    let element = match extract_bytes(&args[3]) {
        Some(v) => v,
        None => return err_wrong_args("LINSERT"),
    };

    let before = if position.eq_ignore_ascii_case(b"BEFORE") {
        true
    } else if position.eq_ignore_ascii_case(b"AFTER") {
        false
    } else {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    };

    // A missing key answers 0 and is not created.
    match list_route(db, key) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(ListRoute::Listpack)) => linsert_listpack(db, key, pivot, element, before),
        Ok(Some(ListRoute::Full)) => linsert_eager(db, key, pivot, element, before),
    }
}

/// `LINSERT` against a `ListListpack`: insert in place when the result still
/// fits the policy, promote and take the eager path when it does not.
///
/// A missing pivot is `-1` and changes NOTHING, the encoding included: redis
/// 7.2+ converts before it searches, but moon's list policy (128 elements /
/// 64 B) is not redis's 8 KB node budget, so exact encoding parity for an
/// over-limit element is unreachable either way, and promoting on a no-op is
/// the one choice that changes state for no reply-visible reason.
fn linsert_listpack(
    db: &mut Database,
    key: &Bytes,
    pivot: &[u8],
    element: &Bytes,
    before: bool,
) -> Frame {
    let limits = db.encoding_limits();
    let lp = match db.get_or_create_list_listpack(key) {
        Ok(Some(lp)) => lp,
        Ok(None) => return linsert_eager(db, key, pivot, element, before),
        Err(e) => return e,
    };
    // THE consultation, from the one authority (moon#896): would the list
    // still fit with one more element of this length in it?
    if !limits.fits(Shape::List, lp.len() + 1, element.len()) {
        if lp.find(pivot).is_none() {
            return Frame::Integer(-1);
        }
        let after = lp.estimate_memory();
        promote_list_listpack(db, key, after);
        return linsert_eager(db, key, pivot, element, before);
    }
    let before_bytes = lp.estimate_memory();
    if !lp.insert_relative(pivot, element, before) {
        return Frame::Integer(-1);
    }
    let len = lp.len();
    let after = lp.estimate_memory();
    // `lp`'s borrow of `db` ends here.
    db.adjust_memory(before_bytes, after);
    Frame::Integer(len as i64)
}

/// `LINSERT` against the full `VecDeque` (or a cold value, which
/// `get_or_create_list` promotes back).
fn linsert_eager(
    db: &mut Database,
    key: &Bytes,
    pivot: &[u8],
    element: &Bytes,
    before: bool,
) -> Frame {
    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    match list.iter().position(|v| v.as_ref() == pivot) {
        None => Frame::Integer(-1),
        Some(idx) => {
            let insert_at = if before { idx } else { idx + 1 };
            let cost = list_elem_cost(element);
            // moon#1160: an exact-size copy, never a slice of the request buffer.
            list.insert(insert_at, detach(element));
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
///
/// # moon#1173
///
/// Removed matches one at a time with `VecDeque::remove`, each shifting up to
/// half the list: O(N*K). `LREM l 0 a` on a 200K-element list with 100K
/// matches held the shard for 13.1 s where redis takes 0.30 s. Both encodings
/// are now ONE compaction pass bounded by `count` -- [`lrem_deque`] with read
/// and write cursors, `Listpack::remove_matches` on the byte buffer.
///
/// # moon#1174 §1
///
/// The existence/WRONGTYPE gate was `db.get_list` (= `get_promoted`, two
/// probes and an unconditional listpack flatten) followed by
/// `get_or_create_list` (two more probes, flatten again): the command
/// permanently converted every small list it touched. The gate is now the
/// one-probe `&self` [`list_route`], and a listpack stays a listpack.
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
        Some(v) => v.as_ref(),
        None => return err_wrong_args("LREM"),
    };
    // count == 0 removes every match; otherwise |count| of them, from the head
    // for a positive count and from the tail for a negative one.
    let max_remove = if count == 0 {
        usize::MAX
    } else {
        usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX)
    };
    let from_tail = count < 0;

    match list_route(db, key) {
        Err(e) => e,
        Ok(None) => Frame::Integer(0),
        Ok(Some(ListRoute::Listpack)) => lrem_listpack(db, key, element, from_tail, max_remove),
        Ok(Some(ListRoute::Full)) => lrem_eager(db, key, element, from_tail, max_remove),
    }
}

/// `LREM` against a `ListListpack`, in place.
fn lrem_listpack(
    db: &mut Database,
    key: &Bytes,
    element: &[u8],
    from_tail: bool,
    max_remove: usize,
) -> Frame {
    let limits = db.encoding_limits();
    let lp = match db.get_or_create_list_listpack(key) {
        Ok(Some(lp)) => lp,
        // The probe said listpack; see `pop_listpack` for why this falls back.
        Ok(None) => return lrem_eager(db, key, element, from_tail, max_remove),
        Err(e) => return e,
    };
    let before = lp.estimate_memory();
    let removed = lp.remove_matches(element, from_tail, max_remove);
    if removed > 0 {
        lp.shrink_if_sparse();
    }
    let after = lp.estimate_memory();
    let empty = lp.is_empty();
    // Removal only shrinks, so this is false for every listpack a push could
    // have produced -- the same post-mutation check the pops run (moon#896).
    let should_upgrade = !limits.listpack_fits(Shape::List, lp);
    // `lp`'s borrow of `db` ends here.
    db.adjust_memory(before, after);
    if empty {
        db.remove(key);
    } else if should_upgrade {
        promote_list_listpack(db, key, after);
    }
    Frame::Integer(removed as i64)
}

/// `LREM` against the full `VecDeque` (or a cold value, which
/// `get_or_create_list` promotes back).
fn lrem_eager(
    db: &mut Database,
    key: &Bytes,
    element: &[u8],
    from_tail: bool,
    max_remove: usize,
) -> Frame {
    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    let removed = lrem_deque(list, element, from_tail, max_remove);
    let is_empty = list.is_empty();
    // `list`'s borrow of `db` ends above. Every removed element compared equal
    // to `element` (LREM semantics), so a single per-element cost applies to
    // all of them — O(1), no need to track each removed value individually.
    if removed > 0 {
        db.credit_memory(removed * list_elem_cost(element));
    }
    if is_empty {
        db.remove(key);
    }
    Frame::Integer(removed as i64)
}

// ---------------------------------------------------------------------------
// LTRIM key start stop
// ---------------------------------------------------------------------------

/// LTRIM key start stop
///
/// # moon#1174 §1
///
/// `LPUSH k x; LTRIM k 0 99` -- the capped recent-items list, probably the
/// most common list idiom there is -- flattened the listpack on its FIRST
/// trim: the gate was `db.get_list` + `get_or_create_list`, whose
/// `ListKind::upgrade` is unconditional and one-way, and every later `LPUSH`
/// then stayed on the `VecDeque` for the key's lifetime (~2x the memory of a
/// 100 x 20 B listpack). The gate is now the one-probe `&self` [`list_route`]
/// and a listpack is trimmed in place, in one move of its kept bytes.
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

    match list_route(db, key) {
        Err(e) => e,
        Ok(None) => Frame::SimpleString(Bytes::from_static(b"OK")),
        Ok(Some(ListRoute::Listpack)) => ltrim_listpack(db, key, start, stop),
        Ok(Some(ListRoute::Full)) => ltrim_eager(db, key, start, stop),
    }
}

/// Resolve `LTRIM`'s `start`/`stop` against `len`: the inclusive window to
/// keep, or `None` when it is empty (the whole list goes).
fn trim_window(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len = len as i64;
    let s = if start < 0 { len + start } else { start }.max(0);
    let e = if stop < 0 { len + stop } else { stop }.min(len - 1);
    if s > e || s >= len {
        None
    } else {
        Some((s as usize, e as usize))
    }
}

/// `LTRIM` against a `ListListpack`, in place.
fn ltrim_listpack(db: &mut Database, key: &Bytes, start: i64, stop: i64) -> Frame {
    let limits = db.encoding_limits();
    let lp = match db.get_or_create_list_listpack(key) {
        Ok(Some(lp)) => lp,
        Ok(None) => return ltrim_eager(db, key, start, stop),
        Err(e) => return e,
    };
    let Some((s, e)) = trim_window(start, stop, lp.len()) else {
        // Everything goes. Whole-key removal credits the entry as it stands,
        // listpack included, so nothing needs trimming first.
        db.remove(key);
        return Frame::SimpleString(Bytes::from_static(b"OK"));
    };
    let before = lp.estimate_memory();
    if s > 0 || e + 1 < lp.len() {
        lp.retain_range(s, e);
        lp.shrink_if_sparse();
    }
    let after = lp.estimate_memory();
    // Trimming only shrinks; this is the pops' post-mutation check (moon#896).
    let should_upgrade = !limits.listpack_fits(Shape::List, lp);
    // `lp`'s borrow of `db` ends here.
    db.adjust_memory(before, after);
    if should_upgrade {
        promote_list_listpack(db, key, after);
    }
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// `LTRIM` against the full `VecDeque` (or a cold value, which
/// `get_or_create_list` promotes back).
fn ltrim_eager(db: &mut Database, key: &Bytes, start: i64, stop: i64) -> Frame {
    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };

    let mut credit: usize = 0;
    match trim_window(start, stop, list.len()) {
        None => {
            // Empty range -- clear the list. Credit every dropped element's
            // cost (proportional to what's removed, same as the drains below).
            credit = list.iter().map(|v| list_elem_cost(v)).sum();
            list.clear();
        }
        Some((s, e)) => {
            // Keep only [s..=e]. Drain from the back first, then the front.
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
///
/// # moon#1174 §1
///
/// The type checks were `db.get_list` on BOTH keys and the move went through
/// `Database::list_pop_*` / `list_push_*`: about seven probes where redis does
/// two, and a listpack flatten at every one of the four accessors -- the
/// reliable-queue pattern (`RPOPLPUSH q processing`) turned both lists into
/// `linkedlist`s on its first move. Each key is now routed once through the
/// `&self` [`list_route`], a listpack is popped and pushed in place exactly as
/// `LPOP`/`LPUSH` do it, and a destination that does not exist yet is born a
/// listpack when the element fits, as `LPUSH` would make it.
///
/// The redis ORDER of refusals is kept: a missing source answers nil before
/// the destination is looked at; a wrong-typed source, then a wrong-typed
/// destination, answer WRONGTYPE before anything is popped.
///
/// # Rotation (`source == destination`)
///
/// Rotates IN PLACE. The old shape popped -- deleting the key when that was
/// its only element, and with it the key's TTL -- then pushed into a freshly
/// created key: `RPUSH k a; EXPIRE k 100; LMOVE k k LEFT RIGHT; TTL k`
/// answered -1 where redis answers 100 (measured against 7.0.15).
fn lmove_inner(
    db: &mut Database,
    source: Bytes,
    destination: Bytes,
    wherefrom: crate::blocking::Direction,
    whereto: crate::blocking::Direction,
) -> Frame {
    use crate::blocking::Direction;
    let pop_front = wherefrom == Direction::Left;
    let push_front = whereto == Direction::Left;

    let src_route = match list_route(db, &source) {
        Err(e) => return e,
        Ok(None) => return Frame::Null,
        Ok(Some(route)) => route,
    };
    if source == destination {
        return lmove_rotate(db, &source, src_route, pop_front, push_front);
    }
    if let Err(e) = list_route(db, &destination) {
        return e;
    }

    let value = match src_route {
        ListRoute::Listpack => match pop_listpack(db, &source, None, pop_front) {
            Frame::BulkString(v) => v,
            Frame::Error(e) => return Frame::Error(e),
            _ => return Frame::Null,
        },
        ListRoute::Full => {
            let popped = if pop_front {
                db.list_pop_front(&source)
            } else {
                db.list_pop_back(&source)
            };
            match popped {
                Some(v) => v,
                None => return Frame::Null,
            }
        }
    };
    if let Err(e) = push_one(db, &destination, &value, push_front) {
        // moon#1225: refused, not lost — back to the end it came from.
        if push_one(db, &source, &value, pop_front).is_err() {
            tracing::error!("LMOVE: destination refused and source restore failed (moon#1225)");
        }
        return e;
    }
    Frame::BulkString(value)
}

/// `LMOVE k k …`: pop one end and push the same element onto an end of the
/// same list, without ever letting the key go empty.
fn lmove_rotate(
    db: &mut Database,
    key: &Bytes,
    route: ListRoute,
    pop_front: bool,
    push_front: bool,
) -> Frame {
    if let ListRoute::Listpack = route {
        match db.get_or_create_list_listpack(key) {
            Ok(Some(lp)) => {
                let before = lp.estimate_memory();
                let Some(value) = listpack_pop_end(lp, pop_front) else {
                    return Frame::Null;
                };
                if push_front {
                    lp.push_front(&value);
                } else {
                    lp.push_back(&value);
                }
                let after = lp.estimate_memory();
                // `lp`'s borrow of `db` ends here. Same element, same count:
                // the policy verdict cannot move.
                db.adjust_memory(before, after);
                return Frame::BulkString(value);
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }
    let list = match db.get_or_create_list(key) {
        Ok(l) => l,
        Err(e) => return e,
    };
    let popped = if pop_front {
        list.pop_front()
    } else {
        list.pop_back()
    };
    let Some(value) = popped else {
        return Frame::Null;
    };
    // The element stays in the list, so its ledger charge stays with it.
    if push_front {
        list.push_front(value.clone());
    } else {
        list.push_back(value.clone());
    }
    Frame::BulkString(value)
}

/// Push one element onto `key` exactly as a one-element `LPUSH`/`RPUSH`
/// would: onto a listpack in place (creating the key as a listpack when it
/// is missing and the element fits the policy), promoting past the policy,
/// and onto the full `VecDeque` otherwise.
///
/// moon#1225: a refusal is RETURNED, not swallowed. The route refuses a wrong
/// type or unreadable cold copy before the pop; a cold file that fails only on
/// this promotion's second read is still reachable, and dropped the element.
fn push_one(db: &mut Database, key: &Bytes, value: &Bytes, front: bool) -> Result<(), Frame> {
    let limits = db.encoding_limits();
    if limits.fits(Shape::List, 1, value.len()) {
        if let Some(lp) = db.get_or_create_list_listpack(key)? {
            let before = lp.estimate_memory();
            if front {
                lp.push_front(value);
            } else {
                lp.push_back(value);
            }
            let after = lp.estimate_memory();
            let should_upgrade = !limits.listpack_fits(Shape::List, lp);
            // `lp`'s borrow of `db` ends here.
            db.adjust_memory(before, after);
            if should_upgrade {
                promote_list_listpack(db, key, after);
            }
            return Ok(());
        }
    }
    let list = db.get_or_create_list(key)?;
    if front {
        list.push_front(value.clone());
    } else {
        list.push_back(value.clone());
    }
    db.charge_memory(list_elem_cost(value));
    Ok(())
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

    // The exists-and-is-a-list gate. moon#942: this used to be
    // `db.get_list(key)` = `get_promoted`, which costs TWO probes and, being
    // a `&mut self` accessor whose `ListKind::upgrade` is unconditional,
    // flattens the compact encoding just to answer a yes/no — the same pair
    // moon#897 took out of `LPOP`'s gate. `list_route` is the `&self` router:
    // one probe, and its receiver makes the rewrite unrepresentable.
    //
    // The mutable accessor below is deliberately unchanged, so the ENCODING
    // outcome is unchanged too (LPUSHX still flattens a listpack — moon#832,
    // still open, pinned by `lpushx_and_rpushx_still_flatten_a_listpack_moon832`).
    // Nor is this `get_mut_if_present`, which would fold both accessors into
    // one and save a second probe: that one stamps the mutation BEFORE it can
    // answer WRONGTYPE, so it would widen moon#940 to a command that today
    // refuses without dirtying a watched key.
    match list_route(db, key) {
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
        // moon#1160: an exact-size copy, never a slice of the request buffer.
        let val = match extract_bytes(arg) {
            Some(v) => detach(v),
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

    // The same one-probe, non-flattening gate `LPUSHX` takes; see the comment
    // there for why it is not `get_promoted` and not `get_mut_if_present`.
    match list_route(db, key) {
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
        // moon#1160: an exact-size copy, never a slice of the request buffer.
        let val = match extract_bytes(arg) {
            Some(v) => detach(v),
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

    // moon#1174 §1: each key is routed through the one-probe `&self`
    // `list_route` and popped by the same two bodies `LPOP key count` uses, so
    // a listpack stays a listpack. The old loop asked `db.get_list` (a
    // flattening `get_promoted`) just to read the length, then flattened again
    // through `get_or_create_list` for the pop.
    for i in 0..numkeys {
        let key = match extract_bytes(&args[1 + i]) {
            Some(k) => k,
            None => return err_wrong_args("LMPOP"),
        };
        let popped = match list_route(db, key) {
            Err(e) => return e,
            Ok(None) => continue,
            Ok(Some(ListRoute::Listpack)) => pop_listpack(db, key, Some(count), left),
            Ok(Some(ListRoute::Full)) => pop_eager(db, key, Some(count), left),
        };
        match popped {
            Frame::Array(items) if !items.is_empty() => {
                return Frame::Array(framevec![
                    Frame::BulkString(key.clone()),
                    Frame::Array(items),
                ]);
            }
            Frame::Error(e) => return Frame::Error(e),
            _ => continue,
        }
    }

    // No key held anything: LMPOP's miss is a null ARRAY (moon#482).
    Frame::NullArray
}
