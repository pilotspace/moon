use bytes::Bytes;
use rand::RngExt;
use std::collections::HashSet;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::db::{SetRef, Shape, set_member_cost, set_table_bytes};
use crate::storage::entry::{Entry, boxed_payload_block};

use super::{collect_sets, parse_int};
use crate::command::helpers::{err_wrong_args, extract_bytes};

// ---------------------------------------------------------------------------
// SADD key member [member ...]
// ---------------------------------------------------------------------------

/// Try to parse a byte slice as an i64.
fn try_parse_i64(b: &[u8]) -> Option<i64> {
    // Canonical forms only -- a member that does not render back to the
    // caller's exact bytes must not go into an intset. See `storage::numeric`.
    crate::storage::numeric::canonical_i64(b)
}

/// SADD command handler: add members to a set.
/// Returns Integer(count of new members added).
///
/// For new keys where all members are valid integers (and count <= 512),
/// creates a SetIntset encoding for memory efficiency. Otherwise uses
/// the standard HashSet encoding.
pub fn sadd(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SADD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SADD"),
    };

    // Check if all members are valid integers (for intset optimization)
    let all_integers = args[1..].iter().all(|a| {
        extract_bytes(a)
            .map(|b| try_parse_i64(b).is_some())
            .unwrap_or(false)
    });
    let member_count = args.len() - 1;
    // Every threshold below comes from the ONE authority (moon#896).
    let limits = db.encoding_limits();

    // Try intset path: new key with all-integer members, or existing intset
    if all_integers && limits.intset_fits(member_count) {
        match db.get_or_create_intset(key) {
            Ok(Some(intset)) => {
                // `Intset::estimate_memory()` is O(1) (capacity-based).
                let before = intset.estimate_memory();
                let mut added = 0i64;
                // Index into `args` of the first member the intset did NOT
                // absorb — where the upgraded `IndexSet` has to resume.
                // `None` means the whole batch fit and there is no upgrade.
                let mut resume_at: Option<usize> = None;
                for (i, arg) in args[1..].iter().enumerate() {
                    // Neither skip below is reachable: the `all_integers` gate
                    // above already proved that EVERY member of this batch is
                    // a bulk frame holding a canonical integer. That is what
                    // makes the tail boundary exact — no member is ever passed
                    // over before `resume_at`, so everything before it is in
                    // the intset and everything from it on is not.
                    if let Some(member) = extract_bytes(arg) {
                        let Some(val) = try_parse_i64(member) else {
                            continue;
                        };
                        if intset.insert(val) {
                            added += 1;
                        }
                        if !limits.intset_fits(intset.len()) {
                            // `args[1 + i]` is the member that tripped the
                            // ceiling. It IS in the intset and IS counted, so
                            // the unabsorbed tail starts one past it.
                            resume_at = Some(i + 2);
                            break;
                        }
                    }
                }
                let after = intset.estimate_memory();
                // `intset`'s borrow of `db` ends here.
                if after >= before {
                    db.charge_memory(after - before);
                } else {
                    db.credit_memory(before - after);
                }
                if let Some(resume_at) = resume_at {
                    // One-time cost-model swing (intset -> HashSet) — see the
                    // matching comment in hash_write.rs's hset.
                    let set = db.upgrade_intset_to_set(key);
                    // moon#944: the REPLY is part of the contract. `added`
                    // stopped at the `break`, so the tail has to keep counting
                    // into it, and `IndexSet::insert`'s `bool` is the only
                    // thing that says whether a member was new. Discarding it
                    // under-reported every member past the crossing: measured
                    // against redis 7.4.0 with `set-max-intset-entries 512`,
                    // moon replied 3 for a batch that added 22, so the sum of
                    // a client's SADD replies disagreed with `SCARD`.
                    //
                    // Only `args[resume_at..]` is walked. `args[1..resume_at]`
                    // is already IN `set`: `Intset::to_set_value` renders each
                    // value with `to_string`, and every value reached the
                    // intset through `canonical_i64`, so that rendering is the
                    // caller's exact bytes (moon#795). Re-walking the absorbed
                    // prefix would be an O(batch) no-op that also spends one
                    // `Bytes` clone per member for nothing — the clone that
                    // survives below is the unavoidable ownership transfer of
                    // a member that genuinely has to be stored, exactly as on
                    // the standard path.
                    for arg in &args[resume_at..] {
                        if let Some(member) = extract_bytes(arg)
                            && set.insert(member.clone())
                        {
                            added += 1;
                        }
                    }
                    // `boxed_payload_block`: the `IndexSet` the members move
                    // into is a boxed payload of its own — 80 B the intset did
                    // not have. See the matching note in hash_write.rs.
                    let new_cost: usize = boxed_payload_block(set)
                        + set_table_bytes(set)
                        + set.iter().map(|m| set_member_cost(m)).sum::<usize>();
                    db.credit_memory(after);
                    db.charge_memory(new_cost);
                    return Frame::Integer(added);
                }
                return Frame::Integer(added);
            }
            Ok(None) => {
                // Key exists but is not an intset (it's a HashSet or SetListpack)
                // Fall through to normal path
            }
            Err(e) => return e, // WRONGTYPE
        }
    }

    // Listpack path for small string sets (moon#787). Mirrors HSET/RPUSH:
    // Redis keeps a set in a listpack until it exceeds set-max-listpack-entries
    // (128) or set-max-listpack-value (64), and moon reported `hashtable` from
    // the first member because no accessor ever produced a surviving
    // `SetListpack`.
    //
    // The entry gate, from the ONE authority (moon#896): the longest member
    // in the batch against `set-max-listpack-value`, the batch size against
    // `set-max-listpack-entries`. The same predicate bounds a batch far below
    // the listpack header's u16 range (moon#865) -- the upgrade check runs
    // AFTER the push loop, which cannot stop a batch already too big; measured
    // before the bound existed, `SADD` of 70,000 members replied 70000 and
    // `SCARD` then replied 4464. It also closes the O(n^2) window: membership
    // here is a linear scan, so an unbounded batch is quadratic inside one
    // command on one shard thread.
    let max_member = args[1..]
        .iter()
        .map(|a| extract_bytes(a).map_or(0, |b| b.len()))
        .max()
        .unwrap_or(0);
    if limits.fits(Shape::Set, member_count, max_member) {
        // moon#899: a string joining a SMALL intset lands in a listpack, as
        // in redis 7.2+; before this edge existed it went straight to a
        // hashtable (3 ints + "abc": moon `hashtable`, redis `listpack`).
        // Redis's rule, stated on the authority: the intset's members plus
        // one still fit the entry threshold, and neither the incoming batch's
        // longest member nor the widest rendered integer exceeds the value
        // threshold. The push loop's upgrade check below then handles a
        // batch that overflows the converted listpack, exactly as it does
        // for any other listpack.
        let absorb = |members: usize, widest: usize| {
            limits.fits(Shape::Set, members + 1, max_member.max(widest))
        };
        match db.get_or_create_set_listpack(key, absorb) {
            Ok(Some(lp)) => {
                let mut added = 0i64;
                // Listpack `estimate_memory()` is O(1) (capacity-based), so a
                // before/after snapshot is cheap — no per-element formula.
                let before = lp.estimate_memory();
                for arg in &args[1..] {
                    // Same skip the standard path below applies. A non-bulk
                    // frame is never an error from INSIDE this loop: that
                    // would return from the `before … adjust_memory` window
                    // with members already pushed and never charged — the
                    // moon#814 shape — and moon#823 closed the only source
                    // of such a frame at the Lua boundary anyway.
                    let Some(member) = extract_bytes(arg) else {
                        continue;
                    };
                    // Borrowed scan: `contains_element` compares against each
                    // entry in place. The owning `iter().any(as_bytes ==)`
                    // shape allocated one `Vec` per entry walked — the exact
                    // lookup moon#801 removed — so it must not come back here.
                    if !lp.contains_element(member) {
                        lp.push_back(member);
                        added += 1;
                    }
                }
                let after = lp.estimate_memory();
                // The upgrade check, from the same authority as the gate.
                let should_upgrade = !limits.listpack_fits(Shape::Set, lp);
                // `lp`'s borrow of `db` ends here — safe to call back into
                // `db` for accounting from this point on.
                db.adjust_memory(before, after);
                if should_upgrade {
                    // One-time cost-model swing (listpack -> IndexSet). The
                    // accessor bills it itself through `SetKind::upgrade`, so
                    // the entries `Vec` and the index table are charged from
                    // their real capacity (moon#788/#810), not a per-member
                    // guess that under-counts the whole table.
                    db.upgrade_set_listpack_to_set(key);
                }
                return Frame::Integer(added);
            }
            // Already an IndexSet or a SetIntset: fall through.
            Ok(None) => {}
            Err(e) => return e, // WRONGTYPE
        }
    }

    // Standard path: get_or_create_set (creates HashSet, upgrades compact encodings)
    let set = match db.get_or_create_set(key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut added = 0i64;
    let mut mem_delta: usize = 0;
    // moon#788: the `IndexSet`'s own entries `Vec` and index table are charged
    // from their REAL capacity, snapshotted around the mutation (O(1), two
    // `capacity()` reads) — the same pattern the listpack/intset paths use.
    // A per-member constant cannot model a doubling table.
    let table_before = set_table_bytes(set);
    for arg in &args[1..] {
        if let Some(member) = extract_bytes(arg) {
            if set.insert(member.clone()) {
                added += 1;
                mem_delta += set_member_cost(member);
            }
        }
    }
    let table_after = set_table_bytes(set);
    // `set`'s borrow of `db` ends above.
    db.charge_memory(mem_delta);
    db.adjust_memory(table_before, table_after);
    Frame::Integer(added)
}

// ---------------------------------------------------------------------------
// SREM key member [member ...]
// ---------------------------------------------------------------------------

/// Which encoding the key holds RIGHT NOW, as answered by a `&self` probe that
/// cannot rewrite it.
///
/// `Copy` and field-free on purpose: it is the whole of what a `SetRef` borrow
/// tells the router, so the borrow of `db` ends at the `match` that produces
/// one and the mutation below is free to take `&mut db`.
#[derive(Clone, Copy)]
enum SetRoute {
    /// A live `SetListpack` — mutate it in place.
    Listpack,
    /// A live `SetIntset` — mutate it in place.
    Intset,
    /// The full `IndexSet`, a cold-spilled value, an expired entry, or no key
    /// at all: the pre-existing eager path, byte for byte.
    Full,
}

/// Ask what `key` holds without rewriting it.
///
/// `get_set_ref_if_alive` takes `&self`, so — unlike every `get_or_create_*`
/// and `get_promoted` accessor — asking this question cannot itself flatten
/// the compact encoding (moon#832). A missing, expired or cold-spilled key
/// answers `Full`, which routes to exactly the code that ran before this
/// function existed; only the two compact forms take a new path.
fn set_route(db: &Database, key: &[u8]) -> Result<SetRoute, Frame> {
    match db.get_set_ref_if_alive(key, db.now_ms()) {
        Ok(Some(SetRef::Listpack(_))) => Ok(SetRoute::Listpack),
        Ok(Some(SetRef::Intset(_))) => Ok(SetRoute::Intset),
        Ok(Some(SetRef::Hash(_) | SetRef::Owned(_))) | Ok(None) => Ok(SetRoute::Full),
        Err(e) => Err(e),
    }
}

/// SREM command handler: remove members from a set.
/// Returns Integer(count removed). Removes key if set becomes empty.
///
/// # Encoding (moon#897)
///
/// A `SREM` of one member used to flatten a three-member `listpack` set to a
/// `hashtable` — permanently, because nothing demotes (moon#832) — by reaching
/// for `get_or_create_set`, whose `SetKind::upgrade` materialises the full form
/// unconditionally. Redis mutates the listpack in place and promotes only when
/// a threshold is genuinely crossed; so does this now, for BOTH compact source
/// encodings (an `intset` stays an `intset`, a `listpack` stays a `listpack` —
/// they are different code paths and moon got both wrong).
///
/// A removal only ever shrinks a container, so it cannot make one newly
/// oversized; the authority is still consulted after the mutation, because a
/// listpack that was ALREADY past the policy — the one case where a container
/// legitimately belongs in the full form — must still promote.
pub fn srem(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SREM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SREM"),
    };

    match set_route(db, key) {
        Err(e) => e,
        Ok(SetRoute::Listpack) => srem_listpack(db, key, args),
        Ok(SetRoute::Intset) => srem_intset(db, key, args),
        Ok(SetRoute::Full) => srem_eager(db, key, args),
    }
}

/// The pre-moon#897 path, unchanged: materialise the full `IndexSet` and
/// `swap_remove` from it.
///
/// Reached for a set that is ALREADY a hashtable, for a cold-spilled value
/// (which `get_or_create_set` promotes back), for an expired entry (which it
/// drops), and for a missing key (which it fabricates and the cleanup below
/// then removes) — i.e. for every case where there is no compact encoding to
/// preserve, so the routing costs one `&self` probe and changes nothing.
fn srem_eager(db: &mut Database, key: &Bytes, args: &[Frame]) -> Frame {
    let set = match db.get_or_create_set(key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut removed = 0i64;
    let mut credit: usize = 0;
    let table_before = set_table_bytes(set);
    for arg in &args[1..] {
        if let Some(member) = extract_bytes(arg) {
            if set.swap_remove(member) {
                removed += 1;
                credit += set_member_cost(member);
            }
        }
    }
    // `swap_remove` does NOT shrink the table, so this normally nets zero —
    // which is the truth the old per-member credit got wrong by handing back
    // table bytes the allocator still holds.
    let table_after = set_table_bytes(set);
    // `set`'s borrow of `db` ends above.
    db.credit_memory(credit);
    db.adjust_memory(table_before, table_after);
    // Clean up empty set. `get_set_ref_if_alive` rather than `get_set`: the
    // latter is `get_promoted`, so the emptiness PROBE itself re-flattened the
    // container this function had just kept compact (moon#832).
    let now_ms = db.now_ms();
    let empty = matches!(db.get_set_ref_if_alive(key, now_ms), Ok(Some(s)) if s.len() == 0);
    if empty {
        db.remove(key);
    }
    Frame::Integer(removed)
}

/// `SREM` against a `SetListpack`: remove in place, keep the listpack.
///
/// The `absorb` closure answers `false` unconditionally. It is the moon#899
/// `intset -> listpack` edge, and it belongs to `SADD` alone: this arm is only
/// reached for a key the `&self` probe already saw AS a listpack, and a removal
/// can never be the thing that makes a set newly fit one.
fn srem_listpack(db: &mut Database, key: &Bytes, args: &[Frame]) -> Frame {
    let limits = db.encoding_limits();
    let lp = match db.get_or_create_set_listpack(key, |_, _| false) {
        Ok(Some(lp)) => lp,
        // The probe said listpack; anything else means the value changed
        // between the probe and here, which one shard thread cannot do. Fall
        // back to the eager path rather than assume.
        Ok(None) => return srem_eager(db, key, args),
        Err(e) => return e,
    };
    // Listpack `estimate_memory()` is O(1) (capacity-based), so a before/after
    // snapshot is cheap — no per-member formula.
    let before = lp.estimate_memory();
    let mut removed = 0i64;
    for arg in &args[1..] {
        // A non-bulk frame is skipped, not an error, exactly as the eager path
        // below skips it — the reply and the count must not depend on which
        // encoding the set happened to be in.
        let Some(member) = extract_bytes(arg) else {
            continue;
        };
        // `find` is a BORROWED scan (`ListpackRef::eq_bytes`): it allocates
        // neither the probe nor the entries it walks past, and it applies the
        // canonical-integer rule, so a stored `Integer(7)` answers to `b"7"`
        // and to nothing else. `remove_at` discards the entry without decoding
        // it. Bytes of every SURVIVING member are untouched (moon#795/#903).
        if let Some(idx) = lp.find(member) {
            lp.remove_at(idx);
            removed += 1;
        }
    }
    let after = lp.estimate_memory();
    let empty = lp.is_empty();
    // The upgrade check, from the ONE authority (moon#896) and the same
    // predicate `SADD`'s push loop uses. A shrink cannot cross the threshold
    // upward, so this is false for every listpack `SADD` could have produced;
    // it fires only for one that was already past the policy, which is exactly
    // the container that belongs in the full form.
    let should_upgrade = !limits.listpack_fits(Shape::Set, lp);
    // `lp`'s borrow of `db` ends here — safe to call back into `db` now.
    db.adjust_memory(before, after);
    if empty {
        db.remove(key);
    } else if should_upgrade {
        // The accessor bills the swing itself through `SetKind::upgrade`
        // (moon#788/#810).
        db.upgrade_set_listpack_to_set(key);
    }
    Frame::Integer(removed)
}

/// `SREM` against a `SetIntset`: remove in place, keep the intset.
fn srem_intset(db: &mut Database, key: &Bytes, args: &[Frame]) -> Frame {
    let is = match db.get_or_create_intset(key) {
        Ok(Some(is)) => is,
        Ok(None) => return srem_eager(db, key, args),
        Err(e) => return e,
    };
    // `Intset::estimate_memory()` is O(1) (capacity-based).
    let before = is.estimate_memory();
    let mut removed = 0i64;
    for arg in &args[1..] {
        let Some(member) = extract_bytes(arg) else {
            continue;
        };
        // Canonical spellings only. An intset stores `i64`s and answers to
        // their exact `itoa` rendering, so `+5`, `007` and `-0` are simply not
        // members of a set holding `5`, `7` and `0` — the same verdict redis
        // reaches through its own `string2ll` gate, and the rule that keeps the
        // encoding byte-transparent (moon#795).
        let Some(val) = try_parse_i64(member) else {
            continue;
        };
        if is.remove(val) {
            removed += 1;
        }
    }
    let after = is.estimate_memory();
    let empty = is.is_empty();
    // `is`'s borrow of `db` ends here.
    db.adjust_memory(before, after);
    // No upgrade check: `set-max-intset-entries` bounds a COUNT, and a removal
    // only lowers it. An intset can never become too large by shrinking, and
    // (unlike a listpack) it has no element-size dimension to cross.
    if empty {
        db.remove(key);
    }
    Frame::Integer(removed)
}

// ---------------------------------------------------------------------------
// SPOP key [count]
// ---------------------------------------------------------------------------

/// SPOP command handler: remove and return random members.
/// Without count: remove and return one random member or Null.
/// With count: remove and return Array of members.
pub fn spop(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("SPOP");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SPOP"),
    };

    // Index-first, no clone. The set is an `IndexSet`, so a member is
    // addressable in O(1); the previous shape cloned the WHOLE set and then
    // collected every member into a `Vec` just to choose one.
    let len = match db.get_set(key) {
        Ok(Some(s)) => s.len(),
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };

    if len == 0 {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(framevec![])
        };
    }

    let mut rng = rand::rng();

    if args.len() == 1 {
        let idx = rng.random_range(0..len);
        let Some(chosen) = (match db.get_set(key) {
            Ok(Some(s)) => s.get_index(idx).cloned(),
            _ => None,
        }) else {
            return Frame::Null;
        };
        let Ok(set) = db.get_or_create_set(key) else {
            return Frame::Null;
        };
        let table_before = set_table_bytes(set);
        set.swap_remove(&chosen);
        let empty = set.is_empty();
        let table_after = set_table_bytes(set);
        // `set`'s borrow of `db` ends above.
        // moon#788: credit the member and the table shrink FIRST, in every
        // case. The empty branch used to lean on `db.remove` to "recompute
        // the now-empty entry cost", but that recompute no longer sees the
        // member just popped, and a hashbrown `capacity()` shrinks as entries
        // are erased — so the last member's bytes and the table delta were
        // stranded on every create/drain cycle.
        db.credit_memory(set_member_cost(&chosen));
        db.adjust_memory(table_before, table_after);
        if empty {
            db.remove(key);
        }
        return Frame::BulkString(chosen);
    }

    let count = match parse_int(&args[1]) {
        Some(c) if c >= 0 => c as usize,
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
    };

    if count == 0 {
        return Frame::Array(framevec![]);
    }

    // Draw n distinct INDICES in O(n), then resolve them to members. Note the
    // indices must all be resolved BEFORE any removal: `swap_remove` moves the
    // last element into the freed slot, so an index taken after a removal
    // would address a different member than the one drawn.
    let n = std::cmp::min(count, len);
    let picks = rand::seq::index::sample(&mut rng, len, n);
    let chosen: Vec<Bytes> = match db.get_set(key) {
        Ok(Some(s)) => picks
            .into_iter()
            .filter_map(|i| s.get_index(i).cloned())
            .collect(),
        _ => return Frame::Array(framevec![]),
    };

    // Remove chosen members from the set
    // Key confirmed as set type above via get_set(); get_or_create_set() cannot fail here
    let Ok(set) = db.get_or_create_set(key) else {
        return Frame::Array(framevec![]);
    };
    let table_before = set_table_bytes(set);
    for m in &chosen {
        set.swap_remove(m);
    }
    let empty = set.is_empty();
    let table_after = set_table_bytes(set);
    // `set`'s borrow of `db` ends above.
    // Credit unconditionally -- see the note in the single-member branch.
    let credit: usize = chosen.iter().map(|m| set_member_cost(m)).sum();
    db.credit_memory(credit);
    db.adjust_memory(table_before, table_after);
    if empty {
        db.remove(key);
    }

    let result: Vec<Frame> = chosen.into_iter().map(Frame::BulkString).collect();
    Frame::Array(result.into())
}

// ---------------------------------------------------------------------------
// Raw set algebra returning HashSet (for *STORE variants)
// ---------------------------------------------------------------------------

fn sinter_raw(db: &mut Database, args: &[Frame]) -> Result<HashSet<Bytes>, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("SINTERSTORE"));
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return Err(err_wrong_args("SINTERSTORE"));
    }

    let sets = collect_sets(db, &keys)?;

    let mut concrete: Vec<HashSet<Bytes>> = Vec::new();
    for s in sets {
        match s {
            Some(set) => concrete.push(set),
            None => return Ok(HashSet::new()),
        }
    }

    if concrete.is_empty() {
        return Ok(HashSet::new());
    }

    concrete.sort_by_key(|s| s.len());
    let mut result = concrete[0].clone();
    for other in &concrete[1..] {
        result.retain(|m| other.contains(m));
    }
    Ok(result)
}

fn sunion_raw(db: &mut Database, args: &[Frame]) -> Result<HashSet<Bytes>, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("SUNIONSTORE"));
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return Err(err_wrong_args("SUNIONSTORE"));
    }

    let sets = collect_sets(db, &keys)?;

    let mut result = HashSet::new();
    for s in sets {
        if let Some(set) = s {
            result.extend(set);
        }
    }
    Ok(result)
}

fn sdiff_raw(db: &mut Database, args: &[Frame]) -> Result<HashSet<Bytes>, Frame> {
    if args.is_empty() {
        return Err(err_wrong_args("SDIFFSTORE"));
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return Err(err_wrong_args("SDIFFSTORE"));
    }

    let sets = collect_sets(db, &keys)?;

    let mut result = match &sets[0] {
        Some(set) => set.clone(),
        None => return Ok(HashSet::new()),
    };

    for s in &sets[1..] {
        if let Some(set) = s {
            result.retain(|m| !set.contains(m));
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// SINTERSTORE destination key [key ...]
// ---------------------------------------------------------------------------

/// SINTERSTORE command handler: compute SINTER and store in destination.
pub fn sinterstore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SINTERSTORE");
    }
    let dest = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SINTERSTORE"),
    };

    // Compute intersection using source keys (args[1..])
    let result = sinter_raw(db, &args[1..]);
    let result = match result {
        Ok(set) => set,
        Err(e) => return e,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut entry = Entry::new_set();
        if let Some(crate::storage::entry::RedisValue::Set(s)) = entry.value.as_redis_value_mut() {
            // Set algebra computes in a `HashSet`; the stored representation is
            // an `IndexSet` so SPOP/SRANDMEMBER can address a member by index.
            **s = result.into_iter().collect();
        }
        db.set(dest, entry);
    }
    Frame::Integer(count)
}

// ---------------------------------------------------------------------------
// SUNIONSTORE destination key [key ...]
// ---------------------------------------------------------------------------

/// SUNIONSTORE command handler: compute SUNION and store in destination.
pub fn sunionstore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SUNIONSTORE");
    }
    let dest = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SUNIONSTORE"),
    };

    let result = sunion_raw(db, &args[1..]);
    let result = match result {
        Ok(set) => set,
        Err(e) => return e,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut entry = Entry::new_set();
        if let Some(crate::storage::entry::RedisValue::Set(s)) = entry.value.as_redis_value_mut() {
            // Set algebra computes in a `HashSet`; the stored representation is
            // an `IndexSet` so SPOP/SRANDMEMBER can address a member by index.
            **s = result.into_iter().collect();
        }
        db.set(dest, entry);
    }
    Frame::Integer(count)
}

// ---------------------------------------------------------------------------
// SDIFFSTORE destination key [key ...]
// ---------------------------------------------------------------------------

/// SDIFFSTORE command handler: compute SDIFF and store in destination.
pub fn sdiffstore(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SDIFFSTORE");
    }
    let dest = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SDIFFSTORE"),
    };

    let result = sdiff_raw(db, &args[1..]);
    let result = match result {
        Ok(set) => set,
        Err(e) => return e,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(dest);
    } else {
        let mut entry = Entry::new_set();
        if let Some(crate::storage::entry::RedisValue::Set(s)) = entry.value.as_redis_value_mut() {
            // Set algebra computes in a `HashSet`; the stored representation is
            // an `IndexSet` so SPOP/SRANDMEMBER can address a member by index.
            **s = result.into_iter().collect();
        }
        db.set(dest, entry);
    }
    Frame::Integer(count)
}

// ---------------------------------------------------------------------------
// SMOVE source destination member
// ---------------------------------------------------------------------------

/// SMOVE source destination member
pub fn smove(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("SMOVE");
    }
    let source = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SMOVE"),
    };
    let destination = match extract_bytes(&args[1]) {
        Some(k) => k,
        None => return err_wrong_args("SMOVE"),
    };
    let member = match extract_bytes(&args[2]) {
        Some(m) => m.clone(),
        None => return err_wrong_args("SMOVE"),
    };

    match db.get_set(source) {
        Ok(None) => return Frame::Integer(0),
        Err(e) => return e,
        Ok(Some(_)) => {}
    }
    match db.get_set(destination) {
        Ok(_) => {}
        Err(e) => return e,
    }

    if source == destination {
        let src_set = match db.get_or_create_set(source) {
            Ok(s) => s,
            Err(e) => return e,
        };
        return if src_set.contains(&member) {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
    }

    let src_set = match db.get_or_create_set(source) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let src_table_before = set_table_bytes(src_set);
    if !src_set.swap_remove(&member) {
        return Frame::Integer(0);
    }
    let src_empty = src_set.is_empty();
    let src_table_after = set_table_bytes(src_set);
    // `src_set`'s borrow of `db` ends above.
    // Credit unconditionally -- see the note in `spop`.
    db.credit_memory(set_member_cost(&member));
    db.adjust_memory(src_table_before, src_table_after);
    if src_empty {
        db.remove(source);
    }

    let dst_set = match db.get_or_create_set(destination) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let dst_table_before = set_table_bytes(dst_set);
    let inserted = dst_set.insert(member.clone());
    let dst_table_after = set_table_bytes(dst_set);
    // `dst_set`'s borrow of `db` ends above.
    if inserted {
        db.charge_memory(set_member_cost(&member));
    }
    db.adjust_memory(dst_table_before, dst_table_after);

    Frame::Integer(1)
}
