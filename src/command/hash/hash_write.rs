use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::{Frame, FrameVec};
use crate::storage::Database;
use crate::storage::db::{HashTtlCond, Shape, hash_field_cost, hash_field_cost_len};
use crate::storage::entry::boxed_payload_block;
use crate::storage::owned_bytes::detach;

use crate::command::helpers::{err_wrong_args, extract_bytes, ok};
use crate::storage::listpack::{ListpackRef, PairUpdate};

/// Widest decimal rendering of an `i64` — `-9223372036854775808`, 20 bytes.
///
/// HINCRBY's entry gate needs an upper bound on the value it is about to
/// write, and the actual value is not known until the CURRENT one has been
/// read — which needs the listpack borrow the gate must precede. Every `i64`
/// `itoa` can render fits in this, so gating on it is conservative in the one
/// safe direction: it can only send an oversized write to the full form,
/// never squeeze an oversized value into a listpack.
const I64_MAX_RENDERED_LEN: usize = 20;

/// An `i64` rendered on the stack. `SmallVec` with a 20-byte inline array
/// never reaches the allocator for a value `itoa` can produce, so this is the
/// hot-path-legal way to hand bytes OUT of a closure — the shape
/// `zset_score::ScoreBuf` already uses for ZINCRBY.
type IntBuf = SmallVec<[u8; I64_MAX_RENDERED_LEN]>;

/// Render `v` into an owned stack buffer.
///
/// `Listpack::update_pair_value`'s closure cannot return `itoa::Buffer::format`'s
/// `&str`: that borrow is derived from a variable the closure captures, and a
/// closure may not hand out a borrow of its own capture. The replacement has
/// to own its bytes, which is what this is.
#[inline]
fn render_i64(v: i64) -> IntBuf {
    let mut ibuf = itoa::Buffer::new();
    IntBuf::from_slice(ibuf.format(v).as_bytes())
}

/// Settle the memory ledger after a listpack-path hash write and perform the
/// one-time `HashListpack -> Hash` upgrade when the listpack no longer fits.
///
/// Every listpack write in this file ends the same way: charge or credit the
/// listpack's O(1) capacity delta, then, if the authority says the container
/// has outgrown the compact form, swap the encoding and re-bill it under the
/// full form's per-field cost model. Four hand-copied tails is exactly how
/// three sites came to disagree about a threshold (moon#896), so the paths
/// moon#897 adds share HSET's rather than reproducing it.
///
/// `before`/`after` are `Listpack::estimate_memory()` around the mutation.
/// `should_upgrade` is `!limits.listpack_fits(Shape::Hash, lp)`, evaluated
/// while the listpack was still borrowed — the authority's predicate, never a
/// hand-rolled count.
fn settle_hash_listpack_write(
    db: &mut Database,
    key: &[u8],
    before: usize,
    after: usize,
    should_upgrade: bool,
) {
    if after >= before {
        db.charge_memory(after - before);
    } else {
        db.credit_memory(before - after);
    }
    if !should_upgrade {
        return;
    }
    // One-time listpack -> Hash upgrade: the cost MODEL changes
    // (capacity-based -> per-field sum), so the swing is charged via a single
    // O(n) recompute. This fires once per key at the threshold, not per
    // mutation.
    let new_cost: usize = {
        let map = db.upgrade_hash_listpack_to_hash(key);
        // The `HashMap` the fields move into is itself a boxed payload — a
        // second allocation the listpack did not have. Omit it and the running
        // ledger drifts 48 B below a full recompute on every key that crosses
        // the threshold.
        boxed_payload_block(map)
            + map
                .iter()
                .map(|(k, v)| hash_field_cost(k, v))
                .sum::<usize>()
    };
    db.credit_memory(after);
    db.charge_memory(new_cost);
}

/// HSET key field value [field value ...]
///
/// Sets field-value pairs in the hash stored at key. Returns the number
/// of fields that were newly added (not updated).
pub fn hset(db: &mut Database, args: &[Frame]) -> Frame {
    // Need at least key + one field-value pair, and args count must be odd (key + pairs)
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return err_wrong_args("HSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSET"),
    };

    // ONE walk of the argv does both jobs (moon#942). It used to be two:
    // `all_args_are_bytes` matched every frame, then the `max` chain matched
    // every frame again to read its length.
    //
    // moon#823 (the validation half): refuse a non-argument-shaped frame
    // BEFORE the mutation window opens. The write loop below bails on the
    // first one `extract_bytes` rejects, and by then it has already written
    // part of the command — a partial write that is applied on the master
    // and, because propagation is gated on the reply not being an error,
    // never reaches the AOF or a replica. Returning from this walk keeps the
    // refusal strictly before `get_or_create_hash_listpack`.
    //
    // moon#896 (the measurement half): the entry gate takes the longest field
    // OR value anywhere in the batch — position does not matter — against
    // `hash-max-listpack-value`.
    let mut max_elem = 0usize;
    for a in &args[1..] {
        match extract_bytes(a) {
            Some(b) => max_elem = max_elem.max(b.len()),
            None => return err_wrong_args("HSET"),
        }
    }

    // The entry gate, from the ONE authority (moon#896): that longest element
    // against `hash-max-listpack-value`, the batch size against
    // `hash-max-listpack-entries`. The same predicate bounds a batch far
    // below the listpack header's u16 range (moon#865) — the upgrade check
    // below runs after the push loop, which is too late to stop a batch that
    // is already too big.
    let limits = db.encoding_limits();
    // `args.len() - 1` is listpack ENTRIES (two per field); the policy is in
    // FIELDS, and the shape converts. Passing the entry count here was
    // moon#896: a bulk HSET of 65 fields (argv 130) refused the listpack path
    // that the same hash built one field at a time stayed on until 128.
    if limits.fits(Shape::Hash, Shape::Hash.items_in(args.len() - 1), max_elem) {
        // Try listpack path for small hashes. HashWithTtl returns Ok(None) here
        // (get_or_create_hash_listpack is now HashWithTtl-aware), so it falls
        // through to the full HashMap path — correct, because TTL'd hashes never
        // compact back to listpack.
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                let mut count = 0i64;
                // Listpack `estimate_memory()` is O(1) (capacity-based), so a
                // before/after snapshot is cheap — no per-element formula needed.
                let before = lp.estimate_memory();
                let mut i = 1;
                while i < args.len() {
                    let field = match extract_bytes(&args[i]) {
                        Some(f) => f,
                        None => return err_wrong_args("HSET"),
                    };
                    let value = match extract_bytes(&args[i + 1]) {
                        Some(v) => v,
                        None => return err_wrong_args("HSET"),
                    };
                    // ONE borrowed scan locates the field and overwrites its
                    // value in place. Locating it and then calling
                    // `replace_at` walked the listpack a second time from the
                    // head to reach a position the first walk had already
                    // arrived at (moon#799).
                    if !lp.replace_pair_value(field.as_ref(), value) {
                        lp.push_back(field);
                        lp.push_back(value);
                        count += 1;
                    }
                    i += 2;
                }
                let after = lp.estimate_memory();
                // The upgrade check, from the same authority as the gate.
                let should_upgrade = !limits.listpack_fits(Shape::Hash, lp);
                // `lp`'s borrow of `db` ends here (last use above) — safe to
                // call back into `db` for accounting from this point on.
                settle_hash_listpack_write(db, key, before, after, should_upgrade);
                return Frame::Integer(count);
            }
            Ok(None) => {
                // Already a full HashMap or HashWithTtl -- fall through to standard path
            }
            Err(e) => return e,
        }
    }

    // Full HashMap path (large elements, already upgraded, or HashWithTtl).
    // Collect touched field byte-slices before the mutable borrow of `db`.
    // SmallVec avoids heap allocation for the common ≤8-field case.
    let mut touched: SmallVec<[&[u8]; 8]> = SmallVec::new();
    let mut i = 1;
    while i < args.len() {
        if let Some(f) = extract_bytes(&args[i]) {
            touched.push(f.as_ref());
        }
        i += 2;
    }

    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut new_count: i64 = 0;
    // Net signed byte delta across all fields in this call — O(1) per field
    // (no full-map rescan); applied to `used_memory` once `map`'s borrow ends.
    let mut mem_delta: i64 = 0;
    let mut i = 1;
    while i < args.len() {
        let field = match extract_bytes(&args[i]) {
            Some(f) => f,
            None => return err_wrong_args("HSET"),
        };
        let value = match extract_bytes(&args[i + 1]) {
            Some(v) => v,
            None => return err_wrong_args("HSET"),
        };
        let field_len = field.len();
        let value_len = value.len();
        // moon#1160: stored as exact-size copies, never as slices of the
        // request buffer (see `storage::owned_bytes`).
        match map.insert(detach(field), detach(value)) {
            Some(old_value) => {
                let old_cost = hash_field_cost_len(field_len, old_value.len()) as i64;
                let new_cost = hash_field_cost_len(field_len, value_len) as i64;
                mem_delta += new_cost - old_cost;
            }
            None => {
                new_count += 1;
                mem_delta += hash_field_cost_len(field_len, value_len) as i64;
            }
        }
        i += 2;
    }
    // `map`'s borrow of `db` ends above (last use in the loop) — safe to
    // call back into `db` for accounting and the TTL-sidecar clear below.
    if mem_delta >= 0 {
        db.charge_memory(mem_delta as usize);
    } else {
        db.credit_memory((-mem_delta) as usize);
    }
    // Clear TTL sidecar entries for all touched fields (Valkey: HSET unconditionally
    // persists the field).  No-op for plain Hash; cheap enum-match on HashWithTtl.
    db.hash_clear_field_ttls(key, &touched);
    Frame::Integer(new_count)
}

/// HDEL key field [field ...]
///
/// Removes fields from the hash. Returns the number of fields removed.
/// If the hash becomes empty, the key is removed entirely.
pub fn hdel(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HDEL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("HDEL"),
    };
    // ONE accessor for the whole command (moon#942). The per-field loop this
    // replaces called `hash_delete_field` once per argument, and that method
    // costs TWO DashTable probes — its own `data.get_mut` and, on a real
    // removal, `stamp_hash_field_mutation`'s — so `HDEL h f1 f2 f3` hashed
    // the key six times. Redis pays one `dictFind` for the command and then
    // looks each field up inside the hash, which is what this does.
    //
    // It also fixes what the loop could not express: emptiness was tracked in
    // a variable reassigned on EVERY iteration, so `HDEL h only absent`
    // overwrote the emptiness the real removal reported and the key survived
    // as a hash with zero fields.
    //
    // `SmallVec` keeps the common batch off the heap; the same shape `hset`
    // already uses for its `touched` list.
    let mut fields: SmallVec<[&[u8]; 8]> = SmallVec::new();
    for arg in &args[1..] {
        if let Some(field) = extract_bytes(arg) {
            fields.push(field.as_ref());
        }
    }
    let (count, now_empty) = match db.hash_delete_fields(key, &fields) {
        Ok(r) => r,
        Err(e) => return e,
    };
    // An emptied hash does not outlive its last field.
    if now_empty {
        db.remove(key);
    }
    Frame::Integer(count)
}

/// HMSET key field value [field value ...]
///
/// Sets multiple field-value pairs. Legacy command, always returns OK.
pub fn hmset(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 3 || args.len().is_multiple_of(2) {
        return err_wrong_args("HMSET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMSET"),
    };

    // ONE walk validates (moon#823) and measures (moon#896) — see `hset`.
    let mut max_elem = 0usize;
    for a in &args[1..] {
        match extract_bytes(a) {
            Some(b) => max_elem = max_elem.max(b.len()),
            None => return err_wrong_args("HMSET"),
        }
    }

    // Entry gate from the ONE authority — see `hset`.
    let limits = db.encoding_limits();
    // Fields, not entries — the moon#896 unit, see `hset`.
    if limits.fits(Shape::Hash, Shape::Hash.items_in(args.len() - 1), max_elem) {
        // HashWithTtl returns Ok(None), falling through — same as HSET.
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                // Listpack `estimate_memory()` is O(1) (capacity-based).
                let before = lp.estimate_memory();
                let mut i = 1;
                while i < args.len() {
                    let field = match extract_bytes(&args[i]) {
                        Some(f) => f,
                        None => return err_wrong_args("HMSET"),
                    };
                    let value = match extract_bytes(&args[i + 1]) {
                        Some(v) => v,
                        None => return err_wrong_args("HMSET"),
                    };
                    // One borrowed scan -- see the matching comment in `hset`.
                    if !lp.replace_pair_value(field.as_ref(), value) {
                        lp.push_back(field);
                        lp.push_back(value);
                    }
                    i += 2;
                }
                let after = lp.estimate_memory();
                let should_upgrade = !limits.listpack_fits(Shape::Hash, lp);
                // `lp`'s borrow of `db` ends here.
                settle_hash_listpack_write(db, key, before, after, should_upgrade);
                return ok();
            }
            Ok(None) => {}
            Err(e) => return e,
        }
    }

    // Full HashMap path — collect touched fields before the mutable borrow.
    let mut touched: SmallVec<[&[u8]; 8]> = SmallVec::new();
    let mut i = 1;
    while i < args.len() {
        if let Some(f) = extract_bytes(&args[i]) {
            touched.push(f.as_ref());
        }
        i += 2;
    }

    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut mem_delta: i64 = 0;
    let mut i = 1;
    while i < args.len() {
        let field = match extract_bytes(&args[i]) {
            Some(f) => f,
            None => return err_wrong_args("HMSET"),
        };
        let value = match extract_bytes(&args[i + 1]) {
            Some(v) => v,
            None => return err_wrong_args("HMSET"),
        };
        let field_len = field.len();
        let value_len = value.len();
        // moon#1160: stored as exact-size copies, never as slices of the
        // request buffer (see `storage::owned_bytes`).
        match map.insert(detach(field), detach(value)) {
            Some(old_value) => {
                let old_cost = hash_field_cost_len(field_len, old_value.len()) as i64;
                let new_cost = hash_field_cost_len(field_len, value_len) as i64;
                mem_delta += new_cost - old_cost;
            }
            None => {
                mem_delta += hash_field_cost_len(field_len, value_len) as i64;
            }
        }
        i += 2;
    }
    // `map`'s borrow of `db` ends above.
    if mem_delta >= 0 {
        db.charge_memory(mem_delta as usize);
    } else {
        db.credit_memory((-mem_delta) as usize);
    }
    // Valkey: HMSET clears TTL for every overwritten field.
    db.hash_clear_field_ttls(key, &touched);
    ok()
}

/// HINCRBY key field increment
///
/// Increments the integer value of a hash field by the given number.
/// The TTL on the field (if any) is preserved — `get_or_create_hash` now
/// returns the fields sub-map for HashWithTtl, so the increment lands there
/// without touching the ttls sidecar.
pub fn hincrby(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("HINCRBY");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HINCRBY"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f.clone(),
        None => return err_wrong_args("HINCRBY"),
    };
    let increment: i64 = match extract_bytes(&args[2]) {
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                return Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                ));
            }
        },
        None => return err_wrong_args("HINCRBY"),
    };

    // moon#897: a secondary write must not FLATTEN a small hash.
    //
    // HINCRBY used to reach straight for `get_or_create_hash`, whose contract
    // is an EAGER upgrade to the full `HashMap` — so one HINCRBY on a
    // three-field hash promoted it to `hashtable`, and because nothing
    // demotes (moon#832) it stayed there for the key's lifetime. Redis
    // mutates the listpack in place and promotes only when a threshold is
    // genuinely crossed; `HDEL` was already the only moon hash write that
    // did the same. This is that path.
    //
    // The gate is the ONE authority HSET's gate is (moon#896): one field is
    // at most one new item, and the element this command writes is an
    // `itoa`-rendered `i64`, bounded by `I64_MAX_RENDERED_LEN`. The upgrade
    // check after the mutation is the same authority's `listpack_fits`, so a
    // hash that legitimately outgrows the compact form still promotes.
    let limits = db.encoding_limits();
    if limits.fits(Shape::Hash, 1, field.len().max(I64_MAX_RENDERED_LEN)) {
        // `HashWithTtl` returns `Ok(None)` here and falls through to the
        // HashMap path — a TTL'd hash never compacts back to a listpack, so
        // the per-field TTL sidecar is untouched by this arm.
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                // ONE scan reads the current value AND writes the new one
                // (moon#942). This used to be `pair_value` — a borrowed scan
                // that located the field — followed by `replace_pair_value`,
                // which located the SAME field all over again from the head.
                // That is the defect moon#799 removed for HSET and moon#942
                // removed for ZADD/ZINCRBY; `update_pair_value` is the
                // primitive built for exactly this shape, and HINCRBY is the
                // last hash caller still walking twice.
                //
                // The parse inside the closure is the SAME one the HashMap
                // path below applies, on the same bytes. A listpack `Integer`
                // entry only ever holds a value whose canonical decimal
                // spelling is what the caller wrote (moon#795), so its
                // rendering round-trips; a `Str` entry holds the caller's
                // bytes verbatim and gets the identical `str::parse::<i64>`
                // treatment, error string included.
                //
                // Listpack `estimate_memory()` is O(1) (capacity-based), and
                // the snapshot has to be taken before the scan that writes.
                let before = lp.estimate_memory();
                // `None` out of the closure would mean "leave the pair
                // alone", which HINCRBY never wants; a parse failure is
                // carried out in `parse_failed` instead, and answered after
                // the borrow ends so the error path writes nothing.
                let mut parse_failed = false;
                // moon#952, answered after the closure for the same reason
                // `parse_failed` is: the closure cannot return a Frame.
                let mut overflowed = false;
                // Seeded with the value an ABSENT field produces: Redis
                // treats a missing hash field as 0, so the new value is the
                // increment itself. The closure overwrites it when the field
                // is there.
                let mut new_value = increment;
                let outcome = lp.update_pair_value(field.as_ref(), |current| {
                    let parsed = match current {
                        ListpackRef::Integer(n) => Some(n),
                        ListpackRef::Str(s) => std::str::from_utf8(s)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok()),
                    };
                    match parsed {
                        Some(n) => {
                            // moon#952: a plain `+` wraps in release, so
                            // i64::MAX + 1 was stored as i64::MIN and reported
                            // as success. `INCR` on a string already refuses
                            // this. Declining leaves the field untouched; the
                            // error is answered after the borrow ends.
                            let Some(sum) = n.checked_add(increment) else {
                                overflowed = true;
                                return None;
                            };
                            new_value = sum;
                            // Canonical by construction, so re-encoding it
                            // into the listpack is byte-transparent
                            // (moon#795): `itoa` never emits a leading zero,
                            // a `+` sign, or `-0`. One stack buffer, the
                            // shape `ZINCRBY`'s `ScoreBuf` already uses — the
                            // closure cannot hand back a borrow of its own
                            // capture.
                            Some(render_i64(new_value))
                        }
                        None => {
                            parse_failed = true;
                            None
                        }
                    }
                });
                if overflowed {
                    return Frame::Error(Bytes::from_static(
                        b"ERR increment or decrement would overflow",
                    ));
                }
                if parse_failed {
                    // Answered here, after the scan declined to write: the
                    // listpack is byte-identical to what it was, so a
                    // rejected HINCRBY leaves the value alone.
                    return Frame::Error(Bytes::from_static(b"ERR hash value is not an integer"));
                }
                if matches!(outcome, PairUpdate::Absent) {
                    let rendered = render_i64(new_value);
                    lp.push_back(field.as_ref());
                    lp.push_back(rendered.as_ref());
                }
                let after = lp.estimate_memory();
                let should_upgrade = !limits.listpack_fits(Shape::Hash, lp);
                // `lp`'s borrow of `db` ends here.
                settle_hash_listpack_write(db, key, before, after, should_upgrade);
                return Frame::Integer(new_value);
            }
            Ok(None) => {
                // Already a full HashMap or HashWithTtl — fall through.
            }
            Err(e) => return e,
        }
    }

    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    // ONE probe finds the slot for both the read and the write.
    let slot = map.get_mut(field.as_ref());
    let current = match slot.as_deref() {
        Some(v) => match std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR hash value is not an integer")),
        },
        None => 0,
    };
    // moon#952: the owned arm carried the same plain `+`.
    let Some(new_value) = current.checked_add(increment) else {
        return Frame::Error(Bytes::from_static(
            b"ERR increment or decrement would overflow",
        ));
    };
    let mut ibuf = itoa::Buffer::new();
    let field_len = field.len();
    let new_bytes = Bytes::copy_from_slice(ibuf.format(new_value).as_bytes());
    let new_value_len = new_bytes.len();
    // moon#1160: a NEW field is stored as an exact-size copy, never as a
    // slice of the request buffer; an existing field keeps its stored key.
    let old_value_len = match slot {
        Some(slot) => Some(std::mem::replace(slot, new_bytes).len()),
        None => map.insert(detach(&field), new_bytes).map(|v| v.len()),
    };
    // `map`'s borrow of `db` ends above.
    let new_cost = hash_field_cost_len(field_len, new_value_len) as i64;
    let old_cost = old_value_len.map_or(0, |l| hash_field_cost_len(field_len, l) as i64);
    let mem_delta = new_cost - old_cost;
    if mem_delta >= 0 {
        db.charge_memory(mem_delta as usize);
    } else {
        db.credit_memory((-mem_delta) as usize);
    }
    Frame::Integer(new_value)
}

/// HINCRBYFLOAT key field increment
///
/// Increments the float value of a hash field by the given amount.
/// Returns the new value as a bulk string.
/// The TTL on the field (if any) is preserved — same reasoning as HINCRBY.
pub fn hincrbyfloat(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("HINCRBYFLOAT");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HINCRBYFLOAT"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f.clone(),
        None => return err_wrong_args("HINCRBYFLOAT"),
    };
    let increment: f64 = match extract_bytes(&args[2]) {
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR value is not a valid float")),
        },
        None => return err_wrong_args("HINCRBYFLOAT"),
    };
    // moon#958. HINCRBYFLOAT was the eighth secondary writer, and the one
    // moon#897 missed: it reached straight for the EAGER `get_or_create_hash`,
    // which upgrades on ACCESS, so a single HINCRBYFLOAT flattened a small hash
    // to `hashtable` permanently (nothing demotes). Redis increments inside the
    // listpack and converts only when a threshold is genuinely crossed.
    //
    // The gate is the same ONE authority the siblings use (moon#896): one field
    // is at most one new item. Only the FIELD length is known before the value
    // is computed — a rendered f64 has no useful constant bound, since
    // `format_float` never uses exponent form — so the post-mutation
    // `listpack_fits` below is the real authority, exactly as it is for
    // HINCRBY. A value that renders too long to stay compact promotes there.
    let limits = db.encoding_limits();
    if limits.fits(Shape::Hash, 1, field.len()) {
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                let before = lp.estimate_memory();
                // `None` out of the closure means "leave the pair alone",
                // which is how the parse failure below writes nothing.
                let mut parse_failed = false;
                let mut new_value = increment;
                let outcome = lp.update_pair_value(field.as_ref(), |current| {
                    let parsed = match current {
                        ListpackRef::Integer(n) => Some(n as f64),
                        ListpackRef::Str(b) => std::str::from_utf8(b)
                            .ok()
                            .and_then(|t| t.parse::<f64>().ok()),
                    };
                    match parsed {
                        Some(n) => {
                            new_value = n + increment;
                            Some(format_float(new_value).into_bytes())
                        }
                        None => {
                            parse_failed = true;
                            None
                        }
                    }
                });
                if parse_failed {
                    // The listpack is byte-identical to what it was, so a
                    // rejected HINCRBYFLOAT leaves the value alone.
                    return Frame::Error(Bytes::from_static(
                        b"ERR hash value is not a valid float",
                    ));
                }
                let formatted = format_float(new_value);
                if matches!(outcome, PairUpdate::Absent) {
                    // An absent field starts at 0.0, so the new value is the
                    // increment itself — already in `new_value`.
                    lp.push_back(field.as_ref());
                    lp.push_back(formatted.as_bytes());
                }
                let after = lp.estimate_memory();
                // `listpack_fits` bounds the COUNT, not the width of what was
                // just written, and a rendered f64 has no useful constant
                // bound — `format_float` never uses exponent form, so
                // `1e300 + 1` renders 301 characters. Without this check the
                // gate above (which could only measure the FIELD) would let an
                // over-long value stay compact: measured against redis 8.6.1,
                // which promotes it at `hash-max-listpack-value`.
                let should_upgrade = !limits.listpack_fits(Shape::Hash, lp)
                    || formatted.len() > limits.max_value(Shape::Hash);
                // `lp`'s borrow of `db` ends here.
                settle_hash_listpack_write(db, key, before, after, should_upgrade);
                return Frame::BulkString(Bytes::from(formatted));
            }
            // Already a full HashMap or HashWithTtl — fall through.
            Ok(None) => {}
            Err(e) => return e,
        }
    }

    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let slot = map.get_mut(field.as_ref());
    let current: f64 = match slot.as_deref() {
        Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                return Frame::Error(Bytes::from_static(b"ERR hash value is not a valid float"));
            }
        },
        None => 0.0,
    };
    let new_value = current + increment;
    // Format like Redis: integer-like floats get no decimal, otherwise trim trailing zeros
    let formatted = format_float(new_value);
    let field_len = field.len();
    let new_value_len = formatted.len();
    let new_bytes = Bytes::from(formatted.clone());
    // moon#1160: see HINCRBY.
    let old_value_len = match slot {
        Some(slot) => Some(std::mem::replace(slot, new_bytes).len()),
        None => map.insert(detach(&field), new_bytes).map(|v| v.len()),
    };
    // `map`'s borrow of `db` ends above.
    let new_cost = hash_field_cost_len(field_len, new_value_len) as i64;
    let old_cost = old_value_len.map_or(0, |l| hash_field_cost_len(field_len, l) as i64);
    let mem_delta = new_cost - old_cost;
    if mem_delta >= 0 {
        db.charge_memory(mem_delta as usize);
    } else {
        db.credit_memory((-mem_delta) as usize);
    }
    Frame::BulkString(Bytes::from(formatted))
}

/// Format a float value in Redis style.
/// If the value is an exact integer, format without decimal point.
/// Otherwise, format with necessary precision, trimming trailing zeros.
pub(super) fn format_float(v: f64) -> String {
    if v == v.floor() && v.is_finite() {
        // Check if it fits in i64 range for clean integer formatting
        if v >= i64::MIN as f64 && v <= i64::MAX as f64 {
            return format!("{}", v as i64);
        }
    }
    // Use enough precision and trim trailing zeros
    let s = format!("{:.17}", v);
    let s = s.trim_end_matches('0');
    // Don't leave trailing dot
    let s = s.trim_end_matches('.');
    s.to_string()
}

/// HSETNX key field value
///
/// Sets field only if it does not already exist. Returns 1 if set, 0 if not.
/// Does NOT clear TTL when the field already exists (no write happened).
pub fn hsetnx(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("HSETNX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSETNX"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f.clone(),
        None => return err_wrong_args("HSETNX"),
    };
    let value = match extract_bytes(&args[2]) {
        Some(v) => v.clone(),
        None => return err_wrong_args("HSETNX"),
    };

    // moon#897: same defect, same fix as HINCRBY above — `get_or_create_hash`
    // flattened a three-field hash to `hashtable` on the first HSETNX, for
    // good. The gate is HSET's, for the same single pair: the longer of the
    // field and the value against `hash-max-listpack-value`, one item against
    // `hash-max-listpack-entries`, both from the ONE authority.
    let limits = db.encoding_limits();
    if limits.fits(Shape::Hash, 1, field.len().max(value.len())) {
        // `HashWithTtl` returns `Ok(None)` and falls through — a TTL'd hash
        // never compacts back to a listpack.
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                // The no-op contract, unchanged: an existing field is NOT
                // overwritten, its TTL is NOT cleared, and nothing about the
                // container changes — its encoding included. Returning here
                // before touching the listpack is what makes that true.
                if lp.find_pair_index(field.as_ref()).is_some() {
                    return Frame::Integer(0);
                }
                // Listpack `estimate_memory()` is O(1) (capacity-based).
                let before = lp.estimate_memory();
                // The caller's bytes go in verbatim: the listpack integer
                // encoding is reserved for canonical spellings (moon#795), so
                // `+5`, `007` and ` 7` are stored as strings and read back
                // unchanged.
                lp.push_back(field.as_ref());
                lp.push_back(value.as_ref());
                let after = lp.estimate_memory();
                let should_upgrade = !limits.listpack_fits(Shape::Hash, lp);
                // `lp`'s borrow of `db` ends here.
                settle_hash_listpack_write(db, key, before, after, should_upgrade);
                return Frame::Integer(1);
            }
            Ok(None) => {
                // Already a full HashMap or HashWithTtl — fall through.
            }
            Err(e) => return e,
        }
    }

    // get_or_create_hash is now HashWithTtl-aware: returns fields sub-map.
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    if map.contains_key(&field) {
        Frame::Integer(0)
    } else {
        let cost = hash_field_cost(&field, &value);
        // moon#1160: exact-size copies, not slices of the request buffer.
        map.insert(detach(&field), detach(&value));
        // `map`'s borrow of `db` ends above — field was absent, so this is a
        // pure charge (no prior cost to net out).
        db.charge_memory(cost);
        Frame::Integer(1)
    }
}

// ── HEXPIRE-family — Valkey 9.0 per-field TTL write commands ─────────────────

/// Parsed arguments for the HEXPIRE family.
struct HexpireArgs<'a> {
    key: &'a [u8],
    /// Absolute deadline in unix-milliseconds (already converted from the
    /// wire format).
    abs_ms: u64,
    cond: HashTtlCond,
    /// Field names to operate on.
    fields: SmallVec<[&'a [u8]; 4]>,
}

// ---------------------------------------------------------------------------
// Shared parse helper for HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME / HPERSIST
// ---------------------------------------------------------------------------

/// Parsed result of the `key FIELDS numfields field [field ...]` wire layout
/// used by all five phase-198 commands (no condition flag, no time argument).
pub(super) struct KeyAndFields<'a> {
    /// The hash key.
    pub key: &'a [u8],
    /// Field names in the order given on the wire.
    pub fields: SmallVec<[&'a [u8]; 4]>,
}

/// Parse `key FIELDS numfields field [field ...]` from `args`.
///
/// `cmd` is the command name used in error messages (e.g. `"HTTL"`).
///
/// # Wire layout
/// ```text
/// CMD key FIELDS numfields field [field ...]
/// ^-- already consumed; args starts at 'key'
/// ```
///
/// Returns `Err(Frame::Error(_))` on any parse or validation failure.
pub(super) fn parse_key_and_fields<'a>(
    args: &'a [Frame],
    cmd: &'static str,
) -> Result<KeyAndFields<'a>, Frame> {
    // Minimum: key + FIELDS + numfields + ≥1 field = 4 elements.
    if args.len() < 4 {
        return Err(crate::command::helpers::err_wrong_args(cmd));
    }

    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => {
            return Err(crate::command::helpers::err_wrong_args(cmd));
        }
    };

    // args[1] must be "FIELDS" (case-insensitive).
    let fields_kw = match extract_bytes(&args[1]) {
        Some(t) => t,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR syntax error, FIELDS keyword not found",
            )));
        }
    };
    if !fields_kw.eq_ignore_ascii_case(b"FIELDS") {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR syntax error, FIELDS keyword not found",
        )));
    }

    // args[2] = numfields
    let numfields: usize = match extract_bytes(&args[2]) {
        Some(b) => match std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )));
            }
        },
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };

    if numfields == 0 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Parameter `numFields` should be greater than 0",
        )));
    }

    let first_field_pos = 3; // args[3..3+numfields]
    let actual_fields = args.len().saturating_sub(first_field_pos);
    if actual_fields < numfields {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Parameter `numFields` is more than number of arguments",
        )));
    }

    let mut fields: SmallVec<[&'a [u8]; 4]> = SmallVec::new();
    for i in first_field_pos..first_field_pos + numfields {
        match extract_bytes(&args[i]) {
            Some(f) => fields.push(f.as_ref()),
            None => {
                return Err(Frame::Error(Bytes::from_static(b"ERR invalid field name")));
            }
        }
    }

    Ok(KeyAndFields { key, fields })
}

/// Parse the wire format shared by HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT:
///
/// ```text
/// HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
/// ```
///
/// `expects_ms`  — when `true` the `when` argument is already in milliseconds
///                 (HPEXPIRE / HPEXPIREAT).
/// `expects_abs` — when `true` the `when` argument is an absolute unix
///                 timestamp (HEXPIREAT / HPEXPIREAT); otherwise it is a
///                 relative offset from `now_ms`.
fn parse_hexpire_args<'a>(
    args: &'a [Frame],
    now_ms: u64,
    expects_ms: bool,
    expects_abs: bool,
) -> Result<HexpireArgs<'a>, Frame> {
    // Minimum wire layout (no condition flag):
    //   args[0] key  args[1] when  args[2] FIELDS  args[3] numfields  args[4+] fields
    if args.len() < 5 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'hexpire' command",
        )));
    }

    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR invalid key argument",
            )));
        }
    };

    // Parse `when` as i64 (negative values → past expiry → code 2).
    let when_val: i64 = match extract_bytes(&args[1]) {
        Some(b) => match std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )));
            }
        },
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };

    // Compute abs_ms using saturating i128 arithmetic to avoid overflow on
    // extreme values before clamping to u64.
    let when_ms_i128: i128 = if expects_ms {
        when_val as i128
    } else {
        (when_val as i128).saturating_mul(1000)
    };
    let abs_ms_i128: i128 = if expects_abs {
        when_ms_i128
    } else {
        (now_ms as i128).saturating_add(when_ms_i128)
    };
    let abs_ms: u64 = abs_ms_i128.clamp(0, u64::MAX as i128) as u64;

    // Scan args[2..] for an optional condition flag then FIELDS keyword.
    // Valid layouts:
    //   [FIELDS numfields field...]          — no condition
    //   [NX|XX|GT|LT FIELDS numfields field...] — one condition
    //   [NX XX ...] — mutual-exclusion error
    let mut cond = HashTtlCond::Always;
    let mut cond_count = 0u8;
    let mut fields_keyword_pos: Option<usize> = None; // index into args

    for pos in 2..args.len() {
        if let Some(tok) = extract_bytes(&args[pos]) {
            if tok.eq_ignore_ascii_case(b"FIELDS") {
                fields_keyword_pos = Some(pos);
                break;
            }
            // Must be a condition flag.
            let c = if tok.eq_ignore_ascii_case(b"NX") {
                HashTtlCond::Nx
            } else if tok.eq_ignore_ascii_case(b"XX") {
                HashTtlCond::Xx
            } else if tok.eq_ignore_ascii_case(b"GT") {
                HashTtlCond::Gt
            } else if tok.eq_ignore_ascii_case(b"LT") {
                HashTtlCond::Lt
            } else {
                return Err(Frame::Error(Bytes::from(format!(
                    "ERR unsupported option '{}'",
                    String::from_utf8_lossy(tok)
                ))));
            };
            cond_count += 1;
            if cond_count > 1 {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR NX, XX, GT, and LT options at the same time are not compatible",
                )));
            }
            cond = c;
        }
    }

    let fkw_pos = match fields_keyword_pos {
        Some(p) => p,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR syntax error, FIELDS keyword not found",
            )));
        }
    };

    // args[fkw_pos+1] = numfields
    let numfields_pos = fkw_pos + 1;
    if numfields_pos >= args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'hexpire' command",
        )));
    }
    let numfields: usize = match extract_bytes(&args[numfields_pos]) {
        Some(b) => match std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )));
            }
        },
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };

    if numfields == 0 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Parameter `numFields` should be greater than 0",
        )));
    }

    let first_field_pos = numfields_pos + 1;
    let actual_fields = args.len().saturating_sub(first_field_pos);
    if actual_fields < numfields {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Parameter `numFields` is more than number of arguments",
        )));
    }

    let mut fields: SmallVec<[&'a [u8]; 4]> = SmallVec::new();
    for i in first_field_pos..first_field_pos + numfields {
        match extract_bytes(&args[i]) {
            Some(f) => fields.push(f.as_ref()),
            None => {
                return Err(Frame::Error(Bytes::from_static(b"ERR invalid field name")));
            }
        }
    }

    Ok(HexpireArgs {
        key,
        abs_ms,
        cond,
        fields,
    })
}

/// Core executor for HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT.
///
/// Calls `hash_set_field_ttl` for each field in order, collects result codes
/// into a RESP Array of integers.  On WRONGTYPE returns the error immediately.
fn do_hexpire(db: &mut Database, args: &[Frame], expects_ms: bool, expects_abs: bool) -> Frame {
    let now_ms = db.now_ms();
    let parsed = match parse_hexpire_args(args, now_ms, expects_ms, expects_abs) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let mut codes: Vec<Frame> = Vec::with_capacity(parsed.fields.len());
    for field in &parsed.fields {
        match db.hash_set_field_ttl(parsed.key, field, parsed.abs_ms, parsed.cond) {
            Ok(code) => codes.push(Frame::Integer(code)),
            Err(_wrong_type) => {
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        }
    }

    Frame::Array(FrameVec::from_vec(codes))
}

/// HEXPIRE key seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
pub fn hexpire(db: &mut Database, args: &[Frame]) -> Frame {
    do_hexpire(db, args, false, false)
}

/// HPEXPIRE key milliseconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
pub fn hpexpire(db: &mut Database, args: &[Frame]) -> Frame {
    do_hexpire(db, args, true, false)
}

/// HEXPIREAT key unix-time-seconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
pub fn hexpireat(db: &mut Database, args: &[Frame]) -> Frame {
    do_hexpire(db, args, false, true)
}

/// HPEXPIREAT key unix-time-milliseconds [NX|XX|GT|LT] FIELDS numfields field [field ...]
pub fn hpexpireat(db: &mut Database, args: &[Frame]) -> Frame {
    do_hexpire(db, args, true, true)
}

/// HGETDEL key FIELDS numfields field [field ...]
///
/// Atomically returns the value(s) of the specified fields and deletes them
/// from the hash in a single operation. Returns a RESP Array with one entry
/// per requested field: `BulkString(value)` when the field existed, or
/// `Null` when the key or field was absent.
///
/// If the hash becomes empty after all deletes the key is removed entirely.
///
/// # Atomicity
/// Guaranteed by per-shard single-threaded execution — no explicit locking
/// is needed. No client can observe the partial state between reads and
/// deletes within this call.
pub fn hgetdel(db: &mut Database, args: &[Frame]) -> Frame {
    let parsed = match parse_key_and_fields(args, "HGETDEL") {
        Ok(p) => p,
        Err(e) => return e,
    };

    // Upfront WRONGTYPE check: attempt to read the hash before any mutation.
    // Avoids partially processing fields before surfacing the type error.
    {
        let now_ms = db.now_ms();
        match db.get_hash_ref_if_alive(parsed.key, now_ms) {
            Ok(_) => {}
            Err(e) => return e,
        }
    }

    let mut results: Vec<Frame> = Vec::with_capacity(parsed.fields.len());
    for field in &parsed.fields {
        match db.hash_get_and_delete_field(parsed.key, field) {
            Ok(Some(v)) => results.push(Frame::BulkString(v)),
            Ok(None) => results.push(Frame::Null),
            Err(_wrong_type) => {
                // Should not be reachable after the upfront check, but keep
                // the error path for safety.
                return Frame::Error(Bytes::from_static(
                    b"WRONGTYPE Operation against a key holding the wrong kind of value",
                ));
            }
        }
    }

    // Remove the key if the hash has become empty after all deletes.
    db.cleanup_empty_hash(parsed.key);

    Frame::Array(results.into())
}

// ── HGETEX — Valkey 9.1 atomic get-with-TTL-update command ───────────────────

/// TTL-update mode for `HGETEX`.
///
/// Parsed once before the per-field loop so the match is lifted out of the
/// hot path.  `None` is the fast path (no TTL mutation).
#[derive(Debug, Clone, Copy)]
enum HgetexMode {
    /// No TTL change — pure read.
    NoOp,
    /// Relative seconds from now.
    Ex(i64),
    /// Relative milliseconds from now.
    Px(i64),
    /// Absolute unix-seconds.
    ExAt(i64),
    /// Absolute unix-milliseconds.
    PxAt(i64),
    /// Remove any existing TTL on the field.
    Persist,
}

/// Parse `key [EX s | PX ms | EXAT us | PXAT ums | PERSIST] FIELDS numfields field [...]`.
///
/// Returns `(key, mode, fields)` or a `Frame::Error` on any parse failure.
/// Mode tokens are mutually exclusive — duplicate or conflicting tokens return
/// `ERR syntax error`.
fn parse_hgetex_args<'a>(args: &'a [Frame]) -> Result<HgetexParsed<'a>, Frame> {
    // Minimum: key + FIELDS + numfields + ≥1 field = 4 elements.
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'hgetex' command",
        )));
    }

    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'hgetex' command",
            )));
        }
    };

    // Scan args[1..] for optional mode token(s) then FIELDS keyword.
    // Valid layouts:
    //   key FIELDS numfields field ...
    //   key EX s FIELDS numfields field ...
    //   key PX ms FIELDS numfields field ...
    //   key EXAT us FIELDS numfields field ...
    //   key PXAT ums FIELDS numfields field ...
    //   key PERSIST FIELDS numfields field ...
    let mut mode = HgetexMode::NoOp;
    let mut mode_count = 0u8;
    let mut fields_keyword_pos: Option<usize> = None;

    let mut i = 1usize;
    while i < args.len() {
        let tok = match extract_bytes(&args[i]) {
            Some(t) => t,
            None => {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
        };

        if tok.eq_ignore_ascii_case(b"FIELDS") {
            fields_keyword_pos = Some(i);
            break;
        }

        if tok.eq_ignore_ascii_case(b"PERSIST") {
            mode_count += 1;
            if mode_count > 1 {
                return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
            }
            mode = HgetexMode::Persist;
            i += 1;
            continue;
        }

        // EX / PX / EXAT / PXAT all expect a numeric argument next.
        let next_mode = if tok.eq_ignore_ascii_case(b"EX") {
            Some(HgetexMode::Ex(0))
        } else if tok.eq_ignore_ascii_case(b"PX") {
            Some(HgetexMode::Px(0))
        } else if tok.eq_ignore_ascii_case(b"EXAT") {
            Some(HgetexMode::ExAt(0))
        } else if tok.eq_ignore_ascii_case(b"PXAT") {
            Some(HgetexMode::PxAt(0))
        } else {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        };

        mode_count += 1;
        if mode_count > 1 {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        }

        // Consume the numeric argument.
        i += 1;
        if i >= args.len() {
            return Err(Frame::Error(Bytes::from_static(b"ERR syntax error")));
        }
        let num_tok = match extract_bytes(&args[i]) {
            Some(t) => t,
            None => {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )));
            }
        };
        let n: i64 = match std::str::from_utf8(num_tok)
            .ok()
            .and_then(|s| s.parse().ok())
        {
            Some(v) => v,
            None => {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )));
            }
        };
        mode = match next_mode {
            Some(HgetexMode::Ex(_)) => HgetexMode::Ex(n),
            Some(HgetexMode::Px(_)) => HgetexMode::Px(n),
            Some(HgetexMode::ExAt(_)) => HgetexMode::ExAt(n),
            Some(HgetexMode::PxAt(_)) => HgetexMode::PxAt(n),
            _ => unreachable!(),
        };
        i += 1;
    }

    let fkw_pos = match fields_keyword_pos {
        Some(p) => p,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR syntax error, FIELDS keyword not found",
            )));
        }
    };

    // args[fkw_pos+1] = numfields
    let numfields_pos = fkw_pos + 1;
    if numfields_pos >= args.len() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'hgetex' command",
        )));
    }
    let numfields: usize = match extract_bytes(&args[numfields_pos]) {
        Some(b) => match std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(n) => n,
            None => {
                return Err(Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )));
            }
        },
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };

    if numfields == 0 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Parameter `numFields` should be greater than 0",
        )));
    }

    let first_field_pos = numfields_pos + 1;
    let actual_fields = args.len().saturating_sub(first_field_pos);
    if actual_fields < numfields {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR Parameter `numFields` is more than number of arguments",
        )));
    }

    let mut fields: SmallVec<[&'a [u8]; 4]> = SmallVec::new();
    for j in first_field_pos..first_field_pos + numfields {
        match extract_bytes(&args[j]) {
            Some(f) => fields.push(f.as_ref()),
            None => {
                return Err(Frame::Error(Bytes::from_static(b"ERR invalid field name")));
            }
        }
    }

    Ok(HgetexParsed { key, mode, fields })
}

/// Parsed HGETEX arguments.
struct HgetexParsed<'a> {
    key: &'a [u8],
    mode: HgetexMode,
    fields: SmallVec<[&'a [u8]; 4]>,
}

/// HGETEX key [EX s | PX ms | EXAT unix-s | PXAT unix-ms | PERSIST] FIELDS numfields field [...]
///
/// Atomically returns the value(s) of the specified fields and optionally
/// updates (or removes) their per-field TTLs in a single operation.
///
/// Returns a RESP Array with one entry per requested field:
/// - `BulkString(value)` when the field exists and is not expired.
/// - `Null` when the key, field is absent or already expired.
///
/// TTL mode semantics (only applied when the field is found and live):
/// - `EX s`        — set relative expiry in seconds from now.
/// - `PX ms`       — set relative expiry in milliseconds from now.
/// - `EXAT unix-s` — set absolute expiry as unix-seconds.
/// - `PXAT unix-ms`— set absolute expiry as unix-milliseconds.
/// - `PERSIST`     — remove any existing per-field TTL.
/// - (none)        — pure read; no TTL change.
///
/// # Atomicity
/// Guaranteed by per-shard single-threaded execution — no explicit locking
/// is needed. The read and optional TTL-update are a single atomic unit.
pub fn hgetex(db: &mut Database, args: &[Frame]) -> Frame {
    let parsed = match parse_hgetex_args(args) {
        Ok(p) => p,
        Err(e) => return e,
    };

    // Upfront WRONGTYPE check via immutable borrow before any mutation.
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(parsed.key, now_ms) {
        Ok(_) => {}
        Err(e) => return e,
    }

    // Compute the absolute expiry deadline once (outside the per-field loop)
    // for the EX/PX/EXAT/PXAT modes.  Use saturating i128 arithmetic to
    // avoid overflow on extreme values, then clamp to u64 — mirrors the
    // parse_hexpire_args approach from phase 196.
    let abs_ms_opt: Option<u64> = match parsed.mode {
        HgetexMode::NoOp | HgetexMode::Persist => None,
        HgetexMode::Ex(s) => {
            let ms = (now_ms as i128).saturating_add((s as i128).saturating_mul(1_000));
            Some(ms.clamp(0, u64::MAX as i128) as u64)
        }
        HgetexMode::Px(ms) => {
            let abs = (now_ms as i128).saturating_add(ms as i128);
            Some(abs.clamp(0, u64::MAX as i128) as u64)
        }
        HgetexMode::ExAt(s) => {
            let ms = (s as i128).saturating_mul(1_000);
            Some(ms.clamp(0, u64::MAX as i128) as u64)
        }
        HgetexMode::PxAt(ms) => Some((ms as i128).clamp(0, u64::MAX as i128) as u64),
    };

    let mut results: Vec<Frame> = Vec::with_capacity(parsed.fields.len());
    for field in &parsed.fields {
        // Read the live value first via the read-only path (respects lazy TTL
        // filtering for HashWithTtl without triggering a mutable borrow).
        let value = {
            // Re-borrow immutably each iteration — the previous iteration's
            // mutable TTL write (if any) has already dropped.
            match db.get_hash_ref_if_alive(parsed.key, now_ms) {
                Ok(Some(href)) => href.get_field(field),
                Ok(None) => None,
                Err(_) => None, // type-checked above; unreachable in practice
            }
        };

        match value {
            None => {
                // Field missing or expired — push Null; do NOT touch TTL.
                results.push(Frame::Null);
            }
            Some(v) => {
                // Apply TTL mutation only for live fields.
                match parsed.mode {
                    HgetexMode::NoOp => {}
                    HgetexMode::Persist => {
                        db.hash_persist_field(parsed.key, field);
                    }
                    _ => {
                        // All TTL-setting modes share the same path via abs_ms_opt.
                        if let Some(abs_ms) = abs_ms_opt {
                            // Ignore result code — field existence already confirmed.
                            let _ = db.hash_set_field_ttl(
                                parsed.key,
                                field,
                                abs_ms,
                                HashTtlCond::Always,
                            );
                        }
                    }
                }
                results.push(Frame::BulkString(v));
            }
        }
    }

    Frame::Array(results.into())
}

/// HPERSIST key FIELDS numfields field [field ...]
///
/// Removes the per-field TTL from each named field.  Returns a RESP Array
/// with one integer per field using Valkey 9.0 semantics:
/// - `-2` — field does not exist in the hash (or key does not exist)
/// - `-1` — field exists but has no TTL
/// -  `1` — TTL was present and has been removed
///
/// **Downgrade behaviour**: when the last per-field TTL is removed, the
/// encoding is automatically downgraded from `HashWithTtl` back to plain
/// `Hash` by `hash_persist_field` (implemented in phase 195 — no reimplementation
/// needed here).
///
/// **Short-circuit optimisation**: if the key is missing or is a plain `Hash`
/// / `HashListpack` (i.e. no TTL sidecar at all), every field unconditionally
/// maps to `-2` (missing) or `-1` (no TTL), so we skip the per-field
/// `hash_persist_field` call entirely.
pub fn hpersist(db: &mut Database, args: &[Frame]) -> Frame {
    let parsed = match parse_key_and_fields(args, "HPERSIST") {
        Ok(p) => p,
        Err(e) => return e,
    };

    // WRONGTYPE probe: borrow as read-only to check the key type before
    // any mutation.  `get_hash_ref_if_alive` returns:
    //   Ok(None)   — key missing or whole-key TTL expired → all -2
    //   Ok(Some(_)) — hash variant (Hash, HashListpack, HashWithTtl)
    //   Err(frame) — wrong type → propagate
    let now_ms = db.now_ms();
    let href_check = match db.get_hash_ref_if_alive(parsed.key, now_ms) {
        Ok(h) => h,
        Err(e) => return e,
    };

    // Missing key: all fields are -2.
    let Some(_href) = href_check else {
        let codes: Vec<Frame> = parsed.fields.iter().map(|_| Frame::Integer(-2)).collect();
        return Frame::Array(FrameVec::from_vec(codes));
    };

    // For each field, determine state then call hash_persist_field when needed.
    let mut codes: Vec<Frame> = Vec::with_capacity(parsed.fields.len());
    for field in &parsed.fields {
        // Re-read field state after each mutation so that the downgrade is
        // visible to subsequent fields in the same call.
        use crate::storage::db::FieldState;
        let state = db.hash_field_state(parsed.key, field, now_ms);
        let code = match state {
            FieldState::Missing => -2i64,
            FieldState::NoTtl => -1i64,
            FieldState::Ttl(_) => {
                db.hash_persist_field(parsed.key, field);
                1i64
            }
        };
        codes.push(Frame::Integer(code));
    }

    Frame::Array(FrameVec::from_vec(codes))
}
