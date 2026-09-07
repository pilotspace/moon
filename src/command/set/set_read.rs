use bytes::Bytes;
use rand::RngExt;
use std::collections::HashSet;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use super::{glob_match, parse_int};
use crate::command::helpers::{err_wrong_args, extract_bytes};

// ---------------------------------------------------------------------------
// SMEMBERS key
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// moon#832 -- the mutable dispatch path reads through the SHARED one
//
// Every handler below is a pure read, and every one used to reach the set
// through `Database::get_set`, whose `get_promoted` core calls `K::upgrade`
// unconditionally. That conversion is one-way -- nothing ever downgrades --
// so a single SCARD taken on the mutable path (inside MULTI/EXEC, inside a
// Lua script, or from `try_inline_dispatch`) permanently flattened an
// `intset`/`listpack` set to a `hashtable` for the rest of its life.
// Measured on the unmodified binary: 1000 eight-member integer sets went
// 333,055 -> 1,149,055 bytes of `used_memory` (3.45x) after ONE `SCARD` each,
// and `OBJECT ENCODING` went `intset -> hashtable`.
//
// The fix is to take the read through `&Database`. That is not a convention:
// a shared borrow CANNOT reach `K::upgrade`, so the compiler enforces the
// invariant that reading does not rewrite. It also collapses the two
// implementations of every one of these commands into one, which removes the
// divergence #610 came from -- the answer no longer depends on which of
// moon's three dispatch paths the command took.
//
// What is deliberately NOT preserved: the mutable path used to reclaim an
// expired key and to promote a cold-tier hit back into hot RAM as a side
// effect of the read. Neither is a correctness property -- `get_ref_if_alive`
// still treats an expired key as absent and still reads the cold tier
// through -- and the hash family (`hash_read.rs`) has shipped exactly this
// shape since it moved to `get_hash_ref_if_alive`. Active expiry and the
// write paths still reclaim.
// ---------------------------------------------------------------------------
/// SMEMBERS key -- all members of a set.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn smembers(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    smembers_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SCARD key
// ---------------------------------------------------------------------------

/// SCARD key -- cardinality of a set.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn scard(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    scard_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SISMEMBER key member
// ---------------------------------------------------------------------------

/// SISMEMBER key member -- membership test.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn sismember(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    sismember_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SMISMEMBER key member [member ...]
// ---------------------------------------------------------------------------

/// SMISMEMBER key member [member ...] -- multi membership test.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn smismember(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    smismember_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SINTER key [key ...]
// ---------------------------------------------------------------------------

/// SINTER key [key ...] -- intersection of all sets.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn sinter(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    sinter_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SUNION key [key ...]
// ---------------------------------------------------------------------------

/// SUNION key [key ...] -- union of all sets.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn sunion(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    sunion_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SDIFF key [key ...]
// ---------------------------------------------------------------------------

/// SDIFF key [key ...] -- first set minus all others.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn sdiff(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    sdiff_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SRANDMEMBER key [count]
// ---------------------------------------------------------------------------

/// SRANDMEMBER key [count] -- random member(s), no removal.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn srandmember(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    srandmember_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// SSCAN key cursor [MATCH pattern] [COUNT count]
// ---------------------------------------------------------------------------

/// SSCAN key cursor [MATCH pattern] [COUNT count] -- iterate members.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn sscan(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    sscan_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// Collect sets read-only (no mutation, no expiry removal).
fn collect_sets_readonly(
    db: &Database,
    keys: &[&Bytes],
    now_ms: u64,
) -> Result<Vec<Option<HashSet<Bytes>>>, Frame> {
    let mut sets = Vec::with_capacity(keys.len());
    for key in keys {
        match db.get_set_ref_if_alive(key, now_ms) {
            Ok(Some(sref)) => sets.push(Some(sref.to_hash_set())),
            Ok(None) => sets.push(None),
            Err(e) => return Err(e),
        }
    }
    Ok(sets)
}

/// SMEMBERS (read-only).
pub fn smembers_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("SMEMBERS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SMEMBERS"),
    };
    match db.get_set_ref_if_alive(key, now_ms) {
        Ok(Some(sref)) => {
            let members: Vec<Frame> = sref.members().into_iter().map(Frame::BulkString).collect();
            Frame::Array(members.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// SCARD (read-only).
pub fn scard_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("SCARD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SCARD"),
    };
    match db.get_set_ref_if_alive(key, now_ms) {
        Ok(Some(sref)) => Frame::Integer(sref.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// SISMEMBER (read-only).
pub fn sismember_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("SISMEMBER");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SISMEMBER"),
    };
    let member = match extract_bytes(&args[1]) {
        Some(m) => m,
        None => return err_wrong_args("SISMEMBER"),
    };
    match db.get_set_ref_if_alive(key, now_ms) {
        Ok(Some(sref)) => {
            if sref.contains(member) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// SMISMEMBER (read-only).
pub fn smismember_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SMISMEMBER");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SMISMEMBER"),
    };
    match db.get_set_ref_if_alive(key, now_ms) {
        Ok(maybe_sref) => {
            let results: Vec<Frame> = args[1..]
                .iter()
                .map(|arg| {
                    let member = extract_bytes(arg);
                    match (&maybe_sref, member) {
                        (Some(sref), Some(m)) => {
                            if sref.contains(m) {
                                Frame::Integer(1)
                            } else {
                                Frame::Integer(0)
                            }
                        }
                        _ => Frame::Integer(0),
                    }
                })
                .collect();
            Frame::Array(results.into())
        }
        Err(e) => e,
    }
}

/// SINTER (read-only).
pub fn sinter_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SINTER");
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return err_wrong_args("SINTER");
    }
    let sets = match collect_sets_readonly(db, &keys, now_ms) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut concrete: Vec<HashSet<Bytes>> = Vec::new();
    for s in sets {
        match s {
            Some(set) => concrete.push(set),
            None => return Frame::Array(framevec![]),
        }
    }
    if concrete.is_empty() {
        return Frame::Array(framevec![]);
    }
    concrete.sort_by_key(|s| s.len());
    let mut result = concrete[0].clone();
    for other in &concrete[1..] {
        result.retain(|m| other.contains(m));
    }
    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members.into())
}

/// SUNION (read-only).
pub fn sunion_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SUNION");
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return err_wrong_args("SUNION");
    }
    let sets = match collect_sets_readonly(db, &keys, now_ms) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut result = HashSet::new();
    for s in sets {
        if let Some(set) = s {
            result.extend(set);
        }
    }
    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members.into())
}

/// SDIFF (read-only).
pub fn sdiff_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SDIFF");
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return err_wrong_args("SDIFF");
    }
    let sets = match collect_sets_readonly(db, &keys, now_ms) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut result = match &sets[0] {
        Some(set) => set.clone(),
        None => return Frame::Array(framevec![]),
    };
    for s in &sets[1..] {
        if let Some(set) = s {
            result.retain(|m| !set.contains(m));
        }
    }
    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members.into())
}

/// SRANDMEMBER (read-only).
pub fn srandmember_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("SRANDMEMBER");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SRANDMEMBER"),
    };
    // `sref.members()` clones EVERY member into a fresh Vec; calling it to pick
    // one was the read-only half of the same O(n) defect as the mutable path.
    // `SetRef::nth` addresses a member directly on all four representations.
    let sref = match db.get_set_ref_if_alive(key, now_ms) {
        Ok(Some(sref)) => sref,
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };
    let len = sref.len();
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
        return match sref.nth(idx) {
            Some(m) => Frame::BulkString(m),
            None => Frame::Null,
        };
    }
    let count = match parse_int(&args[1]) {
        Some(c) => c,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    if count == 0 {
        return Frame::Array(framevec![]);
    }
    if count > 0 {
        // O(n) in the COUNT requested, not in the size of the set.
        let n = std::cmp::min(count as usize, len);
        let chosen: Vec<Frame> = rand::seq::index::sample(&mut rng, len, n)
            .into_iter()
            .filter_map(|i| sref.nth(i).map(Frame::BulkString))
            .collect();
        Frame::Array(chosen.into())
    } else {
        // DoS guard: refuse a huge negative COUNT loudly instead of letting it
        // drive an unbounded Vec::with_capacity -> allocator abort. Within the
        // cap, Redis semantics apply: exactly |COUNT| elements, duplicates ok.
        let n = count.unsigned_abs() as usize;
        if n > crate::command::RAND_DUP_COUNT_MAX {
            return Frame::Error(Bytes::from_static(crate::command::ERR_RAND_COUNT_RANGE));
        }
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            if let Some(m) = sref.nth(rng.random_range(0..len)) {
                result.push(Frame::BulkString(m));
            }
        }
        Frame::Array(result.into())
    }
}

/// SSCAN (read-only).
pub fn sscan_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SSCAN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SSCAN"),
    };
    let cursor: usize = match extract_bytes(&args[1])
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
    };
    // One parser for the whole family — see `command::scan_options`.
    let opts = match crate::command::scan_options::parse_scan_options(
        crate::command::scan_options::ScanKind::Set,
        &args[2..],
    ) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let match_pattern = opts.pattern;
    let count = opts.count;
    let members: Vec<Bytes> = match db.get_set_ref_if_alive(key, now_ms) {
        Ok(Some(sref)) => {
            let mut v = sref.members();
            v.sort();
            v
        }
        Ok(None) => vec![],
        Err(e) => return e,
    };
    let total = members.len();
    let mut results = Vec::new();
    let mut pos = cursor;
    let mut checked = 0;
    while pos < total && checked < count {
        let member = &members[pos];
        pos += 1;
        checked += 1;
        if let Some(pattern) = match_pattern {
            if !glob_match(pattern, member) {
                continue;
            }
        }
        results.push(Frame::BulkString(member.clone()));
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
// SINTERCARD numkeys key [key ...] [LIMIT limit]
// ---------------------------------------------------------------------------

/// SINTERCARD numkeys key [key ...] [LIMIT limit] -- size of the intersection.
///
/// Reads through the shared-borrow implementation so the set's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn sintercard(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    sintercard_readonly(db, args, now_ms)
}

/// SINTERCARD readonly path
pub fn sintercard_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SINTERCARD");
    }
    let numkeys = match parse_int(&args[0]) {
        Some(n) if n > 0 => n as usize,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR numkeys can't be non-positive value",
            ));
        }
    };
    if args.len() < 1 + numkeys {
        return err_wrong_args("SINTERCARD");
    }

    let mut limit: usize = 0;
    let remaining = &args[1 + numkeys..];
    if remaining.len() >= 2 {
        let kw = match extract_bytes(&remaining[0]) {
            Some(b) => b,
            None => return Frame::Error(Bytes::from_static(b"ERR syntax error")),
        };
        if kw.eq_ignore_ascii_case(b"LIMIT") {
            match parse_int(&remaining[1]) {
                Some(l) if l >= 0 => limit = l as usize,
                _ => {
                    return Frame::Error(Bytes::from_static(b"ERR LIMIT can't be negative"));
                }
            }
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    } else if !remaining.is_empty() {
        return Frame::Error(Bytes::from_static(b"ERR syntax error"));
    }

    let key_frames = &args[1..1 + numkeys];
    let keys: Vec<&Bytes> = key_frames.iter().filter_map(extract_bytes).collect();
    if keys.len() != numkeys {
        return err_wrong_args("SINTERCARD");
    }

    // Use readonly path to get sets
    let mut concrete: Vec<HashSet<Bytes>> = Vec::new();
    for key in &keys {
        match db.get_set_ref_if_alive(key, now_ms) {
            Ok(Some(sref)) => concrete.push(sref.members().into_iter().collect()),
            Ok(None) => return Frame::Integer(0),
            Err(e) => return e,
        }
    }

    if concrete.is_empty() {
        return Frame::Integer(0);
    }

    concrete.sort_by_key(|s| s.len());
    let smallest = &concrete[0];
    let rest = &concrete[1..];

    let mut count: usize = 0;
    for member in smallest {
        if rest.iter().all(|s| s.contains(member)) {
            count += 1;
            if limit > 0 && count >= limit {
                break;
            }
        }
    }

    Frame::Integer(count as i64)
}
