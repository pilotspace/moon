use bytes::Bytes;
use rand::seq::IndexedRandom;
use std::collections::HashSet;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use super::{collect_sets, glob_match, parse_int};
use crate::command::helpers::{err_wrong_args, extract_bytes};

// ---------------------------------------------------------------------------
// SMEMBERS key
// ---------------------------------------------------------------------------

/// SMEMBERS command handler: return all members of a set.
pub fn smembers(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("SMEMBERS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SMEMBERS"),
    };
    match db.get_set(key) {
        Ok(Some(set)) => {
            let members: Vec<Frame> = set.iter().map(|m| Frame::BulkString(m.clone())).collect();
            Frame::Array(members.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// SCARD key
// ---------------------------------------------------------------------------

/// SCARD command handler: return the cardinality of a set.
pub fn scard(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("SCARD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SCARD"),
    };
    match db.get_set(key) {
        Ok(Some(set)) => Frame::Integer(set.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// SISMEMBER key member
// ---------------------------------------------------------------------------

/// SISMEMBER command handler: check if member is in a set.
pub fn sismember(db: &mut Database, args: &[Frame]) -> Frame {
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
    match db.get_set(key) {
        Ok(Some(set)) => {
            if set.contains(member) {
                Frame::Integer(1)
            } else {
                Frame::Integer(0)
            }
        }
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// SMISMEMBER key member [member ...]
// ---------------------------------------------------------------------------

/// SMISMEMBER command handler: check if multiple members are in a set.
pub fn smismember(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SMISMEMBER");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SMISMEMBER"),
    };
    match db.get_set(key) {
        Ok(maybe_set) => {
            let results: Vec<Frame> = args[1..]
                .iter()
                .map(|arg| {
                    let member = extract_bytes(arg);
                    match (maybe_set, member) {
                        (Some(set), Some(m)) => {
                            if set.contains(m) {
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

// ---------------------------------------------------------------------------
// SINTER key [key ...]
// ---------------------------------------------------------------------------

/// SINTER command handler: intersection of all sets.
/// If any key is missing, result is empty.
pub fn sinter(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SINTER");
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return err_wrong_args("SINTER");
    }

    let sets = match collect_sets(db, &keys) {
        Ok(s) => s,
        Err(e) => return e,
    };

    // If any key is missing, intersection is empty
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

    // Start with the smallest set for efficiency
    concrete.sort_by_key(|s| s.len());
    let mut result = concrete[0].clone();
    for other in &concrete[1..] {
        result.retain(|m| other.contains(m));
    }

    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members.into())
}

// ---------------------------------------------------------------------------
// SUNION key [key ...]
// ---------------------------------------------------------------------------

/// SUNION command handler: union of all sets. Missing keys treated as empty.
pub fn sunion(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SUNION");
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return err_wrong_args("SUNION");
    }

    let sets = match collect_sets(db, &keys) {
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

// ---------------------------------------------------------------------------
// SDIFF key [key ...]
// ---------------------------------------------------------------------------

/// SDIFF command handler: first set minus all others. Missing keys treated as empty.
pub fn sdiff(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() {
        return err_wrong_args("SDIFF");
    }
    let keys: Vec<&Bytes> = args.iter().filter_map(extract_bytes).collect();
    if keys.len() != args.len() {
        return err_wrong_args("SDIFF");
    }

    let sets = match collect_sets(db, &keys) {
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

// ---------------------------------------------------------------------------
// SRANDMEMBER key [count]
// ---------------------------------------------------------------------------

/// SRANDMEMBER command handler: return random members.
/// Without count: return one random BulkString or Null.
/// Positive count: return distinct elements (up to set size).
/// Negative count: return abs(count) elements with possible duplicates.
pub fn srandmember(db: &mut Database, args: &[Frame]) -> Frame {
    if args.is_empty() || args.len() > 2 {
        return err_wrong_args("SRANDMEMBER");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SRANDMEMBER"),
    };

    let set = match db.get_set(key) {
        Ok(Some(s)) => s.clone(),
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };

    if set.is_empty() {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(framevec![])
        };
    }

    let members: Vec<&Bytes> = set.iter().collect();
    let mut rng = rand::rng();

    if args.len() == 1 {
        // Single random member
        let chosen = members.choose(&mut rng).unwrap();
        return Frame::BulkString((*chosen).clone());
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
        // Distinct elements
        let n = std::cmp::min(count as usize, members.len());
        let chosen: Vec<Frame> = members
            .sample(&mut rng, n)
            .map(|m| Frame::BulkString((*m).clone()))
            .collect();
        Frame::Array(chosen.into())
    } else {
        // Allow duplicates
        let n = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            let chosen = members.choose(&mut rng).unwrap();
            result.push(Frame::BulkString((*chosen).clone()));
        }
        Frame::Array(result.into())
    }
}

// ---------------------------------------------------------------------------
// SSCAN key cursor [MATCH pattern] [COUNT count]
// ---------------------------------------------------------------------------

/// SSCAN command handler: incrementally iterate set members.
pub fn sscan(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SSCAN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SSCAN"),
    };

    // Parse cursor
    let cursor: usize = match extract_bytes(&args[1])
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => {
            return Frame::Error(Bytes::from_static(b"ERR invalid cursor"));
        }
    };

    // Parse optional arguments
    let mut match_pattern: Option<&[u8]> = None;
    let mut count: usize = 10;

    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            i += 1;
            if i < args.len() {
                match_pattern = extract_bytes(&args[i]).map(|b| b.as_ref());
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i < args.len() {
                if let Some(c) = parse_int(&args[i]) {
                    if c > 0 {
                        count = c as usize;
                    }
                }
            }
        }
        i += 1;
    }

    // Get the set
    let members: Vec<Bytes> = match db.get_set(key) {
        Ok(Some(set)) => {
            let mut v: Vec<Bytes> = set.iter().cloned().collect();
            v.sort(); // Deterministic ordering
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
    let set_members = match db.get_set_ref_if_alive(key, now_ms) {
        Ok(Some(sref)) => sref.members(),
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };
    if set_members.is_empty() {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(framevec![])
        };
    }
    let members: Vec<&Bytes> = set_members.iter().collect();
    let mut rng = rand::rng();
    if args.len() == 1 {
        let chosen = members.choose(&mut rng).unwrap();
        return Frame::BulkString((*chosen).clone());
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
        let n = std::cmp::min(count as usize, members.len());
        let chosen: Vec<Frame> = members
            .sample(&mut rng, n)
            .map(|m| Frame::BulkString((*m).clone()))
            .collect();
        Frame::Array(chosen.into())
    } else {
        let n = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            let chosen = members.choose(&mut rng).unwrap();
            result.push(Frame::BulkString((*chosen).clone()));
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
    let mut match_pattern: Option<&[u8]> = None;
    let mut count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => {
                i += 1;
                continue;
            }
        };
        if opt.eq_ignore_ascii_case(b"MATCH") {
            i += 1;
            if i < args.len() {
                match_pattern = extract_bytes(&args[i]).map(|b| b.as_ref());
            }
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            i += 1;
            if i < args.len() {
                if let Some(c) = parse_int(&args[i]) {
                    if c > 0 {
                        count = c as usize;
                    }
                }
            }
        }
        i += 1;
    }
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
