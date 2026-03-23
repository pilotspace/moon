use bytes::Bytes;
use rand::seq::IndexedRandom;
use std::collections::HashSet;

use crate::protocol::Frame;
use crate::storage::entry::Entry;
use crate::storage::Database;

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

/// Helper: parse an integer from a frame.
fn parse_int(frame: &Frame) -> Option<i64> {
    let b = extract_bytes(frame)?;
    std::str::from_utf8(b).ok()?.parse().ok()
}

/// Helper: simple glob match (reused pattern from key.rs).
fn glob_match(pattern: &[u8], string: &[u8]) -> bool {
    let mut pi = 0;
    let mut si = 0;
    let mut star_pi = usize::MAX;
    let mut star_si = usize::MAX;

    while si < string.len() {
        if pi < pattern.len() && pattern[pi] == b'\\' {
            pi += 1;
            if pi < pattern.len() && pattern[pi] == string[si] {
                pi += 1;
                si += 1;
                continue;
            }
        } else if pi < pattern.len() && pattern[pi] == b'?' {
            pi += 1;
            si += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == b'*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
            continue;
        } else if pi < pattern.len() && pattern[pi] == string[si] {
            pi += 1;
            si += 1;
            continue;
        }

        if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
            continue;
        }

        return false;
    }

    while pi < pattern.len() && pattern[pi] == b'*' {
        pi += 1;
    }

    pi == pattern.len()
}

// ---------------------------------------------------------------------------
// SADD key member [member ...]
// ---------------------------------------------------------------------------

/// SADD command handler: add members to a set.
/// Returns Integer(count of new members added).
pub fn sadd(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SADD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SADD"),
    };
    let set = match db.get_or_create_set(key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut added = 0i64;
    for arg in &args[1..] {
        if let Some(member) = extract_bytes(arg) {
            if set.insert(member.clone()) {
                added += 1;
            }
        }
    }
    Frame::Integer(added)
}

// ---------------------------------------------------------------------------
// SREM key member [member ...]
// ---------------------------------------------------------------------------

/// SREM command handler: remove members from a set.
/// Returns Integer(count removed). Removes key if set becomes empty.
pub fn srem(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("SREM");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("SREM"),
    };
    let set = match db.get_or_create_set(key) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let mut removed = 0i64;
    for arg in &args[1..] {
        if let Some(member) = extract_bytes(arg) {
            if set.remove(member) {
                removed += 1;
            }
        }
    }
    // Clean up empty set
    let key_clone = key.clone();
    if let Ok(Some(s)) = db.get_set(&key_clone) {
        if s.is_empty() {
            db.remove(&key_clone);
        }
    }
    Frame::Integer(removed)
}

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
            Frame::Array(members)
        }
        Ok(None) => Frame::Array(vec![]),
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
            Frame::Array(results)
        }
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// Set algebra helpers
// ---------------------------------------------------------------------------

/// Collect sets from database as cloned HashSets to avoid borrow conflicts.
/// Returns Err(WRONGTYPE) if any key is the wrong type.
/// `missing_as_empty`: if true, treat missing keys as empty sets; if false,
/// return None for that key.
fn collect_sets(
    db: &mut Database,
    keys: &[&Bytes],
) -> Result<Vec<Option<HashSet<Bytes>>>, Frame> {
    let mut sets = Vec::with_capacity(keys.len());
    for key in keys {
        match db.get_set(key) {
            Ok(Some(set)) => sets.push(Some(set.clone())),
            Ok(None) => sets.push(None),
            Err(e) => return Err(e),
        }
    }
    Ok(sets)
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
            None => return Frame::Array(vec![]),
        }
    }

    if concrete.is_empty() {
        return Frame::Array(vec![]);
    }

    // Start with the smallest set for efficiency
    concrete.sort_by_key(|s| s.len());
    let mut result = concrete[0].clone();
    for other in &concrete[1..] {
        result.retain(|m| other.contains(m));
    }

    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members)
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
    Frame::Array(members)
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
        None => return Frame::Array(vec![]),
    };

    for s in &sets[1..] {
        if let Some(set) = s {
            result.retain(|m| !set.contains(m));
        }
    }

    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members)
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
        Some(k) => k.clone(),
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
        db.remove(&dest);
    } else {
        let mut entry = Entry::new_set();
        if let crate::storage::entry::RedisValue::Set(ref mut s) = entry.value {
            *s = result;
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
        Some(k) => k.clone(),
        None => return err_wrong_args("SUNIONSTORE"),
    };

    let result = sunion_raw(db, &args[1..]);
    let result = match result {
        Ok(set) => set,
        Err(e) => return e,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(&dest);
    } else {
        let mut entry = Entry::new_set();
        if let crate::storage::entry::RedisValue::Set(ref mut s) = entry.value {
            *s = result;
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
        Some(k) => k.clone(),
        None => return err_wrong_args("SDIFFSTORE"),
    };

    let result = sdiff_raw(db, &args[1..]);
    let result = match result {
        Ok(set) => set,
        Err(e) => return e,
    };

    let count = result.len() as i64;
    if result.is_empty() {
        db.remove(&dest);
    } else {
        let mut entry = Entry::new_set();
        if let crate::storage::entry::RedisValue::Set(ref mut s) = entry.value {
            *s = result;
        }
        db.set(dest, entry);
    }
    Frame::Integer(count)
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
                Frame::Array(vec![])
            };
        }
        Err(e) => return e,
    };

    if set.is_empty() {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(vec![])
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
            ))
        }
    };

    if count == 0 {
        return Frame::Array(vec![]);
    }

    if count > 0 {
        // Distinct elements
        let n = std::cmp::min(count as usize, members.len());
        let chosen: Vec<Frame> = members
            .choose_multiple(&mut rng, n)
            .map(|m| Frame::BulkString((*m).clone())
            )
            .collect();
        Frame::Array(chosen)
    } else {
        // Allow duplicates
        let n = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            let chosen = members.choose(&mut rng).unwrap();
            result.push(Frame::BulkString((*chosen).clone()));
        }
        Frame::Array(result)
    }
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
        Some(k) => k.clone(),
        None => return err_wrong_args("SPOP"),
    };

    // Clone the set to pick random members, then mutate
    let set_clone = match db.get_set(&key) {
        Ok(Some(s)) => s.clone(),
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(vec![])
            };
        }
        Err(e) => return e,
    };

    if set_clone.is_empty() {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(vec![])
        };
    }

    let members: Vec<Bytes> = set_clone.into_iter().collect();
    let mut rng = rand::rng();

    if args.len() == 1 {
        // Single random member
        let chosen = members.choose(&mut rng).unwrap().clone();
        let set = db.get_or_create_set(&key).unwrap();
        set.remove(&chosen);
        if set.is_empty() {
            db.remove(&key);
        }
        return Frame::BulkString(chosen);
    }

    let count = match parse_int(&args[1]) {
        Some(c) if c >= 0 => c as usize,
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
    };

    if count == 0 {
        return Frame::Array(vec![]);
    }

    let n = std::cmp::min(count, members.len());
    let chosen: Vec<Bytes> = members
        .choose_multiple(&mut rng, n)
        .cloned()
        .collect();

    // Remove chosen members from the set
    let set = db.get_or_create_set(&key).unwrap();
    for m in &chosen {
        set.remove(m);
    }
    if set.is_empty() {
        db.remove(&key);
    }

    let result: Vec<Frame> = chosen.into_iter().map(Frame::BulkString).collect();
    Frame::Array(result)
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

    Frame::Array(vec![
        Frame::BulkString(next_cursor),
        Frame::Array(results),
    ])
}

// ===========================================================================
// Tests
// ===========================================================================

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
        match db.get_set_if_alive(key, now_ms) {
            Ok(Some(set)) => sets.push(Some(set.clone())),
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
    match db.get_set_if_alive(key, now_ms) {
        Ok(Some(set)) => {
            let members: Vec<Frame> = set.iter().map(|m| Frame::BulkString(m.clone())).collect();
            Frame::Array(members)
        }
        Ok(None) => Frame::Array(vec![]),
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
    match db.get_set_if_alive(key, now_ms) {
        Ok(Some(set)) => Frame::Integer(set.len() as i64),
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
    match db.get_set_if_alive(key, now_ms) {
        Ok(Some(set)) => {
            if set.contains(member) { Frame::Integer(1) } else { Frame::Integer(0) }
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
    match db.get_set_if_alive(key, now_ms) {
        Ok(maybe_set) => {
            let results: Vec<Frame> = args[1..]
                .iter()
                .map(|arg| {
                    let member = extract_bytes(arg);
                    match (maybe_set, member) {
                        (Some(set), Some(m)) => {
                            if set.contains(m) { Frame::Integer(1) } else { Frame::Integer(0) }
                        }
                        _ => Frame::Integer(0),
                    }
                })
                .collect();
            Frame::Array(results)
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
            None => return Frame::Array(vec![]),
        }
    }
    if concrete.is_empty() {
        return Frame::Array(vec![]);
    }
    concrete.sort_by_key(|s| s.len());
    let mut result = concrete[0].clone();
    for other in &concrete[1..] {
        result.retain(|m| other.contains(m));
    }
    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members)
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
    Frame::Array(members)
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
        None => return Frame::Array(vec![]),
    };
    for s in &sets[1..] {
        if let Some(set) = s {
            result.retain(|m| !set.contains(m));
        }
    }
    let members: Vec<Frame> = result.into_iter().map(Frame::BulkString).collect();
    Frame::Array(members)
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
    let set = match db.get_set_if_alive(key, now_ms) {
        Ok(Some(s)) => s.clone(),
        Ok(None) => {
            return if args.len() == 1 { Frame::Null } else { Frame::Array(vec![]) };
        }
        Err(e) => return e,
    };
    if set.is_empty() {
        return if args.len() == 1 { Frame::Null } else { Frame::Array(vec![]) };
    }
    let members: Vec<&Bytes> = set.iter().collect();
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
            ))
        }
    };
    if count == 0 {
        return Frame::Array(vec![]);
    }
    if count > 0 {
        let n = std::cmp::min(count as usize, members.len());
        let chosen: Vec<Frame> = members.choose_multiple(&mut rng, n)
            .map(|m| Frame::BulkString((*m).clone()))
            .collect();
        Frame::Array(chosen)
    } else {
        let n = count.unsigned_abs() as usize;
        let mut result = Vec::with_capacity(n);
        for _ in 0..n {
            let chosen = members.choose(&mut rng).unwrap();
            result.push(Frame::BulkString((*chosen).clone()));
        }
        Frame::Array(result)
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
            None => { i += 1; continue; }
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
                    if c > 0 { count = c as usize; }
                }
            }
        }
        i += 1;
    }
    let members: Vec<Bytes> = match db.get_set_if_alive(key, now_ms) {
        Ok(Some(set)) => {
            let mut v: Vec<Bytes> = set.iter().cloned().collect();
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
            if !glob_match(pattern, member) { continue; }
        }
        results.push(Frame::BulkString(member.clone()));
    }
    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        Bytes::from(pos.to_string())
    };
    Frame::Array(vec![
        Frame::BulkString(next_cursor),
        Frame::Array(results),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bs(s: &[u8]) -> Frame {
        Frame::BulkString(Bytes::copy_from_slice(s))
    }

    fn setup_set(db: &mut Database, key: &[u8], members: &[&[u8]]) {
        for m in members {
            sadd(db, &[bs(key), bs(m)]);
        }
    }

    // --- SADD / SREM tests ---

    #[test]
    fn test_sadd_basic() {
        let mut db = Database::new();
        let result = sadd(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(3));

        // Adding duplicates
        let result = sadd(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"d")]);
        assert_eq!(result, Frame::Integer(1)); // only "d" is new
    }

    #[test]
    fn test_srem_basic() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = srem(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"d")]);
        assert_eq!(result, Frame::Integer(1)); // only "a" removed

        // Remove remaining to trigger auto-delete
        let result = srem(&mut db, &[bs(b"myset"), bs(b"b"), bs(b"c")]);
        assert_eq!(result, Frame::Integer(2));
        assert!(!db.exists(b"myset")); // key should be deleted
    }

    // --- SMEMBERS / SCARD tests ---

    #[test]
    fn test_smembers_and_scard() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = scard(&mut db, &[bs(b"myset")]);
        assert_eq!(result, Frame::Integer(3));

        let result = smembers(&mut db, &[bs(b"myset")]);
        match result {
            Frame::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("expected array"),
        }

        // Missing key
        let result = scard(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Integer(0));

        let result = smembers(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Array(vec![]));
    }

    // --- SISMEMBER / SMISMEMBER tests ---

    #[test]
    fn test_sismember() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        assert_eq!(sismember(&mut db, &[bs(b"myset"), bs(b"a")]), Frame::Integer(1));
        assert_eq!(sismember(&mut db, &[bs(b"myset"), bs(b"c")]), Frame::Integer(0));
        assert_eq!(sismember(&mut db, &[bs(b"missing"), bs(b"a")]), Frame::Integer(0));
    }

    #[test]
    fn test_smismember() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        let result = smismember(&mut db, &[bs(b"myset"), bs(b"a"), bs(b"c"), bs(b"b")]);
        assert_eq!(
            result,
            Frame::Array(vec![Frame::Integer(1), Frame::Integer(0), Frame::Integer(1)])
        );
    }

    // --- SINTER tests ---

    #[test]
    fn test_sinter() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sinter(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                let members: HashSet<Bytes> = arr
                    .into_iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b,
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(members.len(), 2);
                assert!(members.contains(&Bytes::from_static(b"b")));
                assert!(members.contains(&Bytes::from_static(b"c")));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sinter_missing_key() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);

        let result = sinter(&mut db, &[bs(b"s1"), bs(b"missing")]);
        assert_eq!(result, Frame::Array(vec![]));
    }

    // --- SUNION tests ---

    #[test]
    fn test_sunion() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sunion(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                let members: HashSet<Bytes> = arr
                    .into_iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b,
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(members.len(), 3);
                assert!(members.contains(&Bytes::from_static(b"a")));
                assert!(members.contains(&Bytes::from_static(b"b")));
                assert!(members.contains(&Bytes::from_static(b"c")));
            }
            _ => panic!("expected array"),
        }
    }

    // --- SDIFF tests ---

    #[test]
    fn test_sdiff() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sdiff(&mut db, &[bs(b"s1"), bs(b"s2")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"a")));
            }
            _ => panic!("expected array"),
        }
    }

    // --- SINTERSTORE / SUNIONSTORE / SDIFFSTORE tests ---

    #[test]
    fn test_sinterstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c", b"d"]);

        let result = sinterstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(2));

        let result = scard(&mut db, &[bs(b"dest")]);
        assert_eq!(result, Frame::Integer(2));
    }

    #[test]
    fn test_sunionstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sunionstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(3));
    }

    #[test]
    fn test_sdiffstore() {
        let mut db = Database::new();
        setup_set(&mut db, b"s1", &[b"a", b"b", b"c"]);
        setup_set(&mut db, b"s2", &[b"b", b"c"]);

        let result = sdiffstore(&mut db, &[bs(b"dest"), bs(b"s1"), bs(b"s2")]);
        assert_eq!(result, Frame::Integer(1));
    }

    // --- SRANDMEMBER tests ---

    #[test]
    fn test_srandmember_single() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = srandmember(&mut db, &[bs(b"myset")]);
        match result {
            Frame::BulkString(_) => {} // any member is fine
            _ => panic!("expected bulkstring"),
        }

        // Missing key
        let result = srandmember(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_srandmember_positive_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        // Request more than set size: should return all distinct
        let result = srandmember(&mut db, &[bs(b"myset"), bs(b"10")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3); // min(10, 3) = 3
                // Check uniqueness by extracting bytes into a HashSet
                let set: HashSet<Bytes> = arr
                    .iter()
                    .map(|f| match f {
                        Frame::BulkString(b) => b.clone(),
                        _ => panic!("expected bulkstring"),
                    })
                    .collect();
                assert_eq!(set.len(), 3);
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_srandmember_negative_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a"]);

        // Negative count: may have duplicates, always returns abs(count) elements
        let result = srandmember(&mut db, &[bs(b"myset"), bs(b"-5")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 5); // always returns 5 elements
            }
            _ => panic!("expected array"),
        }
    }

    // --- SPOP tests ---

    #[test]
    fn test_spop_single() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = spop(&mut db, &[bs(b"myset")]);
        match result {
            Frame::BulkString(_) => {}
            _ => panic!("expected bulkstring"),
        }

        assert_eq!(scard(&mut db, &[bs(b"myset")]), Frame::Integer(2));
    }

    #[test]
    fn test_spop_count() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = spop(&mut db, &[bs(b"myset"), bs(b"2")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
            }
            _ => panic!("expected array"),
        }

        assert_eq!(scard(&mut db, &[bs(b"myset")]), Frame::Integer(1));
    }

    #[test]
    fn test_spop_all_auto_delete() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b"]);

        let _ = spop(&mut db, &[bs(b"myset"), bs(b"10")]);
        assert!(!db.exists(b"myset")); // key should be removed
    }

    #[test]
    fn test_spop_missing_key() {
        let mut db = Database::new();
        let result = spop(&mut db, &[bs(b"missing")]);
        assert_eq!(result, Frame::Null);

        let result = spop(&mut db, &[bs(b"missing"), bs(b"3")]);
        assert_eq!(result, Frame::Array(vec![]));
    }

    // --- SSCAN tests ---

    #[test]
    fn test_sscan_basic() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"a", b"b", b"c"]);

        let result = sscan(&mut db, &[bs(b"myset"), bs(b"0")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                // Should return cursor "0" (all scanned) and array of members
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"0")));
                match &arr[1] {
                    Frame::Array(members) => assert_eq!(members.len(), 3),
                    _ => panic!("expected inner array"),
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sscan_with_match() {
        let mut db = Database::new();
        setup_set(&mut db, b"myset", &[b"apple", b"banana", b"apricot"]);

        let result = sscan(
            &mut db,
            &[bs(b"myset"), bs(b"0"), bs(b"MATCH"), bs(b"ap*")],
        );
        match result {
            Frame::Array(arr) => {
                match &arr[1] {
                    Frame::Array(members) => {
                        assert_eq!(members.len(), 2);
                    }
                    _ => panic!("expected inner array"),
                }
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_sscan_missing_key() {
        let mut db = Database::new();
        let result = sscan(&mut db, &[bs(b"missing"), bs(b"0")]);
        match result {
            Frame::Array(arr) => {
                assert_eq!(arr[0], Frame::BulkString(Bytes::from_static(b"0")));
                assert_eq!(arr[1], Frame::Array(vec![]));
            }
            _ => panic!("expected array"),
        }
    }

    // --- WRONGTYPE tests ---

    #[test]
    fn test_wrongtype_on_string_key() {
        let mut db = Database::new();
        db.set_string(Bytes::from_static(b"mystr"), Bytes::from_static(b"hello"));

        let result = sadd(&mut db, &[bs(b"mystr"), bs(b"a")]);
        match result {
            Frame::Error(e) => assert!(e.starts_with(b"WRONGTYPE")),
            _ => panic!("expected WRONGTYPE error"),
        }
    }

    // --- Wrong args tests ---

    #[test]
    fn test_wrong_args() {
        let mut db = Database::new();
        match sadd(&mut db, &[bs(b"key")]) {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number")),
            _ => panic!("expected error"),
        }
        match srem(&mut db, &[bs(b"key")]) {
            Frame::Error(e) => assert!(e.starts_with(b"ERR wrong number")),
            _ => panic!("expected error"),
        }
    }
}
