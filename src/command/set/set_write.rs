use bytes::Bytes;
use rand::seq::IndexedRandom;
use std::collections::HashSet;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
use crate::storage::entry::Entry;

use super::{collect_sets, parse_int};
use crate::command::helpers::{err_wrong_args, extract_bytes};

// ---------------------------------------------------------------------------
// SADD key member [member ...]
// ---------------------------------------------------------------------------

/// Maximum intset entries before upgrading to full HashSet encoding.
const INTSET_MAX_ENTRIES: usize = 512;

/// Try to parse a byte slice as an i64.
fn try_parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
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

    // Try intset path: new key with all-integer members, or existing intset
    if all_integers && member_count <= INTSET_MAX_ENTRIES {
        match db.get_or_create_intset(key) {
            Ok(Some(intset)) => {
                let mut added = 0i64;
                let mut needs_upgrade = false;
                for arg in &args[1..] {
                    if let Some(member) = extract_bytes(arg) {
                        let val = try_parse_i64(member).unwrap();
                        if intset.insert(val) {
                            added += 1;
                        }
                        if intset.len() > INTSET_MAX_ENTRIES {
                            needs_upgrade = true;
                            break;
                        }
                    }
                }
                if needs_upgrade {
                    // Upgrade intset to HashSet and continue adding remaining members
                    let set = db.upgrade_intset_to_set(key);
                    // Re-add remaining members (some may already be in the set from intset)
                    for arg in &args[1..] {
                        if let Some(member) = extract_bytes(arg) {
                            set.insert(member.clone());
                        }
                    }
                    // Recount: we need accurate count of new members
                    // Since we already inserted into intset and then upgraded,
                    // just count total unique members vs original
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

    // Standard path: get_or_create_set (creates HashSet, upgrades compact encodings)
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
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };

    if set_clone.is_empty() {
        return if args.len() == 1 {
            Frame::Null
        } else {
            Frame::Array(framevec![])
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

    let n = std::cmp::min(count, members.len());
    let chosen: Vec<Bytes> = members.sample(&mut rng, n).cloned().collect();

    // Remove chosen members from the set
    let set = db.get_or_create_set(&key).unwrap();
    for m in &chosen {
        set.remove(m);
    }
    if set.is_empty() {
        db.remove(&key);
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
        if let Some(crate::storage::entry::RedisValue::Set(s)) = entry.value.as_redis_value_mut() {
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
        if let Some(crate::storage::entry::RedisValue::Set(s)) = entry.value.as_redis_value_mut() {
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
        if let Some(crate::storage::entry::RedisValue::Set(s)) = entry.value.as_redis_value_mut() {
            *s = result;
        }
        db.set(dest, entry);
    }
    Frame::Integer(count)
}
