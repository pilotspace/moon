use bytes::Bytes;
use smallvec::SmallVec;

use crate::protocol::{Frame, FrameVec};
use crate::storage::Database;
use crate::storage::db::{HashTtlCond, LISTPACK_MAX_ELEMENT_SIZE, LISTPACK_MAX_ENTRIES};

use crate::command::helpers::{err_wrong_args, extract_bytes, ok};

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

    // Check if any field or value exceeds LISTPACK_MAX_ELEMENT_SIZE
    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        // Try listpack path for small hashes. HashWithTtl returns Ok(None) here
        // (get_or_create_hash_listpack is now HashWithTtl-aware), so it falls
        // through to the full HashMap path — correct, because TTL'd hashes never
        // compact back to listpack.
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
                let mut count = 0i64;
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
                    // Search for existing field in listpack pairs
                    let mut found = false;
                    let mut idx = 0;
                    for (f, _v) in lp.iter_pairs() {
                        if f.as_bytes() == field.as_ref() {
                            // Replace value at idx*2+1
                            lp.replace_at(idx * 2 + 1, value);
                            found = true;
                            break;
                        }
                        idx += 1;
                    }
                    if !found {
                        lp.push_back(field);
                        lp.push_back(value);
                        count += 1;
                    }
                    i += 2;
                }
                // Check threshold: lp.len()/2 fields > LISTPACK_MAX_ENTRIES
                if lp.len() / 2 > LISTPACK_MAX_ENTRIES {
                    db.upgrade_hash_listpack_to_hash(key);
                }
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
    let mut i = 1;
    while i < args.len() {
        let field = match extract_bytes(&args[i]) {
            Some(f) => f.clone(),
            None => return err_wrong_args("HSET"),
        };
        let value = match extract_bytes(&args[i + 1]) {
            Some(v) => v.clone(),
            None => return err_wrong_args("HSET"),
        };
        if map.insert(field, value).is_none() {
            new_count += 1;
        }
        i += 2;
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
        Some(k) => k.clone(),
        None => return err_wrong_args("HDEL"),
    };
    let mut count: i64 = 0;
    let mut last_was_empty = false;
    for arg in &args[1..] {
        if let Some(field) = extract_bytes(arg) {
            match db.hash_delete_field(&key, field) {
                Ok((removed, empty)) => {
                    if removed {
                        count += 1;
                    }
                    last_was_empty = empty;
                }
                Err(e) => return e,
            }
        }
    }
    // If the last deletion left the hash empty, remove the key.
    if last_was_empty {
        db.remove(&key);
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

    // Check if any field or value exceeds LISTPACK_MAX_ELEMENT_SIZE
    let has_large_element = args[1..].iter().any(|a| {
        extract_bytes(a)
            .map(|b| b.len() > LISTPACK_MAX_ELEMENT_SIZE)
            .unwrap_or(false)
    });

    if !has_large_element {
        // HashWithTtl returns Ok(None), falling through — same as HSET.
        match db.get_or_create_hash_listpack(key) {
            Ok(Some(lp)) => {
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
                    let mut found = false;
                    let mut idx = 0;
                    for (f, _v) in lp.iter_pairs() {
                        if f.as_bytes() == field.as_ref() {
                            lp.replace_at(idx * 2 + 1, value);
                            found = true;
                            break;
                        }
                        idx += 1;
                    }
                    if !found {
                        lp.push_back(field);
                        lp.push_back(value);
                    }
                    i += 2;
                }
                if lp.len() / 2 > LISTPACK_MAX_ENTRIES {
                    db.upgrade_hash_listpack_to_hash(key);
                }
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
    let mut i = 1;
    while i < args.len() {
        let field = match extract_bytes(&args[i]) {
            Some(f) => f.clone(),
            None => return err_wrong_args("HMSET"),
        };
        let value = match extract_bytes(&args[i + 1]) {
            Some(v) => v.clone(),
            None => return err_wrong_args("HMSET"),
        };
        map.insert(field, value);
        i += 2;
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
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let current = match map.get(&field) {
        Some(v) => match std::str::from_utf8(v)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR hash value is not an integer")),
        },
        None => 0,
    };
    let new_value = current + increment;
    let mut ibuf = itoa::Buffer::new();
    map.insert(
        field,
        Bytes::copy_from_slice(ibuf.format(new_value).as_bytes()),
    );
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
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let current: f64 = match map.get(&field) {
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
    map.insert(field, Bytes::from(formatted.clone()));
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
    // get_or_create_hash is now HashWithTtl-aware: returns fields sub-map.
    let map = match db.get_or_create_hash(key) {
        Ok(m) => m,
        Err(e) => return e,
    };
    if map.contains_key(&field) {
        Frame::Integer(0)
    } else {
        map.insert(field, value);
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
        return Err(Frame::Error(Bytes::from(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd
        ))));
    }

    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => {
            return Err(Frame::Error(Bytes::from(format!(
                "ERR wrong number of arguments for '{}' command",
                cmd
            ))));
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
            b"ERR wrong number of arguments for 'HEXPIRE' command",
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
            b"ERR wrong number of arguments for 'HEXPIRE' command",
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
        let codes: Vec<Frame> = parsed
            .fields
            .iter()
            .map(|_| Frame::Integer(-2))
            .collect();
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
