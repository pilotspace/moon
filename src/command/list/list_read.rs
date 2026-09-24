use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use super::{parse_i64, resolve_index};
use crate::command::helpers::{err_wrong_args, extract_bytes};

// ---------------------------------------------------------------------------
// LLEN key
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// moon#832 -- the mutable dispatch path reads through the SHARED one
//
// Every handler below is a pure read, and every one used to reach the list
// through `Database::get_list`, whose `get_promoted` core calls `K::upgrade`
// unconditionally. That conversion is one-way -- nothing ever downgrades --
// so a single LLEN taken on the mutable path (inside MULTI/EXEC, inside a
// Lua script, or from `try_inline_dispatch`) permanently flattened a
// `listpack` list to a `linkedlist` for the rest of its life. Measured on the
// unmodified binary: `RPUSH l a b c` then `OBJECT ENCODING l` reported
// `listpack`; a plain `LLEN l` left it `listpack`; the SAME `LLEN` inside
// MULTI/EXEC left it `linkedlist`.
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
/// LLEN key -- length of a list.
///
/// Reads through the shared-borrow implementation so the list's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn llen(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    llen_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// LINDEX key index
// ---------------------------------------------------------------------------

/// LINDEX key index -- element at index.
///
/// Reads through the shared-borrow implementation so the list's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn lindex(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    lindex_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// LRANGE key start stop
// ---------------------------------------------------------------------------

/// LRANGE key start stop -- a range of elements.
///
/// Reads through the shared-borrow implementation so the list's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn lrange(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    lrange_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]
// ---------------------------------------------------------------------------

/// LPOS key element [RANK r] [COUNT n] [MAXLEN m] -- index of a match.
///
/// Reads through the shared-borrow implementation so the list's compact
/// encoding survives the read (moon#832 -- see the block above).
pub fn lpos(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    lpos_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// LLEN (read-only).
pub fn llen_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("LLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LLEN"),
    };
    match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(lref)) => Frame::Integer(lref.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// LRANGE (read-only).
pub fn lrange_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 3 {
        return err_wrong_args("LRANGE");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LRANGE"),
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
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Array(framevec![]),
        Err(e) => return e,
    };
    let len = lref.len() as i64;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return Frame::Array(framevec![]);
    }
    // One pass straight into the reply (moon#1174 §2): no intermediate
    // `Vec<Bytes>`, and one seek on a listpack instead of one per element.
    let mut items: Vec<Frame> = Vec::with_capacity((e - s + 1) as usize);
    lref.for_each_in_range(s as usize, e as usize, |b| items.push(Frame::BulkString(b)));
    Frame::Array(items.into())
}

/// LINDEX (read-only).
pub fn lindex_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("LINDEX");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LINDEX"),
    };
    let index = match parse_i64(&args[1]) {
        Some(v) => v,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => return Frame::Null,
        Err(e) => return e,
    };
    match resolve_index(index, lref.len()) {
        Some(i) => match lref.get(i) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        None => Frame::Null,
    }
}

/// `LPOS`'s `RANK 0` refusal, byte-for-byte what redis answers.
const ERR_LPOS_RANK_ZERO: &[u8] = b"ERR RANK can't be zero: use 1 to start from the first match, \
2 from the second ... or use negative to start from the end of the list";

/// LPOS (read-only).
///
/// # moon#1173
///
/// This used to clone the WHOLE list into a `Vec` (`ListRef::iter_bytes`) --
/// two allocations per element on a listpack -- before it looked at `MAXLEN`,
/// `RANK` or `COUNT`: `LPOS l a MAXLEN 10` on a million-element list took
/// 38.9 ms against redis's 78 us. It now scans the list IN PLACE through
/// [`ListRef::for_each_match`], forward, or backward for a negative `RANK`,
/// and stops at `MAXLEN` or once `COUNT` matches are in hand. Nothing is
/// copied; the only allocation is the reply.
///
/// # Option parsing (redis parity)
///
/// Redis decides the OPTION first and only then parses its value, so an
/// unknown option is `ERR syntax error` whatever follows it, and `COUNT` /
/// `MAXLEN` answer their own message for a non-integer as well as a negative
/// value (`getPositiveLongFromObjectOrReply` with a message). moon used to
/// parse the value first and answered `ERR value is not an integer or out of
/// range` for both, and spelled the `RANK 0` refusal differently. Measured
/// against redis 7.0.15.
///
/// [`ListRef::for_each_match`]: crate::storage::db::ListRef::for_each_match
pub fn lpos_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("LPOS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args("LPOS"),
    };
    let element = match extract_bytes(&args[1]) {
        Some(v) => v.as_ref(),
        None => return err_wrong_args("LPOS"),
    };
    let mut rank: i64 = 1;
    let mut count: Option<usize> = None;
    let mut maxlen: usize = 0;
    let mut i = 2;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(o) => o.as_ref(),
            None => return err_wrong_args("LPOS"),
        };
        let Some(value) = args.get(i + 1) else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        };
        i += 2;
        if opt.eq_ignore_ascii_case(b"RANK") {
            rank = match parse_i64(value) {
                Some(0) => return Frame::Error(Bytes::from_static(ERR_LPOS_RANK_ZERO)),
                Some(v) => v,
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR value is not an integer or out of range",
                    ));
                }
            };
        } else if opt.eq_ignore_ascii_case(b"COUNT") {
            count = match parse_i64(value) {
                Some(v) if v >= 0 => Some(usize::try_from(v).unwrap_or(usize::MAX)),
                _ => return Frame::Error(Bytes::from_static(b"ERR COUNT can't be negative")),
            };
        } else if opt.eq_ignore_ascii_case(b"MAXLEN") {
            maxlen = match parse_i64(value) {
                Some(v) if v >= 0 => usize::try_from(v).unwrap_or(usize::MAX),
                _ => return Frame::Error(Bytes::from_static(b"ERR MAXLEN can't be negative")),
            };
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    }
    let lref = match db.get_list_ref_if_alive(key, now_ms) {
        Ok(Some(l)) => l,
        Ok(None) => {
            return if count.is_some() {
                Frame::Array(framevec![])
            } else {
                Frame::Null
            };
        }
        Err(e) => return e,
    };
    let len = lref.len();
    let scan_limit = if maxlen > 0 { maxlen.min(len) } else { len };
    let max_count = match count {
        Some(0) => usize::MAX,
        Some(c) => c,
        None => 1,
    };
    // `unsigned_abs`, not `-rank`: `RANK -9223372036854775808` must not
    // overflow. It asks for a match no list can hold, so it finds none.
    let mut skip = usize::try_from(rank.unsigned_abs() - 1).unwrap_or(usize::MAX);
    let mut first: Option<i64> = None;
    let mut matches: Vec<Frame> = Vec::new();
    let mut found = 0usize;
    lref.for_each_match(element, rank < 0, scan_limit, |idx| {
        if skip > 0 {
            skip -= 1;
            return true;
        }
        found += 1;
        if count.is_some() {
            matches.push(Frame::Integer(idx as i64));
        } else {
            first = Some(idx as i64);
        }
        found < max_count
    });
    if count.is_some() {
        Frame::Array(matches.into())
    } else {
        first.map(Frame::Integer).unwrap_or(Frame::Null)
    }
}
