use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err_wrong_args, extract_bytes};

/// HGET key field
///
/// Returns the value associated with field in the hash at key, or Null.
/// Skips fields whose per-field TTL has expired (lazy expiry via `HashRef::WithTtl`).
pub fn hget(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGET"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HGET"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => match href.get_field(field) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// HMGET key field [field ...]
///
/// Returns values for multiple fields. Null for missing or expired fields.
pub fn hmget(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HMGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMGET"),
    };
    let now_ms = db.now_ms();
    let href_opt = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut results = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let field = match extract_bytes(arg) {
            Some(f) => f,
            None => {
                results.push(Frame::Null);
                continue;
            }
        };
        match &href_opt {
            Some(href) => match href.get_field(field) {
                Some(v) => results.push(Frame::BulkString(v)),
                None => results.push(Frame::Null),
            },
            None => results.push(Frame::Null),
        }
    }
    Frame::Array(results.into())
}

/// HGETALL key
///
/// Returns all live field-value pairs as alternating elements in an array.
/// Expired fields are omitted.
pub fn hgetall(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HGETALL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGETALL"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let entries = href.entries();
            let mut result = Vec::with_capacity(entries.len() * 2);
            for (field, value) in entries {
                result.push(Frame::BulkString(field));
                result.push(Frame::BulkString(value));
            }
            Frame::Array(result.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HEXISTS key field
///
/// Returns 1 if field exists and has not expired, 0 otherwise.
pub fn hexists(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HEXISTS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HEXISTS"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HEXISTS"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        // Presence only: the value is never materialized (moon#1174 §3).
        Ok(Some(href)) => Frame::Integer(i64::from(href.contains_field(field))),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HLEN key
///
/// Returns the number of live fields in the hash, or 0 if key missing.
///
/// O(1) for plain `Hash` and `HashListpack`.
/// O(N) for `HashWithTtl` (filters expired fields on each call).
pub fn hlen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HLEN"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => Frame::Integer(href.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HSTRLEN key field
///
/// The string length of the value stored at `field`, or 0 when the field, the
/// key, or the value itself is empty. redis does not distinguish those three
/// and neither do we — there is no nil reply on this command (moon#636).
pub fn hstrlen(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HSTRLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSTRLEN"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HSTRLEN"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        // The VALUE's length, not the field name's — the one thing an
        // implementation can plausibly get backwards. Measured in place, never
        // copied out (moon#1174 §3).
        Ok(Some(href)) => Frame::Integer(href.field_len(field).map_or(0, |n| n as i64)),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HKEYS key
///
/// Returns all live field names in the hash. Expired fields are omitted.
pub fn hkeys(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HKEYS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HKEYS"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        // Field names only, straight into the reply: no value is materialized
        // and no intermediate pair vector is built (moon#1174 §3).
        Ok(Some(href)) => {
            let mut fields: Vec<Frame> = Vec::with_capacity(href.len_hint());
            href.for_each_field(|k| fields.push(Frame::BulkString(k)));
            Frame::Array(fields.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HVALS key
///
/// Returns all live values in the hash. Values of expired fields are omitted.
pub fn hvals(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HVALS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HVALS"),
    };
    let now_ms = db.now_ms();
    match db.get_hash_ref_if_alive(key, now_ms) {
        // Values only, straight into the reply (moon#1174 §3).
        Ok(Some(href)) => {
            let mut values: Vec<Frame> = Vec::with_capacity(href.len_hint());
            href.for_each_value(|v| values.push(Frame::BulkString(v)));
            Frame::Array(values.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HSCAN key cursor [MATCH pattern] [COUNT count]
///
/// Incrementally iterates hash fields using a cursor. Expired fields are omitted.
pub fn hscan(db: &mut Database, args: &[Frame]) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HSCAN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSCAN"),
    };

    // Parse cursor
    let cursor: usize = match extract_bytes(&args[1]) {
        Some(c) => match std::str::from_utf8(c).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
        },
        None => return err_wrong_args("HSCAN"),
    };

    // One parser for the whole family — see `command::scan_options` for why
    // eight hand-copied ones is how `NOVALUES` came to be silently dropped.
    let opts = match crate::command::scan_options::parse_scan_options(
        crate::command::scan_options::ScanKind::Hash,
        &args[2..],
    ) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let match_pattern = opts.pattern;
    let count = opts.count;

    // Collect live (field, value) pairs; HashRef::entries() filters expired fields.
    let now_ms = db.now_ms();
    let entries = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let mut e = href.entries();
            e.sort_by(|a, b| a.0.cmp(&b.0));
            e
        }
        Ok(None) => {
            return Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(framevec![]),
            ]);
        }
        Err(e) => return e,
    };

    let total = entries.len();
    // DoS guard: bound the pre-size by the actual field count so a huge COUNT
    // hint can't drive an unbounded Vec::with_capacity -> allocator abort.
    let mut results = Vec::with_capacity(count.min(total).saturating_mul(2));
    let mut pos = cursor;
    let mut checked = 0;

    while pos < total && checked < count {
        let (ref field, ref value) = entries[pos];
        pos += 1;
        checked += 1;

        // MATCH filter on field name
        if let Some(pattern) = match_pattern {
            if !crate::command::key::glob_match(pattern, field) {
                continue;
            }
        }

        results.push(Frame::BulkString(field.clone()));
        // NOVALUES: field names only. A client that passes it parses the reply
        // as a flat list of names, so emitting the value here hands it a name
        // that does not exist (moon#630).
        if !opts.novalues {
            results.push(Frame::BulkString(value.clone()));
        }
    }

    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        let mut ibuf = itoa::Buffer::new();
        Bytes::copy_from_slice(ibuf.format(pos).as_bytes())
    };

    Frame::Array(framevec![
        Frame::BulkString(next_cursor),
        Frame::Array(results.into()),
    ])
}

// ---------------------------------------------------------------------------
// Read-only variants for RwLock read path
// ---------------------------------------------------------------------------

/// HGET (read-only).
pub fn hget_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGET"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HGET"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => match href.get_field(field) {
            Some(v) => Frame::BulkString(v),
            None => Frame::Null,
        },
        Ok(None) => Frame::Null,
        Err(e) => e,
    }
}

/// HSTRLEN (read-only twin).
pub fn hstrlen_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HSTRLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSTRLEN"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HSTRLEN"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => Frame::Integer(href.field_len(field).map_or(0, |n| n as i64)),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HMGET (read-only).
pub fn hmget_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HMGET");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HMGET"),
    };
    let href_opt = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(m) => m,
        Err(e) => return e,
    };
    let mut results = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let field = match extract_bytes(arg) {
            Some(f) => f,
            None => {
                results.push(Frame::Null);
                continue;
            }
        };
        match &href_opt {
            Some(href) => match href.get_field(field) {
                Some(v) => results.push(Frame::BulkString(v)),
                None => results.push(Frame::Null),
            },
            None => results.push(Frame::Null),
        }
    }
    Frame::Array(results.into())
}

/// HGETALL (read-only).
pub fn hgetall_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HGETALL");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HGETALL"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let entries = href.entries();
            let mut result = Vec::with_capacity(entries.len() * 2);
            for (field, value) in entries {
                result.push(Frame::BulkString(field));
                result.push(Frame::BulkString(value));
            }
            Frame::Array(result.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HLEN (read-only).
pub fn hlen_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HLEN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HLEN"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => Frame::Integer(href.len() as i64),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HKEYS (read-only).
pub fn hkeys_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HKEYS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HKEYS"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        // Field names only, straight into the reply: no value is materialized
        // and no intermediate pair vector is built (moon#1174 §3).
        Ok(Some(href)) => {
            let mut fields: Vec<Frame> = Vec::with_capacity(href.len_hint());
            href.for_each_field(|k| fields.push(Frame::BulkString(k)));
            Frame::Array(fields.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HVALS (read-only).
pub fn hvals_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 1 {
        return err_wrong_args("HVALS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HVALS"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        // Values only, straight into the reply (moon#1174 §3).
        Ok(Some(href)) => {
            let mut values: Vec<Frame> = Vec::with_capacity(href.len_hint());
            href.for_each_value(|v| values.push(Frame::BulkString(v)));
            Frame::Array(values.into())
        }
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}

/// HEXISTS (read-only).
pub fn hexists_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() != 2 {
        return err_wrong_args("HEXISTS");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HEXISTS"),
    };
    let field = match extract_bytes(&args[1]) {
        Some(f) => f,
        None => return err_wrong_args("HEXISTS"),
    };
    match db.get_hash_ref_if_alive(key, now_ms) {
        // Presence only: the value is never materialized (moon#1174 §3).
        Ok(Some(href)) => Frame::Integer(i64::from(href.contains_field(field))),
        Ok(None) => Frame::Integer(0),
        Err(e) => e,
    }
}

/// HSCAN (read-only).
pub fn hscan_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    if args.len() < 2 {
        return err_wrong_args("HSCAN");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HSCAN"),
    };
    let cursor: usize = match extract_bytes(&args[1]) {
        Some(c) => match std::str::from_utf8(c).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Frame::Error(Bytes::from_static(b"ERR invalid cursor")),
        },
        None => return err_wrong_args("HSCAN"),
    };
    // One parser for the whole family — see `command::scan_options` for why
    // eight hand-copied ones is how `NOVALUES` came to be silently dropped.
    let opts = match crate::command::scan_options::parse_scan_options(
        crate::command::scan_options::ScanKind::Hash,
        &args[2..],
    ) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let match_pattern = opts.pattern;
    let count = opts.count;
    let entries = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(href)) => {
            let mut e = href.entries();
            e.sort_by(|a, b| a.0.cmp(&b.0));
            e
        }
        Ok(None) => {
            return Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"0")),
                Frame::Array(framevec![]),
            ]);
        }
        Err(e) => return e,
    };
    let total = entries.len();
    let mut results = Vec::new();
    let mut pos = cursor;
    let mut checked = 0;
    while pos < total && checked < count {
        let (ref field, ref value) = entries[pos];
        pos += 1;
        checked += 1;
        if let Some(pattern) = match_pattern {
            if !crate::command::key::glob_match(pattern, field) {
                continue;
            }
        }
        results.push(Frame::BulkString(field.clone()));
        // NOVALUES: field names only. A client that passes it parses the reply
        // as a flat list of names, so emitting the value here hands it a name
        // that does not exist (moon#630).
        if !opts.novalues {
            results.push(Frame::BulkString(value.clone()));
        }
    }
    let next_cursor = if pos >= total {
        Bytes::from_static(b"0")
    } else {
        let mut ibuf = itoa::Buffer::new();
        Bytes::copy_from_slice(ibuf.format(pos).as_bytes())
    };
    Frame::Array(framevec![
        Frame::BulkString(next_cursor),
        Frame::Array(results.into()),
    ])
}

// ---------------------------------------------------------------------------
// HRANDFIELD key [count [WITHVALUES]]
// ---------------------------------------------------------------------------

/// HRANDFIELD key [count [WITHVALUES]]
///
/// Returns random fields from the hash. Expired fields are never returned.
///
/// Delegates to the shared-borrow implementation: both already read through
/// `get_hash_ref_if_alive`, so the mutable dispatch path (cross-shard SPSC,
/// MULTI/EXEC, Lua) and `dispatch_read` now run ONE body (moon#1171).
pub fn hrandfield(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    hrandfield_readonly(db, args, now_ms)
}

// ---------------------------------------------------------------------------
// moon#1171 — HRANDFIELD without materializing the hash
//
// Both entry points used to call `HashRef::entries()`, which clones EVERY
// live field and value (two refcount round-trips per pair on a full hash,
// four allocations per pair on a listpack), then built a SECOND N-element
// index `Vec` to sample from — O(N) allocation to return one field.
//
// Now: the live length is read (O(1) except on the rare field-TTL hash),
// the wanted positions are drawn up front, and ONE borrowed walk over the
// hash resolves all of them, cloning only the pairs that are returned. The
// walk itself is still O(position): a full hash is a `std::HashMap`, which
// has no positional access. O(1) needs the full-hash representation moved
// to an `IndexMap` the way sets moved to `IndexSet` — a
// `storage/entry.rs` change owned by the storage-core workstream, recorded
// as DEFERRED in plans/WS2-datatype-commands/SUMMARY.md.
// ---------------------------------------------------------------------------

/// One side of a live hash pair, borrowed from wherever the hash keeps it.
#[derive(Clone, Copy)]
enum PairSide<'a> {
    Stored(&'a Bytes),
    Packed(crate::storage::listpack::ListpackRef<'a>),
}

impl PairSide<'_> {
    /// The reply bytes: a refcount bump for a stored `Bytes`, one copy for a
    /// listpack entry (integers rendered through `itoa`, no `to_string`).
    fn to_reply(self) -> Bytes {
        match self {
            PairSide::Stored(b) => b.clone(),
            PairSide::Packed(crate::storage::listpack::ListpackRef::Str(s)) => {
                Bytes::copy_from_slice(s)
            }
            PairSide::Packed(crate::storage::listpack::ListpackRef::Integer(v)) => {
                let mut buf = itoa::Buffer::new();
                Bytes::copy_from_slice(buf.format(v).as_bytes())
            }
        }
    }
}

/// Visit the live `(field, value)` pairs in the hash's own iteration order,
/// borrowed, until `visit` returns `false`. The liveness rule is exactly
/// `HashRef::len`'s, so position `i` here is position `i` of that count.
fn walk_live_pairs<'a>(
    href: &'a crate::storage::db::HashRef<'_>,
    mut visit: impl FnMut(PairSide<'a>, PairSide<'a>) -> bool,
) {
    use crate::storage::db::HashRef;
    fn walk_ttl<'a>(
        fields: &'a std::collections::HashMap<Bytes, Bytes>,
        ttls: &'a std::collections::HashMap<Bytes, u64>,
        now_ms: u64,
        min_expiry_ms: u64,
        visit: &mut impl FnMut(PairSide<'a>, PairSide<'a>) -> bool,
    ) {
        let all_live = now_ms < min_expiry_ms;
        for (f, v) in fields {
            if (all_live || ttls.get(f).is_none_or(|&t| t > now_ms))
                && !visit(PairSide::Stored(f), PairSide::Stored(v))
            {
                return;
            }
        }
    }
    match href {
        HashRef::Map(map) => {
            for (f, v) in map.iter() {
                if !visit(PairSide::Stored(f), PairSide::Stored(v)) {
                    return;
                }
            }
        }
        HashRef::Owned(map) => {
            for (f, v) in map.iter() {
                if !visit(PairSide::Stored(f), PairSide::Stored(v)) {
                    return;
                }
            }
        }
        HashRef::Listpack(lp) => {
            for (f, v) in lp.iter_pair_refs() {
                if !visit(PairSide::Packed(f), PairSide::Packed(v)) {
                    return;
                }
            }
        }
        HashRef::WithTtl {
            fields,
            ttls,
            now_ms,
            min_expiry_ms,
        } => walk_ttl(fields, ttls, *now_ms, *min_expiry_ms, &mut visit),
        HashRef::OwnedWithTtl {
            fields,
            ttls,
            now_ms,
            min_expiry_ms,
        } => walk_ttl(fields, ttls, *now_ms, *min_expiry_ms, &mut visit),
    }
}

/// Resolve live positions `picks` (any order, repeats allowed) to reply
/// pairs, IN THE ORDER GIVEN, with a single walk that stops at the largest
/// position. The value is materialized only when `with_values`.
fn resolve_picks(
    href: &crate::storage::db::HashRef<'_>,
    picks: &[usize],
    with_values: bool,
) -> Vec<(Bytes, Option<Bytes>)> {
    // Walk order: the slots sorted by the position they want.
    let mut order: Vec<usize> = (0..picks.len()).collect();
    order.sort_unstable_by_key(|&slot| picks[slot]);
    let mut out: Vec<Option<(Bytes, Option<Bytes>)>> = vec![None; picks.len()];
    let mut next = 0usize;
    let mut pos = 0usize;
    walk_live_pairs(href, |f, v| {
        while next < order.len() && picks[order[next]] == pos {
            let value = with_values.then(|| v.to_reply());
            out[order[next]] = Some((f.to_reply(), value));
            next += 1;
        }
        pos += 1;
        next < order.len()
    });
    out.into_iter().flatten().collect()
}

/// Push resolved pairs as a flat HRANDFIELD reply.
fn pairs_to_frame(pairs: Vec<(Bytes, Option<Bytes>)>, with_values: bool) -> Frame {
    let mut result = Vec::with_capacity(if with_values {
        pairs.len() * 2
    } else {
        pairs.len()
    });
    for (field, value) in pairs {
        result.push(Frame::BulkString(field));
        if let Some(v) = value {
            result.push(Frame::BulkString(v));
        }
    }
    Frame::Array(result.into())
}

/// HRANDFIELD readonly path
pub fn hrandfield_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    use rand::RngExt;
    if args.is_empty() || args.len() > 3 {
        return err_wrong_args("HRANDFIELD");
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k.as_ref(),
        None => return err_wrong_args("HRANDFIELD"),
    };
    let href = match db.get_hash_ref_if_alive(key, now_ms) {
        Ok(Some(h)) => h,
        Ok(None) => {
            return if args.len() == 1 {
                Frame::Null
            } else {
                Frame::Array(framevec![])
            };
        }
        Err(e) => return e,
    };
    let len = href.len();
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
        let mut chosen = None;
        let mut pos = 0usize;
        walk_live_pairs(&href, |f, _| {
            if pos == idx {
                chosen = Some(f.to_reply());
                return false;
            }
            pos += 1;
            true
        });
        return match chosen {
            Some(field) => Frame::BulkString(field),
            None => Frame::Null,
        };
    }
    let count_bytes = match extract_bytes(&args[1]) {
        Some(b) => b,
        None => return err_wrong_args("HRANDFIELD"),
    };
    let count: i64 = match std::str::from_utf8(count_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(c) => c,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let with_values = if args.len() == 3 {
        let opt = match extract_bytes(&args[2]) {
            Some(b) => b,
            None => return err_wrong_args("HRANDFIELD"),
        };
        if opt.eq_ignore_ascii_case(b"WITHVALUES") {
            true
        } else {
            return Frame::Error(Bytes::from_static(b"ERR syntax error"));
        }
    } else {
        false
    };
    if count == 0 {
        return Frame::Array(framevec![]);
    }
    if count > 0 {
        let n = std::cmp::min(count as usize, len);
        if n == len {
            // The whole hash, in its own iteration order — Redis's CASE 2
            // ("count >= size: return the whole hash") — with no sampling.
            let mut pairs = Vec::with_capacity(len);
            walk_live_pairs(&href, |f, v| {
                pairs.push((f.to_reply(), with_values.then(|| v.to_reply())));
                true
            });
            return pairs_to_frame(pairs, with_values);
        }
        // n distinct positions in O(n), in random order.
        let picks = rand::seq::index::sample(&mut rng, len, n).into_vec();
        pairs_to_frame(resolve_picks(&href, &picks, with_values), with_values)
    } else {
        // Negative count: allow duplicates — exactly |COUNT| of them (Redis
        // contract). The DoS guard refuses extreme counts loudly instead of
        // silently truncating.
        let n = count.unsigned_abs() as usize;
        if n > crate::command::RAND_DUP_COUNT_MAX {
            return Frame::Error(Bytes::from_static(crate::command::ERR_RAND_COUNT_RANGE));
        }
        let picks: Vec<usize> = (0..n).map(|_| rng.random_range(0..len)).collect();
        pairs_to_frame(resolve_picks(&href, &picks, with_values), with_values)
    }
}

// ---------------------------------------------------------------------------
// Phase 198 — HEXPIRETIME / HPEXPIRETIME / HTTL / HPTTL (read-only)
// ---------------------------------------------------------------------------
//
// Per-field return codes (Valkey 9.0):
//   -2  field does not exist in the hash (or key is missing); note that a
//       missing key is NOT a WRONGTYPE error — it returns -2 per field.
//   -1  field exists but carries no TTL
//   ≥0  HEXPIRETIME/HPEXPIRETIME: absolute unix time (seconds or ms)
//       HTTL/HPTTL: remaining TTL (seconds or ms).
//       An already-expired but not-yet-reaped field yields 0 (Valkey
//       semantics), never -1 or -2.
//
// All four handlers share one core helper:
//   1. Parse key + FIELDS clause via `parse_key_and_fields`.
//   2. WRONGTYPE probe via `get_hash_ref_if_alive` (immutable read, no side effects).
//   3. Per-field loop: map `FieldState` → integer via `map_ttl` closure.

use super::hash_write::parse_key_and_fields;
use crate::protocol::FrameVec;
use crate::storage::db::FieldState;

/// Core driver for HEXPIRETIME / HPEXPIRETIME / HTTL / HPTTL.
///
/// `map_ttl(abs_ms, now_ms) -> i64` converts an absolute expiry timestamp
/// (unix-ms) and the current cached time into the command-specific integer.
/// It is called only for `FieldState::Ttl(_)` arms; `-2` and `-1` are
/// returned unconditionally for `Missing` and `NoTtl`.
fn do_hexpiretime_read(
    db: &Database,
    args: &[Frame],
    cmd: &'static str,
    map_ttl: impl Fn(u64, u64) -> i64,
) -> Frame {
    let parsed = match parse_key_and_fields(args, cmd) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let now_ms = db.now_ms();

    // WRONGTYPE probe — one immutable DashTable lookup.
    // Ok(None)  → key missing; valid — all fields return -2 below.
    // Ok(Some) → hash variant; proceed.
    // Err(f)   → WRONGTYPE; propagate immediately.
    match db.get_hash_ref_if_alive(parsed.key, now_ms) {
        Ok(_) => {}
        Err(e) => return e,
    }

    let mut codes: Vec<Frame> = Vec::with_capacity(parsed.fields.len());
    for field in &parsed.fields {
        let state = db.hash_field_state(parsed.key, field, now_ms);
        let code: i64 = match state {
            FieldState::Missing => -2,
            FieldState::NoTtl => -1,
            // already-expired-but-not-reaped: saturating_sub → 0 for remaining,
            // verbatim abs_ms for absolute-time commands (past timestamp is correct).
            FieldState::Ttl(abs_ms) => map_ttl(abs_ms, now_ms),
        };
        codes.push(Frame::Integer(code));
    }

    Frame::Array(FrameVec::from_vec(codes))
}

/// HEXPIRETIME key FIELDS numfields field [field ...]
///
/// Returns the absolute TTL of each field as a unix timestamp in **seconds**.
/// `-2` = field missing, `-1` = no TTL, `≥0` = absolute unix-seconds.
pub fn hexpiretime(db: &Database, args: &[Frame]) -> Frame {
    do_hexpiretime_read(db, args, "HEXPIRETIME", |abs_ms, _now_ms| {
        (abs_ms / 1000) as i64
    })
}

/// HPEXPIRETIME key FIELDS numfields field [field ...]
///
/// Returns the absolute TTL of each field as a unix timestamp in **milliseconds**.
/// `-2` = field missing, `-1` = no TTL, `≥0` = absolute unix-ms.
pub fn hpexpiretime(db: &Database, args: &[Frame]) -> Frame {
    do_hexpiretime_read(db, args, "HPEXPIRETIME", |abs_ms, _now_ms| abs_ms as i64)
}

/// HTTL key FIELDS numfields field [field ...]
///
/// Returns the remaining TTL of each field in **seconds**.
/// `-2` = field missing, `-1` = no TTL, `0` = already expired (not yet reaped),
/// `>0` = remaining seconds (floor division).
pub fn httl(db: &Database, args: &[Frame]) -> Frame {
    do_hexpiretime_read(db, args, "HTTL", |abs_ms, now_ms| {
        // saturating_sub prevents negative values: already-expired → 0.
        (abs_ms.saturating_sub(now_ms) / 1000) as i64
    })
}

/// HPTTL key FIELDS numfields field [field ...]
///
/// Returns the remaining TTL of each field in **milliseconds**.
/// `-2` = field missing, `-1` = no TTL, `0` = already expired (not yet reaped),
/// `>0` = remaining ms.
pub fn hpttl(db: &Database, args: &[Frame]) -> Frame {
    do_hexpiretime_read(db, args, "HPTTL", |abs_ms, now_ms| {
        abs_ms.saturating_sub(now_ms) as i64
    })
}

// ---------------------------------------------------------------------------
// moon#1171 — HRANDFIELD contract and cost
// ---------------------------------------------------------------------------

#[cfg(test)]
mod hrandfield_1171 {
    use super::*;
    use crate::storage::db::HashRef;
    use std::collections::{HashMap, HashSet};

    fn frames(args: &[&[u8]]) -> Vec<Frame> {
        args.iter()
            .map(|a| Frame::BulkString(Bytes::copy_from_slice(a)))
            .collect()
    }

    fn load(db: &mut Database, key: &[u8], n: usize) -> HashMap<Bytes, Bytes> {
        let mut want = HashMap::new();
        let mut args: Vec<Vec<u8>> = vec![key.to_vec()];
        for i in 0..n {
            let f = format!("f{i}");
            // Integers too, so listpack integer entries are rendered back.
            let v = if i % 3 == 0 {
                i.to_string()
            } else {
                format!("value-{i}")
            };
            args.push(f.clone().into_bytes());
            args.push(v.clone().into_bytes());
            want.insert(Bytes::from(f), Bytes::from(v));
        }
        let refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        crate::command::hash::hset(db, &frames(&refs));
        want
    }

    fn pairs(f: &Frame, with_values: bool) -> Vec<(Bytes, Option<Bytes>)> {
        let Frame::Array(items) = f else {
            panic!("expected array, got {f:?}");
        };
        let items: Vec<Bytes> = items
            .iter()
            .map(|x| match x {
                Frame::BulkString(b) => b.clone(),
                other => panic!("not a bulk: {other:?}"),
            })
            .collect();
        if with_values {
            items
                .chunks(2)
                .map(|c| (c[0].clone(), Some(c[1].clone())))
                .collect()
        } else {
            items.into_iter().map(|m| (m, None)).collect()
        }
    }

    fn check(db: &mut Database, want: &HashMap<Bytes, Bytes>, now_ms: u64) {
        let n = want.len();
        let mut seen = HashSet::new();
        for _ in 0..(n * 40) {
            match hrandfield_readonly(db, &frames(&[b"h"]), now_ms) {
                Frame::BulkString(f) => {
                    assert!(want.contains_key(&f), "drew a dead/non field {f:?}");
                    seen.insert(f);
                }
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(seen.len(), n, "some live field was never drawn");
        for k in [1usize, 3, n.saturating_sub(1).max(1), n, n + 5] {
            for with_values in [false, true] {
                let k_s = k.to_string();
                let mut args: Vec<&[u8]> = vec![b"h", k_s.as_bytes()];
                if with_values {
                    args.push(b"WITHVALUES");
                }
                let got = pairs(
                    &hrandfield_readonly(db, &frames(&args), now_ms),
                    with_values,
                );
                assert_eq!(got.len(), k.min(n), "count {k}");
                let distinct: HashSet<_> = got.iter().map(|(f, _)| f.clone()).collect();
                assert_eq!(distinct.len(), got.len(), "count {k}: duplicate field");
                for (f, v) in &got {
                    let live = want.get(f).expect("returned a dead field");
                    if let Some(v) = v {
                        assert_eq!(v, live, "value of {f:?}");
                    }
                }
            }
        }
        let got = pairs(
            &hrandfield_readonly(db, &frames(&[b"h", b"-37", b"WITHVALUES"]), now_ms),
            true,
        );
        assert_eq!(got.len(), 37);
        for (f, v) in &got {
            assert_eq!(want.get(f), v.as_ref(), "negative count pair {f:?}");
        }
        // The mutable path runs the same body.
        let got = pairs(&hrandfield(db, &frames(&[b"h", b"3"])), false);
        assert_eq!(got.len(), 3.min(n));
    }

    #[test]
    fn hrandfield_contract_on_every_representation() {
        // 300 fields: full HashMap. 12 fields: listpack.
        for n in [300usize, 12] {
            let mut db = Database::new();
            let want = load(&mut db, b"h", n);
            let now = db.now_ms();
            check(&mut db, &want, now);
        }
        // Field TTLs: a third of the fields expire; read "after" their
        // deadline through the shared-borrow path, which filters them.
        let mut db = Database::new();
        let mut want = load(&mut db, b"h", 300);
        let now = db.now_ms();
        let deadline = (now + 1_000).to_string();
        let doomed: Vec<Bytes> = want.keys().filter(|f| f.len() % 3 == 0).cloned().collect();
        let numfields = doomed.len().to_string();
        let mut args: Vec<&[u8]> = vec![b"h", deadline.as_bytes(), b"FIELDS", numfields.as_bytes()];
        for f in &doomed {
            args.push(f);
        }
        crate::command::hash::hpexpireat(&mut db, &frames(&args));
        for f in &doomed {
            want.remove(f);
        }
        assert!(matches!(
            db.get_hash_ref_if_alive(b"h", now + 2_000),
            Ok(Some(HashRef::WithTtl { .. }))
        ));
        check(&mut db, &want, now + 2_000);
    }

    fn best_ns(reps: usize, iters: usize, mut f: impl FnMut()) -> u128 {
        (0..reps)
            .map(|_| {
                let t = std::time::Instant::now();
                for _ in 0..iters {
                    f();
                }
                t.elapsed().as_nanos()
            })
            .min()
            .unwrap_or(0)
    }

    /// HRANDFIELD must cost well under what materializing the hash costs —
    /// HEAD `935c555` called `HashRef::entries()` (a clone of every pair) and
    /// then built an N-element index Vec, so it cost MORE than `entries()`.
    /// The borrowed walk stops at the drawn position and clones one pair
    /// (measured in a debug build: ~1.6 ms vs ~85 ms per call on 200K fields;
    /// the bound is 5x).
    #[test]
    fn hrandfield_does_not_materialize_the_hash() {
        let mut db = Database::new();
        load(&mut db, b"h", 200_000);
        let now = db.now_ms();
        let href = db.get_hash_ref_if_alive(b"h", now).unwrap().unwrap();
        let t_entries = best_ns(5, 5, || {
            std::hint::black_box(href.entries());
        });
        for args in [frames(&[b"h"]), frames(&[b"h", b"5", b"WITHVALUES"])] {
            let t_rand = best_ns(5, 5, || {
                std::hint::black_box(hrandfield_readonly(&db, std::hint::black_box(&args), now));
            });
            assert!(
                t_rand * 5 < t_entries,
                "HRANDFIELD ({} args) took {t_rand} ns vs {t_entries} ns to clone the hash",
                args.len()
            );
        }
    }
}
