//! Implementation of `MOVE key db` (T2.2) and `COPY ... DB n` (T2.3).
//!
//! Both commands operate on two databases simultaneously and cannot go through
//! the central `dispatch()` function which only receives one `&mut Database`.
//! Each handler intercepts these commands before reaching `dispatch()`.
//!
//! # MOVE semantics
//! `MOVE key db` moves a key from the connection's currently-selected database
//! to another database on the **same shard**. Cross-shard moves return an error
//! directing users to `MIGRATE` (T2.8).
//!
//! Returns `:1` on success, `:0` if key is missing or target has the key.
//!
//! # COPY DB n semantics
//! `COPY src dst DB n [REPLACE]` copies a key to a different database.
//! Returns `:1` on success, `:0` on collision without REPLACE.
//!
//! # Lock ordering
//! Lower database index is always locked first to prevent deadlocks with
//! concurrent reverse MOVE/COPY-DB operations.

use bytes::Bytes;
use parking_lot::RwLock;

use crate::command::helpers::extract_bytes;
use crate::protocol::Frame;
use crate::storage::Database;

// ── MOVE core logic ────────────────────────────────────────────────────────────

/// Move a key from `src` to `dst`. Pure data-plane logic — no locking, no WAL.
///
/// Returns `:1` on success, `:0` on no-op (key absent or collision in dst).
///
/// # Preconditions
/// - `src` and `dst` are two **distinct** databases from the same shard
/// - The caller holds exclusive (write) access to both
pub fn move_core(src: &mut Database, dst: &mut Database, key: &[u8]) -> Frame {
    // Key must exist in src (lazy expiry applied inside `remove`)
    let entry = match src.remove(key) {
        Some(e) => e,
        None => return Frame::Integer(0),
    };

    // Collision check: key must NOT exist in dst
    if dst.exists(key) {
        // Restore the entry to src — the move did not happen
        src.set(key, entry);
        return Frame::Integer(0);
    }

    // Move: insert into dst, TTL is carried inside the Entry value
    dst.set(key, entry);
    Frame::Integer(1)
}

// ── COPY core logic ────────────────────────────────────────────────────────────

/// Copy `src_key` from database `src` to `dst_key` in database `dst`.
/// Pure data-plane logic — no locking, no WAL.
///
/// Returns `:1` on success, `:0` on collision when `replace` is false.
///
/// # Preconditions
/// - `src` and `dst` are two **distinct** databases from the same shard
/// - The caller holds exclusive (write) access to both
pub fn copy_core(
    src: &mut Database,
    dst: &mut Database,
    src_key: &[u8],
    dst_key: &[u8],
    replace: bool,
) -> Frame {
    // Source must exist
    let entry = match src.get(src_key) {
        Some(e) => e.clone(),
        None => return Frame::Integer(0),
    };

    // Same src and dst key in different dbs is allowed; same key same db is
    // rejected by parse_copy_db_args. Nothing special needed here.

    // Collision in dst
    if dst.exists(dst_key) {
        if !replace {
            return Frame::Integer(0);
        }
        // REPLACE: overwrite dst
    }

    dst.set(dst_key, entry);
    Frame::Integer(1)
}

// ── Argument parsing ───────────────────────────────────────────────────────────

/// Parse `MOVE key db` args (everything after the command name).
///
/// Returns `(key_bytes, target_db_index)` or an error frame.
pub fn parse_move_args(args: &[Frame], db_count: usize) -> Result<(Bytes, usize), Frame> {
    if args.len() != 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'move' command",
        )));
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) if !k.is_empty() => Bytes::copy_from_slice(k),
        _ => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR invalid key for MOVE command",
            )));
        }
    };
    let db_str = match extract_bytes(&args[1]) {
        Some(s) => s,
        None => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };
    let db_index = parse_db_index(db_str, db_count)?;
    Ok((key, db_index))
}

/// Parse a destination db token the way redis does: a token that is not an
/// integer is `ERR value is not an integer or out of range`; an integer that
/// names no database — negative included — is `ERR DB index is out of range`.
fn parse_db_index(tok: &[u8], db_count: usize) -> Result<usize, Frame> {
    let n = std::str::from_utf8(tok)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| {
            Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ))
        })?;
    match usize::try_from(n) {
        Ok(idx) if idx < db_count => Ok(idx),
        _ => Err(Frame::Error(Bytes::from_static(ERR_DB_OUT_OF_RANGE))),
    }
}

/// Redis's reply for a db index that names no database.
const ERR_DB_OUT_OF_RANGE: &[u8] = b"ERR DB index is out of range";

/// Redis's reply for `MOVE key <current db>` and `COPY k k [DB <current db>]`.
pub const ERR_SAME_OBJECT: &[u8] = b"ERR source and destination objects are the same";

/// `MOVE key db` resolved against the source db: the key and the destination
/// db, or the command's error reply.
///
/// Unlike [`parse_move_args`] this also refuses a destination equal to
/// `src_db` with redis's `ERR source and destination objects are the same`
/// (redis checks it before looking the key up, so a missing key gets the same
/// error). Every live MOVE path and the transaction executors use this, so the
/// command answers identically wherever it runs.
pub fn resolve_move(
    args: &[Frame],
    src_db: usize,
    db_count: usize,
) -> Result<(Bytes, usize), Frame> {
    let (key, dst_db) = parse_move_args(args, db_count)?;
    if dst_db == src_db {
        return Err(Frame::Error(Bytes::from_static(ERR_SAME_OBJECT)));
    }
    Ok((key, dst_db))
}

/// Parsed result for COPY when it includes a `DB n` clause targeting a different database.
#[derive(Debug)]
pub struct CopyDbArgs {
    pub src_key: Bytes,
    pub dst_key: Bytes,
    pub dst_db: usize,
    pub replace: bool,
}

/// Parse `COPY src dst [DB n] [REPLACE]` args for the cross-db case.
///
/// Returns `Some(Ok(CopyDbArgs))` when a `DB n` clause is present and `n`
/// differs from `current_db` (cross-db operation — must be intercepted).
///
/// Returns `Some(Err(frame))` when the `DB n` clause is present but invalid.
///
/// Returns `None` when no `DB n` clause is present — caller falls through
/// to the existing `key_extra::copy()` single-db path.
///
/// When `n == current_db`, returns `None` so the call falls through to the
/// single-db path (same-db COPY is fully handled by `key_extra::copy`).
pub fn parse_copy_db_args(
    args: &[Frame],
    current_db: usize,
    db_count: usize,
) -> Option<Result<CopyDbArgs, Frame>> {
    if args.len() < 2 {
        return None; // wrong arity — let key_extra::copy produce the error
    }
    let src_key = extract_bytes(&args[0])?;
    let dst_key = extract_bytes(&args[1])?;

    let mut replace = false;
    let mut dst_db_opt: Option<usize> = None;
    let mut i = 2;
    while i < args.len() {
        let tok = match extract_bytes(&args[i]) {
            Some(t) => t,
            None => return Some(Err(Frame::Error(Bytes::from_static(b"ERR syntax error")))),
        };
        if tok.eq_ignore_ascii_case(b"REPLACE") {
            replace = true;
            i += 1;
        } else if tok.eq_ignore_ascii_case(b"DB") {
            i += 1;
            let db_tok = match args.get(i).and_then(|f| extract_bytes(f)) {
                Some(t) => t,
                None => return Some(Err(Frame::Error(Bytes::from_static(b"ERR syntax error")))),
            };
            let n = match parse_db_index(db_tok, db_count) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };
            dst_db_opt = Some(n);
            i += 1;
        } else {
            return Some(Err(Frame::Error(Bytes::from_static(b"ERR syntax error"))));
        }
    }

    // None here means no DB clause was present — fall through to key_extra::copy.
    let dst_db = dst_db_opt?;

    if dst_db == current_db {
        // Same db: fall through to key_extra::copy (which handles same-db correctly)
        return None;
    }

    Some(Ok(CopyDbArgs {
        src_key: Bytes::copy_from_slice(src_key),
        dst_key: Bytes::copy_from_slice(dst_key),
        dst_db,
        replace,
    }))
}

// ── Transaction-executor entry point ──────────────────────────────────────────

/// A `MOVE`, or a `COPY` whose `DB` clause names another database, resolved
/// against the database it runs in.
///
/// The MULTI/EXEC executors run each queued command through the single-db
/// `dispatch()`, which cannot see a second database. They ask
/// [`resolve_two_db`] first and, on `Some(Ok(op))`, hand [`TwoDbOp::apply`]
/// the source and destination databases — the same [`move_core`] /
/// [`copy_core`] the live intercepts, the replica apply path and AOF replay
/// use, so every path writes the same keyspace.
#[derive(Debug)]
pub enum TwoDbOp {
    /// `MOVE key dst_db`.
    Move { key: Bytes, dst_db: usize },
    /// `COPY src dst DB dst_db [REPLACE]`.
    Copy(CopyDbArgs),
}

impl TwoDbOp {
    /// The database the command writes into (never the source db).
    #[must_use]
    pub fn dst_db(&self) -> usize {
        match self {
            TwoDbOp::Move { dst_db, .. } => *dst_db,
            TwoDbOp::Copy(ca) => ca.dst_db,
        }
    }

    /// Run the command against its source and destination databases.
    /// `:1` means the keyspace changed; anything else wrote nothing.
    pub fn apply(&self, src: &mut Database, dst: &mut Database) -> Frame {
        match self {
            TwoDbOp::Move { key, .. } => move_core(src, dst, key),
            TwoDbOp::Copy(ca) => copy_core(src, dst, &ca.src_key, &ca.dst_key, ca.replace),
        }
    }
}

/// Classify one command for the transaction executors.
///
/// * `None` — not a two-db command. That includes a `COPY` without a `DB`
///   clause and one whose `DB` names `src_db`: both are ordinary same-db
///   copies for the single-db dispatch.
/// * `Some(Err(reply))` — the command's own error, which is its whole reply
///   (redis's wording: see [`resolve_move`] and [`parse_copy_db_args`]).
/// * `Some(Ok(op))` — apply `op` to `(src_db, op.dst_db())`.
#[must_use]
pub fn resolve_two_db(
    cmd: &[u8],
    args: &[Frame],
    src_db: usize,
    db_count: usize,
) -> Option<Result<TwoDbOp, Frame>> {
    if cmd.eq_ignore_ascii_case(b"MOVE") {
        return Some(
            resolve_move(args, src_db, db_count).map(|(key, dst_db)| TwoDbOp::Move { key, dst_db }),
        );
    }
    if cmd.eq_ignore_ascii_case(b"COPY") {
        return parse_copy_db_args(args, src_db, db_count).map(|r| r.map(TwoDbOp::Copy));
    }
    None
}

// ── RwLock-based two-db helper (handler_single path) ──────────────────────────

/// Acquire write locks on two databases in a deadlock-safe order (lower index first).
///
/// The closure receives `(src, dst)` with `src` being the database at `src_idx`
/// and `dst` the database at `dst_idx`.
///
/// # Panics
/// Panics if either index is out of bounds.
pub fn with_two_dbs_locked<R>(
    dbs: &[RwLock<Database>],
    src_idx: usize,
    dst_idx: usize,
    f: impl FnOnce(&mut Database, &mut Database) -> R,
) -> R {
    assert!(
        src_idx < dbs.len(),
        "src_idx {src_idx} out of range ({} dbs)",
        dbs.len()
    );
    assert!(
        dst_idx < dbs.len(),
        "dst_idx {dst_idx} out of range ({} dbs)",
        dbs.len()
    );
    // Same index would self-deadlock on the second write() — parking_lot
    // RwLock is not reentrant. Callers (MOVE/COPY DB n) short-circuit
    // src == dst to :0 before reaching here; hard-assert in release.
    assert_ne!(
        src_idx, dst_idx,
        "with_two_dbs_locked called with src_idx == dst_idx; caller must short-circuit"
    );

    if src_idx < dst_idx {
        let mut lo = dbs[src_idx].write();
        let mut hi = dbs[dst_idx].write();
        f(&mut lo, &mut hi)
    } else {
        let mut lo = dbs[dst_idx].write();
        let mut hi = dbs[src_idx].write();
        f(&mut hi, &mut lo)
    }
}

// ── Slice-based two-db helper (ShardSlice path: no RwLock) ────────────────────

/// Borrow two disjoint databases from a `&mut [Database]` slice.
///
/// Uses `split_at_mut` to produce two non-aliasing `&mut Database` references.
/// The closure receives `(src, dst)`.
///
/// # Panics
/// Panics if either index is out of bounds, or if `src_idx == dst_idx`.
pub fn with_two_slice_dbs<R>(
    dbs: &mut [Database],
    src_idx: usize,
    dst_idx: usize,
    f: impl FnOnce(&mut Database, &mut Database) -> R,
) -> R {
    assert_ne!(
        src_idx, dst_idx,
        "with_two_slice_dbs: src and dst must differ"
    );
    assert!(
        src_idx < dbs.len(),
        "src_idx {src_idx} out of range ({} dbs)",
        dbs.len()
    );
    assert!(
        dst_idx < dbs.len(),
        "dst_idx {dst_idx} out of range ({} dbs)",
        dbs.len()
    );

    if src_idx < dst_idx {
        let (lo, hi) = dbs.split_at_mut(dst_idx);
        f(&mut lo[src_idx], &mut hi[0])
    } else {
        // dst_idx < src_idx
        let (lo, hi) = dbs.split_at_mut(src_idx);
        f(&mut hi[0], &mut lo[dst_idx])
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::Entry;

    fn make_db() -> Database {
        Database::new()
    }

    fn set_str(db: &mut Database, key: &str, val: &str) {
        let entry = Entry::new_string(Bytes::from(val.to_owned()));
        db.set(key.as_bytes(), entry);
    }

    // ── move_core ───────────────────────────────────────────────────────────────

    #[test]
    fn test_move_core_success() {
        let mut src = make_db();
        let mut dst = make_db();
        set_str(&mut src, "k", "v");

        let frame = move_core(&mut src, &mut dst, b"k");
        assert_eq!(frame, Frame::Integer(1));
        assert!(!src.exists(b"k"), "key must be removed from src");
        assert!(dst.exists(b"k"), "key must be present in dst");
    }

    #[test]
    fn test_move_core_key_missing_in_src() {
        let mut src = make_db();
        let mut dst = make_db();
        let frame = move_core(&mut src, &mut dst, b"missing");
        assert_eq!(frame, Frame::Integer(0));
    }

    #[test]
    fn test_move_core_collision_in_dst() {
        let mut src = make_db();
        let mut dst = make_db();
        set_str(&mut src, "k", "src-val");
        set_str(&mut dst, "k", "dst-val");

        let frame = move_core(&mut src, &mut dst, b"k");
        assert_eq!(frame, Frame::Integer(0));
        assert!(src.exists(b"k"), "src key must be restored on collision");
        assert!(dst.exists(b"k"), "dst key must survive");
    }

    // ── copy_core ───────────────────────────────────────────────────────────────

    #[test]
    fn test_copy_core_success() {
        let mut src = make_db();
        let mut dst = make_db();
        set_str(&mut src, "src", "hello");

        let frame = copy_core(&mut src, &mut dst, b"src", b"dst", false);
        assert_eq!(frame, Frame::Integer(1));
        assert!(src.exists(b"src"), "src must still exist after copy");
        assert!(dst.exists(b"dst"), "dst must have the copied value");
    }

    #[test]
    fn test_copy_core_missing_src() {
        let mut src = make_db();
        let mut dst = make_db();
        let frame = copy_core(&mut src, &mut dst, b"missing", b"dst", false);
        assert_eq!(frame, Frame::Integer(0));
    }

    #[test]
    fn test_copy_core_collision_no_replace() {
        let mut src = make_db();
        let mut dst = make_db();
        set_str(&mut src, "src", "new");
        set_str(&mut dst, "dst", "old");

        let frame = copy_core(&mut src, &mut dst, b"src", b"dst", false);
        assert_eq!(frame, Frame::Integer(0));
    }

    #[test]
    fn test_copy_core_collision_replace() {
        let mut src = make_db();
        let mut dst = make_db();
        set_str(&mut src, "src", "new");
        set_str(&mut dst, "dst", "old");

        let frame = copy_core(&mut src, &mut dst, b"src", b"dst", true);
        assert_eq!(frame, Frame::Integer(1));
        assert!(dst.exists(b"dst"));
    }

    // ── parse_move_args ─────────────────────────────────────────────────────────

    #[test]
    fn test_parse_move_args_ok() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"mykey")),
            Frame::BulkString(Bytes::from_static(b"3")),
        ];
        let (key, idx) = parse_move_args(&args, 16).unwrap();
        assert_eq!(&key[..], b"mykey");
        assert_eq!(idx, 3);
    }

    #[test]
    fn test_parse_move_args_wrong_arity() {
        let args = vec![Frame::BulkString(Bytes::from_static(b"k"))];
        assert!(parse_move_args(&args, 16).is_err());
    }

    #[test]
    fn test_parse_move_args_negative_db() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::BulkString(Bytes::from_static(b"-1")),
        ];
        assert!(parse_move_args(&args, 16).is_err());
    }

    #[test]
    fn test_parse_move_args_db_out_of_range() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::BulkString(Bytes::from_static(b"16")),
        ];
        let err = parse_move_args(&args, 16).unwrap_err();
        assert!(matches!(err, Frame::Error(_)));
    }

    #[test]
    fn test_parse_move_args_nonnumeric_db() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"k")),
            Frame::BulkString(Bytes::from_static(b"abc")),
        ];
        assert!(parse_move_args(&args, 16).is_err());
    }

    // ── parse_copy_db_args ──────────────────────────────────────────────────────

    #[test]
    fn test_parse_copy_db_args_no_db_clause() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"src")),
            Frame::BulkString(Bytes::from_static(b"dst")),
        ];
        // No DB clause → returns None (fall through to key_extra::copy)
        assert!(parse_copy_db_args(&args, 0, 16).is_none());
    }

    #[test]
    fn test_parse_copy_db_args_same_db() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"src")),
            Frame::BulkString(Bytes::from_static(b"dst")),
            Frame::BulkString(Bytes::from_static(b"DB")),
            Frame::BulkString(Bytes::from_static(b"0")),
        ];
        // DB 0 == current_db 0 → None (same-db, fall through)
        assert!(parse_copy_db_args(&args, 0, 16).is_none());
    }

    #[test]
    fn test_parse_copy_db_args_cross_db() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"src")),
            Frame::BulkString(Bytes::from_static(b"dst")),
            Frame::BulkString(Bytes::from_static(b"DB")),
            Frame::BulkString(Bytes::from_static(b"3")),
        ];
        let result = parse_copy_db_args(&args, 0, 16).unwrap().unwrap();
        assert_eq!(&result.src_key[..], b"src");
        assert_eq!(&result.dst_key[..], b"dst");
        assert_eq!(result.dst_db, 3);
        assert!(!result.replace);
    }

    #[test]
    fn test_parse_copy_db_args_with_replace() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"src")),
            Frame::BulkString(Bytes::from_static(b"dst")),
            Frame::BulkString(Bytes::from_static(b"DB")),
            Frame::BulkString(Bytes::from_static(b"3")),
            Frame::BulkString(Bytes::from_static(b"REPLACE")),
        ];
        let result = parse_copy_db_args(&args, 0, 16).unwrap().unwrap();
        assert!(result.replace);
    }

    #[test]
    fn test_parse_copy_db_args_invalid_db() {
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"src")),
            Frame::BulkString(Bytes::from_static(b"dst")),
            Frame::BulkString(Bytes::from_static(b"DB")),
            Frame::BulkString(Bytes::from_static(b"99")),
        ];
        let err = parse_copy_db_args(&args, 0, 16).unwrap().unwrap_err();
        assert!(matches!(err, Frame::Error(_)));
    }

    // ── redis error wording (moon#1062) ─────────────────────────────────────────

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Bytes::from(s.to_owned()))
    }

    fn err_text(f: &Frame) -> &[u8] {
        match f {
            Frame::Error(e) => e,
            other => panic!("expected an error frame, got {other:?}"),
        }
    }

    #[test]
    fn db_index_errors_use_redis_wording() {
        // A token that is not an integer, then integers naming no database —
        // negative included, which redis reports as out of range, not as
        // "not an integer".
        let e = parse_move_args(&[bulk("k"), bulk("x")], 16).unwrap_err();
        assert_eq!(err_text(&e), b"ERR value is not an integer or out of range");
        for bad in ["-1", "16", "99"] {
            let e = parse_move_args(&[bulk("k"), bulk(bad)], 16).unwrap_err();
            assert_eq!(err_text(&e), ERR_DB_OUT_OF_RANGE, "MOVE k {bad}");
            let e = parse_copy_db_args(&[bulk("a"), bulk("b"), bulk("DB"), bulk(bad)], 0, 16)
                .unwrap()
                .unwrap_err();
            assert_eq!(err_text(&e), ERR_DB_OUT_OF_RANGE, "COPY a b DB {bad}");
        }
        let e = parse_copy_db_args(&[bulk("a"), bulk("b"), bulk("DB"), bulk("x")], 0, 16)
            .unwrap()
            .unwrap_err();
        assert_eq!(err_text(&e), b"ERR value is not an integer or out of range");
    }

    #[test]
    fn move_into_the_source_db_is_redis_same_object_error() {
        let e = resolve_move(&[bulk("k"), bulk("2")], 2, 16).unwrap_err();
        assert_eq!(err_text(&e), ERR_SAME_OBJECT);
        let (key, dst) = resolve_move(&[bulk("k"), bulk("3")], 2, 16).unwrap();
        assert_eq!((&key[..], dst), (&b"k"[..], 3));
    }

    #[test]
    fn resolve_two_db_classifies_what_needs_two_databases() {
        // Not a two-db command at all.
        assert!(resolve_two_db(b"SET", &[bulk("k"), bulk("v")], 0, 16).is_none());
        // COPY without a DB clause, and with one naming the source db, is an
        // ordinary same-db copy for the single-db dispatch.
        assert!(resolve_two_db(b"COPY", &[bulk("a"), bulk("b")], 0, 16).is_none());
        assert!(
            resolve_two_db(
                b"copy",
                &[bulk("a"), bulk("b"), bulk("DB"), bulk("5")],
                5,
                16
            )
            .is_none()
        );
        // Errors are the whole reply.
        let e = resolve_two_db(b"MOVE", &[bulk("k"), bulk("0")], 0, 16)
            .unwrap()
            .unwrap_err();
        assert_eq!(err_text(&e), ERR_SAME_OBJECT);
        // Both commands, any casing, name their destination.
        let op = resolve_two_db(b"move", &[bulk("k"), bulk("3")], 0, 16)
            .unwrap()
            .unwrap();
        assert_eq!(op.dst_db(), 3);
        let op = resolve_two_db(
            b"COPY",
            &[bulk("a"), bulk("b"), bulk("db"), bulk("4"), bulk("replace")],
            0,
            16,
        )
        .unwrap()
        .unwrap();
        assert_eq!(op.dst_db(), 4);
    }

    #[test]
    fn two_db_op_apply_moves_and_copies_between_the_given_dbs() {
        let mut src = make_db();
        let mut dst = make_db();
        set_str(&mut src, "m", "mv");
        set_str(&mut src, "c", "cv");
        let mv = resolve_two_db(b"MOVE", &[bulk("m"), bulk("1")], 0, 16)
            .unwrap()
            .unwrap();
        assert_eq!(mv.apply(&mut src, &mut dst), Frame::Integer(1));
        assert!(!src.exists(b"m") && dst.exists(b"m"));
        // A second MOVE finds the source empty: a no-op.
        assert_eq!(mv.apply(&mut src, &mut dst), Frame::Integer(0));

        let cp = resolve_two_db(
            b"COPY",
            &[bulk("c"), bulk("c2"), bulk("DB"), bulk("1")],
            0,
            16,
        )
        .unwrap()
        .unwrap();
        assert_eq!(cp.apply(&mut src, &mut dst), Frame::Integer(1));
        assert!(src.exists(b"c") && dst.exists(b"c2") && !src.exists(b"c2"));
        // Without REPLACE a second copy collides.
        assert_eq!(cp.apply(&mut src, &mut dst), Frame::Integer(0));
    }

    // ── with_two_dbs_locked ─────────────────────────────────────────────────────

    #[test]
    fn test_with_two_dbs_locked_lower_first() {
        let dbs: Vec<RwLock<Database>> = (0..4).map(|_| RwLock::new(make_db())).collect();
        with_two_dbs_locked(&dbs, 0, 2, |src, dst| {
            set_str(src, "x", "hello");
            let frame = move_core(src, dst, b"x");
            assert_eq!(frame, Frame::Integer(1));
        });
    }

    #[test]
    fn test_with_two_dbs_locked_higher_first() {
        let dbs: Vec<RwLock<Database>> = (0..4).map(|_| RwLock::new(make_db())).collect();
        with_two_dbs_locked(&dbs, 3, 1, |src, dst| {
            set_str(src, "y", "world");
            let frame = move_core(src, dst, b"y");
            assert_eq!(frame, Frame::Integer(1));
        });
    }

    // ── with_two_slice_dbs ──────────────────────────────────────────────────────

    #[test]
    fn test_with_two_slice_dbs_lower_src() {
        let mut dbs: Vec<Database> = (0..4).map(|_| make_db()).collect();
        set_str(&mut dbs[0], "z", "val");
        with_two_slice_dbs(&mut dbs, 0, 2, |src, dst| {
            let frame = move_core(src, dst, b"z");
            assert_eq!(frame, Frame::Integer(1));
        });
        assert!(!dbs[0].exists(b"z"));
        assert!(dbs[2].exists(b"z"));
    }

    #[test]
    fn test_with_two_slice_dbs_higher_src() {
        let mut dbs: Vec<Database> = (0..4).map(|_| make_db()).collect();
        set_str(&mut dbs[3], "w", "val");
        with_two_slice_dbs(&mut dbs, 3, 1, |src, dst| {
            let frame = move_core(src, dst, b"w");
            assert_eq!(frame, Frame::Integer(1));
        });
        assert!(!dbs[3].exists(b"w"));
        assert!(dbs[1].exists(b"w"));
    }
}
