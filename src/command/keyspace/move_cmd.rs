//! Implementation of `MOVE key db` (T2.2).
//!
//! `MOVE` operates on two databases simultaneously and cannot go through
//! the central `dispatch()` function which only receives one `&mut Database`.
//! Each handler intercepts the command before reaching `dispatch()`.
//!
//! # MOVE semantics
//! `MOVE key db` moves a key from the connection's currently-selected database
//! to another database on the **same shard**. Cross-shard moves return an error
//! directing users to `MIGRATE` (T2.8).
//!
//! Returns `:1` on success, `:0` if key is missing or target has the key.
//!
//! # Lock ordering
//! Lower database index is always locked first to prevent deadlocks with
//! concurrent reverse MOVE operations.

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
        src.set(Bytes::copy_from_slice(key), entry);
        return Frame::Integer(0);
    }

    // Move: insert into dst, TTL is carried inside the Entry value
    dst.set(Bytes::copy_from_slice(key), entry);
    Frame::Integer(1)
}

// ── Argument parsing ───────────────────────────────────────────────────────────

/// Parse `MOVE key db` args (everything after the command name).
///
/// Returns `(key_bytes, target_db_index)` or an error frame.
pub fn parse_move_args(args: &[Frame], db_count: usize) -> Result<(Bytes, usize), Frame> {
    if args.len() != 2 {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'MOVE' command",
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
    let db_index: usize = match std::str::from_utf8(db_str)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(n) if n >= 0 => n as usize,
        _ => {
            return Err(Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            )));
        }
    };
    if db_index >= db_count {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR DB index is out of range",
        )));
    }
    Ok((key, db_index))
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

    if src_idx < dst_idx {
        let mut lo = dbs[src_idx].write();
        let mut hi = dbs[dst_idx].write();
        f(&mut lo, &mut hi)
    } else {
        // dst_idx < src_idx (equality rejected by caller: src == dst → :0)
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
        db.set(Bytes::from(key.to_owned()), entry);
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
