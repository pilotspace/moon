//! `ZRANGEBYLEX` and `ZREVRANGEBYLEX` (moon#959).
//!
//! Own file rather than `sorted_set_read.rs`, which already sits at the
//! 1500-line rule. Same shape as `zrangebyscore_readonly`: the mutable-path
//! entry delegates to the shared-borrow twin so a listpack zset survives the
//! read (moon#928), and both encodings answer through the ONE pair of range
//! helpers `ZRANGE ... BYLEX` already uses, so the legacy spelling and the
//! unified one cannot drift apart. Tests stay in `mod.rs`.

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use crate::command::helpers::{err, err_wrong_args, extract_bytes};

use super::{parse_lex_bound, zrange_by_lex, zrange_from_entries};

/// ZRANGEBYLEX key min max [LIMIT offset count].
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928).
pub fn zrangebylex(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrangebylex_readonly(db, args, now_ms)
}

/// ZREVRANGEBYLEX key max min [LIMIT offset count].
///
/// Reads through the shared-borrow implementation so the zset's compact
/// encoding survives the read (moon#928).
pub fn zrevrangebylex(db: &mut Database, args: &[Frame]) -> Frame {
    let now_ms = db.now_ms();
    zrevrangebylex_readonly(db, args, now_ms)
}

/// ZRANGEBYLEX (read-only).
pub fn zrangebylex_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    zrangebylex_impl(db, args, now_ms, false)
}

/// ZREVRANGEBYLEX (read-only).
pub fn zrevrangebylex_readonly(db: &Database, args: &[Frame], now_ms: u64) -> Frame {
    zrangebylex_impl(db, args, now_ms, true)
}

/// The one implementation behind both spellings.
///
/// Error precedence follows Redis's `zrangeGenericCommand`, verified against
/// redis-server 8.6.1: the option loop first (a dangling `LIMIT` or an unknown
/// token is `syntax error`, a non-integer `LIMIT` value is the generic integer
/// error), then `WITHSCORES` — which the legacy spelling parses but refuses
/// with its own message, BEFORE it looks at the bounds — then the range
/// grammar (`min or max not valid string range item`), and only then the key.
/// The bounds are therefore validated BEFORE the lookup, so `ZRANGEBYLEX
/// nokey a b` is an error and not an empty array. The first draft checked the
/// bounds before WITHSCORES; the oracle sweep of the built binary caught it —
/// `ZRANGEBYLEX k a b WITHSCORES` is the WITHSCORES error on redis.
fn zrangebylex_impl(db: &Database, args: &[Frame], now_ms: u64, rev: bool) -> Frame {
    let cmd = if rev { "ZREVRANGEBYLEX" } else { "ZRANGEBYLEX" };
    if args.len() < 3 {
        return err_wrong_args(cmd);
    }
    let key = match extract_bytes(&args[0]) {
        Some(k) => k,
        None => return err_wrong_args(cmd),
    };
    // ZREVRANGEBYLEX takes `max min`; the range helpers take `(min, max)` in
    // semantic order and only ever use `rev` for iteration direction, exactly
    // as `zrevrangebyscore_readonly` does.
    let (min_idx, max_idx) = if rev { (2, 1) } else { (1, 2) };
    let min_arg = match extract_bytes(&args[min_idx]) {
        Some(b) => b,
        None => return err_wrong_args(cmd),
    };
    let max_arg = match extract_bytes(&args[max_idx]) {
        Some(b) => b,
        None => return err_wrong_args(cmd),
    };

    let mut withscores = false;
    let mut limit_offset: Option<i64> = None;
    let mut limit_count: Option<i64> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = match extract_bytes(&args[i]) {
            Some(b) => b.as_ref(),
            None => return err("ERR syntax error"),
        };
        if opt.eq_ignore_ascii_case(b"LIMIT") {
            // A `LIMIT` with fewer than two values is `syntax error`, not an
            // arity error: the arity floor was already met above.
            if i + 2 >= args.len() {
                return err("ERR syntax error");
            }
            let (Some(off_b), Some(cnt_b)) =
                (extract_bytes(&args[i + 1]), extract_bytes(&args[i + 2]))
            else {
                return err("ERR syntax error");
            };
            limit_offset = std::str::from_utf8(off_b).ok().and_then(|s| s.parse().ok());
            limit_count = std::str::from_utf8(cnt_b).ok().and_then(|s| s.parse().ok());
            if limit_offset.is_none() || limit_count.is_none() {
                return err("ERR value is not an integer or out of range");
            }
            i += 3;
        } else if opt.eq_ignore_ascii_case(b"WITHSCORES") {
            // Parsed here, refused below, after the whole option loop: a
            // later dangling `LIMIT` still wins.
            withscores = true;
            i += 1;
        } else {
            return err("ERR syntax error");
        }
    }

    if withscores {
        return err("ERR syntax error, WITHSCORES not supported in combination with BYLEX");
    }
    // Validate the grammar before the key is consulted. The helpers below
    // parse the bounds again; that second pass is two small copies on a path
    // that is about to materialise the reply, and it keeps the ONE grammar
    // `ZRANGE ... BYLEX` uses rather than a second parser to drift from it.
    if let Err(e) = parse_lex_bound(min_arg) {
        return e;
    }
    if let Err(e) = parse_lex_bound(max_arg) {
        return e;
    }

    match db.get_sorted_set_ref_if_alive(key, now_ms) {
        Ok(Some(zref)) => match (zref.members_map(), zref.bptree()) {
            (Some(members), Some(scores)) => zrange_by_lex(
                scores,
                min_arg,
                max_arg,
                rev,
                false,
                members,
                limit_offset,
                limit_count,
            ),
            _ => {
                let entries = zref.entries_sorted();
                zrange_from_entries(
                    &entries,
                    min_arg,
                    max_arg,
                    false,
                    true,
                    rev,
                    false,
                    limit_offset,
                    limit_count,
                )
            }
        },
        Ok(None) => Frame::Array(framevec![]),
        Err(e) => e,
    }
}
