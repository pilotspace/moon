//! Blocking list/zset pops queued inside `MULTI` (moon#524).
//!
//! A blocking command inside a transaction must not block: Redis runs it with
//! `CLIENT_DENY_BLOCKING`, which makes the command take its "nothing available"
//! exit immediately. Crucially it is still the ORIGINAL command that runs, so
//! the reply keeps the original SHAPE.
//!
//! Moon used to rewrite the queued frame into a non-blocking sibling instead —
//! `BLPOP k t` -> `LPOP k` — and four of those siblings answer a DIFFERENT
//! shape:
//!
//! ```text
//! redis 8.6.1  RPUSH q v1 / MULTI / BLPOP q 0 / EXEC -> [["q", "v1"]]
//! moon (old)                                        -> ["v1"]
//! ```
//!
//! The key is not decoration: `BLPOP` takes MANY keys and the caller cannot
//! otherwise tell which one served it. The rewrite was also wrong on the
//! multi-key path in a way the shape hid — `BLPOP q1 q2 0` became `LPOP q1 q2`,
//! where `q2` is LPOP's optional COUNT, so Moon popped up to two elements from
//! `q1` and never looked at `q2`.
//!
//! So those four commands are now queued UNREWRITTEN and executed here, at
//! EXEC, in immediate-only mode. The reply comes from
//! [`try_immediate_pop`](super::blocking::try_immediate_pop) — the same helper
//! the live (non-transactional) fast path uses — so the in-MULTI reply is
//! shape-identical to the live one by construction rather than by a second
//! implementation that has to be kept in sync.

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;

use super::blocking::{parse_blocking_timeout, try_immediate_pop};
use super::util::extract_bytes;

/// One blocking command executed in immediate-only mode.
pub(crate) struct BlockingTxnOutcome {
    /// The reply for this slot of the EXEC array.
    pub reply: Frame,
    /// The non-blocking command that REPRODUCES the mutation this call made,
    /// for the AOF and the replication stream. `None` when nothing was popped
    /// (a miss, a wrong type, or a bad argument mutates nothing).
    ///
    /// Synthesised rather than reusing the queued frame, because the queued
    /// frame is the blocking command itself: a replica applying a literal
    /// `BLPOP` would block its apply loop, and an AOF replaying one would
    /// stall recovery. It is also scoped to the key that actually served,
    /// which the multi-key form of the command is not.
    pub effect: Option<Frame>,
}

impl BlockingTxnOutcome {
    fn reply_only(reply: Frame) -> Self {
        Self {
            reply,
            effect: None,
        }
    }
}

/// Does this blocking command have to be queued UNREWRITTEN inside `MULTI`?
///
/// True for exactly the commands whose non-blocking sibling answers a
/// different shape. `BLMPOP`/`BZMPOP`/`BRPOPLPUSH`/`BLMOVE` are not here: their
/// siblings (`LMPOP`/`ZMPOP`/`LMOVE`) are shape-correct AND arity-correct, so
/// they keep the rewrite and keep the audited dispatch-table implementation.
///
/// Must stay in lockstep with [`try_exec_blocking_in_txn`] — a command added
/// here without a branch there would fall through to the KV dispatch table at
/// EXEC and answer "unknown command". The unit test
/// `every_unrewritten_command_has_an_executor` is that lockstep.
#[inline]
pub(crate) fn queues_unrewritten(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"BLPOP")
        || cmd.eq_ignore_ascii_case(b"BRPOP")
        || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
        || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
}

/// The non-blocking command whose EFFECT (not reply) is identical, used to
/// build the AOF/replication record for a pop that actually happened.
#[inline]
fn effect_sibling(cmd: &[u8]) -> Option<(&'static [u8], bool)> {
    if cmd.eq_ignore_ascii_case(b"BLPOP") {
        Some((b"LPOP", false))
    } else if cmd.eq_ignore_ascii_case(b"BRPOP") {
        Some((b"RPOP", false))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMIN") {
        Some((b"ZPOPMIN", true))
    } else if cmd.eq_ignore_ascii_case(b"BZPOPMAX") {
        Some((b"ZPOPMAX", true))
    } else {
        None
    }
}

/// Execute a queued blocking pop in immediate-only mode.
///
/// Returns `None` when `cmd` is not one of the unrewritten blocking commands,
/// so the caller falls through to normal dispatch unchanged.
///
/// Semantics, each measured against redis-server 8.6.1 (see
/// `tests/multi_exec_queue_semantics.rs::me7*`):
///
/// * keys are scanned left to right and the FIRST non-empty one serves;
/// * the reply names that key — `[key, value]` / `[key, member, score]`;
/// * nothing available anywhere is a Null **Array** (`*-1` on RESP2, `_` on
///   RESP3 — the serializer picks, this function does not);
/// * a key of the wrong type is a `WRONGTYPE` error, not a null. Losing this
///   would be the quiet regression of the fix: the dispatch table gave the old
///   `LPOP` rewrite its type check for free, and a null reads to a caller as
///   "queue empty".
pub(crate) fn try_exec_blocking_in_txn(
    cmd: &[u8],
    args: &[Frame],
    db: &mut Database,
) -> Option<BlockingTxnOutcome> {
    let (sibling, is_zset) = effect_sibling(cmd)?;

    // `<cmd> key [key ...] timeout` — at least one key and a timeout.
    if args.len() < 2 {
        return Some(BlockingTxnOutcome::reply_only(Frame::Error(
            Bytes::from_static(b"ERR wrong number of arguments for blocking command"),
        )));
    }
    // Redis parses the timeout before touching a key, and rejects a bad one
    // even where it can never wait on it. Value discarded on purpose: inside
    // MULTI every timeout is a zero timeout.
    if let Err(e) = parse_blocking_timeout(cmd, args) {
        return Some(BlockingTxnOutcome::reply_only(e));
    }

    for key_frame in &args[..args.len() - 1] {
        let Some(key) = extract_bytes(key_frame) else {
            continue;
        };
        // Look the key up BEFORE attempting the pop. Two jobs, both load-
        // bearing:
        //
        // 1. Type gate. An existing key of the wrong type is an error even if
        //    a LATER key could have served — matching what the dispatch table
        //    gave the old `LPOP` rewrite for free. A null here would read to
        //    the caller as "queue empty".
        // 2. Absence gate. `try_immediate_pop` reaches the store through
        //    `list_pop_front`/`zset_pop_min`, which are `get_or_CREATE`
        //    helpers: called on an absent key they insert an empty collection
        //    and return `None` without removing it again. Popping only keys
        //    that already exist keeps `MULTI; BLPOP absent 0; EXEC` from
        //    leaving a phantom key behind (`EXISTS absent` -> 1, `TYPE` ->
        //    list). A collection is deleted when its last element goes, so
        //    "exists" and "has something to pop" are the same question here.
        let present = if is_zset {
            match db.get_sorted_set(&key) {
                Err(e) => return Some(BlockingTxnOutcome::reply_only(e)),
                Ok(found) => found.is_some(),
            }
        } else {
            match db.get_list(&key) {
                Err(e) => return Some(BlockingTxnOutcome::reply_only(e)),
                Ok(found) => found.is_some(),
            }
        };
        if !present {
            continue;
        }
        if let Some(reply) = try_immediate_pop(cmd, db, &key, args) {
            return Some(BlockingTxnOutcome {
                reply,
                effect: Some(Frame::Array(framevec![
                    Frame::BulkString(Bytes::from_static(sibling)),
                    Frame::BulkString(key),
                ])),
            });
        }
    }

    Some(BlockingTxnOutcome::reply_only(Frame::NullArray))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::entry::Entry;

    fn args(parts: &[&str]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p.as_bytes())))
            .collect()
    }

    /// The lockstep guard: every command queued unrewritten MUST have an
    /// executor branch, or EXEC answers "unknown command" for it.
    #[test]
    fn every_unrewritten_command_has_an_executor() {
        let mut db = Database::new();
        for cmd in [
            &b"BLPOP"[..],
            &b"BRPOP"[..],
            &b"BZPOPMIN"[..],
            &b"BZPOPMAX"[..],
        ] {
            assert!(
                queues_unrewritten(cmd),
                "{:?} must be queued unrewritten",
                std::str::from_utf8(cmd)
            );
            assert!(
                try_exec_blocking_in_txn(cmd, &args(&["k", "0"]), &mut db).is_some(),
                "{:?} is queued unrewritten but has no EXEC-time executor",
                std::str::from_utf8(cmd)
            );
        }
        // And the rewritten family must NOT be claimed here — they still go
        // through the dispatch table as their non-blocking sibling.
        for cmd in [&b"BLMOVE"[..], &b"BLMPOP"[..], &b"BZMPOP"[..], &b"LPOP"[..]] {
            assert!(!queues_unrewritten(cmd));
            assert!(try_exec_blocking_in_txn(cmd, &args(&["k", "0"]), &mut db).is_none());
        }
    }

    #[test]
    fn blpop_hit_replies_key_and_value_and_logs_lpop() {
        let mut db = Database::new();
        db.list_push_back(b"q", Bytes::from_static(b"v1"));
        let out = try_exec_blocking_in_txn(b"BLPOP", &args(&["q", "0"]), &mut db)
            .expect("BLPOP is executed here");
        assert_eq!(
            out.reply,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"q")),
                Frame::BulkString(Bytes::from_static(b"v1")),
            ]),
            "BLPOP must answer [key, value], not a bare value"
        );
        assert_eq!(
            out.effect,
            Some(Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"LPOP")),
                Frame::BulkString(Bytes::from_static(b"q")),
            ])),
            "the AOF/replication record must be the single-key sibling, never \
             the blocking command itself"
        );
        assert!(db.list_pop_front(b"q").is_none(), "the pop really happened");
    }

    #[test]
    fn multi_key_blpop_scans_in_order_and_names_the_server() {
        let mut db = Database::new();
        db.list_push_back(b"q2", Bytes::from_static(b"v2"));
        let out = try_exec_blocking_in_txn(b"BLPOP", &args(&["q1", "q2", "0"]), &mut db)
            .expect("BLPOP is executed here");
        assert_eq!(
            out.reply,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"q2")),
                Frame::BulkString(Bytes::from_static(b"v2")),
            ])
        );
        // The old rewrite produced `LPOP q1 q2`, i.e. LPOP with COUNT=2 on q1 —
        // this is the assertion that the second key is a KEY again.
        assert_eq!(
            out.effect,
            Some(Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"LPOP")),
                Frame::BulkString(Bytes::from_static(b"q2")),
            ]))
        );
    }

    #[test]
    fn brpop_takes_the_tail() {
        let mut db = Database::new();
        db.list_push_back(b"r", Bytes::from_static(b"a"));
        db.list_push_back(b"r", Bytes::from_static(b"b"));
        let out = try_exec_blocking_in_txn(b"BRPOP", &args(&["r", "0"]), &mut db)
            .expect("BRPOP is executed here");
        assert_eq!(
            out.reply,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"r")),
                Frame::BulkString(Bytes::from_static(b"b")),
            ])
        );
        assert_eq!(
            out.effect,
            Some(Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"RPOP")),
                Frame::BulkString(Bytes::from_static(b"r")),
            ]))
        );
    }

    #[test]
    fn bzpopmin_and_max_reply_key_member_score() {
        let mut db = Database::new();
        db.zset_restore(b"z", Bytes::from_static(b"lo"), 1.0);
        db.zset_restore(b"z", Bytes::from_static(b"hi"), 9.0);

        let out = try_exec_blocking_in_txn(b"BZPOPMIN", &args(&["z", "0"]), &mut db)
            .expect("BZPOPMIN is executed here");
        assert_eq!(
            out.reply,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"z")),
                Frame::BulkString(Bytes::from_static(b"lo")),
                Frame::BulkString(Bytes::from_static(b"1")),
            ]),
            "BZPOPMIN must answer [key, member, score]"
        );
        assert_eq!(
            out.effect,
            Some(Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"ZPOPMIN")),
                Frame::BulkString(Bytes::from_static(b"z")),
            ]))
        );

        let out = try_exec_blocking_in_txn(b"BZPOPMAX", &args(&["z", "0"]), &mut db)
            .expect("BZPOPMAX is executed here");
        assert_eq!(
            out.reply,
            Frame::Array(framevec![
                Frame::BulkString(Bytes::from_static(b"z")),
                Frame::BulkString(Bytes::from_static(b"hi")),
                Frame::BulkString(Bytes::from_static(b"9")),
            ])
        );
    }

    /// The miss is a null ARRAY, not a null bulk — moon#482's distinction,
    /// which the `LPOP` rewrite silently undid inside MULTI (moon#524).
    #[test]
    fn miss_is_a_null_array_and_mutates_nothing() {
        let mut db = Database::new();
        let out = try_exec_blocking_in_txn(b"BLPOP", &args(&["absent-l", "0"]), &mut db)
            .expect("BLPOP is executed here");
        assert_eq!(out.reply, Frame::NullArray);
        assert!(out.effect.is_none(), "a miss must not reach the AOF");

        let out = try_exec_blocking_in_txn(b"BZPOPMIN", &args(&["absent-z", "0"]), &mut db)
            .expect("BZPOPMIN is executed here");
        assert_eq!(out.reply, Frame::NullArray);
        assert!(out.effect.is_none());
    }

    /// A miss must not CREATE the key it failed to find.
    ///
    /// `list_pop_front`/`zset_pop_min` are `get_or_create` helpers — calling
    /// them on an absent key inserts an empty collection and returns `None`
    /// without cleaning it up. Reusing the live path's pop helper therefore
    /// came with a phantom-key side effect attached, which this pins closed.
    /// Caught by `miss_is_a_null_array_and_mutates_nothing` failing WRONGTYPE
    /// on its second probe: the first (list) probe had conjured the key.
    #[test]
    fn a_miss_does_not_conjure_the_key() {
        let mut db = Database::new();
        for cmd in [&b"BLPOP"[..], &b"BRPOP"[..]] {
            assert!(try_exec_blocking_in_txn(cmd, &args(&["ghost", "0"]), &mut db).is_some());
            assert!(
                db.get(b"ghost").is_none(),
                "{:?} on an absent key must leave it absent — a phantom empty \
                 list makes EXISTS answer 1 and TYPE answer `list`",
                std::str::from_utf8(cmd)
            );
        }
        for cmd in [&b"BZPOPMIN"[..], &b"BZPOPMAX"[..]] {
            assert!(try_exec_blocking_in_txn(cmd, &args(&["ghostz", "0"]), &mut db).is_some());
            assert!(db.get(b"ghostz").is_none());
        }
        // And a multi-key miss must not conjure ANY of the keys it scanned.
        assert!(
            try_exec_blocking_in_txn(b"BLPOP", &args(&["g1", "g2", "g3", "0"]), &mut db).is_some()
        );
        for k in [&b"g1"[..], &b"g2"[..], &b"g3"[..]] {
            assert!(
                db.get(k).is_none(),
                "{:?} was conjured",
                std::str::from_utf8(k)
            );
        }
    }

    #[test]
    fn wrong_type_is_an_error_not_a_null() {
        let mut db = Database::new();
        db.set(b"str", Entry::new_string(Bytes::from_static(b"v")));
        let out = try_exec_blocking_in_txn(b"BLPOP", &args(&["str", "0"]), &mut db)
            .expect("BLPOP is executed here");
        match out.reply {
            Frame::Error(ref e) => assert!(
                e.starts_with(b"WRONGTYPE"),
                "expected WRONGTYPE, got {:?}",
                String::from_utf8_lossy(e)
            ),
            other => panic!("expected a WRONGTYPE error, got {other:?}"),
        }
        assert!(out.effect.is_none());

        // Same for the zset family, against a list this time.
        db.list_push_back(b"l", Bytes::from_static(b"x"));
        let out = try_exec_blocking_in_txn(b"BZPOPMAX", &args(&["l", "0"]), &mut db)
            .expect("BZPOPMAX is executed here");
        assert!(matches!(out.reply, Frame::Error(_)));
    }

    #[test]
    fn a_bad_timeout_is_rejected_even_though_it_can_never_elapse() {
        let mut db = Database::new();
        db.list_push_back(b"q", Bytes::from_static(b"v1"));
        let out = try_exec_blocking_in_txn(b"BLPOP", &args(&["q", "nope"]), &mut db)
            .expect("BLPOP is executed here");
        assert!(
            matches!(out.reply, Frame::Error(_)),
            "a malformed timeout must still be an error inside MULTI"
        );
        assert!(
            out.effect.is_none(),
            "a rejected command must not have popped anything"
        );
        assert_eq!(
            db.list_pop_front(b"q"),
            Some(Bytes::from_static(b"v1")),
            "the list must be untouched"
        );
    }
}
