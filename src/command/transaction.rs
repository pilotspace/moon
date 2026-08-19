//! TXN.* commands for cross-store ACID transactions.
//!
//! Commands:
//! - TXN.BEGIN: Start a new cross-store transaction
//! - TXN.COMMIT: Commit all changes atomically
//! - TXN.ABORT: Roll back all changes
//!
//! Note: These commands are connection-scoped and require handler integration.
//! The handlers intercept TXN.* commands BEFORE dispatch (like MULTI/EXEC)
//! to manage CrossStoreTxn lifecycle on ConnectionState.

use bytes::Bytes;

use crate::protocol::Frame;

/// Error: already in a transaction.
pub const ERR_ALREADY_IN_TXN: &[u8] = b"ERR already in a cross-store transaction";

/// Error: not in a transaction.
pub const ERR_NOT_IN_TXN: &[u8] = b"ERR not in a cross-store transaction";

/// Error: cannot mix TXN with MULTI.
pub const ERR_TXN_MULTI_CONFLICT: &[u8] = b"ERR cannot use TXN while in MULTI block";

/// Error: cannot mix MULTI with TXN.
pub const ERR_MULTI_TXN_CONFLICT: &[u8] = b"ERR cannot use MULTI while in TXN block";

/// Error: cross-shard write attempted inside TXN.
///
/// Moon TXN uses a per-connection undo log that is shard-local. Writes that
/// route to a different shard via SPSC are not captured in the undo log and
/// therefore cannot be rolled back on TXN.ABORT.
///
/// Fix: use Redis cluster hash tags (e.g. {tag}:key) to force all transaction
/// keys to route to the same shard as the TXN connection.
pub const ERR_TXN_CROSS_SHARD: &[u8] = b"ERR TXN does not support cross-shard writes \
      -- use hash tags {tag} to co-locate keys (e.g. SET {txn}:key value)";

/// #499: the error `TXN.COMMIT` answers when the body contained rejected ops.
///
/// Every guard rejection inside a TXN body (cross-shard write, `MOVE`,
/// `COPY ... DB`, `SWAPDB`, cross-shard Cypher write) poisons the
/// transaction. Committing the accepted subset would turn a routing mistake
/// into silent partial application — the caller inspects the COMMIT reply,
/// not the replies of the individual body commands, exactly as a driver
/// inspects `EXEC` and not the `QUEUED`s.
///
/// Semantics match Redis's `CLIENT_DIRTY_EXEC`: the whole transaction is
/// rolled back and discarded, and the reply carries the `EXECABORT` code so
/// drivers classify it as a transaction abort rather than a generic command
/// error.
///
/// Wording note: the message says "rolled back and NOT committed", not
/// "nothing was applied". Rollback runs the `TXN.ABORT` path, which is
/// best-effort by construction — `MSET` and multi-key `DEL` bypass undo
/// capture today (#500), so an absolute claim would be a promise this code
/// cannot keep. What IS guaranteed: the commit did not happen, and the
/// transaction is discarded.
pub fn err_txn_commit_dirty(rejected: u32, first_cmd: Option<&[u8]>) -> Frame {
    let mut msg = bytes::BytesMut::new();
    use std::fmt::Write as _;
    let _ = write!(
        msg,
        "EXECABORT TXN.COMMIT discarded because of previous errors: \
         {rejected} operation(s) rejected inside the transaction"
    );
    if let Some(cmd) = first_cmd {
        let _ = write!(msg, " (first: {})", String::from_utf8_lossy(cmd));
    }
    let _ = write!(msg, " -- rolled back and NOT committed");
    Frame::Error(msg.freeze())
}

/// TXN.BEGIN - Start a new cross-store transaction.
///
/// Returns: +OK on success, or error if already in transaction.
///
/// This function validates preconditions only. The actual transaction
/// creation is done at the handler level with access to TransactionManager.
pub fn txn_begin_validate(in_multi: bool, in_cross_txn: bool) -> Result<(), Frame> {
    if in_multi {
        return Err(Frame::Error(Bytes::from_static(ERR_TXN_MULTI_CONFLICT)));
    }
    if in_cross_txn {
        return Err(Frame::Error(Bytes::from_static(ERR_ALREADY_IN_TXN)));
    }
    Ok(())
}

/// TXN.COMMIT - Commit the active transaction.
///
/// Returns: +OK on success, or error if not in transaction.
///
/// This function validates preconditions only. The actual commit
/// (WAL record, bitmap update) is done at the handler level.
pub fn txn_commit_validate(in_cross_txn: bool) -> Result<(), Frame> {
    if !in_cross_txn {
        return Err(Frame::Error(Bytes::from_static(ERR_NOT_IN_TXN)));
    }
    Ok(())
}

/// TXN.ABORT - Abort the active transaction.
///
/// Returns: +OK on success, or error if not in transaction.
///
/// This function validates preconditions only. The actual abort
/// (undo log replay, intent release) is done at the handler level.
pub fn txn_abort_validate(in_cross_txn: bool) -> Result<(), Frame> {
    if !in_cross_txn {
        return Err(Frame::Error(Bytes::from_static(ERR_NOT_IN_TXN)));
    }
    Ok(())
}

/// Parse TXN subcommand from args.
///
/// Returns the subcommand name (uppercase) or error if invalid.
pub fn parse_txn_subcommand(args: &[Frame]) -> Result<&[u8], Frame> {
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'TXN' command",
        )));
    }
    match &args[0] {
        Frame::BulkString(data) => Ok(data),
        _ => Err(Frame::Error(Bytes::from_static(
            b"ERR invalid argument type",
        ))),
    }
}

/// Check if a command is TXN.BEGIN.
#[inline]
pub fn is_txn_begin(cmd: &[u8], args: &[Frame]) -> bool {
    if !cmd.eq_ignore_ascii_case(b"TXN") {
        return false;
    }
    matches!(args.first(), Some(Frame::BulkString(sub)) if sub.eq_ignore_ascii_case(b"BEGIN"))
}

/// Check if a command is TXN.COMMIT.
#[inline]
pub fn is_txn_commit(cmd: &[u8], args: &[Frame]) -> bool {
    if !cmd.eq_ignore_ascii_case(b"TXN") {
        return false;
    }
    matches!(args.first(), Some(Frame::BulkString(sub)) if sub.eq_ignore_ascii_case(b"COMMIT"))
}

/// Check if a command is TXN.ABORT.
#[inline]
pub fn is_txn_abort(cmd: &[u8], args: &[Frame]) -> bool {
    if !cmd.eq_ignore_ascii_case(b"TXN") {
        return false;
    }
    matches!(args.first(), Some(Frame::BulkString(sub)) if sub.eq_ignore_ascii_case(b"ABORT"))
}

/// The error a `TXN` invocation earns when no subcommand intercept claimed it.
///
/// `TXN` is served entirely by the three predicates above, which run before
/// dispatch. Anything reaching dispatch is therefore a bare `TXN` or an
/// unrecognised subcommand — previously answered `unknown command 'TXN'`, which
/// is false (the command exists) and misleads a driver into concluding Moon has
/// no cross-store transactions at all.
///
/// Shapes follow Redis's container commands: a missing subcommand is an arity
/// error, an unrecognised one names the offending token.
pub fn err_txn_subcommand(args: &[Frame]) -> Frame {
    let Some(sub) = args.first() else {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'txn' command",
        ));
    };
    let name = match sub {
        Frame::BulkString(s) | Frame::SimpleString(s) => String::from_utf8_lossy(s).into_owned(),
        _ => String::new(),
    };
    Frame::Error(Bytes::from(format!(
        "ERR Unknown TXN subcommand or wrong number of arguments for '{name}'"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txn_begin_validate_success() {
        assert!(txn_begin_validate(false, false).is_ok());
    }

    #[test]
    fn test_txn_begin_validate_in_multi_fails() {
        let result = txn_begin_validate(true, false);
        assert!(result.is_err());
        if let Err(Frame::Error(e)) = result {
            assert!(e.as_ref().starts_with(b"ERR cannot use TXN"));
        }
    }

    #[test]
    fn test_txn_begin_validate_in_txn_fails() {
        let result = txn_begin_validate(false, true);
        assert!(result.is_err());
        if let Err(Frame::Error(e)) = result {
            assert!(e.as_ref().starts_with(b"ERR already"));
        }
    }

    #[test]
    fn test_txn_commit_validate_success() {
        assert!(txn_commit_validate(true).is_ok());
    }

    #[test]
    fn test_txn_commit_validate_not_in_txn_fails() {
        let result = txn_commit_validate(false);
        assert!(result.is_err());
    }

    #[test]
    fn test_txn_abort_validate_success() {
        assert!(txn_abort_validate(true).is_ok());
    }

    #[test]
    fn test_txn_abort_validate_not_in_txn_fails() {
        let result = txn_abort_validate(false);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_subcommand_empty_args() {
        let result = parse_txn_subcommand(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_txn_begin() {
        let args = vec![Frame::BulkString(Bytes::from_static(b"BEGIN"))];
        assert!(is_txn_begin(b"TXN", &args));
        assert!(is_txn_begin(b"txn", &args));
        assert!(!is_txn_begin(b"SET", &args));
    }

    #[test]
    fn test_is_txn_commit() {
        let args = vec![Frame::BulkString(Bytes::from_static(b"COMMIT"))];
        assert!(is_txn_commit(b"TXN", &args));
        assert!(!is_txn_commit(
            b"TXN",
            &[Frame::BulkString(Bytes::from_static(b"BEGIN"))]
        ));
    }

    #[test]
    fn test_is_txn_abort() {
        let args = vec![Frame::BulkString(Bytes::from_static(b"ABORT"))];
        assert!(is_txn_abort(b"TXN", &args));
    }

    /// #499: the commit-time abort error must carry the `EXECABORT` code, the
    /// rejected-op count, the first offending command, and say plainly that
    /// nothing was applied.
    #[test]
    fn test_err_txn_commit_dirty_shape() {
        let Frame::Error(msg) = err_txn_commit_dirty(3, Some(b"SET")) else {
            panic!("err_txn_commit_dirty must return Frame::Error");
        };
        let msg = String::from_utf8_lossy(&msg).into_owned();
        assert!(msg.starts_with("EXECABORT "), "{msg}");
        assert!(msg.contains("3 operation(s) rejected"), "{msg}");
        assert!(msg.contains("(first: SET)"), "{msg}");
        assert!(msg.contains("rolled back and NOT committed"), "{msg}");
        assert!(!msg.contains('\r') && !msg.contains('\n'), "{msg}");
    }

    /// A missing command name must not produce a dangling `(first: )`.
    #[test]
    fn test_err_txn_commit_dirty_without_cmd_name() {
        let Frame::Error(msg) = err_txn_commit_dirty(1, None) else {
            panic!("err_txn_commit_dirty must return Frame::Error");
        };
        let msg = String::from_utf8_lossy(&msg).into_owned();
        assert!(msg.contains("1 operation(s) rejected"), "{msg}");
        assert!(!msg.contains("first:"), "{msg}");
    }

    #[test]
    fn test_err_txn_cross_shard_is_defined() {
        assert!(!ERR_TXN_CROSS_SHARD.is_empty());
        assert!(ERR_TXN_CROSS_SHARD.starts_with(b"ERR TXN"));
    }
}
