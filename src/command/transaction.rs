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
pub const ERR_TXN_CROSS_SHARD: &[u8] =
    b"ERR TXN does not support cross-shard writes \
      -- use hash tags {tag} to co-locate keys (e.g. SET {txn}:key value)";

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
        _ => Err(Frame::Error(Bytes::from_static(b"ERR invalid argument type"))),
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
        assert!(!is_txn_commit(b"TXN", &[Frame::BulkString(Bytes::from_static(b"BEGIN"))]));
    }

    #[test]
    fn test_is_txn_abort() {
        let args = vec![Frame::BulkString(Bytes::from_static(b"ABORT"))];
        assert!(is_txn_abort(b"TXN", &args));
    }

    #[test]
    fn test_err_txn_cross_shard_is_defined() {
        assert!(!ERR_TXN_CROSS_SHARD.is_empty());
        assert!(ERR_TXN_CROSS_SHARD.starts_with(b"ERR TXN"));
    }
}
