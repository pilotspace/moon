//! WS.* command validation and helpers.
//!
//! Commands:
//! - WS CREATE <name>: Create a new workspace
//! - WS DROP <ws_id>: Delete a workspace and all its data
//! - WS AUTH <ws_id>: Bind connection to a workspace
//! - WS INFO <ws_id>: Return workspace statistics
//! - WS LIST: List all workspaces
//!
//! Handler integration: WS.* commands are intercepted BEFORE dispatch
//! (same pattern as TXN.* and TEMPORAL.*) in handler_monoio.rs.

use bytes::Bytes;

use crate::protocol::Frame;

/// Error: wrong number of arguments for 'WS' command.
pub const ERR_WS_ARGS: &[u8] = b"ERR wrong number of arguments for 'WS' command";

/// Error: wrong number of arguments for 'WS CREATE' command.
pub const ERR_WS_CREATE_ARGS: &[u8] = b"ERR wrong number of arguments for 'WS CREATE' command";

/// Error: wrong number of arguments for 'WS DROP' command.
pub const ERR_WS_DROP_ARGS: &[u8] = b"ERR wrong number of arguments for 'WS DROP' command";

/// Error: wrong number of arguments for 'WS AUTH' command.
pub const ERR_WS_AUTH_ARGS: &[u8] = b"ERR wrong number of arguments for 'WS AUTH' command";

/// Error: wrong number of arguments for 'WS INFO' command.
pub const ERR_WS_INFO_ARGS: &[u8] = b"ERR wrong number of arguments for 'WS INFO' command";

/// Error: wrong number of arguments for 'WS LIST' command.
pub const ERR_WS_LIST_ARGS: &[u8] = b"ERR wrong number of arguments for 'WS LIST' command";

/// Error: workspace not found.
pub const ERR_WS_NOT_FOUND: &[u8] = b"ERR workspace not found";

/// Error: connection already bound to a workspace.
pub const ERR_WS_ALREADY_BOUND: &[u8] = b"ERR connection already bound to a workspace";

/// Error: invalid workspace ID format.
pub const ERR_WS_INVALID_ID: &[u8] = b"ERR invalid workspace ID format";

/// Error: workspace name exceeds 64 bytes.
pub const ERR_WS_NAME_TOO_LONG: &[u8] = b"ERR workspace name exceeds 64 bytes";

/// Error: unknown WS subcommand.
pub const ERR_WS_UNKNOWN_SUB: &[u8] = b"ERR unknown WS subcommand";

/// Maximum workspace name length in bytes.
const MAX_WS_NAME_LEN: usize = 64;

/// Parse the WS subcommand from args.
///
/// Returns the subcommand bytes (e.g., `b"CREATE"`) or an error Frame.
pub fn parse_ws_subcommand(args: &[Frame]) -> Result<&[u8], Frame> {
    if args.is_empty() {
        return Err(Frame::Error(Bytes::from_static(ERR_WS_ARGS)));
    }
    match &args[0] {
        Frame::BulkString(data) => Ok(data),
        _ => Err(Frame::Error(Bytes::from_static(ERR_WS_ARGS))),
    }
}

/// Validate WS CREATE arguments.
///
/// Expected: `WS CREATE <name>` (args = [CREATE, name]).
/// Returns the workspace name on success.
pub fn validate_ws_create(args: &[Frame]) -> Result<Bytes, Frame> {
    if args.len() != 2 {
        return Err(Frame::Error(Bytes::from_static(ERR_WS_CREATE_ARGS)));
    }
    match &args[1] {
        Frame::BulkString(name) => {
            if name.is_empty() {
                return Err(Frame::Error(Bytes::from_static(ERR_WS_CREATE_ARGS)));
            }
            if name.len() > MAX_WS_NAME_LEN {
                return Err(Frame::Error(Bytes::from_static(ERR_WS_NAME_TOO_LONG)));
            }
            Ok(name.clone())
        }
        _ => Err(Frame::Error(Bytes::from_static(ERR_WS_CREATE_ARGS))),
    }
}

/// Validate WS DROP arguments.
///
/// Expected: `WS DROP <ws_id>` (args = [DROP, ws_id]).
/// Returns the raw workspace ID bytes on success.
pub fn validate_ws_drop(args: &[Frame]) -> Result<Bytes, Frame> {
    if args.len() != 2 {
        return Err(Frame::Error(Bytes::from_static(ERR_WS_DROP_ARGS)));
    }
    match &args[1] {
        Frame::BulkString(ws_id) => Ok(ws_id.clone()),
        _ => Err(Frame::Error(Bytes::from_static(ERR_WS_DROP_ARGS))),
    }
}

/// Validate WS AUTH arguments.
///
/// Expected: `WS AUTH <ws_id>` (args = [AUTH, ws_id]).
/// Returns the raw workspace ID bytes on success.
pub fn validate_ws_auth(args: &[Frame]) -> Result<Bytes, Frame> {
    if args.len() != 2 {
        return Err(Frame::Error(Bytes::from_static(ERR_WS_AUTH_ARGS)));
    }
    match &args[1] {
        Frame::BulkString(ws_id) => Ok(ws_id.clone()),
        _ => Err(Frame::Error(Bytes::from_static(ERR_WS_AUTH_ARGS))),
    }
}

/// Validate WS INFO arguments.
///
/// Expected: `WS INFO <ws_id>` (args = [INFO, ws_id]).
/// Returns the raw workspace ID bytes on success.
pub fn validate_ws_info(args: &[Frame]) -> Result<Bytes, Frame> {
    if args.len() != 2 {
        return Err(Frame::Error(Bytes::from_static(ERR_WS_INFO_ARGS)));
    }
    match &args[1] {
        Frame::BulkString(ws_id) => Ok(ws_id.clone()),
        _ => Err(Frame::Error(Bytes::from_static(ERR_WS_INFO_ARGS))),
    }
}

/// Validate WS LIST arguments.
///
/// Expected: `WS LIST` (args = [LIST]).
/// Returns `Ok(())` on success.
pub fn validate_ws_list(args: &[Frame]) -> Result<(), Frame> {
    if args.len() != 1 {
        return Err(Frame::Error(Bytes::from_static(ERR_WS_LIST_ARGS)));
    }
    Ok(())
}

/// Parse a workspace ID from raw bytes (UUID string format).
///
/// Accepts both standard 36-char UUID format (with dashes) and 32-char hex
/// (no dashes). Returns `None` on any parse failure.
pub fn parse_workspace_id_from_bytes(raw: &[u8]) -> Option<crate::workspace::WorkspaceId> {
    let s = std::str::from_utf8(raw).ok()?;
    let parsed = uuid::Uuid::try_parse(s).ok()?;
    Some(crate::workspace::WorkspaceId::from_bytes(
        parsed.into_bytes(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_ws_subcommand ---

    #[test]
    fn test_parse_ws_subcommand_empty_args() {
        assert!(parse_ws_subcommand(&[]).is_err());
    }

    #[test]
    fn test_parse_ws_subcommand_valid() {
        let args = [Frame::BulkString(Bytes::from_static(b"CREATE"))];
        let sub = parse_ws_subcommand(&args).unwrap();
        assert_eq!(sub, b"CREATE");
    }

    #[test]
    fn test_parse_ws_subcommand_wrong_type() {
        let args = [Frame::Integer(42)];
        assert!(parse_ws_subcommand(&args).is_err());
    }

    // --- validate_ws_create ---

    #[test]
    fn test_validate_ws_create_valid() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"CREATE")),
            Frame::BulkString(Bytes::from_static(b"my-workspace")),
        ];
        let name = validate_ws_create(&args).unwrap();
        assert_eq!(name.as_ref(), b"my-workspace");
    }

    #[test]
    fn test_validate_ws_create_missing_name() {
        let args = [Frame::BulkString(Bytes::from_static(b"CREATE"))];
        assert!(validate_ws_create(&args).is_err());
    }

    #[test]
    fn test_validate_ws_create_empty_name() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"CREATE")),
            Frame::BulkString(Bytes::new()),
        ];
        assert!(validate_ws_create(&args).is_err());
    }

    #[test]
    fn test_validate_ws_create_name_too_long() {
        let long_name = Bytes::from(vec![b'a'; 65]);
        let args = [
            Frame::BulkString(Bytes::from_static(b"CREATE")),
            Frame::BulkString(long_name),
        ];
        let err = validate_ws_create(&args).unwrap_err();
        match err {
            Frame::Error(msg) => assert_eq!(msg.as_ref(), ERR_WS_NAME_TOO_LONG),
            _ => panic!("expected Error frame"),
        }
    }

    #[test]
    fn test_validate_ws_create_max_name_ok() {
        let name = Bytes::from(vec![b'x'; 64]);
        let args = [
            Frame::BulkString(Bytes::from_static(b"CREATE")),
            Frame::BulkString(name.clone()),
        ];
        let result = validate_ws_create(&args).unwrap();
        assert_eq!(result.len(), 64);
    }

    #[test]
    fn test_validate_ws_create_too_many_args() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"CREATE")),
            Frame::BulkString(Bytes::from_static(b"name")),
            Frame::BulkString(Bytes::from_static(b"extra")),
        ];
        assert!(validate_ws_create(&args).is_err());
    }

    #[test]
    fn test_validate_ws_create_wrong_type() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"CREATE")),
            Frame::Integer(42),
        ];
        assert!(validate_ws_create(&args).is_err());
    }

    // --- validate_ws_drop ---

    #[test]
    fn test_validate_ws_drop_valid() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"DROP")),
            Frame::BulkString(Bytes::from_static(
                b"0193a9f2-e456-7890-abcd-ef1234567890",
            )),
        ];
        let ws_id = validate_ws_drop(&args).unwrap();
        assert_eq!(
            ws_id.as_ref(),
            b"0193a9f2-e456-7890-abcd-ef1234567890"
        );
    }

    #[test]
    fn test_validate_ws_drop_missing_id() {
        let args = [Frame::BulkString(Bytes::from_static(b"DROP"))];
        assert!(validate_ws_drop(&args).is_err());
    }

    #[test]
    fn test_validate_ws_drop_wrong_type() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"DROP")),
            Frame::Integer(42),
        ];
        assert!(validate_ws_drop(&args).is_err());
    }

    // --- validate_ws_auth ---

    #[test]
    fn test_validate_ws_auth_valid() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"AUTH")),
            Frame::BulkString(Bytes::from_static(
                b"0193a9f2-e456-7890-abcd-ef1234567890",
            )),
        ];
        let ws_id = validate_ws_auth(&args).unwrap();
        assert!(!ws_id.is_empty());
    }

    #[test]
    fn test_validate_ws_auth_missing_id() {
        let args = [Frame::BulkString(Bytes::from_static(b"AUTH"))];
        assert!(validate_ws_auth(&args).is_err());
    }

    // --- validate_ws_info ---

    #[test]
    fn test_validate_ws_info_valid() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"INFO")),
            Frame::BulkString(Bytes::from_static(
                b"0193a9f2-e456-7890-abcd-ef1234567890",
            )),
        ];
        let ws_id = validate_ws_info(&args).unwrap();
        assert!(!ws_id.is_empty());
    }

    #[test]
    fn test_validate_ws_info_missing_id() {
        let args = [Frame::BulkString(Bytes::from_static(b"INFO"))];
        assert!(validate_ws_info(&args).is_err());
    }

    // --- validate_ws_list ---

    #[test]
    fn test_validate_ws_list_valid() {
        let args = [Frame::BulkString(Bytes::from_static(b"LIST"))];
        assert!(validate_ws_list(&args).is_ok());
    }

    #[test]
    fn test_validate_ws_list_extra_args() {
        let args = [
            Frame::BulkString(Bytes::from_static(b"LIST")),
            Frame::BulkString(Bytes::from_static(b"extra")),
        ];
        assert!(validate_ws_list(&args).is_err());
    }

    #[test]
    fn test_validate_ws_list_no_args() {
        let args: [Frame; 0] = [];
        assert!(validate_ws_list(&args).is_err());
    }

    // --- parse_workspace_id_from_bytes ---

    #[test]
    fn test_parse_workspace_id_standard_uuid() {
        let raw = b"0193a9f2-e456-7890-abcd-ef1234567890";
        let id = parse_workspace_id_from_bytes(raw).unwrap();
        let expected = uuid::Uuid::try_parse("0193a9f2-e456-7890-abcd-ef1234567890").unwrap();
        assert_eq!(*id.as_bytes(), expected.into_bytes());
    }

    #[test]
    fn test_parse_workspace_id_hex_no_dashes() {
        let raw = b"0193a9f2e4567890abcdef1234567890";
        let id = parse_workspace_id_from_bytes(raw).unwrap();
        let expected = uuid::Uuid::try_parse("0193a9f2e4567890abcdef1234567890").unwrap();
        assert_eq!(*id.as_bytes(), expected.into_bytes());
    }

    #[test]
    fn test_parse_workspace_id_garbage() {
        assert!(parse_workspace_id_from_bytes(b"not-a-uuid").is_none());
        assert!(parse_workspace_id_from_bytes(b"").is_none());
        assert!(parse_workspace_id_from_bytes(b"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz").is_none());
    }

    #[test]
    fn test_parse_workspace_id_invalid_utf8() {
        let raw = &[0xFF, 0xFE, 0xFD];
        assert!(parse_workspace_id_from_bytes(raw).is_none());
    }
}
