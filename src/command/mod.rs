pub mod connection;
pub mod key;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

/// Result of command dispatch.
pub enum DispatchResult {
    /// Normal response to send back to the client.
    Response(Frame),
    /// Response to send, then close the connection (QUIT).
    Quit(Frame),
}

/// Dispatch a command frame to the appropriate handler.
///
/// Extracts the command name from the first element of a Frame::Array,
/// converts it to uppercase for case-insensitive matching, and routes
/// to the appropriate handler function.
pub fn dispatch(
    db: &mut Database,
    frame: Frame,
    selected_db: &mut usize,
    db_count: usize,
) -> DispatchResult {
    let args = match frame {
        Frame::Array(args) if !args.is_empty() => args,
        _ => {
            return DispatchResult::Response(Frame::Error(Bytes::from_static(
                b"ERR invalid command format",
            )))
        }
    };

    let cmd_name = match &args[0] {
        Frame::BulkString(s) => s.to_ascii_uppercase(),
        Frame::SimpleString(s) => s.to_ascii_uppercase(),
        _ => {
            return DispatchResult::Response(Frame::Error(Bytes::from_static(
                b"ERR invalid command name",
            )))
        }
    };

    let cmd_args = &args[1..];

    match cmd_name.as_slice() {
        b"PING" => DispatchResult::Response(connection::ping(cmd_args)),
        b"ECHO" => DispatchResult::Response(connection::echo(cmd_args)),
        b"QUIT" => DispatchResult::Quit(Frame::SimpleString(Bytes::from_static(b"OK"))),
        b"SELECT" => {
            DispatchResult::Response(connection::select(cmd_args, selected_db, db_count))
        }
        b"COMMAND" => DispatchResult::Response(connection::command(cmd_args)),
        b"INFO" => DispatchResult::Response(connection::info(db, cmd_args)),
        // Key management commands
        b"DEL" => DispatchResult::Response(key::del(db, cmd_args)),
        b"EXISTS" => DispatchResult::Response(key::exists(db, cmd_args)),
        b"EXPIRE" => DispatchResult::Response(key::expire(db, cmd_args)),
        b"PEXPIRE" => DispatchResult::Response(key::pexpire(db, cmd_args)),
        b"TTL" => DispatchResult::Response(key::ttl(db, cmd_args)),
        b"PTTL" => DispatchResult::Response(key::pttl(db, cmd_args)),
        b"PERSIST" => DispatchResult::Response(key::persist(db, cmd_args)),
        b"TYPE" => DispatchResult::Response(key::type_cmd(db, cmd_args)),
        _ => DispatchResult::Response(Frame::Error(Bytes::from(format!(
            "ERR unknown command '{}', with args beginning with: ",
            String::from_utf8_lossy(&cmd_name)
        )))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_command(parts: &[&[u8]]) -> Frame {
        Frame::Array(
            parts
                .iter()
                .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
                .collect(),
        )
    }

    #[test]
    fn test_dispatch_ping() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, make_command(&[b"PING"]), &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"PONG")))
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_dispatch_case_insensitive() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, make_command(&[b"ping"]), &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"PONG")))
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_dispatch_quit() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, make_command(&[b"QUIT"]), &mut selected, 16);
        match result {
            DispatchResult::Quit(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"OK")))
            }
            _ => panic!("Expected Quit"),
        }
    }

    #[test]
    fn test_dispatch_unknown_command() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(
            &mut db,
            make_command(&[b"FAKECMD"]),
            &mut selected,
            16,
        );
        match result {
            DispatchResult::Response(Frame::Error(s)) => {
                assert!(s.starts_with(b"ERR unknown command"));
            }
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_dispatch_invalid_format() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(
            &mut db,
            Frame::SimpleString(Bytes::from_static(b"PING")),
            &mut selected,
            16,
        );
        match result {
            DispatchResult::Response(Frame::Error(_)) => {}
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_dispatch_empty_array() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, Frame::Array(vec![]), &mut selected, 16);
        match result {
            DispatchResult::Response(Frame::Error(_)) => {}
            _ => panic!("Expected error response"),
        }
    }
}
