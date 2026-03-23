pub mod connection;
pub mod hash;
pub mod key;
pub mod list;
pub mod set;
pub mod string;

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
        b"UNLINK" => DispatchResult::Response(key::unlink(db, cmd_args)),
        b"SCAN" => DispatchResult::Response(key::scan(db, cmd_args)),
        b"KEYS" => DispatchResult::Response(key::keys(db, cmd_args)),
        b"RENAME" => DispatchResult::Response(key::rename(db, cmd_args)),
        b"RENAMENX" => DispatchResult::Response(key::renamenx(db, cmd_args)),
        // String commands
        b"GET" => DispatchResult::Response(string::get(db, cmd_args)),
        b"SET" => DispatchResult::Response(string::set(db, cmd_args)),
        b"MGET" => DispatchResult::Response(string::mget(db, cmd_args)),
        b"MSET" => DispatchResult::Response(string::mset(db, cmd_args)),
        b"INCR" => DispatchResult::Response(string::incr(db, cmd_args)),
        b"DECR" => DispatchResult::Response(string::decr(db, cmd_args)),
        b"INCRBY" => DispatchResult::Response(string::incrby(db, cmd_args)),
        b"DECRBY" => DispatchResult::Response(string::decrby(db, cmd_args)),
        b"INCRBYFLOAT" => DispatchResult::Response(string::incrbyfloat(db, cmd_args)),
        b"APPEND" => DispatchResult::Response(string::append(db, cmd_args)),
        b"STRLEN" => DispatchResult::Response(string::strlen(db, cmd_args)),
        b"SETNX" => DispatchResult::Response(string::setnx(db, cmd_args)),
        b"SETEX" => DispatchResult::Response(string::setex(db, cmd_args)),
        b"PSETEX" => DispatchResult::Response(string::psetex(db, cmd_args)),
        b"GETSET" => DispatchResult::Response(string::getset(db, cmd_args)),
        b"GETDEL" => DispatchResult::Response(string::getdel(db, cmd_args)),
        b"GETEX" => DispatchResult::Response(string::getex(db, cmd_args)),
        // Hash commands
        b"HSET" => DispatchResult::Response(hash::hset(db, cmd_args)),
        b"HGET" => DispatchResult::Response(hash::hget(db, cmd_args)),
        b"HDEL" => DispatchResult::Response(hash::hdel(db, cmd_args)),
        b"HMSET" => DispatchResult::Response(hash::hmset(db, cmd_args)),
        b"HMGET" => DispatchResult::Response(hash::hmget(db, cmd_args)),
        b"HGETALL" => DispatchResult::Response(hash::hgetall(db, cmd_args)),
        b"HEXISTS" => DispatchResult::Response(hash::hexists(db, cmd_args)),
        b"HLEN" => DispatchResult::Response(hash::hlen(db, cmd_args)),
        b"HKEYS" => DispatchResult::Response(hash::hkeys(db, cmd_args)),
        b"HVALS" => DispatchResult::Response(hash::hvals(db, cmd_args)),
        b"HINCRBY" => DispatchResult::Response(hash::hincrby(db, cmd_args)),
        b"HINCRBYFLOAT" => DispatchResult::Response(hash::hincrbyfloat(db, cmd_args)),
        b"HSETNX" => DispatchResult::Response(hash::hsetnx(db, cmd_args)),
        b"HSCAN" => DispatchResult::Response(hash::hscan(db, cmd_args)),
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
    fn test_dispatch_get_set() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // SET foo bar
        let result = dispatch(
            &mut db,
            make_command(&[b"SET", b"foo", b"bar"]),
            &mut selected,
            16,
        );
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"OK")));
            }
            _ => panic!("Expected Response"),
        }
        // GET foo
        let result = dispatch(
            &mut db,
            make_command(&[b"GET", b"foo"]),
            &mut selected,
            16,
        );
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from_static(b"bar")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_dispatch_hset_hget() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // HSET myhash field1 value1
        let result = dispatch(
            &mut db,
            make_command(&[b"HSET", b"myhash", b"field1", b"value1"]),
            &mut selected,
            16,
        );
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::Integer(1));
            }
            _ => panic!("Expected Response"),
        }
        // HGET myhash field1
        let result = dispatch(
            &mut db,
            make_command(&[b"HGET", b"myhash", b"field1"]),
            &mut selected,
            16,
        );
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from_static(b"value1")));
            }
            _ => panic!("Expected Response"),
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
