use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::framevec;
use crate::protocol::Frame;
use crate::storage::Database;
/// Global monotonic client ID counter.
static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate a unique client connection ID.
pub fn next_client_id() -> u64 {
    NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed)
}

/// CLIENT ID command: return the connection's unique ID.
pub fn client_id(id: u64) -> Frame {
    Frame::Integer(id as i64)
}

/// PING command handler.
///
/// No args: return PONG as SimpleString.
/// One arg: return the arg as BulkString.
/// More args: return arity error.
pub fn ping(args: &[Frame]) -> Frame {
    match args.len() {
        0 => Frame::SimpleString(Bytes::from_static(b"PONG")),
        1 => match &args[0] {
            Frame::BulkString(s) => Frame::BulkString(s.clone()),
            Frame::SimpleString(s) => Frame::BulkString(s.clone()),
            _ => Frame::BulkString(Bytes::from(format!("{:?}", args[0]))),
        },
        _ => Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'PING' command",
        )),
    }
}

/// ECHO command handler.
///
/// Exactly one arg: return it as BulkString.
/// Wrong arity: return error.
pub fn echo(args: &[Frame]) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'ECHO' command",
        ));
    }
    match &args[0] {
        Frame::BulkString(s) => Frame::BulkString(s.clone()),
        Frame::SimpleString(s) => Frame::BulkString(s.clone()),
        _ => Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'ECHO' command",
        )),
    }
}

/// SELECT command handler.
///
/// Parse arg as integer, validate range 0..db_count, set selected_db.
pub fn select(args: &[Frame], selected_db: &mut usize, db_count: usize) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'SELECT' command",
        ));
    }
    let index_str = match &args[0] {
        Frame::BulkString(s) => s,
        Frame::SimpleString(s) => s,
        _ => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let index_str = match std::str::from_utf8(index_str) {
        Ok(s) => s,
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    let index: usize = match index_str.parse() {
        Ok(n) => n,
        Err(_) => {
            return Frame::Error(Bytes::from_static(
                b"ERR value is not an integer or out of range",
            ));
        }
    };
    if index >= db_count {
        return Frame::Error(Bytes::from_static(b"ERR DB index is out of range"));
    }
    *selected_db = index;
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// COMMAND command handler.
///
/// COMMAND (bare): return integer 0.
/// COMMAND DOCS: return empty array.
/// Any other subcommand: return empty array.
pub fn command(args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Integer(0);
    }
    // Check for DOCS subcommand (case-insensitive, zero-alloc)
    if let Some(Frame::BulkString(sub)) | Some(Frame::SimpleString(sub)) = args.first() {
        if sub.eq_ignore_ascii_case(b"DOCS") || sub.eq_ignore_ascii_case(b"COUNT") {
            return Frame::Array(framevec![]);
        }
    }
    Frame::Array(framevec![])
}

/// INFO command handler.
///
/// Returns a BulkString with minimal INFO sections.
pub fn info(db: &Database, _args: &[Frame]) -> Frame {
    let mut sections = String::new();

    sections.push_str("# Server\r\n");
    sections.push_str("redis_version:0.1.0\r\n");
    sections.push_str("moon:true\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Clients\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Memory\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Persistence\r\n");
    sections.push_str(&format!(
        "loading:0\r\n\
         rdb_bgsave_in_progress:{}\r\n\
         rdb_last_save_time:{}\r\n\
         rdb_last_bgsave_status:{}\r\n\
         aof_enabled:0\r\n\
         aof_rewrite_in_progress:0\r\n",
        if crate::command::persistence::SAVE_IN_PROGRESS.load(std::sync::atomic::Ordering::Relaxed)
        {
            1
        } else {
            0
        },
        crate::command::persistence::LAST_SAVE_TIME.load(std::sync::atomic::Ordering::Relaxed),
        if crate::command::persistence::BGSAVE_LAST_STATUS
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            "ok"
        } else {
            "err"
        },
    ));
    sections.push_str("\r\n");

    sections.push_str("# Vector\r\n");
    sections.push_str(&format!(
        "vector_indexes:{}\r\n\
         vector_total_vectors:{}\r\n\
         vector_memory_bytes:{}\r\n\
         vector_search_total:{}\r\n\
         vector_search_latency_us:{}\r\n\
         vector_compaction_count:{}\r\n\
         vector_compaction_duration_ms:{}\r\n\
         vector_mutable_segment_bytes:{}\r\n",
        crate::vector::metrics::VECTOR_INDEXES.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_TOTAL_VECTORS.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_MEMORY_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_SEARCH_TOTAL.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_SEARCH_LATENCY_US.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_COMPACTION_COUNT.load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_COMPACTION_DURATION_MS
            .load(std::sync::atomic::Ordering::Relaxed),
        crate::vector::metrics::VECTOR_MUTABLE_SEGMENT_BYTES
            .load(std::sync::atomic::Ordering::Relaxed),
    ));
    sections.push_str("\r\n");

    sections.push_str("# MoonStore\r\n");
    use std::fmt::Write as _;
    let _ = write!(
        sections,
        "disk_offload_enabled:{}\r\n",
        crate::vector::metrics::MOONSTORE_DISK_OFFLOAD_ENABLED
            .load(std::sync::atomic::Ordering::Relaxed) as u8
    );
    sections.push_str("\r\n");

    // # Stats
    sections.push_str("# Stats\r\n");
    let _ = write!(
        sections,
        "total_commands_processed:{}\r\n\
         total_connections_received:{}\r\n",
        crate::admin::metrics_setup::total_commands_processed(),
        crate::admin::metrics_setup::total_connections_received(),
    );
    sections.push_str("\r\n");

    // # CPU
    sections.push_str("# CPU\r\n");
    let (cpu_sys, cpu_user) = crate::admin::metrics_setup::get_cpu_usage();
    let _ = write!(
        sections,
        "used_cpu_sys:{:.6}\r\n\
         used_cpu_user:{:.6}\r\n",
        cpu_sys, cpu_user,
    );
    sections.push_str("\r\n");

    // # Replication
    sections.push_str("# Replication\r\n");
    sections.push_str("role:master\r\n");
    sections.push_str("connected_slaves:0\r\n");
    sections.push_str("\r\n");

    sections.push_str("# Keyspace\r\n");
    let key_count = db.len();
    let expires_count = db.expires_count();
    if key_count > 0 {
        let _ = write!(
            sections,
            "db0:keys={},expires={},avg_ttl=0\r\n",
            key_count, expires_count
        );
    }

    Frame::BulkString(Bytes::from(sections))
}

/// INFO command handler (read-only variant for RwLock read path).
///
/// Identical to info() -- Database methods used (len, expires_count) are already &self.
pub fn info_readonly(db: &Database, args: &[Frame]) -> Frame {
    info(db, args)
}

/// AUTH command handler.
///
/// Authenticates the client with the configured password.
/// Returns OK on success, WRONGPASS on mismatch, or ERR if no password is configured.
pub fn auth(args: &[Frame], requirepass: &Option<String>) -> Frame {
    if args.len() != 1 {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'AUTH' command",
        ));
    }

    let password = match requirepass {
        Some(p) => p,
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR Client sent AUTH, but no password is set",
            ));
        }
    };

    let provided = match &args[0] {
        Frame::BulkString(s) => s,
        Frame::SimpleString(s) => s,
        _ => {
            return Frame::Error(Bytes::from_static(b"ERR invalid password type"));
        }
    };

    if provided.as_ref() == password.as_bytes() {
        Frame::SimpleString(Bytes::from_static(b"OK"))
    } else {
        Frame::Error(Bytes::from_static(
            b"WRONGPASS invalid username-password pair or user is disabled.",
        ))
    }
}

/// AUTH command handler -- ACL-aware.
/// Handles 1-arg form (AUTH password -> authenticate as "default") and
/// 2-arg form (AUTH username password -> authenticate as named user).
/// Returns (response_frame, Option<authenticated_username>).
pub fn auth_acl(
    args: &[Frame],
    acl_table: &std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>>,
) -> (Frame, Option<String>) {
    match args.len() {
        1 => {
            let password = match extract_bytes_ref(&args[0]) {
                Some(p) => String::from_utf8_lossy(p).to_string(),
                None => {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR invalid password")),
                        None,
                    );
                }
            };
            // Fail closed: if the ACL lock is poisoned, deny authentication
            let Ok(table) = acl_table.read() else {
                return (
                    Frame::Error(Bytes::from_static(b"ERR internal ACL error")),
                    None,
                );
            };
            match table.authenticate("default", &password) {
                Some(username) => (
                    Frame::SimpleString(Bytes::from_static(b"OK")),
                    Some(username),
                ),
                None => (
                    Frame::Error(Bytes::from_static(
                        b"WRONGPASS invalid username-password pair or user is disabled.",
                    )),
                    None,
                ),
            }
        }
        2 => {
            let username = match extract_bytes_ref(&args[0]) {
                Some(u) => String::from_utf8_lossy(u).to_string(),
                None => {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR invalid username")),
                        None,
                    );
                }
            };
            let password = match extract_bytes_ref(&args[1]) {
                Some(p) => String::from_utf8_lossy(p).to_string(),
                None => {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR invalid password")),
                        None,
                    );
                }
            };
            // Fail closed: if the ACL lock is poisoned, deny authentication
            let Ok(table) = acl_table.read() else {
                return (
                    Frame::Error(Bytes::from_static(b"ERR internal ACL error")),
                    None,
                );
            };
            match table.authenticate(&username, &password) {
                Some(uname) => (Frame::SimpleString(Bytes::from_static(b"OK")), Some(uname)),
                None => (
                    Frame::Error(Bytes::from_static(
                        b"WRONGPASS invalid username-password pair or user is disabled.",
                    )),
                    None,
                ),
            }
        }
        _ => (
            Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'AUTH' command",
            )),
            None,
        ),
    }
}

/// HELLO command handler -- ACL-aware variant.
///
/// Like hello() but uses acl_table for AUTH option instead of requirepass.
/// Returns (response_frame, new_protocol_version, new_client_name, authenticated_username)
pub fn hello_acl(
    args: &[Frame],
    current_proto: u8,
    client_id: u64,
    acl_table: &std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>>,
    authenticated: &mut bool,
) -> (Frame, u8, Option<Bytes>, Option<String>) {
    let mut proto = current_proto;
    let mut client_name: Option<Bytes> = None;
    let mut auth_user: Option<String> = None;
    let mut i = 0;

    // Parse optional protover
    if i < args.len() {
        if let Some(ver_bytes) = extract_bytes_ref(&args[i]) {
            if let Ok(ver_str) = std::str::from_utf8(ver_bytes) {
                if let Ok(ver) = ver_str.parse::<u8>() {
                    if ver != 2 && ver != 3 {
                        return (
                            Frame::Error(Bytes::from_static(
                                b"NOPROTO unsupported protocol version",
                            )),
                            current_proto,
                            None,
                            None,
                        );
                    }
                    proto = ver;
                    i += 1;
                }
            }
        }
    }

    // Parse optional AUTH and SETNAME
    while i < args.len() {
        if let Some(keyword) = extract_bytes_ref(&args[i]) {
            if keyword.eq_ignore_ascii_case(b"AUTH") {
                if i + 2 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'auth'",
                        )),
                        current_proto,
                        None,
                        None,
                    );
                }
                // AUTH username password
                let username = match extract_bytes_ref(&args[i + 1]) {
                    Some(u) => String::from_utf8_lossy(u).to_string(),
                    None => {
                        return (
                            Frame::Error(Bytes::from_static(b"ERR invalid username")),
                            current_proto,
                            None,
                            None,
                        );
                    }
                };
                let password = match extract_bytes_ref(&args[i + 2]) {
                    Some(p) => String::from_utf8_lossy(p).to_string(),
                    None => {
                        return (
                            Frame::Error(Bytes::from_static(b"ERR invalid password")),
                            current_proto,
                            None,
                            None,
                        );
                    }
                };
                // Fail closed: if the ACL lock is poisoned, deny authentication
                let Ok(table) = acl_table.read() else {
                    return (
                        Frame::Error(Bytes::from_static(b"ERR internal ACL error")),
                        current_proto,
                        None,
                        None,
                    );
                };
                match table.authenticate(&username, &password) {
                    Some(uname) => {
                        *authenticated = true;
                        auth_user = Some(uname);
                    }
                    None => {
                        return (
                            Frame::Error(Bytes::from_static(
                                b"WRONGPASS invalid username-password pair or user is disabled.",
                            )),
                            current_proto,
                            None,
                            None,
                        );
                    }
                }
                i += 3;
            } else if keyword.eq_ignore_ascii_case(b"SETNAME") {
                if i + 1 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'setname'",
                        )),
                        current_proto,
                        None,
                        None,
                    );
                }
                client_name = extract_bytes_owned(&args[i + 1]);
                i += 2;
            } else {
                return (
                    Frame::Error(Bytes::from(format!(
                        "ERR Unrecognized HELLO option: {:?}",
                        String::from_utf8_lossy(keyword)
                    ))),
                    current_proto,
                    None,
                    None,
                );
            }
        } else {
            break;
        }
    }

    // Build response Map
    let response = Frame::Map(vec![
        (
            Frame::BulkString(Bytes::from_static(b"server")),
            Frame::BulkString(Bytes::from_static(b"moon")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"version")),
            Frame::BulkString(Bytes::from_static(b"0.1.0")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"proto")),
            Frame::Integer(proto as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"id")),
            Frame::Integer(client_id as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"mode")),
            Frame::BulkString(Bytes::from_static(b"standalone")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"role")),
            Frame::BulkString(Bytes::from_static(b"master")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"modules")),
            Frame::Array(framevec![]),
        ),
    ]);

    (response, proto, client_name, auth_user)
}

/// Action to take after REPLICAOF command is parsed.
pub enum ReplicaofAction {
    /// Connect to master and start replication.
    StartReplication { host: String, port: u16 },
    /// Promote to master: copy repl_id to repl_id2, generate new repl_id.
    PromoteToMaster,
    /// Already master, REPLICAOF NO ONE is a no-op.
    NoOp,
}

/// REPLICAOF host port -- initiate replication from master at host:port.
/// REPLICAOF NO ONE -- promote this replica to master.
///
/// Returns (response_frame, optional action for the caller to execute).
pub fn replicaof(args: &[Frame]) -> (Frame, Option<ReplicaofAction>) {
    if args.len() != 2 {
        return (
            Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'REPLICAOF' command",
            )),
            None,
        );
    }
    let host = match extract_bytes_ref(&args[0]) {
        Some(h) => String::from_utf8_lossy(h).to_string(),
        None => return (Frame::Error(Bytes::from_static(b"ERR invalid host")), None),
    };
    let port_str = match extract_bytes_ref(&args[1]) {
        Some(p) => String::from_utf8_lossy(p).to_string(),
        None => return (Frame::Error(Bytes::from_static(b"ERR invalid port")), None),
    };

    // REPLICAOF NO ONE -- promote to master
    if host.eq_ignore_ascii_case("NO") && port_str.eq_ignore_ascii_case("ONE") {
        return (
            Frame::SimpleString(Bytes::from_static(b"OK")),
            Some(ReplicaofAction::PromoteToMaster),
        );
    }

    let port: u16 = match port_str.parse() {
        Ok(p) => p,
        Err(_) => {
            return (
                Frame::Error(Bytes::from_static(
                    b"ERR value is not an integer or out of range",
                )),
                None,
            );
        }
    };

    (
        Frame::SimpleString(Bytes::from_static(b"OK")),
        Some(ReplicaofAction::StartReplication { host, port }),
    )
}

/// REPLCONF -- replication configuration handshake.
/// Responds OK to: listening-port <port>, capa eof, capa psync2, getack *.
/// Used during the PSYNC2 handshake sequence.
pub fn replconf(args: &[Frame]) -> Frame {
    if args.is_empty() {
        return Frame::Error(Bytes::from_static(
            b"ERR wrong number of arguments for 'REPLCONF' command",
        ));
    }
    // Accept all known REPLCONF subcommands with OK.
    Frame::SimpleString(Bytes::from_static(b"OK"))
}

/// Extract a byte slice reference from a Frame argument (zero-alloc).
fn extract_bytes_ref(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

/// Extract an owned Bytes from a Frame argument.
fn extract_bytes_owned(frame: &Frame) -> Option<Bytes> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.clone()),
        _ => None,
    }
}

/// HELLO command handler.
///
/// HELLO [protover [AUTH username password] [SETNAME clientname]]
/// Returns server info Map. Sets protocol_version if protover given.
/// Returns (response_frame, new_protocol_version, new_client_name)
pub fn hello(
    args: &[Frame],
    current_proto: u8,
    client_id: u64,
    requirepass: &Option<String>,
    authenticated: &mut bool,
) -> (Frame, u8, Option<Bytes>) {
    let mut proto = current_proto;
    let mut client_name: Option<Bytes> = None;
    let mut i = 0;

    // Parse optional protover
    if i < args.len() {
        if let Some(ver_bytes) = extract_bytes_ref(&args[i]) {
            if let Ok(ver_str) = std::str::from_utf8(ver_bytes) {
                if let Ok(ver) = ver_str.parse::<u8>() {
                    if ver != 2 && ver != 3 {
                        return (
                            Frame::Error(Bytes::from_static(
                                b"NOPROTO unsupported protocol version",
                            )),
                            current_proto,
                            None,
                        );
                    }
                    proto = ver;
                    i += 1;
                }
            }
        }
    }

    // Parse optional AUTH and SETNAME (can appear in any order after protover)
    while i < args.len() {
        if let Some(keyword) = extract_bytes_ref(&args[i]) {
            if keyword.eq_ignore_ascii_case(b"AUTH") {
                // Need username and password (2 more args)
                if i + 2 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'auth'",
                        )),
                        current_proto,
                        None,
                    );
                }
                // username is args[i+1] (we ignore it -- single-user mode)
                // password is args[i+2]
                let auth_result = auth(&[args[i + 2].clone()], requirepass);
                if matches!(&auth_result, Frame::Error(_)) {
                    return (auth_result, current_proto, None); // Auth failed, don't change proto
                }
                *authenticated = true;
                i += 3;
            } else if keyword.eq_ignore_ascii_case(b"SETNAME") {
                if i + 1 >= args.len() {
                    return (
                        Frame::Error(Bytes::from_static(
                            b"ERR Syntax error in HELLO option 'setname'",
                        )),
                        current_proto,
                        None,
                    );
                }
                client_name = extract_bytes_owned(&args[i + 1]);
                i += 2;
            } else {
                return (
                    Frame::Error(Bytes::from(format!(
                        "ERR Unrecognized HELLO option: {:?}",
                        String::from_utf8_lossy(keyword)
                    ))),
                    current_proto,
                    None,
                );
            }
        } else {
            break;
        }
    }

    // Build response Map
    let response = Frame::Map(vec![
        (
            Frame::BulkString(Bytes::from_static(b"server")),
            Frame::BulkString(Bytes::from_static(b"moon")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"version")),
            Frame::BulkString(Bytes::from_static(b"0.1.0")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"proto")),
            Frame::Integer(proto as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"id")),
            Frame::Integer(client_id as i64),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"mode")),
            Frame::BulkString(Bytes::from_static(b"standalone")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"role")),
            Frame::BulkString(Bytes::from_static(b"master")),
        ),
        (
            Frame::BulkString(Bytes::from_static(b"modules")),
            Frame::Array(framevec![]),
        ),
    ]);

    (response, proto, client_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_no_args() {
        let result = ping(&[]);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"PONG")));
    }

    #[test]
    fn test_ping_with_arg() {
        let result = ping(&[Frame::BulkString(Bytes::from_static(b"hello"))]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_ping_too_many_args() {
        let result = ping(&[
            Frame::BulkString(Bytes::from_static(b"a")),
            Frame::BulkString(Bytes::from_static(b"b")),
        ]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_echo() {
        let result = echo(&[Frame::BulkString(Bytes::from_static(b"hello"))]);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"hello")));
    }

    #[test]
    fn test_echo_wrong_arity() {
        let result = echo(&[]);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_select_valid() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"5"))],
            &mut selected,
            16,
        );
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(selected, 5);
    }

    #[test]
    fn test_select_out_of_range() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"16"))],
            &mut selected,
            16,
        );
        assert!(
            matches!(result, Frame::Error(ref s) if s.as_ref() == b"ERR DB index is out of range")
        );
        assert_eq!(selected, 0); // unchanged
    }

    #[test]
    fn test_select_non_integer() {
        let mut selected = 0usize;
        let result = select(
            &[Frame::BulkString(Bytes::from_static(b"abc"))],
            &mut selected,
            16,
        );
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_command_bare() {
        let result = command(&[]);
        assert_eq!(result, Frame::Integer(0));
    }

    #[test]
    fn test_command_docs() {
        let result = command(&[Frame::BulkString(Bytes::from_static(b"DOCS"))]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    #[test]
    fn test_command_docs_lowercase() {
        let result = command(&[Frame::BulkString(Bytes::from_static(b"docs"))]);
        assert_eq!(result, Frame::Array(framevec![]));
    }

    #[test]
    fn test_info_basic() {
        let db = Database::new();
        let result = info(&db, &[]);
        match result {
            Frame::BulkString(s) => {
                let text = std::str::from_utf8(&s).unwrap();
                assert!(text.contains("# Server"));
                assert!(text.contains("redis_version:0.1.0"));
                assert!(text.contains("# Keyspace"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_info_with_keys() {
        let mut db = Database::new();
        db.set(
            Bytes::from_static(b"key1"),
            crate::storage::Entry::new_string(Bytes::from_static(b"val")),
        );
        let result = info(&db, &[]);
        match result {
            Frame::BulkString(s) => {
                let text = std::str::from_utf8(&s).unwrap();
                assert!(text.contains("db0:keys=1,expires=0,avg_ttl=0"));
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_auth_correct_password() {
        let pass = Some("secret123".to_string());
        let result = auth(
            &[Frame::BulkString(Bytes::from_static(b"secret123"))],
            &pass,
        );
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_auth_wrong_password() {
        let pass = Some("secret123".to_string());
        let result = auth(&[Frame::BulkString(Bytes::from_static(b"wrong"))], &pass);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
    }

    #[test]
    fn test_auth_no_password_configured() {
        let pass: Option<String> = None;
        let result = auth(&[Frame::BulkString(Bytes::from_static(b"anything"))], &pass);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR Client sent AUTH")));
    }

    #[test]
    fn test_auth_wrong_arity() {
        let pass = Some("secret".to_string());
        let result = auth(&[], &pass);
        assert!(matches!(result, Frame::Error(ref s) if s.starts_with(b"ERR wrong number")));
    }

    // === HELLO command tests ===

    fn get_proto_from_hello_response(frame: &Frame) -> Option<i64> {
        if let Frame::Map(entries) = frame {
            for (k, v) in entries {
                if let Frame::BulkString(key) = k {
                    if key.as_ref() == b"proto" {
                        if let Frame::Integer(n) = v {
                            return Some(*n);
                        }
                    }
                }
            }
        }
        None
    }

    #[test]
    fn test_hello_no_args_returns_current_proto() {
        let mut auth = true;
        let (resp, proto, name) = hello(&[], 2, 1, &None, &mut auth);
        assert!(matches!(resp, Frame::Map(_)));
        assert_eq!(get_proto_from_hello_response(&resp), Some(2));
        assert_eq!(proto, 2);
        assert!(name.is_none());
    }

    #[test]
    fn test_hello_upgrade_to_resp3() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"3"))],
            2,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(proto, 3);
        assert_eq!(get_proto_from_hello_response(&resp), Some(3));
    }

    #[test]
    fn test_hello_downgrade_to_resp2() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"2"))],
            3,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(proto, 2);
        assert_eq!(get_proto_from_hello_response(&resp), Some(2));
    }

    #[test]
    fn test_hello_with_auth_success() {
        let pass = Some("secret".to_string());
        let mut auth = false;
        let (resp, proto, _) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"secret")),
            ],
            2,
            1,
            &pass,
            &mut auth,
        );
        assert_eq!(proto, 3);
        assert!(matches!(resp, Frame::Map(_)));
        assert!(auth); // authenticated
    }

    #[test]
    fn test_hello_with_auth_failure() {
        let pass = Some("secret".to_string());
        let mut auth = false;
        let (resp, proto, _) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"wrong")),
            ],
            2,
            1,
            &pass,
            &mut auth,
        );
        // Auth failed: proto stays at current, response is error
        assert_eq!(proto, 2);
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(!auth); // not authenticated
    }

    #[test]
    fn test_hello_with_setname() {
        let mut auth = true;
        let (_, _, name) = hello(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"SETNAME")),
                Frame::BulkString(Bytes::from_static(b"myclient")),
            ],
            2,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(name, Some(Bytes::from_static(b"myclient")));
    }

    #[test]
    fn test_hello_noproto() {
        let mut auth = true;
        let (resp, proto, _) = hello(
            &[Frame::BulkString(Bytes::from_static(b"4"))],
            2,
            1,
            &None,
            &mut auth,
        );
        assert_eq!(proto, 2); // unchanged
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"NOPROTO")));
    }

    #[test]
    fn test_client_id_returns_integer() {
        let result = client_id(42);
        assert_eq!(result, Frame::Integer(42));
    }

    // === REPLICAOF command tests ===

    #[test]
    fn test_replicaof_start_replication() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"127.0.0.1")),
            Frame::BulkString(Bytes::from_static(b"6379")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(matches!(
            action,
            Some(ReplicaofAction::StartReplication { ref host, port })
            if host == "127.0.0.1" && port == 6379
        ));
    }

    #[test]
    fn test_replicaof_no_one() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"NO")),
            Frame::BulkString(Bytes::from_static(b"ONE")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(matches!(action, Some(ReplicaofAction::PromoteToMaster)));
    }

    #[test]
    fn test_replicaof_no_one_case_insensitive() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"no")),
            Frame::BulkString(Bytes::from_static(b"one")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert!(matches!(action, Some(ReplicaofAction::PromoteToMaster)));
    }

    #[test]
    fn test_replicaof_wrong_arity() {
        let (resp, action) = replicaof(&[Frame::BulkString(Bytes::from_static(b"host"))]);
        assert!(matches!(resp, Frame::Error(_)));
        assert!(action.is_none());
    }

    #[test]
    fn test_replicaof_invalid_port() {
        let (resp, action) = replicaof(&[
            Frame::BulkString(Bytes::from_static(b"localhost")),
            Frame::BulkString(Bytes::from_static(b"notaport")),
        ]);
        assert!(matches!(resp, Frame::Error(_)));
        assert!(action.is_none());
    }

    // === REPLCONF command tests ===

    #[test]
    fn test_replconf_listening_port() {
        let resp = replconf(&[
            Frame::BulkString(Bytes::from_static(b"listening-port")),
            Frame::BulkString(Bytes::from_static(b"6380")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_replconf_capa() {
        let resp = replconf(&[
            Frame::BulkString(Bytes::from_static(b"capa")),
            Frame::BulkString(Bytes::from_static(b"psync2")),
        ]);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
    }

    #[test]
    fn test_replconf_empty_args() {
        let resp = replconf(&[]);
        assert!(matches!(resp, Frame::Error(_)));
    }

    // === auth_acl tests ===

    fn make_acl_table() -> std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>> {
        use crate::acl::{AclTable, AclUser};
        let mut table = AclTable::new();
        table.set_user("default".to_string(), AclUser::new_default_nopass());
        std::sync::Arc::new(std::sync::RwLock::new(table))
    }

    fn make_acl_table_with_password() -> std::sync::Arc<std::sync::RwLock<crate::acl::AclTable>> {
        use crate::acl::{AclTable, AclUser};
        let mut table = AclTable::new();
        table.set_user(
            "default".to_string(),
            AclUser::new_default_with_password("secret"),
        );
        std::sync::Arc::new(std::sync::RwLock::new(table))
    }

    #[test]
    fn test_auth_acl_1arg_nopass() {
        let table = make_acl_table();
        let (resp, user) = auth_acl(&[Frame::BulkString(Bytes::from_static(b"anypass"))], &table);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(user, Some("default".to_string()));
    }

    #[test]
    fn test_auth_acl_1arg_wrong_password() {
        let table = make_acl_table_with_password();
        let (resp, user) = auth_acl(&[Frame::BulkString(Bytes::from_static(b"wrong"))], &table);
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(user.is_none());
    }

    #[test]
    fn test_auth_acl_1arg_correct_password() {
        let table = make_acl_table_with_password();
        let (resp, user) = auth_acl(&[Frame::BulkString(Bytes::from_static(b"secret"))], &table);
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(user, Some("default".to_string()));
    }

    #[test]
    fn test_auth_acl_2arg_named_user() {
        let table = make_acl_table();
        // Create alice with password
        {
            let mut t = table.write().unwrap();
            t.apply_setuser("alice", &["on", ">alicepass", "~*", "+@all"]);
        }
        let (resp, user) = auth_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"alice")),
                Frame::BulkString(Bytes::from_static(b"alicepass")),
            ],
            &table,
        );
        assert_eq!(resp, Frame::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(user, Some("alice".to_string()));
    }

    #[test]
    fn test_auth_acl_2arg_wrong_password() {
        let table = make_acl_table();
        {
            let mut t = table.write().unwrap();
            t.apply_setuser("alice", &["on", ">alicepass", "~*", "+@all"]);
        }
        let (resp, user) = auth_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"alice")),
                Frame::BulkString(Bytes::from_static(b"wrong")),
            ],
            &table,
        );
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(user.is_none());
    }

    #[test]
    fn test_auth_acl_disabled_user() {
        let table = make_acl_table();
        {
            let mut t = table.write().unwrap();
            t.apply_setuser("alice", &["off", ">alicepass"]);
        }
        let (resp, user) = auth_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"alice")),
                Frame::BulkString(Bytes::from_static(b"alicepass")),
            ],
            &table,
        );
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(user.is_none());
    }

    #[test]
    fn test_auth_acl_wrong_arity() {
        let table = make_acl_table();
        let (resp, user) = auth_acl(&[], &table);
        assert!(matches!(resp, Frame::Error(_)));
        assert!(user.is_none());
    }

    // === hello_acl tests ===

    #[test]
    fn test_hello_acl_no_args() {
        let table = make_acl_table();
        let mut auth = true;
        let (resp, proto, name, user) = hello_acl(&[], 2, 1, &table, &mut auth);
        assert!(matches!(resp, Frame::Map(_)));
        assert_eq!(proto, 2);
        assert!(name.is_none());
        assert!(user.is_none());
    }

    #[test]
    fn test_hello_acl_with_auth_success() {
        let table = make_acl_table_with_password();
        let mut auth = false;
        let (resp, proto, _, user) = hello_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"secret")),
            ],
            2,
            1,
            &table,
            &mut auth,
        );
        assert_eq!(proto, 3);
        assert!(matches!(resp, Frame::Map(_)));
        assert!(auth);
        assert_eq!(user, Some("default".to_string()));
    }

    #[test]
    fn test_hello_acl_with_auth_failure() {
        let table = make_acl_table_with_password();
        let mut auth = false;
        let (resp, proto, _, user) = hello_acl(
            &[
                Frame::BulkString(Bytes::from_static(b"3")),
                Frame::BulkString(Bytes::from_static(b"AUTH")),
                Frame::BulkString(Bytes::from_static(b"default")),
                Frame::BulkString(Bytes::from_static(b"wrong")),
            ],
            2,
            1,
            &table,
            &mut auth,
        );
        assert_eq!(proto, 2); // unchanged
        assert!(matches!(resp, Frame::Error(ref s) if s.starts_with(b"WRONGPASS")));
        assert!(!auth);
        assert!(user.is_none());
    }
}
