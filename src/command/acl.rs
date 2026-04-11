use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::acl::rules::get_category_commands;
use crate::acl::{AclLog, AclLogEntry, AclTable, CommandPermissions};
use crate::config::RuntimeConfig;
use crate::framevec;
use crate::protocol::Frame;
fn extract_str(frame: &Frame) -> Option<&str> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => std::str::from_utf8(b).ok(),
        _ => None,
    }
}

/// Handle ACL <subcommand> [args...]
/// acl_table: shared table for reads+writes
/// acl_log: per-connection mutable log (passed from connection state)
/// current_user: the authenticated username on this connection (for WHOAMI)
/// client_addr: peer address string (for LOG entries)
/// runtime_config: for aclfile path (ACL SAVE/LOAD)
pub fn handle_acl(
    sub_and_args: &[Frame],
    acl_table: &Arc<RwLock<AclTable>>,
    acl_log: &mut AclLog,
    current_user: &str,
    _client_addr: &str,
    runtime_config: &Arc<parking_lot::RwLock<RuntimeConfig>>,
) -> Frame {
    let sub = match sub_and_args.first().and_then(|f| extract_str(f)) {
        Some(s) => s.to_ascii_uppercase(),
        None => {
            return Frame::Error(Bytes::from_static(
                b"ERR wrong number of arguments for 'ACL' command",
            ));
        }
    };
    let args = if sub_and_args.len() > 1 {
        &sub_and_args[1..]
    } else {
        &[]
    };

    match sub.as_str() {
        "WHOAMI" => Frame::BulkString(Bytes::copy_from_slice(current_user.as_bytes())),

        "LIST" => {
            let Ok(table) = acl_table.read() else {
                return Frame::Error(Bytes::from_static(b"ERR internal ACL error"));
            };
            let lines: Vec<Frame> = table
                .list_users()
                .iter()
                .map(|u| {
                    let line = crate::acl::io::user_to_acl_line(u);
                    Frame::BulkString(Bytes::from(line))
                })
                .collect();
            Frame::Array(lines.into())
        }

        "GETUSER" => {
            let username = match args.first().and_then(|f| extract_str(f)) {
                Some(u) => u.to_string(),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR wrong number of arguments for 'ACL|GETUSER' command",
                    ));
                }
            };
            let Ok(table) = acl_table.read() else {
                return Frame::Error(Bytes::from_static(b"ERR internal ACL error"));
            };
            match table.get_user(&username) {
                None => Frame::Null,
                Some(user) => {
                    let flags: Vec<Frame> = {
                        let mut f = vec![];
                        if user.enabled {
                            f.push(Frame::BulkString(Bytes::from_static(b"on")));
                        } else {
                            f.push(Frame::BulkString(Bytes::from_static(b"off")));
                        }
                        if user.nopass {
                            f.push(Frame::BulkString(Bytes::from_static(b"nopass")));
                        }
                        f
                    };
                    let passwords: Vec<Frame> = user
                        .passwords
                        .iter()
                        .map(|h| Frame::BulkString(Bytes::from(format!("#{}", h))))
                        .collect();
                    let keys: Vec<Frame> = user
                        .key_patterns
                        .iter()
                        .map(|kp| {
                            let pat = if kp.read && kp.write {
                                format!("~{}", kp.pattern)
                            } else if kp.read {
                                format!("%R~{}", kp.pattern)
                            } else {
                                format!("%W~{}", kp.pattern)
                            };
                            Frame::BulkString(Bytes::from(pat))
                        })
                        .collect();
                    let channels: Vec<Frame> = user
                        .channel_patterns
                        .iter()
                        .map(|cp| Frame::BulkString(Bytes::from(format!("&{}", cp))))
                        .collect();
                    let commands = match &user.allowed_commands {
                        CommandPermissions::AllAllowed => "+@all".to_string(),
                        CommandPermissions::Specific { allowed, denied } => {
                            let mut parts = vec!["-@all".to_string()];
                            let mut allowed_sorted: Vec<&String> = allowed.iter().collect();
                            allowed_sorted.sort();
                            for a in allowed_sorted {
                                parts.push(format!("+{}", a));
                            }
                            let mut denied_sorted: Vec<&String> = denied.iter().collect();
                            denied_sorted.sort();
                            for d in denied_sorted {
                                parts.push(format!("-{}", d));
                            }
                            parts.join(" ")
                        }
                    };
                    Frame::Array(framevec![
                        Frame::BulkString(Bytes::from_static(b"username")),
                        Frame::BulkString(Bytes::from(user.username.clone())),
                        Frame::BulkString(Bytes::from_static(b"flags")),
                        Frame::Array(flags.into()),
                        Frame::BulkString(Bytes::from_static(b"passwords")),
                        Frame::Array(passwords.into()),
                        Frame::BulkString(Bytes::from_static(b"keys")),
                        Frame::Array(keys.into()),
                        Frame::BulkString(Bytes::from_static(b"channels")),
                        Frame::Array(channels.into()),
                        Frame::BulkString(Bytes::from_static(b"commands")),
                        Frame::BulkString(Bytes::from(commands)),
                    ])
                }
            }
        }

        "SETUSER" => {
            let username = match args.first().and_then(|f| extract_str(f)) {
                Some(u) => u.to_string(),
                None => {
                    return Frame::Error(Bytes::from_static(
                        b"ERR wrong number of arguments for 'ACL|SETUSER' command",
                    ));
                }
            };
            let rules: Vec<&str> = args[1..].iter().filter_map(|f| extract_str(f)).collect();
            let Ok(mut table) = acl_table.write() else {
                return Frame::Error(Bytes::from_static(b"ERR internal ACL error"));
            };
            table.apply_setuser(&username, &rules);
            Frame::SimpleString(Bytes::from_static(b"OK"))
        }

        "DELUSER" => {
            if args.is_empty() {
                return Frame::Error(Bytes::from_static(
                    b"ERR wrong number of arguments for 'ACL|DELUSER' command",
                ));
            }
            let mut count = 0i64;
            let Ok(mut table) = acl_table.write() else {
                return Frame::Error(Bytes::from_static(b"ERR internal ACL error"));
            };
            for arg in args {
                if let Some(name) = extract_str(arg) {
                    if name == "default" {
                        return Frame::Error(Bytes::from_static(
                            b"ERR The 'default' user cannot be removed",
                        ));
                    }
                    if table.del_user(name) {
                        count += 1;
                    }
                }
            }
            Frame::Integer(count)
        }

        "CAT" => {
            if args.is_empty() {
                // Return all category names
                let cats = vec![
                    "read",
                    "write",
                    "string",
                    "hash",
                    "list",
                    "set",
                    "sortedset",
                    "stream",
                    "pubsub",
                    "admin",
                    "dangerous",
                    "keyspace",
                    "connection",
                    "transaction",
                    "scripting",
                    "cluster",
                    "all",
                ];
                Frame::Array(
                    cats.iter()
                        .map(|c| Frame::BulkString(Bytes::copy_from_slice(c.as_bytes())))
                        .collect(),
                )
            } else {
                let cat = match extract_str(&args[0]) {
                    Some(c) => c.to_string(),
                    None => return Frame::Error(Bytes::from_static(b"ERR invalid category")),
                };
                let cat_stripped = cat.trim_start_matches('@');
                let known_cats = [
                    "all",
                    "read",
                    "write",
                    "string",
                    "hash",
                    "list",
                    "set",
                    "sortedset",
                    "stream",
                    "pubsub",
                    "admin",
                    "dangerous",
                    "keyspace",
                    "connection",
                    "transaction",
                    "scripting",
                    "cluster",
                ];
                if !known_cats.contains(&cat_stripped) {
                    return Frame::Error(Bytes::from(format!(
                        "ERR Unknown category '{}'. Try ACL CAT with no arguments to get a list of all available categories",
                        cat
                    )));
                }
                let cmds = get_category_commands(&cat);
                Frame::Array(
                    cmds.iter()
                        .map(|c| Frame::BulkString(Bytes::copy_from_slice(c.as_bytes())))
                        .collect(),
                )
            }
        }

        "LOG" => {
            if let Some(first) = args.first() {
                if let Some(s) = extract_str(first) {
                    if s.eq_ignore_ascii_case("RESET") {
                        acl_log.reset();
                        return Frame::SimpleString(Bytes::from_static(b"OK"));
                    }
                    if let Ok(n) = s.parse::<usize>() {
                        let entries = acl_log.entries(Some(n));
                        return Frame::Array(
                            entries.iter().map(|e| log_entry_to_frame(e)).collect(),
                        );
                    }
                }
            }
            let entries = acl_log.entries(None);
            Frame::Array(entries.iter().map(|e| log_entry_to_frame(e)).collect())
        }

        "SAVE" => {
            let cfg = runtime_config.read();
            let aclfile = cfg.aclfile.clone();
            drop(cfg);
            match aclfile {
                None => Frame::Error(Bytes::from_static(
                    b"ERR ACL file not configured. Use --aclfile or CONFIG SET aclfile",
                )),
                Some(path) => {
                    let Ok(table) = acl_table.read() else {
                        return Frame::Error(Bytes::from_static(b"ERR internal ACL error"));
                    };
                    // Blocking save -- acceptable for admin command
                    let content: String = table
                        .list_users()
                        .iter()
                        .map(|u| crate::acl::io::user_to_acl_line(u) + "\n")
                        .collect();
                    drop(table);
                    let tmp = format!("{}.tmp", path);
                    match std::fs::write(&tmp, content.as_bytes())
                        .and_then(|_| std::fs::rename(&tmp, &path))
                    {
                        Ok(_) => Frame::SimpleString(Bytes::from_static(b"OK")),
                        Err(e) => Frame::Error(Bytes::from(format!("ERR ACL save failed: {}", e))),
                    }
                }
            }
        }

        "LOAD" => {
            let cfg = runtime_config.read();
            let aclfile = cfg.aclfile.clone();
            drop(cfg);
            match aclfile {
                None => Frame::Error(Bytes::from_static(b"ERR ACL file not configured")),
                Some(path) => match std::fs::read_to_string(&path) {
                    Err(e) => Frame::Error(Bytes::from(format!("ERR ACL load failed: {}", e))),
                    Ok(content) => {
                        let mut new_table = AclTable::new_empty();
                        for line in content.lines() {
                            if let Some(user) = crate::acl::io::parse_acl_line(line) {
                                new_table.set_user(user.username.clone(), user);
                            }
                        }
                        let Ok(mut table) = acl_table.write() else {
                            return Frame::Error(Bytes::from_static(b"ERR internal ACL error"));
                        };
                        *table = new_table;
                        Frame::SimpleString(Bytes::from_static(b"OK"))
                    }
                },
            }
        }

        "GENPASS" => {
            // ACL GENPASS [bits] — generate cryptographically secure random password
            let bits: usize = if let Some(arg) = args.first() {
                match extract_str(arg).and_then(|s| s.parse().ok()) {
                    Some(b) if b > 0 && b <= 4096 => b,
                    _ => {
                        return Frame::Error(Bytes::from_static(
                            b"ERR ACL GENPASS argument must be a positive integer up to 4096",
                        ));
                    }
                }
            } else {
                256 // default: 256 bits = 64 hex chars
            };
            let byte_count = (bits + 7) / 8;
            let mut buf = vec![0u8; byte_count];
            rand::RngExt::fill(&mut rand::rng(), &mut buf[..]);
            let hex = hex::encode(&buf);
            // Truncate to exact number of hex chars for the requested bits
            let hex_chars = (bits + 3) / 4; // 4 bits per hex char
            Frame::BulkString(Bytes::from(hex[..hex_chars].to_string()))
        }

        _ => Frame::Error(Bytes::from(format!(
            "ERR unknown subcommand '{}'. Try ACL HELP.",
            sub
        ))),
    }
}

fn log_entry_to_frame(entry: &AclLogEntry) -> Frame {
    Frame::Array(framevec![
        Frame::BulkString(Bytes::from_static(b"reason")),
        Frame::BulkString(Bytes::from(entry.reason.clone())),
        Frame::BulkString(Bytes::from_static(b"object")),
        Frame::BulkString(Bytes::from(entry.object.clone())),
        Frame::BulkString(Bytes::from_static(b"username")),
        Frame::BulkString(Bytes::from(entry.username.clone())),
        Frame::BulkString(Bytes::from_static(b"client-addr")),
        Frame::BulkString(Bytes::from(entry.client_addr.clone())),
        Frame::BulkString(Bytes::from_static(b"timestamp-ms")),
        Frame::Integer(entry.timestamp_ms as i64),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::{AclLog, AclTable};
    use crate::config::RuntimeConfig;

    fn make_acl_table() -> Arc<RwLock<AclTable>> {
        let mut table = AclTable::new();
        // Default user with nopass
        let default_user = crate::acl::AclUser::new_default_nopass();
        table.set_user("default".to_string(), default_user);
        Arc::new(RwLock::new(table))
    }

    fn make_runtime_config() -> Arc<parking_lot::RwLock<RuntimeConfig>> {
        Arc::new(parking_lot::RwLock::new(RuntimeConfig::default()))
    }

    #[test]
    fn test_acl_whoami() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![Frame::BulkString(Bytes::from_static(b"WHOAMI"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert_eq!(result, Frame::BulkString(Bytes::from_static(b"default")));
    }

    #[test]
    fn test_acl_setuser_and_getuser() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();

        // SETUSER alice on >secret ~* +@all
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"SETUSER")),
            Frame::BulkString(Bytes::from_static(b"alice")),
            Frame::BulkString(Bytes::from_static(b"on")),
            Frame::BulkString(Bytes::from_static(b">secret")),
            Frame::BulkString(Bytes::from_static(b"~*")),
            Frame::BulkString(Bytes::from_static(b"+@all")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // GETUSER alice
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"GETUSER")),
            Frame::BulkString(Bytes::from_static(b"alice")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::Array(ref fields) => {
                // Should have username, flags, passwords, keys, channels, commands
                assert_eq!(fields.len(), 12); // 6 pairs
                assert_eq!(
                    fields[0],
                    Frame::BulkString(Bytes::from_static(b"username"))
                );
                assert_eq!(fields[1], Frame::BulkString(Bytes::from_static(b"alice")));
            }
            _ => panic!("Expected Array from GETUSER, got {:?}", result),
        }
    }

    #[test]
    fn test_acl_getuser_nonexistent() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"GETUSER")),
            Frame::BulkString(Bytes::from_static(b"nonexistent")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert_eq!(result, Frame::Null);
    }

    #[test]
    fn test_acl_deluser() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();

        // Create alice first
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"SETUSER")),
            Frame::BulkString(Bytes::from_static(b"alice")),
            Frame::BulkString(Bytes::from_static(b"on")),
        ];
        handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);

        // DELUSER alice
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"DELUSER")),
            Frame::BulkString(Bytes::from_static(b"alice")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert_eq!(result, Frame::Integer(1));
    }

    #[test]
    fn test_acl_deluser_default_fails() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"DELUSER")),
            Frame::BulkString(Bytes::from_static(b"default")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_acl_list() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![Frame::BulkString(Bytes::from_static(b"LIST"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::Array(ref items) => {
                assert!(!items.is_empty());
                // Each entry should start with "user "
                if let Frame::BulkString(ref line) = items[0] {
                    assert!(line.starts_with(b"user "));
                }
            }
            _ => panic!("Expected Array from LIST"),
        }
    }

    #[test]
    fn test_acl_cat_all_categories() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![Frame::BulkString(Bytes::from_static(b"CAT"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::Array(ref cats) => {
                assert!(!cats.is_empty());
            }
            _ => panic!("Expected Array from CAT"),
        }
    }

    #[test]
    fn test_acl_cat_string_category() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"CAT")),
            Frame::BulkString(Bytes::from_static(b"string")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::Array(ref cmds) => {
                assert!(!cmds.is_empty());
            }
            _ => panic!("Expected Array from CAT string"),
        }
    }

    #[test]
    fn test_acl_cat_unknown_category() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"CAT")),
            Frame::BulkString(Bytes::from_static(b"nonexistent")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_acl_log_and_reset() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();

        // Push some entries
        log.push(AclLogEntry {
            reason: "command".to_string(),
            object: "SET".to_string(),
            username: "alice".to_string(),
            client_addr: "127.0.0.1:1234".to_string(),
            timestamp_ms: 100,
        });

        let args = vec![Frame::BulkString(Bytes::from_static(b"LOG"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::Array(ref entries) => assert_eq!(entries.len(), 1),
            _ => panic!("Expected Array from LOG"),
        }

        // LOG RESET
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"LOG")),
            Frame::BulkString(Bytes::from_static(b"RESET")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify log is empty
        let args = vec![Frame::BulkString(Bytes::from_static(b"LOG"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::Array(ref entries) => assert_eq!(entries.len(), 0),
            _ => panic!("Expected Array from LOG after RESET"),
        }
    }

    #[test]
    fn test_acl_log_with_count() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();

        for i in 0..10 {
            log.push(AclLogEntry {
                reason: "command".to_string(),
                object: format!("CMD_{}", i),
                username: "alice".to_string(),
                client_addr: "127.0.0.1:1234".to_string(),
                timestamp_ms: i as u64,
            });
        }

        let args = vec![
            Frame::BulkString(Bytes::from_static(b"LOG")),
            Frame::BulkString(Bytes::from_static(b"5")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::Array(ref entries) => assert_eq!(entries.len(), 5),
            _ => panic!("Expected Array from LOG 5"),
        }
    }

    #[test]
    fn test_acl_save_no_aclfile() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config(); // no aclfile configured
        let args = vec![Frame::BulkString(Bytes::from_static(b"SAVE"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_acl_load_no_aclfile() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config(); // no aclfile configured
        let args = vec![Frame::BulkString(Bytes::from_static(b"LOAD"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_acl_save_and_load() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);

        // Create a temp dir for aclfile
        let dir = tempfile::tempdir().unwrap();
        let aclfile = dir.path().join("test.acl");
        let aclfile_str = aclfile.to_str().unwrap().to_string();

        let rc = Arc::new(parking_lot::RwLock::new(RuntimeConfig {
            aclfile: Some(aclfile_str.clone()),
            ..RuntimeConfig::default()
        }));

        // Add a user first
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"SETUSER")),
            Frame::BulkString(Bytes::from_static(b"alice")),
            Frame::BulkString(Bytes::from_static(b"on")),
            Frame::BulkString(Bytes::from_static(b"nopass")),
            Frame::BulkString(Bytes::from_static(b"~*")),
            Frame::BulkString(Bytes::from_static(b"+@all")),
        ];
        handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);

        // SAVE
        let args = vec![Frame::BulkString(Bytes::from_static(b"SAVE"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify file exists
        assert!(aclfile.exists());

        // LOAD into a fresh table
        let args = vec![Frame::BulkString(Bytes::from_static(b"LOAD"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert_eq!(result, Frame::SimpleString(Bytes::from_static(b"OK")));

        // Verify alice still exists after load
        let loaded_table = table.read().unwrap();
        assert!(loaded_table.get_user("alice").is_some());
    }

    #[test]
    fn test_acl_unknown_subcommand() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![Frame::BulkString(Bytes::from_static(b"INVALID"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert!(matches!(result, Frame::Error(_)));
    }

    #[test]
    fn test_acl_genpass_default() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![Frame::BulkString(Bytes::from_static(b"GENPASS"))];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::BulkString(b) => {
                assert_eq!(b.len(), 64); // 256 bits = 64 hex chars
                assert!(b.iter().all(|&c| c.is_ascii_hexdigit()));
            }
            _ => panic!("Expected BulkString, got {:?}", result),
        }
    }

    #[test]
    fn test_acl_genpass_custom_bits() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"GENPASS")),
            Frame::BulkString(Bytes::from_static(b"128")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        match result {
            Frame::BulkString(b) => {
                assert_eq!(b.len(), 32); // 128 bits = 32 hex chars
            }
            _ => panic!("Expected BulkString"),
        }
    }

    #[test]
    fn test_acl_genpass_invalid_bits() {
        let table = make_acl_table();
        let mut log = AclLog::new(128);
        let rc = make_runtime_config();
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"GENPASS")),
            Frame::BulkString(Bytes::from_static(b"0")),
        ];
        let result = handle_acl(&args, &table, &mut log, "default", "127.0.0.1:1234", &rc);
        assert!(matches!(result, Frame::Error(_)));
    }
}
