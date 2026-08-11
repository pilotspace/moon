use std::io::BufRead;

use super::rules::apply_rule;
use super::table::{AclTable, AclUser, CommandPermissions};

/// Serialize a user to an ACL file line:
/// "user <name> on|off [nopass] [#hash ...] [~pattern ...] [&channel ...] [+@all|-@all ...]"
pub fn user_to_acl_line(user: &AclUser) -> String {
    let mut parts = vec![format!("user {}", user.username)];
    parts.push(if user.enabled {
        "on".to_string()
    } else {
        "off".to_string()
    });
    if user.nopass {
        parts.push("nopass".to_string());
    }
    for hash in &user.passwords {
        parts.push(format!("#{}", hash));
    }
    for kp in &user.key_patterns {
        if kp.read && kp.write {
            parts.push(format!("~{}", kp.pattern));
        } else if kp.read {
            parts.push(format!("%R~{}", kp.pattern));
        } else if kp.write {
            parts.push(format!("%W~{}", kp.pattern));
        }
    }
    for cp in &user.channel_patterns {
        parts.push(format!("&{}", cp));
    }
    // Serialize command permissions
    match &user.allowed_commands {
        CommandPermissions::AllAllowed => parts.push("+@all".to_string()),
        CommandPermissions::Specific { allowed, denied } => {
            if allowed.is_empty() && denied.is_empty() {
                parts.push("-@all".to_string());
            } else {
                parts.push("-@all".to_string());
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
            }
        }
    }
    parts.join(" ")
}

/// Parse a single ACL file line. Returns None for blank/comment lines.
pub fn parse_acl_line(line: &str) -> Option<AclUser> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let mut tokens = line.split_whitespace();
    if tokens.next()? != "user" {
        return None;
    }
    let username = tokens.next()?.to_string();
    let mut user = AclUser::default_deny(username);
    for token in tokens {
        apply_rule(&mut user, token);
    }
    Some(user)
}

/// Save all users to an ACL file.
///
/// Atomic via `atomic_write_durable` (task #49, kernel M4 prep): temp
/// file, fsync, rename, parent-dir fsync, so a kill-9 mid-save can never
/// leave a torn or (post-rename, pre-dir-fsync) reverted ACL file. The
/// prior code did a bare tmp-write + rename with no fsync at all -- on
/// ext4/xfs a crash right after `rename()` returns can still lose the
/// directory-entry update, silently reverting `path` to its old contents
/// (or nothing at all, on first save).
pub fn acl_save(path: &str, table: &AclTable) -> std::io::Result<()> {
    let mut content = String::new();
    for user in table.list_users() {
        content.push_str(&user_to_acl_line(user));
        content.push('\n');
    }
    crate::persistence::atomic::atomic_write_durable(
        std::path::Path::new(path),
        content.as_bytes(),
    )?;
    Ok(())
}

/// Load an ACL table from file. Returns error if file can't be read.
pub fn acl_load(path: &str) -> std::io::Result<AclTable> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let mut table = AclTable::new();
    for line in reader.lines() {
        let line = line?;
        if let Some(user) = parse_acl_line(&line) {
            table.set_user(user.username.clone(), user);
        }
    }
    Ok(table)
}

/// Load AclTable from config: uses aclfile if configured, otherwise bootstraps from requirepass.
/// Non-fatal: if aclfile missing or unreadable, falls back to requirepass-based defaults.
pub fn acl_table_from_config(config: &crate::config::ServerConfig) -> AclTable {
    if let Some(ref path) = config.aclfile {
        match acl_load(path) {
            Ok(mut table) => {
                // c10k B2: the permission checks now DENY an unknown user, so
                // an ACL file that never defines `default` would lock out
                // every connection. Redis guarantees `default` exists; so do
                // we, seeded from requirepass exactly as the no-file path below.
                table.ensure_default_user(config.requirepass.as_deref());
                return table;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // File doesn't exist yet -- start with defaults
            }
            Err(e) => {
                eprintln!("Warning: could not load ACL file '{}': {}", path, e);
            }
        }
    }
    // Bootstrap default user from requirepass (NOT load_or_default to avoid recursion)
    let mut table = AclTable::new();
    let default_user = match &config.requirepass {
        Some(p) if !p.is_empty() => AclUser::new_default_with_password(p),
        _ => AclUser::new_default_nopass(),
    };
    table.set_user("default".to_string(), default_user);
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::rules::hash_password;
    use crate::acl::table::KeyPattern;

    #[test]
    fn test_user_to_acl_line_default_nopass() {
        let user = AclUser::new_default_nopass();
        let line = user_to_acl_line(&user);
        assert!(line.starts_with("user default on"));
        assert!(line.contains("nopass"));
        assert!(line.contains("~*"));
        assert!(line.contains("&*"));
        assert!(line.contains("+@all"));
    }

    #[test]
    fn test_user_to_acl_line_with_password() {
        let user = AclUser::new_default_with_password("secret");
        let line = user_to_acl_line(&user);
        assert!(line.contains(&format!("#{}", hash_password("secret"))));
        assert!(!line.contains("nopass"));
    }

    #[test]
    fn test_parse_acl_line_basic() {
        let line = "user alice on nopass ~* &* +@all";
        let user = parse_acl_line(line).unwrap();
        assert_eq!(user.username, "alice");
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::AllAllowed
        ));
    }

    #[test]
    fn test_parse_acl_line_comment() {
        assert!(parse_acl_line("# this is a comment").is_none());
        assert!(parse_acl_line("").is_none());
        assert!(parse_acl_line("  ").is_none());
    }

    #[test]
    fn test_parse_acl_line_non_user() {
        assert!(parse_acl_line("notuser alice on").is_none());
    }

    #[test]
    fn test_roundtrip_user_to_acl_line_and_parse() {
        let user = AclUser::new_default_nopass();
        let line = user_to_acl_line(&user);
        let parsed = parse_acl_line(&line).unwrap();
        assert_eq!(parsed.username, user.username);
        assert_eq!(parsed.enabled, user.enabled);
        assert_eq!(parsed.nopass, user.nopass);
        assert!(matches!(
            parsed.allowed_commands,
            CommandPermissions::AllAllowed
        ));
    }

    #[test]
    fn test_acl_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.acl");
        let path_str = path.to_str().unwrap();

        let mut table = AclTable::new();
        let mut alice = AclUser::default_deny("alice".to_string());
        alice.enabled = true;
        alice.nopass = true;
        alice.key_patterns.push(KeyPattern {
            pattern: "*".to_string(),
            read: true,
            write: true,
        });
        alice.channel_patterns.push("*".to_string());
        alice.allowed_commands = CommandPermissions::AllAllowed;
        table.set_user("alice".to_string(), alice);

        let default_user = AclUser::new_default_nopass();
        table.set_user("default".to_string(), default_user);

        acl_save(path_str, &table).unwrap();

        let loaded = acl_load(path_str).unwrap();
        let alice_loaded = loaded.get_user("alice").unwrap();
        assert!(alice_loaded.enabled);
        assert!(alice_loaded.nopass);
        assert!(matches!(
            alice_loaded.allowed_commands,
            CommandPermissions::AllAllowed
        ));

        let default_loaded = loaded.get_user("default").unwrap();
        assert!(default_loaded.enabled);
        assert!(default_loaded.nopass);
    }

    /// Task #49: `acl_save` must go through `atomic_write_durable`, not a
    /// bare tmp-write+rename. Regression pin: no leftover `.tmp` file and
    /// the directory holds exactly the final `.acl` file after a
    /// successful save -- what a hand-rolled `format!("{}.tmp", path)`
    /// helper (the pre-fix code) would also achieve on the happy path, but
    /// silently skip the fsync/dir-fsync durability steps.
    #[test]
    fn test_acl_save_leaves_no_leftover_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.acl");
        let path_str = path.to_str().unwrap();

        let mut table = AclTable::new();
        table.set_user("default".to_string(), AclUser::new_default_nopass());

        acl_save(path_str, &table).unwrap();

        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(entries, vec![std::ffi::OsString::from("test.acl")]);
    }
}
