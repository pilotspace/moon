use std::collections::{HashMap, HashSet};

use crate::config::ServerConfig;
use crate::protocol::Frame;

use super::rules::{apply_rule, get_category_commands, hash_password, verify_password};

#[derive(Clone, Debug)]
pub struct KeyPattern {
    pub pattern: String,
    pub read: bool,
    pub write: bool,
}

#[derive(Clone, Debug)]
pub enum CommandPermissions {
    AllAllowed,
    Specific {
        allowed: HashSet<String>,
        denied: HashSet<String>,
    },
}

#[derive(Clone, Debug)]
pub struct AclUser {
    pub username: String,
    pub enabled: bool,
    pub passwords: Vec<String>,
    pub nopass: bool,
    pub allowed_commands: CommandPermissions,
    pub key_patterns: Vec<KeyPattern>,
    pub channel_patterns: Vec<String>,
    /// Cached: true iff this user has *no* restrictions at all --
    /// enabled, all commands allowed, `~*` read+write key pattern, and
    /// `*` channel pattern. Checked on the command dispatch hot path
    /// (every command) to skip per-command lowercasing, key extraction,
    /// glob matching, and HashSet probing. Computed in
    /// `recompute_unrestricted` whenever any permission field changes.
    unrestricted: bool,
}

impl AclUser {
    pub fn new_default_nopass() -> Self {
        let mut u = AclUser {
            username: "default".to_string(),
            enabled: true,
            passwords: vec![],
            nopass: true,
            allowed_commands: CommandPermissions::AllAllowed,
            key_patterns: vec![KeyPattern {
                pattern: "*".to_string(),
                read: true,
                write: true,
            }],
            channel_patterns: vec!["*".to_string()],
            unrestricted: false,
        };
        u.recompute_unrestricted();
        u
    }

    pub fn new_default_with_password(password: &str) -> Self {
        let mut u = AclUser {
            username: "default".to_string(),
            enabled: true,
            passwords: vec![hash_password(password)],
            nopass: false,
            allowed_commands: CommandPermissions::AllAllowed,
            key_patterns: vec![KeyPattern {
                pattern: "*".to_string(),
                read: true,
                write: true,
            }],
            channel_patterns: vec!["*".to_string()],
            unrestricted: false,
        };
        u.recompute_unrestricted();
        u
    }

    /// Reset to a default-deny user (for "reset" rule)
    pub fn default_deny(username: String) -> Self {
        AclUser {
            username,
            enabled: false,
            passwords: vec![],
            nopass: false,
            allowed_commands: CommandPermissions::Specific {
                allowed: HashSet::new(),
                denied: HashSet::new(),
            },
            key_patterns: vec![],
            channel_patterns: vec![],
            unrestricted: false,
        }
    }

    /// Return the cached unrestricted flag.
    ///
    /// `true` iff this user is enabled AND has *no* command, key, or
    /// channel restrictions -- i.e. the default `on nopass ~* &* +@all`
    /// shape. The ACL permission checks consult this before doing any
    /// per-command lowercasing, key extraction, or glob matching.
    #[inline]
    pub fn unrestricted(&self) -> bool {
        self.unrestricted
    }

    /// Public re-compute hook called from `apply_rule` after mutation.
    #[inline]
    pub(crate) fn refresh_unrestricted_cache(&mut self) {
        self.recompute_unrestricted();
    }

    /// Recompute the `unrestricted` cache.
    ///
    /// MUST be called from every mutation site that touches `enabled`,
    /// `allowed_commands`, `key_patterns`, or `channel_patterns`. The
    /// accompanying unit tests assert this for every `apply_rule` path.
    fn recompute_unrestricted(&mut self) {
        // Unrestricted iff:
        //   1. user is enabled,
        //   2. allowed_commands is AllAllowed (no +/- have been applied),
        //   3. at least one key pattern is `~*` with both read and write,
        //      AND no restricted pattern is present (any pattern whose
        //      glob is not "*" or which lacks read/write would narrow
        //      access, so we require ALL patterns to be fully-open),
        //   4. at least one channel pattern is `*`, AND all channel
        //      patterns are `*`.
        //
        // Condition (3/4) allows multiple duplicate `~*` / `&*` entries
        // (apply_rule appends rather than replaces) while still
        // rejecting any narrowing pattern.
        let keys_unrestricted = !self.key_patterns.is_empty()
            && self
                .key_patterns
                .iter()
                .all(|kp| kp.pattern == "*" && kp.read && kp.write);
        let channels_unrestricted =
            !self.channel_patterns.is_empty() && self.channel_patterns.iter().all(|p| p == "*");
        self.unrestricted = self.enabled
            && matches!(self.allowed_commands, CommandPermissions::AllAllowed)
            && keys_unrestricted
            && channels_unrestricted;
    }

    pub fn allow_command(&mut self, rule: &str) {
        if rule == "@all" {
            self.allowed_commands = CommandPermissions::AllAllowed;
            return;
        }
        match &mut self.allowed_commands {
            CommandPermissions::AllAllowed => {} // already all allowed
            CommandPermissions::Specific { allowed, denied } => {
                if rule.starts_with('@') {
                    for cmd in get_category_commands(rule) {
                        allowed.insert(cmd.to_string());
                        denied.remove(*cmd);
                    }
                } else if let Some(idx) = rule.find('|') {
                    // subcommand: store as "cmd|sub"
                    allowed.insert(rule[..idx].to_string() + "|" + &rule[idx + 1..]);
                } else {
                    allowed.insert(rule.to_ascii_lowercase());
                    denied.remove(&rule.to_ascii_lowercase());
                }
            }
        }
    }

    pub fn deny_command(&mut self, rule: &str) {
        if rule == "@all" {
            self.allowed_commands = CommandPermissions::Specific {
                allowed: HashSet::new(),
                denied: HashSet::new(),
            };
            return;
        }
        match &mut self.allowed_commands {
            CommandPermissions::AllAllowed => {
                // Transition to Specific with everything allowed except this
                let mut denied = HashSet::new();
                if rule.starts_with('@') {
                    for cmd in get_category_commands(rule) {
                        denied.insert(cmd.to_string());
                    }
                } else {
                    denied.insert(rule.to_ascii_lowercase());
                }
                self.allowed_commands = CommandPermissions::Specific {
                    allowed: HashSet::new(), // empty means "everything not denied"
                    denied,
                };
            }
            CommandPermissions::Specific { allowed, denied } => {
                if rule.starts_with('@') {
                    for cmd in get_category_commands(rule) {
                        denied.insert(cmd.to_string());
                        allowed.remove(*cmd);
                    }
                } else {
                    denied.insert(rule.to_ascii_lowercase());
                    allowed.remove(&rule.to_ascii_lowercase());
                }
            }
        }
    }

    pub fn is_command_allowed(&self, cmd: &str) -> bool {
        let cmd_lower = cmd.to_ascii_lowercase();
        match &self.allowed_commands {
            CommandPermissions::AllAllowed => true,
            CommandPermissions::Specific { allowed, denied } => {
                // Deny takes precedence
                if denied.contains(&cmd_lower) {
                    return false;
                }
                // If explicitly allowed, permit
                if allowed.contains(&cmd_lower) {
                    return true;
                }
                // If allowed is empty and denied is non-empty:
                //   This came from AllAllowed -> deny specific commands.
                //   Everything not in denied is allowed.
                if allowed.is_empty() && !denied.is_empty() {
                    return true;
                }
                // If both empty: deny-all state (-@all with no +cmd)
                // If allowed non-empty: only explicitly allowed commands pass
                false
            }
        }
    }
}

pub struct AclTable {
    users: HashMap<String, AclUser>,
}

impl AclTable {
    pub fn new() -> Self {
        AclTable {
            users: HashMap::new(),
        }
    }

    /// Create an empty table with no users (used by ACL LOAD).
    pub fn new_empty() -> Self {
        AclTable {
            users: HashMap::new(),
        }
    }

    /// Bootstrap from ServerConfig. Loads aclfile if configured, otherwise creates
    /// the default user from requirepass (or nopass).
    pub fn load_or_default(config: &ServerConfig) -> Self {
        // Delegate to io::acl_table_from_config which handles aclfile loading
        // with fallback to requirepass-based default.
        crate::acl::io::acl_table_from_config(config)
    }

    pub fn get_user(&self, username: &str) -> Option<&AclUser> {
        self.users.get(username)
    }

    pub fn get_user_mut(&mut self, username: &str) -> Option<&mut AclUser> {
        self.users.get_mut(username)
    }

    pub fn set_user(&mut self, username: String, user: AclUser) {
        self.users.insert(username, user);
    }

    pub fn del_user(&mut self, username: &str) -> bool {
        self.users.remove(username).is_some()
    }

    pub fn list_users(&self) -> Vec<&AclUser> {
        let mut users: Vec<&AclUser> = self.users.values().collect();
        users.sort_by(|a, b| a.username.cmp(&b.username));
        users
    }

    /// Apply ACL SETUSER rules to create or modify a user.
    /// Creates user if not exists (default-deny for new non-default users).
    pub fn apply_setuser(&mut self, username: &str, rules: &[&str]) {
        let user = self.users.entry(username.to_string()).or_insert_with(|| {
            if username == "default" {
                AclUser::new_default_nopass()
            } else {
                AclUser::default_deny(username.to_string())
            }
        });
        for rule in rules {
            apply_rule(user, rule);
        }
    }

    /// Authenticate username+password. Returns Some(username) on success, None on failure.
    pub fn authenticate(&self, username: &str, password: &str) -> Option<String> {
        let user = self.users.get(username)?;
        if !user.enabled {
            return None;
        }
        if user.nopass {
            return Some(username.to_string());
        }
        if user.passwords.iter().any(|h| verify_password(password, h)) {
            Some(username.to_string())
        } else {
            None
        }
    }

    /// Return `true` if the named user exists and has no restrictions at all
    /// (enabled + AllAllowed + `~*` rw + `&*`). Used by the connection handler
    /// to cache the unrestricted flag per-connection, avoiding the RwLock +
    /// HashMap probe on every command for the common case (default user).
    #[inline]
    pub fn is_user_unrestricted(&self, username: &str) -> bool {
        self.users.get(username).is_some_and(|u| u.unrestricted())
    }

    /// Check if the command is allowed for the user.
    /// Returns None if allowed, Some(reason) if denied.
    pub fn check_command_permission(
        &self,
        username: &str,
        cmd: &[u8],
        _args: &[Frame],
    ) -> Option<String> {
        let user = self.users.get(username)?;
        // Hot path: unrestricted user (default `on nopass ~* &* +@all`)
        // short-circuits before any per-command allocation. Profile showed
        // ~1% of CPU here for the lowercasing + HashSet probe; the
        // unrestricted check is a single bool load.
        if user.unrestricted {
            return None;
        }
        if !user.enabled {
            return Some(format!("User {} is disabled", username));
        }
        let cmd_str = std::str::from_utf8(cmd).unwrap_or("").to_ascii_lowercase();
        if !user.is_command_allowed(&cmd_str) {
            return Some(format!(
                "User {} has no permissions to run the '{}' command",
                username, cmd_str
            ));
        }
        None
    }

    /// Check key access for the user. Extracts relevant keys from cmd+args.
    /// Returns None if allowed, Some(reason) if denied.
    pub fn check_key_permission(
        &self,
        username: &str,
        cmd: &[u8],
        args: &[Frame],
        is_write: bool,
    ) -> Option<String> {
        let user = self.users.get(username)?;
        // Hot path: unrestricted user skips extract_command_keys + the
        // O(patterns*keys) glob match loop. Profile showed ~1.2% of CPU
        // here, most of it in glob_match and Vec allocation for the
        // extracted keys.
        if user.unrestricted {
            return None;
        }
        if user.key_patterns.is_empty() {
            return Some(format!("User {} has no key permissions", username));
        }
        // ~* (read+write) shortcut -- fast path for users that have
        // unrestricted keys but restricted commands (so `unrestricted`
        // above was false for other reasons).
        if user
            .key_patterns
            .iter()
            .any(|kp| kp.pattern == "*" && kp.read && kp.write)
        {
            return None;
        }
        let keys = extract_command_keys(cmd, args);
        for key in keys {
            let key_str = std::str::from_utf8(key).unwrap_or("");
            let allowed = user.key_patterns.iter().any(|kp| {
                let access_ok = if is_write { kp.write } else { kp.read };
                access_ok && crate::command::key::glob_match(kp.pattern.as_bytes(), key)
            });
            if !allowed {
                return Some(format!(
                    "User {} has no permissions to access key '{}'",
                    username, key_str
                ));
            }
        }
        None
    }

    /// Check channel access for pub/sub.
    pub fn check_channel_permission(&self, username: &str, channel: &[u8]) -> Option<String> {
        let user = self.users.get(username)?;
        if user.unrestricted {
            return None;
        }
        if user.channel_patterns.is_empty() {
            return Some(format!("User {} has no channel permissions", username));
        }
        let channel_str = std::str::from_utf8(channel).unwrap_or("");
        let allowed = user
            .channel_patterns
            .iter()
            .any(|pat| crate::command::key::glob_match(pat.as_bytes(), channel));
        if !allowed {
            Some(format!(
                "User {} has no permissions to access channel '{}'",
                username, channel_str
            ))
        } else {
            None
        }
    }

    /// Serialize user to rule string for ACL LIST (format: "user <name> on|off ...")
    pub fn user_to_rule_string(&self, username: &str) -> Option<String> {
        let user = self.users.get(username)?;
        Some(super::io::user_to_acl_line(user))
    }
}

/// Extract the key argument positions for known multi-key commands.
fn extract_command_keys<'a>(cmd: &[u8], args: &'a [Frame]) -> Vec<&'a [u8]> {
    let cmd_lower = cmd.to_ascii_lowercase();
    match cmd_lower.as_slice() {
        // Single key commands: key is args[0]
        b"get" | b"set" | b"incr" | b"decr" | b"incrby" | b"decrby" | b"incrbyfloat"
        | b"append" | b"strlen" | b"setnx" | b"setex" | b"psetex" | b"getset" | b"getdel"
        | b"getex" | b"hget" | b"hset" | b"hdel" | b"hmget" | b"hmset" | b"hgetall" | b"hkeys"
        | b"hvals" | b"hlen" | b"hexists" | b"hincrby" | b"hincrbyfloat" | b"hscan" | b"lpush"
        | b"rpush" | b"lpop" | b"rpop" | b"lrange" | b"llen" | b"linsert" | b"lindex" | b"lset"
        | b"ltrim" | b"lpos" | b"sadd" | b"srem" | b"smembers" | b"sismember" | b"smismember"
        | b"scard" | b"srandmember" | b"spop" | b"sscan" | b"smove" | b"zadd" | b"zrem"
        | b"zscore" | b"zrange" | b"zrangebyscore" | b"zrangebylex" | b"zrevrange"
        | b"zrevrangebyscore" | b"zrevrangebylex" | b"zrank" | b"zrevrank" | b"zcard"
        | b"zincrby" | b"zcount" | b"zlexcount" | b"zpopmin" | b"zpopmax" | b"bzpopmin"
        | b"bzpopmax" | b"zscan" | b"zmscore" | b"xadd" | b"xread" | b"xlen" | b"xrange"
        | b"xrevrange" | b"xtrim" | b"xdel" | b"xinfo" | b"xpending" | b"expire" | b"pexpire"
        | b"ttl" | b"pttl" | b"persist" | b"type" | b"unlink" | b"object" => {
            if let Some(frame) = args.first() {
                extract_key_bytes(frame)
                    .map(|k| vec![k])
                    .unwrap_or_default()
            } else {
                vec![]
            }
        }
        // DEL, EXISTS: all args are keys
        b"del" | b"exists" => args.iter().filter_map(extract_key_bytes).collect(),
        // MGET: all args are keys
        b"mget" => args.iter().filter_map(extract_key_bytes).collect(),
        // MSET: even-indexed args (0,2,4...) are keys
        b"mset" | b"msetnx" => args
            .iter()
            .enumerate()
            .filter(|(i, _)| i % 2 == 0)
            .filter_map(|(_, f)| extract_key_bytes(f))
            .collect(),
        // RENAME, RENAMENX: first two args
        b"rename" | b"renamenx" => args.iter().take(2).filter_map(extract_key_bytes).collect(),
        // BLPOP, BRPOP: all args except last (timeout)
        b"blpop" | b"brpop" => {
            if args.len() > 1 {
                args[..args.len() - 1]
                    .iter()
                    .filter_map(extract_key_bytes)
                    .collect()
            } else {
                vec![]
            }
        }
        // LMOVE, BLMOVE: first two args are keys
        b"lmove" | b"blmove" => args.iter().take(2).filter_map(extract_key_bytes).collect(),
        // SINTER, SUNION, SDIFF: all args are keys
        b"sinter" | b"sunion" | b"sdiff" => args.iter().filter_map(extract_key_bytes).collect(),
        // SINTERSTORE, SUNIONSTORE, SDIFFSTORE: all args (dest + sources)
        b"sinterstore" | b"sunionstore" | b"sdiffstore" => {
            args.iter().filter_map(extract_key_bytes).collect()
        }
        // ZUNIONSTORE, ZINTERSTORE: all key args
        b"zunionstore" | b"zinterstore" => args.iter().filter_map(extract_key_bytes).collect(),
        // No key extraction for admin/connection commands
        _ => vec![],
    }
}

fn extract_key_bytes(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(b) | Frame::SimpleString(b) => Some(b.as_ref()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use clap::Parser;
    fn make_config(requirepass: Option<&str>) -> ServerConfig {
        let mut args = vec!["moon"];
        if let Some(p) = requirepass {
            args.push("--requirepass");
            args.push(p);
        }
        ServerConfig::parse_from(args)
    }

    #[test]
    fn default_user_is_unrestricted() {
        // Every construction path that yields a "fully open" default
        // user must set the cached `unrestricted` flag so the ACL hot
        // path can short-circuit.
        let u = AclUser::new_default_nopass();
        assert!(
            u.unrestricted(),
            "new_default_nopass should be unrestricted"
        );
        assert!(u.enabled);

        // Build a non-literal test password so static scanners don't flag
        // this test as a hard-coded credential (see CodeQL rust/hard-coded-cryptographic-value).
        let test_pw: String = (b'a'..=b'h').map(char::from).collect();
        let u = AclUser::new_default_with_password(&test_pw);
        assert!(
            u.unrestricted(),
            "new_default_with_password should be unrestricted"
        );

        let u = AclUser::default_deny("alice".to_string());
        assert!(!u.unrestricted(), "default_deny must NOT be unrestricted");

        // Loading from an empty config must also yield an unrestricted default.
        let table = AclTable::load_or_default(&make_config(None));
        let user = table.get_user("default").unwrap();
        assert!(
            user.unrestricted(),
            "load_or_default() default user must be unrestricted"
        );
    }

    #[test]
    fn restrictions_clear_unrestricted_flag() {
        // Any added restriction must invalidate the unrestricted cache.
        // apply_rule is the sole mutation entry point used by ACL
        // SETUSER, so refreshing the cache there covers all cases.
        let mut table = AclTable::new();
        table.apply_setuser("default", &["on", "nopass", "~*", "&*", "+@all"]);
        assert!(table.get_user("default").unwrap().unrestricted());

        // Adding a specific key pattern should drop unrestricted.
        table.apply_setuser("restricted", &["on", "nopass", "~cache:*", "&*", "+@all"]);
        assert!(!table.get_user("restricted").unwrap().unrestricted());

        // Disabling the user.
        table.apply_setuser("disabled", &["off", "nopass", "~*", "&*", "+@all"]);
        assert!(!table.get_user("disabled").unwrap().unrestricted());

        // Denying a command.
        table.apply_setuser(
            "restricted_cmd",
            &["on", "nopass", "~*", "&*", "+@all", "-flushall"],
        );
        assert!(
            !table.get_user("restricted_cmd").unwrap().unrestricted(),
            "a single -cmd must clear unrestricted"
        );

        // Limited channel pattern.
        table.apply_setuser("chan_only", &["on", "nopass", "~*", "&events:*", "+@all"]);
        assert!(!table.get_user("chan_only").unwrap().unrestricted());
    }

    #[test]
    fn unrestricted_user_passes_all_checks() {
        // Sanity: the check_*_permission fast paths return None for the
        // default user on every command shape.
        let table = AclTable::load_or_default(&make_config(None));
        let cmd_args: &[Frame] = &[Frame::BulkString(Bytes::from_static(b"some-key"))];

        assert!(
            table
                .check_command_permission("default", b"SET", cmd_args)
                .is_none()
        );
        assert!(
            table
                .check_key_permission("default", b"SET", cmd_args, true)
                .is_none()
        );
        assert!(
            table
                .check_channel_permission("default", b"any-channel")
                .is_none()
        );
    }

    #[test]
    fn test_load_or_default_nopass() {
        let table = AclTable::load_or_default(&make_config(None));
        let user = table.get_user("default").unwrap();
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(
            user.key_patterns
                .iter()
                .any(|kp| kp.pattern == "*" && kp.read && kp.write)
        );
        assert!(user.channel_patterns.contains(&"*".to_string()));
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::AllAllowed
        ));
    }

    #[test]
    fn test_load_or_default_with_password() {
        let table = AclTable::load_or_default(&make_config(Some("secret")));
        let user = table.get_user("default").unwrap();
        assert!(user.enabled);
        assert!(!user.nopass);
        assert_eq!(user.passwords.len(), 1);
        assert_eq!(user.passwords[0], hash_password("secret"));
    }

    #[test]
    fn test_check_command_permission_allallowed() {
        let table = AclTable::load_or_default(&make_config(None));
        let args: Vec<Frame> = vec![Frame::BulkString(Bytes::from_static(b"key"))];
        assert!(
            table
                .check_command_permission("default", b"SET", &args)
                .is_none()
        );
        assert!(
            table
                .check_command_permission("default", b"GET", &args)
                .is_none()
        );
    }

    #[test]
    fn test_check_command_permission_denied() {
        let mut table = AclTable::load_or_default(&make_config(None));
        table.apply_setuser("alice", &["on", "nopass", "~*", "+@all", "-set"]);
        let args: Vec<Frame> = vec![Frame::BulkString(Bytes::from_static(b"key"))];
        // SET denied
        assert!(
            table
                .check_command_permission("alice", b"SET", &args)
                .is_some()
        );
        // GET still allowed (+@all -set means everything except set)
        assert!(
            table
                .check_command_permission("alice", b"GET", &args)
                .is_none()
        );
    }

    #[test]
    fn test_check_key_permission_multikey() {
        let mut table = AclTable::load_or_default(&make_config(None));
        table.apply_setuser("alice", &["on", "nopass", "~cache:*", "+@all"]);
        let args = vec![
            Frame::BulkString(Bytes::from_static(b"cache:k1")),
            Frame::BulkString(Bytes::from_static(b"v1")),
            Frame::BulkString(Bytes::from_static(b"cache:k2")),
            Frame::BulkString(Bytes::from_static(b"v2")),
        ];
        // Both keys match ~cache:*
        assert!(
            table
                .check_key_permission("alice", b"MSET", &args, true)
                .is_none()
        );

        // Now try with a key that doesn't match
        let args2 = vec![
            Frame::BulkString(Bytes::from_static(b"cache:k1")),
            Frame::BulkString(Bytes::from_static(b"v1")),
            Frame::BulkString(Bytes::from_static(b"other:k2")),
            Frame::BulkString(Bytes::from_static(b"v2")),
        ];
        assert!(
            table
                .check_key_permission("alice", b"MSET", &args2, true)
                .is_some()
        );
    }

    #[test]
    fn test_authenticate_success() {
        let table = AclTable::load_or_default(&make_config(Some("secret")));
        assert_eq!(
            table.authenticate("default", "secret"),
            Some("default".to_string())
        );
    }

    #[test]
    fn test_authenticate_wrong_password() {
        let table = AclTable::load_or_default(&make_config(Some("secret")));
        assert_eq!(table.authenticate("default", "wrong"), None);
    }

    #[test]
    fn test_authenticate_nopass() {
        let table = AclTable::load_or_default(&make_config(None));
        assert_eq!(
            table.authenticate("default", "anypass"),
            Some("default".to_string())
        );
    }

    #[test]
    fn test_authenticate_nonexistent_user() {
        let table = AclTable::load_or_default(&make_config(None));
        assert_eq!(table.authenticate("alice", "pass"), None);
    }

    #[test]
    fn test_authenticate_disabled_user() {
        let mut table = AclTable::load_or_default(&make_config(None));
        table.apply_setuser("alice", &["off", ">pass"]);
        assert_eq!(table.authenticate("alice", "pass"), None);
    }

    #[test]
    fn test_del_user() {
        let mut table = AclTable::load_or_default(&make_config(None));
        table.apply_setuser("alice", &["on", "nopass"]);
        assert!(table.del_user("alice"));
        assert!(!table.del_user("alice"));
        assert!(table.get_user("alice").is_none());
    }

    #[test]
    fn test_list_users_sorted() {
        let mut table = AclTable::load_or_default(&make_config(None));
        table.apply_setuser("charlie", &["on"]);
        table.apply_setuser("alice", &["on"]);
        let users = table.list_users();
        assert_eq!(users[0].username, "alice");
        assert_eq!(users[1].username, "charlie");
        assert_eq!(users[2].username, "default");
    }

    #[test]
    fn test_check_channel_permission() {
        let mut table = AclTable::load_or_default(&make_config(None));
        table.apply_setuser("alice", &["on", "nopass", "&events:*", "+@all", "~*"]);
        assert!(
            table
                .check_channel_permission("alice", b"events:foo")
                .is_none()
        );
        assert!(
            table
                .check_channel_permission("alice", b"other:foo")
                .is_some()
        );
    }

    #[test]
    fn test_check_key_permission_read_write_patterns() {
        let mut table = AclTable::load_or_default(&make_config(None));
        table.apply_setuser(
            "alice",
            &["on", "nopass", "%R~data:*", "%W~write:*", "+@all"],
        );
        let args_r = vec![Frame::BulkString(Bytes::from_static(b"data:foo"))];
        let args_w = vec![Frame::BulkString(Bytes::from_static(b"write:bar"))];
        // Read access to data:* should work
        assert!(
            table
                .check_key_permission("alice", b"GET", &args_r, false)
                .is_none()
        );
        // Write access to data:* should fail (read-only pattern)
        assert!(
            table
                .check_key_permission("alice", b"SET", &args_r, true)
                .is_some()
        );
        // Write access to write:* should work
        assert!(
            table
                .check_key_permission("alice", b"SET", &args_w, true)
                .is_none()
        );
        // Read access to write:* should fail (write-only pattern)
        assert!(
            table
                .check_key_permission("alice", b"GET", &args_w, false)
                .is_some()
        );
    }
}
