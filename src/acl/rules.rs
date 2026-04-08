use sha2::{Digest, Sha256};
use std::collections::HashSet;

use super::table::{AclUser, CommandPermissions, KeyPattern};

pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn verify_password(provided: &str, stored_hash: &str) -> bool {
    hash_password(provided) == stored_hash
}

pub fn apply_rule(user: &mut AclUser, rule: &str) {
    apply_rule_inner(user, rule);
    // Any mutation that could affect the unrestricted fast-path flag
    // must refresh the cached bool. Doing this once at the end of
    // apply_rule covers every field (enabled, allowed_commands,
    // key_patterns, channel_patterns) and every call site.
    user.refresh_unrestricted_cache();
}

fn apply_rule_inner(user: &mut AclUser, rule: &str) {
    match rule {
        "on" => user.enabled = true,
        "off" => user.enabled = false,
        "nopass" => user.nopass = true,
        "resetpass" => {
            user.passwords.clear();
            user.nopass = false;
        }
        "resetkeys" => user.key_patterns.clear(),
        "resetchannels" => user.channel_patterns.clear(),
        "reset" => {
            user.enabled = false;
            user.passwords.clear();
            user.nopass = false;
            user.key_patterns.clear();
            user.channel_patterns.clear();
            user.allowed_commands = CommandPermissions::Specific {
                allowed: HashSet::new(),
                denied: HashSet::new(),
            };
        }
        _ if rule.starts_with('>') => {
            let hash = hash_password(&rule[1..]);
            if !user.passwords.contains(&hash) {
                user.passwords.push(hash);
            }
        }
        _ if rule.starts_with('<') => {
            let hash = hash_password(&rule[1..]);
            user.passwords.retain(|p| p != &hash);
        }
        _ if rule.starts_with('#') => {
            let hash = rule[1..].to_string();
            if !user.passwords.contains(&hash) {
                user.passwords.push(hash);
            }
        }
        _ if rule.starts_with('!') => {
            let hash = rule[1..].to_string();
            user.passwords.retain(|p| p != &hash);
        }
        _ if rule.starts_with("%R~") => {
            user.key_patterns.push(KeyPattern {
                pattern: rule[3..].to_string(),
                read: true,
                write: false,
            });
        }
        _ if rule.starts_with("%W~") => {
            user.key_patterns.push(KeyPattern {
                pattern: rule[3..].to_string(),
                read: false,
                write: true,
            });
        }
        _ if rule.starts_with('~') => {
            user.key_patterns.push(KeyPattern {
                pattern: rule[1..].to_string(),
                read: true,
                write: true,
            });
        }
        _ if rule.starts_with('&') => {
            user.channel_patterns.push(rule[1..].to_string());
        }
        _ if rule.starts_with('+') => user.allow_command(&rule[1..]),
        _ if rule.starts_with('-') => user.deny_command(&rule[1..]),
        _ => {} // unknown rule: ignore
    }
}

/// Returns all command names belonging to the given category (without @).
/// Category string may or may not include the @ prefix.
pub fn get_category_commands(category: &str) -> &'static [&'static str] {
    let cat = category.trim_start_matches('@');
    match cat {
        "all" => &[
            "get",
            "set",
            "mget",
            "mset",
            "incr",
            "decr",
            "incrby",
            "decrby",
            "incrbyfloat",
            "append",
            "strlen",
            "setnx",
            "setex",
            "psetex",
            "getset",
            "getdel",
            "getex",
            "hget",
            "hset",
            "hdel",
            "hmget",
            "hmset",
            "hgetall",
            "hkeys",
            "hvals",
            "hlen",
            "hexists",
            "hincrby",
            "hincrbyfloat",
            "hscan",
            "lpush",
            "rpush",
            "lpop",
            "rpop",
            "lrange",
            "llen",
            "linsert",
            "lindex",
            "lset",
            "ltrim",
            "lpos",
            "lmove",
            "blpop",
            "brpop",
            "blmove",
            "sadd",
            "srem",
            "smembers",
            "sinter",
            "sunion",
            "sdiff",
            "sinterstore",
            "sunionstore",
            "sdiffstore",
            "sismember",
            "smismember",
            "scard",
            "srandmember",
            "spop",
            "sscan",
            "smove",
            "zadd",
            "zrem",
            "zscore",
            "zrange",
            "zrangebyscore",
            "zrangebylex",
            "zrevrange",
            "zrevrangebyscore",
            "zrevrangebylex",
            "zrank",
            "zrevrank",
            "zcard",
            "zincrby",
            "zcount",
            "zlexcount",
            "zunionstore",
            "zinterstore",
            "zpopmin",
            "zpopmax",
            "bzpopmin",
            "bzpopmax",
            "zscan",
            "zrangestore",
            "zmscore",
            "xadd",
            "xread",
            "xgroup",
            "xreadgroup",
            "xack",
            "xlen",
            "xrange",
            "xrevrange",
            "xtrim",
            "xdel",
            "xinfo",
            "xpending",
            "xclaim",
            "xautoclaim",
            "subscribe",
            "unsubscribe",
            "psubscribe",
            "punsubscribe",
            "publish",
            "pubsub",
            "del",
            "exists",
            "expire",
            "pexpire",
            "ttl",
            "pttl",
            "persist",
            "type",
            "unlink",
            "scan",
            "keys",
            "rename",
            "renamenx",
            "object",
            "wait",
            "sort",
            "dump",
            "restore",
            "migrate",
            "move",
            "copy",
            "auth",
            "hello",
            "ping",
            "quit",
            "select",
            "command",
            "info",
            "client",
            "reset",
            "multi",
            "exec",
            "discard",
            "watch",
            "unwatch",
            "eval",
            "evalsha",
            "script",
            "cluster",
            "asking",
            "readonly",
            "readwrite",
            "config",
            "debug",
            "bgsave",
            "bgrewriteaof",
            "save",
            "lastsave",
            "flushdb",
            "flushall",
            "dbsize",
            "acl",
            "slowlog",
            "time",
            "memory",
            "replicaof",
            "psync",
            "replconf",
            "failover",
        ],
        "read" => &[
            "get",
            "mget",
            "strlen",
            "getex",
            "getdel",
            "hget",
            "hmget",
            "hgetall",
            "hkeys",
            "hvals",
            "hlen",
            "hexists",
            "hscan",
            "lrange",
            "llen",
            "lindex",
            "lpos",
            "smembers",
            "sinter",
            "sunion",
            "sdiff",
            "sismember",
            "smismember",
            "scard",
            "srandmember",
            "sscan",
            "zscore",
            "zrange",
            "zrangebyscore",
            "zrangebylex",
            "zrevrange",
            "zrevrangebyscore",
            "zrevrangebylex",
            "zrank",
            "zrevrank",
            "zcard",
            "zcount",
            "zlexcount",
            "zscan",
            "zmscore",
            "xread",
            "xlen",
            "xrange",
            "xrevrange",
            "xinfo",
            "xpending",
            "exists",
            "ttl",
            "pttl",
            "type",
            "object",
            "scan",
            "keys",
            "sort",
            "dump",
        ],
        "write" => &[
            "set",
            "mset",
            "incr",
            "decr",
            "incrby",
            "decrby",
            "incrbyfloat",
            "append",
            "setnx",
            "setex",
            "psetex",
            "getset",
            "getdel",
            "getex",
            "hset",
            "hdel",
            "hmset",
            "hincrby",
            "hincrbyfloat",
            "lpush",
            "rpush",
            "lpop",
            "rpop",
            "linsert",
            "lset",
            "ltrim",
            "lmove",
            "sadd",
            "srem",
            "sinterstore",
            "sunionstore",
            "sdiffstore",
            "spop",
            "smove",
            "zadd",
            "zrem",
            "zincrby",
            "zunionstore",
            "zinterstore",
            "zpopmin",
            "zpopmax",
            "zrangestore",
            "xadd",
            "xgroup",
            "xreadgroup",
            "xack",
            "xtrim",
            "xdel",
            "xclaim",
            "xautoclaim",
            "del",
            "expire",
            "pexpire",
            "persist",
            "unlink",
            "rename",
            "renamenx",
            "restore",
            "copy",
            "move",
            "sort",
        ],
        "string" => &[
            "get",
            "set",
            "mget",
            "mset",
            "incr",
            "decr",
            "incrby",
            "decrby",
            "incrbyfloat",
            "append",
            "strlen",
            "setnx",
            "setex",
            "psetex",
            "getset",
            "getdel",
            "getex",
        ],
        "hash" => &[
            "hget",
            "hset",
            "hdel",
            "hmget",
            "hmset",
            "hgetall",
            "hkeys",
            "hvals",
            "hlen",
            "hexists",
            "hincrby",
            "hincrbyfloat",
            "hscan",
        ],
        "list" => &[
            "lpush", "rpush", "lpop", "rpop", "lrange", "llen", "linsert", "lindex", "lset",
            "ltrim", "lpos", "lmove", "blpop", "brpop", "blmove",
        ],
        "set" => &[
            "sadd",
            "srem",
            "smembers",
            "sinter",
            "sunion",
            "sdiff",
            "sinterstore",
            "sunionstore",
            "sdiffstore",
            "sismember",
            "smismember",
            "scard",
            "srandmember",
            "spop",
            "sscan",
            "smove",
        ],
        "sortedset" => &[
            "zadd",
            "zrem",
            "zscore",
            "zrange",
            "zrangebyscore",
            "zrangebylex",
            "zrevrange",
            "zrevrangebyscore",
            "zrevrangebylex",
            "zrank",
            "zrevrank",
            "zcard",
            "zincrby",
            "zcount",
            "zlexcount",
            "zunionstore",
            "zinterstore",
            "zpopmin",
            "zpopmax",
            "bzpopmin",
            "bzpopmax",
            "zscan",
            "zrangestore",
            "zmscore",
        ],
        "stream" => &[
            "xadd",
            "xread",
            "xgroup",
            "xreadgroup",
            "xack",
            "xlen",
            "xrange",
            "xrevrange",
            "xtrim",
            "xdel",
            "xinfo",
            "xpending",
            "xclaim",
            "xautoclaim",
        ],
        "pubsub" => &[
            "subscribe",
            "unsubscribe",
            "psubscribe",
            "punsubscribe",
            "publish",
            "pubsub",
        ],
        "admin" => &[
            "config",
            "info",
            "debug",
            "bgsave",
            "bgrewriteaof",
            "save",
            "lastsave",
            "acl",
            "slowlog",
            "time",
            "memory",
            "replicaof",
            "psync",
            "replconf",
            "failover",
            "cluster",
            "dbsize",
        ],
        "dangerous" => &[
            "flushdb",
            "flushall",
            "debug",
            "config",
            "acl",
            "keys",
            "sort",
            "object",
            "migrate",
            "replicaof",
            "failover",
        ],
        "keyspace" => &[
            "del", "exists", "expire", "pexpire", "ttl", "pttl", "persist", "type", "unlink",
            "scan", "keys", "rename", "renamenx", "object", "wait", "sort", "dump", "restore",
            "migrate", "move", "copy", "dbsize",
        ],
        "connection" => &[
            "auth", "hello", "ping", "quit", "select", "command", "client", "reset",
        ],
        "transaction" => &["multi", "exec", "discard", "watch", "unwatch"],
        "scripting" => &["eval", "evalsha", "script"],
        "cluster" => &["cluster", "asking", "readonly", "readwrite"],
        _ => &[],
    }
}

pub fn is_command_in_category(cmd: &str, category: &str) -> bool {
    let cmd_lower = cmd.to_ascii_lowercase();
    get_category_commands(category).contains(&cmd_lower.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_password_sha256() {
        let hash = hash_password("password");
        assert_eq!(hash.len(), 64); // 256 bits = 64 hex chars
        // Verify it's lowercase hex
        assert!(
            hash.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        // Known SHA256 of "password"
        assert_eq!(
            hash,
            "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
        );
    }

    #[test]
    fn test_verify_password() {
        let hash = hash_password("secret");
        assert!(verify_password("secret", &hash));
        assert!(!verify_password("wrong", &hash));
    }

    #[test]
    fn test_apply_rule_on_off() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "on");
        assert!(user.enabled);
        apply_rule(&mut user, "off");
        assert!(!user.enabled);
    }

    #[test]
    fn test_apply_rule_nopass() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "nopass");
        assert!(user.nopass);
    }

    #[test]
    fn test_apply_rule_add_password() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, ">secret");
        assert_eq!(user.passwords.len(), 1);
        assert_eq!(user.passwords[0], hash_password("secret"));
        // No duplicates
        apply_rule(&mut user, ">secret");
        assert_eq!(user.passwords.len(), 1);
    }

    #[test]
    fn test_apply_rule_remove_password() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, ">secret");
        assert_eq!(user.passwords.len(), 1);
        apply_rule(&mut user, "<secret");
        assert_eq!(user.passwords.len(), 0);
    }

    #[test]
    fn test_apply_rule_prehashed() {
        let mut user = AclUser::default_deny("test".to_string());
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        apply_rule(&mut user, &format!("#{}", hash));
        assert_eq!(user.passwords.len(), 1);
        assert_eq!(user.passwords[0], hash);
    }

    #[test]
    fn test_apply_rule_key_patterns() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "~cache:*");
        assert_eq!(user.key_patterns.len(), 1);
        assert_eq!(user.key_patterns[0].pattern, "cache:*");
        assert!(user.key_patterns[0].read);
        assert!(user.key_patterns[0].write);
    }

    #[test]
    fn test_apply_rule_read_write_key_patterns() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "%R~data:*");
        assert_eq!(user.key_patterns.len(), 1);
        assert!(user.key_patterns[0].read);
        assert!(!user.key_patterns[0].write);

        apply_rule(&mut user, "%W~data:*");
        assert_eq!(user.key_patterns.len(), 2);
        assert!(!user.key_patterns[1].read);
        assert!(user.key_patterns[1].write);
    }

    #[test]
    fn test_apply_rule_channel_pattern() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "&events:*");
        assert_eq!(user.channel_patterns, vec!["events:*".to_string()]);
    }

    #[test]
    fn test_apply_rule_commands() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "+@all");
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::AllAllowed
        ));

        apply_rule(&mut user, "-@all");
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::Specific { .. }
        ));

        apply_rule(&mut user, "+get");
        if let CommandPermissions::Specific { allowed, .. } = &user.allowed_commands {
            assert!(allowed.contains("get"));
        }

        apply_rule(&mut user, "-set");
        if let CommandPermissions::Specific { denied, .. } = &user.allowed_commands {
            assert!(denied.contains("set"));
        }
    }

    #[test]
    fn test_apply_rule_resetkeys() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "~*");
        assert_eq!(user.key_patterns.len(), 1);
        apply_rule(&mut user, "resetkeys");
        assert!(user.key_patterns.is_empty());
    }

    #[test]
    fn test_apply_rule_resetchannels() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "&*");
        assert_eq!(user.channel_patterns.len(), 1);
        apply_rule(&mut user, "resetchannels");
        assert!(user.channel_patterns.is_empty());
    }

    #[test]
    fn test_apply_rule_resetpass() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, ">pass1");
        apply_rule(&mut user, "nopass");
        assert!(user.nopass);
        apply_rule(&mut user, "resetpass");
        assert!(user.passwords.is_empty());
        assert!(!user.nopass);
    }

    #[test]
    fn test_apply_rule_reset() {
        let mut user = AclUser::new_default_nopass();
        apply_rule(&mut user, "reset");
        assert!(!user.enabled);
        assert!(user.passwords.is_empty());
        assert!(!user.nopass);
        assert!(user.key_patterns.is_empty());
        assert!(user.channel_patterns.is_empty());
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::Specific { .. }
        ));
    }

    #[test]
    fn test_get_category_commands_string() {
        let cmds = get_category_commands("@string");
        assert!(cmds.contains(&"get"));
        assert!(cmds.contains(&"set"));
        assert!(cmds.contains(&"mget"));
        assert!(cmds.contains(&"mset"));
        assert!(cmds.contains(&"incr"));
        assert!(cmds.contains(&"decr"));
        assert!(cmds.contains(&"append"));
        assert!(cmds.contains(&"strlen"));
    }

    #[test]
    fn test_get_category_commands_all() {
        let cmds = get_category_commands("@all");
        assert!(!cmds.is_empty());
        assert!(cmds.contains(&"get"));
        assert!(cmds.contains(&"set"));
        assert!(cmds.contains(&"hget"));
        assert!(cmds.contains(&"lpush"));
        assert!(cmds.contains(&"sadd"));
        assert!(cmds.contains(&"zadd"));
    }

    #[test]
    fn test_is_command_in_category() {
        assert!(is_command_in_category("GET", "string"));
        assert!(is_command_in_category("get", "string"));
        assert!(!is_command_in_category("hget", "string"));
    }
}
