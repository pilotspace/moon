use sha2::{Digest, Sha256};
use std::collections::HashSet;

use super::table::{AclUser, CommandPermissions, KeyPattern};

/// A rule token that Moon refuses to apply.
///
/// Only category resolution reports through this today (#978). The wider ACL
/// token grammar -- unknown *tokens*, `nocommands`, case-insensitive
/// modifiers, validate-then-commit atomicity -- is #970/#979 and will extend
/// this enum rather than add a second error channel.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AclRuleError {
    /// `+@foo` / `-@foo` where `foo` names no category Moon resolves.
    ///
    /// Before #978 this was not an error at all: the category resolved to an
    /// empty command list and `deny_command` turned that into a base-allow
    /// permission set, granting every command.
    #[error("Unknown command or category name in ACL")]
    UnknownCategory(String),
}

impl AclRuleError {
    /// The redis-compatible `ACL SETUSER` error text for `rule`.
    ///
    /// Matches `redis-server` 8.6.1 byte for byte:
    /// `ERR Error in ACL SETUSER modifier '-@bogus': Unknown command or
    /// category name in ACL`.
    pub fn to_setuser_error(&self, rule: &str) -> String {
        format!("ERR Error in ACL SETUSER modifier '{rule}': {self}")
    }
}

pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn verify_password(provided: &str, stored_hash: &str) -> bool {
    hash_password(provided) == stored_hash
}

/// Apply one ACL rule token to `user`.
///
/// Returns `Err` when the token names a category Moon cannot resolve, and in
/// that case `user` is left **untouched** -- `allow_command`/`deny_command`
/// resolve the category before they mutate anything. Callers must surface the
/// error; swallowing it reinstates #978.
pub fn apply_rule(user: &mut AclUser, rule: &str) -> Result<(), AclRuleError> {
    let result = apply_rule_inner(user, rule);
    // Any mutation that could affect the unrestricted fast-path flag
    // must refresh the cached bool. Doing this once at the end of
    // apply_rule covers every field (enabled, allowed_commands,
    // key_patterns, channel_patterns) and every call site. Run it even on
    // the error path: a rejected rule mutates nothing, but recomputing a
    // pure function of the current state can never be wrong, and skipping
    // it on one path is how caches drift.
    user.refresh_unrestricted_cache();
    result
}

fn apply_rule_inner(user: &mut AclUser, rule: &str) -> Result<(), AclRuleError> {
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
                base_allow: false,
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
        _ if rule.starts_with('+') => return user.allow_command(&rule[1..]),
        _ if rule.starts_with('-') => return user.deny_command(&rule[1..]),
        // Unknown *token* (as opposed to unknown category) is still ignored.
        // Rejecting it is #970/#979 (node N7), which owns the token grammar
        // and validate-then-commit atomicity; widening the error surface here
        // would collide with that change.
        _ => {}
    }
    Ok(())
}

/// Every ACL category Moon resolves, paired with its members.
///
/// **ONE table, two consumers.** `ACL CAT` (no args) publishes these names and
/// [`get_category_commands`] resolves exactly these names, so a category can
/// never be published without being resolvable or -- far worse, #978 --
/// resolve to nothing while `ACL SETUSER -@<name>` still answers `OK`.
/// `src/command/acl.rs` used to carry two more hand-maintained copies of the
/// name list; both are gone.
///
/// Membership is derived from a live `redis-server 8.6.1` `ACL CAT <cat>`,
/// restricted to commands Moon implements. Redis classifies per *subcommand*
/// (`acl|setuser`, `config|get`); [`AclTable::check_command_permission`] only
/// ever sees the bare container name, so redis's `foo|sub in C` collapses to
/// bare `foo in C` whenever **at least one** subcommand of `foo` is in `C`.
/// That direction is deliberate: `-@dangerous` then denies the whole
/// container. Requiring *every* subcommand to match instead would leave
/// `ACL`, `CONFIG`, `CLIENT`, `CLUSTER`, `MODULE` and `SLOWLOG` reachable
/// under `-@dangerous`, which is bug #980. The cost is that `+@read` grants a
/// container outright (e.g. all of `OBJECT`); per-subcommand matching is the
/// real fix and is tracked separately.
///
/// The first 21 names are redis 8.6.1's categories verbatim. `search`,
/// `graph` and `cluster` are Moon extensions, and `all` is the wildcard.
static CATEGORY_TABLE: &[(&str, &[&str])] = &[
    // wildcard: every bare command Moon dispatches. Redis rejects
    // `ACL CAT all`; Moon has always accepted it, so it stays resolvable.
    (
        "all",
        &[
            "acl",
            "append",
            "asking",
            "auth",
            "bgrewriteaof",
            "bgsave",
            "bitcount",
            "bitfield",
            "bitfield_ro",
            "bitop",
            "bitpos",
            "blmove",
            "blmpop",
            "blpop",
            "brpop",
            "brpoplpush",
            "bzmpop",
            "bzpopmax",
            "bzpopmin",
            "cdc.read",
            "client",
            "cluster",
            "command",
            "config",
            "copy",
            "dbsize",
            "debug",
            "decr",
            "decrby",
            "del",
            "discard",
            "dump",
            "echo",
            "eval",
            "eval_ro",
            "evalsha",
            "evalsha_ro",
            "exec",
            "exists",
            "expire",
            "expireat",
            "expiretime",
            "fcall",
            "fcall_ro",
            "flushall",
            "flushdb",
            "ft._list",
            "ft.aggregate",
            "ft.cachesearch",
            "ft.compact",
            "ft.config",
            "ft.create",
            "ft.dropindex",
            "ft.expand",
            "ft.info",
            "ft.navigate",
            "ft.recommend",
            "ft.search",
            "function",
            "geoadd",
            "geodist",
            "geohash",
            "geopos",
            "georadius",
            "georadius_ro",
            "georadiusbymember",
            "georadiusbymember_ro",
            "geosearch",
            "geosearchstore",
            "get",
            "getbit",
            "getdel",
            "getex",
            "getrange",
            "getset",
            "graph.addedge",
            "graph.addnode",
            "graph.create",
            "graph.delete",
            "graph.drop",
            "graph.explain",
            "graph.hybrid",
            "graph.info",
            "graph.list",
            "graph.neighbors",
            "graph.profile",
            "graph.query",
            "graph.ro_query",
            "graph.vsearch",
            "hdel",
            "hello",
            "hexists",
            "hexpire",
            "hexpireat",
            "hexpiretime",
            "hget",
            "hgetall",
            "hgetdel",
            "hgetex",
            "hincrby",
            "hincrbyfloat",
            "hkeys",
            "hlen",
            "hmget",
            "hmset",
            "hotkeys",
            "hpersist",
            "hpexpire",
            "hpexpireat",
            "hpexpiretime",
            "hpttl",
            "hrandfield",
            "hscan",
            "hset",
            "hsetnx",
            "hstrlen",
            "httl",
            "hvals",
            "incr",
            "incrby",
            "incrbyfloat",
            "info",
            "keys",
            "kill",
            "lastsave",
            "lcs",
            "lindex",
            "linsert",
            "llen",
            "lmove",
            "lmpop",
            "lolwut",
            "lpop",
            "lpos",
            "lpush",
            "lpushx",
            "lrange",
            "lrem",
            "lset",
            "ltrim",
            "memory",
            "mget",
            "module",
            "monitor",
            "move",
            "mq",
            "mset",
            "msetnx",
            "multi",
            "object",
            "persist",
            "pexpire",
            "pexpireat",
            "pexpiretime",
            "pfadd",
            "pfcount",
            "pfmerge",
            "ping",
            "psetex",
            "psubscribe",
            "psync",
            "pttl",
            "publish",
            "pubsub",
            "punsubscribe",
            "quit",
            "randomkey",
            "readonly",
            "readwrite",
            "rename",
            "renamenx",
            "replconf",
            "replicaof",
            "reset",
            "restore",
            "role",
            "rpop",
            "rpoplpush",
            "rpush",
            "rpushx",
            "sadd",
            "save",
            "scan",
            "scard",
            "script",
            "sdiff",
            "sdiffstore",
            "select",
            "set",
            "setbit",
            "setex",
            "setnx",
            "setrange",
            "shutdown",
            "sinter",
            "sintercard",
            "sinterstore",
            "sismember",
            "slaveof",
            "slowlog",
            "smembers",
            "smismember",
            "smove",
            "sort",
            "sort_ro",
            "spop",
            "spublish",
            "srandmember",
            "srem",
            "sscan",
            "ssubscribe",
            "strlen",
            "subscribe",
            "substr",
            "sunion",
            "sunionstore",
            "sunsubscribe",
            "swapdb",
            "temporal.invalidate",
            "temporal.snapshot_at",
            "time",
            "touch",
            "ttl",
            "txn",
            "type",
            "unlink",
            "unsubscribe",
            "unwatch",
            "vacuum",
            "wait",
            "watch",
            "ws",
            "xack",
            "xadd",
            "xautoclaim",
            "xclaim",
            "xdel",
            "xgroup",
            "xinfo",
            "xlen",
            "xpending",
            "xrange",
            "xread",
            "xreadgroup",
            "xrevrange",
            "xsetid",
            "xtrim",
            "zadd",
            "zcard",
            "zcount",
            "zdiff",
            "zincrby",
            "zinter",
            "zintercard",
            "zinterstore",
            "zlexcount",
            "zmpop",
            "zmscore",
            "zpopmax",
            "zpopmin",
            "zrandmember",
            "zrange",
            "zrangebyscore",
            "zrangestore",
            "zrank",
            "zrem",
            "zrevrange",
            "zrevrangebyscore",
            "zrevrank",
            "zscan",
            "zscore",
            "zunion",
            "zunionstore",
        ],
    ),
    // #980: `getdel`, `getex` and `sort` are NOT here -- all three mutate.
    (
        "read",
        &[
            "bitcount",
            "bitfield_ro",
            "bitpos",
            "cdc.read",
            "dbsize",
            "dump",
            "exists",
            "expiretime",
            "ft._list",
            "ft.aggregate",
            "ft.cachesearch",
            "ft.info",
            "ft.navigate",
            "ft.recommend",
            "ft.search",
            "geodist",
            "geohash",
            "geopos",
            "georadius_ro",
            "georadiusbymember_ro",
            "geosearch",
            "get",
            "getbit",
            "getrange",
            "graph.explain",
            "graph.hybrid",
            "graph.info",
            "graph.list",
            "graph.neighbors",
            "graph.profile",
            "graph.ro_query",
            "graph.vsearch",
            "hexists",
            "hexpiretime",
            "hget",
            "hgetall",
            "hkeys",
            "hlen",
            "hmget",
            "hpexpiretime",
            "hpttl",
            "hrandfield",
            "hscan",
            "hstrlen",
            "httl",
            "hvals",
            "keys",
            "lcs",
            "lindex",
            "llen",
            "lolwut",
            "lpos",
            "lrange",
            "memory",
            "mget",
            "object",
            "pexpiretime",
            "pfcount",
            "pttl",
            "randomkey",
            "scan",
            "scard",
            "sdiff",
            "sinter",
            "sintercard",
            "sismember",
            "smembers",
            "smismember",
            "sort_ro",
            "srandmember",
            "sscan",
            "strlen",
            "substr",
            "sunion",
            "temporal.snapshot_at",
            "touch",
            "ttl",
            "type",
            "xinfo",
            "xlen",
            "xpending",
            "xrange",
            "xread",
            "xrevrange",
            "zcard",
            "zcount",
            "zdiff",
            "zinter",
            "zintercard",
            "zlexcount",
            "zmscore",
            "zrandmember",
            "zrange",
            "zrangebyscore",
            "zrank",
            "zrevrange",
            "zrevrangebyscore",
            "zrevrank",
            "zscan",
            "zscore",
            "zunion",
        ],
    ),
    (
        "write",
        &[
            "append",
            "bitfield",
            "bitop",
            "blmove",
            "blmpop",
            "blpop",
            "brpop",
            "brpoplpush",
            "bzmpop",
            "bzpopmax",
            "bzpopmin",
            "copy",
            "decr",
            "decrby",
            "del",
            "expire",
            "expireat",
            "flushall",
            "flushdb",
            "ft.compact",
            "ft.config",
            "ft.create",
            "ft.dropindex",
            "ft.expand",
            "function",
            "geoadd",
            "georadius",
            "georadiusbymember",
            "geosearchstore",
            "getdel",
            "getex",
            "getset",
            "graph.addedge",
            "graph.addnode",
            "graph.create",
            "graph.delete",
            "graph.drop",
            "graph.query",
            "hdel",
            "hexpire",
            "hexpireat",
            "hgetdel",
            "hgetex",
            "hincrby",
            "hincrbyfloat",
            "hmset",
            "hpersist",
            "hpexpire",
            "hpexpireat",
            "hset",
            "hsetnx",
            "incr",
            "incrby",
            "incrbyfloat",
            "linsert",
            "lmove",
            "lmpop",
            "lpop",
            "lpush",
            "lpushx",
            "lrem",
            "lset",
            "ltrim",
            "move",
            "mq",
            "mset",
            "msetnx",
            "persist",
            "pexpire",
            "pexpireat",
            "pfadd",
            "pfmerge",
            "psetex",
            "rename",
            "renamenx",
            "restore",
            "rpop",
            "rpoplpush",
            "rpush",
            "rpushx",
            "sadd",
            "sdiffstore",
            "set",
            "setbit",
            "setex",
            "setnx",
            "setrange",
            "sinterstore",
            "smove",
            "sort",
            "spop",
            "srem",
            "sunionstore",
            "swapdb",
            "temporal.invalidate",
            "unlink",
            "xack",
            "xadd",
            "xautoclaim",
            "xclaim",
            "xdel",
            "xgroup",
            "xreadgroup",
            "xsetid",
            "xtrim",
            "zadd",
            "zincrby",
            "zinterstore",
            "zmpop",
            "zpopmax",
            "zpopmin",
            "zrangestore",
            "zrem",
            "zunionstore",
        ],
    ),
    (
        "keyspace",
        &[
            "copy",
            "dbsize",
            "del",
            "dump",
            "exists",
            "expire",
            "expireat",
            "expiretime",
            "flushall",
            "flushdb",
            "keys",
            "move",
            "object",
            "persist",
            "pexpire",
            "pexpireat",
            "pexpiretime",
            "pttl",
            "randomkey",
            "rename",
            "renamenx",
            "restore",
            "scan",
            "swapdb",
            "touch",
            "ttl",
            "type",
            "unlink",
            "vacuum",
        ],
    ),
    (
        "string",
        &[
            "append",
            "decr",
            "decrby",
            "get",
            "getdel",
            "getex",
            "getrange",
            "getset",
            "incr",
            "incrby",
            "incrbyfloat",
            "lcs",
            "mget",
            "mset",
            "msetnx",
            "psetex",
            "set",
            "setex",
            "setnx",
            "setrange",
            "strlen",
            "substr",
        ],
    ),
    (
        "bitmap",
        &[
            "bitcount",
            "bitfield",
            "bitfield_ro",
            "bitop",
            "bitpos",
            "getbit",
            "setbit",
        ],
    ),
    ("hyperloglog", &["pfadd", "pfcount", "pfmerge"]),
    (
        "geo",
        &[
            "geoadd",
            "geodist",
            "geohash",
            "geopos",
            "georadius",
            "georadius_ro",
            "georadiusbymember",
            "georadiusbymember_ro",
            "geosearch",
            "geosearchstore",
        ],
    ),
    (
        "hash",
        &[
            "hdel",
            "hexists",
            "hexpire",
            "hexpireat",
            "hexpiretime",
            "hget",
            "hgetall",
            "hgetdel",
            "hgetex",
            "hincrby",
            "hincrbyfloat",
            "hkeys",
            "hlen",
            "hmget",
            "hmset",
            "hpersist",
            "hpexpire",
            "hpexpireat",
            "hpexpiretime",
            "hpttl",
            "hrandfield",
            "hscan",
            "hset",
            "hsetnx",
            "hstrlen",
            "httl",
            "hvals",
        ],
    ),
    (
        "list",
        &[
            "blmove",
            "blmpop",
            "blpop",
            "brpop",
            "brpoplpush",
            "lindex",
            "linsert",
            "llen",
            "lmove",
            "lmpop",
            "lpop",
            "lpos",
            "lpush",
            "lpushx",
            "lrange",
            "lrem",
            "lset",
            "ltrim",
            "rpop",
            "rpoplpush",
            "rpush",
            "rpushx",
            "sort",
            "sort_ro",
        ],
    ),
    (
        "set",
        &[
            "sadd",
            "scard",
            "sdiff",
            "sdiffstore",
            "sinter",
            "sintercard",
            "sinterstore",
            "sismember",
            "smembers",
            "smismember",
            "smove",
            "sort",
            "sort_ro",
            "spop",
            "srandmember",
            "srem",
            "sscan",
            "sunion",
            "sunionstore",
        ],
    ),
    (
        "sortedset",
        &[
            "bzmpop",
            "bzpopmax",
            "bzpopmin",
            "sort",
            "sort_ro",
            "zadd",
            "zcard",
            "zcount",
            "zdiff",
            "zincrby",
            "zinter",
            "zintercard",
            "zinterstore",
            "zlexcount",
            "zmpop",
            "zmscore",
            "zpopmax",
            "zpopmin",
            "zrandmember",
            "zrange",
            "zrangebyscore",
            "zrangestore",
            "zrank",
            "zrem",
            "zrevrange",
            "zrevrangebyscore",
            "zrevrank",
            "zscan",
            "zscore",
            "zunion",
            "zunionstore",
        ],
    ),
    (
        "stream",
        &[
            "xack",
            "xadd",
            "xautoclaim",
            "xclaim",
            "xdel",
            "xgroup",
            "xinfo",
            "xlen",
            "xpending",
            "xrange",
            "xread",
            "xreadgroup",
            "xrevrange",
            "xsetid",
            "xtrim",
        ],
    ),
    (
        "pubsub",
        &[
            "psubscribe",
            "publish",
            "pubsub",
            "punsubscribe",
            "spublish",
            "ssubscribe",
            "subscribe",
            "sunsubscribe",
            "unsubscribe",
        ],
    ),
    (
        "scripting",
        &[
            "eval",
            "eval_ro",
            "evalsha",
            "evalsha_ro",
            "fcall",
            "fcall_ro",
            "function",
            "script",
        ],
    ),
    (
        "transaction",
        // `temporal.*` are Moon's MVCC extensions and belong here with
        // TXN/MULTI. The old table listed a bare `"temporal"`, which is not a
        // command Moon dispatches -- the two real names are
        // `TEMPORAL.SNAPSHOT_AT` and `TEMPORAL.INVALIDATE`, so the intended
        // `-@transaction` carve-out never covered either of them, and the unit
        // test that "proved" it did probed the un-dispatchable bare name.
        &[
            "discard",
            "exec",
            "multi",
            "temporal.invalidate",
            "temporal.snapshot_at",
            "txn",
            "unwatch",
            "watch",
        ],
    ),
    (
        "connection",
        &[
            "asking",
            "auth",
            "client",
            "command",
            "echo",
            "hello",
            "ping",
            "quit",
            "readonly",
            "readwrite",
            "reset",
            "select",
            "wait",
        ],
    ),
    (
        "admin",
        &[
            "acl",
            "bgrewriteaof",
            "bgsave",
            "cdc.read",
            "client",
            "cluster",
            "config",
            "debug",
            "ft.compact",
            "ft.config",
            "hotkeys",
            "kill",
            "lastsave",
            "module",
            "monitor",
            "psync",
            "replconf",
            "replicaof",
            "role",
            "save",
            "shutdown",
            "slaveof",
            "slowlog",
            "vacuum",
            "ws",
        ],
    ),
    // #980: `swapdb`, `info`, `client`, `role`, `shutdown` were missing,
    // so `-@dangerous` left them granted.
    (
        "dangerous",
        &[
            "acl",
            "bgrewriteaof",
            "bgsave",
            "cdc.read",
            "client",
            "cluster",
            "config",
            "debug",
            "flushall",
            "flushdb",
            "ft.config",
            "ft.dropindex",
            "graph.delete",
            "graph.drop",
            "hotkeys",
            "info",
            "keys",
            "kill",
            "lastsave",
            "module",
            "monitor",
            "psync",
            "replconf",
            "replicaof",
            "restore",
            "role",
            "save",
            "shutdown",
            "slaveof",
            "slowlog",
            "sort",
            "sort_ro",
            "swapdb",
            "temporal.invalidate",
            "vacuum",
            "ws",
        ],
    ),
    (
        "fast",
        &[
            "append",
            "asking",
            "auth",
            "bitfield_ro",
            "bzpopmax",
            "bzpopmin",
            "dbsize",
            "decr",
            "decrby",
            "discard",
            "echo",
            "exists",
            "expire",
            "expireat",
            "expiretime",
            "get",
            "getbit",
            "getdel",
            "getex",
            "getset",
            "hdel",
            "hello",
            "hexists",
            "hexpire",
            "hexpireat",
            "hexpiretime",
            "hget",
            "hgetdel",
            "hgetex",
            "hincrby",
            "hincrbyfloat",
            "hlen",
            "hmget",
            "hmset",
            "hpersist",
            "hpexpire",
            "hpexpireat",
            "hpexpiretime",
            "hpttl",
            "hset",
            "hsetnx",
            "hstrlen",
            "httl",
            "incr",
            "incrby",
            "incrbyfloat",
            "lastsave",
            "llen",
            "lolwut",
            "lpop",
            "lpush",
            "lpushx",
            "mget",
            "move",
            "multi",
            "persist",
            "pexpire",
            "pexpireat",
            "pexpiretime",
            "pfadd",
            "ping",
            "pttl",
            "publish",
            "quit",
            "readonly",
            "readwrite",
            "renamenx",
            "reset",
            "role",
            "rpop",
            "rpush",
            "rpushx",
            "sadd",
            "scard",
            "select",
            "setnx",
            "sismember",
            "smismember",
            "smove",
            "spop",
            "spublish",
            "srem",
            "strlen",
            "swapdb",
            "time",
            "touch",
            "ttl",
            "type",
            "unlink",
            "unwatch",
            "watch",
            "xack",
            "xadd",
            "xautoclaim",
            "xclaim",
            "xdel",
            "xlen",
            "xsetid",
            "zadd",
            "zcard",
            "zcount",
            "zincrby",
            "zlexcount",
            "zmscore",
            "zpopmax",
            "zpopmin",
            "zrank",
            "zrem",
            "zrevrank",
            "zscore",
        ],
    ),
    (
        "slow",
        &[
            "bgrewriteaof",
            "bgsave",
            "bitcount",
            "bitfield",
            "bitop",
            "bitpos",
            "blmove",
            "blmpop",
            "blpop",
            "brpop",
            "brpoplpush",
            "bzmpop",
            "cdc.read",
            "command",
            "copy",
            "debug",
            "del",
            "dump",
            "eval",
            "eval_ro",
            "evalsha",
            "evalsha_ro",
            "exec",
            "fcall",
            "fcall_ro",
            "flushall",
            "flushdb",
            "ft._list",
            "ft.aggregate",
            "ft.cachesearch",
            "ft.compact",
            "ft.config",
            "ft.create",
            "ft.dropindex",
            "ft.expand",
            "ft.info",
            "ft.navigate",
            "ft.recommend",
            "ft.search",
            "function",
            "geoadd",
            "geodist",
            "geohash",
            "geopos",
            "georadius",
            "georadius_ro",
            "georadiusbymember",
            "georadiusbymember_ro",
            "geosearch",
            "geosearchstore",
            "getrange",
            "graph.addedge",
            "graph.addnode",
            "graph.create",
            "graph.delete",
            "graph.drop",
            "graph.explain",
            "graph.hybrid",
            "graph.info",
            "graph.list",
            "graph.neighbors",
            "graph.profile",
            "graph.query",
            "graph.ro_query",
            "graph.vsearch",
            "hgetall",
            "hkeys",
            "hrandfield",
            "hscan",
            "hvals",
            "info",
            "keys",
            "kill",
            "lcs",
            "lindex",
            "linsert",
            "lmove",
            "lmpop",
            "lpos",
            "lrange",
            "lrem",
            "lset",
            "ltrim",
            "memory",
            "monitor",
            "mq",
            "mset",
            "msetnx",
            "object",
            "pfcount",
            "pfmerge",
            "psetex",
            "psubscribe",
            "psync",
            "pubsub",
            "punsubscribe",
            "randomkey",
            "rename",
            "replconf",
            "replicaof",
            "restore",
            "rpoplpush",
            "save",
            "scan",
            "script",
            "sdiff",
            "sdiffstore",
            "set",
            "setbit",
            "setex",
            "setrange",
            "shutdown",
            "sinter",
            "sintercard",
            "sinterstore",
            "slaveof",
            "smembers",
            "sort",
            "sort_ro",
            "srandmember",
            "sscan",
            "ssubscribe",
            "subscribe",
            "substr",
            "sunion",
            "sunionstore",
            "sunsubscribe",
            "temporal.invalidate",
            "temporal.snapshot_at",
            "txn",
            "unsubscribe",
            "vacuum",
            "wait",
            "ws",
            "xgroup",
            "xinfo",
            "xpending",
            "xrange",
            "xread",
            "xreadgroup",
            "xrevrange",
            "xtrim",
            "zdiff",
            "zinter",
            "zintercard",
            "zinterstore",
            "zmpop",
            "zrandmember",
            "zrange",
            "zrangebyscore",
            "zrangestore",
            "zrevrange",
            "zrevrangebyscore",
            "zscan",
            "zunion",
            "zunionstore",
        ],
    ),
    (
        "blocking",
        &[
            "blmove",
            "blmpop",
            "blpop",
            "brpop",
            "brpoplpush",
            "bzmpop",
            "bzpopmax",
            "bzpopmin",
            "wait",
            "xread",
            "xreadgroup",
        ],
    ),
    // Moon extension: FT.* vector/text search engine.
    (
        "search",
        &[
            "ft._list",
            "ft.aggregate",
            "ft.cachesearch",
            "ft.compact",
            "ft.config",
            "ft.create",
            "ft.dropindex",
            "ft.expand",
            "ft.info",
            "ft.navigate",
            "ft.recommend",
            "ft.search",
        ],
    ),
    // Moon extension: GRAPH.* Cypher engine.
    (
        "graph",
        &[
            "graph.addedge",
            "graph.addnode",
            "graph.create",
            "graph.delete",
            "graph.drop",
            "graph.explain",
            "graph.hybrid",
            "graph.info",
            "graph.list",
            "graph.neighbors",
            "graph.profile",
            "graph.query",
            "graph.ro_query",
            "graph.vsearch",
        ],
    ),
    // Moon extension: Redis has no @cluster category.
    ("cluster", &["asking", "cluster", "readonly", "readwrite"]),
];

/// Every category name `ACL CAT` publishes and `+@`/`-@` resolves.
pub fn acl_category_names() -> impl Iterator<Item = &'static str> {
    CATEGORY_TABLE.iter().map(|(name, _)| *name)
}

/// Resolves `category` (with or without a leading `@`) to its command names,
/// or `None` when Moon knows no such category.
///
/// # Why `Option`, never an empty slice (#978, CRITICAL)
///
/// This function used to end in `_ => &[]`. An unknown category therefore
/// resolved to "no commands at all", and [`AclUser::deny_command`] walked that
/// empty slice, inserted nothing, and then *unconditionally* rebuilt the
/// permission set as `Specific { base_allow: true, allowed: {}, denied: {} }`
/// -- base-allow with an empty deny set, i.e. **every command permitted**.
/// `is_command_allowed` fell through to `base_allow == true`, while
/// `user_to_acl_line` (which discards `base_allow`) printed the user as
/// `-@all`. So `ACL SETUSER v on >pw ~* &* +@all -@bitmap` answered `OK`, left
/// `SETBIT` and `FLUSHALL` runnable, and reported a user with no permissions.
///
/// Six *real* redis categories reached that arm -- `bitmap`, `hyperloglog`,
/// `geo`, `fast`, `slow`, `blocking` -- as did every non-lowercase spelling of
/// a category Moon did have, because the old `match` compared against
/// lowercase literals while redis category names are case-insensitive.
/// `-@DANGEROUS` was a full grant. Both holes are closed here: the lookup is
/// case-insensitive, and an unresolvable name is an error the caller must
/// handle rather than a silent no-op.
pub fn get_category_commands(category: &str) -> Option<&'static [&'static str]> {
    let cat = category.trim_start_matches('@');
    CATEGORY_TABLE
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(cat))
        .map(|(_, cmds)| *cmds)
}

/// True when `cmd` belongs to `category`. An unknown category is never a
/// match -- see [`get_category_commands`] for why that must not be "matches
/// nothing, silently".
pub fn is_command_in_category(cmd: &str, category: &str) -> bool {
    let cmd_lower = cmd.to_ascii_lowercase();
    get_category_commands(category).is_some_and(|cmds| cmds.contains(&cmd_lower.as_str()))
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
        apply_rule(&mut user, "on").expect("rule must apply");
        assert!(user.enabled);
        apply_rule(&mut user, "off").expect("rule must apply");
        assert!(!user.enabled);
    }

    #[test]
    fn test_apply_rule_nopass() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "nopass").expect("rule must apply");
        assert!(user.nopass);
    }

    #[test]
    fn test_apply_rule_add_password() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, ">secret").expect("rule must apply");
        assert_eq!(user.passwords.len(), 1);
        assert_eq!(user.passwords[0], hash_password("secret"));
        // No duplicates
        apply_rule(&mut user, ">secret").expect("rule must apply");
        assert_eq!(user.passwords.len(), 1);
    }

    #[test]
    fn test_apply_rule_remove_password() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, ">secret").expect("rule must apply");
        assert_eq!(user.passwords.len(), 1);
        apply_rule(&mut user, "<secret").expect("rule must apply");
        assert_eq!(user.passwords.len(), 0);
    }

    #[test]
    fn test_apply_rule_prehashed() {
        let mut user = AclUser::default_deny("test".to_string());
        let hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        apply_rule(&mut user, &format!("#{}", hash)).expect("rule must apply");
        assert_eq!(user.passwords.len(), 1);
        assert_eq!(user.passwords[0], hash);
    }

    #[test]
    fn test_apply_rule_key_patterns() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "~cache:*").expect("rule must apply");
        assert_eq!(user.key_patterns.len(), 1);
        assert_eq!(user.key_patterns[0].pattern, "cache:*");
        assert!(user.key_patterns[0].read);
        assert!(user.key_patterns[0].write);
    }

    #[test]
    fn test_apply_rule_read_write_key_patterns() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "%R~data:*").expect("rule must apply");
        assert_eq!(user.key_patterns.len(), 1);
        assert!(user.key_patterns[0].read);
        assert!(!user.key_patterns[0].write);

        apply_rule(&mut user, "%W~data:*").expect("rule must apply");
        assert_eq!(user.key_patterns.len(), 2);
        assert!(!user.key_patterns[1].read);
        assert!(user.key_patterns[1].write);
    }

    #[test]
    fn test_apply_rule_channel_pattern() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "&events:*").expect("rule must apply");
        assert_eq!(user.channel_patterns, vec!["events:*".to_string()]);
    }

    #[test]
    fn test_apply_rule_commands() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "+@all").expect("rule must apply");
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::AllAllowed
        ));

        apply_rule(&mut user, "-@all").expect("rule must apply");
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::Specific { .. }
        ));

        apply_rule(&mut user, "+get").expect("rule must apply");
        if let CommandPermissions::Specific { allowed, .. } = &user.allowed_commands {
            assert!(allowed.contains("get"));
        }

        apply_rule(&mut user, "-set").expect("rule must apply");
        if let CommandPermissions::Specific { denied, .. } = &user.allowed_commands {
            assert!(denied.contains("set"));
        }
    }

    #[test]
    fn test_apply_rule_resetkeys() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "~*").expect("rule must apply");
        assert_eq!(user.key_patterns.len(), 1);
        apply_rule(&mut user, "resetkeys").expect("rule must apply");
        assert!(user.key_patterns.is_empty());
    }

    #[test]
    fn test_apply_rule_resetchannels() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, "&*").expect("rule must apply");
        assert_eq!(user.channel_patterns.len(), 1);
        apply_rule(&mut user, "resetchannels").expect("rule must apply");
        assert!(user.channel_patterns.is_empty());
    }

    #[test]
    fn test_apply_rule_resetpass() {
        let mut user = AclUser::default_deny("test".to_string());
        apply_rule(&mut user, ">pass1").expect("rule must apply");
        apply_rule(&mut user, "nopass").expect("rule must apply");
        assert!(user.nopass);
        apply_rule(&mut user, "resetpass").expect("rule must apply");
        assert!(user.passwords.is_empty());
        assert!(!user.nopass);
    }

    #[test]
    fn test_apply_rule_reset() {
        let mut user = AclUser::new_default_nopass();
        apply_rule(&mut user, "reset").expect("rule must apply");
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
        let cmds = get_category_commands("@string").expect("@string resolves");
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
        let cmds = get_category_commands("@all").expect("@all resolves");
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

    // ---------------------------------------------------------------
    // #978 -- an unknown category must be an ERROR, never an empty set.
    // #980 -- category MEMBERSHIP must not grant mutating commands.
    //
    // Every assertion below was checked against a live `redis-server
    // 8.6.1` before it was written. The membership table is derived from
    // that server's `ACL CAT`, so these guard the derivation as much as
    // the resolution path.
    // ---------------------------------------------------------------

    /// The sink that caused #978. `_ => &[]` made every unknown category
    /// resolve to "no commands", which `deny_command` turned into a
    /// base-allow permission set granting everything.
    #[test]
    fn test_unknown_category_is_an_error_not_an_empty_set() {
        assert!(get_category_commands("@bogusnope").is_none());
        assert!(get_category_commands("bogusnope").is_none());
        // ...and every category that IS known resolves to a NON-empty set.
        // An empty set would reproduce #978's shape through a name that
        // passes validation.
        for name in acl_category_names() {
            let cmds = get_category_commands(name)
                .unwrap_or_else(|| panic!("published category @{name} does not resolve"));
            assert!(
                !cmds.is_empty(),
                "@{name} resolves to an EMPTY set -- that is the #978 shape"
            );
        }
    }

    /// The six real redis categories that fell through to `_ => &[]`.
    #[test]
    fn test_six_missing_redis_categories_now_resolve() {
        for cat in ["bitmap", "hyperloglog", "geo", "fast", "slow", "blocking"] {
            assert!(
                get_category_commands(cat).is_some(),
                "@{cat} is a real redis category and must resolve"
            );
        }
        let bitmap = get_category_commands("@bitmap").expect("@bitmap resolves");
        assert!(bitmap.contains(&"setbit"));
        assert!(bitmap.contains(&"bitcount"));
    }

    /// Redis category names are case-insensitive. Moon matched lowercase
    /// literals, so `-@DANGEROUS` took the `_ => &[]` arm -- a full grant.
    #[test]
    fn test_category_lookup_is_case_insensitive() {
        let lower = get_category_commands("@dangerous").expect("@dangerous");
        for spelling in ["@DANGEROUS", "@Dangerous", "DANGEROUS", "dAnGeRoUs"] {
            let got = get_category_commands(spelling)
                .unwrap_or_else(|| panic!("{spelling} must resolve like @dangerous"));
            assert_eq!(got, lower, "{spelling} resolved to a different set");
        }
    }

    /// #980: `+@read` granted GETDEL, which DELETED the key. GETEX and
    /// SORT (which has a STORE clause) were granted the same way.
    #[test]
    fn test_read_category_contains_no_mutating_command() {
        let read = get_category_commands("@read").expect("@read");
        for mutator in ["getdel", "getex", "sort", "set", "del", "flushall"] {
            assert!(
                !read.contains(&mutator),
                "@read must not contain the mutating command {mutator}"
            );
        }
        // ...while still containing the obvious read-only ones.
        for reader in ["get", "mget", "strlen", "exists", "ttl", "sort_ro"] {
            assert!(read.contains(&reader), "@read must contain {reader}");
        }
    }

    /// #980: `-@dangerous` left SWAPDB, CLIENT LIST and INFO granted.
    #[test]
    fn test_dangerous_category_covers_what_redis_covers() {
        let dangerous = get_category_commands("@dangerous").expect("@dangerous");
        for cmd in [
            "swapdb",
            "info",
            "client",
            "keys",
            "sort",
            "flushall",
            "flushdb",
            "shutdown",
            "monitor",
            "debug",
            "config",
            "acl",
            "cluster",
            "replicaof",
            "restore",
        ] {
            assert!(
                dangerous.contains(&cmd),
                "@dangerous must contain {cmd} -- otherwise -@dangerous leaves it granted"
            );
        }
    }

    /// Moon-only families must be classified EXPLICITLY. Leaving them out
    /// recreates the `_ => &[]` shape one level down: `-@dangerous` would
    /// not touch `WS`, and `+@read` would not grant `FT.SEARCH`.
    #[test]
    fn test_moon_only_families_are_categorised() {
        let read = get_category_commands("@read").expect("@read");
        let write = get_category_commands("@write").expect("@write");
        let dangerous = get_category_commands("@dangerous").expect("@dangerous");
        let admin = get_category_commands("@admin").expect("@admin");

        assert!(read.contains(&"ft.search"));
        assert!(read.contains(&"graph.ro_query"));
        assert!(write.contains(&"ft.create"));
        assert!(write.contains(&"graph.query"));
        assert!(write.contains(&"mq"));
        assert!(dangerous.contains(&"ws"));
        assert!(dangerous.contains(&"cdc.read"));
        assert!(dangerous.contains(&"graph.drop"));
        assert!(admin.contains(&"ws"));
        assert!(admin.contains(&"vacuum"));

        // The Moon-extension categories exist and are non-empty.
        assert!(
            get_category_commands("@search")
                .expect("@search")
                .contains(&"ft.search")
        );
        assert!(
            get_category_commands("@graph")
                .expect("@graph")
                .contains(&"graph.query")
        );
    }

    /// `ACL CAT` publishes exactly what `+@`/`-@` resolves. Before #978
    /// these were three separate hand-maintained lists.
    #[test]
    fn test_published_names_are_exactly_the_resolvable_names() {
        let published: Vec<&str> = acl_category_names().collect();
        assert!(!published.is_empty());
        for name in &published {
            assert!(get_category_commands(name).is_some());
        }
        // All 21 of redis 8.6.1's categories are published.
        for cat in [
            "admin",
            "bitmap",
            "blocking",
            "connection",
            "dangerous",
            "fast",
            "geo",
            "hash",
            "hyperloglog",
            "keyspace",
            "list",
            "pubsub",
            "read",
            "scripting",
            "set",
            "slow",
            "sortedset",
            "stream",
            "string",
            "transaction",
            "write",
        ] {
            assert!(
                published.contains(&cat),
                "redis category @{cat} is not published"
            );
        }
    }

    /// The escalation, end to end, at the rule layer: `+@all` then a deny
    /// of an unknown category used to leave every command runnable while
    /// `ACL LIST` printed `-@all`.
    #[test]
    fn test_deny_unknown_category_leaves_user_untouched() {
        let mut user = AclUser::default_deny("v".to_string());
        apply_rule(&mut user, "on").expect("rule must apply");
        apply_rule(&mut user, "~*").expect("rule must apply");
        apply_rule(&mut user, "&*").expect("rule must apply");
        apply_rule(&mut user, "+@all").expect("rule must apply");

        let err = apply_rule(&mut user, "-@bogusnope").expect_err("unknown category must fail");
        assert_eq!(err, AclRuleError::UnknownCategory("bogusnope".to_string()));
        // Rejected rule mutated NOTHING: still AllAllowed, still unrestricted.
        assert!(matches!(
            user.allowed_commands,
            CommandPermissions::AllAllowed
        ));
        assert!(
            user.unrestricted(),
            "a rejected rule must not demote the user"
        );

        // And the known category it was mistaken for really does deny.
        apply_rule(&mut user, "-@bitmap").expect("rule must apply");
        assert!(
            !user.is_command_allowed("setbit"),
            "-@bitmap must deny SETBIT"
        );
        assert!(user.is_command_allowed("get"), "-@bitmap must not deny GET");
    }

    /// The `+` direction of the same hole.
    #[test]
    fn test_allow_unknown_category_leaves_user_untouched() {
        let mut user = AclUser::default_deny("v".to_string());
        apply_rule(&mut user, "on").expect("rule must apply");
        let err = apply_rule(&mut user, "+@bogusnope").expect_err("unknown category must fail");
        assert_eq!(err, AclRuleError::UnknownCategory("bogusnope".to_string()));
        assert!(!user.is_command_allowed("get"));
        assert!(!user.is_command_allowed("set"));
    }

    /// Redis's exact `ACL SETUSER` rejection text, transcribed from
    /// redis-server 8.6.1.
    #[test]
    fn test_setuser_error_text_matches_redis() {
        let err = AclRuleError::UnknownCategory("bogusnope".to_string());
        assert_eq!(
            err.to_setuser_error("-@bogusnope"),
            "ERR Error in ACL SETUSER modifier '-@bogusnope': \
             Unknown command or category name in ACL"
        );
    }
}
