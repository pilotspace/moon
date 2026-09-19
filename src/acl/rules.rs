use sha2::{Digest, Sha256};
use std::collections::HashSet;

use super::table::{AclUser, CommandPermissions, KeyPattern};

/// A rule token that Moon refuses to apply.
///
/// Every variant's `Display` is the text `redis-server` 8.6.1 puts after
/// `ERR Error in ACL SETUSER modifier '<rule>': ` for the same input, so
/// [`AclRuleError::to_setuser_error`] is byte-identical to the oracle.
/// `SelectorsUnsupported` is the one exception: redis accepts selectors and
/// Moon does not implement them, so there is no oracle text to match.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AclRuleError {
    /// `+@foo` / `-@foo` where `foo` names no category Moon resolves.
    ///
    /// Before #978 this was not an error at all: the category resolved to an
    /// empty command list and `deny_command` turned that into a base-allow
    /// permission set, granting every command.
    #[error("Unknown command or category name in ACL")]
    UnknownCategory(String),
    /// `+foo` / `-foo` / `+foo|bar` where `foo` is no command Moon dispatches,
    /// or `bar` is no subcommand of a container that has a subcommand table.
    ///
    /// Before #979 this was accepted: `-flushal` (typo) answered `OK` and the
    /// user kept `FLUSHALL`. Redis validates the name and uses the same text
    /// as for an unknown category.
    #[error("Unknown command or category name in ACL")]
    UnknownCommand(String),
    /// A token that matches no keyword and starts with no rule prefix, or a
    /// prefixed token whose shape is malformed (`%X~k`, `%`, `+get|`).
    ///
    /// Before #979 every such token fell into `_ => {}` and was dropped with
    /// `OK`. That is how `nocommands`, `OFF`, `RESET` and every uppercase
    /// spelling "succeeded" while leaving a privileged user untouched.
    #[error("Syntax error")]
    Syntax,
    /// `#hash` / `!hash` whose payload is not 64 lowercase hex characters.
    ///
    /// Moon stores hashes verbatim and compares them against
    /// [`hash_password`]'s lowercase hex, so an uppercase or short hash used
    /// to be accepted and then never match: the account silently could not
    /// log in.
    #[error(
        "The password hash must be exactly 64 characters and contain only lowercase hexadecimal characters"
    )]
    BadPasswordHash,
    /// `<pw` / `!hash` naming a password the user does not hold.
    #[error("The password you are trying to remove from the user does not exist")]
    NoSuchPassword,
    /// A `(...)` selector. Redis accepts these; Moon has no selector support
    /// and refuses rather than silently dropping a grant the operator asked
    /// for.
    #[error("ACL selectors are not supported")]
    SelectorsUnsupported,
    /// A rule argument that is not valid UTF-8. Redis accepts binary rule
    /// bytes (a binary `>password`, say); Moon's user record stores rules as
    /// `String` and cannot represent one. Before #970's fix `ACL SETUSER`
    /// silently DROPPED such an argument and still answered `OK` -- the same
    /// "unknown means ignored" hole as the `_ => {}` arm, one layer up.
    #[error("ACL rules must be valid UTF-8")]
    NotUtf8,
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

/// A rule token with no payload. Matched **case-insensitively**, the way
/// redis's `ACLSetUser` compares them with `strcasecmp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keyword {
    On,
    Off,
    Nopass,
    Resetpass,
    Resetkeys,
    Resetchannels,
    Reset,
    Allkeys,
    Allchannels,
    Allcommands,
    Nocommands,
    /// Accepted as a no-op: Moon does not sanitize RESTORE payloads per user,
    /// and every `ACL LIST` line redis emits carries this token, so refusing
    /// it would make a redis-exported ACL file unloadable.
    SanitizePayload,
    /// Accepted as a no-op, see [`Keyword::SanitizePayload`].
    SkipSanitizePayload,
    /// Accepted as a no-op: Moon has no selectors, so there is nothing to
    /// clear.
    Clearselectors,
}

/// The ONE place a keyword's spelling is compared. `OFF`, `Off` and `off`
/// all resolve here; nothing downstream sees the operator's casing.
const KEYWORDS: &[(&str, Keyword)] = &[
    ("on", Keyword::On),
    ("off", Keyword::Off),
    ("nopass", Keyword::Nopass),
    ("resetpass", Keyword::Resetpass),
    ("resetkeys", Keyword::Resetkeys),
    ("resetchannels", Keyword::Resetchannels),
    ("reset", Keyword::Reset),
    ("allkeys", Keyword::Allkeys),
    ("allchannels", Keyword::Allchannels),
    ("allcommands", Keyword::Allcommands),
    ("nocommands", Keyword::Nocommands),
    ("sanitize-payload", Keyword::SanitizePayload),
    ("skip-sanitize-payload", Keyword::SkipSanitizePayload),
    ("clearselectors", Keyword::Clearselectors),
];

/// One parsed `ACL SETUSER` rule token.
///
/// Prefixed forms keep their payload **verbatim**: passwords, key patterns
/// and channel patterns are case-sensitive in redis too. Only the prefix
/// itself (`%R~` vs `%r~`) is case-folded, inside [`parse_rule`].
#[derive(Debug, PartialEq, Eq)]
enum Rule<'a> {
    /// The empty string. Redis: "Empty string is a no-operation."
    Noop,
    Keyword(Keyword),
    /// `>password`
    AddPassword(&'a str),
    /// `<password`
    RemovePassword(&'a str),
    /// `#hash`, already validated as 64 lowercase hex.
    AddHash(&'a str),
    /// `!hash`, already validated as 64 lowercase hex.
    RemoveHash(&'a str),
    /// `~pattern`, `%R~pattern`, `%W~pattern`, `%RW~pattern`.
    KeyPattern {
        pattern: &'a str,
        read: bool,
        write: bool,
    },
    /// `&pattern`
    Channel(&'a str),
    /// `+cmd`, `+cmd|sub`, `+@category`. Category resolution happens in
    /// [`AclUser::allow_command`]; the command name was validated here.
    Allow(&'a str),
    /// `-cmd`, `-cmd|sub`, `-@category`.
    Deny(&'a str),
}

/// Tokenize one rule. Stateless: everything that can be rejected without
/// looking at the user is rejected here, so a caller that wants to
/// pre-validate a rule list can do so without cloning anything.
fn parse_rule(rule: &str) -> Result<Rule<'_>, AclRuleError> {
    let Some(prefix) = rule.chars().next() else {
        return Ok(Rule::Noop);
    };
    if let Some((_, kw)) = KEYWORDS
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(rule))
    {
        return Ok(Rule::Keyword(*kw));
    }
    // Slice after the first CHARACTER, not the first byte. The rule is
    // attacker-shaped text (`ACL SETUSER`, an aclfile line): `&rule[1..]` on
    // `éx` split a multi-byte char, panicked the shard thread and aborted the
    // whole server. `len_utf8` is always a char boundary; every real prefix is
    // one ASCII byte, and any other first char falls to `Syntax` below.
    let payload = &rule[prefix.len_utf8()..];
    match prefix {
        '>' => Ok(Rule::AddPassword(payload)),
        '<' => Ok(Rule::RemovePassword(payload)),
        '#' => {
            validate_password_hash(payload)?;
            Ok(Rule::AddHash(payload))
        }
        '!' => {
            validate_password_hash(payload)?;
            Ok(Rule::RemoveHash(payload))
        }
        // Redis treats the literal `~*` / `&*` as the `allkeys` /
        // `allchannels` flag (it REPLACES the list), not as one more pattern.
        '~' if payload == "*" => Ok(Rule::Keyword(Keyword::Allkeys)),
        '~' => Ok(Rule::KeyPattern {
            pattern: payload,
            read: true,
            write: true,
        }),
        '%' => parse_key_pattern_flags(payload),
        '&' if payload == "*" => Ok(Rule::Keyword(Keyword::Allchannels)),
        '&' => Ok(Rule::Channel(payload)),
        '+' => {
            validate_command_rule(payload)?;
            Ok(Rule::Allow(payload))
        }
        '-' => {
            validate_command_rule(payload)?;
            Ok(Rule::Deny(payload))
        }
        '(' => Err(AclRuleError::SelectorsUnsupported),
        _ => Err(AclRuleError::Syntax),
    }
}

/// `%<flags>[~pattern]` after the `%`. Redis: one or more of `R`/`W`
/// (case-insensitive, each at most once) optionally followed by `~` and the
/// pattern; a missing `~` means the empty pattern (`%R` renders as `%R~`).
/// No flags at all (`%~k`, `%`), a repeated flag (`%RR~k`) or any other
/// character (`%X~k`) is a syntax error. Each of those was checked against
/// redis-server 8.6.1.
fn parse_key_pattern_flags(s: &str) -> Result<Rule<'_>, AclRuleError> {
    let (mut read, mut write) = (false, false);
    for (i, b) in s.bytes().enumerate() {
        match b.to_ascii_uppercase() {
            b'R' if !read => read = true,
            b'W' if !write => write = true,
            b'~' => {
                if !read && !write {
                    return Err(AclRuleError::Syntax);
                }
                return Ok(Rule::KeyPattern {
                    pattern: &s[i + 1..],
                    read,
                    write,
                });
            }
            _ => return Err(AclRuleError::Syntax),
        }
    }
    if !read && !write {
        return Err(AclRuleError::Syntax);
    }
    Ok(Rule::KeyPattern {
        pattern: "",
        read,
        write,
    })
}

/// Redis: "The password hash must be exactly 64 characters and contain only
/// lowercase hexadecimal characters." Uppercase hex is rejected too, which
/// matters here because [`verify_password`] compares against lowercase hex.
fn validate_password_hash(hash: &str) -> Result<(), AclRuleError> {
    if hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Ok(())
    } else {
        Err(AclRuleError::BadPasswordHash)
    }
}

/// The payload of `+`/`-`: `@category`, `cmd` or `cmd|sub`.
///
/// Categories are resolved later by `allow_command`/`deny_command` (that
/// ordering is #978's fix and is left in place). Command names are checked
/// here against `@all`, which is every bare command Moon dispatches and is
/// identical to the `COMMAND_META` registry. For `cmd|sub`, a container that
/// publishes a subcommand table must name one of its subcommands
/// (`+config|bogus` is an error); any other command takes the first-arg form
/// verbatim (`+get|foo` is accepted), both as redis does.
fn validate_command_rule(payload: &str) -> Result<(), AclRuleError> {
    if payload.starts_with('@') {
        return Ok(());
    }
    let (cmd, sub) = match payload.split_once('|') {
        Some((cmd, sub)) => (cmd, Some(sub)),
        None => (payload, None),
    };
    let unknown = || AclRuleError::UnknownCommand(payload.to_string());
    let cmd_lower = cmd.to_ascii_lowercase();
    // `@all` always resolves; the table is static.
    let known = get_category_commands("@all").is_some_and(|all| all.contains(&cmd_lower.as_str()));
    if cmd.is_empty() || !known {
        return Err(unknown());
    }
    match sub {
        None => Ok(()),
        Some("") => Err(AclRuleError::Syntax),
        Some(sub) => {
            let container = cmd.to_ascii_uppercase();
            let has_table =
                crate::command::metadata::SUBCOMMAND_META.contains_key(container.as_str());
            if has_table
                && !crate::command::metadata::is_known_subcommand(cmd.as_bytes(), sub.as_bytes())
            {
                return Err(unknown());
            }
            Ok(())
        }
    }
}

/// Apply one ACL rule token to `user`.
///
/// Returns `Err` for any token the grammar rejects -- unknown keyword,
/// unknown command or category, malformed prefix, bad hash, removing a
/// password the user does not hold -- and in that case `user` is left
/// **untouched**. Callers must surface the error; swallowing it reinstates
/// #978 (unknown category) and #979 (every other token).
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
    match parse_rule(rule)? {
        Rule::Noop => {}
        Rule::Keyword(kw) => return apply_keyword(user, kw),
        Rule::AddPassword(pw) => {
            let hash = hash_password(pw);
            if !user.passwords.contains(&hash) {
                user.passwords.push(hash);
            }
            // Redis clears `nopass` when a password is added. Leaving it set
            // is fail-OPEN: the operator just required a password and the
            // account still accepts any.
            user.nopass = false;
        }
        Rule::RemovePassword(pw) => {
            let hash = hash_password(pw);
            remove_password_hash(user, &hash)?;
        }
        Rule::AddHash(hash) => {
            if !user.passwords.iter().any(|p| p == hash) {
                user.passwords.push(hash.to_string());
            }
            user.nopass = false;
        }
        Rule::RemoveHash(hash) => remove_password_hash(user, hash)?,
        Rule::KeyPattern {
            pattern,
            read,
            write,
        } => user.key_patterns.push(KeyPattern {
            pattern: pattern.to_string(),
            read,
            write,
        }),
        Rule::Channel(pattern) => user.channel_patterns.push(pattern.to_string()),
        Rule::Allow(name) => return user.allow_command(name),
        Rule::Deny(name) => return user.deny_command(name),
    }
    Ok(())
}

/// Remove `hash`; an absent hash is an error, as in redis, rather than a
/// silent `OK` that leaves the credential live.
fn remove_password_hash(user: &mut AclUser, hash: &str) -> Result<(), AclRuleError> {
    let before = user.passwords.len();
    user.passwords.retain(|p| p != hash);
    if user.passwords.len() == before {
        return Err(AclRuleError::NoSuchPassword);
    }
    Ok(())
}

fn apply_keyword(user: &mut AclUser, kw: Keyword) -> Result<(), AclRuleError> {
    match kw {
        Keyword::On => user.enabled = true,
        Keyword::Off => user.enabled = false,
        Keyword::Nopass => {
            // Redis: "All the set passwords of the user are removed, and the
            // user is flagged as requiring no password." Keeping the old
            // hashes meant `nopass >new` left the OLD credential valid.
            user.nopass = true;
            user.passwords.clear();
        }
        Keyword::Resetpass => {
            user.passwords.clear();
            user.nopass = false;
        }
        Keyword::Resetkeys => user.key_patterns.clear(),
        Keyword::Resetchannels => user.channel_patterns.clear(),
        Keyword::Reset => {
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
        // `allkeys` / `~*` and `allchannels` / `&*` REPLACE the list, as in
        // redis 8.6.1 (`ACLSetSelector` empties it and sets the ALL flag).
        // `*` read+write is a superset of every other pattern, so dropping
        // the rest changes no permission -- only what LIST/GETUSER/SAVE
        // render, which now matches redis (`~a allkeys` -> `~*`, not
        // `~a ~*`). A line saved by an older moon as `~a ~*` loads to the
        // identical permission set.
        Keyword::Allkeys => {
            user.key_patterns.clear();
            user.key_patterns.push(KeyPattern {
                pattern: "*".to_string(),
                read: true,
                write: true,
            });
        }
        Keyword::Allchannels => {
            user.channel_patterns.clear();
            user.channel_patterns.push("*".to_string());
        }
        Keyword::Allcommands => return user.allow_command("@all"),
        Keyword::Nocommands => return user.deny_command("@all"),
        Keyword::SanitizePayload | Keyword::SkipSanitizePayload | Keyword::Clearselectors => {}
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
/// (`acl|setuser`, `config|get`); this table is keyed on bare names (explicit
/// `+foo|sub` / `-foo|sub` rules are per subcommand, see `acl::subcommand`),
/// so redis's `foo|sub in C` collapses to
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

    // ---------------------------------------------------------------
    // #979 -- the token grammar. Every reply text below was transcribed
    // from a live `redis-server 8.6.1`; every "must apply" token was
    // confirmed to be honoured there.
    //
    // Before this change the parser matched lowercase literals and ended
    // in `_ => {}`: `nocommands`, `OFF`, `RESET`, `RESETKEYS` and every
    // uppercase spelling answered `OK` and changed nothing, so a
    // revocation reported success and left a privileged user privileged.
    // ---------------------------------------------------------------

    /// A user holding everything: the state an emergency lockdown starts
    /// from, and the state every dropped revocation left behind.
    fn full_user() -> AclUser {
        let mut user = AclUser::default_deny("v".to_string());
        for rule in ["on", ">pw", "~*", "&*", "+@all"] {
            apply_rule(&mut user, rule).expect("rule must apply");
        }
        assert!(user.unrestricted(), "fixture must start unrestricted");
        user
    }

    /// Every keyword, in three spellings, must land on the same state as
    /// its lowercase form. Compared field by field so a keyword that is
    /// accepted but mis-routed (say `OFF` hitting the `on` arm) fails too.
    #[test]
    fn test_keywords_are_case_insensitive() {
        for (name, _) in KEYWORDS {
            let upper = name.to_ascii_uppercase();
            // "rEsEtKeYs"-style: every other letter upper-cased.
            let mixed: String = name
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if i % 2 == 1 {
                        c.to_ascii_uppercase()
                    } else {
                        c
                    }
                })
                .collect();
            for spelling in [upper.as_str(), mixed.as_str()] {
                let mut expected = full_user();
                apply_rule(&mut expected, name).expect("lowercase keyword must apply");
                let mut actual = full_user();
                apply_rule(&mut actual, spelling)
                    .unwrap_or_else(|e| panic!("`{spelling}` must apply like `{name}`: {e}"));
                assert_eq!(actual.enabled, expected.enabled, "{spelling}: enabled");
                assert_eq!(actual.nopass, expected.nopass, "{spelling}: nopass");
                assert_eq!(
                    actual.passwords, expected.passwords,
                    "{spelling}: passwords"
                );
                assert_eq!(
                    actual.key_patterns.len(),
                    expected.key_patterns.len(),
                    "{spelling}: key patterns"
                );
                assert_eq!(
                    actual.channel_patterns, expected.channel_patterns,
                    "{spelling}: channel patterns"
                );
                assert_eq!(
                    actual.is_command_allowed("flushall"),
                    expected.is_command_allowed("flushall"),
                    "{spelling}: command permission"
                );
                assert_eq!(
                    actual.unrestricted(),
                    expected.unrestricted(),
                    "{spelling}: unrestricted cache"
                );
            }
        }
    }

    /// The headline row of #979: `nocommands` on a `+@all` user must deny.
    /// Observed through ENFORCEMENT, not through a flag read-back.
    #[test]
    fn test_nocommands_revokes_everything() {
        for spelling in ["nocommands", "NOCOMMANDS", "NoCommands"] {
            let mut user = full_user();
            apply_rule(&mut user, spelling).expect("must apply");
            assert!(!user.is_command_allowed("flushall"), "{spelling}: FLUSHALL");
            assert!(!user.is_command_allowed("get"), "{spelling}: GET");
            assert!(!user.is_command_allowed("ping"), "{spelling}: PING");
            assert!(!user.unrestricted(), "{spelling}: cache must drop");
        }
    }

    #[test]
    fn test_off_and_reset_disable_the_account() {
        for spelling in ["OFF", "Off", "RESET", "Reset"] {
            let mut user = full_user();
            apply_rule(&mut user, spelling).expect("must apply");
            assert!(!user.enabled, "{spelling}: must disable");
            assert!(!user.unrestricted(), "{spelling}: cache must drop");
        }
        let mut user = full_user();
        apply_rule(&mut user, "RESET").expect("must apply");
        assert!(user.passwords.is_empty());
        assert!(user.key_patterns.is_empty());
        assert!(user.channel_patterns.is_empty());
        assert!(!user.is_command_allowed("get"));
    }

    #[test]
    fn test_resetkeys_resetchannels_resetpass_uppercase() {
        let mut user = full_user();
        apply_rule(&mut user, "RESETKEYS").expect("must apply");
        assert!(user.key_patterns.is_empty());
        let mut user = full_user();
        apply_rule(&mut user, "RESETCHANNELS").expect("must apply");
        assert!(user.channel_patterns.is_empty());
        let mut user = full_user();
        apply_rule(&mut user, "RESETPASS").expect("must apply");
        assert!(user.passwords.is_empty());
        assert!(!user.nopass);
    }

    /// The grant-side keywords redis has and moon lacked. A dropped grant
    /// is fail-closed but still a lie on the wire.
    #[test]
    fn test_allkeys_allchannels_allcommands() {
        let mut user = AclUser::default_deny("v".to_string());
        apply_rule(&mut user, "on").expect("must apply");
        assert!(!user.is_command_allowed("set"));
        for rule in ["ALLKEYS", "AllCommands", "allchannels"] {
            apply_rule(&mut user, rule).expect("must apply");
        }
        assert!(user.is_command_allowed("set"));
        assert_eq!(user.key_patterns.len(), 1);
        assert_eq!(user.key_patterns[0].pattern, "*");
        assert!(user.key_patterns[0].read && user.key_patterns[0].write);
        assert_eq!(user.channel_patterns, vec!["*".to_string()]);
        assert!(
            user.unrestricted(),
            "allkeys+allcommands+allchannels == ~* +@all &*"
        );
    }

    /// moon#970: `allkeys` and `~*` REPLACE the key-pattern list in redis
    /// 8.6.1 (`ACLSetSelector` sets the ALLKEYS flag and empties the list),
    /// so `~a %R~b allkeys` is rendered by `ACL LIST` as plain `~*`. Moon used
    /// to append, rendering `~a %R~b ~*` -- the same grant, a different report
    /// and a different saved line. Same for `allchannels` / `&*`.
    #[test]
    fn test_allkeys_and_tilde_star_replace_the_pattern_list() {
        for all_keys in ["allkeys", "ALLKEYS", "~*"] {
            let mut user = AclUser::default_deny("v".to_string());
            for rule in ["~a", "%R~b", all_keys] {
                apply_rule(&mut user, rule).expect("must apply");
            }
            let patterns: Vec<(&str, bool, bool)> = user
                .key_patterns
                .iter()
                .map(|kp| (kp.pattern.as_str(), kp.read, kp.write))
                .collect();
            assert_eq!(patterns, vec![("*", true, true)], "after `{all_keys}`");
        }
        for all_channels in ["allchannels", "AllChannels", "&*"] {
            let mut user = AclUser::default_deny("v".to_string());
            for rule in ["&a", "&b", all_channels] {
                apply_rule(&mut user, rule).expect("must apply");
            }
            assert_eq!(
                user.channel_patterns,
                vec!["*".to_string()],
                "after `{all_channels}`"
            );
        }
    }

    /// Only the literal `~*` / `&*` spelling is the all-keys / all-channels
    /// flag in redis. A pattern that merely CONTAINS `*` is an ordinary
    /// pattern and appends, and a pattern AFTER `~*` still appends (redis
    /// rejects that; moon keeps accepting it so an existing aclfile holding
    /// `~* ~x` still loads -- see the CHANGELOG).
    #[test]
    fn test_only_the_literal_star_pattern_replaces() {
        let mut user = AclUser::default_deny("v".to_string());
        for rule in ["~a", "~b*", "%W~*"] {
            apply_rule(&mut user, rule).expect("must apply");
        }
        let names: Vec<&str> = user
            .key_patterns
            .iter()
            .map(|kp| kp.pattern.as_str())
            .collect();
        assert_eq!(names, vec!["a", "b*", "*"]);

        let mut user = AclUser::default_deny("v".to_string());
        for rule in ["~*", "~x"] {
            apply_rule(&mut user, rule).expect("must apply");
        }
        let names: Vec<&str> = user
            .key_patterns
            .iter()
            .map(|kp| kp.pattern.as_str())
            .collect();
        assert_eq!(names, vec!["*", "x"]);
    }

    /// Tokens redis accepts that have no effect on moon. They must be
    /// ACCEPTED (every redis `ACL LIST` line carries `sanitize-payload`)
    /// and must change nothing.
    #[test]
    fn test_noop_keywords_are_accepted_and_inert() {
        for rule in [
            "sanitize-payload",
            "SKIP-SANITIZE-PAYLOAD",
            "clearselectors",
            "",
        ] {
            let mut user = full_user();
            apply_rule(&mut user, rule).unwrap_or_else(|e| panic!("`{rule}` must apply: {e}"));
            assert!(user.unrestricted(), "`{rule}` must be inert");
        }
    }

    /// The `_ => {}` sink. An unknown token is a syntax error and mutates
    /// nothing. Whitespace is NOT trimmed -- redis rejects `' on'` too.
    #[test]
    fn test_unknown_token_is_a_syntax_error() {
        for rule in [
            "bogus",
            "BOGUS",
            "@read",
            "nocommand",
            " on",
            "on ",
            ")",
            "*",
            "user",
        ] {
            let mut user = full_user();
            let err = apply_rule(&mut user, rule).expect_err(rule);
            assert_eq!(err, AclRuleError::Syntax, "{rule}");
            assert!(user.unrestricted(), "`{rule}` must not mutate");
        }
    }

    /// A token whose FIRST character is multi-byte UTF-8. The tokenizer
    /// sliced `&rule[1..]` before looking at the prefix, and byte 1 of `é` is
    /// not a char boundary: `ACL SETUSER u on éx` PANICKED the shard thread and
    /// aborted the whole server (and the same token in an aclfile would have
    /// crashed every boot). Redis answers `Syntax error`. Multi-byte bytes
    /// AFTER an ASCII prefix are ordinary payload and must still apply.
    #[test]
    fn test_multibyte_first_char_is_a_syntax_error_not_a_panic() {
        for rule in ["éx", "é", "€", "💥+get", "ü~*", "\u{a0}on"] {
            let mut user = full_user();
            let err = apply_rule(&mut user, rule).expect_err(rule);
            assert_eq!(err, AclRuleError::Syntax, "{rule}");
            assert!(user.unrestricted(), "`{rule}` must not mutate");
        }
        let mut user = AclUser::default_deny("v".to_string());
        for rule in [">pässwörd", "~kéy:*", "&chän", "%R~ü"] {
            apply_rule(&mut user, rule).unwrap_or_else(|e| panic!("`{rule}` must apply: {e}"));
        }
        assert_eq!(user.passwords, vec![hash_password("pässwörd")]);
        assert_eq!(user.key_patterns[0].pattern, "kéy:*");
        assert_eq!(user.key_patterns[1].pattern, "ü");
        assert_eq!(user.channel_patterns, vec!["chän".to_string()]);
    }

    /// `%` flags: case-insensitive, any order, each at most once, `~`
    /// optional (missing `~` == empty pattern). Everything else is a
    /// syntax error. Each row checked against redis-server 8.6.1.
    #[test]
    fn test_key_pattern_flags() {
        let cases: &[(&str, &str, bool, bool)] = &[
            ("%R~k", "k", true, false),
            ("%r~k", "k", true, false),
            ("%W~k", "k", false, true),
            ("%w~k", "k", false, true),
            ("%RW~k", "k", true, true),
            ("%rw~k", "k", true, true),
            ("%WR~k", "k", true, true),
            ("%Rw~k", "k", true, true),
            ("%R", "", true, false),
            ("%RW", "", true, true),
            ("%R~", "", true, false),
            ("%R~Mixed:Case*", "Mixed:Case*", true, false),
        ];
        for (rule, pattern, read, write) in cases {
            let mut user = AclUser::default_deny("v".to_string());
            apply_rule(&mut user, rule).unwrap_or_else(|e| panic!("`{rule}` must apply: {e}"));
            assert_eq!(user.key_patterns.len(), 1, "{rule}");
            let kp = &user.key_patterns[0];
            assert_eq!(kp.pattern, *pattern, "{rule}: pattern is verbatim");
            assert_eq!(kp.read, *read, "{rule}: read");
            assert_eq!(kp.write, *write, "{rule}: write");
        }
        for rule in ["%X~k", "%RR~k", "%WW~k", "%~k", "%", "%RX~k"] {
            let mut user = AclUser::default_deny("v".to_string());
            let err = apply_rule(&mut user, rule).expect_err(rule);
            assert_eq!(err, AclRuleError::Syntax, "{rule}");
            assert!(user.key_patterns.is_empty(), "`{rule}` must not mutate");
        }
    }

    /// Payloads stay case-sensitive: `~PAT` and `&CHAN` and `>PW` are not
    /// folded. Folding them would silently widen or narrow the grant.
    #[test]
    fn test_prefixed_payloads_keep_their_case() {
        let mut user = AclUser::default_deny("v".to_string());
        apply_rule(&mut user, "~Cache:*").expect("must apply");
        apply_rule(&mut user, "&Events").expect("must apply");
        apply_rule(&mut user, ">Secret").expect("must apply");
        assert_eq!(user.key_patterns[0].pattern, "Cache:*");
        assert_eq!(user.channel_patterns[0], "Events");
        assert!(user.passwords.contains(&hash_password("Secret")));
        assert!(!user.passwords.contains(&hash_password("secret")));
    }

    #[test]
    fn test_password_hash_must_be_64_lowercase_hex() {
        let good = "30c952fab122c3f9759f02a6d95c3758b246b4fee239957b2d4fee46e26170c4";
        let upper = good.to_ascii_uppercase();
        for bad in ["zz", "abc", upper.as_str(), &good[..63], ""] {
            for prefix in ['#', '!'] {
                let rule = format!("{prefix}{bad}");
                let mut user = full_user();
                let err = apply_rule(&mut user, &rule).expect_err(&rule);
                assert_eq!(err, AclRuleError::BadPasswordHash, "{rule}");
                assert!(user.unrestricted(), "`{rule}` must not mutate");
            }
        }
        let mut user = AclUser::default_deny("v".to_string());
        apply_rule(&mut user, &format!("#{good}")).expect("must apply");
        assert_eq!(user.passwords, vec![good.to_string()]);
    }

    /// Removing a credential the user does not hold is an error, not a
    /// silent `OK`.
    #[test]
    fn test_removing_an_absent_password_is_an_error() {
        let zero = "0".repeat(64);
        for rule in ["<nope", format!("!{zero}").as_str()] {
            let mut user = full_user();
            let err = apply_rule(&mut user, rule).expect_err(rule);
            assert_eq!(err, AclRuleError::NoSuchPassword, "{rule}");
            assert_eq!(
                user.passwords,
                vec![hash_password("pw")],
                "{rule}: untouched"
            );
        }
        // ...and removing one it does hold still works, by password and by hash.
        let mut user = full_user();
        apply_rule(&mut user, "<pw").expect("must apply");
        assert!(user.passwords.is_empty());
        let mut user = full_user();
        let hash = hash_password("pw");
        apply_rule(&mut user, &format!("!{hash}")).expect("must apply");
        assert!(user.passwords.is_empty());
    }

    /// Fail-OPEN before #979: `nopass` then `>pw` left `nopass` set, so the
    /// operator who just required a password still had a passwordless
    /// account. Redis clears the flag on `>` and on `#`.
    #[test]
    fn test_adding_a_password_clears_nopass() {
        let mut user = AclUser::default_deny("v".to_string());
        apply_rule(&mut user, ">old").expect("must apply");
        apply_rule(&mut user, "nopass").expect("must apply");
        assert!(user.nopass);
        assert!(user.passwords.is_empty(), "nopass removes every password");
        apply_rule(&mut user, ">pw").expect("must apply");
        assert!(!user.nopass, ">pw must clear nopass");
        assert_eq!(
            user.passwords,
            vec![hash_password("pw")],
            "the old credential must not survive the rotation"
        );

        let mut user = AclUser::default_deny("v".to_string());
        apply_rule(&mut user, "nopass").expect("must apply");
        let hash = hash_password("pw");
        apply_rule(&mut user, &format!("#{hash}")).expect("must apply");
        assert!(!user.nopass, "#hash must clear nopass");
    }

    /// `+`/`-` payloads are validated: a typo in a revocation must not
    /// answer `OK`. Same error text as an unknown category, as in redis.
    #[test]
    fn test_unknown_command_is_an_error() {
        for rule in [
            "+bogus",
            "-bogus",
            "-flushal",
            "+",
            "-",
            "+|get",
            "-|get",
            "+config|bogus",
            "-CONFIG|Bogus",
        ] {
            let mut user = full_user();
            let err = apply_rule(&mut user, rule).expect_err(rule);
            assert!(
                matches!(err, AclRuleError::UnknownCommand(_)),
                "{rule}: got {err:?}"
            );
            assert_eq!(
                err.to_string(),
                "Unknown command or category name in ACL",
                "{rule}"
            );
            assert!(user.unrestricted(), "`{rule}` must not mutate");
        }
        // `+get|` is a SYNTAX error on redis, distinct from an unknown name.
        let mut user = full_user();
        assert_eq!(
            apply_rule(&mut user, "+get|").expect_err("+get|"),
            AclRuleError::Syntax
        );
        assert!(user.unrestricted());
    }

    /// Command names and subcommand names are case-insensitive; a
    /// non-container takes the first-arg form verbatim (redis accepts
    /// `+get|foo`).
    #[test]
    fn test_command_rules_accept_every_case_and_first_arg_form() {
        let mut user = full_user();
        // Order matters: `+@Read` re-grants GET, so the category rules go
        // first and the per-command denies last (last rule wins).
        for rule in [
            "+@Read",
            "-@DANGEROUS",
            "+CONFIG|GET",
            "-config|Set",
            "-acl|SETUSER",
            "+get|foo",
            "-FLUSHALL",
            "-Get",
        ] {
            apply_rule(&mut user, rule).unwrap_or_else(|e| panic!("`{rule}` must apply: {e}"));
        }
        assert!(!user.is_command_allowed("flushall"), "-FLUSHALL must deny");
        assert!(!user.is_command_allowed("FLUSHALL"));
        assert!(!user.is_command_allowed("get"), "-Get must deny");
    }

    /// Selectors are valid redis grammar that moon does not implement.
    /// Silently accepting one drops a grant the operator asked for, so it
    /// is refused instead.
    #[test]
    fn test_selectors_are_refused_not_dropped() {
        for rule in ["(+get ~k)", "(", "()", "( +get"] {
            let mut user = full_user();
            let err = apply_rule(&mut user, rule).expect_err(rule);
            assert_eq!(err, AclRuleError::SelectorsUnsupported, "{rule}");
            assert!(user.unrestricted(), "`{rule}` must not mutate");
        }
    }

    /// Every error's wire text, transcribed from redis-server 8.6.1.
    #[test]
    fn test_every_error_text_matches_redis() {
        let cases = [
            (
                AclRuleError::UnknownCategory("bogus".into()),
                "+@bogus",
                "ERR Error in ACL SETUSER modifier '+@bogus': Unknown command or category name in ACL",
            ),
            (
                AclRuleError::UnknownCommand("bogus".into()),
                "-bogus",
                "ERR Error in ACL SETUSER modifier '-bogus': Unknown command or category name in ACL",
            ),
            (
                AclRuleError::Syntax,
                "BOGUS",
                "ERR Error in ACL SETUSER modifier 'BOGUS': Syntax error",
            ),
            (
                AclRuleError::BadPasswordHash,
                "#zz",
                "ERR Error in ACL SETUSER modifier '#zz': The password hash must be exactly 64 \
                 characters and contain only lowercase hexadecimal characters",
            ),
            (
                AclRuleError::NoSuchPassword,
                "<nope",
                "ERR Error in ACL SETUSER modifier '<nope': The password you are trying to remove \
                 from the user does not exist",
            ),
        ];
        for (err, rule, expected) in cases {
            assert_eq!(err.to_setuser_error(rule), expected);
        }
    }

    /// The keyword table is the only place spellings are compared, so it
    /// must be complete: every keyword redis 8.6.1 accepts is here.
    #[test]
    fn test_keyword_table_covers_redis_grammar() {
        let redis = [
            "on",
            "off",
            "nopass",
            "resetpass",
            "resetkeys",
            "resetchannels",
            "reset",
            "allkeys",
            "allchannels",
            "allcommands",
            "nocommands",
            "sanitize-payload",
            "skip-sanitize-payload",
            "clearselectors",
        ];
        for kw in redis {
            assert!(
                KEYWORDS.iter().any(|(name, _)| *name == kw),
                "redis keyword `{kw}` is missing from KEYWORDS"
            );
        }
        assert_eq!(
            KEYWORDS.len(),
            redis.len(),
            "KEYWORDS has an entry redis does not"
        );
    }
}
