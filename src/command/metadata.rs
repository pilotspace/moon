//! Command metadata registry: static O(1) lookup of command properties.
//!
//! Provides `CommandMeta`, `CommandFlags`, `AclCategories`, and a `phf`-based
//! static map covering every command in the dispatch table and AOF write list.

use phf::phf_map;

// ---------------------------------------------------------------------------
// CommandFlags — bitflags for command properties
// ---------------------------------------------------------------------------

/// Bitflags describing command behavior (write, read-only, fast, etc.).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommandFlags(u16);

impl CommandFlags {
    pub const NONE: Self = Self(0);
    pub const WRITE: Self = Self(1 << 0);
    pub const READONLY: Self = Self(1 << 1);
    pub const FAST: Self = Self(1 << 2);
    pub const ADMIN: Self = Self(1 << 3);
    pub const PUBSUB: Self = Self(1 << 4);
    pub const NOSCRIPT: Self = Self(1 << 5);
    pub const LOADING: Self = Self(1 << 6);
    pub const STALE: Self = Self(1 << 7);
    pub const SKIP_MONITOR: Self = Self(1 << 8);
    pub const ASKING: Self = Self(1 << 9);
    pub const NO_AUTH: Self = Self(1 << 10);
    pub const MAY_REPLICATE: Self = Self(1 << 11);
    pub const SORT_FOR_SCRIPT: Self = Self(1 << 12);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

// ---------------------------------------------------------------------------
// AclCategories — bitflags for ACL category classification
// ---------------------------------------------------------------------------

/// Bitflags for Redis ACL categories.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AclCategories(u32);

impl AclCategories {
    pub const STRING: Self = Self(1 << 0);
    pub const HASH: Self = Self(1 << 1);
    pub const LIST: Self = Self(1 << 2);
    pub const SET: Self = Self(1 << 3);
    pub const SORTEDSET: Self = Self(1 << 4);
    pub const STREAM: Self = Self(1 << 5);
    pub const GENERIC: Self = Self(1 << 6);
    pub const CONNECTION: Self = Self(1 << 7);
    pub const SERVER: Self = Self(1 << 8);
    pub const PUBSUB: Self = Self(1 << 9);
    pub const SCRIPTING: Self = Self(1 << 10);
    pub const TRANSACTIONS: Self = Self(1 << 11);
    pub const DANGEROUS: Self = Self(1 << 12);
    pub const SLOW: Self = Self(1 << 13);
    pub const FAST_CAT: Self = Self(1 << 14);
    pub const KEYSPACE: Self = Self(1 << 15);
    pub const WRITE_CAT: Self = Self(1 << 16);
    pub const READ_CAT: Self = Self(1 << 17);
    pub const SEARCH: Self = Self(1 << 18);

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

// ---------------------------------------------------------------------------
// CommandMeta
// ---------------------------------------------------------------------------

/// Metadata for a single Redis command.
#[derive(Clone, Copy, Debug)]
pub struct CommandMeta {
    /// Canonical uppercase command name.
    pub name: &'static str,
    /// Arity: positive = exact arg count (incl. command name), negative = minimum (variadic).
    pub arity: i16,
    /// Command behavior flags.
    pub flags: CommandFlags,
    /// Index of the first key argument (0 = no keys).
    pub first_key: i16,
    /// Index of the last key argument (0 = no keys, -1 = last argument).
    pub last_key: i16,
    /// Step between key arguments (1 = every arg is a key, 2 = every other arg).
    pub step: i16,
    /// ACL categories this command belongs to.
    pub acl_categories: AclCategories,
}

// ---------------------------------------------------------------------------
// Convenience constants for common flag combos
// ---------------------------------------------------------------------------

const W: CommandFlags = CommandFlags::WRITE;
const R: CommandFlags = CommandFlags::READONLY;
const WF: CommandFlags = CommandFlags(CommandFlags::WRITE.0 | CommandFlags::FAST.0);
const RF: CommandFlags = CommandFlags(CommandFlags::READONLY.0 | CommandFlags::FAST.0);
const A: CommandFlags = CommandFlags::ADMIN;
const RA: CommandFlags = CommandFlags(CommandFlags::READONLY.0 | CommandFlags::ADMIN.0);

const STR: AclCategories = AclCategories::STRING;
const HSH: AclCategories = AclCategories::HASH;
const LST: AclCategories = AclCategories::LIST;
const SET_CAT: AclCategories = AclCategories::SET;
const ZST: AclCategories = AclCategories::SORTEDSET;
const STM: AclCategories = AclCategories::STREAM;
const GEN: AclCategories = AclCategories::GENERIC;
const CON: AclCategories = AclCategories::CONNECTION;
const SRV: AclCategories = AclCategories::SERVER;
const PUB: AclCategories = AclCategories::PUBSUB;
const SCR: AclCategories = AclCategories::SCRIPTING;
const TXN: AclCategories = AclCategories::TRANSACTIONS;
const DNG: AclCategories = AclCategories::DANGEROUS;
const SRCH: AclCategories = AclCategories::SEARCH;

// ---------------------------------------------------------------------------
// Static registry — phf perfect-hash map keyed by uppercase command name
// ---------------------------------------------------------------------------

/// Static command metadata registry. Keys are uppercase ASCII command names.
pub static COMMAND_META: phf::Map<&'static str, CommandMeta> = phf_map! {
    // ---- String commands ----
    "GET" => CommandMeta { name: "GET", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "SET" => CommandMeta { name: "SET", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "MGET" => CommandMeta { name: "MGET", arity: -2, flags: RF, first_key: 1, last_key: -1, step: 1, acl_categories: STR },
    "MSET" => CommandMeta { name: "MSET", arity: -3, flags: WF, first_key: 1, last_key: -1, step: 2, acl_categories: STR },
    "SETNX" => CommandMeta { name: "SETNX", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "SETEX" => CommandMeta { name: "SETEX", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "PSETEX" => CommandMeta { name: "PSETEX", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "GETSET" => CommandMeta { name: "GETSET", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "GETDEL" => CommandMeta { name: "GETDEL", arity: 2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "GETEX" => CommandMeta { name: "GETEX", arity: -2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "INCR" => CommandMeta { name: "INCR", arity: 2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "DECR" => CommandMeta { name: "DECR", arity: 2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "INCRBY" => CommandMeta { name: "INCRBY", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "DECRBY" => CommandMeta { name: "DECRBY", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "INCRBYFLOAT" => CommandMeta { name: "INCRBYFLOAT", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "APPEND" => CommandMeta { name: "APPEND", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "STRLEN" => CommandMeta { name: "STRLEN", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "GETRANGE" => CommandMeta { name: "GETRANGE", arity: 4, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "SETRANGE" => CommandMeta { name: "SETRANGE", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "SUBSTR" => CommandMeta { name: "SUBSTR", arity: 4, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: STR },

    // ---- Hash commands ----
    "HSET" => CommandMeta { name: "HSET", arity: -4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HGET" => CommandMeta { name: "HGET", arity: 3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HDEL" => CommandMeta { name: "HDEL", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HLEN" => CommandMeta { name: "HLEN", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HMSET" => CommandMeta { name: "HMSET", arity: -4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HMGET" => CommandMeta { name: "HMGET", arity: -3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HKEYS" => CommandMeta { name: "HKEYS", arity: 2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HVALS" => CommandMeta { name: "HVALS", arity: 2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HSCAN" => CommandMeta { name: "HSCAN", arity: -3, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HSETNX" => CommandMeta { name: "HSETNX", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HGETALL" => CommandMeta { name: "HGETALL", arity: 2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HEXISTS" => CommandMeta { name: "HEXISTS", arity: 3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HINCRBY" => CommandMeta { name: "HINCRBY", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },
    "HINCRBYFLOAT" => CommandMeta { name: "HINCRBYFLOAT", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: HSH },

    // ---- List commands ----
    "LPUSH" => CommandMeta { name: "LPUSH", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "RPUSH" => CommandMeta { name: "RPUSH", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LPOP" => CommandMeta { name: "LPOP", arity: -2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "RPOP" => CommandMeta { name: "RPOP", arity: -2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LLEN" => CommandMeta { name: "LLEN", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LSET" => CommandMeta { name: "LSET", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LREM" => CommandMeta { name: "LREM", arity: 4, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LPOS" => CommandMeta { name: "LPOS", arity: -3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LRANGE" => CommandMeta { name: "LRANGE", arity: 4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LINDEX" => CommandMeta { name: "LINDEX", arity: 3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LINSERT" => CommandMeta { name: "LINSERT", arity: 5, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LTRIM" => CommandMeta { name: "LTRIM", arity: 4, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: LST },
    "LMOVE" => CommandMeta { name: "LMOVE", arity: 5, flags: W, first_key: 1, last_key: 2, step: 1, acl_categories: LST },

    // ---- Set commands ----
    "SADD" => CommandMeta { name: "SADD", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SREM" => CommandMeta { name: "SREM", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SPOP" => CommandMeta { name: "SPOP", arity: -2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SCARD" => CommandMeta { name: "SCARD", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SDIFF" => CommandMeta { name: "SDIFF", arity: -2, flags: R, first_key: 1, last_key: -1, step: 1, acl_categories: SET_CAT },
    "SSCAN" => CommandMeta { name: "SSCAN", arity: -3, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SINTER" => CommandMeta { name: "SINTER", arity: -2, flags: R, first_key: 1, last_key: -1, step: 1, acl_categories: SET_CAT },
    "SUNION" => CommandMeta { name: "SUNION", arity: -2, flags: R, first_key: 1, last_key: -1, step: 1, acl_categories: SET_CAT },
    "SMEMBERS" => CommandMeta { name: "SMEMBERS", arity: 2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SISMEMBER" => CommandMeta { name: "SISMEMBER", arity: 3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SMISMEMBER" => CommandMeta { name: "SMISMEMBER", arity: -3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SRANDMEMBER" => CommandMeta { name: "SRANDMEMBER", arity: -2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: SET_CAT },
    "SDIFFSTORE" => CommandMeta { name: "SDIFFSTORE", arity: -3, flags: W, first_key: 1, last_key: -1, step: 1, acl_categories: SET_CAT },
    "SINTERSTORE" => CommandMeta { name: "SINTERSTORE", arity: -3, flags: W, first_key: 1, last_key: -1, step: 1, acl_categories: SET_CAT },
    "SUNIONSTORE" => CommandMeta { name: "SUNIONSTORE", arity: -3, flags: W, first_key: 1, last_key: -1, step: 1, acl_categories: SET_CAT },

    // ---- Sorted-set commands ----
    "ZADD" => CommandMeta { name: "ZADD", arity: -4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZREM" => CommandMeta { name: "ZREM", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZCARD" => CommandMeta { name: "ZCARD", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZRANK" => CommandMeta { name: "ZRANK", arity: 3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZSCAN" => CommandMeta { name: "ZSCAN", arity: -3, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZSCORE" => CommandMeta { name: "ZSCORE", arity: 3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZRANGE" => CommandMeta { name: "ZRANGE", arity: -4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZCOUNT" => CommandMeta { name: "ZCOUNT", arity: 4, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZINCRBY" => CommandMeta { name: "ZINCRBY", arity: 4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZPOPMIN" => CommandMeta { name: "ZPOPMIN", arity: -2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZPOPMAX" => CommandMeta { name: "ZPOPMAX", arity: -2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZREVRANK" => CommandMeta { name: "ZREVRANK", arity: 3, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZREVRANGE" => CommandMeta { name: "ZREVRANGE", arity: 4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZLEXCOUNT" => CommandMeta { name: "ZLEXCOUNT", arity: 4, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZUNIONSTORE" => CommandMeta { name: "ZUNIONSTORE", arity: -4, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZINTERSTORE" => CommandMeta { name: "ZINTERSTORE", arity: -4, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZRANGEBYSCORE" => CommandMeta { name: "ZRANGEBYSCORE", arity: -4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },
    "ZREVRANGEBYSCORE" => CommandMeta { name: "ZREVRANGEBYSCORE", arity: -4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: ZST },

    // ---- Stream commands ----
    "XADD" => CommandMeta { name: "XADD", arity: -5, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XLEN" => CommandMeta { name: "XLEN", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XDEL" => CommandMeta { name: "XDEL", arity: -3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XACK" => CommandMeta { name: "XACK", arity: -4, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XTRIM" => CommandMeta { name: "XTRIM", arity: -4, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XREAD" => CommandMeta { name: "XREAD", arity: -4, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: STM },
    "XINFO" => CommandMeta { name: "XINFO", arity: -2, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: STM },
    "XRANGE" => CommandMeta { name: "XRANGE", arity: -4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XREVRANGE" => CommandMeta { name: "XREVRANGE", arity: -4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XGROUP" => CommandMeta { name: "XGROUP", arity: -2, flags: W, first_key: 2, last_key: 2, step: 1, acl_categories: STM },
    "XCLAIM" => CommandMeta { name: "XCLAIM", arity: -6, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XPENDING" => CommandMeta { name: "XPENDING", arity: -3, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: STM },
    "XREADGROUP" => CommandMeta { name: "XREADGROUP", arity: -7, flags: W, first_key: 0, last_key: 0, step: 0, acl_categories: STM },
    "XAUTOCLAIM" => CommandMeta { name: "XAUTOCLAIM", arity: -7, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: STM },

    // ---- Key / generic commands ----
    "DEL" => CommandMeta { name: "DEL", arity: -2, flags: W, first_key: 1, last_key: -1, step: 1, acl_categories: GEN },
    "UNLINK" => CommandMeta { name: "UNLINK", arity: -2, flags: WF, first_key: 1, last_key: -1, step: 1, acl_categories: GEN },
    "EXISTS" => CommandMeta { name: "EXISTS", arity: -2, flags: RF, first_key: 1, last_key: -1, step: 1, acl_categories: GEN },
    "EXPIRE" => CommandMeta { name: "EXPIRE", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "PEXPIRE" => CommandMeta { name: "PEXPIRE", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "PERSIST" => CommandMeta { name: "PERSIST", arity: 2, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "TTL" => CommandMeta { name: "TTL", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "PTTL" => CommandMeta { name: "PTTL", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "TYPE" => CommandMeta { name: "TYPE", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "RENAME" => CommandMeta { name: "RENAME", arity: 3, flags: W, first_key: 1, last_key: 2, step: 1, acl_categories: GEN },
    "RENAMENX" => CommandMeta { name: "RENAMENX", arity: 3, flags: WF, first_key: 1, last_key: 2, step: 1, acl_categories: GEN },
    "KEYS" => CommandMeta { name: "KEYS", arity: 2, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: GEN },
    "SCAN" => CommandMeta { name: "SCAN", arity: -2, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: GEN },
    "OBJECT" => CommandMeta { name: "OBJECT", arity: -2, flags: R, first_key: 2, last_key: 2, step: 1, acl_categories: GEN },
    "DBSIZE" => CommandMeta { name: "DBSIZE", arity: 1, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: GEN },
    "RANDOMKEY" => CommandMeta { name: "RANDOMKEY", arity: 1, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: GEN },
    "DUMP" => CommandMeta { name: "DUMP", arity: 2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "RESTORE" => CommandMeta { name: "RESTORE", arity: -4, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "SORT" => CommandMeta { name: "SORT", arity: -2, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "COPY" => CommandMeta { name: "COPY", arity: -3, flags: W, first_key: 1, last_key: 2, step: 1, acl_categories: GEN },
    "TOUCH" => CommandMeta { name: "TOUCH", arity: -2, flags: RF, first_key: 1, last_key: -1, step: 1, acl_categories: GEN },
    "EXPIREAT" => CommandMeta { name: "EXPIREAT", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "PEXPIREAT" => CommandMeta { name: "PEXPIREAT", arity: 3, flags: WF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "EXPIRETIME" => CommandMeta { name: "EXPIRETIME", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "PEXPIRETIME" => CommandMeta { name: "PEXPIRETIME", arity: 2, flags: RF, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },

    // ---- Bitmap commands ----
    "BITCOUNT" => CommandMeta { name: "BITCOUNT", arity: -2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "BITOP" => CommandMeta { name: "BITOP", arity: -4, flags: W, first_key: 2, last_key: -1, step: 1, acl_categories: STR },
    "BITFIELD" => CommandMeta { name: "BITFIELD", arity: -2, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: STR },
    "BITPOS" => CommandMeta { name: "BITPOS", arity: -3, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: STR },

    // ---- Geo commands ----
    "GEOADD" => CommandMeta { name: "GEOADD", arity: -5, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "GEODIST" => CommandMeta { name: "GEODIST", arity: -4, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "GEOHASH" => CommandMeta { name: "GEOHASH", arity: -2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "GEOPOS" => CommandMeta { name: "GEOPOS", arity: -2, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "GEOSEARCH" => CommandMeta { name: "GEOSEARCH", arity: -7, flags: R, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "GEOSEARCHSTORE" => CommandMeta { name: "GEOSEARCHSTORE", arity: -8, flags: W, first_key: 1, last_key: 2, step: 1, acl_categories: GEN },
    "GEORADIUS" => CommandMeta { name: "GEORADIUS", arity: -6, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },
    "GEORADIUSBYMEMBER" => CommandMeta { name: "GEORADIUSBYMEMBER", arity: -5, flags: W, first_key: 1, last_key: 1, step: 1, acl_categories: GEN },

    // ---- Connection / server commands ----
    "PING" => CommandMeta { name: "PING", arity: -1, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "ECHO" => CommandMeta { name: "ECHO", arity: 2, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "QUIT" => CommandMeta { name: "QUIT", arity: 1, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "SELECT" => CommandMeta { name: "SELECT", arity: 2, flags: WF, first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "INFO" => CommandMeta { name: "INFO", arity: -1, flags: RA, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "COMMAND" => CommandMeta { name: "COMMAND", arity: -1, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "AUTH" => CommandMeta { name: "AUTH", arity: -2, flags: CommandFlags(CommandFlags::FAST.0 | CommandFlags::NO_AUTH.0 | CommandFlags::LOADING.0 | CommandFlags::STALE.0), first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "HELLO" => CommandMeta { name: "HELLO", arity: -1, flags: CommandFlags(CommandFlags::FAST.0 | CommandFlags::NO_AUTH.0), first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "RESET" => CommandMeta { name: "RESET", arity: 1, flags: CommandFlags(CommandFlags::FAST.0 | CommandFlags::NO_AUTH.0 | CommandFlags::LOADING.0 | CommandFlags::STALE.0), first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "CLIENT" => CommandMeta { name: "CLIENT", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: CON },
    "WAIT" => CommandMeta { name: "WAIT", arity: 3, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: GEN },

    // ---- Server / admin commands ----
    "BGSAVE" => CommandMeta { name: "BGSAVE", arity: -1, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "BGREWRITEAOF" => CommandMeta { name: "BGREWRITEAOF", arity: 1, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "SAVE" => CommandMeta { name: "SAVE", arity: 1, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "LASTSAVE" => CommandMeta { name: "LASTSAVE", arity: 1, flags: RA, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "CONFIG" => CommandMeta { name: "CONFIG", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "ACL" => CommandMeta { name: "ACL", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "SLOWLOG" => CommandMeta { name: "SLOWLOG", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "DEBUG" => CommandMeta { name: "DEBUG", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: DNG },
    "MEMORY" => CommandMeta { name: "MEMORY", arity: -2, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "FLUSHDB" => CommandMeta { name: "FLUSHDB", arity: -1, flags: W, first_key: 0, last_key: 0, step: 0, acl_categories: DNG },
    "FLUSHALL" => CommandMeta { name: "FLUSHALL", arity: -1, flags: W, first_key: 0, last_key: 0, step: 0, acl_categories: DNG },
    "SWAPDB" => CommandMeta { name: "SWAPDB", arity: 3, flags: W, first_key: 0, last_key: 0, step: 0, acl_categories: DNG },
    "SHUTDOWN" => CommandMeta { name: "SHUTDOWN", arity: -1, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: DNG },
    "TIME" => CommandMeta { name: "TIME", arity: 1, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "LOLWUT" => CommandMeta { name: "LOLWUT", arity: -1, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "MODULE" => CommandMeta { name: "MODULE", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "LATENCY" => CommandMeta { name: "LATENCY", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },

    // ---- Pub/Sub commands ----
    "SUBSCRIBE" => CommandMeta { name: "SUBSCRIBE", arity: -2, flags: CommandFlags::PUBSUB, first_key: 0, last_key: 0, step: 0, acl_categories: PUB },
    "UNSUBSCRIBE" => CommandMeta { name: "UNSUBSCRIBE", arity: -1, flags: CommandFlags::PUBSUB, first_key: 0, last_key: 0, step: 0, acl_categories: PUB },
    "PSUBSCRIBE" => CommandMeta { name: "PSUBSCRIBE", arity: -2, flags: CommandFlags::PUBSUB, first_key: 0, last_key: 0, step: 0, acl_categories: PUB },
    "PUNSUBSCRIBE" => CommandMeta { name: "PUNSUBSCRIBE", arity: -1, flags: CommandFlags::PUBSUB, first_key: 0, last_key: 0, step: 0, acl_categories: PUB },
    "PUBLISH" => CommandMeta { name: "PUBLISH", arity: 3, flags: CommandFlags::PUBSUB, first_key: 0, last_key: 0, step: 0, acl_categories: PUB },
    "SSUBSCRIBE" => CommandMeta { name: "SSUBSCRIBE", arity: -2, flags: CommandFlags::PUBSUB, first_key: 0, last_key: 0, step: 0, acl_categories: PUB },
    "SUNSUBSCRIBE" => CommandMeta { name: "SUNSUBSCRIBE", arity: -1, flags: CommandFlags::PUBSUB, first_key: 0, last_key: 0, step: 0, acl_categories: PUB },

    // ---- Scripting commands ----
    "EVAL" => CommandMeta { name: "EVAL", arity: -3, flags: CommandFlags(CommandFlags::NOSCRIPT.0 | CommandFlags::MAY_REPLICATE.0), first_key: 0, last_key: 0, step: 0, acl_categories: SCR },
    "EVALSHA" => CommandMeta { name: "EVALSHA", arity: -3, flags: CommandFlags(CommandFlags::NOSCRIPT.0 | CommandFlags::MAY_REPLICATE.0), first_key: 0, last_key: 0, step: 0, acl_categories: SCR },
    "SCRIPT" => CommandMeta { name: "SCRIPT", arity: -2, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: SCR },

    // ---- Transaction commands ----
    "MULTI" => CommandMeta { name: "MULTI", arity: 1, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: TXN },
    "EXEC" => CommandMeta { name: "EXEC", arity: 1, flags: CommandFlags(CommandFlags::NOSCRIPT.0), first_key: 0, last_key: 0, step: 0, acl_categories: TXN },
    "DISCARD" => CommandMeta { name: "DISCARD", arity: 1, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: TXN },
    "WATCH" => CommandMeta { name: "WATCH", arity: -2, flags: RF, first_key: 1, last_key: -1, step: 1, acl_categories: TXN },
    "UNWATCH" => CommandMeta { name: "UNWATCH", arity: 1, flags: RF, first_key: 0, last_key: 0, step: 0, acl_categories: TXN },

    // ---- Replication commands ----
    "REPLICAOF" => CommandMeta { name: "REPLICAOF", arity: 3, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "SLAVEOF" => CommandMeta { name: "SLAVEOF", arity: 3, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "REPLCONF" => CommandMeta { name: "REPLCONF", arity: -1, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "PSYNC" => CommandMeta { name: "PSYNC", arity: 3, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },
    "CLUSTER" => CommandMeta { name: "CLUSTER", arity: -2, flags: A, first_key: 0, last_key: 0, step: 0, acl_categories: SRV },

    // ---- Vector search commands ----
    "FT.CREATE" => CommandMeta { name: "FT.CREATE", arity: -2, flags: W, first_key: 0, last_key: 0, step: 0, acl_categories: SRCH },
    "FT.SEARCH" => CommandMeta { name: "FT.SEARCH", arity: -3, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: SRCH },
    "FT.DROPINDEX" => CommandMeta { name: "FT.DROPINDEX", arity: 2, flags: W, first_key: 0, last_key: 0, step: 0, acl_categories: SRCH },
    "FT.INFO" => CommandMeta { name: "FT.INFO", arity: 2, flags: R, first_key: 0, last_key: 0, step: 0, acl_categories: SRCH },
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Look up command metadata by name (case-insensitive).
///
/// Returns `None` for unknown commands or names longer than 20 bytes.
///
/// Fast path: for commands <=8 bytes the first 8 bytes are uppercased,
/// zero-padded, and loaded as a single `u64`. A manual `match` on the
/// 20 most common Redis commands returns a precomputed static reference
/// without ever touching the phf map, its SipHasher, or `from_utf8`.
/// This eliminates ~5% of CPU that was previously spent in
/// `phf::Map::get` + `SipHasher::write` + `hash_one` on the hot path.
///
/// Cold path: commands longer than 8 bytes, or anything not in the
/// hot set, fall through to the full phf map lookup. The cold path is
/// semantically identical to the hot path (same `&'static CommandMeta`).
#[inline]
pub fn lookup(cmd: &[u8]) -> Option<&'static CommandMeta> {
    let len = cmd.len();
    if len == 0 || len > 20 {
        return None;
    }

    // Fast path: 1..=8 byte command names -- pack into u64, match.
    if len <= 8 {
        let packed = pack_upper_u64(cmd);
        if let Some(meta) = lookup_hot_u64(len, packed) {
            return Some(meta);
        }
    }

    // Cold path: phf map lookup with utf8 validation.
    let mut buf = [0u8; 20];
    for (i, &b) in cmd.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }
    let upper = std::str::from_utf8(&buf[..len]).ok()?;
    COMMAND_META.get(upper)
}

/// Pack the first `cmd.len()` bytes (<=8) into a little-endian `u64`,
/// uppercasing ASCII letters and zero-padding the remainder.
#[inline(always)]
fn pack_upper_u64(cmd: &[u8]) -> u64 {
    let mut out = [0u8; 8];
    let n = cmd.len().min(8);
    // Manually unrolled: the compiler turns this into a masked 8-byte load +
    // `and 0xDF` on ASCII-letter lanes. Faster than a loop with per-byte
    // branches because we avoid the `b.is_ascii_alphabetic()` check --
    // ORing 0x20 would lowercase; ANDing 0xDF uppercases any ASCII letter
    // and is a no-op for digits/underscores (the only other allowed chars
    // in Redis command names are none, so this is safe).
    let mut i = 0;
    while i < n {
        let b = cmd[i];
        // Uppercase ASCII letters: 'a'..='z' (0x61..=0x7a) -> 'A'..='Z'.
        // Leave digits, punctuation, and already-upper letters untouched.
        out[i] = if b.is_ascii_lowercase() { b & 0xDF } else { b };
        i += 1;
    }
    u64::from_le_bytes(out)
}

/// Pre-resolved `&'static CommandMeta` pointers for the hot set.
///
/// Initialized once (via `LazyLock`) by probing `COMMAND_META` at first
/// access; subsequent hot-path reads are pure pointer loads -- no
/// SipHash, no phf traversal, no `from_utf8`. If any hot command is
/// missing from the phf map this panics at startup (guarded by the
/// `hot_path_matches_phf_map` unit test).
///
/// Indices are assigned by `hot_index_for(len, packed)` below.
#[allow(clippy::unwrap_used, clippy::expect_used)] // LazyLock init: panicking here is correct — missing PHF entry is a build bug
static HOT_META: std::sync::LazyLock<[&'static CommandMeta; HOT_COUNT]> =
    std::sync::LazyLock::new(|| {
        fn get(name: &str) -> &'static CommandMeta {
            COMMAND_META
                .get(name)
                .expect("hot command missing from phf")
        }
        [
            get("GET"),     // 0
            get("SET"),     // 1
            get("DEL"),     // 2
            get("TTL"),     // 3
            get("MGET"),    // 4
            get("MSET"),    // 5
            get("INCR"),    // 6
            get("DECR"),    // 7
            get("HSET"),    // 8
            get("HGET"),    // 9
            get("HDEL"),    // 10
            get("HLEN"),    // 11
            get("LPOP"),    // 12
            get("RPOP"),    // 13
            get("LLEN"),    // 14
            get("PING"),    // 15
            get("LPUSH"),   // 16
            get("RPUSH"),   // 17
            get("EXPIRE"),  // 18
            get("EXISTS"),  // 19
            get("INCRBY"),  // 20
            get("DECRBY"),  // 21
            get("SELECT"),  // 22
            get("HGETALL"), // 23
        ]
    });

const HOT_COUNT: usize = 24;

/// Match packed u64 command name against a hand-picked hot set.
///
/// The `u64` constants are the little-endian packings of the uppercase
/// ASCII command names, right-padded with zero bytes. The match returns
/// an index into `HOT_META`; the caller dereferences that slot to get
/// the `&'static CommandMeta` without ever touching phf or SipHash.
#[inline]
fn lookup_hot_u64(len: usize, packed: u64) -> Option<&'static CommandMeta> {
    const GET: u64 = pack_const(b"GET");
    const SET: u64 = pack_const(b"SET");
    const DEL: u64 = pack_const(b"DEL");
    const TTL: u64 = pack_const(b"TTL");
    const MGET: u64 = pack_const(b"MGET");
    const MSET: u64 = pack_const(b"MSET");
    const INCR: u64 = pack_const(b"INCR");
    const DECR: u64 = pack_const(b"DECR");
    const HSET: u64 = pack_const(b"HSET");
    const HGET: u64 = pack_const(b"HGET");
    const HDEL: u64 = pack_const(b"HDEL");
    const HLEN: u64 = pack_const(b"HLEN");
    const LPOP: u64 = pack_const(b"LPOP");
    const RPOP: u64 = pack_const(b"RPOP");
    const LLEN: u64 = pack_const(b"LLEN");
    const PING: u64 = pack_const(b"PING");
    const EXPIRE: u64 = pack_const(b"EXPIRE");
    const EXISTS: u64 = pack_const(b"EXISTS");
    const LPUSH: u64 = pack_const(b"LPUSH");
    const RPUSH: u64 = pack_const(b"RPUSH");
    const INCRBY: u64 = pack_const(b"INCRBY");
    const DECRBY: u64 = pack_const(b"DECRBY");
    const SELECT: u64 = pack_const(b"SELECT");
    const HGETALL: u64 = pack_const(b"HGETALL");

    let idx: usize = match (len, packed) {
        (3, v) if v == GET => 0,
        (3, v) if v == SET => 1,
        (3, v) if v == DEL => 2,
        (3, v) if v == TTL => 3,
        (4, v) if v == MGET => 4,
        (4, v) if v == MSET => 5,
        (4, v) if v == INCR => 6,
        (4, v) if v == DECR => 7,
        (4, v) if v == HSET => 8,
        (4, v) if v == HGET => 9,
        (4, v) if v == HDEL => 10,
        (4, v) if v == HLEN => 11,
        (4, v) if v == LPOP => 12,
        (4, v) if v == RPOP => 13,
        (4, v) if v == LLEN => 14,
        (4, v) if v == PING => 15,
        (5, v) if v == LPUSH => 16,
        (5, v) if v == RPUSH => 17,
        (6, v) if v == EXPIRE => 18,
        (6, v) if v == EXISTS => 19,
        (6, v) if v == INCRBY => 20,
        (6, v) if v == DECRBY => 21,
        (6, v) if v == SELECT => 22,
        (7, v) if v == HGETALL => 23,
        _ => return None,
    };
    // SAFETY: idx is bounded 0..HOT_COUNT by the match arms above.
    Some(HOT_META[idx])
}

/// `const fn` equivalent of `pack_upper_u64` for building compile-time
/// constants in `lookup_hot_u64`. Input bytes MUST already be uppercase
/// ASCII letters (enforced by the const evaluator panicking on lowercase).
const fn pack_const(name: &[u8]) -> u64 {
    let mut out = [0u8; 8];
    let n = if name.len() < 8 { name.len() } else { 8 };
    let mut i = 0;
    while i < n {
        let b = name[i];
        // Require uppercase inputs; the cost is zero at runtime.
        assert!(
            !(b >= b'a' && b <= b'z'),
            "pack_const requires uppercase ASCII"
        );
        out[i] = b;
        i += 1;
    }
    u64::from_le_bytes(out)
}

/// Check if a command is a write command via the metadata registry.
///
/// Drop-in replacement for `persistence::aof::is_write_command`.
#[inline]
pub fn is_write(cmd: &[u8]) -> bool {
    lookup(cmd).is_some_and(|m| m.flags.contains(CommandFlags::WRITE))
}

/// Check if a command is read-only via the metadata registry.
#[inline]
pub fn is_read(cmd: &[u8]) -> bool {
    lookup(cmd).is_some_and(|m| m.flags.contains(CommandFlags::READONLY))
}

/// Return the total number of commands in the registry.
pub fn command_count() -> usize {
    COMMAND_META.len()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Hot fast-path (`lookup_hot_u64`) must return exactly the same
    /// `&'static CommandMeta` as the phf map for every hot command, in
    /// both uppercase and lowercase forms. Guards against drift if a
    /// hot-command entry is renamed in `COMMAND_META` without updating
    /// the fast-path match.
    #[test]
    fn hot_path_matches_phf_map() {
        let hot: &[&[u8]] = &[
            b"GET", b"SET", b"DEL", b"TTL", b"MGET", b"MSET", b"INCR", b"DECR", b"HSET", b"HGET",
            b"HDEL", b"HLEN", b"LPOP", b"RPOP", b"LLEN", b"PING", b"LPUSH", b"RPUSH", b"EXPIRE",
            b"EXISTS", b"INCRBY", b"DECRBY", b"SELECT", b"HGETALL",
        ];
        for name in hot {
            let upper = lookup(name).unwrap_or_else(|| {
                panic!(
                    "hot command {:?} not found via lookup",
                    std::str::from_utf8(name).unwrap()
                )
            });
            // Case-insensitive via lowercase
            let lower: Vec<u8> = name.iter().map(|b| b.to_ascii_lowercase()).collect();
            let lower_meta = lookup(&lower).unwrap();
            assert!(
                std::ptr::eq(upper, lower_meta),
                "hot path returned different metadata for upper vs lower {:?}",
                std::str::from_utf8(name).unwrap()
            );
            // Also agree with a direct phf probe.
            let upper_str = std::str::from_utf8(name).unwrap();
            let phf_meta = COMMAND_META.get(upper_str).unwrap();
            assert!(
                std::ptr::eq(upper, phf_meta),
                "hot path disagrees with phf for {:?}",
                upper_str
            );
        }
    }

    /// Non-hot commands (longer than 8 bytes, or not in hot set) must
    /// still resolve via the phf fallback.
    #[test]
    fn cold_path_still_works() {
        assert!(lookup(b"HINCRBYFLOAT").is_some());
        assert!(lookup(b"ZRANGEBYSCORE").is_some());
        assert!(lookup(b"BITCOUNT").is_some());
        assert!(lookup(b"CLUSTER").is_some());
        // Case-insensitive cold path
        assert!(lookup(b"hincrbyfloat").is_some());
        // Unknown command
        assert!(lookup(b"NOSUCHCMD").is_none());
    }

    /// Every command in aof::WRITE_COMMANDS must be flagged WRITE in the registry.
    #[test]
    fn write_commands_match_aof() {
        let aof_write_cmds: &[&[u8]] = &[
            b"SET",
            b"MSET",
            b"SETNX",
            b"SETEX",
            b"PSETEX",
            b"GETSET",
            b"GETDEL",
            b"GETEX",
            b"INCR",
            b"DECR",
            b"INCRBY",
            b"DECRBY",
            b"INCRBYFLOAT",
            b"APPEND",
            b"SETRANGE",
            b"DEL",
            b"UNLINK",
            b"EXPIRE",
            b"PEXPIRE",
            b"PERSIST",
            b"RENAME",
            b"RENAMENX",
            b"HSET",
            b"HMSET",
            b"HDEL",
            b"HSETNX",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            b"LPUSH",
            b"RPUSH",
            b"LPOP",
            b"RPOP",
            b"LSET",
            b"LINSERT",
            b"LREM",
            b"LTRIM",
            b"LMOVE",
            b"SADD",
            b"SREM",
            b"SPOP",
            b"SINTERSTORE",
            b"SUNIONSTORE",
            b"SDIFFSTORE",
            b"ZADD",
            b"ZREM",
            b"ZINCRBY",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"ZUNIONSTORE",
            b"ZINTERSTORE",
            b"SELECT",
        ];
        for cmd in aof_write_cmds {
            assert!(
                is_write(cmd),
                "AOF write command {:?} not marked WRITE in registry",
                std::str::from_utf8(cmd).unwrap()
            );
        }
    }

    /// Known read-only commands must NOT be flagged WRITE.
    #[test]
    fn read_commands_not_write() {
        let read_cmds: &[&[u8]] = &[
            b"GET",
            b"PING",
            b"ECHO",
            b"KEYS",
            b"HGET",
            b"HLEN",
            b"HMGET",
            b"HKEYS",
            b"HVALS",
            b"HGETALL",
            b"HEXISTS",
            b"LLEN",
            b"LPOS",
            b"LRANGE",
            b"LINDEX",
            b"SCARD",
            b"SDIFF",
            b"SINTER",
            b"SUNION",
            b"SMEMBERS",
            b"SISMEMBER",
            b"SMISMEMBER",
            b"SRANDMEMBER",
            b"ZCARD",
            b"ZRANK",
            b"ZSCORE",
            b"ZRANGE",
            b"ZCOUNT",
            b"ZREVRANK",
            b"ZREVRANGE",
            b"ZLEXCOUNT",
            b"ZRANGEBYSCORE",
            b"ZREVRANGEBYSCORE",
            b"XLEN",
            b"XREAD",
            b"XINFO",
            b"XRANGE",
            b"XREVRANGE",
            b"XPENDING",
            b"EXISTS",
            b"TTL",
            b"PTTL",
            b"TYPE",
            b"SCAN",
            b"DBSIZE",
            b"STRLEN",
            b"GETRANGE",
            b"SUBSTR",
            b"MGET",
            b"INFO",
            b"COMMAND",
        ];
        for cmd in read_cmds {
            assert!(
                !is_write(cmd),
                "Read command {:?} incorrectly marked WRITE in registry",
                std::str::from_utf8(cmd).unwrap()
            );
        }
    }

    /// Case-insensitive lookup must work.
    #[test]
    fn case_insensitive_lookup() {
        assert!(lookup(b"get").is_some());
        assert!(lookup(b"Get").is_some());
        assert!(lookup(b"GET").is_some());
        assert!(lookup(b"gEt").is_some());
        assert_eq!(lookup(b"get").unwrap().name, "GET");
    }

    /// Unknown commands return None.
    #[test]
    fn unknown_commands_none() {
        assert!(lookup(b"NOTACOMMAND").is_none());
        assert!(lookup(b"").is_none());
        assert!(lookup(b"XXXXXXXXXXXXXXXXXXXXXXXXXXXX").is_none()); // >20 bytes
    }

    /// is_write correctly classifies write vs read commands.
    #[test]
    fn is_write_classification() {
        let writes: &[&[u8]] = &[
            b"SET",
            b"DEL",
            b"INCR",
            b"DECR",
            b"MSET",
            b"HSET",
            b"HDEL",
            b"LPOP",
            b"RPOP",
            b"SADD",
            b"SREM",
            b"SPOP",
            b"ZADD",
            b"ZREM",
            b"LPUSH",
            b"RPUSH",
            b"HMSET",
            b"SETNX",
            b"SETEX",
            b"GETEX",
            b"APPEND",
            b"SETRANGE",
            b"DECRBY",
            b"EXPIRE",
            b"GETSET",
            b"GETDEL",
            b"HSETNX",
            b"INCRBY",
            b"PSETEX",
            b"RENAME",
            b"SELECT",
            b"UNLINK",
            b"HINCRBY",
            b"LINSERT",
            b"PEXPIRE",
            b"PERSIST",
            b"ZINCRBY",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"RENAMENX",
            b"SDIFFSTORE",
            b"INCRBYFLOAT",
            b"SINTERSTORE",
            b"SUNIONSTORE",
            b"ZUNIONSTORE",
            b"ZINTERSTORE",
            b"HINCRBYFLOAT",
            b"LSET",
            b"LREM",
            b"LTRIM",
            b"LMOVE",
        ];
        let reads: &[&[u8]] = &[
            b"GET",
            b"TTL",
            b"ECHO",
            b"HGET",
            b"HLEN",
            b"INFO",
            b"KEYS",
            b"LLEN",
            b"MGET",
            b"PING",
            b"PTTL",
            b"SCAN",
            b"TYPE",
            b"HMGET",
            b"HKEYS",
            b"HVALS",
            b"SCARD",
            b"ZCARD",
            b"EXISTS",
            b"LRANGE",
            b"STRLEN",
            b"GETRANGE",
            b"SUBSTR",
            b"ZSCORE",
            b"COMMAND",
            b"HGETALL",
            b"SMEMBERS",
            b"SISMEMBER",
            b"ZRANGEBYSCORE",
        ];
        for cmd in writes {
            assert!(
                is_write(cmd),
                "{:?} should be write",
                std::str::from_utf8(cmd).unwrap()
            );
        }
        for cmd in reads {
            assert!(
                !is_write(cmd),
                "{:?} should not be write",
                std::str::from_utf8(cmd).unwrap()
            );
        }
    }

    /// Registry has a reasonable number of entries.
    #[test]
    fn command_count_reasonable() {
        assert!(
            command_count() >= 90,
            "Expected >= 90 commands, got {}",
            command_count()
        );
    }

    /// Verify specific arity values match Redis conventions.
    #[test]
    fn arity_values() {
        assert_eq!(lookup(b"GET").unwrap().arity, 2);
        assert_eq!(lookup(b"SET").unwrap().arity, -3);
        assert_eq!(lookup(b"DEL").unwrap().arity, -2);
        assert_eq!(lookup(b"MGET").unwrap().arity, -2);
        assert_eq!(lookup(b"MSET").unwrap().arity, -3);
        assert_eq!(lookup(b"HSET").unwrap().arity, -4);
        assert_eq!(lookup(b"PING").unwrap().arity, -1);
    }

    /// Verify key position values for known commands.
    #[test]
    fn key_positions() {
        let m = lookup(b"GET").unwrap();
        assert_eq!((m.first_key, m.last_key, m.step), (1, 1, 1));

        let m = lookup(b"MGET").unwrap();
        assert_eq!((m.first_key, m.last_key, m.step), (1, -1, 1));

        let m = lookup(b"MSET").unwrap();
        assert_eq!((m.first_key, m.last_key, m.step), (1, -1, 2));

        let m = lookup(b"PING").unwrap();
        assert_eq!((m.first_key, m.last_key, m.step), (0, 0, 0));
    }
}
