import type { languages } from "monaco-editor";

export const respLanguageId = "resp";

export const respLanguage: languages.IMonarchLanguage = {
  ignoreCase: true,
  keywords: [
    // String
    "GET", "SET", "SETNX", "SETEX", "PSETEX", "MGET", "MSET", "MSETNX",
    "GETSET", "GETDEL", "GETEX", "GETRANGE", "SETRANGE", "STRLEN",
    "APPEND", "INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
    // Hash
    "HGET", "HSET", "HSETNX", "HMGET", "HMSET", "HDEL", "HEXISTS",
    "HGETALL", "HKEYS", "HVALS", "HLEN", "HINCRBY", "HINCRBYFLOAT",
    "HSCAN", "HRANDFIELD",
    // List
    "LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN", "LINDEX",
    "LSET", "LINSERT", "LREM", "LTRIM", "RPOPLPUSH", "LMOVE", "LPOS",
    "LMPOP", "BLPOP", "BRPOP", "BLMOVE",
    // Set
    "SADD", "SREM", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD",
    "SRANDMEMBER", "SPOP", "SDIFF", "SDIFFSTORE", "SINTER",
    "SINTERSTORE", "SINTERCARD", "SUNION", "SUNIONSTORE", "SSCAN",
    // Sorted Set
    "ZADD", "ZREM", "ZSCORE", "ZRANK", "ZREVRANK", "ZRANGE",
    "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZRANGEBYLEX", "ZCARD",
    "ZCOUNT", "ZLEXCOUNT", "ZINCRBY", "ZUNIONSTORE", "ZINTERSTORE",
    "ZRANDMEMBER", "ZMSCORE", "ZPOPMIN", "ZPOPMAX", "ZSCAN",
    "ZRANGESTORE", "ZDIFF", "ZDIFFSTORE", "ZINTER", "ZUNION",
    // Key
    "DEL", "EXISTS", "EXPIRE", "EXPIREAT", "EXPIRETIME", "TTL", "PTTL",
    "PEXPIRE", "PEXPIREAT", "PERSIST", "TYPE", "RENAME", "RENAMENX",
    "KEYS", "SCAN", "RANDOMKEY", "OBJECT", "DUMP", "RESTORE", "SORT",
    "TOUCH", "UNLINK", "WAIT", "COPY", "MOVE",
    // Server
    "PING", "ECHO", "SELECT", "AUTH", "QUIT", "INFO", "DBSIZE",
    "FLUSHDB", "FLUSHALL", "SAVE", "BGSAVE", "BGREWRITEAOF",
    "LASTSAVE", "TIME", "CONFIG", "CLIENT", "SLOWLOG", "DEBUG",
    "COMMAND", "MEMORY", "LATENCY", "MODULE", "RESET", "HELLO",
    // Pub/Sub
    "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "PSUBSCRIBE", "PUNSUBSCRIBE",
    "SSUBSCRIBE", "SUNSUBSCRIBE", "PUBSUB",
    // Stream
    "XADD", "XLEN", "XRANGE", "XREVRANGE", "XREAD", "XTRIM",
    "XDEL", "XINFO", "XACK", "XCLAIM", "XAUTOCLAIM", "XPENDING",
    "XGROUP",
    // Script
    "EVAL", "EVALSHA", "SCRIPT",
    // Transaction
    "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
    // Graph
    "GRAPH.QUERY", "GRAPH.RO_QUERY", "GRAPH.DELETE", "GRAPH.EXPLAIN",
    "GRAPH.PROFILE", "GRAPH.SLOWLOG", "GRAPH.CONFIG", "GRAPH.INFO",
    "GRAPH.LIST", "GRAPH.CONSTRAINT",
    // Vector / Search
    "FT.CREATE", "FT.DROPINDEX", "FT.INFO", "FT.SEARCH", "FT.COMPACT",
    // ACL
    "ACL",
    // Cluster
    "CLUSTER",
  ],
  tokenizer: {
    root: [
      [/#.*$/, "comment"],
      [/"[^"]*"/, "string"],
      [/'[^']*'/, "string"],
      [/\b\d+(\.\d+)?\b/, "number"],
      [/[A-Z][A-Z0-9_.]*/, { cases: { "@keywords": "keyword", "@default": "identifier" } }],
      [/[a-z][a-zA-Z0-9_:{}]*/, "variable"],
      [/\*|\$/, "operator"],
    ],
  },
};

export const respThemeRules: { token: string; foreground: string }[] = [
  { token: "keyword", foreground: "569CD6" },
  { token: "string", foreground: "CE9178" },
  { token: "number", foreground: "B5CEA8" },
  { token: "comment", foreground: "6A9955" },
  { token: "variable", foreground: "9CDCFE" },
  { token: "operator", foreground: "D4D4D4" },
];
