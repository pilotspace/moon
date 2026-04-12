import type { CommandInfo } from "@/types/console";

/** All 206 Moon-supported RESP commands with parameter hints */
export const COMMANDS: CommandInfo[] = [
  // ── String (20) ──
  { name: "GET", summary: "Get the value of a key", args: "key", group: "string" },
  { name: "SET", summary: "Set the value of a key", args: "key value [EX seconds] [PX ms] [EXAT unix] [PXAT unix-ms] [NX|XX] [KEEPTTL] [GET]", group: "string" },
  { name: "SETNX", summary: "Set key if it does not exist", args: "key value", group: "string" },
  { name: "SETEX", summary: "Set key with expiration in seconds", args: "key seconds value", group: "string" },
  { name: "PSETEX", summary: "Set key with expiration in milliseconds", args: "key milliseconds value", group: "string" },
  { name: "MGET", summary: "Get values of multiple keys", args: "key [key ...]", group: "string" },
  { name: "MSET", summary: "Set multiple keys", args: "key value [key value ...]", group: "string" },
  { name: "MSETNX", summary: "Set multiple keys only if none exist", args: "key value [key value ...]", group: "string" },
  { name: "GETSET", summary: "Set key and return old value", args: "key value", group: "string" },
  { name: "GETDEL", summary: "Get and delete key", args: "key", group: "string" },
  { name: "GETEX", summary: "Get key and optionally set expiration", args: "key [EX seconds] [PX ms] [EXAT unix] [PXAT unix-ms] [PERSIST]", group: "string" },
  { name: "GETRANGE", summary: "Get substring of string value", args: "key start end", group: "string" },
  { name: "SETRANGE", summary: "Overwrite part of string at offset", args: "key offset value", group: "string" },
  { name: "STRLEN", summary: "Get length of string value", args: "key", group: "string" },
  { name: "APPEND", summary: "Append value to key", args: "key value", group: "string" },
  { name: "INCR", summary: "Increment integer value by 1", args: "key", group: "string" },
  { name: "INCRBY", summary: "Increment integer value by amount", args: "key increment", group: "string" },
  { name: "INCRBYFLOAT", summary: "Increment float value by amount", args: "key increment", group: "string" },
  { name: "DECR", summary: "Decrement integer value by 1", args: "key", group: "string" },
  { name: "DECRBY", summary: "Decrement integer value by amount", args: "key decrement", group: "string" },

  // ── Hash (15) ──
  { name: "HGET", summary: "Get value of a hash field", args: "key field", group: "hash" },
  { name: "HSET", summary: "Set hash field(s)", args: "key field value [field value ...]", group: "hash" },
  { name: "HSETNX", summary: "Set hash field only if not exists", args: "key field value", group: "hash" },
  { name: "HMGET", summary: "Get values of multiple hash fields", args: "key field [field ...]", group: "hash" },
  { name: "HMSET", summary: "Set multiple hash fields", args: "key field value [field value ...]", group: "hash" },
  { name: "HDEL", summary: "Delete hash field(s)", args: "key field [field ...]", group: "hash" },
  { name: "HEXISTS", summary: "Check if hash field exists", args: "key field", group: "hash" },
  { name: "HGETALL", summary: "Get all fields and values", args: "key", group: "hash" },
  { name: "HKEYS", summary: "Get all hash field names", args: "key", group: "hash" },
  { name: "HVALS", summary: "Get all hash values", args: "key", group: "hash" },
  { name: "HLEN", summary: "Get number of hash fields", args: "key", group: "hash" },
  { name: "HINCRBY", summary: "Increment hash field integer value", args: "key field increment", group: "hash" },
  { name: "HINCRBYFLOAT", summary: "Increment hash field float value", args: "key field increment", group: "hash" },
  { name: "HSCAN", summary: "Iterate hash fields", args: "key cursor [MATCH pattern] [COUNT count]", group: "hash" },
  { name: "HRANDFIELD", summary: "Get random hash field(s)", args: "key [count [WITHVALUES]]", group: "hash" },

  // ── List (18) ──
  { name: "LPUSH", summary: "Prepend element(s) to list", args: "key element [element ...]", group: "list" },
  { name: "RPUSH", summary: "Append element(s) to list", args: "key element [element ...]", group: "list" },
  { name: "LPOP", summary: "Remove and get first element", args: "key [count]", group: "list" },
  { name: "RPOP", summary: "Remove and get last element", args: "key [count]", group: "list" },
  { name: "LRANGE", summary: "Get range of list elements", args: "key start stop", group: "list" },
  { name: "LLEN", summary: "Get list length", args: "key", group: "list" },
  { name: "LINDEX", summary: "Get element by index", args: "key index", group: "list" },
  { name: "LSET", summary: "Set list element at index", args: "key index element", group: "list" },
  { name: "LINSERT", summary: "Insert element before or after pivot", args: "key BEFORE|AFTER pivot element", group: "list" },
  { name: "LREM", summary: "Remove elements from list", args: "key count element", group: "list" },
  { name: "LTRIM", summary: "Trim list to range", args: "key start stop", group: "list" },
  { name: "RPOPLPUSH", summary: "Pop from one list, push to another", args: "source destination", group: "list" },
  { name: "LMOVE", summary: "Move element between lists", args: "source destination LEFT|RIGHT LEFT|RIGHT", group: "list" },
  { name: "LPOS", summary: "Get index of element in list", args: "key element [RANK rank] [COUNT count] [MAXLEN len]", group: "list" },
  { name: "LMPOP", summary: "Pop element(s) from multiple lists", args: "numkeys key [key ...] LEFT|RIGHT [COUNT count]", group: "list" },
  { name: "BLPOP", summary: "Blocking left pop", args: "key [key ...] timeout", group: "list" },
  { name: "BRPOP", summary: "Blocking right pop", args: "key [key ...] timeout", group: "list" },
  { name: "BLMOVE", summary: "Blocking move between lists", args: "source destination LEFT|RIGHT LEFT|RIGHT timeout", group: "list" },

  // ── Set (16) ──
  { name: "SADD", summary: "Add member(s) to set", args: "key member [member ...]", group: "set" },
  { name: "SREM", summary: "Remove member(s) from set", args: "key member [member ...]", group: "set" },
  { name: "SMEMBERS", summary: "Get all set members", args: "key", group: "set" },
  { name: "SISMEMBER", summary: "Check if member exists in set", args: "key member", group: "set" },
  { name: "SMISMEMBER", summary: "Check multiple members exist in set", args: "key member [member ...]", group: "set" },
  { name: "SCARD", summary: "Get set cardinality", args: "key", group: "set" },
  { name: "SRANDMEMBER", summary: "Get random member(s)", args: "key [count]", group: "set" },
  { name: "SPOP", summary: "Remove and return random member(s)", args: "key [count]", group: "set" },
  { name: "SDIFF", summary: "Subtract sets", args: "key [key ...]", group: "set" },
  { name: "SDIFFSTORE", summary: "Subtract sets and store result", args: "destination key [key ...]", group: "set" },
  { name: "SINTER", summary: "Intersect sets", args: "key [key ...]", group: "set" },
  { name: "SINTERSTORE", summary: "Intersect sets and store result", args: "destination key [key ...]", group: "set" },
  { name: "SINTERCARD", summary: "Intersect sets and return cardinality", args: "numkeys key [key ...] [LIMIT limit]", group: "set" },
  { name: "SUNION", summary: "Union sets", args: "key [key ...]", group: "set" },
  { name: "SUNIONSTORE", summary: "Union sets and store result", args: "destination key [key ...]", group: "set" },
  { name: "SSCAN", summary: "Iterate set members", args: "key cursor [MATCH pattern] [COUNT count]", group: "set" },

  // ── Sorted Set (24) ──
  { name: "ZADD", summary: "Add member(s) with score(s)", args: "key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]", group: "zset" },
  { name: "ZREM", summary: "Remove member(s) from sorted set", args: "key member [member ...]", group: "zset" },
  { name: "ZSCORE", summary: "Get score of member", args: "key member", group: "zset" },
  { name: "ZRANK", summary: "Get rank of member (ascending)", args: "key member", group: "zset" },
  { name: "ZREVRANK", summary: "Get rank of member (descending)", args: "key member", group: "zset" },
  { name: "ZRANGE", summary: "Get range of members by index/score/lex", args: "key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]", group: "zset" },
  { name: "ZRANGEBYSCORE", summary: "Get members by score range", args: "key min max [WITHSCORES] [LIMIT offset count]", group: "zset" },
  { name: "ZREVRANGEBYSCORE", summary: "Get members by score range (descending)", args: "key max min [WITHSCORES] [LIMIT offset count]", group: "zset" },
  { name: "ZRANGEBYLEX", summary: "Get members by lex range", args: "key min max [LIMIT offset count]", group: "zset" },
  { name: "ZCARD", summary: "Get sorted set cardinality", args: "key", group: "zset" },
  { name: "ZCOUNT", summary: "Count members in score range", args: "key min max", group: "zset" },
  { name: "ZLEXCOUNT", summary: "Count members in lex range", args: "key min max", group: "zset" },
  { name: "ZINCRBY", summary: "Increment member score", args: "key increment member", group: "zset" },
  { name: "ZUNIONSTORE", summary: "Union sorted sets and store", args: "destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]", group: "zset" },
  { name: "ZINTERSTORE", summary: "Intersect sorted sets and store", args: "destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]", group: "zset" },
  { name: "ZRANDMEMBER", summary: "Get random member(s)", args: "key [count [WITHSCORES]]", group: "zset" },
  { name: "ZMSCORE", summary: "Get scores of multiple members", args: "key member [member ...]", group: "zset" },
  { name: "ZPOPMIN", summary: "Remove and return lowest-scored member(s)", args: "key [count]", group: "zset" },
  { name: "ZPOPMAX", summary: "Remove and return highest-scored member(s)", args: "key [count]", group: "zset" },
  { name: "ZSCAN", summary: "Iterate sorted set members", args: "key cursor [MATCH pattern] [COUNT count]", group: "zset" },
  { name: "ZRANGESTORE", summary: "Store a range of sorted set members", args: "dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]", group: "zset" },
  { name: "ZDIFF", summary: "Subtract sorted sets", args: "numkeys key [key ...] [WITHSCORES]", group: "zset" },
  { name: "ZDIFFSTORE", summary: "Subtract sorted sets and store", args: "destination numkeys key [key ...]", group: "zset" },
  { name: "ZINTER", summary: "Intersect sorted sets", args: "numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]", group: "zset" },
  { name: "ZUNION", summary: "Union sorted sets", args: "numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]", group: "zset" },

  // ── Key (23) ──
  { name: "DEL", summary: "Delete key(s)", args: "key [key ...]", group: "key" },
  { name: "EXISTS", summary: "Check if key(s) exist", args: "key [key ...]", group: "key" },
  { name: "EXPIRE", summary: "Set key expiration in seconds", args: "key seconds [NX|XX|GT|LT]", group: "key" },
  { name: "EXPIREAT", summary: "Set key expiration at unix timestamp", args: "key unix-time [NX|XX|GT|LT]", group: "key" },
  { name: "EXPIRETIME", summary: "Get key expiration as unix timestamp", args: "key", group: "key" },
  { name: "TTL", summary: "Get key time-to-live in seconds", args: "key", group: "key" },
  { name: "PTTL", summary: "Get key time-to-live in milliseconds", args: "key", group: "key" },
  { name: "PEXPIRE", summary: "Set key expiration in milliseconds", args: "key milliseconds [NX|XX|GT|LT]", group: "key" },
  { name: "PEXPIREAT", summary: "Set key expiration at unix ms timestamp", args: "key unix-time-ms [NX|XX|GT|LT]", group: "key" },
  { name: "PERSIST", summary: "Remove key expiration", args: "key", group: "key" },
  { name: "TYPE", summary: "Get key type", args: "key", group: "key" },
  { name: "RENAME", summary: "Rename key", args: "key newkey", group: "key" },
  { name: "RENAMENX", summary: "Rename key only if new name doesn't exist", args: "key newkey", group: "key" },
  { name: "KEYS", summary: "Find keys matching pattern", args: "pattern", group: "key" },
  { name: "SCAN", summary: "Incrementally iterate keys", args: "cursor [MATCH pattern] [COUNT count] [TYPE type]", group: "key" },
  { name: "RANDOMKEY", summary: "Get a random key", args: "", group: "key" },
  { name: "OBJECT", summary: "Inspect key internals", args: "subcommand [arguments]", group: "key" },
  { name: "DUMP", summary: "Serialize key value", args: "key", group: "key" },
  { name: "RESTORE", summary: "Deserialize and set key value", args: "key ttl serialized-value [REPLACE]", group: "key" },
  { name: "SORT", summary: "Sort elements in list/set/zset", args: "key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]", group: "key" },
  { name: "TOUCH", summary: "Touch key(s) to update last access", args: "key [key ...]", group: "key" },
  { name: "UNLINK", summary: "Delete key(s) asynchronously", args: "key [key ...]", group: "key" },
  { name: "COPY", summary: "Copy key to another key", args: "source destination [DB destination-db] [REPLACE]", group: "key" },
  { name: "MOVE", summary: "Move key to another database", args: "key db", group: "key" },
  { name: "WAIT", summary: "Wait for replicas to acknowledge writes", args: "numreplicas timeout", group: "key" },

  // ── Server (24) ──
  { name: "PING", summary: "Test server connection", args: "[message]", group: "server" },
  { name: "ECHO", summary: "Echo message back", args: "message", group: "server" },
  { name: "SELECT", summary: "Select database", args: "index", group: "server" },
  { name: "AUTH", summary: "Authenticate to server", args: "[username] password", group: "server" },
  { name: "QUIT", summary: "Close connection", args: "", group: "server" },
  { name: "INFO", summary: "Get server information", args: "[section ...]", group: "server" },
  { name: "DBSIZE", summary: "Get number of keys in database", args: "", group: "server" },
  { name: "FLUSHDB", summary: "Delete all keys in current database", args: "[ASYNC|SYNC]", group: "server" },
  { name: "FLUSHALL", summary: "Delete all keys in all databases", args: "[ASYNC|SYNC]", group: "server" },
  { name: "SAVE", summary: "Synchronous RDB save", args: "", group: "server" },
  { name: "BGSAVE", summary: "Background RDB save", args: "[SCHEDULE]", group: "server" },
  { name: "BGREWRITEAOF", summary: "Background AOF rewrite", args: "", group: "server" },
  { name: "LASTSAVE", summary: "Get unix timestamp of last save", args: "", group: "server" },
  { name: "TIME", summary: "Get server time", args: "", group: "server" },
  { name: "CONFIG", summary: "Get/set/reset server configuration", args: "subcommand [arguments]", group: "server" },
  { name: "CLIENT", summary: "Manage client connections", args: "subcommand [arguments]", group: "server" },
  { name: "SLOWLOG", summary: "Manage slow query log", args: "subcommand [arguments]", group: "server" },
  { name: "DEBUG", summary: "Debug commands", args: "subcommand [arguments]", group: "server" },
  { name: "COMMAND", summary: "Get command details", args: "[subcommand [arguments]]", group: "server" },
  { name: "MEMORY", summary: "Memory usage and diagnostics", args: "subcommand [arguments]", group: "server" },
  { name: "LATENCY", summary: "Latency monitoring", args: "subcommand [arguments]", group: "server" },
  { name: "MODULE", summary: "Manage modules", args: "subcommand [arguments]", group: "server" },
  { name: "RESET", summary: "Reset connection state", args: "", group: "server" },
  { name: "HELLO", summary: "Switch protocol and authenticate", args: "[protover [AUTH username password] [SETNAME clientname]]", group: "server" },

  // ── Pub/Sub (8) ──
  { name: "SUBSCRIBE", summary: "Subscribe to channel(s)", args: "channel [channel ...]", group: "pubsub" },
  { name: "UNSUBSCRIBE", summary: "Unsubscribe from channel(s)", args: "[channel [channel ...]]", group: "pubsub" },
  { name: "PUBLISH", summary: "Publish message to channel", args: "channel message", group: "pubsub" },
  { name: "PSUBSCRIBE", summary: "Subscribe to pattern(s)", args: "pattern [pattern ...]", group: "pubsub" },
  { name: "PUNSUBSCRIBE", summary: "Unsubscribe from pattern(s)", args: "[pattern [pattern ...]]", group: "pubsub" },
  { name: "SSUBSCRIBE", summary: "Subscribe to shard channel(s)", args: "shardchannel [shardchannel ...]", group: "pubsub" },
  { name: "SUNSUBSCRIBE", summary: "Unsubscribe from shard channel(s)", args: "[shardchannel [shardchannel ...]]", group: "pubsub" },
  { name: "PUBSUB", summary: "Pub/Sub introspection", args: "subcommand [arguments]", group: "pubsub" },

  // ── Stream (13) ──
  { name: "XADD", summary: "Append entry to stream", args: "key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]", group: "stream" },
  { name: "XLEN", summary: "Get stream length", args: "key", group: "stream" },
  { name: "XRANGE", summary: "Get range of stream entries", args: "key start end [COUNT count]", group: "stream" },
  { name: "XREVRANGE", summary: "Get range of stream entries (reverse)", args: "key end start [COUNT count]", group: "stream" },
  { name: "XREAD", summary: "Read entries from stream(s)", args: "[COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]", group: "stream" },
  { name: "XTRIM", summary: "Trim stream to max length", args: "key MAXLEN|MINID [=|~] threshold", group: "stream" },
  { name: "XDEL", summary: "Delete stream entry(s)", args: "key id [id ...]", group: "stream" },
  { name: "XINFO", summary: "Stream introspection", args: "subcommand [arguments]", group: "stream" },
  { name: "XACK", summary: "Acknowledge stream message(s)", args: "key group id [id ...]", group: "stream" },
  { name: "XCLAIM", summary: "Claim stream message ownership", args: "key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]", group: "stream" },
  { name: "XAUTOCLAIM", summary: "Auto-claim idle stream messages", args: "key group consumer min-idle-time start [COUNT count] [JUSTID]", group: "stream" },
  { name: "XPENDING", summary: "Get pending stream entries", args: "key group [[IDLE min-idle-time] start end count [consumer]]", group: "stream" },
  { name: "XGROUP", summary: "Manage consumer groups", args: "subcommand [arguments]", group: "stream" },

  // ── Script (3) ──
  { name: "EVAL", summary: "Execute Lua script", args: "script numkeys [key ...] [arg ...]", group: "script" },
  { name: "EVALSHA", summary: "Execute cached Lua script by SHA", args: "sha1 numkeys [key ...] [arg ...]", group: "script" },
  { name: "SCRIPT", summary: "Manage Lua script cache", args: "subcommand [arguments]", group: "script" },

  // ── Transaction (5) ──
  { name: "MULTI", summary: "Start transaction", args: "", group: "transaction" },
  { name: "EXEC", summary: "Execute queued commands", args: "", group: "transaction" },
  { name: "DISCARD", summary: "Discard queued commands", args: "", group: "transaction" },
  { name: "WATCH", summary: "Watch key(s) for optimistic locking", args: "key [key ...]", group: "transaction" },
  { name: "UNWATCH", summary: "Unwatch all watched keys", args: "", group: "transaction" },

  // ── Graph (10) ──
  { name: "GRAPH.QUERY", summary: "Execute a Cypher query", args: "graph query [--compact]", group: "graph" },
  { name: "GRAPH.RO_QUERY", summary: "Execute a read-only Cypher query", args: "graph query [--compact]", group: "graph" },
  { name: "GRAPH.DELETE", summary: "Delete a graph", args: "graph", group: "graph" },
  { name: "GRAPH.EXPLAIN", summary: "Show query execution plan", args: "graph query", group: "graph" },
  { name: "GRAPH.PROFILE", summary: "Execute and profile a query", args: "graph query", group: "graph" },
  { name: "GRAPH.SLOWLOG", summary: "Get graph slow query log", args: "graph [subcommand]", group: "graph" },
  { name: "GRAPH.CONFIG", summary: "Get/set graph configuration", args: "subcommand [arguments]", group: "graph" },
  { name: "GRAPH.INFO", summary: "Get graph information", args: "graph", group: "graph" },
  { name: "GRAPH.LIST", summary: "List all graphs", args: "", group: "graph" },
  { name: "GRAPH.CONSTRAINT", summary: "Manage graph constraints", args: "subcommand graph [arguments]", group: "graph" },

  // ── Vector / Search (5) ──
  { name: "FT.CREATE", summary: "Create a search index", args: "index [ON HASH|JSON] [PREFIX count prefix ...] SCHEMA field type [field type ...]", group: "search" },
  { name: "FT.DROPINDEX", summary: "Delete a search index", args: "index [DD]", group: "search" },
  { name: "FT.INFO", summary: "Get index information", args: "index", group: "search" },
  { name: "FT.SEARCH", summary: "Search an index", args: "index query [KNN k @field $vec] [RETURN count field ...] [LIMIT offset num]", group: "search" },
  { name: "FT.COMPACT", summary: "Force compaction of index segments", args: "index", group: "search" },

  // ── HyperLogLog (3) ──
  { name: "PFADD", summary: "Add elements to HyperLogLog", args: "key [element ...]", group: "hyperloglog" },
  { name: "PFCOUNT", summary: "Count unique elements", args: "key [key ...]", group: "hyperloglog" },
  { name: "PFMERGE", summary: "Merge HyperLogLog values", args: "destkey sourcekey [sourcekey ...]", group: "hyperloglog" },

  // ── Geo (7) ──
  { name: "GEOADD", summary: "Add geospatial item(s)", args: "key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]", group: "geo" },
  { name: "GEODIST", summary: "Distance between two members", args: "key member1 member2 [M|KM|FT|MI]", group: "geo" },
  { name: "GEOHASH", summary: "Get geohash of member(s)", args: "key member [member ...]", group: "geo" },
  { name: "GEOPOS", summary: "Get position of member(s)", args: "key member [member ...]", group: "geo" },
  { name: "GEORADIUS", summary: "Find members within radius", args: "key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC] [STORE key] [STOREDIST key]", group: "geo" },
  { name: "GEORADIUSBYMEMBER", summary: "Find members within radius of member", args: "key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]", group: "geo" },
  { name: "GEOSEARCH", summary: "Search geospatial members", args: "key [FROMMEMBER member] [FROMLONLAT longitude latitude] [BYRADIUS radius M|KM|FT|MI] [BYBOX width height M|KM|FT|MI] [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]", group: "geo" },
  { name: "GEOSEARCHSTORE", summary: "Search and store geospatial results", args: "destination source [FROMMEMBER member] [FROMLONLAT lon lat] [BYRADIUS radius unit] [BYBOX width height unit] [ASC|DESC] [COUNT count [ANY]] [STOREDIST]", group: "geo" },

  // ── Bitmap (6) ──
  { name: "SETBIT", summary: "Set bit at offset", args: "key offset value", group: "bitmap" },
  { name: "GETBIT", summary: "Get bit at offset", args: "key offset", group: "bitmap" },
  { name: "BITCOUNT", summary: "Count set bits", args: "key [start end [BYTE|BIT]]", group: "bitmap" },
  { name: "BITOP", summary: "Bitwise operation between keys", args: "AND|OR|XOR|NOT destkey key [key ...]", group: "bitmap" },
  { name: "BITFIELD", summary: "Perform bitfield operations", args: "key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]", group: "bitmap" },
  { name: "BITPOS", summary: "Find first set/clear bit", args: "key bit [start [end [BYTE|BIT]]]", group: "bitmap" },

  // ── ACL (1 compound) ──
  { name: "ACL", summary: "Access control list management", args: "subcommand [arguments]", group: "acl" },

  // ── Cluster (1 compound) ──
  { name: "CLUSTER", summary: "Cluster management", args: "subcommand [arguments]", group: "cluster" },

  // ── Object subcommands (4) ──
  { name: "OBJECT HELP", summary: "Show OBJECT subcommands", args: "", group: "key" },
  { name: "OBJECT ENCODING", summary: "Get internal encoding of key", args: "key", group: "key" },
  { name: "OBJECT FREQ", summary: "Get access frequency of key", args: "key", group: "key" },
  { name: "OBJECT IDLETIME", summary: "Get idle time of key", args: "key", group: "key" },
  { name: "OBJECT REFCOUNT", summary: "Get reference count of key", args: "key", group: "key" },

  // ── Client subcommands (7) ──
  { name: "CLIENT ID", summary: "Get current client ID", args: "", group: "server" },
  { name: "CLIENT GETNAME", summary: "Get connection name", args: "", group: "server" },
  { name: "CLIENT SETNAME", summary: "Set connection name", args: "name", group: "server" },
  { name: "CLIENT LIST", summary: "List client connections", args: "[TYPE normal|master|replica|pubsub]", group: "server" },
  { name: "CLIENT KILL", summary: "Kill client connection", args: "[ID client-id] [TYPE normal|master|replica|pubsub] [ADDR ip:port]", group: "server" },
  { name: "CLIENT INFO", summary: "Get current client info", args: "", group: "server" },
  { name: "CLIENT NO-EVICT", summary: "Set client no-evict mode", args: "ON|OFF", group: "server" },

  // ── Config subcommands (3) ──
  { name: "CONFIG GET", summary: "Get configuration parameter(s)", args: "parameter [parameter ...]", group: "server" },
  { name: "CONFIG SET", summary: "Set configuration parameter", args: "parameter value [parameter value ...]", group: "server" },
  { name: "CONFIG RESETSTAT", summary: "Reset server statistics", args: "", group: "server" },

  // ── XINFO subcommands (3) ──
  { name: "XINFO STREAM", summary: "Get stream information", args: "key [FULL [COUNT count]]", group: "stream" },
  { name: "XINFO GROUPS", summary: "Get consumer groups for stream", args: "key", group: "stream" },
  { name: "XINFO CONSUMERS", summary: "Get consumers in a group", args: "key group", group: "stream" },

  // ── XGROUP subcommands (4) ──
  { name: "XGROUP CREATE", summary: "Create consumer group", args: "key group id|$ [MKSTREAM] [ENTRIESREAD entries-read]", group: "stream" },
  { name: "XGROUP DESTROY", summary: "Destroy consumer group", args: "key group", group: "stream" },
  { name: "XGROUP DELCONSUMER", summary: "Delete consumer from group", args: "key group consumer", group: "stream" },
  { name: "XGROUP SETID", summary: "Set consumer group last-delivered-id", args: "key group id|$ [ENTRIESREAD entries-read]", group: "stream" },

  // ── Misc (1) ──
  { name: "SUBSTR", summary: "Get substring (deprecated, use GETRANGE)", args: "key start end", group: "string" },
  { name: "OBJECT HELP", summary: "Show OBJECT subcommands", args: "", group: "key" },
  { name: "LCS", summary: "Find longest common subsequence", args: "key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]", group: "string" },
  { name: "SRANDMEMBER", summary: "Get random member(s) from set", args: "key [count]", group: "set" },
  { name: "WAITAOF", summary: "Wait for AOF flush on replicas", args: "numlocal numreplicas timeout", group: "server" },
];

/**
 * Create a Monaco CompletionItemProvider for Moon RESP commands.
 * Register on the "resp" language ID.
 */
export function createCompletionProvider(
  monaco: typeof import("monaco-editor"),
): import("monaco-editor").languages.CompletionItemProvider {
  return {
    triggerCharacters: [],
    provideCompletionItems(model, position) {
      const word = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };

      // Also check for multi-word commands like "GRAPH." or "FT."
      const lineContent = model.getLineContent(position.lineNumber);
      const textBefore = lineContent.substring(0, position.column - 1).trimStart();

      const suggestions = COMMANDS.filter((cmd) => {
        const upper = textBefore.toUpperCase();
        // Match commands that start with what's typed
        return cmd.name.toUpperCase().startsWith(upper) || cmd.name.toUpperCase().startsWith(word.word.toUpperCase());
      }).map((cmd) => ({
        label: cmd.name,
        kind: monaco.languages.CompletionItemKind.Function,
        insertText: cmd.name + " ",
        detail: cmd.args,
        documentation: `[${cmd.group}] ${cmd.summary}`,
        range,
      }));

      return { suggestions };
    },
  };
}
