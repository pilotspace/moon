pub mod client;
pub mod config;
pub mod connection;
pub mod hash;
pub mod key;
pub mod list;
pub mod persistence;
pub mod set;
pub mod sorted_set;
pub mod stream;
pub mod string;

use bytes::Bytes;

use crate::protocol::Frame;
use crate::storage::Database;

/// Result of command dispatch.
pub enum DispatchResult {
    /// Normal response to send back to the client.
    Response(Frame),
    /// Response to send, then close the connection (QUIT).
    Quit(Frame),
}

/// Dispatch a pre-extracted command to the appropriate handler.
///
/// `cmd` is the raw command name bytes (not uppercased).
/// `args` is the slice of argument frames (excluding the command name).
/// Uses `eq_ignore_ascii_case` for zero-allocation case-insensitive matching.
pub fn dispatch(
    db: &mut Database,
    cmd: &[u8],
    args: &[Frame],
    selected_db: &mut usize,
    db_count: usize,
) -> DispatchResult {
    if cmd.eq_ignore_ascii_case(b"PING") { return DispatchResult::Response(connection::ping(args)); }
    if cmd.eq_ignore_ascii_case(b"ECHO") { return DispatchResult::Response(connection::echo(args)); }
    if cmd.eq_ignore_ascii_case(b"QUIT") { return DispatchResult::Quit(Frame::SimpleString(Bytes::from_static(b"OK"))); }
    if cmd.eq_ignore_ascii_case(b"SELECT") { return DispatchResult::Response(connection::select(args, selected_db, db_count)); }
    if cmd.eq_ignore_ascii_case(b"COMMAND") { return DispatchResult::Response(connection::command(args)); }
    if cmd.eq_ignore_ascii_case(b"INFO") { return DispatchResult::Response(connection::info(db, args)); }
    // Key management commands
    if cmd.eq_ignore_ascii_case(b"DEL") { return DispatchResult::Response(key::del(db, args)); }
    if cmd.eq_ignore_ascii_case(b"EXISTS") { return DispatchResult::Response(key::exists(db, args)); }
    if cmd.eq_ignore_ascii_case(b"EXPIRE") { return DispatchResult::Response(key::expire(db, args)); }
    if cmd.eq_ignore_ascii_case(b"PEXPIRE") { return DispatchResult::Response(key::pexpire(db, args)); }
    if cmd.eq_ignore_ascii_case(b"TTL") { return DispatchResult::Response(key::ttl(db, args)); }
    if cmd.eq_ignore_ascii_case(b"PTTL") { return DispatchResult::Response(key::pttl(db, args)); }
    if cmd.eq_ignore_ascii_case(b"PERSIST") { return DispatchResult::Response(key::persist(db, args)); }
    if cmd.eq_ignore_ascii_case(b"TYPE") { return DispatchResult::Response(key::type_cmd(db, args)); }
    if cmd.eq_ignore_ascii_case(b"UNLINK") { return DispatchResult::Response(key::unlink(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SCAN") { return DispatchResult::Response(key::scan(db, args)); }
    if cmd.eq_ignore_ascii_case(b"KEYS") { return DispatchResult::Response(key::keys(db, args)); }
    if cmd.eq_ignore_ascii_case(b"RENAME") { return DispatchResult::Response(key::rename(db, args)); }
    if cmd.eq_ignore_ascii_case(b"RENAMENX") { return DispatchResult::Response(key::renamenx(db, args)); }
    if cmd.eq_ignore_ascii_case(b"OBJECT") { return DispatchResult::Response(key::object(db, args)); }
    // String commands
    if cmd.eq_ignore_ascii_case(b"GET") { return DispatchResult::Response(string::get(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SET") { return DispatchResult::Response(string::set(db, args)); }
    if cmd.eq_ignore_ascii_case(b"MGET") { return DispatchResult::Response(string::mget(db, args)); }
    if cmd.eq_ignore_ascii_case(b"MSET") { return DispatchResult::Response(string::mset(db, args)); }
    if cmd.eq_ignore_ascii_case(b"INCR") { return DispatchResult::Response(string::incr(db, args)); }
    if cmd.eq_ignore_ascii_case(b"DECR") { return DispatchResult::Response(string::decr(db, args)); }
    if cmd.eq_ignore_ascii_case(b"INCRBY") { return DispatchResult::Response(string::incrby(db, args)); }
    if cmd.eq_ignore_ascii_case(b"DECRBY") { return DispatchResult::Response(string::decrby(db, args)); }
    if cmd.eq_ignore_ascii_case(b"INCRBYFLOAT") { return DispatchResult::Response(string::incrbyfloat(db, args)); }
    if cmd.eq_ignore_ascii_case(b"APPEND") { return DispatchResult::Response(string::append(db, args)); }
    if cmd.eq_ignore_ascii_case(b"STRLEN") { return DispatchResult::Response(string::strlen(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SETNX") { return DispatchResult::Response(string::setnx(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SETEX") { return DispatchResult::Response(string::setex(db, args)); }
    if cmd.eq_ignore_ascii_case(b"PSETEX") { return DispatchResult::Response(string::psetex(db, args)); }
    if cmd.eq_ignore_ascii_case(b"GETSET") { return DispatchResult::Response(string::getset(db, args)); }
    if cmd.eq_ignore_ascii_case(b"GETDEL") { return DispatchResult::Response(string::getdel(db, args)); }
    if cmd.eq_ignore_ascii_case(b"GETEX") { return DispatchResult::Response(string::getex(db, args)); }
    // Hash commands
    if cmd.eq_ignore_ascii_case(b"HSET") { return DispatchResult::Response(hash::hset(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HGET") { return DispatchResult::Response(hash::hget(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HDEL") { return DispatchResult::Response(hash::hdel(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HMSET") { return DispatchResult::Response(hash::hmset(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HMGET") { return DispatchResult::Response(hash::hmget(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HGETALL") { return DispatchResult::Response(hash::hgetall(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HEXISTS") { return DispatchResult::Response(hash::hexists(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HLEN") { return DispatchResult::Response(hash::hlen(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HKEYS") { return DispatchResult::Response(hash::hkeys(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HVALS") { return DispatchResult::Response(hash::hvals(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HINCRBY") { return DispatchResult::Response(hash::hincrby(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HINCRBYFLOAT") { return DispatchResult::Response(hash::hincrbyfloat(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HSETNX") { return DispatchResult::Response(hash::hsetnx(db, args)); }
    if cmd.eq_ignore_ascii_case(b"HSCAN") { return DispatchResult::Response(hash::hscan(db, args)); }
    // List commands
    if cmd.eq_ignore_ascii_case(b"LPUSH") { return DispatchResult::Response(list::lpush(db, args)); }
    if cmd.eq_ignore_ascii_case(b"RPUSH") { return DispatchResult::Response(list::rpush(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LPOP") { return DispatchResult::Response(list::lpop(db, args)); }
    if cmd.eq_ignore_ascii_case(b"RPOP") { return DispatchResult::Response(list::rpop(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LLEN") { return DispatchResult::Response(list::llen(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LRANGE") { return DispatchResult::Response(list::lrange(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LINDEX") { return DispatchResult::Response(list::lindex(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LSET") { return DispatchResult::Response(list::lset(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LINSERT") { return DispatchResult::Response(list::linsert(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LREM") { return DispatchResult::Response(list::lrem(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LTRIM") { return DispatchResult::Response(list::ltrim(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LPOS") { return DispatchResult::Response(list::lpos(db, args)); }
    if cmd.eq_ignore_ascii_case(b"LMOVE") { return DispatchResult::Response(list::lmove(db, args)); }
    // Set commands
    if cmd.eq_ignore_ascii_case(b"SADD") { return DispatchResult::Response(set::sadd(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SREM") { return DispatchResult::Response(set::srem(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SMEMBERS") { return DispatchResult::Response(set::smembers(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SCARD") { return DispatchResult::Response(set::scard(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SISMEMBER") { return DispatchResult::Response(set::sismember(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SMISMEMBER") { return DispatchResult::Response(set::smismember(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SINTER") { return DispatchResult::Response(set::sinter(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SUNION") { return DispatchResult::Response(set::sunion(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SDIFF") { return DispatchResult::Response(set::sdiff(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SINTERSTORE") { return DispatchResult::Response(set::sinterstore(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SUNIONSTORE") { return DispatchResult::Response(set::sunionstore(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SDIFFSTORE") { return DispatchResult::Response(set::sdiffstore(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SRANDMEMBER") { return DispatchResult::Response(set::srandmember(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SPOP") { return DispatchResult::Response(set::spop(db, args)); }
    if cmd.eq_ignore_ascii_case(b"SSCAN") { return DispatchResult::Response(set::sscan(db, args)); }
    // Sorted set commands
    if cmd.eq_ignore_ascii_case(b"ZADD") { return DispatchResult::Response(sorted_set::zadd(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZREM") { return DispatchResult::Response(sorted_set::zrem(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZSCORE") { return DispatchResult::Response(sorted_set::zscore(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZCARD") { return DispatchResult::Response(sorted_set::zcard(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZINCRBY") { return DispatchResult::Response(sorted_set::zincrby(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZRANK") { return DispatchResult::Response(sorted_set::zrank(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZREVRANK") { return DispatchResult::Response(sorted_set::zrevrank(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZPOPMIN") { return DispatchResult::Response(sorted_set::zpopmin(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZPOPMAX") { return DispatchResult::Response(sorted_set::zpopmax(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZSCAN") { return DispatchResult::Response(sorted_set::zscan(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZRANGE") { return DispatchResult::Response(sorted_set::zrange(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZREVRANGE") { return DispatchResult::Response(sorted_set::zrevrange(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZRANGEBYSCORE") { return DispatchResult::Response(sorted_set::zrangebyscore(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE") { return DispatchResult::Response(sorted_set::zrevrangebyscore(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZCOUNT") { return DispatchResult::Response(sorted_set::zcount(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZLEXCOUNT") { return DispatchResult::Response(sorted_set::zlexcount(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZUNIONSTORE") { return DispatchResult::Response(sorted_set::zunionstore(db, args)); }
    if cmd.eq_ignore_ascii_case(b"ZINTERSTORE") { return DispatchResult::Response(sorted_set::zinterstore(db, args)); }
    // Stream commands
    if cmd.eq_ignore_ascii_case(b"XADD") { return DispatchResult::Response(stream::xadd(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XLEN") { return DispatchResult::Response(stream::xlen(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XRANGE") { return DispatchResult::Response(stream::xrange(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XREVRANGE") { return DispatchResult::Response(stream::xrevrange(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XTRIM") { return DispatchResult::Response(stream::xtrim(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XDEL") { return DispatchResult::Response(stream::xdel(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XREAD") { return DispatchResult::Response(stream::xread(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XGROUP") { return DispatchResult::Response(stream::xgroup(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XREADGROUP") { return DispatchResult::Response(stream::xreadgroup(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XACK") { return DispatchResult::Response(stream::xack(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XPENDING") { return DispatchResult::Response(stream::xpending(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XCLAIM") { return DispatchResult::Response(stream::xclaim(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XAUTOCLAIM") { return DispatchResult::Response(stream::xautoclaim(db, args)); }
    if cmd.eq_ignore_ascii_case(b"XINFO") { return DispatchResult::Response(stream::xinfo(db, args)); }

    DispatchResult::Response(Frame::Error(Bytes::from(format!(
        "ERR unknown command '{}', with args beginning with: ",
        String::from_utf8_lossy(cmd)
    ))))
}

/// Check if a command is read-only (safe to execute under a shared read lock).
pub fn is_read_command(cmd: &[u8]) -> bool {
    cmd.eq_ignore_ascii_case(b"GET")
        || cmd.eq_ignore_ascii_case(b"MGET")
        || cmd.eq_ignore_ascii_case(b"EXISTS")
        || cmd.eq_ignore_ascii_case(b"TTL")
        || cmd.eq_ignore_ascii_case(b"PTTL")
        || cmd.eq_ignore_ascii_case(b"TYPE")
        || cmd.eq_ignore_ascii_case(b"STRLEN")
        || cmd.eq_ignore_ascii_case(b"KEYS")
        || cmd.eq_ignore_ascii_case(b"SCAN")
        || cmd.eq_ignore_ascii_case(b"HGET")
        || cmd.eq_ignore_ascii_case(b"HMGET")
        || cmd.eq_ignore_ascii_case(b"HGETALL")
        || cmd.eq_ignore_ascii_case(b"HLEN")
        || cmd.eq_ignore_ascii_case(b"HKEYS")
        || cmd.eq_ignore_ascii_case(b"HVALS")
        || cmd.eq_ignore_ascii_case(b"HEXISTS")
        || cmd.eq_ignore_ascii_case(b"HSCAN")
        || cmd.eq_ignore_ascii_case(b"LLEN")
        || cmd.eq_ignore_ascii_case(b"LRANGE")
        || cmd.eq_ignore_ascii_case(b"LINDEX")
        || cmd.eq_ignore_ascii_case(b"LPOS")
        || cmd.eq_ignore_ascii_case(b"SCARD")
        || cmd.eq_ignore_ascii_case(b"SISMEMBER")
        || cmd.eq_ignore_ascii_case(b"SMISMEMBER")
        || cmd.eq_ignore_ascii_case(b"SMEMBERS")
        || cmd.eq_ignore_ascii_case(b"SINTER")
        || cmd.eq_ignore_ascii_case(b"SUNION")
        || cmd.eq_ignore_ascii_case(b"SDIFF")
        || cmd.eq_ignore_ascii_case(b"SRANDMEMBER")
        || cmd.eq_ignore_ascii_case(b"SSCAN")
        || cmd.eq_ignore_ascii_case(b"ZSCORE")
        || cmd.eq_ignore_ascii_case(b"ZCARD")
        || cmd.eq_ignore_ascii_case(b"ZRANK")
        || cmd.eq_ignore_ascii_case(b"ZREVRANK")
        || cmd.eq_ignore_ascii_case(b"ZRANGE")
        || cmd.eq_ignore_ascii_case(b"ZREVRANGE")
        || cmd.eq_ignore_ascii_case(b"ZRANGEBYSCORE")
        || cmd.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE")
        || cmd.eq_ignore_ascii_case(b"ZCOUNT")
        || cmd.eq_ignore_ascii_case(b"ZLEXCOUNT")
        || cmd.eq_ignore_ascii_case(b"ZSCAN")
        || cmd.eq_ignore_ascii_case(b"PING")
        || cmd.eq_ignore_ascii_case(b"ECHO")
        || cmd.eq_ignore_ascii_case(b"COMMAND")
        || cmd.eq_ignore_ascii_case(b"INFO")
}

/// Dispatch a read-only command under a shared read lock.
///
/// Takes `&Database` (immutable) and `now_ms` for expiry checks.
/// Only called for commands where `is_read_command` returns true.
/// SELECT is NOT a read command (it mutates connection state) but is handled
/// separately in the connection handler.
pub fn dispatch_read(
    db: &Database,
    cmd: &[u8],
    args: &[Frame],
    now_ms: u64,
    _selected_db: &mut usize,
    _db_count: usize,
) -> DispatchResult {
    // Connection-level commands (no db mutation)
    if cmd.eq_ignore_ascii_case(b"PING") { return DispatchResult::Response(connection::ping(args)); }
    if cmd.eq_ignore_ascii_case(b"ECHO") { return DispatchResult::Response(connection::echo(args)); }
    if cmd.eq_ignore_ascii_case(b"COMMAND") { return DispatchResult::Response(connection::command(args)); }
    if cmd.eq_ignore_ascii_case(b"INFO") { return DispatchResult::Response(connection::info_readonly(db, args)); }
    // String read commands
    if cmd.eq_ignore_ascii_case(b"GET") { return DispatchResult::Response(string::get_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"MGET") { return DispatchResult::Response(string::mget_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"STRLEN") { return DispatchResult::Response(string::strlen_readonly(db, args, now_ms)); }
    // Key read commands
    if cmd.eq_ignore_ascii_case(b"EXISTS") { return DispatchResult::Response(key::exists_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"TTL") { return DispatchResult::Response(key::ttl_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"PTTL") { return DispatchResult::Response(key::pttl_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"TYPE") { return DispatchResult::Response(key::type_cmd_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"KEYS") { return DispatchResult::Response(key::keys_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SCAN") { return DispatchResult::Response(key::scan_readonly(db, args, now_ms)); }
    // Hash read commands
    if cmd.eq_ignore_ascii_case(b"HGET") { return DispatchResult::Response(hash::hget_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"HMGET") { return DispatchResult::Response(hash::hmget_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"HGETALL") { return DispatchResult::Response(hash::hgetall_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"HLEN") { return DispatchResult::Response(hash::hlen_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"HKEYS") { return DispatchResult::Response(hash::hkeys_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"HVALS") { return DispatchResult::Response(hash::hvals_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"HEXISTS") { return DispatchResult::Response(hash::hexists_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"HSCAN") { return DispatchResult::Response(hash::hscan_readonly(db, args, now_ms)); }
    // List read commands
    if cmd.eq_ignore_ascii_case(b"LLEN") { return DispatchResult::Response(list::llen_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"LRANGE") { return DispatchResult::Response(list::lrange_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"LINDEX") { return DispatchResult::Response(list::lindex_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"LPOS") { return DispatchResult::Response(list::lpos_readonly(db, args, now_ms)); }
    // Set read commands
    if cmd.eq_ignore_ascii_case(b"SMEMBERS") { return DispatchResult::Response(set::smembers_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SCARD") { return DispatchResult::Response(set::scard_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SISMEMBER") { return DispatchResult::Response(set::sismember_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SMISMEMBER") { return DispatchResult::Response(set::smismember_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SINTER") { return DispatchResult::Response(set::sinter_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SUNION") { return DispatchResult::Response(set::sunion_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SDIFF") { return DispatchResult::Response(set::sdiff_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SRANDMEMBER") { return DispatchResult::Response(set::srandmember_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"SSCAN") { return DispatchResult::Response(set::sscan_readonly(db, args, now_ms)); }
    // Sorted set read commands
    if cmd.eq_ignore_ascii_case(b"ZSCORE") { return DispatchResult::Response(sorted_set::zscore_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZCARD") { return DispatchResult::Response(sorted_set::zcard_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZRANK") { return DispatchResult::Response(sorted_set::zrank_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZREVRANK") { return DispatchResult::Response(sorted_set::zrevrank_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZRANGE") { return DispatchResult::Response(sorted_set::zrange_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZREVRANGE") { return DispatchResult::Response(sorted_set::zrevrange_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZRANGEBYSCORE") { return DispatchResult::Response(sorted_set::zrangebyscore_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE") { return DispatchResult::Response(sorted_set::zrevrangebyscore_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZCOUNT") { return DispatchResult::Response(sorted_set::zcount_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZLEXCOUNT") { return DispatchResult::Response(sorted_set::zlexcount_readonly(db, args, now_ms)); }
    if cmd.eq_ignore_ascii_case(b"ZSCAN") { return DispatchResult::Response(sorted_set::zscan_readonly(db, args, now_ms)); }

    // Fallback: should not be reached if is_read_command is correct
    // but for safety, return error
    DispatchResult::Response(Frame::Error(Bytes::from(format!(
        "ERR unknown command '{}', with args beginning with: ",
        String::from_utf8_lossy(cmd)
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_args(parts: &[&[u8]]) -> Vec<Frame> {
        parts
            .iter()
            .map(|p| Frame::BulkString(Bytes::copy_from_slice(p)))
            .collect()
    }

    #[test]
    fn test_dispatch_ping() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, b"PING", &[], &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"PONG")))
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_dispatch_case_insensitive() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, b"ping", &[], &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"PONG")))
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_dispatch_quit() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, b"QUIT", &[], &mut selected, 16);
        match result {
            DispatchResult::Quit(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"OK")))
            }
            _ => panic!("Expected Quit"),
        }
    }

    #[test]
    fn test_dispatch_unknown_command() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let result = dispatch(&mut db, b"FAKECMD", &[], &mut selected, 16);
        match result {
            DispatchResult::Response(Frame::Error(s)) => {
                assert!(s.starts_with(b"ERR unknown command"));
            }
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_dispatch_get_set() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"foo", b"bar"]);
        let result = dispatch(&mut db, b"SET", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::SimpleString(Bytes::from_static(b"OK")));
            }
            _ => panic!("Expected Response"),
        }
        let args = make_args(&[b"foo"]);
        let result = dispatch(&mut db, b"GET", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from_static(b"bar")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_dispatch_hset_hget() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"myhash", b"field1", b"value1"]);
        let result = dispatch(&mut db, b"HSET", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::Integer(1));
            }
            _ => panic!("Expected Response"),
        }
        let args = make_args(&[b"myhash", b"field1"]);
        let result = dispatch(&mut db, b"HGET", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from_static(b"value1")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_dispatch_lpush_lrange() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"mylist", b"a", b"b", b"c"]);
        let result = dispatch(&mut db, b"LPUSH", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::Integer(3));
            }
            _ => panic!("Expected Response"),
        }
        let args = make_args(&[b"mylist", b"0", b"-1"]);
        let result = dispatch(&mut db, b"LRANGE", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(Frame::Array(items)) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0], Frame::BulkString(Bytes::from_static(b"c")));
                assert_eq!(items[1], Frame::BulkString(Bytes::from_static(b"b")));
                assert_eq!(items[2], Frame::BulkString(Bytes::from_static(b"a")));
            }
            _ => panic!("Expected Array response"),
        }
    }

    #[test]
    fn test_dispatch_sadd_smembers() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"myset", b"a", b"b", b"c"]);
        let result = dispatch(&mut db, b"SADD", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::Integer(3));
            }
            _ => panic!("Expected Response"),
        }
        let args = make_args(&[b"myset"]);
        let result = dispatch(&mut db, b"SMEMBERS", &args, &mut selected, 16);
        match result {
            DispatchResult::Response(Frame::Array(members)) => {
                assert_eq!(members.len(), 3);
            }
            _ => panic!("Expected Array response"),
        }
    }

    #[test]
    fn test_object_encoding_string() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // Set a string value
        let args = make_args(&[b"mykey", b"hello"]);
        dispatch(&mut db, b"SET", &args, &mut selected, 16);
        // OBJECT ENCODING mykey
        let args = make_args(&[b"ENCODING", b"mykey"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("embstr")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_int() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"mykey", b"42"]);
        dispatch(&mut db, b"SET", &args, &mut selected, 16);
        let args = make_args(&[b"ENCODING", b"mykey"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("int")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_hash() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // Small hash should start as listpack
        let args = make_args(&[b"myhash", b"f", b"v"]);
        dispatch(&mut db, b"HSET", &args, &mut selected, 16);
        let args = make_args(&[b"ENCODING", b"myhash"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("listpack")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_list() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // Small list should start as listpack
        let args = make_args(&[b"mylist", b"a"]);
        dispatch(&mut db, b"LPUSH", &args, &mut selected, 16);
        let args = make_args(&[b"ENCODING", b"mylist"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("listpack")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_hash_upgrade() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // Add 129 fields to trigger upgrade from listpack to hashtable
        for i in 0..129 {
            let field = format!("f{}", i);
            let value = format!("v{}", i);
            let args = make_args(&[b"myhash", field.as_bytes(), value.as_bytes()]);
            dispatch(&mut db, b"HSET", &args, &mut selected, 16);
        }
        let args = make_args(&[b"ENCODING", b"myhash"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("hashtable")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_list_upgrade() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // Add 129 elements to trigger upgrade from listpack to linkedlist
        for i in 0..129 {
            let val = format!("v{}", i);
            let args = make_args(&[b"mylist", val.as_bytes()]);
            dispatch(&mut db, b"LPUSH", &args, &mut selected, 16);
        }
        let args = make_args(&[b"ENCODING", b"mylist"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("linkedlist")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_set_intset() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // SADD with integer-only members should create intset
        let args = make_args(&[b"myset", b"1", b"2", b"3"]);
        dispatch(&mut db, b"SADD", &args, &mut selected, 16);
        let args = make_args(&[b"ENCODING", b"myset"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("intset")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_set_hashtable() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // SADD with non-integer members should create hashtable
        let args = make_args(&[b"myset", b"hello", b"world"]);
        dispatch(&mut db, b"SADD", &args, &mut selected, 16);
        let args = make_args(&[b"ENCODING", b"myset"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("hashtable")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_missing_key() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"ENCODING", b"nokey"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::Null);
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_sadd_intset_upgrade_on_non_integer() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // First create intset with integers
        let args = make_args(&[b"myset", b"1", b"2", b"3"]);
        dispatch(&mut db, b"SADD", &args, &mut selected, 16);
        // Verify it's intset
        let args = make_args(&[b"ENCODING", b"myset"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("intset")));
            }
            _ => panic!("Expected Response"),
        }
        // Add non-integer member -- should upgrade to hashtable
        let args = make_args(&[b"myset", b"hello"]);
        dispatch(&mut db, b"SADD", &args, &mut selected, 16);
        // Verify it's now hashtable
        let args = make_args(&[b"ENCODING", b"myset"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("hashtable")));
            }
            _ => panic!("Expected Response"),
        }
        // Verify all 4 members are present
        let args = make_args(&[b"myset"]);
        match dispatch(&mut db, b"SCARD", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::Integer(4));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_sadd_intset_add_more_integers() {
        let mut db = Database::new();
        let mut selected = 0usize;
        // Create intset
        let args = make_args(&[b"myset", b"1", b"2"]);
        dispatch(&mut db, b"SADD", &args, &mut selected, 16);
        // Add more integers -- should stay intset
        let args = make_args(&[b"myset", b"3", b"4"]);
        match dispatch(&mut db, b"SADD", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::Integer(2));
            }
            _ => panic!("Expected Response"),
        }
        // Verify still intset
        let args = make_args(&[b"ENCODING", b"myset"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("intset")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_encoding_sorted_set() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"myzset", b"1", b"a"]);
        dispatch(&mut db, b"ZADD", &args, &mut selected, 16);
        let args = make_args(&[b"ENCODING", b"myzset"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(f) => {
                assert_eq!(f, Frame::BulkString(Bytes::from("skiplist")));
            }
            _ => panic!("Expected Response"),
        }
    }

    #[test]
    fn test_object_help() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"HELP"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(Frame::Array(items)) => {
                assert!(items.len() >= 2);
            }
            _ => panic!("Expected Array response"),
        }
    }

    #[test]
    fn test_object_unknown_subcommand() {
        let mut db = Database::new();
        let mut selected = 0usize;
        let args = make_args(&[b"BADCMD"]);
        match dispatch(&mut db, b"OBJECT", &args, &mut selected, 16) {
            DispatchResult::Response(Frame::Error(_)) => {}
            _ => panic!("Expected Error response"),
        }
    }
}
