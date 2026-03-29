pub mod acl;
pub mod client;
pub mod config;
pub mod connection;
pub mod hash;
pub mod helpers;
pub mod key;
pub mod list;
pub mod metadata;
pub mod persistence;
pub mod set;
pub mod sorted_set;
pub mod stream;
pub mod string;
// NOTE: ACL is an intercepted command handled at the connection level (like AUTH/BGSAVE),
// not dispatched through the dispatch() function below.

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
/// Uses two-level dispatch: match on `(command_length, first_byte_lowercase)` to narrow
/// to 1-5 candidates, then do final `eq_ignore_ascii_case` check. This gives O(1)
/// dispatch vs the previous O(N) linear if-chain (~15ns vs ~60-150ns).
pub fn dispatch(
    db: &mut Database,
    cmd: &[u8],
    args: &[Frame],
    selected_db: &mut usize,
    db_count: usize,
) -> DispatchResult {
    let len = cmd.len();
    if len == 0 {
        return DispatchResult::Response(err_unknown(cmd));
    }
    let b0 = cmd[0] | 0x20; // lowercase first byte

    #[inline(always)]
    fn resp(f: Frame) -> DispatchResult {
        DispatchResult::Response(f)
    }

    match (len, b0) {
        // 3-letter commands
        (3, b'd') => {
            // DEL
            if cmd.eq_ignore_ascii_case(b"DEL") {
                return resp(key::del(db, args));
            }
        }
        (3, b'g') => {
            // GET
            if cmd.eq_ignore_ascii_case(b"GET") {
                return resp(string::get(db, args));
            }
        }
        (3, b's') => {
            // SET
            if cmd.eq_ignore_ascii_case(b"SET") {
                return resp(string::set(db, args));
            }
        }
        (3, b't') => {
            // TTL
            if cmd.eq_ignore_ascii_case(b"TTL") {
                return resp(key::ttl(db, args));
            }
        }
        // 4-letter commands
        (4, b'd') => {
            // DECR
            if cmd.eq_ignore_ascii_case(b"DECR") {
                return resp(string::decr(db, args));
            }
        }
        (4, b'e') => {
            // ECHO
            if cmd.eq_ignore_ascii_case(b"ECHO") {
                return resp(connection::echo(args));
            }
        }
        (4, b'h') => {
            // HSET HGET HDEL HLEN
            if cmd.eq_ignore_ascii_case(b"HSET") {
                return resp(hash::hset(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HGET") {
                return resp(hash::hget(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HDEL") {
                return resp(hash::hdel(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HLEN") {
                return resp(hash::hlen(db, args));
            }
        }
        (4, b'i') => {
            // INCR INFO
            if cmd.eq_ignore_ascii_case(b"INCR") {
                return resp(string::incr(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"INFO") {
                return resp(connection::info(db, args));
            }
        }
        (4, b'k') => {
            // KEYS
            if cmd.eq_ignore_ascii_case(b"KEYS") {
                return resp(key::keys(db, args));
            }
        }
        (4, b'l') => {
            // LPOP LLEN LSET LREM LPOS
            if cmd.eq_ignore_ascii_case(b"LPOP") {
                return resp(list::lpop(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"LLEN") {
                return resp(list::llen(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"LSET") {
                return resp(list::lset(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"LREM") {
                return resp(list::lrem(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"LPOS") {
                return resp(list::lpos(db, args));
            }
        }
        (4, b'm') => {
            // MGET MSET
            if cmd.eq_ignore_ascii_case(b"MGET") {
                return resp(string::mget(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"MSET") {
                return resp(string::mset(db, args));
            }
        }
        (4, b'p') => {
            // PING PTTL
            if cmd.eq_ignore_ascii_case(b"PING") {
                return resp(connection::ping(args));
            }
            if cmd.eq_ignore_ascii_case(b"PTTL") {
                return resp(key::pttl(db, args));
            }
        }
        (4, b'q') => {
            // QUIT
            if cmd.eq_ignore_ascii_case(b"QUIT") {
                return DispatchResult::Quit(Frame::SimpleString(Bytes::from_static(b"OK")));
            }
        }
        (4, b'r') => {
            // RPOP
            if cmd.eq_ignore_ascii_case(b"RPOP") {
                return resp(list::rpop(db, args));
            }
        }
        (4, b's') => {
            // SCAN SADD SREM SPOP
            if cmd.eq_ignore_ascii_case(b"SCAN") {
                return resp(key::scan(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"SADD") {
                return resp(set::sadd(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"SREM") {
                return resp(set::srem(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"SPOP") {
                return resp(set::spop(db, args));
            }
        }
        (4, b't') => {
            // TYPE
            if cmd.eq_ignore_ascii_case(b"TYPE") {
                return resp(key::type_cmd(db, args));
            }
        }
        (4, b'x') => {
            // XADD XLEN XDEL XACK
            if cmd.eq_ignore_ascii_case(b"XADD") {
                return resp(stream::xadd(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XLEN") {
                return resp(stream::xlen(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XDEL") {
                return resp(stream::xdel(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XACK") {
                return resp(stream::xack(db, args));
            }
        }
        (4, b'z') => {
            // ZADD ZREM
            if cmd.eq_ignore_ascii_case(b"ZADD") {
                return resp(sorted_set::zadd(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZREM") {
                return resp(sorted_set::zrem(db, args));
            }
        }
        // 5-letter commands
        (5, b'g') => {
            // GETEX
            if cmd.eq_ignore_ascii_case(b"GETEX") {
                return resp(string::getex(db, args));
            }
        }
        (5, b'h') => {
            // HMSET HMGET HKEYS HVALS HSCAN
            if cmd.eq_ignore_ascii_case(b"HMSET") {
                return resp(hash::hmset(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HMGET") {
                return resp(hash::hmget(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HKEYS") {
                return resp(hash::hkeys(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HVALS") {
                return resp(hash::hvals(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HSCAN") {
                return resp(hash::hscan(db, args));
            }
        }
        (5, b'l') => {
            // LPUSH LTRIM LMOVE
            if cmd.eq_ignore_ascii_case(b"LPUSH") {
                return resp(list::lpush(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"LTRIM") {
                return resp(list::ltrim(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"LMOVE") {
                return resp(list::lmove(db, args));
            }
        }
        (5, b'r') => {
            // RPUSH
            if cmd.eq_ignore_ascii_case(b"RPUSH") {
                return resp(list::rpush(db, args));
            }
        }
        (5, b's') => {
            // SETNX SETEX SCARD SDIFF SSCAN
            let b1 = cmd[1] | 0x20;
            match b1 {
                b'e' => {
                    // SETxx
                    if cmd.eq_ignore_ascii_case(b"SETNX") {
                        return resp(string::setnx(db, args));
                    }
                    if cmd.eq_ignore_ascii_case(b"SETEX") {
                        return resp(string::setex(db, args));
                    }
                }
                b'c' => {
                    // SCARD
                    if cmd.eq_ignore_ascii_case(b"SCARD") {
                        return resp(set::scard(db, args));
                    }
                }
                b'd' => {
                    // SDIFF
                    if cmd.eq_ignore_ascii_case(b"SDIFF") {
                        return resp(set::sdiff(db, args));
                    }
                }
                b's' => {
                    // SSCAN
                    if cmd.eq_ignore_ascii_case(b"SSCAN") {
                        return resp(set::sscan(db, args));
                    }
                }
                _ => {}
            }
        }
        (5, b'x') => {
            // XTRIM XREAD XINFO
            if cmd.eq_ignore_ascii_case(b"XTRIM") {
                return resp(stream::xtrim(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XREAD") {
                return resp(stream::xread(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XINFO") {
                return resp(stream::xinfo(db, args));
            }
        }
        (5, b'z') => {
            // ZCARD ZRANK ZSCAN
            if cmd.eq_ignore_ascii_case(b"ZCARD") {
                return resp(sorted_set::zcard(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZRANK") {
                return resp(sorted_set::zrank(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZSCAN") {
                return resp(sorted_set::zscan(db, args));
            }
        }
        // 6-letter commands
        (6, b'a') => {
            // APPEND
            if cmd.eq_ignore_ascii_case(b"APPEND") {
                return resp(string::append(db, args));
            }
        }
        (6, b'd') => {
            // DBSIZE DECRBY
            if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                return resp(key::dbsize(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"DECRBY") {
                return resp(string::decrby(db, args));
            }
        }
        (6, b'e') => {
            // EXISTS EXPIRE
            if cmd.eq_ignore_ascii_case(b"EXISTS") {
                return resp(key::exists(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"EXPIRE") {
                return resp(key::expire(db, args));
            }
        }
        (6, b'g') => {
            // GETSET GETDEL
            if cmd.eq_ignore_ascii_case(b"GETSET") {
                return resp(string::getset(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"GETDEL") {
                return resp(string::getdel(db, args));
            }
        }
        (6, b'h') => {
            // HSETNX
            if cmd.eq_ignore_ascii_case(b"HSETNX") {
                return resp(hash::hsetnx(db, args));
            }
        }
        (6, b'i') => {
            // INCRBY
            if cmd.eq_ignore_ascii_case(b"INCRBY") {
                return resp(string::incrby(db, args));
            }
        }
        (6, b'l') => {
            // LRANGE LINDEX
            if cmd.eq_ignore_ascii_case(b"LRANGE") {
                return resp(list::lrange(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"LINDEX") {
                return resp(list::lindex(db, args));
            }
        }
        (6, b'o') => {
            // OBJECT
            if cmd.eq_ignore_ascii_case(b"OBJECT") {
                return resp(key::object(db, args));
            }
        }
        (6, b'p') => {
            // PSETEX
            if cmd.eq_ignore_ascii_case(b"PSETEX") {
                return resp(string::psetex(db, args));
            }
        }
        (6, b'r') => {
            // RENAME
            if cmd.eq_ignore_ascii_case(b"RENAME") {
                return resp(key::rename(db, args));
            }
        }
        (6, b's') => {
            // SELECT STRLEN SUBSTR SINTER SUNION
            let b1 = cmd[1] | 0x20;
            match b1 {
                b'e' => {
                    if cmd.eq_ignore_ascii_case(b"SELECT") {
                        return resp(connection::select(args, selected_db, db_count));
                    }
                }
                b't' => {
                    if cmd.eq_ignore_ascii_case(b"STRLEN") {
                        return resp(string::strlen(db, args));
                    }
                }
                b'i' => {
                    if cmd.eq_ignore_ascii_case(b"SINTER") {
                        return resp(set::sinter(db, args));
                    }
                }
                b'u' => {
                    if cmd.eq_ignore_ascii_case(b"SUNION") {
                        return resp(set::sunion(db, args));
                    }
                    if cmd.eq_ignore_ascii_case(b"SUBSTR") {
                        return resp(string::getrange(db, args));
                    }
                }
                _ => {}
            }
        }
        (6, b'u') => {
            // UNLINK
            if cmd.eq_ignore_ascii_case(b"UNLINK") {
                return resp(key::unlink(db, args));
            }
        }
        (6, b'x') => {
            // XRANGE XGROUP XCLAIM
            if cmd.eq_ignore_ascii_case(b"XRANGE") {
                return resp(stream::xrange(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XGROUP") {
                return resp(stream::xgroup(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XCLAIM") {
                return resp(stream::xclaim(db, args));
            }
        }
        (6, b'z') => {
            // ZSCORE ZRANGE ZCOUNT
            if cmd.eq_ignore_ascii_case(b"ZSCORE") {
                return resp(sorted_set::zscore(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZRANGE") {
                return resp(sorted_set::zrange(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZCOUNT") {
                return resp(sorted_set::zcount(db, args));
            }
        }
        // 7-letter commands
        (7, b'c') => {
            // COMMAND
            if cmd.eq_ignore_ascii_case(b"COMMAND") {
                return resp(connection::command(args));
            }
        }
        (7, b'h') => {
            // HGETALL HEXISTS HINCRBY
            if cmd.eq_ignore_ascii_case(b"HGETALL") {
                return resp(hash::hgetall(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HEXISTS") {
                return resp(hash::hexists(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"HINCRBY") {
                return resp(hash::hincrby(db, args));
            }
        }
        (7, b'l') => {
            // LINSERT
            if cmd.eq_ignore_ascii_case(b"LINSERT") {
                return resp(list::linsert(db, args));
            }
        }
        (7, b'p') => {
            // PEXPIRE PERSIST
            if cmd.eq_ignore_ascii_case(b"PEXPIRE") {
                return resp(key::pexpire(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"PERSIST") {
                return resp(key::persist(db, args));
            }
        }
        (7, b'z') => {
            // ZINCRBY ZPOPMIN ZPOPMAX
            if cmd.eq_ignore_ascii_case(b"ZINCRBY") {
                return resp(sorted_set::zincrby(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZPOPMIN") {
                return resp(sorted_set::zpopmin(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZPOPMAX") {
                return resp(sorted_set::zpopmax(db, args));
            }
        }
        // 8-letter commands
        (8, b'g') => {
            // GETRANGE
            if cmd.eq_ignore_ascii_case(b"GETRANGE") {
                return resp(string::getrange(db, args));
            }
        }
        (8, b'r') => {
            // RENAMENX
            if cmd.eq_ignore_ascii_case(b"RENAMENX") {
                return resp(key::renamenx(db, args));
            }
        }
        (8, b's') => {
            // SMEMBERS SETRANGE
            if cmd.eq_ignore_ascii_case(b"SMEMBERS") {
                return resp(set::smembers(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"SETRANGE") {
                return resp(string::setrange(db, args));
            }
        }
        (8, b'x') => {
            // XPENDING
            if cmd.eq_ignore_ascii_case(b"XPENDING") {
                return resp(stream::xpending(db, args));
            }
        }
        (8, b'z') => {
            // ZREVRANK
            if cmd.eq_ignore_ascii_case(b"ZREVRANK") {
                return resp(sorted_set::zrevrank(db, args));
            }
        }
        // 9-letter commands
        (9, b's') => {
            // SISMEMBER
            if cmd.eq_ignore_ascii_case(b"SISMEMBER") {
                return resp(set::sismember(db, args));
            }
        }
        (9, b'x') => {
            // XREVRANGE
            if cmd.eq_ignore_ascii_case(b"XREVRANGE") {
                return resp(stream::xrevrange(db, args));
            }
        }
        (9, b'z') => {
            // ZREVRANGE ZLEXCOUNT
            if cmd.eq_ignore_ascii_case(b"ZREVRANGE") {
                return resp(sorted_set::zrevrange(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZLEXCOUNT") {
                return resp(sorted_set::zlexcount(db, args));
            }
        }
        // 10-letter commands
        (10, b's') => {
            // SMISMEMBER SDIFFSTORE
            if cmd.eq_ignore_ascii_case(b"SMISMEMBER") {
                return resp(set::smismember(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"SDIFFSTORE") {
                return resp(set::sdiffstore(db, args));
            }
        }
        (10, b'x') => {
            // XREADGROUP XAUTOCLAIM
            if cmd.eq_ignore_ascii_case(b"XREADGROUP") {
                return resp(stream::xreadgroup(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"XAUTOCLAIM") {
                return resp(stream::xautoclaim(db, args));
            }
        }
        // 11-letter commands
        (11, b'i') => {
            // INCRBYFLOAT
            if cmd.eq_ignore_ascii_case(b"INCRBYFLOAT") {
                return resp(string::incrbyfloat(db, args));
            }
        }
        (11, b's') => {
            // SINTERSTORE SUNIONSTORE SRANDMEMBER
            if cmd.eq_ignore_ascii_case(b"SINTERSTORE") {
                return resp(set::sinterstore(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"SUNIONSTORE") {
                return resp(set::sunionstore(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"SRANDMEMBER") {
                return resp(set::srandmember(db, args));
            }
        }
        (11, b'z') => {
            // ZUNIONSTORE ZINTERSTORE
            if cmd.eq_ignore_ascii_case(b"ZUNIONSTORE") {
                return resp(sorted_set::zunionstore(db, args));
            }
            if cmd.eq_ignore_ascii_case(b"ZINTERSTORE") {
                return resp(sorted_set::zinterstore(db, args));
            }
        }
        // 12-letter commands
        (12, b'h') => {
            // HINCRBYFLOAT
            if cmd.eq_ignore_ascii_case(b"HINCRBYFLOAT") {
                return resp(hash::hincrbyfloat(db, args));
            }
        }
        // 13-letter commands
        (13, b'z') => {
            // ZRANGEBYSCORE
            if cmd.eq_ignore_ascii_case(b"ZRANGEBYSCORE") {
                return resp(sorted_set::zrangebyscore(db, args));
            }
        }
        // 16-letter commands
        (16, b'z') => {
            // ZREVRANGEBYSCORE
            if cmd.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE") {
                return resp(sorted_set::zrevrangebyscore(db, args));
            }
        }
        _ => {}
    }

    DispatchResult::Response(err_unknown(cmd))
}

#[inline]
fn err_unknown(cmd: &[u8]) -> Frame {
    Frame::Error(Bytes::from(format!(
        "ERR unknown command '{}', with args beginning with: ",
        String::from_utf8_lossy(cmd)
    )))
}

/// Dispatch a read-only command under a shared read lock.
///
/// Takes `&Database` (immutable) and `now_ms` for expiry checks.
/// Only called for commands where `metadata::is_read()` returns true.
/// Uses (length, first_byte) dispatch for O(1) lookup.
/// Check whether a command is supported by the shared-read dispatch path.
///
/// Returns `true` for all commands handled by `dispatch_read()`. Uses the same
/// `(len, b0)` two-level dispatch pattern for O(1) lookup. This guard is used
/// to avoid acquiring a read lock on the shared database when the command would
/// just fall through to the unknown-command error.
pub fn is_dispatch_read_supported(cmd: &[u8]) -> bool {
    let len = cmd.len();
    if len == 0 {
        return false;
    }
    let b0 = cmd[0] | 0x20;
    matches!(
        (len, b0),
        (3, b'g')  // GET
        | (3, b't')  // TTL
        | (4, b'e')  // ECHO
        | (4, b'h')  // HGET, HLEN
        | (4, b'i')  // INFO
        | (4, b'k')  // KEYS
        | (4, b'l')  // LLEN, LPOS
        | (4, b'm')  // MGET
        | (4, b'p')  // PTTL, PING
        | (4, b's')  // SCAN
        | (4, b't')  // TYPE
        | (5, b'h')  // HMGET, HKEYS, HVALS, HSCAN
        | (5, b's')  // SCARD, SDIFF, SSCAN
        | (5, b'z')  // ZCARD, ZRANK, ZSCAN
        | (6, b'e')  // EXISTS
        | (6, b'l')  // LRANGE, LINDEX
        | (6, b's')  // STRLEN, SUBSTR, SINTER, SUNION
        | (6, b'z')  // ZSCORE, ZRANGE, ZCOUNT
        | (7, b'c')  // COMMAND
        | (7, b'h')  // HGETALL, HEXISTS
        | (8, b'g')  // GETRANGE
        | (8, b's')  // SMEMBERS
        | (8, b'z')  // ZREVRANK
        | (9, b's')  // SISMEMBER
        | (9, b'z')  // ZREVRANGE, ZLEXCOUNT
        | (10, b's') // SMISMEMBER
        | (11, b's') // SRANDMEMBER
        | (13, b'z') // ZRANGEBYSCORE
        | (16, b'z') // ZREVRANGEBYSCORE
    )
}

pub fn dispatch_read(
    db: &Database,
    cmd: &[u8],
    args: &[Frame],
    now_ms: u64,
    _selected_db: &mut usize,
    _db_count: usize,
) -> DispatchResult {
    let len = cmd.len();
    if len == 0 {
        return DispatchResult::Response(err_unknown(cmd));
    }
    let b0 = cmd[0] | 0x20;

    #[inline(always)]
    fn resp(f: Frame) -> DispatchResult {
        DispatchResult::Response(f)
    }

    match (len, b0) {
        (3, b'g') => {
            // GET
            if cmd.eq_ignore_ascii_case(b"GET") {
                return resp(string::get_readonly(db, args, now_ms));
            }
        }
        (3, b't') => {
            // TTL
            if cmd.eq_ignore_ascii_case(b"TTL") {
                return resp(key::ttl_readonly(db, args, now_ms));
            }
        }
        (4, b'e') => {
            // ECHO
            if cmd.eq_ignore_ascii_case(b"ECHO") {
                return resp(connection::echo(args));
            }
        }
        (4, b'h') => {
            // HGET HLEN
            if cmd.eq_ignore_ascii_case(b"HGET") {
                return resp(hash::hget_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"HLEN") {
                return resp(hash::hlen_readonly(db, args, now_ms));
            }
        }
        (4, b'i') => {
            // INFO
            if cmd.eq_ignore_ascii_case(b"INFO") {
                return resp(connection::info_readonly(db, args));
            }
        }
        (4, b'k') => {
            // KEYS
            if cmd.eq_ignore_ascii_case(b"KEYS") {
                return resp(key::keys_readonly(db, args, now_ms));
            }
        }
        (4, b'l') => {
            // LLEN LPOS
            if cmd.eq_ignore_ascii_case(b"LLEN") {
                return resp(list::llen_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"LPOS") {
                return resp(list::lpos_readonly(db, args, now_ms));
            }
        }
        (4, b'm') => {
            // MGET
            if cmd.eq_ignore_ascii_case(b"MGET") {
                return resp(string::mget_readonly(db, args, now_ms));
            }
        }
        (4, b'p') => {
            // PTTL PING
            if cmd.eq_ignore_ascii_case(b"PTTL") {
                return resp(key::pttl_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"PING") {
                return resp(connection::ping(args));
            }
        }
        (4, b's') => {
            // SCAN
            if cmd.eq_ignore_ascii_case(b"SCAN") {
                return resp(key::scan_readonly(db, args, now_ms));
            }
        }
        (4, b't') => {
            // TYPE
            if cmd.eq_ignore_ascii_case(b"TYPE") {
                return resp(key::type_cmd_readonly(db, args, now_ms));
            }
        }
        (5, b'h') => {
            // HMGET HKEYS HVALS HSCAN
            if cmd.eq_ignore_ascii_case(b"HMGET") {
                return resp(hash::hmget_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"HKEYS") {
                return resp(hash::hkeys_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"HVALS") {
                return resp(hash::hvals_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"HSCAN") {
                return resp(hash::hscan_readonly(db, args, now_ms));
            }
        }
        (5, b's') => {
            // SCARD SDIFF SSCAN
            if cmd.eq_ignore_ascii_case(b"SCARD") {
                return resp(set::scard_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"SDIFF") {
                return resp(set::sdiff_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"SSCAN") {
                return resp(set::sscan_readonly(db, args, now_ms));
            }
        }
        (5, b'z') => {
            // ZCARD ZRANK ZSCAN
            if cmd.eq_ignore_ascii_case(b"ZCARD") {
                return resp(sorted_set::zcard_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"ZRANK") {
                return resp(sorted_set::zrank_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"ZSCAN") {
                return resp(sorted_set::zscan_readonly(db, args, now_ms));
            }
        }
        (6, b'e') => {
            // EXISTS
            if cmd.eq_ignore_ascii_case(b"EXISTS") {
                return resp(key::exists_readonly(db, args, now_ms));
            }
        }
        (6, b'l') => {
            // LRANGE LINDEX
            if cmd.eq_ignore_ascii_case(b"LRANGE") {
                return resp(list::lrange_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"LINDEX") {
                return resp(list::lindex_readonly(db, args, now_ms));
            }
        }
        (6, b's') => {
            // STRLEN SUBSTR SINTER SUNION
            if cmd.eq_ignore_ascii_case(b"STRLEN") {
                return resp(string::strlen_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"SUBSTR") {
                return resp(string::getrange_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"SINTER") {
                return resp(set::sinter_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"SUNION") {
                return resp(set::sunion_readonly(db, args, now_ms));
            }
        }
        (6, b'z') => {
            // ZSCORE ZRANGE ZCOUNT
            if cmd.eq_ignore_ascii_case(b"ZSCORE") {
                return resp(sorted_set::zscore_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"ZRANGE") {
                return resp(sorted_set::zrange_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"ZCOUNT") {
                return resp(sorted_set::zcount_readonly(db, args, now_ms));
            }
        }
        (7, b'c') => {
            // COMMAND
            if cmd.eq_ignore_ascii_case(b"COMMAND") {
                return resp(connection::command(args));
            }
        }
        (7, b'h') => {
            // HGETALL HEXISTS
            if cmd.eq_ignore_ascii_case(b"HGETALL") {
                return resp(hash::hgetall_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"HEXISTS") {
                return resp(hash::hexists_readonly(db, args, now_ms));
            }
        }
        (8, b'g') => {
            // GETRANGE
            if cmd.eq_ignore_ascii_case(b"GETRANGE") {
                return resp(string::getrange_readonly(db, args, now_ms));
            }
        }
        (8, b's') => {
            // SMEMBERS
            if cmd.eq_ignore_ascii_case(b"SMEMBERS") {
                return resp(set::smembers_readonly(db, args, now_ms));
            }
        }
        (8, b'z') => {
            // ZREVRANK
            if cmd.eq_ignore_ascii_case(b"ZREVRANK") {
                return resp(sorted_set::zrevrank_readonly(db, args, now_ms));
            }
        }
        (9, b's') => {
            // SISMEMBER
            if cmd.eq_ignore_ascii_case(b"SISMEMBER") {
                return resp(set::sismember_readonly(db, args, now_ms));
            }
        }
        (9, b'z') => {
            // ZREVRANGE ZLEXCOUNT
            if cmd.eq_ignore_ascii_case(b"ZREVRANGE") {
                return resp(sorted_set::zrevrange_readonly(db, args, now_ms));
            }
            if cmd.eq_ignore_ascii_case(b"ZLEXCOUNT") {
                return resp(sorted_set::zlexcount_readonly(db, args, now_ms));
            }
        }
        (10, b's') => {
            // SMISMEMBER
            if cmd.eq_ignore_ascii_case(b"SMISMEMBER") {
                return resp(set::smismember_readonly(db, args, now_ms));
            }
        }
        (11, b's') => {
            // SRANDMEMBER
            if cmd.eq_ignore_ascii_case(b"SRANDMEMBER") {
                return resp(set::srandmember_readonly(db, args, now_ms));
            }
        }
        (13, b'z') => {
            // ZRANGEBYSCORE
            if cmd.eq_ignore_ascii_case(b"ZRANGEBYSCORE") {
                return resp(sorted_set::zrangebyscore_readonly(db, args, now_ms));
            }
        }
        (16, b'z') => {
            // ZREVRANGEBYSCORE
            if cmd.eq_ignore_ascii_case(b"ZREVRANGEBYSCORE") {
                return resp(sorted_set::zrevrangebyscore_readonly(db, args, now_ms));
            }
        }
        _ => {}
    }

    // Fallback: should not be reached if metadata::is_read() is correct
    DispatchResult::Response(err_unknown(cmd))
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
