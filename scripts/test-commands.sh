#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# test-commands.sh -- Comprehensive command coverage test
#
# Tests ALL Redis commands that moon supports via redis-cli, comparing output
# against Redis as ground truth. Also runs redis-benchmark throughput tests
# for benchmarkable commands.
#
# Usage:
#   ./scripts/test-commands.sh                  # Run all tests
#   ./scripts/test-commands.sh --category NAME  # Run single category
#   ./scripts/test-commands.sh --list           # List categories
#   ./scripts/test-commands.sh --shards N       # moon shard count (default: 1)
#   ./scripts/test-commands.sh --skip-build     # Skip cargo build
#   ./scripts/test-commands.sh --skip-bench     # Skip redis-benchmark throughput
#   ./scripts/test-commands.sh --bench-only     # Only redis-benchmark throughput
#   ./scripts/test-commands.sh --moon-only      # Test moon without Redis comparison
###############################################################################

PORT_REDIS=6399
PORT_RUST=6400
SHARDS=1
SKIP_BUILD=false
SKIP_BENCH=false
BENCH_ONLY=false
MOON_ONLY=false
CATEGORY_FILTER=""
RUST_BINARY="./target/release/moon"

PASS=0
FAIL=0
SKIP=0
TOTAL=0
BENCH_PASS=0
BENCH_FAIL=0
RUST_PID=""
REDIS_PID=""

# ===========================================================================
# Argument Parsing
# ===========================================================================

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards)       SHARDS="$2"; shift 2 ;;
        --skip-build)   SKIP_BUILD=true; shift ;;
        --skip-bench)   SKIP_BENCH=true; shift ;;
        --bench-only)   BENCH_ONLY=true; shift ;;
        --moon-only)    MOON_ONLY=true; shift ;;
        --category)     CATEGORY_FILTER="$2"; shift 2 ;;
        --list)
            echo "Available categories:"
            echo "  string       - String commands (GET, SET, MGET, APPEND, INCR, etc.)"
            echo "  list         - List commands (LPUSH, RPUSH, LPOP, LRANGE, etc.)"
            echo "  hash         - Hash commands (HSET, HGET, HMGET, HGETALL, etc.)"
            echo "  set          - Set commands (SADD, SMEMBERS, SINTER, SDIFF, etc.)"
            echo "  sorted_set   - Sorted set commands (ZADD, ZRANGE, ZSCORE, etc.)"
            echo "  key          - Key commands (DEL, EXISTS, EXPIRE, TTL, RENAME, etc.)"
            echo "  stream       - Stream commands (XADD, XREAD, XRANGE, XGROUP, etc.)"
            echo "  connection   - Connection commands (PING, ECHO, SELECT, INFO, etc.)"
            echo "  pubsub       - Pub/Sub commands (SUBSCRIBE, PUBLISH, etc.)"
            echo "  transaction  - Transaction commands (MULTI, EXEC, DISCARD)"
            echo "  scripting    - Lua scripting (EVAL, EVALSHA)"
            echo "  persistence  - Persistence commands (BGSAVE, BGREWRITEAOF, etc.)"
            echo "  blocking     - Blocking commands (BLPOP, BRPOP, BZPOPMIN, etc.)"
            echo "  benchmark    - redis-benchmark throughput for all benchmarkable commands"
            exit 0
            ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

# ===========================================================================
# Helpers
# ===========================================================================

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

cleanup() {
    log "Cleaning up..."
    [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
}
trap cleanup EXIT

rcli() {
    # Run redis-cli against Redis
    redis-cli -p "$PORT_REDIS" "$@" 2>/dev/null
}

mcli() {
    # Run redis-cli against moon
    redis-cli -p "$PORT_RUST" "$@" 2>/dev/null
}

rcli_raw() {
    redis-cli -p "$PORT_REDIS" --no-auth-warning "$@" 2>/dev/null
}

mcli_raw() {
    redis-cli -p "$PORT_RUST" --no-auth-warning "$@" 2>/dev/null
}

# Compare redis-cli output between Redis and moon
assert_match() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null || echo "__REDIS_ERROR__")
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if [[ "$redis_out" == "$moon_out" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    CMD:   redis-cli $*"
        echo "    REDIS: $(echo "$redis_out" | head -3)"
        echo "    MOON:  $(echo "$moon_out" | head -3)"
    fi
}

# Compare sorted output (for unordered results like HGETALL, SMEMBERS, SUNION)
assert_match_sorted() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null | sort || echo "__REDIS_ERROR__")
    moon_out=$(mcli "$@" 2>/dev/null | sort || echo "__MOON_ERROR__")
    if [[ "$redis_out" == "$moon_out" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (sorted comparison)"
        echo "    CMD:   redis-cli $*"
        echo "    REDIS: $(echo "$redis_out" | head -3)"
        echo "    MOON:  $(echo "$moon_out" | head -3)"
    fi
}

# Compare with TTL tolerance (±5 seconds)
assert_match_ttl() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    moon_out=$(mcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    local diff=$(( redis_out - moon_out ))
    if [[ "$diff" -lt 0 ]]; then diff=$(( -diff )); fi
    if [[ "$diff" -le 5 ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (TTL diff=$diff, tolerance=5)"
        echo "    REDIS: $redis_out"
        echo "    MOON:  $moon_out"
    fi
}

# Compare with millisecond TTL tolerance (±5000ms)
assert_match_pttl() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local redis_out moon_out
    redis_out=$(rcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    moon_out=$(mcli "$@" 2>/dev/null | tr -d '(integer) ' || echo "0")
    local diff=$(( redis_out - moon_out ))
    if [[ "$diff" -lt 0 ]]; then diff=$(( -diff )); fi
    if [[ "$diff" -le 5000 ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (PTTL diff=$diff, tolerance=5000)"
        echo "    REDIS: $redis_out"
        echo "    MOON:  $moon_out"
    fi
}

# Test moon-only (no Redis comparison)
assert_moon() {
    local desc="$1" expected="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if [[ "$moon_out" == "$expected" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    CMD:      redis-cli $*"
        echo "    EXPECTED: $expected"
        echo "    GOT:      $(echo "$moon_out" | head -3)"
    fi
}

# Test moon response contains expected substring
assert_moon_contains() {
    local desc="$1" expected="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if echo "$moon_out" | grep -qF "$expected"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (expected substring '$expected')"
        echo "    CMD:  redis-cli $*"
        echo "    GOT:  $(echo "$moon_out" | head -3)"
    fi
}

# Test that moon returns non-error response
assert_moon_ok() {
    local desc="$1"
    shift
    TOTAL=$((TOTAL + 1))
    local moon_out
    moon_out=$(mcli "$@" 2>/dev/null || echo "__MOON_ERROR__")
    if echo "$moon_out" | grep -qvE "^(\(error\)|ERR |__MOON_ERROR__)"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc (got error)"
        echo "    CMD: redis-cli $*"
        echo "    GOT: $(echo "$moon_out" | head -3)"
    fi
}

# Run redis-benchmark and check it completes
assert_bench() {
    local desc="$1" cmd="$2"
    shift 2
    TOTAL=$((TOTAL + 1))
    local raw rps
    raw=$(redis-benchmark -p "$PORT_RUST" -n 5000 -c 50 $cmd "$@" 2>&1 | tr '\r' '\n')
    rps=$(echo "$raw" | grep -i "requests per second" | tail -1 | awk '{for(i=1;i<=NF;i++) if($i ~ /^[0-9]/ && $(i+1) ~ /requests/) print $i}' | sed 's/,//g')
    # Fallback: try -q mode format "COMMAND: NNN.NN requests per second"
    if [[ -z "$rps" ]]; then
        rps=$(echo "$raw" | grep "requests per second" | tail -1 | sed 's/.*: \([0-9.]*\) requests.*/\1/' | sed 's/,//g')
    fi
    if [[ -n "$rps" ]] && [[ "$rps" != "0" ]] && [[ "$rps" != "0.00" ]]; then
        BENCH_PASS=$((BENCH_PASS + 1))
        PASS=$((PASS + 1))
        printf "    %-40s %s rps\n" "$desc" "$rps"
    else
        BENCH_FAIL=$((BENCH_FAIL + 1))
        FAIL=$((FAIL + 1))
        echo "    FAIL: $desc (benchmark returned no results)"
    fi
}

flush_both() {
    rcli FLUSHALL >/dev/null 2>&1 || true
    mcli FLUSHALL >/dev/null 2>&1 || true
}

should_run() {
    [[ -z "$CATEGORY_FILTER" ]] || [[ "$CATEGORY_FILTER" == "$1" ]]
}

# ===========================================================================
# Setup
# ===========================================================================

log "=== Moon Command Coverage Test ==="

if [[ "$SKIP_BUILD" == "false" ]]; then
    log "Building moon..."
    cargo build --release --quiet 2>/dev/null
fi

if [[ "$MOON_ONLY" == "false" ]]; then
    log "Starting Redis on port $PORT_REDIS..."
    redis-server --port "$PORT_REDIS" --save "" --appendonly no --loglevel warning --protected-mode no &
    REDIS_PID=$!
fi

log "Starting moon on port $PORT_RUST ($SHARDS shards)..."
RUST_LOG=warn "$RUST_BINARY" --port "$PORT_RUST" --shards "$SHARDS" --protected-mode no &
RUST_PID=$!

sleep 1

# Verify servers are up
if [[ "$MOON_ONLY" == "false" ]]; then
    rcli PING >/dev/null 2>&1 || { echo "Redis failed to start"; exit 1; }
fi
mcli PING >/dev/null 2>&1 || { echo "moon failed to start"; exit 1; }

log "Servers ready."

if [[ "$BENCH_ONLY" == "true" ]]; then
    CATEGORY_FILTER="benchmark"
fi

# ===========================================================================
# STRING COMMANDS
# ===========================================================================

if should_run "string"; then
    echo ""
    echo "=== STRING COMMANDS ==="
    flush_both

    if [[ "$MOON_ONLY" == "true" ]]; then
        assert_moon "SET basic"          "OK"    SET str:k1 hello
        assert_moon "GET basic"          "hello" GET str:k1
        assert_moon "SET EX"             "OK"    SET str:k2 world EX 100
        assert_moon "SET PX"             "OK"    SET str:k3 val PX 100000
        assert_moon "SET NX (new)"       "OK"    SET str:k4 new NX
        assert_moon "SET NX (exists)"    ""      SET str:k4 newer NX
        assert_moon "SET XX (exists)"    "OK"    SET str:k4 updated XX
        assert_moon "SET XX (missing)"   ""      SET str:k999 x XX
        assert_moon "SETNX (new)"        "(integer) 1" SETNX str:k5 val
        assert_moon "SETNX (exists)"     "(integer) 0" SETNX str:k5 val2
        assert_moon "SETEX"              "OK"    SETEX str:k6 100 myval
        assert_moon "PSETEX"             "OK"    PSETEX str:k7 100000 myval
        assert_moon "GET (missing)"      ""      GET str:missing
        assert_moon "GETSET"             "hello" GETSET str:k1 newhello
        assert_moon "GETDEL"             "newhello" GETDEL str:k1
        assert_moon "GETDEL (gone)"      ""      GET str:k1
        assert_moon "APPEND"             "(integer) 8" APPEND str:a1 hello
        assert_moon "APPEND (existing)"  "(integer) 13" APPEND str:a1 world
        assert_moon "STRLEN"             "(integer) 13" STRLEN str:a1
        assert_moon "INCR"               "(integer) 1" INCR str:cnt1
        assert_moon "INCR (again)"       "(integer) 2" INCR str:cnt1
        assert_moon "INCRBY"             "(integer) 12" INCRBY str:cnt1 10
        assert_moon "DECR"               "(integer) 11" DECR str:cnt1
        assert_moon "DECRBY"             "(integer) 6" DECRBY str:cnt1 5
        assert_moon "INCRBYFLOAT"        "6.5"   INCRBYFLOAT str:cnt1 0.5
        assert_moon "MSET"               "OK"    MSET str:m1 a str:m2 b str:m3 c
        assert_moon_ok "MGET"            MGET str:m1 str:m2 str:m3
        assert_moon_ok "GETEX with EX"   GETEX str:m1 EX 100
    else
        assert_match "SET basic"         SET str:k1 hello
        assert_match "GET basic"         GET str:k1
        assert_match "SET EX"            SET str:k2 world EX 100
        assert_match "SET PX"            SET str:k3 val PX 100000
        assert_match "SET NX (new)"      SET str:k4 new NX
        assert_match "SET NX (exists)"   SET str:k4 newer NX
        assert_match "SET XX (exists)"   SET str:k4 updated XX
        assert_match "SET XX (missing)"  SET str:k999 x XX
        assert_match "SETNX (new)"       SETNX str:k5 val
        assert_match "SETNX (exists)"    SETNX str:k5 val2
        assert_match "SETEX"             SETEX str:k6 100 myval
        assert_match "PSETEX"            PSETEX str:k7 100000 myval
        assert_match "GET (missing)"     GET str:missing
        assert_match "APPEND"            APPEND str:a1 helloworld
        assert_match "STRLEN"            STRLEN str:a1
        assert_match "INCR"              INCR str:cnt1
        assert_match "INCR (again)"      INCR str:cnt1
        assert_match "INCRBY"            INCRBY str:cnt1 10
        assert_match "DECR"              DECR str:cnt1
        assert_match "DECRBY"            DECRBY str:cnt1 5
        assert_match "INCRBYFLOAT"       INCRBYFLOAT str:cnt1 0.5
        assert_match "MSET"              MSET str:m1 a str:m2 b str:m3 c
        assert_match "MGET"              MGET str:m1 str:m2 str:m3
        assert_match "GETEX with EX"     GETEX str:m1 EX 100
    fi
fi

# ===========================================================================
# LIST COMMANDS
# ===========================================================================

if should_run "list"; then
    echo ""
    echo "=== LIST COMMANDS ==="
    flush_both

    assert_match "LPUSH"               LPUSH lst:k1 a b c
    assert_match "RPUSH"               RPUSH lst:k1 x y z
    assert_match "LLEN"                LLEN lst:k1
    assert_match "LRANGE all"          LRANGE lst:k1 0 -1
    assert_match "LRANGE partial"      LRANGE lst:k1 1 3
    assert_match "LINDEX"              LINDEX lst:k1 0
    assert_match "LINDEX negative"     LINDEX lst:k1 -1
    assert_match "LPOP"                LPOP lst:k1
    assert_match "RPOP"                RPOP lst:k1
    assert_match "LPOP count"          LPOP lst:k1 2
    assert_match "RPOP count"          RPOP lst:k1 2
    # Rebuild for remaining tests
    flush_both
    rcli RPUSH lst:k2 a b c d e >/dev/null 2>&1; mcli RPUSH lst:k2 a b c d e >/dev/null 2>&1
    assert_match "LSET"                LSET lst:k2 2 REPLACED
    assert_match "LRANGE after LSET"   LRANGE lst:k2 0 -1
    assert_match "LREM"                LREM lst:k2 1 a
    assert_match "LTRIM"               LTRIM lst:k2 0 2
    assert_match "LRANGE after LTRIM"  LRANGE lst:k2 0 -1
    rcli RPUSH lst:k3 a b c >/dev/null 2>&1; mcli RPUSH lst:k3 a b c >/dev/null 2>&1
    assert_match "LINSERT BEFORE"      LINSERT lst:k3 BEFORE b INSERTED
    assert_match "LINSERT AFTER"       LINSERT lst:k3 AFTER c APPENDED
    assert_match "LRANGE after INSERT" LRANGE lst:k3 0 -1
    rcli RPUSH lst:k4 a >/dev/null 2>&1; mcli RPUSH lst:k4 a >/dev/null 2>&1
    assert_match "LPOS"                LPOS lst:k4 a
    rcli RPUSH lst:src x y z >/dev/null 2>&1; mcli RPUSH lst:src x y z >/dev/null 2>&1
    assert_match "LMOVE"               LMOVE lst:src lst:dst LEFT RIGHT
fi

# ===========================================================================
# HASH COMMANDS
# ===========================================================================

if should_run "hash"; then
    echo ""
    echo "=== HASH COMMANDS ==="
    flush_both

    assert_match "HSET single"         HSET hsh:k1 f1 v1
    assert_match "HSET multi"          HSET hsh:k1 f2 v2 f3 v3
    assert_match "HGET"                HGET hsh:k1 f1
    assert_match "HGET (missing)"      HGET hsh:k1 missing
    assert_match "HMSET"               HMSET hsh:k1 f4 v4 f5 v5
    assert_match "HMGET"               HMGET hsh:k1 f1 f2 f3 f4 f5 missing
    assert_match_sorted "HGETALL"      HGETALL hsh:k1
    assert_match_sorted "HKEYS"       HKEYS hsh:k1
    assert_match_sorted "HVALS"       HVALS hsh:k1
    assert_match "HLEN"                HLEN hsh:k1
    assert_match "HEXISTS (yes)"       HEXISTS hsh:k1 f1
    assert_match "HEXISTS (no)"        HEXISTS hsh:k1 missing
    assert_match "HDEL"                HDEL hsh:k1 f5
    assert_match "HSETNX (new)"        HSETNX hsh:k1 f6 v6
    assert_match "HSETNX (exists)"     HSETNX hsh:k1 f6 v6b
    assert_moon_ok "HSCAN"             HSCAN hsh:k1 0
    assert_match "HINCRBY"             HINCRBY hsh:k1 counter 10
    assert_match "HINCRBY (again)"     HINCRBY hsh:k1 counter 5
    assert_match "HINCRBYFLOAT"        HINCRBYFLOAT hsh:k1 fcounter 1.5
fi

# ===========================================================================
# SET COMMANDS
# ===========================================================================

if should_run "set"; then
    echo ""
    echo "=== SET COMMANDS ==="
    flush_both

    assert_match "SADD"                SADD s:k1 a b c d e
    assert_match "SADD dup"            SADD s:k1 a b f
    assert_match "SCARD"               SCARD s:k1
    assert_match "SISMEMBER (yes)"     SISMEMBER s:k1 a
    assert_match "SISMEMBER (no)"      SISMEMBER s:k1 z
    assert_match "SMISMEMBER"          SMISMEMBER s:k1 a z c
    assert_match "SREM"                SREM s:k1 a f
    assert_match "SCARD after SREM"    SCARD s:k1
    # Set ops
    rcli SADD s:A 1 2 3 >/dev/null 2>&1; mcli SADD s:A 1 2 3 >/dev/null 2>&1
    rcli SADD s:B 2 3 4 >/dev/null 2>&1; mcli SADD s:B 2 3 4 >/dev/null 2>&1
    assert_match_sorted "SINTER"       SINTER s:A s:B
    assert_match_sorted "SUNION"      SUNION s:A s:B
    assert_match_sorted "SDIFF"       SDIFF s:A s:B
    assert_match "SINTERSTORE"         SINTERSTORE s:intres s:A s:B
    assert_match "SUNIONSTORE"         SUNIONSTORE s:unires s:A s:B
    assert_match "SDIFFSTORE"          SDIFFSTORE s:difres s:A s:B
    assert_moon_ok "SPOP"              SPOP s:k1
    assert_moon_ok "SRANDMEMBER"       SRANDMEMBER s:A
    assert_moon_ok "SMEMBERS"          SMEMBERS s:A
    assert_moon_ok "SSCAN"             SSCAN s:A 0
fi

# ===========================================================================
# SORTED SET COMMANDS
# ===========================================================================

if should_run "sorted_set"; then
    echo ""
    echo "=== SORTED SET COMMANDS ==="
    flush_both

    assert_match "ZADD"                ZADD z:k1 1 a 2 b 3 c 4 d 5 e
    assert_match "ZADD update"         ZADD z:k1 10 a
    assert_match "ZCARD"               ZCARD z:k1
    assert_match "ZSCORE"              ZSCORE z:k1 a
    assert_match "ZSCORE (missing)"    ZSCORE z:k1 missing
    assert_match "ZRANK"               ZRANK z:k1 b
    assert_match "ZREVRANK"            ZREVRANK z:k1 b
    assert_match "ZRANGE"              ZRANGE z:k1 0 -1
    assert_match "ZRANGE WITHSCORES"   ZRANGE z:k1 0 -1 WITHSCORES
    assert_match "ZREVRANGE"           ZREVRANGE z:k1 0 2
    assert_match "ZRANGEBYSCORE"       ZRANGEBYSCORE z:k1 2 5
    # ZREVRANGEBYSCORE on clean key (ZPOPMIN/ZPOPMAX above mutated z:k1)
    rcli ZADD z:revtest 1 alpha 2 beta 3 gamma >/dev/null 2>&1; mcli ZADD z:revtest 1 alpha 2 beta 3 gamma >/dev/null 2>&1
    assert_match "ZRANGEBYSCORE 2"     ZRANGEBYSCORE z:revtest 1 3
    # ZREVRANGEBYSCORE: known issue — moon returns empty for rev range queries
    # TODO: investigate src/command/sorted_set.rs zrevrangebyscore implementation
    assert_match "ZREVRANGEBYSCORE"    ZREVRANGEBYSCORE z:revtest +inf -inf
    assert_match "ZCOUNT"              ZCOUNT z:k1 2 5
    assert_match "ZINCRBY"             ZINCRBY z:k1 100 b
    assert_match "ZREM"                ZREM z:k1 e
    assert_match "ZPOPMIN"             ZPOPMIN z:k1
    assert_match "ZPOPMAX"             ZPOPMAX z:k1
    assert_match "ZLEXCOUNT"           ZLEXCOUNT z:k1 - +
    # Store ops
    rcli ZADD z:A 1 a 2 b 3 c >/dev/null 2>&1; mcli ZADD z:A 1 a 2 b 3 c >/dev/null 2>&1
    rcli ZADD z:B 2 b 3 c 4 d >/dev/null 2>&1; mcli ZADD z:B 2 b 3 c 4 d >/dev/null 2>&1
    assert_match "ZUNIONSTORE"         ZUNIONSTORE z:union 2 z:A z:B
    assert_match "ZINTERSTORE"         ZINTERSTORE z:inter 2 z:A z:B
    assert_moon_ok "ZSCAN"             ZSCAN z:A 0
fi

# ===========================================================================
# KEY COMMANDS
# ===========================================================================

if should_run "key"; then
    echo ""
    echo "=== KEY COMMANDS ==="
    flush_both

    rcli SET k:1 v1 >/dev/null 2>&1; mcli SET k:1 v1 >/dev/null 2>&1
    rcli SET k:2 v2 >/dev/null 2>&1; mcli SET k:2 v2 >/dev/null 2>&1
    rcli SET k:3 v3 >/dev/null 2>&1; mcli SET k:3 v3 >/dev/null 2>&1
    assert_match "EXISTS (yes)"        EXISTS k:1
    assert_match "EXISTS (no)"         EXISTS k:missing
    assert_match "EXISTS multi"        EXISTS k:1 k:2 k:missing
    assert_match "DEL single"          DEL k:3
    assert_match "DEL multi"           DEL k:1 k:2
    rcli SET k:ttl v EX 1000 >/dev/null 2>&1; mcli SET k:ttl v EX 1000 >/dev/null 2>&1
    assert_match_ttl "TTL (with expiry)" TTL k:ttl
    assert_match_pttl "PTTL (with expiry)" PTTL k:ttl
    rcli SET k:nox v >/dev/null 2>&1; mcli SET k:nox v >/dev/null 2>&1
    assert_match "TTL (no expiry)"     TTL k:nox
    assert_match "EXPIRE"              EXPIRE k:nox 500
    assert_match_ttl "TTL after EXPIRE" TTL k:nox
    assert_match "PEXPIRE"             PEXPIRE k:nox 500000
    assert_match "PERSIST"             PERSIST k:nox
    assert_match "TTL after PERSIST"   TTL k:nox
    assert_match "TYPE string"         TYPE k:nox
    rcli LPUSH k:lst a >/dev/null 2>&1; mcli LPUSH k:lst a >/dev/null 2>&1
    assert_match "TYPE list"           TYPE k:lst
    rcli SADD k:st a >/dev/null 2>&1; mcli SADD k:st a >/dev/null 2>&1
    assert_match "TYPE set"            TYPE k:st
    rcli ZADD k:zs 1 a >/dev/null 2>&1; mcli ZADD k:zs 1 a >/dev/null 2>&1
    assert_match "TYPE zset"           TYPE k:zs
    rcli HSET k:hs f v >/dev/null 2>&1; mcli HSET k:hs f v >/dev/null 2>&1
    assert_match "TYPE hash"           TYPE k:hs
    rcli SET k:ren oldval >/dev/null 2>&1; mcli SET k:ren oldval >/dev/null 2>&1
    assert_match "RENAME"              RENAME k:ren k:renamed
    assert_match "GET after RENAME"    GET k:renamed
    rcli SET k:rnx1 v1 >/dev/null 2>&1; mcli SET k:rnx1 v1 >/dev/null 2>&1
    rcli SET k:rnx2 v2 >/dev/null 2>&1; mcli SET k:rnx2 v2 >/dev/null 2>&1
    assert_match "RENAMENX (blocked)"  RENAMENX k:rnx1 k:rnx2
    assert_match "UNLINK"              UNLINK k:renamed
    assert_moon_ok "DBSIZE"            DBSIZE
    assert_moon_ok "SCAN cursor"       SCAN 0
    assert_moon_ok "KEYS pattern"      KEYS "k:*"
    assert_moon_ok "OBJECT HELP"       OBJECT HELP
fi

# ===========================================================================
# STREAM COMMANDS
# ===========================================================================

if should_run "stream"; then
    echo ""
    echo "=== STREAM COMMANDS ==="
    flush_both

    # XADD with auto-IDs: can't compare IDs across servers, test moon-only
    assert_moon_ok "XADD"              XADD stream:k1 '*' field1 value1
    assert_moon_ok "XADD 2"            XADD stream:k1 '*' field2 value2
    assert_moon_ok "XADD 3"            XADD stream:k1 '*' field3 value3
    assert_moon "XLEN"                 "3" XLEN stream:k1
    assert_moon_ok "XRANGE all"        XRANGE stream:k1 - +
    assert_moon_ok "XREVRANGE"         XREVRANGE stream:k1 + -
    assert_moon_ok "XINFO STREAM"      XINFO STREAM stream:k1
    assert_moon_ok "XTRIM MAXLEN"      XTRIM stream:k1 MAXLEN 10
    # Consumer groups
    assert_moon_ok "XGROUP CREATE"     XGROUP CREATE stream:k1 grp1 0
    assert_moon_ok "XREADGROUP"        XREADGROUP GROUP grp1 consumer1 COUNT 1 STREAMS stream:k1 '>'
    assert_moon_ok "XACK"              XACK stream:k1 grp1 0-0
    assert_moon_ok "XPENDING summary"  XPENDING stream:k1 grp1 - + 10
fi

# ===========================================================================
# CONNECTION COMMANDS
# ===========================================================================

if should_run "connection"; then
    echo ""
    echo "=== CONNECTION COMMANDS ==="

    assert_match "PING"                PING
    assert_match "PING message"        PING hello
    assert_match "ECHO"                ECHO "hello world"
    assert_moon_ok "SELECT 0"          SELECT 0
    assert_moon_ok "SELECT 1"          SELECT 1
    assert_moon_contains "INFO server" "redis_version" INFO server
    assert_moon_ok "DBSIZE"            DBSIZE
    assert_moon_ok "COMMAND"           COMMAND
    assert_moon_ok "COMMAND COUNT"     COMMAND COUNT
fi

# ===========================================================================
# PUB/SUB COMMANDS
# ===========================================================================

if should_run "pubsub"; then
    echo ""
    echo "=== PUB/SUB COMMANDS ==="
    flush_both

    # Publish to a channel (no subscribers = 0)
    assert_match "PUBLISH (no subs)"   PUBLISH chan:test "hello"
fi

# ===========================================================================
# TRANSACTION COMMANDS
# ===========================================================================

if should_run "transaction"; then
    echo ""
    echo "=== TRANSACTION COMMANDS ==="
    flush_both

    # Test MULTI/EXEC via pipe (using \n not \r\n for redis-cli pipe mode)
    TOTAL=$((TOTAL + 1))
    tx_moon=$(printf 'MULTI\nSET tx:k1 v1\nSET tx:k2 v2\nGET tx:k1\nEXEC\n' | redis-cli -p "$PORT_RUST" 2>/dev/null)
    if echo "$tx_moon" | grep -q "v1"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: MULTI/EXEC pipeline"
        echo "    GOT: $(echo "$tx_moon" | head -5)"
    fi

    # DISCARD (must be inside MULTI)
    TOTAL=$((TOTAL + 1))
    tx_discard=$(printf 'MULTI\nDISCARD\n' | redis-cli -p "$PORT_RUST" 2>/dev/null)
    if echo "$tx_discard" | grep -q "OK"; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: MULTI/DISCARD"
    fi
fi

# ===========================================================================
# SCRIPTING COMMANDS
# ===========================================================================

if should_run "scripting"; then
    echo ""
    echo "=== SCRIPTING COMMANDS ==="
    flush_both

    assert_match "EVAL return"         EVAL "return 42" 0
    assert_match "EVAL string"         EVAL "return 'hello'" 0
    rcli SET lua:k1 luaval >/dev/null 2>&1; mcli SET lua:k1 luaval >/dev/null 2>&1
    assert_match "EVAL redis.call"     EVAL "return redis.call('GET', KEYS[1])" 1 lua:k1
    assert_match "EVAL table"          EVAL "return {1,2,3}" 0
fi

# ===========================================================================
# PERSISTENCE COMMANDS
# ===========================================================================

if should_run "persistence"; then
    echo ""
    echo "=== PERSISTENCE COMMANDS ==="

    assert_moon "BGSAVE"               "Background saving started" BGSAVE
    sleep 1
fi

# ===========================================================================
# BLOCKING COMMANDS (short timeouts)
# ===========================================================================

if should_run "blocking"; then
    echo ""
    echo "=== BLOCKING COMMANDS ==="
    flush_both

    # Pre-populate so blocking commands return immediately
    rcli RPUSH blk:l1 val1 >/dev/null 2>&1; mcli RPUSH blk:l1 val1 >/dev/null 2>&1
    assert_match "BLPOP (ready)"       BLPOP blk:l1 1

    rcli RPUSH blk:l2 val2 >/dev/null 2>&1; mcli RPUSH blk:l2 val2 >/dev/null 2>&1
    assert_match "BRPOP (ready)"       BRPOP blk:l2 1

    rcli ZADD blk:z1 1 a 2 b >/dev/null 2>&1; mcli ZADD blk:z1 1 a 2 b >/dev/null 2>&1
    assert_match "BZPOPMIN (ready)"    BZPOPMIN blk:z1 1

    rcli ZADD blk:z2 1 a 2 b >/dev/null 2>&1; mcli ZADD blk:z2 1 a 2 b >/dev/null 2>&1
    assert_match "BZPOPMAX (ready)"    BZPOPMAX blk:z2 1

    rcli RPUSH blk:src x y z >/dev/null 2>&1; mcli RPUSH blk:src x y z >/dev/null 2>&1
    assert_match "BLMOVE (ready)"      BLMOVE blk:src blk:dst LEFT RIGHT 1
fi

# ===========================================================================
# REDIS-BENCHMARK THROUGHPUT
# ===========================================================================

if should_run "benchmark" && [[ "$SKIP_BENCH" == "false" ]]; then
    echo ""
    echo "=== REDIS-BENCHMARK THROUGHPUT (moon, 1000 requests each) ==="
    echo ""

    # String commands
    assert_bench "SET"                    "-t set"
    assert_bench "GET"                    "-t get"
    assert_bench "MSET (10 keys)"         "-t mset"
    assert_bench "INCR"                   "-t incr"
    assert_bench "APPEND"                 "" -c 50 APPEND bench:append hello

    # List commands
    assert_bench "LPUSH"                  "-t lpush"
    assert_bench "RPUSH"                  "-t rpush"
    assert_bench "LPOP"                   "-t lpop"
    assert_bench "RPOP"                   "-t rpop"
    assert_bench "LRANGE 100"             "" -c 50 LRANGE bench:list 0 99
    assert_bench "LRANGE 300"             "" -c 50 LRANGE bench:list 0 299

    # Hash commands
    assert_bench "HSET"                   "-t hset"

    # Set commands
    assert_bench "SADD"                   "-t sadd"
    assert_bench "SPOP"                   "-t spop"

    # Sorted set commands
    assert_bench "ZADD"                   "-t zadd"
    assert_bench "ZPOPMIN"                "-t zpopmin"

    # Key commands
    assert_bench "PING inline"            "-t ping_inline"
    assert_bench "PING mbulk"             "-t ping_mbulk"

    # Pipeline scaling
    echo ""
    echo "  --- Pipeline scaling (SET) ---"
    for p in 1 4 16 64; do
        assert_bench "SET p=$p"           "-t set -P $p"
    done
    echo ""
    echo "  --- Pipeline scaling (GET) ---"
    for p in 1 4 16 64; do
        assert_bench "GET p=$p"           "-t get -P $p"
    done
fi

# ===========================================================================
# Summary
# ===========================================================================

echo ""
echo "==========================================="
echo "  COMMAND COVERAGE TEST RESULTS"
echo "==========================================="
echo ""
echo "  Total:  $TOTAL"
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
if [[ "$SKIP_BENCH" == "false" ]] && should_run "benchmark"; then
    echo ""
    echo "  Benchmarks: $BENCH_PASS passed, $BENCH_FAIL failed"
fi
echo ""

if [[ "$FAIL" -gt 0 ]]; then
    echo "  STATUS: FAIL ($FAIL failures)"
    exit 1
else
    echo "  STATUS: ALL PASSED"
    exit 0
fi
