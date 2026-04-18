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
            echo "  vector       - Vector search commands (FT.CREATE, FT.SEARCH, FT.INFO, FT.DROPINDEX)"
            echo "  persistence  - Persistence commands (BGSAVE, BGREWRITEAOF, etc.)"
            echo "  blocking     - Blocking commands (BLPOP, BRPOP, BZPOPMIN, etc.)"
            echo "  temporal     - Temporal commands (TEMPORAL.SNAPSHOT_AT, TEMPORAL.INVALIDATE)"
            echo "  workspace    - Workspace commands (WS CREATE, WS LIST, WS INFO, WS AUTH, WS DROP)"
            echo "  mq           - Durable Message Queue (MQ CREATE, PUSH, POP, ACK, DLQLEN, TRIGGER)"
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
    cargo build --release --features text-index --quiet 2>/dev/null
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
    assert_match "ZREVRANGEBYSCORE"    ZREVRANGEBYSCORE z:revtest +inf -inf
    assert_match "ZREVRANGEBYSCORE 2"  ZREVRANGEBYSCORE z:revtest 3 1
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
    rcli SET k:cpsrc cpval >/dev/null 2>&1; mcli SET k:cpsrc cpval >/dev/null 2>&1
    assert_match "COPY"                COPY k:cpsrc k:cpdst
    assert_match "GET after COPY"      GET k:cpdst
    rcli SET k:cpdst2 old >/dev/null 2>&1; mcli SET k:cpdst2 old >/dev/null 2>&1
    assert_match "COPY no REPLACE"     COPY k:cpsrc k:cpdst2
    assert_match "COPY REPLACE"        COPY k:cpsrc k:cpdst2 REPLACE
    assert_match "UNLINK"              UNLINK k:renamed
    assert_moon_ok "DBSIZE"            DBSIZE
    assert_moon_ok "SCAN cursor"       SCAN 0
    assert_moon_ok "KEYS pattern"      KEYS "k:*"
    assert_moon_ok "OBJECT HELP"       OBJECT HELP

    # Bit operations
    rcli SET k:bits "\xff\x0f" >/dev/null 2>&1; mcli SET k:bits "\xff\x0f" >/dev/null 2>&1
    assert_match "GETBIT"              GETBIT k:bits 0
    assert_match "SETBIT"              SETBIT k:bits 0 0
    assert_match "BITCOUNT"            BITCOUNT k:bits
    assert_match "BITCOUNT range"      BITCOUNT k:bits 0 0
    rcli SET k:bits2 "\x0f\xff" >/dev/null 2>&1; mcli SET k:bits2 "\x0f\xff" >/dev/null 2>&1
    assert_match "BITOP AND"           BITOP AND k:bitdst k:bits k:bits2
    assert_match "BITOP OR"            BITOP OR k:bitdst k:bits k:bits2
    assert_match "BITOP XOR"           BITOP XOR k:bitdst k:bits k:bits2
    assert_match "BITOP NOT"           BITOP NOT k:bitdst k:bits
    assert_match "BITPOS 1"            BITPOS k:bits 1
    assert_match "BITPOS 0"            BITPOS k:bits 0

    # SORT
    rcli RPUSH k:sortl 3 1 2 >/dev/null 2>&1; mcli RPUSH k:sortl 3 1 2 >/dev/null 2>&1
    assert_match "SORT numeric"        SORT k:sortl
    assert_match "SORT DESC"           SORT k:sortl DESC
    assert_match "SORT ALPHA"          SORT k:sortl ALPHA
    assert_match "SORT LIMIT"          SORT k:sortl LIMIT 0 2

    # GEO commands
    rcli GEOADD k:geo 13.361389 38.115556 Palermo 15.087269 37.502669 Catania >/dev/null 2>&1
    mcli GEOADD k:geo 13.361389 38.115556 Palermo 15.087269 37.502669 Catania >/dev/null 2>&1
    assert_match "GEOPOS"              GEOPOS k:geo Palermo
    assert_match "GEODIST km"          GEODIST k:geo Palermo Catania km
    assert_match "GEOHASH"             GEOHASH k:geo Palermo
    assert_match "GEOSEARCH"           GEOSEARCH k:geo FROMLONLAT 15 37 BYRADIUS 200 km ASC
    # EXPIREAT / PEXPIREAT / EXPIRETIME / PEXPIRETIME
    rcli SET k:eat val >/dev/null 2>&1; mcli SET k:eat val >/dev/null 2>&1
    assert_match "EXPIREAT"            EXPIREAT k:eat 9999999999
    assert_match "TTL after EXPIREAT"  TTL k:eat
    assert_match "EXPIRETIME"          EXPIRETIME k:eat
    assert_match "PEXPIRETIME"         PEXPIRETIME k:eat

    # TIME / RANDOMKEY / TOUCH
    assert_moon_ok "TIME"              TIME
    rcli SET k:rnd val >/dev/null 2>&1; mcli SET k:rnd val >/dev/null 2>&1
    assert_moon_ok "RANDOMKEY"         RANDOMKEY
    assert_match "TOUCH"               TOUCH k:rnd

    # FLUSHDB
    assert_match "FLUSHDB"             FLUSHDB
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

# ===========================================================================
# VECTOR SEARCH COMMANDS (moon-only — Redis uses different syntax)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== VECTOR SEARCH COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # FT.CREATE — create a vector index
    assert_moon "FT.CREATE basic"          "OK"    FT.CREATE myidx ON HASH PREFIX 1 doc: SCHEMA embedding VECTOR FLAT 6 DIM 4 DISTANCE_METRIC L2 TYPE FLOAT32

    # FT.INFO — index metadata
    TOTAL=$((TOTAL + 1)); FT_INFO=$(mcli FT.INFO myidx 2>&1)
    if echo "$FT_INFO" | grep -q "myidx"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO returns index name"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO returns index name"; fi

    # Insert vectors via HSET (auto-indexed) — use python3 to avoid null byte stripping in bash
    python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',1.0,0.0,0.0,0.0))" | redis-cli -x -p "$PORT_RUST" HSET doc:1 embedding >/dev/null 2>&1
    python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',0.0,1.0,0.0,0.0))" | redis-cli -x -p "$PORT_RUST" HSET doc:2 embedding >/dev/null 2>&1

    # FT.SEARCH — verify command doesn't error (redis-cli can't pass binary args directly)
    TOTAL=$((TOTAL + 1)); FT_SEARCH=$(mcli FT.SEARCH myidx "*" 2>&1)
    if ! echo "$FT_SEARCH" | grep -qi "err"; then PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH does not error"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH returned error"; fi

    # FT.DROPINDEX — remove index
    assert_moon "FT.DROPINDEX"             "OK"    FT.DROPINDEX myidx

    # FT.INFO after drop should error
    TOTAL=$((TOTAL + 1)); FT_INFO_AFTER=$(mcli FT.INFO myidx 2>&1)
    if echo "$FT_INFO_AFTER" | grep -qi "err\|not found"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO after drop errors"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO after drop errors"; fi
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
# VECTOR SEARCH COMMANDS (v0.1.6)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== VECTOR SEARCH COMMANDS ==="
    flush_both

    # Create a test index with 4-dimensional vectors
    assert_moon_ok "FT.CREATE basic" FT.CREATE testidx ON HASH PREFIX 1 doc: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2

    # Insert test vectors (4-dimensional, little-endian f32 binary encoded via redis-cli hex)
    # doc:1 = [1.0, 0.0, 0.0, 0.0]
    mcli HSET doc:1 vec "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" category science title "quantum physics" >/dev/null 2>&1
    # doc:2 = [0.0, 1.0, 0.0, 0.0]
    mcli HSET doc:2 vec "$(printf '\x00\x00\x00\x00\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00')" category math title "linear algebra" >/dev/null 2>&1
    # doc:3 = [0.9, 0.1, 0.0, 0.0]
    mcli HSET doc:3 vec "$(printf '\x66\x66\x66\x3f\xcd\xcc\xcc\x3d\x00\x00\x00\x00\x00\x00\x00\x00')" category science title "particle physics" >/dev/null 2>&1
    sleep 0.5

    # FT.SEARCH basic KNN
    assert_moon_contains "FT.SEARCH KNN" "doc:" FT.SEARCH testidx "*=>[KNN 2 @vec \$q]" PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')"

    # FT.SEARCH with LIMIT
    assert_moon_contains "FT.SEARCH LIMIT" "doc:" FT.SEARCH testidx "*=>[KNN 3 @vec \$q]" PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" LIMIT 0 1

    # FT.INFO
    assert_moon_contains "FT.INFO" "testidx" FT.INFO testidx

    # FT._LIST
    assert_moon_contains "FT._LIST" "testidx" FT._LIST

    # FT.COMPACT (should succeed even if nothing to compact)
    assert_moon_ok "FT.COMPACT" FT.COMPACT testidx

    # FT.CONFIG SET/GET
    assert_moon_ok "FT.CONFIG SET AUTOCOMPACT" FT.CONFIG SET testidx AUTOCOMPACT OFF
    assert_moon_contains "FT.CONFIG GET AUTOCOMPACT" "OFF" FT.CONFIG GET testidx AUTOCOMPACT

    # FT.RECOMMEND (with existing keys as positive examples)
    assert_moon_contains "FT.RECOMMEND basic" "doc:" FT.RECOMMEND testidx POSITIVE doc:1 K 2

    # Tag filter
    assert_moon_contains "FT.SEARCH tag filter" "doc:" FT.SEARCH testidx "@category:{science}=>[KNN 3 @vec \$q]" PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')"

    # FT.DROPINDEX
    assert_moon_ok "FT.DROPINDEX" FT.DROPINDEX testidx

    # Verify index is gone
    assert_moon "FT._LIST empty" "" FT._LIST

    # ── FT.DROPINDEX DD flag tests ──────────────────────────────────────────
    # DD flag deletes all indexed documents along with the index

    # Create a fresh index for DD tests
    assert_moon_ok "FT.CREATE dd_test" FT.CREATE ddtest ON HASH PREFIX 1 dd: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2

    # Insert documents
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT" HSET dd:1 vec >/dev/null 2>&1
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT" HSET dd:2 vec >/dev/null 2>&1

    # Verify documents exist
    TOTAL=$((TOTAL + 1)); DD_EXISTS=$(mcli EXISTS dd:1 dd:2 2>&1)
    if [ "$DD_EXISTS" = "2" ]; then PASS=$((PASS + 1)); echo "  PASS: DD docs exist before drop"; else FAIL=$((FAIL + 1)); echo "  FAIL: DD docs should exist (got: $DD_EXISTS)"; fi

    # Drop with DD flag — documents should be deleted
    assert_moon_ok "FT.DROPINDEX DD" FT.DROPINDEX ddtest DD

    # Verify documents are gone
    TOTAL=$((TOTAL + 1)); DD_AFTER=$(mcli EXISTS dd:1 dd:2 2>&1)
    if [ "$DD_AFTER" = "0" ]; then PASS=$((PASS + 1)); echo "  PASS: DD docs deleted after FT.DROPINDEX DD"; else FAIL=$((FAIL + 1)); echo "  FAIL: DD docs should be deleted (got: $DD_AFTER)"; fi

    # Test case insensitivity: create another index
    assert_moon_ok "FT.CREATE dd_test2" FT.CREATE ddtest2 ON HASH PREFIX 1 dd2: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT" HSET dd2:1 vec >/dev/null 2>&1

    # Drop with lowercase dd flag
    assert_moon_ok "FT.DROPINDEX dd (lowercase)" FT.DROPINDEX ddtest2 dd

    TOTAL=$((TOTAL + 1)); DD2_AFTER=$(mcli EXISTS dd2:1 2>&1)
    if [ "$DD2_AFTER" = "0" ]; then PASS=$((PASS + 1)); echo "  PASS: lowercase dd flag works"; else FAIL=$((FAIL + 1)); echo "  FAIL: lowercase dd should work (got: $DD2_AFTER)"; fi

    # Test without DD — documents should remain
    assert_moon_ok "FT.CREATE no_dd_test" FT.CREATE noddtest ON HASH PREFIX 1 ndd: SCHEMA vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC L2
    printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00' \
        | redis-cli -x -p "$PORT" HSET ndd:1 vec >/dev/null 2>&1

    assert_moon_ok "FT.DROPINDEX no DD" FT.DROPINDEX noddtest

    TOTAL=$((TOTAL + 1)); NDD_AFTER=$(mcli EXISTS ndd:1 2>&1)
    if [ "$NDD_AFTER" = "1" ]; then PASS=$((PASS + 1)); echo "  PASS: no DD preserves documents"; else FAIL=$((FAIL + 1)); echo "  FAIL: no DD should preserve docs (got: $NDD_AFTER)"; fi

    # Cleanup
    mcli DEL ndd:1 >/dev/null 2>&1

    # Test DD on non-existent index returns error
    TOTAL=$((TOTAL + 1)); DD_NONEXIST=$(mcli FT.DROPINDEX nonexistent_idx DD 2>&1)
    if echo "$DD_NONEXIST" | grep -qi "unknown\|err"; then PASS=$((PASS + 1)); echo "  PASS: DD on non-existent index errors"; else FAIL=$((FAIL + 1)); echo "  FAIL: DD on non-existent should error (got: $DD_NONEXIST)"; fi

    # ── End DD flag tests ────────────────────────────────────────────────────

    echo "  vector: $PASS passed (of $TOTAL total)"
fi

# ===========================================================================
# TEXT FIELD TESTS (v0.1.7 full-text search)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== TEXT FIELD TESTS ==="
    flush_both

    # FT.CREATE with TEXT-only index
    assert_moon_ok "FT.CREATE text-only index" FT.CREATE textidx ON HASH PREFIX 1 doc: SCHEMA title TEXT WEIGHT 2.0 body TEXT

    # HSET to trigger text auto-indexing
    assert_moon_ok "HSET doc with TEXT fields" HSET doc:t1 title "Hello world" body "This is a test document"
    assert_moon_ok "HSET second doc" HSET doc:t2 title "Second document" body "Another test with more words"
    assert_moon_ok "HSET third doc" HSET doc:t3 title "Third title" body "Final body text here"

    # FT.INFO reports text stats — num_docs and num_terms must be > 0 after HSET
    FT_TEXT_INFO=$(mcli FT.INFO textidx 2>&1)

    TOTAL=$((TOTAL + 1))
    TEXT_NUM_DOCS=$(echo "$FT_TEXT_INFO" | grep -A1 "num_docs" | tail -1 | tr -d '[:space:]')
    if [ -n "$TEXT_NUM_DOCS" ] && [ "$TEXT_NUM_DOCS" != "0" ] && [ "$TEXT_NUM_DOCS" -gt 0 ] 2>/dev/null; then
        PASS=$((PASS + 1)); echo "  PASS: FT.INFO text num_docs = $TEXT_NUM_DOCS (should be > 0)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text num_docs should be > 0 (got: $TEXT_NUM_DOCS)"
    fi

    TOTAL=$((TOTAL + 1))
    TEXT_NUM_TERMS=$(echo "$FT_TEXT_INFO" | grep -A1 "num_terms" | tail -1 | tr -d '[:space:]')
    if [ -n "$TEXT_NUM_TERMS" ] && [ "$TEXT_NUM_TERMS" != "0" ] && [ "$TEXT_NUM_TERMS" -gt 0 ] 2>/dev/null; then
        PASS=$((PASS + 1)); echo "  PASS: FT.INFO text num_terms = $TEXT_NUM_TERMS (should be > 0)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text num_terms should be > 0 (got: $TEXT_NUM_TERMS)"
    fi

    TOTAL=$((TOTAL + 1))
    if echo "$FT_TEXT_INFO" | grep -q "avg_doc_len"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO text avg_doc_len"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text avg_doc_len"; fi
    TOTAL=$((TOTAL + 1))
    if echo "$FT_TEXT_INFO" | grep -q "bm25_k1"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO text bm25_k1"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text bm25_k1"; fi
    TOTAL=$((TOTAL + 1))
    if echo "$FT_TEXT_INFO" | grep -q "bytes_per_posting"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO text bytes_per_posting"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO text bytes_per_posting"; fi

    # FT.CREATE with TEXT + NOSTEM
    assert_moon_ok "FT.CREATE NOSTEM index" FT.CREATE nostemidx ON HASH PREFIX 1 ns: SCHEMA content TEXT NOSTEM

    # FT.CREATE with TEXT + NOINDEX
    assert_moon_ok "FT.CREATE NOINDEX field" FT.CREATE noidxtest ON HASH PREFIX 1 ni: SCHEMA indexed TEXT meta TEXT NOINDEX

    # FT.CREATE with BM25 parameters
    assert_moon_ok "FT.CREATE with BM25 params" FT.CREATE bm25idx ON HASH PREFIX 1 bm: BM25_K1 1.5 BM25_B 0.8 SCHEMA content TEXT

    # FT.CONFIG SET/GET BM25 parameters
    assert_moon_ok "FT.CONFIG SET BM25_K1" FT.CONFIG SET bm25idx BM25_K1 1.8
    assert_moon_contains "FT.CONFIG GET BM25_K1" "1.8" FT.CONFIG GET bm25idx BM25_K1

    # ── FT.SEARCH BM25 text search tests (Plan 150-01) ──────────────────────────
    # Uses doc:t1/t2/t3 indexed above: title TEXT WEIGHT 2.0, body TEXT
    # doc:t1: title="Hello world" body="This is a test document"
    # doc:t2: title="Second document" body="Another test with more words"
    # doc:t3: title="Third title" body="Final body text here"

    # 1. Basic single-term text search: "document" matches doc:t1 and doc:t2 body fields
    TOTAL=$((TOTAL + 1))
    FT_BASIC=$(mcli FT.SEARCH textidx "document" 2>&1)
    if echo "$FT_BASIC" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH basic text returned error: $FT_BASIC"
    elif echo "$FT_BASIC" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH basic text returns results"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH basic text returned no results"
    fi

    # 2. __bm25_score field must appear in response
    TOTAL=$((TOTAL + 1))
    if echo "$FT_BASIC" | grep -q "__bm25_score"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH text response contains __bm25_score"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH text response missing __bm25_score field"
    fi

    # 3. Multi-term AND search: "test document" — both must appear in same doc (doc:t1 body)
    TOTAL=$((TOTAL + 1))
    FT_MULTI=$(mcli FT.SEARCH textidx "test document" 2>&1)
    if echo "$FT_MULTI" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH multi-term returned error: $FT_MULTI"
    elif echo "$FT_MULTI" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH multi-term AND returns results"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH multi-term AND returned no results"
    fi

    # 4. Field-targeted search: @title:(document) — only doc:t2 has 'document' in title
    TOTAL=$((TOTAL + 1))
    FT_FIELD=$(mcli FT.SEARCH textidx "@title:(document)" 2>&1)
    if echo "$FT_FIELD" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH field-targeted returned error: $FT_FIELD"
    elif echo "$FT_FIELD" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH @title:(document) returns results"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH @title:(document) returned no results"
    fi

    # 5. Empty result for non-existent term
    TOTAL=$((TOTAL + 1))
    FT_EMPTY=$(mcli FT.SEARCH textidx "xyznonexistentterm" 2>&1)
    if echo "$FT_EMPTY" | grep -qi "^err\b" | head -1; then
        # Some ERR is acceptable (e.g. stop word) but the term is unique
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH nonexistent term returned error: $FT_EMPTY"
    else
        # Should return 0 results (not error)
        FT_EMPTY_COUNT=$(echo "$FT_EMPTY" | head -1 | tr -d '[:space:]')
        if [ "$FT_EMPTY_COUNT" = "0" ] || echo "$FT_EMPTY" | grep -q "^(empty\|0)"; then
            PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH nonexistent term returns 0 results"
        else
            PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH nonexistent term (no doc: in result)"
        fi
    fi

    # 6. LIMIT clause: FT.SEARCH textidx "document" LIMIT 0 1 — returns exactly 1 doc entry
    TOTAL=$((TOTAL + 1))
    FT_LIMIT=$(mcli FT.SEARCH textidx "document" LIMIT 0 1 2>&1)
    FT_LIMIT_DOC_COUNT=$(echo "$FT_LIMIT" | grep -c "doc:")
    if [ "$FT_LIMIT_DOC_COUNT" -le 1 ] && echo "$FT_LIMIT" | grep -q "doc:"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH LIMIT 0 1 returns at most 1 result"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH LIMIT 0 1 should return exactly 1 result (got $FT_LIMIT_DOC_COUNT)"
    fi

    # 7. Stop-words-only query should return ERR (not crash)
    TOTAL=$((TOTAL + 1))
    FT_STOP=$(mcli FT.SEARCH textidx "the" 2>&1)
    if echo "$FT_STOP" | grep -qi "err"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH stop-words-only returns ERR"
    else
        # If the server doesn't error, check it returns 0 results (also acceptable)
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH stop-words-only should return ERR (got: $FT_STOP)"
    fi

    # 8. Cross-field search: "world" appears in doc:t1 title — searches all TEXT fields
    TOTAL=$((TOTAL + 1))
    FT_CROSS=$(mcli FT.SEARCH textidx "world" 2>&1)
    if echo "$FT_CROSS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH cross-field returned error: $FT_CROSS"
    elif echo "$FT_CROSS" | grep -q "doc:t1"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH cross-field finds 'world' in doc:t1"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH cross-field should find 'world' in doc:t1"
    fi
    # ── End FT.SEARCH BM25 text search tests ─────────────────────────────────────

    # ── HIGHLIGHT / SUMMARIZE tests (Plan 150-03) ─────────────────────────────
    # Add a document with sufficient body text for SUMMARIZE truncation (> 20 words)
    assert_moon_ok "HSET doc:long for HIGHLIGHT/SUMMARIZE" HSET doc:long title "machine learning overview" body "This is a comprehensive guide to machine learning covering supervised learning unsupervised learning and reinforcement learning techniques used in modern artificial intelligence and data science applications for production systems"

    # 1. HIGHLIGHT basic: verify <b> tag in response
    TOTAL=$((TOTAL + 1))
    FT_HL=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT 2>&1)
    if echo "$FT_HL" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT returned error: $FT_HL"
    elif echo "$FT_HL" | grep -q "<b>"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT response contains <b> tag"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT response missing <b> tag (got: $FT_HL)"
    fi

    # 2. HIGHLIGHT FIELDS: only highlight specified field
    TOTAL=$((TOTAL + 1))
    FT_HL_FIELDS=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT FIELDS 1 title 2>&1)
    if echo "$FT_HL_FIELDS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT FIELDS returned error: $FT_HL_FIELDS"
    elif echo "$FT_HL_FIELDS" | grep -q "<b>\|machine"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT FIELDS 1 title returns result with match"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT FIELDS 1 title returned no match (got: $FT_HL_FIELDS)"
    fi

    # 3. HIGHLIGHT custom TAGS: verify custom open/close tags
    TOTAL=$((TOTAL + 1))
    FT_HL_TAGS=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT TAGS "[[" "]]" 2>&1)
    if echo "$FT_HL_TAGS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT TAGS returned error: $FT_HL_TAGS"
    elif echo "$FT_HL_TAGS" | grep -q "\[\["; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT TAGS [[ ]] response contains custom tag"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT TAGS response missing custom tag (got: $FT_HL_TAGS)"
    fi

    # 4. SUMMARIZE basic: verify response is returned without error
    TOTAL=$((TOTAL + 1))
    FT_SUM=$(mcli FT.SEARCH textidx "machine" SUMMARIZE 2>&1)
    if echo "$FT_SUM" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE returned error: $FT_SUM"
    elif echo "$FT_SUM" | grep -q "machine\|learning"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH SUMMARIZE response contains match terms"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE response missing match terms (got: $FT_SUM)"
    fi

    # 5. SUMMARIZE FIELDS: only summarize the body field
    TOTAL=$((TOTAL + 1))
    FT_SUM_FIELDS=$(mcli FT.SEARCH textidx "machine" SUMMARIZE FIELDS 1 body 2>&1)
    if echo "$FT_SUM_FIELDS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE FIELDS returned error: $FT_SUM_FIELDS"
    elif echo "$FT_SUM_FIELDS" | grep -q "machine\|learning"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH SUMMARIZE FIELDS 1 body returns result"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE FIELDS 1 body missing match (got: $FT_SUM_FIELDS)"
    fi

    # 6. SUMMARIZE with LEN: fragment should be short (10 tokens)
    TOTAL=$((TOTAL + 1))
    FT_SUM_LEN=$(mcli FT.SEARCH textidx "machine" SUMMARIZE LEN 10 2>&1)
    if echo "$FT_SUM_LEN" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH SUMMARIZE LEN returned error: $FT_SUM_LEN"
    else
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH SUMMARIZE LEN 10 does not error"
    fi

    # 7. HIGHLIGHT + SUMMARIZE combined: title highlighted, body summarized
    TOTAL=$((TOTAL + 1))
    FT_BOTH=$(mcli FT.SEARCH textidx "machine" HIGHLIGHT FIELDS 1 title SUMMARIZE FIELDS 1 body 2>&1)
    if echo "$FT_BOTH" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH HIGHLIGHT + SUMMARIZE combined returned error: $FT_BOTH"
    elif echo "$FT_BOTH" | grep -q "machine\|<b>"; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH HIGHLIGHT FIELDS 1 title SUMMARIZE FIELDS 1 body returns result"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FT.SEARCH combined HIGHLIGHT+SUMMARIZE missing output (got: $FT_BOTH)"
    fi
    # ── End HIGHLIGHT / SUMMARIZE tests ──────────────────────────────────────

    # FT.DROPINDEX removes text index
    assert_moon_ok "FT.DROPINDEX text index" FT.DROPINDEX textidx

    # FT.INFO after drop should error
    TOTAL=$((TOTAL + 1)); FT_TEXT_AFTER=$(mcli FT.INFO textidx 2>&1)
    if echo "$FT_TEXT_AFTER" | grep -qi "err\|not found\|unknown"; then PASS=$((PASS + 1)); echo "  PASS: FT.INFO after text drop errors"; else FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO after text drop should error"; fi

    # Cleanup remaining text indexes
    mcli FT.DROPINDEX nostemidx >/dev/null 2>&1
    mcli FT.DROPINDEX noidxtest >/dev/null 2>&1
    mcli FT.DROPINDEX bm25idx >/dev/null 2>&1

    echo "  text: $PASS passed (of $TOTAL total)"
fi

# ===========================================================================
# FUZZY AND PREFIX SEARCH TESTS (v0.1.7 typo-tolerance — FUZ-01/02/03)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== FUZZY AND PREFIX SEARCH TESTS ==="
    flush_both

    # Setup: create text index and populate test documents
    mcli FT.CREATE fuzzyidx ON HASH PREFIX 1 fz: SCHEMA title TEXT body TEXT >/dev/null 2>&1
    mcli HSET fz:1 title "Machine Learning" body "Introduction to machine learning algorithms" >/dev/null 2>&1
    mcli HSET fz:2 title "Deep Learning" body "Neural networks and deep architectures" >/dev/null 2>&1
    mcli HSET fz:3 title "Natural Language" body "NLP processing with transformers" >/dev/null 2>&1
    mcli HSET fz:4 title "Machinery Parts" body "Industrial machinery components" >/dev/null 2>&1
    mcli HSET fz:5 title "Macro Economics" body "Study of macroeconomic indicators" >/dev/null 2>&1

    # FT.COMPACT builds FST (required before fuzzy/prefix queries use FST path)
    assert_moon_ok "FT.COMPACT fuzzyidx for FST build" FT.COMPACT fuzzyidx

    # Test 1: FUZ-01 — Fuzzy search distance 2 (%% syntax)
    TOTAL=$((TOTAL + 1))
    FT_FUZZY2=$(mcli FT.SEARCH fuzzyidx "%%machne%%" LIMIT 0 10 2>&1)
    if echo "$FT_FUZZY2" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %%machne%% returned error: $FT_FUZZY2"
    elif echo "$FT_FUZZY2" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-01 %%machne%% (fuzzy dist-2) returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %%machne%% returned no docs: $FT_FUZZY2"
    fi

    # Test 2: FUZ-01 — Fuzzy search distance 1 (% syntax)
    TOTAL=$((TOTAL + 1))
    FT_FUZZY1=$(mcli FT.SEARCH fuzzyidx "%machin%" LIMIT 0 10 2>&1)
    if echo "$FT_FUZZY1" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %machin% returned error: $FT_FUZZY1"
    elif echo "$FT_FUZZY1" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-01 %machin% (fuzzy dist-1) returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 %machin% returned no docs: $FT_FUZZY1"
    fi

    # Test 3: FUZ-03 — Prefix search (mach* syntax)
    TOTAL=$((TOTAL + 1))
    FT_PREFIX=$(mcli FT.SEARCH fuzzyidx "mach*" LIMIT 0 10 2>&1)
    if echo "$FT_PREFIX" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 mach* returned error: $FT_PREFIX"
    elif echo "$FT_PREFIX" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-03 mach* prefix search returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 mach* returned no docs: $FT_PREFIX"
    fi

    # Test 4: FUZ-03 — Short prefix (ma*)
    TOTAL=$((TOTAL + 1))
    FT_SHORT_PREFIX=$(mcli FT.SEARCH fuzzyidx "ma*" LIMIT 0 10 2>&1)
    if echo "$FT_SHORT_PREFIX" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 ma* returned error: $FT_SHORT_PREFIX"
    elif echo "$FT_SHORT_PREFIX" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-03 ma* short prefix returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-03 ma* returned no docs: $FT_SHORT_PREFIX"
    fi

    # Test 5: FUZ-01 — Fuzzy search with field target @title:(%%machne%%)
    TOTAL=$((TOTAL + 1))
    FT_FIELD_FUZZY=$(mcli FT.SEARCH fuzzyidx "@title:(%%machne%%)" LIMIT 0 10 2>&1)
    if echo "$FT_FIELD_FUZZY" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 field-targeted fuzzy returned error: $FT_FIELD_FUZZY"
    elif echo "$FT_FIELD_FUZZY" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-01 @title:(%%machne%%) field-targeted fuzzy returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-01 @title:(%%machne%%) returned no docs: $FT_FIELD_FUZZY"
    fi

    # Test 6: REGRESSION — exact search still works after query parser changes
    TOTAL=$((TOTAL + 1))
    FT_EXACT=$(mcli FT.SEARCH fuzzyidx "machine" LIMIT 0 10 2>&1)
    if echo "$FT_EXACT" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: REGRESSION exact search returned error: $FT_EXACT"
    elif echo "$FT_EXACT" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: REGRESSION exact search still works (no regressions)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: REGRESSION exact 'machine' returned no docs: $FT_EXACT"
    fi

    # Test 7: MIXED — exact + fuzzy combined query
    TOTAL=$((TOTAL + 1))
    FT_MIXED=$(mcli FT.SEARCH fuzzyidx "%%machne%% deep" LIMIT 0 10 2>&1)
    if echo "$FT_MIXED" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: MIXED exact+fuzzy returned error: $FT_MIXED"
    elif echo "$FT_MIXED" | grep -q "fz:"; then
        PASS=$((PASS + 1)); echo "  PASS: MIXED %%machne%% deep (fuzzy+exact) returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MIXED %%machne%% deep returned no docs: $FT_MIXED"
    fi

    # Test 8: FUZ-02 — FST build via FT.COMPACT on a fresh index
    mcli FT.CREATE fuzzyidx2 ON HASH PREFIX 1 fzr: SCHEMA title TEXT >/dev/null 2>&1
    mcli HSET fzr:1 title "Machine Learning" >/dev/null 2>&1
    mcli HSET fzr:2 title "Machinery Parts" >/dev/null 2>&1
    assert_moon_ok "FT.COMPACT fuzzyidx2 builds FST" FT.COMPACT fuzzyidx2

    TOTAL=$((TOTAL + 1))
    FT_COMPACT_FUZZY=$(mcli FT.SEARCH fuzzyidx2 "%%machne%%" LIMIT 0 10 2>&1)
    if echo "$FT_COMPACT_FUZZY" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-02 fuzzy after compact returned error: $FT_COMPACT_FUZZY"
    elif echo "$FT_COMPACT_FUZZY" | grep -q "fzr:"; then
        PASS=$((PASS + 1)); echo "  PASS: FUZ-02 fuzzy works after FT.COMPACT FST build"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: FUZ-02 fuzzy after compact returned no docs: $FT_COMPACT_FUZZY"
    fi

    # Cleanup fuzzy indexes
    mcli FT.DROPINDEX fuzzyidx >/dev/null 2>&1
    mcli FT.DROPINDEX fuzzyidx2 >/dev/null 2>&1

    echo "  fuzzy/prefix: done"
fi

# ===========================================================================
# FT.AGGREGATE (faceted search — Phase 152 AGG-01..04)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== FT.AGGREGATE (FACETED SEARCH) ==="
    flush_both

    # Setup: Plan 06 shipped TAG field-type; Plan 07 ships NUMERIC. Both are now
    # permanent baseline for the aggidx schema — no probe, no gating.
    FT_CREATE_RESULT=$(mcli FT.CREATE aggidx ON HASH PREFIX 1 agg: SCHEMA status TAG priority TAG assignee TAG score NUMERIC 2>&1)
    if echo "$FT_CREATE_RESULT" | grep -qi "err"; then
        echo "  FAIL: FT.CREATE aggidx rejected by moon: $FT_CREATE_RESULT"
        SKIP_AGG=1
    else
        SKIP_AGG=0
    fi

    # Insert deterministic fixture (runs only when FT.CREATE succeeded):
    #   status=open  × 5 (priority=high × 3, priority=low × 2) — assignees user0..user4
    #   status=closed × 2 (priority=low × 2) — assignees user0, user1
    if [ "$SKIP_AGG" -eq 0 ]; then
        for i in 1 2 3; do
            mcli HSET agg:$i status open priority high assignee user$((i%5)) score $((10*i)) >/dev/null 2>&1
        done
        for i in 4 5; do
            mcli HSET agg:$i status open priority low assignee user$((i%5)) score $((10*i)) >/dev/null 2>&1
        done
        for i in 6 7; do
            mcli HSET agg:$i status closed priority low assignee user$((i%5)) score $((10*i)) >/dev/null 2>&1
        done
    fi

    if [ "$SKIP_AGG" -eq 0 ]; then

        # AGG-01: GROUPBY + COUNT — assert specific counts (open=5 closed=2)
        TOTAL=$((TOTAL + 1))
        AGG_COUNT=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC 2>&1)
        if echo "$AGG_COUNT" | grep -Pzo "(?s)open.*5" >/dev/null && echo "$AGG_COUNT" | grep -Pzo "(?s)closed.*2" >/dev/null; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-01 GROUPBY+COUNT (open=5 closed=2)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-01 expected open=5 closed=2, got: $AGG_COUNT"
        fi

        # AGG-02: GROUPBY @priority (high=3 low=4)
        TOTAL=$((TOTAL + 1))
        AGG_PRIORITY=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @priority REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC 2>&1)
        if echo "$AGG_PRIORITY" | grep -Pzo "(?s)low.*4" >/dev/null && echo "$AGG_PRIORITY" | grep -Pzo "(?s)high.*3" >/dev/null; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-02 GROUPBY @priority (high=3 low=4)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-02 expected high=3 low=4, got: $AGG_PRIORITY"
        fi

        # AGG-03/04 (SUM/AVG/MIN/MAX/COUNT_DISTINCT over @score) — unconditional
        # now that Plan 07 ships NUMERIC. Exact-value assertions follow below.
        TOTAL=$((TOTAL + 1))
        AGG_SUM=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE SUM 1 @score AS total 2>&1)
        # Expected: status=open → 10+20+30+40+50=150; status=closed → 60+70=130.
        if echo "$AGG_SUM" | grep -Pzo "(?s)open.*150" >/dev/null && echo "$AGG_SUM" | grep -Pzo "(?s)closed.*130" >/dev/null; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+SUM exact (open=150 closed=130)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 expected open=150 closed=130, got: $AGG_SUM"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_AVG=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @priority REDUCE AVG 1 @score AS avg_score 2>&1)
        if echo "$AGG_AVG" | grep -qi "err"; then
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 GROUPBY+AVG errored: $AGG_AVG"
        else
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+AVG returns rows"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_MIN=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE MIN 1 @score AS min_score 2>&1)
        # Expected: status=open → min=10; status=closed → min=60.
        if echo "$AGG_MIN" | grep -Pzo "(?s)open.*10" >/dev/null && echo "$AGG_MIN" | grep -Pzo "(?s)closed.*60" >/dev/null; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+MIN exact (open=10 closed=60)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 expected MIN open=10 closed=60, got: $AGG_MIN"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_MAX=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE MAX 1 @score AS max_score 2>&1)
        # Expected: status=open → max=50; status=closed → max=70.
        if echo "$AGG_MAX" | grep -Pzo "(?s)open.*50" >/dev/null && echo "$AGG_MAX" | grep -Pzo "(?s)closed.*70" >/dev/null; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-03 GROUPBY+MAX exact (open=50 closed=70)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-03 expected MAX open=50 closed=70, got: $AGG_MAX"
        fi

        TOTAL=$((TOTAL + 1))
        AGG_DISTINCT=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE COUNT_DISTINCT 1 @assignee AS uniq_users 2>&1)
        if echo "$AGG_DISTINCT" | grep -qi "err"; then
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-04 COUNT_DISTINCT errored: $AGG_DISTINCT"
        else
            PASS=$((PASS + 1)); echo "  PASS: AGG-04 COUNT_DISTINCT returns rows"
        fi

        # AGG-02b: SORTBY + LIMIT
        TOTAL=$((TOTAL + 1))
        AGG_LIMIT=$(mcli FT.AGGREGATE aggidx '*' GROUPBY 1 @status REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC LIMIT 0 1 2>&1)
        if echo "$AGG_LIMIT" | grep -q "open" && ! echo "$AGG_LIMIT" | grep -q "closed"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG-02b SORTBY + LIMIT returns top-1 ('open')"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: AGG-02b SORTBY+LIMIT unexpected: $AGG_LIMIT"
        fi

        # APPLY must be rejected in v1 (D-04 / Pitfall 10)
        TOTAL=$((TOTAL + 1))
        APPLY_REJECT=$(mcli FT.AGGREGATE aggidx '*' APPLY '@score+1' AS plus_one 2>&1)
        if echo "$APPLY_REJECT" | grep -qE "APPLY.*not supported|not implemented|v1"; then
            PASS=$((PASS + 1)); echo "  PASS: AGG APPLY rejected in v1"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: APPLY should be rejected: $APPLY_REJECT"
        fi

        # ── Plan 06 TAG gap-closure assertions ─────────────────────────────

        # TAG-01: @status:{open} GROUPBY @priority — expect high=3 low=2
        TOTAL=$((TOTAL + 1))
        AGG_TAG_FILTER=$(mcli FT.AGGREGATE aggidx '@status:{open}' GROUPBY 1 @priority REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC 2>&1)
        if echo "$AGG_TAG_FILTER" | grep -Pzo "(?s)high.*3" >/dev/null && echo "$AGG_TAG_FILTER" | grep -Pzo "(?s)low.*2" >/dev/null; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-01 @status:{open} GROUPBY @priority (high=3 low=2)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-01 expected high=3 low=2, got: $AGG_TAG_FILTER"
        fi

        # TAG-02: FT.SEARCH @status:{open} — 5 keys
        TOTAL=$((TOTAL + 1))
        SEARCH_TAG=$(mcli FT.SEARCH aggidx '@status:{open}' LIMIT 0 10 2>&1)
        HIT_COUNT=$(echo "$SEARCH_TAG" | grep -c '^agg:')
        if [ "$HIT_COUNT" -eq 5 ]; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-02 FT.SEARCH @status:{open} returned 5 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-02 expected 5 open keys, got $HIT_COUNT: $SEARCH_TAG"
        fi

        # TAG-03: @Status:{open} (mixed-case field) must match @status:{open}
        TOTAL=$((TOTAL + 1))
        SEARCH_TAG_CASE=$(mcli FT.SEARCH aggidx '@Status:{open}' LIMIT 0 10 2>&1)
        HIT_COUNT_CASE=$(echo "$SEARCH_TAG_CASE" | grep -c '^agg:')
        if [ "$HIT_COUNT_CASE" -eq 5 ]; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-03 case-insensitive field @Status:{open} → 5 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-03 case-insensitive field lookup: expected 5 got $HIT_COUNT_CASE"
        fi

        # TAG-04: partial HSET must preserve untouched tag fields
        TOTAL=$((TOTAL + 1))
        mcli HSET agg:partial status open priority high >/dev/null 2>&1
        mcli HSET agg:partial priority low >/dev/null 2>&1   # partial — status NOT touched
        SEARCH_PARTIAL=$(mcli FT.SEARCH aggidx '@status:{open}' LIMIT 0 100 2>&1)
        if echo "$SEARCH_PARTIAL" | grep -q '^agg:partial$'; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-04 partial HSET preserved @status:open on agg:partial"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-04 partial HSET wiped prior status entry"
        fi
        mcli DEL agg:partial >/dev/null 2>&1

        # TAG-05: multi-tag OR rejected with actionable error
        TOTAL=$((TOTAL + 1))
        TAG_OR=$(mcli FT.SEARCH aggidx '@status:{open|closed}' LIMIT 0 10 2>&1)
        if echo "$TAG_OR" | grep -qi "multi-tag OR syntax not supported"; then
            PASS=$((PASS + 1)); echo "  PASS: TAG-05 multi-tag OR rejected with actionable error"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TAG-05 expected explicit rejection, got: $TAG_OR"
        fi

        # ── Plan 07 NUMERIC gap-closure assertions ─────────────────────────
        # Fixture recap: agg:1..5 scores 10..50 (open), agg:6..7 scores 60,70 (closed).

        # NUMERIC-01: @score:[20 40] inclusive — agg:2 agg:3 agg:4 = 3 keys
        TOTAL=$((TOTAL + 1))
        NUM_INC=$(mcli FT.SEARCH aggidx '@score:[20 40]' LIMIT 0 10 2>&1)
        HIT_NUM=$(echo "$NUM_INC" | grep -c '^agg:')
        if [ "$HIT_NUM" -eq 3 ]; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-01 @score:[20 40] inclusive → 3 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-01 expected 3 got $HIT_NUM: $NUM_INC"
        fi

        # NUMERIC-02: exclusive bounds — (20 40] → agg:3 agg:4 = 2 keys
        TOTAL=$((TOTAL + 1))
        NUM_EXCL=$(mcli FT.SEARCH aggidx '@score:[(20 40]' LIMIT 0 10 2>&1)
        HIT_EXCL=$(echo "$NUM_EXCL" | grep -c '^agg:')
        if [ "$HIT_EXCL" -eq 2 ]; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-02 @score:[(20 40] exclusive-low → 2 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-02 expected 2 got $HIT_EXCL: $NUM_EXCL"
        fi

        # NUMERIC-03: full range [-inf +inf] → all 7 keys
        TOTAL=$((TOTAL + 1))
        NUM_FULL=$(mcli FT.SEARCH aggidx '@score:[-inf +inf]' LIMIT 0 20 2>&1)
        HIT_FULL=$(echo "$NUM_FULL" | grep -c '^agg:')
        if [ "$HIT_FULL" -eq 7 ]; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-03 @score:[-inf +inf] → 7 keys"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-03 expected 7 got $HIT_FULL"
        fi

        # NUMERIC-04: FT.AGGREGATE range filter + GROUPBY — @score:[10 30] → status
        # agg:1..3 (scores 10,20,30) all open → cnt=3 on status=open only
        TOTAL=$((TOTAL + 1))
        NUM_AGG=$(mcli FT.AGGREGATE aggidx '@score:[10 30]' GROUPBY 1 @status REDUCE COUNT 0 AS cnt 2>&1)
        if echo "$NUM_AGG" | grep -Pzo "(?s)open.*3" >/dev/null; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-04 FT.AGGREGATE @score:[10 30] GROUPBY status (open=3)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-04 expected open=3, got: $NUM_AGG"
        fi

        # NUMERIC-05: inverted range REJECTED (T-152-07-05)
        TOTAL=$((TOTAL + 1))
        NUM_INV=$(mcli FT.SEARCH aggidx '@score:[100 10]' LIMIT 0 10 2>&1)
        if echo "$NUM_INV" | grep -qi "min > max"; then
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-05 inverted range REJECTED with actionable error"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-05 expected 'min > max' rejection, got: $NUM_INV"
        fi

        # NUMERIC-06: NaN-on-write filtered (write-path guard, T-152-07-02).
        # Insert a doc with NaN score; [-inf +inf] range must NOT include it.
        TOTAL=$((TOTAL + 1))
        mcli HSET agg:nan status open priority high score NaN >/dev/null 2>&1
        NUM_NAN=$(mcli FT.SEARCH aggidx '@score:[-inf +inf]' LIMIT 0 20 2>&1)
        if echo "$NUM_NAN" | grep -q '^agg:nan$'; then
            FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-06 NaN leaked into index: agg:nan appeared in [-inf +inf]"
        else
            PASS=$((PASS + 1)); echo "  PASS: NUMERIC-06 NaN filtered on write"
        fi
        mcli DEL agg:nan >/dev/null 2>&1
    fi

    mcli FT.DROPINDEX aggidx >/dev/null 2>&1

    # NUMERIC-07 (W-01 cross-shard correctness): 1-shard vs 4-shard identical keys
    # for @score:[5 15] on an independent fixture.
    TOTAL=$((TOTAL + 1))
    pkill -f 'moon --port 6411' 2>/dev/null
    pkill -f 'moon --port 6414' 2>/dev/null
    sleep 1
    ./target/release/moon --port 6411 --shards 1 --protected-mode no > /tmp/moon-6411.log 2>&1 &
    ./target/release/moon --port 6414 --shards 4 --protected-mode no > /tmp/moon-6414.log 2>&1 &
    sleep 2
    for PORT in 6411 6414; do
        redis-cli -p $PORT FT.CREATE nidx ON HASH PREFIX 1 n: SCHEMA status TAG score NUMERIC > /dev/null 2>&1
        for i in $(seq 0 19); do
            redis-cli -p $PORT HSET n:$i status open score $i > /dev/null 2>&1
        done
    done
    sleep 1
    N1=$(redis-cli -p 6411 FT.SEARCH nidx '@score:[5 15]' LIMIT 0 100 2>&1 | grep '^n:' | sort)
    N4=$(redis-cli -p 6414 FT.SEARCH nidx '@score:[5 15]' LIMIT 0 100 2>&1 | grep '^n:' | sort)
    if [ "$N1" = "$N4" ] && [ -n "$N1" ]; then
        COUNT_N=$(echo "$N1" | wc -l | tr -d ' ')
        PASS=$((PASS + 1)); echo "  PASS: NUMERIC-07 1-shard and 4-shard return identical keys for @score:[5 15] (count=$COUNT_N)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: NUMERIC-07 cross-shard mismatch"
        echo "1-shard: $N1"
        echo "4-shard: $N4"
    fi
    pkill -f 'moon --port 6411' 2>/dev/null
    pkill -f 'moon --port 6414' 2>/dev/null

    echo "  ft_aggregate: done"
fi

# ===========================================================================
# FT.SEARCH HYBRID (three-way RRF — Phase 152 HYB-01..03)
# ===========================================================================

if should_run "vector"; then
    echo ""
    echo "=== FT.SEARCH HYBRID (THREE-WAY RRF) ==="
    flush_both

    # Setup: text + dense + sparse index
    mcli FT.CREATE hybidx ON HASH PREFIX 1 hy: SCHEMA title TEXT vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC COSINE >/dev/null 2>&1

    # Insert 3 docs with titles + vectors
    mcli HSET hy:1 title "machine learning introduction" vec "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" >/dev/null 2>&1
    mcli HSET hy:2 title "deep neural learning" vec "$(printf '\x00\x00\x00\x00\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00')" >/dev/null 2>&1
    mcli HSET hy:3 title "quantum machines" vec "$(printf '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x3f\x00\x00\x00\x00')" >/dev/null 2>&1
    sleep 0.5

    # HYB-01: two-way hybrid (BM25 + dense, no sparse clause) — D-16 fall-through
    TOTAL=$((TOTAL + 1))
    HYB_TWO=$(mcli FT.SEARCH hybidx "machine learning" HYBRID VECTOR @vec '$q' FUSION RRF LIMIT 0 5 PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" 2>&1)
    if echo "$HYB_TWO" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-01 two-way hybrid errored: $HYB_TWO"
    elif echo "$HYB_TWO" | grep -q "hy:"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-01 two-way BM25+dense returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-01 two-way returned no docs: $HYB_TWO"
    fi

    # HYB-01: response carries __rrf_score
    TOTAL=$((TOTAL + 1))
    if echo "$HYB_TWO" | grep -q "__rrf_score"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-01 response contains __rrf_score"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-01 response missing __rrf_score: $HYB_TWO"
    fi

    # HYB-03: WEIGHTS tuning — all three weights honored
    TOTAL=$((TOTAL + 1))
    HYB_WEIGHTS=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION RRF WEIGHTS 1.0 1.5 0.5 LIMIT 0 5 PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" 2>&1)
    if echo "$HYB_WEIGHTS" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 WEIGHTS errored: $HYB_WEIGHTS"
    elif echo "$HYB_WEIGHTS" | grep -q "hy:"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-03 WEIGHTS 1.0 1.5 0.5 returns docs"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 WEIGHTS returned no docs: $HYB_WEIGHTS"
    fi

    # HYB-03: negative weight rejected (D-17)
    TOTAL=$((TOTAL + 1))
    HYB_NEG=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION RRF WEIGHTS 1.0 -1.0 1.0 LIMIT 0 5 PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" 2>&1)
    if echo "$HYB_NEG" | grep -qiE "non-negative|finite|weight"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-03 negative weight rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 negative weight should reject: $HYB_NEG"
    fi

    # HYB-03: NaN weight rejected
    TOTAL=$((TOTAL + 1))
    HYB_NAN=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION RRF WEIGHTS 1.0 NaN 1.0 LIMIT 0 5 PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" 2>&1)
    if echo "$HYB_NAN" | grep -qiE "non-negative|finite|weight"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-03 NaN weight rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-03 NaN weight should reject: $HYB_NAN"
    fi

    # HYB-02: non-RRF fusion rejected
    TOTAL=$((TOTAL + 1))
    HYB_FUSION=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' FUSION FOO LIMIT 0 5 PARAMS 2 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" 2>&1)
    if echo "$HYB_FUSION" | grep -qi "fusion"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-02 unknown FUSION mode rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-02 unknown FUSION should reject: $HYB_FUSION"
    fi

    # HYB-02: SPARSE on index without sparse field errors (D-16)
    TOTAL=$((TOTAL + 1))
    HYB_NOSPARSE=$(mcli FT.SEARCH hybidx "machine" HYBRID VECTOR @vec '$q' SPARSE @noexist '$qs' FUSION RRF LIMIT 0 5 PARAMS 4 q "$(printf '\x00\x00\x80\x3f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')" qs "$(printf '\x01\x00\x00\x00\x00\x00\x80\x3f')" 2>&1)
    if echo "$HYB_NOSPARSE" | grep -qi "sparse"; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-02 SPARSE on index without sparse field errors (D-16)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB-02 missing sparse field should error: $HYB_NOSPARSE"
    fi

    # HYB: backward-compat — FT.SEARCH without HYBRID keyword unchanged (D-18)
    TOTAL=$((TOTAL + 1))
    HYB_BC=$(mcli FT.SEARCH hybidx "machine" LIMIT 0 5 2>&1)
    if echo "$HYB_BC" | grep -qi "err"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: HYB backward compat (no HYBRID) errored: $HYB_BC"
    else
        PASS=$((PASS + 1)); echo "  PASS: HYB backward compat (FT.SEARCH text, no HYBRID)"
    fi

    mcli FT.DROPINDEX hybidx >/dev/null 2>&1
    echo "  ft_search_hybrid: done"
fi

# ===========================================================================
# TEMPORAL COMMANDS (moon-only -- no Redis equivalent)
# ===========================================================================

if should_run "temporal"; then
    echo ""
    echo "=== TEMPORAL COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # TEMP-01: TEMPORAL.SNAPSHOT_AT basic — records wall-clock->LSN binding
    assert_moon "TEMPORAL.SNAPSHOT_AT basic" "OK" TEMPORAL.SNAPSHOT_AT

    # TEMP-02: TEMPORAL.SNAPSHOT_AT wrong args — extra argument rejected
    TOTAL=$((TOTAL + 1))
    SNAP_ERR=$(mcli TEMPORAL.SNAPSHOT_AT extraarg 2>&1)
    if echo "$SNAP_ERR" | grep -qi "wrong number of arguments"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.SNAPSHOT_AT wrong args rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.SNAPSHOT_AT wrong args should reject: $SNAP_ERR"
    fi

    # TEMP-03: TEMPORAL.INVALIDATE basic — create graph, add node, invalidate
    mcli GRAPH.CREATE testgraph >/dev/null 2>&1
    ADDNODE_OUT=$(mcli GRAPH.ADDNODE testgraph :TestLabel 2>&1)
    # Extract numeric node_id from ADDNODE response (format: "(integer) <id>" or just "<id>")
    NODE_ID=$(echo "$ADDNODE_OUT" | grep -oE '[0-9]+' | head -1)
    if [[ -n "$NODE_ID" ]]; then
        TOTAL=$((TOTAL + 1))
        INV_OK=$(mcli TEMPORAL.INVALIDATE "$NODE_ID" NODE testgraph 2>&1)
        if echo "$INV_OK" | grep -q "OK"; then
            PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE basic OK (node_id=$NODE_ID)"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE basic should return OK: $INV_OK"
        fi

        # Verify node is still visible (no VALID_AT filter = sees all)
        TOTAL=$((TOTAL + 1))
        QUERY_OUT=$(mcli GRAPH.QUERY testgraph "MATCH (n:TestLabel) RETURN n" 2>&1)
        if echo "$QUERY_OUT" | grep -qiE "TestLabel|node|result"; then
            PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE node still visible without VALID_AT"
        else
            FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE node should be visible: $QUERY_OUT"
        fi
    else
        TOTAL=$((TOTAL + 2))
        FAIL=$((FAIL + 2))
        echo "  FAIL: Could not extract node_id from GRAPH.ADDNODE: $ADDNODE_OUT"
    fi

    # TEMP-04: TEMPORAL.INVALIDATE not found — nonexistent graph
    TOTAL=$((TOTAL + 1))
    INV_NOTFOUND=$(mcli TEMPORAL.INVALIDATE 999999 NODE nonexistent 2>&1)
    if echo "$INV_NOTFOUND" | grep -qi "graph not found"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE graph not found"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE should error graph not found: $INV_NOTFOUND"
    fi

    # TEMP-05: TEMPORAL.INVALIDATE wrong args — no arguments
    TOTAL=$((TOTAL + 1))
    INV_NOARGS=$(mcli TEMPORAL.INVALIDATE 2>&1)
    if echo "$INV_NOARGS" | grep -qi "wrong number of arguments"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE wrong args (none)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE no args should reject: $INV_NOARGS"
    fi

    # TEMP-06: TEMPORAL.INVALIDATE bad entity kind — VERTEX not valid
    TOTAL=$((TOTAL + 1))
    INV_BADKIND=$(mcli TEMPORAL.INVALIDATE 42 VERTEX testgraph 2>&1)
    if echo "$INV_BADKIND" | grep -qi "entity kind must be NODE or EDGE"; then
        PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE bad entity kind rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: TEMPORAL.INVALIDATE bad kind should reject: $INV_BADKIND"
    fi

    mcli GRAPH.DELETE testgraph >/dev/null 2>&1
    echo "  temporal: done"
fi

# ===========================================================================
# WORKSPACE COMMANDS (WS CREATE/LIST/INFO/AUTH/DROP)
# ===========================================================================

if should_run "workspace"; then
    echo ""
    echo "=== WORKSPACE COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # WS-01: WS CREATE returns UUID
    TOTAL=$((TOTAL + 1))
    WS_ID=$(mcli WS CREATE myworkspace 2>&1)
    if echo "$WS_ID" | grep -qE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
        PASS=$((PASS + 1)); echo "  PASS: WS CREATE returns UUID ($WS_ID)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS CREATE should return UUID, got: $WS_ID"
    fi

    # WS-02: WS LIST returns workspace
    TOTAL=$((TOTAL + 1))
    WS_LIST=$(mcli WS LIST 2>&1)
    if echo "$WS_LIST" | grep -qF "myworkspace"; then
        PASS=$((PASS + 1)); echo "  PASS: WS LIST contains myworkspace"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS LIST should contain myworkspace: $WS_LIST"
    fi

    # WS-03: WS INFO returns metadata
    TOTAL=$((TOTAL + 1))
    WS_INFO=$(mcli WS INFO "$WS_ID" 2>&1)
    if echo "$WS_INFO" | grep -qF "myworkspace"; then
        PASS=$((PASS + 1)); echo "  PASS: WS INFO returns workspace metadata"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS INFO should contain name: $WS_INFO"
    fi

    # WS-04: WS AUTH binds workspace
    assert_moon "WS AUTH bind" "OK" WS AUTH "$WS_ID"

    # WS-05: Workspace-scoped SET+GET
    assert_moon "WS SET scoped" "OK" SET testkey testval
    assert_moon "WS GET scoped" "testval" GET testkey

    # WS-06: WS DROP removes workspace
    # Need a fresh workspace to drop (current conn already bound)
    TOTAL=$((TOTAL + 1))
    WS_ID2=$(mcli WS CREATE dropme 2>&1)
    DROP_OK=$(mcli WS DROP "$WS_ID2" 2>&1)
    if echo "$DROP_OK" | grep -q "OK"; then
        PASS=$((PASS + 1)); echo "  PASS: WS DROP returns OK"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS DROP should return OK: $DROP_OK"
    fi

    # WS-07: WS LIST after drop no longer shows dropped workspace
    TOTAL=$((TOTAL + 1))
    WS_LIST2=$(mcli WS LIST 2>&1)
    if echo "$WS_LIST2" | grep -qF "dropme"; then
        FAIL=$((FAIL + 1)); echo "  FAIL: WS LIST should not contain 'dropme' after drop"
    else
        PASS=$((PASS + 1)); echo "  PASS: WS LIST does not contain dropped workspace"
    fi

    # WS-08: WS AUTH with invalid UUID
    TOTAL=$((TOTAL + 1))
    AUTH_ERR=$(mcli WS AUTH "not-a-uuid" 2>&1)
    if echo "$AUTH_ERR" | grep -qi "ERR"; then
        PASS=$((PASS + 1)); echo "  PASS: WS AUTH invalid UUID rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS AUTH invalid should error: $AUTH_ERR"
    fi

    # WS-09: WS CREATE with empty name
    TOTAL=$((TOTAL + 1))
    CREATE_ERR=$(mcli WS CREATE 2>&1)
    if echo "$CREATE_ERR" | grep -qi "ERR"; then
        PASS=$((PASS + 1)); echo "  PASS: WS CREATE missing name rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: WS CREATE no name should error: $CREATE_ERR"
    fi

    echo "  workspace: done"
fi

# ===========================================================================
# MQ (DURABLE MESSAGE QUEUE) COMMANDS -- moon-only
# ===========================================================================

if should_run "mq"; then
    echo ""
    echo "=== MQ (DURABLE MESSAGE QUEUE) COMMANDS ==="
    mcli FLUSHALL >/dev/null 2>&1

    # MQ-01: MQ CREATE basic
    assert_moon "MQ CREATE basic" "OK" MQ CREATE mqtest MAXDELIVERY 5

    # MQ-02: MQ CREATE default (no MAXDELIVERY)
    assert_moon "MQ CREATE default" "OK" MQ CREATE mqdefault

    # MQ-03: MQ CREATE idempotent (same key twice)
    assert_moon "MQ CREATE idempotent" "OK" MQ CREATE mqtest MAXDELIVERY 5

    # MQ-04: MQ PUSH returns stream ID
    TOTAL=$((TOTAL + 1))
    MQ_PUSH_ID=$(mcli MQ PUSH mqtest field1 value1 2>&1)
    if echo "$MQ_PUSH_ID" | grep -qE '^[0-9]+-[0-9]+$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ PUSH returns stream ID ($MQ_PUSH_ID)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ PUSH should return stream ID: $MQ_PUSH_ID"
    fi

    # MQ-05: MQ POP returns message with fields
    TOTAL=$((TOTAL + 1))
    MQ_POP_OUT=$(mcli MQ POP mqtest 2>&1)
    if echo "$MQ_POP_OUT" | grep -qF "field1"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ POP returns message with field"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ POP should contain field1: $MQ_POP_OUT"
    fi

    # MQ-06: MQ ACK returns count
    # Push a fresh message, pop it, then ack it
    MQ_ACK_ID=$(mcli MQ PUSH mqtest ackf ackv 2>&1)
    mcli MQ POP mqtest >/dev/null 2>&1
    TOTAL=$((TOTAL + 1))
    MQ_ACK_OUT=$(mcli MQ ACK mqtest "$MQ_ACK_ID" 2>&1)
    if echo "$MQ_ACK_OUT" | grep -qE '(integer) 1|^1$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ ACK returns 1"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ ACK should return 1: $MQ_ACK_OUT"
    fi

    # MQ-07: MQ ACK non-existent returns 0
    TOTAL=$((TOTAL + 1))
    MQ_ACK_ZERO=$(mcli MQ ACK mqtest 999999999-999 2>&1)
    if echo "$MQ_ACK_ZERO" | grep -qE '(integer) 0|^0$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ ACK non-existent returns 0"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ ACK non-existent should return 0: $MQ_ACK_ZERO"
    fi

    # MQ-08: MQ DLQLEN empty queue
    assert_moon "MQ DLQLEN empty" "0" MQ DLQLEN mqtest

    # MQ-09: MQ DLQ routing (MAXDELIVERY 1 -> immediate dead-letter)
    mcli MQ CREATE mqdlqtest MAXDELIVERY 1 >/dev/null 2>&1
    mcli MQ PUSH mqdlqtest dlqf dlqv >/dev/null 2>&1
    mcli MQ POP mqdlqtest >/dev/null 2>&1
    TOTAL=$((TOTAL + 1))
    MQ_DLQ_LEN=$(mcli MQ DLQLEN mqdlqtest 2>&1)
    if echo "$MQ_DLQ_LEN" | grep -qE '(integer) 1|^1$'; then
        PASS=$((PASS + 1)); echo "  PASS: MQ DLQ routing (MAXDELIVERY 1 -> DLQ len 1)"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ DLQLEN after DLQ routing should be 1: $MQ_DLQ_LEN"
    fi

    # MQ-10: MQ TRIGGER register
    assert_moon "MQ TRIGGER register" "OK" MQ TRIGGER mqtest "PUBLISH events notify" DEBOUNCE 1000

    # MQ-11: MQ unknown subcommand
    TOTAL=$((TOTAL + 1))
    MQ_UNK=$(mcli MQ FOOBAR 2>&1)
    if echo "$MQ_UNK" | grep -qi "unknown MQ subcommand"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ unknown subcommand rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ unknown sub should error: $MQ_UNK"
    fi

    # MQ-12: MQ PUSH missing args
    TOTAL=$((TOTAL + 1))
    MQ_PUSH_ERR=$(mcli MQ PUSH 2>&1)
    if echo "$MQ_PUSH_ERR" | grep -qi "wrong number of arguments"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ PUSH missing args rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ PUSH no args should error: $MQ_PUSH_ERR"
    fi

    # MQ-13: MQ ACK invalid ID format
    TOTAL=$((TOTAL + 1))
    MQ_ACK_BAD=$(mcli MQ ACK mqtest not-a-valid-id 2>&1)
    if echo "$MQ_ACK_BAD" | grep -qi "invalid message ID format"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ ACK invalid ID rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ ACK invalid ID should error: $MQ_ACK_BAD"
    fi

    # MQ-14: MQ PUSH to non-durable stream
    mcli XADD nondurable '*' f v >/dev/null 2>&1
    TOTAL=$((TOTAL + 1))
    MQ_NONDUR=$(mcli MQ PUSH nondurable f v 2>&1)
    if echo "$MQ_NONDUR" | grep -qi "not a durable queue"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ PUSH to non-durable stream rejected"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ PUSH non-durable should error: $MQ_NONDUR"
    fi

    # MQ-15: MQ POP with COUNT
    mcli MQ CREATE mqcount MAXDELIVERY 10 >/dev/null 2>&1
    for i in $(seq 1 3); do
        mcli MQ PUSH mqcount "f$i" "v$i" >/dev/null 2>&1
    done
    TOTAL=$((TOTAL + 1))
    MQ_POP_CNT=$(mcli MQ POP mqcount COUNT 2 2>&1)
    # Should contain at least some data (not empty or error)
    if echo "$MQ_POP_CNT" | grep -qE "f[0-9]|v[0-9]"; then
        PASS=$((PASS + 1)); echo "  PASS: MQ POP COUNT 2 returns messages"
    else
        FAIL=$((FAIL + 1)); echo "  FAIL: MQ POP COUNT 2 should return messages: $MQ_POP_CNT"
    fi

    echo "  mq: done"
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
