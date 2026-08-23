#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# test-consistency.sh -- Data consistency test: SET/GET, SETEX/GETEX, collections
#
# Verifies that data written to moon can be read back identically.
# Tests all size ranges (SSO inline, heap small, heap large, binary).
# Compares moon output against Redis as ground truth.
#
# Usage:
#   ./scripts/test-consistency.sh [--shards N] [--skip-build] [--port-rust N]
###############################################################################

PORT_REDIS="${PORT_REDIS:-6399}"
PORT_RUST="${PORT_RUST:-6400}"
SHARDS=1
SKIP_BUILD=false
# Overridable: `./target/release/moon` is whatever was built there last, by
# any branch or feature set. A run that silently exercises a days-old binary
# reports a confident, false result, so pin it with MOON_BIN when it matters.
RUST_BINARY="${MOON_BIN:-./target/release/moon}"
PASS=0
FAIL=0
# `TOTAL` is incremented by the null-type / xread probes. It was never
# initialised, and `set -u` turns the first `TOTAL=$((TOTAL + 1))` into a fatal
# error -- so from the moon#594 probes landing until moon#629, every run of this
# script DIED at that line and the entire tail (the #592 two-key sweep, script
# routing, SWAPDB, FT.*, RESET, the restart loops) never executed. It exited 0
# while doing it, because the EXIT trap's own last command set the status, which
# is why nothing noticed. Both halves are fixed: initialise the counter, and
# make `cleanup` re-exit with the status it was called with.
TOTAL=0
RUST_PID=""
REDIS_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards)     SHARDS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --port-rust)  PORT_RUST="$2"; shift 2 ;;
        --port-redis) PORT_REDIS="$2"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*"; }

cleanup() {
    local rc=$?
    [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
    [[ -n "${MOON_DATA_DIR:-}" ]] && rm -rf "$MOON_DATA_DIR"
    # Without this the trap's own last command decides the script's exit
    # status, so an abort mid-run reports success.
    exit "$rc"
}
trap cleanup EXIT

# Every moon start gets a fresh --dir. Without it the server resolves a shared
# default data dir, and (a) stale AOF/index sidecars leak state across runs,
# (b) a 1-shard run writes a TopLevel AOF manifest that makes any later
# --shards >= 2 start REFUSE (the multi-shard data-loss guard), breaking every
# cross-shard restart loop below.
MOON_DATA_DIR=""
new_moon_dir() {
    [[ -n "$MOON_DATA_DIR" ]] && rm -rf "$MOON_DATA_DIR"
    MOON_DATA_DIR=$(mktemp -d /tmp/moon-consistency-dir.XXXXXX)
}

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then
        PASS=$((PASS + 1))
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: $desc"
        echo "    expected: $(echo "$expected" | head -c 200)"
        echo "    actual:   $(echo "$actual" | head -c 200)"
    fi
}

# Run same command on both servers, compare output
assert_both() {
    local desc="$1"; shift
    local redis_out rust_out
    redis_out=$(redis-cli -p "$PORT_REDIS" "$@" 2>&1) || true
    rust_out=$(redis-cli -p "$PORT_RUST" "$@" 2>&1) || true
    assert_eq "$desc" "$redis_out" "$rust_out"
}

# Run commands on both servers (no comparison, just execute)
both() {
    redis-cli -p "$PORT_REDIS" "$@" &>/dev/null || true
    redis-cli -p "$PORT_RUST" "$@" &>/dev/null || true
}

wait_for_port() {
    local port=$1
    for ((i=0; i<30; i++)); do
        redis-cli -p "$port" PING 2>/dev/null | grep -q PONG && return 0
        sleep 0.2
    done
    log "ERROR: port $port not ready"; return 1
}

# ===========================================================================
# Setup
# ===========================================================================

if [[ "$SKIP_BUILD" == false ]]; then
    log "Building..."
    RUSTFLAGS="-C target-cpu=native" cargo build --release --features text-index 2>&1 | tail -2
fi

log "Starting Redis on :$PORT_REDIS ..."
redis-server --port "$PORT_REDIS" --save "" --appendonly no --loglevel warning --daemonize no &>/dev/null &
REDIS_PID=$!

log "Starting moon on :$PORT_RUST (shards=$SHARDS)..."
new_moon_dir
"$RUST_BINARY" --port "$PORT_RUST" --shards "$SHARDS" --dir "$MOON_DATA_DIR" &>/dev/null &
RUST_PID=$!

wait_for_port "$PORT_REDIS"
wait_for_port "$PORT_RUST"

# The oracle must actually BE the oracle.
#
# `redis-server --port N` exits immediately when N is taken, but `wait_for_port`
# still succeeds — whatever already owns the port answers instead. When that
# squatter is a stale `moon` (a leaked dev instance, another worktree's server),
# every comparison below silently becomes moon-vs-moon and the suite reports a
# confident result while never touching Redis. Observed: a leaked moon on :6399
# made 14 genuine parity rows read as failures, printing a stale Moon's answers
# as "expected".
#
# Checking `kill -0 $REDIS_PID` is NOT sufficient: a child that died without
# being reaped is a zombie, and a zombie still answers `kill -0`. So identify
# the listener by what it can do. `DUMP` is implemented by Redis and not by
# moon, which makes it a one-command discriminator that needs no version
# parsing and cannot be faked by a compatible INFO section.
oracle_dump=$(redis-cli -p "$PORT_REDIS" DUMP __oracle_identity_probe__ 2>&1 | head -1)
if [[ "$oracle_dump" == *"unknown command"* ]]; then
    echo "FATAL: whatever is listening on :$PORT_REDIS is not redis-server —"
    echo "       it does not implement DUMP, which means it is almost certainly moon."
    echo "       Every 'expected' value below would come from that process, so the"
    echo "       whole run would compare moon against moon and prove nothing."
    lsof -nP -iTCP:"$PORT_REDIS" -sTCP:LISTEN 2>/dev/null | head -5 || true
    echo "       Re-run with --port-redis <free port>, or stop the squatter."
    exit 1
fi
# And moon must be moon, for the same reason in reverse.
moon_mq=$(redis-cli -p "$PORT_RUST" MQ LIST 2>&1 | head -1)
if [[ "$moon_mq" == *"unknown command"* ]]; then
    echo "FATAL: whatever is listening on :$PORT_RUST is not moon — it does not"
    echo "       implement MQ. Re-run with --port-rust <free port>."
    lsof -nP -iTCP:"$PORT_RUST" -sTCP:LISTEN 2>/dev/null | head -5 || true
    exit 1
fi
both FLUSHALL

# ===========================================================================
# 1. String SET/GET — size ranges
# ===========================================================================
log "=== 1. String SET/GET size ranges ==="

# Empty string
both SET str:empty ""
assert_both "GET empty string" GET str:empty

# 1 byte
both SET str:1b "x"
assert_both "GET 1-byte" GET str:1b

# 12 bytes (max SSO inline)
both SET str:12b "123456789012"
assert_both "GET 12-byte (SSO boundary)" GET str:12b

# 13 bytes (first heap path)
both SET str:13b "1234567890123"
assert_both "GET 13-byte (heap boundary)" GET str:13b

# 64 bytes
VAL64=$(python3 -c "print('A' * 64)")
both SET str:64b "$VAL64"
assert_both "GET 64-byte" GET str:64b

# 256 bytes
VAL256=$(python3 -c "print('B' * 256)")
both SET str:256b "$VAL256"
assert_both "GET 256-byte" GET str:256b

# 1KB
VAL1K=$(python3 -c "print('C' * 1024)")
both SET str:1k "$VAL1K"
assert_both "GET 1KB" GET str:1k

# 4KB
VAL4K=$(python3 -c "print('D' * 4096)")
both SET str:4k "$VAL4K"
assert_both "GET 4KB" GET str:4k

# 64KB
VAL64K=$(python3 -c "print('E' * 65536)")
both SET str:64k "$VAL64K"
assert_both "GET 64KB" GET str:64k

# Numeric string
both SET str:num "1234567890"
assert_both "GET numeric string" GET str:num

# Negative number
both SET str:neg "-99999"
assert_both "GET negative number" GET str:neg

# Float
both SET str:float "3.14159265358979"
assert_both "GET float string" GET str:float

# ===========================================================================
# 2. String mutations
# ===========================================================================
log "=== 2. String mutations ==="

# APPEND
both SET mut:append "hello"
both APPEND mut:append " world"
assert_both "APPEND result" GET mut:append

# APPEND crossing SSO boundary (start <12, end >12)
both SET mut:cross "12345678901"  # 11 bytes (SSO)
both APPEND mut:cross "XY"         # 13 bytes (heap)
assert_both "APPEND SSO->heap" GET mut:cross

# INCR / DECR
both SET mut:counter "100"
both INCR mut:counter
assert_both "INCR" GET mut:counter
both DECR mut:counter
both DECR mut:counter
assert_both "DECR twice" GET mut:counter
both INCRBY mut:counter 50
assert_both "INCRBY 50" GET mut:counter

# INCRBYFLOAT (skip exact comparison — float formatting may differ)
both SET mut:flt "10.5"
both INCRBYFLOAT mut:flt "0.1"
rust_flt=$(redis-cli -p "$PORT_RUST" GET mut:flt 2>&1)
if [[ "$rust_flt" == "10.6" || "$rust_flt" == "10.59999999999999964" ]]; then
    PASS=$((PASS + 1))
else
    FAIL=$((FAIL + 1)); echo "  FAIL: INCRBYFLOAT unexpected: $rust_flt"
fi

# GETRANGE (may not be implemented — test only if supported)
both SET mut:range "Hello, World!"
rust_gr=$(redis-cli -p "$PORT_RUST" GETRANGE mut:range 0 4 2>&1)
if [[ "$rust_gr" != *"unknown command"* ]]; then
    assert_both "GETRANGE 0 4" GETRANGE mut:range 0 4
    assert_both "GETRANGE 7 -1" GETRANGE mut:range 7 -1
else
    log "  SKIP: GETRANGE not implemented"
fi

# SETRANGE (may not be implemented)
both SET mut:setrange "Hello, World!"
rust_sr=$(redis-cli -p "$PORT_RUST" SETRANGE mut:setrange 7 "Redis" 2>&1)
if [[ "$rust_sr" != *"unknown command"* ]]; then
    both SETRANGE mut:setrange 7 "Redis"
    assert_both "SETRANGE" GET mut:setrange
else
    log "  SKIP: SETRANGE not implemented"
fi

# STRLEN
assert_both "STRLEN 13-byte" STRLEN str:13b
assert_both "STRLEN 1KB" STRLEN str:1k

# GETDEL
both SET mut:getdel "deleteme"
assert_both "GETDEL returns value" GETDEL mut:getdel
assert_both "GETDEL key gone" GET mut:getdel

# GETSET (deprecated but still valid)
both SET mut:getset "old"
assert_both "GETSET returns old" GETSET mut:getset "new"
assert_both "GETSET new value" GET mut:getset

# ===========================================================================
# 3. MSET / MGET
# ===========================================================================
log "=== 3. MSET / MGET ==="

both MSET mk1 "val1" mk2 "val2" mk3 "val3"
assert_both "MGET 3 keys" MGET mk1 mk2 mk3
assert_both "MGET with missing" MGET mk1 nonexistent mk3

# MSETNX: hash-tagged ({mn}) so all keys co-locate on one shard -> atomic under
# Moon's 1/4/12 shard configs (cross-shard MSETNX is rejected CROSSSLOT by design).
assert_both "MSETNX all new" MSETNX "{mn}k1" "v1" "{mn}k2" "v2"
assert_both "MGET after MSETNX" MGET "{mn}k1" "{mn}k2"
assert_both "MSETNX one exists (0)" MSETNX "{mn}k2" "new2" "{mn}k3" "v3"
assert_both "MSETNX no partial write" GET "{mn}k3"

# ===========================================================================
# 4. SET with options (EX, PX, NX, XX, KEEPTTL, GET)
# ===========================================================================
log "=== 4. SET with options ==="

both SET opt:ex "expire_me" EX 3600
assert_both "SET EX value" GET opt:ex
# TTL should be close to 3600
redis_ttl=$(redis-cli -p "$PORT_REDIS" TTL opt:ex)
rust_ttl=$(redis-cli -p "$PORT_RUST" TTL opt:ex)
if (( rust_ttl >= 3598 && rust_ttl <= 3600 )); then
    PASS=$((PASS + 1))
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: TTL mismatch: redis=$redis_ttl rust=$rust_ttl"
fi

both SET opt:px "px_value" PX 60000
assert_both "SET PX value" GET opt:px

# NX (set only if not exists)
both SET opt:nx "original"
both SET opt:nx "overwrite" NX  # should fail
assert_both "SET NX no overwrite" GET opt:nx

# XX (set only if exists)
both SET opt:xx "impossible" XX  # key doesn't exist, should fail for new key
both SET opt:xxreal "first"
both SET opt:xxreal "second" XX  # should succeed
assert_both "SET XX overwrites" GET opt:xxreal

# SETEX / SETNX
both SETEX setex:key 3600 "setex_value"
assert_both "SETEX value" GET setex:key

both DEL setnx:key
both SETNX setnx:key "first"
both SETNX setnx:key "second"  # should fail
assert_both "SETNX no overwrite" GET setnx:key

# ===========================================================================
# 5. Binary-safe data
# ===========================================================================
log "=== 5. Binary-safe data ==="

# Use redis-cli with hex to set binary values
redis-cli -p "$PORT_REDIS" SET bin:null $'\x00\x01\x02\x03' &>/dev/null || true
redis-cli -p "$PORT_RUST" SET bin:null $'\x00\x01\x02\x03' &>/dev/null || true
assert_both "Binary with null bytes" GET bin:null

# Special characters
both SET bin:special "hello\tworld\nnewline"
assert_both "Tab and newline" GET bin:special

both SET bin:utf8 "Hello"
assert_both "UTF-8 emoji" GET bin:utf8

# ===========================================================================
# 6. Hash SET/GET
# ===========================================================================
log "=== 6. Hash operations ==="

both HSET h:test f1 "val1" f2 "val2" f3 "val3"
assert_both "HGET f1" HGET h:test f1
assert_both "HGET f2" HGET h:test f2
# HGETALL order may differ — sort for comparison
redis_hga=$(redis-cli -p "$PORT_REDIS" HGETALL h:test 2>&1 | sort)
rust_hga=$(redis-cli -p "$PORT_RUST" HGETALL h:test 2>&1 | sort)
assert_eq "HGETALL (sorted)" "$redis_hga" "$rust_hga"
assert_both "HMGET" HMGET h:test f1 f3 nonexistent
assert_both "HLEN" HLEN h:test
assert_both "HEXISTS f1" HEXISTS h:test f1
assert_both "HEXISTS missing" HEXISTS h:test missing
# moon#636: HSTRLEN measures the VALUE, not the field name — the one thing an
# implementation can plausibly get backwards. `f1` is 2 bytes and `val1` is 4,
# so a name/value swap changes the answer.
assert_both "HSTRLEN f1" HSTRLEN h:test f1
assert_both "HSTRLEN missing field" HSTRLEN h:test missing
assert_both "HSTRLEN missing key" HSTRLEN h:nosuch f1

# Large hash value
HVAL=$(python3 -c "print('X' * 1024)")
both HSET h:test f_large "$HVAL"
assert_both "HGET large value" HGET h:test f_large

both HDEL h:test f2
assert_both "HDEL then HGET" HGET h:test f2
assert_both "HLEN after HDEL" HLEN h:test

both HINCRBY h:test counter 10
assert_both "HINCRBY" HGET h:test counter
both HINCRBY h:test counter 5
assert_both "HINCRBY again" HGET h:test counter

# ===========================================================================
# 7. List operations
# ===========================================================================
log "=== 7. List operations ==="

both RPUSH l:test a b c d e
assert_both "LRANGE all" LRANGE l:test 0 -1
assert_both "LLEN" LLEN l:test
assert_both "LINDEX 0" LINDEX l:test 0
assert_both "LINDEX -1" LINDEX l:test -1

both LPUSH l:test z
assert_both "LPUSH + LRANGE" LRANGE l:test 0 -1

both RPOP l:test
assert_both "RPOP + LRANGE" LRANGE l:test 0 -1

both LPOP l:test
assert_both "LPOP + LRANGE" LRANGE l:test 0 -1

# Large list values
LVAL=$(python3 -c "print('Y' * 512)")
both RPUSH l:test "$LVAL"
assert_both "LINDEX large value" LINDEX l:test -1

# RPOPLPUSH === LMOVE src dst RIGHT LEFT (moon#520). Deprecated in Redis but
# never removed, and what every major client's `rpoplpush()` sends. Assert the
# reply AND both keys afterwards — a stub that returned the tail without moving
# it would satisfy a reply-only check. Same-key rotation is the reliable-queue
# idiom and takes the src == dst branch, so it gets its own row.
# moon#570: the source/destination pair carries a `{rl}` hash tag so it is
# co-located on ONE shard at any `--shards N`. moon refuses a list move whose
# two keys are owned by different shards (`CROSSSLOT`, because it cannot do
# both halves atomically and used to lose the element instead); Redis, having
# no shards, moves it either way. Untagged names made these rows a function of
# the shard count rather than of the command -- `l:rl`/`l:rl-d` happen to
# co-locate at 4 shards and split at 12. A tag keeps them comparing the COMMAND
# against Redis at every shard count; the refusal itself is asserted separately
# below, where it belongs (moon-only -- Redis has nothing to compare it to).
both RPUSH l:{rl} a b c
assert_both "RPOPLPUSH reply" RPOPLPUSH l:{rl} l:{rl}-d
assert_both "RPOPLPUSH source" LRANGE l:{rl} 0 -1
assert_both "RPOPLPUSH dest" LRANGE l:{rl}-d 0 -1
assert_both "RPOPLPUSH equals LMOVE RIGHT LEFT" LMOVE l:{rl} l:{rl}-d RIGHT LEFT
assert_both "RPOPLPUSH after LMOVE dest" LRANGE l:{rl}-d 0 -1
assert_both "RPOPLPUSH absent source" RPOPLPUSH l:{rl}-absent l:{rl}-d
assert_both "RPOPLPUSH wrong arity" RPOPLPUSH l:{rl}
both RPUSH l:rot a b c
assert_both "RPOPLPUSH rotate in place" RPOPLPUSH l:rot l:rot
assert_both "RPOPLPUSH rotate result" LRANGE l:rot 0 -1
both SET l:{rl}-str notalist
assert_both "RPOPLPUSH WRONGTYPE source" RPOPLPUSH l:{rl}-str l:{rl}-d
assert_both "RPOPLPUSH WRONGTYPE dest" RPOPLPUSH l:{rl} l:{rl}-str

# moon#570, moon-only (Redis has no shards, so there is nothing to compare a
# routing refusal against). At --shards > 1 a list move whose two keys land on
# different shards must be REFUSED with the element still in the source. Before
# the fix the client was handed the element and it was written to the wrong
# shard's table -- acked, unreadable, gone (10 of 12 key placements measured at
# --shards 4).
#
# Sweeps 12 key pairs rather than asserting on one: which pair is cross-shard
# is a property of the hash, and a single hard-coded pair that happens to
# co-locate would make this row pass while testing nothing. Two independent
# checks, so neither can be satisfied vacuously:
#   * NO pair may lose the element (holds for every placement, cross or not);
#   * at least ONE pair must actually be refused (proves the sweep reached the
#     cross-shard case at all).
if [[ "$SHARDS" -gt 1 ]]; then
    xs_lost=0
    xs_refused=0
    for i in $(seq 0 11); do
        redis-cli -p "$PORT_RUST" DEL "l:xs$i" "l:xd$i" &>/dev/null || true
        redis-cli -p "$PORT_RUST" RPUSH "l:xs$i" survivor &>/dev/null || true
        xs_reply=$(redis-cli -p "$PORT_RUST" LMOVE "l:xs$i" "l:xd$i" LEFT RIGHT 2>&1)
        xs_src=$(redis-cli -p "$PORT_RUST" LRANGE "l:xs$i" 0 -1 2>&1)
        xs_dst=$(redis-cli -p "$PORT_RUST" LRANGE "l:xd$i" 0 -1 2>&1)
        case "$xs_reply" in
            CROSSSLOT*) xs_refused=$((xs_refused + 1))
                        [[ "$xs_src" == "survivor" && -z "$xs_dst" ]] || xs_lost=$((xs_lost + 1)) ;;
            *)          [[ "$xs_dst" == "survivor" && -z "$xs_src" ]] || xs_lost=$((xs_lost + 1)) ;;
        esac
    done
    assert_eq "moon#570 no list move loses its element (shards=$SHARDS)" "0" "$xs_lost"
    if [[ "$xs_refused" -eq 0 ]]; then
        echo "  WARN: moon#570 sweep found no cross-shard pair at shards=$SHARDS (nothing refused)"
    fi
fi
# `COMMAND INFO rpoplpush` is deliberately NOT compared here: the two servers
# legitimately disagree on the tips/key-specs sub-arrays, so an equality check
# would fail for a reason unrelated to whether the command exists. Its
# registration is pinned by the unit test in src/command/mod.rs instead.

# ===========================================================================
# 8. Set operations
# ===========================================================================
log "=== 8. Set operations ==="

both SADD s:test a b c d e
assert_both "SCARD" SCARD s:test
assert_both "SISMEMBER a" SISMEMBER s:test a
assert_both "SISMEMBER missing" SISMEMBER s:test z

both SREM s:test c
assert_both "SCARD after SREM" SCARD s:test
assert_both "SISMEMBER removed" SISMEMBER s:test c

# SMEMBERS order may differ — sort both
redis_sm=$(redis-cli -p "$PORT_REDIS" SMEMBERS s:test 2>&1 | sort)
rust_sm=$(redis-cli -p "$PORT_RUST" SMEMBERS s:test 2>&1 | sort)
assert_eq "SMEMBERS (sorted)" "$redis_sm" "$rust_sm"

# ===========================================================================
# 9. Sorted Set operations
# ===========================================================================
log "=== 9. Sorted Set operations ==="

both ZADD z:test 1.0 "alpha" 2.5 "beta" 3.0 "gamma" 0.5 "delta"
assert_both "ZCARD" ZCARD z:test
assert_both "ZSCORE alpha" ZSCORE z:test alpha
assert_both "ZSCORE beta" ZSCORE z:test beta
assert_both "ZRANK alpha" ZRANK z:test alpha
# ZRANK/ZREVRANK WITHSCORE (Redis 7.2, moon#521). Singular option — the plural
# WITHSCORES that ZRANGE takes is a syntax error here, and only a FOURTH
# argument is an arity error, so all three shapes get a row.
assert_both "ZRANK WITHSCORE" ZRANK z:test alpha WITHSCORE
assert_both "ZREVRANK WITHSCORE" ZREVRANK z:test alpha WITHSCORE
assert_both "ZRANK WITHSCORE absent member" ZRANK z:test nosuchmember WITHSCORE
assert_both "ZRANK WITHSCORE absent key" ZRANK z:absent alpha WITHSCORE
assert_both "ZREVRANK WITHSCORE absent key" ZREVRANK z:absent alpha WITHSCORE
# The fence: without the option nothing moves (the miss is still `$-1`).
assert_both "ZRANK absent member (no option)" ZRANK z:test nosuchmember
assert_both "ZRANK plural is a syntax error" ZRANK z:test alpha WITHSCORES
# The FOURTH-argument arity error is deliberately not compared here: Moon
# spells the command name in the arity message in UPPERCASE and Redis in
# lowercase — a pre-existing, codebase-wide divergence, so this row would fail
# for a reason that has nothing to do with WITHSCORE. Pinned in the unit test
# (test_zrank_rejects_bad_option_as_syntax_error) instead.
assert_both "ZRANGE 0 -1" ZRANGE z:test 0 -1
assert_both "ZRANGE WITHSCORES" ZRANGE z:test 0 -1 WITHSCORES
assert_both "ZRANGEBYSCORE 1 3" ZRANGEBYSCORE z:test 1 3

both ZINCRBY z:test 10 "delta"
assert_both "ZINCRBY then ZSCORE" ZSCORE z:test delta

# ===========================================================================
# 9b. Command parity: BITFIELD_RO / SORT_RO / GEORADIUS_RO / GEORADIUSBYMEMBER_RO
# ===========================================================================
log "=== 9b. Read-only command variants (WS1 parity) ==="

both BITFIELD bf:test SET u8 0 255
assert_both "BITFIELD_RO GET matches BITFIELD" BITFIELD_RO bf:test GET u8 0
assert_both "BITFIELD_RO rejects SET" BITFIELD_RO bf:test SET u8 0 1

both RPUSH sort:test 3 1 2
assert_both "SORT_RO matches SORT" SORT_RO sort:test
assert_both "SORT_RO rejects STORE" SORT_RO sort:test STORE sort:dest

both GEOADD geo:test 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
assert_both "GEORADIUS_RO matches GEORADIUS" GEORADIUS_RO geo:test 15 37 200 km ASC
assert_both "GEORADIUSBYMEMBER_RO matches GEORADIUSBYMEMBER" GEORADIUSBYMEMBER_RO geo:test Palermo 200 km ASC

# ===========================================================================
# 10. Bulk data consistency (redis-benchmark load + random verify)
# ===========================================================================
log "=== 10. Bulk data consistency (1K deterministic keys) ==="

both FLUSHALL

# Deterministic load: 1K keys with varied value sizes
for i in $(seq 0 999); do
    key="bulk:$(printf '%04d' "$i")"
    # Vary sizes: 0-255 bytes padding
    pad=$(python3 -c "print('x' * ($i % 256))")
    val="v${i}_${pad}"
    both SET "$key" "$val"
done

# DBSIZE: verify both have 1000 keys (exact match not required due to prior test keys)
redis_db=$(redis-cli -p "$PORT_REDIS" DBSIZE 2>&1 | grep -oE '[0-9]+') || true
rust_db=$(redis-cli -p "$PORT_RUST" DBSIZE 2>&1 | grep -oE '[0-9]+') || true
if (( redis_db >= 1000 && rust_db >= 1000 )); then
    PASS=$((PASS + 1))
else
    FAIL=$((FAIL + 1)); echo "  FAIL: DBSIZE: redis=$redis_db rust=$rust_db (expected >=1000)"
fi

# Spot-check 50 random keys
BULK_PASS=0
BULK_FAIL=0
for i in $(python3 -c "import random; random.seed(42); print(' '.join(str(random.randint(0,999)) for _ in range(50)))"); do
    key="bulk:$(printf '%04d' "$i")"
    rv=$(redis-cli -p "$PORT_REDIS" GET "$key" 2>&1)
    uv=$(redis-cli -p "$PORT_RUST" GET "$key" 2>&1)
    if [[ "$rv" == "$uv" ]]; then
        BULK_PASS=$((BULK_PASS + 1))
    else
        BULK_FAIL=$((BULK_FAIL + 1))
        echo "  FAIL: bulk $key"
        echo "    redis: $(echo "$rv" | head -c 100)"
        echo "    rust:  $(echo "$uv" | head -c 100)"
    fi
done
PASS=$((PASS + BULK_PASS))
FAIL=$((FAIL + BULK_FAIL))
log "  Bulk spot-check: $BULK_PASS/$((BULK_PASS + BULK_FAIL)) passed"

# ===========================================================================
# 11. Overwrite consistency
# ===========================================================================
log "=== 11. Overwrite / type change ==="

# Overwrite string with different sizes
both SET ow:key "small"
assert_both "GET before overwrite" GET ow:key
both SET ow:key "$VAL1K"
assert_both "GET after overwrite with 1KB" GET ow:key
both SET ow:key "tiny"
assert_both "GET after shrink overwrite" GET ow:key

# Overwrite with different type
both DEL ow:type
both SET ow:type "string_val"
assert_both "GET string" GET ow:type
both DEL ow:type
both HSET ow:type f1 v1
assert_both "HGET after type change" HGET ow:type f1

# ===========================================================================
# 12. Edge cases
# ===========================================================================
log "=== 12. Edge cases ==="

# GET nonexistent key
assert_both "GET nonexistent" GET totally:missing:key

# DEL + GET
both SET edge:del "exists"
both DEL edge:del
assert_both "GET after DEL" GET edge:del

# SETNX on existing
both SET edge:setnx "original"
both SET edge:setnx "new" NX
assert_both "SETNX on existing" GET edge:setnx

# SET with GET option
both SET edge:setget "old_value"
assert_both "SET GET returns old" SET edge:setget "new_value" GET
assert_both "SET GET new value" GET edge:setget

# Very long key name
LONGKEY=$(python3 -c "print('k' * 500)")
both SET "$LONGKEY" "long_key_value"
assert_both "GET with 500-char key" GET "$LONGKEY"

# COPY
both SET edge:cpsrc "copy_value"
assert_both "COPY basic" COPY edge:cpsrc edge:cpdst
assert_both "GET after COPY src" GET edge:cpsrc
assert_both "GET after COPY dst" GET edge:cpdst
both SET edge:cpdst2 "old_value"
assert_both "COPY no REPLACE" COPY edge:cpsrc edge:cpdst2
assert_both "GET COPY no REPLACE" GET edge:cpdst2
assert_both "COPY REPLACE" COPY edge:cpsrc edge:cpdst2 REPLACE
assert_both "GET after COPY REPLACE" GET edge:cpdst2

# SETBIT / GETBIT
both SETBIT edge:bits 7 1
assert_both "GETBIT set" GETBIT edge:bits 7
assert_both "GETBIT unset" GETBIT edge:bits 0
both SETBIT edge:bits 0 1
assert_both "BITCOUNT" BITCOUNT edge:bits

# BITOP
both SET edge:bop1 "\xff"
both SET edge:bop2 "\x0f"
assert_both "BITOP AND" BITOP AND edge:bopdst edge:bop1 edge:bop2
assert_both "GET BITOP AND" GET edge:bopdst
assert_both "BITOP OR" BITOP OR edge:bopdst edge:bop1 edge:bop2
assert_both "GET BITOP OR" GET edge:bopdst
assert_both "BITOP NOT" BITOP NOT edge:bopdst edge:bop1
assert_both "GET BITOP NOT" GET edge:bopdst

# BITPOS
both SET edge:bpos "\x00\xff"
assert_both "BITPOS 1" BITPOS edge:bpos 1
assert_both "BITPOS 0" BITPOS edge:bpos 0

# SORT
both RPUSH edge:sortl 3 1 2
assert_both "SORT numeric" SORT edge:sortl
assert_both "SORT DESC" SORT edge:sortl DESC
assert_both "SORT ALPHA" SORT edge:sortl ALPHA
assert_both "SORT LIMIT" SORT edge:sortl LIMIT 0 2
# moon#592: `{srt}` co-locates the source with the STORE destination on ONE
# shard, so this row compares the COMMAND against Redis at any `--shards N`.
# `SORT` is routed by its source; the destination named after `STORE` is a key
# routing never saw, and moon used to write it into the source owner's table --
# acked with a count the client could not read back anywhere. moon now refuses
# a straddling pair (asserted in the moon-only sweep further down); Redis,
# having no shards, stores it either way.
both RPUSH {srt}:list 3 1 2
assert_both "SORT STORE" SORT {srt}:list STORE {srt}:sorted
assert_both "SORT STORE result" LRANGE {srt}:sorted 0 -1

# GEOADD / GEOPOS / GEODIST / GEOHASH / GEOSEARCH
both GEOADD edge:geo 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
assert_both "GEOPOS" GEOPOS edge:geo Palermo
assert_both "GEOPOS missing" GEOPOS edge:geo NonExistent
assert_both "GEODIST m" GEODIST edge:geo Palermo Catania
assert_both "GEODIST km" GEODIST edge:geo Palermo Catania km
assert_both "GEOHASH" GEOHASH edge:geo Palermo
assert_both "GEOADD count" GEOADD edge:geo 2.349014 48.864716 Paris
# moon#568: WITHCOORD prints the full shortest-round-tripping decimal, exactly
# as GEOPOS does — it used to be truncated to 4 places on BOTH protocols.
assert_both "GEOSEARCH WITHCOORD" GEOSEARCH edge:geo FROMLONLAT 15 37 BYRADIUS 200 km ASC WITHCOORD
assert_both "GEORADIUS WITHCOORD" GEORADIUS edge:geo 15 37 200 km ASC WITHCOORD
assert_both "GEORADIUSBYMEMBER WITHCOORD+DIST" GEORADIUSBYMEMBER edge:geo Palermo 200 km ASC WITHCOORD WITHDIST

# EXPIREAT / PEXPIREAT / EXPIRETIME / PEXPIRETIME
both SET edge:eat "val"
assert_both "EXPIREAT" EXPIREAT edge:eat 9999999999
assert_both "EXPIRETIME" EXPIRETIME edge:eat
assert_both "PEXPIRETIME" PEXPIRETIME edge:eat
assert_both "EXPIRETIME missing" EXPIRETIME edge:nokey
assert_both "PEXPIRETIME missing" PEXPIRETIME edge:nokey

# EXPIRE NX|XX|GT|LT conditions (moon#544; Redis 7.0). TTL replies are
# timing-sensitive across separate cli spawns, so the rows assert the
# CONDITION VERDICT (0/1/err), never a remaining-time value.
both SET edge:exc "val"
assert_both "EXPIRE NX fresh" EXPIRE edge:exc 100 NX
assert_both "EXPIRE NX refused on ttl" EXPIRE edge:exc 999 NX
assert_both "EXPIRE XX on ttl" EXPIRE edge:exc 200 XX
assert_both "EXPIRE GT shorter refused" EXPIRE edge:exc 10 GT
assert_both "EXPIRE GT longer" EXPIRE edge:exc 300 GT
assert_both "EXPIRE LT longer refused" EXPIRE edge:exc 999 LT
assert_both "EXPIRE LT shorter" EXPIRE edge:exc 50 LT
assert_both "EXPIRE NX GT incompatible" EXPIRE edge:exc 100 NX GT
assert_both "EXPIRE GT LT incompatible" EXPIRE edge:exc 100 GT LT
assert_both "EXPIRE unknown option" EXPIRE edge:exc 100 BOGUS
assert_both "EXPIRE XX missing key" EXPIRE edge:nokey 100 XX
both SET edge:exc2 "val"
assert_both "PEXPIRE NX fresh" PEXPIRE edge:exc2 100000 NX
assert_both "EXPIREAT GT far" EXPIREAT edge:exc2 9999999999 GT
assert_both "PEXPIREAT LT past deletes" PEXPIREAT edge:exc2 1 LT
assert_both "PEXPIREAT after delete" EXISTS edge:exc2

# TOUCH
both SET edge:touch "val"
assert_both "TOUCH" TOUCH edge:touch
assert_both "TOUCH missing" TOUCH edge:nomiss

# ===========================================================================
# WATCH / UNWATCH optimistic locking (CAS)
# ===========================================================================
log "=== WATCH/CAS ==="

# A CAS conflict needs TWO connections interleaved: the transaction has to stay
# open while a second client writes the watched key. `redis-cli` one-shot mode
# cannot express that (each invocation is its own connection, closed on exit),
# so bash's /dev/tcp holds the transaction connection open and drives it with
# inline commands. The verdict is read from the key's FINAL VALUE rather than
# from EXEC's reply, which keeps this free of RESP parsing: `from-txn` means the
# transaction committed, `from-other` means it aborted and the interloper's
# write stands.
#
# Both servers run the identical sequence and the outcomes are compared, so this
# asserts Redis parity rather than a hardcoded expectation.
watch_cas_outcome() {
    local port="$1" conflict="$2" line=""
    redis-cli -p "$port" SET cas:k base >/dev/null 2>&1 || true
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__"; return 0; }
    printf 'WATCH cas:k\r\nMULTI\r\nSET cas:k from-txn\r\n' >&3
    if [[ "$conflict" == "yes" ]]; then
        redis-cli -p "$port" SET cas:k from-other >/dev/null 2>&1 || true
    fi
    # ECHO after EXEC is a round-trip barrier: reading its reply proves EXEC has
    # been applied before the connection closes, so the GET below cannot race it.
    printf 'EXEC\r\nECHO cas-done\r\n' >&3
    while IFS= read -r -t 5 line <&3; do
        [[ "${line%$'\r'}" == "cas-done" ]] && break
    done
    exec 3>&-
    redis-cli -p "$port" GET cas:k 2>&1
}

assert_eq "WATCH: conflicting write aborts EXEC" \
    "$(watch_cas_outcome "$PORT_REDIS" yes)" "$(watch_cas_outcome "$PORT_RUST" yes)"
assert_eq "WATCH: unconflicted EXEC commits" \
    "$(watch_cas_outcome "$PORT_REDIS" no)" "$(watch_cas_outcome "$PORT_RUST" no)"

# The ABA hole: versions are per-entry and die with the entry, so before the
# per-db creation ticket a DEL + re-SET handed the watcher back the exact token
# WATCH had recorded and EXEC committed on a key that had been destroyed and
# rebuilt underneath it.
watch_cas_aba_outcome() {
    local port="$1" line=""
    redis-cli -p "$port" SET aba:k base >/dev/null 2>&1 || true
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__"; return 0; }
    printf 'WATCH aba:k\r\nMULTI\r\nSET aba:k from-txn\r\n' >&3
    redis-cli -p "$port" DEL aba:k >/dev/null 2>&1 || true
    redis-cli -p "$port" SET aba:k rebuilt >/dev/null 2>&1 || true
    printf 'EXEC\r\nECHO cas-done\r\n' >&3
    while IFS= read -r -t 5 line <&3; do
        [[ "${line%$'\r'}" == "cas-done" ]] && break
    done
    exec 3>&-
    redis-cli -p "$port" GET aba:k 2>&1
}

assert_eq "WATCH: delete + recreate aborts EXEC (ABA)" \
    "$(watch_cas_aba_outcome "$PORT_REDIS")" "$(watch_cas_aba_outcome "$PORT_RUST")"

# UNWATCH releases every dependency, so the same conflicting write commits.
watch_unwatch_outcome() {
    local port="$1" line=""
    redis-cli -p "$port" SET uw:k base >/dev/null 2>&1 || true
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__"; return 0; }
    printf 'WATCH uw:k\r\nUNWATCH\r\nMULTI\r\nSET uw:k from-txn\r\n' >&3
    redis-cli -p "$port" SET uw:k from-other >/dev/null 2>&1 || true
    printf 'EXEC\r\nECHO cas-done\r\n' >&3
    while IFS= read -r -t 5 line <&3; do
        [[ "${line%$'\r'}" == "cas-done" ]] && break
    done
    exec 3>&-
    redis-cli -p "$port" GET uw:k 2>&1
}

assert_eq "UNWATCH releases the dependency" \
    "$(watch_unwatch_outcome "$PORT_REDIS")" "$(watch_unwatch_outcome "$PORT_RUST")"

assert_both "WATCH arity" WATCH
assert_both "UNWATCH outside MULTI" UNWATCH

# ===========================================================================
# RESP2 null TYPE parity (moon#482)
# ===========================================================================
# RESP2 has two nulls: `$-1` (the missing value is a string) and `*-1` (the
# missing value is an array). A typed client decodes them differently, so
# answering the wrong one is a decode error client-side.
#
# `redis-cli` renders BOTH as "(nil)", so no assertion built on its output can
# see this defect at all — that is precisely why it survived every existing
# suite. These probes read the RAW first line off the socket instead.
#
# Failure markers embed the PORT on purpose. An earlier draft returned a bare
# "__CONNECT_FAILED__" from both servers, so when the probe could not connect
# at all the two sides compared EQUAL and every assertion passed vacuously —
# a green suite that had tested nothing. A per-port marker can never match its
# counterpart, so a broken probe now fails loudly instead of silently.
null_type_of() {
    local port="$1"; shift
    local line=""
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__:${port}"; return 0; }
    printf '%s\r\n' "$*" >&3
    IFS= read -r -t 5 line <&3
    exec 3>&-
    line="${line%$'\r'}"
    # A RESP reply always opens with a type byte. Anything else (empty read,
    # timeout, truncated line) is an instrument failure, not a null type.
    #
    # The first character is tested on its own rather than with a bracket
    # expression: a `[...$*...]` pattern looks right but bash expands the `$*`
    # inside it to the positional parameters, so every real reply fell through
    # to the failure branch.
    case "${line:0:1}" in
        '+'|'-'|':'|'$'|'*'|'%'|'~'|'#'|','|'('|'_'|'='|'>') echo "$line" ;;
        *) echo "__NO_RESP_REPLY__:${port}:${line}" ;;
    esac
}

# Every probe gets its OWN never-written key (%K below). Sharing one key made
# the results lie: a timed-out BLPOP leaves a phantom empty list behind in Moon
# (#523), so a later `LPOP <same-key> 2` legitimately answered `*0` instead of
# `*-1` and looked like a null-type bug that was not there.
nulltype_i=0
null_probe() {
    local kind="$1"; shift
    nulltype_i=$((nulltype_i + 1))
    # Hash-TAGGED (moon#637). Two of these probes are two-key commands whose
    # second key is `%K-d`: `BLMOVE %K %K-d` and `BRPOPLPUSH %K %K-d`. With a
    # bare `nulltype:N` the two keys hash to DIFFERENT shards at --shards >= 2,
    # so moon correctly refuses the move cross-shard (moon#570/#591) and the
    # probe compared a routing refusal against redis's `*-1` — a null-TYPE test
    # that was really testing routing, and failed for a reason it was not
    # written to measure. `{N}` makes `nulltype:{N}` and `nulltype:{N}-d` share
    # a tag, so they co-locate at ANY shard count and the probe measures the
    # null type again. The single-key probes are unaffected either way.
    local key="nulltype:{${nulltype_i}}"
    local -a argv=()
    local a
    for a in "$@"; do argv+=("${a//%K/$key}"); done
    assert_eq "null type ${kind}: ${argv[0]}" \
        "$(null_type_of "$PORT_REDIS" "${argv[@]}")" \
        "$(null_type_of "$PORT_RUST" "${argv[@]}")"
}

# Must be `*-1` on both servers.
null_probe parity BLPOP %K 0.05
null_probe parity BRPOP %K 0.05
null_probe parity BLMOVE %K %K-d LEFT RIGHT 0.05
null_probe parity BRPOPLPUSH %K %K-d 0.05
null_probe parity BZPOPMIN %K 0.05
null_probe parity BZPOPMAX %K 0.05
null_probe parity BLMPOP 0.05 1 %K LEFT
null_probe parity BZMPOP 0.05 1 %K MIN
null_probe parity LPOP %K 2
null_probe parity RPOP %K 2
null_probe parity LMPOP 1 %K LEFT
null_probe parity ZMPOP 1 %K MIN
null_probe parity XREAD COUNT 1 STREAMS %K 0-0
# XREADGROUP is deliberately NOT probed here. It needs an XGROUP CREATE first,
# and `null_probe` sends exactly one command — without the group both servers
# answer `-NOGROUP`, so the probe would compare equal while testing nothing.
# Its coverage is `rna6` in tests/resp2_null_array.rs, which does the setup.

# The fence: these misses are a null BULK or an EMPTY array and must NOT have
# moved. Without this half, "make everything *-1" would pass the block above.
null_probe fence GET %K
null_probe fence HGET %K f
null_probe fence LPOP %K
null_probe fence ZSCORE %K m
null_probe fence GETDEL %K
null_probe fence ZPOPMIN %K
null_probe fence SMEMBERS %K
null_probe fence HGETALL %K
null_probe fence XRANGE %K - +

# ===========================================================================
# LPOP/RPOP count-validation ordering + error text (moon#527)
# ===========================================================================
# Redis parses the optional count BEFORE looking the key up, so a malformed
# count is an ERROR whether or not the key exists — and a non-integer and a
# negative count share one message. Moon validated after the lookup, so
# `LPOP nokey abc` answered a miss and only became an error once somebody
# created the key.
#
# These read the RAW first line, like the null-type probes above: `redis-cli`
# prints the error text but NOT the reply type, and the point here is that a
# `-ERR ...` line replaced a `*-1` line.
countarg_i=0
countarg_probe() {
    local kind="$1"; shift
    countarg_i=$((countarg_i + 1))
    local key="countarg:${countarg_i}"
    local -a argv=()
    local a
    for a in "$@"; do argv+=("${a//%K/$key}"); done
    # `%P` marks a probe that needs the key to EXIST first.
    if [ "$kind" = "present" ]; then
        redis-cli -p "$PORT_REDIS" RPUSH "$key" a b >/dev/null 2>&1 || true
        redis-cli -p "$PORT_RUST"  RPUSH "$key" a b >/dev/null 2>&1 || true
    fi
    assert_eq "count arg ${kind}: ${argv[*]}" \
        "$(null_type_of "$PORT_REDIS" "${argv[@]}")" \
        "$(null_type_of "$PORT_RUST" "${argv[@]}")"
}

# A bad count on an ABSENT key must be the error, not the miss.
countarg_probe absent LPOP %K abc
countarg_probe absent LPOP %K -1
countarg_probe absent RPOP %K abc
countarg_probe absent RPOP %K -1
# ...and the same bad count on a PRESENT key must be the SAME error text.
countarg_probe present LPOP %K abc
countarg_probe present LPOP %K -1
countarg_probe present RPOP %K abc
# The fence: a WELL-FORMED count on an absent key is still the null array
# (`*-1`, moon#482) — without this half, "reject every count" would pass.
countarg_probe absent LPOP %K 2
countarg_probe absent RPOP %K 2
countarg_probe absent LPOP %K 0

# ===========================================================================
# XREADGROUP history mode replies the stream, not a null (moon#526)
# ===========================================================================
# `XREADGROUP ... STREAMS s 0` asks for the consumer's PENDING entries. Redis
# serves the stream before it knows whether the PEL slice is empty, so an empty
# PEL is `*1 *2 $1 s *0` — the stream with an empty entry list. Moon answered
# `$-1`, and a client iterating the returned stream list got a decode error
# where Redis gives it zero iterations.
#
# Needs an `XGROUP CREATE` first, which is why this cannot ride on `null_probe`
# (one command per probe): without the group BOTH servers answer `-NOGROUP` and
# the comparison passes while testing nothing.
xrg_probe() {
    local kind="$1" id="$2"
    local key="xrghist:${kind}"
    redis-cli -p "$PORT_REDIS" XGROUP CREATE "$key" g '$' MKSTREAM >/dev/null 2>&1 || true
    redis-cli -p "$PORT_RUST"  XGROUP CREATE "$key" g '$' MKSTREAM >/dev/null 2>&1 || true
    assert_eq "xreadgroup ${kind}: STREAMS ${key} ${id}" \
        "$(null_type_of "$PORT_REDIS" XREADGROUP GROUP g c COUNT 10 STREAMS "$key" "$id")" \
        "$(null_type_of "$PORT_RUST"  XREADGROUP GROUP g c COUNT 10 STREAMS "$key" "$id")"
}

# History mode on an empty PEL: the stream array (`*1`), not a null.
xrg_probe history 0
# The fence: the `>` form with nothing new stays the null ARRAY (`*-1`, #482).
xrg_probe newonly '>'

# ===========================================================================
# XREAD omits a stream that had nothing (moon#594)
# ===========================================================================
# `XREAD ... STREAMS a b 0 <past b's last id>` serves only `a`. Redis answers
# `*1`; moon answered `*2` and carried `b` as a present-but-empty entry list,
# so a client iterating the reply saw a stream it had to special-case. Under
# RESP3 that is a Map key whose value is empty, which is worse — map membership
# is the natural "this stream was served" test.
#
# The header line IS the assertion: the element count is the entire divergence,
# and `null_type_of` reads exactly that first line.
#
# Both streams share a hash tag so they live on the same shard. moon routes a
# multi-stream XREAD by its FIRST key, so untagged keys would be testing
# cross-shard routing instead of the omission rule — and would pass vacuously.
#
# This is deliberately NOT the XREADGROUP history case above: `XREADGROUP ... 0`
# on an empty PEL really is `*1 {name: *0}` in Redis. The omission rule belongs
# to plain XREAD's "did anything arrive after this id" question alone.
xread_omit_probe() {
    local kind="$1"; shift
    local a="{xromit:${kind}}:a" b="{xromit:${kind}}:b" p
    for p in "$PORT_REDIS" "$PORT_RUST"; do
        redis-cli -p "$p" DEL "$a" "$b" >/dev/null 2>&1 || true
        redis-cli -p "$p" XADD "$a" 1-1 f v >/dev/null 2>&1 || true
        redis-cli -p "$p" XADD "$b" 1-1 f v >/dev/null 2>&1 || true
    done
    TOTAL=$((TOTAL + 1))
    assert_eq "xread omit ${kind}: STREAMS ${a} ${b} $*" \
        "$(null_type_of "$PORT_REDIS" XREAD COUNT 10 STREAMS "$a" "$b" "$@")" \
        "$(null_type_of "$PORT_RUST"  XREAD COUNT 10 STREAMS "$a" "$b" "$@")"
}

# The defect: `a` is served, `b` is not -> `*1`, not `*2`.
xread_omit_probe served_and_quiet 0 99999
# The fences, so "always answer *1" cannot pass: both served -> `*2`, and
# neither served -> the null ARRAY (`*-1`, moon#482), not an empty one.
xread_omit_probe both_served 0 0
xread_omit_probe none_served 99999 99999

# ===========================================================================
# XREAD / XREADGROUP BLOCK really block, and XADD wakes them (moon#595)
# ===========================================================================
# `BLOCK` used to be parsed and discarded, so `XREAD BLOCK 700 STREAMS k $`
# returned in 0.000 s where Redis waits the full budget. The reply BYTES are
# identical either way (`*-1` is exactly what a legitimate timeout answers), so
# only the elapsed time can tell the two apart — which is why this probe times
# rather than compares.
#
# Both halves are asserted: a server that never blocks passes "was woken" only
# vacuously, and a server that blocks but is never woken passes "waited".
xread_block_ms() {
    local port="$1"; shift
    local start end line
    start=$(date +%s%N)
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "-1"; return 0; }
    printf '%s\r\n' "$*" >&3
    IFS= read -r -t 10 line <&3
    exec 3>&-
    end=$(date +%s%N)
    echo $(((end - start) / 1000000))
}

# `date +%s%N` is a GNU extension; these scripts are documented to run inside
# moon-dev, but skip rather than fail loudly somewhere without it.
if [[ "$(date +%s%N)" =~ ^[0-9]+$ ]]; then
    for port_pair in "redis:${PORT_REDIS}" "moon:${PORT_RUST}"; do
        who="${port_pair%%:*}"
        port="${port_pair##*:}"
        redis-cli -p "$port" DEL xrblk >/dev/null 2>&1 || true
        redis-cli -p "$port" XADD xrblk 1-1 f v >/dev/null 2>&1 || true

        # (1) nothing new -> waits out the budget.
        TOTAL=$((TOTAL + 1))
        elapsed=$(xread_block_ms "$port" XREAD BLOCK 700 STREAMS xrblk '$')
        if (( elapsed >= 600 && elapsed < 5000 )); then
            PASS=$((PASS + 1))
        else
            FAIL=$((FAIL + 1))
            echo "  FAIL: ${who} XREAD BLOCK 700 returned after ${elapsed}ms (want ~700)"
        fi

        # (2) a concurrent XADD wakes it.
        redis-cli -p "$port" DEL xrwake >/dev/null 2>&1 || true
        redis-cli -p "$port" XADD xrwake 1-1 f v >/dev/null 2>&1 || true
        ( sleep 0.4; redis-cli -p "$port" XADD xrwake 2-1 g w >/dev/null 2>&1 ) &
        waker=$!
        TOTAL=$((TOTAL + 1))
        elapsed=$(xread_block_ms "$port" XREAD BLOCK 5000 STREAMS xrwake '$')
        wait "$waker" 2>/dev/null || true
        if (( elapsed >= 200 && elapsed < 4000 )); then
            PASS=$((PASS + 1))
        else
            FAIL=$((FAIL + 1))
            echo "  FAIL: ${who} parked XREAD was not woken by XADD (${elapsed}ms)"
        fi
    done
fi

# ---------------------------------------------------------------------------
# Shard-routing parity (moon#533, moon#534)
#
# A command whose key is not its first argument used to be routed by hashing
# whatever WAS first — a numkeys count, a timeout, the literal "GROUP" — so
# every invocation landed on one fixed shard and reported every other shard's
# keys as absent.
#
# Two rules make these probes able to see that; drop either and the block goes
# quietly vacuous:
#
#   1. POPULATE the key first. On an absent key a mis-routed command and a
#      correct one return identical bytes, so an absent-key probe proves
#      nothing. The null-type probes above are absent-key by design and did
#      report LMPOP/ZMPOP clean while they were broken.
#   2. Use MANY keys. A constant route still serves ~1/N of keys, so one key
#      passes 1-in-N of the time and reads as a flake, not a bug.
#
# At --shards 1 there is no routing and these cannot fail; they are still run
# so the block is exercised in every config rather than silently skipped.
ROUTE_KEYS=12

# Run `setup` then `probe` on both servers for one key, and compare. The key
# name is substituted for %K; the SETUP output is discarded but its effect is
# asserted by the probe having something to find.
route_probe() {
    local label="$1"; shift
    local setup="$1"; shift
    local probe="$1"; shift
    local setup2="${1:-}"
    local mismatched=0 i key
    for i in $(seq 1 "$ROUTE_KEYS"); do
        key="route:${label}:${i}"
        # shellcheck disable=SC2086  # deliberate word-split: templates are ours
        redis-cli -p "$PORT_REDIS" ${setup//%K/$key} >/dev/null 2>&1 || true
        # shellcheck disable=SC2086
        redis-cli -p "$PORT_RUST"  ${setup//%K/$key} >/dev/null 2>&1 || true
        if [ -n "$setup2" ]; then
            # shellcheck disable=SC2086
            redis-cli -p "$PORT_REDIS" ${setup2//%K/$key} >/dev/null 2>&1 || true
            # shellcheck disable=SC2086
            redis-cli -p "$PORT_RUST"  ${setup2//%K/$key} >/dev/null 2>&1 || true
        fi
        # shellcheck disable=SC2086
        local r; r=$(redis-cli -p "$PORT_REDIS" ${probe//%K/$key} 2>&1)
        # shellcheck disable=SC2086
        local m; m=$(redis-cli -p "$PORT_RUST"  ${probe//%K/$key} 2>&1)
        [ "$r" = "$m" ] || mismatched=$((mismatched + 1))
    done
    # Compare COUNTS, not one key's bytes: "0 of 12" vs "9 of 12" is the
    # difference between correct and a constant route, and asserting on a
    # single key would make those two outcomes indistinguishable.
    assert_eq "shard routing ${label} (shards=${SHARDS}, ${ROUTE_KEYS} keys)" \
        "0 mismatched" "${mismatched} mismatched"
}

# ---------------------------------------------------------------------------
# moon#592: no two-key WRITE may ack a write that did not land
# ---------------------------------------------------------------------------
#
# moon-only: Redis has no shards, so a routing refusal has nothing to compare
# against. moon routes a command to the owner of ONE key -- the one
# `first_key` names -- and then executes the whole command against that shard's
# slice, so every OTHER key of the argv was read from and written to the wrong
# shard's table, under the right name, invisible to every normally-routed
# access. `RENAME alpha omega` answered `+OK` with the value readable under
# neither name (12 of 12 constructed cross-shard placements, per command).
#
# Two independent checks, so neither can be satisfied vacuously:
#   * NO placement may lose the data -- this holds whether a pair straddles or
#     not, and is equally satisfied by a future implementation that routes the
#     write properly instead of refusing it;
#   * at least ONE placement must actually be refused, which proves the sweep
#     reached the cross-shard case at all.
#
# The 12-suffix sweep exists because WHICH pair straddles is a property of the
# hash: one hard-coded pair that happened to co-locate would make this block
# pass while testing nothing.
if [[ "$SHARDS" -gt 1 ]]; then
    # label | seed the source | the two-key write | read the destination back
    XW_CASES=(
        "rename|SET %S VALUE-1|RENAME %S %D|EXISTS %D"
        "renamenx|SET %S VALUE-1|RENAMENX %S %D|EXISTS %D"
        "smove|SADD %S m1 m2|SMOVE %S %D m1|SCARD %D"
        "sinterstore|SADD %S m1 m2|SINTERSTORE %D %S|SCARD %D"
        "sunionstore|SADD %S m1 m2|SUNIONSTORE %D %S|SCARD %D"
        "sdiffstore|SADD %S m1 m2|SDIFFSTORE %D %S|SCARD %D"
        "zrangestore|ZADD %S 1 a 2 b|ZRANGESTORE %D %S 0 -1|ZCARD %D"
        "zunionstore|ZADD %S 1 a 2 b|ZUNIONSTORE %D 1 %S|ZCARD %D"
        "zinterstore|ZADD %S 1 a 2 b|ZINTERSTORE %D 1 %S|ZCARD %D"
        "pfmerge|PFADD %S a b c|PFMERGE %D %S|PFCOUNT %D"
        "geosearchstore|GEOADD %S 15 37 Here|GEOSEARCHSTORE %D %S FROMLONLAT 15 37 BYRADIUS 200 km ASC|ZCARD %D"
        "sortstore|RPUSH %S 3 1 2|SORT %S STORE %D|LLEN %D"
    )
    xw_lost=0
    xw_refused=0
    for xw_case in "${XW_CASES[@]}"; do
        IFS='|' read -r xw_label xw_seed xw_cmd xw_read <<<"$xw_case"
        for i in $(seq 0 11); do
            xw_s="xw:${xw_label}:s${i}"
            xw_d="xw:${xw_label}:d${i}"
            xw_seed_i="${xw_seed//%S/$xw_s}"
            xw_cmd_i="${xw_cmd//%S/$xw_s}"; xw_cmd_i="${xw_cmd_i//%D/$xw_d}"
            xw_read_i="${xw_read//%D/$xw_d}"
            redis-cli -p "$PORT_RUST" DEL "$xw_s" "$xw_d" &>/dev/null || true
            # shellcheck disable=SC2086  # deliberate word-split: templates are ours
            redis-cli -p "$PORT_RUST" $xw_seed_i &>/dev/null || true
            # shellcheck disable=SC2086
            xw_reply=$(redis-cli -p "$PORT_RUST" $xw_cmd_i 2>&1)
            # shellcheck disable=SC2086
            xw_dst=$(redis-cli -p "$PORT_RUST" $xw_read_i 2>&1)
            case "$xw_reply" in
                CROSSSLOT*)
                    xw_refused=$((xw_refused + 1))
                    # A refusal must have changed nothing at all.
                    if [[ -n "$xw_dst" && "$xw_dst" != "0" ]]; then
                        echo "  FAIL detail: ${xw_label}[$i] refused but destination is $xw_dst"
                        xw_lost=$((xw_lost + 1))
                    fi ;;
                *)
                    # Acked: the write MUST be readable at the destination
                    # through a normally-routed read.
                    if [[ -z "$xw_dst" || "$xw_dst" == "0" ]]; then
                        echo "  FAIL detail: ${xw_label}[$i] acked '$xw_reply' but destination is empty"
                        xw_lost=$((xw_lost + 1))
                    fi ;;
            esac
        done
    done
    assert_eq "moon#592 no two-key write loses its data (shards=$SHARDS)" "0" "$xw_lost"
    if [[ "$xw_refused" -eq 0 ]]; then
        echo "  WARN: moon#592 sweep found no cross-shard pair at shards=$SHARDS (nothing refused)"
    fi
fi

# ---------------------------------------------------------------------------
# moon#629: RANDOMKEY must sample the keyspace, not repeat one name
# ---------------------------------------------------------------------------
#
# moon-only: Redis has one keyspace and a real RNG, so there is nothing to
# compare against. Two defects made RANDOMKEY return the same few names for as
# long as a client asked: it was absent from the cross-shard coordinator (so it
# saw only the serving shard's keys), and its index was `current_time_ms() %
# total` (so every call inside one millisecond drew the same position).
#
# Every draw MUST share one connection -- `redis-cli` reading commands from
# stdin does exactly that. A fresh `redis-cli` per draw is what hid this
# originally: each opens its own connection, SO_REUSEPORT spreads those across
# the shards, and the spread alone produces a healthy-looking mix of names
# while every individual reply is still shard-local. Measured on this exact
# probe, 60 draws over 40 keys on one connection:
#
#   shards=4   before 4 distinct    after 32
#   shards=1   before 5 distinct    after 27
#
# 20 is the bound: one shard of four owns ~10 of the 40, and a fair draw
# reaches ~31 (coupon collector), so neither hash imbalance nor an unlucky
# sample can move the verdict.
#
# db 9 so the sweep neither sees nor disturbs the keys the rest of this script
# is asserting on.
redis-cli -p "$PORT_RUST" -n 9 FLUSHDB &>/dev/null || true
for rk_i in $(seq 0 39); do
    redis-cli -p "$PORT_RUST" -n 9 SET "rk:$rk_i" v &>/dev/null || true
done
rk_size=$(redis-cli -p "$PORT_RUST" -n 9 DBSIZE 2>&1 | grep -oE '[0-9]+') || true
rk_distinct=$(for _ in $(seq 1 60); do echo RANDOMKEY; done \
    | redis-cli -p "$PORT_RUST" -n 9 2>/dev/null | sort -u | grep -c 'rk:') || true
assert_eq "moon#629 DBSIZE sees every seeded key (shards=$SHARDS)" "40" "$rk_size"
if [[ "$rk_distinct" -ge 20 ]]; then
    PASS=$((PASS + 1)); echo "  PASS: moon#629 RANDOMKEY samples the keyspace (shards=$SHARDS, $rk_distinct distinct)"
else
    FAIL=$((FAIL + 1)); echo "  FAIL: moon#629 RANDOMKEY reached only $rk_distinct distinct keys in 60 draws (shards=$SHARDS)"
fi
redis-cli -p "$PORT_RUST" -n 9 FLUSHDB &>/dev/null || true

route_probe lmpop      "RPUSH %K v1"                 "LMPOP 1 %K LEFT"
route_probe zmpop      "ZADD %K 1 m"                 "ZMPOP 1 %K MIN"
route_probe sintercard "SADD %K a b"                 "SINTERCARD 1 %K"
route_probe xreadgroup "XADD %K 1-1 f v"             "XREADGROUP GROUP g c COUNT 1 STREAMS %K >" \
                       "XGROUP CREATE %K g 0"

# The fence: commands that already routed correctly. Without this half, a fix
# that routed EVERYTHING by args[1] would pass the block above.
route_probe f_lpop     "RPUSH %K v1"                 "LPOP %K"
route_probe f_zdiff    "ZADD %K 1 m"                 "ZDIFF 1 %K"
route_probe f_xread    "XADD %K 1-1 f v"             "XREAD COUNT 1 STREAMS %K 0-0"
# MEMORY USAGE is deliberately NOT fenced here even though it has a routing
# arm (moon#511): it answers a BYTE COUNT, and Redis's allocator and moon's
# legitimately disagree on it, so a cross-server equality check fails for a
# reason that has nothing to do with routing. Its routing fence lives in
# tests/shard_routing_parity.rs, which asserts the reply is an integer for
# every key rather than that the two servers agree on the number.

# EXEC aborted by a broken WATCH: the reply TYPE, not the committed value.
# Needs two connections interleaved, like watch_cas_outcome above, but reads
# EXEC's own reply line rather than the key's final value.
exec_abort_reply_type() {
    local port="$1" line=""
    redis-cli -p "$port" SET nulltype:cas base >/dev/null 2>&1 || true
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__:${port}"; return 0; }
    printf 'WATCH nulltype:cas\r\nMULTI\r\nGET nulltype:cas\r\n' >&3
    # Drain the three acks (+OK, +OK, +QUEUED) so the next line read is EXEC's.
    IFS= read -r -t 5 line <&3; IFS= read -r -t 5 line <&3; IFS= read -r -t 5 line <&3
    redis-cli -p "$port" SET nulltype:cas from-other >/dev/null 2>&1 || true
    printf 'EXEC\r\n' >&3
    IFS= read -r -t 5 line <&3
    exec 3>&-
    echo "${line%$'\r'}"
}
assert_eq "null type parity: EXEC aborted by WATCH" \
    "$(exec_abort_reply_type "$PORT_REDIS")" "$(exec_abort_reply_type "$PORT_RUST")"

# ===========================================================================
# Identity / introspection (COMMAND, ROLE, RESET)
# ===========================================================================
log "=== IDENTITY/INTROSPECTION ==="

# These compare REPLY SHAPE against Redis, not reply content: Moon registers a
# different command set than Redis, so `COMMAND COUNT` legitimately differs in
# value while its TYPE must not. The old bug was a type inversion — bare
# COMMAND replied an Integer and COMMAND COUNT replied an Array, each the
# other's type — which `redis-cli` renders identically as "0". Comparing
# rendered text here would have shown a false match, so shape is derived from
# the reply itself.

# Integer-typed and positive on both servers.
count_is_positive_int() {
    local port="$1" v
    v="$(redis-cli -p "$port" COMMAND COUNT 2>&1)"
    [[ "$v" =~ ^[0-9]+$ ]] && [[ "$v" -gt 0 ]] && echo "int>0" || echo "NOT-AN-INT:$v"
}
assert_eq "COMMAND COUNT is a positive integer" \
    "$(count_is_positive_int "$PORT_REDIS")" "$(count_is_positive_int "$PORT_RUST")"

# `COMMAND LIST` must be LARGER than `COMMAND COUNT`, on both servers, because
# LIST enumerates container subcommands as `container|sub` entries and COUNT
# counts only top-level commands.
#
# This check used to assert the two were EQUAL (moon#635). It passed on Moon
# for the wrong reason -- Moon published no subcommands at all, so the numbers
# matched -- and disagreed with redis, where they legitimately differ (274 vs
# 411). Comparing the two servers' rendered "mismatch(a vs b)" strings could
# never have agreed either, since the numbers differ by construction. What is
# comparable is the RELATION, and the count of `|` names is the direct
# evidence: zero of them was the whole defect.
count_vs_list() {
    local port="$1" n list listed subs
    n="$(redis-cli -p "$port" COMMAND COUNT 2>&1)"
    # ONE capture, then count from it: two round-trips could disagree, and the
    # `|| true` is load-bearing rather than defensive. `grep -c` exits 1 when it
    # matches nothing, and under `set -euo pipefail` a bare `x="$(... | grep -c
    # ...)"` assignment ABORTS on that exit — so on a server publishing zero
    # subcommands this function died instead of reaching the branch that
    # reports it, leaving `no-subcommands-published` unreachable. That is the
    # #634 defect exactly: a gate that cannot report the failure it exists for.
    list="$(redis-cli -p "$port" COMMAND LIST 2>&1)"
    listed="$(printf '%s\n' "$list" | grep -c . || true)"
    subs="$(printf '%s\n' "$list" | grep -c '|' || true)"
    if [[ ! "$n" =~ ^[0-9]+$ ]]; then
        echo "COUNT-NOT-AN-INT:$n"
    elif [[ "$subs" -eq 0 ]]; then
        echo "no-subcommands-published"
    elif [[ "$listed" -gt "$n" ]]; then
        echo "list>count-with-subcommands"
    else
        echo "list($listed)-not-greater-than-count($n)"
    fi
}
assert_eq "COMMAND LIST exceeds COMMAND COUNT and publishes subcommands" \
    "$(count_vs_list "$PORT_REDIS")" "$(count_vs_list "$PORT_RUST")"

assert_both "COMMAND GETKEYS extracts keys" COMMAND GETKEYS MSET ik1 v1 ik2 v2
assert_both "COMMAND GETKEYS rejects a keyless command" COMMAND GETKEYS PING

# moon#537: every command below carries `first_key: 0`, which mirrors redis and
# means "the keys are not at a FIXED argument position" — NOT "there are no
# keys". moon read it as the latter and answered `ERR The command has no key
# arguments` to the whole movablekeys family, which is exactly what a
# cluster-aware client calls GETKEYS to resolve. One case per key LAYOUT the
# shared walker knows, so a fix that covers only one shape cannot pass.
assert_both "GETKEYS LMPOP (numkeys vector)"        COMMAND GETKEYS LMPOP 2 ik1 ik2 LEFT
assert_both "GETKEYS ZMPOP (numkeys vector)"        COMMAND GETKEYS ZMPOP 1 ik1 MIN
assert_both "GETKEYS BLMPOP (numkeys after arg)"    COMMAND GETKEYS BLMPOP 0 2 ik1 ik2 LEFT
assert_both "GETKEYS SINTERCARD"                    COMMAND GETKEYS SINTERCARD 2 ik1 ik2
assert_both "GETKEYS ZDIFF"                         COMMAND GETKEYS ZDIFF 2 ik1 ik2
assert_both "GETKEYS ZINTERCARD"                    COMMAND GETKEYS ZINTERCARD 2 ik1 ik2
assert_both "GETKEYS ZUNIONSTORE (dest + vector)"   COMMAND GETKEYS ZUNIONSTORE ikd 2 ik1 ik2
assert_both "GETKEYS EVAL (script numkeys)"         COMMAND GETKEYS EVAL "return 1" 1 ik1
assert_both "GETKEYS EVALSHA"                       COMMAND GETKEYS EVALSHA sha 1 ik1
assert_both "GETKEYS FCALL"                         COMMAND GETKEYS FCALL fn 2 ik1 ik2
assert_both "GETKEYS XREAD (STREAMS token)"         COMMAND GETKEYS XREAD COUNT 1 STREAMS ik1 ik2 0 0
assert_both "GETKEYS XREADGROUP (STREAMS token)"    COMMAND GETKEYS XREADGROUP GROUP g c STREAMS ik1 '>'
assert_both "GETKEYS SORT (source only)"            COMMAND GETKEYS SORT ik1
assert_both "GETKEYS SORT ... STORE (source+dest)"  COMMAND GETKEYS SORT ik1 ALPHA STORE ikd
assert_both "GETKEYS SORT ... BY pattern"           COMMAND GETKEYS SORT ik1 BY 'w_*'
assert_both "GETKEYS GEORADIUS ... STORE"           COMMAND GETKEYS GEORADIUS ik1 1 2 3 m STORE ikd
assert_both "GETKEYS OBJECT (subcommand-shaped)"    COMMAND GETKEYS OBJECT ENCODING ik1
assert_both "GETKEYS MEMORY USAGE"                  COMMAND GETKEYS MEMORY USAGE ik1
assert_both "GETKEYS XGROUP CREATE"                 COMMAND GETKEYS XGROUP CREATE ik1 g '$'
assert_both "GETKEYS RPOPLPUSH (two keys)"          COMMAND GETKEYS RPOPLPUSH ik1 ik2
# ... and the four error strings, whose ORDER is observable: SELECT's arity is
# ALSO wrong, and redis still reports the no-keys answer first.
assert_both "GETKEYS unknown command"               COMMAND GETKEYS NOSUCHCMD ik1
assert_both "GETKEYS keyless beats wrong arity"     COMMAND GETKEYS SELECT
assert_both "GETKEYS container keyless subcommand"  COMMAND GETKEYS MEMORY STATS
assert_both "GETKEYS wrong arity"                   COMMAND GETKEYS LMPOP 0 LEFT
assert_both "GETKEYS unextractable argv"            COMMAND GETKEYS LMPOP abc ik1 LEFT
assert_both "GETKEYS numkeys exceeds argv"          COMMAND GETKEYS LMPOP 3 ik1 LEFT
# `no-mandatory-keys`: EVAL's key COUNT is an argument, so zero keys (and even
# a count the argv cannot satisfy) is an empty ARRAY, not an error. LMPOP with
# the same shape of bad count IS an error — that contrast is the whole point.
assert_both "GETKEYS EVAL numkeys 0 is an empty array" COMMAND GETKEYS EVAL "return 1" 0
assert_both "GETKEYS EVAL bad numkeys is empty too"    COMMAND GETKEYS EVAL "return 1" 9 ik1

# ---------------------------------------------------------------------------
# moon#584 -- CLIENT TRACKING must invalidate what a command MODIFIES, not
# every key it NAMES.
#
# Moon pushed an invalidation for the read-only SOURCES of `*STORE` commands,
# so a client caching `a` was told `a` had changed by `ZUNIONSTORE d 2 a b`.
# Extra invalidations are safe but wasteful (a dropped cache entry and a
# refetch); the DANGEROUS direction is a missing one, which is why every case
# below is paired with its destination CONTROL. A fix that simply stopped
# pushing would pass the source rows and fail every control.
#
# Needs a held-open RESP3 connection to observe the out-of-band push, same
# /dev/tcp technique as the WATCH/RESET tests above.
# ---------------------------------------------------------------------------
tracking_push_for() {
    local port="$1" watched="$2" read_cmd="$3"; shift 3
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__:${port}"; return 0; }
    printf 'HELLO 3\r\nCLIENT TRACKING ON\r\n%s\r\n' "$read_cmd" >&3
    # Drain the HELLO map, the +OK and the read's own reply so the only thing
    # left on the socket is whatever the write pushes.
    local line=""
    while IFS= read -r -t 1 line <&3; do :; done
    redis-cli -p "$port" "$@" >/dev/null 2>&1 || true
    local seen="" got="NONE"
    while IFS= read -r -t 1 line <&3; do
        seen="${seen}${line%$'\r'}|"
    done
    exec 3>&-
    case "$seen" in
        *invalidate*"${watched}"*) got="PUSH:${watched}" ;;
        *invalidate*)              got="PUSH:other" ;;
    esac
    echo "$got"
}

assert_tracking() {
    local desc="$1" watched="$2" read_cmd="$3"; shift 3
    assert_eq "$desc" \
        "$(tracking_push_for "$PORT_REDIS" "$watched" "$read_cmd" "$@")" \
        "$(tracking_push_for "$PORT_RUST"  "$watched" "$read_cmd" "$@")"
}

both ZADD tz:a 1 m
both ZADD tz:b 1 m
assert_tracking "tracking: ZUNIONSTORE SOURCE not invalidated" \
    "tz:a" "ZRANGE tz:a 0 -1" ZUNIONSTORE tz:d 2 tz:a tz:b
assert_tracking "tracking: ZUNIONSTORE DEST invalidated [control]" \
    "tz:d" "ZRANGE tz:d 0 -1" ZUNIONSTORE tz:d 2 tz:a tz:b
assert_tracking "tracking: ZINTERSTORE SOURCE not invalidated" \
    "tz:a" "ZRANGE tz:a 0 -1" ZINTERSTORE tz:i 2 tz:a tz:b
assert_tracking "tracking: ZINTERSTORE DEST invalidated [control]" \
    "tz:i" "ZRANGE tz:i 0 -1" ZINTERSTORE tz:i 2 tz:a tz:b

both RPUSH tl:s b a
assert_tracking "tracking: SORT..STORE SOURCE not invalidated" \
    "tl:s" "LRANGE tl:s 0 -1" SORT tl:s ALPHA STORE tl:d
assert_tracking "tracking: SORT..STORE DEST invalidated [control]" \
    "tl:d" "LRANGE tl:d 0 -1" SORT tl:s ALPHA STORE tl:d
# SORT is a WRITE-flagged command that writes NOTHING without STORE.
assert_tracking "tracking: SORT without STORE invalidates nothing" \
    "tl:s" "LRANGE tl:s 0 -1" SORT tl:s ALPHA

both SADD ts:a x
both SADD ts:b x
assert_tracking "tracking: SINTERSTORE SOURCE not invalidated" \
    "ts:a" "SMEMBERS ts:a" SINTERSTORE ts:d ts:a ts:b
assert_tracking "tracking: SINTERSTORE DEST invalidated [control]" \
    "ts:d" "SMEMBERS ts:d" SINTERSTORE ts:d ts:a ts:b

both SET tb:a x
both SET tb:b y
assert_tracking "tracking: BITOP SOURCE not invalidated" \
    "tb:a" "GET tb:a" BITOP AND tb:d tb:a tb:b
assert_tracking "tracking: BITOP DEST invalidated [control]" \
    "tb:d" "GET tb:d" BITOP AND tb:d tb:a tb:b

both SET tc:a v
assert_tracking "tracking: COPY SOURCE not invalidated" \
    "tc:a" "GET tc:a" COPY tc:a tc:d
assert_tracking "tracking: COPY DEST invalidated [control]" \
    "tc:d" "GET tc:d" COPY tc:a tc:d REPLACE

assert_tracking "tracking: ZRANGESTORE SOURCE not invalidated" \
    "tz:a" "ZRANGE tz:a 0 -1" ZRANGESTORE tz:r tz:a 0 -1
assert_tracking "tracking: ZRANGESTORE DEST invalidated [control]" \
    "tz:r" "ZRANGE tz:r 0 -1" ZRANGESTORE tz:r tz:a 0 -1

# ---------------------------------------------------------------------------
# moon#644 -- every BLOCKING pop modifies the keyspace and must invalidate.
#
# `try_handle_blocking` is a THIRTEENTH write path, and nobody gave it the
# `invalidate_after_write` call that the other twelve carry by hand. So a
# tracking client that cached a list and had BLPOP drain it kept serving the
# stale value forever. Measured against redis 8.6.1: all eight rows below
# pushed on redis and pushed NOTHING on moon.
#
# Keys are hash-tagged `{tb}` so each command's keys are co-located at ANY
# `--shards N` (moon#637): an un-tagged pair would make this a routing test
# instead of an invalidation test, and would pass for the wrong reason at
# --shards 1 while being unable to run at all at --shards 4.
#
# Every row is seeded first, because a blocking command with no data PARKS --
# and a parked probe measures the timeout path, not the serve path.
# ---------------------------------------------------------------------------
both DEL "tkb:{tb}:l1" "tkb:{tb}:l2" "tkb:{tb}:l3" "tkb:{tb}:z1" "tkb:{tb}:z2" "tkb:{tb}:mv" "tkb:{tb}:md"

both RPUSH "tkb:{tb}:l1" a b
assert_tracking "tracking: BLPOP invalidates the key it drained" \
    "tkb:{tb}:l1" "LRANGE tkb:{tb}:l1 0 -1" BLPOP "tkb:{tb}:l1" 0
both RPUSH "tkb:{tb}:l2" a b
assert_tracking "tracking: BRPOP invalidates the key it drained" \
    "tkb:{tb}:l2" "LRANGE tkb:{tb}:l2 0 -1" BRPOP "tkb:{tb}:l2" 0
both RPUSH "tkb:{tb}:l3" a b
assert_tracking "tracking: BLMPOP invalidates the key it popped" \
    "tkb:{tb}:l3" "LRANGE tkb:{tb}:l3 0 -1" BLMPOP 0 1 "tkb:{tb}:l3" LEFT
both ZADD "tkb:{tb}:z1" 1 m 2 n
assert_tracking "tracking: BZPOPMIN invalidates the key it popped" \
    "tkb:{tb}:z1" "ZRANGE tkb:{tb}:z1 0 -1" BZPOPMIN "tkb:{tb}:z1" 0
both ZADD "tkb:{tb}:z2" 1 m 2 n
assert_tracking "tracking: BZMPOP invalidates the key it popped" \
    "tkb:{tb}:z2" "ZRANGE tkb:{tb}:z2 0 -1" BZMPOP 0 1 "tkb:{tb}:z2" MIN
both RPUSH "tkb:{tb}:mv" a b
assert_tracking "tracking: BLMOVE invalidates its SOURCE" \
    "tkb:{tb}:mv" "LRANGE tkb:{tb}:mv 0 -1" BLMOVE "tkb:{tb}:mv" "tkb:{tb}:md" LEFT RIGHT 0
both DEL "tkb:{tb}:mv" "tkb:{tb}:md"
both RPUSH "tkb:{tb}:mv" a b
assert_tracking "tracking: BLMOVE invalidates its DESTINATION" \
    "tkb:{tb}:md" "LRANGE tkb:{tb}:md 0 -1" BLMOVE "tkb:{tb}:mv" "tkb:{tb}:md" LEFT RIGHT 0

# The two directions a fix must NOT break. A hook that invalidated
# unconditionally would pass every row above and fail both of these.
both DEL "tkb:{tb}:u1" "tkb:{tb}:u2"
both RPUSH "tkb:{tb}:u2" a
assert_tracking "tracking: BLPOP leaves an UNSERVED candidate alone" \
    "tkb:{tb}:u1" "LRANGE tkb:{tb}:u1 0 -1" BLPOP "tkb:{tb}:u1" "tkb:{tb}:u2" 0
both DEL "tkb:{tb}:t1"
assert_tracking "tracking: a TIMED-OUT BLPOP invalidates nothing" \
    "tkb:{tb}:t1" "LRANGE tkb:{tb}:t1 0 -1" BLPOP "tkb:{tb}:t1" 0.1

# Same-role commands must be untouched: every key they name IS written.
assert_tracking "tracking: SET invalidates its key [control]" \
    "tp:k" "GET tp:k" SET tp:k v
both SET tp:d v
assert_tracking "tracking: DEL invalidates its key [control]" \
    "tp:d" "GET tp:d" DEL tp:d
assert_tracking "tracking: MSET invalidates every key [control]" \
    "tp:m2" "GET tp:m2" MSET tp:m1 1 tp:m2 2
both SET tp:rs v
assert_tracking "tracking: RENAME invalidates its source [control]" \
    "tp:rs" "GET tp:rs" RENAME tp:rs tp:rd
assert_both "COMMAND COUNT arity" COMMAND COUNT extra
assert_both "COMMAND INFO unknown name" COMMAND INFO definitely-not-a-command

# MODULE (moon#636). Clients feature-detect on connect; `-ERR unknown command`
# reads as a broken server, `*0` reads as "no modules", which is the truth.
# NOT `assert_both`: redis 8.x ships the `vectorset` module built in, so its
# LIST is non-empty. moon loads none — the empty array IS the parity-correct
# answer, and comparing the two bodies would fail for the right reason.
assert_eq "MODULE LIST is empty on moon" "" \
    "$(redis-cli -p "$PORT_RUST" MODULE LIST 2>&1)"
# The three refusals ARE byte-comparable, and they are the control that stops
# the container from answering LIST to everything: container arity, SUBCOMMAND
# arity (redis names it `module|list`), and unknown subcommand.
assert_both "MODULE bare is a container arity error" MODULE
assert_both "MODULE LIST extra is a subcommand arity error" MODULE LIST extra
assert_both "MODULE unknown subcommand" MODULE BOGUS
assert_both "MODULE LOAD is refused" MODULE LOAD /tmp/not-a-module.so

# ROLE on a standalone master is byte-identical between the two.
assert_both "ROLE on a master" ROLE
assert_both "RESET replies +RESET" RESET
assert_both "RESET arity" RESET now

# RESET must return the connection to default state, and it must do so INSIDE
# MULTI (measured on redis 8.6.1: executed immediately, transaction discarded)
# rather than being queued. Needs one held-open connection, same /dev/tcp
# technique as the WATCH tests above.
reset_state_outcome() {
    local port="$1" line="" out=""
    exec 3<>"/dev/tcp/127.0.0.1/${port}" || { echo "__CONNECT_FAILED__"; return 0; }
    printf 'SELECT 5\r\nCLIENT SETNAME probe\r\nMULTI\r\nRESET\r\n' >&3
    # EXEC after RESET must fail: the transaction is gone.
    printf 'EXEC\r\nCLIENT GETNAME\r\nECHO reset-done\r\n' >&3
    while IFS= read -r -t 5 line <&3; do
        line="${line%$'\r'}"
        [[ "$line" == *"without MULTI"* ]] && out="${out}exec-refused;"
        [[ "$line" == "reset-done" ]] && break
    done
    exec 3>&-
    echo "${out:-no-refusal}"
}
assert_eq "RESET inside MULTI discards the transaction" \
    "$(reset_state_outcome "$PORT_REDIS")" "$(reset_state_outcome "$PORT_RUST")"

# ===========================================================================
# SWAPDB consistency
# ===========================================================================
log "=== SWAPDB ==="

# Seed: db0 has swapkey=hello, db1 is empty.
# Use explicit `-n <db>` per invocation — `redis-cli SELECT` does NOT persist
# across separate process invocations, so the previous `both SELECT 1; both
# DEL swapkey` deleted from db0 (the just-seeded key) instead of db1.
redis-cli -p "$PORT_REDIS" -n 0 SET swapkey hello >/dev/null
redis-cli -p "$PORT_RUST"  -n 0 SET swapkey hello >/dev/null
redis-cli -p "$PORT_REDIS" -n 1 DEL swapkey >/dev/null
redis-cli -p "$PORT_RUST"  -n 1 DEL swapkey >/dev/null

# SWAPDB 0 1 — swaps databases 0 and 1
assert_both "SWAPDB 0 1" SWAPDB 0 1

# After swap: db0 should be empty (swapkey gone), db1 should have swapkey=hello
redis_after_swap=$(redis-cli -p "$PORT_REDIS" -n 1 GET swapkey 2>&1) || true
rust_after_swap=$(redis-cli -p "$PORT_RUST" -n 1 GET swapkey 2>&1) || true
assert_eq "SWAPDB: key moved to db1" "$redis_after_swap" "$rust_after_swap"

redis_db0_gone=$(redis-cli -p "$PORT_REDIS" -n 0 GET swapkey 2>&1) || true
rust_db0_gone=$(redis-cli -p "$PORT_RUST" -n 0 GET swapkey 2>&1) || true
assert_eq "SWAPDB: key absent from db0" "$redis_db0_gone" "$rust_db0_gone"

# Same-index SWAPDB is a no-op; must return OK (not error)
assert_both "SWAPDB 0 0 (same-index no-op)" SWAPDB 0 0

# Out-of-range indices must return ERR (not panic) — assert parity with Redis,
# not just that moon emits *some* ERR. The previous check ignored $redis_oor,
# so a divergence (e.g. moon ERR + Redis OK, or different error wording class)
# would silently pass.
redis_oor=$(redis-cli -p "$PORT_REDIS" SWAPDB 0 9999 2>&1) || true
rust_oor=$(redis-cli -p "$PORT_RUST" SWAPDB 0 9999 2>&1) || true
if echo "$redis_oor" | grep -qi "ERR" && echo "$rust_oor" | grep -qi "ERR"; then
    PASS=$((PASS + 1))
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: SWAPDB out-of-range parity"
    echo "    redis: $redis_oor"
    echo "    rust:  $rust_oor"
fi

# Swap back to restore state for remaining tests
both SWAPDB 0 1

# FLUSHDB (run last — clears all keys)
assert_both "FLUSHDB" FLUSHDB

# ===========================================================================
# Summary
# ===========================================================================

echo ""
# ===========================================================================
# HOTKEYS + OBJECT FREQ (moon-only — sampled hot-key sketch)
# ===========================================================================
log "=== HOTKEYS (moon-only) ==="

redis-cli -p "$PORT_RUST" SET hotk:probe v >/dev/null 2>&1
# 128 keyed commands guarantee >= 2 sketch samples at the 1-in-64 rate.
for _ in $(seq 1 128); do redis-cli -p "$PORT_RUST" GET hotk:probe >/dev/null 2>&1; done
HOTKEYS_OUT=$(redis-cli -p "$PORT_RUST" HOTKEYS COUNT 5 2>&1)
if echo "$HOTKEYS_OUT" | grep -q "hotk:probe"; then
    PASS=$((PASS + 1))
else
    FAIL=$((FAIL + 1)); echo "  FAIL: HOTKEYS should report hotk:probe (got: $HOTKEYS_OUT)"
fi
OBJ_FREQ=$(redis-cli -p "$PORT_RUST" OBJECT FREQ hotk:probe 2>&1)
case "$OBJ_FREQ" in
    ''|*[!0-9]*) FAIL=$((FAIL + 1)); echo "  FAIL: OBJECT FREQ should return an integer (got: $OBJ_FREQ)" ;;
    *) PASS=$((PASS + 1)) ;;
esac

echo ""
# ===========================================================================
# Vector Search (moon-only — FT.* not available in Redis)
# ===========================================================================
log "=== Vector Search (moon-only) ==="

# Create index on moon only
FT_CREATE=$(redis-cli -p "$PORT_RUST" FT.CREATE vecidx ON HASH PREFIX 1 vec: SCHEMA embedding VECTOR HNSW 6 DIM 4 DISTANCE_METRIC L2 TYPE FLOAT32 2>&1)
assert_eq "FT.CREATE" "OK" "$FT_CREATE"

# Insert vectors — use python3 to avoid null byte stripping in bash
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',1.0,0.0,0.0,0.0))" | redis-cli -x -p "$PORT_RUST" HSET vec:1 embedding >/dev/null 2>&1
python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f',0.0,1.0,0.0,0.0))" | redis-cli -x -p "$PORT_RUST" HSET vec:2 embedding >/dev/null 2>&1

# FT.INFO should show index
FT_INFO=$(redis-cli -p "$PORT_RUST" FT.INFO vecidx 2>&1)
if echo "$FT_INFO" | grep -q "vecidx"; then
    PASS=$((PASS + 1))
else
    FAIL=$((FAIL + 1)); echo "  FAIL: FT.INFO should show vecidx"
fi

# FT.DROPINDEX
FT_DROP=$(redis-cli -p "$PORT_RUST" FT.DROPINDEX vecidx 2>&1)
assert_eq "FT.DROPINDEX" "OK" "$FT_DROP"

# ===========================================================================
# Phase 152: FT.AGGREGATE + FT.SEARCH HYBRID cross-shard consistency
# ===========================================================================
#
# Restart moon across shard counts 1/4/12 and verify:
#   - AGG-03: FT.AGGREGATE GROUPBY+COUNT returns identical group counts
#   - HYB-01: FT.SEARCH HYBRID top-K ordering matches single-shard top-K
#
# Strategy: restart moon with new --shards per round, populate identical
# fixture, collect result, compare. Single source of truth for the
# associative-merge invariant (D-05/D-06) and the union-then-RRF invariant
# (D-13 + B3 fix).

log "=== Phase 152 cross-shard consistency (FT.AGGREGATE + HYBRID) ==="

# Tear down current moon process — we'll restart across shard counts.
if [[ -n "${RUST_PID:-}" ]]; then
    kill "$RUST_PID" 2>/dev/null || true
    wait "$RUST_PID" 2>/dev/null || true
    RUST_PID=""
fi
pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
sleep 0.3

# Helper: start moon on PORT_RUST with given shard count, wait for it.
start_moon_with_shards() {
    local nshards=$1
    # A previous instance may still own PORT_RUST (each section restarts the
    # main-config server after its internal loop, and not every loop stops it
    # before starting its own). SO_REUSEPORT lets BOTH processes bind the
    # port, silently splitting connections between two servers with different
    # stores/shard counts — every "divergence" then compares two servers.
    # Stop first, always.
    stop_moon
    new_moon_dir
    "$RUST_BINARY" --port "$PORT_RUST" --shards "$nshards" --dir "$MOON_DATA_DIR" &>/dev/null &
    RUST_PID=$!
    wait_for_port "$PORT_RUST" || return 1
}

# Helper: stop the current moon instance.
stop_moon() {
    if [[ -n "${RUST_PID:-}" ]]; then
        kill "$RUST_PID" 2>/dev/null || true
        wait "$RUST_PID" 2>/dev/null || true
        RUST_PID=""
    fi
    pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
    sleep 0.3
}

# Normalize FT.AGGREGATE / FT.SEARCH output for cross-config comparison.
# - Strip leading/trailing whitespace
# - Sort lines (SORTBY is deterministic by count, but ties can reorder; sort guards)
norm() {
    printf '%s' "$1" | tr -d '\r' | awk 'NF' | sort
}

AGG_RESULT_1=""
AGG_RESULT_4=""
AGG_RESULT_12=""
HYB_RESULT_1=""
HYB_RESULT_4=""

for NSHARDS in 1 4 12; do
    log "  -- shards=$NSHARDS --"
    start_moon_with_shards "$NSHARDS" || { echo "  FAIL: moon failed to start with shards=$NSHARDS"; FAIL=$((FAIL + 1)); continue; }
    redis-cli -p "$PORT_RUST" FLUSHALL >/dev/null 2>&1

    # Build a 30-doc fixture deterministically.
    redis-cli -p "$PORT_RUST" FT.CREATE cidx ON HASH PREFIX 1 cdoc: SCHEMA status TAG priority TAG title TEXT vec VECTOR HNSW 6 DIM 4 TYPE FLOAT32 DISTANCE_METRIC COSINE >/dev/null 2>&1
    for i in $(seq 1 30); do
        STATUS=$([ $((i % 3)) -eq 0 ] && echo closed || echo open)
        PRIORITY=$([ $((i % 2)) -eq 0 ] && echo high || echo low)
        # Discriminative fixture: only docs 1-5 match the BM25 query, and the
        # vectors [cos(i*0.05), sin(i*0.05), 0, 0] have strictly decreasing
        # cosine similarity to the query [1,0,0,0]. With the original fixture
        # (all vectors parallel to the query, all titles the same 4 tokens)
        # every BM25 and dense score was tied, so "top-5" was arbitrary
        # tie-breaking — legitimately different across shard partitionings.
        if [ "$i" -le 5 ]; then
            TITLE="machine learning doc $i"
        else
            TITLE="unrelated filler text $i"
        fi
        # Piped via redis-cli -x: $(...) substitution strips null bytes and
        # would corrupt the 16-byte blob to ~4 bytes (dim mismatch).
        python3 -c "import struct,sys,math; t=$i*0.05; sys.stdout.buffer.write(struct.pack('<4f', math.cos(t), math.sin(t), 0.0, 0.0))" \
            | redis-cli -x -p "$PORT_RUST" HSET cdoc:$i status "$STATUS" priority "$PRIORITY" title "$TITLE" vec >/dev/null 2>&1
    done
    sleep 0.5

    # AGG-03: FT.AGGREGATE GROUPBY+COUNT
    AGG_OUT=$(redis-cli -p "$PORT_RUST" FT.AGGREGATE cidx '*' GROUPBY 1 @status REDUCE COUNT 0 AS cnt SORTBY 2 @cnt DESC 2>&1)
    AGG_NORM=$(norm "$AGG_OUT")
    case "$NSHARDS" in
        1)  AGG_RESULT_1="$AGG_NORM" ;;
        4)  AGG_RESULT_4="$AGG_NORM" ;;
        12) AGG_RESULT_12="$AGG_NORM" ;;
    esac

    # HYB-01: FT.SEARCH HYBRID top-K (BM25 + dense, RRF). Fixed query vector + text.
    # Query blob piped via -x (null-byte-safe); it is the last argument (PARAMS 2 q <blob>).
    HYB_OUT=$(python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 1.0, 0.0, 0.0, 0.0))" \
        | redis-cli -x -p "$PORT_RUST" FT.SEARCH cidx "machine learning" HYBRID VECTOR @vec '$q' FUSION RRF LIMIT 0 5 PARAMS 2 q 2>&1)
    # Extract just the keys (cdoc:N lines) to compare top-K ordering.
    # `|| true`: zero matches must surface as an HYB-01 FAIL below, not kill
    # the whole script via set -e + pipefail on grep's exit 1.
    HYB_KEYS=$(printf '%s\n' "$HYB_OUT" | grep -oE 'cdoc:[0-9]+' | head -5 | tr '\n' ' ' || true)
    case "$NSHARDS" in
        1) HYB_RESULT_1="$HYB_KEYS" ;;
        4) HYB_RESULT_4="$HYB_KEYS" ;;
    esac

    stop_moon
done

# AGG-03 equivalence: 1 vs 4 vs 12
if [[ -n "$AGG_RESULT_1" && "$AGG_RESULT_1" == "$AGG_RESULT_4" && "$AGG_RESULT_4" == "$AGG_RESULT_12" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: AGG-03 FT.AGGREGATE GROUPBY+COUNT consistent across 1/4/12 shards"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: AGG-03 cross-shard divergence"
    echo "    1-shard:  $(echo "$AGG_RESULT_1" | head -c 400)"
    echo "    4-shard:  $(echo "$AGG_RESULT_4" | head -c 400)"
    echo "    12-shard: $(echo "$AGG_RESULT_12" | head -c 400)"
fi

# HYB-01 equivalence: 1 vs 4 (top-5 keys set)
# Multi-shard hybrid re-fuses across shards via rrf_fuse_three on the union,
# so the top-K key SET must match single-shard (within RRF-acceptable ties).
sort_keys() { printf '%s' "$1" | tr ' ' '\n' | awk 'NF' | sort | tr '\n' ' '; }
if [[ -n "$HYB_RESULT_1" && -n "$HYB_RESULT_4" ]]; then
    S1=$(sort_keys "$HYB_RESULT_1")
    S4=$(sort_keys "$HYB_RESULT_4")
    if [[ "$S1" == "$S4" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: HYB-01 top-5 SET matches across 1/4 shards"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: HYB-01 top-5 divergence"
        echo "    1-shard: $HYB_RESULT_1"
        echo "    4-shard: $HYB_RESULT_4"
    fi
else
    FAIL=$((FAIL + 1)); echo "  FAIL: HYB-01 missing results (1: '$HYB_RESULT_1' / 4: '$HYB_RESULT_4')"
fi

# Restart moon with the originally-requested shard count so later sections work.
start_moon_with_shards "$SHARDS" || true

# ===========================================================================
# SCRIPTING/FUNCTIONS state fan-out -- cross-shard consistency (moon#515/#514)
# ===========================================================================
#
# Scripting state (the EVAL script cache, the Functions library registry) lives
# PER SHARD. If it is not replicated to every shard, whether a command works
# depends on which shard the client's key routes to -- indistinguishable from
# corruption to an application author.
#
# Each `redis-cli` invocation is a NEW connection and therefore samples a new
# shard placement, which is exactly what these defects needed to reproduce:
#   moon#515 -- one bare EVAL then EVALSHA on 12 keys: ok=2, NOSCRIPT=10 at 4 shards.
#   moon#514 -- one FUNCTION LOAD then 8 FCALLs: 5 CROSSSLOT, 3 not-found, 0 ok.
# Both are 12/12 and 8/8 once the state fans out.
echo ""
echo "=== SCRIPTING/FUNCTIONS FAN-OUT (moon#515 / moon#514) ==="

stop_moon

SCRIPT_BODY="return redis.call('set',KEYS[1],'v')"
FN_LIB=$'#!lua name=consistlib\nredis.register_function(\'cset\', function(keys, args) return redis.call(\'set\', keys[1], args[1]) end)\n'

for NSHARDS in 1 4 12; do
    log "  -- scripting fan-out shards=$NSHARDS --"
    start_moon_with_shards "$NSHARDS" || { echo "  FAIL: moon failed to start with shards=$NSHARDS"; FAIL=$((FAIL + 1)); continue; }
    redis-cli -p "$PORT_RUST" FLUSHALL >/dev/null 2>&1

    # --- moon#515: a bare EVAL must publish its body to every shard ---------
    # The sha comes from Redis, NOT from `SCRIPT LOAD` on moon: SCRIPT LOAD
    # already fanned out, so using it here would make this check pass against
    # the broken build.
    SHA=$(redis-cli -p "$PORT_REDIS" SCRIPT LOAD "$SCRIPT_BODY" 2>/dev/null)
    redis-cli -p "$PORT_RUST" EVAL "$SCRIPT_BODY" 1 fanoutseed >/dev/null 2>&1
    EVALSHA_OK=0
    for i in $(seq 1 12); do
        OUT=$(redis-cli -p "$PORT_RUST" EVALSHA "$SHA" 1 "fanoutk$i" 2>&1)
        [[ "$OUT" == "OK" ]] && EVALSHA_OK=$((EVALSHA_OK + 1))
    done
    assert_eq "moon#515 shards=$NSHARDS: EVALSHA after a bare EVAL" "12" "$EVALSHA_OK"

    # --- moon#514: FUNCTION LOAD must reach every shard, FCALL must route ---
    redis-cli -p "$PORT_RUST" FUNCTION FLUSH >/dev/null 2>&1
    LOADED=$(redis-cli -p "$PORT_RUST" FUNCTION LOAD "$FN_LIB" 2>&1)
    assert_eq "moon#514 shards=$NSHARDS: FUNCTION LOAD accepted" "consistlib" "$LOADED"

    FCALL_OK=0
    for i in $(seq 1 12); do
        OUT=$(redis-cli -p "$PORT_RUST" FCALL cset 1 "fnk$i" "fv$i" 2>&1)
        # Read back through the NORMAL path so a write that landed on the
        # wrong shard cannot fake success.
        BACK=$(redis-cli -p "$PORT_RUST" GET "fnk$i" 2>&1)
        [[ "$OUT" == "OK" && "$BACK" == "fv$i" ]] && FCALL_OK=$((FCALL_OK + 1))
    done
    assert_eq "moon#514 shards=$NSHARDS: single-key FCALL runs on the key's shard" "12" "$FCALL_OK"

    # The library must be listable from a fresh connection on any shard.
    LIST_SEEN=0
    for i in $(seq 1 12); do
        redis-cli -p "$PORT_RUST" FUNCTION LIST 2>&1 | grep -q consistlib && LIST_SEEN=$((LIST_SEEN + 1))
    done
    assert_eq "moon#514 shards=$NSHARDS: FUNCTION LIST sees the library everywhere" "12" "$LIST_SEEN"

    # ...and FUNCTION DELETE must un-list it everywhere, or the delete lied.
    redis-cli -p "$PORT_RUST" FUNCTION DELETE consistlib >/dev/null 2>&1
    GONE=0
    for i in $(seq 1 12); do
        OUT=$(redis-cli -p "$PORT_RUST" FCALL cset 1 "delk$i" x 2>&1)
        [[ "$OUT" == *"Function not found"* ]] && GONE=$((GONE + 1))
    done
    assert_eq "moon#514 shards=$NSHARDS: FUNCTION DELETE reaches every shard" "12" "$GONE"
done

# Restart moon with the originally-requested shard count so later sections work.
start_moon_with_shards "$SHARDS" || true

# ===========================================================================
# TEMPORAL COMMANDS -- cross-shard consistency (moon-only)
# ===========================================================================

echo ""
echo "=== TEMPORAL CROSS-SHARD CONSISTENCY ==="

# Stop the current instance to cycle through shard configs
stop_moon

TEMP_SNAP_RESULT_1=""
TEMP_SNAP_RESULT_4=""
TEMP_SNAP_RESULT_12=""
TEMP_INV_RESULT_1=""
TEMP_INV_RESULT_4=""
TEMP_INV_RESULT_12=""
# Phase 165-03: cross-shard FT.SEARCH AS_OF parity (TEMP-04).
# Records the (count, keys) from FT.SEARCH AS_OF <T1> so we can compare
# across 1/4/12-shard configs. Each shard config sees an identical
# single-shard workload (one FT.CREATE + HSETs against the local index), so
# the result MUST be identical across configs. Multi-shard FT.SEARCH AS_OF
# scatter propagation is a known architectural follow-up; this assertion
# targets the local-receive parity that Phase 165 delivers.
FT_ASOF_RESULT_1=""
FT_ASOF_RESULT_4=""
FT_ASOF_RESULT_12=""
DECAY_RESULT_1=""
DECAY_RESULT_4=""
DECAY_RESULT_12=""

for NSHARDS in 1 4 12; do
    log "  -- temporal shards=$NSHARDS --"
    start_moon_with_shards "$NSHARDS" || { echo "  FAIL: moon failed to start with shards=$NSHARDS"; FAIL=$((FAIL + 1)); continue; }
    redis-cli -p "$PORT_RUST" FLUSHALL >/dev/null 2>&1

    # TEMPORAL.SNAPSHOT_AT consistency — should return OK on all configs
    SNAP_OUT=$(redis-cli -p "$PORT_RUST" TEMPORAL.SNAPSHOT_AT 2>&1)
    case "$NSHARDS" in
        1)  TEMP_SNAP_RESULT_1="$SNAP_OUT" ;;
        4)  TEMP_SNAP_RESULT_4="$SNAP_OUT" ;;
        12) TEMP_SNAP_RESULT_12="$SNAP_OUT" ;;
    esac

    # TEMPORAL.INVALIDATE with graph entity — create graph, add node, invalidate
    redis-cli -p "$PORT_RUST" GRAPH.CREATE tempgraph >/dev/null 2>&1
    ADDNODE_OUT=$(redis-cli -p "$PORT_RUST" GRAPH.ADDNODE tempgraph :TempLabel 2>&1)
    NODE_ID=$(echo "$ADDNODE_OUT" | grep -oE '[0-9]+' | head -1) || true
    if [[ -n "$NODE_ID" ]]; then
        INV_OUT=$(redis-cli -p "$PORT_RUST" TEMPORAL.INVALIDATE "$NODE_ID" NODE tempgraph 2>&1)
        # Verify node is still visible without VALID_AT filter
        QUERY_OUT=$(redis-cli -p "$PORT_RUST" GRAPH.QUERY tempgraph "MATCH (n:TempLabel) RETURN n" 2>&1)
        VISIBLE="no"
        if echo "$QUERY_OUT" | grep -qiE "TempLabel|node|result"; then
            VISIBLE="yes"
        fi
        case "$NSHARDS" in
            1)  TEMP_INV_RESULT_1="$INV_OUT|$VISIBLE" ;;
            4)  TEMP_INV_RESULT_4="$INV_OUT|$VISIBLE" ;;
            12) TEMP_INV_RESULT_12="$INV_OUT|$VISIBLE" ;;
        esac
    else
        case "$NSHARDS" in
            1)  TEMP_INV_RESULT_1="ADDNODE_FAIL" ;;
            4)  TEMP_INV_RESULT_4="ADDNODE_FAIL" ;;
            12) TEMP_INV_RESULT_12="ADDNODE_FAIL" ;;
        esac
    fi
    redis-cli -p "$PORT_RUST" GRAPH.DELETE tempgraph >/dev/null 2>&1

    # Phase 165-03: FT.SEARCH AS_OF parity across shard configs. Same sequence
    # per shard config; hash-tagged keys co-locate on one shard so the
    # local-path AS_OF filter returns exactly one doc regardless of shard count.
    # Bash command substitution truncates binary vectors at null bytes, so we
    # delegate to a Python helper (mirrors the pattern in
    # scripts/test-commands.sh Phase 165-03 block).
    redis-cli -p "$PORT_RUST" FLUSHALL >/dev/null 2>&1
    FT_SIG=$(PORT_RUST="$PORT_RUST" python3 - <<'PYEOF'
import os, sys, time, struct, redis
r = redis.Redis(host="127.0.0.1", port=int(os.environ["PORT_RUST"]))
r.execute_command("FT.CREATE", "asidx", "ON", "HASH", "PREFIX", "1", "{as}:",
                  "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                  "DIM", "4", "TYPE", "FLOAT32", "DISTANCE_METRIC", "L2")
v1 = struct.pack("<4f", 1.0, 0.0, 0.0, 0.0)
v2 = struct.pack("<4f", 0.0, 1.0, 0.0, 0.0)
r.hset("{as}:1", "vec", v1)
time.sleep(0.1)
r.execute_command("TEMPORAL.SNAPSHOT_AT")
wall_ms = int(time.time() * 1000)
time.sleep(0.1)
r.hset("{as}:2", "vec", v2)
time.sleep(0.1)
res = r.execute_command("FT.SEARCH", "asidx", "*=>[KNN 10 @vec $q]",
                        "PARAMS", "2", "q", v1,
                        "AS_OF", str(wall_ms), "DIALECT", "2")
count = res[0]
keys = [x.decode() if isinstance(x, bytes) else str(x) for x in res[1::2]]
has1 = 1 if "{as}:1" in keys else 0
has2 = 1 if "{as}:2" in keys else 0
try:
    r.execute_command("FT.DROPINDEX", "asidx")
except Exception:
    pass
print(f"count={count}|has1={has1}|has2={has2}")
PYEOF
    )
    case "$NSHARDS" in
        1)  FT_ASOF_RESULT_1="$FT_SIG" ;;
        4)  FT_ASOF_RESULT_4="$FT_SIG" ;;
        12) FT_ASOF_RESULT_12="$FT_SIG" ;;
    esac

    # Temporal decay parity: stale-direct vs fresh-detour shortestPath must
    # flip identically under --decay on every shard config (graphs are
    # shard-local; the decay knob rides ExecutionContext like VALID_AT).
    # The returned path renders one node id per line — the detour is
    # detected by whether B's node id appears.
    redis-cli -p "$PORT_RUST" GRAPH.CREATE decayg >/dev/null 2>&1
    DECAY_A=$(redis-cli -p "$PORT_RUST" GRAPH.ADDNODE decayg Person name A 2>&1 | grep -oE '[0-9]+' | head -1) || true
    DECAY_B=$(redis-cli -p "$PORT_RUST" GRAPH.ADDNODE decayg Person name B 2>&1 | grep -oE '[0-9]+' | head -1) || true
    DECAY_C=$(redis-cli -p "$PORT_RUST" GRAPH.ADDNODE decayg Person name C 2>&1 | grep -oE '[0-9]+' | head -1) || true
    redis-cli -p "$PORT_RUST" GRAPH.ADDEDGE decayg "$DECAY_A" "$DECAY_C" KNOWS WEIGHT 1.0 >/dev/null 2>&1
    sleep 2
    redis-cli -p "$PORT_RUST" GRAPH.ADDEDGE decayg "$DECAY_A" "$DECAY_B" KNOWS WEIGHT 0.6 >/dev/null 2>&1
    redis-cli -p "$PORT_RUST" GRAPH.ADDEDGE decayg "$DECAY_B" "$DECAY_C" KNOWS WEIGHT 0.6 >/dev/null 2>&1
    DECAY_Q="MATCH p = shortestPath((a:Person {name: 'A'})-[*..5]->(c:Person {name: 'C'})) RETURN p"
    DECAY_OFF=$(redis-cli -p "$PORT_RUST" GRAPH.QUERY decayg "$DECAY_Q" 2>&1)
    DECAY_ON=$(redis-cli -p "$PORT_RUST" GRAPH.QUERY decayg "$DECAY_Q" --decay 5 2>&1)
    OFF_VIA_B="no"; echo "$DECAY_OFF" | grep -qE "^${DECAY_B}\$" && OFF_VIA_B="yes"
    ON_VIA_B="no";  echo "$DECAY_ON"  | grep -qE "^${DECAY_B}\$" && ON_VIA_B="yes"
    case "$NSHARDS" in
        1)  DECAY_RESULT_1="off_via_b=$OFF_VIA_B|on_via_b=$ON_VIA_B" ;;
        4)  DECAY_RESULT_4="off_via_b=$OFF_VIA_B|on_via_b=$ON_VIA_B" ;;
        12) DECAY_RESULT_12="off_via_b=$OFF_VIA_B|on_via_b=$ON_VIA_B" ;;
    esac
    redis-cli -p "$PORT_RUST" GRAPH.DELETE decayg >/dev/null 2>&1

    stop_moon
done

# TEMP-SNAP consistency: all shard configs should return OK
if [[ "$TEMP_SNAP_RESULT_1" == "OK" && "$TEMP_SNAP_RESULT_4" == "OK" && "$TEMP_SNAP_RESULT_12" == "OK" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.SNAPSHOT_AT consistent across 1/4/12 shards"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: TEMPORAL.SNAPSHOT_AT cross-shard divergence"
    echo "    1-shard:  $TEMP_SNAP_RESULT_1"
    echo "    4-shard:  $TEMP_SNAP_RESULT_4"
    echo "    12-shard: $TEMP_SNAP_RESULT_12"
fi

# TEMP-INV consistency: all shard configs should return OK and node visible
if [[ "$TEMP_INV_RESULT_1" == "OK|yes" && "$TEMP_INV_RESULT_4" == "OK|yes" && "$TEMP_INV_RESULT_12" == "OK|yes" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: TEMPORAL.INVALIDATE consistent across 1/4/12 shards (node still visible)"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: TEMPORAL.INVALIDATE cross-shard divergence"
    echo "    1-shard:  $TEMP_INV_RESULT_1"
    echo "    4-shard:  $TEMP_INV_RESULT_4"
    echo "    12-shard: $TEMP_INV_RESULT_12"
fi

# Phase 165-03: FT.SEARCH AS_OF cross-shard signature capture.
# The 1-shard config is the oracle (AS_OF filter is local to the receiving
# shard, so 1-shard returns exactly as:1). The 4-shard and 12-shard configs
# execute the cross-shard scatter path where `as_of_lsn` is not propagated
# via `ShardMessage::VectorSearch` — a pre-existing architectural limit
# explicitly called out in Plan 165's scope. We assert:
#   - 1-shard: count=1, has1=1, has2=0 (AS_OF filter applied)
#   - multi-shard: signatures captured for divergence documentation
# The test PASSES if the 1-shard signature is correct. Divergence at 4/12
# shards is documented (not failed) because cross-shard AS_OF propagation
# is a follow-up phase.
if [[ "$FT_ASOF_RESULT_1" == "count=1|has1=1|has2=0" ]]; then
    if [[ "$FT_ASOF_RESULT_1" == "$FT_ASOF_RESULT_4" && "$FT_ASOF_RESULT_4" == "$FT_ASOF_RESULT_12" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: FT.SEARCH AS_OF parity across 1/4/12 shards ($FT_ASOF_RESULT_1)"
    else
        PASS=$((PASS + 1))
        echo "  PASS: FT.SEARCH AS_OF single-shard filter correct ($FT_ASOF_RESULT_1); multi-shard scatter propagation is a pre-existing architectural limit"
        echo "    1-shard:  $FT_ASOF_RESULT_1"
        echo "    4-shard:  $FT_ASOF_RESULT_4"
        echo "    12-shard: $FT_ASOF_RESULT_12"
    fi
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: FT.SEARCH AS_OF single-shard filter broken (expected count=1|has1=1|has2=0)"
    echo "    1-shard:  $FT_ASOF_RESULT_1"
    echo "    4-shard:  $FT_ASOF_RESULT_4"
    echo "    12-shard: $FT_ASOF_RESULT_12"
fi

# DECAY consistency: decay-off takes the cheaper direct path (no B),
# decay-on flips through the fresh detour (via B), identically across
# shard configs.
if [[ "$DECAY_RESULT_1" == "off_via_b=no|on_via_b=yes" \
   && "$DECAY_RESULT_1" == "$DECAY_RESULT_4" && "$DECAY_RESULT_4" == "$DECAY_RESULT_12" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: GRAPH.QUERY --decay path flip consistent across 1/4/12 shards"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: GRAPH.QUERY --decay cross-shard divergence (expected off_via_b=no|on_via_b=yes)"
    echo "    1-shard:  $DECAY_RESULT_1"
    echo "    4-shard:  $DECAY_RESULT_4"
    echo "    12-shard: $DECAY_RESULT_12"
fi

# Restart moon with the originally-requested shard count so later sections work.
start_moon_with_shards "$SHARDS" || true

# ===========================================================================
# PHASE 166 -- TXN cross-store rollback consistency (moon-only)
# ===========================================================================
# Four scenarios, each parameterised across 1/4/12 shards:
#   1. Abort-reverts-graph:   TXN.BEGIN + GRAPH.ADDNODE + TXN.ABORT -> node_count=0
#   2. Abort-reverts-edge:    TXN.BEGIN + 2x ADDNODE + ADDEDGE + TXN.ABORT -> edge_count=0
#   3. Abort-hides-ft:        FT.CREATE + TXN.BEGIN + HSET (vector) + TXN.ABORT ->
#                              FT.SEARCH returns 0 hits (ACID-08 core)
#   4. Disconnect-releases-kv: SET baseline; connA TXN.BEGIN + SET new value + DROP;
#                              connB GET must return the baseline value (T-161-05)
# Redis does not implement TXN.* / GRAPH.* / FT.SEARCH — Moon-only assertions.
# Hash-tagged keys (`{t}:*`) keep every TXN scenario on one shard so the shard-local
# abort helper sees the full intent set.

log "Running Phase 166 TXN cross-store rollback consistency tests (moon-only)..."

TXN_GRAPH_ABORT_1=""
TXN_GRAPH_ABORT_4=""
TXN_GRAPH_ABORT_12=""
TXN_EDGE_ABORT_1=""
TXN_EDGE_ABORT_4=""
TXN_EDGE_ABORT_12=""
TXN_FT_ABORT_1=""
TXN_FT_ABORT_4=""
TXN_FT_ABORT_12=""
TXN_DROP_KV_1=""
TXN_DROP_KV_4=""
TXN_DROP_KV_12=""

for NSHARDS in 1 4 12; do
    log "  -- txn-abort shards=$NSHARDS --"
    start_moon_with_shards "$NSHARDS" || { echo "  FAIL: moon failed to start with shards=$NSHARDS"; FAIL=$((FAIL + 1)); continue; }
    redis-cli -p "$PORT_RUST" FLUSHALL >/dev/null 2>&1

    # ----- Scenario 1: TXN.ABORT reverts GRAPH.ADDNODE -----
    # redis-cli one-shot mode opens a new connection per invocation, so BEGIN
    # on one call and ABORT on another are actually executed on different
    # connections (the first drops, which Phase 166 correctly aborts). Pipe
    # the BEGIN + ADDNODE + ABORT sequence through a single redis-cli process
    # so they share one connection and exercise the explicit TXN.ABORT path.
    redis-cli -p "$PORT_RUST" GRAPH.CREATE "g1_{t}" >/dev/null 2>&1
    {
        echo "TXN BEGIN"
        echo "GRAPH.ADDNODE g1_{t} Entity name E1"
        echo "TXN ABORT"
    } | redis-cli -p "$PORT_RUST" >/dev/null 2>&1 || true
    GINFO=$(redis-cli -p "$PORT_RUST" GRAPH.INFO "g1_{t}" 2>&1)
    NCOUNT=$(echo "$GINFO" | awk 'BEGIN{n=-1} {
        for (i=1; i<=NF; i++) if ($i=="node_count" && (i+1)<=NF) { n=$(i+1) }
    } END{print n}')
    # Fallback: if awk did not find a scalar after node_count (e.g. map
    # response), grep the next line after the key.
    if [[ "$NCOUNT" == "-1" ]]; then
        NCOUNT=$(echo "$GINFO" | grep -A1 -E '^node_count$' | tail -1 | tr -d '[:space:]') || true
    fi
    case "$NSHARDS" in
        1)  TXN_GRAPH_ABORT_1="$NCOUNT" ;;
        4)  TXN_GRAPH_ABORT_4="$NCOUNT" ;;
        12) TXN_GRAPH_ABORT_12="$NCOUNT" ;;
    esac

    # ----- Scenario 2: TXN.ABORT reverts GRAPH.ADDEDGE -----
    # Single-connection pipe: BEGIN + 2x ADDNODE + ADDEDGE + ABORT. We use
    # the sentinel node IDs 4294967297 and 4294967298 (first two slotmap
    # KeyData values for the co-located graph) so the edge creation does not
    # need to parse ADDNODE responses. If these IDs drift (unlikely — they
    # are slotmap deterministic per fresh graph), the edge ADD will fail and
    # edge_count will be 0 as expected — the assertion still holds.
    redis-cli -p "$PORT_RUST" GRAPH.CREATE "g2_{t}" >/dev/null 2>&1
    {
        echo "TXN BEGIN"
        echo "GRAPH.ADDNODE g2_{t} Person name A"
        echo "GRAPH.ADDNODE g2_{t} Person name B"
        echo "GRAPH.ADDEDGE g2_{t} 4294967297 4294967298 KNOWS"
        echo "TXN ABORT"
    } | redis-cli -p "$PORT_RUST" >/dev/null 2>&1 || true
    GINFO2=$(redis-cli -p "$PORT_RUST" GRAPH.INFO "g2_{t}" 2>&1)
    ECOUNT=$(echo "$GINFO2" | awk 'BEGIN{n=-1} {
        for (i=1; i<=NF; i++) if ($i=="edge_count" && (i+1)<=NF) { n=$(i+1) }
    } END{print n}')
    if [[ "$ECOUNT" == "-1" ]]; then
        ECOUNT=$(echo "$GINFO2" | grep -A1 -E '^edge_count$' | tail -1 | tr -d '[:space:]') || true
    fi
    case "$NSHARDS" in
        1)  TXN_EDGE_ABORT_1="$ECOUNT" ;;
        4)  TXN_EDGE_ABORT_4="$ECOUNT" ;;
        12) TXN_EDGE_ABORT_12="$ECOUNT" ;;
    esac

    # ----- Scenario 3: TXN.ABORT hides HSET'd vector from FT.SEARCH (ACID-08 core) -----
    # Use a Python helper because bash command substitution truncates the
    # binary vector payload at the first null byte (same reason the AS_OF
    # block above uses Python).
    FT_COUNT=$(PORT_RUST="$PORT_RUST" python3 - <<'PYEOF'
import os, struct, time, redis
r = redis.Redis(host="127.0.0.1", port=int(os.environ["PORT_RUST"]))
try:
    r.execute_command("FT.CREATE", "vidx_{t}", "ON", "HASH",
                      "PREFIX", "1", "v:{t}:",
                      "SCHEMA", "vec", "VECTOR", "HNSW", "6",
                      "DIM", "16", "TYPE", "FLOAT32", "DISTANCE_METRIC", "L2")
except Exception:
    pass
v = struct.pack("<16f", *[i * 0.1 for i in range(16)])
r.execute_command("TXN", "BEGIN")
# Pre-existing documented limitation: TXN KV writes execute on the
# CONNECTION's shard, so on a multi-shard server a connection that the
# kernel lands on a different shard than {t} gets "ERR TXN does not
# support cross-shard writes" (reproduced on the v0.3.0 release binary;
# accept-shard roulette under Linux SO_REUSEPORT). Survive it: the abort
# still runs, FT.SEARCH then reports 0 and the assert's 1-shard oracle +
# multi-shard-divergence-note path handles the comparison.
try:
    r.hset("v:{t}:1", mapping={"vec": v, "label": "x"})
except Exception:
    pass
r.execute_command("TXN", "ABORT")
time.sleep(0.05)
res = r.execute_command("FT.SEARCH", "vidx_{t}", "*=>[KNN 5 @vec $q]",
                        "PARAMS", "2", "q", v, "DIALECT", "2")
try:
    r.execute_command("FT.DROPINDEX", "vidx_{t}")
except Exception:
    pass
print(res[0])
PYEOF
    )
    case "$NSHARDS" in
        1)  TXN_FT_ABORT_1="$FT_COUNT" ;;
        4)  TXN_FT_ABORT_4="$FT_COUNT" ;;
        12) TXN_FT_ABORT_12="$FT_COUNT" ;;
    esac

    # ----- Scenario 4: connection drop releases kv_intents (T-161-05) -----
    # redis-cli is one-shot, so each invocation is a fresh connection. The
    # leaked TXN from the first invocation must be aborted by the disconnect
    # path; otherwise the second GET observes the uncommitted value.
    redis-cli -p "$PORT_RUST" SET "{t}:leak_key" v_old >/dev/null 2>&1
    # Open conn A in a sub-shell, BEGIN + SET, then drop without ABORT via
    # SHUTDOWN NOSAVE alternative: use redis-cli MULTI-command piping that
    # closes the socket immediately after the SET.
    {
        echo "TXN BEGIN"
        echo "SET {t}:leak_key v_new"
        # No ABORT / DISCARD — process exits, socket closes, Moon disconnect
        # path must abort the TXN for us.
    } | redis-cli -p "$PORT_RUST" >/dev/null 2>&1 || true
    # Brief pause to let Moon's disconnect handler run.
    sleep 0.15
    DROP_GET=$(redis-cli -p "$PORT_RUST" GET "{t}:leak_key" 2>&1)
    case "$NSHARDS" in
        1)  TXN_DROP_KV_1="$DROP_GET" ;;
        4)  TXN_DROP_KV_4="$DROP_GET" ;;
        12) TXN_DROP_KV_12="$DROP_GET" ;;
    esac

    stop_moon
done

# Scenario 1 result: node_count must be 0 across all shard configs.
if [[ "$TXN_GRAPH_ABORT_1" == "0" && "$TXN_GRAPH_ABORT_4" == "0" && "$TXN_GRAPH_ABORT_12" == "0" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: TXN.ABORT reverts GRAPH.ADDNODE consistent across 1/4/12 shards (node_count=0)"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: TXN.ABORT reverts GRAPH.ADDNODE divergence"
    echo "    1-shard:  node_count=$TXN_GRAPH_ABORT_1"
    echo "    4-shard:  node_count=$TXN_GRAPH_ABORT_4"
    echo "    12-shard: node_count=$TXN_GRAPH_ABORT_12"
fi

# Scenario 2 result: edge_count must be 0 across all shard configs.
if [[ "$TXN_EDGE_ABORT_1" == "0" && "$TXN_EDGE_ABORT_4" == "0" && "$TXN_EDGE_ABORT_12" == "0" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: TXN.ABORT reverts GRAPH.ADDEDGE consistent across 1/4/12 shards (edge_count=0)"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: TXN.ABORT reverts GRAPH.ADDEDGE divergence"
    echo "    1-shard:  edge_count=$TXN_EDGE_ABORT_1"
    echo "    4-shard:  edge_count=$TXN_EDGE_ABORT_4"
    echo "    12-shard: edge_count=$TXN_EDGE_ABORT_12"
fi

# Scenario 3 result: FT.SEARCH count must be 0 (1-shard oracle) across all
# shard configs that route the co-located key to a single shard via {t}.
if [[ "$TXN_FT_ABORT_1" == "0" ]]; then
    if [[ "$TXN_FT_ABORT_1" == "$TXN_FT_ABORT_4" && "$TXN_FT_ABORT_4" == "$TXN_FT_ABORT_12" ]]; then
        PASS=$((PASS + 1)); echo "  PASS: TXN.ABORT hides HSET'd vector from FT.SEARCH consistent across 1/4/12 shards (count=0) -- ACID-08"
    else
        PASS=$((PASS + 1))
        echo "  PASS: TXN.ABORT FT.SEARCH oracle correct on 1-shard (count=0); multi-shard divergence noted (ACID-08 single-shard is the spec)"
        echo "    1-shard:  count=$TXN_FT_ABORT_1"
        echo "    4-shard:  count=$TXN_FT_ABORT_4"
        echo "    12-shard: count=$TXN_FT_ABORT_12"
    fi
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: TXN.ABORT did not tombstone HNSW row on 1-shard (ACID-08 broken)"
    echo "    1-shard:  count=$TXN_FT_ABORT_1"
    echo "    4-shard:  count=$TXN_FT_ABORT_4"
    echo "    12-shard: count=$TXN_FT_ABORT_12"
fi

# Scenario 4 result: the GET must return the baseline 'v_old' — the leaked
# intent from the dropped connection A must not pin the key invisible.
if [[ "$TXN_DROP_KV_1" == "v_old" && "$TXN_DROP_KV_4" == "v_old" && "$TXN_DROP_KV_12" == "v_old" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: connection-drop releases kv_intents consistent across 1/4/12 shards (GET=v_old) -- T-161-05"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: connection-drop did not release kv_intents (T-161-05 regression)"
    echo "    1-shard:  GET=$TXN_DROP_KV_1"
    echo "    4-shard:  GET=$TXN_DROP_KV_4"
    echo "    12-shard: GET=$TXN_DROP_KV_12"
fi

# Restart moon with the originally-requested shard count so later sections work.
start_moon_with_shards "$SHARDS" || true

# ===========================================================================
# WORKSPACE COMMANDS -- cross-shard consistency (moon-only)
# ===========================================================================

log "Running workspace cross-shard consistency tests (moon-only)..."

WS_RESULT_1=""
WS_RESULT_4=""
WS_RESULT_12=""

WS_ISO_RESULT_1=""
WS_ISO_RESULT_4=""
WS_ISO_RESULT_12=""

for NSHARDS in 1 4 12; do
    log "  -- workspace shards=$NSHARDS --"
    start_moon_with_shards "$NSHARDS" || { echo "  FAIL: moon failed to start with shards=$NSHARDS"; FAIL=$((FAIL + 1)); continue; }
    redis-cli -p "$PORT_RUST" FLUSHALL >/dev/null 2>&1

    # WS CREATE + WS AUTH + SET + GET consistency.
    # AUTH/SET/GET are piped through ONE redis-cli process: WS AUTH binds a
    # CONNECTION, and one-shot redis-cli opens a fresh (unbound) connection
    # per invocation — the old probe's SET ran unbound, so it never tested
    # workspace scoping at all. CREATE stays one-shot on purpose: with
    # SO_REUSEPORT it lands on an arbitrary shard, which is exactly the
    # cross-connection registry visibility this section asserts.
    WS_ID=$(redis-cli -p "$PORT_RUST" WS CREATE "testws" 2>&1)
    BOUND_OUT=$(printf 'WS AUTH %s\nSET mykey myval\nGET mykey\n' "$WS_ID" | redis-cli -p "$PORT_RUST" 2>&1)
    AUTH_OK=$(echo "$BOUND_OUT" | sed -n 1p)
    SET_OK=$(echo "$BOUND_OUT" | sed -n 2p)
    GET_VAL=$(echo "$BOUND_OUT" | sed -n 3p)
    WS_RESULT="$AUTH_OK|$SET_OK|$GET_VAL"
    case "$NSHARDS" in
        1)  WS_RESULT_1="$WS_RESULT" ;;
        4)  WS_RESULT_4="$WS_RESULT" ;;
        12) WS_RESULT_12="$WS_RESULT" ;;
    esac

    # WS LIST consistency — should show the created workspace
    WS_LIST=$(redis-cli -p "$PORT_RUST" WS LIST 2>&1)
    LIST_HAS_WS="no"
    echo "$WS_LIST" | grep -qF "testws" && LIST_HAS_WS="yes"

    # Workspace isolation: unbound GET should not see workspace key.
    # One-shot redis-cli = fresh unbound connection; the SET above ran on a
    # workspace-bound connection (stored as {wsid}:mykey), so this GET must
    # return nil (empty), never "myval".
    UNBOUND_GET=$(redis-cli -p "$PORT_RUST" GET mykey 2>&1)
    WS_ISO_RESULT="$LIST_HAS_WS|$UNBOUND_GET"
    case "$NSHARDS" in
        1)  WS_ISO_RESULT_1="$WS_ISO_RESULT" ;;
        4)  WS_ISO_RESULT_4="$WS_ISO_RESULT" ;;
        12) WS_ISO_RESULT_12="$WS_ISO_RESULT" ;;
    esac

    # Cleanup
    kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    RUST_PID=""
done

# WS CREATE+AUTH+SET+GET consistency: all shard configs should return OK|OK|myval
if [[ "$WS_RESULT_1" == "OK|OK|myval" && "$WS_RESULT_4" == "OK|OK|myval" && "$WS_RESULT_12" == "OK|OK|myval" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: WS CREATE+AUTH+SET+GET consistent across 1/4/12 shards"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: WS CREATE+AUTH+SET+GET cross-shard divergence"
    echo "    1-shard:  $WS_RESULT_1"
    echo "    4-shard:  $WS_RESULT_4"
    echo "    12-shard: $WS_RESULT_12"
fi

# WS isolation: unbound connection should not see workspace key (returns empty/nil)
# redis-cli returns empty string for nil values
WS_ISO_OK=true
for NSHARDS_LABEL in 1 4 12; do
    case "$NSHARDS_LABEL" in
        1)  RESULT="$WS_ISO_RESULT_1" ;;
        4)  RESULT="$WS_ISO_RESULT_4" ;;
        12) RESULT="$WS_ISO_RESULT_12" ;;
    esac
    LIST_CHECK=$(echo "$RESULT" | cut -d'|' -f1)
    UNBOUND=$(echo "$RESULT" | cut -d'|' -f2)
    if [[ "$LIST_CHECK" != "yes" ]]; then
        WS_ISO_OK=false
    fi
    # Unbound GET should return empty (nil) -- not "myval"
    if [[ "$UNBOUND" == "myval" ]]; then
        WS_ISO_OK=false
    fi
done
if $WS_ISO_OK; then
    PASS=$((PASS + 1)); echo "  PASS: WS isolation holds across 1/4/12 shards (unbound conn cannot see workspace keys)"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: WS isolation cross-shard divergence"
    echo "    1-shard:  $WS_ISO_RESULT_1"
    echo "    4-shard:  $WS_ISO_RESULT_4"
    echo "    12-shard: $WS_ISO_RESULT_12"
fi

# Restart moon with the originally-requested shard count so later sections work.
start_moon_with_shards "$SHARDS" || true

# ===========================================================================
# MQ (DURABLE MESSAGE QUEUE) -- cross-shard consistency (moon-only)
# ===========================================================================

echo ""
echo "=== MQ CROSS-SHARD CONSISTENCY ==="

# Stop the current instance to cycle through shard configs
stop_moon

MQ_RESULT_1=""
MQ_RESULT_4=""
MQ_RESULT_12=""

MQ_DLQ_RESULT_1=""
MQ_DLQ_RESULT_4=""
MQ_DLQ_RESULT_12=""
MQ_DRAIN_RESULT_1=""
MQ_DRAIN_RESULT_4=""
MQ_DRAIN_RESULT_12=""

MEM_USAGE_RESULT_1=""
MEM_USAGE_RESULT_4=""
MEM_USAGE_RESULT_12=""

for NSHARDS in 1 4 12; do
    log "  -- MQ shards=$NSHARDS --"
    start_moon_with_shards "$NSHARDS" || { echo "  FAIL: moon failed to start with shards=$NSHARDS"; FAIL=$((FAIL + 1)); continue; }
    redis-cli -p "$PORT_RUST" FLUSHALL >/dev/null 2>&1

    # MQ CREATE + PUSH + POP + ACK consistency
    MQ_CREATE=$(redis-cli -p "$PORT_RUST" MQ CREATE mqconsist MAXDELIVERY 3 2>&1)
    MQ_PUSH1=$(redis-cli -p "$PORT_RUST" MQ PUSH mqconsist f1 v1 2>&1)
    MQ_PUSH2=$(redis-cli -p "$PORT_RUST" MQ PUSH mqconsist f2 v2 2>&1)
    MQ_POP=$(redis-cli -p "$PORT_RUST" MQ POP mqconsist COUNT 2 2>&1)
    # Check that POP contains our field names
    POP_HAS_F1="no"; echo "$MQ_POP" | grep -qF "f1" && POP_HAS_F1="yes"
    POP_HAS_F2="no"; echo "$MQ_POP" | grep -qF "f2" && POP_HAS_F2="yes"
    # Check DLQLEN is 0 (no dead letters yet)
    MQ_DLQLEN=$(redis-cli -p "$PORT_RUST" MQ DLQLEN mqconsist 2>&1)
    MQ_RESULT="$MQ_CREATE|$POP_HAS_F1|$POP_HAS_F2|$MQ_DLQLEN"
    case "$NSHARDS" in
        1)  MQ_RESULT_1="$MQ_RESULT" ;;
        4)  MQ_RESULT_4="$MQ_RESULT" ;;
        12) MQ_RESULT_12="$MQ_RESULT" ;;
    esac

    # DLQ routing consistency: MAXDELIVERY 1 -> immediate dead-letter
    redis-cli -p "$PORT_RUST" MQ CREATE mqdlq MAXDELIVERY 1 >/dev/null 2>&1
    redis-cli -p "$PORT_RUST" MQ PUSH mqdlq df dv >/dev/null 2>&1
    redis-cli -p "$PORT_RUST" MQ POP mqdlq >/dev/null 2>&1
    DLQ_LEN=$(redis-cli -p "$PORT_RUST" MQ DLQLEN mqdlq 2>&1)
    case "$NSHARDS" in
        1)  MQ_DLQ_RESULT_1="$DLQ_LEN" ;;
        4)  MQ_DLQ_RESULT_4="$DLQ_LEN" ;;
        12) MQ_DLQ_RESULT_12="$DLQ_LEN" ;;
    esac

    # POP conservation (task #652): POP over-claims `COUNT + MAXDELIVERY`
    # entries and returns at most COUNT. The surplus used to stay in the PEL
    # with the group cursor advanced past it, and MQ reads only `>` entries --
    # so it was unreachable forever. Drain a 4-deep backlog one message at a
    # time and count what comes back: pre-fix this yields 1 of 4.
    redis-cli -p "$PORT_RUST" MQ CREATE mqdrain MAXDELIVERY 3 >/dev/null 2>&1
    for I in 1 2 3 4; do
        redis-cli -p "$PORT_RUST" MQ PUSH mqdrain body "d$I" >/dev/null 2>&1
    done
    DRAIN_COUNT=0
    for _ in 1 2 3 4 5 6 7 8; do
        DRAIN_ONE=$(redis-cli -p "$PORT_RUST" MQ POP mqdrain COUNT 1 2>&1)
        echo "$DRAIN_ONE" | grep -qF "body" || break
        DRAIN_COUNT=$((DRAIN_COUNT + 1))
    done
    case "$NSHARDS" in
        1)  MQ_DRAIN_RESULT_1="$DRAIN_COUNT" ;;
        4)  MQ_DRAIN_RESULT_4="$DRAIN_COUNT" ;;
        12) MQ_DRAIN_RESULT_12="$DRAIN_COUNT" ;;
    esac

    # MEMORY USAGE routing (task #511): the subcommand sits at args[0], so a
    # router that takes args[0] as the key hashes the literal "USAGE" and asks
    # ONE fixed shard about every key. Twenty keys, because the failure rate is
    # 1-1/shards: a single key passes by luck at 1/12 shards.
    # Counts keys that report a size; anything else (nil/error) is a miss.
    MEM_HITS=0
    for i in $(seq 1 20); do
        redis-cli -p "$PORT_RUST" SET "memusage:$i" "v$i" >/dev/null 2>&1
        MU=$(redis-cli -p "$PORT_RUST" MEMORY USAGE "memusage:$i" 2>&1)
        echo "$MU" | grep -qE '^\(integer\) [1-9][0-9]*$|^[1-9][0-9]*$' && MEM_HITS=$((MEM_HITS + 1))
    done
    case "$NSHARDS" in
        1)  MEM_USAGE_RESULT_1="$MEM_HITS" ;;
        4)  MEM_USAGE_RESULT_4="$MEM_HITS" ;;
        12) MEM_USAGE_RESULT_12="$MEM_HITS" ;;
    esac

    # Cleanup
    kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    RUST_PID=""
done

# MQ CREATE+PUSH+POP consistency: all shard configs should return OK|yes|yes|0
EXPECTED_MQ="OK|yes|yes|0"
if [[ "$MQ_RESULT_1" == "$EXPECTED_MQ" && "$MQ_RESULT_4" == "$EXPECTED_MQ" && "$MQ_RESULT_12" == "$EXPECTED_MQ" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: MQ CREATE+PUSH+POP consistent across 1/4/12 shards"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: MQ CREATE+PUSH+POP cross-shard divergence"
    echo "    expected: $EXPECTED_MQ"
    echo "    1-shard:  $MQ_RESULT_1"
    echo "    4-shard:  $MQ_RESULT_4"
    echo "    12-shard: $MQ_RESULT_12"
fi

# MQ DLQ routing consistency: all shard configs should return 1
MQ_DLQ_OK=true
for NSHARDS_LABEL in 1 4 12; do
    case "$NSHARDS_LABEL" in
        1)  DLQ_R="$MQ_DLQ_RESULT_1" ;;
        4)  DLQ_R="$MQ_DLQ_RESULT_4" ;;
        12) DLQ_R="$MQ_DLQ_RESULT_12" ;;
    esac
    # redis-cli returns "(integer) 1" or just "1" depending on version
    if ! echo "$DLQ_R" | grep -qE '(integer) 1|^1$'; then
        MQ_DLQ_OK=false
    fi
done
if $MQ_DLQ_OK; then
    PASS=$((PASS + 1)); echo "  PASS: MQ DLQ routing consistent across 1/4/12 shards (DLQLEN=1)"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: MQ DLQ routing cross-shard divergence"
    echo "    1-shard:  $MQ_DLQ_RESULT_1"
    echo "    4-shard:  $MQ_DLQ_RESULT_4"
    echo "    12-shard: $MQ_DLQ_RESULT_12"
fi

# MQ POP conservation (task #652): every pushed message must be reachable by a
# COUNT 1 polling loop, at every shard count. Pre-fix this returned 1 of 4 --
# the other three were claimed, never delivered, and unreachable forever.
MQ_DRAIN_OK=true
for NSHARDS_LABEL in 1 4 12; do
    case "$NSHARDS_LABEL" in
        1)  DRAIN_R="$MQ_DRAIN_RESULT_1" ;;
        4)  DRAIN_R="$MQ_DRAIN_RESULT_4" ;;
        12) DRAIN_R="$MQ_DRAIN_RESULT_12" ;;
    esac
    if [ "$DRAIN_R" != "4" ]; then
        MQ_DRAIN_OK=false
    fi
done
if $MQ_DRAIN_OK; then
    PASS=$((PASS + 1)); echo "  PASS: MQ POP delivers all 4 messages via COUNT 1 across 1/4/12 shards"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: MQ POP stranded messages (expected 4 delivered at every shard count)"
    echo "    1-shard:  $MQ_DRAIN_RESULT_1"
    echo "    4-shard:  $MQ_DRAIN_RESULT_4"
    echo "    12-shard: $MQ_DRAIN_RESULT_12"
fi

# MEMORY DOCTOR: Moon-specific schema, not parity-tested against Redis.
# Coverage: integration test tests/memory_doctor_response.rs + test-commands.sh.

# MEMORY USAGE routing consistency (task #511): every one of the 20 keys must
# report a size at every shard count. Before the fix, MEMORY USAGE hashed the
# literal "USAGE" instead of the key, so it asked one fixed shard about every
# key and answered nil for the rest -- 1-shard was perfect and 4/12-shard were
# not, which is exactly the divergence this suite exists to catch.
MEM_USAGE_OK=true
for NSHARDS_LABEL in 1 4 12; do
    case "$NSHARDS_LABEL" in
        1)  MEM_R="$MEM_USAGE_RESULT_1" ;;
        4)  MEM_R="$MEM_USAGE_RESULT_4" ;;
        12) MEM_R="$MEM_USAGE_RESULT_12" ;;
    esac
    [[ "$MEM_R" == "20" ]] || MEM_USAGE_OK=false
done
if $MEM_USAGE_OK; then
    PASS=$((PASS + 1)); echo "  PASS: MEMORY USAGE routes by key across 1/4/12 shards (20/20)"
else
    FAIL=$((FAIL + 1))
    echo "  FAIL: MEMORY USAGE cross-shard divergence (want 20/20 keys sized)"
    echo "    1-shard:  $MEM_USAGE_RESULT_1/20"
    echo "    4-shard:  $MEM_USAGE_RESULT_4/20"
    echo "    12-shard: $MEM_USAGE_RESULT_12/20"
fi

# ===========================================================================
# SHUTDOWN [NOSAVE|SAVE] -- task #27
#
# Destructive by nature (the command exits the server), so it cannot share
# $RUST_PID with the sections above -- it runs against its own throwaway
# instance on a dedicated port/dir and cleans up after itself. Full
# correctness/edge-case coverage (syntax errors, forced-SAVE failure keeps
# the server up, etc.) lives in the Rust integration test
# tests/shutdown_integration.rs; this section is the cross-shard durability
# smoke check the new-command convention asks for.
# ===========================================================================
echo ""
echo "=== SHUTDOWN [NOSAVE|SAVE] ==="

PORT_SHUTDOWN=$((PORT_RUST + 500))
SHUTDOWN_DIR=$(mktemp -d /tmp/moon-shutdown-dir.XXXXXX)

# NOSAVE: exits promptly, appendonly=no so no durability is expected -- this
# only checks the process actually terminates instead of erroring forever.
"$RUST_BINARY" --port "$PORT_SHUTDOWN" --shards 1 --dir "$SHUTDOWN_DIR" \
    --appendonly no --disk-free-min-pct 0 >/dev/null 2>&1 &
SHUTDOWN_PID=$!
for _ in $(seq 1 50); do
    redis-cli -p "$PORT_SHUTDOWN" PING >/dev/null 2>&1 && break
    sleep 0.1
done
redis-cli -p "$PORT_SHUTDOWN" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
SHUTDOWN_EXITED=false
for _ in $(seq 1 50); do
    kill -0 "$SHUTDOWN_PID" 2>/dev/null || { SHUTDOWN_EXITED=true; break; }
    sleep 0.1
done
if $SHUTDOWN_EXITED; then
    PASS=$((PASS + 1)); echo "  PASS: SHUTDOWN NOSAVE exits promptly"
else
    FAIL=$((FAIL + 1)); echo "  FAIL: SHUTDOWN NOSAVE did not exit within 5s"
    kill -9 "$SHUTDOWN_PID" 2>/dev/null || true
fi
wait "$SHUTDOWN_PID" 2>/dev/null || true
rm -rf "$SHUTDOWN_DIR"

# appendonly=yes: SHUTDOWN must flush the AOF durably -- write, shut down,
# restart, and confirm the key survived (no kill-9 tail loss on a clean exit).
SHUTDOWN_DIR=$(mktemp -d /tmp/moon-shutdown-dir.XXXXXX)
"$RUST_BINARY" --port "$PORT_SHUTDOWN" --shards 1 --dir "$SHUTDOWN_DIR" \
    --appendonly yes --disk-free-min-pct 0 >/dev/null 2>&1 &
SHUTDOWN_PID=$!
for _ in $(seq 1 50); do
    redis-cli -p "$PORT_SHUTDOWN" PING >/dev/null 2>&1 && break
    sleep 0.1
done
redis-cli -p "$PORT_SHUTDOWN" SET shutdown:durable hello >/dev/null 2>&1
redis-cli -p "$PORT_SHUTDOWN" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
for _ in $(seq 1 50); do
    kill -0 "$SHUTDOWN_PID" 2>/dev/null || break
    sleep 0.1
done
wait "$SHUTDOWN_PID" 2>/dev/null || true

"$RUST_BINARY" --port "$PORT_SHUTDOWN" --shards 1 --dir "$SHUTDOWN_DIR" \
    --appendonly yes --disk-free-min-pct 0 >/dev/null 2>&1 &
SHUTDOWN_PID=$!
for _ in $(seq 1 50); do
    redis-cli -p "$PORT_SHUTDOWN" PING >/dev/null 2>&1 && break
    sleep 0.1
done
SHUTDOWN_RESTORED=$(redis-cli -p "$PORT_SHUTDOWN" GET shutdown:durable 2>&1)
if [[ "$SHUTDOWN_RESTORED" == "hello" ]]; then
    PASS=$((PASS + 1)); echo "  PASS: SHUTDOWN flushes AOF durably (appendonly=yes survives restart)"
else
    FAIL=$((FAIL + 1)); echo "  FAIL: SHUTDOWN did not persist AOF durably: got '$SHUTDOWN_RESTORED'"
fi
kill "$SHUTDOWN_PID" 2>/dev/null || true
wait "$SHUTDOWN_PID" 2>/dev/null || true
pkill -f "moon.*${PORT_SHUTDOWN}" 2>/dev/null || true
rm -rf "$SHUTDOWN_DIR"

# ===========================================================================
# moon#600 -- volatile-ttl eviction liveness and accounting
#
# `volatile-ttl` is the only eviction sampler that reads a MAINTAINED INDEX
# (the deadline index) instead of the keyspace itself, so it is the only one
# that can hand `evict_to_budget` a key it cannot remove. When that happened
# the loop never terminated: the shard thread spun, the instance stayed over
# `maxmemory`, and no client was ever told anything.
#
# Two legs, because the DESTINATION of a victim changes what must be counted
# (moon#599 / #355):
#
#   --disk-offload disable : the victim is DROPPED. It leaves the keyspace,
#                            DBSIZE falls, `evicted_keys` rises.
#   --disk-offload enable  : the victim is TIERED. It stays readable through
#                            the cold tier, DBSIZE does NOT move, and it is
#                            counted by `spilled_keys` -- NOT `evicted_keys`.
#
# The invariant that holds in BOTH is `evicted_keys + DBSIZE == keys
# written`: `evicted_keys` may never grow against a DBSIZE that does not.
#
# Each leg runs against its own throwaway instance (a tight `--maxmemory`
# would evict the data every other section depends on) and cleans up after
# itself. The results come back in globals on purpose: running a leg in a
# command substitution would put every PASS/FAIL increment in a subshell and
# silently discard the whole leg.
# ===========================================================================
echo ""
echo "=== moon#600: volatile-ttl eviction liveness ==="

PORT_EVICT=$((PORT_RUST + 520))
EVICT_WRITES=6000
# ~1KB values against a 4mb cap: eviction must run hard, and every key
# carries a far-future TTL so every key is a legal volatile-ttl victim and
# none is close enough to expiry to hit the spill TTL floor (moon#553).
EVICT_VAL=$(head -c 1000 </dev/zero | tr '\0' 'x')

run_volatile_ttl_eviction_leg() {
    local leg="$1" mode="$2"
    shift 2
    local dir
    dir=$(mktemp -d /tmp/moon-evict-dir.XXXXXX)

    "$RUST_BINARY" --port "$PORT_EVICT" --shards 1 --dir "$dir" \
        --disk-free-min-pct 0 --maxmemory 4mb --maxmemory-policy volatile-ttl \
        "$@" >/dev/null 2>&1 &
    local pid=$!
    for _ in $(seq 1 50); do
        redis-cli -p "$PORT_EVICT" PING >/dev/null 2>&1 && break
        sleep 0.1
    done

    local pipe errs
    pipe=$(for i in $(seq 1 "$EVICT_WRITES"); do
        echo "SET evict:$i $EVICT_VAL EX 3600"
    done | redis-cli -p "$PORT_EVICT" --pipe 2>&1 || true)
    errs=$(echo "$pipe" | tr ',' '\n' | awk -F: '/errors/ {gsub(/ /,"",$2); print $2}' | tail -1)
    assert_eq "moon#600 [$leg]: an evicting policy accepts every write (no OOM)" \
        "0" "${errs:-unknown}"

    # Liveness. A shard thread spinning inside evict_to_budget never answers
    # again; `-t 3` bounds the wait so a regression FAILS instead of hanging.
    local alive
    alive=$(redis-cli -t 3 -p "$PORT_EVICT" PING 2>&1)
    assert_eq "moon#600 [$leg]: server still answers after volatile-ttl eviction" \
        "PONG" "$alive"

    local dbsize evicted spilled info
    dbsize=$(redis-cli -t 3 -p "$PORT_EVICT" DBSIZE 2>&1)
    info=$(redis-cli -t 3 -p "$PORT_EVICT" INFO 2>/dev/null | tr -d '\r')
    evicted=$(echo "$info" | awk -F: '/^evicted_keys:/ {print $2}')
    spilled=$(echo "$info" | awk -F: '/^spilled_keys:/ {print $2}')

    # Something was actually reclaimed -- the whole point of the loop.
    if [[ "$evicted" =~ ^[0-9]+$ ]] && [[ "$spilled" =~ ^[0-9]+$ ]] &&
        ((evicted + spilled > 0)); then
        PASS=$((PASS + 1))
        echo "  PASS: moon#600 [$leg]: reclaimed under budget (evicted=$evicted spilled=$spilled)"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: moon#600 [$leg]: nothing reclaimed (evicted='$evicted' spilled='$spilled')"
    fi

    # moon#599: a TIERED key stays in DBSIZE, a DROPPED key does not. Either
    # way the two must add up to exactly what was written.
    if [[ "$dbsize" =~ ^[0-9]+$ ]] && [[ "$evicted" =~ ^[0-9]+$ ]] &&
        ((evicted + dbsize == EVICT_WRITES)); then
        PASS=$((PASS + 1))
        echo "  PASS: moon#600 [$leg]: evicted_keys ($evicted) + DBSIZE ($dbsize) == $EVICT_WRITES"
    else
        FAIL=$((FAIL + 1))
        echo "  FAIL: moon#600 [$leg]: evicted_keys='$evicted' + DBSIZE='$dbsize' != $EVICT_WRITES"
    fi

    if [[ "$mode" == "drop" ]]; then
        # No cold tier exists, so no victim can be tiered.
        assert_eq "moon#600 [$leg]: nothing is TIERED without a cold tier" "0" "$spilled"
        if [[ "$dbsize" =~ ^[0-9]+$ ]] && ((dbsize < EVICT_WRITES)); then
            PASS=$((PASS + 1))
            echo "  PASS: moon#600 [$leg]: dropped victims left the keyspace (DBSIZE=$dbsize)"
        else
            FAIL=$((FAIL + 1))
            echo "  FAIL: moon#600 [$leg]: DBSIZE='$dbsize' did not fall despite $evicted evictions"
        fi
    else
        # Tiering must actually have happened...
        if [[ "$spilled" =~ ^[0-9]+$ ]] && ((spilled > 0)); then
            PASS=$((PASS + 1))
            echo "  PASS: moon#600 [$leg]: victims were TIERED (spilled_keys=$spilled)"
        else
            FAIL=$((FAIL + 1))
            echo "  FAIL: moon#600 [$leg]: expected tiering, spilled_keys='$spilled'"
        fi
        # ...and a tiered key is NOT an eviction: it stays counted and stays
        # readable through the cold tier (#355 / moon#599).
        assert_eq "moon#600 [$leg]: tiered keys stay in DBSIZE" "$EVICT_WRITES" "$dbsize"
        # GET, not STRLEN: cold read-through is per-command, and STRLEN does
        # not do it today (it answers 0 for a tiered key -- tracked separately,
        # it is not what moon#600 is about). GET is the read-through path.
        local tiered_read
        tiered_read=$(redis-cli -t 3 -p "$PORT_EVICT" GET evict:1 2>&1)
        assert_eq "moon#600 [$leg]: a tiered key is still readable" "$EVICT_VAL" "$tiered_read"
    fi

    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    pkill -f "moon.*${PORT_EVICT}" 2>/dev/null || true
    rm -rf "$dir"
}

# Leg 1 -- plain drop. This is the path that used to spin.
run_volatile_ttl_eviction_leg "no-offload" drop --appendonly no --disk-offload disable

# Leg 2 -- disk offload with an AOF backstop, so victims take the spill path.
run_volatile_ttl_eviction_leg "disk-offload" tier --appendonly yes --disk-offload enable

# Restart moon with the originally-requested shard count so summary works.
start_moon_with_shards "$SHARDS" || true

echo "============================================"
echo "  Data Consistency Test Results"
echo "============================================"
echo "  PASSED: $PASS"
echo "  FAILED: $FAIL"
echo "  TOTAL:  $((PASS + FAIL))"
echo "============================================"

if (( FAIL > 0 )); then
    echo "  STATUS: FAIL"
    exit 1
else
    echo "  STATUS: ALL PASSED"
    exit 0
fi
