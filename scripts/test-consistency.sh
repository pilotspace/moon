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

PORT_REDIS=6399
PORT_RUST=6400
SHARDS=1
SKIP_BUILD=false
RUST_BINARY="./target/release/moon"
PASS=0
FAIL=0
RUST_PID=""
REDIS_PID=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --shards)     SHARDS="$2"; shift 2 ;;
        --skip-build) SKIP_BUILD=true; shift ;;
        --port-rust)  PORT_RUST="$2"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

log() { echo "[$(date '+%H:%M:%S')] $*"; }

cleanup() {
    [[ -n "${RUST_PID:-}" ]] && kill "$RUST_PID" 2>/dev/null; wait "$RUST_PID" 2>/dev/null || true
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null; wait "$REDIS_PID" 2>/dev/null || true
    pkill -f "redis-server.*${PORT_REDIS}" 2>/dev/null || true
    pkill -f "moon.*${PORT_RUST}" 2>/dev/null || true
}
trap cleanup EXIT

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
"$RUST_BINARY" --port "$PORT_RUST" --shards "$SHARDS" &>/dev/null &
RUST_PID=$!

wait_for_port "$PORT_REDIS"
wait_for_port "$PORT_RUST"
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
assert_both "ZRANGE 0 -1" ZRANGE z:test 0 -1
assert_both "ZRANGE WITHSCORES" ZRANGE z:test 0 -1 WITHSCORES
assert_both "ZRANGEBYSCORE 1 3" ZRANGEBYSCORE z:test 1 3

both ZINCRBY z:test 10 "delta"
assert_both "ZINCRBY then ZSCORE" ZSCORE z:test delta

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
redis_db=$(redis-cli -p "$PORT_REDIS" DBSIZE 2>&1 | grep -oE '[0-9]+')
rust_db=$(redis-cli -p "$PORT_RUST" DBSIZE 2>&1 | grep -oE '[0-9]+')
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
assert_both "SORT STORE" SORT edge:sortl STORE edge:sorted
assert_both "SORT STORE result" LRANGE edge:sorted 0 -1

# GEOADD / GEOPOS / GEODIST / GEOHASH / GEOSEARCH
both GEOADD edge:geo 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
assert_both "GEOPOS" GEOPOS edge:geo Palermo
assert_both "GEOPOS missing" GEOPOS edge:geo NonExistent
assert_both "GEODIST m" GEODIST edge:geo Palermo Catania
assert_both "GEODIST km" GEODIST edge:geo Palermo Catania km
assert_both "GEOHASH" GEOHASH edge:geo Palermo
assert_both "GEOADD count" GEOADD edge:geo 2.349014 48.864716 Paris

# EXPIREAT / PEXPIREAT / EXPIRETIME / PEXPIRETIME
both SET edge:eat "val"
assert_both "EXPIREAT" EXPIREAT edge:eat 9999999999
assert_both "EXPIRETIME" EXPIRETIME edge:eat
assert_both "PEXPIRETIME" PEXPIRETIME edge:eat
assert_both "EXPIRETIME missing" EXPIRETIME edge:nokey
assert_both "PEXPIRETIME missing" PEXPIRETIME edge:nokey

# TOUCH
both SET edge:touch "val"
assert_both "TOUCH" TOUCH edge:touch
assert_both "TOUCH missing" TOUCH edge:nomiss

# FLUSHDB (run last — clears all keys)
assert_both "FLUSHDB" FLUSHDB

# ===========================================================================
# Summary
# ===========================================================================

echo ""
# ===========================================================================
# Vector Search (moon-only — FT.* not available in Redis)
# ===========================================================================
log "=== Vector Search (moon-only) ==="

# Create index on moon only
FT_CREATE=$(redis-cli -p "$PORT_RUST" FT.CREATE vecidx ON HASH PREFIX 1 vec: SCHEMA embedding VECTOR FLAT 6 DIM 4 DISTANCE_METRIC L2 TYPE FLOAT32 2>&1)
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
    "$RUST_BINARY" --port "$PORT_RUST" --shards "$nshards" &>/dev/null &
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
        TITLE="machine learning doc $i"
        # Deterministic vector: [i/30, 0, 0, 0] as f32 LE
        VECTOR=$(python3 -c "import struct,sys; v=$i/30.0; sys.stdout.buffer.write(struct.pack('<4f', v, 0.0, 0.0, 0.0))")
        redis-cli -p "$PORT_RUST" HSET cdoc:$i status "$STATUS" priority "$PRIORITY" title "$TITLE" vec "$VECTOR" >/dev/null 2>&1
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
    QVEC=$(python3 -c "import struct,sys; sys.stdout.buffer.write(struct.pack('<4f', 1.0, 0.0, 0.0, 0.0))")
    HYB_OUT=$(redis-cli -p "$PORT_RUST" FT.SEARCH cidx "machine learning" HYBRID VECTOR @vec '$q' FUSION RRF LIMIT 0 5 PARAMS 2 q "$QVEC" 2>&1)
    # Extract just the keys (cdoc:N lines) to compare top-K ordering.
    HYB_KEYS=$(printf '%s\n' "$HYB_OUT" | grep -oE 'cdoc:[0-9]+' | head -5 | tr '\n' ' ')
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
    NODE_ID=$(echo "$ADDNODE_OUT" | grep -oE '[0-9]+' | head -1)
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

# Restart moon with the originally-requested shard count so later sections work.
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
